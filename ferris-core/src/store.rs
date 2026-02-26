//! KeyStore: Sharded concurrent key-value storage
//!
//! Uses DashMap for lock-free concurrent access with per-shard RwLocks.
//! Operations on different keys in different shards are fully concurrent.
//!
//! # Memory Tracking
//!
//! The KeyStore integrates with `MemoryTracker` to track memory usage
//! and trigger eviction when needed. Memory is tracked at the database
//! level and aggregated in the shared tracker.

use crate::memory::{EvictionPolicy, MemoryTracker};
use crate::value::Entry;
use bytes::Bytes;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::Instant;

/// Number of databases (like Redis's SELECT)
pub const DEFAULT_NUM_DATABASES: usize = 16;

/// Default number of samples for LRU/LFU eviction
pub const DEFAULT_EVICTION_SAMPLES: usize = 5;

/// Result of a set operation that may require eviction
#[derive(Debug)]
pub enum SetResult {
    /// Set succeeded
    Ok,
    /// Out of memory and noeviction policy
    OutOfMemory,
}

/// Action to take after an update operation
#[derive(Debug)]
pub enum UpdateAction {
    /// Keep the entry as-is (modified in place), recalculate memory
    Keep,
    /// Keep the entry with a specific memory delta (positive = added, negative = removed)
    /// Use this for collections where memory_usage() is O(n) to avoid expensive recalculation
    KeepWithDelta(isize),
    /// Replace with a new entry
    Set(Entry),
    /// Delete the key
    Delete,
}

/// The main key-value store with multiple databases
#[derive(Debug)]
pub struct KeyStore {
    databases: Vec<Database>,
    /// Shared memory tracker for all databases
    memory_tracker: Arc<MemoryTracker>,
}

/// A single database (one of the 16 default databases)
#[derive(Debug, Clone)]
pub struct Database {
    data: Arc<DashMap<Bytes, Entry>>,
    /// Reference to the shared memory tracker
    memory_tracker: Arc<MemoryTracker>,
}

impl Default for Database {
    fn default() -> Self {
        Self::new(Arc::new(MemoryTracker::new()))
    }
}

impl Database {
    /// Create a new empty database with a memory tracker
    #[must_use]
    pub fn new(memory_tracker: Arc<MemoryTracker>) -> Self {
        Self {
            data: Arc::new(DashMap::new()),
            memory_tracker,
        }
    }

    /// Calculate the memory size of a key-entry pair
    #[inline]
    fn entry_size(key: &[u8], entry: &Entry) -> usize {
        // Key length + key overhead + entry memory usage
        key.len() + 32 + entry.memory_usage()
    }

    /// Get an entry by key, updating LRU/LFU metadata
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<Entry> {
        // Use get_mut to update access time in place
        self.data.get_mut(key).map(|mut r| {
            r.touch();
            r.value().clone()
        })
    }

    /// Get an entry by key without updating LRU/LFU metadata
    /// Use this for internal operations that shouldn't affect eviction order
    #[must_use]
    pub fn get_no_touch(&self, key: &[u8]) -> Option<Entry> {
        self.data.get(key).map(|r| r.value().clone())
    }

    /// Set an entry, tracking memory usage
    /// Automatically increments the version for optimistic locking (WATCH)
    pub fn set(&self, key: Bytes, mut entry: Entry) {
        use dashmap::mapref::entry::Entry as DashEntry;

        // Increment version for optimistic locking (WATCH command)
        entry.increment_version();

        let new_size = Self::entry_size(&key, &entry);

        // Use entry API for single lock acquisition (atomic get+set)
        match self.data.entry(key.clone()) {
            DashEntry::Occupied(mut occupied) => {
                // Key exists - calculate old size and replace
                let old_size = Self::entry_size(&key, occupied.get());
                occupied.insert(entry);
                // Update memory tracking
                self.memory_tracker.subtract(old_size);
                self.memory_tracker.add(new_size);
            }
            DashEntry::Vacant(vacant) => {
                // Key doesn't exist - just insert
                vacant.insert(entry);
                self.memory_tracker.add(new_size);
            }
        }
    }

    /// Set an entry without tracking memory (for internal operations like swap)
    pub(crate) fn set_no_track(&self, key: Bytes, entry: Entry) {
        self.data.insert(key, entry);
    }

    /// Set an entry with eviction if needed
    /// Returns `SetResult::OutOfMemory` if eviction fails and policy is `NoEviction`
    /// Automatically increments the version for optimistic locking (WATCH)
    pub fn set_with_eviction(&self, key: Bytes, mut entry: Entry) -> SetResult {
        // Increment version for optimistic locking (WATCH command)
        entry.increment_version();

        let new_size = Self::entry_size(&key, &entry);

        // Check if we're overwriting an existing key
        let old_size = self
            .data
            .get(&key)
            .map(|r| Self::entry_size(&key, r.value()))
            .unwrap_or(0);

        let net_increase = new_size.saturating_sub(old_size);

        // Check if we need to evict
        if self.memory_tracker.should_evict(net_increase) {
            let policy = self.memory_tracker.eviction_policy();

            if policy == EvictionPolicy::NoEviction {
                return SetResult::OutOfMemory;
            }

            // Try to free enough memory
            let bytes_to_free = self.memory_tracker.bytes_to_free(net_increase);
            let mut freed = 0;

            while freed < bytes_to_free {
                if let Some((victim_key, victim_size)) = self.select_victim(policy) {
                    if self.delete_internal(&victim_key) {
                        self.memory_tracker.record_eviction();
                        freed += victim_size;
                    }
                } else {
                    // No more victims available
                    if self.memory_tracker.should_evict(net_increase) {
                        return SetResult::OutOfMemory;
                    }
                    break;
                }
            }
        }

        // Now set the value
        self.set(key, entry);
        SetResult::Ok
    }

    /// Delete a key, returning true if it existed
    pub fn delete(&self, key: &[u8]) -> bool {
        if let Some((k, entry)) = self.data.remove(key) {
            let size = Self::entry_size(&k, &entry);
            self.memory_tracker.subtract(size);
            true
        } else {
            false
        }
    }

    /// Delete a key without tracking memory (internal use)
    fn delete_internal(&self, key: &[u8]) -> bool {
        if let Some((k, entry)) = self.data.remove(key) {
            let size = Self::entry_size(&k, &entry);
            self.memory_tracker.subtract(size);
            true
        } else {
            false
        }
    }

    /// Check if a key exists (does not update LRU/LFU)
    #[must_use]
    pub fn exists(&self, key: &[u8]) -> bool {
        self.data.contains_key(key)
    }

    /// Get the number of keys in this database
    #[must_use]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if the database is empty
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Clear all keys from this database
    pub fn flush(&self) {
        // Calculate total memory to subtract
        let total_size: usize = self
            .data
            .iter()
            .map(|r| Self::entry_size(r.key(), r.value()))
            .sum();

        self.data.clear();
        self.memory_tracker.subtract(total_size);
    }

    /// Clear all keys without tracking memory (for internal operations like swap)
    pub(crate) fn flush_no_track(&self) {
        self.data.clear();
    }

    /// Get all keys matching a pattern (for KEYS command)
    /// Note: This is O(n) and should be used carefully
    #[must_use]
    pub fn keys(&self, pattern: &str) -> Vec<Bytes> {
        self.data
            .iter()
            .filter(|r| glob_match::glob_match(pattern, &String::from_utf8_lossy(r.key())))
            .map(|r| r.key().clone())
            .collect()
    }

    /// Sample up to `count` random keys from the database
    /// Returns keys with their entries for eviction selection
    #[must_use]
    pub fn sample_keys(&self, count: usize) -> Vec<(Bytes, Entry)> {
        // DashMap iteration order is arbitrary, which gives us pseudo-random sampling
        self.data
            .iter()
            .take(count)
            .map(|r| (r.key().clone(), r.value().clone()))
            .collect()
    }

    /// Sample up to `count` keys that have an expiry set.
    /// Returns keys with their entries for eviction selection
    #[must_use]
    pub fn sample_volatile_keys(&self, count: usize) -> Vec<(Bytes, bool)> {
        let mut result = Vec::with_capacity(count);
        let mut seen = 0;
        for entry_ref in self.data.iter() {
            if seen >= count {
                break;
            }
            if entry_ref.value().expires_at.is_some() {
                let expired = entry_ref.value().is_expired();
                result.push((entry_ref.key().clone(), expired));
                seen += 1;
            }
        }
        result
    }

    /// Sample up to `count` volatile keys with their entries
    #[must_use]
    pub fn sample_volatile_entries(&self, count: usize) -> Vec<(Bytes, Entry)> {
        let mut result = Vec::with_capacity(count);
        for entry_ref in self.data.iter() {
            if result.len() >= count {
                break;
            }
            if entry_ref.value().expires_at.is_some() {
                result.push((entry_ref.key().clone(), entry_ref.value().clone()));
            }
        }
        result
    }

    /// Select a victim key for eviction based on the policy
    /// Returns the key and its memory size
    #[must_use]
    pub fn select_victim(&self, policy: EvictionPolicy) -> Option<(Bytes, usize)> {
        match policy {
            EvictionPolicy::NoEviction => None,

            EvictionPolicy::AllKeysLru => {
                // Sample keys and find the one with oldest last_access
                let samples = self.sample_keys(DEFAULT_EVICTION_SAMPLES);
                samples
                    .into_iter()
                    .min_by_key(|(_, entry)| entry.last_access)
                    .map(|(key, entry)| {
                        let size = Self::entry_size(&key, &entry);
                        (key, size)
                    })
            }

            EvictionPolicy::VolatileLru => {
                // Sample volatile keys and find the one with oldest last_access
                let samples = self.sample_volatile_entries(DEFAULT_EVICTION_SAMPLES);
                samples
                    .into_iter()
                    .min_by_key(|(_, entry)| entry.last_access)
                    .map(|(key, entry)| {
                        let size = Self::entry_size(&key, &entry);
                        (key, size)
                    })
            }

            EvictionPolicy::AllKeysLfu => {
                // Sample keys and find the one with lowest lfu_counter
                let samples = self.sample_keys(DEFAULT_EVICTION_SAMPLES);
                samples
                    .into_iter()
                    .min_by_key(|(_, entry)| entry.lfu_counter)
                    .map(|(key, entry)| {
                        let size = Self::entry_size(&key, &entry);
                        (key, size)
                    })
            }

            EvictionPolicy::VolatileLfu => {
                // Sample volatile keys and find the one with lowest lfu_counter
                let samples = self.sample_volatile_entries(DEFAULT_EVICTION_SAMPLES);
                samples
                    .into_iter()
                    .min_by_key(|(_, entry)| entry.lfu_counter)
                    .map(|(key, entry)| {
                        let size = Self::entry_size(&key, &entry);
                        (key, size)
                    })
            }

            EvictionPolicy::AllKeysRandom => {
                // Just pick the first sampled key
                self.sample_keys(1).into_iter().next().map(|(key, entry)| {
                    let size = Self::entry_size(&key, &entry);
                    (key, size)
                })
            }

            EvictionPolicy::VolatileRandom => {
                // Pick a random volatile key
                self.sample_volatile_entries(1)
                    .into_iter()
                    .next()
                    .map(|(key, entry)| {
                        let size = Self::entry_size(&key, &entry);
                        (key, size)
                    })
            }

            EvictionPolicy::VolatileTtl => {
                // Sample volatile keys and find the one with shortest TTL
                let samples = self.sample_volatile_entries(DEFAULT_EVICTION_SAMPLES);
                let now = Instant::now();
                samples
                    .into_iter()
                    .filter_map(|(key, entry)| {
                        entry.expires_at.map(|exp| {
                            let remaining = if exp > now {
                                exp.duration_since(now)
                            } else {
                                std::time::Duration::ZERO
                            };
                            (key, entry, remaining)
                        })
                    })
                    .min_by_key(|(_, _, remaining)| *remaining)
                    .map(|(key, entry, _)| {
                        let size = Self::entry_size(&key, &entry);
                        (key, size)
                    })
            }
        }
    }

    /// Delete a key only if it is expired.
    /// Returns true if the key was expired and deleted.
    pub fn delete_if_expired(&self, key: &[u8]) -> bool {
        // Use DashMap::remove_if to atomically check and remove
        if let Some((k, entry)) = self.data.remove_if(key, |_, entry| entry.is_expired()) {
            let size = Self::entry_size(&k, &entry);
            self.memory_tracker.subtract(size);
            true
        } else {
            false
        }
    }

    /// Atomically update an entry or insert a new one if it doesn't exist.
    /// This is more efficient than get() + set() as it only acquires the lock once.
    ///
    /// The closure receives `Option<&mut Entry>`:
    /// - `Some(&mut entry)` if the key exists and is not expired
    /// - `None` if the key doesn't exist or is expired
    ///
    /// The closure returns `(UpdateAction, R)`:
    /// - `UpdateAction::Keep` - keep the entry as-is (or as modified in place)
    /// - `UpdateAction::Set(entry)` - replace with a new entry
    /// - `UpdateAction::Delete` - delete the key
    ///
    /// Note: When `UpdateAction::Keep` is returned and entry was `None`, no key is created.
    pub fn update<F, R>(&self, key: Bytes, f: F) -> R
    where
        F: FnOnce(Option<&mut Entry>) -> (UpdateAction, R),
    {
        use dashmap::mapref::entry::Entry as DashEntry;

        match self.data.entry(key.clone()) {
            DashEntry::Occupied(mut occupied) => {
                // Check if expired first (cheap)
                let is_expired = occupied.get().is_expired();
                let (action, result) = if is_expired {
                    f(None)
                } else {
                    f(Some(occupied.get_mut()))
                };

                match action {
                    UpdateAction::Keep => {
                        // Entry was modified in place, no memory tracking
                        // Note: Caller should use KeepWithDelta for collections
                        // to maintain accurate memory tracking
                        if is_expired {
                            // If expired and Keep, remove it
                            let old_size = Self::entry_size(&key, occupied.get());
                            occupied.remove();
                            self.memory_tracker.subtract(old_size);
                        } else {
                            occupied.get_mut().increment_version();
                        }
                    }
                    UpdateAction::KeepWithDelta(delta) => {
                        // Entry was modified in place, apply memory delta directly
                        // This is O(1) instead of O(n) for large collections
                        if is_expired {
                            // If expired, we need the full size to subtract
                            let old_size = Self::entry_size(&key, occupied.get());
                            occupied.remove();
                            self.memory_tracker.subtract(old_size);
                        } else {
                            if delta > 0 {
                                self.memory_tracker.add(delta as usize);
                            } else if delta < 0 {
                                self.memory_tracker.subtract((-delta) as usize);
                            }
                            occupied.get_mut().increment_version();
                        }
                    }
                    UpdateAction::Set(mut entry) => {
                        let old_size = Self::entry_size(&key, occupied.get());
                        entry.increment_version();
                        let new_size = Self::entry_size(&key, &entry);
                        occupied.insert(entry);
                        self.memory_tracker.subtract(old_size);
                        self.memory_tracker.add(new_size);
                    }
                    UpdateAction::Delete => {
                        let old_size = Self::entry_size(&key, occupied.get());
                        occupied.remove();
                        self.memory_tracker.subtract(old_size);
                    }
                }
                result
            }
            DashEntry::Vacant(vacant) => {
                let (action, result) = f(None);
                match action {
                    UpdateAction::Set(mut entry) => {
                        entry.increment_version();
                        let new_size = Self::entry_size(&key, &entry);
                        vacant.insert(entry);
                        self.memory_tracker.add(new_size);
                    }
                    UpdateAction::KeepWithDelta(_) | UpdateAction::Keep | UpdateAction::Delete => {
                        // Keep/Delete/KeepWithDelta on vacant are no-ops
                    }
                }
                result
            }
        }
    }

    /// Atomically modify an existing entry's value in place.
    /// This is the most efficient method for modifying existing values as it
    /// avoids cloning the value entirely.
    ///
    /// Returns None if the key doesn't exist or is expired.
    /// Returns Some(R) with the result of the modification if successful.
    ///
    /// Note: This does NOT update memory tracking, so only use when size doesn't change.
    pub fn modify_in_place<F, R>(&self, key: &[u8], f: F) -> Option<R>
    where
        F: FnOnce(&mut Entry) -> R,
    {
        self.data.get_mut(key).and_then(|mut entry_ref| {
            if entry_ref.is_expired() {
                None
            } else {
                let result = f(entry_ref.value_mut());
                Some(result)
            }
        })
    }

    /// Get the memory tracker for this database
    #[must_use]
    pub fn memory_tracker(&self) -> &Arc<MemoryTracker> {
        &self.memory_tracker
    }
}

impl Default for KeyStore {
    fn default() -> Self {
        Self::new(DEFAULT_NUM_DATABASES)
    }
}

impl KeyStore {
    /// Create a new KeyStore with the specified number of databases
    #[must_use]
    pub fn new(num_databases: usize) -> Self {
        let memory_tracker = Arc::new(MemoryTracker::new());
        let databases = (0..num_databases)
            .map(|_| Database::new(Arc::clone(&memory_tracker)))
            .collect();
        Self {
            databases,
            memory_tracker,
        }
    }

    /// Create a new KeyStore with memory limits
    #[must_use]
    pub fn with_memory_config(
        num_databases: usize,
        max_memory: usize,
        policy: EvictionPolicy,
    ) -> Self {
        let memory_tracker = Arc::new(MemoryTracker::with_config(max_memory, policy));
        let databases = (0..num_databases)
            .map(|_| Database::new(Arc::clone(&memory_tracker)))
            .collect();
        Self {
            databases,
            memory_tracker,
        }
    }

    /// Get a reference to a database by index
    ///
    /// # Panics
    /// Panics if db_index >= number of databases
    #[must_use]
    pub fn database(&self, db_index: usize) -> &Database {
        &self.databases[db_index]
    }

    /// Get the number of databases
    #[must_use]
    pub fn num_databases(&self) -> usize {
        self.databases.len()
    }

    /// Get the memory tracker
    #[must_use]
    pub fn memory_tracker(&self) -> &Arc<MemoryTracker> {
        &self.memory_tracker
    }

    /// Get current memory usage across all databases
    #[must_use]
    pub fn memory_used(&self) -> usize {
        self.memory_tracker.used()
    }

    /// Get peak memory usage
    #[must_use]
    pub fn memory_peak(&self) -> usize {
        self.memory_tracker.peak()
    }

    /// Get the configured max memory (0 = unlimited)
    #[must_use]
    pub fn max_memory(&self) -> usize {
        self.memory_tracker.max_memory()
    }

    /// Set the max memory limit
    pub fn set_max_memory(&self, bytes: usize) {
        self.memory_tracker.set_max_memory(bytes);
    }

    /// Get the eviction policy
    #[must_use]
    pub fn eviction_policy(&self) -> EvictionPolicy {
        self.memory_tracker.eviction_policy()
    }

    /// Set the eviction policy
    pub fn set_eviction_policy(&self, policy: EvictionPolicy) {
        self.memory_tracker.set_eviction_policy(policy);
    }

    /// Get the number of evicted keys
    #[must_use]
    pub fn evicted_keys(&self) -> usize {
        self.memory_tracker.evicted_keys()
    }

    /// Reset memory stats
    pub fn reset_memory_stats(&self) {
        self.memory_tracker.reset_stats();
    }

    /// Get an entry with its version number for optimistic locking (WATCH)
    /// Returns None if the key doesn't exist or has expired
    pub fn get_with_version(&self, db_index: usize, key: &[u8]) -> Option<(Entry, u64)> {
        self.database(db_index).get_no_touch(key).and_then(|entry| {
            if entry.is_expired() {
                None
            } else {
                Some((entry.clone(), entry.version))
            }
        })
    }

    /// Swap two databases by index.
    ///
    /// # Panics
    /// Panics if either index is out of range.
    pub fn swap_databases(&self, idx1: usize, idx2: usize) {
        if idx1 == idx2 {
            return;
        }
        // Swap the underlying Arc<DashMap> between the two databases
        let db1 = &self.databases[idx1];
        let db2 = &self.databases[idx2];

        // We need to swap all entries between the two databases.
        // Since Database wraps Arc<DashMap>, we can't swap the Arcs directly
        // (they're behind shared references). Instead, we drain both and
        // re-populate.
        // Note: We don't need to adjust memory tracking since we're just swapping
        let entries1: Vec<(Bytes, Entry)> = db1
            .data
            .iter()
            .map(|r| (r.key().clone(), r.value().clone()))
            .collect();
        let entries2: Vec<(Bytes, Entry)> = db2
            .data
            .iter()
            .map(|r| (r.key().clone(), r.value().clone()))
            .collect();

        db1.flush_no_track();
        db2.flush_no_track();

        for (k, v) in entries2 {
            db1.set_no_track(k, v);
        }
        for (k, v) in entries1 {
            db2.set_no_track(k, v);
        }
    }

    /// Try to free memory by evicting keys from the specified database.
    /// Returns the number of keys evicted.
    pub fn try_evict(&self, db_index: usize, bytes_needed: usize) -> usize {
        let db = &self.databases[db_index];
        let policy = self.memory_tracker.eviction_policy();

        if policy == EvictionPolicy::NoEviction {
            return 0;
        }

        let mut evicted = 0;
        let mut freed = 0;

        while freed < bytes_needed {
            if let Some((key, entry_size)) = db.select_victim(policy) {
                if db.delete(&key) {
                    self.memory_tracker.record_eviction();
                    evicted += 1;
                    freed += entry_size;
                }
            } else {
                // No suitable victim found
                break;
            }
        }

        evicted
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::RedisValue;
    use std::time::Duration;

    #[test]
    fn test_keystore_creation() {
        let store = KeyStore::new(16);
        assert_eq!(store.num_databases(), 16);
        assert_eq!(store.memory_used(), 0);
    }

    #[test]
    fn test_keystore_with_memory_config() {
        let store = KeyStore::with_memory_config(16, 1024 * 1024, EvictionPolicy::AllKeysLru);
        assert_eq!(store.max_memory(), 1024 * 1024);
        assert_eq!(store.eviction_policy(), EvictionPolicy::AllKeysLru);
    }

    #[test]
    fn test_database_get_set() {
        let db = Database::default();
        let key = Bytes::from("test_key");
        let entry = Entry::new(RedisValue::String(Bytes::from("test_value")));

        assert!(db.get(&key).is_none());
        db.set(key.clone(), entry);
        assert!(db.get(&key).is_some());
    }

    #[test]
    fn test_database_delete() {
        let db = Database::default();
        let key = Bytes::from("test_key");
        let entry = Entry::new(RedisValue::String(Bytes::from("test_value")));

        db.set(key.clone(), entry);
        assert!(db.exists(&key));
        assert!(db.delete(&key));
        assert!(!db.exists(&key));
        assert!(!db.delete(&key)); // Already deleted
    }

    #[test]
    fn test_database_len() {
        let db = Database::default();
        assert_eq!(db.len(), 0);
        assert!(db.is_empty());

        db.set(
            Bytes::from("key1"),
            Entry::new(RedisValue::String(Bytes::from("v1"))),
        );
        db.set(
            Bytes::from("key2"),
            Entry::new(RedisValue::String(Bytes::from("v2"))),
        );
        assert_eq!(db.len(), 2);
        assert!(!db.is_empty());
    }

    #[test]
    fn test_database_flush() {
        let db = Database::default();
        db.set(
            Bytes::from("key1"),
            Entry::new(RedisValue::String(Bytes::from("v1"))),
        );
        db.set(
            Bytes::from("key2"),
            Entry::new(RedisValue::String(Bytes::from("v2"))),
        );
        assert_eq!(db.len(), 2);

        db.flush();
        assert_eq!(db.len(), 0);
    }

    #[test]
    fn test_database_keys_pattern() {
        let db = Database::default();
        db.set(
            Bytes::from("user:1"),
            Entry::new(RedisValue::String(Bytes::from("a"))),
        );
        db.set(
            Bytes::from("user:2"),
            Entry::new(RedisValue::String(Bytes::from("b"))),
        );
        db.set(
            Bytes::from("order:1"),
            Entry::new(RedisValue::String(Bytes::from("c"))),
        );

        let user_keys = db.keys("user:*");
        assert_eq!(user_keys.len(), 2);

        let all_keys = db.keys("*");
        assert_eq!(all_keys.len(), 3);

        let order_keys = db.keys("order:*");
        assert_eq!(order_keys.len(), 1);
    }

    #[test]
    fn test_memory_tracking_on_set() {
        let tracker = Arc::new(MemoryTracker::new());
        let db = Database::new(Arc::clone(&tracker));

        assert_eq!(tracker.used(), 0);

        db.set(
            Bytes::from("key1"),
            Entry::new(RedisValue::String(Bytes::from("value1"))),
        );

        assert!(tracker.used() > 0);
        let first_size = tracker.used();

        db.set(
            Bytes::from("key2"),
            Entry::new(RedisValue::String(Bytes::from("value2"))),
        );

        assert!(tracker.used() > first_size);
    }

    #[test]
    fn test_memory_tracking_on_overwrite() {
        let tracker = Arc::new(MemoryTracker::new());
        let db = Database::new(Arc::clone(&tracker));

        // Set initial value
        db.set(
            Bytes::from("key"),
            Entry::new(RedisValue::String(Bytes::from("short"))),
        );
        let size_after_short = tracker.used();

        // Overwrite with longer value
        db.set(
            Bytes::from("key"),
            Entry::new(RedisValue::String(Bytes::from(
                "this is a much longer value",
            ))),
        );
        let size_after_long = tracker.used();

        assert!(size_after_long > size_after_short);

        // Overwrite with shorter value
        db.set(
            Bytes::from("key"),
            Entry::new(RedisValue::String(Bytes::from("tiny"))),
        );
        let size_after_tiny = tracker.used();

        assert!(size_after_tiny < size_after_long);
    }

    #[test]
    fn test_memory_tracking_on_delete() {
        let tracker = Arc::new(MemoryTracker::new());
        let db = Database::new(Arc::clone(&tracker));

        db.set(
            Bytes::from("key1"),
            Entry::new(RedisValue::String(Bytes::from("value1"))),
        );
        db.set(
            Bytes::from("key2"),
            Entry::new(RedisValue::String(Bytes::from("value2"))),
        );

        let size_before_delete = tracker.used();

        db.delete(b"key1");

        assert!(tracker.used() < size_before_delete);
    }

    #[test]
    fn test_memory_tracking_on_flush() {
        let tracker = Arc::new(MemoryTracker::new());
        let db = Database::new(Arc::clone(&tracker));

        for i in 0..10 {
            db.set(
                Bytes::from(format!("key{i}")),
                Entry::new(RedisValue::String(Bytes::from(format!("value{i}")))),
            );
        }

        assert!(tracker.used() > 0);

        db.flush();

        assert_eq!(tracker.used(), 0);
    }

    #[test]
    fn test_get_updates_lru() {
        let db = Database::default();
        let key = Bytes::from("test_key");

        let mut entry = Entry::new(RedisValue::String(Bytes::from("test_value")));
        let initial_access = entry.last_access;
        db.set(key.clone(), entry);

        std::thread::sleep(Duration::from_millis(10));

        let retrieved = db.get(&key).unwrap();
        assert!(retrieved.last_access > initial_access);
    }

    #[test]
    fn test_get_no_touch() {
        let db = Database::default();
        let key = Bytes::from("test_key");

        let entry = Entry::new(RedisValue::String(Bytes::from("test_value")));
        db.set(key.clone(), entry);

        // Get with no touch should not update last_access
        let entry1 = db.get_no_touch(&key).unwrap();
        std::thread::sleep(Duration::from_millis(10));
        let entry2 = db.get_no_touch(&key).unwrap();

        // The entries should have the same last_access time
        assert_eq!(entry1.last_access, entry2.last_access);
    }

    #[test]
    fn test_sample_keys() {
        let db = Database::default();

        for i in 0..20 {
            db.set(
                Bytes::from(format!("key{i}")),
                Entry::new(RedisValue::String(Bytes::from(format!("value{i}")))),
            );
        }

        let samples = db.sample_keys(5);
        assert_eq!(samples.len(), 5);
    }

    #[test]
    fn test_sample_volatile_entries() {
        let db = Database::default();

        // Add some volatile keys
        for i in 0..10 {
            let mut entry = Entry::new(RedisValue::String(Bytes::from(format!("value{i}"))));
            entry.set_ttl(Duration::from_secs(100));
            db.set(Bytes::from(format!("volatile{i}")), entry);
        }

        // Add some persistent keys
        for i in 0..10 {
            let entry = Entry::new(RedisValue::String(Bytes::from(format!("value{i}"))));
            db.set(Bytes::from(format!("persistent{i}")), entry);
        }

        let samples = db.sample_volatile_entries(5);
        assert_eq!(samples.len(), 5);

        // All sampled entries should have TTL
        for (_, entry) in samples {
            assert!(entry.expires_at.is_some());
        }
    }

    #[test]
    fn test_select_victim_lru() {
        let db = Database::default();

        // Add keys with different access times
        for i in 0..5 {
            let entry = Entry::new(RedisValue::String(Bytes::from(format!("value{i}"))));
            db.set(Bytes::from(format!("key{i}")), entry);
            std::thread::sleep(Duration::from_millis(5));
        }

        // Access some keys to update their LRU time
        db.get(b"key0");
        db.get(b"key1");
        db.get(b"key2");

        // key3 and key4 should be candidates for eviction (older access)
        let victim = db.select_victim(EvictionPolicy::AllKeysLru);
        assert!(victim.is_some());
    }

    #[test]
    fn test_select_victim_volatile_only() {
        let tracker = Arc::new(MemoryTracker::new());
        let db = Database::new(tracker);

        // Add only persistent keys
        for i in 0..5 {
            let entry = Entry::new(RedisValue::String(Bytes::from(format!("value{i}"))));
            db.set(Bytes::from(format!("key{i}")), entry);
        }

        // Volatile policies should find no victims
        assert!(db.select_victim(EvictionPolicy::VolatileLru).is_none());
        assert!(db.select_victim(EvictionPolicy::VolatileLfu).is_none());
        assert!(db.select_victim(EvictionPolicy::VolatileRandom).is_none());
        assert!(db.select_victim(EvictionPolicy::VolatileTtl).is_none());
    }

    #[test]
    fn test_set_with_eviction_noeviction_policy() {
        let tracker = Arc::new(MemoryTracker::with_config(500, EvictionPolicy::NoEviction));
        let db = Database::new(Arc::clone(&tracker));

        // Fill up memory
        db.set(
            Bytes::from("key1"),
            Entry::new(RedisValue::String(Bytes::from("x".repeat(200)))),
        );
        db.set(
            Bytes::from("key2"),
            Entry::new(RedisValue::String(Bytes::from("x".repeat(200)))),
        );

        // This should fail with OOM
        let result = db.set_with_eviction(
            Bytes::from("key3"),
            Entry::new(RedisValue::String(Bytes::from("x".repeat(200)))),
        );

        assert!(matches!(result, SetResult::OutOfMemory));
    }

    #[test]
    fn test_set_with_eviction_lru_policy() {
        let tracker = Arc::new(MemoryTracker::with_config(500, EvictionPolicy::AllKeysLru));
        let db = Database::new(Arc::clone(&tracker));

        // Fill up memory
        db.set(
            Bytes::from("key1"),
            Entry::new(RedisValue::String(Bytes::from("x".repeat(150)))),
        );
        std::thread::sleep(Duration::from_millis(5));
        db.set(
            Bytes::from("key2"),
            Entry::new(RedisValue::String(Bytes::from("x".repeat(150)))),
        );
        std::thread::sleep(Duration::from_millis(5));

        // Access key1 to make it more recently used
        db.get(b"key1");

        let initial_keys = db.len();

        // This should trigger eviction (key2 should be evicted as it's older)
        let result = db.set_with_eviction(
            Bytes::from("key3"),
            Entry::new(RedisValue::String(Bytes::from("x".repeat(150)))),
        );

        assert!(matches!(result, SetResult::Ok));
        // We should have evicted at least one key and added one
        assert!(db.len() <= initial_keys);
        assert!(tracker.evicted_keys() > 0);
    }

    #[test]
    fn test_keystore_memory_methods() {
        let store = KeyStore::with_memory_config(16, 1024 * 1024, EvictionPolicy::AllKeysLru);

        assert_eq!(store.max_memory(), 1024 * 1024);
        assert_eq!(store.eviction_policy(), EvictionPolicy::AllKeysLru);

        store.set_max_memory(2048 * 1024);
        assert_eq!(store.max_memory(), 2048 * 1024);

        store.set_eviction_policy(EvictionPolicy::VolatileLfu);
        assert_eq!(store.eviction_policy(), EvictionPolicy::VolatileLfu);
    }

    #[test]
    fn test_delete_if_expired_tracks_memory() {
        let tracker = Arc::new(MemoryTracker::new());
        let db = Database::new(Arc::clone(&tracker));

        let mut entry = Entry::new(RedisValue::String(Bytes::from("value")));
        entry.expires_at = Some(Instant::now() - Duration::from_secs(1)); // Already expired

        db.set(Bytes::from("expired_key"), entry);
        let size_before = tracker.used();

        let deleted = db.delete_if_expired(b"expired_key");
        assert!(deleted);
        assert!(tracker.used() < size_before);
    }
}

//! Redis value types and entry metadata
//!
//! This module defines the core data types that ferris-db stores.

use bytes::Bytes;
use ordered_float::OrderedFloat;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::time::Instant;

/// All possible Redis value types
#[derive(Clone, Debug)]
pub enum RedisValue {
    /// String or binary data
    String(Bytes),

    /// Doubly-ended queue for list operations
    List(VecDeque<Bytes>),

    /// Unordered set of unique elements
    Set(HashSet<Bytes>),

    /// Field-value map
    Hash(HashMap<Bytes, Bytes>),

    /// Sorted set with scores
    SortedSet {
        /// Member to score lookup
        scores: HashMap<Bytes, f64>,
        /// (score, member) for range queries - BTreeMap maintains order
        members: BTreeMap<(OrderedFloat<f64>, Bytes), ()>,
    },
}

impl RedisValue {
    /// Get the type name as Redis returns it
    #[must_use]
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::String(_) => "string",
            Self::List(_) => "list",
            Self::Set(_) => "set",
            Self::Hash(_) => "hash",
            Self::SortedSet { .. } => "zset",
        }
    }

    /// Approximate memory usage in bytes
    #[must_use]
    pub fn memory_usage(&self) -> usize {
        match self {
            Self::String(s) => s.len() + 24, // Bytes overhead
            Self::List(l) => l.iter().map(|e| e.len() + 24).sum::<usize>() + 64,
            Self::Set(s) => s.iter().map(|e| e.len() + 32).sum::<usize>() + 64,
            Self::Hash(h) => h.iter().map(|(k, v)| k.len() + v.len() + 48).sum::<usize>() + 64,
            Self::SortedSet { scores, .. } => {
                scores.iter().map(|(k, _)| k.len() + 8 + 48).sum::<usize>() + 128
            }
        }
    }

    /// Create a new empty list
    #[must_use]
    pub fn new_list() -> Self {
        Self::List(VecDeque::new())
    }

    /// Create a new empty set
    #[must_use]
    pub fn new_set() -> Self {
        Self::Set(HashSet::new())
    }

    /// Create a new empty hash
    #[must_use]
    pub fn new_hash() -> Self {
        Self::Hash(HashMap::new())
    }

    /// Create a new empty sorted set
    #[must_use]
    pub fn new_sorted_set() -> Self {
        Self::SortedSet {
            scores: HashMap::new(),
            members: BTreeMap::new(),
        }
    }
}

/// An entry in the key-value store with metadata
#[derive(Clone, Debug)]
pub struct Entry {
    /// The actual value
    pub value: RedisValue,

    /// Optional expiration time
    pub expires_at: Option<Instant>,

    /// Monotonic version for optimistic locking (WATCH)
    pub version: u64,

    /// Last access time for LRU eviction
    pub last_access: Instant,

    /// Access frequency counter for LFU eviction (Morris counter)
    pub lfu_counter: u8,
}

impl Entry {
    /// Create a new entry with the given value
    #[must_use]
    pub fn new(value: RedisValue) -> Self {
        Self {
            value,
            expires_at: None,
            version: 0,
            last_access: Instant::now(),
            lfu_counter: 5, // Initial LFU counter value (like Redis)
        }
    }

    /// Check if this entry has expired
    #[must_use]
    pub fn is_expired(&self) -> bool {
        self.expires_at.map_or(false, |exp| Instant::now() >= exp)
    }

    /// Update access time and LFU counter (call on every access)
    pub fn touch(&mut self) {
        self.last_access = Instant::now();
        // LFU: probabilistic increment (Morris counter)
        if self.lfu_counter < 255 {
            let r: f64 = rand::random();
            let p = 1.0 / (f64::from(self.lfu_counter) * 10.0 + 1.0);
            if r < p {
                self.lfu_counter += 1;
            }
        }
    }

    /// Increment the version (call on every write)
    pub fn increment_version(&mut self) {
        self.version = self.version.wrapping_add(1);
    }

    /// Get the time-to-live in milliseconds, or None if no expiry
    #[must_use]
    pub fn ttl_millis(&self) -> Option<u64> {
        self.expires_at.map(|exp| {
            let now = Instant::now();
            if exp > now {
                exp.duration_since(now).as_millis() as u64
            } else {
                0
            }
        })
    }

    /// Set expiration time relative to now
    pub fn set_ttl(&mut self, duration: std::time::Duration) {
        self.expires_at = Some(Instant::now() + duration);
    }

    /// Remove expiration (persist the key)
    pub fn persist(&mut self) {
        self.expires_at = None;
    }

    /// Get approximate memory usage
    #[must_use]
    pub fn memory_usage(&self) -> usize {
        // Entry struct overhead + value size
        std::mem::size_of::<Self>() + self.value.memory_usage()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_redis_value_type_name() {
        assert_eq!(RedisValue::String(Bytes::new()).type_name(), "string");
        assert_eq!(RedisValue::new_list().type_name(), "list");
        assert_eq!(RedisValue::new_set().type_name(), "set");
        assert_eq!(RedisValue::new_hash().type_name(), "hash");
        assert_eq!(RedisValue::new_sorted_set().type_name(), "zset");
    }

    #[test]
    fn test_entry_creation() {
        let entry = Entry::new(RedisValue::String(Bytes::from("hello")));
        assert!(!entry.is_expired());
        assert!(entry.expires_at.is_none());
        assert_eq!(entry.version, 0);
    }

    #[test]
    fn test_entry_expiry() {
        let mut entry = Entry::new(RedisValue::String(Bytes::from("hello")));
        assert!(!entry.is_expired());

        // Set TTL to 0 (already expired)
        entry.expires_at = Some(Instant::now() - Duration::from_secs(1));
        assert!(entry.is_expired());

        // Set TTL to future
        entry.set_ttl(Duration::from_secs(3600));
        assert!(!entry.is_expired());
        assert!(entry.ttl_millis().unwrap_or(0) > 0);
    }

    #[test]
    fn test_entry_persist() {
        let mut entry = Entry::new(RedisValue::String(Bytes::from("hello")));
        entry.set_ttl(Duration::from_secs(100));
        assert!(entry.expires_at.is_some());

        entry.persist();
        assert!(entry.expires_at.is_none());
    }

    #[test]
    fn test_entry_version() {
        let mut entry = Entry::new(RedisValue::String(Bytes::from("hello")));
        assert_eq!(entry.version, 0);

        entry.increment_version();
        assert_eq!(entry.version, 1);

        entry.increment_version();
        assert_eq!(entry.version, 2);
    }

    #[test]
    fn test_entry_touch() {
        let mut entry = Entry::new(RedisValue::String(Bytes::from("hello")));
        let initial_access = entry.last_access;

        std::thread::sleep(Duration::from_millis(10));
        entry.touch();

        assert!(entry.last_access > initial_access);
    }

    #[test]
    fn test_memory_usage() {
        let entry = Entry::new(RedisValue::String(Bytes::from("hello")));
        assert!(entry.memory_usage() > 0);

        let list_entry = Entry::new(RedisValue::List(
            vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")].into(),
        ));
        assert!(list_entry.memory_usage() > entry.memory_usage());
    }
}

//! Redis value types and entry metadata
//!
//! This module defines the core data types that ferris-db stores.

use bytes::Bytes;
use ordered_float::OrderedFloat;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Stream entry ID consisting of timestamp and sequence number
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamId {
    /// Milliseconds since Unix epoch
    pub timestamp: u64,
    /// Sequence number within the same millisecond
    pub sequence: u64,
}

impl StreamId {
    /// Create a new stream ID
    #[must_use]
    pub fn new(timestamp: u64, sequence: u64) -> Self {
        Self {
            timestamp,
            sequence,
        }
    }

    /// Create the minimum possible stream ID
    #[must_use]
    pub fn min() -> Self {
        Self {
            timestamp: 0,
            sequence: 0,
        }
    }

    /// Create the maximum possible stream ID
    #[must_use]
    pub fn max() -> Self {
        Self {
            timestamp: u64::MAX,
            sequence: u64::MAX,
        }
    }

    /// Generate a new ID based on current time
    #[must_use]
    pub fn generate(last_id: Option<&StreamId>) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        match last_id {
            Some(last) if last.timestamp >= now => {
                // Same or future timestamp, increment sequence
                Self {
                    timestamp: last.timestamp,
                    sequence: last.sequence + 1,
                }
            }
            _ => Self {
                timestamp: now,
                sequence: 0,
            },
        }
    }

    /// Parse a stream ID from string (e.g., "1234567890123-0" or "*")
    pub fn parse(s: &str) -> Option<Self> {
        if s == "*" {
            return None; // Auto-generate
        }
        if s == "-" {
            return Some(Self::min());
        }
        if s == "+" {
            return Some(Self::max());
        }
        let parts: Vec<&str> = s.split('-').collect();
        match parts.as_slice() {
            [ts] => {
                let timestamp = ts.parse().ok()?;
                Some(Self {
                    timestamp,
                    sequence: 0,
                })
            }
            [ts, seq] => {
                let timestamp = ts.parse().ok()?;
                let sequence = if *seq == "*" { 0 } else { seq.parse().ok()? };
                Some(Self {
                    timestamp,
                    sequence,
                })
            }
            _ => None,
        }
    }
}

impl std::fmt::Display for StreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.timestamp, self.sequence)
    }
}

/// A stream entry with ID and field-value pairs
#[derive(Clone, Debug)]
pub struct StreamEntry {
    /// The entry ID
    pub id: StreamId,
    /// Field-value pairs
    pub fields: Vec<(Bytes, Bytes)>,
}

/// Consumer group for stream processing
#[derive(Clone, Debug)]
pub struct ConsumerGroup {
    /// Name of the consumer group
    pub name: Bytes,
    /// Last delivered ID
    pub last_delivered_id: StreamId,
    /// Pending entries (entry ID -> consumer name, delivery time, delivery count)
    pub pending: HashMap<StreamId, PendingEntry>,
    /// Known consumers in this group
    pub consumers: HashMap<Bytes, Consumer>,
}

impl ConsumerGroup {
    /// Create a new consumer group
    #[must_use]
    pub fn new(name: Bytes, start_id: StreamId) -> Self {
        Self {
            name,
            last_delivered_id: start_id,
            pending: HashMap::new(),
            consumers: HashMap::new(),
        }
    }
}

/// A pending entry in a consumer group
#[derive(Clone, Debug)]
pub struct PendingEntry {
    /// Consumer that owns this entry
    pub consumer: Bytes,
    /// Time when the entry was delivered
    pub delivery_time: Instant,
    /// Number of times this entry has been delivered
    pub delivery_count: u64,
}

/// A consumer in a consumer group
#[derive(Clone, Debug)]
pub struct Consumer {
    /// Consumer name
    pub name: Bytes,
    /// Number of pending entries for this consumer
    pub pending_count: usize,
    /// Last time this consumer was seen
    pub last_seen: Instant,
}

/// Stream data structure
#[derive(Clone, Debug)]
pub struct StreamData {
    /// Entries stored in order by ID
    pub entries: BTreeMap<StreamId, Vec<(Bytes, Bytes)>>,
    /// Last generated/added entry ID
    pub last_id: StreamId,
    /// Consumer groups
    pub groups: HashMap<Bytes, ConsumerGroup>,
    /// Maximum length (for MAXLEN trimming)
    pub max_len: Option<usize>,
    /// First entry ID (for efficient XINFO)
    pub first_id: Option<StreamId>,
}

impl StreamData {
    /// Create a new empty stream
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            last_id: StreamId::new(0, 0),
            groups: HashMap::new(),
            max_len: None,
            first_id: None,
        }
    }

    /// Add an entry to the stream
    pub fn add(&mut self, id: Option<StreamId>, fields: Vec<(Bytes, Bytes)>) -> StreamId {
        let entry_id = id.unwrap_or_else(|| StreamId::generate(Some(&self.last_id)));

        // Ensure ID is greater than last_id
        if entry_id <= self.last_id && self.last_id.timestamp > 0 {
            // In a real implementation, we'd return an error
            // For now, generate a valid ID
            let new_id = StreamId::new(self.last_id.timestamp, self.last_id.sequence + 1);
            self.entries.insert(new_id.clone(), fields);
            if self.first_id.is_none() {
                self.first_id = Some(new_id.clone());
            }
            self.last_id = new_id.clone();
            return new_id;
        }

        self.entries.insert(entry_id.clone(), fields);
        if self.first_id.is_none() {
            self.first_id = Some(entry_id.clone());
        }
        self.last_id = entry_id.clone();

        // Apply MAXLEN trimming if configured
        if let Some(max_len) = self.max_len {
            while self.entries.len() > max_len {
                if let Some(first_key) = self.entries.keys().next().cloned() {
                    self.entries.remove(&first_key);
                    self.first_id = self.entries.keys().next().cloned();
                }
            }
        }

        entry_id
    }

    /// Get the length of the stream
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if the stream is empty
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get entries in a range
    #[must_use]
    pub fn range(
        &self,
        start: &StreamId,
        end: &StreamId,
        count: Option<usize>,
    ) -> Vec<StreamEntry> {
        let mut result = Vec::new();
        for (id, fields) in self.entries.range(start.clone()..=end.clone()) {
            result.push(StreamEntry {
                id: id.clone(),
                fields: fields.clone(),
            });
            if let Some(c) = count {
                if result.len() >= c {
                    break;
                }
            }
        }
        result
    }

    /// Get entries in reverse range
    #[must_use]
    pub fn rev_range(
        &self,
        start: &StreamId,
        end: &StreamId,
        count: Option<usize>,
    ) -> Vec<StreamEntry> {
        let mut result = Vec::new();
        for (id, fields) in self.entries.range(end.clone()..=start.clone()).rev() {
            result.push(StreamEntry {
                id: id.clone(),
                fields: fields.clone(),
            });
            if let Some(c) = count {
                if result.len() >= c {
                    break;
                }
            }
        }
        result
    }

    /// Read entries after a given ID
    #[must_use]
    pub fn read_after(&self, after_id: &StreamId, count: Option<usize>) -> Vec<StreamEntry> {
        let mut result = Vec::new();
        let start = StreamId::new(after_id.timestamp, after_id.sequence.saturating_add(1));

        for (id, fields) in self.entries.range(start..) {
            result.push(StreamEntry {
                id: id.clone(),
                fields: fields.clone(),
            });
            if let Some(c) = count {
                if result.len() >= c {
                    break;
                }
            }
        }
        result
    }

    /// Delete entries by ID
    pub fn delete(&mut self, ids: &[StreamId]) -> usize {
        let mut deleted = 0;
        for id in ids {
            if self.entries.remove(id).is_some() {
                deleted += 1;
            }
        }
        // Update first_id if needed
        self.first_id = self.entries.keys().next().cloned();
        deleted
    }

    /// Trim the stream to a maximum length
    pub fn trim(&mut self, max_len: usize, approx: bool) -> usize {
        let current_len = self.entries.len();
        if current_len <= max_len {
            return 0;
        }

        let to_remove = if approx {
            // Approximate trimming: remove roughly the excess
            (current_len - max_len).min(current_len)
        } else {
            current_len - max_len
        };

        let mut removed = 0;
        while removed < to_remove {
            if let Some(first_key) = self.entries.keys().next().cloned() {
                self.entries.remove(&first_key);
                removed += 1;
            } else {
                break;
            }
        }

        self.first_id = self.entries.keys().next().cloned();
        removed
    }

    /// Create or get a consumer group
    pub fn create_group(
        &mut self,
        name: Bytes,
        start_id: StreamId,
        _mkstream: bool,
    ) -> Result<(), &'static str> {
        if self.groups.contains_key(&name) {
            return Err("BUSYGROUP Consumer Group name already exists");
        }
        self.groups
            .insert(name.clone(), ConsumerGroup::new(name, start_id));
        Ok(())
    }

    /// Get a consumer group
    #[must_use]
    pub fn get_group(&self, name: &Bytes) -> Option<&ConsumerGroup> {
        self.groups.get(name)
    }

    /// Get a mutable consumer group
    pub fn get_group_mut(&mut self, name: &Bytes) -> Option<&mut ConsumerGroup> {
        self.groups.get_mut(name)
    }

    /// Memory usage approximation
    #[must_use]
    pub fn memory_usage(&self) -> usize {
        let entries_size: usize = self
            .entries
            .iter()
            .map(|(_id, fields)| {
                16 + // StreamId size
                fields.iter().map(|(k, v)| k.len() + v.len() + 48).sum::<usize>()
            })
            .sum();

        let groups_size: usize = self
            .groups
            .iter()
            .map(|(name, group)| {
                name.len() + 64 + // Group overhead
                group.pending.len() * 64 + // Pending entries
                group.consumers.len() * 64 // Consumers
            })
            .sum();

        entries_size + groups_size + 128 // Base overhead
    }
}

impl Default for StreamData {
    fn default() -> Self {
        Self::new()
    }
}

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

    /// Stream - append-only log with consumer groups
    Stream(StreamData),
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
            Self::Stream(_) => "stream",
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
            Self::Stream(s) => s.memory_usage(),
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

    /// Create a new empty stream
    #[must_use]
    pub fn new_stream() -> Self {
        Self::Stream(StreamData::new())
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
        assert_eq!(RedisValue::new_stream().type_name(), "stream");
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

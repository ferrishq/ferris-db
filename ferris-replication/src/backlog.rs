//! Replication backlog - circular buffer for storing recent commands
//!
//! The backlog enables partial resynchronization when replicas reconnect.

#![allow(clippy::missing_panics_doc)] // Lock poisoning is unrecoverable
#![allow(clippy::expect_used)] // Lock poisoning is fatal error
#![allow(clippy::significant_drop_tightening)] // False positive - drop order is fine

use bytes::Bytes;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

/// A single entry in the replication backlog
#[derive(Debug, Clone)]
pub struct BacklogEntry {
    /// Starting byte offset of this entry in the replication stream
    pub offset: u64,
    /// Serialized RESP command data
    pub data: Bytes,
}

/// Configuration for the replication backlog
#[derive(Debug, Clone)]
pub struct BacklogConfig {
    /// Maximum size in bytes (default 1MB like Redis)
    pub capacity: usize,
}

impl Default for BacklogConfig {
    fn default() -> Self {
        Self {
            capacity: 1024 * 1024, // 1MB
        }
    }
}

/// Circular buffer storing recent commands for partial resync
///
/// This buffer stores commands in a ring buffer. When it fills up,
/// old entries are evicted. Replicas can request commands starting
/// from any offset that's still in the buffer.
pub struct ReplicationBacklog {
    config: BacklogConfig,
    entries: RwLock<VecDeque<BacklogEntry>>,
    current_size: AtomicU64,
    first_offset: AtomicU64,
    last_offset: AtomicU64,
}

impl ReplicationBacklog {
    /// Create a new replication backlog with default configuration
    #[must_use]
    pub fn new() -> Self {
        Self::with_config(BacklogConfig::default())
    }

    /// Create a new replication backlog with the given configuration
    #[must_use]
    pub const fn with_config(config: BacklogConfig) -> Self {
        Self {
            config,
            entries: RwLock::new(VecDeque::new()),
            current_size: AtomicU64::new(0),
            first_offset: AtomicU64::new(0),
            last_offset: AtomicU64::new(0),
        }
    }

    /// Append a command to the backlog
    /// Returns the starting offset of the appended entry
    #[allow(clippy::missing_panics_doc)] // Lock poisoning is unrecoverable
    pub fn append(&self, data: Bytes) -> u64 {
        let data_len = data.len() as u64;
        let offset = self.last_offset.load(Ordering::Acquire);

        let entry = BacklogEntry { offset, data };

        let mut entries = self.entries.write().expect("entries lock poisoned");

        // Add the new entry
        entries.push_back(entry);
        self.current_size.fetch_add(data_len, Ordering::AcqRel);
        let new_offset = self.last_offset.fetch_add(data_len, Ordering::AcqRel) + data_len;

        // Evict old entries if we exceed capacity
        while self.current_size.load(Ordering::Acquire) > self.config.capacity as u64
            && !entries.is_empty()
        {
            if let Some(removed) = entries.pop_front() {
                let removed_len = removed.data.len() as u64;
                self.current_size.fetch_sub(removed_len, Ordering::AcqRel);
                self.first_offset
                    .store(removed.offset + removed_len, Ordering::Release);
            }
        }

        // Update first_offset if this is the first entry
        if entries.len() == 1 {
            self.first_offset.store(offset, Ordering::Release);
        }

        drop(entries);
        new_offset
    }

    /// Get entries starting from the given offset
    ///
    /// Returns:
    /// - `Some(vec)` if the offset is valid and entries exist
    /// - `None` if the offset is too old (evicted) or too new (future)
    #[allow(clippy::missing_panics_doc)] // Lock poisoning is unrecoverable
    pub fn get_from_offset(&self, offset: u64) -> Option<Vec<BacklogEntry>> {
        let entries = self.entries.read().expect("entries lock poisoned");

        if entries.is_empty() {
            return None;
        }

        let first = self.first_offset.load(Ordering::Acquire);
        let last = self.last_offset.load(Ordering::Acquire);

        // Offset is too old (data evicted)
        if offset < first {
            return None;
        }

        // Offset is too new (in the future)
        if offset > last {
            return None;
        }

        // Exact match at last offset means we're up to date
        if offset == last {
            return Some(vec![]);
        }

        // Find entries starting from the requested offset
        let mut result = Vec::new();
        for entry in entries.iter() {
            if entry.offset >= offset {
                result.push(entry.clone());
            }
        }

        Some(result)
    }

    /// Get the first valid offset in the backlog
    #[must_use]
    pub fn first_offset(&self) -> u64 {
        self.first_offset.load(Ordering::Acquire)
    }

    /// Get the last offset in the backlog (next offset to be written)
    #[must_use]
    pub fn last_offset(&self) -> u64 {
        self.last_offset.load(Ordering::Acquire)
    }

    /// Get current size in bytes
    #[must_use]
    pub fn size(&self) -> u64 {
        self.current_size.load(Ordering::Acquire)
    }

    /// Check if an offset can be used for partial resync
    #[must_use]
    pub fn can_partial_resync(&self, offset: u64) -> bool {
        let first = self.first_offset.load(Ordering::Acquire);
        let last = self.last_offset.load(Ordering::Acquire);
        offset >= first && offset <= last
    }

    /// Clear the backlog
    #[allow(clippy::missing_panics_doc)] // Lock poisoning is unrecoverable
    pub fn clear(&self) {
        let mut entries = self.entries.write().expect("entries lock poisoned");
        entries.clear();
        self.current_size.store(0, Ordering::Release);
        let last = self.last_offset.load(Ordering::Acquire);
        self.first_offset.store(last, Ordering::Release);
    }

    /// Get the number of entries in the backlog
    #[must_use]
    #[allow(clippy::missing_panics_doc)] // Lock poisoning is unrecoverable
    pub fn len(&self) -> usize {
        self.entries.read().expect("entries lock poisoned").len()
    }

    /// Check if the backlog is empty
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for ReplicationBacklog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[allow(clippy::redundant_clone)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_new_backlog_is_empty() {
        let backlog = ReplicationBacklog::new();
        assert_eq!(backlog.len(), 0);
        assert!(backlog.is_empty());
        assert_eq!(backlog.size(), 0);
        assert_eq!(backlog.first_offset(), 0);
        assert_eq!(backlog.last_offset(), 0);
    }

    #[test]
    fn test_append_single_entry() {
        let backlog = ReplicationBacklog::new();
        let data = Bytes::from("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n");
        let offset = backlog.append(data.clone());

        assert_eq!(offset, data.len() as u64);
        assert_eq!(backlog.len(), 1);
        assert_eq!(backlog.size(), data.len() as u64);
        assert_eq!(backlog.first_offset(), 0);
        assert_eq!(backlog.last_offset(), data.len() as u64);
    }

    #[test]
    fn test_append_multiple_entries() {
        let backlog = ReplicationBacklog::new();

        let data1 = Bytes::from("*3\r\n$3\r\nSET\r\n$2\r\nk1\r\n$2\r\nv1\r\n");
        let data2 = Bytes::from("*3\r\n$3\r\nSET\r\n$2\r\nk2\r\n$2\r\nv2\r\n");
        let data3 = Bytes::from("*3\r\n$3\r\nSET\r\n$2\r\nk3\r\n$2\r\nv3\r\n");

        let offset1 = backlog.append(data1.clone());
        let offset2 = backlog.append(data2.clone());
        let offset3 = backlog.append(data3.clone());

        assert_eq!(offset1, data1.len() as u64);
        assert_eq!(offset2, (data1.len() + data2.len()) as u64);
        assert_eq!(offset3, (data1.len() + data2.len() + data3.len()) as u64);
        assert_eq!(backlog.len(), 3);
    }

    #[test]
    fn test_offset_increments_correctly() {
        let backlog = ReplicationBacklog::new();

        let data1 = Bytes::from_static(b"command1");
        let data2 = Bytes::from_static(b"command2");

        let off1 = backlog.append(data1.clone());
        let off2 = backlog.append(data2.clone());

        assert_eq!(off1, 8); // "command1".len()
        assert_eq!(off2, 16); // "command1".len() + "command2".len()
    }

    #[test]
    fn test_get_from_offset_found() {
        let backlog = ReplicationBacklog::new();

        let data1 = Bytes::from_static(b"cmd1");
        let data2 = Bytes::from_static(b"cmd2");
        let data3 = Bytes::from_static(b"cmd3");

        backlog.append(data1.clone());
        let offset2 = backlog.append(data2.clone());
        backlog.append(data3.clone());

        // Get from second command
        let entries = backlog.get_from_offset(offset2 - data2.len() as u64);
        assert!(entries.is_some());
        let entries = entries.unwrap();
        assert_eq!(entries.len(), 2); // cmd2 and cmd3
    }

    #[test]
    fn test_get_from_offset_too_old() {
        let backlog = ReplicationBacklog::with_config(BacklogConfig { capacity: 20 });

        // Fill beyond capacity to evict early entries
        for i in 0..10 {
            let data = Bytes::from(format!("command_{i:02}"));
            backlog.append(data);
        }

        // Try to get from offset 0 (should be evicted)
        let entries = backlog.get_from_offset(0);
        assert!(entries.is_none());
    }

    #[test]
    fn test_get_from_offset_too_new() {
        let backlog = ReplicationBacklog::new();
        let data = Bytes::from_static(b"command");
        backlog.append(data);

        // Try to get from future offset
        let entries = backlog.get_from_offset(1000);
        assert!(entries.is_none());
    }

    #[test]
    fn test_get_from_exact_last_offset() {
        let backlog = ReplicationBacklog::new();
        let data = Bytes::from_static(b"command");
        let offset = backlog.append(data);

        // Getting from exact last offset should return empty vec (up to date)
        let entries = backlog.get_from_offset(offset);
        assert!(entries.is_some());
        assert_eq!(entries.unwrap().len(), 0);
    }

    #[test]
    fn test_backlog_wraps_when_full() {
        let backlog = ReplicationBacklog::with_config(BacklogConfig { capacity: 30 });

        let data1 = Bytes::from_static(b"command_01");
        let data2 = Bytes::from_static(b"command_02");
        let data3 = Bytes::from_static(b"command_03");
        let data4 = Bytes::from_static(b"command_04");

        backlog.append(data1.clone());
        backlog.append(data2.clone());
        backlog.append(data3.clone());
        backlog.append(data4.clone());

        // Should have evicted first entry
        assert!(backlog.size() <= 30);
        assert!(backlog.first_offset() > 0);
    }

    #[test]
    fn test_size_tracking() {
        let backlog = ReplicationBacklog::new();
        assert_eq!(backlog.size(), 0);

        let data = Bytes::from_static(b"test");
        backlog.append(data.clone());
        assert_eq!(backlog.size(), data.len() as u64);

        backlog.append(data.clone());
        assert_eq!(backlog.size(), (data.len() * 2) as u64);
    }

    #[test]
    fn test_can_partial_resync() {
        let backlog = ReplicationBacklog::new();

        let data = Bytes::from_static(b"command");
        backlog.append(data.clone());

        assert!(backlog.can_partial_resync(0)); // First offset
        assert!(backlog.can_partial_resync(data.len() as u64)); // Last offset
        assert!(!backlog.can_partial_resync(1000)); // Future offset
    }

    #[test]
    fn test_clear_backlog() {
        let backlog = ReplicationBacklog::new();

        let data = Bytes::from_static(b"command");
        let last_offset = backlog.append(data);

        backlog.clear();

        assert!(backlog.is_empty());
        assert_eq!(backlog.size(), 0);
        assert_eq!(backlog.first_offset(), last_offset);
    }

    #[test]
    fn test_concurrent_appends() {
        use std::sync::Arc;
        use std::thread;

        let backlog = Arc::new(ReplicationBacklog::new());
        let mut handles = vec![];

        for i in 0..10 {
            let backlog_clone = Arc::clone(&backlog);
            handles.push(thread::spawn(move || {
                for j in 0..10 {
                    let data = Bytes::from(format!("thread_{i}_cmd_{j}"));
                    backlog_clone.append(data);
                }
            }));
        }

        for handle in handles {
            handle.join().expect("thread panicked");
        }

        // Should have 100 entries (10 threads * 10 commands)
        assert_eq!(backlog.len(), 100);
    }
}

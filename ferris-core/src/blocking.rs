//! Blocking command infrastructure
//!
//! Provides per-key notification mechanism for blocking commands like
//! BLPOP, BRPOP, BLMOVE, BZPOPMIN, BZPOPMAX, etc.
//!
//! When a blocking command finds no data, it registers interest in keys.
//! When a write command (LPUSH, RPUSH, ZADD, etc.) mutates a key, it
//! notifies all waiters registered for that key.

use bytes::Bytes;
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::Notify;

/// A key in the blocking registry: (database index, key name)
type BlockingKey = (usize, Bytes);

/// Registry for per-key blocking notifications.
///
/// This is shared across all connections and the command executor.
/// Write commands call `notify_key` after mutating data.
/// Blocking commands use `get_or_create_notify` to get a `Notify` handle,
/// then `.notified().await` on it.
#[derive(Debug, Default)]
pub struct BlockingRegistry {
    /// Map from (db_index, key) to notification handle.
    /// Uses `DashMap` for concurrent access without global locks.
    waiters: DashMap<BlockingKey, Arc<Notify>>,
}

impl BlockingRegistry {
    /// Create a new empty blocking registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            waiters: DashMap::new(),
        }
    }

    /// Get or create a `Notify` handle for a given (db, key) pair.
    ///
    /// Blocking commands call this to register interest before waiting.
    /// The returned `Arc<Notify>` can be used with `.notified().await`.
    pub fn get_or_create_notify(&self, db_index: usize, key: &Bytes) -> Arc<Notify> {
        let bk = (db_index, key.clone());
        self.waiters
            .entry(bk)
            .or_insert_with(|| Arc::new(Notify::new()))
            .value()
            .clone()
    }

    /// Notify all waiters for a given (db, key) pair.
    ///
    /// Write commands (LPUSH, RPUSH, ZADD, etc.) call this after
    /// successfully adding elements to a key.
    pub fn notify_key(&self, db_index: usize, key: &Bytes) {
        let bk = (db_index, key.clone());
        if let Some(notify) = self.waiters.get(&bk) {
            notify.notify_waiters();
        }
    }

    /// Remove a notification entry if there are no more waiters.
    ///
    /// This is a best-effort cleanup to prevent unbounded growth.
    /// Called by blocking commands after they complete or time out.
    pub fn cleanup_key(&self, db_index: usize, key: &Bytes) {
        let bk = (db_index, key.clone());
        // Only remove if the Arc is not shared (i.e., no other waiters)
        self.waiters.remove_if(&bk, |_, notify| {
            // If strong_count == 1, we hold the only reference in the map,
            // so no one else is waiting on it.
            Arc::strong_count(notify) == 1
        });
    }

    /// Clean up multiple keys at once.
    pub fn cleanup_keys(&self, db_index: usize, keys: &[Bytes]) {
        for key in keys {
            self.cleanup_key(db_index, key);
        }
    }

    /// Get the number of active notification entries (for diagnostics).
    #[must_use]
    pub fn len(&self) -> usize {
        self.waiters.len()
    }

    /// Check if the registry has no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.waiters.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blocking_registry_creation() {
        let registry = BlockingRegistry::new();
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn test_get_or_create_notify() {
        let registry = BlockingRegistry::new();
        let key = Bytes::from("mykey");

        let notify1 = registry.get_or_create_notify(0, &key);
        let notify2 = registry.get_or_create_notify(0, &key);

        // Same key should return the same Arc
        assert!(Arc::ptr_eq(&notify1, &notify2));
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn test_different_keys_different_notifiers() {
        let registry = BlockingRegistry::new();
        let key1 = Bytes::from("key1");
        let key2 = Bytes::from("key2");

        let notify1 = registry.get_or_create_notify(0, &key1);
        let notify2 = registry.get_or_create_notify(0, &key2);

        assert!(!Arc::ptr_eq(&notify1, &notify2));
        assert_eq!(registry.len(), 2);
    }

    #[test]
    fn test_different_dbs_different_notifiers() {
        let registry = BlockingRegistry::new();
        let key = Bytes::from("mykey");

        let notify1 = registry.get_or_create_notify(0, &key);
        let notify2 = registry.get_or_create_notify(1, &key);

        assert!(!Arc::ptr_eq(&notify1, &notify2));
        assert_eq!(registry.len(), 2);
    }

    #[test]
    fn test_notify_key_without_waiters() {
        let registry = BlockingRegistry::new();
        let key = Bytes::from("mykey");

        // Should not panic even if no waiters exist
        registry.notify_key(0, &key);
    }

    #[tokio::test]
    async fn test_notify_wakes_waiter() {
        let registry = Arc::new(BlockingRegistry::new());
        let key = Bytes::from("mykey");

        let notify = registry.get_or_create_notify(0, &key);

        let registry_clone = registry.clone();
        let key_clone = key.clone();

        let handle = tokio::spawn(async move {
            // Small delay then notify
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            registry_clone.notify_key(0, &key_clone);
        });

        // Wait for notification (with timeout)
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            notify.notified(),
        )
        .await;

        assert!(result.is_ok(), "should have been notified");
        handle.await.expect("spawned task should complete");
    }

    #[test]
    fn test_cleanup_key_removes_unused() {
        let registry = BlockingRegistry::new();
        let key = Bytes::from("mykey");

        // Create and immediately drop the Arc
        let _notify = registry.get_or_create_notify(0, &key);
        assert_eq!(registry.len(), 1);

        // Drop the external reference
        drop(_notify);

        // Cleanup should remove it since only the map holds a reference
        registry.cleanup_key(0, &key);
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn test_cleanup_key_keeps_active() {
        let registry = BlockingRegistry::new();
        let key = Bytes::from("mykey");

        let _notify = registry.get_or_create_notify(0, &key);
        assert_eq!(registry.len(), 1);

        // _notify still held, so cleanup should NOT remove
        registry.cleanup_key(0, &key);
        // The map holds 1 ref, _notify holds another = strong_count 2
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn test_cleanup_keys() {
        let registry = BlockingRegistry::new();
        let key1 = Bytes::from("key1");
        let key2 = Bytes::from("key2");
        let key3 = Bytes::from("key3");

        let _n1 = registry.get_or_create_notify(0, &key1);
        let _n2 = registry.get_or_create_notify(0, &key2);
        let _n3 = registry.get_or_create_notify(0, &key3);
        assert_eq!(registry.len(), 3);

        drop(_n1);
        drop(_n2);
        // _n3 still alive

        registry.cleanup_keys(0, &[key1.clone(), key2.clone(), key3.clone()]);

        // key1 and key2 cleaned up, key3 still active
        assert_eq!(registry.len(), 1);
    }
}

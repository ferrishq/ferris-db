//! Key expiration management
//!
//! Handles TTL-based key expiration with both:
//! - Lazy expiry: Keys are checked on access
//! - Active expiry: Background task samples keys periodically

use std::sync::Arc;
use tokio::sync::Notify;

/// Number of keys to sample per active expiry iteration
const ACTIVE_EXPIRY_SAMPLE_SIZE: usize = 20;

/// If this fraction of sampled keys are expired, repeat immediately
const ACTIVE_EXPIRY_REPEAT_THRESHOLD: f64 = 0.25;

/// Sleep between active expiry cycles (milliseconds)
const ACTIVE_EXPIRY_CYCLE_MS: u64 = 100;

/// Manages key expiration across all databases
#[derive(Debug)]
pub struct ExpiryManager {
    /// Notification for shutdown
    shutdown: Arc<Notify>,
}

impl Default for ExpiryManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ExpiryManager {
    /// Create a new expiry manager
    #[must_use]
    pub fn new() -> Self {
        Self {
            shutdown: Arc::new(Notify::new()),
        }
    }

    /// Signal the expiry manager to stop
    pub fn shutdown(&self) {
        self.shutdown.notify_one();
    }

    /// Start the background expiry task
    ///
    /// This task periodically samples keys and removes expired ones.
    /// The algorithm matches Redis's active expiry:
    /// 1. For each database, sample up to 20 random keys with TTL
    /// 2. Delete expired keys
    /// 3. If > 25% were expired, repeat immediately for that database
    /// 4. Otherwise, sleep for 100ms before next cycle
    pub async fn run(&self, store: Arc<crate::store::KeyStore>) {
        loop {
            // Check if shutdown was requested
            tokio::select! {
                () = self.shutdown.notified() => {
                    return;
                }
                () = tokio::time::sleep(std::time::Duration::from_millis(ACTIVE_EXPIRY_CYCLE_MS)) => {
                    // Run one expiry cycle
                    self.expire_cycle(&store);
                }
            }
        }
    }

    /// Run one cycle of active expiry across all databases
    fn expire_cycle(&self, store: &crate::store::KeyStore) {
        let num_dbs = store.num_databases();
        for db_idx in 0..num_dbs {
            let db = store.database(db_idx);
            if db.is_empty() {
                continue;
            }
            self.expire_database(db);
        }
    }

    /// Run active expiry for a single database.
    /// Repeats sampling if more than 25% of sampled keys were expired.
    fn expire_database(&self, db: &crate::store::Database) {
        loop {
            let sampled = db.sample_volatile_keys(ACTIVE_EXPIRY_SAMPLE_SIZE);
            if sampled.is_empty() {
                break;
            }

            let total = sampled.len();
            let mut expired_count = 0;

            for (key, is_expired) in &sampled {
                if *is_expired {
                    if db.delete_if_expired(key) {
                        expired_count += 1;
                    }
                }
            }

            // If less than 25% expired, we're done with this database
            let expired_ratio = expired_count as f64 / total as f64;
            if expired_ratio < ACTIVE_EXPIRY_REPEAT_THRESHOLD {
                break;
            }

            // If we expired 25%+ of sampled keys, there may be more.
            // Repeat immediately (but don't loop forever if db is small).
            if total < ACTIVE_EXPIRY_SAMPLE_SIZE {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::KeyStore;
    use crate::value::{Entry, RedisValue};
    use bytes::Bytes;
    use std::time::{Duration, Instant};

    #[test]
    fn test_expiry_manager_creation() {
        let manager = ExpiryManager::new();
        manager.shutdown(); // Should not panic
    }

    #[test]
    fn test_expire_cycle_removes_expired_keys() {
        let store = Arc::new(KeyStore::default());
        let db = store.database(0);

        // Add some keys with expired TTL
        let mut entry1 = Entry::new(RedisValue::String(Bytes::from("val1")));
        entry1.expires_at = Some(Instant::now() - Duration::from_secs(1));
        db.set(Bytes::from("expired1"), entry1);

        let mut entry2 = Entry::new(RedisValue::String(Bytes::from("val2")));
        entry2.expires_at = Some(Instant::now() - Duration::from_secs(1));
        db.set(Bytes::from("expired2"), entry2);

        // Add a key that should NOT expire
        let mut entry3 = Entry::new(RedisValue::String(Bytes::from("val3")));
        entry3.expires_at = Some(Instant::now() + Duration::from_secs(3600));
        db.set(Bytes::from("alive"), entry3);

        // Add a key with no TTL
        db.set(
            Bytes::from("no_ttl"),
            Entry::new(RedisValue::String(Bytes::from("val4"))),
        );

        assert_eq!(db.len(), 4);

        let manager = ExpiryManager::new();
        manager.expire_cycle(&store);

        // Expired keys should be removed, others should remain
        assert_eq!(db.len(), 2);
        assert!(!db.exists(b"expired1"));
        assert!(!db.exists(b"expired2"));
        assert!(db.exists(b"alive"));
        assert!(db.exists(b"no_ttl"));
    }

    #[test]
    fn test_expire_cycle_no_keys() {
        let store = Arc::new(KeyStore::default());
        let manager = ExpiryManager::new();
        // Should not panic on empty databases
        manager.expire_cycle(&store);
    }

    #[test]
    fn test_expire_cycle_no_volatile_keys() {
        let store = Arc::new(KeyStore::default());
        let db = store.database(0);

        // Keys without TTL
        db.set(
            Bytes::from("key1"),
            Entry::new(RedisValue::String(Bytes::from("val1"))),
        );
        db.set(
            Bytes::from("key2"),
            Entry::new(RedisValue::String(Bytes::from("val2"))),
        );

        let manager = ExpiryManager::new();
        manager.expire_cycle(&store);

        // All keys should remain
        assert_eq!(db.len(), 2);
    }

    #[test]
    fn test_expire_database_with_many_expired() {
        let store = Arc::new(KeyStore::default());
        let db = store.database(0);

        // Add 30 expired keys (more than sample size)
        for i in 0..30 {
            let mut entry = Entry::new(RedisValue::String(Bytes::from(format!("val{i}"))));
            entry.expires_at = Some(Instant::now() - Duration::from_secs(1));
            db.set(Bytes::from(format!("expired{i}")), entry);
        }

        // Add 5 alive keys
        for i in 0..5 {
            let mut entry = Entry::new(RedisValue::String(Bytes::from(format!("alive{i}"))));
            entry.expires_at = Some(Instant::now() + Duration::from_secs(3600));
            db.set(Bytes::from(format!("alive{i}")), entry);
        }

        assert_eq!(db.len(), 35);

        let manager = ExpiryManager::new();
        manager.expire_database(db);

        // All 30 expired keys should be removed, 5 alive should remain
        assert_eq!(db.len(), 5);
        for i in 0..5 {
            assert!(db.exists(format!("alive{i}").as_bytes()));
        }
    }

    #[test]
    fn test_expire_cycle_multiple_databases() {
        let store = Arc::new(KeyStore::default());

        // Add expired keys to db 0 and db 1
        let mut entry0 = Entry::new(RedisValue::String(Bytes::from("val0")));
        entry0.expires_at = Some(Instant::now() - Duration::from_secs(1));
        store.database(0).set(Bytes::from("key0"), entry0);

        let mut entry1 = Entry::new(RedisValue::String(Bytes::from("val1")));
        entry1.expires_at = Some(Instant::now() - Duration::from_secs(1));
        store.database(1).set(Bytes::from("key1"), entry1);

        // Add alive key to db 2
        store.database(2).set(
            Bytes::from("key2"),
            Entry::new(RedisValue::String(Bytes::from("val2"))),
        );

        let manager = ExpiryManager::new();
        manager.expire_cycle(&store);

        assert_eq!(store.database(0).len(), 0);
        assert_eq!(store.database(1).len(), 0);
        assert_eq!(store.database(2).len(), 1);
    }

    #[tokio::test]
    async fn test_expiry_manager_run_and_shutdown() {
        let store = Arc::new(KeyStore::default());
        let manager = Arc::new(ExpiryManager::new());

        let manager_clone = manager.clone();
        let store_clone = store.clone();
        let handle = tokio::spawn(async move {
            manager_clone.run(store_clone).await;
        });

        // Let it run briefly
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Shutdown
        manager.shutdown();

        // Should complete without hanging
        let result = tokio::time::timeout(Duration::from_secs(2), handle).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_expiry_manager_cleans_expired_during_run() {
        let store = Arc::new(KeyStore::default());
        let db = store.database(0);

        // Add an expired key
        let mut entry = Entry::new(RedisValue::String(Bytes::from("val")));
        entry.expires_at = Some(Instant::now() - Duration::from_secs(1));
        db.set(Bytes::from("expired_key"), entry);

        assert_eq!(db.len(), 1);

        let manager = Arc::new(ExpiryManager::new());
        let manager_clone = manager.clone();
        let store_clone = store.clone();
        let handle = tokio::spawn(async move {
            manager_clone.run(store_clone).await;
        });

        // Wait for at least one cycle (100ms sleep + processing)
        tokio::time::sleep(Duration::from_millis(250)).await;

        // The expired key should have been cleaned
        assert_eq!(db.len(), 0);

        manager.shutdown();
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
    }

    #[test]
    fn test_sample_volatile_keys() {
        let store = KeyStore::default();
        let db = store.database(0);

        // Mix of volatile and non-volatile keys
        let mut volatile = Entry::new(RedisValue::String(Bytes::from("v")));
        volatile.expires_at = Some(Instant::now() + Duration::from_secs(3600));
        db.set(Bytes::from("volatile1"), volatile.clone());
        db.set(Bytes::from("volatile2"), volatile);

        db.set(
            Bytes::from("permanent"),
            Entry::new(RedisValue::String(Bytes::from("p"))),
        );

        let sampled = db.sample_volatile_keys(20);
        assert_eq!(sampled.len(), 2); // Only volatile keys
        assert!(sampled.iter().all(|(_, expired)| !expired));
    }

    #[test]
    fn test_delete_if_expired() {
        let store = KeyStore::default();
        let db = store.database(0);

        // Non-expired key
        let mut alive = Entry::new(RedisValue::String(Bytes::from("v")));
        alive.expires_at = Some(Instant::now() + Duration::from_secs(3600));
        db.set(Bytes::from("alive"), alive);

        // Expired key
        let mut dead = Entry::new(RedisValue::String(Bytes::from("v")));
        dead.expires_at = Some(Instant::now() - Duration::from_secs(1));
        db.set(Bytes::from("dead"), dead);

        assert!(!db.delete_if_expired(b"alive"));
        assert!(db.exists(b"alive"));

        assert!(db.delete_if_expired(b"dead"));
        assert!(!db.exists(b"dead"));

        // Non-existent key
        assert!(!db.delete_if_expired(b"nonexistent"));
    }
}

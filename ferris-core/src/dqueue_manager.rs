//! Background task for distributed queue maintenance.
//!
//! The `DQueueManager` periodically scans all databases for inflight messages
//! whose visibility deadline has passed and re-queues them (or moves them to
//! the dead-letter queue if the attempt limit is exceeded).
//!
//! This complements the lazy re-queue that happens on every POP call —
//! the background task ensures orphaned queues (where no consumer is calling
//! POP) are also maintained, providing reliable at-least-once delivery
//! even when consumers are offline.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Notify;

use crate::store::UpdateAction;
use crate::value::{Entry, RedisValue};
use bytes::Bytes;

/// How often the background task scans for expired inflight messages (ms)
const SCAN_INTERVAL_MS: u64 = 1_000;

/// Maximum delivery attempts before sending to DLQ (mirrors dqueue.rs constant)
const MAX_DELIVERY_ATTEMPTS: u32 = 3;

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ─────────────────────────────────────────────────────────────────────────────
// Internal types (must be at module level, not inside functions)
// ─────────────────────────────────────────────────────────────────────────────

struct ExpiredInflight {
    msg_id: u64,
    attempts: u32,
    payload: Vec<u8>,
}

// ─────────────────────────────────────────────────────────────────────────────
// DQueueManager
// ─────────────────────────────────────────────────────────────────────────────

/// Background manager for distributed queue inflight maintenance.
#[derive(Debug)]
pub struct DQueueManager {
    shutdown: Arc<Notify>,
}

impl Default for DQueueManager {
    fn default() -> Self {
        Self::new()
    }
}

impl DQueueManager {
    /// Create a new `DQueueManager`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            shutdown: Arc::new(Notify::new()),
        }
    }

    /// Signal the manager to stop.
    pub fn shutdown(&self) {
        self.shutdown.notify_one();
    }

    /// Run the background maintenance loop.
    pub async fn run(&self, store: Arc<crate::store::KeyStore>) {
        loop {
            tokio::select! {
                () = self.shutdown.notified() => {
                    return;
                }
                () = tokio::time::sleep(std::time::Duration::from_millis(SCAN_INTERVAL_MS)) => {
                    self.scan_cycle(&store);
                }
            }
        }
    }

    /// Run one maintenance cycle across all databases.
    pub fn scan_cycle(&self, store: &crate::store::KeyStore) {
        let num_dbs = store.num_databases();
        let now = now_ms();
        for db_idx in 0..num_dbs {
            let db = store.database(db_idx);
            if db.is_empty() {
                continue;
            }
            scan_database(db, now);
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Database-level scan helpers (free functions to keep scan_database short)
// ─────────────────────────────────────────────────────────────────────────────

/// Drain expired entries from the inflight hash, returning them for processing.
fn collect_expired(db: &crate::store::Database, ikey: &Bytes, now: u64) -> Vec<ExpiredInflight> {
    db.update(ikey.clone(), |entry| {
        let Some(e) = entry else {
            return (UpdateAction::Keep, vec![]);
        };
        let RedisValue::Hash(map) = &mut e.value else {
            return (UpdateAction::Keep, vec![]);
        };

        let mut expired_ids: Vec<String> = Vec::new();
        let mut results: Vec<ExpiredInflight> = Vec::new();

        for (id_bytes, val_bytes) in map.iter() {
            let s = std::str::from_utf8(val_bytes).unwrap_or("");
            // Inflight format: deadline_ms|attempts|payload
            let mut parts = s.splitn(3, '|');
            let deadline_ms: u64 = parts
                .next()
                .and_then(|p| p.parse().ok())
                .unwrap_or(u64::MAX);
            let attempts: u32 = parts.next().and_then(|p| p.parse().ok()).unwrap_or(0);
            let payload_escaped = parts.next().unwrap_or("");

            if deadline_ms <= now {
                let id_str = std::str::from_utf8(id_bytes).unwrap_or("").to_owned();
                if let Ok(msg_id) = id_str.parse::<u64>() {
                    expired_ids.push(id_str);
                    results.push(ExpiredInflight {
                        msg_id,
                        attempts,
                        payload: unescape(payload_escaped),
                    });
                }
            }
        }

        for id in &expired_ids {
            map.remove(id.as_bytes());
        }

        if map.is_empty() {
            (UpdateAction::Delete, results)
        } else {
            (UpdateAction::Keep, results)
        }
    })
}

/// Move a message to the dead-letter queue.
fn push_dlq(db: &crate::store::Database, dkey: &Bytes, exp: &ExpiredInflight, now: u64) {
    let encoded = format!(
        "{}|{}|{}|{}",
        exp.msg_id,
        exp.attempts,
        now,
        escape(&exp.payload)
    );
    db.update(dkey.clone(), |entry| {
        if let Some(e) = entry {
            if let RedisValue::List(list) = &mut e.value {
                list.push_back(Bytes::from(encoded.clone()));
            }
            (UpdateAction::Keep, ())
        } else {
            let mut list = VecDeque::new();
            list.push_back(Bytes::from(encoded.clone()));
            (UpdateAction::Set(Entry::new(RedisValue::List(list))), ())
        }
    });
}

/// Re-queue an expired inflight message back to the pending list.
fn push_pending(db: &crate::store::Database, pkey: &Bytes, exp: &ExpiredInflight, now: u64) {
    // pending format: msg_id|priority|visible_at_ms|attempts|payload
    let encoded = format!(
        "{}|0|{}|{}|{}",
        exp.msg_id,
        now,
        exp.attempts,
        escape(&exp.payload)
    );
    db.update(pkey.clone(), |entry| {
        if let Some(e) = entry {
            if let RedisValue::List(list) = &mut e.value {
                list.push_front(Bytes::from(encoded.clone()));
            }
            (UpdateAction::Keep, ())
        } else {
            let mut list = VecDeque::new();
            list.push_front(Bytes::from(encoded.clone()));
            (UpdateAction::Set(Entry::new(RedisValue::List(list))), ())
        }
    });
}

/// Scan a single database for expired inflight messages.
fn scan_database(db: &crate::store::Database, now: u64) {
    let inflight_prefix = b"__dqueue:";
    let inflight_suffix = b":inflight";

    // Collect all inflight keys (O(n) scan, acceptable for background task)
    let inflight_keys: Vec<Bytes> = db
        .scan_keys_matching(|key: &[u8]| {
            key.starts_with(inflight_prefix) && key.ends_with(inflight_suffix)
        })
        .into_iter()
        .map(Bytes::from)
        .collect();

    for ikey in inflight_keys {
        // Extract queue name: `__dqueue:<name>:inflight` → `<name>`
        let key_bytes = &ikey[..];
        let prefix_len = inflight_prefix.len();
        let suffix_len = inflight_suffix.len();
        if key_bytes.len() <= prefix_len + suffix_len {
            continue;
        }
        let name = &key_bytes[prefix_len..key_bytes.len() - suffix_len];

        // Build sibling keys
        let pkey = {
            let mut v = b"__dqueue:".to_vec();
            v.extend_from_slice(name);
            v.extend_from_slice(b":pending");
            Bytes::from(v)
        };
        let dkey = {
            let mut v = b"__dqueue:".to_vec();
            v.extend_from_slice(name);
            v.extend_from_slice(b":dlq");
            Bytes::from(v)
        };

        let expired = collect_expired(db, &ikey, now);
        if expired.is_empty() {
            continue;
        }

        for exp in &expired {
            if exp.attempts >= MAX_DELIVERY_ATTEMPTS {
                push_dlq(db, &dkey, exp, now);
            } else {
                push_pending(db, &pkey, exp, now);
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers (duplicated from dqueue.rs to avoid cross-crate dep on ferris-commands)
// ─────────────────────────────────────────────────────────────────────────────

fn escape(s: &[u8]) -> String {
    let escaped = s
        .iter()
        .flat_map(|&b| {
            if b == b'\\' {
                vec![b'\\', b'\\']
            } else if b == b'|' {
                vec![b'\\', b'|']
            } else {
                vec![b]
            }
        })
        .collect::<Vec<u8>>();
    String::from_utf8_lossy(&escaped).into_owned()
}

fn unescape(s: &str) -> Vec<u8> {
    let bytes = s.as_bytes();
    let mut result = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\\' && i + 1 < bytes.len() {
            result.push(bytes[i + 1]);
            i += 2;
        } else {
            result.push(bytes[i]);
            i += 1;
        }
    }
    result
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::KeyStore;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn make_inflight_entry(deadline_ms: u64, attempts: u32, payload: &[u8]) -> String {
        format!("{}|{}|{}", deadline_ms, attempts, escape(payload))
    }

    fn insert_inflight(
        db: &crate::store::Database,
        queue: &str,
        msg_id: u64,
        deadline_ms: u64,
        attempts: u32,
        payload: &[u8],
    ) {
        let ikey = Bytes::from(format!("__dqueue:{}:inflight", queue));
        let val = make_inflight_entry(deadline_ms, attempts, payload);
        db.update(ikey.clone(), |entry| {
            if let Some(e) = entry {
                if let RedisValue::Hash(map) = &mut e.value {
                    map.insert(Bytes::from(msg_id.to_string()), Bytes::from(val.clone()));
                }
                (UpdateAction::Keep, ())
            } else {
                let mut map = HashMap::new();
                map.insert(Bytes::from(msg_id.to_string()), Bytes::from(val.clone()));
                (UpdateAction::Set(Entry::new(RedisValue::Hash(map))), ())
            }
        });
    }

    fn pending_len(db: &crate::store::Database, queue: &str) -> usize {
        let pkey = Bytes::from(format!("__dqueue:{}:pending", queue));
        match db.get(&pkey) {
            Some(e) => {
                if let RedisValue::List(list) = &e.value {
                    list.len()
                } else {
                    0
                }
            }
            None => 0,
        }
    }

    fn dlq_len(db: &crate::store::Database, queue: &str) -> usize {
        let dkey = Bytes::from(format!("__dqueue:{}:dlq", queue));
        match db.get(&dkey) {
            Some(e) => {
                if let RedisValue::List(list) = &e.value {
                    list.len()
                } else {
                    0
                }
            }
            None => 0,
        }
    }

    fn inflight_len(db: &crate::store::Database, queue: &str) -> usize {
        let ikey = Bytes::from(format!("__dqueue:{}:inflight", queue));
        match db.get(&ikey) {
            Some(e) => {
                if let RedisValue::Hash(map) = &e.value {
                    map.len()
                } else {
                    0
                }
            }
            None => 0,
        }
    }

    #[test]
    fn test_manager_creation() {
        let manager = DQueueManager::new();
        manager.shutdown(); // Should not panic
    }

    #[test]
    fn test_scan_cycle_empty_store() {
        let store = Arc::new(KeyStore::default());
        let manager = DQueueManager::new();
        manager.scan_cycle(&store); // Must not panic
    }

    #[test]
    fn test_scan_requeues_expired_inflight() {
        let store = Arc::new(KeyStore::default());
        let db = store.database(0);

        // Insert an expired inflight message (attempts=1, deadline=0 = already expired)
        insert_inflight(db, "myqueue", 1, 0, 1, b"hello");

        assert_eq!(inflight_len(db, "myqueue"), 1);
        assert_eq!(pending_len(db, "myqueue"), 0);

        let manager = DQueueManager::new();
        manager.scan_cycle(&store);

        // Should be moved to pending
        assert_eq!(inflight_len(db, "myqueue"), 0);
        assert_eq!(pending_len(db, "myqueue"), 1);
        assert_eq!(dlq_len(db, "myqueue"), 0);
    }

    #[test]
    fn test_scan_moves_to_dlq_at_max_attempts() {
        let store = Arc::new(KeyStore::default());
        let db = store.database(0);

        // Insert an expired inflight message that has hit max attempts
        insert_inflight(db, "myqueue", 1, 0, MAX_DELIVERY_ATTEMPTS, b"doomed");

        let manager = DQueueManager::new();
        manager.scan_cycle(&store);

        // Should be moved to DLQ, not pending
        assert_eq!(inflight_len(db, "myqueue"), 0);
        assert_eq!(pending_len(db, "myqueue"), 0);
        assert_eq!(dlq_len(db, "myqueue"), 1);
    }

    #[test]
    fn test_scan_does_not_touch_non_expired_inflight() {
        let store = Arc::new(KeyStore::default());
        let db = store.database(0);

        // Far-future deadline — should NOT be re-queued
        let future_deadline = now_ms() + 60_000;
        insert_inflight(db, "myqueue", 1, future_deadline, 1, b"pending");

        let manager = DQueueManager::new();
        manager.scan_cycle(&store);

        // Should remain in inflight
        assert_eq!(inflight_len(db, "myqueue"), 1);
        assert_eq!(pending_len(db, "myqueue"), 0);
    }

    #[test]
    fn test_scan_multiple_databases() {
        let store = Arc::new(KeyStore::default());

        // Expired inflight in db 0
        insert_inflight(store.database(0), "q1", 1, 0, 1, b"db0-msg");
        // Expired inflight in db 1
        insert_inflight(store.database(1), "q2", 2, 0, 1, b"db1-msg");

        let manager = DQueueManager::new();
        manager.scan_cycle(&store);

        assert_eq!(pending_len(store.database(0), "q1"), 1);
        assert_eq!(pending_len(store.database(1), "q2"), 1);
    }

    #[test]
    fn test_scan_multiple_queues_same_db() {
        let store = Arc::new(KeyStore::default());
        let db = store.database(0);

        insert_inflight(db, "queue-a", 1, 0, 1, b"msg-a");
        insert_inflight(db, "queue-b", 2, 0, 1, b"msg-b");

        let manager = DQueueManager::new();
        manager.scan_cycle(&store);

        assert_eq!(pending_len(db, "queue-a"), 1);
        assert_eq!(pending_len(db, "queue-b"), 1);
    }

    #[tokio::test]
    async fn test_manager_run_and_shutdown() {
        let store = Arc::new(KeyStore::default());
        let manager = Arc::new(DQueueManager::new());

        let m = manager.clone();
        let s = store.clone();
        let handle = tokio::spawn(async move { m.run(s).await });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        manager.shutdown();
        let result = tokio::time::timeout(std::time::Duration::from_secs(2), handle).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_manager_requeues_during_run() {
        let store = Arc::new(KeyStore::default());
        let db = store.database(0);

        // Insert expired inflight
        insert_inflight(db, "myq", 1, 0, 1, b"payload");
        assert_eq!(inflight_len(db, "myq"), 1);

        let manager = Arc::new(DQueueManager::new());
        let m = manager.clone();
        let s = store.clone();
        let handle = tokio::spawn(async move { m.run(s).await });

        // Wait for at least one scan cycle (1s interval)
        tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

        // The message should have been re-queued to pending
        assert_eq!(
            pending_len(db, "myq"),
            1,
            "expired inflight should be in pending after scan"
        );

        manager.shutdown();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), handle).await;
    }
}

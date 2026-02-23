//! Watch registry for transaction WATCH command
//!
//! Tracks which connections are watching which keys, and notifies
//! them when watched keys are modified (for optimistic locking).

use bytes::Bytes;
use dashmap::DashMap;
use std::collections::HashSet;
use std::sync::Arc;

/// Client ID type (same as in CommandContext)
pub type ClientId = u64;

/// Registry for tracking WATCHed keys across connections
#[derive(Debug, Clone)]
pub struct WatchRegistry {
    /// Map of (db_index, key) -> set of client IDs watching this key
    watches: Arc<DashMap<(usize, Bytes), HashSet<ClientId>>>,
}

impl Default for WatchRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl WatchRegistry {
    /// Create a new watch registry
    pub fn new() -> Self {
        Self {
            watches: Arc::new(DashMap::new()),
        }
    }

    /// Register a client as watching a key
    pub fn watch_key(&self, client_id: ClientId, db_index: usize, key: Bytes) {
        self.watches
            .entry((db_index, key))
            .or_insert_with(HashSet::new)
            .insert(client_id);
    }

    /// Remove all watches for a client
    pub fn unwatch_all(&self, client_id: ClientId) {
        // Remove client from all watch sets
        self.watches.retain(|_, clients| {
            clients.remove(&client_id);
            !clients.is_empty() // Remove entry if no clients left
        });
    }

    /// Notify that a key was modified, returning the list of client IDs
    /// that were watching it.
    pub fn notify_modification(&self, db_index: usize, key: &[u8]) -> Vec<ClientId> {
        let key_bytes = Bytes::copy_from_slice(key);
        if let Some(entry) = self.watches.get(&(db_index, key_bytes)) {
            // Return all watching clients
            entry.iter().copied().collect()
        } else {
            Vec::new()
        }
    }

    /// Check if a key is being watched by any client
    pub fn is_watched(&self, db_index: usize, key: &[u8]) -> bool {
        let key_bytes = Bytes::copy_from_slice(key);
        self.watches
            .get(&(db_index, key_bytes))
            .map_or(false, |clients| !clients.is_empty())
    }

    /// Get the number of keys being watched
    #[cfg(test)]
    pub fn watch_count(&self) -> usize {
        self.watches.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_watch_registry_new() {
        let registry = WatchRegistry::new();
        assert_eq!(registry.watch_count(), 0);
    }

    #[test]
    fn test_watch_key() {
        let registry = WatchRegistry::new();
        registry.watch_key(1, 0, Bytes::from("key1"));
        registry.watch_key(2, 0, Bytes::from("key1"));
        registry.watch_key(1, 0, Bytes::from("key2"));

        assert!(registry.is_watched(0, b"key1"));
        assert!(registry.is_watched(0, b"key2"));
        assert!(!registry.is_watched(0, b"key3"));
    }

    #[test]
    fn test_unwatch_all() {
        let registry = WatchRegistry::new();
        registry.watch_key(1, 0, Bytes::from("key1"));
        registry.watch_key(2, 0, Bytes::from("key1"));
        registry.watch_key(1, 0, Bytes::from("key2"));

        registry.unwatch_all(1);

        // key1 should still be watched by client 2
        assert!(registry.is_watched(0, b"key1"));
        // key2 should no longer be watched (only client 1 was watching it)
        assert!(!registry.is_watched(0, b"key2"));
    }

    #[test]
    fn test_notify_modification() {
        let registry = WatchRegistry::new();
        registry.watch_key(1, 0, Bytes::from("key1"));
        registry.watch_key(2, 0, Bytes::from("key1"));
        registry.watch_key(3, 1, Bytes::from("key1")); // Different DB

        let clients = registry.notify_modification(0, b"key1");
        assert_eq!(clients.len(), 2);
        assert!(clients.contains(&1));
        assert!(clients.contains(&2));
        assert!(!clients.contains(&3)); // Different DB
    }

    #[test]
    fn test_watch_different_databases() {
        let registry = WatchRegistry::new();
        registry.watch_key(1, 0, Bytes::from("key"));
        registry.watch_key(2, 1, Bytes::from("key"));

        assert!(registry.is_watched(0, b"key"));
        assert!(registry.is_watched(1, b"key"));

        let clients_db0 = registry.notify_modification(0, b"key");
        let clients_db1 = registry.notify_modification(1, b"key");

        assert_eq!(clients_db0, vec![1]);
        assert_eq!(clients_db1, vec![2]);
    }
}

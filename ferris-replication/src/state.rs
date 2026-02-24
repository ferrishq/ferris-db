//! Replication state management
//!
//! Tracks the replication role, IDs, and offsets for a ferris-db instance.

#![allow(clippy::missing_panics_doc)] // Lock poisoning is unrecoverable
#![allow(clippy::expect_used)] // Lock poisoning is fatal error

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

/// The replication role of this instance
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationRole {
    /// This instance is a master (leader)
    Master,
    /// This instance is a replica (follower)
    Replica,
}

#[allow(clippy::derivable_impls)] // Explicit default is clearer than derive
impl Default for ReplicationRole {
    fn default() -> Self {
        Self::Master
    }
}

impl std::fmt::Display for ReplicationRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Master => write!(f, "master"),
            Self::Replica => write!(f, "slave"),
        }
    }
}

/// Master configuration when this instance is a replica
#[derive(Debug, Clone, Default)]
pub struct MasterInfo {
    /// Host of the master
    pub host: String,
    /// Port of the master
    pub port: u16,
}

/// Replication state for a ferris-db instance
///
/// This struct is thread-safe and can be shared across connections.
#[derive(Debug)]
pub struct ReplicationState {
    /// Current replication role
    role: RwLock<ReplicationRole>,

    /// Replication ID - 40-character hex string unique to this master
    /// Changes when a replica is promoted to master
    repl_id: RwLock<String>,

    /// Secondary replication ID for PSYNC2
    /// Used when a replica becomes master and needs to accept replicas
    /// that were syncing with the old master
    repl_id2: RwLock<String>,

    /// Offset at which `repl_id2` is valid (for partial resync with old ID)
    repl_id2_offset: AtomicU64,

    /// Current replication offset (bytes written to replication stream)
    master_repl_offset: AtomicU64,

    /// Master information (when this instance is a replica)
    master_info: RwLock<Option<MasterInfo>>,

    /// Number of connected replicas (when this instance is a master)
    connected_replicas: AtomicU64,
}

impl Default for ReplicationState {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplicationState {
    /// Create a new replication state with a fresh replication ID
    #[must_use]
    pub fn new() -> Self {
        Self {
            role: RwLock::new(ReplicationRole::Master),
            repl_id: RwLock::new(Self::generate_repl_id()),
            repl_id2: RwLock::new("0".repeat(40)), // "0000...0000" means no secondary ID
            repl_id2_offset: AtomicU64::new(u64::MAX), // Invalid offset
            master_repl_offset: AtomicU64::new(0),
            master_info: RwLock::new(None),
            connected_replicas: AtomicU64::new(0),
        }
    }

    /// Generate a new 40-character hex replication ID
    fn generate_repl_id() -> String {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let bytes: [u8; 20] = rng.gen();
        bytes.iter().fold(String::with_capacity(40), |mut acc, b| {
            use std::fmt::Write;
            let _ = write!(acc, "{b:02x}");
            acc
        })
    }

    /// Get the current replication role
    #[must_use]
    pub fn role(&self) -> ReplicationRole {
        *self.role.read().expect("role lock poisoned")
    }

    /// Set the replication role
    pub fn set_role(&self, role: ReplicationRole) {
        *self.role.write().expect("role lock poisoned") = role;
    }

    /// Get the current replication ID
    #[must_use]
    pub fn repl_id(&self) -> String {
        self.repl_id.read().expect("repl_id lock poisoned").clone()
    }

    /// Get the secondary replication ID
    #[must_use]
    pub fn repl_id2(&self) -> String {
        self.repl_id2
            .read()
            .expect("repl_id2 lock poisoned")
            .clone()
    }

    /// Get the offset at which `repl_id2` is valid
    #[must_use]
    pub fn repl_id2_offset(&self) -> u64 {
        self.repl_id2_offset.load(Ordering::Acquire)
    }

    /// Get the current replication offset
    #[must_use]
    pub fn master_repl_offset(&self) -> u64 {
        self.master_repl_offset.load(Ordering::Acquire)
    }

    /// Increment the replication offset by the given amount
    /// Returns the new offset
    pub fn increment_offset(&self, bytes: u64) -> u64 {
        self.master_repl_offset.fetch_add(bytes, Ordering::AcqRel) + bytes
    }

    /// Set the replication offset to a specific value
    pub fn set_offset(&self, offset: u64) {
        self.master_repl_offset.store(offset, Ordering::Release);
    }

    /// Get the master info (if this instance is a replica)
    #[must_use]
    pub fn master_info(&self) -> Option<MasterInfo> {
        self.master_info
            .read()
            .expect("master_info lock poisoned")
            .clone()
    }

    /// Set the master info (making this instance a replica)
    pub fn set_master(&self, host: String, port: u16) {
        self.set_role(ReplicationRole::Replica);
        *self.master_info.write().expect("master_info lock poisoned") =
            Some(MasterInfo { host, port });
    }

    /// Clear the master info (promoting to master)
    pub fn promote_to_master(&self) {
        // Store current ID as secondary ID for PSYNC2
        let current_id = self.repl_id();
        let current_offset = self.master_repl_offset();

        *self.repl_id2.write().expect("repl_id2 lock poisoned") = current_id;
        self.repl_id2_offset
            .store(current_offset, Ordering::Release);

        // Generate new ID and become master
        *self.repl_id.write().expect("repl_id lock poisoned") = Self::generate_repl_id();
        self.set_role(ReplicationRole::Master);
        *self.master_info.write().expect("master_info lock poisoned") = None;
    }

    /// Get the number of connected replicas
    #[must_use]
    pub fn connected_replicas(&self) -> u64 {
        self.connected_replicas.load(Ordering::Acquire)
    }

    /// Increment the connected replicas count
    pub fn add_replica(&self) -> u64 {
        self.connected_replicas.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// Decrement the connected replicas count
    pub fn remove_replica(&self) -> u64 {
        let prev = self.connected_replicas.fetch_sub(1, Ordering::AcqRel);
        prev.saturating_sub(1)
    }

    /// Check if this instance is a master
    #[must_use]
    pub fn is_master(&self) -> bool {
        self.role() == ReplicationRole::Master
    }

    /// Check if this instance is a replica
    #[must_use]
    pub fn is_replica(&self) -> bool {
        self.role() == ReplicationRole::Replica
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_role_is_master() {
        let state = ReplicationState::new();
        assert_eq!(state.role(), ReplicationRole::Master);
        assert!(state.is_master());
        assert!(!state.is_replica());
    }

    #[test]
    fn test_generate_repl_id_is_40_hex_chars() {
        let id = ReplicationState::generate_repl_id();
        assert_eq!(id.len(), 40);
        assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_generate_repl_id_is_unique() {
        let id1 = ReplicationState::generate_repl_id();
        let id2 = ReplicationState::generate_repl_id();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_initial_offset_is_zero() {
        let state = ReplicationState::new();
        assert_eq!(state.master_repl_offset(), 0);
    }

    #[test]
    fn test_increment_offset() {
        let state = ReplicationState::new();
        assert_eq!(state.increment_offset(100), 100);
        assert_eq!(state.master_repl_offset(), 100);
        assert_eq!(state.increment_offset(50), 150);
        assert_eq!(state.master_repl_offset(), 150);
    }

    #[test]
    fn test_set_offset() {
        let state = ReplicationState::new();
        state.set_offset(500);
        assert_eq!(state.master_repl_offset(), 500);
    }

    #[test]
    fn test_set_master() {
        let state = ReplicationState::new();
        state.set_master("127.0.0.1".to_string(), 6379);

        assert!(state.is_replica());
        let master = state.master_info().expect("should have master info");
        assert_eq!(master.host, "127.0.0.1");
        assert_eq!(master.port, 6379);
    }

    #[test]
    fn test_promote_to_master() {
        let state = ReplicationState::new();
        let original_id = state.repl_id();

        // Become a replica first
        state.set_master("127.0.0.1".to_string(), 6379);
        state.set_offset(1000);
        assert!(state.is_replica());

        // Promote to master
        state.promote_to_master();
        assert!(state.is_master());
        assert!(state.master_info().is_none());

        // New ID should be generated
        assert_ne!(state.repl_id(), original_id);

        // Original ID should be stored as secondary
        assert_eq!(state.repl_id2(), original_id);
        assert_eq!(state.repl_id2_offset(), 1000);
    }

    #[test]
    fn test_connected_replicas() {
        let state = ReplicationState::new();
        assert_eq!(state.connected_replicas(), 0);

        assert_eq!(state.add_replica(), 1);
        assert_eq!(state.add_replica(), 2);
        assert_eq!(state.connected_replicas(), 2);

        assert_eq!(state.remove_replica(), 1);
        assert_eq!(state.connected_replicas(), 1);
    }

    #[test]
    fn test_role_display() {
        assert_eq!(format!("{}", ReplicationRole::Master), "master");
        assert_eq!(format!("{}", ReplicationRole::Replica), "slave");
    }

    #[test]
    fn test_thread_safety() {
        use std::sync::Arc;
        use std::thread;

        let state = Arc::new(ReplicationState::new());
        let mut handles = vec![];

        // Spawn multiple threads to increment offset
        for _ in 0..10 {
            let state_clone = Arc::clone(&state);
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    state_clone.increment_offset(1);
                }
            }));
        }

        for handle in handles {
            handle.join().expect("thread panicked");
        }

        // 10 threads * 100 increments = 1000
        assert_eq!(state.master_repl_offset(), 1000);
    }

    #[test]
    fn test_repl_id2_initial_state() {
        let state = ReplicationState::new();
        assert_eq!(state.repl_id2(), "0".repeat(40));
        assert_eq!(state.repl_id2_offset(), u64::MAX);
    }

    #[test]
    fn test_master_info_cleared_on_promotion() {
        let state = ReplicationState::new();
        state.set_master("localhost".to_string(), 6380);
        assert!(state.master_info().is_some());

        state.promote_to_master();
        assert!(state.master_info().is_none());
    }
}

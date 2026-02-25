//! Replication manager - high-level coordination of replication state and backlog

use crate::backlog::{BacklogConfig, ReplicationBacklog};
use crate::consistency::ConsistencyMode;
use crate::follower::Follower;
use crate::follower_tracker::FollowerTracker;
use crate::state::ReplicationState;
use bytes::{BufMut, Bytes, BytesMut};
use ferris_protocol::RespValue;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};

/// High-level replication manager
///
/// Coordinates replication state, backlog, and connected replicas.
pub struct ReplicationManager {
    state: ReplicationState,
    backlog: ReplicationBacklog,
    /// Optional follower instance (when this server is a replica)
    follower: Arc<RwLock<Option<Arc<Follower>>>>,
    /// Tracker for connected followers (when this server is a leader)
    follower_tracker: Arc<FollowerTracker>,
    /// Broadcast channel for streaming new commands to followers
    command_broadcast: broadcast::Sender<Bytes>,
    /// Replication consistency mode
    consistency_mode: Arc<RwLock<ConsistencyMode>>,
}

impl Default for ReplicationManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplicationManager {
    /// Create a new replication manager with default configuration
    #[must_use]
    pub fn new() -> Self {
        // Create broadcast channel for streaming commands to followers
        // Capacity of 1000 commands - if a follower lags more than this, it will miss commands
        let (command_broadcast, _) = broadcast::channel(1000);

        Self {
            state: ReplicationState::new(),
            backlog: ReplicationBacklog::new(),
            follower: Arc::new(RwLock::new(None)),
            follower_tracker: Arc::new(FollowerTracker::new()),
            command_broadcast,
            consistency_mode: Arc::new(RwLock::new(ConsistencyMode::default())),
        }
    }

    /// Create a new replication manager with custom backlog configuration
    #[must_use]
    pub fn with_config(backlog_config: BacklogConfig) -> Self {
        let (command_broadcast, _) = broadcast::channel(1000);

        Self {
            state: ReplicationState::new(),
            backlog: ReplicationBacklog::with_config(backlog_config),
            follower: Arc::new(RwLock::new(None)),
            follower_tracker: Arc::new(FollowerTracker::new()),
            command_broadcast,
            consistency_mode: Arc::new(RwLock::new(ConsistencyMode::default())),
        }
    }

    /// Subscribe to the command broadcast channel
    ///
    /// Returns a receiver that will get all new commands appended to the backlog
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<Bytes> {
        self.command_broadcast.subscribe()
    }

    /// Get the follower tracker
    #[must_use]
    pub const fn follower_tracker(&self) -> &Arc<FollowerTracker> {
        &self.follower_tracker
    }

    /// Get the follower instance (if this server is a replica)
    #[must_use]
    pub fn follower(&self) -> Arc<RwLock<Option<Arc<Follower>>>> {
        self.follower.clone()
    }

    /// Set the follower instance
    pub async fn set_follower(&self, follower: Option<Arc<Follower>>) {
        *self.follower.write().await = follower;
    }

    /// Get a reference to the replication state
    #[must_use]
    pub const fn state(&self) -> &ReplicationState {
        &self.state
    }

    /// Get a reference to the backlog
    #[must_use]
    pub const fn backlog(&self) -> &ReplicationBacklog {
        &self.backlog
    }

    /// Append a command to the replication stream
    ///
    /// Serializes the command to RESP format and adds it to the backlog.
    /// Also broadcasts the command to all connected followers via the broadcast channel.
    /// Returns the new replication offset.
    pub fn append_command(&self, command: &[RespValue], db: usize) -> u64 {
        // Serialize command to RESP format
        let data = Self::serialize_command(command, db);

        // Append to backlog
        let offset = self.backlog.append(data.clone());

        // Update state offset
        self.state.set_offset(offset);

        // Broadcast to all followers via the channel
        // This is best-effort - if all receivers have lagged, send() may fail
        // but that's okay as followers will get the data from backlog on reconnect
        let _ = self.command_broadcast.send(data.clone());

        // Also broadcast via FollowerTracker (for the old implementation)
        // This is redundant now but kept for compatibility
        let tracker = self.follower_tracker.clone();
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::spawn(async move {
                tracker.broadcast_command(data).await;
            });
        }

        offset
    }

    /// Serialize a command to RESP format with SELECT if needed
    fn serialize_command(command: &[RespValue], db: usize) -> Bytes {
        let mut buf = BytesMut::new();

        // If not db 0, prepend SELECT command
        if db != 0 {
            // SELECT db_number
            let select_array = RespValue::Array(vec![
                RespValue::bulk_string("SELECT"),
                RespValue::bulk_string(db.to_string()),
            ]);
            Self::write_resp_value(&mut buf, &select_array);
        }

        // Write the actual command
        let command_array = RespValue::Array(command.to_vec());
        Self::write_resp_value(&mut buf, &command_array);

        buf.freeze()
    }

    /// Write a RESP value to a buffer
    fn write_resp_value(buf: &mut BytesMut, value: &RespValue) {
        match value {
            RespValue::SimpleString(s) => {
                buf.put_u8(b'+');
                buf.put(s.as_bytes());
                buf.put(&b"\r\n"[..]);
            }
            RespValue::Error(e) => {
                buf.put_u8(b'-');
                buf.put(e.as_bytes());
                buf.put(&b"\r\n"[..]);
            }
            RespValue::Integer(i) => {
                buf.put_u8(b':');
                buf.put(i.to_string().as_bytes());
                buf.put(&b"\r\n"[..]);
            }
            RespValue::BulkString(data) => {
                buf.put_u8(b'$');
                buf.put(data.len().to_string().as_bytes());
                buf.put(&b"\r\n"[..]);
                buf.put(data.as_ref());
                buf.put(&b"\r\n"[..]);
            }
            RespValue::Array(arr) => {
                buf.put_u8(b'*');
                buf.put(arr.len().to_string().as_bytes());
                buf.put(&b"\r\n"[..]);
                for item in arr {
                    Self::write_resp_value(buf, item);
                }
            }
            RespValue::Null => {
                buf.put(&b"$-1\r\n"[..]);
            }
            // RESP3 types - not typically used in replication stream
            RespValue::Double(f) => {
                buf.put_u8(b',');
                buf.put(f.to_string().as_bytes());
                buf.put(&b"\r\n"[..]);
            }
            RespValue::Boolean(b) => {
                buf.put_u8(b'#');
                buf.put_u8(if *b { b't' } else { b'f' });
                buf.put(&b"\r\n"[..]);
            }
            RespValue::BigNumber(s) => {
                buf.put_u8(b'(');
                buf.put(s.as_bytes());
                buf.put(&b"\r\n"[..]);
            }
            RespValue::VerbatimString { encoding, data } => {
                let total_len = encoding.len() + 1 + data.len(); // encoding + ':' + data
                buf.put_u8(b'=');
                buf.put(total_len.to_string().as_bytes());
                buf.put(&b"\r\n"[..]);
                buf.put(encoding.as_bytes());
                buf.put_u8(b':');
                buf.put(data.as_ref());
                buf.put(&b"\r\n"[..]);
            }
            RespValue::Map(map) => {
                buf.put_u8(b'%');
                buf.put(map.len().to_string().as_bytes());
                buf.put(&b"\r\n"[..]);
                for (k, v) in map {
                    Self::write_resp_value(buf, &RespValue::BulkString(Bytes::from(k.clone())));
                    Self::write_resp_value(buf, v);
                }
            }
            RespValue::Set(set) => {
                buf.put_u8(b'~');
                buf.put(set.len().to_string().as_bytes());
                buf.put(&b"\r\n"[..]);
                for item in set {
                    Self::write_resp_value(buf, item);
                }
            }
            RespValue::Push(arr) => {
                buf.put_u8(b'>');
                buf.put(arr.len().to_string().as_bytes());
                buf.put(&b"\r\n"[..]);
                for item in arr {
                    Self::write_resp_value(buf, item);
                }
            }
        }
    }

    /// Get replication information for INFO command
    #[must_use]
    pub fn info(&self) -> ReplicationInfo {
        let master_info = self.state.master_info();

        ReplicationInfo {
            role: self.state.role().to_string(),
            connected_slaves: self.state.connected_replicas(),
            master_replid: self.state.repl_id(),
            master_replid2: self.state.repl_id2(),
            master_repl_offset: self.state.master_repl_offset(),
            repl_backlog_active: !self.backlog.is_empty(),
            repl_backlog_size: self.backlog.size(),
            repl_backlog_first_byte_offset: self.backlog.first_offset(),
            master_host: master_info.as_ref().map(|m| m.host.clone()),
            master_port: master_info.as_ref().map(|m| m.port),
        }
    }

    /// Check if we can do partial resync from the given replication ID and offset
    #[must_use]
    pub fn can_partial_resync(&self, repl_id: &str, offset: u64) -> bool {
        let current_id = self.state.repl_id();
        let secondary_id = self.state.repl_id2();
        let secondary_offset = self.state.repl_id2_offset();

        // Check if repl_id matches current or secondary
        let id_matches = repl_id == current_id
            || (repl_id == secondary_id
                && offset >= secondary_offset
                && offset <= self.state.master_repl_offset());

        // Check if offset is in backlog
        let offset_valid = self.backlog.can_partial_resync(offset);

        id_matches && offset_valid
    }

    /// Get the current consistency mode
    pub async fn consistency_mode(&self) -> ConsistencyMode {
        *self.consistency_mode.read().await
    }

    /// Set the consistency mode
    pub async fn set_consistency_mode(&self, mode: ConsistencyMode) {
        *self.consistency_mode.write().await = mode;
    }

    /// Wait for replicas according to the current consistency mode
    ///
    /// This should be called after propagating write commands.
    /// Returns the number of replicas that acknowledged within the timeout.
    ///
    /// For async mode, returns immediately without waiting.
    /// For semi-sync/sync, blocks until enough replicas acknowledge or timeout.
    pub async fn wait_for_consistency(&self, current_offset: u64) -> usize {
        // Only wait if we're a master
        if !self.state.is_master() {
            return 0;
        }

        let mode = self.consistency_mode().await;

        // Get wait parameters based on mode
        let follower_count = self.follower_tracker.follower_count().await;
        let wait_params = mode.wait_params(follower_count);

        match wait_params {
            None => {
                // Async mode - no waiting
                0
            }
            Some((num_replicas, timeout)) => {
                // Wait for replicas to reach the offset
                self.follower_tracker
                    .wait_for_offset(num_replicas, current_offset, Some(timeout))
                    .await
            }
        }
    }
}

/// Replication information for INFO command
#[derive(Debug, Clone)]
pub struct ReplicationInfo {
    /// Current role (master/slave)
    pub role: String,
    /// Number of connected slaves (if master)
    pub connected_slaves: u64,
    /// Current replication ID
    pub master_replid: String,
    /// Secondary replication ID (for PSYNC2)
    pub master_replid2: String,
    /// Current replication offset
    pub master_repl_offset: u64,
    /// Whether backlog is active
    pub repl_backlog_active: bool,
    /// Current backlog size in bytes
    pub repl_backlog_size: u64,
    /// First byte offset in backlog
    pub repl_backlog_first_byte_offset: u64,
    /// Master host (if this is a replica)
    pub master_host: Option<String>,
    /// Master port (if this is a replica)
    pub master_port: Option<u16>,
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::state::ReplicationRole;

    #[test]
    fn test_new_manager_is_master() {
        let manager = ReplicationManager::new();
        assert_eq!(manager.state().role(), ReplicationRole::Master);
        assert!(manager.backlog().is_empty());
    }

    #[test]
    fn test_append_command_increments_offset() {
        let manager = ReplicationManager::new();

        let command = vec![
            RespValue::bulk_string("SET"),
            RespValue::bulk_string("key"),
            RespValue::bulk_string("value"),
        ];

        let offset = manager.append_command(&command, 0);
        assert!(offset > 0);
        assert_eq!(manager.state().master_repl_offset(), offset);
    }

    #[test]
    fn test_append_command_to_backlog() {
        let manager = ReplicationManager::new();

        let command = vec![
            RespValue::bulk_string("SET"),
            RespValue::bulk_string("mykey"),
            RespValue::bulk_string("myvalue"),
        ];

        manager.append_command(&command, 0);
        assert_eq!(manager.backlog().len(), 1);
    }

    #[test]
    fn test_append_command_with_non_zero_db() {
        let manager = ReplicationManager::new();

        let command = vec![
            RespValue::bulk_string("SET"),
            RespValue::bulk_string("key"),
            RespValue::bulk_string("value"),
        ];

        manager.append_command(&command, 5);

        // Should have two entries: SELECT 5 and SET key value
        // But they're combined into one backlog entry
        assert_eq!(manager.backlog().len(), 1);
    }

    #[test]
    fn test_info_returns_correct_data() {
        let manager = ReplicationManager::new();

        let command = vec![RespValue::bulk_string("PING")];
        manager.append_command(&command, 0);

        let info = manager.info();
        assert_eq!(info.role, "master");
        assert_eq!(info.connected_slaves, 0);
        assert_eq!(info.master_replid.len(), 40);
        assert!(info.master_repl_offset > 0);
        assert!(info.repl_backlog_active);
        assert!(info.master_host.is_none());
        assert!(info.master_port.is_none());
    }

    #[test]
    fn test_can_partial_resync_valid() {
        let manager = ReplicationManager::new();
        let repl_id = manager.state().repl_id();

        let command = vec![RespValue::bulk_string("PING")];
        manager.append_command(&command, 0);

        // Can resync from offset 0 with matching ID
        assert!(manager.can_partial_resync(&repl_id, 0));
    }

    #[test]
    fn test_can_partial_resync_wrong_id() {
        let manager = ReplicationManager::new();

        let command = vec![RespValue::bulk_string("PING")];
        manager.append_command(&command, 0);

        // Wrong replication ID
        assert!(!manager.can_partial_resync("wrong_id_0000000000000000000000000000", 0));
    }

    #[test]
    fn test_can_partial_resync_offset_too_old() {
        let manager = ReplicationManager::with_config(BacklogConfig { capacity: 20 });

        // Fill backlog beyond capacity to evict old entries
        for _ in 0..20 {
            let command = vec![RespValue::bulk_string("PING")];
            manager.append_command(&command, 0);
        }

        let repl_id = manager.state().repl_id();

        // Offset 0 should be evicted
        assert!(!manager.can_partial_resync(&repl_id, 0));
    }

    #[test]
    fn test_serialize_command_format() {
        let command = vec![
            RespValue::bulk_string("SET"),
            RespValue::bulk_string("key"),
            RespValue::bulk_string("val"),
        ];

        let data = ReplicationManager::serialize_command(&command, 0);
        let s = String::from_utf8_lossy(&data);

        // Should be: *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\nval\r\n
        assert!(s.starts_with("*3\r\n"));
        assert!(s.contains("$3\r\nSET\r\n"));
        assert!(s.contains("$3\r\nkey\r\n"));
        assert!(s.contains("$3\r\nval\r\n"));
    }

    #[test]
    fn test_manager_thread_safe() {
        use std::sync::Arc;
        use std::thread;

        let manager = Arc::new(ReplicationManager::new());
        let mut handles = vec![];

        for i in 0..10 {
            let manager_clone = Arc::clone(&manager);
            handles.push(thread::spawn(move || {
                for j in 0..10 {
                    let command = vec![
                        RespValue::bulk_string("SET"),
                        RespValue::bulk_string(format!("key_{i}_{j}")),
                        RespValue::bulk_string(format!("value_{i}_{j}")),
                    ];
                    manager_clone.append_command(&command, 0);
                }
            }));
        }

        for handle in handles {
            handle.join().expect("thread panicked");
        }

        // Should have 100 entries
        assert_eq!(manager.backlog().len(), 100);
    }
}

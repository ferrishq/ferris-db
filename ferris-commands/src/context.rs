//! Command execution context

use crate::transaction::TransactionState;
use ferris_core::{BlockingRegistry, KeyStore, PubSubMessage, PubSubRegistry, SubscriberId};
use ferris_persistence::AofWriter;
use ferris_protocol::RespValue;
use ferris_replication::ReplicationManager;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::warn;

/// Global counter for assigning unique client IDs
static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

/// Context for command execution, holds connection state
pub struct CommandContext {
    /// Reference to the key store
    store: Arc<KeyStore>,
    /// Reference to the blocking registry (for BLPOP, BRPOP, etc.)
    blocking_registry: Arc<BlockingRegistry>,
    /// Reference to the pub/sub registry
    pubsub_registry: Arc<PubSubRegistry>,
    /// Subscriber ID for pub/sub
    subscriber_id: SubscriberId,
    /// Receiver for pub/sub messages (kept alive to prevent channel closure)
    #[allow(dead_code)]
    pubsub_receiver: mpsc::UnboundedReceiver<PubSubMessage>,
    /// Optional AOF writer for persistence
    aof_writer: Option<Arc<AofWriter>>,
    /// Optional replication manager
    replication_manager: Option<Arc<ReplicationManager>>,
    /// Unique client ID assigned when the context is created
    client_id: u64,
    /// Currently selected database index
    selected_db: usize,
    /// Whether this connection is authenticated
    authenticated: bool,
    /// Client name (from CLIENT SETNAME)
    client_name: Option<String>,
    /// RESP protocol version (2 or 3)
    protocol_version: u32,
    /// Transaction state (MULTI/EXEC/WATCH)
    transaction_state: TransactionState,
    /// Whether we're applying replicated commands (bypass read-only check)
    applying_replication: bool,
    /// Current command name being executed (for propagation)
    current_command: Option<String>,
}

impl CommandContext {
    /// Create a new command context with a unique client ID
    #[must_use]
    pub fn new(store: Arc<KeyStore>) -> Self {
        let pubsub_registry = Arc::new(PubSubRegistry::new());
        let (subscriber_id, pubsub_receiver) = pubsub_registry.register_subscriber();
        Self {
            store,
            blocking_registry: Arc::new(BlockingRegistry::new()),
            pubsub_registry,
            subscriber_id,
            pubsub_receiver,
            aof_writer: None,
            replication_manager: None,
            client_id: NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed),
            selected_db: 0,
            authenticated: false,
            client_name: None,
            protocol_version: 2,
            transaction_state: TransactionState::new(),
            applying_replication: false,
            current_command: None,
        }
    }

    /// Create a context with a shared pub/sub registry
    #[must_use]
    pub fn with_pubsub(store: Arc<KeyStore>, pubsub_registry: Arc<PubSubRegistry>) -> Self {
        let (subscriber_id, pubsub_receiver) = pubsub_registry.register_subscriber();
        Self {
            store,
            blocking_registry: Arc::new(BlockingRegistry::new()),
            pubsub_registry,
            subscriber_id,
            pubsub_receiver,
            aof_writer: None,
            replication_manager: None,
            client_id: NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed),
            selected_db: 0,
            authenticated: false,
            client_name: None,
            protocol_version: 2,
            transaction_state: TransactionState::new(),
            applying_replication: false,
            current_command: None,
        }
    }

    /// Create a new command context with a shared blocking registry
    #[must_use]
    pub fn with_blocking_registry(
        store: Arc<KeyStore>,
        blocking_registry: Arc<BlockingRegistry>,
    ) -> Self {
        let pubsub_registry = Arc::new(PubSubRegistry::new());
        let (subscriber_id, pubsub_receiver) = pubsub_registry.register_subscriber();
        Self {
            store,
            blocking_registry,
            pubsub_registry,
            subscriber_id,
            pubsub_receiver,
            aof_writer: None,
            replication_manager: None,
            client_id: NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed),
            selected_db: 0,
            authenticated: false,
            client_name: None,
            protocol_version: 2,
            transaction_state: TransactionState::new(),
            applying_replication: false,
            current_command: None,
        }
    }

    /// Create a new command context with all shared resources
    #[must_use]
    pub fn with_resources(
        store: Arc<KeyStore>,
        blocking_registry: Arc<BlockingRegistry>,
        pubsub_registry: Arc<PubSubRegistry>,
        aof_writer: Option<Arc<AofWriter>>,
        replication_manager: Option<Arc<ReplicationManager>>,
    ) -> Self {
        let (subscriber_id, pubsub_receiver) = pubsub_registry.register_subscriber();
        Self {
            store,
            blocking_registry,
            pubsub_registry,
            subscriber_id,
            pubsub_receiver,
            aof_writer,
            replication_manager,
            client_id: NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed),
            selected_db: 0,
            authenticated: false,
            client_name: None,
            protocol_version: 2,
            transaction_state: TransactionState::new(),
            applying_replication: false,
            current_command: None,
        }
    }

    /// Get the unique client ID for this connection
    #[must_use]
    pub const fn client_id(&self) -> u64 {
        self.client_id
    }

    /// Get a reference to the key store
    #[must_use]
    pub fn store(&self) -> &KeyStore {
        &self.store
    }

    /// Get an Arc clone of the key store
    #[must_use]
    pub fn store_arc(&self) -> &Arc<KeyStore> {
        &self.store
    }

    /// Get a reference to the blocking registry
    #[must_use]
    pub fn blocking_registry(&self) -> &Arc<BlockingRegistry> {
        &self.blocking_registry
    }

    /// Get a reference to the pub/sub registry
    #[must_use]
    pub fn pubsub_registry_ref(&self) -> &Arc<PubSubRegistry> {
        &self.pubsub_registry
    }

    /// Get a reference to the AOF writer
    #[must_use]
    pub fn aof_writer(&self) -> Option<&Arc<AofWriter>> {
        self.aof_writer.as_ref()
    }

    /// Get the currently selected database index
    #[must_use]
    pub const fn selected_db(&self) -> usize {
        self.selected_db
    }

    /// Select a different database
    pub fn select_db(&mut self, db_index: usize) {
        self.selected_db = db_index;
    }

    /// Check if the connection is authenticated
    #[must_use]
    pub const fn is_authenticated(&self) -> bool {
        self.authenticated
    }

    /// Set authentication status
    pub fn set_authenticated(&mut self, authenticated: bool) {
        self.authenticated = authenticated;
    }

    /// Get the client name
    #[must_use]
    pub fn client_name(&self) -> Option<&str> {
        self.client_name.as_deref()
    }

    /// Set the client name
    pub fn set_client_name(&mut self, name: Option<String>) {
        self.client_name = name;
    }

    /// Get the RESP protocol version (2 or 3)
    #[must_use]
    pub const fn protocol_version(&self) -> u32 {
        self.protocol_version
    }

    /// Set the RESP protocol version
    pub fn set_protocol_version(&mut self, version: u32) {
        self.protocol_version = version;
    }

    /// Reset connection state to defaults (for RESET command)
    pub fn reset(&mut self) {
        self.selected_db = 0;
        self.authenticated = false;
        self.client_name = None;
        self.protocol_version = 2;
        self.transaction_state = TransactionState::new();
    }

    /// Get a reference to the transaction state
    #[must_use]
    pub fn transaction_state(&self) -> &TransactionState {
        &self.transaction_state
    }

    /// Get a mutable reference to the transaction state
    pub fn transaction_state_mut(&mut self) -> &mut TransactionState {
        &mut self.transaction_state
    }

    /// Get a reference to the pub/sub registry
    #[must_use]
    pub fn pubsub_registry(&self) -> &Arc<PubSubRegistry> {
        &self.pubsub_registry
    }

    /// Get the subscriber ID for this connection
    #[must_use]
    pub const fn subscriber_id(&self) -> SubscriberId {
        self.subscriber_id
    }

    /// Propagate a write command to AOF (non-blocking)
    ///
    /// This should be called after successfully executing a write command.
    /// The command will be queued for writing to the AOF file.
    ///
    /// # Arguments
    ///
    /// * `command` - The command and its arguments to write to AOF
    pub fn propagate_to_aof(&self, command: Vec<RespValue>) {
        if let Some(ref aof_writer) = self.aof_writer {
            let aof_writer = Arc::clone(aof_writer);
            let db = self.selected_db;

            // Spawn a task to send to AOF (non-blocking)
            // Note: This is fire-and-forget. If the channel is full,
            // the command will be dropped (which is acceptable for AOF).
            tokio::spawn(async move {
                use ferris_persistence::aof::AofEntry;
                if let Err(e) = aof_writer.append(AofEntry { command, db }).await {
                    warn!(error = %e, "Failed to append command to AOF");
                }
            });
        }
    }

    /// Get a reference to the replication manager
    #[must_use]
    pub fn replication_manager(&self) -> Option<&Arc<ReplicationManager>> {
        self.replication_manager.as_ref()
    }

    /// Check if this server is currently a replica (follower)
    ///
    /// Returns true if the server is in replica mode and should reject writes.
    #[must_use]
    pub fn is_replica(&self) -> bool {
        if let Some(manager) = &self.replication_manager {
            manager.state().is_replica()
        } else {
            false
        }
    }

    /// Set whether we're applying replicated commands
    ///
    /// When true, write commands will be allowed even if this is a replica.
    pub fn set_applying_replication(&mut self, applying: bool) {
        self.applying_replication = applying;
    }

    /// Check if we're applying replicated commands
    #[must_use]
    pub const fn is_applying_replication(&self) -> bool {
        self.applying_replication
    }

    /// Set the current command name (called by executor before executing command)
    pub fn set_current_command(&mut self, command: String) {
        self.current_command = Some(command);
    }

    /// Get the current command name
    #[must_use]
    pub fn current_command(&self) -> Option<&str> {
        self.current_command.as_deref()
    }

    /// Propagate a write command to replication backlog (non-blocking)
    ///
    /// This should be called after successfully executing a write command.
    /// The command will be added to the replication backlog and the offset updated.
    ///
    /// # Arguments
    ///
    /// * `command` - The command and its arguments to add to replication stream
    pub fn propagate_to_replication(&self, command: &[RespValue]) {
        if let Some(ref manager) = self.replication_manager {
            manager.append_command(command, self.selected_db);
        }
    }

    /// Propagate a write command to both AOF and replication (convenience method)
    ///
    /// This should be called after successfully executing a write command.
    /// The command will be sent to both AOF and replication backlog.
    ///
    /// # Arguments
    ///
    /// * `command` - The command and its arguments to propagate (full command including name)
    pub fn propagate(&self, command: &[RespValue]) {
        self.propagate_to_aof(command.to_vec());
        self.propagate_to_replication(command);
    }

    /// Propagate current command's args to both AOF and replication
    ///
    /// Uses the current command name set by the executor.
    /// This should be called after successfully executing a write command.
    ///
    /// If replication consistency mode is semi-sync or sync, this will block
    /// until enough replicas acknowledge the write (or timeout).
    ///
    /// # Arguments
    ///
    /// * `args` - The command arguments (without command name)
    ///
    /// # Panics
    ///
    /// Panics if current_command is not set (should always be set by executor)
    pub fn propagate_args(&self, args: &[RespValue]) {
        use bytes::Bytes;

        // If we're applying replicated commands from a leader, don't propagate again
        // This prevents infinite loops and unnecessary work
        if self.applying_replication {
            return;
        }

        // If current_command is not set (e.g., in unit tests that call commands directly),
        // we can't propagate. This is OK for unit tests as they don't have AOF/replication.
        let Some(cmd_name) = self.current_command.as_ref() else {
            return;
        };

        let mut full_command = Vec::with_capacity(args.len() + 1);
        full_command.push(RespValue::BulkString(Bytes::from(cmd_name.clone())));
        full_command.extend_from_slice(args);

        self.propagate(&full_command);

        // TODO(consistency): Add blocking wait for consistency modes here
        // Currently, only async mode (fire-and-forget) is supported from sync context.
        // To implement semi-sync/sync modes, we need to either:
        // 1. Refactor command execution to be fully async, or
        // 2. Use a thread pool for blocking waits
        //
        // For now, all writes use async replication (commands return immediately,
        // replication happens in background). The infrastructure for consistency
        // modes is in place in ReplicationManager.
    }

    /// Propagate a write command with name and args to both AOF and replication
    ///
    /// This is a convenience method for commands that have the name and args separately.
    ///
    /// # Arguments
    ///
    /// * `name` - The command name (e.g., "SET")
    /// * `args` - The command arguments
    pub fn propagate_with_name(&self, name: &str, args: &[RespValue]) {
        use bytes::Bytes;

        let mut full_command = Vec::with_capacity(args.len() + 1);
        full_command.push(RespValue::BulkString(Bytes::from(name.to_uppercase())));
        full_command.extend_from_slice(args);

        self.propagate(&full_command);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_context_creation() {
        let store = Arc::new(KeyStore::default());
        let ctx = CommandContext::new(store);

        assert_eq!(ctx.selected_db(), 0);
        assert!(!ctx.is_authenticated());
        assert!(ctx.client_name().is_none());
    }

    #[test]
    fn test_context_select_db() {
        let store = Arc::new(KeyStore::default());
        let mut ctx = CommandContext::new(store);

        ctx.select_db(5);
        assert_eq!(ctx.selected_db(), 5);
    }

    #[test]
    fn test_context_authentication() {
        let store = Arc::new(KeyStore::default());
        let mut ctx = CommandContext::new(store);

        assert!(!ctx.is_authenticated());
        ctx.set_authenticated(true);
        assert!(ctx.is_authenticated());
    }

    #[test]
    fn test_context_client_name() {
        let store = Arc::new(KeyStore::default());
        let mut ctx = CommandContext::new(store);

        ctx.set_client_name(Some("my-client".to_string()));
        assert_eq!(ctx.client_name(), Some("my-client"));
    }
}

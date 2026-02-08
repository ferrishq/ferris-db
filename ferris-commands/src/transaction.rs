//! Transaction commands: MULTI, EXEC, DISCARD, WATCH, UNWATCH
//!
//! Redis transactions allow batching commands for atomic execution.
//! WATCH provides optimistic locking.

use crate::{CommandContext, CommandError, CommandResult};
use bytes::Bytes;
use ferris_protocol::RespValue;
use std::collections::HashSet;

/// Transaction state for a client connection
#[derive(Debug, Clone, Default)]
pub struct TransactionState {
    /// Whether the client is in MULTI mode
    in_transaction: bool,
    /// Queued commands to execute on EXEC
    queued_commands: Vec<(String, Vec<RespValue>)>,
    /// Keys being watched for modifications (WATCH command)
    watched_keys: HashSet<(usize, Bytes)>, // (db_index, key)
    /// Whether any watched key was modified (transaction should abort)
    transaction_aborted: bool,
}

impl TransactionState {
    /// Create a new transaction state
    pub fn new() -> Self {
        Self::default()
    }

    /// Check if the client is in a transaction
    pub fn in_transaction(&self) -> bool {
        self.in_transaction
    }

    /// Start a transaction (MULTI)
    pub fn begin(&mut self) {
        self.in_transaction = true;
        self.queued_commands.clear();
    }

    /// Queue a command for execution
    pub fn queue_command(&mut self, name: String, args: Vec<RespValue>) {
        self.queued_commands.push((name, args));
    }

    /// Get all queued commands
    pub fn queued_commands(&self) -> &[(String, Vec<RespValue>)] {
        &self.queued_commands
    }

    /// Discard the transaction
    pub fn discard(&mut self) {
        self.in_transaction = false;
        self.queued_commands.clear();
        // Keep watched keys - they persist until EXEC/DISCARD/UNWATCH
    }

    /// Complete the transaction
    pub fn finish(&mut self) {
        self.in_transaction = false;
        self.queued_commands.clear();
        self.watched_keys.clear();
        self.transaction_aborted = false;
    }

    /// Add a key to the watch list
    pub fn watch_key(&mut self, db_index: usize, key: Bytes) {
        self.watched_keys.insert((db_index, key));
    }

    /// Clear all watched keys
    pub fn unwatch(&mut self) {
        self.watched_keys.clear();
        self.transaction_aborted = false;
    }

    /// Check if a key is being watched
    pub fn is_watching(&self, db_index: usize, key: &[u8]) -> bool {
        self.watched_keys
            .contains(&(db_index, Bytes::copy_from_slice(key)))
    }

    /// Mark transaction as aborted (watched key was modified)
    pub fn abort(&mut self) {
        self.transaction_aborted = true;
    }

    /// Check if transaction should abort
    pub fn is_aborted(&self) -> bool {
        self.transaction_aborted
    }

    /// Get watched keys for a specific database
    pub fn watched_keys_in_db(&self, db_index: usize) -> Vec<Bytes> {
        self.watched_keys
            .iter()
            .filter(|(db, _)| *db == db_index)
            .map(|(_, key)| key.clone())
            .collect()
    }
}

/// MULTI - Mark the start of a transaction block
///
/// Subsequent commands will be queued until EXEC is called.
///
/// Time complexity: O(1)
pub fn multi(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if !args.is_empty() {
        return Err(CommandError::WrongArity("MULTI".to_string()));
    }

    if ctx.transaction_state().in_transaction() {
        return Err(CommandError::InvalidArgument(
            "MULTI calls can not be nested".to_string(),
        ));
    }

    ctx.transaction_state_mut().begin();
    Ok(RespValue::ok())
}

/// EXEC - Execute all commands issued after MULTI
///
/// Executes all queued commands atomically. If any WATCH'ed key
/// was modified, returns nil and aborts the transaction.
///
/// Time complexity: Depends on queued commands
pub fn exec(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if !args.is_empty() {
        return Err(CommandError::WrongArity("EXEC".to_string()));
    }

    if !ctx.transaction_state().in_transaction() {
        return Err(CommandError::InvalidArgument(
            "EXEC without MULTI".to_string(),
        ));
    }

    // Check if transaction should abort due to watched key modification
    if ctx.transaction_state().is_aborted() {
        ctx.transaction_state_mut().finish();
        return Ok(RespValue::Null);
    }

    // Get queued commands before finishing transaction
    let commands = ctx.transaction_state().queued_commands().to_vec();
    ctx.transaction_state_mut().finish();

    // Execute all queued commands
    let mut results = Vec::with_capacity(commands.len());

    for (cmd_name, cmd_args) in commands {
        // Execute each command using the executor
        use crate::executor::CommandExecutor;
        use ferris_protocol::Command;

        let executor = CommandExecutor::new();

        // Convert RespValue args to Bytes
        let mut bytes_args = Vec::new();
        for arg in &cmd_args {
            let bytes = arg
                .as_bytes()
                .cloned()
                .or_else(|| arg.as_str().map(|s| Bytes::from(s.to_owned())))
                .unwrap_or_else(|| Bytes::new());
            bytes_args.push(bytes);
        }

        let command = Command {
            name: cmd_name,
            args: bytes_args,
        };

        match executor.execute(ctx, &command) {
            Ok(result) => results.push(result),
            Err(e) => {
                // In transactions, errors are returned as error strings
                results.push(RespValue::Error(e.to_string()));
            }
        }
    }

    Ok(RespValue::Array(results))
}

/// DISCARD - Discard all commands issued after MULTI
///
/// Flushes all queued commands and exits transaction mode.
///
/// Time complexity: O(N) where N is the number of queued commands
pub fn discard(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if !args.is_empty() {
        return Err(CommandError::WrongArity("DISCARD".to_string()));
    }

    if !ctx.transaction_state().in_transaction() {
        return Err(CommandError::InvalidArgument(
            "DISCARD without MULTI".to_string(),
        ));
    }

    ctx.transaction_state_mut().discard();
    Ok(RespValue::ok())
}

/// WATCH key [key ...]
///
/// Marks the given keys to be watched for conditional execution of a transaction.
/// If any watched key is modified before EXEC, the transaction will abort.
///
/// Time complexity: O(1) for every key
pub fn watch(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("WATCH".to_string()));
    }

    if ctx.transaction_state().in_transaction() {
        return Err(CommandError::InvalidArgument(
            "WATCH inside MULTI is not allowed".to_string(),
        ));
    }

    let db_index = ctx.selected_db();

    for arg in args {
        let key = arg
            .as_bytes()
            .cloned()
            .or_else(|| arg.as_str().map(|s| Bytes::from(s.to_owned())))
            .ok_or_else(|| CommandError::InvalidArgument("invalid key".to_string()))?;

        ctx.transaction_state_mut().watch_key(db_index, key);
    }

    Ok(RespValue::ok())
}

/// UNWATCH
///
/// Flushes all the previously watched keys for a transaction.
///
/// Time complexity: O(1)
pub fn unwatch(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if !args.is_empty() {
        return Err(CommandError::WrongArity("UNWATCH".to_string()));
    }

    ctx.transaction_state_mut().unwatch();
    Ok(RespValue::ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferris_core::KeyStore;
    use std::sync::Arc;

    #[test]
    fn test_transaction_state_new() {
        let state = TransactionState::new();
        assert!(!state.in_transaction());
        assert!(state.queued_commands().is_empty());
        assert!(!state.is_aborted());
    }

    #[test]
    fn test_transaction_begin_and_queue() {
        let mut state = TransactionState::new();
        state.begin();
        assert!(state.in_transaction());

        state.queue_command(
            "SET".to_string(),
            vec![
                RespValue::bulk_string("key"),
                RespValue::bulk_string("value"),
            ],
        );
        assert_eq!(state.queued_commands().len(), 1);
    }

    #[test]
    fn test_transaction_discard() {
        let mut state = TransactionState::new();
        state.begin();
        state.queue_command("SET".to_string(), vec![]);
        state.discard();

        assert!(!state.in_transaction());
        assert!(state.queued_commands().is_empty());
    }

    #[test]
    fn test_transaction_finish() {
        let mut state = TransactionState::new();
        state.begin();
        state.queue_command("SET".to_string(), vec![]);
        state.watch_key(0, Bytes::from("key"));
        state.finish();

        assert!(!state.in_transaction());
        assert!(state.queued_commands().is_empty());
        assert!(!state.is_watching(0, b"key"));
    }

    #[test]
    fn test_watch_keys() {
        let mut state = TransactionState::new();
        state.watch_key(0, Bytes::from("key1"));
        state.watch_key(0, Bytes::from("key2"));
        state.watch_key(1, Bytes::from("key3"));

        assert!(state.is_watching(0, b"key1"));
        assert!(state.is_watching(0, b"key2"));
        assert!(state.is_watching(1, b"key3"));
        assert!(!state.is_watching(0, b"key3"));
    }

    #[test]
    fn test_unwatch() {
        let mut state = TransactionState::new();
        state.watch_key(0, Bytes::from("key"));
        state.unwatch();

        assert!(!state.is_watching(0, b"key"));
    }

    #[test]
    fn test_transaction_abort() {
        let mut state = TransactionState::new();
        assert!(!state.is_aborted());

        state.abort();
        assert!(state.is_aborted());
    }

    #[test]
    fn test_multi_command() {
        let store = Arc::new(KeyStore::default());
        let mut ctx = CommandContext::new(store);

        let result = multi(&mut ctx, &[]);
        assert!(result.is_ok());
        assert!(ctx.transaction_state().in_transaction());
    }

    #[test]
    fn test_multi_nested_error() {
        let store = Arc::new(KeyStore::default());
        let mut ctx = CommandContext::new(store);

        multi(&mut ctx, &[]).unwrap();
        let result = multi(&mut ctx, &[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_multi_wrong_arity() {
        let store = Arc::new(KeyStore::default());
        let mut ctx = CommandContext::new(store);

        let result = multi(&mut ctx, &[RespValue::bulk_string("extra")]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_discard_command() {
        let store = Arc::new(KeyStore::default());
        let mut ctx = CommandContext::new(store);

        multi(&mut ctx, &[]).unwrap();
        let result = discard(&mut ctx, &[]);
        assert!(result.is_ok());
        assert!(!ctx.transaction_state().in_transaction());
    }

    #[test]
    fn test_discard_without_multi() {
        let store = Arc::new(KeyStore::default());
        let mut ctx = CommandContext::new(store);

        let result = discard(&mut ctx, &[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_discard_wrong_arity() {
        let store = Arc::new(KeyStore::default());
        let mut ctx = CommandContext::new(store);

        let result = discard(&mut ctx, &[RespValue::bulk_string("extra")]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_watch_command() {
        let store = Arc::new(KeyStore::default());
        let mut ctx = CommandContext::new(store);

        let result = watch(
            &mut ctx,
            &[
                RespValue::bulk_string("key1"),
                RespValue::bulk_string("key2"),
            ],
        );
        assert!(result.is_ok());
        assert!(ctx.transaction_state().is_watching(0, b"key1"));
        assert!(ctx.transaction_state().is_watching(0, b"key2"));
    }

    #[test]
    fn test_watch_inside_multi_error() {
        let store = Arc::new(KeyStore::default());
        let mut ctx = CommandContext::new(store);

        multi(&mut ctx, &[]).unwrap();
        let result = watch(&mut ctx, &[RespValue::bulk_string("key")]);
        assert!(result.is_err());
    }

    #[test]
    fn test_watch_wrong_arity() {
        let store = Arc::new(KeyStore::default());
        let mut ctx = CommandContext::new(store);

        let result = watch(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_unwatch_command() {
        let store = Arc::new(KeyStore::default());
        let mut ctx = CommandContext::new(store);

        watch(&mut ctx, &[RespValue::bulk_string("key")]).unwrap();
        let result = unwatch(&mut ctx, &[]);
        assert!(result.is_ok());
        assert!(!ctx.transaction_state().is_watching(0, b"key"));
    }

    #[test]
    fn test_unwatch_wrong_arity() {
        let store = Arc::new(KeyStore::default());
        let mut ctx = CommandContext::new(store);

        let result = unwatch(&mut ctx, &[RespValue::bulk_string("extra")]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_exec_without_multi() {
        let store = Arc::new(KeyStore::default());
        let mut ctx = CommandContext::new(store);

        let result = exec(&mut ctx, &[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_exec_wrong_arity() {
        let store = Arc::new(KeyStore::default());
        let mut ctx = CommandContext::new(store);

        let result = exec(&mut ctx, &[RespValue::bulk_string("extra")]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }
}

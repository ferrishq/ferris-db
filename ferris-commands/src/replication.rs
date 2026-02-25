//! Replication commands: REPLICAOF, ROLE, PSYNC, REPLCONF, WAIT
//!
//! Redis replication allows follower instances to replicate data from a leader.
//! This module implements the basic replication commands.

use crate::{CommandContext, CommandError, CommandResult};
use ferris_protocol::RespValue;

/// Helper to parse an integer from RespValue
fn parse_int(arg: &RespValue) -> Result<i64, CommandError> {
    let s = arg.as_str().ok_or(CommandError::NotAnInteger)?;
    s.parse().map_err(|_| CommandError::NotAnInteger)
}

/// REPLICAOF host port
///
/// Make this instance a follower of another Redis instance at host:port.
/// Use REPLICAOF NO ONE to stop replication and promote to leader.
///
/// Time complexity: O(1)
pub fn replicaof(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    use ferris_replication::{Follower, FollowerConfig};
    use std::sync::Arc;

    if args.len() != 2 {
        return Err(CommandError::WrongArity("REPLICAOF".to_string()));
    }

    let host = args[0]
        .as_str()
        .or_else(|| args[0].as_bytes().and_then(|b| std::str::from_utf8(b).ok()))
        .ok_or_else(|| CommandError::InvalidArgument("invalid host".to_string()))?;

    let port_str = args[1]
        .as_str()
        .or_else(|| args[1].as_bytes().and_then(|b| std::str::from_utf8(b).ok()))
        .ok_or_else(|| CommandError::InvalidArgument("invalid port".to_string()))?;

    // Check for REPLICAOF NO ONE (promote to leader)
    if host.eq_ignore_ascii_case("no") && port_str.eq_ignore_ascii_case("one") {
        // Promote to leader - disconnect from master, stop replication
        if let Some(manager) = ctx.replication_manager() {
            let manager_clone = manager.clone();

            // Spawn async task to stop follower
            tokio::spawn(async move {
                let follower_arc = manager_clone.follower();
                let follower_guard = follower_arc.read().await;
                if let Some(follower) = follower_guard.as_ref() {
                    follower.stop().await;
                }
                drop(follower_guard);

                // Clear follower
                manager_clone.set_follower(None).await;

                // Promote to master
                manager_clone.state().promote_to_master();

                tracing::info!("Promoted to master (REPLICAOF NO ONE)");
            });

            return Ok(RespValue::SimpleString("OK".to_string()));
        }

        return Err(CommandError::Internal(
            "replication not available".to_string(),
        ));
    }

    // Parse port
    let port: u16 = port_str
        .parse()
        .map_err(|_| CommandError::InvalidArgument("port must be a number".to_string()))?;

    // Start replication with the specified master
    if let Some(manager) = ctx.replication_manager() {
        let manager_clone = manager.clone();
        let host = host.to_string();
        let store_clone = Arc::clone(ctx.store_arc());
        let blocking_registry = Arc::clone(ctx.blocking_registry());
        let pubsub_registry = Arc::clone(ctx.pubsub_registry_ref());
        let aof_writer = ctx.aof_writer().cloned();
        let replication_manager = ctx.replication_manager().cloned();

        // Spawn async task to start follower
        tokio::spawn(async move {
            // Stop any existing follower first
            {
                let follower_arc = manager_clone.follower();
                let follower_guard = follower_arc.read().await;
                if let Some(follower) = follower_guard.as_ref() {
                    follower.stop().await;
                }
            }

            // Create new follower
            let (follower, mut rx) = Follower::new();
            let follower = Arc::new(follower);

            // Store follower in manager
            manager_clone.set_follower(Some(follower.clone())).await;

            // Set as replica in state
            manager_clone.state().set_master(host.clone(), port);

            tracing::info!("Starting replication from {}:{}", host, port);

            // Configure and start follower
            let config = FollowerConfig::new(host.clone(), port, 6380); // TODO: Get actual listening port
            follower.start(config).await;

            // Spawn task to receive and process replication commands
            // Apply commands to the store using the executor
            tokio::spawn(async move {
                use crate::{CommandContext, CommandExecutor};
                use bytes::Bytes;
                use ferris_protocol::Command;

                let executor = CommandExecutor::new();
                let mut ctx = CommandContext::with_resources(
                    store_clone,
                    blocking_registry,
                    pubsub_registry,
                    aof_writer,
                    replication_manager,
                );

                while let Some(repl_cmd) = rx.recv().await {
                    match repl_cmd {
                        ferris_replication::ReplicationCommand::Command(parts) => {
                            if parts.is_empty() {
                                tracing::warn!("Received empty command from leader");
                                continue;
                            }

                            // Convert to Command struct
                            let name = match std::str::from_utf8(&parts[0]) {
                                Ok(s) => s.to_uppercase(),
                                Err(e) => {
                                    tracing::warn!(error = %e, "Failed to parse command name");
                                    continue;
                                }
                            };

                            let args: Vec<Bytes> = parts.into_iter().skip(1).collect();

                            let cmd = Command { name, args };

                            tracing::debug!("Applying replicated command: {:?}", cmd.name);

                            // Execute the command
                            match executor.execute(&mut ctx, &cmd) {
                                Ok(resp) => {
                                    tracing::trace!("Command executed successfully: {:?}", resp);
                                }
                                Err(e) => {
                                    tracing::error!(
                                        error = %e,
                                        command = %cmd.name,
                                        "Failed to execute replicated command"
                                    );
                                }
                            }
                        }
                        ferris_replication::ReplicationCommand::FullSyncStart {
                            repl_id,
                            offset,
                        } => {
                            tracing::info!(
                                repl_id = %repl_id,
                                offset = offset,
                                "Full sync started"
                            );
                        }
                        ferris_replication::ReplicationCommand::PartialSyncStart { repl_id } => {
                            tracing::info!(repl_id = %repl_id, "Partial sync started");
                        }
                    }
                }

                tracing::info!("Replication command receiver stopped");
            });
        });

        return Ok(RespValue::SimpleString("OK".to_string()));
    }

    Err(CommandError::Internal(
        "replication not available".to_string(),
    ))
}

/// SLAVEOF host port
///
/// Legacy alias for REPLICAOF command.
pub fn slaveof(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    replicaof(ctx, args)
}

/// ROLE
///
/// Returns the replication role of the instance (master, slave, or sentinel).
///
/// Time complexity: O(1)
pub fn role(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    use bytes::Bytes;

    if !args.is_empty() {
        return Err(CommandError::WrongArity("ROLE".to_string()));
    }

    // Check if we have a replication manager
    if let Some(manager) = ctx.replication_manager() {
        let info = manager.info();

        if info.role == "master" {
            // Master format: ["master", master_repl_offset, [connected_slaves]]
            Ok(RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("master")),
                RespValue::Integer(info.master_repl_offset as i64),
                RespValue::Array(vec![]), // TODO: List connected replicas
            ]))
        } else {
            // Replica format: ["slave", master_host, master_port, state, repl_offset]
            let host = info.master_host.unwrap_or_default();
            let port = info.master_port.unwrap_or(0);

            Ok(RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("slave")),
                RespValue::BulkString(Bytes::from(host)),
                RespValue::Integer(port as i64),
                RespValue::BulkString(Bytes::from("connected")), // connection state
                RespValue::Integer(info.master_repl_offset as i64),
            ]))
        }
    } else {
        // Fallback if no replication manager - return default master role
        Ok(RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("master")),
            RespValue::Integer(0),
            RespValue::Array(vec![]),
        ]))
    }
}

/// WAIT numreplicas timeout
///
/// Blocks until the specified number of replicas have acknowledged writes,
/// or until timeout milliseconds have elapsed.
///
/// Time complexity: O(N) where N is the number of replicas
pub fn wait(_ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongArity("WAIT".to_string()));
    }

    let _numreplicas = parse_int(&args[0])?;
    let _timeout = parse_int(&args[1])?;

    // TODO: Actual WAIT logic:
    // 1. Track write commands with offsets
    // 2. Wait for N replicas to ack offset
    // 3. Return number of replicas that acked (may be < N if timeout)

    // For now, return 0 (no replicas)
    Ok(RespValue::Integer(0))
}

/// WAITAOF local_offset replica_offset timeout
///
/// Similar to WAIT but specifically for AOF synchronization.
/// Returns array of [local_ack, replica_ack_count].
///
/// Time complexity: O(N) where N is the number of replicas
pub fn waitaof(_ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() != 3 {
        return Err(CommandError::WrongArity("WAITAOF".to_string()));
    }

    let _local = parse_int(&args[0])?;
    let _replica = parse_int(&args[1])?;
    let _timeout = parse_int(&args[2])?;

    // TODO: Implement WAITAOF logic
    // For now, return [0, 0] (nothing acked)
    Ok(RespValue::Array(vec![
        RespValue::Integer(0), // local ack
        RespValue::Integer(0), // replica ack count
    ]))
}

/// PSYNC replicationid offset
///
/// Used by replicas to initiate replication with a leader.
/// Returns either FULLRESYNC or CONTINUE based on replication state.
///
/// Time complexity: O(1) for CONTINUE, O(N) for FULLRESYNC where N is dataset size
pub fn psync(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongArity("PSYNC".to_string()));
    }

    let repl_id = args[0]
        .as_str()
        .or_else(|| args[0].as_bytes().and_then(|b| std::str::from_utf8(b).ok()))
        .ok_or_else(|| CommandError::InvalidArgument("invalid replication id".to_string()))?;

    let offset_str = args[1]
        .as_str()
        .or_else(|| args[1].as_bytes().and_then(|b| std::str::from_utf8(b).ok()))
        .ok_or_else(|| CommandError::InvalidArgument("invalid offset".to_string()))?;

    // Check for initial sync request: PSYNC ? -1
    if repl_id == "?" && offset_str == "-1" {
        // First time sync - perform full resynchronization
        if let Some(manager) = ctx.replication_manager() {
            let info = manager.info();

            // Return FULLRESYNC with our replication ID and current offset
            let response = format!(
                "FULLRESYNC {} {}",
                info.master_replid, info.master_repl_offset
            );

            // TODO: Send RDB snapshot after this response
            // For now, we just return the FULLRESYNC response
            // The follower will then expect the RDB data and command stream

            return Ok(RespValue::SimpleString(response));
        }

        // No replication manager - cannot perform PSYNC
        return Err(CommandError::Internal(
            "replication not available".to_string(),
        ));
    }

    // Partial resync request: PSYNC <repl_id> <offset>
    let offset: i64 = offset_str
        .parse()
        .map_err(|_| CommandError::InvalidArgument("invalid offset".to_string()))?;

    if let Some(manager) = ctx.replication_manager() {
        let info = manager.info();

        // Check if we can do partial resync
        // 1. Replication ID must match
        // 2. Offset must be in backlog range

        if repl_id == info.master_replid {
            // TODO: Check if offset is in backlog
            // For now, we always do full resync if offset doesn't match exactly

            if offset >= 0 && offset as u64 == info.master_repl_offset {
                // Replica is up to date, continue with current ID
                let response = format!("CONTINUE {}", info.master_replid);
                return Ok(RespValue::SimpleString(response));
            }
        }

        // Cannot do partial resync, fall back to full resync
        let response = format!(
            "FULLRESYNC {} {}",
            info.master_replid, info.master_repl_offset
        );
        return Ok(RespValue::SimpleString(response));
    }

    Err(CommandError::Internal(
        "replication not available".to_string(),
    ))
}

/// REPLCONF option value [option value ...]
///
/// Configuration command sent by replicas to leaders.
/// Used for capability negotiation and acknowledgment.
///
/// Time complexity: O(1)
pub fn replconf(_ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() || args.len() % 2 != 0 {
        return Err(CommandError::WrongArity("REPLCONF".to_string()));
    }

    // Parse option-value pairs
    for chunk in args.chunks(2) {
        let option = chunk[0]
            .as_str()
            .or_else(|| {
                chunk[0]
                    .as_bytes()
                    .and_then(|b| std::str::from_utf8(b).ok())
            })
            .ok_or_else(|| CommandError::InvalidArgument("invalid option".to_string()))?;

        let _value = chunk[1]
            .as_str()
            .or_else(|| {
                chunk[1]
                    .as_bytes()
                    .and_then(|b| std::str::from_utf8(b).ok())
            })
            .ok_or_else(|| CommandError::InvalidArgument("invalid value".to_string()))?;

        // Handle different options
        match option.to_uppercase().as_str() {
            "LISTENING-PORT" => {
                // Replica tells us its listening port
                // TODO: Store this information
            }
            "CAPA" => {
                // Capability negotiation (eof, psync2, etc.)
                // TODO: Store replica capabilities
            }
            "ACK" => {
                // Replica acknowledges replication offset
                // TODO: Update replica offset tracking
            }
            "GETACK" => {
                // Leader requests ACK from replica
                // This is handled on replica side
            }
            _ => {
                // Unknown option, ignore for compatibility
            }
        }
    }

    // Always return OK for REPLCONF
    Ok(RespValue::ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferris_core::KeyStore;
    use std::sync::Arc;

    #[test]
    fn test_replicaof_wrong_arity() {
        let store = Arc::new(KeyStore::default());
        let mut ctx = CommandContext::new(store);

        let result = replicaof(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));

        let result = replicaof(&mut ctx, &[RespValue::bulk_string("host")]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[tokio::test]
    async fn test_replicaof_no_one() {
        use ferris_core::{BlockingRegistry, PubSubRegistry};
        use ferris_replication::ReplicationManager;

        let store = Arc::new(KeyStore::default());
        let blocking_registry = Arc::new(BlockingRegistry::new());
        let pubsub_registry = Arc::new(PubSubRegistry::new());
        let replication_manager = Arc::new(ReplicationManager::new());
        let mut ctx = CommandContext::with_resources(
            store,
            blocking_registry,
            pubsub_registry,
            None,
            Some(replication_manager),
        );

        let result = replicaof(
            &mut ctx,
            &[RespValue::bulk_string("NO"), RespValue::bulk_string("ONE")],
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), RespValue::SimpleString("OK".to_string()));
    }

    #[tokio::test]
    async fn test_replicaof_with_host_port() {
        use ferris_core::{BlockingRegistry, PubSubRegistry};
        use ferris_replication::ReplicationManager;

        let store = Arc::new(KeyStore::default());
        let blocking_registry = Arc::new(BlockingRegistry::new());
        let pubsub_registry = Arc::new(PubSubRegistry::new());
        let replication_manager = Arc::new(ReplicationManager::new());
        let mut ctx = CommandContext::with_resources(
            store,
            blocking_registry,
            pubsub_registry,
            None,
            Some(replication_manager),
        );

        let result = replicaof(
            &mut ctx,
            &[
                RespValue::bulk_string("127.0.0.1"),
                RespValue::bulk_string("6379"),
            ],
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_replicaof_invalid_port() {
        let store = Arc::new(KeyStore::default());
        let mut ctx = CommandContext::new(store);

        let result = replicaof(
            &mut ctx,
            &[
                RespValue::bulk_string("127.0.0.1"),
                RespValue::bulk_string("invalid"),
            ],
        );
        assert!(matches!(result, Err(CommandError::InvalidArgument(_))));
    }

    #[tokio::test]
    async fn test_slaveof_alias() {
        use ferris_core::{BlockingRegistry, PubSubRegistry};
        use ferris_replication::ReplicationManager;

        let store = Arc::new(KeyStore::default());
        let blocking_registry = Arc::new(BlockingRegistry::new());
        let pubsub_registry = Arc::new(PubSubRegistry::new());
        let replication_manager = Arc::new(ReplicationManager::new());
        let mut ctx = CommandContext::with_resources(
            store,
            blocking_registry,
            pubsub_registry,
            None,
            Some(replication_manager),
        );

        let result = slaveof(
            &mut ctx,
            &[RespValue::bulk_string("NO"), RespValue::bulk_string("ONE")],
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_role_returns_master() {
        let store = Arc::new(KeyStore::default());
        let mut ctx = CommandContext::new(store);

        let result = role(&mut ctx, &[]);
        assert!(result.is_ok());

        match result.unwrap() {
            RespValue::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], RespValue::BulkString("master".into()));
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_role_wrong_arity() {
        let store = Arc::new(KeyStore::default());
        let mut ctx = CommandContext::new(store);

        let result = role(&mut ctx, &[RespValue::bulk_string("extra")]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_wait_basic() {
        let store = Arc::new(KeyStore::default());
        let mut ctx = CommandContext::new(store);

        let result = wait(
            &mut ctx,
            &[RespValue::bulk_string("1"), RespValue::bulk_string("1000")],
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), RespValue::Integer(0));
    }

    #[test]
    fn test_wait_wrong_arity() {
        let store = Arc::new(KeyStore::default());
        let mut ctx = CommandContext::new(store);

        let result = wait(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_waitaof_basic() {
        let store = Arc::new(KeyStore::default());
        let mut ctx = CommandContext::new(store);

        let result = waitaof(
            &mut ctx,
            &[
                RespValue::bulk_string("0"),
                RespValue::bulk_string("0"),
                RespValue::bulk_string("1000"),
            ],
        );
        assert!(result.is_ok());
    }
}

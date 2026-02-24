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
pub fn replicaof(_ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
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
        // TODO: Promote to leader - disconnect from master, stop replication
        return Ok(RespValue::SimpleString("OK".to_string()));
    }

    // Parse port
    let _port: u16 = port_str
        .parse()
        .map_err(|_| CommandError::InvalidArgument("port must be a number".to_string()))?;

    // TODO: Actual replication logic:
    // 1. Store host:port in replication state
    // 2. Connect to leader in background
    // 3. Start replication stream
    // 4. Enter follower mode (reject writes)

    Ok(RespValue::SimpleString("OK".to_string()))
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

    #[test]
    fn test_replicaof_no_one() {
        let store = Arc::new(KeyStore::default());
        let mut ctx = CommandContext::new(store);

        let result = replicaof(
            &mut ctx,
            &[RespValue::bulk_string("NO"), RespValue::bulk_string("ONE")],
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), RespValue::SimpleString("OK".to_string()));
    }

    #[test]
    fn test_replicaof_with_host_port() {
        let store = Arc::new(KeyStore::default());
        let mut ctx = CommandContext::new(store);

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

    #[test]
    fn test_slaveof_alias() {
        let store = Arc::new(KeyStore::default());
        let mut ctx = CommandContext::new(store);

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

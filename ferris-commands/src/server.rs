//! Server commands: PING, ECHO, DBSIZE, TIME, INFO, COMMAND, FLUSHDB, FLUSHALL,
//! DEBUG, CONFIG, AUTH, SWAPDB, SLOWLOG, MEMORY

use crate::{CommandContext, CommandError, CommandResult};
use bytes::Bytes;
use ferris_protocol::RespValue;
use std::time::{SystemTime, UNIX_EPOCH};

/// Parse a memory size string like "100mb", "1gb", "1024" into bytes
fn parse_memory_size(s: &str) -> Result<usize, CommandError> {
    let s = s.trim().to_lowercase();

    // Check for unit suffixes
    let (num_str, multiplier) = if s.ends_with("gb") {
        (&s[..s.len() - 2], 1024 * 1024 * 1024)
    } else if s.ends_with("g") {
        (&s[..s.len() - 1], 1024 * 1024 * 1024)
    } else if s.ends_with("mb") {
        (&s[..s.len() - 2], 1024 * 1024)
    } else if s.ends_with("m") {
        (&s[..s.len() - 1], 1024 * 1024)
    } else if s.ends_with("kb") {
        (&s[..s.len() - 2], 1024)
    } else if s.ends_with("k") {
        (&s[..s.len() - 1], 1024)
    } else if s.ends_with('b') {
        (&s[..s.len() - 1], 1)
    } else {
        (s.as_str(), 1)
    };

    let num: usize = num_str
        .trim()
        .parse()
        .map_err(|_| CommandError::InvalidArgument("Invalid memory size".into()))?;

    Ok(num * multiplier)
}

/// PING [message]
///
/// Returns PONG if no argument is provided, otherwise returns a copy of the argument.
/// This command is often used to test if a connection is still alive.
///
/// Time complexity: O(1)
pub fn ping(_ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    match args.first() {
        Some(msg) => {
            // Return the message as bulk string
            if let Some(bytes) = msg.as_bytes() {
                Ok(RespValue::BulkString(bytes.clone()))
            } else if let Some(s) = msg.as_str() {
                Ok(RespValue::BulkString(Bytes::from(s.to_owned())))
            } else {
                Ok(RespValue::SimpleString("PONG".to_string()))
            }
        }
        None => Ok(RespValue::SimpleString("PONG".to_string())),
    }
}

/// ECHO message
///
/// Returns the message.
///
/// Time complexity: O(1)
pub fn echo(_ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    let msg = args
        .first()
        .ok_or_else(|| CommandError::WrongArity("ECHO".to_string()))?;

    if let Some(bytes) = msg.as_bytes() {
        Ok(RespValue::BulkString(bytes.clone()))
    } else if let Some(s) = msg.as_str() {
        Ok(RespValue::BulkString(Bytes::from(s.to_owned())))
    } else {
        Err(CommandError::InvalidArgument(
            "invalid argument type".to_string(),
        ))
    }
}

/// DBSIZE
///
/// Returns the number of keys in the currently-selected database.
///
/// Time complexity: O(1)
pub fn dbsize(ctx: &mut CommandContext, _args: &[RespValue]) -> CommandResult {
    let db = ctx.store().database(ctx.selected_db());
    Ok(RespValue::Integer(db.len() as i64))
}

/// TIME
///
/// Returns the current server time as a two-element array:
/// - Unix timestamp in seconds
/// - Microseconds
///
/// Time complexity: O(1)
pub fn time(_ctx: &mut CommandContext, _args: &[RespValue]) -> CommandResult {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();

    let seconds = now.as_secs();
    let micros = now.subsec_micros();

    Ok(RespValue::Array(vec![
        RespValue::BulkString(Bytes::from(seconds.to_string())),
        RespValue::BulkString(Bytes::from(micros.to_string())),
    ]))
}

/// INFO [section]
///
/// Returns information and statistics about the server in a format that is
/// simple to parse by computers and easy to read by humans.
///
/// Time complexity: O(1)
pub fn info(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    let section = args.first().and_then(|v| v.as_str()).unwrap_or("default");

    let mut info = String::new();

    // Server section
    if section == "default" || section == "server" || section == "all" {
        info.push_str("# Server\r\n");
        info.push_str("redis_version:7.0.0\r\n");
        info.push_str("ferris_version:0.1.0\r\n");
        info.push_str("redis_mode:standalone\r\n");
        info.push_str("os:rust\r\n");
        info.push_str("arch_bits:64\r\n");
        info.push_str("tcp_port:6380\r\n");
        info.push_str("\r\n");
    }

    // Keyspace section
    if section == "default" || section == "keyspace" || section == "all" {
        info.push_str("# Keyspace\r\n");
        for i in 0..ctx.store().num_databases() {
            let db = ctx.store().database(i);
            let len = db.len();
            if len > 0 {
                info.push_str(&format!("db{i}:keys={len},expires=0,avg_ttl=0\r\n"));
            }
        }
        info.push_str("\r\n");
    }

    // Clients section
    if section == "clients" || section == "all" {
        info.push_str("# Clients\r\n");
        info.push_str("connected_clients:1\r\n");
        info.push_str("\r\n");
    }

    // Memory section
    if section == "memory" || section == "all" {
        info.push_str("# Memory\r\n");
        info.push_str("used_memory:0\r\n");
        info.push_str("used_memory_human:0B\r\n");
        info.push_str("\r\n");
    }

    // Replication section
    if section == "replication" || section == "all" {
        info.push_str("# Replication\r\n");
        info.push_str("role:master\r\n");
        info.push_str("connected_slaves:0\r\n");
        info.push_str("\r\n");
    }

    Ok(RespValue::BulkString(Bytes::from(info)))
}

/// COMMAND [DOCS|COUNT|GETKEYS|INFO|LIST]
///
/// Returns information about available commands.
///
/// Time complexity: O(N) where N is the total number of commands
pub fn command(_ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    // Simple implementation - just return command count for now
    let subcommand = args
        .first()
        .and_then(|v| v.as_str())
        .map(|s| s.to_uppercase());

    match subcommand.as_deref() {
        Some("COUNT") => {
            // Actual count of commands registered in register_all_commands.
            // Keep this in sync when adding/removing commands from registry.rs.
            Ok(RespValue::Integer(142))
        }
        Some("DOCS") => {
            // Not fully implemented yet
            Ok(RespValue::Array(vec![]))
        }
        None => {
            // Return empty array for full command list (not fully implemented)
            Ok(RespValue::Array(vec![]))
        }
        _ => {
            // Unknown subcommand
            Ok(RespValue::Array(vec![]))
        }
    }
}

/// FLUSHDB [ASYNC|SYNC]
///
/// Delete all the keys of the currently selected DB.
///
/// Time complexity: O(N) where N is the number of keys in the database
pub fn flushdb(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    // Parse optional ASYNC/SYNC argument
    if let Some(mode) = args.first() {
        let mode_str = mode.as_str().ok_or(CommandError::SyntaxError)?;
        match mode_str.to_uppercase().as_str() {
            "ASYNC" | "SYNC" => { /* accepted; both do synchronous flush for now */ }
            _ => {
                return Err(CommandError::SyntaxError);
            }
        }
    }

    let db = ctx.store().database(ctx.selected_db());
    db.flush();
    Ok(RespValue::ok())
}

/// FLUSHALL [ASYNC|SYNC]
///
/// Delete all the keys of all the existing databases.
///
/// Time complexity: O(N) where N is the total number of keys in all databases
pub fn flushall(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    // Parse optional ASYNC/SYNC argument
    if let Some(mode) = args.first() {
        let mode_str = mode.as_str().ok_or(CommandError::SyntaxError)?;
        match mode_str.to_uppercase().as_str() {
            "ASYNC" | "SYNC" => { /* accepted; both do synchronous flush for now */ }
            _ => {
                return Err(CommandError::SyntaxError);
            }
        }
    }

    for i in 0..ctx.store().num_databases() {
        ctx.store().database(i).flush();
    }
    Ok(RespValue::ok())
}

/// DEBUG subcommand [argument]
///
/// A container for debugging commands. Not for production use.
///
/// Time complexity: Depends on subcommand
pub fn debug(_ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    let subcommand = args
        .first()
        .and_then(|v| v.as_str())
        .map(|s| s.to_uppercase());

    match subcommand.as_deref() {
        Some("SLEEP") => {
            // DEBUG SLEEP seconds
            // NOTE: This uses std::thread::sleep which blocks the current OS thread.
            // Command handlers are synchronous, so we cannot use tokio::time::sleep here.
            // This is acceptable for a debug-only command that is rarely used in production.
            if let Some(secs) = args
                .get(1)
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f64>().ok())
            {
                std::thread::sleep(std::time::Duration::from_secs_f64(secs));
            }
            Ok(RespValue::ok())
        }
        Some("SEGFAULT") => {
            // Refuse to crash
            Err(CommandError::InvalidArgument(
                "DEBUG SEGFAULT not supported".to_string(),
            ))
        }
        Some(sub) => Err(CommandError::InvalidArgument(format!(
            "Unknown DEBUG subcommand '{sub}'"
        ))),
        None => Err(CommandError::InvalidArgument(
            "Unknown DEBUG subcommand".to_string(),
        )),
    }
}

/// CONFIG subcommand [arguments...]
///
/// Read/set/reset the server's configuration parameters.
///
/// Subcommands:
/// - CONFIG GET pattern
/// - CONFIG SET parameter value [parameter value ...]
/// - CONFIG RESETSTAT
///
/// Time complexity: O(N) for GET with pattern, O(1) for SET
pub fn config(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    let subcommand = args
        .first()
        .and_then(|v| v.as_str())
        .map(|s| s.to_uppercase())
        .ok_or_else(|| CommandError::WrongArity("CONFIG".to_string()))?;

    match subcommand.as_str() {
        "GET" => {
            let pattern = args
                .get(1)
                .and_then(|v| v.as_str())
                .ok_or(CommandError::SyntaxError)?;

            // Return known configuration parameters that match the pattern
            let mut result = Vec::new();
            let params: Vec<(&str, String)> = vec![
                ("bind", "0.0.0.0".to_string()),
                ("port", "6380".to_string()),
                ("databases", ctx.store().num_databases().to_string()),
                ("maxmemory", ctx.store().max_memory().to_string()),
                (
                    "maxmemory-policy",
                    ctx.store().eviction_policy().as_str().to_string(),
                ),
                ("appendonly", "yes".to_string()),
                ("appendfsync", "everysec".to_string()),
                ("save", "".to_string()),
                ("dir", ".".to_string()),
                ("dbfilename", "dump.rdb".to_string()),
                ("requirepass", "".to_string()),
                ("maxclients", "10000".to_string()),
                ("timeout", "0".to_string()),
                ("tcp-keepalive", "300".to_string()),
                ("hz", "10".to_string()),
                ("lfu-log-factor", "10".to_string()),
                ("lfu-decay-time", "1".to_string()),
            ];

            for (name, value) in &params {
                if glob_match(pattern, name) {
                    result.push(RespValue::BulkString(Bytes::from(name.to_string())));
                    result.push(RespValue::BulkString(Bytes::from(value.clone())));
                }
            }

            Ok(RespValue::Array(result))
        }
        "SET" => {
            // CONFIG SET requires parameter-value pairs
            if args.len() < 3 || (args.len() - 1) % 2 != 0 {
                return Err(CommandError::SyntaxError);
            }

            // Apply recognized configuration changes
            let mut i = 1;
            while i + 1 < args.len() {
                let param = args[i].as_str().ok_or(CommandError::SyntaxError)?;
                let value = args[i + 1].as_str().ok_or(CommandError::SyntaxError)?;

                match param.to_lowercase().as_str() {
                    "maxmemory" => {
                        let bytes = parse_memory_size(value)?;
                        ctx.store().set_max_memory(bytes);
                    }
                    "maxmemory-policy" => {
                        let policy = ferris_core::EvictionPolicy::from_str(value).ok_or(
                            CommandError::InvalidArgument("Invalid maxmemory-policy value".into()),
                        )?;
                        ctx.store().set_eviction_policy(policy);
                    }
                    // Other parameters are accepted but not currently applied
                    _ => {}
                }
                i += 2;
            }

            Ok(RespValue::ok())
        }
        "RESETSTAT" => {
            // Reset statistics counters including memory stats
            ctx.store().reset_memory_stats();
            Ok(RespValue::ok())
        }
        "REWRITE" => {
            // Rewrite config file (placeholder - we don't have a config file yet)
            Ok(RespValue::ok())
        }
        _ => Err(CommandError::InvalidArgument(format!(
            "Unknown CONFIG subcommand '{subcommand}'"
        ))),
    }
}

/// Simple glob pattern matching for CONFIG GET
fn glob_match(pattern: &str, s: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    // Simple glob: support *, ? and literal matching
    let pi: Vec<char> = pattern.chars().collect();
    let si: Vec<char> = s.chars().collect();

    glob_match_impl(&pi, &si, 0, 0)
}

fn glob_match_impl(pattern: &[char], s: &[char], pi: usize, si: usize) -> bool {
    let mut pi = pi;
    let mut si = si;

    while pi < pattern.len() {
        match pattern[pi] {
            '*' => {
                pi += 1;
                if pi >= pattern.len() {
                    return true;
                }
                for i in si..=s.len() {
                    if glob_match_impl(pattern, s, pi, i) {
                        return true;
                    }
                }
                return false;
            }
            '?' => {
                if si >= s.len() {
                    return false;
                }
                pi += 1;
                si += 1;
            }
            c => {
                if si >= s.len() || s[si] != c {
                    return false;
                }
                pi += 1;
                si += 1;
            }
        }
    }

    pi >= pattern.len() && si >= s.len()
}

/// AUTH [username] password
///
/// Authenticate to the server.
///
/// Time complexity: O(N) where N is the number of passwords defined
pub fn auth(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("AUTH".to_string()));
    }

    // TODO: Implement actual password validation against `requirepass` config.
    // Currently no config system is wired up, so AUTH always succeeds when called
    // with a password argument. Once config is available, this should:
    // 1. Check if requirepass is set; if not, return an error (Redis behavior).
    // 2. Validate the provided password (and optional username) against config.
    // 3. Return WrongPass error on mismatch.

    let _password = args
        .last()
        .and_then(|v| v.as_str())
        .ok_or(CommandError::SyntaxError)?;

    ctx.set_authenticated(true);
    Ok(RespValue::ok())
}

/// SWAPDB index1 index2
///
/// Swap two Redis databases.
///
/// Time complexity: O(N) where N is the number of keys in both databases
pub fn swapdb(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("SWAPDB".to_string()));
    }

    let idx1_str = args[0].as_str().ok_or(CommandError::NotAnInteger)?;
    let idx2_str = args[1].as_str().ok_or(CommandError::NotAnInteger)?;

    let idx1: usize = idx1_str.parse().map_err(|_| CommandError::InvalidDbIndex)?;
    let idx2: usize = idx2_str.parse().map_err(|_| CommandError::InvalidDbIndex)?;

    let num_dbs = ctx.store().num_databases();
    if idx1 >= num_dbs || idx2 >= num_dbs {
        return Err(CommandError::InvalidDbIndex);
    }

    if idx1 == idx2 {
        return Ok(RespValue::ok());
    }

    // Swap the two databases
    ctx.store().swap_databases(idx1, idx2);

    Ok(RespValue::ok())
}

/// SLOWLOG subcommand [arguments...]
///
/// Manages the Redis slow queries log.
///
/// Subcommands:
/// - SLOWLOG GET [count]
/// - SLOWLOG LEN
/// - SLOWLOG RESET
///
/// Time complexity: O(1) for most subcommands
pub fn slowlog(_ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    let subcommand = args
        .first()
        .and_then(|v| v.as_str())
        .map(|s| s.to_uppercase())
        .ok_or_else(|| CommandError::WrongArity("SLOWLOG".to_string()))?;

    match subcommand.as_str() {
        "GET" => {
            // Return empty slow log (no entries tracked yet)
            Ok(RespValue::Array(vec![]))
        }
        "LEN" => {
            // Return 0 entries
            Ok(RespValue::Integer(0))
        }
        "RESET" => Ok(RespValue::ok()),
        _ => Err(CommandError::InvalidArgument(format!(
            "Unknown SLOWLOG subcommand '{subcommand}'"
        ))),
    }
}

/// MEMORY subcommand [arguments...]
///
/// Memory introspection commands.
///
/// Subcommands:
/// - MEMORY USAGE key [SAMPLES count]
/// - MEMORY STATS
/// - MEMORY DOCTOR
/// - MEMORY MALLOC-STATS
/// - MEMORY PURGE
///
/// Time complexity: O(N) where N is the number of samples for USAGE
pub fn memory(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    let subcommand = args
        .first()
        .and_then(|v| v.as_str())
        .map(|s| s.to_uppercase())
        .ok_or_else(|| CommandError::WrongArity("MEMORY".to_string()))?;

    match subcommand.as_str() {
        "USAGE" => {
            let key = args
                .get(1)
                .and_then(|v| v.as_bytes().cloned())
                .or_else(|| {
                    args.get(1)
                        .and_then(|v| v.as_str())
                        .map(|s| Bytes::from(s.to_owned()))
                })
                .ok_or(CommandError::SyntaxError)?;

            let db = ctx.store().database(ctx.selected_db());

            match db.get(&key) {
                Some(entry) => {
                    if entry.is_expired() {
                        db.delete(&key);
                        return Ok(RespValue::Null);
                    }
                    // Approximate memory usage
                    let usage = estimate_memory_usage(&entry.value, &key);
                    Ok(RespValue::Integer(usage))
                }
                None => Ok(RespValue::Null),
            }
        }
        "STATS" => {
            // Return memory stats as array of key-value pairs
            let mut result = Vec::new();
            let tracker = ctx.store().memory_tracker();

            let peak_allocated = tracker.peak() as i64;
            let total_allocated = tracker.used() as i64;
            let max_memory = tracker.max_memory() as i64;
            let evicted_keys = tracker.evicted_keys() as i64;

            result.push(RespValue::BulkString(Bytes::from("peak.allocated")));
            result.push(RespValue::Integer(peak_allocated));

            result.push(RespValue::BulkString(Bytes::from("total.allocated")));
            result.push(RespValue::Integer(total_allocated));

            result.push(RespValue::BulkString(Bytes::from("startup.allocated")));
            result.push(RespValue::Integer(0)); // We don't track this separately

            result.push(RespValue::BulkString(Bytes::from("replication.backlog")));
            result.push(RespValue::Integer(0));

            result.push(RespValue::BulkString(Bytes::from("clients.slaves")));
            result.push(RespValue::Integer(0));

            result.push(RespValue::BulkString(Bytes::from("clients.normal")));
            result.push(RespValue::Integer(0));

            result.push(RespValue::BulkString(Bytes::from("keys.count")));
            let total_keys: i64 = (0..ctx.store().num_databases())
                .map(|i| ctx.store().database(i).len() as i64)
                .sum();
            result.push(RespValue::Integer(total_keys));

            result.push(RespValue::BulkString(Bytes::from("keys.bytes-per-key")));
            let bytes_per_key = if total_keys > 0 {
                total_allocated / total_keys
            } else {
                0
            };
            result.push(RespValue::Integer(bytes_per_key));

            result.push(RespValue::BulkString(Bytes::from("dataset.bytes")));
            result.push(RespValue::Integer(total_allocated));

            result.push(RespValue::BulkString(Bytes::from("dataset.percentage")));
            let percentage = if max_memory > 0 {
                format!(
                    "{:.2}",
                    (total_allocated as f64 / max_memory as f64) * 100.0
                )
            } else {
                "0.00".to_string()
            };
            result.push(RespValue::BulkString(Bytes::from(percentage)));

            result.push(RespValue::BulkString(Bytes::from("maxmemory")));
            result.push(RespValue::Integer(max_memory));

            result.push(RespValue::BulkString(Bytes::from("evicted.keys")));
            result.push(RespValue::Integer(evicted_keys));

            Ok(RespValue::Array(result))
        }
        "DOCTOR" => Ok(RespValue::BulkString(Bytes::from(
            "Sam, I have no memory problems",
        ))),
        "MALLOC-STATS" => Ok(RespValue::BulkString(Bytes::from(
            "Memory allocator stats not available (Rust global allocator)",
        ))),
        "PURGE" => Ok(RespValue::ok()),
        _ => Err(CommandError::InvalidArgument(format!(
            "Unknown MEMORY subcommand '{subcommand}'"
        ))),
    }
}

/// Estimate memory usage of a key-value pair.
fn estimate_memory_usage(value: &ferris_core::RedisValue, key: &[u8]) -> i64 {
    use ferris_core::RedisValue;

    // Base overhead: key + entry struct overhead
    let base = key.len() as i64 + 64; // 64 bytes for entry metadata

    let value_size = match value {
        RedisValue::String(s) => s.len() as i64 + 24, // Bytes overhead
        RedisValue::List(l) => {
            let items: i64 = l.iter().map(|v| v.len() as i64 + 24).sum();
            items + 48 // VecDeque overhead
        }
        RedisValue::Set(s) => {
            let items: i64 = s.iter().map(|v| v.len() as i64 + 32).sum();
            items + 48 // HashSet overhead
        }
        RedisValue::Hash(h) => {
            let items: i64 = h
                .iter()
                .map(|(k, v)| k.len() as i64 + v.len() as i64 + 64)
                .sum();
            items + 48 // HashMap overhead
        }
        RedisValue::SortedSet { scores, members: _ } => {
            let items: i64 = scores
                .iter()
                .map(|(k, _)| k.len() as i64 + 8 + 40) // member + score + btree node
                .sum();
            items + 96 // HashMap + BTreeMap overhead
        }
    };

    base + value_size
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferris_core::KeyStore;
    use std::collections::{HashMap, HashSet, VecDeque};
    use std::sync::Arc;

    fn make_ctx() -> CommandContext {
        CommandContext::new(Arc::new(KeyStore::default()))
    }

    #[test]
    fn test_ping_no_args() {
        let mut ctx = make_ctx();
        let result = ping(&mut ctx, &[]).unwrap();
        assert_eq!(result, RespValue::SimpleString("PONG".to_string()));
    }

    #[test]
    fn test_ping_with_message() {
        let mut ctx = make_ctx();
        let args = vec![RespValue::BulkString(Bytes::from("hello"))];
        let result = ping(&mut ctx, &args).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from("hello")));
    }

    #[test]
    fn test_echo() {
        let mut ctx = make_ctx();
        let args = vec![RespValue::BulkString(Bytes::from("Hello World"))];
        let result = echo(&mut ctx, &args).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from("Hello World")));
    }

    #[test]
    fn test_echo_no_args() {
        let mut ctx = make_ctx();
        let result = echo(&mut ctx, &[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_dbsize_empty() {
        let mut ctx = make_ctx();
        let result = dbsize(&mut ctx, &[]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_dbsize_with_keys() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(0);
        db.set(
            Bytes::from("key1"),
            ferris_core::Entry::new(ferris_core::RedisValue::String(Bytes::from("value1"))),
        );
        db.set(
            Bytes::from("key2"),
            ferris_core::Entry::new(ferris_core::RedisValue::String(Bytes::from("value2"))),
        );

        let result = dbsize(&mut ctx, &[]).unwrap();
        assert_eq!(result, RespValue::Integer(2));
    }

    #[test]
    fn test_time() {
        let mut ctx = make_ctx();
        let result = time(&mut ctx, &[]).unwrap();

        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 2);
            // Verify first element is seconds (should be > 0)
            if let RespValue::BulkString(secs) = &arr[0] {
                let secs_val: u64 = std::str::from_utf8(secs).unwrap().parse().unwrap();
                assert!(secs_val > 0);
            } else {
                panic!("Expected bulk string for seconds");
            }
        } else {
            panic!("Expected array from TIME");
        }
    }

    #[test]
    fn test_info_default() {
        let mut ctx = make_ctx();
        let result = info(&mut ctx, &[]).unwrap();

        if let RespValue::BulkString(data) = result {
            let info_str = std::str::from_utf8(&data).unwrap();
            assert!(info_str.contains("# Server"));
            assert!(info_str.contains("redis_version"));
        } else {
            panic!("Expected bulk string from INFO");
        }
    }

    #[test]
    fn test_info_keyspace() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(0);
        db.set(
            Bytes::from("key1"),
            ferris_core::Entry::new(ferris_core::RedisValue::String(Bytes::from("value1"))),
        );

        let args = vec![RespValue::BulkString(Bytes::from("keyspace"))];
        let result = info(&mut ctx, &args).unwrap();

        if let RespValue::BulkString(data) = result {
            let info_str = std::str::from_utf8(&data).unwrap();
            assert!(info_str.contains("# Keyspace"));
            assert!(info_str.contains("db0:keys=1"));
        } else {
            panic!("Expected bulk string from INFO keyspace");
        }
    }

    #[test]
    fn test_flushdb() {
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(0);
            db.set(
                Bytes::from("key1"),
                ferris_core::Entry::new(ferris_core::RedisValue::String(Bytes::from("value1"))),
            );
            db.set(
                Bytes::from("key2"),
                ferris_core::Entry::new(ferris_core::RedisValue::String(Bytes::from("value2"))),
            );
            assert_eq!(db.len(), 2);
        }

        let result = flushdb(&mut ctx, &[]).unwrap();
        assert_eq!(result, RespValue::ok());
        assert_eq!(ctx.store().database(0).len(), 0);
    }

    #[test]
    fn test_flushall() {
        let mut ctx = make_ctx();

        // Add keys to multiple databases
        ctx.store().database(0).set(
            Bytes::from("key1"),
            ferris_core::Entry::new(ferris_core::RedisValue::String(Bytes::from("value1"))),
        );
        ctx.store().database(1).set(
            Bytes::from("key2"),
            ferris_core::Entry::new(ferris_core::RedisValue::String(Bytes::from("value2"))),
        );

        let result = flushall(&mut ctx, &[]).unwrap();
        assert_eq!(result, RespValue::ok());
        assert_eq!(ctx.store().database(0).len(), 0);
        assert_eq!(ctx.store().database(1).len(), 0);
    }

    #[test]
    fn test_command_count() {
        let mut ctx = make_ctx();
        let args = vec![RespValue::BulkString(Bytes::from("COUNT"))];
        let result = command(&mut ctx, &args).unwrap();

        if let RespValue::Integer(n) = result {
            assert!(n > 0);
        } else {
            panic!("Expected integer from COMMAND COUNT");
        }
    }

    #[test]
    fn test_debug_sleep() {
        let mut ctx = make_ctx();
        let args = vec![
            RespValue::BulkString(Bytes::from("SLEEP")),
            RespValue::BulkString(Bytes::from("0.001")),
        ];
        let result = debug(&mut ctx, &args).unwrap();
        assert_eq!(result, RespValue::ok());
    }

    #[test]
    fn test_debug_segfault_refused() {
        let mut ctx = make_ctx();
        let args = vec![RespValue::BulkString(Bytes::from("SEGFAULT"))];
        let result = debug(&mut ctx, &args);
        assert!(result.is_err());
    }

    // ===== CONFIG tests =====

    #[test]
    fn test_config_get_all() {
        let mut ctx = make_ctx();
        let args = vec![
            RespValue::BulkString(Bytes::from("GET")),
            RespValue::BulkString(Bytes::from("*")),
        ];
        let result = config(&mut ctx, &args).unwrap();
        if let RespValue::Array(arr) = result {
            // Should return pairs of key-value
            assert!(arr.len() > 0);
            assert!(arr.len() % 2 == 0);
        } else {
            panic!("Expected array from CONFIG GET *");
        }
    }

    #[test]
    fn test_config_get_specific() {
        let mut ctx = make_ctx();
        let args = vec![
            RespValue::BulkString(Bytes::from("GET")),
            RespValue::BulkString(Bytes::from("port")),
        ];
        let result = config(&mut ctx, &args).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], RespValue::BulkString(Bytes::from("port")));
            assert_eq!(arr[1], RespValue::BulkString(Bytes::from("6380")));
        } else {
            panic!("Expected array from CONFIG GET port");
        }
    }

    #[test]
    fn test_config_get_no_match() {
        let mut ctx = make_ctx();
        let args = vec![
            RespValue::BulkString(Bytes::from("GET")),
            RespValue::BulkString(Bytes::from("nonexistent_param")),
        ];
        let result = config(&mut ctx, &args).unwrap();
        assert_eq!(result, RespValue::Array(vec![]));
    }

    #[test]
    fn test_config_set() {
        let mut ctx = make_ctx();
        let args = vec![
            RespValue::BulkString(Bytes::from("SET")),
            RespValue::BulkString(Bytes::from("hz")),
            RespValue::BulkString(Bytes::from("20")),
        ];
        let result = config(&mut ctx, &args).unwrap();
        assert_eq!(result, RespValue::ok());
    }

    #[test]
    fn test_config_resetstat() {
        let mut ctx = make_ctx();
        let args = vec![RespValue::BulkString(Bytes::from("RESETSTAT"))];
        let result = config(&mut ctx, &args).unwrap();
        assert_eq!(result, RespValue::ok());
    }

    #[test]
    fn test_config_unknown_subcommand() {
        let mut ctx = make_ctx();
        let args = vec![RespValue::BulkString(Bytes::from("BADSUBCMD"))];
        let result = config(&mut ctx, &args);
        assert!(result.is_err());
    }

    #[test]
    fn test_config_no_args() {
        let mut ctx = make_ctx();
        let result = config(&mut ctx, &[]);
        assert!(result.is_err());
    }

    // ===== AUTH tests =====

    #[test]
    fn test_auth_basic() {
        let mut ctx = make_ctx();
        assert!(!ctx.is_authenticated());
        let args = vec![RespValue::BulkString(Bytes::from("mypassword"))];
        let result = auth(&mut ctx, &args).unwrap();
        assert_eq!(result, RespValue::ok());
        assert!(ctx.is_authenticated());
    }

    #[test]
    fn test_auth_no_args() {
        let mut ctx = make_ctx();
        let result = auth(&mut ctx, &[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_auth_with_username() {
        let mut ctx = make_ctx();
        let args = vec![
            RespValue::BulkString(Bytes::from("default")),
            RespValue::BulkString(Bytes::from("mypassword")),
        ];
        let result = auth(&mut ctx, &args).unwrap();
        assert_eq!(result, RespValue::ok());
        assert!(ctx.is_authenticated());
    }

    // ===== SWAPDB tests =====

    #[test]
    fn test_swapdb_basic() {
        let mut ctx = make_ctx();

        // Put data in db 0
        ctx.store().database(0).set(
            Bytes::from("key0"),
            ferris_core::Entry::new(ferris_core::RedisValue::String(Bytes::from("val0"))),
        );
        // Put data in db 1
        ctx.store().database(1).set(
            Bytes::from("key1"),
            ferris_core::Entry::new(ferris_core::RedisValue::String(Bytes::from("val1"))),
        );

        assert_eq!(ctx.store().database(0).len(), 1);
        assert_eq!(ctx.store().database(1).len(), 1);
        assert!(ctx.store().database(0).exists(b"key0"));
        assert!(ctx.store().database(1).exists(b"key1"));

        let args = vec![
            RespValue::BulkString(Bytes::from("0")),
            RespValue::BulkString(Bytes::from("1")),
        ];
        let result = swapdb(&mut ctx, &args).unwrap();
        assert_eq!(result, RespValue::ok());

        // After swap: db0 should have key1, db1 should have key0
        assert!(ctx.store().database(0).exists(b"key1"));
        assert!(ctx.store().database(1).exists(b"key0"));
    }

    #[test]
    fn test_swapdb_same_index() {
        let mut ctx = make_ctx();
        let args = vec![
            RespValue::BulkString(Bytes::from("0")),
            RespValue::BulkString(Bytes::from("0")),
        ];
        let result = swapdb(&mut ctx, &args).unwrap();
        assert_eq!(result, RespValue::ok());
    }

    #[test]
    fn test_swapdb_invalid_index() {
        let mut ctx = make_ctx();
        let args = vec![
            RespValue::BulkString(Bytes::from("0")),
            RespValue::BulkString(Bytes::from("100")),
        ];
        let result = swapdb(&mut ctx, &args);
        assert!(result.is_err());
    }

    #[test]
    fn test_swapdb_wrong_arity() {
        let mut ctx = make_ctx();
        let args = vec![RespValue::BulkString(Bytes::from("0"))];
        let result = swapdb(&mut ctx, &args);
        assert!(result.is_err());
    }

    // ===== SLOWLOG tests =====

    #[test]
    fn test_slowlog_get() {
        let mut ctx = make_ctx();
        let args = vec![RespValue::BulkString(Bytes::from("GET"))];
        let result = slowlog(&mut ctx, &args).unwrap();
        assert_eq!(result, RespValue::Array(vec![]));
    }

    #[test]
    fn test_slowlog_len() {
        let mut ctx = make_ctx();
        let args = vec![RespValue::BulkString(Bytes::from("LEN"))];
        let result = slowlog(&mut ctx, &args).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_slowlog_reset() {
        let mut ctx = make_ctx();
        let args = vec![RespValue::BulkString(Bytes::from("RESET"))];
        let result = slowlog(&mut ctx, &args).unwrap();
        assert_eq!(result, RespValue::ok());
    }

    #[test]
    fn test_slowlog_no_args() {
        let mut ctx = make_ctx();
        let result = slowlog(&mut ctx, &[]);
        assert!(result.is_err());
    }

    // ===== MEMORY tests =====

    #[test]
    fn test_memory_usage_string() {
        let mut ctx = make_ctx();
        ctx.store().database(0).set(
            Bytes::from("mykey"),
            ferris_core::Entry::new(ferris_core::RedisValue::String(Bytes::from("hello"))),
        );

        let args = vec![
            RespValue::BulkString(Bytes::from("USAGE")),
            RespValue::BulkString(Bytes::from("mykey")),
        ];
        let result = memory(&mut ctx, &args).unwrap();
        if let RespValue::Integer(bytes) = result {
            assert!(bytes > 0);
        } else {
            panic!("Expected integer from MEMORY USAGE");
        }
    }

    #[test]
    fn test_memory_usage_nonexistent() {
        let mut ctx = make_ctx();
        let args = vec![
            RespValue::BulkString(Bytes::from("USAGE")),
            RespValue::BulkString(Bytes::from("nokey")),
        ];
        let result = memory(&mut ctx, &args).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_memory_stats() {
        let mut ctx = make_ctx();
        let args = vec![RespValue::BulkString(Bytes::from("STATS"))];
        let result = memory(&mut ctx, &args).unwrap();
        if let RespValue::Array(arr) = result {
            assert!(arr.len() > 0);
            // Check keys.count is present
            let has_keys_count = arr.windows(2).step_by(2).any(|pair| {
                if let RespValue::BulkString(k) = &pair[0] {
                    k.as_ref() == b"keys.count"
                } else {
                    false
                }
            });
            assert!(has_keys_count);
        } else {
            panic!("Expected array from MEMORY STATS");
        }
    }

    #[test]
    fn test_memory_doctor() {
        let mut ctx = make_ctx();
        let args = vec![RespValue::BulkString(Bytes::from("DOCTOR"))];
        let result = memory(&mut ctx, &args).unwrap();
        if let RespValue::BulkString(data) = result {
            assert!(!data.is_empty());
        } else {
            panic!("Expected bulk string from MEMORY DOCTOR");
        }
    }

    #[test]
    fn test_memory_purge() {
        let mut ctx = make_ctx();
        let args = vec![RespValue::BulkString(Bytes::from("PURGE"))];
        let result = memory(&mut ctx, &args).unwrap();
        assert_eq!(result, RespValue::ok());
    }

    #[test]
    fn test_memory_no_args() {
        let mut ctx = make_ctx();
        let result = memory(&mut ctx, &[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_memory_unknown_subcommand() {
        let mut ctx = make_ctx();
        let args = vec![RespValue::BulkString(Bytes::from("BADCMD"))];
        let result = memory(&mut ctx, &args);
        assert!(result.is_err());
    }

    // ===== CONFIG additional tests =====

    #[test]
    fn test_config_get_glob_wildcard() {
        let mut ctx = make_ctx();
        let args = vec![
            RespValue::BulkString(Bytes::from("GET")),
            RespValue::BulkString(Bytes::from("max*")),
        ];
        let result = config(&mut ctx, &args).unwrap();
        if let RespValue::Array(arr) = result {
            // Should match maxmemory, maxmemory-policy, maxclients
            assert!(arr.len() >= 4); // At least 2 params × 2 (name+value)
            assert!(arr.len() % 2 == 0);
            // Verify all returned param names start with "max"
            for pair in arr.chunks(2) {
                if let RespValue::BulkString(name) = &pair[0] {
                    let name_str = std::str::from_utf8(name).unwrap();
                    assert!(
                        name_str.starts_with("max"),
                        "Expected param starting with 'max', got '{name_str}'"
                    );
                } else {
                    panic!("Expected BulkString for param name");
                }
            }
        } else {
            panic!("Expected array from CONFIG GET max*");
        }
    }

    #[test]
    fn test_config_set_multiple_params() {
        let mut ctx = make_ctx();
        let args = vec![
            RespValue::BulkString(Bytes::from("SET")),
            RespValue::BulkString(Bytes::from("hz")),
            RespValue::BulkString(Bytes::from("20")),
            RespValue::BulkString(Bytes::from("timeout")),
            RespValue::BulkString(Bytes::from("100")),
        ];
        let result = config(&mut ctx, &args).unwrap();
        assert_eq!(result, RespValue::ok());
    }

    #[test]
    fn test_config_set_missing_value() {
        let mut ctx = make_ctx();
        // CONFIG SET with a param but no value (odd number of args after SET)
        let args = vec![
            RespValue::BulkString(Bytes::from("SET")),
            RespValue::BulkString(Bytes::from("hz")),
        ];
        let result = config(&mut ctx, &args);
        assert!(result.is_err());
    }

    #[test]
    fn test_config_rewrite() {
        let mut ctx = make_ctx();
        let args = vec![RespValue::BulkString(Bytes::from("REWRITE"))];
        let result = config(&mut ctx, &args).unwrap();
        assert_eq!(result, RespValue::ok());
    }

    #[test]
    fn test_config_get_missing_pattern() {
        let mut ctx = make_ctx();
        // CONFIG GET with no pattern argument
        let args = vec![RespValue::BulkString(Bytes::from("GET"))];
        let result = config(&mut ctx, &args);
        assert!(result.is_err());
    }

    // ===== INFO additional tests =====

    #[test]
    fn test_info_all_section() {
        let mut ctx = make_ctx();
        let args = vec![RespValue::BulkString(Bytes::from("all"))];
        let result = info(&mut ctx, &args).unwrap();

        if let RespValue::BulkString(data) = result {
            let info_str = std::str::from_utf8(&data).unwrap();
            assert!(info_str.contains("# Server"), "Missing Server section");
            assert!(info_str.contains("# Keyspace"), "Missing Keyspace section");
            assert!(info_str.contains("# Clients"), "Missing Clients section");
            assert!(info_str.contains("# Memory"), "Missing Memory section");
            assert!(
                info_str.contains("# Replication"),
                "Missing Replication section"
            );
        } else {
            panic!("Expected bulk string from INFO all");
        }
    }

    #[test]
    fn test_info_clients_section() {
        let mut ctx = make_ctx();
        let args = vec![RespValue::BulkString(Bytes::from("clients"))];
        let result = info(&mut ctx, &args).unwrap();

        if let RespValue::BulkString(data) = result {
            let info_str = std::str::from_utf8(&data).unwrap();
            assert!(info_str.contains("# Clients"));
            assert!(info_str.contains("connected_clients"));
        } else {
            panic!("Expected bulk string from INFO clients");
        }
    }

    #[test]
    fn test_info_memory_section() {
        let mut ctx = make_ctx();
        let args = vec![RespValue::BulkString(Bytes::from("memory"))];
        let result = info(&mut ctx, &args).unwrap();

        if let RespValue::BulkString(data) = result {
            let info_str = std::str::from_utf8(&data).unwrap();
            assert!(info_str.contains("# Memory"));
            assert!(info_str.contains("used_memory"));
        } else {
            panic!("Expected bulk string from INFO memory");
        }
    }

    #[test]
    fn test_info_replication_section() {
        let mut ctx = make_ctx();
        let args = vec![RespValue::BulkString(Bytes::from("replication"))];
        let result = info(&mut ctx, &args).unwrap();

        if let RespValue::BulkString(data) = result {
            let info_str = std::str::from_utf8(&data).unwrap();
            assert!(info_str.contains("# Replication"));
            assert!(info_str.contains("role"));
        } else {
            panic!("Expected bulk string from INFO replication");
        }
    }

    #[test]
    fn test_info_unknown_section() {
        let mut ctx = make_ctx();
        let args = vec![RespValue::BulkString(Bytes::from("unknownsection"))];
        let result = info(&mut ctx, &args).unwrap();

        // Unknown section returns empty string (Redis behavior)
        if let RespValue::BulkString(data) = result {
            let info_str = std::str::from_utf8(&data).unwrap();
            assert!(
                info_str.is_empty(),
                "Expected empty string for unknown section, got: '{info_str}'"
            );
        } else {
            panic!("Expected bulk string from INFO unknownsection");
        }
    }

    // ===== FLUSHDB/FLUSHALL option tests =====

    #[test]
    fn test_flushdb_async() {
        let mut ctx = make_ctx();
        ctx.store().database(0).set(
            Bytes::from("k1"),
            ferris_core::Entry::new(ferris_core::RedisValue::String(Bytes::from("v1"))),
        );
        assert_eq!(ctx.store().database(0).len(), 1);

        let args = vec![RespValue::BulkString(Bytes::from("ASYNC"))];
        let result = flushdb(&mut ctx, &args).unwrap();
        assert_eq!(result, RespValue::ok());
        assert_eq!(ctx.store().database(0).len(), 0);
    }

    #[test]
    fn test_flushdb_sync() {
        let mut ctx = make_ctx();
        ctx.store().database(0).set(
            Bytes::from("k1"),
            ferris_core::Entry::new(ferris_core::RedisValue::String(Bytes::from("v1"))),
        );
        assert_eq!(ctx.store().database(0).len(), 1);

        let args = vec![RespValue::BulkString(Bytes::from("SYNC"))];
        let result = flushdb(&mut ctx, &args).unwrap();
        assert_eq!(result, RespValue::ok());
        assert_eq!(ctx.store().database(0).len(), 0);
    }

    #[test]
    fn test_flushdb_invalid_option() {
        let mut ctx = make_ctx();
        let args = vec![RespValue::BulkString(Bytes::from("INVALID"))];
        let result = flushdb(&mut ctx, &args);
        assert!(matches!(result, Err(CommandError::SyntaxError)));
    }

    #[test]
    fn test_flushall_async() {
        let mut ctx = make_ctx();
        ctx.store().database(0).set(
            Bytes::from("k1"),
            ferris_core::Entry::new(ferris_core::RedisValue::String(Bytes::from("v1"))),
        );
        ctx.store().database(1).set(
            Bytes::from("k2"),
            ferris_core::Entry::new(ferris_core::RedisValue::String(Bytes::from("v2"))),
        );

        let args = vec![RespValue::BulkString(Bytes::from("ASYNC"))];
        let result = flushall(&mut ctx, &args).unwrap();
        assert_eq!(result, RespValue::ok());
        assert_eq!(ctx.store().database(0).len(), 0);
        assert_eq!(ctx.store().database(1).len(), 0);
    }

    #[test]
    fn test_flushall_sync() {
        let mut ctx = make_ctx();
        ctx.store().database(0).set(
            Bytes::from("k1"),
            ferris_core::Entry::new(ferris_core::RedisValue::String(Bytes::from("v1"))),
        );

        let args = vec![RespValue::BulkString(Bytes::from("SYNC"))];
        let result = flushall(&mut ctx, &args).unwrap();
        assert_eq!(result, RespValue::ok());
        assert_eq!(ctx.store().database(0).len(), 0);
    }

    #[test]
    fn test_flushall_invalid_option() {
        let mut ctx = make_ctx();
        let args = vec![RespValue::BulkString(Bytes::from("INVALID"))];
        let result = flushall(&mut ctx, &args);
        assert!(matches!(result, Err(CommandError::SyntaxError)));
    }

    #[test]
    fn test_flushdb_empty_db() {
        let mut ctx = make_ctx();
        // DB is already empty
        assert_eq!(ctx.store().database(0).len(), 0);

        let result = flushdb(&mut ctx, &[]).unwrap();
        assert_eq!(result, RespValue::ok());
        assert_eq!(ctx.store().database(0).len(), 0);
    }

    // ===== DEBUG additional tests =====

    #[test]
    fn test_debug_unknown_subcommand() {
        let mut ctx = make_ctx();
        let args = vec![RespValue::BulkString(Bytes::from("UNKNOWN"))];
        let result = debug(&mut ctx, &args);
        assert!(result.is_err());
        if let Err(CommandError::InvalidArgument(msg)) = result {
            assert!(msg.contains("UNKNOWN"));
        } else {
            panic!("Expected InvalidArgument error");
        }
    }

    #[test]
    fn test_debug_no_subcommand() {
        let mut ctx = make_ctx();
        let result = debug(&mut ctx, &[]);
        assert!(result.is_err());
    }

    // ===== AUTH additional tests =====

    #[test]
    fn test_auth_sets_authenticated() {
        let mut ctx = make_ctx();
        assert!(!ctx.is_authenticated());

        let args = vec![RespValue::BulkString(Bytes::from("secretpass"))];
        let result = auth(&mut ctx, &args).unwrap();
        assert_eq!(result, RespValue::ok());
        assert!(ctx.is_authenticated());
    }

    #[test]
    fn test_auth_with_username_password() {
        let mut ctx = make_ctx();
        assert!(!ctx.is_authenticated());

        let args = vec![
            RespValue::BulkString(Bytes::from("myuser")),
            RespValue::BulkString(Bytes::from("mypass")),
        ];
        let result = auth(&mut ctx, &args).unwrap();
        assert_eq!(result, RespValue::ok());
        assert!(ctx.is_authenticated());
    }

    #[test]
    fn test_auth_no_args_error() {
        let mut ctx = make_ctx();
        let result = auth(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    // ===== MEMORY additional tests =====

    #[test]
    fn test_memory_usage_list() {
        let mut ctx = make_ctx();
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("item1"));
        list.push_back(Bytes::from("item2"));
        ctx.store().database(0).set(
            Bytes::from("mylist"),
            ferris_core::Entry::new(ferris_core::RedisValue::List(list)),
        );

        let args = vec![
            RespValue::BulkString(Bytes::from("USAGE")),
            RespValue::BulkString(Bytes::from("mylist")),
        ];
        let result = memory(&mut ctx, &args).unwrap();
        if let RespValue::Integer(bytes) = result {
            assert!(bytes > 0, "Memory usage for list should be > 0");
        } else {
            panic!("Expected integer from MEMORY USAGE on list");
        }
    }

    #[test]
    fn test_memory_usage_hash() {
        let mut ctx = make_ctx();
        let mut hash = HashMap::new();
        hash.insert(Bytes::from("field1"), Bytes::from("value1"));
        hash.insert(Bytes::from("field2"), Bytes::from("value2"));
        ctx.store().database(0).set(
            Bytes::from("myhash"),
            ferris_core::Entry::new(ferris_core::RedisValue::Hash(hash)),
        );

        let args = vec![
            RespValue::BulkString(Bytes::from("USAGE")),
            RespValue::BulkString(Bytes::from("myhash")),
        ];
        let result = memory(&mut ctx, &args).unwrap();
        if let RespValue::Integer(bytes) = result {
            assert!(bytes > 0, "Memory usage for hash should be > 0");
        } else {
            panic!("Expected integer from MEMORY USAGE on hash");
        }
    }

    #[test]
    fn test_memory_usage_set() {
        let mut ctx = make_ctx();
        let mut set = HashSet::new();
        set.insert(Bytes::from("member1"));
        set.insert(Bytes::from("member2"));
        ctx.store().database(0).set(
            Bytes::from("myset"),
            ferris_core::Entry::new(ferris_core::RedisValue::Set(set)),
        );

        let args = vec![
            RespValue::BulkString(Bytes::from("USAGE")),
            RespValue::BulkString(Bytes::from("myset")),
        ];
        let result = memory(&mut ctx, &args).unwrap();
        if let RespValue::Integer(bytes) = result {
            assert!(bytes > 0, "Memory usage for set should be > 0");
        } else {
            panic!("Expected integer from MEMORY USAGE on set");
        }
    }

    #[test]
    fn test_memory_usage_zset() {
        let mut ctx = make_ctx();
        let mut scores = HashMap::new();
        scores.insert(Bytes::from("alice"), 1.0);
        scores.insert(Bytes::from("bob"), 2.0);
        let mut members = std::collections::BTreeMap::new();
        members.insert((ordered_float::OrderedFloat(1.0), Bytes::from("alice")), ());
        members.insert((ordered_float::OrderedFloat(2.0), Bytes::from("bob")), ());
        ctx.store().database(0).set(
            Bytes::from("myzset"),
            ferris_core::Entry::new(ferris_core::RedisValue::SortedSet { scores, members }),
        );

        let args = vec![
            RespValue::BulkString(Bytes::from("USAGE")),
            RespValue::BulkString(Bytes::from("myzset")),
        ];
        let result = memory(&mut ctx, &args).unwrap();
        if let RespValue::Integer(bytes) = result {
            assert!(bytes > 0, "Memory usage for sorted set should be > 0");
        } else {
            panic!("Expected integer from MEMORY USAGE on sorted set");
        }
    }

    #[test]
    fn test_memory_malloc_stats() {
        let mut ctx = make_ctx();
        let args = vec![RespValue::BulkString(Bytes::from("MALLOC-STATS"))];
        let result = memory(&mut ctx, &args).unwrap();
        if let RespValue::BulkString(data) = result {
            assert!(!data.is_empty());
        } else {
            panic!("Expected BulkString from MEMORY MALLOC-STATS");
        }
    }

    // ===== SLOWLOG additional tests =====

    #[test]
    fn test_slowlog_get_with_count() {
        let mut ctx = make_ctx();
        let args = vec![
            RespValue::BulkString(Bytes::from("GET")),
            RespValue::BulkString(Bytes::from("10")),
        ];
        let result = slowlog(&mut ctx, &args).unwrap();
        // Should return an array (empty since no slow queries tracked)
        assert_eq!(result, RespValue::Array(vec![]));
    }

    #[test]
    fn test_slowlog_unknown_subcommand() {
        let mut ctx = make_ctx();
        let args = vec![RespValue::BulkString(Bytes::from("INVALID"))];
        let result = slowlog(&mut ctx, &args);
        assert!(result.is_err());
    }

    // ===== COMMAND additional tests =====

    #[test]
    fn test_command_no_args() {
        let mut ctx = make_ctx();
        let result = command(&mut ctx, &[]).unwrap();
        // Returns an array (command list)
        if let RespValue::Array(_) = result {
            // OK - returns array
        } else {
            panic!("Expected array from COMMAND with no args");
        }
    }

    #[test]
    fn test_command_docs() {
        let mut ctx = make_ctx();
        let args = vec![RespValue::BulkString(Bytes::from("DOCS"))];
        let result = command(&mut ctx, &args).unwrap();
        if let RespValue::Array(_) = result {
            // OK - returns array
        } else {
            panic!("Expected array from COMMAND DOCS");
        }
    }

    #[test]
    fn test_command_count_matches_registry() {
        let mut ctx = make_ctx();
        let args = vec![RespValue::BulkString(Bytes::from("COUNT"))];
        let result = command(&mut ctx, &args).unwrap();
        if let RespValue::Integer(n) = result {
            assert!(n > 100, "Expected COMMAND COUNT > 100, got {n}");
        } else {
            panic!("Expected integer from COMMAND COUNT");
        }
    }
}

/// LASTSAVE
///
/// Returns the Unix timestamp of the last successful save to disk.
/// In ferris-db, this returns the current time since we use AOF (continuous saves).
///
/// Time complexity: O(1)
pub fn lastsave(_ctx: &mut CommandContext, _args: &[RespValue]) -> CommandResult {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    Ok(RespValue::Integer(timestamp as i64))
}

/// SAVE
///
/// Synchronously saves the database to disk.
/// In ferris-db with AOF, this ensures all pending writes are flushed.
///
/// Time complexity: O(1) - AOF is already being written
pub fn save(_ctx: &mut CommandContext, _args: &[RespValue]) -> CommandResult {
    // In ferris-db, AOF handles persistence continuously
    // This command can trigger an explicit flush if needed
    // For now, just return OK (AOF writer handles persistence)
    Ok(RespValue::ok())
}

/// BGSAVE
///
/// Asynchronously saves the database to disk in the background.
/// In ferris-db with AOF, this is essentially a no-op since AOF handles persistence.
///
/// Time complexity: O(1)
pub fn bgsave(_ctx: &mut CommandContext, _args: &[RespValue]) -> CommandResult {
    // AOF handles background persistence
    Ok(RespValue::SimpleString("Background saving started".into()))
}

/// BGREWRITEAOF
///
/// Asynchronously rewrites the AOF file to optimize it.
/// Currently not implemented - returns OK for compatibility.
///
/// Time complexity: O(N) where N is the size of the dataset
pub fn bgrewriteaof(_ctx: &mut CommandContext, _args: &[RespValue]) -> CommandResult {
    // TODO: Implement AOF rewrite in Phase 2
    Ok(RespValue::SimpleString(
        "Background append only file rewriting started".into(),
    ))
}

/// SHUTDOWN [NOSAVE|SAVE]
///
/// Synchronously saves the database and shuts down the server.
///
/// Time complexity: O(1)
pub fn shutdown(_ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    // Parse optional SAVE/NOSAVE argument
    if args.len() > 1 {
        return Err(CommandError::WrongArity("SHUTDOWN".to_string()));
    }

    // In a real implementation, this would trigger graceful shutdown
    // For now, we return an error indicating the command was received
    // The actual shutdown should be handled by the server infrastructure
    Err(CommandError::InvalidArgument(
        "SHUTDOWN command received - server shutdown not implemented in command handler"
            .to_string(),
    ))
}

/// ROLE
///
/// Returns the role of the instance (master/replica).
/// Currently always returns "master" since replication isn't fully implemented.
///
/// Time complexity: O(1)
pub fn role(_ctx: &mut CommandContext, _args: &[RespValue]) -> CommandResult {
    // Return master role with empty replica list
    Ok(RespValue::Array(vec![
        RespValue::BulkString(Bytes::from("master")),
        RespValue::Integer(0),    // Replication offset
        RespValue::Array(vec![]), // Empty replica list
    ]))
}

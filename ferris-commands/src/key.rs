//! Key commands: DEL, EXISTS, EXPIRE, TTL, TYPE, KEYS, SCAN, RENAME, etc.

use crate::{CommandContext, CommandError, CommandResult};
use bytes::Bytes;
use ferris_core::{Entry, RedisValue};
use ferris_protocol::RespValue;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Helper to parse an integer from RespValue
fn parse_int(arg: &RespValue) -> Result<i64, CommandError> {
    let s = arg.as_str().ok_or(CommandError::NotAnInteger)?;
    s.parse().map_err(|_| CommandError::NotAnInteger)
}

/// Helper to get bytes from RespValue
fn get_bytes(arg: &RespValue) -> Result<Bytes, CommandError> {
    arg.as_bytes()
        .cloned()
        .or_else(|| arg.as_str().map(|s| Bytes::from(s.to_owned())))
        .ok_or_else(|| CommandError::InvalidArgument("invalid argument".to_string()))
}

/// Condition flags for EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT (Redis 7.0+)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExpireCondition {
    /// No condition — always set the expiry
    None,
    /// NX — Set expiry only when the key has no expiry
    Nx,
    /// XX — Set expiry only when the key has an existing expiry
    Xx,
    /// GT — Set expiry only when the new expiry is greater than current
    Gt,
    /// LT — Set expiry only when the new expiry is less than current
    Lt,
}

/// Parse the optional expire condition flags from command arguments starting at `start_idx`.
///
/// Supports NX, XX, GT, LT (case-insensitive). Validates mutual exclusivity:
/// - NX and XX are mutually exclusive
/// - NX and GT are mutually exclusive
/// - NX and LT are mutually exclusive
/// - GT and LT are mutually exclusive
/// - XX can combine with GT or LT (but that's handled by the caller via separate flags)
///
/// Since Redis only allows a single flag token (NX, XX, GT, or LT) for these commands,
/// we parse exactly one optional argument.
fn parse_expire_condition(
    args: &[RespValue],
    start_idx: usize,
) -> Result<ExpireCondition, CommandError> {
    let arg = match args.get(start_idx) {
        Some(a) => a,
        scala => {
            // No more arguments — no condition
            let _ = scala;
            return Ok(ExpireCondition::None);
        }
    };

    let flag = arg
        .as_str()
        .ok_or(CommandError::SyntaxError)?
        .to_uppercase();

    match flag.as_str() {
        "NX" => Ok(ExpireCondition::Nx),
        "XX" => Ok(ExpireCondition::Xx),
        "GT" => Ok(ExpireCondition::Gt),
        "LT" => Ok(ExpireCondition::Lt),
        _ => Err(CommandError::SyntaxError),
    }
}

/// DEL key [key ...]
///
/// Removes the specified keys. A key is ignored if it does not exist.
///
/// Time complexity: O(N) where N is the number of keys
pub fn del(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("DEL".to_string()));
    }

    // Validate all keys are in the same slot (cluster mode)
    let keys: Vec<&[u8]> = args
        .iter()
        .filter_map(|arg| arg.as_bytes().map(|b| b.as_ref()))
        .collect();
    crate::cluster::validate_same_slot(ctx, &keys)?;

    let db = ctx.store().database(ctx.selected_db());
    let mut deleted = 0i64;

    for arg in args {
        let key = get_bytes(arg)?;
        if db.delete(&key) {
            deleted += 1;
        }
    }

    if deleted > 0 {
        // Propagate to AOF and replication
        ctx.propagate_args(args);
    }

    Ok(RespValue::Integer(deleted))
}

/// UNLINK key [key ...]
///
/// Like DEL but removes keys in a non-blocking way (same behavior here).
///
/// Time complexity: O(1) for each key removed
pub fn unlink(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    // In our implementation, UNLINK behaves the same as DEL
    del(ctx, args)
}

/// EXISTS key [key ...]
///
/// Returns if key exists. Returns the count of existing keys.
///
/// Time complexity: O(N) where N is the number of keys
pub fn exists(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("EXISTS".to_string()));
    }

    // Validate all keys are in the same slot (cluster mode)
    let keys: Vec<&[u8]> = args
        .iter()
        .filter_map(|arg| arg.as_bytes().map(|b| b.as_ref()))
        .collect();
    crate::cluster::validate_same_slot(ctx, &keys)?;

    let db = ctx.store().database(ctx.selected_db());
    let mut count = 0i64;

    for arg in args {
        let key = get_bytes(arg)?;
        if let Some(entry) = db.get(&key) {
            if entry.is_expired() {
                db.delete(&key);
            } else {
                count += 1;
            }
        }
    }

    Ok(RespValue::Integer(count))
}

/// EXPIRE key seconds [NX | XX | GT | LT]
///
/// Set a timeout on key. After the timeout has expired, the key will be deleted.
/// Optional flags (Redis 7.0+): NX, XX, GT, LT.
///
/// Time complexity: O(1)
pub fn expire(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("EXPIRE".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let seconds = parse_int(&args[1])?;
    let condition = parse_expire_condition(args, 2)?;

    let result = set_ttl_impl(
        ctx,
        &key,
        Duration::from_secs(seconds.max(0) as u64),
        condition,
    )?;

    if result == RespValue::Integer(1) {
        // Propagate to AOF and replication
        ctx.propagate_args(args);
    }

    Ok(result)
}

/// PEXPIRE key milliseconds [NX | XX | GT | LT]
///
/// Set a timeout on key in milliseconds.
/// Optional flags (Redis 7.0+): NX, XX, GT, LT.
///
/// Time complexity: O(1)
pub fn pexpire(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("PEXPIRE".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let ms = parse_int(&args[1])?;
    let condition = parse_expire_condition(args, 2)?;

    let result = set_ttl_impl(
        ctx,
        &key,
        Duration::from_millis(ms.max(0) as u64),
        condition,
    )?;

    if result == RespValue::Integer(1) {
        // Propagate to AOF and replication
        ctx.propagate_args(args);
    }

    Ok(result)
}

/// EXPIREAT key timestamp [NX | XX | GT | LT]
///
/// Set an absolute Unix timestamp (seconds since epoch) when the key will expire.
/// Optional flags (Redis 7.0+): NX, XX, GT, LT.
///
/// Time complexity: O(1)
pub fn expireat(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("EXPIREAT".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let timestamp = parse_int(&args[1])?;
    let condition = parse_expire_condition(args, 2)?;

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    let ttl_secs = (timestamp - now).max(0);
    let result = set_ttl_impl(ctx, &key, Duration::from_secs(ttl_secs as u64), condition)?;

    if result == RespValue::Integer(1) {
        // Propagate to AOF and replication
        ctx.propagate_args(args);
    }

    Ok(result)
}

/// PEXPIREAT key timestamp [NX | XX | GT | LT]
///
/// Set an absolute Unix timestamp (milliseconds since epoch) when the key will expire.
/// Optional flags (Redis 7.0+): NX, XX, GT, LT.
///
/// Time complexity: O(1)
pub fn pexpireat(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("PEXPIREAT".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let timestamp = parse_int(&args[1])?;
    let condition = parse_expire_condition(args, 2)?;

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;

    let ttl_ms = (timestamp - now).max(0);
    let result = set_ttl_impl(ctx, &key, Duration::from_millis(ttl_ms as u64), condition)?;

    if result == RespValue::Integer(1) {
        // Propagate to AOF and replication
        ctx.propagate_args(args);
    }

    Ok(result)
}

fn set_ttl_impl(
    ctx: &mut CommandContext,
    key: &Bytes,
    ttl: Duration,
    condition: ExpireCondition,
) -> CommandResult {
    let db = ctx.store().database(ctx.selected_db());

    match db.get(key) {
        Some(mut entry) => {
            if entry.is_expired() {
                db.delete(key);
                return Ok(RespValue::Integer(0));
            }

            let new_expires_at = std::time::Instant::now() + ttl;

            match condition {
                ExpireCondition::None => {
                    // No condition — always set
                }
                ExpireCondition::Nx => {
                    // Only set if key has NO existing expiry
                    if entry.expires_at.is_some() {
                        return Ok(RespValue::Integer(0));
                    }
                }
                ExpireCondition::Xx => {
                    // Only set if key HAS an existing expiry
                    if entry.expires_at.is_none() {
                        return Ok(RespValue::Integer(0));
                    }
                }
                ExpireCondition::Gt => {
                    // Only set if new expiry is GREATER than current
                    if let Some(current_exp) = entry.expires_at {
                        if new_expires_at <= current_exp {
                            return Ok(RespValue::Integer(0));
                        }
                    }
                    // If no current expiry, GT always sets (matching Redis behavior)
                }
                ExpireCondition::Lt => {
                    // Only set if new expiry is LESS than current
                    if let Some(current_exp) = entry.expires_at {
                        if new_expires_at >= current_exp {
                            return Ok(RespValue::Integer(0));
                        }
                    }
                    // If no current expiry, LT always sets (matching Redis behavior)
                }
            }

            entry.set_ttl(ttl);
            db.set(key.clone(), entry);
            Ok(RespValue::Integer(1))
        }
        None => Ok(RespValue::Integer(0)),
    }
}

/// TTL key
///
/// Returns the remaining time to live of a key that has a timeout.
///
/// Time complexity: O(1)
pub fn ttl(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    let key = get_bytes(
        args.first()
            .ok_or_else(|| CommandError::WrongArity("TTL".to_string()))?,
    )?;

    let db = ctx.store().database(ctx.selected_db());

    match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                return Ok(RespValue::Integer(-2)); // Key does not exist
            }
            match entry.ttl_millis() {
                Some(ms) => Ok(RespValue::Integer((ms / 1000) as i64)),
                None => Ok(RespValue::Integer(-1)), // No TTL
            }
        }
        None => Ok(RespValue::Integer(-2)), // Key does not exist
    }
}

/// PTTL key
///
/// Returns the remaining time to live of a key in milliseconds.
///
/// Time complexity: O(1)
pub fn pttl(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    let key = get_bytes(
        args.first()
            .ok_or_else(|| CommandError::WrongArity("PTTL".to_string()))?,
    )?;

    let db = ctx.store().database(ctx.selected_db());

    match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                return Ok(RespValue::Integer(-2));
            }
            match entry.ttl_millis() {
                Some(ms) => Ok(RespValue::Integer(ms as i64)),
                None => Ok(RespValue::Integer(-1)),
            }
        }
        None => Ok(RespValue::Integer(-2)),
    }
}

/// PERSIST key
///
/// Remove the existing timeout on key.
///
/// Time complexity: O(1)
pub fn persist(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    let key = get_bytes(
        args.first()
            .ok_or_else(|| CommandError::WrongArity("PERSIST".to_string()))?,
    )?;

    let db = ctx.store().database(ctx.selected_db());

    match db.get(&key) {
        Some(mut entry) => {
            if entry.is_expired() {
                db.delete(&key);
                return Ok(RespValue::Integer(0));
            }
            if entry.expires_at.is_some() {
                entry.persist();
                db.set(key.clone(), entry);

                // Propagate to AOF and replication
                ctx.propagate_args(args);

                Ok(RespValue::Integer(1))
            } else {
                Ok(RespValue::Integer(0))
            }
        }
        None => Ok(RespValue::Integer(0)),
    }
}

/// TYPE key
///
/// Returns the string representation of the type of the value stored at key.
///
/// Time complexity: O(1)
pub fn type_cmd(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    let key = get_bytes(
        args.first()
            .ok_or_else(|| CommandError::WrongArity("TYPE".to_string()))?,
    )?;

    let db = ctx.store().database(ctx.selected_db());

    match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                return Ok(RespValue::SimpleString("none".to_string()));
            }
            Ok(RespValue::SimpleString(entry.value.type_name().to_string()))
        }
        None => Ok(RespValue::SimpleString("none".to_string())),
    }
}

/// KEYS pattern
///
/// Returns all keys matching pattern.
///
/// Time complexity: O(N) with N being the number of keys in the database
pub fn keys(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    let pattern = args
        .first()
        .and_then(|v| v.as_str())
        .ok_or_else(|| CommandError::WrongArity("KEYS".to_string()))?;

    let db = ctx.store().database(ctx.selected_db());
    let matched_keys = db.keys(pattern);

    let results: Vec<RespValue> = matched_keys
        .into_iter()
        .map(RespValue::BulkString)
        .collect();

    Ok(RespValue::Array(results))
}

/// SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]
///
/// Incrementally iterate the keys space.
///
/// Time complexity: O(1) for every call, O(N) for full iteration
pub fn scan(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("SCAN".to_string()));
    }

    let cursor = parse_int(&args[0])? as usize;
    let mut pattern = "*";
    let mut count = 10usize;
    let mut type_filter: Option<&str> = None;

    // Parse options
    let mut i = 1;
    while i < args.len() {
        let opt = args[i].as_str().map(|s| s.to_uppercase());
        match opt.as_deref() {
            Some("MATCH") => {
                i += 1;
                pattern = args.get(i).and_then(|v| v.as_str()).unwrap_or("*");
            }
            Some("COUNT") => {
                i += 1;
                count = args
                    .get(i)
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(10);
            }
            Some("TYPE") => {
                i += 1;
                type_filter = args.get(i).and_then(|v| v.as_str());
            }
            _ => {}
        }
        i += 1;
    }

    let db = ctx.store().database(ctx.selected_db());
    let all_keys = db.keys(pattern);

    // Simple cursor implementation: cursor is the starting index
    let total = all_keys.len();
    let start = cursor.min(total);
    let end = (start + count).min(total);

    let next_cursor = if end >= total { 0 } else { end };

    let selected_keys: Vec<Bytes> = all_keys
        .into_iter()
        .skip(start)
        .take(count)
        .filter(|key| {
            if let Some(type_name) = type_filter {
                if let Some(entry) = db.get(key) {
                    if entry.is_expired() {
                        return false;
                    }
                    return entry.value.type_name() == type_name;
                }
                false
            } else {
                true
            }
        })
        .collect();

    let keys_array: Vec<RespValue> = selected_keys
        .into_iter()
        .map(RespValue::BulkString)
        .collect();

    Ok(RespValue::Array(vec![
        RespValue::BulkString(Bytes::from(next_cursor.to_string())),
        RespValue::Array(keys_array),
    ]))
}

/// RENAME key newkey
///
/// Renames key to newkey.
///
/// Time complexity: O(1)
pub fn rename(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("RENAME".to_string()));
    }

    let old_key = get_bytes(&args[0])?;
    let new_key = get_bytes(&args[1])?;

    let db = ctx.store().database(ctx.selected_db());

    match db.get(&old_key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&old_key);
                return Err(CommandError::NoSuchKey);
            }
            db.delete(&old_key);
            db.set(new_key, entry);

            // Propagate to AOF and replication
            ctx.propagate_args(args);

            Ok(RespValue::ok())
        }
        None => Err(CommandError::NoSuchKey),
    }
}

/// RENAMENX key newkey
///
/// Renames key to newkey if newkey does not yet exist.
///
/// Time complexity: O(1)
pub fn renamenx(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("RENAMENX".to_string()));
    }

    let old_key = get_bytes(&args[0])?;
    let new_key = get_bytes(&args[1])?;

    let db = ctx.store().database(ctx.selected_db());

    // Check if new key exists
    if let Some(entry) = db.get(&new_key) {
        if !entry.is_expired() {
            return Ok(RespValue::Integer(0));
        }
        db.delete(&new_key);
    }

    match db.get(&old_key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&old_key);
                return Err(CommandError::NoSuchKey);
            }
            db.delete(&old_key);
            db.set(new_key, entry);

            // Propagate to AOF and replication
            ctx.propagate_args(args);

            Ok(RespValue::Integer(1))
        }
        None => Err(CommandError::NoSuchKey),
    }
}

/// COPY source destination [DB destination-db] [REPLACE]
///
/// Copy the value stored at the source key to the destination key.
///
/// Time complexity: O(N)
pub fn copy(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("COPY".to_string()));
    }

    let src_key = get_bytes(&args[0])?;
    let dst_key = get_bytes(&args[1])?;

    let mut dst_db_idx = ctx.selected_db();
    let mut replace = false;

    // Parse options
    let mut i = 2;
    while i < args.len() {
        let opt = args[i].as_str().map(|s| s.to_uppercase());
        match opt.as_deref() {
            Some("DB") => {
                i += 1;
                dst_db_idx = args
                    .get(i)
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse().ok())
                    .ok_or(CommandError::InvalidDbIndex)?;
            }
            Some("REPLACE") => {
                replace = true;
            }
            _ => {}
        }
        i += 1;
    }

    if dst_db_idx >= ctx.store().num_databases() {
        return Err(CommandError::InvalidDbIndex);
    }

    let src_db = ctx.store().database(ctx.selected_db());
    let dst_db = ctx.store().database(dst_db_idx);

    // Get source entry
    let src_entry = match src_db.get(&src_key) {
        Some(entry) => {
            if entry.is_expired() {
                src_db.delete(&src_key);
                return Ok(RespValue::Integer(0));
            }
            entry
        }
        None => return Ok(RespValue::Integer(0)),
    };

    // Check if destination exists
    if let Some(dst_entry) = dst_db.get(&dst_key) {
        if !dst_entry.is_expired() && !replace {
            return Ok(RespValue::Integer(0));
        }
    }

    // Copy the entry
    dst_db.set(dst_key, src_entry);
    Ok(RespValue::Integer(1))
}

/// TOUCH key [key ...]
///
/// Alters the last access time of a key(s).
///
/// Time complexity: O(N) where N is the number of keys
pub fn touch(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("TOUCH".to_string()));
    }

    // Validate all keys are in the same slot (cluster mode)
    let keys: Vec<&[u8]> = args
        .iter()
        .filter_map(|arg| arg.as_bytes().map(|b| b.as_ref()))
        .collect();
    crate::cluster::validate_same_slot(ctx, &keys)?;

    let db = ctx.store().database(ctx.selected_db());
    let mut count = 0i64;

    for arg in args {
        let key = get_bytes(arg)?;
        if let Some(mut entry) = db.get(&key) {
            if !entry.is_expired() {
                entry.touch();
                db.set(key, entry);
                count += 1;
            }
        }
    }

    Ok(RespValue::Integer(count))
}

/// OBJECT subcommand [arguments]
///
/// Inspect the internals of Redis Objects.
///
/// Time complexity: O(1)
pub fn object(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    let subcommand = args
        .first()
        .and_then(|v| v.as_str())
        .map(|s| s.to_uppercase())
        .ok_or_else(|| CommandError::WrongArity("OBJECT".to_string()))?;

    match subcommand.as_str() {
        "ENCODING" => {
            let key = get_bytes(args.get(1).ok_or(CommandError::SyntaxError)?)?;
            let db = ctx.store().database(ctx.selected_db());

            match db.get(&key) {
                Some(entry) => {
                    if entry.is_expired() {
                        db.delete(&key);
                        return Ok(RespValue::Null);
                    }
                    let encoding = match &entry.value {
                        RedisValue::String(s) => {
                            // Check if it's an integer encoding
                            if std::str::from_utf8(s).map_or(false, |s| s.parse::<i64>().is_ok()) {
                                "int"
                            } else if s.len() <= 44 {
                                "embstr"
                            } else {
                                "raw"
                            }
                        }
                        RedisValue::List(l) => {
                            if l.len() <= 128 {
                                "listpack"
                            } else {
                                "quicklist"
                            }
                        }
                        RedisValue::Set(s) => {
                            if s.len() <= 128 {
                                "listpack"
                            } else {
                                "hashtable"
                            }
                        }
                        RedisValue::Hash(h) => {
                            if h.len() <= 128 {
                                "listpack"
                            } else {
                                "hashtable"
                            }
                        }
                        RedisValue::SortedSet { scores, .. } => {
                            if scores.len() <= 128 {
                                "listpack"
                            } else {
                                "skiplist"
                            }
                        }
                        RedisValue::Stream(s) => {
                            if s.len() <= 128 {
                                "listpack"
                            } else {
                                "radix-tree"
                            }
                        }
                    };
                    Ok(RespValue::BulkString(Bytes::from(encoding)))
                }
                None => Ok(RespValue::Null),
            }
        }
        "REFCOUNT" => {
            // Always return 1 for simplicity
            Ok(RespValue::Integer(1))
        }
        "IDLETIME" => {
            let key = get_bytes(args.get(1).ok_or(CommandError::SyntaxError)?)?;
            let db = ctx.store().database(ctx.selected_db());

            match db.get(&key) {
                Some(entry) => {
                    if entry.is_expired() {
                        db.delete(&key);
                        return Ok(RespValue::Null);
                    }
                    let idle = entry.last_access.elapsed().as_secs();
                    Ok(RespValue::Integer(idle as i64))
                }
                None => Ok(RespValue::Null),
            }
        }
        "FREQ" => {
            let key = get_bytes(args.get(1).ok_or(CommandError::SyntaxError)?)?;
            let db = ctx.store().database(ctx.selected_db());

            match db.get(&key) {
                Some(entry) => {
                    if entry.is_expired() {
                        db.delete(&key);
                        return Ok(RespValue::Null);
                    }
                    Ok(RespValue::Integer(i64::from(entry.lfu_counter)))
                }
                None => Ok(RespValue::Null),
            }
        }
        "HELP" => {
            let help = vec![
                RespValue::BulkString(Bytes::from("OBJECT ENCODING <key>")),
                RespValue::BulkString(Bytes::from("OBJECT REFCOUNT <key>")),
                RespValue::BulkString(Bytes::from("OBJECT IDLETIME <key>")),
                RespValue::BulkString(Bytes::from("OBJECT FREQ <key>")),
            ];
            Ok(RespValue::Array(help))
        }
        _ => Err(CommandError::InvalidArgument(format!(
            "Unknown OBJECT subcommand '{subcommand}'"
        ))),
    }
}

/// DUMP key
///
/// Serialize the value stored at key (simplified - returns type + value).
///
/// Time complexity: O(1) to access the key and additional O(N*M) for serialization
pub fn dump(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    let key = get_bytes(
        args.first()
            .ok_or_else(|| CommandError::WrongArity("DUMP".to_string()))?,
    )?;

    let db = ctx.store().database(ctx.selected_db());

    match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                return Ok(RespValue::Null);
            }
            // Simplified dump - just return type info
            let type_byte = match &entry.value {
                RedisValue::String(_) => 0u8,
                RedisValue::List(_) => 1u8,
                RedisValue::Set(_) => 2u8,
                RedisValue::Hash(_) => 4u8,
                RedisValue::SortedSet { .. } => 3u8,
                RedisValue::Stream(_) => 15u8,
            };
            // Very simplified serialization
            Ok(RespValue::BulkString(Bytes::from(vec![type_byte])))
        }
        None => Ok(RespValue::Null),
    }
}

/// RESTORE key ttl serialized-value [REPLACE] [ABSTTL] [IDLETIME seconds] [FREQ frequency]
///
/// Create a key using the provided serialized value (simplified).
///
/// Time complexity: O(1)
pub fn restore(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongArity("RESTORE".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let ttl_ms = parse_int(&args[1])?;
    let _data = get_bytes(&args[2])?;

    let mut replace = false;
    let mut i = 3;
    while i < args.len() {
        if let Some(opt) = args[i].as_str() {
            if opt.to_uppercase() == "REPLACE" {
                replace = true;
            }
        }
        i += 1;
    }

    let db = ctx.store().database(ctx.selected_db());

    // Check if key exists
    if !replace {
        if let Some(entry) = db.get(&key) {
            if !entry.is_expired() {
                return Err(CommandError::InvalidArgument(
                    "BUSYKEY Target key name already exists".to_string(),
                ));
            }
        }
    }

    // Create a simple string entry (simplified restore)
    let mut entry = Entry::new(RedisValue::String(Bytes::from("restored")));
    if ttl_ms > 0 {
        entry.set_ttl(Duration::from_millis(ttl_ms as u64));
    }

    db.set(key, entry);
    Ok(RespValue::ok())
}

/// RANDOMKEY
///
/// Return a random key from the currently selected database.
///
/// Time complexity: O(1)
pub fn randomkey(ctx: &mut CommandContext, _args: &[RespValue]) -> CommandResult {
    let db = ctx.store().database(ctx.selected_db());
    let all_keys = db.keys("*");

    if all_keys.is_empty() {
        return Ok(RespValue::Null);
    }

    // Simple random selection using current time as seed
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as usize;
    let idx = now % all_keys.len();

    Ok(RespValue::BulkString(all_keys[idx].clone()))
}

/// EXPIRETIME key
///
/// Returns the absolute Unix timestamp (since January 1, 1970) in seconds at which the key will expire.
///
/// Time complexity: O(1)
pub fn expiretime(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    let key = get_bytes(
        args.first()
            .ok_or_else(|| CommandError::WrongArity("EXPIRETIME".to_string()))?,
    )?;

    let db = ctx.store().database(ctx.selected_db());

    match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                return Ok(RespValue::Integer(-2));
            }
            match entry.expires_at {
                Some(exp) => {
                    // Convert Instant to Unix timestamp (approximation)
                    let now = std::time::Instant::now();
                    let sys_now = SystemTime::now();
                    let duration_until_exp = exp.saturating_duration_since(now);
                    let exp_time = sys_now + duration_until_exp;
                    let timestamp = exp_time
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    Ok(RespValue::Integer(timestamp as i64))
                }
                None => Ok(RespValue::Integer(-1)),
            }
        }
        None => Ok(RespValue::Integer(-2)),
    }
}

/// PEXPIRETIME key
///
/// Returns the absolute Unix timestamp in milliseconds at which the key will expire.
///
/// Time complexity: O(1)
pub fn pexpiretime(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    let key = get_bytes(
        args.first()
            .ok_or_else(|| CommandError::WrongArity("PEXPIRETIME".to_string()))?,
    )?;

    let db = ctx.store().database(ctx.selected_db());

    match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                return Ok(RespValue::Integer(-2));
            }
            match entry.expires_at {
                Some(exp) => {
                    let now = std::time::Instant::now();
                    let sys_now = SystemTime::now();
                    let duration_until_exp = exp.saturating_duration_since(now);
                    let exp_time = sys_now + duration_until_exp;
                    let timestamp = exp_time
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis();
                    Ok(RespValue::Integer(timestamp as i64))
                }
                None => Ok(RespValue::Integer(-1)),
            }
        }
        None => Ok(RespValue::Integer(-2)),
    }
}

/// SORT key [BY pattern] [LIMIT offset count] [GET pattern [GET pattern ...]]
///     [ASC | DESC] [ALPHA] [STORE destination]
///
/// Sort the elements in a list, set or sorted set.
///
/// Time complexity: O(N+M*log(M)) where N is the number of elements and M is
/// the number of elements to return
pub fn sort(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("SORT".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let db = ctx.store().database(ctx.selected_db());

    // Get the collection to sort
    let elements = match db.get(&key) {
        Some(entry) if !entry.is_expired() => match &entry.value {
            RedisValue::List(list) => list.iter().cloned().collect::<Vec<_>>(),
            RedisValue::Set(set) => set.iter().cloned().collect::<Vec<_>>(),
            RedisValue::SortedSet { scores, .. } => scores.keys().cloned().collect::<Vec<_>>(),
            _ => return Err(CommandError::WrongType),
        },
        _ => vec![],
    };

    // Parse options
    let mut by_pattern: Option<String> = None;
    let mut get_patterns: Vec<String> = vec![];
    let mut limit: Option<(usize, usize)> = None;
    let mut alpha = false;
    let mut desc = false;
    let mut store_dest: Option<Bytes> = None;

    let mut i = 1;
    while i < args.len() {
        let opt = args[i]
            .as_str()
            .map(|s| s.to_uppercase())
            .unwrap_or_default();
        match opt.as_str() {
            "BY" => {
                i += 1;
                by_pattern = Some(
                    args.get(i)
                        .and_then(|v| v.as_str())
                        .ok_or(CommandError::SyntaxError)?
                        .to_string(),
                );
            }
            "LIMIT" => {
                i += 1;
                let offset = parse_int(args.get(i).ok_or(CommandError::SyntaxError)?)? as usize;
                i += 1;
                let count = parse_int(args.get(i).ok_or(CommandError::SyntaxError)?)? as usize;
                limit = Some((offset, count));
            }
            "GET" => {
                i += 1;
                get_patterns.push(
                    args.get(i)
                        .and_then(|v| v.as_str())
                        .ok_or(CommandError::SyntaxError)?
                        .to_string(),
                );
            }
            "ASC" => desc = false,
            "DESC" => desc = true,
            "ALPHA" => alpha = true,
            "STORE" => {
                i += 1;
                store_dest = Some(get_bytes(args.get(i).ok_or(CommandError::SyntaxError)?)?);
            }
            _ => return Err(CommandError::SyntaxError),
        }
        i += 1;
    }

    // Create sortable items
    let mut items: Vec<(Bytes, f64, String)> = elements
        .into_iter()
        .map(|elem| {
            let sort_key = if let Some(ref pattern) = by_pattern {
                // Replace * with element value in pattern
                let lookup_key = pattern.replace('*', &String::from_utf8_lossy(&elem));
                // Look up the value for sorting
                if let Some(entry) = db.get(lookup_key.as_bytes()) {
                    match &entry.value {
                        RedisValue::String(s) => String::from_utf8_lossy(s).to_string(),
                        _ => String::new(),
                    }
                } else {
                    String::new()
                }
            } else {
                String::from_utf8_lossy(&elem).to_string()
            };

            let score = if alpha {
                0.0 // Will sort by string
            } else {
                sort_key.parse::<f64>().unwrap_or(0.0)
            };

            (elem, score, sort_key)
        })
        .collect();

    // Sort
    if alpha {
        items.sort_by(|a, b| if desc { b.2.cmp(&a.2) } else { a.2.cmp(&b.2) });
    } else {
        items.sort_by(|a, b| {
            use ordered_float::OrderedFloat;
            if desc {
                OrderedFloat(b.1).cmp(&OrderedFloat(a.1))
            } else {
                OrderedFloat(a.1).cmp(&OrderedFloat(b.1))
            }
        });
    }

    // Apply LIMIT
    let items = if let Some((offset, count)) = limit {
        items
            .into_iter()
            .skip(offset)
            .take(count)
            .collect::<Vec<_>>()
    } else {
        items
    };

    // Build result
    let result = if get_patterns.is_empty() {
        // Return the sorted elements themselves
        items
            .into_iter()
            .map(|(elem, _, _)| RespValue::BulkString(elem))
            .collect::<Vec<_>>()
    } else {
        // Return values fetched by GET patterns
        let mut result = vec![];
        for (elem, _, _) in &items {
            for pattern in &get_patterns {
                if pattern == "#" {
                    // # means the element itself
                    result.push(RespValue::BulkString(elem.clone()));
                } else {
                    // Replace * with element value
                    let lookup_key = pattern.replace('*', &String::from_utf8_lossy(elem));
                    let value = if let Some(entry) = db.get(lookup_key.as_bytes()) {
                        match &entry.value {
                            RedisValue::String(s) => RespValue::BulkString(s.clone()),
                            _ => RespValue::Null,
                        }
                    } else {
                        RespValue::Null
                    };
                    result.push(value);
                }
            }
        }
        result
    };

    // STORE or return
    if let Some(dest) = store_dest {
        // Store result as a list
        let list: std::collections::VecDeque<Bytes> = result
            .into_iter()
            .filter_map(|v| {
                if let RespValue::BulkString(b) = v {
                    Some(b)
                } else {
                    None
                }
            })
            .collect();
        let len = list.len();
        db.set(dest, Entry::new(RedisValue::List(list)));
        Ok(RespValue::Integer(len as i64))
    } else {
        Ok(RespValue::Array(result))
    }
}

/// SORT_RO key [BY pattern] [LIMIT offset count] [GET pattern [GET pattern ...]]
///     [ASC | DESC] [ALPHA]
///
/// Read-only variant of SORT. Same as SORT but without STORE option.
///
/// Time complexity: O(N+M*log(M))
pub fn sort_ro(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    // Check for STORE option (not allowed in SORT_RO)
    for arg in args.iter().skip(1) {
        if let Some(s) = arg.as_str() {
            if s.to_uppercase() == "STORE" {
                return Err(CommandError::InvalidArgument(
                    "SORT_RO doesn't support the STORE option".to_string(),
                ));
            }
        }
    }

    // Call regular SORT (which won't store since we checked)
    sort(ctx, args)
}
/// MOVE key db
///
/// Moves a key from the currently selected database to the specified database.
/// Returns 1 if the key was moved, 0 otherwise.
///
/// Time complexity: O(1)
pub fn move_key(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("MOVE".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let target_db = parse_int(&args[1])?;

    if target_db < 0 || target_db >= 16 {
        return Err(CommandError::InvalidArgument(
            "invalid DB index".to_string(),
        ));
    }

    let source_db_idx = ctx.selected_db();
    let target_db_idx = target_db as usize;

    // Can't move to same database
    if source_db_idx == target_db_idx {
        return Ok(RespValue::Integer(0));
    }

    let store = ctx.store();
    let source_db = store.database(source_db_idx);

    // Get the entry from source
    let entry = match source_db.get(&key) {
        Some(e) => {
            if e.is_expired() {
                source_db.delete(&key);
                return Ok(RespValue::Integer(0));
            }
            e
        }
        None => return Ok(RespValue::Integer(0)),
    };

    // Check if key exists in target
    let target_db = store.database(target_db_idx);
    if let Some(target_entry) = target_db.get(&key) {
        if !target_entry.is_expired() {
            return Ok(RespValue::Integer(0));
        }
    }

    // Move the key
    target_db.set(key.clone(), entry);
    source_db.delete(&key);

    // Propagate to AOF and replication
    ctx.propagate_args(args);

    Ok(RespValue::Integer(1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferris_core::KeyStore;
    use ordered_float::OrderedFloat;
    use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
    use std::sync::Arc;

    fn make_ctx() -> CommandContext {
        CommandContext::new(Arc::new(KeyStore::default()))
    }

    fn bulk(s: &str) -> RespValue {
        RespValue::BulkString(Bytes::from(s.to_owned()))
    }

    fn set_key(ctx: &mut CommandContext, key: &str, value: &str) {
        let db = ctx.store().database(ctx.selected_db());
        db.set(
            Bytes::from(key.to_owned()),
            Entry::new(RedisValue::String(Bytes::from(value.to_owned()))),
        );
    }

    #[test]
    fn test_del_single() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key1", "value1");

        let result = del(&mut ctx, &[bulk("key1")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let db = ctx.store().database(0);
        assert!(!db.exists(b"key1"));
    }

    #[test]
    fn test_del_multiple() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key1", "v1");
        set_key(&mut ctx, "key2", "v2");
        set_key(&mut ctx, "key3", "v3");

        let result = del(&mut ctx, &[bulk("key1"), bulk("key2"), bulk("nonexistent")]).unwrap();
        assert_eq!(result, RespValue::Integer(2));
    }

    #[test]
    fn test_exists_single() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key1", "value1");

        let result = exists(&mut ctx, &[bulk("key1")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = exists(&mut ctx, &[bulk("nokey")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_exists_multiple() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key1", "v1");
        set_key(&mut ctx, "key2", "v2");

        // Same key twice counts twice if it exists
        let result = exists(&mut ctx, &[bulk("key1"), bulk("key2"), bulk("key1")]).unwrap();
        assert_eq!(result, RespValue::Integer(3));
    }

    #[test]
    fn test_expire_and_ttl() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        // No TTL initially
        let result = ttl(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Integer(-1));

        // Set TTL
        let result = expire(&mut ctx, &[bulk("key"), bulk("100")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // Check TTL
        let result = ttl(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t > 0 && t <= 100);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_expire_nonexistent() {
        let mut ctx = make_ctx();
        let result = expire(&mut ctx, &[bulk("nokey"), bulk("100")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_pexpire_and_pttl() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        let result = pexpire(&mut ctx, &[bulk("key"), bulk("5000")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = pttl(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t > 0 && t <= 5000);
        }
    }

    #[test]
    fn test_persist() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        // Set TTL
        expire(&mut ctx, &[bulk("key"), bulk("100")]).unwrap();
        assert!(matches!(ttl(&mut ctx, &[bulk("key")]).unwrap(), RespValue::Integer(t) if t > 0));

        // Persist
        let result = persist(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // TTL should be -1
        let result = ttl(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Integer(-1));
    }

    #[test]
    fn test_type_cmd() {
        let mut ctx = make_ctx();

        // String
        set_key(&mut ctx, "str", "value");
        let result = type_cmd(&mut ctx, &[bulk("str")]).unwrap();
        assert_eq!(result, RespValue::SimpleString("string".to_string()));

        // List
        let db = ctx.store().database(0);
        db.set(Bytes::from("list"), Entry::new(RedisValue::new_list()));
        let result = type_cmd(&mut ctx, &[bulk("list")]).unwrap();
        assert_eq!(result, RespValue::SimpleString("list".to_string()));

        // None
        let result = type_cmd(&mut ctx, &[bulk("nokey")]).unwrap();
        assert_eq!(result, RespValue::SimpleString("none".to_string()));
    }

    #[test]
    fn test_keys_pattern() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "user:1", "a");
        set_key(&mut ctx, "user:2", "b");
        set_key(&mut ctx, "order:1", "c");

        let result = keys(&mut ctx, &[bulk("user:*")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 2);
        } else {
            panic!("Expected array");
        }

        let result = keys(&mut ctx, &[bulk("*")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 3);
        }
    }

    #[test]
    fn test_scan_basic() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key1", "v1");
        set_key(&mut ctx, "key2", "v2");
        set_key(&mut ctx, "key3", "v3");

        let result = scan(&mut ctx, &[bulk("0")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 2);
            // First element is cursor
            // Second element is array of keys
            if let RespValue::Array(keys) = &arr[1] {
                assert!(!keys.is_empty());
            }
        }
    }

    #[test]
    fn test_rename() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "old", "value");

        let result = rename(&mut ctx, &[bulk("old"), bulk("new")]).unwrap();
        assert_eq!(result, RespValue::ok());

        let db = ctx.store().database(0);
        assert!(!db.exists(b"old"));
        assert!(db.exists(b"new"));
    }

    #[test]
    fn test_rename_nonexistent() {
        let mut ctx = make_ctx();
        let result = rename(&mut ctx, &[bulk("nokey"), bulk("new")]);
        assert!(matches!(result, Err(CommandError::NoSuchKey)));
    }

    #[test]
    fn test_renamenx() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "old", "value");
        set_key(&mut ctx, "existing", "other");

        // Should fail - existing key exists
        let result = renamenx(&mut ctx, &[bulk("old"), bulk("existing")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));

        // Should succeed
        let result = renamenx(&mut ctx, &[bulk("old"), bulk("new")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_copy() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "src", "value");

        let result = copy(&mut ctx, &[bulk("src"), bulk("dst")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let db = ctx.store().database(0);
        assert!(db.exists(b"src"));
        assert!(db.exists(b"dst"));
    }

    #[test]
    fn test_copy_no_replace() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "src", "value1");
        set_key(&mut ctx, "dst", "value2");

        let result = copy(&mut ctx, &[bulk("src"), bulk("dst")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_copy_with_replace() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "src", "value1");
        set_key(&mut ctx, "dst", "value2");

        let result = copy(&mut ctx, &[bulk("src"), bulk("dst"), bulk("REPLACE")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_touch() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key1", "v1");
        set_key(&mut ctx, "key2", "v2");

        let result = touch(&mut ctx, &[bulk("key1"), bulk("key2"), bulk("nokey")]).unwrap();
        assert_eq!(result, RespValue::Integer(2));
    }

    #[test]
    fn test_object_encoding() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "num", "12345");
        set_key(&mut ctx, "short", "hello");

        let result = object(&mut ctx, &[bulk("ENCODING"), bulk("num")]).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from("int")));

        let result = object(&mut ctx, &[bulk("ENCODING"), bulk("short")]).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from("embstr")));
    }

    #[test]
    fn test_ttl_nonexistent() {
        let mut ctx = make_ctx();
        let result = ttl(&mut ctx, &[bulk("nokey")]).unwrap();
        assert_eq!(result, RespValue::Integer(-2));
    }

    #[test]
    fn test_randomkey_empty() {
        let mut ctx = make_ctx();
        let result = randomkey(&mut ctx, &[]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_randomkey_with_keys() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key1", "v1");

        let result = randomkey(&mut ctx, &[]).unwrap();
        if let RespValue::BulkString(b) = result {
            assert!(!b.is_empty());
        } else {
            panic!("Expected bulk string");
        }
    }

    #[test]
    fn test_unlink() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key1", "v1");
        set_key(&mut ctx, "key2", "v2");

        let result = unlink(&mut ctx, &[bulk("key1"), bulk("key2")]).unwrap();
        assert_eq!(result, RespValue::Integer(2));
    }

    #[test]
    fn test_expireat() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        let future = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 1000;

        let result = expireat(&mut ctx, &[bulk("key"), bulk(&future.to_string())]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = ttl(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t > 0);
        }
    }

    // ----------------------------------------------------------------
    // Tests for EXPIRE NX/XX/GT/LT
    // ----------------------------------------------------------------

    #[test]
    fn test_expire_nx_no_existing_ttl() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        // Key has no TTL, NX should set it
        let result = expire(&mut ctx, &[bulk("key"), bulk("100"), bulk("NX")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = ttl(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t > 0 && t <= 100);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_expire_nx_existing_ttl() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        // First, set a TTL
        expire(&mut ctx, &[bulk("key"), bulk("200")]).unwrap();

        // NX should NOT change it because TTL already exists
        let result = expire(&mut ctx, &[bulk("key"), bulk("100"), bulk("NX")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));

        // TTL should still be around 200
        let result = ttl(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t > 100); // Still closer to 200 than 100
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_expire_xx_no_existing_ttl() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        // Key has no TTL, XX should NOT set it
        let result = expire(&mut ctx, &[bulk("key"), bulk("100"), bulk("XX")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));

        // TTL should still be -1
        let result = ttl(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Integer(-1));
    }

    #[test]
    fn test_expire_xx_existing_ttl() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        // Set initial TTL
        expire(&mut ctx, &[bulk("key"), bulk("200")]).unwrap();

        // XX should update because TTL exists
        let result = expire(&mut ctx, &[bulk("key"), bulk("50"), bulk("XX")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // TTL should now be around 50
        let result = ttl(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t <= 50);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_expire_gt_greater() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        // Set initial TTL to 100
        expire(&mut ctx, &[bulk("key"), bulk("100")]).unwrap();

        // GT with 200 should update (200 > 100)
        let result = expire(&mut ctx, &[bulk("key"), bulk("200"), bulk("GT")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = ttl(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t > 100); // Should be around 200
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_expire_gt_lesser() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        // Set initial TTL to 100
        expire(&mut ctx, &[bulk("key"), bulk("100")]).unwrap();

        // GT with 50 should NOT update (50 < 100)
        let result = expire(&mut ctx, &[bulk("key"), bulk("50"), bulk("GT")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));

        // TTL should still be around 100
        let result = ttl(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t > 50);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_expire_gt_no_existing_ttl() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        // No TTL, GT should set it (matching Redis behavior)
        let result = expire(&mut ctx, &[bulk("key"), bulk("100"), bulk("GT")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = ttl(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t > 0 && t <= 100);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_expire_lt_lesser() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        // Set initial TTL to 100
        expire(&mut ctx, &[bulk("key"), bulk("100")]).unwrap();

        // LT with 50 should update (50 < 100)
        let result = expire(&mut ctx, &[bulk("key"), bulk("50"), bulk("LT")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = ttl(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t <= 50);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_expire_lt_greater() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        // Set initial TTL to 100
        expire(&mut ctx, &[bulk("key"), bulk("100")]).unwrap();

        // LT with 200 should NOT update (200 > 100)
        let result = expire(&mut ctx, &[bulk("key"), bulk("200"), bulk("LT")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));

        let result = ttl(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t <= 100);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_expire_lt_no_existing_ttl() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        // No TTL, LT should set it (matching Redis behavior)
        let result = expire(&mut ctx, &[bulk("key"), bulk("100"), bulk("LT")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = ttl(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t > 0 && t <= 100);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_expire_invalid_flag() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        let result = expire(&mut ctx, &[bulk("key"), bulk("100"), bulk("NOTAFLAG")]);
        assert!(matches!(result, Err(CommandError::SyntaxError)));
    }

    // ----------------------------------------------------------------
    // Tests for PEXPIRE NX/XX/GT/LT
    // ----------------------------------------------------------------

    #[test]
    fn test_pexpire_nx() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        // No TTL, NX should set it
        let result = pexpire(&mut ctx, &[bulk("key"), bulk("10000"), bulk("NX")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = pttl(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t > 0 && t <= 10000);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_pexpire_xx() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        // Set initial TTL
        pexpire(&mut ctx, &[bulk("key"), bulk("10000")]).unwrap();

        // XX should update because TTL exists
        let result = pexpire(&mut ctx, &[bulk("key"), bulk("5000"), bulk("XX")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = pttl(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t > 0 && t <= 5000);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_pexpire_gt() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        // Set initial TTL to 10000ms
        pexpire(&mut ctx, &[bulk("key"), bulk("10000")]).unwrap();

        // GT with 20000 should update (20000 > 10000)
        let result = pexpire(&mut ctx, &[bulk("key"), bulk("20000"), bulk("GT")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = pttl(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t > 10000); // Should be around 20000
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_pexpire_lt() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        // Set initial TTL to 10000ms
        pexpire(&mut ctx, &[bulk("key"), bulk("10000")]).unwrap();

        // LT with 5000 should update (5000 < 10000)
        let result = pexpire(&mut ctx, &[bulk("key"), bulk("5000"), bulk("LT")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = pttl(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t <= 5000);
        } else {
            panic!("Expected integer");
        }
    }

    // ----------------------------------------------------------------
    // Tests for EXPIREAT NX/XX/GT/LT
    // ----------------------------------------------------------------

    #[test]
    fn test_expireat_nx() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        let future = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 1000;

        // No TTL, NX should set it
        let result = expireat(
            &mut ctx,
            &[bulk("key"), bulk(&future.to_string()), bulk("NX")],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = ttl(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t > 0);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_expireat_xx_no_ttl() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        let future = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 1000;

        // No TTL, XX should NOT set it
        let result = expireat(
            &mut ctx,
            &[bulk("key"), bulk(&future.to_string()), bulk("XX")],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(0));

        let result = ttl(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Integer(-1));
    }

    #[test]
    fn test_expireat_gt() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Set initial expiry at now + 500
        expireat(
            &mut ctx,
            &[bulk("key"), bulk(&(now_secs + 500).to_string())],
        )
        .unwrap();

        // GT with now + 1000 should update (later timestamp)
        let result = expireat(
            &mut ctx,
            &[
                bulk("key"),
                bulk(&(now_secs + 1000).to_string()),
                bulk("GT"),
            ],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = ttl(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t > 500); // Should be around 1000
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_expireat_lt() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Set initial expiry at now + 1000
        expireat(
            &mut ctx,
            &[bulk("key"), bulk(&(now_secs + 1000).to_string())],
        )
        .unwrap();

        // LT with now + 200 should update (earlier timestamp)
        let result = expireat(
            &mut ctx,
            &[bulk("key"), bulk(&(now_secs + 200).to_string()), bulk("LT")],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = ttl(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t <= 200);
        } else {
            panic!("Expected integer");
        }
    }

    // ----------------------------------------------------------------
    // Tests for PEXPIREAT
    // ----------------------------------------------------------------

    #[test]
    fn test_pexpireat_basic() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        let future_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + 60_000; // 60 seconds from now

        let result = pexpireat(&mut ctx, &[bulk("key"), bulk(&future_ms.to_string())]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = pttl(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t > 0 && t <= 60_000);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_pexpireat_nonexistent_key() {
        let mut ctx = make_ctx();

        let future_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + 60_000;

        let result = pexpireat(&mut ctx, &[bulk("nokey"), bulk(&future_ms.to_string())]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_pexpireat_nx() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        let future_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + 60_000;

        // No TTL, NX should set it
        let result = pexpireat(
            &mut ctx,
            &[bulk("key"), bulk(&future_ms.to_string()), bulk("NX")],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = pttl(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t > 0);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_pexpireat_xx() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        // Set initial TTL via pexpireat
        pexpireat(
            &mut ctx,
            &[bulk("key"), bulk(&(now_ms + 30_000).to_string())],
        )
        .unwrap();

        // XX should update because TTL exists
        let result = pexpireat(
            &mut ctx,
            &[
                bulk("key"),
                bulk(&(now_ms + 60_000).to_string()),
                bulk("XX"),
            ],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = pttl(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t > 30_000); // Should be closer to 60s
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_pexpireat_past_timestamp() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        // Timestamp in the past — key should expire immediately (ttl clamped to 0)
        let past_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            - 10_000; // 10 seconds ago

        let result = pexpireat(&mut ctx, &[bulk("key"), bulk(&past_ms.to_string())]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // The key is immediately expired, so PTTL should return -2 (key no longer exists)
        let result = pttl(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Integer(-2));
    }

    // ----------------------------------------------------------------
    // Tests for SCAN options
    // ----------------------------------------------------------------

    #[test]
    fn test_scan_match_pattern() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "user:1", "a");
        set_key(&mut ctx, "user:2", "b");
        set_key(&mut ctx, "order:1", "c");

        let result = scan(&mut ctx, &[bulk("0"), bulk("MATCH"), bulk("user:*")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 2);
            if let RespValue::Array(ref keys) = arr[1] {
                assert_eq!(keys.len(), 2);
                for k in keys {
                    if let RespValue::BulkString(b) = k {
                        assert!(b.starts_with(b"user:"));
                    } else {
                        panic!("Expected bulk string key");
                    }
                }
            } else {
                panic!("Expected array of keys");
            }
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_scan_count_option() {
        let mut ctx = make_ctx();
        // Create more keys than COUNT to test partial results
        for i in 0..10 {
            set_key(&mut ctx, &format!("k{i}"), "v");
        }

        let result = scan(&mut ctx, &[bulk("0"), bulk("COUNT"), bulk("2")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 2);
            // Cursor should be non-zero since we have more keys
            if let RespValue::BulkString(cursor) = &arr[0] {
                let cursor_val: usize = std::str::from_utf8(cursor).unwrap().parse().unwrap();
                assert!(cursor_val > 0); // More keys remain
            }
            if let RespValue::Array(ref keys) = arr[1] {
                assert_eq!(keys.len(), 2);
            }
        }
    }

    #[test]
    fn test_scan_type_filter() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "str1", "hello");
        set_key(&mut ctx, "str2", "world");

        // Add a list key
        let db = ctx.store().database(0);
        db.set(Bytes::from("list1"), Entry::new(RedisValue::new_list()));

        let result = scan(&mut ctx, &[bulk("0"), bulk("TYPE"), bulk("string")]).unwrap();
        if let RespValue::Array(arr) = result {
            if let RespValue::Array(ref keys) = arr[1] {
                // Should only contain string keys, not the list
                for k in keys {
                    if let RespValue::BulkString(b) = k {
                        assert!(b.starts_with(b"str"));
                    } else {
                        panic!("Expected bulk string");
                    }
                }
                assert_eq!(keys.len(), 2);
            }
        }
    }

    #[test]
    fn test_scan_full_iteration() {
        let mut ctx = make_ctx();
        let num_keys = 25;
        for i in 0..num_keys {
            set_key(&mut ctx, &format!("key:{i}"), "val");
        }

        let mut all_keys = Vec::new();
        let mut cursor = 0usize;

        loop {
            let result = scan(
                &mut ctx,
                &[bulk(&cursor.to_string()), bulk("COUNT"), bulk("5")],
            )
            .unwrap();
            if let RespValue::Array(arr) = result {
                if let RespValue::BulkString(ref c) = arr[0] {
                    cursor = std::str::from_utf8(c).unwrap().parse().unwrap();
                }
                if let RespValue::Array(ref keys) = arr[1] {
                    for k in keys {
                        all_keys.push(k.clone());
                    }
                }
            }
            if cursor == 0 {
                break;
            }
        }

        assert_eq!(all_keys.len(), num_keys);
    }

    #[test]
    fn test_scan_empty_db() {
        let mut ctx = make_ctx();

        let result = scan(&mut ctx, &[bulk("0")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], RespValue::BulkString(Bytes::from("0")));
            if let RespValue::Array(ref keys) = arr[1] {
                assert!(keys.is_empty());
            }
        }
    }

    #[test]
    fn test_scan_match_and_count_combined() {
        let mut ctx = make_ctx();
        for i in 0..10 {
            set_key(&mut ctx, &format!("key:{i}"), "v");
        }
        for i in 0..5 {
            set_key(&mut ctx, &format!("other:{i}"), "v");
        }

        // SCAN with MATCH key:* COUNT 5
        let result = scan(
            &mut ctx,
            &[
                bulk("0"),
                bulk("MATCH"),
                bulk("key:*"),
                bulk("COUNT"),
                bulk("5"),
            ],
        )
        .unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 2);
            if let RespValue::Array(ref keys) = arr[1] {
                // All returned keys should match the pattern
                for k in keys {
                    if let RespValue::BulkString(b) = k {
                        assert!(b.starts_with(b"key:"));
                    }
                }
                assert!(keys.len() <= 5);
            }
        }
    }

    // ----------------------------------------------------------------
    // Tests for COPY options
    // ----------------------------------------------------------------

    #[test]
    fn test_copy_nonexistent_source() {
        let mut ctx = make_ctx();
        let result = copy(&mut ctx, &[bulk("nosrc"), bulk("dst")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_copy_db_option() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "src", "hello");

        // Copy src to dst in DB 1
        let result = copy(&mut ctx, &[bulk("src"), bulk("dst"), bulk("DB"), bulk("1")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // dst should exist in DB 1
        let db1 = ctx.store().database(1);
        assert!(db1.exists(b"dst"));

        // dst should NOT exist in DB 0
        let db0 = ctx.store().database(0);
        assert!(!db0.exists(b"dst"));
    }

    #[test]
    fn test_copy_invalid_db() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "src", "hello");

        // DB 100 is out of range (default is 16 databases)
        let result = copy(
            &mut ctx,
            &[bulk("src"), bulk("dst"), bulk("DB"), bulk("100")],
        );
        assert!(matches!(result, Err(CommandError::InvalidDbIndex)));
    }

    #[test]
    fn test_copy_replace_overwrites_different_type() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "src", "string_value");

        // Set dst as a list
        {
            let db = ctx.store().database(0);
            let mut list = VecDeque::new();
            list.push_back(Bytes::from("item1"));
            db.set(Bytes::from("dst"), Entry::new(RedisValue::List(list)));
        }

        // Without REPLACE, should fail (dst exists)
        let result = copy(&mut ctx, &[bulk("src"), bulk("dst")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));

        // With REPLACE, should overwrite the list with the string
        let result = copy(&mut ctx, &[bulk("src"), bulk("dst"), bulk("REPLACE")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // dst should now be a string
        let entry = ctx.store().database(0).get(b"dst").unwrap();
        assert_eq!(entry.value.type_name(), "string");
    }

    // ----------------------------------------------------------------
    // Tests for OBJECT subcommands
    // ----------------------------------------------------------------

    #[test]
    fn test_object_encoding_raw() {
        let mut ctx = make_ctx();
        // String > 44 bytes should be "raw"
        let long_str = "a".repeat(50);
        set_key(&mut ctx, "longkey", &long_str);

        let result = object(&mut ctx, &[bulk("ENCODING"), bulk("longkey")]).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from("raw")));
    }

    #[test]
    fn test_object_encoding_list() {
        let mut ctx = make_ctx();

        // Small list (<= 128 elements) → "listpack"
        {
            let db = ctx.store().database(0);
            let mut small_list = VecDeque::new();
            for i in 0..5 {
                small_list.push_back(Bytes::from(format!("item{i}")));
            }
            db.set(
                Bytes::from("smalllist"),
                Entry::new(RedisValue::List(small_list)),
            );
        }

        let result = object(&mut ctx, &[bulk("ENCODING"), bulk("smalllist")]).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from("listpack")));

        // Large list (> 128 elements) → "quicklist"
        {
            let db = ctx.store().database(0);
            let mut big_list = VecDeque::new();
            for i in 0..130 {
                big_list.push_back(Bytes::from(format!("item{i}")));
            }
            db.set(
                Bytes::from("biglist"),
                Entry::new(RedisValue::List(big_list)),
            );
        }

        let result = object(&mut ctx, &[bulk("ENCODING"), bulk("biglist")]).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from("quicklist")));
    }

    #[test]
    fn test_object_encoding_set() {
        let mut ctx = make_ctx();

        // Small set (<= 128 elements) → "listpack"
        {
            let db = ctx.store().database(0);
            let mut small_set = HashSet::new();
            for i in 0..5 {
                small_set.insert(Bytes::from(format!("member{i}")));
            }
            db.set(
                Bytes::from("smallset"),
                Entry::new(RedisValue::Set(small_set)),
            );
        }

        let result = object(&mut ctx, &[bulk("ENCODING"), bulk("smallset")]).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from("listpack")));

        // Large set (> 128 elements) → "hashtable"
        {
            let db = ctx.store().database(0);
            let mut big_set = HashSet::new();
            for i in 0..130 {
                big_set.insert(Bytes::from(format!("member{i}")));
            }
            db.set(Bytes::from("bigset"), Entry::new(RedisValue::Set(big_set)));
        }

        let result = object(&mut ctx, &[bulk("ENCODING"), bulk("bigset")]).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from("hashtable")));
    }

    #[test]
    fn test_object_encoding_hash() {
        let mut ctx = make_ctx();

        // Small hash (<= 128 entries) → "listpack"
        {
            let db = ctx.store().database(0);
            let mut small_hash = HashMap::new();
            for i in 0..5 {
                small_hash.insert(
                    Bytes::from(format!("field{i}")),
                    Bytes::from(format!("value{i}")),
                );
            }
            db.set(
                Bytes::from("smallhash"),
                Entry::new(RedisValue::Hash(small_hash)),
            );
        }

        let result = object(&mut ctx, &[bulk("ENCODING"), bulk("smallhash")]).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from("listpack")));

        // Large hash (> 128 entries) → "hashtable"
        {
            let db = ctx.store().database(0);
            let mut big_hash = HashMap::new();
            for i in 0..130 {
                big_hash.insert(
                    Bytes::from(format!("field{i}")),
                    Bytes::from(format!("value{i}")),
                );
            }
            db.set(
                Bytes::from("bighash"),
                Entry::new(RedisValue::Hash(big_hash)),
            );
        }

        let result = object(&mut ctx, &[bulk("ENCODING"), bulk("bighash")]).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from("hashtable")));
    }

    #[test]
    fn test_object_encoding_zset() {
        let mut ctx = make_ctx();

        // Small sorted set (<= 128 elements) → "listpack"
        {
            let db = ctx.store().database(0);
            let mut small_scores = HashMap::new();
            let mut small_members = BTreeMap::new();
            for i in 0..5 {
                let member = Bytes::from(format!("member{i}"));
                let score = i as f64;
                small_scores.insert(member.clone(), score);
                small_members.insert((OrderedFloat(score), member), ());
            }
            db.set(
                Bytes::from("smallzset"),
                Entry::new(RedisValue::SortedSet {
                    scores: small_scores,
                    members: small_members,
                }),
            );
        }

        let result = object(&mut ctx, &[bulk("ENCODING"), bulk("smallzset")]).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from("listpack")));

        // Large sorted set (> 128 elements) → "skiplist"
        {
            let db = ctx.store().database(0);
            let mut big_scores = HashMap::new();
            let mut big_members = BTreeMap::new();
            for i in 0..130 {
                let member = Bytes::from(format!("member{i}"));
                let score = i as f64;
                big_scores.insert(member.clone(), score);
                big_members.insert((OrderedFloat(score), member), ());
            }
            db.set(
                Bytes::from("bigzset"),
                Entry::new(RedisValue::SortedSet {
                    scores: big_scores,
                    members: big_members,
                }),
            );
        }

        let result = object(&mut ctx, &[bulk("ENCODING"), bulk("bigzset")]).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from("skiplist")));
    }

    #[test]
    fn test_object_refcount() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        let result = object(&mut ctx, &[bulk("REFCOUNT"), bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_object_idletime() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        let result = object(&mut ctx, &[bulk("IDLETIME"), bulk("key")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t >= 0);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_object_freq() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        let result = object(&mut ctx, &[bulk("FREQ"), bulk("key")]).unwrap();
        assert!(matches!(result, RespValue::Integer(_)));
    }

    #[test]
    fn test_object_help() {
        let mut ctx = make_ctx();

        let result = object(&mut ctx, &[bulk("HELP")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert!(!arr.is_empty());
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_object_unknown_subcommand() {
        let mut ctx = make_ctx();

        let result = object(&mut ctx, &[bulk("INVALID")]);
        assert!(matches!(result, Err(CommandError::InvalidArgument(_))));
    }

    #[test]
    fn test_object_nonexistent_key() {
        let mut ctx = make_ctx();

        let result = object(&mut ctx, &[bulk("ENCODING"), bulk("nonexistent")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    // ================================================================
    // Additional EXPIRE tests (7 new)
    // ================================================================

    #[test]
    fn test_expire_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        let result = expire(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_expire_wrong_arity_one_arg() {
        let mut ctx = make_ctx();
        let result = expire(&mut ctx, &[bulk("key")]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_expire_zero_seconds() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        // Zero seconds → TTL of 0 → key should expire immediately
        let result = expire(&mut ctx, &[bulk("key"), bulk("0")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // Key should be effectively expired (TTL reports -2)
        let result = pttl(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Integer(-2));
    }

    #[test]
    fn test_expire_negative_seconds() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        // Negative seconds → clamped to 0 → key expires immediately
        let result = expire(&mut ctx, &[bulk("key"), bulk("-5")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // Key should be effectively expired
        let result = pttl(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Integer(-2));
    }

    #[test]
    fn test_expire_non_integer() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        let result = expire(&mut ctx, &[bulk("key"), bulk("abc")]);
        assert!(matches!(result, Err(CommandError::NotAnInteger)));
    }

    #[test]
    fn test_expire_on_list_key() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(0);
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("item1"));
        db.set(Bytes::from("mylist"), Entry::new(RedisValue::List(list)));

        // EXPIRE should work on any key type
        let result = expire(&mut ctx, &[bulk("mylist"), bulk("100")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = ttl(&mut ctx, &[bulk("mylist")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t > 0 && t <= 100);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_expire_returns_integer_type() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        let result = expire(&mut ctx, &[bulk("key"), bulk("100")]).unwrap();
        assert!(matches!(result, RespValue::Integer(1)));

        // Also verify nonexistent key returns Integer(0)
        let result = expire(&mut ctx, &[bulk("nokey"), bulk("100")]).unwrap();
        assert!(matches!(result, RespValue::Integer(0)));
    }

    // ================================================================
    // Additional PEXPIRE tests (15 new)
    // ================================================================

    #[test]
    fn test_pexpire_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        let result = pexpire(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_pexpire_wrong_arity_one_arg() {
        let mut ctx = make_ctx();
        let result = pexpire(&mut ctx, &[bulk("key")]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_pexpire_nonexistent_key() {
        let mut ctx = make_ctx();
        let result = pexpire(&mut ctx, &[bulk("nokey"), bulk("5000")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_pexpire_basic() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        let result = pexpire(&mut ctx, &[bulk("key"), bulk("50000")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = pttl(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t > 0 && t <= 50000);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_pexpire_zero_ms() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        // Zero ms → key expires immediately
        let result = pexpire(&mut ctx, &[bulk("key"), bulk("0")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = pttl(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Integer(-2));
    }

    #[test]
    fn test_pexpire_negative_ms() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        // Negative ms → clamped to 0 → key expires immediately
        let result = pexpire(&mut ctx, &[bulk("key"), bulk("-1000")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = pttl(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Integer(-2));
    }

    #[test]
    fn test_pexpire_non_integer() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        let result = pexpire(&mut ctx, &[bulk("key"), bulk("abc")]);
        assert!(matches!(result, Err(CommandError::NotAnInteger)));
    }

    #[test]
    fn test_pexpire_on_list_key() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(0);
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("item1"));
        db.set(Bytes::from("mylist"), Entry::new(RedisValue::List(list)));

        let result = pexpire(&mut ctx, &[bulk("mylist"), bulk("50000")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = pttl(&mut ctx, &[bulk("mylist")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t > 0 && t <= 50000);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_pexpire_returns_integer_type() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        let result = pexpire(&mut ctx, &[bulk("key"), bulk("5000")]).unwrap();
        assert!(matches!(result, RespValue::Integer(1)));

        let result = pexpire(&mut ctx, &[bulk("nokey"), bulk("5000")]).unwrap();
        assert!(matches!(result, RespValue::Integer(0)));
    }

    #[test]
    fn test_pexpire_overwrites_existing_ttl() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        // Set initial TTL of 50000ms
        pexpire(&mut ctx, &[bulk("key"), bulk("50000")]).unwrap();

        // Overwrite with 10000ms
        let result = pexpire(&mut ctx, &[bulk("key"), bulk("10000")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = pttl(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t > 0 && t <= 10000);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_pexpire_nx_no_ttl() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        // Key has no TTL, NX should succeed
        let result = pexpire(&mut ctx, &[bulk("key"), bulk("30000"), bulk("NX")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = pttl(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t > 0 && t <= 30000);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_pexpire_nx_has_ttl() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        // Set initial TTL
        pexpire(&mut ctx, &[bulk("key"), bulk("50000")]).unwrap();

        // NX should fail because TTL already exists
        let result = pexpire(&mut ctx, &[bulk("key"), bulk("10000"), bulk("NX")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));

        // TTL should still be around 50000
        let result = pttl(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t > 10000);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_pexpire_xx_no_ttl() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        // Key has no TTL, XX should fail
        let result = pexpire(&mut ctx, &[bulk("key"), bulk("30000"), bulk("XX")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));

        // TTL should still be -1 (no TTL)
        let result = ttl(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Integer(-1));
    }

    #[test]
    fn test_pexpire_xx_has_ttl() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        // Set initial TTL
        pexpire(&mut ctx, &[bulk("key"), bulk("50000")]).unwrap();

        // XX should succeed because TTL exists
        let result = pexpire(&mut ctx, &[bulk("key"), bulk("10000"), bulk("XX")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // TTL should now be around 10000
        let result = pttl(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t > 0 && t <= 10000);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_pexpire_large_ms() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        // Very large ms value (30 days)
        let large_ms = 30 * 24 * 60 * 60 * 1000i64; // 2592000000ms
        let result = pexpire(&mut ctx, &[bulk("key"), bulk(&large_ms.to_string())]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = pttl(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t > 0);
        } else {
            panic!("Expected integer");
        }
    }

    // ================================================================
    // Additional EXPIREAT tests (15 new)
    // ================================================================

    #[test]
    fn test_expireat_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        let result = expireat(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_expireat_wrong_arity_one_arg() {
        let mut ctx = make_ctx();
        let result = expireat(&mut ctx, &[bulk("key")]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_expireat_nonexistent_key() {
        let mut ctx = make_ctx();
        let future = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 1000;

        let result = expireat(&mut ctx, &[bulk("nokey"), bulk(&future.to_string())]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_expireat_basic_future() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        let future = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 500;

        let result = expireat(&mut ctx, &[bulk("key"), bulk(&future.to_string())]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = ttl(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t > 0 && t <= 500);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_expireat_past_timestamp() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        // Timestamp in the past → TTL clamped to 0 → key expires immediately
        let past = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - 100;

        let result = expireat(&mut ctx, &[bulk("key"), bulk(&past.to_string())]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // Key should be effectively expired
        let result = pttl(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Integer(-2));
    }

    #[test]
    fn test_expireat_zero_timestamp() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        // Zero timestamp (epoch) is in the past → key expires immediately
        let result = expireat(&mut ctx, &[bulk("key"), bulk("0")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = pttl(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Integer(-2));
    }

    #[test]
    fn test_expireat_negative_timestamp() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        // Negative timestamp → in the past → TTL clamped to 0 → expires immediately
        let result = expireat(&mut ctx, &[bulk("key"), bulk("-100")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = pttl(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Integer(-2));
    }

    #[test]
    fn test_expireat_non_integer() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        let result = expireat(&mut ctx, &[bulk("key"), bulk("abc")]);
        assert!(matches!(result, Err(CommandError::NotAnInteger)));
    }

    #[test]
    fn test_expireat_on_list_key() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(0);
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("item1"));
        db.set(Bytes::from("mylist"), Entry::new(RedisValue::List(list)));

        let future = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 1000;

        let result = expireat(&mut ctx, &[bulk("mylist"), bulk(&future.to_string())]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = ttl(&mut ctx, &[bulk("mylist")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t > 0);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_expireat_returns_integer_type() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        let future = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 1000;

        let result = expireat(&mut ctx, &[bulk("key"), bulk(&future.to_string())]).unwrap();
        assert!(matches!(result, RespValue::Integer(1)));

        let result = expireat(&mut ctx, &[bulk("nokey"), bulk(&future.to_string())]).unwrap();
        assert!(matches!(result, RespValue::Integer(0)));
    }

    #[test]
    fn test_expireat_nx_no_ttl() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        let future = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 1000;

        // No TTL, NX should succeed
        let result = expireat(
            &mut ctx,
            &[bulk("key"), bulk(&future.to_string()), bulk("NX")],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = ttl(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t > 0);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_expireat_nx_has_ttl() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Set initial TTL
        expireat(
            &mut ctx,
            &[bulk("key"), bulk(&(now_secs + 500).to_string())],
        )
        .unwrap();

        // NX should fail because TTL already exists
        let result = expireat(
            &mut ctx,
            &[
                bulk("key"),
                bulk(&(now_secs + 1000).to_string()),
                bulk("NX"),
            ],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_expireat_xx_no_ttl2() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        let future = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 1000;

        // No TTL, XX should fail
        let result = expireat(
            &mut ctx,
            &[bulk("key"), bulk(&future.to_string()), bulk("XX")],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(0));

        let result = ttl(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Integer(-1));
    }

    #[test]
    fn test_expireat_gt_greater_ts() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Set initial expiry at now + 200
        expireat(
            &mut ctx,
            &[bulk("key"), bulk(&(now_secs + 200).to_string())],
        )
        .unwrap();

        // GT with now + 800 should succeed (800 > 200)
        let result = expireat(
            &mut ctx,
            &[bulk("key"), bulk(&(now_secs + 800).to_string()), bulk("GT")],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = ttl(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t > 200);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_expireat_overwrites_existing_ttl() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Set initial expiry at now + 1000
        expireat(
            &mut ctx,
            &[bulk("key"), bulk(&(now_secs + 1000).to_string())],
        )
        .unwrap();

        // Overwrite with now + 100
        let result = expireat(
            &mut ctx,
            &[bulk("key"), bulk(&(now_secs + 100).to_string())],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = ttl(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t <= 100);
        } else {
            panic!("Expected integer");
        }
    }

    // ================================================================
    // Additional PEXPIREAT tests (15 new)
    // ================================================================

    #[test]
    fn test_pexpireat_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        let result = pexpireat(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_pexpireat_wrong_arity_one_arg() {
        let mut ctx = make_ctx();
        let result = pexpireat(&mut ctx, &[bulk("key")]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_pexpireat_future() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        let future_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + 120_000; // 2 minutes from now

        let result = pexpireat(&mut ctx, &[bulk("key"), bulk(&future_ms.to_string())]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = pttl(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t > 0 && t <= 120_000);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_pexpireat_returns_integer_type() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        let future_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + 60_000;

        let result = pexpireat(&mut ctx, &[bulk("key"), bulk(&future_ms.to_string())]).unwrap();
        assert!(matches!(result, RespValue::Integer(1)));

        let result = pexpireat(&mut ctx, &[bulk("nokey"), bulk(&future_ms.to_string())]).unwrap();
        assert!(matches!(result, RespValue::Integer(0)));
    }

    #[test]
    fn test_pexpireat_on_list_key() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(0);
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("item1"));
        db.set(Bytes::from("mylist"), Entry::new(RedisValue::List(list)));

        let future_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + 60_000;

        let result = pexpireat(&mut ctx, &[bulk("mylist"), bulk(&future_ms.to_string())]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = pttl(&mut ctx, &[bulk("mylist")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t > 0);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_pexpireat_gt_succeeds() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        // Set initial expiry at now + 30s
        pexpireat(
            &mut ctx,
            &[bulk("key"), bulk(&(now_ms + 30_000).to_string())],
        )
        .unwrap();

        // GT with now + 90s should succeed (90s > 30s)
        let result = pexpireat(
            &mut ctx,
            &[
                bulk("key"),
                bulk(&(now_ms + 90_000).to_string()),
                bulk("GT"),
            ],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = pttl(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t > 30_000);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_pexpireat_gt_fails() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        // Set initial expiry at now + 90s
        pexpireat(
            &mut ctx,
            &[bulk("key"), bulk(&(now_ms + 90_000).to_string())],
        )
        .unwrap();

        // GT with now + 30s should fail (30s < 90s)
        let result = pexpireat(
            &mut ctx,
            &[
                bulk("key"),
                bulk(&(now_ms + 30_000).to_string()),
                bulk("GT"),
            ],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(0));

        // TTL should still be around 90s
        let result = pttl(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t > 30_000);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_pexpireat_lt_succeeds() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        // Set initial expiry at now + 90s
        pexpireat(
            &mut ctx,
            &[bulk("key"), bulk(&(now_ms + 90_000).to_string())],
        )
        .unwrap();

        // LT with now + 30s should succeed (30s < 90s)
        let result = pexpireat(
            &mut ctx,
            &[
                bulk("key"),
                bulk(&(now_ms + 30_000).to_string()),
                bulk("LT"),
            ],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = pttl(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t <= 30_000);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_pexpireat_lt_fails() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        // Set initial expiry at now + 30s
        pexpireat(
            &mut ctx,
            &[bulk("key"), bulk(&(now_ms + 30_000).to_string())],
        )
        .unwrap();

        // LT with now + 90s should fail (90s > 30s)
        let result = pexpireat(
            &mut ctx,
            &[
                bulk("key"),
                bulk(&(now_ms + 90_000).to_string()),
                bulk("LT"),
            ],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_pexpireat_non_integer() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        let result = pexpireat(&mut ctx, &[bulk("key"), bulk("abc")]);
        assert!(matches!(result, Err(CommandError::NotAnInteger)));
    }

    #[test]
    fn test_pexpireat_zero_timestamp() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        // Zero ms timestamp (epoch) is in the past → key expires immediately
        let result = pexpireat(&mut ctx, &[bulk("key"), bulk("0")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = pttl(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Integer(-2));
    }

    #[test]
    fn test_pexpireat_negative_timestamp() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        // Negative ms timestamp → in the past → TTL clamped to 0 → expires immediately
        let result = pexpireat(&mut ctx, &[bulk("key"), bulk("-5000")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = pttl(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Integer(-2));
    }

    #[test]
    fn test_pexpireat_overwrites_existing_ttl() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        // Set initial expiry at now + 60s
        pexpireat(
            &mut ctx,
            &[bulk("key"), bulk(&(now_ms + 60_000).to_string())],
        )
        .unwrap();

        // Overwrite with now + 10s
        let result = pexpireat(
            &mut ctx,
            &[bulk("key"), bulk(&(now_ms + 10_000).to_string())],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = pttl(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t > 0 && t <= 10_000);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_pexpireat_large_timestamp() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        // Timestamp far in the future (year ~2050)
        let large_ts: i64 = 2_524_608_000_000; // ~2050 in ms

        let result = pexpireat(&mut ctx, &[bulk("key"), bulk(&large_ts.to_string())]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = pttl(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t > 0);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_pexpireat_nx_has_ttl() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        // Set initial TTL
        pexpireat(
            &mut ctx,
            &[bulk("key"), bulk(&(now_ms + 60_000).to_string())],
        )
        .unwrap();

        // NX should fail because TTL already exists
        let result = pexpireat(
            &mut ctx,
            &[
                bulk("key"),
                bulk(&(now_ms + 120_000).to_string()),
                bulk("NX"),
            ],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(0));

        // TTL should still be around 60s, not 120s
        let result = pttl(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t <= 60_000);
        } else {
            panic!("Expected integer");
        }
    }

    // ================================================================
    // DEL tests (18 additional)
    // ================================================================

    #[test]
    fn test_del_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        let result = del(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_del_nonexistent_key() {
        let mut ctx = make_ctx();
        let result = del(&mut ctx, &[bulk("nosuchkey")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_del_single_existing() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "mykey", "hello");
        let result = del(&mut ctx, &[bulk("mykey")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_del_multiple_all_exist() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "a", "1");
        set_key(&mut ctx, "b", "2");
        set_key(&mut ctx, "c", "3");
        let result = del(&mut ctx, &[bulk("a"), bulk("b"), bulk("c")]).unwrap();
        assert_eq!(result, RespValue::Integer(3));
    }

    #[test]
    fn test_del_multiple_some_missing() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "a", "1");
        set_key(&mut ctx, "c", "3");
        let result = del(&mut ctx, &[bulk("a"), bulk("b"), bulk("c")]).unwrap();
        assert_eq!(result, RespValue::Integer(2));
    }

    #[test]
    fn test_del_multiple_all_missing() {
        let mut ctx = make_ctx();
        let result = del(&mut ctx, &[bulk("x"), bulk("y"), bulk("z")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_del_duplicate_keys() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "dup", "val");
        // First occurrence deletes, second finds it gone
        let result = del(&mut ctx, &[bulk("dup"), bulk("dup")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_del_actually_removes_key() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "gone", "val");
        del(&mut ctx, &[bulk("gone")]).unwrap();
        let db = ctx.store().database(ctx.selected_db());
        assert!(db.get(b"gone").is_none());
    }

    #[test]
    fn test_del_list_key() {
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(ctx.selected_db());
            db.set(
                Bytes::from("lkey"),
                Entry::new(RedisValue::List(VecDeque::from([Bytes::from("a")]))),
            );
        }
        let result = del(&mut ctx, &[bulk("lkey")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        assert!(ctx
            .store()
            .database(ctx.selected_db())
            .get(b"lkey")
            .is_none());
    }

    #[test]
    fn test_del_hash_key() {
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(ctx.selected_db());
            db.set(
                Bytes::from("hkey"),
                Entry::new(RedisValue::Hash(HashMap::from([(
                    Bytes::from("f"),
                    Bytes::from("v"),
                )]))),
            );
        }
        let result = del(&mut ctx, &[bulk("hkey")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        assert!(ctx
            .store()
            .database(ctx.selected_db())
            .get(b"hkey")
            .is_none());
    }

    #[test]
    fn test_del_set_key() {
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(ctx.selected_db());
            db.set(
                Bytes::from("skey"),
                Entry::new(RedisValue::Set(HashSet::from([Bytes::from("a")]))),
            );
        }
        let result = del(&mut ctx, &[bulk("skey")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        assert!(ctx
            .store()
            .database(ctx.selected_db())
            .get(b"skey")
            .is_none());
    }

    #[test]
    fn test_del_zset_key() {
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(ctx.selected_db());
            let mut scores = HashMap::new();
            scores.insert(Bytes::from("m1"), 1.0);
            let mut members = BTreeMap::new();
            members.insert((OrderedFloat(1.0), Bytes::from("m1")), ());
            db.set(
                Bytes::from("zkey"),
                Entry::new(RedisValue::SortedSet { scores, members }),
            );
        }
        let result = del(&mut ctx, &[bulk("zkey")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        assert!(ctx
            .store()
            .database(ctx.selected_db())
            .get(b"zkey")
            .is_none());
    }

    #[test]
    fn test_del_expired_key() {
        use std::time::Instant;
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(ctx.selected_db());
            let mut entry = Entry::new(RedisValue::String(Bytes::from("val")));
            entry.expires_at = Some(Instant::now() - Duration::from_secs(1));
            db.set(Bytes::from("expkey"), entry);
        }
        // DEL physically removes without checking expiry, so the expired key is still counted
        let result = del(&mut ctx, &[bulk("expkey")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_del_does_not_affect_other_keys() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "keep", "safe");
        set_key(&mut ctx, "remove", "gone");
        del(&mut ctx, &[bulk("remove")]).unwrap();
        let db = ctx.store().database(ctx.selected_db());
        assert!(db.get(b"keep").is_some());
        assert!(db.get(b"remove").is_none());
    }

    #[test]
    fn test_del_returns_integer_type() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "k", "v");
        let result = del(&mut ctx, &[bulk("k")]).unwrap();
        assert!(matches!(result, RespValue::Integer(_)));
    }

    #[test]
    fn test_del_empty_key_name() {
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(ctx.selected_db());
            db.set(
                Bytes::from(""),
                Entry::new(RedisValue::String(Bytes::from("val"))),
            );
        }
        let result = del(&mut ctx, &[bulk("")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_del_special_chars_key() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key:with:colons!@#$%", "val");
        let result = del(&mut ctx, &[bulk("key:with:colons!@#$%")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_del_with_ttl() {
        use std::time::Instant;
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(ctx.selected_db());
            let mut entry = Entry::new(RedisValue::String(Bytes::from("val")));
            entry.expires_at = Some(Instant::now() + Duration::from_secs(3600));
            db.set(Bytes::from("ttlkey"), entry);
        }
        let result = del(&mut ctx, &[bulk("ttlkey")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        assert!(ctx
            .store()
            .database(ctx.selected_db())
            .get(b"ttlkey")
            .is_none());
    }

    // ================================================================
    // EXISTS tests (18 additional)
    // ================================================================

    #[test]
    fn test_exists_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        let result = exists(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_exists_nonexistent() {
        let mut ctx = make_ctx();
        let result = exists(&mut ctx, &[bulk("nosuchkey")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_exists_single_existing() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "mykey", "hello");
        let result = exists(&mut ctx, &[bulk("mykey")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_exists_multiple_all_exist() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "a", "1");
        set_key(&mut ctx, "b", "2");
        set_key(&mut ctx, "c", "3");
        let result = exists(&mut ctx, &[bulk("a"), bulk("b"), bulk("c")]).unwrap();
        assert_eq!(result, RespValue::Integer(3));
    }

    #[test]
    fn test_exists_multiple_some_missing() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "a", "1");
        set_key(&mut ctx, "c", "3");
        let result = exists(&mut ctx, &[bulk("a"), bulk("b"), bulk("c")]).unwrap();
        assert_eq!(result, RespValue::Integer(2));
    }

    #[test]
    fn test_exists_multiple_all_missing() {
        let mut ctx = make_ctx();
        let result = exists(&mut ctx, &[bulk("x"), bulk("y"), bulk("z")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_exists_duplicate_keys() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "dup", "val");
        // Redis counts duplicates: EXISTS dup dup → 2
        let result = exists(&mut ctx, &[bulk("dup"), bulk("dup")]).unwrap();
        assert_eq!(result, RespValue::Integer(2));
    }

    #[test]
    fn test_exists_list_key() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        db.set(
            Bytes::from("lkey"),
            Entry::new(RedisValue::List(VecDeque::from([Bytes::from("a")]))),
        );
        let result = exists(&mut ctx, &[bulk("lkey")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_exists_hash_key() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        db.set(
            Bytes::from("hkey"),
            Entry::new(RedisValue::Hash(HashMap::from([(
                Bytes::from("f"),
                Bytes::from("v"),
            )]))),
        );
        let result = exists(&mut ctx, &[bulk("hkey")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_exists_set_key() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        db.set(
            Bytes::from("skey"),
            Entry::new(RedisValue::Set(HashSet::from([Bytes::from("a")]))),
        );
        let result = exists(&mut ctx, &[bulk("skey")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_exists_zset_key() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut scores = HashMap::new();
        scores.insert(Bytes::from("m1"), 1.0);
        let mut members = BTreeMap::new();
        members.insert((OrderedFloat(1.0), Bytes::from("m1")), ());
        db.set(
            Bytes::from("zkey"),
            Entry::new(RedisValue::SortedSet { scores, members }),
        );
        let result = exists(&mut ctx, &[bulk("zkey")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_exists_expired_key() {
        use std::time::Instant;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut entry = Entry::new(RedisValue::String(Bytes::from("val")));
        entry.expires_at = Some(Instant::now() - Duration::from_secs(1));
        db.set(Bytes::from("expkey"), entry);
        // EXISTS checks expiry; expired key is not counted
        let result = exists(&mut ctx, &[bulk("expkey")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_exists_after_del() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "delme", "val");
        del(&mut ctx, &[bulk("delme")]).unwrap();
        let result = exists(&mut ctx, &[bulk("delme")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_exists_returns_integer_type() {
        let mut ctx = make_ctx();
        let result = exists(&mut ctx, &[bulk("anything")]).unwrap();
        assert!(matches!(result, RespValue::Integer(_)));
    }

    #[test]
    fn test_exists_empty_key_name() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        db.set(
            Bytes::from(""),
            Entry::new(RedisValue::String(Bytes::from("val"))),
        );
        let result = exists(&mut ctx, &[bulk("")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_exists_special_chars_key() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key:with:colons!@#$%", "val");
        let result = exists(&mut ctx, &[bulk("key:with:colons!@#$%")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_exists_with_ttl_valid() {
        use std::time::Instant;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut entry = Entry::new(RedisValue::String(Bytes::from("val")));
        entry.expires_at = Some(Instant::now() + Duration::from_secs(3600));
        db.set(Bytes::from("ttlkey"), entry);
        let result = exists(&mut ctx, &[bulk("ttlkey")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_exists_many_keys() {
        let mut ctx = make_ctx();
        for i in 0..10 {
            set_key(&mut ctx, &format!("k{}", i), &format!("v{}", i));
        }
        let args: Vec<RespValue> = (0..10).map(|i| bulk(&format!("k{}", i))).collect();
        let result = exists(&mut ctx, &args).unwrap();
        assert_eq!(result, RespValue::Integer(10));
    }

    // ================================================================
    // TOUCH tests (19 additional)
    // ================================================================

    #[test]
    fn test_touch_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        let result = touch(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_touch_nonexistent() {
        let mut ctx = make_ctx();
        let result = touch(&mut ctx, &[bulk("nosuchkey")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_touch_single_existing() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "mykey", "hello");
        let result = touch(&mut ctx, &[bulk("mykey")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_touch_multiple_all_exist() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "a", "1");
        set_key(&mut ctx, "b", "2");
        set_key(&mut ctx, "c", "3");
        let result = touch(&mut ctx, &[bulk("a"), bulk("b"), bulk("c")]).unwrap();
        assert_eq!(result, RespValue::Integer(3));
    }

    #[test]
    fn test_touch_multiple_some_missing() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "a", "1");
        set_key(&mut ctx, "c", "3");
        let result = touch(&mut ctx, &[bulk("a"), bulk("b"), bulk("c")]).unwrap();
        assert_eq!(result, RespValue::Integer(2));
    }

    #[test]
    fn test_touch_multiple_all_missing() {
        let mut ctx = make_ctx();
        let result = touch(&mut ctx, &[bulk("x"), bulk("y"), bulk("z")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_touch_duplicate_keys() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "dup", "val");
        // TOUCH counts each occurrence (like Redis)
        let result = touch(&mut ctx, &[bulk("dup"), bulk("dup")]).unwrap();
        assert_eq!(result, RespValue::Integer(2));
    }

    #[test]
    fn test_touch_list_key() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        db.set(
            Bytes::from("lkey"),
            Entry::new(RedisValue::List(VecDeque::from([Bytes::from("a")]))),
        );
        let result = touch(&mut ctx, &[bulk("lkey")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_touch_hash_key() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        db.set(
            Bytes::from("hkey"),
            Entry::new(RedisValue::Hash(HashMap::from([(
                Bytes::from("f"),
                Bytes::from("v"),
            )]))),
        );
        let result = touch(&mut ctx, &[bulk("hkey")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_touch_set_key() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        db.set(
            Bytes::from("skey"),
            Entry::new(RedisValue::Set(HashSet::from([Bytes::from("a")]))),
        );
        let result = touch(&mut ctx, &[bulk("skey")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_touch_zset_key() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut scores = HashMap::new();
        scores.insert(Bytes::from("m1"), 1.0);
        let mut members = BTreeMap::new();
        members.insert((OrderedFloat(1.0), Bytes::from("m1")), ());
        db.set(
            Bytes::from("zkey"),
            Entry::new(RedisValue::SortedSet { scores, members }),
        );
        let result = touch(&mut ctx, &[bulk("zkey")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_touch_expired_key() {
        use std::time::Instant;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut entry = Entry::new(RedisValue::String(Bytes::from("val")));
        entry.expires_at = Some(Instant::now() - Duration::from_secs(1));
        db.set(Bytes::from("expkey"), entry);
        // TOUCH checks expiry; expired key is not counted
        let result = touch(&mut ctx, &[bulk("expkey")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_touch_returns_integer_type() {
        let mut ctx = make_ctx();
        let result = touch(&mut ctx, &[bulk("anything")]).unwrap();
        assert!(matches!(result, RespValue::Integer(_)));
    }

    #[test]
    fn test_touch_empty_key_name() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        db.set(
            Bytes::from(""),
            Entry::new(RedisValue::String(Bytes::from("val"))),
        );
        let result = touch(&mut ctx, &[bulk("")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_touch_special_chars_key() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key:with:colons!@#$%", "val");
        let result = touch(&mut ctx, &[bulk("key:with:colons!@#$%")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_touch_with_ttl_valid() {
        use std::time::Instant;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut entry = Entry::new(RedisValue::String(Bytes::from("val")));
        entry.expires_at = Some(Instant::now() + Duration::from_secs(3600));
        db.set(Bytes::from("ttlkey"), entry);
        let result = touch(&mut ctx, &[bulk("ttlkey")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_touch_many_keys() {
        let mut ctx = make_ctx();
        for i in 0..10 {
            set_key(&mut ctx, &format!("k{}", i), &format!("v{}", i));
        }
        let args: Vec<RespValue> = (0..10).map(|i| bulk(&format!("k{}", i))).collect();
        let result = touch(&mut ctx, &args).unwrap();
        assert_eq!(result, RespValue::Integer(10));
    }

    #[test]
    fn test_touch_does_not_modify_value() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "mykey", "myvalue");
        touch(&mut ctx, &[bulk("mykey")]).unwrap();
        let db = ctx.store().database(ctx.selected_db());
        let entry = db.get(b"mykey").expect("key should exist");
        match &entry.value {
            RedisValue::String(s) => assert_eq!(s.as_ref(), b"myvalue"),
            _ => panic!("Expected string value"),
        }
    }

    #[test]
    fn test_touch_after_del() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "mykey", "val");
        del(&mut ctx, &[bulk("mykey")]).unwrap();
        let result = touch(&mut ctx, &[bulk("mykey")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    // ================================================================
    // UNLINK tests (19 additional)
    // ================================================================

    #[test]
    fn test_unlink_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        let result = unlink(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_unlink_nonexistent() {
        let mut ctx = make_ctx();
        let result = unlink(&mut ctx, &[bulk("nosuchkey")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_unlink_single_existing() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "mykey", "hello");
        let result = unlink(&mut ctx, &[bulk("mykey")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_unlink_multiple_all_exist() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "a", "1");
        set_key(&mut ctx, "b", "2");
        set_key(&mut ctx, "c", "3");
        let result = unlink(&mut ctx, &[bulk("a"), bulk("b"), bulk("c")]).unwrap();
        assert_eq!(result, RespValue::Integer(3));
    }

    #[test]
    fn test_unlink_multiple_some_missing() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "a", "1");
        set_key(&mut ctx, "c", "3");
        let result = unlink(&mut ctx, &[bulk("a"), bulk("b"), bulk("c")]).unwrap();
        assert_eq!(result, RespValue::Integer(2));
    }

    #[test]
    fn test_unlink_multiple_all_missing() {
        let mut ctx = make_ctx();
        let result = unlink(&mut ctx, &[bulk("x"), bulk("y"), bulk("z")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_unlink_duplicate_keys() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "dup", "val");
        // First occurrence removes, second finds it gone
        let result = unlink(&mut ctx, &[bulk("dup"), bulk("dup")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_unlink_actually_removes() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "gone", "val");
        unlink(&mut ctx, &[bulk("gone")]).unwrap();
        let db = ctx.store().database(ctx.selected_db());
        assert!(db.get(b"gone").is_none());
    }

    #[test]
    fn test_unlink_list_key() {
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(ctx.selected_db());
            db.set(
                Bytes::from("lkey"),
                Entry::new(RedisValue::List(VecDeque::from([Bytes::from("a")]))),
            );
        }
        let result = unlink(&mut ctx, &[bulk("lkey")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        assert!(ctx
            .store()
            .database(ctx.selected_db())
            .get(b"lkey")
            .is_none());
    }

    #[test]
    fn test_unlink_hash_key() {
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(ctx.selected_db());
            db.set(
                Bytes::from("hkey"),
                Entry::new(RedisValue::Hash(HashMap::from([(
                    Bytes::from("f"),
                    Bytes::from("v"),
                )]))),
            );
        }
        let result = unlink(&mut ctx, &[bulk("hkey")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        assert!(ctx
            .store()
            .database(ctx.selected_db())
            .get(b"hkey")
            .is_none());
    }

    #[test]
    fn test_unlink_set_key() {
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(ctx.selected_db());
            db.set(
                Bytes::from("skey"),
                Entry::new(RedisValue::Set(HashSet::from([Bytes::from("a")]))),
            );
        }
        let result = unlink(&mut ctx, &[bulk("skey")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        assert!(ctx
            .store()
            .database(ctx.selected_db())
            .get(b"skey")
            .is_none());
    }

    #[test]
    fn test_unlink_zset_key() {
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(ctx.selected_db());
            let mut scores = HashMap::new();
            scores.insert(Bytes::from("m1"), 1.0);
            let mut members = BTreeMap::new();
            members.insert((OrderedFloat(1.0), Bytes::from("m1")), ());
            db.set(
                Bytes::from("zkey"),
                Entry::new(RedisValue::SortedSet { scores, members }),
            );
        }
        let result = unlink(&mut ctx, &[bulk("zkey")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        assert!(ctx
            .store()
            .database(ctx.selected_db())
            .get(b"zkey")
            .is_none());
    }

    #[test]
    fn test_unlink_expired_key() {
        use std::time::Instant;
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(ctx.selected_db());
            let mut entry = Entry::new(RedisValue::String(Bytes::from("val")));
            entry.expires_at = Some(Instant::now() - Duration::from_secs(1));
            db.set(Bytes::from("expkey"), entry);
        }
        // UNLINK delegates to DEL which physically removes without expiry check
        let result = unlink(&mut ctx, &[bulk("expkey")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_unlink_returns_integer_type() {
        let mut ctx = make_ctx();
        let result = unlink(&mut ctx, &[bulk("anything")]).unwrap();
        assert!(matches!(result, RespValue::Integer(_)));
    }

    #[test]
    fn test_unlink_empty_key_name() {
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(ctx.selected_db());
            db.set(
                Bytes::from(""),
                Entry::new(RedisValue::String(Bytes::from("val"))),
            );
        }
        let result = unlink(&mut ctx, &[bulk("")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_unlink_special_chars_key() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key:with:colons!@#$%", "val");
        let result = unlink(&mut ctx, &[bulk("key:with:colons!@#$%")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_unlink_does_not_affect_other_keys() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "keep", "safe");
        set_key(&mut ctx, "remove", "gone");
        unlink(&mut ctx, &[bulk("remove")]).unwrap();
        let db = ctx.store().database(ctx.selected_db());
        assert!(db.get(b"keep").is_some());
        assert!(db.get(b"remove").is_none());
    }

    #[test]
    fn test_unlink_with_ttl() {
        use std::time::Instant;
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(ctx.selected_db());
            let mut entry = Entry::new(RedisValue::String(Bytes::from("val")));
            entry.expires_at = Some(Instant::now() + Duration::from_secs(3600));
            db.set(Bytes::from("ttlkey"), entry);
        }
        let result = unlink(&mut ctx, &[bulk("ttlkey")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        assert!(ctx
            .store()
            .database(ctx.selected_db())
            .get(b"ttlkey")
            .is_none());
    }

    #[test]
    fn test_unlink_many_keys() {
        let mut ctx = make_ctx();
        for i in 0..10 {
            set_key(&mut ctx, &format!("k{}", i), &format!("v{}", i));
        }
        let args: Vec<RespValue> = (0..10).map(|i| bulk(&format!("k{}", i))).collect();
        let result = unlink(&mut ctx, &args).unwrap();
        assert_eq!(result, RespValue::Integer(10));
        let db = ctx.store().database(ctx.selected_db());
        for i in 0..10 {
            assert!(db.get(format!("k{}", i).as_bytes()).is_none());
        }
    }

    // ================================================================
    // Additional COPY tests (13 new)
    // ================================================================

    #[test]
    fn test_copy_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        let result = copy(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_copy_wrong_arity_one_arg() {
        let mut ctx = make_ctx();
        let result = copy(&mut ctx, &[bulk("src")]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_copy_basic() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "csrc", "hello");

        let result = copy(&mut ctx, &[bulk("csrc"), bulk("cdst")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let db = ctx.store().database(0);
        let entry = db.get(b"cdst").unwrap();
        if let RedisValue::String(ref s) = entry.value {
            assert_eq!(s.as_ref(), b"hello");
        } else {
            panic!("Expected string value at cdst");
        }
    }

    #[test]
    fn test_copy_preserves_source() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "cps_src", "original");

        copy(&mut ctx, &[bulk("cps_src"), bulk("cps_dst")]).unwrap();

        let db = ctx.store().database(0);
        let entry = db.get(b"cps_src").unwrap();
        if let RedisValue::String(ref s) = entry.value {
            assert_eq!(s.as_ref(), b"original");
        } else {
            panic!("Expected string value at cps_src");
        }
    }

    #[test]
    fn test_copy_dest_independent() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "cdi_src", "initial");

        copy(&mut ctx, &[bulk("cdi_src"), bulk("cdi_dst")]).unwrap();

        // Overwrite source with a new value
        set_key(&mut ctx, "cdi_src", "changed");

        let db = ctx.store().database(0);
        let entry = db.get(b"cdi_dst").unwrap();
        if let RedisValue::String(ref s) = entry.value {
            assert_eq!(s.as_ref(), b"initial");
        } else {
            panic!("Expected string at cdi_dst");
        }
    }

    #[test]
    fn test_copy_with_ttl() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "ct_src", "value");

        expire(&mut ctx, &[bulk("ct_src"), bulk("300")]).unwrap();

        copy(&mut ctx, &[bulk("ct_src"), bulk("ct_dst")]).unwrap();

        let result = ttl(&mut ctx, &[bulk("ct_dst")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t > 0 && t <= 300, "Expected positive TTL, got {t}");
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_copy_list_type() {
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(0);
            let mut list = VecDeque::new();
            list.push_back(Bytes::from("a"));
            list.push_back(Bytes::from("b"));
            list.push_back(Bytes::from("c"));
            db.set(Bytes::from("cl_src"), Entry::new(RedisValue::List(list)));
        }

        let result = copy(&mut ctx, &[bulk("cl_src"), bulk("cl_dst")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let db = ctx.store().database(0);
        let entry = db.get(b"cl_dst").unwrap();
        if let RedisValue::List(ref l) = entry.value {
            assert_eq!(l.len(), 3);
        } else {
            panic!("Expected list type at cl_dst");
        }
    }

    #[test]
    fn test_copy_hash_type() {
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(0);
            let mut hash = HashMap::new();
            hash.insert(Bytes::from("f1"), Bytes::from("v1"));
            hash.insert(Bytes::from("f2"), Bytes::from("v2"));
            db.set(Bytes::from("ch_src"), Entry::new(RedisValue::Hash(hash)));
        }

        let result = copy(&mut ctx, &[bulk("ch_src"), bulk("ch_dst")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let db = ctx.store().database(0);
        let entry = db.get(b"ch_dst").unwrap();
        if let RedisValue::Hash(ref h) = entry.value {
            assert_eq!(h.len(), 2);
        } else {
            panic!("Expected hash type at ch_dst");
        }
    }

    #[test]
    fn test_copy_set_type() {
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(0);
            let mut set = HashSet::new();
            set.insert(Bytes::from("x"));
            set.insert(Bytes::from("y"));
            db.set(Bytes::from("cs_src"), Entry::new(RedisValue::Set(set)));
        }

        let result = copy(&mut ctx, &[bulk("cs_src"), bulk("cs_dst")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let db = ctx.store().database(0);
        let entry = db.get(b"cs_dst").unwrap();
        if let RedisValue::Set(ref s) = entry.value {
            assert_eq!(s.len(), 2);
        } else {
            panic!("Expected set type at cs_dst");
        }
    }

    #[test]
    fn test_copy_returns_integer_type() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "crit_src", "value");

        let result = copy(&mut ctx, &[bulk("crit_src"), bulk("crit_dst")]).unwrap();
        assert!(matches!(result, RespValue::Integer(_)));

        let result = copy(&mut ctx, &[bulk("nonexistent_crit"), bulk("crit_dst2")]).unwrap();
        assert!(matches!(result, RespValue::Integer(0)));
    }

    #[test]
    fn test_copy_expired_source() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "cexp_src", "value");

        {
            let db = ctx.store().database(0);
            let mut entry = db.get(b"cexp_src").unwrap();
            entry.expires_at = Some(std::time::Instant::now() - Duration::from_secs(1));
            db.set(Bytes::from("cexp_src"), entry);
        }

        let result = copy(&mut ctx, &[bulk("cexp_src"), bulk("cexp_dst")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_copy_empty_key() {
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(0);
            db.set(
                Bytes::from(""),
                Entry::new(RedisValue::String(Bytes::from("emptykey"))),
            );
        }

        let result = copy(&mut ctx, &[bulk(""), bulk("cek_dst")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let db = ctx.store().database(0);
        assert!(db.exists(b"cek_dst"));
    }

    #[test]
    fn test_copy_special_chars() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "csc:with:colons", "val");

        let result = copy(
            &mut ctx,
            &[bulk("csc:with:colons"), bulk("csc{with}braces")],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let db = ctx.store().database(0);
        assert!(db.exists(b"csc{with}braces"));
    }

    // ================================================================
    // Additional OBJECT tests (8 new)
    // ================================================================

    #[test]
    fn test_object_encoding_empty_string() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "obj_emptystr", "");

        let result = object(&mut ctx, &[bulk("ENCODING"), bulk("obj_emptystr")]).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from("embstr")));
    }

    #[test]
    fn test_object_encoding_int_string() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "obj_intkey", "12345");

        let result = object(&mut ctx, &[bulk("ENCODING"), bulk("obj_intkey")]).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from("int")));
    }

    #[test]
    fn test_object_refcount_nonexistent() {
        let mut ctx = make_ctx();
        // REFCOUNT always returns 1 regardless of key existence (current impl)
        let result = object(&mut ctx, &[bulk("REFCOUNT"), bulk("obj_nosuchkey")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_object_idletime_nonexistent() {
        let mut ctx = make_ctx();
        let result = object(&mut ctx, &[bulk("IDLETIME"), bulk("obj_nosuchkey")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_object_encoding_large_string() {
        let mut ctx = make_ctx();
        let long_str = "x".repeat(200);
        set_key(&mut ctx, "obj_bigstr", &long_str);

        let result = object(&mut ctx, &[bulk("ENCODING"), bulk("obj_bigstr")]).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from("raw")));
    }

    #[test]
    fn test_object_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        let result = object(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_object_encoding_expired_key() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "obj_expkey", "value");

        {
            let db = ctx.store().database(0);
            let mut entry = db.get(b"obj_expkey").unwrap();
            entry.expires_at = Some(std::time::Instant::now() - Duration::from_secs(1));
            db.set(Bytes::from("obj_expkey"), entry);
        }

        let result = object(&mut ctx, &[bulk("ENCODING"), bulk("obj_expkey")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_object_encoding_empty_set() {
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(0);
            let empty_set: HashSet<Bytes> = HashSet::new();
            db.set(
                Bytes::from("obj_emptyset"),
                Entry::new(RedisValue::Set(empty_set)),
            );
        }

        let result = object(&mut ctx, &[bulk("ENCODING"), bulk("obj_emptyset")]).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from("listpack")));
    }

    // ================================================================
    // EXPIRETIME tests (20 new)
    // ================================================================

    #[test]
    fn test_expiretime_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        let result = expiretime(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_expiretime_nonexistent_key() {
        let mut ctx = make_ctx();
        let result = expiretime(&mut ctx, &[bulk("et_nosuchkey")]).unwrap();
        assert_eq!(result, RespValue::Integer(-2));
    }

    #[test]
    fn test_expiretime_no_ttl() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "et_key", "value");

        let result = expiretime(&mut ctx, &[bulk("et_key")]).unwrap();
        assert_eq!(result, RespValue::Integer(-1));
    }

    #[test]
    fn test_expiretime_with_ttl() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "et_ttl", "value");
        expire(&mut ctx, &[bulk("et_ttl"), bulk("300")]).unwrap();

        let result = expiretime(&mut ctx, &[bulk("et_ttl")]).unwrap();
        if let RespValue::Integer(ts) = result {
            assert!(ts > 0, "Expected positive timestamp, got {ts}");
            let now_secs = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            assert!(ts >= now_secs + 290 && ts <= now_secs + 310);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_expiretime_on_list_key_no_ttl() {
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(0);
            db.set(Bytes::from("et_list"), Entry::new(RedisValue::new_list()));
        }

        let result = expiretime(&mut ctx, &[bulk("et_list")]).unwrap();
        assert_eq!(result, RespValue::Integer(-1));
    }

    #[test]
    fn test_expiretime_on_expired_key() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "et_exp", "value");

        {
            let db = ctx.store().database(0);
            let mut entry = db.get(b"et_exp").unwrap();
            entry.expires_at = Some(std::time::Instant::now() - Duration::from_secs(1));
            db.set(Bytes::from("et_exp"), entry);
        }

        let result = expiretime(&mut ctx, &[bulk("et_exp")]).unwrap();
        assert_eq!(result, RespValue::Integer(-2));
    }

    #[test]
    fn test_expiretime_returns_integer_type() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "et_int", "value");

        let result = expiretime(&mut ctx, &[bulk("et_int")]).unwrap();
        assert!(matches!(result, RespValue::Integer(_)));
    }

    #[test]
    fn test_expiretime_after_expire() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "et_ae", "value");

        expire(&mut ctx, &[bulk("et_ae"), bulk("600")]).unwrap();

        let result = expiretime(&mut ctx, &[bulk("et_ae")]).unwrap();
        if let RespValue::Integer(ts) = result {
            let now_secs = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            assert!(ts >= now_secs + 590 && ts <= now_secs + 610);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_expiretime_after_pexpire() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "et_pe", "value");

        pexpire(&mut ctx, &[bulk("et_pe"), bulk("60000")]).unwrap();

        let result = expiretime(&mut ctx, &[bulk("et_pe")]).unwrap();
        if let RespValue::Integer(ts) = result {
            let now_secs = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            assert!(ts >= now_secs + 55 && ts <= now_secs + 65);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_expiretime_after_persist() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "et_per", "value");

        expire(&mut ctx, &[bulk("et_per"), bulk("100")]).unwrap();
        persist(&mut ctx, &[bulk("et_per")]).unwrap();

        let result = expiretime(&mut ctx, &[bulk("et_per")]).unwrap();
        assert_eq!(result, RespValue::Integer(-1));
    }

    #[test]
    fn test_expiretime_empty_key() {
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(0);
            db.set(
                Bytes::from(""),
                Entry::new(RedisValue::String(Bytes::from("et_empty"))),
            );
        }

        let result = expiretime(&mut ctx, &[bulk("")]).unwrap();
        assert_eq!(result, RespValue::Integer(-1));
    }

    #[test]
    fn test_expiretime_special_chars() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "et_sp:!@#", "value");

        expire(&mut ctx, &[bulk("et_sp:!@#"), bulk("500")]).unwrap();

        let result = expiretime(&mut ctx, &[bulk("et_sp:!@#")]).unwrap();
        if let RespValue::Integer(ts) = result {
            assert!(ts > 0);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_expiretime_on_hash_key() {
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(0);
            let mut hash = HashMap::new();
            hash.insert(Bytes::from("f1"), Bytes::from("v1"));
            db.set(Bytes::from("et_hash"), Entry::new(RedisValue::Hash(hash)));
        }

        expire(&mut ctx, &[bulk("et_hash"), bulk("200")]).unwrap();

        let result = expiretime(&mut ctx, &[bulk("et_hash")]).unwrap();
        if let RespValue::Integer(ts) = result {
            let now_secs = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            assert!(ts >= now_secs + 190 && ts <= now_secs + 210);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_expiretime_on_set_key() {
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(0);
            let mut set = HashSet::new();
            set.insert(Bytes::from("m1"));
            db.set(Bytes::from("et_set"), Entry::new(RedisValue::Set(set)));
        }

        expire(&mut ctx, &[bulk("et_set"), bulk("150")]).unwrap();

        let result = expiretime(&mut ctx, &[bulk("et_set")]).unwrap();
        if let RespValue::Integer(ts) = result {
            let now_secs = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            assert!(ts >= now_secs + 140 && ts <= now_secs + 160);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_expiretime_timestamp_is_future() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "et_fut", "value");

        expire(&mut ctx, &[bulk("et_fut"), bulk("100")]).unwrap();

        let result = expiretime(&mut ctx, &[bulk("et_fut")]).unwrap();
        if let RespValue::Integer(ts) = result {
            let now_secs = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            assert!(ts > now_secs, "Expected future timestamp");
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_expiretime_different_keys() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "et_dk1", "v1");
        set_key(&mut ctx, "et_dk2", "v2");

        expire(&mut ctx, &[bulk("et_dk1"), bulk("100")]).unwrap();
        expire(&mut ctx, &[bulk("et_dk2"), bulk("500")]).unwrap();

        let r1 = expiretime(&mut ctx, &[bulk("et_dk1")]).unwrap();
        let r2 = expiretime(&mut ctx, &[bulk("et_dk2")]).unwrap();

        if let (RespValue::Integer(ts1), RespValue::Integer(ts2)) = (&r1, &r2) {
            assert!(ts2 > ts1, "et_dk2 should expire later than et_dk1");
        } else {
            panic!("Expected integers");
        }
    }

    #[test]
    fn test_expiretime_after_set_with_ex() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "et_ex", "value");

        let future_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 1000;
        expireat(&mut ctx, &[bulk("et_ex"), bulk(&future_ts.to_string())]).unwrap();

        let result = expiretime(&mut ctx, &[bulk("et_ex")]).unwrap();
        if let RespValue::Integer(ts) = result {
            assert!((ts - future_ts as i64).unsigned_abs() <= 2);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_expiretime_large_ttl() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "et_large", "value");

        expire(&mut ctx, &[bulk("et_large"), bulk("31536000")]).unwrap();

        let result = expiretime(&mut ctx, &[bulk("et_large")]).unwrap();
        if let RespValue::Integer(ts) = result {
            let now_secs = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            assert!(ts >= now_secs + 31535990);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_expiretime_after_rename() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "et_old", "value");

        expire(&mut ctx, &[bulk("et_old"), bulk("500")]).unwrap();
        rename(&mut ctx, &[bulk("et_old"), bulk("et_new")]).unwrap();

        let result = expiretime(&mut ctx, &[bulk("et_old")]).unwrap();
        assert_eq!(result, RespValue::Integer(-2));

        let result = expiretime(&mut ctx, &[bulk("et_new")]).unwrap();
        if let RespValue::Integer(ts) = result {
            assert!(ts > 0, "Expected positive timestamp after rename");
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_expiretime_on_string_key_with_ttl() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "et_str", "hello world");

        expire(&mut ctx, &[bulk("et_str"), bulk("250")]).unwrap();

        let result = expiretime(&mut ctx, &[bulk("et_str")]).unwrap();
        if let RespValue::Integer(ts) = result {
            let now_secs = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            assert!(ts >= now_secs + 240 && ts <= now_secs + 260);
        } else {
            panic!("Expected integer");
        }
    }

    // ================================================================
    // PEXPIRETIME tests (20 new)
    // ================================================================

    #[test]
    fn test_pexpiretime_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        let result = pexpiretime(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_pexpiretime_nonexistent_key() {
        let mut ctx = make_ctx();
        let result = pexpiretime(&mut ctx, &[bulk("pet_nosuchkey")]).unwrap();
        assert_eq!(result, RespValue::Integer(-2));
    }

    #[test]
    fn test_pexpiretime_no_ttl() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "pet_key", "value");

        let result = pexpiretime(&mut ctx, &[bulk("pet_key")]).unwrap();
        assert_eq!(result, RespValue::Integer(-1));
    }

    #[test]
    fn test_pexpiretime_with_ttl() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "pet_ttl", "value");
        pexpire(&mut ctx, &[bulk("pet_ttl"), bulk("300000")]).unwrap();

        let result = pexpiretime(&mut ctx, &[bulk("pet_ttl")]).unwrap();
        if let RespValue::Integer(ts_ms) = result {
            assert!(ts_ms > 0, "Expected positive ms timestamp, got {ts_ms}");
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            assert!(ts_ms >= now_ms + 290_000 && ts_ms <= now_ms + 310_000);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_pexpiretime_on_list_key_no_ttl() {
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(0);
            db.set(Bytes::from("pet_list"), Entry::new(RedisValue::new_list()));
        }

        let result = pexpiretime(&mut ctx, &[bulk("pet_list")]).unwrap();
        assert_eq!(result, RespValue::Integer(-1));
    }

    #[test]
    fn test_pexpiretime_on_expired_key() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "pet_exp", "value");

        {
            let db = ctx.store().database(0);
            let mut entry = db.get(b"pet_exp").unwrap();
            entry.expires_at = Some(std::time::Instant::now() - Duration::from_secs(1));
            db.set(Bytes::from("pet_exp"), entry);
        }

        let result = pexpiretime(&mut ctx, &[bulk("pet_exp")]).unwrap();
        assert_eq!(result, RespValue::Integer(-2));
    }

    #[test]
    fn test_pexpiretime_returns_integer_type() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "pet_int", "value");

        let result = pexpiretime(&mut ctx, &[bulk("pet_int")]).unwrap();
        assert!(matches!(result, RespValue::Integer(_)));
    }

    #[test]
    fn test_pexpiretime_after_pexpire() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "pet_ape", "value");

        pexpire(&mut ctx, &[bulk("pet_ape"), bulk("120000")]).unwrap();

        let result = pexpiretime(&mut ctx, &[bulk("pet_ape")]).unwrap();
        if let RespValue::Integer(ts_ms) = result {
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            assert!(ts_ms >= now_ms + 115_000 && ts_ms <= now_ms + 125_000);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_pexpiretime_after_expire() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "pet_ae", "value");

        expire(&mut ctx, &[bulk("pet_ae"), bulk("60")]).unwrap();

        let result = pexpiretime(&mut ctx, &[bulk("pet_ae")]).unwrap();
        if let RespValue::Integer(ts_ms) = result {
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            assert!(ts_ms >= now_ms + 55_000 && ts_ms <= now_ms + 65_000);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_pexpiretime_after_persist() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "pet_per", "value");

        pexpire(&mut ctx, &[bulk("pet_per"), bulk("100000")]).unwrap();
        persist(&mut ctx, &[bulk("pet_per")]).unwrap();

        let result = pexpiretime(&mut ctx, &[bulk("pet_per")]).unwrap();
        assert_eq!(result, RespValue::Integer(-1));
    }

    #[test]
    fn test_pexpiretime_empty_key() {
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(0);
            db.set(
                Bytes::from(""),
                Entry::new(RedisValue::String(Bytes::from("pet_empty"))),
            );
        }

        let result = pexpiretime(&mut ctx, &[bulk("")]).unwrap();
        assert_eq!(result, RespValue::Integer(-1));
    }

    #[test]
    fn test_pexpiretime_special_chars() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "pet_sp\t\n", "value");

        pexpire(&mut ctx, &[bulk("pet_sp\t\n"), bulk("50000")]).unwrap();

        let result = pexpiretime(&mut ctx, &[bulk("pet_sp\t\n")]).unwrap();
        if let RespValue::Integer(ts_ms) = result {
            assert!(ts_ms > 0);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_pexpiretime_on_hash_key() {
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(0);
            let mut hash = HashMap::new();
            hash.insert(Bytes::from("f1"), Bytes::from("v1"));
            db.set(Bytes::from("pet_hash"), Entry::new(RedisValue::Hash(hash)));
        }

        pexpire(&mut ctx, &[bulk("pet_hash"), bulk("200000")]).unwrap();

        let result = pexpiretime(&mut ctx, &[bulk("pet_hash")]).unwrap();
        if let RespValue::Integer(ts_ms) = result {
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            assert!(ts_ms >= now_ms + 190_000 && ts_ms <= now_ms + 210_000);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_pexpiretime_on_set_key() {
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(0);
            let mut set = HashSet::new();
            set.insert(Bytes::from("m1"));
            db.set(Bytes::from("pet_set"), Entry::new(RedisValue::Set(set)));
        }

        pexpire(&mut ctx, &[bulk("pet_set"), bulk("150000")]).unwrap();

        let result = pexpiretime(&mut ctx, &[bulk("pet_set")]).unwrap();
        if let RespValue::Integer(ts_ms) = result {
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            assert!(ts_ms >= now_ms + 140_000 && ts_ms <= now_ms + 160_000);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_pexpiretime_timestamp_is_future() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "pet_fut", "value");

        pexpire(&mut ctx, &[bulk("pet_fut"), bulk("100000")]).unwrap();

        let result = pexpiretime(&mut ctx, &[bulk("pet_fut")]).unwrap();
        if let RespValue::Integer(ts_ms) = result {
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            assert!(ts_ms > now_ms, "Expected future ms timestamp");
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_pexpiretime_different_keys() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "pet_dk1", "v1");
        set_key(&mut ctx, "pet_dk2", "v2");

        pexpire(&mut ctx, &[bulk("pet_dk1"), bulk("10000")]).unwrap();
        pexpire(&mut ctx, &[bulk("pet_dk2"), bulk("500000")]).unwrap();

        let r1 = pexpiretime(&mut ctx, &[bulk("pet_dk1")]).unwrap();
        let r2 = pexpiretime(&mut ctx, &[bulk("pet_dk2")]).unwrap();

        if let (RespValue::Integer(ts1), RespValue::Integer(ts2)) = (&r1, &r2) {
            assert!(ts2 > ts1, "pet_dk2 should expire later than pet_dk1");
        } else {
            panic!("Expected integers");
        }
    }

    #[test]
    fn test_pexpiretime_after_set_with_px() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "pet_px", "value");

        let future_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + 1_000_000;
        pexpireat(&mut ctx, &[bulk("pet_px"), bulk(&future_ms.to_string())]).unwrap();

        let result = pexpiretime(&mut ctx, &[bulk("pet_px")]).unwrap();
        if let RespValue::Integer(ts_ms) = result {
            assert!((ts_ms - future_ms).unsigned_abs() <= 100);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_pexpiretime_large_ttl() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "pet_large", "value");

        let one_year_ms = 31_536_000_000i64;
        pexpire(
            &mut ctx,
            &[bulk("pet_large"), bulk(&one_year_ms.to_string())],
        )
        .unwrap();

        let result = pexpiretime(&mut ctx, &[bulk("pet_large")]).unwrap();
        if let RespValue::Integer(ts_ms) = result {
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            assert!(ts_ms >= now_ms + one_year_ms - 10_000);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_pexpiretime_ms_precision() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "pet_prec", "value");

        pexpire(&mut ctx, &[bulk("pet_prec"), bulk("123456")]).unwrap();

        let result = pexpiretime(&mut ctx, &[bulk("pet_prec")]).unwrap();
        if let RespValue::Integer(ts_ms) = result {
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            let diff = ts_ms - (now_ms + 123_456);
            assert!(
                diff.abs() <= 500,
                "Expected ~now+123456ms, diff was {diff}ms"
            );
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_pexpiretime_on_string_key_with_ttl() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "pet_str", "hello world");

        pexpire(&mut ctx, &[bulk("pet_str"), bulk("250000")]).unwrap();

        let result = pexpiretime(&mut ctx, &[bulk("pet_str")]).unwrap();
        if let RespValue::Integer(ts_ms) = result {
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            assert!(ts_ms >= now_ms + 240_000 && ts_ms <= now_ms + 260_000);
        } else {
            panic!("Expected integer");
        }
    }
    // ================================================================
    // TTL tests (19 additional)
    // ================================================================

    #[test]
    fn test_ttl_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        let result = ttl(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_ttl_nonexistent_key_returns_minus_2() {
        let mut ctx = make_ctx();
        let result = ttl(&mut ctx, &[bulk("no_such_key")]).unwrap();
        assert_eq!(result, RespValue::Integer(-2));
    }

    #[test]
    fn test_ttl_no_expire() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "mykey", "val");
        let result = ttl(&mut ctx, &[bulk("mykey")]).unwrap();
        assert_eq!(result, RespValue::Integer(-1));
    }

    #[test]
    fn test_ttl_with_expire() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "mykey", "val");
        expire(&mut ctx, &[bulk("mykey"), bulk("300")]).unwrap();
        let result = ttl(&mut ctx, &[bulk("mykey")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t > 0 && t <= 300);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_ttl_on_list_key_no_expire() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(0);
        db.set(
            Bytes::from("mylist"),
            Entry::new(RedisValue::List(VecDeque::from([Bytes::from("a")]))),
        );
        let result = ttl(&mut ctx, &[bulk("mylist")]).unwrap();
        assert_eq!(result, RespValue::Integer(-1));
    }

    #[test]
    fn test_ttl_on_expired_key() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(0);
        let mut entry = Entry::new(RedisValue::String(Bytes::from("val")));
        entry.expires_at = Some(std::time::Instant::now() - std::time::Duration::from_secs(1));
        db.set(Bytes::from("expired"), entry);
        let result = ttl(&mut ctx, &[bulk("expired")]).unwrap();
        assert_eq!(result, RespValue::Integer(-2));
    }

    #[test]
    fn test_ttl_returns_integer_type() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "k", "v");
        let result = ttl(&mut ctx, &[bulk("k")]).unwrap();
        assert!(matches!(result, RespValue::Integer(_)));
    }

    #[test]
    fn test_ttl_after_persist() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "k", "v");
        expire(&mut ctx, &[bulk("k"), bulk("100")]).unwrap();
        persist(&mut ctx, &[bulk("k")]).unwrap();
        let result = ttl(&mut ctx, &[bulk("k")]).unwrap();
        assert_eq!(result, RespValue::Integer(-1));
    }

    #[test]
    fn test_ttl_after_set_with_ex() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "k", "v");
        expire(&mut ctx, &[bulk("k"), bulk("60")]).unwrap();
        let result = ttl(&mut ctx, &[bulk("k")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t > 0 && t <= 60);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_ttl_empty_key() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(0);
        db.set(
            Bytes::from(""),
            Entry::new(RedisValue::String(Bytes::from("val"))),
        );
        let result = ttl(&mut ctx, &[bulk("")]).unwrap();
        assert_eq!(result, RespValue::Integer(-1));
    }

    #[test]
    fn test_ttl_special_chars_key() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key:with:colons!@#$", "val");
        let result = ttl(&mut ctx, &[bulk("key:with:colons!@#$")]).unwrap();
        assert_eq!(result, RespValue::Integer(-1));
    }

    #[test]
    fn test_ttl_after_expire_command() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "mykey", "val");
        assert_eq!(
            ttl(&mut ctx, &[bulk("mykey")]).unwrap(),
            RespValue::Integer(-1)
        );
        expire(&mut ctx, &[bulk("mykey"), bulk("500")]).unwrap();
        if let RespValue::Integer(t) = ttl(&mut ctx, &[bulk("mykey")]).unwrap() {
            assert!(t > 0 && t <= 500);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_ttl_different_keys() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "k1", "v1");
        set_key(&mut ctx, "k2", "v2");
        expire(&mut ctx, &[bulk("k1"), bulk("100")]).unwrap();
        expire(&mut ctx, &[bulk("k2"), bulk("200")]).unwrap();
        if let RespValue::Integer(t1) = ttl(&mut ctx, &[bulk("k1")]).unwrap() {
            if let RespValue::Integer(t2) = ttl(&mut ctx, &[bulk("k2")]).unwrap() {
                assert!(t1 <= 100);
                assert!(t2 <= 200);
                assert!(t2 > t1);
            } else {
                panic!("Expected integer");
            }
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_ttl_large_ttl() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "k", "v");
        expire(&mut ctx, &[bulk("k"), bulk("999999")]).unwrap();
        if let RespValue::Integer(t) = ttl(&mut ctx, &[bulk("k")]).unwrap() {
            assert!(t > 999_000);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_ttl_one_second() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "k", "v");
        expire(&mut ctx, &[bulk("k"), bulk("1")]).unwrap();
        if let RespValue::Integer(t) = ttl(&mut ctx, &[bulk("k")]).unwrap() {
            assert!(t >= 0 && t <= 1);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_ttl_on_hash_key() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(0);
        db.set(
            Bytes::from("myhash"),
            Entry::new(RedisValue::Hash(HashMap::from([(
                Bytes::from("f"),
                Bytes::from("v"),
            )]))),
        );
        let result = ttl(&mut ctx, &[bulk("myhash")]).unwrap();
        assert_eq!(result, RespValue::Integer(-1));
    }

    #[test]
    fn test_ttl_on_set_key() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(0);
        db.set(
            Bytes::from("myset"),
            Entry::new(RedisValue::Set(HashSet::from([Bytes::from("a")]))),
        );
        let result = ttl(&mut ctx, &[bulk("myset")]).unwrap();
        assert_eq!(result, RespValue::Integer(-1));
    }

    #[test]
    fn test_ttl_on_zset_key() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(0);
        let mut scores = HashMap::new();
        let mut members = BTreeMap::new();
        scores.insert(Bytes::from("m"), 1.0);
        members.insert((OrderedFloat(1.0), Bytes::from("m")), ());
        db.set(
            Bytes::from("myzset"),
            Entry::new(RedisValue::SortedSet { scores, members }),
        );
        let result = ttl(&mut ctx, &[bulk("myzset")]).unwrap();
        assert_eq!(result, RespValue::Integer(-1));
    }

    #[test]
    fn test_ttl_after_rename() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "orig", "val");
        expire(&mut ctx, &[bulk("orig"), bulk("500")]).unwrap();
        let before = if let RespValue::Integer(t) = ttl(&mut ctx, &[bulk("orig")]).unwrap() {
            t
        } else {
            panic!("Expected integer");
        };
        rename(&mut ctx, &[bulk("orig"), bulk("newname")]).unwrap();
        assert_eq!(
            ttl(&mut ctx, &[bulk("orig")]).unwrap(),
            RespValue::Integer(-2)
        );
        if let RespValue::Integer(t) = ttl(&mut ctx, &[bulk("newname")]).unwrap() {
            assert!(t > 0);
            assert!((t - before).abs() <= 1);
        } else {
            panic!("Expected integer");
        }
    }

    // ================================================================
    // PTTL tests (20 additional)
    // ================================================================

    #[test]
    fn test_pttl_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        let result = pttl(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_pttl_nonexistent_key() {
        let mut ctx = make_ctx();
        let result = pttl(&mut ctx, &[bulk("no_such_key")]).unwrap();
        assert_eq!(result, RespValue::Integer(-2));
    }

    #[test]
    fn test_pttl_no_expire() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "mykey", "val");
        let result = pttl(&mut ctx, &[bulk("mykey")]).unwrap();
        assert_eq!(result, RespValue::Integer(-1));
    }

    #[test]
    fn test_pttl_with_expire() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "mykey", "val");
        pexpire(&mut ctx, &[bulk("mykey"), bulk("50000")]).unwrap();
        let result = pttl(&mut ctx, &[bulk("mykey")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t > 0 && t <= 50000);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_pttl_on_list_key_no_expire() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(0);
        db.set(
            Bytes::from("mylist"),
            Entry::new(RedisValue::List(VecDeque::from([Bytes::from("a")]))),
        );
        let result = pttl(&mut ctx, &[bulk("mylist")]).unwrap();
        assert_eq!(result, RespValue::Integer(-1));
    }

    #[test]
    fn test_pttl_on_expired_key() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(0);
        let mut entry = Entry::new(RedisValue::String(Bytes::from("val")));
        entry.expires_at = Some(std::time::Instant::now() - std::time::Duration::from_secs(1));
        db.set(Bytes::from("expired"), entry);
        let result = pttl(&mut ctx, &[bulk("expired")]).unwrap();
        assert_eq!(result, RespValue::Integer(-2));
    }

    #[test]
    fn test_pttl_returns_integer_type() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "k", "v");
        let result = pttl(&mut ctx, &[bulk("k")]).unwrap();
        assert!(matches!(result, RespValue::Integer(_)));
    }

    #[test]
    fn test_pttl_after_persist() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "k", "v");
        pexpire(&mut ctx, &[bulk("k"), bulk("60000")]).unwrap();
        persist(&mut ctx, &[bulk("k")]).unwrap();
        let result = pttl(&mut ctx, &[bulk("k")]).unwrap();
        assert_eq!(result, RespValue::Integer(-1));
    }

    #[test]
    fn test_pttl_after_pexpire() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "k", "v");
        pexpire(&mut ctx, &[bulk("k"), bulk("12345")]).unwrap();
        if let RespValue::Integer(t) = pttl(&mut ctx, &[bulk("k")]).unwrap() {
            assert!(t > 0 && t <= 12345);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_pttl_empty_key() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(0);
        db.set(
            Bytes::from(""),
            Entry::new(RedisValue::String(Bytes::from("val"))),
        );
        let result = pttl(&mut ctx, &[bulk("")]).unwrap();
        assert_eq!(result, RespValue::Integer(-1));
    }

    #[test]
    fn test_pttl_special_chars_key() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key:!@#$%^&*()", "val");
        let result = pttl(&mut ctx, &[bulk("key:!@#$%^&*()")]).unwrap();
        assert_eq!(result, RespValue::Integer(-1));
    }

    #[test]
    fn test_pttl_precision() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "k", "v");
        expire(&mut ctx, &[bulk("k"), bulk("10")]).unwrap();
        if let RespValue::Integer(ms) = pttl(&mut ctx, &[bulk("k")]).unwrap() {
            assert!(ms > 9000 && ms <= 10000);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_pttl_different_keys() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "k1", "v1");
        set_key(&mut ctx, "k2", "v2");
        pexpire(&mut ctx, &[bulk("k1"), bulk("5000")]).unwrap();
        pexpire(&mut ctx, &[bulk("k2"), bulk("30000")]).unwrap();
        if let (RespValue::Integer(t1), RespValue::Integer(t2)) = (
            pttl(&mut ctx, &[bulk("k1")]).unwrap(),
            pttl(&mut ctx, &[bulk("k2")]).unwrap(),
        ) {
            assert!(t1 <= 5000);
            assert!(t2 <= 30000);
            assert!(t2 > t1);
        } else {
            panic!("Expected integers");
        }
    }

    #[test]
    fn test_pttl_large_ttl() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "k", "v");
        pexpire(&mut ctx, &[bulk("k"), bulk("86400000")]).unwrap();
        if let RespValue::Integer(t) = pttl(&mut ctx, &[bulk("k")]).unwrap() {
            assert!(t > 86_000_000);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_pttl_one_ms_ttl() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "k", "v");
        pexpire(&mut ctx, &[bulk("k"), bulk("1")]).unwrap();
        let result = pttl(&mut ctx, &[bulk("k")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t <= 1);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_pttl_on_hash_key() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(0);
        db.set(
            Bytes::from("myhash"),
            Entry::new(RedisValue::Hash(HashMap::from([(
                Bytes::from("f"),
                Bytes::from("v"),
            )]))),
        );
        let result = pttl(&mut ctx, &[bulk("myhash")]).unwrap();
        assert_eq!(result, RespValue::Integer(-1));
    }

    #[test]
    fn test_pttl_on_set_key() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(0);
        db.set(
            Bytes::from("myset"),
            Entry::new(RedisValue::Set(HashSet::from([Bytes::from("a")]))),
        );
        let result = pttl(&mut ctx, &[bulk("myset")]).unwrap();
        assert_eq!(result, RespValue::Integer(-1));
    }

    #[test]
    fn test_pttl_on_zset_key() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(0);
        let mut scores = HashMap::new();
        let mut members = BTreeMap::new();
        scores.insert(Bytes::from("m"), 2.5);
        members.insert((OrderedFloat(2.5), Bytes::from("m")), ());
        db.set(
            Bytes::from("myzset"),
            Entry::new(RedisValue::SortedSet { scores, members }),
        );
        let result = pttl(&mut ctx, &[bulk("myzset")]).unwrap();
        assert_eq!(result, RespValue::Integer(-1));
    }

    #[test]
    fn test_pttl_after_rename() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "orig", "val");
        pexpire(&mut ctx, &[bulk("orig"), bulk("50000")]).unwrap();
        let before = if let RespValue::Integer(t) = pttl(&mut ctx, &[bulk("orig")]).unwrap() {
            t
        } else {
            panic!("Expected integer");
        };
        rename(&mut ctx, &[bulk("orig"), bulk("newname")]).unwrap();
        assert_eq!(
            pttl(&mut ctx, &[bulk("orig")]).unwrap(),
            RespValue::Integer(-2)
        );
        if let RespValue::Integer(t) = pttl(&mut ctx, &[bulk("newname")]).unwrap() {
            assert!(t > 0);
            assert!((t - before).abs() < 100);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_pttl_after_set_with_px() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "k", "v");
        pexpire(&mut ctx, &[bulk("k"), bulk("7500")]).unwrap();
        if let RespValue::Integer(t) = pttl(&mut ctx, &[bulk("k")]).unwrap() {
            assert!(t > 0 && t <= 7500);
        } else {
            panic!("Expected integer");
        }
    }

    // ================================================================
    // PERSIST tests (19 additional)
    // ================================================================

    #[test]
    fn test_persist_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        let result = persist(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_persist_nonexistent_key() {
        let mut ctx = make_ctx();
        let result = persist(&mut ctx, &[bulk("no_such_key")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_persist_key_with_ttl() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "k", "v");
        expire(&mut ctx, &[bulk("k"), bulk("100")]).unwrap();
        let result = persist(&mut ctx, &[bulk("k")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_persist_key_without_ttl() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "k", "v");
        let result = persist(&mut ctx, &[bulk("k")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_persist_removes_ttl_verified() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "k", "v");
        expire(&mut ctx, &[bulk("k"), bulk("100")]).unwrap();
        // Verify TTL is set
        assert!(ctx
            .store()
            .database(0)
            .get(b"k")
            .unwrap()
            .expires_at
            .is_some());
        // Persist
        persist(&mut ctx, &[bulk("k")]).unwrap();
        // Verify expires_at is None
        assert!(ctx
            .store()
            .database(0)
            .get(b"k")
            .unwrap()
            .expires_at
            .is_none());
    }

    #[test]
    fn test_persist_on_list_key_with_ttl() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(0);
        let mut entry = Entry::new(RedisValue::List(VecDeque::from([Bytes::from("a")])));
        entry.set_ttl(std::time::Duration::from_secs(100));
        db.set(Bytes::from("mylist"), entry);
        let result = persist(&mut ctx, &[bulk("mylist")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        assert_eq!(
            ttl(&mut ctx, &[bulk("mylist")]).unwrap(),
            RespValue::Integer(-1)
        );
    }

    #[test]
    fn test_persist_on_expired_key() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(0);
        let mut entry = Entry::new(RedisValue::String(Bytes::from("val")));
        entry.expires_at = Some(std::time::Instant::now() - std::time::Duration::from_secs(1));
        db.set(Bytes::from("expired"), entry);
        let result = persist(&mut ctx, &[bulk("expired")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_persist_returns_integer_type() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "k", "v");
        let result = persist(&mut ctx, &[bulk("k")]).unwrap();
        assert!(matches!(result, RespValue::Integer(_)));
    }

    #[test]
    fn test_persist_empty_key() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(0);
        let mut entry = Entry::new(RedisValue::String(Bytes::from("val")));
        entry.set_ttl(std::time::Duration::from_secs(100));
        db.set(Bytes::from(""), entry);
        let result = persist(&mut ctx, &[bulk("")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_persist_special_chars() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key:!@#$", "v");
        expire(&mut ctx, &[bulk("key:!@#$"), bulk("100")]).unwrap();
        let result = persist(&mut ctx, &[bulk("key:!@#$")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_persist_after_expire() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "k", "v");
        expire(&mut ctx, &[bulk("k"), bulk("300")]).unwrap();
        assert!(matches!(ttl(&mut ctx, &[bulk("k")]).unwrap(), RespValue::Integer(t) if t > 0));
        persist(&mut ctx, &[bulk("k")]).unwrap();
        assert_eq!(ttl(&mut ctx, &[bulk("k")]).unwrap(), RespValue::Integer(-1));
    }

    #[test]
    fn test_persist_after_pexpire() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "k", "v");
        pexpire(&mut ctx, &[bulk("k"), bulk("50000")]).unwrap();
        assert!(matches!(pttl(&mut ctx, &[bulk("k")]).unwrap(), RespValue::Integer(t) if t > 0));
        persist(&mut ctx, &[bulk("k")]).unwrap();
        assert_eq!(
            pttl(&mut ctx, &[bulk("k")]).unwrap(),
            RespValue::Integer(-1)
        );
    }

    #[test]
    fn test_persist_twice() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "k", "v");
        expire(&mut ctx, &[bulk("k"), bulk("100")]).unwrap();
        let r1 = persist(&mut ctx, &[bulk("k")]).unwrap();
        assert_eq!(r1, RespValue::Integer(1));
        let r2 = persist(&mut ctx, &[bulk("k")]).unwrap();
        assert_eq!(r2, RespValue::Integer(0));
    }

    #[test]
    fn test_persist_value_unchanged() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "k", "hello");
        expire(&mut ctx, &[bulk("k"), bulk("100")]).unwrap();
        persist(&mut ctx, &[bulk("k")]).unwrap();
        let db = ctx.store().database(0);
        let entry = db.get(b"k").unwrap();
        if let RedisValue::String(ref s) = entry.value {
            assert_eq!(s.as_ref(), b"hello");
        } else {
            panic!("Expected string value");
        }
    }

    #[test]
    fn test_persist_on_hash_key() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(0);
        let mut entry = Entry::new(RedisValue::Hash(HashMap::from([(
            Bytes::from("f"),
            Bytes::from("v"),
        )])));
        entry.set_ttl(std::time::Duration::from_secs(100));
        db.set(Bytes::from("myhash"), entry);
        let result = persist(&mut ctx, &[bulk("myhash")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        assert_eq!(
            ttl(&mut ctx, &[bulk("myhash")]).unwrap(),
            RespValue::Integer(-1)
        );
    }

    #[test]
    fn test_persist_on_set_key() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(0);
        let mut entry = Entry::new(RedisValue::Set(HashSet::from([Bytes::from("a")])));
        entry.set_ttl(std::time::Duration::from_secs(100));
        db.set(Bytes::from("myset"), entry);
        let result = persist(&mut ctx, &[bulk("myset")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_persist_on_zset_key() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(0);
        let mut scores = HashMap::new();
        let mut members = BTreeMap::new();
        scores.insert(Bytes::from("m"), 1.0);
        members.insert((OrderedFloat(1.0), Bytes::from("m")), ());
        let mut entry = Entry::new(RedisValue::SortedSet { scores, members });
        entry.set_ttl(std::time::Duration::from_secs(100));
        db.set(Bytes::from("myzset"), entry);
        let result = persist(&mut ctx, &[bulk("myzset")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_persist_then_ttl_minus_one() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "k", "v");
        expire(&mut ctx, &[bulk("k"), bulk("100")]).unwrap();
        persist(&mut ctx, &[bulk("k")]).unwrap();
        assert_eq!(ttl(&mut ctx, &[bulk("k")]).unwrap(), RespValue::Integer(-1));
        assert_eq!(
            pttl(&mut ctx, &[bulk("k")]).unwrap(),
            RespValue::Integer(-1)
        );
    }

    #[test]
    fn test_persist_does_not_affect_other_keys() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "k1", "v1");
        set_key(&mut ctx, "k2", "v2");
        expire(&mut ctx, &[bulk("k1"), bulk("100")]).unwrap();
        expire(&mut ctx, &[bulk("k2"), bulk("200")]).unwrap();
        persist(&mut ctx, &[bulk("k1")]).unwrap();
        assert_eq!(
            ttl(&mut ctx, &[bulk("k1")]).unwrap(),
            RespValue::Integer(-1)
        );
        if let RespValue::Integer(t) = ttl(&mut ctx, &[bulk("k2")]).unwrap() {
            assert!(t > 0 && t <= 200);
        } else {
            panic!("Expected integer");
        }
    }

    // ================================================================
    // TYPE tests (19 additional)
    // ================================================================

    #[test]
    fn test_type_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        let result = type_cmd(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_type_nonexistent_key() {
        let mut ctx = make_ctx();
        let result = type_cmd(&mut ctx, &[bulk("no_such_key")]).unwrap();
        assert_eq!(result, RespValue::SimpleString("none".to_string()));
    }

    #[test]
    fn test_type_string_key() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "mystr", "hello");
        let result = type_cmd(&mut ctx, &[bulk("mystr")]).unwrap();
        assert_eq!(result, RespValue::SimpleString("string".to_string()));
    }

    #[test]
    fn test_type_list_key() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(0);
        db.set(
            Bytes::from("mylist"),
            Entry::new(RedisValue::List(VecDeque::from([Bytes::from("a")]))),
        );
        let result = type_cmd(&mut ctx, &[bulk("mylist")]).unwrap();
        assert_eq!(result, RespValue::SimpleString("list".to_string()));
    }

    #[test]
    fn test_type_hash_key() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(0);
        db.set(
            Bytes::from("myhash"),
            Entry::new(RedisValue::Hash(HashMap::from([(
                Bytes::from("f"),
                Bytes::from("v"),
            )]))),
        );
        let result = type_cmd(&mut ctx, &[bulk("myhash")]).unwrap();
        assert_eq!(result, RespValue::SimpleString("hash".to_string()));
    }

    #[test]
    fn test_type_set_key() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(0);
        db.set(
            Bytes::from("myset"),
            Entry::new(RedisValue::Set(HashSet::from([Bytes::from("a")]))),
        );
        let result = type_cmd(&mut ctx, &[bulk("myset")]).unwrap();
        assert_eq!(result, RespValue::SimpleString("set".to_string()));
    }

    #[test]
    fn test_type_zset_key() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(0);
        let mut scores = HashMap::new();
        let mut members = BTreeMap::new();
        scores.insert(Bytes::from("m"), 1.0);
        members.insert((OrderedFloat(1.0), Bytes::from("m")), ());
        db.set(
            Bytes::from("myzset"),
            Entry::new(RedisValue::SortedSet { scores, members }),
        );
        let result = type_cmd(&mut ctx, &[bulk("myzset")]).unwrap();
        assert_eq!(result, RespValue::SimpleString("zset".to_string()));
    }

    #[test]
    fn test_type_expired_key() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(0);
        let mut entry = Entry::new(RedisValue::String(Bytes::from("val")));
        entry.expires_at = Some(std::time::Instant::now() - std::time::Duration::from_secs(1));
        db.set(Bytes::from("expired"), entry);
        let result = type_cmd(&mut ctx, &[bulk("expired")]).unwrap();
        assert_eq!(result, RespValue::SimpleString("none".to_string()));
    }

    #[test]
    fn test_type_returns_simple_string() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "k", "v");
        let result = type_cmd(&mut ctx, &[bulk("k")]).unwrap();
        assert!(matches!(result, RespValue::SimpleString(_)));
    }

    #[test]
    fn test_type_empty_key_name() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(0);
        db.set(
            Bytes::from(""),
            Entry::new(RedisValue::String(Bytes::from("val"))),
        );
        let result = type_cmd(&mut ctx, &[bulk("")]).unwrap();
        assert_eq!(result, RespValue::SimpleString("string".to_string()));
    }

    #[test]
    fn test_type_special_chars() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key:!@#$%^&*()", "v");
        let result = type_cmd(&mut ctx, &[bulk("key:!@#$%^&*()")]).unwrap();
        assert_eq!(result, RespValue::SimpleString("string".to_string()));
    }

    #[test]
    fn test_type_after_del() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "k", "v");
        del(&mut ctx, &[bulk("k")]).unwrap();
        let result = type_cmd(&mut ctx, &[bulk("k")]).unwrap();
        assert_eq!(result, RespValue::SimpleString("none".to_string()));
    }

    #[test]
    fn test_type_after_set_overwrite() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "k", "original");
        set_key(&mut ctx, "k", "overwritten");
        let result = type_cmd(&mut ctx, &[bulk("k")]).unwrap();
        assert_eq!(result, RespValue::SimpleString("string".to_string()));
    }

    #[test]
    fn test_type_different_keys() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "str", "val");
        {
            let db = ctx.store().database(0);
            db.set(
                Bytes::from("lst"),
                Entry::new(RedisValue::List(VecDeque::from([Bytes::from("a")]))),
            );
            db.set(
                Bytes::from("hsh"),
                Entry::new(RedisValue::Hash(HashMap::from([(
                    Bytes::from("f"),
                    Bytes::from("v"),
                )]))),
            );
            db.set(
                Bytes::from("st"),
                Entry::new(RedisValue::Set(HashSet::from([Bytes::from("a")]))),
            );
        }
        assert_eq!(
            type_cmd(&mut ctx, &[bulk("str")]).unwrap(),
            RespValue::SimpleString("string".to_string())
        );
        assert_eq!(
            type_cmd(&mut ctx, &[bulk("lst")]).unwrap(),
            RespValue::SimpleString("list".to_string())
        );
        assert_eq!(
            type_cmd(&mut ctx, &[bulk("hsh")]).unwrap(),
            RespValue::SimpleString("hash".to_string())
        );
        assert_eq!(
            type_cmd(&mut ctx, &[bulk("st")]).unwrap(),
            RespValue::SimpleString("set".to_string())
        );
    }

    #[test]
    fn test_type_with_ttl() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "k", "v");
        expire(&mut ctx, &[bulk("k"), bulk("100")]).unwrap();
        let result = type_cmd(&mut ctx, &[bulk("k")]).unwrap();
        assert_eq!(result, RespValue::SimpleString("string".to_string()));
    }

    #[test]
    fn test_type_case_of_response() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "s", "v");
        {
            let db = ctx.store().database(0);
            db.set(Bytes::from("l"), Entry::new(RedisValue::new_list()));
            db.set(Bytes::from("h"), Entry::new(RedisValue::new_hash()));
            db.set(Bytes::from("se"), Entry::new(RedisValue::new_set()));
            db.set(Bytes::from("z"), Entry::new(RedisValue::new_sorted_set()));
        }
        assert_eq!(
            type_cmd(&mut ctx, &[bulk("s")]).unwrap(),
            RespValue::SimpleString("string".to_string())
        );
        assert_eq!(
            type_cmd(&mut ctx, &[bulk("l")]).unwrap(),
            RespValue::SimpleString("list".to_string())
        );
        assert_eq!(
            type_cmd(&mut ctx, &[bulk("h")]).unwrap(),
            RespValue::SimpleString("hash".to_string())
        );
        assert_eq!(
            type_cmd(&mut ctx, &[bulk("se")]).unwrap(),
            RespValue::SimpleString("set".to_string())
        );
        assert_eq!(
            type_cmd(&mut ctx, &[bulk("z")]).unwrap(),
            RespValue::SimpleString("zset".to_string())
        );
        assert_eq!(
            type_cmd(&mut ctx, &[bulk("nonexistent")]).unwrap(),
            RespValue::SimpleString("none".to_string())
        );
    }

    #[test]
    fn test_type_after_rename() {
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(0);
            db.set(
                Bytes::from("mylist"),
                Entry::new(RedisValue::List(VecDeque::from([Bytes::from("a")]))),
            );
        }
        rename(&mut ctx, &[bulk("mylist"), bulk("renamed")]).unwrap();
        assert_eq!(
            type_cmd(&mut ctx, &[bulk("renamed")]).unwrap(),
            RespValue::SimpleString("list".to_string())
        );
        assert_eq!(
            type_cmd(&mut ctx, &[bulk("mylist")]).unwrap(),
            RespValue::SimpleString("none".to_string())
        );
    }

    #[test]
    fn test_type_empty_list() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(0);
        db.set(Bytes::from("emptylist"), Entry::new(RedisValue::new_list()));
        let result = type_cmd(&mut ctx, &[bulk("emptylist")]).unwrap();
        assert_eq!(result, RespValue::SimpleString("list".to_string()));
    }

    #[test]
    fn test_type_empty_hash() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(0);
        db.set(Bytes::from("emptyhash"), Entry::new(RedisValue::new_hash()));
        let result = type_cmd(&mut ctx, &[bulk("emptyhash")]).unwrap();
        assert_eq!(result, RespValue::SimpleString("hash".to_string()));
    }

    // ================================================================
    // KEYS tests (19 additional)
    // ================================================================

    #[test]
    fn test_keys_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        let result = keys(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_keys_star_pattern() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "a", "1");
        set_key(&mut ctx, "b", "2");
        set_key(&mut ctx, "c", "3");
        let result = keys(&mut ctx, &[bulk("*")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 3);
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_keys_empty_db() {
        let mut ctx = make_ctx();
        let result = keys(&mut ctx, &[bulk("*")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert!(arr.is_empty());
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_keys_specific_key() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "hello", "world");
        set_key(&mut ctx, "other", "val");
        let result = keys(&mut ctx, &[bulk("hello")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 1);
            assert_eq!(arr[0], RespValue::BulkString(Bytes::from("hello")));
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_keys_question_mark() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "hello", "1");
        set_key(&mut ctx, "hallo", "2");
        set_key(&mut ctx, "hxllo", "3");
        set_key(&mut ctx, "world", "4");
        let result = keys(&mut ctx, &[bulk("h?llo")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 3);
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_keys_bracket_pattern() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "hallo", "1");
        set_key(&mut ctx, "hello", "2");
        set_key(&mut ctx, "hillo", "3");
        let result = keys(&mut ctx, &[bulk("h[ae]llo")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 2);
            let key_strs: Vec<Bytes> = arr
                .iter()
                .map(|v| {
                    if let RespValue::BulkString(b) = v {
                        b.clone()
                    } else {
                        panic!("Expected BulkString");
                    }
                })
                .collect();
            assert!(key_strs.contains(&Bytes::from("hallo")));
            assert!(key_strs.contains(&Bytes::from("hello")));
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_keys_prefix_pattern() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "user:1", "a");
        set_key(&mut ctx, "user:2", "b");
        set_key(&mut ctx, "order:1", "c");
        let result = keys(&mut ctx, &[bulk("user:*")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 2);
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_keys_suffix_pattern() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "user:name", "a");
        set_key(&mut ctx, "order:name", "b");
        set_key(&mut ctx, "user:email", "c");
        let result = keys(&mut ctx, &[bulk("*:name")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 2);
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_keys_no_match() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "hello", "world");
        let result = keys(&mut ctx, &[bulk("zzz*")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert!(arr.is_empty());
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_keys_excludes_expired() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "alive", "val");
        let db = ctx.store().database(0);
        let mut entry = Entry::new(RedisValue::String(Bytes::from("val")));
        entry.expires_at = Some(std::time::Instant::now() - std::time::Duration::from_secs(1));
        db.set(Bytes::from("dead"), entry);
        let result = keys(&mut ctx, &[bulk("*")]).unwrap();
        if let RespValue::Array(arr) = result {
            let key_strs: Vec<Bytes> = arr
                .iter()
                .map(|v| {
                    if let RespValue::BulkString(b) = v {
                        b.clone()
                    } else {
                        panic!("Expected BulkString");
                    }
                })
                .collect();
            assert!(key_strs.contains(&Bytes::from("alive")));
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_keys_returns_array_type() {
        let mut ctx = make_ctx();
        let result = keys(&mut ctx, &[bulk("*")]).unwrap();
        assert!(matches!(result, RespValue::Array(_)));
    }

    #[test]
    fn test_keys_multiple_matches() {
        let mut ctx = make_ctx();
        for i in 0..5 {
            set_key(&mut ctx, &format!("item:{i}"), "v");
        }
        set_key(&mut ctx, "other", "v");
        let result = keys(&mut ctx, &[bulk("item:*")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 5);
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_keys_special_chars_in_key() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key:with:colons", "v");
        set_key(&mut ctx, "key-with-dashes", "v");
        set_key(&mut ctx, "key_with_underscores", "v");
        let result = keys(&mut ctx, &[bulk("key:*")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 1);
            assert_eq!(
                arr[0],
                RespValue::BulkString(Bytes::from("key:with:colons"))
            );
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_keys_all_types_included() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "str", "val");
        {
            let db = ctx.store().database(0);
            db.set(
                Bytes::from("lst"),
                Entry::new(RedisValue::List(VecDeque::from([Bytes::from("a")]))),
            );
            db.set(
                Bytes::from("hsh"),
                Entry::new(RedisValue::Hash(HashMap::from([(
                    Bytes::from("f"),
                    Bytes::from("v"),
                )]))),
            );
            db.set(
                Bytes::from("st"),
                Entry::new(RedisValue::Set(HashSet::from([Bytes::from("a")]))),
            );
            let mut scores = HashMap::new();
            let mut members = BTreeMap::new();
            scores.insert(Bytes::from("m"), 1.0);
            members.insert((OrderedFloat(1.0), Bytes::from("m")), ());
            db.set(
                Bytes::from("zst"),
                Entry::new(RedisValue::SortedSet { scores, members }),
            );
        }
        let result = keys(&mut ctx, &[bulk("*")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 5);
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_keys_empty_pattern() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "hello", "world");
        {
            let db = ctx.store().database(0);
            db.set(
                Bytes::from(""),
                Entry::new(RedisValue::String(Bytes::from("empty_key_name"))),
            );
        }
        let result = keys(&mut ctx, &[bulk("")]).unwrap();
        if let RespValue::Array(arr) = result {
            let key_strs: Vec<Bytes> = arr
                .iter()
                .filter_map(|v| {
                    if let RespValue::BulkString(b) = v {
                        Some(b.clone())
                    } else {
                        None
                    }
                })
                .collect();
            assert!(key_strs.contains(&Bytes::from("")));
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_keys_single_key_db() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "onlyone", "val");
        let result = keys(&mut ctx, &[bulk("*")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 1);
            assert_eq!(arr[0], RespValue::BulkString(Bytes::from("onlyone")));
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_keys_large_db() {
        let mut ctx = make_ctx();
        for i in 0..100 {
            set_key(&mut ctx, &format!("key:{i}"), "v");
        }
        let result = keys(&mut ctx, &[bulk("*")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 100);
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_keys_pattern_complex() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "abc", "1");
        set_key(&mut ctx, "adc", "2");
        set_key(&mut ctx, "aec", "3");
        set_key(&mut ctx, "axyz", "4");
        let result = keys(&mut ctx, &[bulk("a?c")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 3);
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_keys_does_not_include_deleted() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "k1", "v1");
        set_key(&mut ctx, "k2", "v2");
        set_key(&mut ctx, "k3", "v3");
        del(&mut ctx, &[bulk("k2")]).unwrap();
        let result = keys(&mut ctx, &[bulk("*")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 2);
            let key_strs: Vec<Bytes> = arr
                .iter()
                .filter_map(|v| {
                    if let RespValue::BulkString(b) = v {
                        Some(b.clone())
                    } else {
                        None
                    }
                })
                .collect();
            assert!(key_strs.contains(&Bytes::from("k1")));
            assert!(key_strs.contains(&Bytes::from("k3")));
            assert!(!key_strs.contains(&Bytes::from("k2")));
        } else {
            panic!("Expected array");
        }
    }

    // ================================================================
    // Additional SCAN tests (13 new)
    // ================================================================

    #[test]
    fn test_scan_returns_cursor_and_array() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "a", "1");
        let result = scan(&mut ctx, &[bulk("0")]).unwrap();
        if let RespValue::Array(arr) = &result {
            assert_eq!(arr.len(), 2);
            assert!(matches!(&arr[0], RespValue::BulkString(_)));
            assert!(matches!(&arr[1], RespValue::Array(_)));
        } else {
            panic!("Expected array with [cursor, keys_array]");
        }
    }

    #[test]
    fn test_scan_nonexistent_pattern() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key1", "v1");
        set_key(&mut ctx, "key2", "v2");
        let result = scan(&mut ctx, &[bulk("0"), bulk("MATCH"), bulk("zzz*")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr[0], RespValue::BulkString(Bytes::from("0")));
            if let RespValue::Array(ref keys) = arr[1] {
                assert!(keys.is_empty());
            } else {
                panic!("Expected keys array");
            }
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_scan_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        let result = scan(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_scan_invalid_cursor() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key1", "v1");
        let result = scan(&mut ctx, &[bulk("notanumber")]);
        assert!(matches!(result, Err(CommandError::NotAnInteger)));
    }

    #[test]
    fn test_scan_large_db_200() {
        let mut ctx = make_ctx();
        let num_keys = 200;
        for i in 0..num_keys {
            set_key(&mut ctx, &format!("k{i:04}"), "v");
        }
        let mut all_keys = Vec::new();
        let mut cursor = 0usize;
        loop {
            let result = scan(
                &mut ctx,
                &[bulk(&cursor.to_string()), bulk("COUNT"), bulk("10")],
            )
            .unwrap();
            if let RespValue::Array(arr) = result {
                if let RespValue::BulkString(ref c) = arr[0] {
                    cursor = std::str::from_utf8(c).unwrap().parse().unwrap();
                }
                if let RespValue::Array(ref keys) = arr[1] {
                    for k in keys {
                        all_keys.push(k.clone());
                    }
                }
            }
            if cursor == 0 {
                break;
            }
        }
        assert_eq!(all_keys.len(), num_keys);
    }

    #[test]
    fn test_scan_match_star() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "a", "1");
        set_key(&mut ctx, "b", "2");
        set_key(&mut ctx, "c", "3");
        let result = scan(
            &mut ctx,
            &[
                bulk("0"),
                bulk("MATCH"),
                bulk("*"),
                bulk("COUNT"),
                bulk("100"),
            ],
        )
        .unwrap();
        if let RespValue::Array(arr) = result {
            if let RespValue::Array(ref keys) = arr[1] {
                assert_eq!(keys.len(), 3);
            } else {
                panic!("Expected keys array");
            }
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_scan_type_string_filter() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "str1", "hello");
        let db = ctx.store().database(0);
        db.set(Bytes::from("list1"), Entry::new(RedisValue::new_list()));
        let result = scan(
            &mut ctx,
            &[
                bulk("0"),
                bulk("TYPE"),
                bulk("string"),
                bulk("COUNT"),
                bulk("100"),
            ],
        )
        .unwrap();
        if let RespValue::Array(arr) = result {
            if let RespValue::Array(ref keys) = arr[1] {
                assert_eq!(keys.len(), 1);
                assert_eq!(keys[0], RespValue::BulkString(Bytes::from("str1")));
            } else {
                panic!("Expected keys array");
            }
        }
    }

    #[test]
    fn test_scan_type_list_filter() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "str1", "hello");
        let db = ctx.store().database(0);
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("item"));
        db.set(Bytes::from("list1"), Entry::new(RedisValue::List(list)));
        let result = scan(
            &mut ctx,
            &[
                bulk("0"),
                bulk("TYPE"),
                bulk("list"),
                bulk("COUNT"),
                bulk("100"),
            ],
        )
        .unwrap();
        if let RespValue::Array(arr) = result {
            if let RespValue::Array(ref keys) = arr[1] {
                assert_eq!(keys.len(), 1);
                assert_eq!(keys[0], RespValue::BulkString(Bytes::from("list1")));
            } else {
                panic!("Expected keys array");
            }
        }
    }

    #[test]
    fn test_scan_type_hash_filter() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "str1", "hello");
        let db = ctx.store().database(0);
        let mut h = HashMap::new();
        h.insert(Bytes::from("f1"), Bytes::from("v1"));
        db.set(Bytes::from("hash1"), Entry::new(RedisValue::Hash(h)));
        let result = scan(
            &mut ctx,
            &[
                bulk("0"),
                bulk("TYPE"),
                bulk("hash"),
                bulk("COUNT"),
                bulk("100"),
            ],
        )
        .unwrap();
        if let RespValue::Array(arr) = result {
            if let RespValue::Array(ref keys) = arr[1] {
                assert_eq!(keys.len(), 1);
                assert_eq!(keys[0], RespValue::BulkString(Bytes::from("hash1")));
            } else {
                panic!("Expected keys array");
            }
        }
    }

    #[test]
    fn test_scan_type_set_filter() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "str1", "hello");
        let db = ctx.store().database(0);
        let mut s = HashSet::new();
        s.insert(Bytes::from("m1"));
        db.set(Bytes::from("set1"), Entry::new(RedisValue::Set(s)));
        let result = scan(
            &mut ctx,
            &[
                bulk("0"),
                bulk("TYPE"),
                bulk("set"),
                bulk("COUNT"),
                bulk("100"),
            ],
        )
        .unwrap();
        if let RespValue::Array(arr) = result {
            if let RespValue::Array(ref keys) = arr[1] {
                assert_eq!(keys.len(), 1);
                assert_eq!(keys[0], RespValue::BulkString(Bytes::from("set1")));
            } else {
                panic!("Expected keys array");
            }
        }
    }

    #[test]
    fn test_scan_expired_keys_excluded() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "alive", "v1");
        let db = ctx.store().database(0);
        let mut entry = Entry::new(RedisValue::String(Bytes::from("v2")));
        entry.expires_at = Some(std::time::Instant::now() - Duration::from_secs(1));
        db.set(Bytes::from("dead"), entry);
        let result = scan(
            &mut ctx,
            &[
                bulk("0"),
                bulk("TYPE"),
                bulk("string"),
                bulk("COUNT"),
                bulk("100"),
            ],
        )
        .unwrap();
        if let RespValue::Array(arr) = result {
            if let RespValue::Array(ref keys) = arr[1] {
                for k in keys {
                    if let RespValue::BulkString(b) = k {
                        assert_ne!(b.as_ref(), b"dead");
                    }
                }
            }
        }
    }

    #[test]
    fn test_scan_single_key_only() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "only", "val");
        let result = scan(&mut ctx, &[bulk("0")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr[0], RespValue::BulkString(Bytes::from("0")));
            if let RespValue::Array(ref keys) = arr[1] {
                assert_eq!(keys.len(), 1);
                assert_eq!(keys[0], RespValue::BulkString(Bytes::from("only")));
            }
        }
    }

    #[test]
    fn test_scan_special_chars_pattern() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "hello-world", "v1");
        set_key(&mut ctx, "hello_world", "v2");
        set_key(&mut ctx, "foo", "v3");
        let result = scan(
            &mut ctx,
            &[
                bulk("0"),
                bulk("MATCH"),
                bulk("hello?world"),
                bulk("COUNT"),
                bulk("100"),
            ],
        )
        .unwrap();
        if let RespValue::Array(arr) = result {
            if let RespValue::Array(ref keys) = arr[1] {
                assert_eq!(keys.len(), 2);
            } else {
                panic!("Expected keys array");
            }
        }
    }

    // ================================================================
    // Additional RENAME tests (18 new)
    // ================================================================

    #[test]
    fn test_rename_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        let result = rename(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_rename_wrong_arity_one_arg() {
        let mut ctx = make_ctx();
        let result = rename(&mut ctx, &[bulk("key")]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_rename_basic_new() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "src", "hello");
        let result = rename(&mut ctx, &[bulk("src"), bulk("dst")]).unwrap();
        assert_eq!(result, RespValue::ok());
        let db = ctx.store().database(0);
        assert!(!db.exists(b"src"));
        assert!(db.exists(b"dst"));
    }

    #[test]
    fn test_rename_nonexistent_source_err() {
        let mut ctx = make_ctx();
        let result = rename(&mut ctx, &[bulk("nosrc"), bulk("dst")]);
        assert!(matches!(result, Err(CommandError::NoSuchKey)));
    }

    #[test]
    fn test_rename_overwrites_destination() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "src", "new_value");
        set_key(&mut ctx, "dst", "old_value");
        let result = rename(&mut ctx, &[bulk("src"), bulk("dst")]).unwrap();
        assert_eq!(result, RespValue::ok());
        let db = ctx.store().database(0);
        let entry = db.get(b"dst").unwrap();
        if let RedisValue::String(ref s) = entry.value {
            assert_eq!(s.as_ref(), b"new_value");
        } else {
            panic!("Expected string");
        }
    }

    #[test]
    fn test_rename_same_key() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");
        let result = rename(&mut ctx, &[bulk("key"), bulk("key")]).unwrap();
        assert_eq!(result, RespValue::ok());
        let db = ctx.store().database(0);
        assert!(db.exists(b"key"));
    }

    #[test]
    fn test_rename_preserves_value() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "src", "my_special_value");
        rename(&mut ctx, &[bulk("src"), bulk("dst")]).unwrap();
        let db = ctx.store().database(0);
        let entry = db.get(b"dst").unwrap();
        if let RedisValue::String(ref s) = entry.value {
            assert_eq!(s.as_ref(), b"my_special_value");
        } else {
            panic!("Expected string value");
        }
    }

    #[test]
    fn test_rename_preserves_ttl() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "src", "value");
        expire(&mut ctx, &[bulk("src"), bulk("300")]).unwrap();
        rename(&mut ctx, &[bulk("src"), bulk("dst")]).unwrap();
        let result = ttl(&mut ctx, &[bulk("dst")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t > 0 && t <= 300);
        } else {
            panic!("Expected integer TTL");
        }
    }

    #[test]
    fn test_rename_removes_old_key() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "src", "val");
        rename(&mut ctx, &[bulk("src"), bulk("dst")]).unwrap();
        let db = ctx.store().database(0);
        assert!(!db.exists(b"src"));
        let result = exists(&mut ctx, &[bulk("src")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_rename_list_key() {
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(0);
            let mut list = VecDeque::new();
            list.push_back(Bytes::from("item1"));
            list.push_back(Bytes::from("item2"));
            db.set(Bytes::from("mylist"), Entry::new(RedisValue::List(list)));
        }
        let result = rename(&mut ctx, &[bulk("mylist"), bulk("newlist")]).unwrap();
        assert_eq!(result, RespValue::ok());
        let db = ctx.store().database(0);
        let entry = db.get(b"newlist").unwrap();
        assert_eq!(entry.value.type_name(), "list");
        assert!(!db.exists(b"mylist"));
    }

    #[test]
    fn test_rename_hash_key() {
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(0);
            let mut h = HashMap::new();
            h.insert(Bytes::from("f1"), Bytes::from("v1"));
            db.set(Bytes::from("myhash"), Entry::new(RedisValue::Hash(h)));
        }
        let result = rename(&mut ctx, &[bulk("myhash"), bulk("newhash")]).unwrap();
        assert_eq!(result, RespValue::ok());
        let db = ctx.store().database(0);
        let entry = db.get(b"newhash").unwrap();
        assert_eq!(entry.value.type_name(), "hash");
    }

    #[test]
    fn test_rename_set_key() {
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(0);
            let mut s = HashSet::new();
            s.insert(Bytes::from("m1"));
            s.insert(Bytes::from("m2"));
            db.set(Bytes::from("myset"), Entry::new(RedisValue::Set(s)));
        }
        let result = rename(&mut ctx, &[bulk("myset"), bulk("newset")]).unwrap();
        assert_eq!(result, RespValue::ok());
        let db = ctx.store().database(0);
        let entry = db.get(b"newset").unwrap();
        assert_eq!(entry.value.type_name(), "set");
    }

    #[test]
    fn test_rename_zset_key() {
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(0);
            let mut scores = HashMap::new();
            let mut members = BTreeMap::new();
            scores.insert(Bytes::from("alice"), 1.0);
            members.insert((OrderedFloat(1.0), Bytes::from("alice")), ());
            db.set(
                Bytes::from("myzset"),
                Entry::new(RedisValue::SortedSet { scores, members }),
            );
        }
        let result = rename(&mut ctx, &[bulk("myzset"), bulk("newzset")]).unwrap();
        assert_eq!(result, RespValue::ok());
        let db = ctx.store().database(0);
        let entry = db.get(b"newzset").unwrap();
        assert_eq!(entry.value.type_name(), "zset");
    }

    #[test]
    fn test_rename_expired_source() {
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(0);
            let mut entry = Entry::new(RedisValue::String(Bytes::from("val")));
            entry.expires_at = Some(std::time::Instant::now() - Duration::from_secs(1));
            db.set(Bytes::from("expired"), entry);
        }
        let result = rename(&mut ctx, &[bulk("expired"), bulk("dst")]);
        assert!(matches!(result, Err(CommandError::NoSuchKey)));
    }

    #[test]
    fn test_rename_overwrites_different_type() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "src", "string_val");
        {
            let db = ctx.store().database(0);
            db.set(Bytes::from("dst"), Entry::new(RedisValue::new_list()));
        }
        let result = rename(&mut ctx, &[bulk("src"), bulk("dst")]).unwrap();
        assert_eq!(result, RespValue::ok());
        let db = ctx.store().database(0);
        let entry = db.get(b"dst").unwrap();
        assert_eq!(entry.value.type_name(), "string");
    }

    #[test]
    fn test_rename_returns_ok() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "a", "b");
        let result = rename(&mut ctx, &[bulk("a"), bulk("c")]).unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
    }

    #[test]
    fn test_rename_special_chars() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key with spaces", "val");
        let result = rename(&mut ctx, &[bulk("key with spaces"), bulk("new-key!@#")]).unwrap();
        assert_eq!(result, RespValue::ok());
        let db = ctx.store().database(0);
        assert!(db.exists(b"new-key!@#"));
        assert!(!db.exists(b"key with spaces"));
    }

    #[test]
    fn test_rename_empty_key_names() {
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(0);
            db.set(
                Bytes::from(""),
                Entry::new(RedisValue::String(Bytes::from("val"))),
            );
        }
        let result = rename(&mut ctx, &[bulk(""), bulk("newname")]).unwrap();
        assert_eq!(result, RespValue::ok());
        let db = ctx.store().database(0);
        assert!(db.exists(b"newname"));
        assert!(!db.exists(b""));
    }

    // ================================================================
    // Additional RENAMENX tests (19 new)
    // ================================================================

    #[test]
    fn test_renamenx_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        let result = renamenx(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_renamenx_wrong_arity_one_arg() {
        let mut ctx = make_ctx();
        let result = renamenx(&mut ctx, &[bulk("key")]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_renamenx_basic_new() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "src", "hello");
        let result = renamenx(&mut ctx, &[bulk("src"), bulk("dst")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        let db = ctx.store().database(0);
        assert!(!db.exists(b"src"));
        assert!(db.exists(b"dst"));
    }

    #[test]
    fn test_renamenx_dest_exists() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "src", "val1");
        set_key(&mut ctx, "dst", "val2");
        let result = renamenx(&mut ctx, &[bulk("src"), bulk("dst")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
        let db = ctx.store().database(0);
        assert!(db.exists(b"src"));
    }

    #[test]
    fn test_renamenx_nonexistent_source_err() {
        let mut ctx = make_ctx();
        let result = renamenx(&mut ctx, &[bulk("nosrc"), bulk("dst")]);
        assert!(matches!(result, Err(CommandError::NoSuchKey)));
    }

    #[test]
    fn test_renamenx_preserves_value() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "src", "precious_data");
        renamenx(&mut ctx, &[bulk("src"), bulk("dst")]).unwrap();
        let db = ctx.store().database(0);
        let entry = db.get(b"dst").unwrap();
        if let RedisValue::String(ref s) = entry.value {
            assert_eq!(s.as_ref(), b"precious_data");
        } else {
            panic!("Expected string");
        }
    }

    #[test]
    fn test_renamenx_preserves_ttl() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "src", "value");
        expire(&mut ctx, &[bulk("src"), bulk("500")]).unwrap();
        renamenx(&mut ctx, &[bulk("src"), bulk("dst")]).unwrap();
        let result = ttl(&mut ctx, &[bulk("dst")]).unwrap();
        if let RespValue::Integer(t) = result {
            assert!(t > 0 && t <= 500);
        } else {
            panic!("Expected integer");
        }
    }

    #[test]
    fn test_renamenx_removes_old_key() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "src", "val");
        renamenx(&mut ctx, &[bulk("src"), bulk("dst")]).unwrap();
        let db = ctx.store().database(0);
        assert!(!db.exists(b"src"));
    }

    #[test]
    fn test_renamenx_does_not_overwrite() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "src", "new_val");
        set_key(&mut ctx, "dst", "old_val");
        renamenx(&mut ctx, &[bulk("src"), bulk("dst")]).unwrap();
        let db = ctx.store().database(0);
        let entry = db.get(b"dst").unwrap();
        if let RedisValue::String(ref s) = entry.value {
            assert_eq!(s.as_ref(), b"old_val");
        } else {
            panic!("Expected string");
        }
    }

    #[test]
    fn test_renamenx_same_key() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key", "value");
        let result = renamenx(&mut ctx, &[bulk("key"), bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
        let db = ctx.store().database(0);
        assert!(db.exists(b"key"));
    }

    #[test]
    fn test_renamenx_list_key() {
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(0);
            let mut list = VecDeque::new();
            list.push_back(Bytes::from("item"));
            db.set(Bytes::from("mylist"), Entry::new(RedisValue::List(list)));
        }
        let result = renamenx(&mut ctx, &[bulk("mylist"), bulk("newlist")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        let db = ctx.store().database(0);
        let entry = db.get(b"newlist").unwrap();
        assert_eq!(entry.value.type_name(), "list");
        assert!(!db.exists(b"mylist"));
    }

    #[test]
    fn test_renamenx_hash_key() {
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(0);
            let mut h = HashMap::new();
            h.insert(Bytes::from("f"), Bytes::from("v"));
            db.set(Bytes::from("myhash"), Entry::new(RedisValue::Hash(h)));
        }
        let result = renamenx(&mut ctx, &[bulk("myhash"), bulk("newhash")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        let db = ctx.store().database(0);
        let entry = db.get(b"newhash").unwrap();
        assert_eq!(entry.value.type_name(), "hash");
    }

    #[test]
    fn test_renamenx_returns_integer_type() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "a", "b");
        let result = renamenx(&mut ctx, &[bulk("a"), bulk("c")]).unwrap();
        assert!(matches!(result, RespValue::Integer(1)));
    }

    #[test]
    fn test_renamenx_expired_source() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(0);
        let mut entry = Entry::new(RedisValue::String(Bytes::from("val")));
        entry.expires_at = Some(std::time::Instant::now() - Duration::from_secs(1));
        db.set(Bytes::from("expired"), entry);
        let result = renamenx(&mut ctx, &[bulk("expired"), bulk("dst")]);
        assert!(matches!(result, Err(CommandError::NoSuchKey)));
    }

    #[test]
    fn test_renamenx_expired_dest() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "src", "val");
        {
            let db = ctx.store().database(0);
            let mut entry = Entry::new(RedisValue::String(Bytes::from("old")));
            entry.expires_at = Some(std::time::Instant::now() - Duration::from_secs(1));
            db.set(Bytes::from("dst"), entry);
        }
        let result = renamenx(&mut ctx, &[bulk("src"), bulk("dst")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        let db = ctx.store().database(0);
        assert!(db.exists(b"dst"));
        assert!(!db.exists(b"src"));
    }

    #[test]
    fn test_renamenx_special_chars() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key:with:colons", "v");
        let result = renamenx(&mut ctx, &[bulk("key:with:colons"), bulk("new-key!@#$")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        let db = ctx.store().database(0);
        assert!(db.exists(b"new-key!@#$"));
    }

    #[test]
    fn test_renamenx_empty_key() {
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(0);
            db.set(
                Bytes::from(""),
                Entry::new(RedisValue::String(Bytes::from("val"))),
            );
        }
        let result = renamenx(&mut ctx, &[bulk(""), bulk("notempty")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        let db = ctx.store().database(0);
        assert!(db.exists(b"notempty"));
        assert!(!db.exists(b""));
    }

    #[test]
    fn test_renamenx_dest_different_type() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "src", "string_val");
        {
            let db = ctx.store().database(0);
            db.set(Bytes::from("dst"), Entry::new(RedisValue::new_list()));
        }
        let result = renamenx(&mut ctx, &[bulk("src"), bulk("dst")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
        let db = ctx.store().database(0);
        let entry = db.get(b"dst").unwrap();
        assert_eq!(entry.value.type_name(), "list");
    }

    #[test]
    fn test_renamenx_after_del_dest() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "src", "val");
        set_key(&mut ctx, "dst", "old");
        let result = renamenx(&mut ctx, &[bulk("src"), bulk("dst")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
        del(&mut ctx, &[bulk("dst")]).unwrap();
        let result = renamenx(&mut ctx, &[bulk("src"), bulk("dst")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    // ================================================================
    // Additional RANDOMKEY tests (18 new)
    // ================================================================

    #[test]
    fn test_randomkey_wrong_arity_with_args() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key1", "v1");
        let result = randomkey(&mut ctx, &[bulk("extra")]).unwrap();
        assert!(matches!(result, RespValue::BulkString(_)));
    }

    #[test]
    fn test_randomkey_empty_db_new() {
        let mut ctx = make_ctx();
        let result = randomkey(&mut ctx, &[]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_randomkey_single_key_only() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "only_key", "val");
        let result = randomkey(&mut ctx, &[]).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from("only_key")));
    }

    #[test]
    fn test_randomkey_multiple_keys() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "k1", "v1");
        set_key(&mut ctx, "k2", "v2");
        set_key(&mut ctx, "k3", "v3");
        let result = randomkey(&mut ctx, &[]).unwrap();
        if let RespValue::BulkString(ref b) = result {
            let key_str = std::str::from_utf8(b).unwrap();
            assert!(
                key_str == "k1" || key_str == "k2" || key_str == "k3",
                "Unexpected key: {key_str}"
            );
        } else {
            panic!("Expected BulkString");
        }
    }

    #[test]
    fn test_randomkey_returns_existing_key() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "x", "1");
        set_key(&mut ctx, "y", "2");
        let result = randomkey(&mut ctx, &[]).unwrap();
        if let RespValue::BulkString(ref b) = result {
            let db = ctx.store().database(0);
            assert!(db.exists(b));
        } else {
            panic!("Expected BulkString");
        }
    }

    #[test]
    fn test_randomkey_string_type() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "str_key", "val");
        let result = randomkey(&mut ctx, &[]).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from("str_key")));
        let result = type_cmd(&mut ctx, &[bulk("str_key")]).unwrap();
        assert_eq!(result, RespValue::SimpleString("string".to_string()));
    }

    #[test]
    fn test_randomkey_list_type() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(0);
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("item"));
        db.set(Bytes::from("list_key"), Entry::new(RedisValue::List(list)));
        let result = randomkey(&mut ctx, &[]).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from("list_key")));
    }

    #[test]
    fn test_randomkey_hash_type() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(0);
        let mut h = HashMap::new();
        h.insert(Bytes::from("f"), Bytes::from("v"));
        db.set(Bytes::from("hash_key"), Entry::new(RedisValue::Hash(h)));
        let result = randomkey(&mut ctx, &[]).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from("hash_key")));
    }

    #[test]
    fn test_randomkey_set_type() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(0);
        let mut s = HashSet::new();
        s.insert(Bytes::from("m1"));
        db.set(Bytes::from("set_key"), Entry::new(RedisValue::Set(s)));
        let result = randomkey(&mut ctx, &[]).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from("set_key")));
    }

    #[test]
    fn test_randomkey_returns_bulk_string_type() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "k", "v");
        let result = randomkey(&mut ctx, &[]).unwrap();
        assert!(
            matches!(result, RespValue::BulkString(_)),
            "RANDOMKEY must return BulkString, got: {:?}",
            result
        );
    }

    #[test]
    fn test_randomkey_excludes_expired() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(0);
        let mut entry = Entry::new(RedisValue::String(Bytes::from("val")));
        entry.expires_at = Some(std::time::Instant::now() - Duration::from_secs(1));
        db.set(Bytes::from("expired_key"), entry);
        let result = randomkey(&mut ctx, &[]).unwrap();
        assert!(
            matches!(result, RespValue::Null | RespValue::BulkString(_)),
            "Unexpected result: {:?}",
            result
        );
    }

    #[test]
    fn test_randomkey_many_keys() {
        let mut ctx = make_ctx();
        for i in 0..50 {
            set_key(&mut ctx, &format!("k{i}"), "v");
        }
        let result = randomkey(&mut ctx, &[]).unwrap();
        if let RespValue::BulkString(ref b) = result {
            let key_str = std::str::from_utf8(b).unwrap();
            assert!(key_str.starts_with('k'));
        } else {
            panic!("Expected BulkString");
        }
    }

    #[test]
    fn test_randomkey_after_del() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "k1", "v1");
        set_key(&mut ctx, "k2", "v2");
        del(&mut ctx, &[bulk("k1")]).unwrap();
        let result = randomkey(&mut ctx, &[]).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from("k2")));
    }

    #[test]
    fn test_randomkey_all_keys_deleted() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "k1", "v1");
        set_key(&mut ctx, "k2", "v2");
        del(&mut ctx, &[bulk("k1"), bulk("k2")]).unwrap();
        let result = randomkey(&mut ctx, &[]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_randomkey_does_not_delete() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "k1", "v1");
        randomkey(&mut ctx, &[]).unwrap();
        let db = ctx.store().database(0);
        assert!(db.exists(b"k1"));
    }

    #[test]
    fn test_randomkey_special_chars() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key with spaces & symbols!@#", "val");
        let result = randomkey(&mut ctx, &[]).unwrap();
        assert_eq!(
            result,
            RespValue::BulkString(Bytes::from("key with spaces & symbols!@#"))
        );
    }

    #[test]
    fn test_randomkey_consistency() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "only", "val");
        let r1 = randomkey(&mut ctx, &[]).unwrap();
        let r2 = randomkey(&mut ctx, &[]).unwrap();
        assert_eq!(r1, r2);
        assert_eq!(r1, RespValue::BulkString(Bytes::from("only")));
    }

    #[test]
    fn test_randomkey_after_rename() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "original", "val");
        rename(&mut ctx, &[bulk("original"), bulk("renamed")]).unwrap();
        let result = randomkey(&mut ctx, &[]).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from("renamed")));
    }

    // ==================== DUMP tests ====================

    #[test]
    fn test_dump_wrong_arity() {
        let mut ctx = make_ctx();
        let result = dump(&mut ctx, &[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_dump_string_returns_type_byte_0() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "mystr", "hello");
        let result = dump(&mut ctx, &[bulk("mystr")]).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from(vec![0u8])));
    }

    #[test]
    fn test_dump_list_returns_type_byte_1() {
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(ctx.selected_db());
            let mut list = VecDeque::new();
            list.push_back(Bytes::from("item1"));
            db.set(Bytes::from("mylist"), Entry::new(RedisValue::List(list)));
        }
        let result = dump(&mut ctx, &[bulk("mylist")]).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from(vec![1u8])));
    }

    #[test]
    fn test_dump_set_returns_type_byte_2() {
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(ctx.selected_db());
            let mut set = HashSet::new();
            set.insert(Bytes::from("member1"));
            db.set(Bytes::from("myset"), Entry::new(RedisValue::Set(set)));
        }
        let result = dump(&mut ctx, &[bulk("myset")]).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from(vec![2u8])));
    }

    #[test]
    fn test_dump_sorted_set_returns_type_byte_3() {
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(ctx.selected_db());
            let mut scores = HashMap::new();
            let mut members = BTreeMap::new();
            scores.insert(Bytes::from("m1"), 1.0);
            members.insert((OrderedFloat(1.0), Bytes::from("m1")), ());
            db.set(
                Bytes::from("myzset"),
                Entry::new(RedisValue::SortedSet { scores, members }),
            );
        }
        let result = dump(&mut ctx, &[bulk("myzset")]).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from(vec![3u8])));
    }

    #[test]
    fn test_dump_hash_returns_type_byte_4() {
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(ctx.selected_db());
            let mut hash = HashMap::new();
            hash.insert(Bytes::from("field1"), Bytes::from("value1"));
            db.set(Bytes::from("myhash"), Entry::new(RedisValue::Hash(hash)));
        }
        let result = dump(&mut ctx, &[bulk("myhash")]).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from(vec![4u8])));
    }

    #[test]
    fn test_dump_nonexistent_key_returns_null() {
        let mut ctx = make_ctx();
        let result = dump(&mut ctx, &[bulk("nokey")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_dump_expired_key_returns_null() {
        use std::time::Instant;
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(ctx.selected_db());
            let mut entry = Entry::new(RedisValue::String(Bytes::from("val")));
            entry.expires_at = Some(Instant::now() - Duration::from_secs(1));
            db.set(Bytes::from("expkey"), entry);
        }
        let result = dump(&mut ctx, &[bulk("expkey")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_dump_expired_key_deletes_it() {
        use std::time::Instant;
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(ctx.selected_db());
            let mut entry = Entry::new(RedisValue::String(Bytes::from("val")));
            entry.expires_at = Some(Instant::now() - Duration::from_secs(1));
            db.set(Bytes::from("expkey"), entry);
        }
        dump(&mut ctx, &[bulk("expkey")]).unwrap();
        // Key should have been deleted by the lazy expiry in dump
        let db = ctx.store().database(ctx.selected_db());
        assert!(db.get(b"expkey").is_none());
    }

    #[test]
    fn test_dump_returns_bulk_string_type() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "k", "v");
        let result = dump(&mut ctx, &[bulk("k")]).unwrap();
        assert!(matches!(result, RespValue::BulkString(_)));
    }

    #[test]
    fn test_dump_does_not_modify_key() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "k", "original");
        dump(&mut ctx, &[bulk("k")]).unwrap();
        let db = ctx.store().database(ctx.selected_db());
        let entry = db.get(b"k").unwrap();
        match &entry.value {
            RedisValue::String(s) => assert_eq!(s.as_ref(), b"original"),
            _ => panic!("expected string"),
        }
    }

    #[test]
    fn test_dump_does_not_remove_key() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "k", "v");
        dump(&mut ctx, &[bulk("k")]).unwrap();
        let db = ctx.store().database(ctx.selected_db());
        assert!(db.get(b"k").is_some());
    }

    #[test]
    fn test_dump_special_chars_key() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "key with spaces!", "val");
        let result = dump(&mut ctx, &[bulk("key with spaces!")]).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from(vec![0u8])));
    }

    #[test]
    fn test_dump_empty_string_key() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "", "val");
        let result = dump(&mut ctx, &[bulk("")]).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from(vec![0u8])));
    }

    #[test]
    fn test_dump_empty_value_string() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "k", "");
        let result = dump(&mut ctx, &[bulk("k")]).unwrap();
        // Still a string type, byte 0
        assert_eq!(result, RespValue::BulkString(Bytes::from(vec![0u8])));
    }

    #[test]
    fn test_dump_multiple_keys_independent() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "s1", "hello");
        {
            let db = ctx.store().database(ctx.selected_db());
            let mut list = VecDeque::new();
            list.push_back(Bytes::from("item"));
            db.set(Bytes::from("l1"), Entry::new(RedisValue::List(list)));
        }
        let r1 = dump(&mut ctx, &[bulk("s1")]).unwrap();
        let r2 = dump(&mut ctx, &[bulk("l1")]).unwrap();
        assert_eq!(r1, RespValue::BulkString(Bytes::from(vec![0u8])));
        assert_eq!(r2, RespValue::BulkString(Bytes::from(vec![1u8])));
    }

    #[test]
    fn test_dump_key_with_ttl_still_returns_type() {
        use std::time::Instant;
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(ctx.selected_db());
            let mut entry = Entry::new(RedisValue::String(Bytes::from("val")));
            entry.expires_at = Some(Instant::now() + Duration::from_secs(3600));
            db.set(Bytes::from("k"), entry);
        }
        let result = dump(&mut ctx, &[bulk("k")]).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from(vec![0u8])));
    }

    #[test]
    fn test_dump_extra_args_ignored() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "k", "v");
        // dump only uses args[0], extra args are ignored
        let result = dump(&mut ctx, &[bulk("k"), bulk("extra")]).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from(vec![0u8])));
    }

    #[test]
    fn test_dump_after_key_deleted_returns_null() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "k", "v");
        del(&mut ctx, &[bulk("k")]).unwrap();
        let result = dump(&mut ctx, &[bulk("k")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_dump_list_with_multiple_elements() {
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(ctx.selected_db());
            let mut list = VecDeque::new();
            list.push_back(Bytes::from("a"));
            list.push_back(Bytes::from("b"));
            list.push_back(Bytes::from("c"));
            db.set(Bytes::from("biglist"), Entry::new(RedisValue::List(list)));
        }
        let result = dump(&mut ctx, &[bulk("biglist")]).unwrap();
        // Still type byte 1 regardless of element count
        assert_eq!(result, RespValue::BulkString(Bytes::from(vec![1u8])));
    }

    // ==================== RESTORE tests ====================

    #[test]
    fn test_restore_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        let result = restore(&mut ctx, &[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_restore_wrong_arity_one_arg() {
        let mut ctx = make_ctx();
        let result = restore(&mut ctx, &[bulk("key")]);
        assert!(result.is_err());
    }

    #[test]
    fn test_restore_wrong_arity_two_args() {
        let mut ctx = make_ctx();
        let result = restore(&mut ctx, &[bulk("key"), bulk("0")]);
        assert!(result.is_err());
    }

    #[test]
    fn test_restore_basic() {
        let mut ctx = make_ctx();
        let result = restore(&mut ctx, &[bulk("newkey"), bulk("0"), bulk("data")]).unwrap();
        assert_eq!(result, RespValue::ok());
        let db = ctx.store().database(ctx.selected_db());
        let entry = db.get(b"newkey").unwrap();
        match &entry.value {
            RedisValue::String(s) => assert_eq!(s.as_ref(), b"restored"),
            _ => panic!("expected string"),
        }
    }

    #[test]
    fn test_restore_returns_ok() {
        let mut ctx = make_ctx();
        let result = restore(&mut ctx, &[bulk("k"), bulk("0"), bulk("data")]).unwrap();
        assert_eq!(result, RespValue::ok());
    }

    #[test]
    fn test_restore_with_zero_ttl_no_expiry() {
        let mut ctx = make_ctx();
        restore(&mut ctx, &[bulk("k"), bulk("0"), bulk("data")]).unwrap();
        let db = ctx.store().database(ctx.selected_db());
        let entry = db.get(b"k").unwrap();
        assert!(entry.expires_at.is_none());
    }

    #[test]
    fn test_restore_with_positive_ttl_sets_expiry() {
        let mut ctx = make_ctx();
        restore(&mut ctx, &[bulk("k"), bulk("5000"), bulk("data")]).unwrap();
        let db = ctx.store().database(ctx.selected_db());
        let entry = db.get(b"k").unwrap();
        assert!(entry.expires_at.is_some());
    }

    #[test]
    fn test_restore_key_exists_without_replace_errors() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "existing", "value");
        let result = restore(&mut ctx, &[bulk("existing"), bulk("0"), bulk("data")]);
        assert!(result.is_err());
        match result {
            Err(CommandError::InvalidArgument(msg)) => {
                assert!(msg.contains("BUSYKEY"));
            }
            _ => panic!("expected BUSYKEY error"),
        }
    }

    #[test]
    fn test_restore_key_exists_with_replace_succeeds() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "existing", "old");
        let result = restore(
            &mut ctx,
            &[bulk("existing"), bulk("0"), bulk("data"), bulk("REPLACE")],
        )
        .unwrap();
        assert_eq!(result, RespValue::ok());
        let db = ctx.store().database(ctx.selected_db());
        let entry = db.get(b"existing").unwrap();
        match &entry.value {
            RedisValue::String(s) => assert_eq!(s.as_ref(), b"restored"),
            _ => panic!("expected string"),
        }
    }

    #[test]
    fn test_restore_replace_flag_case_insensitive() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "k", "old");
        let result = restore(
            &mut ctx,
            &[bulk("k"), bulk("0"), bulk("data"), bulk("replace")],
        )
        .unwrap();
        assert_eq!(result, RespValue::ok());
    }

    #[test]
    fn test_restore_replace_flag_mixed_case() {
        let mut ctx = make_ctx();
        set_key(&mut ctx, "k", "old");
        let result = restore(
            &mut ctx,
            &[bulk("k"), bulk("0"), bulk("data"), bulk("Replace")],
        )
        .unwrap();
        assert_eq!(result, RespValue::ok());
    }

    #[test]
    fn test_restore_non_integer_ttl_errors() {
        let mut ctx = make_ctx();
        let result = restore(&mut ctx, &[bulk("k"), bulk("abc"), bulk("data")]);
        assert!(result.is_err());
    }

    #[test]
    fn test_restore_expired_existing_key_without_replace_succeeds() {
        use std::time::Instant;
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(ctx.selected_db());
            let mut entry = Entry::new(RedisValue::String(Bytes::from("old")));
            entry.expires_at = Some(Instant::now() - Duration::from_secs(1));
            db.set(Bytes::from("k"), entry);
        }
        // Key is expired, so restore without REPLACE should succeed
        let result = restore(&mut ctx, &[bulk("k"), bulk("0"), bulk("data")]).unwrap();
        assert_eq!(result, RespValue::ok());
    }

    #[test]
    fn test_restore_creates_value_from_serialized_data() {
        let mut ctx = make_ctx();
        // Regardless of what serialized data we pass, the simplified restore always creates "restored"
        restore(&mut ctx, &[bulk("k"), bulk("0"), bulk("anything")]).unwrap();
        let db = ctx.store().database(ctx.selected_db());
        let entry = db.get(b"k").unwrap();
        match &entry.value {
            RedisValue::String(s) => assert_eq!(s.as_ref(), b"restored"),
            _ => panic!("expected string"),
        }
    }

    #[test]
    fn test_restore_special_chars_key() {
        let mut ctx = make_ctx();
        let result = restore(
            &mut ctx,
            &[bulk("key with spaces!"), bulk("0"), bulk("data")],
        )
        .unwrap();
        assert_eq!(result, RespValue::ok());
        let db = ctx.store().database(ctx.selected_db());
        assert!(db.get(b"key with spaces!").is_some());
    }

    #[test]
    fn test_restore_empty_key_name() {
        let mut ctx = make_ctx();
        let result = restore(&mut ctx, &[bulk(""), bulk("0"), bulk("data")]).unwrap();
        assert_eq!(result, RespValue::ok());
        let db = ctx.store().database(ctx.selected_db());
        assert!(db.get(b"").is_some());
    }

    #[test]
    fn test_restore_overwrites_different_type_with_replace() {
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(ctx.selected_db());
            let mut list = VecDeque::new();
            list.push_back(Bytes::from("item"));
            db.set(Bytes::from("k"), Entry::new(RedisValue::List(list)));
        }
        let result = restore(
            &mut ctx,
            &[bulk("k"), bulk("0"), bulk("data"), bulk("REPLACE")],
        )
        .unwrap();
        assert_eq!(result, RespValue::ok());
        let db = ctx.store().database(ctx.selected_db());
        let entry = db.get(b"k").unwrap();
        assert!(matches!(&entry.value, RedisValue::String(_)));
    }

    #[test]
    fn test_restore_different_type_without_replace_errors() {
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(ctx.selected_db());
            let mut set = HashSet::new();
            set.insert(Bytes::from("member"));
            db.set(Bytes::from("k"), Entry::new(RedisValue::Set(set)));
        }
        let result = restore(&mut ctx, &[bulk("k"), bulk("0"), bulk("data")]);
        assert!(result.is_err());
    }

    #[test]
    fn test_restore_then_dump_returns_string_type() {
        let mut ctx = make_ctx();
        restore(&mut ctx, &[bulk("k"), bulk("0"), bulk("data")]).unwrap();
        let result = dump(&mut ctx, &[bulk("k")]).unwrap();
        // restore creates a string, so dump should return type byte 0
        assert_eq!(result, RespValue::BulkString(Bytes::from(vec![0u8])));
    }

    #[test]
    fn test_restore_negative_ttl_treated_as_no_expiry() {
        let mut ctx = make_ctx();
        // TTL -1 means "no expiry" since ttl_ms > 0 check won't trigger
        restore(&mut ctx, &[bulk("k"), bulk("-1"), bulk("data")]).unwrap();
        let db = ctx.store().database(ctx.selected_db());
        let entry = db.get(b"k").unwrap();
        assert!(entry.expires_at.is_none());
    }

    // SORT tests
    #[test]
    fn test_sort_list_numeric() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());

        let mut list = VecDeque::new();
        list.push_back(Bytes::from("3"));
        list.push_back(Bytes::from("1"));
        list.push_back(Bytes::from("2"));
        db.set(Bytes::from("mylist"), Entry::new(RedisValue::List(list)));

        let result = sort(&mut ctx, &[bulk("mylist")]).unwrap();
        assert_eq!(
            result,
            RespValue::Array(vec![bulk("1"), bulk("2"), bulk("3")])
        );
    }

    #[test]
    fn test_sort_list_alpha() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());

        let mut list = VecDeque::new();
        list.push_back(Bytes::from("charlie"));
        list.push_back(Bytes::from("alice"));
        list.push_back(Bytes::from("bob"));
        db.set(Bytes::from("mylist"), Entry::new(RedisValue::List(list)));

        let result = sort(&mut ctx, &[bulk("mylist"), bulk("ALPHA")]).unwrap();
        assert_eq!(
            result,
            RespValue::Array(vec![bulk("alice"), bulk("bob"), bulk("charlie")])
        );
    }

    #[test]
    fn test_sort_desc() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());

        let mut list = VecDeque::new();
        list.push_back(Bytes::from("1"));
        list.push_back(Bytes::from("2"));
        list.push_back(Bytes::from("3"));
        db.set(Bytes::from("mylist"), Entry::new(RedisValue::List(list)));

        let result = sort(&mut ctx, &[bulk("mylist"), bulk("DESC")]).unwrap();
        assert_eq!(
            result,
            RespValue::Array(vec![bulk("3"), bulk("2"), bulk("1")])
        );
    }

    #[test]
    fn test_sort_with_limit() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());

        let mut list = VecDeque::new();
        list.push_back(Bytes::from("5"));
        list.push_back(Bytes::from("1"));
        list.push_back(Bytes::from("3"));
        list.push_back(Bytes::from("2"));
        list.push_back(Bytes::from("4"));
        db.set(Bytes::from("mylist"), Entry::new(RedisValue::List(list)));

        // Sort and get elements 1 and 2 (0-indexed)
        let result = sort(
            &mut ctx,
            &[bulk("mylist"), bulk("LIMIT"), bulk("1"), bulk("2")],
        )
        .unwrap();
        assert_eq!(result, RespValue::Array(vec![bulk("2"), bulk("3")]));
    }

    #[test]
    fn test_sort_set() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());

        let mut set = HashSet::new();
        set.insert(Bytes::from("3"));
        set.insert(Bytes::from("1"));
        set.insert(Bytes::from("2"));
        db.set(Bytes::from("myset"), Entry::new(RedisValue::Set(set)));

        let result = sort(&mut ctx, &[bulk("myset")]).unwrap();
        // Sets are unordered, but SORT should order them
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], bulk("1"));
            assert_eq!(arr[1], bulk("2"));
            assert_eq!(arr[2], bulk("3"));
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_sort_with_store() {
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(ctx.selected_db());
            let mut list = VecDeque::new();
            list.push_back(Bytes::from("3"));
            list.push_back(Bytes::from("1"));
            list.push_back(Bytes::from("2"));
            db.set(Bytes::from("mylist"), Entry::new(RedisValue::List(list)));
        }

        let result = sort(&mut ctx, &[bulk("mylist"), bulk("STORE"), bulk("sorted")]).unwrap();
        assert_eq!(result, RespValue::Integer(3));

        // Check stored list
        let db = ctx.store().database(ctx.selected_db());
        if let Some(entry) = db.get(b"sorted") {
            if let RedisValue::List(list) = &entry.value {
                assert_eq!(list.len(), 3);
                assert_eq!(list[0], Bytes::from("1"));
                assert_eq!(list[1], Bytes::from("2"));
                assert_eq!(list[2], Bytes::from("3"));
            } else {
                panic!("Expected list");
            }
        } else {
            panic!("Key not found");
        }
    }

    #[test]
    fn test_sort_nonexistent_key() {
        let mut ctx = make_ctx();
        let result = sort(&mut ctx, &[bulk("nokey")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![]));
    }

    #[test]
    fn test_sort_with_get() {
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(ctx.selected_db());

            // Create list with IDs
            let mut list = VecDeque::new();
            list.push_back(Bytes::from("1"));
            list.push_back(Bytes::from("2"));
            db.set(Bytes::from("ids"), Entry::new(RedisValue::List(list)));

            // Create associated values
            db.set(
                Bytes::from("obj:1"),
                Entry::new(RedisValue::String(Bytes::from("first"))),
            );
            db.set(
                Bytes::from("obj:2"),
                Entry::new(RedisValue::String(Bytes::from("second"))),
            );
        }

        let result = sort(&mut ctx, &[bulk("ids"), bulk("GET"), bulk("obj:*")]).unwrap();
        assert_eq!(
            result,
            RespValue::Array(vec![bulk("first"), bulk("second")])
        );
    }

    #[test]
    fn test_sort_ro_basic() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());

        let mut list = VecDeque::new();
        list.push_back(Bytes::from("3"));
        list.push_back(Bytes::from("1"));
        list.push_back(Bytes::from("2"));
        db.set(Bytes::from("mylist"), Entry::new(RedisValue::List(list)));

        let result = sort_ro(&mut ctx, &[bulk("mylist")]).unwrap();
        assert_eq!(
            result,
            RespValue::Array(vec![bulk("1"), bulk("2"), bulk("3")])
        );
    }

    #[test]
    fn test_sort_ro_rejects_store() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());

        let mut list = VecDeque::new();
        list.push_back(Bytes::from("1"));
        db.set(Bytes::from("mylist"), Entry::new(RedisValue::List(list)));

        let result = sort_ro(&mut ctx, &[bulk("mylist"), bulk("STORE"), bulk("dest")]);
        assert!(result.is_err());
    }
}

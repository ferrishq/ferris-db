//! String commands: GET, SET, MGET, MSET, INCR, DECR, APPEND, STRLEN, etc.

use crate::{CommandContext, CommandError, CommandResult};
use bytes::Bytes;
use ferris_core::{store::UpdateAction, Entry, RedisValue};
use ferris_protocol::RespValue;
use std::time::Duration;

/// Helper to parse an integer from RespValue
fn parse_int(arg: &RespValue) -> Result<i64, CommandError> {
    let s = arg.as_str().ok_or(CommandError::NotAnInteger)?;
    s.parse().map_err(|_| CommandError::NotAnInteger)
}

/// Helper to parse a float from RespValue
fn parse_float(arg: &RespValue) -> Result<f64, CommandError> {
    let s = arg.as_str().ok_or(CommandError::NotAFloat)?;
    s.parse().map_err(|_| CommandError::NotAFloat)
}

/// Helper to get bytes from RespValue
#[inline]
fn get_bytes(arg: &RespValue) -> Result<Bytes, CommandError> {
    arg.as_bytes()
        .cloned()
        .or_else(|| arg.as_str().map(|s| Bytes::copy_from_slice(s.as_bytes())))
        .ok_or_else(|| CommandError::InvalidArgument("invalid argument".to_string()))
}

/// GET key
///
/// Get the value of key. If the key does not exist the special value nil is returned.
///
/// Time complexity: O(1)
#[inline]
pub fn get(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    let key = get_bytes(
        args.first()
            .ok_or_else(|| CommandError::WrongArity("GET".to_string()))?,
    )?;

    let db = ctx.store().database(ctx.selected_db());

    match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                return Ok(RespValue::Null);
            }
            match &entry.value {
                RedisValue::String(s) => Ok(RespValue::BulkString(s.clone())),
                _ => Err(CommandError::WrongType),
            }
        }
        None => Ok(RespValue::Null),
    }
}

/// SET key value \[EX seconds | PX milliseconds | EXAT timestamp | PXAT timestamp | KEEPTTL\] \[NX | XX\] \[GET\]
///
/// Set key to hold the string value.
///
/// Time complexity: O(1)
#[inline]
pub fn set(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("SET".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let value = get_bytes(&args[1])?;

    let mut ttl: Option<Duration> = None;
    let mut nx = false;
    let mut xx = false;
    let mut get_old = false;
    let mut keepttl = false;

    // Parse options
    let mut i = 2;
    while i < args.len() {
        let opt = args[i].as_str().map(|s| s.to_uppercase());
        match opt.as_deref() {
            Some("EX") => {
                if ttl.is_some() {
                    return Err(CommandError::SyntaxError);
                }
                i += 1;
                let secs = parse_int(args.get(i).ok_or(CommandError::SyntaxError)?)?;
                if secs <= 0 {
                    return Err(CommandError::InvalidArgument(
                        "invalid expire time in 'set' command".to_string(),
                    ));
                }
                ttl = Some(Duration::from_secs(secs as u64));
            }
            Some("PX") => {
                if ttl.is_some() {
                    return Err(CommandError::SyntaxError);
                }
                i += 1;
                let ms = parse_int(args.get(i).ok_or(CommandError::SyntaxError)?)?;
                if ms <= 0 {
                    return Err(CommandError::InvalidArgument(
                        "invalid expire time in 'set' command".to_string(),
                    ));
                }
                ttl = Some(Duration::from_millis(ms as u64));
            }
            Some("EXAT") => {
                if ttl.is_some() {
                    return Err(CommandError::SyntaxError);
                }
                i += 1;
                let timestamp = parse_int(args.get(i).ok_or(CommandError::SyntaxError)?)?;
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64;
                let secs = timestamp - now;
                if secs <= 0 {
                    ttl = Some(Duration::from_secs(0)); // Already expired
                } else {
                    ttl = Some(Duration::from_secs(secs as u64));
                }
            }
            Some("PXAT") => {
                if ttl.is_some() {
                    return Err(CommandError::SyntaxError);
                }
                i += 1;
                let timestamp = parse_int(args.get(i).ok_or(CommandError::SyntaxError)?)?;
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64;
                let ms = timestamp - now;
                if ms <= 0 {
                    ttl = Some(Duration::from_millis(0)); // Already expired
                } else {
                    ttl = Some(Duration::from_millis(ms as u64));
                }
            }
            Some("NX") => nx = true,
            Some("XX") => xx = true,
            Some("GET") => get_old = true,
            Some("KEEPTTL") => keepttl = true,
            Some(_) => return Err(CommandError::SyntaxError),
            None => return Err(CommandError::SyntaxError),
        }
        i += 1;
    }

    // NX and XX are mutually exclusive
    if nx && xx {
        return Err(CommandError::SyntaxError);
    }

    let db = ctx.store().database(ctx.selected_db());
    let existing = db.get(&key);

    // Handle NX/XX conditions
    if nx {
        if let Some(entry) = existing.as_ref() {
            if !entry.is_expired() {
                return if get_old {
                    match &entry.value {
                        RedisValue::String(s) => Ok(RespValue::BulkString(s.clone())),
                        _ => Err(CommandError::WrongType),
                    }
                } else {
                    Ok(RespValue::Null)
                };
            }
        }
    }
    if xx {
        let should_reject = existing.as_ref().map_or(true, |e| e.is_expired());
        if should_reject {
            return Ok(RespValue::Null);
        }
    }

    // Get old value if requested
    let old_value = if get_old {
        match existing.as_ref() {
            Some(e) if !e.is_expired() => match &e.value {
                RedisValue::String(s) => Some(s.clone()),
                _ => return Err(CommandError::WrongType),
            },
            _ => None,
        }
    } else {
        None
    };

    // Preserve TTL if KEEPTTL
    let preserved_ttl = if keepttl {
        existing.as_ref().and_then(|e| e.expires_at)
    } else {
        None
    };

    // Create new entry
    let mut entry = Entry::new(RedisValue::String(value));
    if let Some(duration) = ttl {
        entry.set_ttl(duration);
    } else if let Some(expires_at) = preserved_ttl {
        entry.expires_at = Some(expires_at);
    }

    db.set(key.clone(), entry);

    // Propagate to AOF and replication
    ctx.propagate_args(args);

    if get_old {
        match old_value {
            Some(v) => Ok(RespValue::BulkString(v)),
            None => Ok(RespValue::Null),
        }
    } else {
        Ok(RespValue::ok())
    }
}

/// SETNX key value
///
/// Set key to hold string value if key does not exist.
///
/// Time complexity: O(1)
pub fn setnx(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("SETNX".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let value = get_bytes(&args[1])?;

    let db = ctx.store().database(ctx.selected_db());

    if let Some(entry) = db.get(&key) {
        if !entry.is_expired() {
            return Ok(RespValue::Integer(0));
        }
    }

    db.set(key.clone(), Entry::new(RedisValue::String(value.clone())));

    // Propagate to AOF and replication
    ctx.propagate(&[
        RespValue::BulkString(Bytes::from("SETNX")),
        RespValue::BulkString(key),
        RespValue::BulkString(value),
    ]);

    Ok(RespValue::Integer(1))
}

/// SETEX key seconds value
///
/// Set key to hold the string value and set key to timeout after a given number of seconds.
///
/// Time complexity: O(1)
pub fn setex(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongArity("SETEX".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let seconds = parse_int(&args[1])?;
    let value = get_bytes(&args[2])?;

    if seconds <= 0 {
        return Err(CommandError::InvalidArgument(
            "invalid expire time in 'setex' command".to_string(),
        ));
    }

    let db = ctx.store().database(ctx.selected_db());
    let mut entry = Entry::new(RedisValue::String(value.clone()));
    entry.set_ttl(Duration::from_secs(seconds as u64));
    db.set(key.clone(), entry);

    // Propagate to AOF and replication
    ctx.propagate(&[
        RespValue::BulkString(Bytes::from("SETEX")),
        RespValue::BulkString(key),
        args[1].clone(),
        RespValue::BulkString(value),
    ]);

    Ok(RespValue::ok())
}

/// PSETEX key milliseconds value
///
/// Set key to hold the string value and set key to timeout after a given number of milliseconds.
///
/// Time complexity: O(1)
pub fn psetex(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongArity("PSETEX".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let ms = parse_int(&args[1])?;
    let value = get_bytes(&args[2])?;

    if ms <= 0 {
        return Err(CommandError::InvalidArgument(
            "invalid expire time in 'psetex' command".to_string(),
        ));
    }

    let db = ctx.store().database(ctx.selected_db());
    let mut entry = Entry::new(RedisValue::String(value.clone()));
    entry.set_ttl(Duration::from_millis(ms as u64));
    db.set(key.clone(), entry);

    // Propagate to AOF and replication
    ctx.propagate(&[
        RespValue::BulkString(Bytes::from("PSETEX")),
        RespValue::BulkString(key),
        args[1].clone(),
        RespValue::BulkString(value),
    ]);

    Ok(RespValue::ok())
}

/// MGET key [key ...]
///
/// Returns the values of all specified keys.
///
/// Time complexity: O(N) where N is the number of keys
#[inline]
pub fn mget(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("MGET".to_string()));
    }

    // Validate all keys are in the same slot (cluster mode)
    let keys: Vec<&[u8]> = args
        .iter()
        .filter_map(|arg| arg.as_bytes().map(|b| b.as_ref()))
        .collect();
    crate::cluster::validate_same_slot(ctx, &keys)?;

    let db = ctx.store().database(ctx.selected_db());
    let mut results = Vec::with_capacity(args.len());

    for arg in args {
        let key = get_bytes(arg)?;
        match db.get(&key) {
            Some(entry) => {
                if entry.is_expired() {
                    db.delete(&key);
                    results.push(RespValue::Null);
                } else {
                    match &entry.value {
                        RedisValue::String(s) => results.push(RespValue::BulkString(s.clone())),
                        _ => results.push(RespValue::Null), // Wrong type returns nil in MGET
                    }
                }
            }
            None => results.push(RespValue::Null),
        }
    }

    Ok(RespValue::Array(results))
}

/// MSET key value [key value ...]
///
/// Sets the given keys to their respective values.
///
/// Time complexity: O(N) where N is the number of keys
#[inline]
pub fn mset(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 || args.len() % 2 != 0 {
        return Err(CommandError::WrongArity("MSET".to_string()));
    }

    // Validate all keys are in the same slot (cluster mode)
    let keys: Vec<&[u8]> = args
        .iter()
        .step_by(2)
        .filter_map(|arg| arg.as_bytes().map(|b| b.as_ref()))
        .collect();
    crate::cluster::validate_same_slot(ctx, &keys)?;

    let db = ctx.store().database(ctx.selected_db());

    for chunk in args.chunks(2) {
        let key = get_bytes(&chunk[0])?;
        let value = get_bytes(&chunk[1])?;
        db.set(key, Entry::new(RedisValue::String(value)));
    }

    // Propagate to AOF and replication
    ctx.propagate_args(args);

    Ok(RespValue::ok())
}

/// MSETNX key value [key value ...]
///
/// Sets the given keys to their respective values, only if none of the keys exist.
///
/// Time complexity: O(N) where N is the number of keys
pub fn msetnx(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 || args.len() % 2 != 0 {
        return Err(CommandError::WrongArity("MSETNX".to_string()));
    }

    // Validate all keys are in the same slot (cluster mode)
    let keys: Vec<&[u8]> = args
        .iter()
        .step_by(2)
        .filter_map(|arg| arg.as_bytes().map(|b| b.as_ref()))
        .collect();
    crate::cluster::validate_same_slot(ctx, &keys)?;

    let db = ctx.store().database(ctx.selected_db());

    // First check if any key exists
    for chunk in args.chunks(2) {
        let key = get_bytes(&chunk[0])?;
        if let Some(entry) = db.get(&key) {
            if !entry.is_expired() {
                return Ok(RespValue::Integer(0));
            }
        }
    }

    // None exist, set all
    for chunk in args.chunks(2) {
        let key = get_bytes(&chunk[0])?;
        let value = get_bytes(&chunk[1])?;
        db.set(key, Entry::new(RedisValue::String(value)));
    }

    // Propagate to AOF and replication
    ctx.propagate_args(args);

    Ok(RespValue::Integer(1))
}

/// INCR key
///
/// Increments the number stored at key by one.
///
/// Time complexity: O(1)
#[inline]
pub fn incr(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    incrby_impl(ctx, args, 1, "INCR")
}

/// INCRBY key increment
///
/// Increments the number stored at key by increment.
///
/// Time complexity: O(1)
#[inline]
pub fn incrby(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("INCRBY".to_string()));
    }
    let increment = parse_int(&args[1])?;
    incrby_impl(ctx, args, increment, "INCRBY")
}

fn incrby_impl(
    ctx: &mut CommandContext,
    args: &[RespValue],
    increment: i64,
    cmd_name: &str,
) -> CommandResult {
    let key = get_bytes(
        args.first()
            .ok_or_else(|| CommandError::WrongArity(cmd_name.to_string()))?,
    )?;

    let db = ctx.store().database(ctx.selected_db());

    // Use db.update() so the entire read-parse-compute-write happens under a
    // single DashMap shard lock — eliminating the TOCTOU race that occurred
    // when two concurrent INCRs both read the same old value before either wrote.
    let result: Result<i64, CommandError> = db.update(key.clone(), |entry| {
        let current: i64 = match entry {
            Some(e) => match &e.value {
                RedisValue::String(s) => {
                    let s_str = match std::str::from_utf8(s) {
                        Ok(v) => v,
                        Err(_) => return (UpdateAction::Keep, Err(CommandError::NotAnInteger)),
                    };
                    match s_str.parse::<i64>() {
                        Ok(v) => v,
                        Err(_) => return (UpdateAction::Keep, Err(CommandError::NotAnInteger)),
                    }
                }
                _ => return (UpdateAction::Keep, Err(CommandError::WrongType)),
            },
            None => 0,
        };

        let new_value = match current.checked_add(increment) {
            Some(v) => v,
            None => return (UpdateAction::Keep, Err(CommandError::NotAnInteger)),
        };

        let new_entry = Entry::new(RedisValue::String(Bytes::from(new_value.to_string())));
        (UpdateAction::Set(new_entry), Ok(new_value))
    });

    let new_value = result?;

    // Propagate to AOF and replication
    ctx.propagate_with_name(cmd_name, args);

    Ok(RespValue::Integer(new_value))
}

/// INCRBYFLOAT key increment
///
/// Increments the floating point number stored at key by increment.
///
/// Time complexity: O(1)
pub fn incrbyfloat(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("INCRBYFLOAT".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let increment = parse_float(&args[1])?;

    let db = ctx.store().database(ctx.selected_db());

    // Atomic read-compute-write under one shard lock (same fix as incrby_impl).
    let result: Result<String, CommandError> = db.update(key.clone(), |entry| {
        let current: f64 = match entry {
            Some(e) => match &e.value {
                RedisValue::String(s) => {
                    let s_str = match std::str::from_utf8(s) {
                        Ok(v) => v,
                        Err(_) => return (UpdateAction::Keep, Err(CommandError::NotAFloat)),
                    };
                    match s_str.parse::<f64>() {
                        Ok(v) => v,
                        Err(_) => return (UpdateAction::Keep, Err(CommandError::NotAFloat)),
                    }
                }
                _ => return (UpdateAction::Keep, Err(CommandError::WrongType)),
            },
            None => 0.0,
        };

        let new_value = current + increment;
        if new_value.is_nan() || new_value.is_infinite() {
            return (UpdateAction::Keep, Err(CommandError::NotAFloat));
        }

        let formatted = if new_value.fract() == 0.0 {
            format!("{}", new_value as i64)
        } else {
            format!("{new_value}")
        };

        let new_entry = Entry::new(RedisValue::String(Bytes::from(formatted.clone())));
        (UpdateAction::Set(new_entry), Ok(formatted))
    });

    let formatted = result?;

    // Propagate to AOF and replication
    ctx.propagate(&[
        RespValue::BulkString(Bytes::from("INCRBYFLOAT")),
        RespValue::BulkString(key),
        args[1].clone(),
    ]);

    Ok(RespValue::BulkString(Bytes::from(formatted)))
}

/// DECR key
///
/// Decrements the number stored at key by one.
///
/// Time complexity: O(1)
#[inline]
pub fn decr(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    incrby_impl(ctx, args, -1, "DECR")
}

/// DECRBY key decrement
///
/// Decrements the number stored at key by decrement.
///
/// Time complexity: O(1)
#[inline]
pub fn decrby(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("DECRBY".to_string()));
    }
    let decrement = parse_int(&args[1])?;
    incrby_impl(ctx, args, -decrement, "DECRBY")
}

/// APPEND key value
///
/// Appends value to the end of string at key.
///
/// Time complexity: O(1)
#[inline]
pub fn append(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("APPEND".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let append_value = get_bytes(&args[1])?;

    let db = ctx.store().database(ctx.selected_db());

    let new_value = match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                append_value.clone()
            } else {
                match &entry.value {
                    RedisValue::String(s) => {
                        let mut new = s.to_vec();
                        new.extend_from_slice(&append_value);
                        Bytes::from(new)
                    }
                    _ => return Err(CommandError::WrongType),
                }
            }
        }
        None => append_value.clone(),
    };

    let len = new_value.len();
    db.set(key.clone(), Entry::new(RedisValue::String(new_value)));

    // Propagate to AOF and replication
    ctx.propagate(&[
        RespValue::BulkString(Bytes::from("APPEND")),
        RespValue::BulkString(key),
        args[1].clone(),
    ]);

    Ok(RespValue::Integer(len as i64))
}

/// STRLEN key
///
/// Returns the length of the string value stored at key.
///
/// Time complexity: O(1)
#[inline]
pub fn strlen(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    let key = get_bytes(
        args.first()
            .ok_or_else(|| CommandError::WrongArity("STRLEN".to_string()))?,
    )?;

    let db = ctx.store().database(ctx.selected_db());

    match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                return Ok(RespValue::Integer(0));
            }
            match &entry.value {
                RedisValue::String(s) => Ok(RespValue::Integer(s.len() as i64)),
                _ => Err(CommandError::WrongType),
            }
        }
        None => Ok(RespValue::Integer(0)),
    }
}

/// GETRANGE key start end
///
/// Returns the substring of the string value stored at key.
///
/// Time complexity: O(N) where N is the length of the returned string
pub fn getrange(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongArity("GETRANGE".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let start = parse_int(&args[1])?;
    let end = parse_int(&args[2])?;

    let db = ctx.store().database(ctx.selected_db());

    match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                return Ok(RespValue::BulkString(Bytes::new()));
            }
            match &entry.value {
                RedisValue::String(s) => {
                    let len = s.len() as i64;
                    if len == 0 {
                        return Ok(RespValue::BulkString(Bytes::new()));
                    }

                    // Normalize indices
                    let start = if start < 0 {
                        (len + start).max(0)
                    } else {
                        start.min(len)
                    };
                    let end = if end < 0 {
                        (len + end).max(-1)
                    } else {
                        end.min(len - 1)
                    };

                    if start > end || start >= len {
                        return Ok(RespValue::BulkString(Bytes::new()));
                    }

                    let result = s.slice(start as usize..=end as usize);
                    Ok(RespValue::BulkString(result))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        None => Ok(RespValue::BulkString(Bytes::new())),
    }
}

/// SUBSTR key start end (deprecated alias for GETRANGE)
///
/// Returns the substring of the string value stored at key.
/// This command is deprecated. Use GETRANGE instead.
///
/// Time complexity: O(N) where N is the length of the returned string
pub fn substr(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    getrange(ctx, args)
}

/// SETRANGE key offset value
///
/// Overwrites part of the string stored at key, starting at the specified offset.
///
/// Time complexity: O(1)
pub fn setrange(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongArity("SETRANGE".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let offset = parse_int(&args[1])?;
    let value = get_bytes(&args[2])?;

    if offset < 0 {
        return Err(CommandError::InvalidArgument(
            "offset is out of range".to_string(),
        ));
    }
    let offset = offset as usize;

    let db = ctx.store().database(ctx.selected_db());

    let current = match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                Vec::new()
            } else {
                match &entry.value {
                    RedisValue::String(s) => s.to_vec(),
                    _ => return Err(CommandError::WrongType),
                }
            }
        }
        None => Vec::new(),
    };

    // Expand if needed
    let needed_len = offset + value.len();
    let mut new_value = current;
    if new_value.len() < needed_len {
        new_value.resize(needed_len, 0);
    }

    // Overwrite
    new_value[offset..offset + value.len()].copy_from_slice(&value);

    let len = new_value.len();
    db.set(
        key.clone(),
        Entry::new(RedisValue::String(Bytes::from(new_value))),
    );

    // Propagate to AOF and replication
    ctx.propagate(&[
        RespValue::BulkString(Bytes::from("SETRANGE")),
        RespValue::BulkString(key),
        args[1].clone(),
        args[2].clone(),
    ]);

    Ok(RespValue::Integer(len as i64))
}

/// GETSET key value (deprecated, use SET ... GET)
///
/// Atomically sets key to value and returns the old value.
///
/// Time complexity: O(1)
pub fn getset(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("GETSET".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let value = get_bytes(&args[1])?;

    let db = ctx.store().database(ctx.selected_db());

    let old = match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                None
            } else {
                match &entry.value {
                    RedisValue::String(s) => Some(s.clone()),
                    _ => return Err(CommandError::WrongType),
                }
            }
        }
        None => None,
    };

    db.set(key.clone(), Entry::new(RedisValue::String(value.clone())));

    // Propagate to AOF and replication
    ctx.propagate(&[
        RespValue::BulkString(Bytes::from("GETSET")),
        RespValue::BulkString(key),
        RespValue::BulkString(value),
    ]);

    match old {
        Some(v) => Ok(RespValue::BulkString(v)),
        None => Ok(RespValue::Null),
    }
}

/// GETDEL key
///
/// Get the value of key and delete the key.
///
/// Time complexity: O(1)
pub fn getdel(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    let key = get_bytes(
        args.first()
            .ok_or_else(|| CommandError::WrongArity("GETDEL".to_string()))?,
    )?;

    let db = ctx.store().database(ctx.selected_db());

    match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                return Ok(RespValue::Null);
            }
            match &entry.value {
                RedisValue::String(s) => {
                    let result = s.clone();
                    db.delete(&key);

                    // Propagate to AOF (as DEL command)
                    ctx.propagate(&[
                        RespValue::BulkString(Bytes::from("DEL")),
                        RespValue::BulkString(key),
                    ]);

                    Ok(RespValue::BulkString(result))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        None => Ok(RespValue::Null),
    }
}

/// GETEX key [EXAT timestamp | PXAT timestamp | EX seconds | PX milliseconds | PERSIST]
///
/// Get the value of key and optionally set its expiration.
///
/// Time complexity: O(1)
pub fn getex(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("GETEX".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let db = ctx.store().database(ctx.selected_db());

    let entry = match db.get(&key) {
        Some(e) => {
            if e.is_expired() {
                db.delete(&key);
                return Ok(RespValue::Null);
            }
            e
        }
        None => return Ok(RespValue::Null),
    };

    let value = match &entry.value {
        RedisValue::String(s) => s.clone(),
        _ => return Err(CommandError::WrongType),
    };

    // Parse options and update TTL if specified
    if args.len() > 1 {
        let opt = args[1].as_str().map(|s| s.to_uppercase());
        let mut new_entry = entry.clone();

        match opt.as_deref() {
            Some("EX") => {
                let secs = parse_int(args.get(2).ok_or(CommandError::SyntaxError)?)?;
                if secs <= 0 {
                    return Err(CommandError::InvalidArgument(
                        "invalid expire time in 'getex' command".to_string(),
                    ));
                }
                new_entry.set_ttl(Duration::from_secs(secs as u64));
            }
            Some("PX") => {
                let ms = parse_int(args.get(2).ok_or(CommandError::SyntaxError)?)?;
                if ms <= 0 {
                    return Err(CommandError::InvalidArgument(
                        "invalid expire time in 'getex' command".to_string(),
                    ));
                }
                new_entry.set_ttl(Duration::from_millis(ms as u64));
            }
            Some("EXAT") => {
                let timestamp = parse_int(args.get(2).ok_or(CommandError::SyntaxError)?)?;
                if timestamp <= 0 {
                    return Err(CommandError::InvalidArgument(
                        "invalid expire time in 'getex' command".to_string(),
                    ));
                }
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64;
                let secs = (timestamp - now).max(0);
                new_entry.set_ttl(Duration::from_secs(secs as u64));
            }
            Some("PXAT") => {
                let timestamp = parse_int(args.get(2).ok_or(CommandError::SyntaxError)?)?;
                if timestamp <= 0 {
                    return Err(CommandError::InvalidArgument(
                        "invalid expire time in 'getex' command".to_string(),
                    ));
                }
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64;
                let ms = (timestamp - now).max(0);
                new_entry.set_ttl(Duration::from_millis(ms as u64));
            }
            Some("PERSIST") => {
                new_entry.persist();
            }
            _ => return Err(CommandError::SyntaxError),
        }

        db.set(key.clone(), new_entry);

        // Propagate to AOF and replication
        ctx.propagate_args(args);
    }

    Ok(RespValue::BulkString(value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferris_core::KeyStore;
    use std::sync::Arc;

    fn make_ctx() -> CommandContext {
        CommandContext::new(Arc::new(KeyStore::default()))
    }

    fn bulk(s: &str) -> RespValue {
        RespValue::BulkString(Bytes::from(s.to_owned()))
    }

    #[test]
    fn test_get_nonexistent() {
        let mut ctx = make_ctx();
        let result = get(&mut ctx, &[bulk("nokey")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_set_get_basic() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("value")]).unwrap();
        let result = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, bulk("value"));
    }

    // ========== GET command tests ==========

    #[test]
    fn test_get_empty_string_value() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("")]).unwrap();
        let result = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from("")));
    }

    #[test]
    fn test_get_wrong_type_list() {
        use std::collections::VecDeque;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("item"));
        db.set(Bytes::from("mylist"), Entry::new(RedisValue::List(list)));

        let result = get(&mut ctx, &[bulk("mylist")]);
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    #[test]
    fn test_get_wrong_type_set() {
        use std::collections::HashSet;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        db.set(
            Bytes::from("myset"),
            Entry::new(RedisValue::Set(HashSet::from([Bytes::from("a")]))),
        );

        let result = get(&mut ctx, &[bulk("myset")]);
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    #[test]
    fn test_get_wrong_type_hash() {
        use std::collections::HashMap;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        db.set(
            Bytes::from("myhash"),
            Entry::new(RedisValue::Hash(HashMap::from([(
                Bytes::from("f"),
                Bytes::from("v"),
            )]))),
        );

        let result = get(&mut ctx, &[bulk("myhash")]);
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    #[test]
    fn test_get_wrong_type_zset() {
        use ordered_float::OrderedFloat;
        use std::collections::{BTreeMap, HashMap};
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut scores = HashMap::new();
        scores.insert(Bytes::from("member"), 1.0_f64);
        let mut members = BTreeMap::new();
        members.insert((OrderedFloat(1.0), Bytes::from("member")), ());
        db.set(
            Bytes::from("myzset"),
            Entry::new(RedisValue::SortedSet { scores, members }),
        );

        let result = get(&mut ctx, &[bulk("myzset")]);
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    #[test]
    fn test_get_expired_key() {
        use std::time::Instant;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut entry = Entry::new(RedisValue::String(Bytes::from("old")));
        entry.expires_at = Some(Instant::now() - Duration::from_secs(1));
        db.set(Bytes::from("key"), entry);

        let result = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_get_binary_value() {
        let mut ctx = make_ctx();
        let binary_data = Bytes::from(vec![0x00, 0xFF, 0x80, 0xFE, 0x01]);
        let db = ctx.store().database(ctx.selected_db());
        db.set(
            Bytes::from("binkey"),
            Entry::new(RedisValue::String(binary_data.clone())),
        );

        let result = get(&mut ctx, &[bulk("binkey")]).unwrap();
        assert_eq!(result, RespValue::BulkString(binary_data));
    }

    #[test]
    fn test_get_large_value() {
        let mut ctx = make_ctx();
        let large_value = "x".repeat(1_000_000);
        set(&mut ctx, &[bulk("bigkey"), bulk(&large_value)]).unwrap();

        let result = get(&mut ctx, &[bulk("bigkey")]).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from(large_value)));
    }

    #[test]
    fn test_get_numeric_string() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("numkey"), bulk("42")]).unwrap();

        let result = get(&mut ctx, &[bulk("numkey")]).unwrap();
        assert_eq!(result, bulk("42"));
        // Ensure it's a BulkString, not an Integer
        assert!(matches!(result, RespValue::BulkString(_)));
    }

    #[test]
    fn test_get_overwritten_value() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("first")]).unwrap();
        set(&mut ctx, &[bulk("key"), bulk("second")]).unwrap();

        let result = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, bulk("second"));
    }

    #[test]
    fn test_get_after_delete() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("value")]).unwrap();

        // Delete using the store directly
        let db = ctx.store().database(ctx.selected_db());
        assert!(db.delete(b"key".as_ref()));

        let result = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_get_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        let result = get(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_get_multiple_keys_isolated() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key1"), bulk("val1")]).unwrap();
        set(&mut ctx, &[bulk("key2"), bulk("val2")]).unwrap();

        let r1 = get(&mut ctx, &[bulk("key1")]).unwrap();
        let r2 = get(&mut ctx, &[bulk("key2")]).unwrap();
        assert_eq!(r1, bulk("val1"));
        assert_eq!(r2, bulk("val2"));
        // Verify they are distinct
        assert_ne!(r1, r2);
    }

    #[test]
    fn test_get_special_chars_key() {
        let mut ctx = make_ctx();
        let special_key = "key:with!special@chars#$%^&*()";
        set(&mut ctx, &[bulk(special_key), bulk("value")]).unwrap();

        let result = get(&mut ctx, &[bulk(special_key)]).unwrap();
        assert_eq!(result, bulk("value"));
    }

    #[test]
    fn test_get_newline_value() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("line1\nline2\r\nline3")]).unwrap();

        let result = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, bulk("line1\nline2\r\nline3"));
    }

    #[test]
    fn test_get_space_in_key() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("my key"), bulk("my value")]).unwrap();

        let result = get(&mut ctx, &[bulk("my key")]).unwrap();
        assert_eq!(result, bulk("my value"));

        // Ensure a different key with no space is not the same
        let result2 = get(&mut ctx, &[bulk("mykey")]).unwrap();
        assert_eq!(result2, RespValue::Null);
    }

    #[test]
    fn test_get_returns_bulk_string() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("value")]).unwrap();

        let result = get(&mut ctx, &[bulk("key")]).unwrap();
        // Verify it's specifically a BulkString, not SimpleString or other variant
        match result {
            RespValue::BulkString(b) => assert_eq!(b, Bytes::from("value")),
            other => panic!("Expected BulkString, got {:?}", other),
        }
    }

    #[test]
    fn test_get_after_set_with_ttl_still_valid() {
        let mut ctx = make_ctx();
        // Set key with a large EX so it won't expire during test
        set(
            &mut ctx,
            &[bulk("key"), bulk("value"), bulk("EX"), bulk("9999")],
        )
        .unwrap();

        let result = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, bulk("value"));

        // Verify that the key does have a TTL set
        let db = ctx.store().database(ctx.selected_db());
        let entry = db.get(b"key").unwrap();
        assert!(entry.expires_at.is_some());
        assert!(!entry.is_expired());
    }

    #[test]
    fn test_set_nx() {
        let mut ctx = make_ctx();

        // First SET NX should succeed
        let result = set(&mut ctx, &[bulk("key"), bulk("v1"), bulk("NX")]).unwrap();
        assert_eq!(result, RespValue::ok());

        // Second SET NX should return Null
        let result = set(&mut ctx, &[bulk("key"), bulk("v2"), bulk("NX")]).unwrap();
        assert_eq!(result, RespValue::Null);

        // Value should still be v1
        let result = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, bulk("v1"));
    }

    #[test]
    fn test_set_xx() {
        let mut ctx = make_ctx();

        // SET XX on nonexistent key should return Null
        let result = set(&mut ctx, &[bulk("key"), bulk("v1"), bulk("XX")]).unwrap();
        assert_eq!(result, RespValue::Null);

        // Key should not exist
        let result = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Null);

        // Create the key
        set(&mut ctx, &[bulk("key"), bulk("v1")]).unwrap();

        // Now SET XX should succeed
        let result = set(&mut ctx, &[bulk("key"), bulk("v2"), bulk("XX")]).unwrap();
        assert_eq!(result, RespValue::ok());

        let result = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, bulk("v2"));
    }

    #[test]
    fn test_set_get_option() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("old")]).unwrap();

        let result = set(&mut ctx, &[bulk("key"), bulk("new"), bulk("GET")]).unwrap();
        assert_eq!(result, bulk("old"));

        let result = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, bulk("new"));
    }

    #[test]
    fn test_setnx() {
        let mut ctx = make_ctx();

        let result = setnx(&mut ctx, &[bulk("key"), bulk("v1")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = setnx(&mut ctx, &[bulk("key"), bulk("v2")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));

        let result = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, bulk("v1"));
    }

    #[test]
    fn test_mget_mset() {
        let mut ctx = make_ctx();

        mset(&mut ctx, &[bulk("k1"), bulk("v1"), bulk("k2"), bulk("v2")]).unwrap();

        let result = mget(&mut ctx, &[bulk("k1"), bulk("k2"), bulk("k3")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], bulk("v1"));
            assert_eq!(arr[1], bulk("v2"));
            assert_eq!(arr[2], RespValue::Null);
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_msetnx() {
        let mut ctx = make_ctx();

        // First MSETNX should succeed
        let result = msetnx(&mut ctx, &[bulk("k1"), bulk("v1"), bulk("k2"), bulk("v2")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // Second MSETNX should fail (k1 exists)
        let result = msetnx(&mut ctx, &[bulk("k1"), bulk("v3"), bulk("k3"), bulk("v3")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));

        // k3 should not exist
        let result = get(&mut ctx, &[bulk("k3")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_incr_decr() {
        let mut ctx = make_ctx();

        let result = incr(&mut ctx, &[bulk("counter")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = incr(&mut ctx, &[bulk("counter")]).unwrap();
        assert_eq!(result, RespValue::Integer(2));

        let result = decr(&mut ctx, &[bulk("counter")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_incrby_decrby() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("num"), bulk("10")]).unwrap();

        let result = incrby(&mut ctx, &[bulk("num"), bulk("5")]).unwrap();
        assert_eq!(result, RespValue::Integer(15));

        let result = decrby(&mut ctx, &[bulk("num"), bulk("3")]).unwrap();
        assert_eq!(result, RespValue::Integer(12));
    }

    #[test]
    fn test_incr_wrong_type() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("not_a_number")]).unwrap();

        let result = incr(&mut ctx, &[bulk("key")]);
        assert!(matches!(result, Err(CommandError::NotAnInteger)));
    }

    #[test]
    fn test_incrbyfloat() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("num"), bulk("10.5")]).unwrap();

        let result = incrbyfloat(&mut ctx, &[bulk("num"), bulk("0.1")]).unwrap();
        if let RespValue::BulkString(b) = result {
            let val: f64 = std::str::from_utf8(&b).unwrap().parse().unwrap();
            assert!((val - 10.6).abs() < 0.0001);
        } else {
            panic!("Expected bulk string");
        }
    }

    #[test]
    fn test_append() {
        let mut ctx = make_ctx();

        let result = append(&mut ctx, &[bulk("key"), bulk("Hello")]).unwrap();
        assert_eq!(result, RespValue::Integer(5));

        let result = append(&mut ctx, &[bulk("key"), bulk(" World")]).unwrap();
        assert_eq!(result, RespValue::Integer(11));

        let result = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, bulk("Hello World"));
    }

    #[test]
    fn test_strlen() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("Hello")]).unwrap();

        let result = strlen(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Integer(5));

        let result = strlen(&mut ctx, &[bulk("nonexistent")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_getrange() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("Hello World")]).unwrap();

        let result = getrange(&mut ctx, &[bulk("key"), bulk("0"), bulk("4")]).unwrap();
        assert_eq!(result, bulk("Hello"));

        let result = getrange(&mut ctx, &[bulk("key"), bulk("-5"), bulk("-1")]).unwrap();
        assert_eq!(result, bulk("World"));

        let result = getrange(&mut ctx, &[bulk("key"), bulk("0"), bulk("-1")]).unwrap();
        assert_eq!(result, bulk("Hello World"));
    }

    #[test]
    fn test_setrange() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("Hello World")]).unwrap();

        let result = setrange(&mut ctx, &[bulk("key"), bulk("6"), bulk("Redis")]).unwrap();
        assert_eq!(result, RespValue::Integer(11));

        let result = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, bulk("Hello Redis"));
    }

    #[test]
    fn test_setrange_extend() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("Hi")]).unwrap();

        let result = setrange(&mut ctx, &[bulk("key"), bulk("5"), bulk("World")]).unwrap();
        assert_eq!(result, RespValue::Integer(10));

        // Bytes 2-4 should be null bytes
        let result = get(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::BulkString(b) = result {
            assert_eq!(b.len(), 10);
            assert_eq!(&b[0..2], b"Hi");
            assert_eq!(&b[5..10], b"World");
        }
    }

    #[test]
    fn test_getset() {
        let mut ctx = make_ctx();

        let result = getset(&mut ctx, &[bulk("key"), bulk("v1")]).unwrap();
        assert_eq!(result, RespValue::Null);

        let result = getset(&mut ctx, &[bulk("key"), bulk("v2")]).unwrap();
        assert_eq!(result, bulk("v1"));

        let result = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, bulk("v2"));
    }

    #[test]
    fn test_getdel() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("value")]).unwrap();

        let result = getdel(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, bulk("value"));

        let result = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_getex() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("value")]).unwrap();

        // GETEX without options
        let result = getex(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, bulk("value"));

        // GETEX with PERSIST (remove TTL if any)
        let result = getex(&mut ctx, &[bulk("key"), bulk("PERSIST")]).unwrap();
        assert_eq!(result, bulk("value"));
    }

    #[test]
    fn test_set_with_ex() {
        let mut ctx = make_ctx();
        set(
            &mut ctx,
            &[bulk("key"), bulk("value"), bulk("EX"), bulk("100")],
        )
        .unwrap();

        // Key should exist
        let result = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, bulk("value"));
    }

    #[test]
    fn test_set_with_px() {
        let mut ctx = make_ctx();
        set(
            &mut ctx,
            &[bulk("key"), bulk("value"), bulk("PX"), bulk("100000")],
        )
        .unwrap();

        let result = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, bulk("value"));
    }

    #[test]
    fn test_setex() {
        let mut ctx = make_ctx();
        let result = setex(&mut ctx, &[bulk("key"), bulk("100"), bulk("value")]).unwrap();
        assert_eq!(result, RespValue::ok());

        let result = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, bulk("value"));
    }

    #[test]
    fn test_psetex() {
        let mut ctx = make_ctx();
        let result = psetex(&mut ctx, &[bulk("key"), bulk("100000"), bulk("value")]).unwrap();
        assert_eq!(result, RespValue::ok());

        let result = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, bulk("value"));
    }

    // ========== SET option tests ==========

    #[test]
    fn test_set_with_exat() {
        let mut ctx = make_ctx();
        // Use a timestamp 1000 seconds in the future
        let future_ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 1000;
        let ts_str = future_ts.to_string();

        let result = set(
            &mut ctx,
            &[bulk("key"), bulk("value"), bulk("EXAT"), bulk(&ts_str)],
        )
        .unwrap();
        assert_eq!(result, RespValue::ok());

        // Key should exist
        let result = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, bulk("value"));

        // TTL should be positive
        let db = ctx.store().database(ctx.selected_db());
        let entry = db.get(b"key").unwrap();
        assert!(entry.expires_at.is_some());
        let ttl_ms = entry.ttl_millis().unwrap();
        assert!(ttl_ms > 0, "TTL should be > 0, got {ttl_ms}");
    }

    #[test]
    fn test_set_with_pxat() {
        let mut ctx = make_ctx();
        // Use a timestamp 1000 seconds in the future (in millis)
        let future_ts_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + 1_000_000;
        let ts_str = future_ts_ms.to_string();

        let result = set(
            &mut ctx,
            &[bulk("key"), bulk("value"), bulk("PXAT"), bulk(&ts_str)],
        )
        .unwrap();
        assert_eq!(result, RespValue::ok());

        // Key should exist
        let result = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, bulk("value"));

        // PTTL should be positive
        let db = ctx.store().database(ctx.selected_db());
        let entry = db.get(b"key").unwrap();
        assert!(entry.expires_at.is_some());
        let ttl_ms = entry.ttl_millis().unwrap();
        assert!(ttl_ms > 0, "PTTL should be > 0, got {ttl_ms}");
    }

    #[test]
    fn test_set_with_keepttl() {
        let mut ctx = make_ctx();
        // Set key with EX 1000
        set(
            &mut ctx,
            &[bulk("key"), bulk("v1"), bulk("EX"), bulk("1000")],
        )
        .unwrap();

        // Verify TTL is set
        {
            let db = ctx.store().database(ctx.selected_db());
            let entry = db.get(b"key").unwrap();
            assert!(entry.expires_at.is_some());
            let ttl_before = entry.ttl_millis().unwrap();
            assert!(ttl_before > 0);
        }

        // SET again with KEEPTTL
        set(&mut ctx, &[bulk("key"), bulk("v2"), bulk("KEEPTTL")]).unwrap();

        // Value should be updated
        let result = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, bulk("v2"));

        // TTL should still be set (preserved)
        let db = ctx.store().database(ctx.selected_db());
        let entry = db.get(b"key").unwrap();
        assert!(entry.expires_at.is_some());
        let ttl_after = entry.ttl_millis().unwrap();
        assert!(ttl_after > 0, "TTL should be preserved after KEEPTTL");
    }

    #[test]
    fn test_set_nx_get_existing() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("old")]).unwrap();

        // SET NX GET on existing key: should return old value, NOT set new value
        let result = set(
            &mut ctx,
            &[bulk("key"), bulk("new"), bulk("NX"), bulk("GET")],
        )
        .unwrap();
        assert_eq!(result, bulk("old"));

        // Value should still be "old"
        let result = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, bulk("old"));
    }

    #[test]
    fn test_set_nx_get_nonexistent() {
        let mut ctx = make_ctx();

        // SET NX GET on nonexistent key: should return Null and set the key
        let result = set(
            &mut ctx,
            &[bulk("key"), bulk("value"), bulk("NX"), bulk("GET")],
        )
        .unwrap();
        assert_eq!(result, RespValue::Null);

        // Key should now be set
        let result = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, bulk("value"));
    }

    #[test]
    fn test_set_xx_get_existing() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("old")]).unwrap();

        // SET XX GET on existing key: should return old value and update
        let result = set(
            &mut ctx,
            &[bulk("key"), bulk("new"), bulk("XX"), bulk("GET")],
        )
        .unwrap();
        assert_eq!(result, bulk("old"));

        // Value should now be "new"
        let result = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, bulk("new"));
    }

    #[test]
    fn test_set_xx_get_nonexistent() {
        let mut ctx = make_ctx();

        // SET XX GET on nonexistent key: should return Null and NOT set
        let result = set(
            &mut ctx,
            &[bulk("key"), bulk("value"), bulk("XX"), bulk("GET")],
        )
        .unwrap();
        assert_eq!(result, RespValue::Null);

        // Key should not exist
        let result = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_set_nx_with_ex() {
        let mut ctx = make_ctx();

        // SET key val NX EX 1000 on nonexistent key
        let result = set(
            &mut ctx,
            &[
                bulk("key"),
                bulk("value"),
                bulk("NX"),
                bulk("EX"),
                bulk("1000"),
            ],
        )
        .unwrap();
        assert_eq!(result, RespValue::ok());

        // Key should exist with value
        let result = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, bulk("value"));

        // Should have a TTL
        let db = ctx.store().database(ctx.selected_db());
        let entry = db.get(b"key").unwrap();
        assert!(entry.expires_at.is_some());
        let ttl_ms = entry.ttl_millis().unwrap();
        assert!(ttl_ms > 0);
    }

    #[test]
    fn test_set_xx_with_px() {
        let mut ctx = make_ctx();
        // Pre-create the key so XX succeeds
        set(&mut ctx, &[bulk("key"), bulk("old")]).unwrap();

        // SET key val XX PX 500000
        let result = set(
            &mut ctx,
            &[
                bulk("key"),
                bulk("new"),
                bulk("XX"),
                bulk("PX"),
                bulk("500000"),
            ],
        )
        .unwrap();
        assert_eq!(result, RespValue::ok());

        // Key should have updated value
        let result = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, bulk("new"));

        // Should have a TTL
        let db = ctx.store().database(ctx.selected_db());
        let entry = db.get(b"key").unwrap();
        assert!(entry.expires_at.is_some());
        let ttl_ms = entry.ttl_millis().unwrap();
        assert!(ttl_ms > 0);
    }

    #[test]
    fn test_set_get_on_wrong_type() {
        use std::collections::VecDeque;

        let mut ctx = make_ctx();
        // Insert a list directly into the store
        let db = ctx.store().database(ctx.selected_db());
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("item"));
        db.set(Bytes::from("mylist"), Entry::new(RedisValue::List(list)));

        // SET mylist val GET should return WRONGTYPE
        let result = set(&mut ctx, &[bulk("mylist"), bulk("value"), bulk("GET")]);
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    #[test]
    fn test_set_nx_xx_mutual_exclusion() {
        let mut ctx = make_ctx();

        let result = set(
            &mut ctx,
            &[bulk("key"), bulk("value"), bulk("NX"), bulk("XX")],
        );
        assert!(matches!(result, Err(CommandError::SyntaxError)));
    }

    #[test]
    fn test_set_multiple_ttl_options_rejected() {
        let mut ctx = make_ctx();

        let result = set(
            &mut ctx,
            &[
                bulk("key"),
                bulk("value"),
                bulk("EX"),
                bulk("10"),
                bulk("PX"),
                bulk("5000"),
            ],
        );
        assert!(matches!(result, Err(CommandError::SyntaxError)));
    }

    #[test]
    fn test_set_ex_zero_rejected() {
        let mut ctx = make_ctx();

        let result = set(
            &mut ctx,
            &[bulk("key"), bulk("value"), bulk("EX"), bulk("0")],
        );
        assert!(matches!(result, Err(CommandError::InvalidArgument(_))));
    }

    #[test]
    fn test_set_ex_negative_rejected() {
        let mut ctx = make_ctx();

        let result = set(
            &mut ctx,
            &[bulk("key"), bulk("value"), bulk("EX"), bulk("-5")],
        );
        assert!(matches!(result, Err(CommandError::InvalidArgument(_))));
    }

    #[test]
    fn test_set_px_zero_rejected() {
        let mut ctx = make_ctx();

        let result = set(
            &mut ctx,
            &[bulk("key"), bulk("value"), bulk("PX"), bulk("0")],
        );
        assert!(matches!(result, Err(CommandError::InvalidArgument(_))));
    }

    #[test]
    fn test_set_missing_ex_value() {
        let mut ctx = make_ctx();

        // SET key val EX (missing the seconds argument)
        let result = set(&mut ctx, &[bulk("key"), bulk("value"), bulk("EX")]);
        assert!(matches!(result, Err(CommandError::SyntaxError)));
    }

    #[test]
    fn test_set_invalid_option() {
        let mut ctx = make_ctx();

        let result = set(&mut ctx, &[bulk("key"), bulk("value"), bulk("NOTANOPTION")]);
        assert!(matches!(result, Err(CommandError::SyntaxError)));
    }

    #[test]
    fn test_set_get_on_nonexistent() {
        let mut ctx = make_ctx();

        // SET key val GET when key doesn't exist → should return Null, key set
        let result = set(&mut ctx, &[bulk("key"), bulk("value"), bulk("GET")]).unwrap();
        assert_eq!(result, RespValue::Null);

        // Key should now be set
        let result = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, bulk("value"));
    }

    #[test]
    fn test_set_get_on_expired_key() {
        use std::time::Instant;

        let mut ctx = make_ctx();
        // Create a key that is already expired
        let db = ctx.store().database(ctx.selected_db());
        let mut entry = Entry::new(RedisValue::String(Bytes::from("old")));
        entry.expires_at = Some(Instant::now() - Duration::from_secs(1));
        db.set(Bytes::from("key"), entry);

        // SET key val GET on expired key → should return Null (treated as nonexistent)
        let result = set(&mut ctx, &[bulk("key"), bulk("new"), bulk("GET")]).unwrap();
        assert_eq!(result, RespValue::Null);

        // Key should now hold "new"
        let result = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, bulk("new"));
    }

    // ========== GETEX option tests ==========

    #[test]
    fn test_getex_with_ex() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("hello")]).unwrap();

        // GETEX key EX 1000
        let result = getex(&mut ctx, &[bulk("key"), bulk("EX"), bulk("1000")]).unwrap();
        assert_eq!(result, bulk("hello"));

        // TTL should be set
        let db = ctx.store().database(ctx.selected_db());
        let entry = db.get(b"key").unwrap();
        assert!(entry.expires_at.is_some());
        let ttl_ms = entry.ttl_millis().unwrap();
        assert!(ttl_ms > 0, "TTL should be > 0 after GETEX EX, got {ttl_ms}");
    }

    #[test]
    fn test_getex_with_px() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("hello")]).unwrap();

        // GETEX key PX 500000
        let result = getex(&mut ctx, &[bulk("key"), bulk("PX"), bulk("500000")]).unwrap();
        assert_eq!(result, bulk("hello"));

        // PTTL should be set
        let db = ctx.store().database(ctx.selected_db());
        let entry = db.get(b"key").unwrap();
        assert!(entry.expires_at.is_some());
        let ttl_ms = entry.ttl_millis().unwrap();
        assert!(
            ttl_ms > 0,
            "PTTL should be > 0 after GETEX PX, got {ttl_ms}"
        );
    }

    #[test]
    fn test_getex_with_exat() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("hello")]).unwrap();

        let future_ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 1000;
        let ts_str = future_ts.to_string();

        let result = getex(&mut ctx, &[bulk("key"), bulk("EXAT"), bulk(&ts_str)]).unwrap();
        assert_eq!(result, bulk("hello"));

        // TTL should be set
        let db = ctx.store().database(ctx.selected_db());
        let entry = db.get(b"key").unwrap();
        assert!(entry.expires_at.is_some());
        let ttl_ms = entry.ttl_millis().unwrap();
        assert!(ttl_ms > 0, "TTL should be > 0 after GETEX EXAT");
    }

    #[test]
    fn test_getex_with_pxat() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("hello")]).unwrap();

        let future_ts_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + 1_000_000;
        let ts_str = future_ts_ms.to_string();

        let result = getex(&mut ctx, &[bulk("key"), bulk("PXAT"), bulk(&ts_str)]).unwrap();
        assert_eq!(result, bulk("hello"));

        // PTTL should be set
        let db = ctx.store().database(ctx.selected_db());
        let entry = db.get(b"key").unwrap();
        assert!(entry.expires_at.is_some());
        let ttl_ms = entry.ttl_millis().unwrap();
        assert!(ttl_ms > 0, "PTTL should be > 0 after GETEX PXAT");
    }

    #[test]
    fn test_getex_persist() {
        let mut ctx = make_ctx();
        // Set key with TTL
        set(
            &mut ctx,
            &[bulk("key"), bulk("hello"), bulk("EX"), bulk("1000")],
        )
        .unwrap();

        // Verify TTL is set
        {
            let db = ctx.store().database(ctx.selected_db());
            let entry = db.get(b"key").unwrap();
            assert!(entry.expires_at.is_some());
        }

        // GETEX PERSIST removes TTL
        let result = getex(&mut ctx, &[bulk("key"), bulk("PERSIST")]).unwrap();
        assert_eq!(result, bulk("hello"));

        // TTL should be removed
        let db = ctx.store().database(ctx.selected_db());
        let entry = db.get(b"key").unwrap();
        assert!(
            entry.expires_at.is_none(),
            "TTL should be removed after GETEX PERSIST"
        );
    }

    #[test]
    fn test_getex_no_options() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("hello")]).unwrap();

        // GETEX without options acts as GET
        let result = getex(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, bulk("hello"));

        // Key should still have no TTL (it didn't before)
        let db = ctx.store().database(ctx.selected_db());
        let entry = db.get(b"key").unwrap();
        assert!(
            entry.expires_at.is_none(),
            "GETEX without options should not add a TTL"
        );
    }

    #[test]
    fn test_getex_nonexistent_key() {
        let mut ctx = make_ctx();

        let result = getex(&mut ctx, &[bulk("nokey")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_getex_wrong_type() {
        use std::collections::VecDeque;

        let mut ctx = make_ctx();
        // Insert a list directly into the store
        let db = ctx.store().database(ctx.selected_db());
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("item"));
        db.set(Bytes::from("mylist"), Entry::new(RedisValue::List(list)));

        // GETEX on list key → WRONGTYPE
        let result = getex(&mut ctx, &[bulk("mylist")]);
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    #[test]
    fn test_getex_ex_zero_rejected() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("hello")]).unwrap();

        let result = getex(&mut ctx, &[bulk("key"), bulk("EX"), bulk("0")]);
        assert!(matches!(result, Err(CommandError::InvalidArgument(_))));
    }

    #[test]
    fn test_getex_ex_negative_rejected() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("hello")]).unwrap();

        let result = getex(&mut ctx, &[bulk("key"), bulk("EX"), bulk("-1")]);
        assert!(matches!(result, Err(CommandError::InvalidArgument(_))));
    }

    #[test]
    fn test_getex_px_zero_rejected() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("hello")]).unwrap();

        let result = getex(&mut ctx, &[bulk("key"), bulk("PX"), bulk("0")]);
        assert!(matches!(result, Err(CommandError::InvalidArgument(_))));
    }

    // ========== INCR command tests ==========

    #[test]
    fn test_incr_nonexistent_key() {
        let mut ctx = make_ctx();
        let result = incr(&mut ctx, &[bulk("counter")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_incr_existing_integer() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("counter"), bulk("10")]).unwrap();
        let result = incr(&mut ctx, &[bulk("counter")]).unwrap();
        assert_eq!(result, RespValue::Integer(11));
    }

    #[test]
    fn test_incr_negative_number() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("counter"), bulk("-10")]).unwrap();
        let result = incr(&mut ctx, &[bulk("counter")]).unwrap();
        assert_eq!(result, RespValue::Integer(-9));
    }

    #[test]
    fn test_incr_zero() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("counter"), bulk("0")]).unwrap();
        let result = incr(&mut ctx, &[bulk("counter")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_incr_wrong_type_list() {
        use std::collections::VecDeque;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("item"));
        db.set(Bytes::from("mylist"), Entry::new(RedisValue::List(list)));

        let result = incr(&mut ctx, &[bulk("mylist")]);
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    #[test]
    fn test_incr_non_numeric_string() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("abc")]).unwrap();
        let result = incr(&mut ctx, &[bulk("key")]);
        assert!(matches!(result, Err(CommandError::NotAnInteger)));
    }

    #[test]
    fn test_incr_float_string() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("1.5")]).unwrap();
        let result = incr(&mut ctx, &[bulk("key")]);
        assert!(matches!(result, Err(CommandError::NotAnInteger)));
    }

    #[test]
    fn test_incr_empty_string() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("")]).unwrap();
        let result = incr(&mut ctx, &[bulk("key")]);
        assert!(matches!(result, Err(CommandError::NotAnInteger)));
    }

    #[test]
    fn test_incr_overflow() {
        let mut ctx = make_ctx();
        let max_str = i64::MAX.to_string();
        set(&mut ctx, &[bulk("key"), bulk(&max_str)]).unwrap();
        let result = incr(&mut ctx, &[bulk("key")]);
        assert!(matches!(result, Err(CommandError::NotAnInteger)));
    }

    #[test]
    fn test_incr_multiple_times() {
        let mut ctx = make_ctx();
        for i in 1..=10 {
            let result = incr(&mut ctx, &[bulk("counter")]).unwrap();
            assert_eq!(result, RespValue::Integer(i));
        }
    }

    #[test]
    fn test_incr_large_number() {
        let mut ctx = make_ctx();
        let large = (i64::MAX - 1).to_string();
        set(&mut ctx, &[bulk("key"), bulk(&large)]).unwrap();
        let result = incr(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Integer(i64::MAX));
    }

    #[test]
    fn test_incr_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        let result = incr(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_incr_expired_key() {
        use std::time::Instant;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut entry = Entry::new(RedisValue::String(Bytes::from("10")));
        entry.expires_at = Some(Instant::now() - Duration::from_secs(1));
        db.set(Bytes::from("key"), entry);

        let result = incr(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_incr_value_stored_as_string() {
        let mut ctx = make_ctx();
        incr(&mut ctx, &[bulk("counter")]).unwrap();

        let db = ctx.store().database(ctx.selected_db());
        let entry = db.get(b"counter").unwrap();
        match &entry.value {
            RedisValue::String(s) => assert_eq!(s.as_ref(), b"1"),
            other => panic!("Expected String, got {:?}", other),
        }
    }

    #[test]
    fn test_incr_minus_one() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("-1")]).unwrap();
        let result = incr(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_incr_leading_zeros() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("007")]).unwrap();
        let result = incr(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Integer(8));
    }

    #[test]
    fn test_incr_whitespace_value() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk(" 10 ")]).unwrap();
        let result = incr(&mut ctx, &[bulk("key")]);
        assert!(matches!(result, Err(CommandError::NotAnInteger)));
    }

    #[test]
    fn test_incr_returns_integer_type() {
        let mut ctx = make_ctx();
        let result = incr(&mut ctx, &[bulk("key")]).unwrap();
        match result {
            RespValue::Integer(v) => assert_eq!(v, 1),
            other => panic!("Expected Integer variant, got {:?}", other),
        }
    }

    // ========== INCRBY command tests ==========

    #[test]
    fn test_incrby_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        let result = incrby(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_incrby_wrong_arity_one_arg() {
        let mut ctx = make_ctx();
        let result = incrby(&mut ctx, &[bulk("key")]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_incrby_nonexistent_key() {
        let mut ctx = make_ctx();
        let result = incrby(&mut ctx, &[bulk("key"), bulk("5")]).unwrap();
        assert_eq!(result, RespValue::Integer(5));
    }

    #[test]
    fn test_incrby_existing_value() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("num"), bulk("10")]).unwrap();
        let result = incrby(&mut ctx, &[bulk("num"), bulk("5")]).unwrap();
        assert_eq!(result, RespValue::Integer(15));
    }

    #[test]
    fn test_incrby_negative_increment() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("num"), bulk("10")]).unwrap();
        let result = incrby(&mut ctx, &[bulk("num"), bulk("-5")]).unwrap();
        assert_eq!(result, RespValue::Integer(5));
    }

    #[test]
    fn test_incrby_zero_increment() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("num"), bulk("10")]).unwrap();
        let result = incrby(&mut ctx, &[bulk("num"), bulk("0")]).unwrap();
        assert_eq!(result, RespValue::Integer(10));
    }

    #[test]
    fn test_incrby_wrong_type_list() {
        use std::collections::VecDeque;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("item"));
        db.set(Bytes::from("mylist"), Entry::new(RedisValue::List(list)));

        let result = incrby(&mut ctx, &[bulk("mylist"), bulk("5")]);
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    #[test]
    fn test_incrby_non_numeric_string() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("abc")]).unwrap();
        let result = incrby(&mut ctx, &[bulk("key"), bulk("5")]);
        assert!(matches!(result, Err(CommandError::NotAnInteger)));
    }

    #[test]
    fn test_incrby_float_string_key() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("1.5")]).unwrap();
        let result = incrby(&mut ctx, &[bulk("key"), bulk("5")]);
        assert!(matches!(result, Err(CommandError::NotAnInteger)));
    }

    #[test]
    fn test_incrby_float_increment() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("10")]).unwrap();
        let result = incrby(&mut ctx, &[bulk("key"), bulk("1.5")]);
        assert!(matches!(result, Err(CommandError::NotAnInteger)));
    }

    #[test]
    fn test_incrby_overflow() {
        let mut ctx = make_ctx();
        let max_str = i64::MAX.to_string();
        set(&mut ctx, &[bulk("key"), bulk(&max_str)]).unwrap();
        let result = incrby(&mut ctx, &[bulk("key"), bulk("1")]);
        assert!(matches!(result, Err(CommandError::NotAnInteger)));
    }

    #[test]
    fn test_incrby_underflow() {
        let mut ctx = make_ctx();
        let min_str = i64::MIN.to_string();
        set(&mut ctx, &[bulk("key"), bulk(&min_str)]).unwrap();
        let result = incrby(&mut ctx, &[bulk("key"), bulk("-1")]);
        assert!(matches!(result, Err(CommandError::NotAnInteger)));
    }

    #[test]
    fn test_incrby_large_positive() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("0")]).unwrap();
        let large = (i64::MAX - 100).to_string();
        let result = incrby(&mut ctx, &[bulk("key"), bulk(&large)]).unwrap();
        assert_eq!(result, RespValue::Integer(i64::MAX - 100));
    }

    #[test]
    fn test_incrby_large_negative() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("0")]).unwrap();
        let large_neg = (i64::MIN + 100).to_string();
        let result = incrby(&mut ctx, &[bulk("key"), bulk(&large_neg)]).unwrap();
        assert_eq!(result, RespValue::Integer(i64::MIN + 100));
    }

    #[test]
    fn test_incrby_expired_key() {
        use std::time::Instant;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut entry = Entry::new(RedisValue::String(Bytes::from("10")));
        entry.expires_at = Some(Instant::now() - Duration::from_secs(1));
        db.set(Bytes::from("key"), entry);

        let result = incrby(&mut ctx, &[bulk("key"), bulk("5")]).unwrap();
        assert_eq!(result, RespValue::Integer(5));
    }

    #[test]
    fn test_incrby_multiple_times() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("0")]).unwrap();
        for i in 1..=5 {
            let result = incrby(&mut ctx, &[bulk("key"), bulk("10")]).unwrap();
            assert_eq!(result, RespValue::Integer(i * 10));
        }
    }

    #[test]
    fn test_incrby_returns_integer_type() {
        let mut ctx = make_ctx();
        let result = incrby(&mut ctx, &[bulk("key"), bulk("42")]).unwrap();
        match result {
            RespValue::Integer(v) => assert_eq!(v, 42),
            other => panic!("Expected Integer variant, got {:?}", other),
        }
    }

    #[test]
    fn test_incrby_increment_by_i64_min() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("0")]).unwrap();
        let min_str = i64::MIN.to_string();
        let result = incrby(&mut ctx, &[bulk("key"), bulk(&min_str)]).unwrap();
        assert_eq!(result, RespValue::Integer(i64::MIN));
    }

    #[test]
    fn test_incrby_empty_string_key() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("")]).unwrap();
        let result = incrby(&mut ctx, &[bulk("key"), bulk("5")]);
        assert!(matches!(result, Err(CommandError::NotAnInteger)));
    }

    // ========== INCRBYFLOAT command tests ==========

    #[test]
    fn test_incrbyfloat_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        let result = incrbyfloat(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_incrbyfloat_wrong_arity_one_arg() {
        let mut ctx = make_ctx();
        let result = incrbyfloat(&mut ctx, &[bulk("key")]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_incrbyfloat_nonexistent_key() {
        let mut ctx = make_ctx();
        let result = incrbyfloat(&mut ctx, &[bulk("key"), bulk("2.5")]).unwrap();
        if let RespValue::BulkString(b) = result {
            let val: f64 = std::str::from_utf8(&b).unwrap().parse().unwrap();
            assert!((val - 2.5).abs() < f64::EPSILON);
        } else {
            panic!("Expected BulkString");
        }
    }

    #[test]
    fn test_incrbyfloat_existing_float() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("10.5")]).unwrap();
        let result = incrbyfloat(&mut ctx, &[bulk("key"), bulk("0.1")]).unwrap();
        if let RespValue::BulkString(b) = result {
            let val: f64 = std::str::from_utf8(&b).unwrap().parse().unwrap();
            assert!((val - 10.6).abs() < 0.0001);
        } else {
            panic!("Expected BulkString");
        }
    }

    #[test]
    fn test_incrbyfloat_existing_integer() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("10")]).unwrap();
        let result = incrbyfloat(&mut ctx, &[bulk("key"), bulk("1.5")]).unwrap();
        if let RespValue::BulkString(b) = result {
            let val: f64 = std::str::from_utf8(&b).unwrap().parse().unwrap();
            assert!((val - 11.5).abs() < f64::EPSILON);
        } else {
            panic!("Expected BulkString");
        }
    }

    #[test]
    fn test_incrbyfloat_negative_increment() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("10.0")]).unwrap();
        let result = incrbyfloat(&mut ctx, &[bulk("key"), bulk("-5.0")]).unwrap();
        if let RespValue::BulkString(b) = result {
            let val: f64 = std::str::from_utf8(&b).unwrap().parse().unwrap();
            assert!((val - 5.0).abs() < f64::EPSILON);
        } else {
            panic!("Expected BulkString");
        }
    }

    #[test]
    fn test_incrbyfloat_zero_increment() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("10.5")]).unwrap();
        let result = incrbyfloat(&mut ctx, &[bulk("key"), bulk("0.0")]).unwrap();
        if let RespValue::BulkString(b) = result {
            let val: f64 = std::str::from_utf8(&b).unwrap().parse().unwrap();
            assert!((val - 10.5).abs() < f64::EPSILON);
        } else {
            panic!("Expected BulkString");
        }
    }

    #[test]
    fn test_incrbyfloat_wrong_type_list() {
        use std::collections::VecDeque;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("item"));
        db.set(Bytes::from("mylist"), Entry::new(RedisValue::List(list)));

        let result = incrbyfloat(&mut ctx, &[bulk("mylist"), bulk("1.0")]);
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    #[test]
    fn test_incrbyfloat_non_numeric_string() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("abc")]).unwrap();
        let result = incrbyfloat(&mut ctx, &[bulk("key"), bulk("1.0")]);
        assert!(matches!(result, Err(CommandError::NotAFloat)));
    }

    #[test]
    fn test_incrbyfloat_nan_rejected() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("0")]).unwrap();
        // "NaN" is not a valid float for parse
        let result = incrbyfloat(&mut ctx, &[bulk("key"), bulk("NaN")]);
        assert!(matches!(result, Err(CommandError::NotAFloat)));
    }

    #[test]
    fn test_incrbyfloat_inf_rejected() {
        let mut ctx = make_ctx();
        let max_f64 = f64::MAX.to_string();
        set(&mut ctx, &[bulk("key"), bulk(&max_f64)]).unwrap();
        // Adding MAX again should produce infinity, which is rejected
        let result = incrbyfloat(&mut ctx, &[bulk("key"), bulk(&max_f64)]);
        assert!(matches!(result, Err(CommandError::NotAFloat)));
    }

    #[test]
    fn test_incrbyfloat_negative_inf_rejected() {
        let mut ctx = make_ctx();
        let neg_max = (-f64::MAX).to_string();
        set(&mut ctx, &[bulk("key"), bulk(&neg_max)]).unwrap();
        // Adding -MAX again should produce negative infinity, which is rejected
        let result = incrbyfloat(&mut ctx, &[bulk("key"), bulk(&neg_max)]);
        assert!(matches!(result, Err(CommandError::NotAFloat)));
    }

    #[test]
    fn test_incrbyfloat_returns_bulk_string() {
        let mut ctx = make_ctx();
        let result = incrbyfloat(&mut ctx, &[bulk("key"), bulk("3.14")]).unwrap();
        match result {
            RespValue::BulkString(_) => {} // correct
            other => panic!("Expected BulkString variant, got {:?}", other),
        }
    }

    #[test]
    fn test_incrbyfloat_precision() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("0.1")]).unwrap();
        let result = incrbyfloat(&mut ctx, &[bulk("key"), bulk("0.2")]).unwrap();
        if let RespValue::BulkString(b) = result {
            let val: f64 = std::str::from_utf8(&b).unwrap().parse().unwrap();
            // 0.1 + 0.2 should be approximately 0.3
            assert!((val - 0.3).abs() < 0.0001, "Expected ~0.3, got {}", val);
        } else {
            panic!("Expected BulkString");
        }
    }

    #[test]
    fn test_incrbyfloat_integer_result_formatted() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("10")]).unwrap();
        let result = incrbyfloat(&mut ctx, &[bulk("key"), bulk("5.0")]).unwrap();
        // 10 + 5.0 = 15.0, which has fract() == 0.0, so formatted as integer "15"
        assert_eq!(result, RespValue::BulkString(Bytes::from("15")));
    }

    #[test]
    fn test_incrbyfloat_expired_key() {
        use std::time::Instant;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut entry = Entry::new(RedisValue::String(Bytes::from("10.5")));
        entry.expires_at = Some(Instant::now() - Duration::from_secs(1));
        db.set(Bytes::from("key"), entry);

        let result = incrbyfloat(&mut ctx, &[bulk("key"), bulk("2.5")]).unwrap();
        if let RespValue::BulkString(b) = result {
            let val: f64 = std::str::from_utf8(&b).unwrap().parse().unwrap();
            assert!((val - 2.5).abs() < f64::EPSILON);
        } else {
            panic!("Expected BulkString");
        }
    }

    #[test]
    fn test_incrbyfloat_multiple_times() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("0")]).unwrap();
        for _ in 0..5 {
            incrbyfloat(&mut ctx, &[bulk("key"), bulk("1.1")]).unwrap();
        }
        let result = get(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::BulkString(b) = result {
            let val: f64 = std::str::from_utf8(&b).unwrap().parse().unwrap();
            assert!((val - 5.5).abs() < 0.0001, "Expected ~5.5, got {}", val);
        } else {
            panic!("Expected BulkString");
        }
    }

    #[test]
    fn test_incrbyfloat_scientific_notation() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("1e2")]).unwrap();
        let result = incrbyfloat(&mut ctx, &[bulk("key"), bulk("0")]).unwrap();
        // 1e2 = 100.0, fract() == 0.0 → formatted as "100"
        assert_eq!(result, RespValue::BulkString(Bytes::from("100")));
    }

    #[test]
    fn test_incrbyfloat_empty_string_key() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("")]).unwrap();
        let result = incrbyfloat(&mut ctx, &[bulk("key"), bulk("1.0")]);
        assert!(matches!(result, Err(CommandError::NotAFloat)));
    }

    // ========== MGET tests (19 additional) ==========

    #[test]
    fn test_mget_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        let result = mget(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_mget_single_existing_key() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("k1"), bulk("v1")]).unwrap();
        let result = mget(&mut ctx, &[bulk("k1")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![bulk("v1")]));
    }

    #[test]
    fn test_mget_single_nonexistent_key() {
        let mut ctx = make_ctx();
        let result = mget(&mut ctx, &[bulk("nokey")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![RespValue::Null]));
    }

    #[test]
    fn test_mget_multiple_all_exist() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("a"), bulk("1")]).unwrap();
        set(&mut ctx, &[bulk("b"), bulk("2")]).unwrap();
        set(&mut ctx, &[bulk("c"), bulk("3")]).unwrap();
        let result = mget(&mut ctx, &[bulk("a"), bulk("b"), bulk("c")]).unwrap();
        assert_eq!(
            result,
            RespValue::Array(vec![bulk("1"), bulk("2"), bulk("3")])
        );
    }

    #[test]
    fn test_mget_multiple_some_missing() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("a"), bulk("1")]).unwrap();
        set(&mut ctx, &[bulk("c"), bulk("3")]).unwrap();
        let result = mget(&mut ctx, &[bulk("a"), bulk("b"), bulk("c")]).unwrap();
        assert_eq!(
            result,
            RespValue::Array(vec![bulk("1"), RespValue::Null, bulk("3")])
        );
    }

    #[test]
    fn test_mget_multiple_all_missing() {
        let mut ctx = make_ctx();
        let result = mget(&mut ctx, &[bulk("x"), bulk("y"), bulk("z")]).unwrap();
        assert_eq!(
            result,
            RespValue::Array(vec![RespValue::Null, RespValue::Null, RespValue::Null])
        );
    }

    #[test]
    fn test_mget_duplicate_keys() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("k"), bulk("val")]).unwrap();
        let result = mget(&mut ctx, &[bulk("k"), bulk("k"), bulk("k")]).unwrap();
        assert_eq!(
            result,
            RespValue::Array(vec![bulk("val"), bulk("val"), bulk("val")])
        );
    }

    #[test]
    fn test_mget_wrong_type_returns_null() {
        use std::collections::VecDeque;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("item"));
        db.set(Bytes::from("mylist"), Entry::new(RedisValue::List(list)));

        // MGET on a list key returns Null, NOT an error
        let result = mget(&mut ctx, &[bulk("mylist")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![RespValue::Null]));
    }

    #[test]
    fn test_mget_mixed_types() {
        use std::collections::VecDeque;
        let mut ctx = make_ctx();

        // String key
        set(&mut ctx, &[bulk("str"), bulk("hello")]).unwrap();

        // List key
        let db = ctx.store().database(ctx.selected_db());
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("item"));
        db.set(Bytes::from("lst"), Entry::new(RedisValue::List(list)));

        // Missing key is not set

        let result = mget(&mut ctx, &[bulk("str"), bulk("lst"), bulk("missing")]).unwrap();
        assert_eq!(
            result,
            RespValue::Array(vec![bulk("hello"), RespValue::Null, RespValue::Null])
        );
    }

    #[test]
    fn test_mget_expired_key() {
        use std::time::Instant;
        let mut ctx = make_ctx();

        let db = ctx.store().database(ctx.selected_db());
        let mut entry = Entry::new(RedisValue::String(Bytes::from("old")));
        entry.expires_at = Some(Instant::now() - Duration::from_secs(1));
        db.set(Bytes::from("expired"), entry);

        let result = mget(&mut ctx, &[bulk("expired")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![RespValue::Null]));
    }

    #[test]
    fn test_mget_empty_string_value() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("k"), bulk("")]).unwrap();
        let result = mget(&mut ctx, &[bulk("k")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![bulk("")]));
    }

    #[test]
    fn test_mget_returns_array_type() {
        let mut ctx = make_ctx();
        let result = mget(&mut ctx, &[bulk("any")]).unwrap();
        assert!(matches!(result, RespValue::Array(_)));
    }

    #[test]
    fn test_mget_preserves_order() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("z"), bulk("last")]).unwrap();
        set(&mut ctx, &[bulk("a"), bulk("first")]).unwrap();
        set(&mut ctx, &[bulk("m"), bulk("middle")]).unwrap();

        let result = mget(&mut ctx, &[bulk("z"), bulk("a"), bulk("m")]).unwrap();
        assert_eq!(
            result,
            RespValue::Array(vec![bulk("last"), bulk("first"), bulk("middle")])
        );
    }

    #[test]
    fn test_mget_large_number_of_keys() {
        let mut ctx = make_ctx();
        for i in 0..100 {
            let k = format!("key{i}");
            let v = format!("val{i}");
            set(&mut ctx, &[bulk(&k), bulk(&v)]).unwrap();
        }

        let args: Vec<RespValue> = (0..100).map(|i| bulk(&format!("key{i}"))).collect();
        let result = mget(&mut ctx, &args).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 100);
            for i in 0..100 {
                assert_eq!(arr[i], bulk(&format!("val{i}")));
            }
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_mget_binary_values() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        db.set(
            Bytes::from("binkey"),
            Entry::new(RedisValue::String(Bytes::from(vec![0x00, 0xFF, 0x80]))),
        );

        let result = mget(&mut ctx, &[bulk("binkey")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 1);
            if let RespValue::BulkString(b) = &arr[0] {
                assert_eq!(b.as_ref(), &[0x00, 0xFF, 0x80]);
            } else {
                panic!("Expected BulkString");
            }
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_mget_numeric_strings() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("n1"), bulk("42")]).unwrap();
        set(&mut ctx, &[bulk("n2"), bulk("-7")]).unwrap();
        set(&mut ctx, &[bulk("n3"), bulk("3.14")]).unwrap();

        let result = mget(&mut ctx, &[bulk("n1"), bulk("n2"), bulk("n3")]).unwrap();
        assert_eq!(
            result,
            RespValue::Array(vec![bulk("42"), bulk("-7"), bulk("3.14")])
        );
    }

    #[test]
    fn test_mget_after_overwrite() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("k"), bulk("old")]).unwrap();
        set(&mut ctx, &[bulk("k"), bulk("new")]).unwrap();

        let result = mget(&mut ctx, &[bulk("k")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![bulk("new")]));
    }

    #[test]
    fn test_mget_special_chars_keys() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key:with:colons"), bulk("a")]).unwrap();
        set(&mut ctx, &[bulk("key.with.dots"), bulk("b")]).unwrap();
        set(&mut ctx, &[bulk("key{with}braces"), bulk("c")]).unwrap();

        let result = mget(
            &mut ctx,
            &[
                bulk("key:with:colons"),
                bulk("key.with.dots"),
                bulk("key{with}braces"),
            ],
        )
        .unwrap();
        assert_eq!(
            result,
            RespValue::Array(vec![bulk("a"), bulk("b"), bulk("c")])
        );
    }

    #[test]
    fn test_mget_after_del() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("k1"), bulk("v1")]).unwrap();
        set(&mut ctx, &[bulk("k2"), bulk("v2")]).unwrap();

        // Delete k1
        let db = ctx.store().database(ctx.selected_db());
        db.delete(b"k1");

        let result = mget(&mut ctx, &[bulk("k1"), bulk("k2")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![RespValue::Null, bulk("v2")]));
    }

    // ========== MSET tests (19 additional) ==========

    #[test]
    fn test_mset_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        let result = mset(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_mset_wrong_arity_odd_args() {
        let mut ctx = make_ctx();
        let result = mset(&mut ctx, &[bulk("k1"), bulk("v1"), bulk("k2")]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_mset_single_pair() {
        let mut ctx = make_ctx();
        let result = mset(&mut ctx, &[bulk("k1"), bulk("v1")]).unwrap();
        assert_eq!(result, RespValue::ok());

        let got = get(&mut ctx, &[bulk("k1")]).unwrap();
        assert_eq!(got, bulk("v1"));
    }

    #[test]
    fn test_mset_multiple_pairs() {
        let mut ctx = make_ctx();
        mset(
            &mut ctx,
            &[
                bulk("a"),
                bulk("1"),
                bulk("b"),
                bulk("2"),
                bulk("c"),
                bulk("3"),
            ],
        )
        .unwrap();

        assert_eq!(get(&mut ctx, &[bulk("a")]).unwrap(), bulk("1"));
        assert_eq!(get(&mut ctx, &[bulk("b")]).unwrap(), bulk("2"));
        assert_eq!(get(&mut ctx, &[bulk("c")]).unwrap(), bulk("3"));
    }

    #[test]
    fn test_mset_overwrites_existing() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("k1"), bulk("old")]).unwrap();

        mset(&mut ctx, &[bulk("k1"), bulk("new")]).unwrap();
        assert_eq!(get(&mut ctx, &[bulk("k1")]).unwrap(), bulk("new"));
    }

    #[test]
    fn test_mset_returns_ok() {
        let mut ctx = make_ctx();
        let result = mset(&mut ctx, &[bulk("k"), bulk("v")]).unwrap();
        assert_eq!(result, RespValue::ok());
    }

    #[test]
    fn test_mset_empty_values() {
        let mut ctx = make_ctx();
        mset(&mut ctx, &[bulk("k1"), bulk(""), bulk("k2"), bulk("")]).unwrap();

        assert_eq!(get(&mut ctx, &[bulk("k1")]).unwrap(), bulk(""));
        assert_eq!(get(&mut ctx, &[bulk("k2")]).unwrap(), bulk(""));
    }

    #[test]
    fn test_mset_overwrites_different_type() {
        use std::collections::VecDeque;
        let mut ctx = make_ctx();

        // Insert a list
        let db = ctx.store().database(ctx.selected_db());
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("item"));
        db.set(Bytes::from("mylist"), Entry::new(RedisValue::List(list)));

        // MSET overwrites list with string
        mset(&mut ctx, &[bulk("mylist"), bulk("now_a_string")]).unwrap();
        assert_eq!(
            get(&mut ctx, &[bulk("mylist")]).unwrap(),
            bulk("now_a_string")
        );
    }

    #[test]
    fn test_mset_duplicate_keys() {
        let mut ctx = make_ctx();
        // Same key twice — last value wins
        mset(
            &mut ctx,
            &[bulk("k"), bulk("first"), bulk("k"), bulk("second")],
        )
        .unwrap();
        assert_eq!(get(&mut ctx, &[bulk("k")]).unwrap(), bulk("second"));
    }

    #[test]
    fn test_mset_large_number_of_pairs() {
        let mut ctx = make_ctx();
        let mut args = Vec::new();
        for i in 0..100 {
            args.push(bulk(&format!("key{i}")));
            args.push(bulk(&format!("val{i}")));
        }
        mset(&mut ctx, &args).unwrap();

        for i in 0..100 {
            let result = get(&mut ctx, &[bulk(&format!("key{i}"))]).unwrap();
            assert_eq!(result, bulk(&format!("val{i}")));
        }
    }

    #[test]
    fn test_mset_numeric_values() {
        let mut ctx = make_ctx();
        mset(
            &mut ctx,
            &[
                bulk("int"),
                bulk("42"),
                bulk("neg"),
                bulk("-1"),
                bulk("float"),
                bulk("3.14"),
            ],
        )
        .unwrap();

        assert_eq!(get(&mut ctx, &[bulk("int")]).unwrap(), bulk("42"));
        assert_eq!(get(&mut ctx, &[bulk("neg")]).unwrap(), bulk("-1"));
        assert_eq!(get(&mut ctx, &[bulk("float")]).unwrap(), bulk("3.14"));
    }

    #[test]
    fn test_mset_special_chars() {
        let mut ctx = make_ctx();
        mset(
            &mut ctx,
            &[
                bulk("key:1"),
                bulk("val:1"),
                bulk("key.2"),
                bulk("val.2"),
                bulk("key{3}"),
                bulk("val{3}"),
            ],
        )
        .unwrap();

        assert_eq!(get(&mut ctx, &[bulk("key:1")]).unwrap(), bulk("val:1"));
        assert_eq!(get(&mut ctx, &[bulk("key.2")]).unwrap(), bulk("val.2"));
        assert_eq!(get(&mut ctx, &[bulk("key{3}")]).unwrap(), bulk("val{3}"));
    }

    #[test]
    fn test_mset_binary_values() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());

        // Store binary directly
        db.set(
            Bytes::from("bin"),
            Entry::new(RedisValue::String(Bytes::from(vec![0x00, 0xFF]))),
        );

        // MSET overwrites it
        mset(&mut ctx, &[bulk("bin"), bulk("text")]).unwrap();
        assert_eq!(get(&mut ctx, &[bulk("bin")]).unwrap(), bulk("text"));
    }

    #[test]
    fn test_mset_removes_ttl() {
        let mut ctx = make_ctx();

        // Set key with TTL
        set(
            &mut ctx,
            &[bulk("k"), bulk("old"), bulk("EX"), bulk("1000")],
        )
        .unwrap();

        // Verify TTL exists
        {
            let db = ctx.store().database(ctx.selected_db());
            let entry = db.get(b"k").unwrap();
            assert!(entry.expires_at.is_some());
        }

        // MSET overwrites — new entry has no TTL
        mset(&mut ctx, &[bulk("k"), bulk("new")]).unwrap();

        let db = ctx.store().database(ctx.selected_db());
        let entry = db.get(b"k").unwrap();
        assert!(
            entry.expires_at.is_none(),
            "MSET should create a new entry without TTL"
        );
    }

    #[test]
    fn test_mset_values_retrievable() {
        let mut ctx = make_ctx();
        mset(
            &mut ctx,
            &[
                bulk("x"),
                bulk("10"),
                bulk("y"),
                bulk("20"),
                bulk("z"),
                bulk("30"),
            ],
        )
        .unwrap();

        let result = mget(&mut ctx, &[bulk("x"), bulk("y"), bulk("z")]).unwrap();
        assert_eq!(
            result,
            RespValue::Array(vec![bulk("10"), bulk("20"), bulk("30")])
        );
    }

    #[test]
    fn test_mset_single_arg_rejected() {
        let mut ctx = make_ctx();
        let result = mset(&mut ctx, &[bulk("k1")]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_mset_three_args_rejected() {
        let mut ctx = make_ctx();
        let result = mset(&mut ctx, &[bulk("k1"), bulk("v1"), bulk("k2")]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_mset_preserves_other_keys() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("existing"), bulk("stay")]).unwrap();

        mset(
            &mut ctx,
            &[bulk("new1"), bulk("a"), bulk("new2"), bulk("b")],
        )
        .unwrap();

        // existing key should be unaffected
        assert_eq!(get(&mut ctx, &[bulk("existing")]).unwrap(), bulk("stay"));
        assert_eq!(get(&mut ctx, &[bulk("new1")]).unwrap(), bulk("a"));
        assert_eq!(get(&mut ctx, &[bulk("new2")]).unwrap(), bulk("b"));
    }

    #[test]
    fn test_mset_atomicity_all_set() {
        let mut ctx = make_ctx();
        // Pre-set one key
        set(&mut ctx, &[bulk("k1"), bulk("old")]).unwrap();

        // MSET with mix of existing and new keys
        mset(
            &mut ctx,
            &[
                bulk("k1"),
                bulk("new1"),
                bulk("k2"),
                bulk("new2"),
                bulk("k3"),
                bulk("new3"),
            ],
        )
        .unwrap();

        assert_eq!(get(&mut ctx, &[bulk("k1")]).unwrap(), bulk("new1"));
        assert_eq!(get(&mut ctx, &[bulk("k2")]).unwrap(), bulk("new2"));
        assert_eq!(get(&mut ctx, &[bulk("k3")]).unwrap(), bulk("new3"));
    }

    // ========== MSETNX tests (19 additional) ==========

    #[test]
    fn test_msetnx_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        let result = msetnx(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_msetnx_wrong_arity_odd_args() {
        let mut ctx = make_ctx();
        let result = msetnx(&mut ctx, &[bulk("k1"), bulk("v1"), bulk("k2")]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_msetnx_all_new_keys() {
        let mut ctx = make_ctx();
        let result = msetnx(
            &mut ctx,
            &[
                bulk("a"),
                bulk("1"),
                bulk("b"),
                bulk("2"),
                bulk("c"),
                bulk("3"),
            ],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(1));

        assert_eq!(get(&mut ctx, &[bulk("a")]).unwrap(), bulk("1"));
        assert_eq!(get(&mut ctx, &[bulk("b")]).unwrap(), bulk("2"));
        assert_eq!(get(&mut ctx, &[bulk("c")]).unwrap(), bulk("3"));
    }

    #[test]
    fn test_msetnx_one_exists() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("b"), bulk("existing")]).unwrap();

        let result = msetnx(
            &mut ctx,
            &[
                bulk("a"),
                bulk("1"),
                bulk("b"),
                bulk("2"),
                bulk("c"),
                bulk("3"),
            ],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(0));

        // None of the new keys should be set
        assert_eq!(get(&mut ctx, &[bulk("a")]).unwrap(), RespValue::Null);
        assert_eq!(get(&mut ctx, &[bulk("b")]).unwrap(), bulk("existing"));
        assert_eq!(get(&mut ctx, &[bulk("c")]).unwrap(), RespValue::Null);
    }

    #[test]
    fn test_msetnx_all_exist() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("a"), bulk("x")]).unwrap();
        set(&mut ctx, &[bulk("b"), bulk("y")]).unwrap();

        let result = msetnx(&mut ctx, &[bulk("a"), bulk("1"), bulk("b"), bulk("2")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));

        // Original values preserved
        assert_eq!(get(&mut ctx, &[bulk("a")]).unwrap(), bulk("x"));
        assert_eq!(get(&mut ctx, &[bulk("b")]).unwrap(), bulk("y"));
    }

    #[test]
    fn test_msetnx_single_pair_new() {
        let mut ctx = make_ctx();
        let result = msetnx(&mut ctx, &[bulk("k"), bulk("v")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        assert_eq!(get(&mut ctx, &[bulk("k")]).unwrap(), bulk("v"));
    }

    #[test]
    fn test_msetnx_single_pair_exists() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("k"), bulk("old")]).unwrap();

        let result = msetnx(&mut ctx, &[bulk("k"), bulk("new")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
        assert_eq!(get(&mut ctx, &[bulk("k")]).unwrap(), bulk("old"));
    }

    #[test]
    fn test_msetnx_atomicity_none_set_on_failure() {
        let mut ctx = make_ctx();
        // k2 exists, so MSETNX should fail — k1 and k3 must NOT be set
        set(&mut ctx, &[bulk("k2"), bulk("exists")]).unwrap();

        let result = msetnx(
            &mut ctx,
            &[
                bulk("k1"),
                bulk("a"),
                bulk("k2"),
                bulk("b"),
                bulk("k3"),
                bulk("c"),
            ],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(0));

        assert_eq!(get(&mut ctx, &[bulk("k1")]).unwrap(), RespValue::Null);
        assert_eq!(get(&mut ctx, &[bulk("k2")]).unwrap(), bulk("exists"));
        assert_eq!(get(&mut ctx, &[bulk("k3")]).unwrap(), RespValue::Null);
    }

    #[test]
    fn test_msetnx_values_retrievable() {
        let mut ctx = make_ctx();
        msetnx(
            &mut ctx,
            &[
                bulk("x"),
                bulk("10"),
                bulk("y"),
                bulk("20"),
                bulk("z"),
                bulk("30"),
            ],
        )
        .unwrap();

        let result = mget(&mut ctx, &[bulk("x"), bulk("y"), bulk("z")]).unwrap();
        assert_eq!(
            result,
            RespValue::Array(vec![bulk("10"), bulk("20"), bulk("30")])
        );
    }

    #[test]
    fn test_msetnx_expired_key_treated_new() {
        use std::time::Instant;
        let mut ctx = make_ctx();

        // Insert an expired key
        let db = ctx.store().database(ctx.selected_db());
        let mut entry = Entry::new(RedisValue::String(Bytes::from("old")));
        entry.expires_at = Some(Instant::now() - Duration::from_secs(1));
        db.set(Bytes::from("k1"), entry);

        // MSETNX should treat expired key as nonexistent → returns 1
        let result = msetnx(&mut ctx, &[bulk("k1"), bulk("new")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        assert_eq!(get(&mut ctx, &[bulk("k1")]).unwrap(), bulk("new"));
    }

    #[test]
    fn test_msetnx_empty_values() {
        let mut ctx = make_ctx();
        let result = msetnx(&mut ctx, &[bulk("k1"), bulk(""), bulk("k2"), bulk("")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        assert_eq!(get(&mut ctx, &[bulk("k1")]).unwrap(), bulk(""));
        assert_eq!(get(&mut ctx, &[bulk("k2")]).unwrap(), bulk(""));
    }

    #[test]
    fn test_msetnx_numeric_values() {
        let mut ctx = make_ctx();
        let result = msetnx(
            &mut ctx,
            &[
                bulk("int"),
                bulk("42"),
                bulk("neg"),
                bulk("-1"),
                bulk("float"),
                bulk("3.14"),
            ],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(1));

        assert_eq!(get(&mut ctx, &[bulk("int")]).unwrap(), bulk("42"));
        assert_eq!(get(&mut ctx, &[bulk("neg")]).unwrap(), bulk("-1"));
        assert_eq!(get(&mut ctx, &[bulk("float")]).unwrap(), bulk("3.14"));
    }

    #[test]
    fn test_msetnx_returns_integer_type() {
        let mut ctx = make_ctx();

        // Success case returns Integer(1)
        let result = msetnx(&mut ctx, &[bulk("k"), bulk("v")]).unwrap();
        assert!(matches!(result, RespValue::Integer(1)));

        // Failure case returns Integer(0)
        let result = msetnx(&mut ctx, &[bulk("k"), bulk("v2")]).unwrap();
        assert!(matches!(result, RespValue::Integer(0)));
    }

    #[test]
    fn test_msetnx_duplicate_keys() {
        let mut ctx = make_ctx();
        // Same key repeated in args — both are "new" so it should succeed
        let result = msetnx(
            &mut ctx,
            &[bulk("k"), bulk("first"), bulk("k"), bulk("second")],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // Last write wins
        assert_eq!(get(&mut ctx, &[bulk("k")]).unwrap(), bulk("second"));
    }

    #[test]
    fn test_msetnx_large_number_of_pairs() {
        let mut ctx = make_ctx();
        let mut args = Vec::new();
        for i in 0..100 {
            args.push(bulk(&format!("key{i}")));
            args.push(bulk(&format!("val{i}")));
        }
        let result = msetnx(&mut ctx, &args).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        for i in 0..100 {
            let got = get(&mut ctx, &[bulk(&format!("key{i}"))]).unwrap();
            assert_eq!(got, bulk(&format!("val{i}")));
        }
    }

    #[test]
    fn test_msetnx_does_not_affect_existing_on_success() {
        let mut ctx = make_ctx();
        // Pre-set a key that is NOT part of the MSETNX call
        set(&mut ctx, &[bulk("other"), bulk("untouched")]).unwrap();

        msetnx(
            &mut ctx,
            &[bulk("new1"), bulk("a"), bulk("new2"), bulk("b")],
        )
        .unwrap();

        // Existing key should be unchanged
        assert_eq!(get(&mut ctx, &[bulk("other")]).unwrap(), bulk("untouched"));
        assert_eq!(get(&mut ctx, &[bulk("new1")]).unwrap(), bulk("a"));
        assert_eq!(get(&mut ctx, &[bulk("new2")]).unwrap(), bulk("b"));
    }

    #[test]
    fn test_msetnx_special_chars() {
        let mut ctx = make_ctx();
        let result = msetnx(
            &mut ctx,
            &[
                bulk("ns:key:1"),
                bulk("a"),
                bulk("key.dot"),
                bulk("b"),
                bulk("key{tag}"),
                bulk("c"),
            ],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(1));

        assert_eq!(get(&mut ctx, &[bulk("ns:key:1")]).unwrap(), bulk("a"));
        assert_eq!(get(&mut ctx, &[bulk("key.dot")]).unwrap(), bulk("b"));
        assert_eq!(get(&mut ctx, &[bulk("key{tag}")]).unwrap(), bulk("c"));
    }

    #[test]
    fn test_msetnx_no_ttl_set() {
        let mut ctx = make_ctx();
        msetnx(&mut ctx, &[bulk("k1"), bulk("v1"), bulk("k2"), bulk("v2")]).unwrap();

        let db = ctx.store().database(ctx.selected_db());
        let e1 = db.get(b"k1").unwrap();
        let e2 = db.get(b"k2").unwrap();
        assert!(e1.expires_at.is_none(), "MSETNX should not set TTL");
        assert!(e2.expires_at.is_none(), "MSETNX should not set TTL");
    }

    #[test]
    fn test_msetnx_wrong_type_key_blocks() {
        use std::collections::VecDeque;
        let mut ctx = make_ctx();

        // Insert a list key
        let db = ctx.store().database(ctx.selected_db());
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("item"));
        db.set(Bytes::from("mylist"), Entry::new(RedisValue::List(list)));

        // MSETNX with mylist as one of the keys — list counts as existing
        let result = msetnx(
            &mut ctx,
            &[bulk("newkey"), bulk("a"), bulk("mylist"), bulk("b")],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(0));

        // newkey should NOT be set
        assert_eq!(get(&mut ctx, &[bulk("newkey")]).unwrap(), RespValue::Null);
    }

    // ========== SETNX additional tests (19) ==========

    #[test]
    fn test_setnx_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        let result = setnx(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_setnx_wrong_arity_one_arg() {
        let mut ctx = make_ctx();
        let result = setnx(&mut ctx, &[bulk("key")]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_setnx_on_existing_key() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("existing")]).unwrap();
        let result = setnx(&mut ctx, &[bulk("key"), bulk("new")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_setnx_on_nonexistent_key() {
        let mut ctx = make_ctx();
        let result = setnx(&mut ctx, &[bulk("key"), bulk("value")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, bulk("value"));
    }

    #[test]
    fn test_setnx_preserves_existing_value() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("original")]).unwrap();
        setnx(&mut ctx, &[bulk("key"), bulk("overwrite")]).unwrap();
        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, bulk("original"));
    }

    #[test]
    fn test_setnx_on_expired_key() {
        use std::time::Instant;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut entry = Entry::new(RedisValue::String(Bytes::from("old")));
        entry.expires_at = Some(Instant::now() - Duration::from_secs(1));
        db.set(Bytes::from("key"), entry);

        let result = setnx(&mut ctx, &[bulk("key"), bulk("new")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, bulk("new"));
    }

    #[test]
    fn test_setnx_empty_string_value() {
        let mut ctx = make_ctx();
        let result = setnx(&mut ctx, &[bulk("key"), bulk("")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, bulk(""));
    }

    #[test]
    fn test_setnx_returns_integer_type() {
        let mut ctx = make_ctx();
        let result = setnx(&mut ctx, &[bulk("key"), bulk("val")]).unwrap();
        assert!(matches!(result, RespValue::Integer(1)));

        let result2 = setnx(&mut ctx, &[bulk("key"), bulk("val2")]).unwrap();
        assert!(matches!(result2, RespValue::Integer(0)));
    }

    #[test]
    fn test_setnx_binary_value() {
        let mut ctx = make_ctx();
        let binary = RespValue::BulkString(Bytes::from(vec![0u8, 1, 2, 255, 254]));
        let result = setnx(&mut ctx, &[bulk("binkey"), binary]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let db = ctx.store().database(ctx.selected_db());
        let entry = db.get(b"binkey").unwrap();
        if let RedisValue::String(s) = &entry.value {
            assert_eq!(s.as_ref(), &[0u8, 1, 2, 255, 254]);
        } else {
            panic!("Expected string value");
        }
    }

    #[test]
    fn test_setnx_large_value() {
        let mut ctx = make_ctx();
        let large = "x".repeat(1_000_000);
        let result = setnx(&mut ctx, &[bulk("key"), bulk(&large)]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let db = ctx.store().database(ctx.selected_db());
        let entry = db.get(b"key").unwrap();
        if let RedisValue::String(s) = &entry.value {
            assert_eq!(s.len(), 1_000_000);
        } else {
            panic!("Expected string value");
        }
    }

    #[test]
    fn test_setnx_special_chars_key() {
        let mut ctx = make_ctx();
        let result = setnx(&mut ctx, &[bulk("key:with:colons!@#$%"), bulk("val")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        let got = get(&mut ctx, &[bulk("key:with:colons!@#$%")]).unwrap();
        assert_eq!(got, bulk("val"));
    }

    #[test]
    fn test_setnx_multiple_calls() {
        let mut ctx = make_ctx();
        let r1 = setnx(&mut ctx, &[bulk("key"), bulk("first")]).unwrap();
        assert_eq!(r1, RespValue::Integer(1));

        let r2 = setnx(&mut ctx, &[bulk("key"), bulk("second")]).unwrap();
        assert_eq!(r2, RespValue::Integer(0));

        let r3 = setnx(&mut ctx, &[bulk("key"), bulk("third")]).unwrap();
        assert_eq!(r3, RespValue::Integer(0));

        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, bulk("first"));
    }

    #[test]
    fn test_setnx_different_keys() {
        let mut ctx = make_ctx();
        let r1 = setnx(&mut ctx, &[bulk("k1"), bulk("v1")]).unwrap();
        assert_eq!(r1, RespValue::Integer(1));

        let r2 = setnx(&mut ctx, &[bulk("k2"), bulk("v2")]).unwrap();
        assert_eq!(r2, RespValue::Integer(1));

        let got1 = get(&mut ctx, &[bulk("k1")]).unwrap();
        assert_eq!(got1, bulk("v1"));
        let got2 = get(&mut ctx, &[bulk("k2")]).unwrap();
        assert_eq!(got2, bulk("v2"));
    }

    #[test]
    fn test_setnx_numeric_value() {
        let mut ctx = make_ctx();
        let result = setnx(&mut ctx, &[bulk("num"), bulk("42")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        let got = get(&mut ctx, &[bulk("num")]).unwrap();
        assert_eq!(got, bulk("42"));
    }

    #[test]
    fn test_setnx_after_del() {
        let mut ctx = make_ctx();
        setnx(&mut ctx, &[bulk("key"), bulk("v1")]).unwrap();

        let db = ctx.store().database(ctx.selected_db());
        db.delete(b"key");

        let result = setnx(&mut ctx, &[bulk("key"), bulk("v2")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, bulk("v2"));
    }

    #[test]
    fn test_setnx_overwrite_wrong_type() {
        use std::collections::VecDeque;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("item"));
        db.set(Bytes::from("mylist"), Entry::new(RedisValue::List(list)));

        // SETNX on a list key: key exists, so returns 0 regardless of type
        let result = setnx(&mut ctx, &[bulk("mylist"), bulk("value")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_setnx_value_is_retrievable() {
        let mut ctx = make_ctx();
        setnx(&mut ctx, &[bulk("mykey"), bulk("hello world")]).unwrap();

        let db = ctx.store().database(ctx.selected_db());
        let entry = db.get(b"mykey").unwrap();
        match &entry.value {
            RedisValue::String(s) => assert_eq!(s.as_ref(), b"hello world"),
            _ => panic!("Expected string value"),
        }
    }

    #[test]
    fn test_setnx_no_ttl_set() {
        let mut ctx = make_ctx();
        setnx(&mut ctx, &[bulk("key"), bulk("value")]).unwrap();

        let db = ctx.store().database(ctx.selected_db());
        let entry = db.get(b"key").unwrap();
        assert!(entry.expires_at.is_none(), "SETNX should not set a TTL");
    }

    #[test]
    fn test_setnx_empty_key() {
        let mut ctx = make_ctx();
        let result = setnx(&mut ctx, &[bulk(""), bulk("value")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        let got = get(&mut ctx, &[bulk("")]).unwrap();
        assert_eq!(got, bulk("value"));
    }

    // ========== SETEX additional tests (19) ==========

    #[test]
    fn test_setex_basic_ttl() {
        let mut ctx = make_ctx();
        let result = setex(&mut ctx, &[bulk("key"), bulk("10"), bulk("value")]).unwrap();
        assert_eq!(result, RespValue::ok());

        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, bulk("value"));

        let db = ctx.store().database(ctx.selected_db());
        let entry = db.get(b"key").unwrap();
        assert!(entry.expires_at.is_some());
        let ttl_ms = entry.ttl_millis().unwrap();
        assert!(
            ttl_ms > 9000 && ttl_ms <= 10_000,
            "TTL should be ~10s, got {ttl_ms}ms"
        );
    }

    #[test]
    fn test_setex_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        let result = setex(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_setex_wrong_arity_two_args() {
        let mut ctx = make_ctx();
        let result = setex(&mut ctx, &[bulk("key"), bulk("10")]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_setex_zero_seconds_rejected() {
        let mut ctx = make_ctx();
        let result = setex(&mut ctx, &[bulk("key"), bulk("0"), bulk("value")]);
        assert!(matches!(result, Err(CommandError::InvalidArgument(_))));
    }

    #[test]
    fn test_setex_negative_seconds_rejected() {
        let mut ctx = make_ctx();
        let result = setex(&mut ctx, &[bulk("key"), bulk("-5"), bulk("value")]);
        assert!(matches!(result, Err(CommandError::InvalidArgument(_))));
    }

    #[test]
    fn test_setex_non_integer_seconds() {
        let mut ctx = make_ctx();
        let result = setex(&mut ctx, &[bulk("key"), bulk("abc"), bulk("value")]);
        assert!(matches!(result, Err(CommandError::NotAnInteger)));
    }

    #[test]
    fn test_setex_overwrites_existing() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("old")]).unwrap();
        setex(&mut ctx, &[bulk("key"), bulk("10"), bulk("new")]).unwrap();

        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, bulk("new"));
    }

    #[test]
    fn test_setex_overwrites_ttl() {
        let mut ctx = make_ctx();
        setex(&mut ctx, &[bulk("key"), bulk("1000"), bulk("v1")]).unwrap();
        setex(&mut ctx, &[bulk("key"), bulk("10"), bulk("v2")]).unwrap();

        let db = ctx.store().database(ctx.selected_db());
        let entry = db.get(b"key").unwrap();
        let ttl_ms = entry.ttl_millis().unwrap();
        assert!(ttl_ms <= 10_000, "TTL should be ~10s, got {ttl_ms}ms");
    }

    #[test]
    fn test_setex_value_retrievable() {
        let mut ctx = make_ctx();
        setex(&mut ctx, &[bulk("key"), bulk("60"), bulk("hello")]).unwrap();
        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, bulk("hello"));
    }

    #[test]
    fn test_setex_returns_ok() {
        let mut ctx = make_ctx();
        let result = setex(&mut ctx, &[bulk("key"), bulk("10"), bulk("value")]).unwrap();
        assert_eq!(result, RespValue::ok());
    }

    #[test]
    fn test_setex_large_ttl() {
        let mut ctx = make_ctx();
        let result = setex(&mut ctx, &[bulk("key"), bulk("315360000"), bulk("value")]).unwrap();
        assert_eq!(result, RespValue::ok());

        let db = ctx.store().database(ctx.selected_db());
        let entry = db.get(b"key").unwrap();
        assert!(entry.expires_at.is_some());
        let ttl_ms = entry.ttl_millis().unwrap();
        assert!(ttl_ms > 315_000_000_000, "TTL should be very large");
    }

    #[test]
    fn test_setex_one_second() {
        let mut ctx = make_ctx();
        let result = setex(&mut ctx, &[bulk("key"), bulk("1"), bulk("value")]).unwrap();
        assert_eq!(result, RespValue::ok());

        let db = ctx.store().database(ctx.selected_db());
        let entry = db.get(b"key").unwrap();
        assert!(entry.expires_at.is_some());
        let ttl_ms = entry.ttl_millis().unwrap();
        assert!(
            ttl_ms > 0 && ttl_ms <= 1000,
            "TTL should be ~1s, got {ttl_ms}ms"
        );
    }

    #[test]
    fn test_setex_on_wrong_type_key() {
        use std::collections::VecDeque;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("item"));
        db.set(Bytes::from("mylist"), Entry::new(RedisValue::List(list)));

        let result = setex(&mut ctx, &[bulk("mylist"), bulk("10"), bulk("string_now")]).unwrap();
        assert_eq!(result, RespValue::ok());

        let got = get(&mut ctx, &[bulk("mylist")]).unwrap();
        assert_eq!(got, bulk("string_now"));
    }

    #[test]
    fn test_setex_empty_value() {
        let mut ctx = make_ctx();
        let result = setex(&mut ctx, &[bulk("key"), bulk("10"), bulk("")]).unwrap();
        assert_eq!(result, RespValue::ok());
        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, bulk(""));
    }

    #[test]
    fn test_setex_numeric_value() {
        let mut ctx = make_ctx();
        let result = setex(&mut ctx, &[bulk("key"), bulk("10"), bulk("12345")]).unwrap();
        assert_eq!(result, RespValue::ok());
        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, bulk("12345"));
    }

    #[test]
    fn test_setex_special_chars() {
        let mut ctx = make_ctx();
        let result = setex(&mut ctx, &[bulk("k:e:y!@#"), bulk("10"), bulk("v@l$%^")]).unwrap();
        assert_eq!(result, RespValue::ok());
        let got = get(&mut ctx, &[bulk("k:e:y!@#")]).unwrap();
        assert_eq!(got, bulk("v@l$%^"));
    }

    #[test]
    fn test_setex_ttl_is_set() {
        let mut ctx = make_ctx();
        setex(&mut ctx, &[bulk("key"), bulk("300"), bulk("value")]).unwrap();

        let db = ctx.store().database(ctx.selected_db());
        let entry = db.get(b"key").unwrap();
        assert!(entry.expires_at.is_some(), "SETEX must set expires_at");
    }

    #[test]
    fn test_setex_expired_key_treated_new() {
        use std::time::Instant;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut entry = Entry::new(RedisValue::String(Bytes::from("old")));
        entry.expires_at = Some(Instant::now() - Duration::from_secs(1));
        db.set(Bytes::from("key"), entry);

        let result = setex(&mut ctx, &[bulk("key"), bulk("60"), bulk("new")]).unwrap();
        assert_eq!(result, RespValue::ok());
        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, bulk("new"));
    }

    #[test]
    fn test_setex_float_seconds_rejected() {
        let mut ctx = make_ctx();
        let result = setex(&mut ctx, &[bulk("key"), bulk("1.5"), bulk("value")]);
        assert!(matches!(result, Err(CommandError::NotAnInteger)));
    }

    // ========== PSETEX additional tests (19) ==========

    #[test]
    fn test_psetex_basic() {
        let mut ctx = make_ctx();
        let result = psetex(&mut ctx, &[bulk("key"), bulk("5000"), bulk("value")]).unwrap();
        assert_eq!(result, RespValue::ok());

        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, bulk("value"));

        let db = ctx.store().database(ctx.selected_db());
        let entry = db.get(b"key").unwrap();
        assert!(entry.expires_at.is_some());
        let ttl_ms = entry.ttl_millis().unwrap();
        assert!(
            ttl_ms > 4000 && ttl_ms <= 5000,
            "TTL should be ~5000ms, got {ttl_ms}ms"
        );
    }

    #[test]
    fn test_psetex_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        let result = psetex(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_psetex_wrong_arity_two_args() {
        let mut ctx = make_ctx();
        let result = psetex(&mut ctx, &[bulk("key"), bulk("5000")]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_psetex_zero_ms_rejected() {
        let mut ctx = make_ctx();
        let result = psetex(&mut ctx, &[bulk("key"), bulk("0"), bulk("value")]);
        assert!(matches!(result, Err(CommandError::InvalidArgument(_))));
    }

    #[test]
    fn test_psetex_negative_ms_rejected() {
        let mut ctx = make_ctx();
        let result = psetex(&mut ctx, &[bulk("key"), bulk("-100"), bulk("value")]);
        assert!(matches!(result, Err(CommandError::InvalidArgument(_))));
    }

    #[test]
    fn test_psetex_non_integer_ms() {
        let mut ctx = make_ctx();
        let result = psetex(&mut ctx, &[bulk("key"), bulk("abc"), bulk("value")]);
        assert!(matches!(result, Err(CommandError::NotAnInteger)));
    }

    #[test]
    fn test_psetex_overwrites_existing() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("old")]).unwrap();
        psetex(&mut ctx, &[bulk("key"), bulk("10000"), bulk("new")]).unwrap();

        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, bulk("new"));
    }

    #[test]
    fn test_psetex_value_retrievable() {
        let mut ctx = make_ctx();
        psetex(&mut ctx, &[bulk("key"), bulk("60000"), bulk("hello")]).unwrap();
        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, bulk("hello"));
    }

    #[test]
    fn test_psetex_returns_ok() {
        let mut ctx = make_ctx();
        let result = psetex(&mut ctx, &[bulk("key"), bulk("5000"), bulk("value")]).unwrap();
        assert_eq!(result, RespValue::ok());
    }

    #[test]
    fn test_psetex_large_ttl() {
        let mut ctx = make_ctx();
        let result = psetex(
            &mut ctx,
            &[bulk("key"), bulk("315360000000"), bulk("value")],
        )
        .unwrap();
        assert_eq!(result, RespValue::ok());

        let db = ctx.store().database(ctx.selected_db());
        let entry = db.get(b"key").unwrap();
        assert!(entry.expires_at.is_some());
        let ttl_ms = entry.ttl_millis().unwrap();
        assert!(ttl_ms > 315_000_000_000, "TTL should be very large");
    }

    #[test]
    fn test_psetex_one_ms() {
        let mut ctx = make_ctx();
        let result = psetex(&mut ctx, &[bulk("key"), bulk("1"), bulk("value")]).unwrap();
        assert_eq!(result, RespValue::ok());

        let db = ctx.store().database(ctx.selected_db());
        let entry = db.get(b"key").unwrap();
        assert!(entry.expires_at.is_some());
    }

    #[test]
    fn test_psetex_overwrites_ttl() {
        let mut ctx = make_ctx();
        psetex(&mut ctx, &[bulk("key"), bulk("1000000"), bulk("v1")]).unwrap();
        psetex(&mut ctx, &[bulk("key"), bulk("5000"), bulk("v2")]).unwrap();

        let db = ctx.store().database(ctx.selected_db());
        let entry = db.get(b"key").unwrap();
        let ttl_ms = entry.ttl_millis().unwrap();
        assert!(ttl_ms <= 5000, "TTL should be ~5000ms, got {ttl_ms}ms");
    }

    #[test]
    fn test_psetex_empty_value() {
        let mut ctx = make_ctx();
        let result = psetex(&mut ctx, &[bulk("key"), bulk("5000"), bulk("")]).unwrap();
        assert_eq!(result, RespValue::ok());
        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, bulk(""));
    }

    #[test]
    fn test_psetex_numeric_value() {
        let mut ctx = make_ctx();
        let result = psetex(&mut ctx, &[bulk("key"), bulk("5000"), bulk("99999")]).unwrap();
        assert_eq!(result, RespValue::ok());
        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, bulk("99999"));
    }

    #[test]
    fn test_psetex_special_chars() {
        let mut ctx = make_ctx();
        let result = psetex(&mut ctx, &[bulk("k:e:y!@#"), bulk("5000"), bulk("v@l$%^")]).unwrap();
        assert_eq!(result, RespValue::ok());
        let got = get(&mut ctx, &[bulk("k:e:y!@#")]).unwrap();
        assert_eq!(got, bulk("v@l$%^"));
    }

    #[test]
    fn test_psetex_ttl_is_set() {
        let mut ctx = make_ctx();
        psetex(&mut ctx, &[bulk("key"), bulk("300000"), bulk("value")]).unwrap();

        let db = ctx.store().database(ctx.selected_db());
        let entry = db.get(b"key").unwrap();
        assert!(entry.expires_at.is_some(), "PSETEX must set expires_at");
    }

    #[test]
    fn test_psetex_on_expired_key() {
        use std::time::Instant;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut entry = Entry::new(RedisValue::String(Bytes::from("old")));
        entry.expires_at = Some(Instant::now() - Duration::from_secs(1));
        db.set(Bytes::from("key"), entry);

        let result = psetex(&mut ctx, &[bulk("key"), bulk("60000"), bulk("new")]).unwrap();
        assert_eq!(result, RespValue::ok());
        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, bulk("new"));
    }

    #[test]
    fn test_psetex_on_wrong_type_key() {
        use std::collections::VecDeque;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("item"));
        db.set(Bytes::from("mylist"), Entry::new(RedisValue::List(list)));

        let result = psetex(
            &mut ctx,
            &[bulk("mylist"), bulk("10000"), bulk("string_now")],
        )
        .unwrap();
        assert_eq!(result, RespValue::ok());

        let got = get(&mut ctx, &[bulk("mylist")]).unwrap();
        assert_eq!(got, bulk("string_now"));
    }

    #[test]
    fn test_psetex_float_ms_rejected() {
        let mut ctx = make_ctx();
        let result = psetex(&mut ctx, &[bulk("key"), bulk("1.5"), bulk("value")]);
        assert!(matches!(result, Err(CommandError::NotAnInteger)));
    }

    // ========== APPEND additional tests (19) ==========

    #[test]
    fn test_append_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        let result = append(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_append_wrong_arity_one_arg() {
        let mut ctx = make_ctx();
        let result = append(&mut ctx, &[bulk("key")]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_append_to_nonexistent_key() {
        let mut ctx = make_ctx();
        let result = append(&mut ctx, &[bulk("newkey"), bulk("hello")]).unwrap();
        assert_eq!(result, RespValue::Integer(5));

        // Key should now exist with value "hello"
        let got = get(&mut ctx, &[bulk("newkey")]).unwrap();
        assert_eq!(got, bulk("hello"));
    }

    #[test]
    fn test_append_to_existing() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("Hello")]).unwrap();

        let result = append(&mut ctx, &[bulk("key"), bulk(" World")]).unwrap();
        assert_eq!(result, RespValue::Integer(11));

        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, bulk("Hello World"));
    }

    #[test]
    fn test_append_multiple_times() {
        let mut ctx = make_ctx();

        let r1 = append(&mut ctx, &[bulk("key"), bulk("a")]).unwrap();
        assert_eq!(r1, RespValue::Integer(1));

        let r2 = append(&mut ctx, &[bulk("key"), bulk("bb")]).unwrap();
        assert_eq!(r2, RespValue::Integer(3));

        let r3 = append(&mut ctx, &[bulk("key"), bulk("ccc")]).unwrap();
        assert_eq!(r3, RespValue::Integer(6));

        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, bulk("abbccc"));
    }

    #[test]
    fn test_append_empty_string_to_existing() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("hello")]).unwrap();

        let result = append(&mut ctx, &[bulk("key"), bulk("")]).unwrap();
        assert_eq!(result, RespValue::Integer(5));

        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, bulk("hello"));
    }

    #[test]
    fn test_append_to_empty_string() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("")]).unwrap();

        let result = append(&mut ctx, &[bulk("key"), bulk("world")]).unwrap();
        assert_eq!(result, RespValue::Integer(5));

        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, bulk("world"));
    }

    #[test]
    fn test_append_wrong_type_list() {
        use std::collections::VecDeque;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("item"));
        db.set(Bytes::from("mylist"), Entry::new(RedisValue::List(list)));

        let result = append(&mut ctx, &[bulk("mylist"), bulk("data")]);
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    #[test]
    fn test_append_returns_new_length() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("abc")]).unwrap();

        let result = append(&mut ctx, &[bulk("key"), bulk("defgh")]).unwrap();
        // "abc" + "defgh" = 8 bytes
        assert_eq!(result, RespValue::Integer(8));
    }

    #[test]
    fn test_append_binary_data() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        db.set(
            Bytes::from("binkey"),
            Entry::new(RedisValue::String(Bytes::from(vec![0x00, 0xFF]))),
        );

        let append_val = RespValue::BulkString(Bytes::from(vec![0x80, 0xFE]));
        let result = append(&mut ctx, &[bulk("binkey"), append_val]).unwrap();
        assert_eq!(result, RespValue::Integer(4));

        let db = ctx.store().database(ctx.selected_db());
        let entry = db.get(b"binkey").unwrap();
        if let RedisValue::String(s) = &entry.value {
            assert_eq!(s.as_ref(), &[0x00, 0xFF, 0x80, 0xFE]);
        } else {
            panic!("Expected string value");
        }
    }

    #[test]
    fn test_append_large_value() {
        let mut ctx = make_ctx();
        let large = "x".repeat(100_000);
        let result = append(&mut ctx, &[bulk("key"), bulk(&large)]).unwrap();
        assert_eq!(result, RespValue::Integer(100_000));

        // Append more
        let result2 = append(&mut ctx, &[bulk("key"), bulk(&large)]).unwrap();
        assert_eq!(result2, RespValue::Integer(200_000));
    }

    #[test]
    fn test_append_numeric_string() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("123")]).unwrap();

        let result = append(&mut ctx, &[bulk("key"), bulk("456")]).unwrap();
        assert_eq!(result, RespValue::Integer(6));

        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, bulk("123456"));
    }

    #[test]
    fn test_append_special_chars() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("hello")]).unwrap();

        let result = append(&mut ctx, &[bulk("key"), bulk("!@#$%^&*()")]).unwrap();
        assert_eq!(result, RespValue::Integer(15));

        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, bulk("hello!@#$%^&*()"));
    }

    #[test]
    fn test_append_expired_key() {
        use std::time::Instant;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut entry = Entry::new(RedisValue::String(Bytes::from("old")));
        entry.expires_at = Some(Instant::now() - Duration::from_secs(1));
        db.set(Bytes::from("key"), entry);

        // Expired key should be treated as nonexistent
        let result = append(&mut ctx, &[bulk("key"), bulk("new")]).unwrap();
        assert_eq!(result, RespValue::Integer(3));

        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, bulk("new"));
    }

    #[test]
    fn test_append_removes_ttl() {
        let mut ctx = make_ctx();
        // Set key with TTL
        set(
            &mut ctx,
            &[bulk("key"), bulk("hello"), bulk("EX"), bulk("1000")],
        )
        .unwrap();

        // Verify TTL exists
        {
            let db = ctx.store().database(ctx.selected_db());
            let entry = db.get(b"key").unwrap();
            assert!(entry.expires_at.is_some());
        }

        // APPEND creates a new Entry without TTL
        append(&mut ctx, &[bulk("key"), bulk(" world")]).unwrap();

        let db = ctx.store().database(ctx.selected_db());
        let entry = db.get(b"key").unwrap();
        assert!(
            entry.expires_at.is_none(),
            "APPEND creates a new Entry, so TTL should be removed"
        );
    }

    #[test]
    fn test_append_value_retrievable() {
        let mut ctx = make_ctx();
        append(&mut ctx, &[bulk("key"), bulk("foo")]).unwrap();
        append(&mut ctx, &[bulk("key"), bulk("bar")]).unwrap();

        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, bulk("foobar"));
    }

    #[test]
    fn test_append_unicode() {
        let mut ctx = make_ctx();
        // "Hello" in Japanese: こんにちは (15 bytes in UTF-8)
        set(&mut ctx, &[bulk("key"), bulk("こんにちは")]).unwrap();

        // "World" in Japanese: 世界 (6 bytes in UTF-8)
        let result = append(&mut ctx, &[bulk("key"), bulk("世界")]).unwrap();
        // 15 + 6 = 21 bytes
        assert_eq!(result, RespValue::Integer(21));

        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, bulk("こんにちは世界"));
    }

    #[test]
    fn test_append_newlines() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("line1\n")]).unwrap();

        let result = append(&mut ctx, &[bulk("key"), bulk("line2\r\n")]).unwrap();
        // "line1\n" = 6 bytes, "line2\r\n" = 7 bytes → 13
        assert_eq!(result, RespValue::Integer(13));

        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, bulk("line1\nline2\r\n"));
    }

    #[test]
    fn test_append_empty_key() {
        let mut ctx = make_ctx();
        // Empty string as key name
        let result = append(&mut ctx, &[bulk(""), bulk("value")]).unwrap();
        assert_eq!(result, RespValue::Integer(5));

        let got = get(&mut ctx, &[bulk("")]).unwrap();
        assert_eq!(got, bulk("value"));
    }

    // ========== STRLEN additional tests (19) ==========

    #[test]
    fn test_strlen_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        let result = strlen(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_strlen_nonexistent_key() {
        let mut ctx = make_ctx();
        let result = strlen(&mut ctx, &[bulk("nokey")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_strlen_empty_string() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("")]).unwrap();
        let result = strlen(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_strlen_single_char() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("x")]).unwrap();
        let result = strlen(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_strlen_regular_string() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("Hello World")]).unwrap();
        let result = strlen(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Integer(11));
    }

    #[test]
    fn test_strlen_wrong_type_list() {
        use std::collections::VecDeque;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("item"));
        db.set(Bytes::from("mylist"), Entry::new(RedisValue::List(list)));

        let result = strlen(&mut ctx, &[bulk("mylist")]);
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    #[test]
    fn test_strlen_wrong_type_hash() {
        use std::collections::HashMap;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        db.set(
            Bytes::from("myhash"),
            Entry::new(RedisValue::Hash(HashMap::from([(
                Bytes::from("field"),
                Bytes::from("value"),
            )]))),
        );

        let result = strlen(&mut ctx, &[bulk("myhash")]);
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    #[test]
    fn test_strlen_binary_data() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        // 5 raw bytes — byte length, not "char" length
        db.set(
            Bytes::from("binkey"),
            Entry::new(RedisValue::String(Bytes::from(vec![
                0x00, 0xFF, 0x80, 0xFE, 0x01,
            ]))),
        );

        let result = strlen(&mut ctx, &[bulk("binkey")]).unwrap();
        assert_eq!(result, RespValue::Integer(5));
    }

    #[test]
    fn test_strlen_large_value() {
        let mut ctx = make_ctx();
        let large = "x".repeat(1_000_000);
        set(&mut ctx, &[bulk("key"), bulk(&large)]).unwrap();

        let result = strlen(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Integer(1_000_000));
    }

    #[test]
    fn test_strlen_numeric_string() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("12345")]).unwrap();
        let result = strlen(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Integer(5));
    }

    #[test]
    fn test_strlen_after_append() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("Hello")]).unwrap();
        append(&mut ctx, &[bulk("key"), bulk(" World")]).unwrap();

        let result = strlen(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Integer(11));
    }

    #[test]
    fn test_strlen_after_setrange() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("Hi")]).unwrap();
        // SETRANGE key 5 "World" → pads with zeroes: "Hi\0\0\0World" = 10 bytes
        setrange(&mut ctx, &[bulk("key"), bulk("5"), bulk("World")]).unwrap();

        let result = strlen(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Integer(10));
    }

    #[test]
    fn test_strlen_expired_key() {
        use std::time::Instant;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut entry = Entry::new(RedisValue::String(Bytes::from("hello")));
        entry.expires_at = Some(Instant::now() - Duration::from_secs(1));
        db.set(Bytes::from("key"), entry);

        let result = strlen(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_strlen_unicode() {
        let mut ctx = make_ctx();
        // "é" is 2 bytes in UTF-8, "€" is 3 bytes, "𝕊" is 4 bytes
        // Total: 2 + 3 + 4 = 9 bytes
        set(&mut ctx, &[bulk("key"), bulk("é€𝕊")]).unwrap();

        let result = strlen(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Integer(9));
    }

    #[test]
    fn test_strlen_space_string() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("   ")]).unwrap();
        let result = strlen(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Integer(3));
    }

    #[test]
    fn test_strlen_special_chars() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("!@#$%^&*()")]).unwrap();
        let result = strlen(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Integer(10));
    }

    #[test]
    fn test_strlen_after_set_overwrite() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("short")]).unwrap();
        assert_eq!(
            strlen(&mut ctx, &[bulk("key")]).unwrap(),
            RespValue::Integer(5)
        );

        set(&mut ctx, &[bulk("key"), bulk("a much longer string")]).unwrap();
        let result = strlen(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Integer(20));
    }

    #[test]
    fn test_strlen_returns_integer_type() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("test")]).unwrap();

        let result = strlen(&mut ctx, &[bulk("key")]).unwrap();
        match result {
            RespValue::Integer(v) => assert_eq!(v, 4),
            other => panic!("Expected Integer variant, got {:?}", other),
        }
    }

    #[test]
    fn test_strlen_newline_value() {
        let mut ctx = make_ctx();
        // "\n" is 1 byte
        set(&mut ctx, &[bulk("key"), bulk("a\nb")]).unwrap();
        let result = strlen(&mut ctx, &[bulk("key")]).unwrap();
        // "a" + "\n" + "b" = 3 bytes
        assert_eq!(result, RespValue::Integer(3));
    }

    // ========== DECR additional tests (19) ==========

    #[test]
    fn test_decr_nonexistent_key() {
        let mut ctx = make_ctx();
        let result = decr(&mut ctx, &[bulk("nokey")]).unwrap();
        assert_eq!(result, RespValue::Integer(-1));
    }

    #[test]
    fn test_decr_existing_positive() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("10")]).unwrap();
        let result = decr(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Integer(9));
    }

    #[test]
    fn test_decr_existing_negative() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("-5")]).unwrap();
        let result = decr(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Integer(-6));
    }

    #[test]
    fn test_decr_zero() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("0")]).unwrap();
        let result = decr(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Integer(-1));
    }

    #[test]
    fn test_decr_wrong_type_list() {
        use std::collections::VecDeque;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("item"));
        db.set(Bytes::from("mylist"), Entry::new(RedisValue::List(list)));

        let result = decr(&mut ctx, &[bulk("mylist")]);
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    #[test]
    fn test_decr_non_numeric_string() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("abc")]).unwrap();
        let result = decr(&mut ctx, &[bulk("key")]);
        assert!(matches!(result, Err(CommandError::NotAnInteger)));
    }

    #[test]
    fn test_decr_float_string() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("1.5")]).unwrap();
        let result = decr(&mut ctx, &[bulk("key")]);
        assert!(matches!(result, Err(CommandError::NotAnInteger)));
    }

    #[test]
    fn test_decr_empty_string() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("")]).unwrap();
        let result = decr(&mut ctx, &[bulk("key")]);
        assert!(matches!(result, Err(CommandError::NotAnInteger)));
    }

    #[test]
    fn test_decr_underflow() {
        let mut ctx = make_ctx();
        let min_str = i64::MIN.to_string();
        set(&mut ctx, &[bulk("key"), bulk(&min_str)]).unwrap();
        let result = decr(&mut ctx, &[bulk("key")]);
        assert!(matches!(result, Err(CommandError::NotAnInteger)));
    }

    #[test]
    fn test_decr_multiple_times() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("10")]).unwrap();
        for expected in [9, 8, 7, 6, 5] {
            let result = decr(&mut ctx, &[bulk("key")]).unwrap();
            assert_eq!(result, RespValue::Integer(expected));
        }
    }

    #[test]
    fn test_decr_large_negative() {
        let mut ctx = make_ctx();
        let val = (i64::MIN + 1).to_string();
        set(&mut ctx, &[bulk("key"), bulk(&val)]).unwrap();
        let result = decr(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Integer(i64::MIN));
    }

    #[test]
    fn test_decr_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        let result = decr(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_decr_expired_key() {
        use std::time::Instant;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut entry = Entry::new(RedisValue::String(Bytes::from("100")));
        entry.expires_at = Some(Instant::now() - Duration::from_secs(1));
        db.set(Bytes::from("key"), entry);

        // Expired key treated as 0, so DECR → -1
        let result = decr(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Integer(-1));
    }

    #[test]
    fn test_decr_value_stored_as_string() {
        let mut ctx = make_ctx();
        decr(&mut ctx, &[bulk("key")]).unwrap();

        let db = ctx.store().database(ctx.selected_db());
        let entry = db.get(b"key").unwrap();
        match &entry.value {
            RedisValue::String(s) => assert_eq!(s.as_ref(), b"-1"),
            other => panic!("Expected String, got {:?}", other),
        }
    }

    #[test]
    fn test_decr_from_one() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("1")]).unwrap();
        let result = decr(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_decr_leading_zeros() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("007")]).unwrap();
        let result = decr(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Integer(6));
    }

    #[test]
    fn test_decr_whitespace_value() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk(" 10 ")]).unwrap();
        let result = decr(&mut ctx, &[bulk("key")]);
        assert!(matches!(result, Err(CommandError::NotAnInteger)));
    }

    #[test]
    fn test_decr_returns_integer_type() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("5")]).unwrap();
        let result = decr(&mut ctx, &[bulk("key")]).unwrap();
        assert!(matches!(result, RespValue::Integer(4)));
    }

    #[test]
    fn test_decr_after_incr() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("42")]).unwrap();
        incr(&mut ctx, &[bulk("key")]).unwrap();
        let result = decr(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Integer(42));
    }

    // ========== DECRBY additional tests (19) ==========

    #[test]
    fn test_decrby_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        let result = decrby(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_decrby_wrong_arity_one_arg() {
        let mut ctx = make_ctx();
        let result = decrby(&mut ctx, &[bulk("key")]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_decrby_nonexistent_key() {
        let mut ctx = make_ctx();
        let result = decrby(&mut ctx, &[bulk("key"), bulk("5")]).unwrap();
        assert_eq!(result, RespValue::Integer(-5));
    }

    #[test]
    fn test_decrby_existing_value() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("10")]).unwrap();
        let result = decrby(&mut ctx, &[bulk("key"), bulk("3")]).unwrap();
        assert_eq!(result, RespValue::Integer(7));
    }

    #[test]
    fn test_decrby_negative_decrement() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("10")]).unwrap();
        let result = decrby(&mut ctx, &[bulk("key"), bulk("-5")]).unwrap();
        assert_eq!(result, RespValue::Integer(15));
    }

    #[test]
    fn test_decrby_zero_decrement() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("10")]).unwrap();
        let result = decrby(&mut ctx, &[bulk("key"), bulk("0")]).unwrap();
        assert_eq!(result, RespValue::Integer(10));
    }

    #[test]
    fn test_decrby_wrong_type_list() {
        use std::collections::VecDeque;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("item"));
        db.set(Bytes::from("mylist"), Entry::new(RedisValue::List(list)));

        let result = decrby(&mut ctx, &[bulk("mylist"), bulk("1")]);
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    #[test]
    fn test_decrby_non_numeric_string() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("abc")]).unwrap();
        let result = decrby(&mut ctx, &[bulk("key"), bulk("1")]);
        assert!(matches!(result, Err(CommandError::NotAnInteger)));
    }

    #[test]
    fn test_decrby_float_string_key() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("1.5")]).unwrap();
        let result = decrby(&mut ctx, &[bulk("key"), bulk("1")]);
        assert!(matches!(result, Err(CommandError::NotAnInteger)));
    }

    #[test]
    fn test_decrby_float_decrement() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("10")]).unwrap();
        let result = decrby(&mut ctx, &[bulk("key"), bulk("1.5")]);
        assert!(matches!(result, Err(CommandError::NotAnInteger)));
    }

    #[test]
    fn test_decrby_underflow() {
        let mut ctx = make_ctx();
        let min_str = i64::MIN.to_string();
        set(&mut ctx, &[bulk("key"), bulk(&min_str)]).unwrap();
        let result = decrby(&mut ctx, &[bulk("key"), bulk("1")]);
        assert!(matches!(result, Err(CommandError::NotAnInteger)));
    }

    #[test]
    fn test_decrby_overflow() {
        // decrby with negative decrement effectively adds, causing overflow
        // key = i64::MAX, decrby -1 → incrby_impl(key, 1) → i64::MAX + 1 → overflow
        let mut ctx = make_ctx();
        let max_str = i64::MAX.to_string();
        set(&mut ctx, &[bulk("key"), bulk(&max_str)]).unwrap();
        let result = decrby(&mut ctx, &[bulk("key"), bulk("-1")]);
        assert!(matches!(result, Err(CommandError::NotAnInteger)));
    }

    #[test]
    fn test_decrby_large_positive() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("0")]).unwrap();
        let large = (i64::MAX - 100).to_string();
        let result = decrby(&mut ctx, &[bulk("key"), bulk(&large)]).unwrap();
        assert_eq!(result, RespValue::Integer(-(i64::MAX - 100)));
    }

    #[test]
    fn test_decrby_large_negative() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("0")]).unwrap();
        let large_neg = (i64::MIN + 100).to_string();
        // decrby key (i64::MIN + 100) → incrby_impl(key, -(i64::MIN + 100))
        // -(i64::MIN + 100) = i64::MAX - 99
        let result = decrby(&mut ctx, &[bulk("key"), bulk(&large_neg)]).unwrap();
        assert_eq!(result, RespValue::Integer(i64::MAX - 99));
    }

    #[test]
    fn test_decrby_expired_key() {
        use std::time::Instant;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut entry = Entry::new(RedisValue::String(Bytes::from("100")));
        entry.expires_at = Some(Instant::now() - Duration::from_secs(1));
        db.set(Bytes::from("key"), entry);

        // Expired key treated as 0, so DECRBY 5 → -5
        let result = decrby(&mut ctx, &[bulk("key"), bulk("5")]).unwrap();
        assert_eq!(result, RespValue::Integer(-5));
    }

    #[test]
    fn test_decrby_multiple_times() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("100")]).unwrap();
        for expected in [90, 80, 70, 60, 50] {
            let result = decrby(&mut ctx, &[bulk("key"), bulk("10")]).unwrap();
            assert_eq!(result, RespValue::Integer(expected));
        }
    }

    #[test]
    fn test_decrby_returns_integer_type() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("20")]).unwrap();
        let result = decrby(&mut ctx, &[bulk("key"), bulk("7")]).unwrap();
        assert!(matches!(result, RespValue::Integer(13)));
    }

    #[test]
    fn test_decrby_empty_string_key() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("")]).unwrap();
        let result = decrby(&mut ctx, &[bulk("key"), bulk("1")]);
        assert!(matches!(result, Err(CommandError::NotAnInteger)));
    }

    #[test]
    fn test_decrby_to_zero() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("5")]).unwrap();
        let result = decrby(&mut ctx, &[bulk("key"), bulk("5")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    // ========== GETRANGE additional tests (19) ==========

    #[test]
    fn test_getrange_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        let result = getrange(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_getrange_wrong_arity_two_args() {
        let mut ctx = make_ctx();
        let result = getrange(&mut ctx, &[bulk("key"), bulk("0")]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_getrange_nonexistent_key() {
        let mut ctx = make_ctx();
        let result = getrange(&mut ctx, &[bulk("nokey"), bulk("0"), bulk("4")]).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::new()));
    }

    #[test]
    fn test_getrange_whole_string() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("Hello World")]).unwrap();
        let result = getrange(&mut ctx, &[bulk("key"), bulk("0"), bulk("-1")]).unwrap();
        assert_eq!(result, bulk("Hello World"));
    }

    #[test]
    fn test_getrange_first_char() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("Hello")]).unwrap();
        let result = getrange(&mut ctx, &[bulk("key"), bulk("0"), bulk("0")]).unwrap();
        assert_eq!(result, bulk("H"));
    }

    #[test]
    fn test_getrange_last_char() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("Hello")]).unwrap();
        let result = getrange(&mut ctx, &[bulk("key"), bulk("-1"), bulk("-1")]).unwrap();
        assert_eq!(result, bulk("o"));
    }

    #[test]
    fn test_getrange_middle_range() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("Hello World")]).unwrap();
        let result = getrange(&mut ctx, &[bulk("key"), bulk("2"), bulk("5")]).unwrap();
        assert_eq!(result, bulk("llo "));
    }

    #[test]
    fn test_getrange_negative_start() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("Hello World")]).unwrap();
        let result = getrange(&mut ctx, &[bulk("key"), bulk("-5"), bulk("-1")]).unwrap();
        assert_eq!(result, bulk("World"));
    }

    #[test]
    fn test_getrange_start_beyond_end() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("Hello")]).unwrap();
        // start=3 > end=1 → empty
        let result = getrange(&mut ctx, &[bulk("key"), bulk("3"), bulk("1")]).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::new()));
    }

    #[test]
    fn test_getrange_start_beyond_length() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("Hello")]).unwrap();
        // start=10 > len=5 → empty
        let result = getrange(&mut ctx, &[bulk("key"), bulk("10"), bulk("20")]).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::new()));
    }

    #[test]
    fn test_getrange_end_beyond_length() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("Hello")]).unwrap();
        // end=100 > len=5, should clamp to end of string
        let result = getrange(&mut ctx, &[bulk("key"), bulk("0"), bulk("100")]).unwrap();
        assert_eq!(result, bulk("Hello"));
    }

    #[test]
    fn test_getrange_both_negative() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("Hello World")]).unwrap();
        // -5 to -2 → "Worl"
        let result = getrange(&mut ctx, &[bulk("key"), bulk("-5"), bulk("-2")]).unwrap();
        assert_eq!(result, bulk("Worl"));
    }

    #[test]
    fn test_getrange_wrong_type_list() {
        use std::collections::VecDeque;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("item"));
        db.set(Bytes::from("mylist"), Entry::new(RedisValue::List(list)));

        let result = getrange(&mut ctx, &[bulk("mylist"), bulk("0"), bulk("4")]);
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    #[test]
    fn test_getrange_empty_string_value() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("")]).unwrap();
        let result = getrange(&mut ctx, &[bulk("key"), bulk("0"), bulk("0")]).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::new()));
    }

    #[test]
    fn test_getrange_expired_key() {
        use std::time::Instant;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut entry = Entry::new(RedisValue::String(Bytes::from("Hello")));
        entry.expires_at = Some(Instant::now() - Duration::from_secs(1));
        db.set(Bytes::from("key"), entry);

        let result = getrange(&mut ctx, &[bulk("key"), bulk("0"), bulk("4")]).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::new()));
    }

    #[test]
    fn test_getrange_non_integer_start() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("Hello")]).unwrap();
        let result = getrange(&mut ctx, &[bulk("key"), bulk("abc"), bulk("4")]);
        assert!(matches!(result, Err(CommandError::NotAnInteger)));
    }

    #[test]
    fn test_getrange_non_integer_end() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("Hello")]).unwrap();
        let result = getrange(&mut ctx, &[bulk("key"), bulk("0"), bulk("xyz")]);
        assert!(matches!(result, Err(CommandError::NotAnInteger)));
    }

    #[test]
    fn test_getrange_single_char_string() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("X")]).unwrap();

        // 0 to 0 → "X"
        let result = getrange(&mut ctx, &[bulk("key"), bulk("0"), bulk("0")]).unwrap();
        assert_eq!(result, bulk("X"));

        // -1 to -1 → "X"
        let result = getrange(&mut ctx, &[bulk("key"), bulk("-1"), bulk("-1")]).unwrap();
        assert_eq!(result, bulk("X"));

        // 0 to -1 → "X"
        let result = getrange(&mut ctx, &[bulk("key"), bulk("0"), bulk("-1")]).unwrap();
        assert_eq!(result, bulk("X"));

        // 1 to 1 → empty (beyond length)
        let result = getrange(&mut ctx, &[bulk("key"), bulk("1"), bulk("1")]).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::new()));
    }

    #[test]
    fn test_getrange_zero_zero_on_string() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("abcdef")]).unwrap();
        let result = getrange(&mut ctx, &[bulk("key"), bulk("0"), bulk("0")]).unwrap();
        assert_eq!(result, bulk("a"));
    }

    // ========== SETRANGE additional tests (18) ==========

    #[test]
    fn test_setrange_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        let result = setrange(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_setrange_wrong_arity_two_args() {
        let mut ctx = make_ctx();
        let result = setrange(&mut ctx, &[bulk("key"), bulk("0")]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_setrange_nonexistent_key() {
        let mut ctx = make_ctx();
        // Setting at offset 5 on nonexistent key creates zero-padded string
        let result = setrange(&mut ctx, &[bulk("nokey"), bulk("5"), bulk("Hi")]).unwrap();
        assert_eq!(result, RespValue::Integer(7));

        let got = get(&mut ctx, &[bulk("nokey")]).unwrap();
        if let RespValue::BulkString(b) = got {
            assert_eq!(b.len(), 7);
            assert_eq!(&b[0..5], &[0u8; 5]); // zero-padded
            assert_eq!(&b[5..7], b"Hi");
        } else {
            panic!("expected BulkString");
        }
    }

    #[test]
    fn test_setrange_at_beginning() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("Hello World")]).unwrap();
        let result = setrange(&mut ctx, &[bulk("key"), bulk("0"), bulk("Yo")]).unwrap();
        assert_eq!(result, RespValue::Integer(11));

        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, bulk("Yollo World"));
    }

    #[test]
    fn test_setrange_in_middle() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("Hello World")]).unwrap();
        let result = setrange(&mut ctx, &[bulk("key"), bulk("3"), bulk("XY")]).unwrap();
        assert_eq!(result, RespValue::Integer(11));

        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, bulk("HelXY World"));
    }

    #[test]
    fn test_setrange_at_end() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("Hello")]).unwrap();
        // offset = len (5), appends
        let result = setrange(&mut ctx, &[bulk("key"), bulk("5"), bulk("!")]).unwrap();
        assert_eq!(result, RespValue::Integer(6));

        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, bulk("Hello!"));
    }

    #[test]
    fn test_setrange_beyond_end() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("Hi")]).unwrap();
        // offset=10, well beyond len=2
        let result = setrange(&mut ctx, &[bulk("key"), bulk("10"), bulk("!")]).unwrap();
        assert_eq!(result, RespValue::Integer(11));

        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::BulkString(b) = got {
            assert_eq!(b.len(), 11);
            assert_eq!(&b[0..2], b"Hi");
            // bytes 2..10 should be zero
            for i in 2..10 {
                assert_eq!(b[i], 0u8, "byte at index {} should be zero", i);
            }
            assert_eq!(b[10], b'!');
        } else {
            panic!("expected BulkString");
        }
    }

    #[test]
    fn test_setrange_negative_offset() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("Hello")]).unwrap();
        let result = setrange(&mut ctx, &[bulk("key"), bulk("-1"), bulk("X")]);
        assert!(matches!(result, Err(CommandError::InvalidArgument(_))));
    }

    #[test]
    fn test_setrange_wrong_type_list() {
        use std::collections::VecDeque;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("item"));
        db.set(Bytes::from("mylist"), Entry::new(RedisValue::List(list)));

        let result = setrange(&mut ctx, &[bulk("mylist"), bulk("0"), bulk("X")]);
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    #[test]
    fn test_setrange_empty_value() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("Hello")]).unwrap();
        // Setting empty value at offset 2 → no change, returns existing length
        let result = setrange(&mut ctx, &[bulk("key"), bulk("2"), bulk("")]).unwrap();
        assert_eq!(result, RespValue::Integer(5));

        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, bulk("Hello"));
    }

    #[test]
    fn test_setrange_expired_key() {
        use std::time::Instant;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut entry = Entry::new(RedisValue::String(Bytes::from("old")));
        entry.expires_at = Some(Instant::now() - Duration::from_secs(1));
        db.set(Bytes::from("key"), entry);

        // Expired key should be treated as new (empty)
        let result = setrange(&mut ctx, &[bulk("key"), bulk("3"), bulk("Hi")]).unwrap();
        assert_eq!(result, RespValue::Integer(5));

        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::BulkString(b) = got {
            assert_eq!(b.len(), 5);
            assert_eq!(&b[0..3], &[0u8; 3]);
            assert_eq!(&b[3..5], b"Hi");
        } else {
            panic!("expected BulkString");
        }
    }

    #[test]
    fn test_setrange_returns_new_length() {
        let mut ctx = make_ctx();
        // No existing key, offset=0, value="ABCDE" → length 5
        let result = setrange(&mut ctx, &[bulk("key"), bulk("0"), bulk("ABCDE")]).unwrap();
        assert_eq!(result, RespValue::Integer(5));

        // Extend: offset=3, value="XYZ" → length 6
        let result = setrange(&mut ctx, &[bulk("key"), bulk("3"), bulk("XYZ")]).unwrap();
        assert_eq!(result, RespValue::Integer(6));
    }

    #[test]
    fn test_setrange_non_integer_offset() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("Hello")]).unwrap();
        let result = setrange(&mut ctx, &[bulk("key"), bulk("abc"), bulk("X")]);
        assert!(matches!(result, Err(CommandError::NotAnInteger)));
    }

    #[test]
    fn test_setrange_large_offset() {
        let mut ctx = make_ctx();
        // Creating string with offset 100 on nonexistent key
        let result = setrange(&mut ctx, &[bulk("key"), bulk("100"), bulk("X")]).unwrap();
        assert_eq!(result, RespValue::Integer(101));

        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::BulkString(b) = got {
            assert_eq!(b.len(), 101);
            // First 100 bytes all zero
            for i in 0..100 {
                assert_eq!(b[i], 0u8, "byte at index {} should be zero", i);
            }
            assert_eq!(b[100], b'X');
        } else {
            panic!("expected BulkString");
        }
    }

    #[test]
    fn test_setrange_overwrite_entire() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("Hello")]).unwrap();
        let result = setrange(&mut ctx, &[bulk("key"), bulk("0"), bulk("World")]).unwrap();
        assert_eq!(result, RespValue::Integer(5));

        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, bulk("World"));
    }

    #[test]
    fn test_setrange_partial_overwrite() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("Hello World")]).unwrap();
        // Overwrite only 2 chars at offset 3
        let result = setrange(&mut ctx, &[bulk("key"), bulk("3"), bulk("XY")]).unwrap();
        assert_eq!(result, RespValue::Integer(11));

        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, bulk("HelXY World"));
    }

    #[test]
    fn test_setrange_on_empty_string() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("")]).unwrap();
        let result = setrange(&mut ctx, &[bulk("key"), bulk("3"), bulk("Hi")]).unwrap();
        assert_eq!(result, RespValue::Integer(5));

        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::BulkString(b) = got {
            assert_eq!(b.len(), 5);
            assert_eq!(&b[0..3], &[0u8; 3]);
            assert_eq!(&b[3..5], b"Hi");
        } else {
            panic!("expected BulkString");
        }
    }

    #[test]
    fn test_setrange_value_retrievable() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("abcdefghij")]).unwrap();

        // Overwrite positions 2..5 with "WXYZ"
        setrange(&mut ctx, &[bulk("key"), bulk("2"), bulk("WXYZ")]).unwrap();

        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, bulk("abWXYZghij"));

        // Also verify via getrange
        let sub = getrange(&mut ctx, &[bulk("key"), bulk("2"), bulk("5")]).unwrap();
        assert_eq!(sub, bulk("WXYZ"));
    }

    // ========== GETSET additional tests (19) ==========

    #[test]
    fn test_getset_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        let result = getset(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_getset_wrong_arity_one_arg() {
        let mut ctx = make_ctx();
        let result = getset(&mut ctx, &[bulk("key")]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_getset_nonexistent_key() {
        let mut ctx = make_ctx();
        let result = getset(&mut ctx, &[bulk("nokey"), bulk("hello")]).unwrap();
        assert_eq!(result, RespValue::Null);

        // Value should now be set
        let got = get(&mut ctx, &[bulk("nokey")]).unwrap();
        assert_eq!(got, bulk("hello"));
    }

    #[test]
    fn test_getset_existing_key() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("old")]).unwrap();

        let result = getset(&mut ctx, &[bulk("key"), bulk("new")]).unwrap();
        assert_eq!(result, bulk("old"));

        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, bulk("new"));
    }

    #[test]
    fn test_getset_multiple_times() {
        let mut ctx = make_ctx();

        let r1 = getset(&mut ctx, &[bulk("key"), bulk("a")]).unwrap();
        assert_eq!(r1, RespValue::Null);

        let r2 = getset(&mut ctx, &[bulk("key"), bulk("b")]).unwrap();
        assert_eq!(r2, bulk("a"));

        let r3 = getset(&mut ctx, &[bulk("key"), bulk("c")]).unwrap();
        assert_eq!(r3, bulk("b"));

        let r4 = getset(&mut ctx, &[bulk("key"), bulk("d")]).unwrap();
        assert_eq!(r4, bulk("c"));

        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, bulk("d"));
    }

    #[test]
    fn test_getset_wrong_type_list() {
        use std::collections::VecDeque;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("item"));
        db.set(Bytes::from("mylist"), Entry::new(RedisValue::List(list)));

        let result = getset(&mut ctx, &[bulk("mylist"), bulk("value")]);
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    #[test]
    fn test_getset_wrong_type_hash() {
        use std::collections::HashMap;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        db.set(
            Bytes::from("myhash"),
            Entry::new(RedisValue::Hash(HashMap::from([(
                Bytes::from("f"),
                Bytes::from("v"),
            )]))),
        );

        let result = getset(&mut ctx, &[bulk("myhash"), bulk("value")]);
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    #[test]
    fn test_getset_expired_key() {
        use std::time::Instant;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut entry = Entry::new(RedisValue::String(Bytes::from("old")));
        entry.expires_at = Some(Instant::now() - Duration::from_secs(1));
        db.set(Bytes::from("key"), entry);

        // Expired key returns Null
        let result = getset(&mut ctx, &[bulk("key"), bulk("new")]).unwrap();
        assert_eq!(result, RespValue::Null);

        // New value should be stored
        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, bulk("new"));
    }

    #[test]
    fn test_getset_empty_string_value() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("hello")]).unwrap();

        let result = getset(&mut ctx, &[bulk("key"), bulk("")]).unwrap();
        assert_eq!(result, bulk("hello"));

        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, bulk(""));
    }

    #[test]
    fn test_getset_replaces_value_type() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("original")]).unwrap();

        getset(&mut ctx, &[bulk("key"), bulk("replaced")]).unwrap();

        // Verify it's stored as a string in the store
        let db = ctx.store().database(ctx.selected_db());
        let entry = db.get(b"key").unwrap();
        assert!(matches!(entry.value, RedisValue::String(_)));
        if let RedisValue::String(s) = &entry.value {
            assert_eq!(s.as_ref(), b"replaced");
        }
    }

    #[test]
    fn test_getset_returns_bulk_string() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("value")]).unwrap();

        let result = getset(&mut ctx, &[bulk("key"), bulk("new")]).unwrap();
        assert!(matches!(result, RespValue::BulkString(_)));
    }

    #[test]
    fn test_getset_binary_value() {
        let mut ctx = make_ctx();
        // Store binary data directly
        {
            let db = ctx.store().database(ctx.selected_db());
            db.set(
                Bytes::from("binkey"),
                Entry::new(RedisValue::String(Bytes::from(vec![0x00, 0xFF, 0x80]))),
            );
        }

        let result = getset(
            &mut ctx,
            &[
                bulk("binkey"),
                RespValue::BulkString(Bytes::from(vec![0xDE, 0xAD])),
            ],
        )
        .unwrap();
        if let RespValue::BulkString(b) = &result {
            assert_eq!(b.as_ref(), &[0x00, 0xFF, 0x80]);
        } else {
            panic!("Expected BulkString");
        }

        // Verify new binary value is stored
        let db = ctx.store().database(ctx.selected_db());
        let entry = db.get(b"binkey").unwrap();
        if let RedisValue::String(s) = &entry.value {
            assert_eq!(s.as_ref(), &[0xDE, 0xAD]);
        } else {
            panic!("Expected string value");
        }
    }

    #[test]
    fn test_getset_numeric_strings() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("42")]).unwrap();

        let result = getset(&mut ctx, &[bulk("key"), bulk("-7")]).unwrap();
        assert_eq!(result, bulk("42"));

        let result = getset(&mut ctx, &[bulk("key"), bulk("3.14")]).unwrap();
        assert_eq!(result, bulk("-7"));

        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, bulk("3.14"));
    }

    #[test]
    fn test_getset_large_value() {
        let mut ctx = make_ctx();
        let large = "x".repeat(1_000_000);
        set(&mut ctx, &[bulk("key"), bulk("small")]).unwrap();

        let result = getset(&mut ctx, &[bulk("key"), bulk(&large)]).unwrap();
        assert_eq!(result, bulk("small"));

        let db = ctx.store().database(ctx.selected_db());
        let entry = db.get(b"key").unwrap();
        if let RedisValue::String(s) = &entry.value {
            assert_eq!(s.len(), 1_000_000);
        } else {
            panic!("Expected string value");
        }
    }

    #[test]
    fn test_getset_special_chars() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("hello!@#$%^&*()")]).unwrap();

        let result = getset(&mut ctx, &[bulk("key"), bulk("new\n\t\rvalue")]).unwrap();
        assert_eq!(result, bulk("hello!@#$%^&*()"));

        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, bulk("new\n\t\rvalue"));
    }

    #[test]
    fn test_getset_removes_ttl() {
        let mut ctx = make_ctx();
        set(
            &mut ctx,
            &[bulk("key"), bulk("old"), bulk("EX"), bulk("1000")],
        )
        .unwrap();

        {
            let db = ctx.store().database(ctx.selected_db());
            let entry = db.get(b"key").unwrap();
            assert!(entry.expires_at.is_some());
        }

        getset(&mut ctx, &[bulk("key"), bulk("new")]).unwrap();

        let db = ctx.store().database(ctx.selected_db());
        let entry = db.get(b"key").unwrap();
        assert!(
            entry.expires_at.is_none(),
            "GETSET should create a new entry without TTL"
        );
    }

    #[test]
    fn test_getset_same_value() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("same")]).unwrap();

        let result = getset(&mut ctx, &[bulk("key"), bulk("same")]).unwrap();
        assert_eq!(result, bulk("same"));

        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, bulk("same"));
    }

    #[test]
    fn test_getset_value_retrievable() {
        let mut ctx = make_ctx();
        getset(&mut ctx, &[bulk("key"), bulk("hello world")]).unwrap();

        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, bulk("hello world"));
    }

    #[test]
    fn test_getset_empty_key() {
        let mut ctx = make_ctx();
        let result = getset(&mut ctx, &[bulk(""), bulk("value")]).unwrap();
        assert_eq!(result, RespValue::Null);

        let got = get(&mut ctx, &[bulk("")]).unwrap();
        assert_eq!(got, bulk("value"));
    }

    // ========== GETDEL additional tests (19) ==========

    #[test]
    fn test_getdel_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        let result = getdel(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_getdel_nonexistent_key() {
        let mut ctx = make_ctx();
        let result = getdel(&mut ctx, &[bulk("nokey")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_getdel_existing_key() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("hello")]).unwrap();

        let result = getdel(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, bulk("hello"));

        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, RespValue::Null);
    }

    #[test]
    fn test_getdel_key_actually_deleted() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("value")]).unwrap();

        getdel(&mut ctx, &[bulk("key")]).unwrap();

        let db = ctx.store().database(ctx.selected_db());
        assert!(db.get(b"key").is_none(), "Key should be deleted from store");
    }

    #[test]
    fn test_getdel_wrong_type_list() {
        use std::collections::VecDeque;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("item"));
        db.set(Bytes::from("mylist"), Entry::new(RedisValue::List(list)));

        let result = getdel(&mut ctx, &[bulk("mylist")]);
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    #[test]
    fn test_getdel_wrong_type_hash() {
        use std::collections::HashMap;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        db.set(
            Bytes::from("myhash"),
            Entry::new(RedisValue::Hash(HashMap::from([(
                Bytes::from("f"),
                Bytes::from("v"),
            )]))),
        );

        let result = getdel(&mut ctx, &[bulk("myhash")]);
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    #[test]
    fn test_getdel_expired_key() {
        use std::time::Instant;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut entry = Entry::new(RedisValue::String(Bytes::from("old")));
        entry.expires_at = Some(Instant::now() - Duration::from_secs(1));
        db.set(Bytes::from("key"), entry);

        let result = getdel(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_getdel_empty_string_value() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("")]).unwrap();

        let result = getdel(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, bulk(""));

        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, RespValue::Null);
    }

    #[test]
    fn test_getdel_returns_bulk_string() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("value")]).unwrap();

        let result = getdel(&mut ctx, &[bulk("key")]).unwrap();
        assert!(matches!(result, RespValue::BulkString(_)));
    }

    #[test]
    fn test_getdel_binary_value() {
        let mut ctx = make_ctx();
        {
            let db = ctx.store().database(ctx.selected_db());
            db.set(
                Bytes::from("binkey"),
                Entry::new(RedisValue::String(Bytes::from(vec![
                    0x00, 0xFF, 0x80, 0x01,
                ]))),
            );
        }

        let result = getdel(&mut ctx, &[bulk("binkey")]).unwrap();
        if let RespValue::BulkString(b) = &result {
            assert_eq!(b.as_ref(), &[0x00, 0xFF, 0x80, 0x01]);
        } else {
            panic!("Expected BulkString");
        }

        // Key should be deleted
        let db = ctx.store().database(ctx.selected_db());
        assert!(db.get(b"binkey").is_none());
    }

    #[test]
    fn test_getdel_numeric_string() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("num"), bulk("42")]).unwrap();

        let result = getdel(&mut ctx, &[bulk("num")]).unwrap();
        assert_eq!(result, bulk("42"));

        let got = get(&mut ctx, &[bulk("num")]).unwrap();
        assert_eq!(got, RespValue::Null);
    }

    #[test]
    fn test_getdel_large_value() {
        let mut ctx = make_ctx();
        let large = "x".repeat(1_000_000);
        set(&mut ctx, &[bulk("key"), bulk(&large)]).unwrap();

        let result = getdel(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::BulkString(b) = &result {
            assert_eq!(b.len(), 1_000_000);
        } else {
            panic!("Expected BulkString");
        }

        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, RespValue::Null);
    }

    #[test]
    fn test_getdel_special_chars() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("!@#$%^&*()_+\n\t")]).unwrap();

        let result = getdel(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, bulk("!@#$%^&*()_+\n\t"));
    }

    #[test]
    fn test_getdel_twice() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("value")]).unwrap();

        let r1 = getdel(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(r1, bulk("value"));

        let r2 = getdel(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(r2, RespValue::Null);
    }

    #[test]
    fn test_getdel_does_not_affect_other_keys() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("k1"), bulk("v1")]).unwrap();
        set(&mut ctx, &[bulk("k2"), bulk("v2")]).unwrap();
        set(&mut ctx, &[bulk("k3"), bulk("v3")]).unwrap();

        getdel(&mut ctx, &[bulk("k2")]).unwrap();

        assert_eq!(get(&mut ctx, &[bulk("k1")]).unwrap(), bulk("v1"));
        assert_eq!(get(&mut ctx, &[bulk("k3")]).unwrap(), bulk("v3"));
        assert_eq!(get(&mut ctx, &[bulk("k2")]).unwrap(), RespValue::Null);
    }

    #[test]
    fn test_getdel_key_with_ttl() {
        let mut ctx = make_ctx();
        set(
            &mut ctx,
            &[bulk("key"), bulk("value"), bulk("EX"), bulk("1000")],
        )
        .unwrap();

        let result = getdel(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, bulk("value"));

        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, RespValue::Null);
    }

    #[test]
    fn test_getdel_empty_key() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk(""), bulk("emptykey")]).unwrap();

        let result = getdel(&mut ctx, &[bulk("")]).unwrap();
        assert_eq!(result, bulk("emptykey"));

        let got = get(&mut ctx, &[bulk("")]).unwrap();
        assert_eq!(got, RespValue::Null);
    }

    #[test]
    fn test_getdel_unicode_value() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("こんにちは世界🌍")]).unwrap();

        let result = getdel(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(result, bulk("こんにちは世界🌍"));

        let got = get(&mut ctx, &[bulk("key")]).unwrap();
        assert_eq!(got, RespValue::Null);
    }

    #[test]
    fn test_getdel_after_set() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("mykey"), bulk("myvalue")]).unwrap();

        let result = getdel(&mut ctx, &[bulk("mykey")]).unwrap();
        assert_eq!(result, bulk("myvalue"));

        let db = ctx.store().database(ctx.selected_db());
        assert!(db.get(b"mykey").is_none());
    }

    // ========== GETEX additional tests (8) ==========

    #[test]
    fn test_getex_invalid_option() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("hello")]).unwrap();

        let result = getex(&mut ctx, &[bulk("key"), bulk("BADOPTION")]);
        assert!(matches!(result, Err(CommandError::SyntaxError)));
    }

    #[test]
    fn test_getex_ex_missing_value() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("hello")]).unwrap();

        let result = getex(&mut ctx, &[bulk("key"), bulk("EX")]);
        assert!(matches!(result, Err(CommandError::SyntaxError)));
    }

    #[test]
    fn test_getex_px_missing_value() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("hello")]).unwrap();

        let result = getex(&mut ctx, &[bulk("key"), bulk("PX")]);
        assert!(matches!(result, Err(CommandError::SyntaxError)));
    }

    #[test]
    fn test_getex_px_negative_rejected() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("hello")]).unwrap();

        let result = getex(&mut ctx, &[bulk("key"), bulk("PX"), bulk("-1")]);
        assert!(matches!(result, Err(CommandError::InvalidArgument(_))));
    }

    #[test]
    fn test_getex_exat_zero_rejected() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("hello")]).unwrap();

        let result = getex(&mut ctx, &[bulk("key"), bulk("EXAT"), bulk("0")]);
        assert!(matches!(result, Err(CommandError::InvalidArgument(_))));
    }

    #[test]
    fn test_getex_pxat_zero_rejected() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("hello")]).unwrap();

        let result = getex(&mut ctx, &[bulk("key"), bulk("PXAT"), bulk("0")]);
        assert!(matches!(result, Err(CommandError::InvalidArgument(_))));
    }

    #[test]
    fn test_getex_expired_key_returns_null() {
        use std::time::Instant;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut entry = Entry::new(RedisValue::String(Bytes::from("old")));
        entry.expires_at = Some(Instant::now() - Duration::from_secs(1));
        db.set(Bytes::from("key"), entry);

        let result = getex(&mut ctx, &[bulk("key"), bulk("EX"), bulk("100")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_getex_binary_value() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        db.set(
            Bytes::from("binkey"),
            Entry::new(RedisValue::String(Bytes::from(vec![
                0x00, 0xFF, 0x80, 0xAB,
            ]))),
        );

        let result = getex(&mut ctx, &[bulk("binkey")]).unwrap();
        if let RespValue::BulkString(b) = &result {
            assert_eq!(b.as_ref(), &[0x00, 0xFF, 0x80, 0xAB]);
        } else {
            panic!("Expected BulkString");
        }
    }

    // BITMAP TESTS

    #[test]
    fn test_getbit_basic() {
        let mut ctx = make_ctx();

        // Set a byte with pattern 10101010
        setbit(&mut ctx, &[bulk("key"), bulk("0"), bulk("1")]).unwrap();
        setbit(&mut ctx, &[bulk("key"), bulk("2"), bulk("1")]).unwrap();
        setbit(&mut ctx, &[bulk("key"), bulk("4"), bulk("1")]).unwrap();
        setbit(&mut ctx, &[bulk("key"), bulk("6"), bulk("1")]).unwrap();

        assert_eq!(
            getbit(&mut ctx, &[bulk("key"), bulk("0")]).unwrap(),
            RespValue::Integer(1)
        );
        assert_eq!(
            getbit(&mut ctx, &[bulk("key"), bulk("1")]).unwrap(),
            RespValue::Integer(0)
        );
        assert_eq!(
            getbit(&mut ctx, &[bulk("key"), bulk("2")]).unwrap(),
            RespValue::Integer(1)
        );
        assert_eq!(
            getbit(&mut ctx, &[bulk("key"), bulk("7")]).unwrap(),
            RespValue::Integer(0)
        );
    }

    #[test]
    fn test_getbit_nonexistent_key() {
        let mut ctx = make_ctx();
        assert_eq!(
            getbit(&mut ctx, &[bulk("nokey"), bulk("0")]).unwrap(),
            RespValue::Integer(0)
        );
    }

    #[test]
    fn test_getbit_out_of_range() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("hello")]).unwrap();

        // Offset beyond string length
        assert_eq!(
            getbit(&mut ctx, &[bulk("key"), bulk("1000")]).unwrap(),
            RespValue::Integer(0)
        );
    }

    #[test]
    fn test_getbit_wrong_type() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        db.set(
            Bytes::from("list"),
            Entry::new(RedisValue::List(Default::default())),
        );

        assert!(getbit(&mut ctx, &[bulk("list"), bulk("0")]).is_err());
    }

    #[test]
    fn test_setbit_basic() {
        let mut ctx = make_ctx();

        assert_eq!(
            setbit(&mut ctx, &[bulk("key"), bulk("7"), bulk("1")]).unwrap(),
            RespValue::Integer(0)
        );
        assert_eq!(
            setbit(&mut ctx, &[bulk("key"), bulk("7"), bulk("0")]).unwrap(),
            RespValue::Integer(1)
        );
    }

    #[test]
    fn test_setbit_expansion() {
        let mut ctx = make_ctx();

        // Set a bit far away - should expand string
        setbit(&mut ctx, &[bulk("key"), bulk("100"), bulk("1")]).unwrap();

        let result = get(&mut ctx, &[bulk("key")]).unwrap();
        if let RespValue::BulkString(bytes) = result {
            assert_eq!(bytes.len(), 13); // 100/8 + 1
        } else {
            panic!("Expected BulkString");
        }
    }

    #[test]
    fn test_setbit_returns_old_value() {
        let mut ctx = make_ctx();

        assert_eq!(
            setbit(&mut ctx, &[bulk("key"), bulk("0"), bulk("1")]).unwrap(),
            RespValue::Integer(0)
        );
        assert_eq!(
            setbit(&mut ctx, &[bulk("key"), bulk("0"), bulk("1")]).unwrap(),
            RespValue::Integer(1)
        );
        assert_eq!(
            setbit(&mut ctx, &[bulk("key"), bulk("0"), bulk("0")]).unwrap(),
            RespValue::Integer(1)
        );
    }

    #[test]
    fn test_setbit_invalid_bit() {
        let mut ctx = make_ctx();

        assert!(setbit(&mut ctx, &[bulk("key"), bulk("0"), bulk("2")]).is_err());
        assert!(setbit(&mut ctx, &[bulk("key"), bulk("0"), bulk("-1")]).is_err());
    }

    #[test]
    fn test_bitcount_basic() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("foobar")]).unwrap();

        // "foobar" in binary has 26 set bits
        assert_eq!(
            bitcount(&mut ctx, &[bulk("key")]).unwrap(),
            RespValue::Integer(26)
        );
    }

    #[test]
    fn test_bitcount_with_range() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("foobar")]).unwrap();

        // Count bits in first byte only (f = 0x66 = 01100110 = 4 bits)
        assert_eq!(
            bitcount(&mut ctx, &[bulk("key"), bulk("0"), bulk("0")]).unwrap(),
            RespValue::Integer(4)
        );

        // Count bits in bytes 1-2 (oo = 0x6F6F)
        assert_eq!(
            bitcount(&mut ctx, &[bulk("key"), bulk("1"), bulk("2")]).unwrap(),
            RespValue::Integer(12)
        );
    }

    #[test]
    fn test_bitcount_negative_indices() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("foobar")]).unwrap();

        // Last byte (r = 0x72 = 01110010 = 4 bits)
        assert_eq!(
            bitcount(&mut ctx, &[bulk("key"), bulk("-1"), bulk("-1")]).unwrap(),
            RespValue::Integer(4)
        );
    }

    #[test]
    fn test_bitcount_nonexistent_key() {
        let mut ctx = make_ctx();
        assert_eq!(
            bitcount(&mut ctx, &[bulk("nokey")]).unwrap(),
            RespValue::Integer(0)
        );
    }

    #[test]
    fn test_bitpos_find_one() {
        let mut ctx = make_ctx();

        // Set byte 0xFF (all ones)
        let db = ctx.store().database(ctx.selected_db());
        db.set(
            Bytes::from("key"),
            Entry::new(RedisValue::String(Bytes::from(vec![0xFF, 0x00, 0x00]))),
        );

        assert_eq!(
            bitpos(&mut ctx, &[bulk("key"), bulk("1")]).unwrap(),
            RespValue::Integer(0)
        );
    }

    #[test]
    fn test_bitpos_find_zero() {
        let mut ctx = make_ctx();

        // Set byte 0xFF (all ones)
        let db = ctx.store().database(ctx.selected_db());
        db.set(
            Bytes::from("key"),
            Entry::new(RedisValue::String(Bytes::from(vec![0xFF, 0x00, 0x00]))),
        );

        // First zero is at bit 8
        assert_eq!(
            bitpos(&mut ctx, &[bulk("key"), bulk("0")]).unwrap(),
            RespValue::Integer(8)
        );
    }

    #[test]
    fn test_bitpos_with_range() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        db.set(
            Bytes::from("key"),
            Entry::new(RedisValue::String(Bytes::from(vec![0x00, 0xFF, 0x00]))),
        );

        // Find first 1 in byte 1
        assert_eq!(
            bitpos(&mut ctx, &[bulk("key"), bulk("1"), bulk("1")]).unwrap(),
            RespValue::Integer(8)
        );
    }

    #[test]
    fn test_bitpos_not_found() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        db.set(
            Bytes::from("key"),
            Entry::new(RedisValue::String(Bytes::from(vec![0xFF]))),
        );

        // No zero bits
        assert_eq!(
            bitpos(&mut ctx, &[bulk("key"), bulk("0")]).unwrap(),
            RespValue::Integer(-1)
        );
    }

    #[test]
    fn test_bitpos_empty_string() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key"), bulk("")]).unwrap();

        assert_eq!(
            bitpos(&mut ctx, &[bulk("key"), bulk("0")]).unwrap(),
            RespValue::Integer(0)
        );
        assert_eq!(
            bitpos(&mut ctx, &[bulk("key"), bulk("1")]).unwrap(),
            RespValue::Integer(-1)
        );
    }

    // LCS tests
    #[test]
    fn test_lcs_basic() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key1"), bulk("ohmytext")]).unwrap();
        set(&mut ctx, &[bulk("key2"), bulk("mynewtext")]).unwrap();

        let result = lcs(&mut ctx, &[bulk("key1"), bulk("key2")]).unwrap();
        assert_eq!(result, bulk("mytext"));
    }

    #[test]
    fn test_lcs_len() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key1"), bulk("ohmytext")]).unwrap();
        set(&mut ctx, &[bulk("key2"), bulk("mynewtext")]).unwrap();

        let result = lcs(&mut ctx, &[bulk("key1"), bulk("key2"), bulk("LEN")]).unwrap();
        assert_eq!(result, RespValue::Integer(6));
    }

    #[test]
    fn test_lcs_empty_string() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key1"), bulk("hello")]).unwrap();
        set(&mut ctx, &[bulk("key2"), bulk("world")]).unwrap();

        let result = lcs(&mut ctx, &[bulk("key1"), bulk("key2")]).unwrap();
        // "hello" and "world" share "l" and "o"
        assert!(matches!(result, RespValue::BulkString(_)));
    }

    #[test]
    fn test_lcs_nonexistent_key() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key1"), bulk("hello")]).unwrap();

        let result = lcs(&mut ctx, &[bulk("key1"), bulk("nokey")]).unwrap();
        assert_eq!(result, bulk(""));
    }

    #[test]
    fn test_lcs_identical_strings() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key1"), bulk("hello")]).unwrap();
        set(&mut ctx, &[bulk("key2"), bulk("hello")]).unwrap();

        let result = lcs(&mut ctx, &[bulk("key1"), bulk("key2")]).unwrap();
        assert_eq!(result, bulk("hello"));
    }

    #[test]
    fn test_lcs_no_common() {
        let mut ctx = make_ctx();
        set(&mut ctx, &[bulk("key1"), bulk("abc")]).unwrap();
        set(&mut ctx, &[bulk("key2"), bulk("def")]).unwrap();

        let result = lcs(&mut ctx, &[bulk("key1"), bulk("key2")]).unwrap();
        assert_eq!(result, bulk(""));
    }

    #[test]
    fn test_lcs_wrong_type() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        db.set(
            Bytes::from("list"),
            Entry::new(RedisValue::List(Default::default())),
        );
        set(&mut ctx, &[bulk("key2"), bulk("hello")]).unwrap();

        assert!(lcs(&mut ctx, &[bulk("list"), bulk("key2")]).is_err());
    }
}

/// GETBIT key offset
///
/// Returns the bit value at offset in the string value stored at key.
///
/// Time complexity: O(1)
pub fn getbit(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongArity("GETBIT".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let offset = parse_int(&args[1])?;

    if offset < 0 {
        return Err(CommandError::InvalidArgument(
            "bit offset is not an integer or out of range".to_string(),
        ));
    }

    let db = ctx.store().database(ctx.selected_db());

    match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                return Ok(RespValue::Integer(0));
            }
            match &entry.value {
                RedisValue::String(s) => {
                    let byte_offset = (offset / 8) as usize;
                    let bit_offset = (offset % 8) as u8;

                    if byte_offset >= s.len() {
                        Ok(RespValue::Integer(0))
                    } else {
                        let byte = s[byte_offset];
                        let bit = (byte >> (7 - bit_offset)) & 1;
                        Ok(RespValue::Integer(i64::from(bit)))
                    }
                }
                _ => Err(CommandError::WrongType),
            }
        }
        None => Ok(RespValue::Integer(0)),
    }
}

/// SETBIT key offset value
///
/// Sets or clears the bit at offset in the string value stored at key.
///
/// Time complexity: O(1)
pub fn setbit(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() != 3 {
        return Err(CommandError::WrongArity("SETBIT".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let offset = parse_int(&args[1])?;
    let value = parse_int(&args[2])?;

    if offset < 0 {
        return Err(CommandError::InvalidArgument(
            "bit offset is not an integer or out of range".to_string(),
        ));
    }

    if value != 0 && value != 1 {
        return Err(CommandError::InvalidArgument(
            "bit is not an integer or out of range".to_string(),
        ));
    }

    let db = ctx.store().database(ctx.selected_db());

    let byte_offset = (offset / 8) as usize;
    let bit_offset = (offset % 8) as u8;

    let old_bit = match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                0
            } else {
                match &entry.value {
                    RedisValue::String(s) => {
                        if byte_offset >= s.len() {
                            0
                        } else {
                            let byte = s[byte_offset];
                            (byte >> (7 - bit_offset)) & 1
                        }
                    }
                    _ => return Err(CommandError::WrongType),
                }
            }
        }
        None => 0,
    };

    // Get or create the string
    let mut bytes_vec = match db.get(&key) {
        Some(entry) if !entry.is_expired() => match &entry.value {
            RedisValue::String(s) => s.to_vec(),
            _ => return Err(CommandError::WrongType),
        },
        _ => Vec::new(),
    };

    // Expand if needed
    if byte_offset >= bytes_vec.len() {
        bytes_vec.resize(byte_offset + 1, 0);
    }

    // Set or clear the bit
    if value == 1 {
        bytes_vec[byte_offset] |= 1 << (7 - bit_offset);
    } else {
        bytes_vec[byte_offset] &= !(1 << (7 - bit_offset));
    }

    db.set(
        key.clone(),
        Entry::new(RedisValue::String(Bytes::from(bytes_vec))),
    );

    // Propagate to AOF and replication
    ctx.propagate(&[
        RespValue::BulkString(Bytes::from("SETBIT")),
        RespValue::BulkString(key),
        args[1].clone(),
        args[2].clone(),
    ]);

    Ok(RespValue::Integer(i64::from(old_bit)))
}

/// BITCOUNT key [start end [BYTE | BIT]]
///
/// Count the number of set bits in a string.
///
/// Time complexity: O(N)
pub fn bitcount(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("BITCOUNT".to_string()));
    }

    let key = get_bytes(&args[0])?;

    let db = ctx.store().database(ctx.selected_db());

    match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                return Ok(RespValue::Integer(0));
            }
            match &entry.value {
                RedisValue::String(s) => {
                    // Parse optional range
                    let (start, end, use_bit_index) = if args.len() >= 3 {
                        let start = parse_int(&args[1])?;
                        let end = parse_int(&args[2])?;
                        let use_bit = if args.len() >= 4 {
                            let mode = args[3].as_str().ok_or_else(|| {
                                CommandError::InvalidArgument("invalid index mode".to_string())
                            })?;
                            match mode.to_uppercase().as_str() {
                                "BYTE" => false,
                                "BIT" => true,
                                _ => return Err(CommandError::SyntaxError),
                            }
                        } else {
                            false
                        };
                        (Some(start), Some(end), use_bit)
                    } else if args.len() == 2 {
                        return Err(CommandError::SyntaxError);
                    } else {
                        (None, None, false)
                    };

                    let count = if let (Some(start), Some(end)) = (start, end) {
                        if use_bit_index {
                            // Bit indexing
                            count_bits_in_bit_range(s, start, end)
                        } else {
                            // Byte indexing
                            count_bits_in_byte_range(s, start, end)
                        }
                    } else {
                        // Count all bits
                        s.iter().map(|byte| byte.count_ones() as i64).sum()
                    };

                    Ok(RespValue::Integer(count))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        None => Ok(RespValue::Integer(0)),
    }
}

/// Helper: Count bits in byte range
fn count_bits_in_byte_range(s: &[u8], start: i64, end: i64) -> i64 {
    let len = s.len() as i64;
    let start = if start < 0 { len + start } else { start };
    let end = if end < 0 { len + end } else { end };

    let start = start.max(0) as usize;
    let end = (end + 1).min(len) as usize;

    if start >= end || start >= s.len() {
        return 0;
    }

    s[start..end.min(s.len())]
        .iter()
        .map(|byte| byte.count_ones() as i64)
        .sum()
}

/// Helper: Count bits in bit range
fn count_bits_in_bit_range(s: &[u8], start: i64, end: i64) -> i64 {
    let total_bits = (s.len() * 8) as i64;
    let start = if start < 0 { total_bits + start } else { start };
    let end = if end < 0 { total_bits + end } else { end };

    if start < 0 || start >= total_bits || end < start {
        return 0;
    }

    let end = end.min(total_bits - 1);

    let mut count = 0;
    for bit_pos in start..=end {
        let byte_idx = (bit_pos / 8) as usize;
        let bit_idx = (bit_pos % 8) as u8;
        if byte_idx < s.len() {
            let byte = s[byte_idx];
            if (byte >> (7 - bit_idx)) & 1 == 1 {
                count += 1;
            }
        }
    }
    count
}

/// BITPOS key bit [start [end [BYTE | BIT]]]
///
/// Find first bit set or clear in a string.
///
/// Time complexity: O(N)
pub fn bitpos(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("BITPOS".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let bit = parse_int(&args[1])?;

    if bit != 0 && bit != 1 {
        return Err(CommandError::InvalidArgument(
            "bit should be 0 or 1".to_string(),
        ));
    }

    let db = ctx.store().database(ctx.selected_db());

    match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                return Ok(RespValue::Integer(if bit == 0 { 0 } else { -1 }));
            }
            match &entry.value {
                RedisValue::String(s) => {
                    // Parse optional range
                    let (start, end, use_bit_index) = if args.len() >= 3 {
                        let start = parse_int(&args[2])?;
                        let end = if args.len() >= 4 {
                            Some(parse_int(&args[3])?)
                        } else {
                            None
                        };
                        let use_bit = if args.len() >= 5 {
                            let mode = args[4].as_str().ok_or_else(|| {
                                CommandError::InvalidArgument("invalid index mode".to_string())
                            })?;
                            match mode.to_uppercase().as_str() {
                                "BYTE" => false,
                                "BIT" => true,
                                _ => return Err(CommandError::SyntaxError),
                            }
                        } else {
                            false
                        };
                        (Some(start), end, use_bit)
                    } else {
                        (None, None, false)
                    };

                    let position = if use_bit_index {
                        find_bit_position_bit_range(s, bit == 1, start, end)
                    } else {
                        find_bit_position_byte_range(s, bit == 1, start, end)
                    };

                    Ok(RespValue::Integer(position))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        None => Ok(RespValue::Integer(if bit == 0 { 0 } else { -1 })),
    }
}

/// Helper: Find bit position in byte range
fn find_bit_position_byte_range(
    s: &[u8],
    find_one: bool,
    start: Option<i64>,
    end: Option<i64>,
) -> i64 {
    if s.is_empty() {
        return if find_one { -1 } else { 0 };
    }

    let len = s.len() as i64;
    let start = start.map(|s| if s < 0 { len + s } else { s }).unwrap_or(0);
    let end = end
        .map(|e| if e < 0 { len + e } else { e })
        .unwrap_or(len - 1);

    let start = start.max(0) as usize;
    let end = (end + 1).min(len) as usize;

    if start >= s.len() {
        return if find_one { -1 } else { (s.len() * 8) as i64 };
    }

    for byte_idx in start..end.min(s.len()) {
        let byte = s[byte_idx];
        for bit_idx in 0..8 {
            let bit_set = (byte >> (7 - bit_idx)) & 1 == 1;
            if bit_set == find_one {
                return ((byte_idx * 8) + bit_idx) as i64;
            }
        }
    }

    -1
}

/// Helper: Find bit position in bit range
fn find_bit_position_bit_range(
    s: &[u8],
    find_one: bool,
    start: Option<i64>,
    end: Option<i64>,
) -> i64 {
    if s.is_empty() {
        return if find_one { -1 } else { 0 };
    }

    let total_bits = (s.len() * 8) as i64;
    let start = start
        .map(|s| if s < 0 { total_bits + s } else { s })
        .unwrap_or(0);
    let end = end
        .map(|e| if e < 0 { total_bits + e } else { e })
        .unwrap_or(total_bits - 1);

    if start < 0 || start >= total_bits {
        return -1;
    }

    let end = end.min(total_bits - 1);

    for bit_pos in start..=end {
        let byte_idx = (bit_pos / 8) as usize;
        let bit_idx = (bit_pos % 8) as u8;
        if byte_idx < s.len() {
            let byte = s[byte_idx];
            let bit_set = (byte >> (7 - bit_idx)) & 1 == 1;
            if bit_set == find_one {
                return bit_pos;
            }
        }
    }

    -1
}

/// LCS key1 key2 \[LEN\] \[IDX\] \[MINMATCHLEN len\] \[WITHMATCHLEN\]
///
/// Find the longest common subsequence between two strings.
///
/// Time complexity: O(N*M) where N and M are the lengths of the two strings
pub fn lcs(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("LCS".to_string()));
    }

    let key1 = get_bytes(&args[0])?;
    let key2 = get_bytes(&args[1])?;

    let db = ctx.store().database(ctx.selected_db());

    // Get both strings
    let str1 = match db.get(&key1) {
        Some(entry) if !entry.is_expired() => match &entry.value {
            RedisValue::String(s) => s.clone(),
            _ => return Err(CommandError::WrongType),
        },
        _ => Bytes::new(),
    };

    let str2 = match db.get(&key2) {
        Some(entry) if !entry.is_expired() => match &entry.value {
            RedisValue::String(s) => s.clone(),
            _ => return Err(CommandError::WrongType),
        },
        _ => Bytes::new(),
    };

    // Parse options
    let mut return_len = false;
    let mut return_idx = false;
    let mut min_match_len = 0;
    let mut with_match_len = false;

    let mut i = 2;
    while i < args.len() {
        let opt = args[i]
            .as_str()
            .map(|s| s.to_uppercase())
            .unwrap_or_default();
        match opt.as_str() {
            "LEN" => return_len = true,
            "IDX" => return_idx = true,
            "MINMATCHLEN" => {
                i += 1;
                min_match_len = parse_int(args.get(i).ok_or(CommandError::SyntaxError)?)? as usize;
            }
            "WITHMATCHLEN" => with_match_len = true,
            _ => return Err(CommandError::SyntaxError),
        }
        i += 1;
    }

    // Compute LCS using dynamic programming
    let (lcs_string, matches) = compute_lcs(&str1, &str2, min_match_len);

    if return_len {
        // Just return the length
        Ok(RespValue::Integer(lcs_string.len() as i64))
    } else if return_idx {
        // Return match indices
        let mut result = vec![];

        result.push(RespValue::BulkString(Bytes::from("matches")));
        let mut match_array = vec![];
        for (s1_pos, s2_pos, len) in matches {
            let mut match_info = vec![];
            // [start1, end1]
            match_info.push(RespValue::Array(vec![
                RespValue::Integer(s1_pos as i64),
                RespValue::Integer((s1_pos + len - 1) as i64),
            ]));
            // [start2, end2]
            match_info.push(RespValue::Array(vec![
                RespValue::Integer(s2_pos as i64),
                RespValue::Integer((s2_pos + len - 1) as i64),
            ]));
            if with_match_len {
                match_info.push(RespValue::Integer(len as i64));
            }
            match_array.push(RespValue::Array(match_info));
        }
        result.push(RespValue::Array(match_array));

        result.push(RespValue::BulkString(Bytes::from("len")));
        result.push(RespValue::Integer(lcs_string.len() as i64));

        Ok(RespValue::Array(result))
    } else {
        // Return the LCS string itself
        Ok(RespValue::BulkString(lcs_string))
    }
}

/// Compute LCS using dynamic programming
/// Returns (lcs_string, matches) where matches is a list of (pos1, pos2, len)
fn compute_lcs(s1: &[u8], s2: &[u8], min_match_len: usize) -> (Bytes, Vec<(usize, usize, usize)>) {
    if s1.is_empty() || s2.is_empty() {
        return (Bytes::new(), vec![]);
    }

    let m = s1.len();
    let n = s2.len();

    // DP table: dp[i][j] = length of LCS of s1[0..i] and s2[0..j]
    let mut dp = vec![vec![0; n + 1]; m + 1];

    for i in 1..=m {
        for j in 1..=n {
            if s1[i - 1] == s2[j - 1] {
                dp[i][j] = dp[i - 1][j - 1] + 1;
            } else {
                dp[i][j] = dp[i - 1][j].max(dp[i][j - 1]);
            }
        }
    }

    // Backtrack to find the LCS string
    let mut lcs = Vec::new();
    let mut i = m;
    let mut j = n;
    while i > 0 && j > 0 {
        if s1[i - 1] == s2[j - 1] {
            lcs.push(s1[i - 1]);
            i -= 1;
            j -= 1;
        } else if dp[i - 1][j] > dp[i][j - 1] {
            i -= 1;
        } else {
            j -= 1;
        }
    }
    lcs.reverse();

    // Find all matching blocks
    let mut matches = Vec::new();
    let mut i = 0;
    let mut j = 0;
    while i < m && j < n {
        if s1[i] == s2[j] {
            let start_i = i;
            let start_j = j;
            let mut len = 0;
            while i < m && j < n && s1[i] == s2[j] {
                i += 1;
                j += 1;
                len += 1;
            }
            if len >= min_match_len {
                matches.push((start_i, start_j, len));
            }
        } else {
            if i + 1 < m && s1[i + 1] == s2[j] {
                i += 1;
            } else {
                j += 1;
            }
        }
    }

    (Bytes::from(lcs), matches)
}

/// BITOP operation destkey key [key ...]
///
/// Performs bitwise operations between strings.
/// Supported operations: AND, OR, XOR, NOT
///
/// Time complexity: O(N) where N is the length of the longest string
pub fn bitop(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongArity("BITOP".to_string()));
    }

    let operation = args[0]
        .as_str()
        .ok_or(CommandError::SyntaxError)?
        .to_uppercase();
    let destkey = get_bytes(&args[1])?;

    let db = ctx.store().database(ctx.selected_db());

    match operation.as_str() {
        "AND" | "OR" | "XOR" => {
            if args.len() < 4 {
                return Err(CommandError::WrongArity("BITOP".to_string()));
            }

            // Get all source strings
            let mut strings: Vec<Vec<u8>> = Vec::new();
            let mut max_len = 0;

            for arg in &args[2..] {
                let key = get_bytes(arg)?;
                let bytes = match db.get(&key) {
                    Some(entry) => {
                        if entry.is_expired() {
                            db.delete(&key);
                            Vec::new()
                        } else {
                            match &entry.value {
                                RedisValue::String(s) => s.to_vec(),
                                _ => return Err(CommandError::WrongType),
                            }
                        }
                    }
                    None => Vec::new(),
                };
                max_len = max_len.max(bytes.len());
                strings.push(bytes);
            }

            // Perform bitwise operation
            let mut result = vec![0u8; max_len];
            for i in 0..max_len {
                let mut byte_val = if operation == "AND" { 0xFF } else { 0x00 };

                for string in &strings {
                    let byte = if i < string.len() { string[i] } else { 0 };
                    byte_val = match operation.as_str() {
                        "AND" => byte_val & byte,
                        "OR" => byte_val | byte,
                        "XOR" => byte_val ^ byte,
                        _ => unreachable!(),
                    };
                }
                result[i] = byte_val;
            }

            let result_len = result.len();
            db.set(
                destkey.clone(),
                Entry::new(RedisValue::String(Bytes::from(result))),
            );

            // Propagate to AOF and replication
            ctx.propagate_args(args);

            Ok(RespValue::Integer(result_len as i64))
        }
        "NOT" => {
            if args.len() != 3 {
                return Err(CommandError::WrongArity("BITOP".to_string()));
            }

            let key = get_bytes(&args[2])?;
            let bytes = match db.get(&key) {
                Some(entry) => {
                    if entry.is_expired() {
                        db.delete(&key);
                        Vec::new()
                    } else {
                        match &entry.value {
                            RedisValue::String(s) => s.to_vec(),
                            _ => return Err(CommandError::WrongType),
                        }
                    }
                }
                None => Vec::new(),
            };

            let result: Vec<u8> = bytes.iter().map(|b| !b).collect();
            let result_len = result.len();
            db.set(
                destkey.clone(),
                Entry::new(RedisValue::String(Bytes::from(result))),
            );

            // Propagate to AOF and replication
            ctx.propagate_args(args);

            Ok(RespValue::Integer(result_len as i64))
        }
        _ => Err(CommandError::SyntaxError),
    }
}

/// BITFIELD key [GET type offset] [SET type offset value] [INCRBY type offset increment] [OVERFLOW WRAP|SAT|FAIL]
///
/// Treat a Redis string as an array of bits and perform arbitrary bitfield operations.
/// Supports GET, SET, and INCRBY subcommands with signed/unsigned integers of various sizes.
///
/// Time complexity: O(1) for each operation
pub fn bitfield(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("BITFIELD".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let db_index = ctx.selected_db();
    let db = ctx.store().database(db_index);

    // Get or create the string
    let mut entry = db
        .get(&key)
        .unwrap_or_else(|| Entry::new(RedisValue::String(Bytes::new())));

    let mut bytes = match &entry.value {
        RedisValue::String(s) => s.to_vec(),
        _ => return Err(CommandError::WrongType),
    };

    let mut results = Vec::new();
    let mut overflow_mode = "WRAP"; // Default overflow mode
    let mut modified = false;
    let mut i = 1;

    while i < args.len() {
        let subcommand = args[i]
            .as_str()
            .ok_or_else(|| CommandError::SyntaxError)?
            .to_uppercase();

        match subcommand.as_str() {
            "GET" => {
                if i + 2 >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                let (_type_str, signed, bits) = parse_type(&args[i + 1])?;
                let offset: i64 = args[i + 2]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| CommandError::NotAnInteger)?;

                let value = get_bitfield_value(&bytes, offset, bits, signed);
                results.push(RespValue::Integer(value));
                i += 3;
            }
            "SET" => {
                if i + 3 >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                let (_type_str, signed, bits) = parse_type(&args[i + 1])?;
                let offset: i64 = args[i + 2]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| CommandError::NotAnInteger)?;
                let value: i64 = args[i + 3]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| CommandError::NotAnInteger)?;

                let old_value = get_bitfield_value(&bytes, offset, bits, signed);
                set_bitfield_value(&mut bytes, offset, bits, value, signed);
                results.push(RespValue::Integer(old_value));
                modified = true;
                i += 4;
            }
            "INCRBY" => {
                if i + 3 >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                let (_type_str, signed, bits) = parse_type(&args[i + 1])?;
                let offset: i64 = args[i + 2]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| CommandError::NotAnInteger)?;
                let increment: i64 = args[i + 3]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| CommandError::NotAnInteger)?;

                let old_value = get_bitfield_value(&bytes, offset, bits, signed);
                let new_value = apply_overflow(old_value, increment, bits, signed, overflow_mode)?;

                if let Some(nv) = new_value {
                    set_bitfield_value(&mut bytes, offset, bits, nv, signed);
                    results.push(RespValue::Integer(nv));
                } else {
                    // Overflow with FAIL mode
                    results.push(RespValue::Null);
                }
                modified = true;
                i += 4;
            }
            "OVERFLOW" => {
                if i + 1 >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                overflow_mode = args[i + 1]
                    .as_str()
                    .ok_or_else(|| CommandError::SyntaxError)?
                    .to_uppercase()
                    .leak();
                if overflow_mode != "WRAP" && overflow_mode != "SAT" && overflow_mode != "FAIL" {
                    return Err(CommandError::SyntaxError);
                }
                i += 2;
            }
            _ => return Err(CommandError::SyntaxError),
        }
    }

    // Save if modified
    if modified {
        entry.value = RedisValue::String(Bytes::from(bytes));
        entry.increment_version();
        db.set(key.clone(), entry);

        // Propagate to AOF and replication
        ctx.propagate_args(args);
    }

    Ok(RespValue::Array(results))
}

/// BITFIELD_RO key [GET type offset]...
///
/// Read-only variant of BITFIELD that only supports GET operations.
/// This is useful for read-only replicas and provides better semantics.
///
/// Time complexity: O(n) where n is the number of GET operations
pub fn bitfield_ro(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("BITFIELD_RO".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let db_index = ctx.selected_db();
    let db = ctx.store().database(db_index);

    // Get the string (read-only, don't create if doesn't exist)
    let bytes_vec: Vec<u8> = if let Some(entry) = db.get(&key) {
        if entry.is_expired() {
            Vec::new() // Empty vector for expired keys
        } else {
            match &entry.value {
                RedisValue::String(s) => s.to_vec(),
                _ => return Err(CommandError::WrongType),
            }
        }
    } else {
        Vec::new() // Empty vector for non-existent keys
    };
    let bytes = bytes_vec.as_slice();

    let mut results = Vec::new();
    let mut i = 1;

    while i < args.len() {
        let subcommand = args[i]
            .as_str()
            .ok_or_else(|| CommandError::SyntaxError)?
            .to_uppercase();

        match subcommand.as_str() {
            "GET" => {
                if i + 2 >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                let (_type_str, signed, bits) = parse_type(&args[i + 1])?;
                let offset: i64 = args[i + 2]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| CommandError::NotAnInteger)?;

                let value = get_bitfield_value(bytes, offset, bits, signed);
                results.push(RespValue::Integer(value));
                i += 3;
            }
            // BITFIELD_RO only supports GET, reject all other subcommands
            "SET" | "INCRBY" | "OVERFLOW" => {
                return Err(CommandError::InvalidArgument(
                    "BITFIELD_RO only supports the GET subcommand".to_string(),
                ));
            }
            _ => return Err(CommandError::SyntaxError),
        }
    }

    Ok(RespValue::Array(results))
}

/// Parse type string like "i8", "u16", etc.
fn parse_type(arg: &RespValue) -> Result<(&str, bool, usize), CommandError> {
    let type_str = arg.as_str().ok_or_else(|| CommandError::SyntaxError)?;

    let (signed, rest) = if let Some(r) = type_str.strip_prefix('i') {
        (true, r)
    } else if let Some(r) = type_str.strip_prefix('u') {
        (false, r)
    } else {
        return Err(CommandError::SyntaxError);
    };

    let bits: usize = rest.parse().map_err(|_| CommandError::SyntaxError)?;

    if bits == 0 || bits > 64 {
        return Err(CommandError::InvalidArgument(
            "bitfield type must be between 1 and 64 bits".to_string(),
        ));
    }

    Ok((type_str, signed, bits))
}

/// Get a bitfield value from byte array
fn get_bitfield_value(bytes: &[u8], offset: i64, bits: usize, signed: bool) -> i64 {
    // Validate bits parameter to prevent overflow
    if bits == 0 || bits > 64 {
        return 0;
    }

    let bit_offset = if offset >= 0 {
        offset as usize
    } else {
        // Negative offset means from the end
        return 0; // Simplified - proper implementation would handle this
    };

    let byte_offset = bit_offset / 8;
    let bit_in_byte = bit_offset % 8;

    let mut value: u64 = 0;
    let mut bits_read = 0;

    while bits_read < bits {
        let byte_idx = byte_offset + (bits_read + bit_in_byte) / 8;
        if byte_idx >= bytes.len() {
            break;
        }

        let bit_idx = (bit_in_byte + bits_read) % 8;
        let bits_available = 8 - bit_idx;
        let bits_to_read = bits.min(bits_read + bits_available) - bits_read;

        let byte = bytes[byte_idx];
        let mask = ((1u64 << bits_to_read) - 1) << (8 - bit_idx - bits_to_read);
        let bits_val = ((byte as u64) & mask) >> (8 - bit_idx - bits_to_read);

        value = (value << bits_to_read) | bits_val;
        bits_read += bits_to_read;
    }

    // Sign extend if signed
    if signed && bits < 64 {
        let sign_bit = 1u64 << (bits - 1);
        if value & sign_bit != 0 {
            value |= !((1u64 << bits) - 1);
        }
    }

    value as i64
}

/// Set a bitfield value in byte array
fn set_bitfield_value(bytes: &mut Vec<u8>, offset: i64, bits: usize, value: i64, _signed: bool) {
    // Validate bits parameter to prevent overflow
    if bits == 0 || bits > 64 {
        return;
    }

    let bit_offset = if offset >= 0 {
        offset as usize
    } else {
        return; // Simplified
    };

    let byte_offset = bit_offset / 8;
    let bit_in_byte = bit_offset % 8;

    // Ensure bytes vector is large enough
    let required_bytes = byte_offset + (bit_in_byte + bits + 7) / 8;
    if bytes.len() < required_bytes {
        bytes.resize(required_bytes, 0);
    }

    let mut val = value as u64;
    if bits < 64 {
        val &= (1u64 << bits) - 1;
    }

    let mut bits_written = 0;

    while bits_written < bits {
        let byte_idx = byte_offset + (bits_written + bit_in_byte) / 8;
        let bit_idx = (bit_in_byte + bits_written) % 8;
        let bits_available = 8 - bit_idx;
        let bits_to_write = bits.min(bits_written + bits_available) - bits_written;

        let shift = bits - bits_written - bits_to_write;
        let bits_val = ((val >> shift) & ((1u64 << bits_to_write) - 1)) as u8;

        // Use u32 for mask calculation to avoid u8 shift overflow
        let mask = ((1u32 << bits_to_write) - 1) as u8;
        let shift_amount = 8 - bit_idx - bits_to_write;
        bytes[byte_idx] = (bytes[byte_idx] & !(mask << shift_amount)) | (bits_val << shift_amount);

        bits_written += bits_to_write;
    }
}

/// Apply overflow handling
fn apply_overflow(
    old_value: i64,
    increment: i64,
    bits: usize,
    signed: bool,
    mode: &str,
) -> Result<Option<i64>, CommandError> {
    let result = old_value.wrapping_add(increment);

    match mode {
        "WRAP" => Ok(Some(result)),
        "SAT" => {
            // Saturate at min/max
            let (min, max) = if signed {
                let max = (1i64 << (bits - 1)) - 1;
                let min = -(1i64 << (bits - 1));
                (min, max)
            } else {
                (0, (1i64 << bits) - 1)
            };

            let saturated = result.max(min).min(max);
            Ok(Some(saturated))
        }
        "FAIL" => {
            // Check for overflow
            let (min, max) = if signed {
                let max = (1i64 << (bits - 1)) - 1;
                let min = -(1i64 << (bits - 1));
                (min, max)
            } else {
                (0, (1i64 << bits) - 1)
            };

            if result < min || result > max {
                Ok(None) // Overflow - return null
            } else {
                Ok(Some(result))
            }
        }
        _ => Err(CommandError::SyntaxError),
    }
}

//! List commands: LPUSH, RPUSH, LPOP, RPOP, LRANGE, LINDEX, LSET, LLEN, LREM,
//! LINSERT, LTRIM, LPOS, LMOVE, LMPOP, LPUSHX, RPUSHX

use crate::{CommandContext, CommandError, CommandResult};
use bytes::Bytes;
use ferris_core::{Entry, RedisValue};
use ferris_protocol::RespValue;
use std::collections::VecDeque;

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

/// Normalize a Redis-style index (supporting negative indices) against a list length.
/// Returns the clamped index in range [0, len].
fn normalize_index(index: i64, len: usize) -> i64 {
    if index < 0 {
        let normalized = len as i64 + index;
        if normalized < 0 {
            0
        } else {
            normalized
        }
    } else {
        index
    }
}

/// If the list is empty after mutation, delete the key (Redis behavior).
fn cleanup_empty_list(db: &ferris_core::Database, key: &Bytes, list: &VecDeque<Bytes>) {
    if list.is_empty() {
        db.delete(key);
    }
}

/// Parse a timeout value (float, >= 0). Returns seconds as f64.
fn parse_float_timeout(arg: &RespValue) -> Result<f64, CommandError> {
    let s = arg.as_str().ok_or(CommandError::NotAFloat)?;
    let val: f64 = s.parse().map_err(|_| CommandError::NotAFloat)?;
    if val < 0.0 {
        return Err(CommandError::InvalidArgument(
            "timeout is negative".to_string(),
        ));
    }
    Ok(val)
}

// ---------------------------------------------------------------------------
// LPUSH key element [element ...]
// ---------------------------------------------------------------------------

/// LPUSH key element [element ...]
///
/// Prepend one or multiple elements to the head of a list.
/// Creates the list if it does not exist.
/// Returns the length of the list after the push operations.
///
/// Time complexity: O(1) per element added, O(N) for N elements.
pub fn lpush(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("LPUSH".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let db = ctx.store().database(ctx.selected_db());

    let mut list = match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                VecDeque::new()
            } else {
                match entry.value {
                    RedisValue::List(l) => l,
                    _ => return Err(CommandError::WrongType),
                }
            }
        }
        None => VecDeque::new(),
    };

    for arg in &args[1..] {
        let element = get_bytes(arg)?;
        list.push_front(element);
    }

    let len = list.len() as i64;
    db.set(key.clone(), Entry::new(RedisValue::List(list)));

    // Notify any blocking waiters (BLPOP, BRPOP, etc.)
    ctx.blocking_registry().notify_key(ctx.selected_db(), &key);

    Ok(RespValue::Integer(len))
}

// ---------------------------------------------------------------------------
// RPUSH key element [element ...]
// ---------------------------------------------------------------------------

/// RPUSH key element [element ...]
///
/// Append one or multiple elements to the tail of a list.
/// Creates the list if it does not exist.
/// Returns the length of the list after the push operations.
///
/// Time complexity: O(1) per element added, O(N) for N elements.
pub fn rpush(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("RPUSH".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let db = ctx.store().database(ctx.selected_db());

    let mut list = match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                VecDeque::new()
            } else {
                match entry.value {
                    RedisValue::List(l) => l,
                    _ => return Err(CommandError::WrongType),
                }
            }
        }
        None => VecDeque::new(),
    };

    for arg in &args[1..] {
        let element = get_bytes(arg)?;
        list.push_back(element);
    }

    let len = list.len() as i64;
    db.set(key.clone(), Entry::new(RedisValue::List(list)));

    // Propagate to AOF
    ctx.propagate_to_aof(args.iter().cloned().collect());

    // Notify any blocking waiters (BLPOP, BRPOP, etc.)
    ctx.blocking_registry().notify_key(ctx.selected_db(), &key);

    Ok(RespValue::Integer(len))
}

// ---------------------------------------------------------------------------
// LPUSHX key element [element ...]
// ---------------------------------------------------------------------------

/// LPUSHX key element [element ...]
///
/// Prepend element(s) to a list, only if the list already exists.
/// Returns the length of the list after the push operation, or 0 if key does not exist.
///
/// Time complexity: O(1) per element, O(N) for N elements.
pub fn lpushx(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("LPUSHX".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let db = ctx.store().database(ctx.selected_db());

    let mut list = match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                return Ok(RespValue::Integer(0));
            }
            match entry.value {
                RedisValue::List(l) => l,
                _ => return Err(CommandError::WrongType),
            }
        }
        None => return Ok(RespValue::Integer(0)),
    };

    for arg in &args[1..] {
        let element = get_bytes(arg)?;
        list.push_front(element);
    }

    let len = list.len() as i64;
    db.set(key.clone(), Entry::new(RedisValue::List(list)));

    // Propagate to AOF
    ctx.propagate_to_aof(args.iter().cloned().collect());

    // Notify any blocking waiters (BLPOP, BRPOP, etc.)
    ctx.blocking_registry().notify_key(ctx.selected_db(), &key);

    Ok(RespValue::Integer(len))
}

// ---------------------------------------------------------------------------
// RPUSHX key element [element ...]
// ---------------------------------------------------------------------------

/// RPUSHX key element [element ...]
///
/// Append element(s) to a list, only if the list already exists.
/// Returns the length of the list after the push operation, or 0 if key does not exist.
///
/// Time complexity: O(1) per element, O(N) for N elements.
pub fn rpushx(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("RPUSHX".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let db = ctx.store().database(ctx.selected_db());

    let mut list = match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                return Ok(RespValue::Integer(0));
            }
            match entry.value {
                RedisValue::List(l) => l,
                _ => return Err(CommandError::WrongType),
            }
        }
        None => return Ok(RespValue::Integer(0)),
    };

    for arg in &args[1..] {
        let element = get_bytes(arg)?;
        list.push_back(element);
    }

    let len = list.len() as i64;
    db.set(key.clone(), Entry::new(RedisValue::List(list)));

    // Propagate to AOF
    ctx.propagate_to_aof(args.iter().cloned().collect());

    // Notify any blocking waiters (BLPOP, BRPOP, etc.)
    ctx.blocking_registry().notify_key(ctx.selected_db(), &key);

    Ok(RespValue::Integer(len))
}

// ---------------------------------------------------------------------------
// LPOP key [count]
// ---------------------------------------------------------------------------

/// LPOP key [count]
///
/// Remove and return the first element(s) of the list stored at key.
/// Without count: returns a single bulk string, or Null if key does not exist.
/// With count: returns an array of elements, or Null if key does not exist.
///
/// Time complexity: O(N) where N is the number of elements returned.
pub fn lpop(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("LPOP".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let count = if args.len() > 1 {
        let c = parse_int(&args[1])?;
        if c < 0 {
            return Err(CommandError::NotAnInteger);
        }
        Some(c as usize)
    } else {
        None
    };

    let db = ctx.store().database(ctx.selected_db());

    let mut list = match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                return Ok(if count.is_some() {
                    RespValue::Null
                } else {
                    RespValue::Null
                });
            }
            match entry.value {
                RedisValue::List(l) => l,
                _ => return Err(CommandError::WrongType),
            }
        }
        None => {
            return Ok(if count.is_some() {
                RespValue::Null
            } else {
                RespValue::Null
            });
        }
    };

    match count {
        None => {
            // Single element mode
            let element = list.pop_front();
            cleanup_empty_list(db, &key, &list);
            if !list.is_empty() {
                db.set(key.clone(), Entry::new(RedisValue::List(list)));
            }

            if element.is_some() {
                // Propagate to AOF
                ctx.propagate_to_aof(args.iter().cloned().collect());
            }

            match element {
                Some(e) => Ok(RespValue::BulkString(e)),
                None => Ok(RespValue::Null),
            }
        }
        Some(c) => {
            let actual = c.min(list.len());
            let mut results = Vec::with_capacity(actual);
            for _ in 0..actual {
                if let Some(e) = list.pop_front() {
                    results.push(RespValue::BulkString(e));
                }
            }
            cleanup_empty_list(db, &key, &list);
            if !list.is_empty() {
                db.set(key.clone(), Entry::new(RedisValue::List(list)));
            }

            if !results.is_empty() {
                // Propagate to AOF
                ctx.propagate_to_aof(args.iter().cloned().collect());
            }

            if results.is_empty() {
                Ok(RespValue::Array(vec![]))
            } else {
                Ok(RespValue::Array(results))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// RPOP key [count]
// ---------------------------------------------------------------------------

/// RPOP key [count]
///
/// Remove and return the last element(s) of the list stored at key.
/// Without count: returns a single bulk string, or Null if key does not exist.
/// With count: returns an array of elements, or Null if key does not exist.
///
/// Time complexity: O(N) where N is the number of elements returned.
pub fn rpop(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("RPOP".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let count = if args.len() > 1 {
        let c = parse_int(&args[1])?;
        if c < 0 {
            return Err(CommandError::NotAnInteger);
        }
        Some(c as usize)
    } else {
        None
    };

    let db = ctx.store().database(ctx.selected_db());

    let mut list = match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                return Ok(RespValue::Null);
            }
            match entry.value {
                RedisValue::List(l) => l,
                _ => return Err(CommandError::WrongType),
            }
        }
        None => return Ok(RespValue::Null),
    };

    match count {
        None => {
            let element = list.pop_back();
            cleanup_empty_list(db, &key, &list);
            if !list.is_empty() {
                db.set(key.clone(), Entry::new(RedisValue::List(list)));
            }

            if element.is_some() {
                // Propagate to AOF
                ctx.propagate_to_aof(args.iter().cloned().collect());
            }

            match element {
                Some(e) => Ok(RespValue::BulkString(e)),
                None => Ok(RespValue::Null),
            }
        }
        Some(c) => {
            let actual = c.min(list.len());
            let mut results = Vec::with_capacity(actual);
            for _ in 0..actual {
                if let Some(e) = list.pop_back() {
                    results.push(RespValue::BulkString(e));
                }
            }
            cleanup_empty_list(db, &key, &list);
            if !list.is_empty() {
                db.set(key.clone(), Entry::new(RedisValue::List(list)));
            }

            if !results.is_empty() {
                // Propagate to AOF
                ctx.propagate_to_aof(args.iter().cloned().collect());
            }

            if results.is_empty() {
                Ok(RespValue::Array(vec![]))
            } else {
                Ok(RespValue::Array(results))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// LRANGE key start stop
// ---------------------------------------------------------------------------

/// LRANGE key start stop
///
/// Get a range of elements from a list. Supports negative indices.
/// Out-of-range indices are clamped. Returns an empty array if start > end after normalization.
///
/// Time complexity: O(S+N) where S is the start offset and N is the number of elements.
pub fn lrange(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongArity("LRANGE".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let start = parse_int(&args[1])?;
    let stop = parse_int(&args[2])?;

    let db = ctx.store().database(ctx.selected_db());

    let list = match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                return Ok(RespValue::Array(vec![]));
            }
            match entry.value {
                RedisValue::List(l) => l,
                _ => return Err(CommandError::WrongType),
            }
        }
        None => return Ok(RespValue::Array(vec![])),
    };

    let len = list.len();
    if len == 0 {
        return Ok(RespValue::Array(vec![]));
    }

    let start = normalize_index(start, len);
    let stop = normalize_index(stop, len);

    // Clamp stop to len - 1
    let stop = stop.min(len as i64 - 1);

    if start > stop || start >= len as i64 {
        return Ok(RespValue::Array(vec![]));
    }

    let start = start as usize;
    let stop = stop as usize;

    let results: Vec<RespValue> = list
        .iter()
        .skip(start)
        .take(stop - start + 1)
        .map(|e| RespValue::BulkString(e.clone()))
        .collect();

    Ok(RespValue::Array(results))
}

// ---------------------------------------------------------------------------
// LINDEX key index
// ---------------------------------------------------------------------------

/// LINDEX key index
///
/// Get an element from a list by its index. Supports negative indices.
/// Returns the element at index, or Null if index is out of range.
///
/// Time complexity: O(N) where N is the number of elements to traverse.
pub fn lindex(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("LINDEX".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let index = parse_int(&args[1])?;

    let db = ctx.store().database(ctx.selected_db());

    let list = match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                return Ok(RespValue::Null);
            }
            match entry.value {
                RedisValue::List(l) => l,
                _ => return Err(CommandError::WrongType),
            }
        }
        None => return Ok(RespValue::Null),
    };

    let len = list.len();
    let normalized = if index < 0 {
        let n = len as i64 + index;
        if n < 0 {
            return Ok(RespValue::Null);
        }
        n as usize
    } else {
        index as usize
    };

    match list.get(normalized) {
        Some(e) => Ok(RespValue::BulkString(e.clone())),
        None => Ok(RespValue::Null),
    }
}

// ---------------------------------------------------------------------------
// LSET key index element
// ---------------------------------------------------------------------------

/// LSET key index element
///
/// Set the list element at index to element. Supports negative indices.
/// Returns an error when the index is out of range or the key does not exist.
///
/// Time complexity: O(N) where N is the number of elements to traverse.
pub fn lset(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongArity("LSET".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let index = parse_int(&args[1])?;
    let element = get_bytes(&args[2])?;

    let db = ctx.store().database(ctx.selected_db());

    let mut list = match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                return Err(CommandError::NoSuchKey);
            }
            match entry.value {
                RedisValue::List(l) => l,
                _ => return Err(CommandError::WrongType),
            }
        }
        None => return Err(CommandError::NoSuchKey),
    };

    let len = list.len();
    let normalized = if index < 0 {
        let n = len as i64 + index;
        if n < 0 {
            return Err(CommandError::InvalidArgument(
                "index out of range".to_string(),
            ));
        }
        n as usize
    } else {
        index as usize
    };

    if normalized >= len {
        return Err(CommandError::InvalidArgument(
            "index out of range".to_string(),
        ));
    }

    list[normalized] = element;
    db.set(key.clone(), Entry::new(RedisValue::List(list)));

    // Propagate to AOF
    ctx.propagate_to_aof(args.iter().cloned().collect());

    Ok(RespValue::ok())
}

// ---------------------------------------------------------------------------
// LLEN key
// ---------------------------------------------------------------------------

/// LLEN key
///
/// Returns the length of the list stored at key.
/// Returns 0 if the key does not exist.
///
/// Time complexity: O(1)
pub fn llen(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("LLEN".to_string()));
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
                RedisValue::List(l) => Ok(RespValue::Integer(l.len() as i64)),
                _ => Err(CommandError::WrongType),
            }
        }
        None => Ok(RespValue::Integer(0)),
    }
}

// ---------------------------------------------------------------------------
// LREM key count element
// ---------------------------------------------------------------------------

/// LREM key count element
///
/// Remove the first `count` occurrences of elements equal to `element` from the list.
/// - count > 0: Remove from head to tail.
/// - count < 0: Remove from tail to head.
/// - count = 0: Remove all occurrences.
///
/// Returns the number of removed elements.
///
/// Time complexity: O(N+M) where N is length of list and M is number of elements removed.
pub fn lrem(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongArity("LREM".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let count = parse_int(&args[1])?;
    let element = get_bytes(&args[2])?;

    let db = ctx.store().database(ctx.selected_db());

    let mut list = match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                return Ok(RespValue::Integer(0));
            }
            match entry.value {
                RedisValue::List(l) => l,
                _ => return Err(CommandError::WrongType),
            }
        }
        None => return Ok(RespValue::Integer(0)),
    };

    let mut removed: i64 = 0;

    if count > 0 {
        // Remove from head to tail
        let limit = count as usize;
        let mut new_list = VecDeque::with_capacity(list.len());
        for item in &list {
            if (removed as usize) < limit && *item == element {
                removed += 1;
            } else {
                new_list.push_back(item.clone());
            }
        }
        list = new_list;
    } else if count < 0 {
        // Remove from tail to head
        let limit = (-count) as usize;
        let mut new_list = VecDeque::with_capacity(list.len());
        // Iterate from back
        for item in list.iter().rev() {
            if (removed as usize) < limit && *item == element {
                removed += 1;
            } else {
                new_list.push_front(item.clone());
            }
        }
        list = new_list;
    } else {
        // count == 0: remove all
        let original_len = list.len();
        list.retain(|item| *item != element);
        removed = (original_len - list.len()) as i64;
    }

    cleanup_empty_list(db, &key, &list);
    if !list.is_empty() {
        db.set(key.clone(), Entry::new(RedisValue::List(list)));
    }

    if removed > 0 {
        // Propagate to AOF
        ctx.propagate_to_aof(args.iter().cloned().collect());
    }

    Ok(RespValue::Integer(removed))
}

// ---------------------------------------------------------------------------
// LINSERT key BEFORE|AFTER pivot element
// ---------------------------------------------------------------------------

/// LINSERT key BEFORE|AFTER pivot element
///
/// Insert an element before or after the pivot element in the list.
/// Returns the length of the list after the insert operation,
/// or -1 when the pivot is not found, or 0 if key does not exist.
///
/// Time complexity: O(N) where N is the number of elements to traverse before seeing the pivot.
pub fn linsert(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 4 {
        return Err(CommandError::WrongArity("LINSERT".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let position = args[1]
        .as_str()
        .map(|s| s.to_uppercase())
        .ok_or(CommandError::SyntaxError)?;

    let before = match position.as_str() {
        "BEFORE" => true,
        "AFTER" => false,
        _ => return Err(CommandError::SyntaxError),
    };

    let pivot = get_bytes(&args[2])?;
    let element = get_bytes(&args[3])?;

    let db = ctx.store().database(ctx.selected_db());

    let mut list = match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                return Ok(RespValue::Integer(0));
            }
            match entry.value {
                RedisValue::List(l) => l,
                _ => return Err(CommandError::WrongType),
            }
        }
        None => return Ok(RespValue::Integer(0)),
    };

    // Find the pivot
    let pivot_idx = list.iter().position(|item| *item == pivot);

    match pivot_idx {
        Some(idx) => {
            let insert_idx = if before { idx } else { idx + 1 };
            list.insert(insert_idx, element);
            let len = list.len() as i64;
            db.set(key.clone(), Entry::new(RedisValue::List(list)));

            // Propagate to AOF
            ctx.propagate_to_aof(args.iter().cloned().collect());

            // Notify any blocking waiters (BLPOP, BRPOP, etc.)
            ctx.blocking_registry().notify_key(ctx.selected_db(), &key);

            Ok(RespValue::Integer(len))
        }
        None => Ok(RespValue::Integer(-1)),
    }
}

// ---------------------------------------------------------------------------
// LTRIM key start stop
// ---------------------------------------------------------------------------

/// LTRIM key start stop
///
/// Trim an existing list so that it will contain only the specified range of elements.
/// Supports negative indices. Returns OK.
///
/// Time complexity: O(N) where N is the number of elements to be removed.
pub fn ltrim(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongArity("LTRIM".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let start = parse_int(&args[1])?;
    let stop = parse_int(&args[2])?;

    let db = ctx.store().database(ctx.selected_db());

    let list = match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                return Ok(RespValue::ok());
            }
            match entry.value {
                RedisValue::List(l) => l,
                _ => return Err(CommandError::WrongType),
            }
        }
        None => return Ok(RespValue::ok()),
    };

    let len = list.len();
    if len == 0 {
        return Ok(RespValue::ok());
    }

    let start = normalize_index(start, len);
    let stop = normalize_index(stop, len);
    let stop = stop.min(len as i64 - 1);

    if start > stop || start >= len as i64 {
        // Entire list is removed
        db.delete(&key);
        return Ok(RespValue::ok());
    }

    let start = start as usize;
    let stop = stop as usize;

    let trimmed: VecDeque<Bytes> = list
        .into_iter()
        .skip(start)
        .take(stop - start + 1)
        .collect();

    if trimmed.is_empty() {
        db.delete(&key);
    } else {
        db.set(key.clone(), Entry::new(RedisValue::List(trimmed)));
    }

    // Propagate to AOF
    ctx.propagate_to_aof(args.iter().cloned().collect());

    Ok(RespValue::ok())
}

// ---------------------------------------------------------------------------
// LPOS key element [RANK rank] [COUNT count] [MAXLEN maxlen]
// ---------------------------------------------------------------------------

/// LPOS key element [RANK rank] [COUNT count] [MAXLEN maxlen]
///
/// Return the position(s) of an element in a list.
/// - RANK: start searching from the Nth match (negative means from tail). Default 1.
/// - COUNT: return up to COUNT matches. 0 means all matches.
/// - MAXLEN: limit the comparison to MAXLEN entries.
///
/// Without COUNT: returns a single integer position or Null.
/// With COUNT: returns an array of positions.
///
/// Time complexity: O(N) where N is the number of elements in the list.
pub fn lpos(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("LPOS".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let element = get_bytes(&args[1])?;

    let mut rank: i64 = 1;
    let mut count: Option<usize> = None;
    let mut maxlen: usize = 0; // 0 means no limit

    // Parse optional arguments
    let mut i = 2;
    while i < args.len() {
        let opt = args[i]
            .as_str()
            .map(|s| s.to_uppercase())
            .ok_or(CommandError::SyntaxError)?;

        match opt.as_str() {
            "RANK" => {
                i += 1;
                rank = parse_int(args.get(i).ok_or(CommandError::SyntaxError)?)?;
                if rank == 0 {
                    return Err(CommandError::InvalidArgument(
                        "RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative values meaning from the last match".to_string(),
                    ));
                }
            }
            "COUNT" => {
                i += 1;
                let c = parse_int(args.get(i).ok_or(CommandError::SyntaxError)?)?;
                if c < 0 {
                    return Err(CommandError::NotAnInteger);
                }
                count = Some(c as usize);
            }
            "MAXLEN" => {
                i += 1;
                let m = parse_int(args.get(i).ok_or(CommandError::SyntaxError)?)?;
                if m < 0 {
                    return Err(CommandError::NotAnInteger);
                }
                maxlen = m as usize;
            }
            _ => return Err(CommandError::SyntaxError),
        }
        i += 1;
    }

    let db = ctx.store().database(ctx.selected_db());

    let list = match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                return Ok(if count.is_some() {
                    RespValue::Array(vec![])
                } else {
                    RespValue::Null
                });
            }
            match entry.value {
                RedisValue::List(l) => l,
                _ => return Err(CommandError::WrongType),
            }
        }
        None => {
            return Ok(if count.is_some() {
                RespValue::Array(vec![])
            } else {
                RespValue::Null
            });
        }
    };

    let len = list.len();
    let limit = if count == Some(0) {
        len
    } else {
        count.unwrap_or(1)
    };
    let effective_maxlen = if maxlen == 0 { len } else { maxlen.min(len) };

    let mut matches: Vec<i64> = Vec::new();

    if rank > 0 {
        // Search from head to tail
        let mut skip = rank as usize - 1; // number of matches to skip
        let mut compared = 0;
        for (idx, item) in list.iter().enumerate() {
            if compared >= effective_maxlen {
                break;
            }
            compared += 1;
            if *item == element {
                if skip > 0 {
                    skip -= 1;
                } else {
                    matches.push(idx as i64);
                    if matches.len() >= limit {
                        break;
                    }
                }
            }
        }
    } else {
        // rank < 0: search from tail to head
        let mut skip = (-rank) as usize - 1;
        let mut compared = 0;
        for (idx, item) in list.iter().enumerate().rev() {
            if compared >= effective_maxlen {
                break;
            }
            compared += 1;
            if *item == element {
                if skip > 0 {
                    skip -= 1;
                } else {
                    matches.push(idx as i64);
                    if matches.len() >= limit {
                        break;
                    }
                }
            }
        }
        // When searching from tail, results are in reverse index order; sort ascending
        matches.sort_unstable();
    }

    match count {
        Some(_) => {
            let arr: Vec<RespValue> = matches.into_iter().map(RespValue::Integer).collect();
            Ok(RespValue::Array(arr))
        }
        None => {
            if matches.is_empty() {
                Ok(RespValue::Null)
            } else {
                Ok(RespValue::Integer(matches[0]))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// LMOVE source destination LEFT|RIGHT LEFT|RIGHT
// ---------------------------------------------------------------------------

/// LMOVE source destination LEFT|RIGHT LEFT|RIGHT
///
/// Atomically return and remove the first/last element of the source list,
/// and push it as the first/last element of the destination list.
///
/// Returns the element being popped and pushed, or Null if source is empty.
///
/// Time complexity: O(1)
pub fn lmove(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 4 {
        return Err(CommandError::WrongArity("LMOVE".to_string()));
    }

    let source_key = get_bytes(&args[0])?;
    let dest_key = get_bytes(&args[1])?;
    let wherefrom = args[2]
        .as_str()
        .map(|s| s.to_uppercase())
        .ok_or(CommandError::SyntaxError)?;
    let whereto = args[3]
        .as_str()
        .map(|s| s.to_uppercase())
        .ok_or(CommandError::SyntaxError)?;

    let pop_left = match wherefrom.as_str() {
        "LEFT" => true,
        "RIGHT" => false,
        _ => return Err(CommandError::SyntaxError),
    };

    let push_left = match whereto.as_str() {
        "LEFT" => true,
        "RIGHT" => false,
        _ => return Err(CommandError::SyntaxError),
    };

    let db = ctx.store().database(ctx.selected_db());

    // Get source list
    let mut source_list = match db.get(&source_key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&source_key);
                return Ok(RespValue::Null);
            }
            match entry.value {
                RedisValue::List(l) => l,
                _ => return Err(CommandError::WrongType),
            }
        }
        None => return Ok(RespValue::Null),
    };

    if source_list.is_empty() {
        return Ok(RespValue::Null);
    }

    // Pop element from source
    let element = if pop_left {
        source_list.pop_front()
    } else {
        source_list.pop_back()
    };

    let element = match element {
        Some(e) => e,
        None => return Ok(RespValue::Null),
    };

    // Get destination list (might be the same key as source)
    let mut dest_list = if source_key == dest_key {
        // Same key: use the already-modified source list
        source_list.clone()
    } else {
        match db.get(&dest_key) {
            Some(entry) => {
                if entry.is_expired() {
                    db.delete(&dest_key);
                    VecDeque::new()
                } else {
                    match entry.value {
                        RedisValue::List(l) => l,
                        _ => return Err(CommandError::WrongType),
                    }
                }
            }
            None => VecDeque::new(),
        }
    };

    // Push element to destination
    if push_left {
        dest_list.push_front(element.clone());
    } else {
        dest_list.push_back(element.clone());
    }

    // Save source (or delete if empty)
    if source_key == dest_key {
        // dest_list is the combined result
        db.set(source_key.clone(), Entry::new(RedisValue::List(dest_list)));
    } else {
        if source_list.is_empty() {
            db.delete(&source_key);
        } else {
            db.set(
                source_key.clone(),
                Entry::new(RedisValue::List(source_list)),
            );
        }
        db.set(dest_key.clone(), Entry::new(RedisValue::List(dest_list)));
    }

    // Propagate to AOF
    ctx.propagate_to_aof(args.iter().cloned().collect());

    Ok(RespValue::BulkString(element))
}

// ---------------------------------------------------------------------------
// LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT count]
// ---------------------------------------------------------------------------

/// LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT count]
///
/// Pop one or more elements from the first non-empty list among the given keys.
/// Returns a two-element array: [key, [elements]], or Null if all lists are empty.
///
/// Time complexity: O(N+M) where N is the number of keys and M the number of elements.
pub fn lmpop(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongArity("LMPOP".to_string()));
    }

    let numkeys = parse_int(&args[0])?;
    if numkeys <= 0 {
        return Err(CommandError::InvalidArgument(
            "numkeys can't be non-positive".to_string(),
        ));
    }
    let numkeys = numkeys as usize;

    // We need at least numkeys keys + direction
    if args.len() < 1 + numkeys + 1 {
        return Err(CommandError::WrongArity("LMPOP".to_string()));
    }

    // Collect keys
    let mut keys = Vec::with_capacity(numkeys);
    for i in 1..=numkeys {
        keys.push(get_bytes(&args[i])?);
    }

    let direction_idx = 1 + numkeys;
    let direction = args[direction_idx]
        .as_str()
        .map(|s| s.to_uppercase())
        .ok_or(CommandError::SyntaxError)?;

    let pop_left = match direction.as_str() {
        "LEFT" => true,
        "RIGHT" => false,
        _ => return Err(CommandError::SyntaxError),
    };

    // Parse optional COUNT
    let mut count: usize = 1;
    let mut i = direction_idx + 1;
    while i < args.len() {
        let opt = args[i]
            .as_str()
            .map(|s| s.to_uppercase())
            .ok_or(CommandError::SyntaxError)?;
        match opt.as_str() {
            "COUNT" => {
                i += 1;
                let c = parse_int(args.get(i).ok_or(CommandError::SyntaxError)?)?;
                if c <= 0 {
                    return Err(CommandError::InvalidArgument(
                        "COUNT value must be positive".to_string(),
                    ));
                }
                count = c as usize;
            }
            _ => return Err(CommandError::SyntaxError),
        }
        i += 1;
    }

    let db = ctx.store().database(ctx.selected_db());

    // Find first non-empty list
    for key in &keys {
        let mut list = match db.get(key) {
            Some(entry) => {
                if entry.is_expired() {
                    db.delete(key);
                    continue;
                }
                match entry.value {
                    RedisValue::List(l) if !l.is_empty() => l,
                    RedisValue::List(_) => continue,
                    _ => return Err(CommandError::WrongType),
                }
            }
            None => continue,
        };

        let actual = count.min(list.len());
        let mut elements = Vec::with_capacity(actual);
        for _ in 0..actual {
            let el = if pop_left {
                list.pop_front()
            } else {
                list.pop_back()
            };
            if let Some(e) = el {
                elements.push(RespValue::BulkString(e));
            }
        }

        if list.is_empty() {
            db.delete(key);
        } else {
            db.set(key.clone(), Entry::new(RedisValue::List(list)));
        }

        // Propagate to AOF
        ctx.propagate_to_aof(args.iter().cloned().collect());

        let result = RespValue::Array(vec![
            RespValue::BulkString(key.clone()),
            RespValue::Array(elements),
        ]);

        return Ok(result);
    }

    Ok(RespValue::Null)
}

// ---------------------------------------------------------------------------
// BLPOP key [key ...] timeout
// ---------------------------------------------------------------------------

/// BLPOP key [key ...] timeout
///
/// Blocking version of LPOP. Removes and returns the first element of the
/// first non-empty list among the given keys. If none of the keys contain
/// a non-empty list, blocks the connection until another client pushes to
/// one of the keys, or until the timeout expires.
///
/// Returns a two-element array [key, element] when data is available,
/// or Null on timeout.
///
/// Time complexity: O(N) where N is the number of provided keys.
pub fn blpop(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("BLPOP".to_string()));
    }

    // Last argument is timeout
    let timeout_secs = parse_float_timeout(&args[args.len() - 1])?;
    let keys: Vec<Bytes> = args[..args.len() - 1]
        .iter()
        .map(get_bytes)
        .collect::<Result<_, _>>()?;

    if keys.is_empty() {
        return Err(CommandError::WrongArity("BLPOP".to_string()));
    }

    let db = ctx.store().database(ctx.selected_db());

    // Try to pop from the first non-empty list
    for key in &keys {
        let mut list = match db.get(key) {
            Some(entry) => {
                if entry.is_expired() {
                    db.delete(key);
                    continue;
                }
                match entry.value {
                    RedisValue::List(l) if !l.is_empty() => l,
                    RedisValue::List(_) => continue,
                    _ => return Err(CommandError::WrongType),
                }
            }
            None => continue,
        };

        let element = list.pop_front();
        if let Some(elem) = element {
            if list.is_empty() {
                db.delete(key);
            } else {
                db.set(key.clone(), Entry::new(RedisValue::List(list)));
            }
            return Ok(RespValue::Array(vec![
                RespValue::BulkString(key.clone()),
                RespValue::BulkString(elem),
            ]));
        }
    }

    // No data available — request blocking
    let timeout = if timeout_secs == 0.0 {
        None // Block forever
    } else {
        Some(std::time::Duration::from_secs_f64(timeout_secs))
    };

    Err(CommandError::Block(crate::BlockingAction {
        db_index: ctx.selected_db(),
        keys: keys.clone(),
        timeout,
        command_name: "BLPOP".to_string(),
        args: args.to_vec(),
    }))
}

// ---------------------------------------------------------------------------
// BRPOP key [key ...] timeout
// ---------------------------------------------------------------------------

/// BRPOP key [key ...] timeout
///
/// Blocking version of RPOP. Removes and returns the last element of the
/// first non-empty list among the given keys. If none of the keys contain
/// a non-empty list, blocks the connection until another client pushes to
/// one of the keys, or until the timeout expires.
///
/// Returns a two-element array [key, element] when data is available,
/// or Null on timeout.
///
/// Time complexity: O(N) where N is the number of provided keys.
pub fn brpop(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("BRPOP".to_string()));
    }

    // Last argument is timeout
    let timeout_secs = parse_float_timeout(&args[args.len() - 1])?;
    let keys: Vec<Bytes> = args[..args.len() - 1]
        .iter()
        .map(get_bytes)
        .collect::<Result<_, _>>()?;

    if keys.is_empty() {
        return Err(CommandError::WrongArity("BRPOP".to_string()));
    }

    let db = ctx.store().database(ctx.selected_db());

    // Try to pop from the first non-empty list
    for key in &keys {
        let mut list = match db.get(key) {
            Some(entry) => {
                if entry.is_expired() {
                    db.delete(key);
                    continue;
                }
                match entry.value {
                    RedisValue::List(l) if !l.is_empty() => l,
                    RedisValue::List(_) => continue,
                    _ => return Err(CommandError::WrongType),
                }
            }
            None => continue,
        };

        let element = list.pop_back();
        if let Some(elem) = element {
            if list.is_empty() {
                db.delete(key);
            } else {
                db.set(key.clone(), Entry::new(RedisValue::List(list)));
            }
            return Ok(RespValue::Array(vec![
                RespValue::BulkString(key.clone()),
                RespValue::BulkString(elem),
            ]));
        }
    }

    // No data available — request blocking
    let timeout = if timeout_secs == 0.0 {
        None
    } else {
        Some(std::time::Duration::from_secs_f64(timeout_secs))
    };

    Err(CommandError::Block(crate::BlockingAction {
        db_index: ctx.selected_db(),
        keys: keys.clone(),
        timeout,
        command_name: "BRPOP".to_string(),
        args: args.to_vec(),
    }))
}

// ---------------------------------------------------------------------------
// BLMOVE source destination LEFT|RIGHT LEFT|RIGHT timeout
// ---------------------------------------------------------------------------

/// BLMOVE source destination LEFT|RIGHT LEFT|RIGHT timeout
///
/// Blocking version of LMOVE. Atomically pops an element from the source
/// list and pushes it to the destination. If the source list is empty,
/// blocks until an element becomes available or the timeout expires.
///
/// Time complexity: O(1)
pub fn blmove(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 5 {
        return Err(CommandError::WrongArity("BLMOVE".to_string()));
    }

    let source_key = get_bytes(&args[0])?;
    let dest_key = get_bytes(&args[1])?;
    let wherefrom = args[2]
        .as_str()
        .map(|s| s.to_uppercase())
        .ok_or(CommandError::SyntaxError)?;
    let whereto = args[3]
        .as_str()
        .map(|s| s.to_uppercase())
        .ok_or(CommandError::SyntaxError)?;
    let timeout_secs = parse_float_timeout(&args[4])?;

    // Validate directions
    match wherefrom.as_str() {
        "LEFT" | "RIGHT" => {}
        _ => return Err(CommandError::SyntaxError),
    }
    match whereto.as_str() {
        "LEFT" | "RIGHT" => {}
        _ => return Err(CommandError::SyntaxError),
    }

    let pop_left = wherefrom == "LEFT";
    let push_left = whereto == "LEFT";

    let db = ctx.store().database(ctx.selected_db());

    // Try non-blocking LMOVE first
    let source_list = match db.get(&source_key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&source_key);
                None
            } else {
                match entry.value {
                    RedisValue::List(l) if !l.is_empty() => Some(l),
                    RedisValue::List(_) => None,
                    _ => return Err(CommandError::WrongType),
                }
            }
        }
        None => None,
    };

    if let Some(mut source_list) = source_list {
        // Pop element from source
        let element = if pop_left {
            source_list.pop_front()
        } else {
            source_list.pop_back()
        };

        if let Some(element) = element {
            // Get/create destination list
            let mut dest_list = if source_key == dest_key {
                source_list.clone()
            } else {
                match db.get(&dest_key) {
                    Some(entry) => {
                        if entry.is_expired() {
                            db.delete(&dest_key);
                            VecDeque::new()
                        } else {
                            match entry.value {
                                RedisValue::List(l) => l,
                                _ => return Err(CommandError::WrongType),
                            }
                        }
                    }
                    None => VecDeque::new(),
                }
            };

            // Push element to destination
            if push_left {
                dest_list.push_front(element.clone());
            } else {
                dest_list.push_back(element.clone());
            }

            // Save source list
            if source_list.is_empty() {
                db.delete(&source_key);
            } else {
                db.set(
                    source_key.clone(),
                    Entry::new(RedisValue::List(source_list)),
                );
            }

            // Save destination list
            db.set(dest_key.clone(), Entry::new(RedisValue::List(dest_list)));

            // Notify waiters on destination key
            ctx.blocking_registry()
                .notify_key(ctx.selected_db(), &dest_key);

            return Ok(RespValue::BulkString(element));
        }
    }

    // No data available — request blocking
    let timeout = if timeout_secs == 0.0 {
        None
    } else {
        Some(std::time::Duration::from_secs_f64(timeout_secs))
    };

    Err(CommandError::Block(crate::BlockingAction {
        db_index: ctx.selected_db(),
        keys: vec![source_key],
        timeout,
        command_name: "BLMOVE".to_string(),
        args: args.to_vec(),
    }))
}

// ---------------------------------------------------------------------------
// RPOPLPUSH source destination (deprecated, use LMOVE)
// ---------------------------------------------------------------------------

/// RPOPLPUSH source destination
///
/// Legacy command - atomically returns and removes the last element (tail) of the list
/// stored at source, and pushes the element at the first element (head) of the list
/// stored at destination.
///
/// This command is equivalent to LMOVE source destination RIGHT LEFT.
///
/// Deprecated in Redis 6.2.0, replaced by LMOVE.
///
/// Time complexity: O(1)
pub fn rpoplpush(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongArity("RPOPLPUSH".to_string()));
    }

    // Convert to LMOVE format: source destination RIGHT LEFT
    let lmove_args = vec![
        args[0].clone(),
        args[1].clone(),
        RespValue::bulk_string("RIGHT"),
        RespValue::bulk_string("LEFT"),
    ];

    lmove(ctx, &lmove_args)
}

// ---------------------------------------------------------------------------
// BRPOPLPUSH source destination timeout (deprecated, use BLMOVE)
// ---------------------------------------------------------------------------

/// BRPOPLPUSH source destination timeout
///
/// Legacy command - blocking version of RPOPLPUSH. It is the blocking variant of RPOPLPUSH.
/// When source is empty, Redis will block the connection until another client pushes to it
/// or until timeout is reached.
///
/// This command is equivalent to BLMOVE source destination RIGHT LEFT timeout.
///
/// Deprecated in Redis 6.2.0, replaced by BLMOVE.
///
/// Time complexity: O(1)
pub fn brpoplpush(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() != 3 {
        return Err(CommandError::WrongArity("BRPOPLPUSH".to_string()));
    }

    // Convert to BLMOVE format: source destination RIGHT LEFT timeout
    let blmove_args = vec![
        args[0].clone(),
        args[1].clone(),
        RespValue::bulk_string("RIGHT"),
        RespValue::bulk_string("LEFT"),
        args[2].clone(), // timeout
    ];

    blmove(ctx, &blmove_args)
}

// ---------------------------------------------------------------------------
// BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT count]
// ---------------------------------------------------------------------------

/// BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT count]
///
/// Blocking version of LMPOP. Pops one or more elements from the first
/// non-empty list among the given keys. If all lists are empty,
/// blocks until data becomes available or the timeout expires.
///
/// Time complexity: O(N+M) where N is the number of keys and M the number of elements.
pub fn blmpop(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 4 {
        return Err(CommandError::WrongArity("BLMPOP".to_string()));
    }

    // First argument is timeout
    let timeout_secs = parse_float_timeout(&args[0])?;
    let numkeys = parse_int(&args[1])?;

    if numkeys <= 0 {
        return Err(CommandError::InvalidArgument(
            "numkeys can't be non-positive".to_string(),
        ));
    }
    let numkeys = numkeys as usize;

    // We need at least timeout + numkeys + keys + direction
    if args.len() < 2 + numkeys + 1 {
        return Err(CommandError::SyntaxError);
    }

    // Collect keys
    let keys: Vec<Bytes> = args[2..2 + numkeys]
        .iter()
        .map(get_bytes)
        .collect::<Result<_, _>>()?;

    let direction_idx = 2 + numkeys;
    let direction = args[direction_idx]
        .as_str()
        .map(|s| s.to_uppercase())
        .ok_or(CommandError::SyntaxError)?;

    let pop_left = match direction.as_str() {
        "LEFT" => true,
        "RIGHT" => false,
        _ => return Err(CommandError::SyntaxError),
    };

    // Parse optional COUNT
    let mut count: usize = 1;
    let mut i = direction_idx + 1;
    while i < args.len() {
        let opt = args[i]
            .as_str()
            .map(|s| s.to_uppercase())
            .ok_or(CommandError::SyntaxError)?;
        match opt.as_str() {
            "COUNT" => {
                i += 1;
                let c = parse_int(args.get(i).ok_or(CommandError::SyntaxError)?)?;
                if c <= 0 {
                    return Err(CommandError::InvalidArgument(
                        "COUNT value must be positive".to_string(),
                    ));
                }
                count = c as usize;
            }
            _ => return Err(CommandError::SyntaxError),
        }
        i += 1;
    }

    let db = ctx.store().database(ctx.selected_db());

    // Try to pop from the first non-empty list
    for key in &keys {
        let mut list = match db.get(key) {
            Some(entry) => {
                if entry.is_expired() {
                    db.delete(key);
                    continue;
                }
                match entry.value {
                    RedisValue::List(l) if !l.is_empty() => l,
                    RedisValue::List(_) => continue,
                    _ => return Err(CommandError::WrongType),
                }
            }
            None => continue,
        };

        let actual = count.min(list.len());
        let mut elements = Vec::with_capacity(actual);
        for _ in 0..actual {
            let el = if pop_left {
                list.pop_front()
            } else {
                list.pop_back()
            };
            if let Some(e) = el {
                elements.push(RespValue::BulkString(e));
            }
        }

        if list.is_empty() {
            db.delete(key);
        } else {
            db.set(key.clone(), Entry::new(RedisValue::List(list)));
        }

        return Ok(RespValue::Array(vec![
            RespValue::BulkString(key.clone()),
            RespValue::Array(elements),
        ]));
    }

    // No data available — request blocking
    let timeout = if timeout_secs == 0.0 {
        None // Block forever
    } else {
        Some(std::time::Duration::from_secs_f64(timeout_secs))
    };

    Err(CommandError::Block(crate::BlockingAction {
        db_index: ctx.selected_db(),
        keys,
        timeout,
        command_name: "BLMPOP".to_string(),
        args: args.to_vec(),
    }))
}

// ===========================================================================
// Tests
// ===========================================================================

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

    // Helper: push multiple elements to the right, returning final length
    fn rpush_elements(ctx: &mut CommandContext, key: &str, elements: &[&str]) -> i64 {
        let mut args: Vec<RespValue> = vec![bulk(key)];
        for e in elements {
            args.push(bulk(e));
        }
        match rpush(ctx, &args).unwrap() {
            RespValue::Integer(n) => n,
            other => panic!("Expected Integer, got {other:?}"),
        }
    }

    // ------------------------------------------------------------------
    // 1. test_lpush_rpush_basic
    // ------------------------------------------------------------------
    #[test]
    fn test_lpush_rpush_basic() {
        let mut ctx = make_ctx();

        // RPUSH mylist a b c  -> [a, b, c]
        let result = rpush(&mut ctx, &[bulk("mylist"), bulk("a"), bulk("b"), bulk("c")]).unwrap();
        assert_eq!(result, RespValue::Integer(3));

        // LPUSH mylist z -> [z, a, b, c]
        let result = lpush(&mut ctx, &[bulk("mylist"), bulk("z")]).unwrap();
        assert_eq!(result, RespValue::Integer(4));

        // Verify order via LRANGE
        let result = lrange(&mut ctx, &[bulk("mylist"), bulk("0"), bulk("-1")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 4);
            assert_eq!(arr[0], bulk("z"));
            assert_eq!(arr[1], bulk("a"));
            assert_eq!(arr[2], bulk("b"));
            assert_eq!(arr[3], bulk("c"));
        } else {
            panic!("Expected Array");
        }
    }

    // ------------------------------------------------------------------
    // 2. test_lpush_creates_list
    // ------------------------------------------------------------------
    #[test]
    fn test_lpush_creates_list() {
        let mut ctx = make_ctx();

        // Key does not exist yet
        let result = llen(&mut ctx, &[bulk("newlist")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));

        // LPUSH should create it
        let result = lpush(&mut ctx, &[bulk("newlist"), bulk("hello")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = llen(&mut ctx, &[bulk("newlist")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    // ------------------------------------------------------------------
    // 3. test_rpush_multiple_elements
    // ------------------------------------------------------------------
    #[test]
    fn test_rpush_multiple_elements() {
        let mut ctx = make_ctx();

        // RPUSH key a b c d e
        let result = rpush(
            &mut ctx,
            &[
                bulk("key"),
                bulk("a"),
                bulk("b"),
                bulk("c"),
                bulk("d"),
                bulk("e"),
            ],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(5));

        // Verify order: a b c d e
        let result = lrange(&mut ctx, &[bulk("key"), bulk("0"), bulk("-1")]).unwrap();
        if let RespValue::Array(arr) = result {
            let vals: Vec<&str> = arr.iter().map(|v| v.as_str().unwrap()).collect();
            assert_eq!(vals, vec!["a", "b", "c", "d", "e"]);
        } else {
            panic!("Expected Array");
        }
    }

    // ------------------------------------------------------------------
    // 4. test_lpushx_nonexistent
    // ------------------------------------------------------------------
    #[test]
    fn test_lpushx_nonexistent() {
        let mut ctx = make_ctx();

        // Key doesn't exist, LPUSHX should return 0
        let result = lpushx(&mut ctx, &[bulk("nokey"), bulk("val")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));

        // Key should still not exist
        let result = llen(&mut ctx, &[bulk("nokey")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    // ------------------------------------------------------------------
    // 5. test_rpushx_existing
    // ------------------------------------------------------------------
    #[test]
    fn test_rpushx_existing() {
        let mut ctx = make_ctx();

        // Create the list first
        rpush(&mut ctx, &[bulk("mylist"), bulk("a")]).unwrap();

        // RPUSHX should work
        let result = rpushx(&mut ctx, &[bulk("mylist"), bulk("b"), bulk("c")]).unwrap();
        assert_eq!(result, RespValue::Integer(3));

        // Verify
        let result = lrange(&mut ctx, &[bulk("mylist"), bulk("0"), bulk("-1")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], bulk("a"));
            assert_eq!(arr[1], bulk("b"));
            assert_eq!(arr[2], bulk("c"));
        } else {
            panic!("Expected Array");
        }
    }

    // ------------------------------------------------------------------
    // 6. test_lpop_basic
    // ------------------------------------------------------------------
    #[test]
    fn test_lpop_basic() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "mylist", &["a", "b", "c"]);

        let result = lpop(&mut ctx, &[bulk("mylist")]).unwrap();
        assert_eq!(result, bulk("a"));

        let result = lpop(&mut ctx, &[bulk("mylist")]).unwrap();
        assert_eq!(result, bulk("b"));

        let result = llen(&mut ctx, &[bulk("mylist")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    // ------------------------------------------------------------------
    // 7. test_rpop_basic
    // ------------------------------------------------------------------
    #[test]
    fn test_rpop_basic() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "mylist", &["a", "b", "c"]);

        let result = rpop(&mut ctx, &[bulk("mylist")]).unwrap();
        assert_eq!(result, bulk("c"));

        let result = rpop(&mut ctx, &[bulk("mylist")]).unwrap();
        assert_eq!(result, bulk("b"));
    }

    // ------------------------------------------------------------------
    // 8. test_lpop_with_count
    // ------------------------------------------------------------------
    #[test]
    fn test_lpop_with_count() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "mylist", &["a", "b", "c", "d", "e"]);

        // Pop 3 from the left
        let result = lpop(&mut ctx, &[bulk("mylist"), bulk("3")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], bulk("a"));
            assert_eq!(arr[1], bulk("b"));
            assert_eq!(arr[2], bulk("c"));
        } else {
            panic!("Expected Array");
        }

        // 2 remaining
        let result = llen(&mut ctx, &[bulk("mylist")]).unwrap();
        assert_eq!(result, RespValue::Integer(2));
    }

    // ------------------------------------------------------------------
    // 9. test_rpop_with_count
    // ------------------------------------------------------------------
    #[test]
    fn test_rpop_with_count() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "mylist", &["a", "b", "c", "d", "e"]);

        let result = rpop(&mut ctx, &[bulk("mylist"), bulk("2")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], bulk("e"));
            assert_eq!(arr[1], bulk("d"));
        } else {
            panic!("Expected Array");
        }
    }

    // ------------------------------------------------------------------
    // 10. test_lpop_empty
    // ------------------------------------------------------------------
    #[test]
    fn test_lpop_empty() {
        let mut ctx = make_ctx();

        // Key doesn't exist
        let result = lpop(&mut ctx, &[bulk("nokey")]).unwrap();
        assert_eq!(result, RespValue::Null);

        // With count, still Null for non-existent
        let result = lpop(&mut ctx, &[bulk("nokey"), bulk("5")]).unwrap();
        assert_eq!(result, RespValue::Null);

        // Create and drain the list
        rpush_elements(&mut ctx, "mylist", &["a"]);
        lpop(&mut ctx, &[bulk("mylist")]).unwrap();

        // Now it should be empty (key deleted)
        let result = lpop(&mut ctx, &[bulk("mylist")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    // ------------------------------------------------------------------
    // 11. test_lrange_basic
    // ------------------------------------------------------------------
    #[test]
    fn test_lrange_basic() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "mylist", &["a", "b", "c", "d", "e"]);

        let result = lrange(&mut ctx, &[bulk("mylist"), bulk("1"), bulk("3")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], bulk("b"));
            assert_eq!(arr[1], bulk("c"));
            assert_eq!(arr[2], bulk("d"));
        } else {
            panic!("Expected Array");
        }
    }

    // ------------------------------------------------------------------
    // 12. test_lrange_negative_indices
    // ------------------------------------------------------------------
    #[test]
    fn test_lrange_negative_indices() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "mylist", &["a", "b", "c", "d", "e"]);

        // Last 3 elements
        let result = lrange(&mut ctx, &[bulk("mylist"), bulk("-3"), bulk("-1")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], bulk("c"));
            assert_eq!(arr[1], bulk("d"));
            assert_eq!(arr[2], bulk("e"));
        } else {
            panic!("Expected Array");
        }

        // Entire list
        let result = lrange(&mut ctx, &[bulk("mylist"), bulk("0"), bulk("-1")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 5);
        } else {
            panic!("Expected Array");
        }
    }

    // ------------------------------------------------------------------
    // 13. test_lrange_out_of_range
    // ------------------------------------------------------------------
    #[test]
    fn test_lrange_out_of_range() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "mylist", &["a", "b", "c"]);

        // Start beyond end -> empty
        let result = lrange(&mut ctx, &[bulk("mylist"), bulk("5"), bulk("10")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![]));

        // Stop clamped to end of list
        let result = lrange(&mut ctx, &[bulk("mylist"), bulk("0"), bulk("100")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 3);
        } else {
            panic!("Expected Array");
        }

        // Non-existent key
        let result = lrange(&mut ctx, &[bulk("nokey"), bulk("0"), bulk("-1")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![]));
    }

    // ------------------------------------------------------------------
    // 14. test_lindex_positive
    // ------------------------------------------------------------------
    #[test]
    fn test_lindex_positive() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "mylist", &["a", "b", "c"]);

        assert_eq!(
            lindex(&mut ctx, &[bulk("mylist"), bulk("0")]).unwrap(),
            bulk("a")
        );
        assert_eq!(
            lindex(&mut ctx, &[bulk("mylist"), bulk("1")]).unwrap(),
            bulk("b")
        );
        assert_eq!(
            lindex(&mut ctx, &[bulk("mylist"), bulk("2")]).unwrap(),
            bulk("c")
        );
        // Out of range
        assert_eq!(
            lindex(&mut ctx, &[bulk("mylist"), bulk("3")]).unwrap(),
            RespValue::Null
        );
    }

    // ------------------------------------------------------------------
    // 15. test_lindex_negative
    // ------------------------------------------------------------------
    #[test]
    fn test_lindex_negative() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "mylist", &["a", "b", "c"]);

        assert_eq!(
            lindex(&mut ctx, &[bulk("mylist"), bulk("-1")]).unwrap(),
            bulk("c")
        );
        assert_eq!(
            lindex(&mut ctx, &[bulk("mylist"), bulk("-2")]).unwrap(),
            bulk("b")
        );
        assert_eq!(
            lindex(&mut ctx, &[bulk("mylist"), bulk("-3")]).unwrap(),
            bulk("a")
        );
        // Out of range
        assert_eq!(
            lindex(&mut ctx, &[bulk("mylist"), bulk("-4")]).unwrap(),
            RespValue::Null
        );
    }

    // ------------------------------------------------------------------
    // 16. test_lset_basic
    // ------------------------------------------------------------------
    #[test]
    fn test_lset_basic() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "mylist", &["a", "b", "c"]);

        let result = lset(&mut ctx, &[bulk("mylist"), bulk("1"), bulk("B")]).unwrap();
        assert_eq!(result, RespValue::ok());

        assert_eq!(
            lindex(&mut ctx, &[bulk("mylist"), bulk("1")]).unwrap(),
            bulk("B")
        );

        // Negative index
        let result = lset(&mut ctx, &[bulk("mylist"), bulk("-1"), bulk("C")]).unwrap();
        assert_eq!(result, RespValue::ok());

        assert_eq!(
            lindex(&mut ctx, &[bulk("mylist"), bulk("2")]).unwrap(),
            bulk("C")
        );
    }

    // ------------------------------------------------------------------
    // 17. test_lset_out_of_range
    // ------------------------------------------------------------------
    #[test]
    fn test_lset_out_of_range() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "mylist", &["a", "b", "c"]);

        // Positive out of range
        let result = lset(&mut ctx, &[bulk("mylist"), bulk("5"), bulk("x")]);
        assert!(result.is_err());

        // Negative out of range
        let result = lset(&mut ctx, &[bulk("mylist"), bulk("-10"), bulk("x")]);
        assert!(result.is_err());

        // Non-existent key
        let result = lset(&mut ctx, &[bulk("nokey"), bulk("0"), bulk("x")]);
        assert!(matches!(result, Err(CommandError::NoSuchKey)));
    }

    // ------------------------------------------------------------------
    // 18. test_llen_basic
    // ------------------------------------------------------------------
    #[test]
    fn test_llen_basic() {
        let mut ctx = make_ctx();

        // Non-existent key
        assert_eq!(
            llen(&mut ctx, &[bulk("nokey")]).unwrap(),
            RespValue::Integer(0)
        );

        rpush_elements(&mut ctx, "mylist", &["a", "b", "c"]);
        assert_eq!(
            llen(&mut ctx, &[bulk("mylist")]).unwrap(),
            RespValue::Integer(3)
        );

        lpop(&mut ctx, &[bulk("mylist")]).unwrap();
        assert_eq!(
            llen(&mut ctx, &[bulk("mylist")]).unwrap(),
            RespValue::Integer(2)
        );
    }

    // ------------------------------------------------------------------
    // 19. test_lrem_positive_count
    // ------------------------------------------------------------------
    #[test]
    fn test_lrem_positive_count() {
        let mut ctx = make_ctx();
        // List: [a, b, a, c, a]
        rpush_elements(&mut ctx, "mylist", &["a", "b", "a", "c", "a"]);

        // Remove first 2 occurrences of "a" from head
        let result = lrem(&mut ctx, &[bulk("mylist"), bulk("2"), bulk("a")]).unwrap();
        assert_eq!(result, RespValue::Integer(2));

        // Remaining: [b, c, a]
        let result = lrange(&mut ctx, &[bulk("mylist"), bulk("0"), bulk("-1")]).unwrap();
        if let RespValue::Array(arr) = result {
            let vals: Vec<&str> = arr.iter().map(|v| v.as_str().unwrap()).collect();
            assert_eq!(vals, vec!["b", "c", "a"]);
        } else {
            panic!("Expected Array");
        }
    }

    // ------------------------------------------------------------------
    // 20. test_lrem_negative_count
    // ------------------------------------------------------------------
    #[test]
    fn test_lrem_negative_count() {
        let mut ctx = make_ctx();
        // List: [a, b, a, c, a]
        rpush_elements(&mut ctx, "mylist", &["a", "b", "a", "c", "a"]);

        // Remove last 2 occurrences of "a" (from tail)
        let result = lrem(&mut ctx, &[bulk("mylist"), bulk("-2"), bulk("a")]).unwrap();
        assert_eq!(result, RespValue::Integer(2));

        // Remaining: [a, b, c]
        let result = lrange(&mut ctx, &[bulk("mylist"), bulk("0"), bulk("-1")]).unwrap();
        if let RespValue::Array(arr) = result {
            let vals: Vec<&str> = arr.iter().map(|v| v.as_str().unwrap()).collect();
            assert_eq!(vals, vec!["a", "b", "c"]);
        } else {
            panic!("Expected Array");
        }
    }

    // ------------------------------------------------------------------
    // 21. test_lrem_zero_count
    // ------------------------------------------------------------------
    #[test]
    fn test_lrem_zero_count() {
        let mut ctx = make_ctx();
        // List: [a, b, a, c, a]
        rpush_elements(&mut ctx, "mylist", &["a", "b", "a", "c", "a"]);

        // Remove all "a"
        let result = lrem(&mut ctx, &[bulk("mylist"), bulk("0"), bulk("a")]).unwrap();
        assert_eq!(result, RespValue::Integer(3));

        // Remaining: [b, c]
        let result = lrange(&mut ctx, &[bulk("mylist"), bulk("0"), bulk("-1")]).unwrap();
        if let RespValue::Array(arr) = result {
            let vals: Vec<&str> = arr.iter().map(|v| v.as_str().unwrap()).collect();
            assert_eq!(vals, vec!["b", "c"]);
        } else {
            panic!("Expected Array");
        }
    }

    // ------------------------------------------------------------------
    // 22. test_linsert_before
    // ------------------------------------------------------------------
    #[test]
    fn test_linsert_before() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "mylist", &["a", "c"]);

        // Insert "b" before "c"
        let result = linsert(
            &mut ctx,
            &[bulk("mylist"), bulk("BEFORE"), bulk("c"), bulk("b")],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(3));

        let result = lrange(&mut ctx, &[bulk("mylist"), bulk("0"), bulk("-1")]).unwrap();
        if let RespValue::Array(arr) = result {
            let vals: Vec<&str> = arr.iter().map(|v| v.as_str().unwrap()).collect();
            assert_eq!(vals, vec!["a", "b", "c"]);
        } else {
            panic!("Expected Array");
        }
    }

    // ------------------------------------------------------------------
    // 23. test_linsert_after
    // ------------------------------------------------------------------
    #[test]
    fn test_linsert_after() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "mylist", &["a", "b"]);

        // Insert "c" after "b"
        let result = linsert(
            &mut ctx,
            &[bulk("mylist"), bulk("AFTER"), bulk("b"), bulk("c")],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(3));

        // Pivot not found
        let result = linsert(
            &mut ctx,
            &[bulk("mylist"), bulk("AFTER"), bulk("z"), bulk("x")],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(-1));

        // Non-existent key
        let result = linsert(
            &mut ctx,
            &[bulk("nokey"), bulk("BEFORE"), bulk("a"), bulk("b")],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    // ------------------------------------------------------------------
    // 24. test_ltrim_basic
    // ------------------------------------------------------------------
    #[test]
    fn test_ltrim_basic() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "mylist", &["a", "b", "c", "d", "e"]);

        // Keep only elements 1..3 -> [b, c, d]
        let result = ltrim(&mut ctx, &[bulk("mylist"), bulk("1"), bulk("3")]).unwrap();
        assert_eq!(result, RespValue::ok());

        let result = lrange(&mut ctx, &[bulk("mylist"), bulk("0"), bulk("-1")]).unwrap();
        if let RespValue::Array(arr) = result {
            let vals: Vec<&str> = arr.iter().map(|v| v.as_str().unwrap()).collect();
            assert_eq!(vals, vec!["b", "c", "d"]);
        } else {
            panic!("Expected Array");
        }

        // Trim to empty: start > stop
        let result = ltrim(&mut ctx, &[bulk("mylist"), bulk("5"), bulk("1")]).unwrap();
        assert_eq!(result, RespValue::ok());

        // List should be deleted
        assert_eq!(
            llen(&mut ctx, &[bulk("mylist")]).unwrap(),
            RespValue::Integer(0)
        );
    }

    // ------------------------------------------------------------------
    // 25. test_lpos_basic
    // ------------------------------------------------------------------
    #[test]
    fn test_lpos_basic() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "mylist", &["a", "b", "c", "b", "d", "b"]);

        // Find first occurrence of "b"
        let result = lpos(&mut ctx, &[bulk("mylist"), bulk("b")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // Find all occurrences of "b" with COUNT 0
        let result = lpos(
            &mut ctx,
            &[bulk("mylist"), bulk("b"), bulk("COUNT"), bulk("0")],
        )
        .unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], RespValue::Integer(1));
            assert_eq!(arr[1], RespValue::Integer(3));
            assert_eq!(arr[2], RespValue::Integer(5));
        } else {
            panic!("Expected Array, got {result:?}");
        }

        // Find second occurrence of "b" using RANK 2
        let result = lpos(
            &mut ctx,
            &[bulk("mylist"), bulk("b"), bulk("RANK"), bulk("2")],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(3));

        // Element not found
        let result = lpos(&mut ctx, &[bulk("mylist"), bulk("z")]).unwrap();
        assert_eq!(result, RespValue::Null);

        // With COUNT, not found -> empty array
        let result = lpos(
            &mut ctx,
            &[bulk("mylist"), bulk("z"), bulk("COUNT"), bulk("0")],
        )
        .unwrap();
        assert_eq!(result, RespValue::Array(vec![]));

        // RANK with negative (from tail)
        let result = lpos(
            &mut ctx,
            &[bulk("mylist"), bulk("b"), bulk("RANK"), bulk("-1")],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(5));

        // MAXLEN
        let result = lpos(
            &mut ctx,
            &[
                bulk("mylist"),
                bulk("b"),
                bulk("COUNT"),
                bulk("0"),
                bulk("MAXLEN"),
                bulk("3"),
            ],
        )
        .unwrap();
        if let RespValue::Array(arr) = result {
            // Only first 3 elements scanned, finds "b" at index 1
            assert_eq!(arr.len(), 1);
            assert_eq!(arr[0], RespValue::Integer(1));
        } else {
            panic!("Expected Array");
        }
    }

    // ------------------------------------------------------------------
    // 26. test_lmove_basic
    // ------------------------------------------------------------------
    #[test]
    fn test_lmove_basic() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "src", &["a", "b", "c"]);

        // Move from LEFT of src to RIGHT of dst
        let result = lmove(
            &mut ctx,
            &[bulk("src"), bulk("dst"), bulk("LEFT"), bulk("RIGHT")],
        )
        .unwrap();
        assert_eq!(result, bulk("a"));

        // src should be [b, c]
        let result = lrange(&mut ctx, &[bulk("src"), bulk("0"), bulk("-1")]).unwrap();
        if let RespValue::Array(arr) = result {
            let vals: Vec<&str> = arr.iter().map(|v| v.as_str().unwrap()).collect();
            assert_eq!(vals, vec!["b", "c"]);
        } else {
            panic!("Expected Array");
        }

        // dst should be [a]
        let result = lrange(&mut ctx, &[bulk("dst"), bulk("0"), bulk("-1")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 1);
            assert_eq!(arr[0], bulk("a"));
        } else {
            panic!("Expected Array");
        }

        // Move from RIGHT of src to LEFT of dst
        let result = lmove(
            &mut ctx,
            &[bulk("src"), bulk("dst"), bulk("RIGHT"), bulk("LEFT")],
        )
        .unwrap();
        assert_eq!(result, bulk("c"));

        // dst should be [c, a]
        let result = lrange(&mut ctx, &[bulk("dst"), bulk("0"), bulk("-1")]).unwrap();
        if let RespValue::Array(arr) = result {
            let vals: Vec<&str> = arr.iter().map(|v| v.as_str().unwrap()).collect();
            assert_eq!(vals, vec!["c", "a"]);
        } else {
            panic!("Expected Array");
        }

        // LMOVE from empty / non-existent source -> Null
        let result = lmove(
            &mut ctx,
            &[bulk("nokey"), bulk("dst"), bulk("LEFT"), bulk("RIGHT")],
        )
        .unwrap();
        assert_eq!(result, RespValue::Null);

        // LMOVE with same source and destination (rotate)
        rpush_elements(&mut ctx, "rotate", &["x", "y", "z"]);
        let result = lmove(
            &mut ctx,
            &[bulk("rotate"), bulk("rotate"), bulk("LEFT"), bulk("RIGHT")],
        )
        .unwrap();
        assert_eq!(result, bulk("x"));
        // rotate should now be [y, z, x]
        let result = lrange(&mut ctx, &[bulk("rotate"), bulk("0"), bulk("-1")]).unwrap();
        if let RespValue::Array(arr) = result {
            let vals: Vec<&str> = arr.iter().map(|v| v.as_str().unwrap()).collect();
            assert_eq!(vals, vec!["y", "z", "x"]);
        } else {
            panic!("Expected Array");
        }
    }

    // ------------------------------------------------------------------
    // 27. test_wrong_type_error
    // ------------------------------------------------------------------
    #[test]
    fn test_wrong_type_error() {
        let mut ctx = make_ctx();

        // Create a string key
        let db = ctx.store().database(ctx.selected_db());
        db.set(
            Bytes::from("strkey"),
            Entry::new(RedisValue::String(Bytes::from("hello"))),
        );

        // All list commands should return WrongType
        assert!(matches!(
            lpush(&mut ctx, &[bulk("strkey"), bulk("a")]),
            Err(CommandError::WrongType)
        ));
        assert!(matches!(
            rpush(&mut ctx, &[bulk("strkey"), bulk("a")]),
            Err(CommandError::WrongType)
        ));
        assert!(matches!(
            lpushx(&mut ctx, &[bulk("strkey"), bulk("a")]),
            Err(CommandError::WrongType)
        ));
        assert!(matches!(
            rpushx(&mut ctx, &[bulk("strkey"), bulk("a")]),
            Err(CommandError::WrongType)
        ));
        assert!(matches!(
            lpop(&mut ctx, &[bulk("strkey")]),
            Err(CommandError::WrongType)
        ));
        assert!(matches!(
            rpop(&mut ctx, &[bulk("strkey")]),
            Err(CommandError::WrongType)
        ));
        assert!(matches!(
            lrange(&mut ctx, &[bulk("strkey"), bulk("0"), bulk("-1")]),
            Err(CommandError::WrongType)
        ));
        assert!(matches!(
            lindex(&mut ctx, &[bulk("strkey"), bulk("0")]),
            Err(CommandError::WrongType)
        ));
        assert!(matches!(
            lset(&mut ctx, &[bulk("strkey"), bulk("0"), bulk("a")]),
            Err(CommandError::WrongType)
        ));
        assert!(matches!(
            llen(&mut ctx, &[bulk("strkey")]),
            Err(CommandError::WrongType)
        ));
        assert!(matches!(
            lrem(&mut ctx, &[bulk("strkey"), bulk("0"), bulk("a")]),
            Err(CommandError::WrongType)
        ));
        assert!(matches!(
            linsert(
                &mut ctx,
                &[bulk("strkey"), bulk("BEFORE"), bulk("a"), bulk("b")]
            ),
            Err(CommandError::WrongType)
        ));
        assert!(matches!(
            ltrim(&mut ctx, &[bulk("strkey"), bulk("0"), bulk("-1")]),
            Err(CommandError::WrongType)
        ));
        assert!(matches!(
            lpos(&mut ctx, &[bulk("strkey"), bulk("a")]),
            Err(CommandError::WrongType)
        ));
        assert!(matches!(
            lmove(
                &mut ctx,
                &[bulk("strkey"), bulk("dst"), bulk("LEFT"), bulk("RIGHT")]
            ),
            Err(CommandError::WrongType)
        ));
    }

    // ------------------------------------------------------------------
    // Additional edge case tests
    // ------------------------------------------------------------------

    #[test]
    fn test_lpush_multiple_order() {
        let mut ctx = make_ctx();
        // LPUSH pushes elements one by one to the front:
        // LPUSH key a b c -> c is pushed last, so list is [c, b, a]
        lpush(&mut ctx, &[bulk("key"), bulk("a"), bulk("b"), bulk("c")]).unwrap();
        let result = lrange(&mut ctx, &[bulk("key"), bulk("0"), bulk("-1")]).unwrap();
        if let RespValue::Array(arr) = result {
            let vals: Vec<&str> = arr.iter().map(|v| v.as_str().unwrap()).collect();
            assert_eq!(vals, vec!["c", "b", "a"]);
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_lrem_removes_all_elements_cleans_up() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "mylist", &["a", "a", "a"]);

        let result = lrem(&mut ctx, &[bulk("mylist"), bulk("0"), bulk("a")]).unwrap();
        assert_eq!(result, RespValue::Integer(3));

        // Key should be deleted since list is empty
        assert_eq!(
            llen(&mut ctx, &[bulk("mylist")]).unwrap(),
            RespValue::Integer(0)
        );
    }

    #[test]
    fn test_lpop_count_exceeds_length() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "mylist", &["a", "b"]);

        // Pop 10, but only 2 exist
        let result = lpop(&mut ctx, &[bulk("mylist"), bulk("10")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], bulk("a"));
            assert_eq!(arr[1], bulk("b"));
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_ltrim_negative_indices() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "mylist", &["a", "b", "c", "d", "e"]);

        // Keep last 3
        ltrim(&mut ctx, &[bulk("mylist"), bulk("-3"), bulk("-1")]).unwrap();

        let result = lrange(&mut ctx, &[bulk("mylist"), bulk("0"), bulk("-1")]).unwrap();
        if let RespValue::Array(arr) = result {
            let vals: Vec<&str> = arr.iter().map(|v| v.as_str().unwrap()).collect();
            assert_eq!(vals, vec!["c", "d", "e"]);
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_lmpop_basic() {
        let mut ctx = make_ctx();
        // Create two lists
        rpush_elements(&mut ctx, "list1", &["a", "b", "c"]);
        rpush_elements(&mut ctx, "list2", &["x", "y", "z"]);

        // Pop from left of first non-empty
        let result = lmpop(
            &mut ctx,
            &[
                bulk("2"),
                bulk("list1"),
                bulk("list2"),
                bulk("LEFT"),
                bulk("COUNT"),
                bulk("2"),
            ],
        )
        .unwrap();

        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 2);
            // First element is the key name
            assert_eq!(arr[0], bulk("list1"));
            // Second element is the array of popped elements
            if let RespValue::Array(elements) = &arr[1] {
                assert_eq!(elements.len(), 2);
                assert_eq!(elements[0], bulk("a"));
                assert_eq!(elements[1], bulk("b"));
            } else {
                panic!("Expected inner Array");
            }
        } else {
            panic!("Expected Array, got {result:?}");
        }

        // list1 should now have [c]
        assert_eq!(
            llen(&mut ctx, &[bulk("list1")]).unwrap(),
            RespValue::Integer(1)
        );
    }

    #[test]
    fn test_lmpop_all_empty() {
        let mut ctx = make_ctx();

        // No lists exist
        let result = lmpop(
            &mut ctx,
            &[bulk("2"), bulk("nokey1"), bulk("nokey2"), bulk("LEFT")],
        )
        .unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_lmpop_skips_empty_lists() {
        let mut ctx = make_ctx();
        // list1 is empty (doesn't exist), list2 has elements
        rpush_elements(&mut ctx, "list2", &["x", "y"]);

        let result = lmpop(
            &mut ctx,
            &[bulk("2"), bulk("list1"), bulk("list2"), bulk("RIGHT")],
        )
        .unwrap();

        if let RespValue::Array(arr) = result {
            assert_eq!(arr[0], bulk("list2"));
            if let RespValue::Array(elements) = &arr[1] {
                assert_eq!(elements.len(), 1);
                assert_eq!(elements[0], bulk("y"));
            } else {
                panic!("Expected inner Array");
            }
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_lpos_rank_zero_error() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "mylist", &["a", "b", "c"]);

        let result = lpos(
            &mut ctx,
            &[bulk("mylist"), bulk("a"), bulk("RANK"), bulk("0")],
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_wrong_arity_errors() {
        let mut ctx = make_ctx();

        // Not enough arguments
        assert!(matches!(
            lpush(&mut ctx, &[bulk("key")]),
            Err(CommandError::WrongArity(_))
        ));
        assert!(matches!(
            rpush(&mut ctx, &[bulk("key")]),
            Err(CommandError::WrongArity(_))
        ));
        assert!(matches!(
            lpushx(&mut ctx, &[bulk("key")]),
            Err(CommandError::WrongArity(_))
        ));
        assert!(matches!(
            rpushx(&mut ctx, &[bulk("key")]),
            Err(CommandError::WrongArity(_))
        ));
        assert!(matches!(
            lpop(&mut ctx, &[]),
            Err(CommandError::WrongArity(_))
        ));
        assert!(matches!(
            rpop(&mut ctx, &[]),
            Err(CommandError::WrongArity(_))
        ));
        assert!(matches!(
            lrange(&mut ctx, &[bulk("key"), bulk("0")]),
            Err(CommandError::WrongArity(_))
        ));
        assert!(matches!(
            lindex(&mut ctx, &[bulk("key")]),
            Err(CommandError::WrongArity(_))
        ));
        assert!(matches!(
            lset(&mut ctx, &[bulk("key"), bulk("0")]),
            Err(CommandError::WrongArity(_))
        ));
        assert!(matches!(
            llen(&mut ctx, &[]),
            Err(CommandError::WrongArity(_))
        ));
        assert!(matches!(
            lrem(&mut ctx, &[bulk("key"), bulk("0")]),
            Err(CommandError::WrongArity(_))
        ));
        assert!(matches!(
            linsert(&mut ctx, &[bulk("key"), bulk("BEFORE"), bulk("a")]),
            Err(CommandError::WrongArity(_))
        ));
        assert!(matches!(
            ltrim(&mut ctx, &[bulk("key"), bulk("0")]),
            Err(CommandError::WrongArity(_))
        ));
        assert!(matches!(
            lpos(&mut ctx, &[bulk("key")]),
            Err(CommandError::WrongArity(_))
        ));
        assert!(matches!(
            lmove(&mut ctx, &[bulk("src"), bulk("dst"), bulk("LEFT")]),
            Err(CommandError::WrongArity(_))
        ));
        assert!(matches!(
            lmpop(&mut ctx, &[bulk("1"), bulk("key")]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_lmove_source_emptied_is_deleted() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "src", &["only"]);

        lmove(
            &mut ctx,
            &[bulk("src"), bulk("dst"), bulk("LEFT"), bulk("RIGHT")],
        )
        .unwrap();

        // src should be deleted
        assert_eq!(
            llen(&mut ctx, &[bulk("src")]).unwrap(),
            RespValue::Integer(0)
        );
        // dst should have the element
        assert_eq!(
            llen(&mut ctx, &[bulk("dst")]).unwrap(),
            RespValue::Integer(1)
        );
    }

    #[test]
    fn test_rpop_count_zero() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "mylist", &["a", "b", "c"]);

        // COUNT 0 returns empty array
        let result = rpop(&mut ctx, &[bulk("mylist"), bulk("0")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![]));

        // List should be unchanged
        assert_eq!(
            llen(&mut ctx, &[bulk("mylist")]).unwrap(),
            RespValue::Integer(3)
        );
    }

    // ------------------------------------------------------------------
    // LPOS combination tests
    // ------------------------------------------------------------------

    #[test]
    fn test_lpos_rank_and_count() {
        let mut ctx = make_ctx();
        // List: [a, b, a, b, a, b]
        rpush_elements(&mut ctx, "mylist", &["a", "b", "a", "b", "a", "b"]);

        // LPOS mylist "a" RANK 2 COUNT 2
        // Skip first match of "a" (index 0), return next 2 matches (index 2, 4)
        let result = lpos(
            &mut ctx,
            &[
                bulk("mylist"),
                bulk("a"),
                bulk("RANK"),
                bulk("2"),
                bulk("COUNT"),
                bulk("2"),
            ],
        )
        .unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], RespValue::Integer(2));
            assert_eq!(arr[1], RespValue::Integer(4));
        } else {
            panic!("Expected Array, got {result:?}");
        }
    }

    #[test]
    fn test_lpos_negative_rank_count() {
        let mut ctx = make_ctx();
        // List: [a, b, a, b, a, b]
        rpush_elements(&mut ctx, "mylist", &["a", "b", "a", "b", "a", "b"]);

        // LPOS mylist "a" RANK -1 COUNT 2
        // From tail: first match is index 4, second match is index 2
        let result = lpos(
            &mut ctx,
            &[
                bulk("mylist"),
                bulk("a"),
                bulk("RANK"),
                bulk("-1"),
                bulk("COUNT"),
                bulk("2"),
            ],
        )
        .unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 2);
            // Results sorted ascending
            assert_eq!(arr[0], RespValue::Integer(2));
            assert_eq!(arr[1], RespValue::Integer(4));
        } else {
            panic!("Expected Array, got {result:?}");
        }
    }

    #[test]
    fn test_lpos_maxlen_and_rank() {
        let mut ctx = make_ctx();
        // List: [x, a, x, a, x, a, x, a]
        rpush_elements(
            &mut ctx,
            "mylist",
            &["x", "a", "x", "a", "x", "a", "x", "a"],
        );

        // LPOS mylist "a" MAXLEN 5 RANK 1
        // Only scan first 5 elements: [x, a, x, a, x]
        // Matches of "a" within first 5: index 1, index 3
        // RANK 1 means return first match
        let result = lpos(
            &mut ctx,
            &[
                bulk("mylist"),
                bulk("a"),
                bulk("MAXLEN"),
                bulk("5"),
                bulk("RANK"),
                bulk("1"),
            ],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_lpos_count_zero() {
        let mut ctx = make_ctx();
        // List: [a, b, a, c, a, d, a]
        rpush_elements(&mut ctx, "mylist", &["a", "b", "a", "c", "a", "d", "a"]);

        // LPOS mylist "a" COUNT 0 -> returns ALL matches
        let result = lpos(
            &mut ctx,
            &[bulk("mylist"), bulk("a"), bulk("COUNT"), bulk("0")],
        )
        .unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 4);
            assert_eq!(arr[0], RespValue::Integer(0));
            assert_eq!(arr[1], RespValue::Integer(2));
            assert_eq!(arr[2], RespValue::Integer(4));
            assert_eq!(arr[3], RespValue::Integer(6));
        } else {
            panic!("Expected Array, got {result:?}");
        }
    }

    #[test]
    fn test_lpos_nonexistent_key() {
        let mut ctx = make_ctx();

        // Without COUNT: returns Null
        let result = lpos(&mut ctx, &[bulk("nokey"), bulk("a")]).unwrap();
        assert_eq!(result, RespValue::Null);

        // With COUNT 0: returns empty array
        let result = lpos(
            &mut ctx,
            &[bulk("nokey"), bulk("a"), bulk("COUNT"), bulk("0")],
        )
        .unwrap();
        assert_eq!(result, RespValue::Array(vec![]));
    }

    #[test]
    fn test_lpos_negative_count_error() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "mylist", &["a", "b", "c"]);

        // LPOS mylist "a" COUNT -1 -> error
        let result = lpos(
            &mut ctx,
            &[bulk("mylist"), bulk("a"), bulk("COUNT"), bulk("-1")],
        );
        assert!(result.is_err());
    }

    // ------------------------------------------------------------------
    // LMPOP additional tests
    // ------------------------------------------------------------------

    #[test]
    fn test_lmpop_right_direction() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "mylist", &["a", "b", "c", "d"]);

        // LMPOP 1 mylist RIGHT COUNT 2
        let result = lmpop(
            &mut ctx,
            &[
                bulk("1"),
                bulk("mylist"),
                bulk("RIGHT"),
                bulk("COUNT"),
                bulk("2"),
            ],
        )
        .unwrap();

        if let RespValue::Array(arr) = result {
            assert_eq!(arr[0], bulk("mylist"));
            if let RespValue::Array(elements) = &arr[1] {
                assert_eq!(elements.len(), 2);
                assert_eq!(elements[0], bulk("d"));
                assert_eq!(elements[1], bulk("c"));
            } else {
                panic!("Expected inner Array");
            }
        } else {
            panic!("Expected Array, got {result:?}");
        }

        // Remaining: [a, b]
        let result = lrange(&mut ctx, &[bulk("mylist"), bulk("0"), bulk("-1")]).unwrap();
        if let RespValue::Array(arr) = result {
            let vals: Vec<&str> = arr.iter().map(|v| v.as_str().unwrap()).collect();
            assert_eq!(vals, vec!["a", "b"]);
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_lmpop_count_exceeds_length() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "mylist", &["a", "b"]);

        // LMPOP 1 mylist LEFT COUNT 100
        let result = lmpop(
            &mut ctx,
            &[
                bulk("1"),
                bulk("mylist"),
                bulk("LEFT"),
                bulk("COUNT"),
                bulk("100"),
            ],
        )
        .unwrap();

        if let RespValue::Array(arr) = result {
            assert_eq!(arr[0], bulk("mylist"));
            if let RespValue::Array(elements) = &arr[1] {
                assert_eq!(elements.len(), 2);
                assert_eq!(elements[0], bulk("a"));
                assert_eq!(elements[1], bulk("b"));
            } else {
                panic!("Expected inner Array");
            }
        } else {
            panic!("Expected Array, got {result:?}");
        }

        // List should be deleted (empty)
        assert_eq!(
            llen(&mut ctx, &[bulk("mylist")]).unwrap(),
            RespValue::Integer(0)
        );
    }

    #[test]
    fn test_lmpop_count_default() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "mylist", &["a", "b", "c"]);

        // LMPOP 1 mylist LEFT (no COUNT -> defaults to 1)
        let result = lmpop(&mut ctx, &[bulk("1"), bulk("mylist"), bulk("LEFT")]).unwrap();

        if let RespValue::Array(arr) = result {
            assert_eq!(arr[0], bulk("mylist"));
            if let RespValue::Array(elements) = &arr[1] {
                assert_eq!(elements.len(), 1);
                assert_eq!(elements[0], bulk("a"));
            } else {
                panic!("Expected inner Array");
            }
        } else {
            panic!("Expected Array, got {result:?}");
        }

        // Remaining: [b, c]
        assert_eq!(
            llen(&mut ctx, &[bulk("mylist")]).unwrap(),
            RespValue::Integer(2)
        );
    }

    #[test]
    fn test_lmpop_wrong_type_in_keys() {
        let mut ctx = make_ctx();

        // Create a string key
        let db = ctx.store().database(ctx.selected_db());
        db.set(
            Bytes::from("strkey"),
            Entry::new(RedisValue::String(Bytes::from("hello"))),
        );

        // LMPOP 1 strkey LEFT -> WRONGTYPE
        let result = lmpop(&mut ctx, &[bulk("1"), bulk("strkey"), bulk("LEFT")]);
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    // ------------------------------------------------------------------
    // LMOVE direction combinations
    // ------------------------------------------------------------------

    #[test]
    fn test_lmove_left_left() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "src", &["a", "b", "c"]);
        rpush_elements(&mut ctx, "dst", &["x", "y"]);

        // LMOVE src dst LEFT LEFT
        // Pop "a" from left of src, push to left of dst
        let result = lmove(
            &mut ctx,
            &[bulk("src"), bulk("dst"), bulk("LEFT"), bulk("LEFT")],
        )
        .unwrap();
        assert_eq!(result, bulk("a"));

        // src: [b, c]
        let result = lrange(&mut ctx, &[bulk("src"), bulk("0"), bulk("-1")]).unwrap();
        if let RespValue::Array(arr) = result {
            let vals: Vec<&str> = arr.iter().map(|v| v.as_str().unwrap()).collect();
            assert_eq!(vals, vec!["b", "c"]);
        } else {
            panic!("Expected Array");
        }

        // dst: [a, x, y]
        let result = lrange(&mut ctx, &[bulk("dst"), bulk("0"), bulk("-1")]).unwrap();
        if let RespValue::Array(arr) = result {
            let vals: Vec<&str> = arr.iter().map(|v| v.as_str().unwrap()).collect();
            assert_eq!(vals, vec!["a", "x", "y"]);
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_lmove_right_right() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "src", &["a", "b", "c"]);
        rpush_elements(&mut ctx, "dst", &["x", "y"]);

        // LMOVE src dst RIGHT RIGHT
        // Pop "c" from right of src, push to right of dst
        let result = lmove(
            &mut ctx,
            &[bulk("src"), bulk("dst"), bulk("RIGHT"), bulk("RIGHT")],
        )
        .unwrap();
        assert_eq!(result, bulk("c"));

        // src: [a, b]
        let result = lrange(&mut ctx, &[bulk("src"), bulk("0"), bulk("-1")]).unwrap();
        if let RespValue::Array(arr) = result {
            let vals: Vec<&str> = arr.iter().map(|v| v.as_str().unwrap()).collect();
            assert_eq!(vals, vec!["a", "b"]);
        } else {
            panic!("Expected Array");
        }

        // dst: [x, y, c]
        let result = lrange(&mut ctx, &[bulk("dst"), bulk("0"), bulk("-1")]).unwrap();
        if let RespValue::Array(arr) = result {
            let vals: Vec<&str> = arr.iter().map(|v| v.as_str().unwrap()).collect();
            assert_eq!(vals, vec!["x", "y", "c"]);
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_lmove_dest_wrong_type() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "src", &["a", "b", "c"]);

        // Create a string key as destination
        let db = ctx.store().database(ctx.selected_db());
        db.set(
            Bytes::from("strkey"),
            Entry::new(RedisValue::String(Bytes::from("hello"))),
        );

        // LMOVE src strkey LEFT RIGHT -> WRONGTYPE
        let result = lmove(
            &mut ctx,
            &[bulk("src"), bulk("strkey"), bulk("LEFT"), bulk("RIGHT")],
        );
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    // ------------------------------------------------------------------
    // RPOP edge cases
    // ------------------------------------------------------------------

    #[test]
    fn test_rpop_negative_count_error() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "mylist", &["a", "b", "c"]);

        // RPOP mylist -1 -> error
        let result = rpop(&mut ctx, &[bulk("mylist"), bulk("-1")]);
        assert!(result.is_err());
    }

    #[test]
    fn test_rpop_count_exceeds_length() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "mylist", &["a", "b", "c"]);

        // RPOP mylist 100 -> returns all 3 elements (from tail)
        let result = rpop(&mut ctx, &[bulk("mylist"), bulk("100")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], bulk("c"));
            assert_eq!(arr[1], bulk("b"));
            assert_eq!(arr[2], bulk("a"));
        } else {
            panic!("Expected Array, got {result:?}");
        }
    }

    #[test]
    fn test_rpop_removes_empty_list() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "mylist", &["only"]);

        // RPOP the single element
        let result = rpop(&mut ctx, &[bulk("mylist")]).unwrap();
        assert_eq!(result, bulk("only"));

        // Key should be deleted (list empty)
        assert_eq!(
            llen(&mut ctx, &[bulk("mylist")]).unwrap(),
            RespValue::Integer(0)
        );

        // Confirm key is truly gone by trying RPOP again
        let result = rpop(&mut ctx, &[bulk("mylist")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    // ------------------------------------------------------------------
    // LPOP edge cases
    // ------------------------------------------------------------------

    #[test]
    fn test_lpop_count_zero() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "mylist", &["a", "b", "c"]);

        // LPOP mylist 0 -> empty array
        let result = lpop(&mut ctx, &[bulk("mylist"), bulk("0")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![]));

        // List should be unchanged
        assert_eq!(
            llen(&mut ctx, &[bulk("mylist")]).unwrap(),
            RespValue::Integer(3)
        );
    }

    #[test]
    fn test_lpop_negative_count_error() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "mylist", &["a", "b", "c"]);

        // LPOP mylist -1 -> error
        let result = lpop(&mut ctx, &[bulk("mylist"), bulk("-1")]);
        assert!(result.is_err());
    }

    // ------------------------------------------------------------------
    // LINSERT edge cases
    // ------------------------------------------------------------------

    #[test]
    fn test_linsert_case_insensitive() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "mylist", &["a", "b", "c"]);

        // Use lowercase "before" — LINSERT uppercases it internally
        let result = linsert(
            &mut ctx,
            &[bulk("mylist"), bulk("before"), bulk("b"), bulk("X")],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(4));

        // Verify: [a, X, b, c]
        let result = lrange(&mut ctx, &[bulk("mylist"), bulk("0"), bulk("-1")]).unwrap();
        if let RespValue::Array(arr) = result {
            let vals: Vec<&str> = arr.iter().map(|v| v.as_str().unwrap()).collect();
            assert_eq!(vals, vec!["a", "X", "b", "c"]);
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_linsert_pivot_not_found() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "mylist", &["a", "b", "c"]);

        // Pivot "z" doesn't exist in list -> returns -1
        let result = linsert(
            &mut ctx,
            &[bulk("mylist"), bulk("BEFORE"), bulk("z"), bulk("X")],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(-1));

        // List unchanged
        let result = lrange(&mut ctx, &[bulk("mylist"), bulk("0"), bulk("-1")]).unwrap();
        if let RespValue::Array(arr) = result {
            let vals: Vec<&str> = arr.iter().map(|v| v.as_str().unwrap()).collect();
            assert_eq!(vals, vec!["a", "b", "c"]);
        } else {
            panic!("Expected Array");
        }
    }

    // ===================================================================
    // NEW TESTS: Bring every command to 20+ tests
    // ===================================================================

    // Helper to create an expired list entry
    fn set_expired_list(ctx: &mut CommandContext, key: &str, elements: &[&str]) {
        use std::time::{Duration, Instant};
        let db = ctx.store().database(ctx.selected_db());
        let mut list = VecDeque::new();
        for e in elements {
            list.push_back(Bytes::from(e.to_string()));
        }
        let mut entry = Entry::new(RedisValue::List(list));
        entry.expires_at = Some(Instant::now() - Duration::from_secs(1));
        db.set(Bytes::from(key.to_owned()), entry);
    }

    // Helper to set a string key for wrong-type tests
    fn set_string_key(ctx: &mut CommandContext, key: &str, val: &str) {
        let db = ctx.store().database(ctx.selected_db());
        db.set(
            Bytes::from(key.to_owned()),
            Entry::new(RedisValue::String(Bytes::from(val.to_owned()))),
        );
    }

    // Helper: push multiple elements to the left, returning final length
    fn lpush_elements(ctx: &mut CommandContext, key: &str, elements: &[&str]) -> i64 {
        let mut args: Vec<RespValue> = vec![bulk(key)];
        for e in elements {
            args.push(bulk(e));
        }
        match lpush(ctx, &args).unwrap() {
            RespValue::Integer(n) => n,
            other => panic!("Expected Integer, got {other:?}"),
        }
    }

    // ------------------------------------------------------------------
    // LPUSH additional tests (need 15 more to reach 20)
    // ------------------------------------------------------------------

    #[test]
    fn test_lpush_single_element() {
        let mut ctx = make_ctx();
        let result = lpush(&mut ctx, &[bulk("k"), bulk("a")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("0")]).unwrap(),
            bulk("a")
        );
    }

    #[test]
    fn test_lpush_to_existing_list() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["x", "y"]);
        let result = lpush(&mut ctx, &[bulk("k"), bulk("z")]).unwrap();
        assert_eq!(result, RespValue::Integer(3));
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("0")]).unwrap(),
            bulk("z")
        );
    }

    #[test]
    fn test_lpush_expired_key_creates_new() {
        let mut ctx = make_ctx();
        set_expired_list(&mut ctx, "k", &["old"]);
        let result = lpush(&mut ctx, &[bulk("k"), bulk("new")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("0")]).unwrap(),
            bulk("new")
        );
    }

    #[test]
    fn test_lpush_special_characters() {
        let mut ctx = make_ctx();
        let result = lpush(
            &mut ctx,
            &[bulk("k"), bulk("hello world"), bulk("foo\nbar"), bulk("🦀")],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(3));
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("0")]).unwrap(),
            bulk("🦀")
        );
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("1")]).unwrap(),
            bulk("foo\nbar")
        );
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("2")]).unwrap(),
            bulk("hello world")
        );
    }

    #[test]
    fn test_lpush_empty_string_element() {
        let mut ctx = make_ctx();
        let result = lpush(&mut ctx, &[bulk("k"), bulk("")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        assert_eq!(lindex(&mut ctx, &[bulk("k"), bulk("0")]).unwrap(), bulk(""));
    }

    #[test]
    fn test_lpush_many_elements() {
        let mut ctx = make_ctx();
        let mut args = vec![bulk("k")];
        for i in 0..100 {
            args.push(bulk(&format!("e{i}")));
        }
        let result = lpush(&mut ctx, &args).unwrap();
        assert_eq!(result, RespValue::Integer(100));
        assert_eq!(
            llen(&mut ctx, &[bulk("k")]).unwrap(),
            RespValue::Integer(100)
        );
    }

    #[test]
    fn test_lpush_return_type_is_integer() {
        let mut ctx = make_ctx();
        let result = lpush(&mut ctx, &[bulk("k"), bulk("a")]).unwrap();
        assert!(matches!(result, RespValue::Integer(_)));
    }

    #[test]
    fn test_lpush_wrong_type_string_key() {
        let mut ctx = make_ctx();
        set_string_key(&mut ctx, "s", "val");
        assert!(matches!(
            lpush(&mut ctx, &[bulk("s"), bulk("a")]),
            Err(CommandError::WrongType)
        ));
    }

    #[test]
    fn test_lpush_wrong_arity_no_elements() {
        let mut ctx = make_ctx();
        assert!(matches!(
            lpush(&mut ctx, &[bulk("k")]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_lpush_wrong_arity_empty() {
        let mut ctx = make_ctx();
        assert!(matches!(
            lpush(&mut ctx, &[]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_lpush_preserves_insertion_order_multiple_calls() {
        let mut ctx = make_ctx();
        lpush(&mut ctx, &[bulk("k"), bulk("c")]).unwrap();
        lpush(&mut ctx, &[bulk("k"), bulk("b")]).unwrap();
        lpush(&mut ctx, &[bulk("k"), bulk("a")]).unwrap();
        let result = lrange(&mut ctx, &[bulk("k"), bulk("0"), bulk("-1")]).unwrap();
        if let RespValue::Array(arr) = result {
            let vals: Vec<&str> = arr.iter().map(|v| v.as_str().unwrap()).collect();
            assert_eq!(vals, vec!["a", "b", "c"]);
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_lpush_duplicate_elements() {
        let mut ctx = make_ctx();
        let result = lpush(&mut ctx, &[bulk("k"), bulk("a"), bulk("a"), bulk("a")]).unwrap();
        assert_eq!(result, RespValue::Integer(3));
        let result = lrange(&mut ctx, &[bulk("k"), bulk("0"), bulk("-1")]).unwrap();
        if let RespValue::Array(arr) = result {
            let vals: Vec<&str> = arr.iter().map(|v| v.as_str().unwrap()).collect();
            assert_eq!(vals, vec!["a", "a", "a"]);
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_lpush_increments_length_correctly() {
        let mut ctx = make_ctx();
        assert_eq!(
            lpush(&mut ctx, &[bulk("k"), bulk("a")]).unwrap(),
            RespValue::Integer(1)
        );
        assert_eq!(
            lpush(&mut ctx, &[bulk("k"), bulk("b")]).unwrap(),
            RespValue::Integer(2)
        );
        assert_eq!(
            lpush(&mut ctx, &[bulk("k"), bulk("c"), bulk("d")]).unwrap(),
            RespValue::Integer(4)
        );
    }

    #[test]
    fn test_lpush_binary_data() {
        let mut ctx = make_ctx();
        let binary = RespValue::BulkString(Bytes::from(vec![0u8, 1, 2, 255]));
        let result = lpush(&mut ctx, &[bulk("k"), binary.clone()]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        let got = lindex(&mut ctx, &[bulk("k"), bulk("0")]).unwrap();
        assert_eq!(got, binary);
    }

    #[test]
    fn test_lpush_long_key_name() {
        let mut ctx = make_ctx();
        let long_key = "k".repeat(10000);
        let result = lpush(&mut ctx, &[bulk(&long_key), bulk("v")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        assert_eq!(
            lindex(&mut ctx, &[bulk(&long_key), bulk("0")]).unwrap(),
            bulk("v")
        );
    }

    // ------------------------------------------------------------------
    // RPUSH additional tests (need 16 more to reach 20)
    // ------------------------------------------------------------------

    #[test]
    fn test_rpush_single_element() {
        let mut ctx = make_ctx();
        let result = rpush(&mut ctx, &[bulk("k"), bulk("a")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("0")]).unwrap(),
            bulk("a")
        );
    }

    #[test]
    fn test_rpush_to_existing_list() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a"]);
        let result = rpush(&mut ctx, &[bulk("k"), bulk("b")]).unwrap();
        assert_eq!(result, RespValue::Integer(2));
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("1")]).unwrap(),
            bulk("b")
        );
    }

    #[test]
    fn test_rpush_expired_key_creates_new() {
        let mut ctx = make_ctx();
        set_expired_list(&mut ctx, "k", &["old"]);
        let result = rpush(&mut ctx, &[bulk("k"), bulk("new")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("0")]).unwrap(),
            bulk("new")
        );
    }

    #[test]
    fn test_rpush_special_characters() {
        let mut ctx = make_ctx();
        let result = rpush(
            &mut ctx,
            &[
                bulk("k"),
                bulk("hello world"),
                bulk("tab\there"),
                bulk("🚀"),
            ],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(3));
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("0")]).unwrap(),
            bulk("hello world")
        );
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("2")]).unwrap(),
            bulk("🚀")
        );
    }

    #[test]
    fn test_rpush_empty_string_element() {
        let mut ctx = make_ctx();
        let result = rpush(&mut ctx, &[bulk("k"), bulk("")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        assert_eq!(lindex(&mut ctx, &[bulk("k"), bulk("0")]).unwrap(), bulk(""));
    }

    #[test]
    fn test_rpush_many_elements() {
        let mut ctx = make_ctx();
        let mut args = vec![bulk("k")];
        for i in 0..100 {
            args.push(bulk(&format!("e{i}")));
        }
        let result = rpush(&mut ctx, &args).unwrap();
        assert_eq!(result, RespValue::Integer(100));
    }

    #[test]
    fn test_rpush_return_type_is_integer() {
        let mut ctx = make_ctx();
        let result = rpush(&mut ctx, &[bulk("k"), bulk("a")]).unwrap();
        assert!(matches!(result, RespValue::Integer(_)));
    }

    #[test]
    fn test_rpush_wrong_type_string_key() {
        let mut ctx = make_ctx();
        set_string_key(&mut ctx, "s", "val");
        assert!(matches!(
            rpush(&mut ctx, &[bulk("s"), bulk("a")]),
            Err(CommandError::WrongType)
        ));
    }

    #[test]
    fn test_rpush_wrong_arity_no_elements() {
        let mut ctx = make_ctx();
        assert!(matches!(
            rpush(&mut ctx, &[bulk("k")]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_rpush_wrong_arity_empty() {
        let mut ctx = make_ctx();
        assert!(matches!(
            rpush(&mut ctx, &[]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_rpush_preserves_insertion_order() {
        let mut ctx = make_ctx();
        rpush(&mut ctx, &[bulk("k"), bulk("a")]).unwrap();
        rpush(&mut ctx, &[bulk("k"), bulk("b")]).unwrap();
        rpush(&mut ctx, &[bulk("k"), bulk("c")]).unwrap();
        let result = lrange(&mut ctx, &[bulk("k"), bulk("0"), bulk("-1")]).unwrap();
        if let RespValue::Array(arr) = result {
            let vals: Vec<&str> = arr.iter().map(|v| v.as_str().unwrap()).collect();
            assert_eq!(vals, vec!["a", "b", "c"]);
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_rpush_duplicate_elements() {
        let mut ctx = make_ctx();
        let result = rpush(&mut ctx, &[bulk("k"), bulk("x"), bulk("x"), bulk("x")]).unwrap();
        assert_eq!(result, RespValue::Integer(3));
        let result = lrange(&mut ctx, &[bulk("k"), bulk("0"), bulk("-1")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert!(arr.iter().all(|v| *v == bulk("x")));
            assert_eq!(arr.len(), 3);
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_rpush_increments_length_correctly() {
        let mut ctx = make_ctx();
        assert_eq!(
            rpush(&mut ctx, &[bulk("k"), bulk("a")]).unwrap(),
            RespValue::Integer(1)
        );
        assert_eq!(
            rpush(&mut ctx, &[bulk("k"), bulk("b")]).unwrap(),
            RespValue::Integer(2)
        );
        assert_eq!(
            rpush(&mut ctx, &[bulk("k"), bulk("c"), bulk("d")]).unwrap(),
            RespValue::Integer(4)
        );
    }

    #[test]
    fn test_rpush_binary_data() {
        let mut ctx = make_ctx();
        let binary = RespValue::BulkString(Bytes::from(vec![0u8, 127, 128, 255]));
        let result = rpush(&mut ctx, &[bulk("k"), binary.clone()]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        let got = lindex(&mut ctx, &[bulk("k"), bulk("0")]).unwrap();
        assert_eq!(got, binary);
    }

    #[test]
    fn test_rpush_creates_list_from_nonexistent() {
        let mut ctx = make_ctx();
        assert_eq!(llen(&mut ctx, &[bulk("k")]).unwrap(), RespValue::Integer(0));
        rpush(&mut ctx, &[bulk("k"), bulk("a")]).unwrap();
        assert_eq!(llen(&mut ctx, &[bulk("k")]).unwrap(), RespValue::Integer(1));
    }

    #[test]
    fn test_rpush_long_value() {
        let mut ctx = make_ctx();
        let long_val = "v".repeat(100_000);
        let result = rpush(&mut ctx, &[bulk("k"), bulk(&long_val)]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        let got = lindex(&mut ctx, &[bulk("k"), bulk("0")]).unwrap();
        assert_eq!(got.as_str().unwrap().len(), 100_000);
    }

    // ------------------------------------------------------------------
    // LPUSHX additional tests (need 17 more to reach 20)
    // ------------------------------------------------------------------

    #[test]
    fn test_lpushx_existing_single() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a"]);
        let result = lpushx(&mut ctx, &[bulk("k"), bulk("z")]).unwrap();
        assert_eq!(result, RespValue::Integer(2));
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("0")]).unwrap(),
            bulk("z")
        );
    }

    #[test]
    fn test_lpushx_existing_multiple() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a"]);
        let result = lpushx(&mut ctx, &[bulk("k"), bulk("b"), bulk("c")]).unwrap();
        assert_eq!(result, RespValue::Integer(3));
        // LPUSHX b c -> c pushed last to front: [c, b, a]
        let result = lrange(&mut ctx, &[bulk("k"), bulk("0"), bulk("-1")]).unwrap();
        if let RespValue::Array(arr) = result {
            let vals: Vec<&str> = arr.iter().map(|v| v.as_str().unwrap()).collect();
            assert_eq!(vals, vec!["c", "b", "a"]);
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_lpushx_nonexistent_returns_zero() {
        let mut ctx = make_ctx();
        let result = lpushx(&mut ctx, &[bulk("nokey"), bulk("a")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_lpushx_nonexistent_does_not_create() {
        let mut ctx = make_ctx();
        lpushx(&mut ctx, &[bulk("nokey"), bulk("a")]).unwrap();
        assert_eq!(
            llen(&mut ctx, &[bulk("nokey")]).unwrap(),
            RespValue::Integer(0)
        );
    }

    #[test]
    fn test_lpushx_expired_key_returns_zero() {
        let mut ctx = make_ctx();
        set_expired_list(&mut ctx, "k", &["old"]);
        let result = lpushx(&mut ctx, &[bulk("k"), bulk("new")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_lpushx_wrong_type() {
        let mut ctx = make_ctx();
        set_string_key(&mut ctx, "s", "val");
        assert!(matches!(
            lpushx(&mut ctx, &[bulk("s"), bulk("a")]),
            Err(CommandError::WrongType)
        ));
    }

    #[test]
    fn test_lpushx_wrong_arity_empty() {
        let mut ctx = make_ctx();
        assert!(matches!(
            lpushx(&mut ctx, &[]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_lpushx_wrong_arity_key_only() {
        let mut ctx = make_ctx();
        assert!(matches!(
            lpushx(&mut ctx, &[bulk("k")]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_lpushx_return_type_is_integer() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a"]);
        let result = lpushx(&mut ctx, &[bulk("k"), bulk("b")]).unwrap();
        assert!(matches!(result, RespValue::Integer(_)));
    }

    #[test]
    fn test_lpushx_special_characters() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a"]);
        let result = lpushx(&mut ctx, &[bulk("k"), bulk("hello\nworld"), bulk("🦀")]).unwrap();
        assert_eq!(result, RespValue::Integer(3));
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("0")]).unwrap(),
            bulk("🦀")
        );
    }

    #[test]
    fn test_lpushx_empty_string_element() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a"]);
        let result = lpushx(&mut ctx, &[bulk("k"), bulk("")]).unwrap();
        assert_eq!(result, RespValue::Integer(2));
        assert_eq!(lindex(&mut ctx, &[bulk("k"), bulk("0")]).unwrap(), bulk(""));
    }

    #[test]
    fn test_lpushx_single_element_list() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["only"]);
        let result = lpushx(&mut ctx, &[bulk("k"), bulk("new")]).unwrap();
        assert_eq!(result, RespValue::Integer(2));
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("0")]).unwrap(),
            bulk("new")
        );
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("1")]).unwrap(),
            bulk("only")
        );
    }

    #[test]
    fn test_lpushx_multiple_calls() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a"]);
        lpushx(&mut ctx, &[bulk("k"), bulk("b")]).unwrap();
        lpushx(&mut ctx, &[bulk("k"), bulk("c")]).unwrap();
        assert_eq!(llen(&mut ctx, &[bulk("k")]).unwrap(), RespValue::Integer(3));
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("0")]).unwrap(),
            bulk("c")
        );
    }

    #[test]
    fn test_lpushx_duplicate_values() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a"]);
        let result = lpushx(&mut ctx, &[bulk("k"), bulk("a"), bulk("a")]).unwrap();
        assert_eq!(result, RespValue::Integer(3));
    }

    #[test]
    fn test_lpushx_many_elements() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["seed"]);
        let mut args = vec![bulk("k")];
        for i in 0..50 {
            args.push(bulk(&format!("e{i}")));
        }
        let result = lpushx(&mut ctx, &args).unwrap();
        assert_eq!(result, RespValue::Integer(51));
    }

    #[test]
    fn test_lpushx_binary_data() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a"]);
        let binary = RespValue::BulkString(Bytes::from(vec![0u8, 255]));
        let result = lpushx(&mut ctx, &[bulk("k"), binary.clone()]).unwrap();
        assert_eq!(result, RespValue::Integer(2));
        assert_eq!(lindex(&mut ctx, &[bulk("k"), bulk("0")]).unwrap(), binary);
    }

    #[test]
    fn test_lpushx_long_key_name() {
        let mut ctx = make_ctx();
        let long_key = "k".repeat(10000);
        rpush(&mut ctx, &[bulk(&long_key), bulk("a")]).unwrap();
        let result = lpushx(&mut ctx, &[bulk(&long_key), bulk("b")]).unwrap();
        assert_eq!(result, RespValue::Integer(2));
    }

    // ------------------------------------------------------------------
    // RPUSHX additional tests (need 17 more to reach 20)
    // ------------------------------------------------------------------

    #[test]
    fn test_rpushx_nonexistent_returns_zero() {
        let mut ctx = make_ctx();
        let result = rpushx(&mut ctx, &[bulk("nokey"), bulk("a")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_rpushx_nonexistent_does_not_create() {
        let mut ctx = make_ctx();
        rpushx(&mut ctx, &[bulk("nokey"), bulk("a")]).unwrap();
        assert_eq!(
            llen(&mut ctx, &[bulk("nokey")]).unwrap(),
            RespValue::Integer(0)
        );
    }

    #[test]
    fn test_rpushx_expired_key_returns_zero() {
        let mut ctx = make_ctx();
        set_expired_list(&mut ctx, "k", &["old"]);
        let result = rpushx(&mut ctx, &[bulk("k"), bulk("new")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_rpushx_wrong_type() {
        let mut ctx = make_ctx();
        set_string_key(&mut ctx, "s", "val");
        assert!(matches!(
            rpushx(&mut ctx, &[bulk("s"), bulk("a")]),
            Err(CommandError::WrongType)
        ));
    }

    #[test]
    fn test_rpushx_wrong_arity_empty() {
        let mut ctx = make_ctx();
        assert!(matches!(
            rpushx(&mut ctx, &[]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_rpushx_wrong_arity_key_only() {
        let mut ctx = make_ctx();
        assert!(matches!(
            rpushx(&mut ctx, &[bulk("k")]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_rpushx_single_element() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a"]);
        let result = rpushx(&mut ctx, &[bulk("k"), bulk("b")]).unwrap();
        assert_eq!(result, RespValue::Integer(2));
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("1")]).unwrap(),
            bulk("b")
        );
    }

    #[test]
    fn test_rpushx_multiple_elements() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a"]);
        let result = rpushx(&mut ctx, &[bulk("k"), bulk("b"), bulk("c"), bulk("d")]).unwrap();
        assert_eq!(result, RespValue::Integer(4));
        let result = lrange(&mut ctx, &[bulk("k"), bulk("0"), bulk("-1")]).unwrap();
        if let RespValue::Array(arr) = result {
            let vals: Vec<&str> = arr.iter().map(|v| v.as_str().unwrap()).collect();
            assert_eq!(vals, vec!["a", "b", "c", "d"]);
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_rpushx_return_type_is_integer() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a"]);
        let result = rpushx(&mut ctx, &[bulk("k"), bulk("b")]).unwrap();
        assert!(matches!(result, RespValue::Integer(_)));
    }

    #[test]
    fn test_rpushx_special_characters() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a"]);
        let result = rpushx(&mut ctx, &[bulk("k"), bulk("hello world"), bulk("🚀")]).unwrap();
        assert_eq!(result, RespValue::Integer(3));
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("2")]).unwrap(),
            bulk("🚀")
        );
    }

    #[test]
    fn test_rpushx_empty_string_element() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a"]);
        let result = rpushx(&mut ctx, &[bulk("k"), bulk("")]).unwrap();
        assert_eq!(result, RespValue::Integer(2));
        assert_eq!(lindex(&mut ctx, &[bulk("k"), bulk("1")]).unwrap(), bulk(""));
    }

    #[test]
    fn test_rpushx_single_element_list_target() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["only"]);
        let result = rpushx(&mut ctx, &[bulk("k"), bulk("new")]).unwrap();
        assert_eq!(result, RespValue::Integer(2));
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("0")]).unwrap(),
            bulk("only")
        );
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("1")]).unwrap(),
            bulk("new")
        );
    }

    #[test]
    fn test_rpushx_multiple_calls() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a"]);
        rpushx(&mut ctx, &[bulk("k"), bulk("b")]).unwrap();
        rpushx(&mut ctx, &[bulk("k"), bulk("c")]).unwrap();
        assert_eq!(llen(&mut ctx, &[bulk("k")]).unwrap(), RespValue::Integer(3));
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("2")]).unwrap(),
            bulk("c")
        );
    }

    #[test]
    fn test_rpushx_duplicate_values() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a"]);
        let result = rpushx(&mut ctx, &[bulk("k"), bulk("a"), bulk("a")]).unwrap();
        assert_eq!(result, RespValue::Integer(3));
    }

    #[test]
    fn test_rpushx_many_elements() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["seed"]);
        let mut args = vec![bulk("k")];
        for i in 0..50 {
            args.push(bulk(&format!("e{i}")));
        }
        let result = rpushx(&mut ctx, &args).unwrap();
        assert_eq!(result, RespValue::Integer(51));
    }

    #[test]
    fn test_rpushx_binary_data() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a"]);
        let binary = RespValue::BulkString(Bytes::from(vec![0u8, 128, 255]));
        let result = rpushx(&mut ctx, &[bulk("k"), binary.clone()]).unwrap();
        assert_eq!(result, RespValue::Integer(2));
        assert_eq!(lindex(&mut ctx, &[bulk("k"), bulk("1")]).unwrap(), binary);
    }

    #[test]
    fn test_rpushx_long_key_name() {
        let mut ctx = make_ctx();
        let long_key = "k".repeat(10000);
        rpush(&mut ctx, &[bulk(&long_key), bulk("a")]).unwrap();
        let result = rpushx(&mut ctx, &[bulk(&long_key), bulk("b")]).unwrap();
        assert_eq!(result, RespValue::Integer(2));
    }

    // ------------------------------------------------------------------
    // LPOP additional tests (need 12 more to reach 20)
    // ------------------------------------------------------------------

    #[test]
    fn test_lpop_single_element_list() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["only"]);
        let result = lpop(&mut ctx, &[bulk("k")]).unwrap();
        assert_eq!(result, bulk("only"));
        // Key should be deleted
        assert_eq!(llen(&mut ctx, &[bulk("k")]).unwrap(), RespValue::Integer(0));
    }

    #[test]
    fn test_lpop_expired_key_returns_null() {
        let mut ctx = make_ctx();
        set_expired_list(&mut ctx, "k", &["a", "b"]);
        let result = lpop(&mut ctx, &[bulk("k")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_lpop_expired_key_with_count_returns_null() {
        let mut ctx = make_ctx();
        set_expired_list(&mut ctx, "k", &["a", "b"]);
        let result = lpop(&mut ctx, &[bulk("k"), bulk("5")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_lpop_wrong_type() {
        let mut ctx = make_ctx();
        set_string_key(&mut ctx, "s", "val");
        assert!(matches!(
            lpop(&mut ctx, &[bulk("s")]),
            Err(CommandError::WrongType)
        ));
    }

    #[test]
    fn test_lpop_wrong_arity() {
        let mut ctx = make_ctx();
        assert!(matches!(
            lpop(&mut ctx, &[]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_lpop_return_type_single_is_bulk_string() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a"]);
        let result = lpop(&mut ctx, &[bulk("k")]).unwrap();
        assert!(matches!(result, RespValue::BulkString(_)));
    }

    #[test]
    fn test_lpop_return_type_with_count_is_array() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b"]);
        let result = lpop(&mut ctx, &[bulk("k"), bulk("1")]).unwrap();
        assert!(matches!(result, RespValue::Array(_)));
    }

    #[test]
    fn test_lpop_special_characters() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["hello\nworld", "🦀"]);
        let result = lpop(&mut ctx, &[bulk("k")]).unwrap();
        assert_eq!(result, bulk("hello\nworld"));
        let result = lpop(&mut ctx, &[bulk("k")]).unwrap();
        assert_eq!(result, bulk("🦀"));
    }

    #[test]
    fn test_lpop_count_one() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b", "c"]);
        let result = lpop(&mut ctx, &[bulk("k"), bulk("1")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 1);
            assert_eq!(arr[0], bulk("a"));
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_lpop_drains_list_with_count() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b"]);
        let result = lpop(&mut ctx, &[bulk("k"), bulk("2")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 2);
        } else {
            panic!("Expected Array");
        }
        // Key should be deleted
        assert_eq!(llen(&mut ctx, &[bulk("k")]).unwrap(), RespValue::Integer(0));
    }

    #[test]
    fn test_lpop_nonexistent_key_returns_null() {
        let mut ctx = make_ctx();
        let result = lpop(&mut ctx, &[bulk("nokey")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_lpop_nonexistent_key_with_count_returns_null() {
        let mut ctx = make_ctx();
        let result = lpop(&mut ctx, &[bulk("nokey"), bulk("5")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    // ------------------------------------------------------------------
    // RPOP additional tests (need 12 more to reach 20)
    // ------------------------------------------------------------------

    #[test]
    fn test_rpop_single_element_list() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["only"]);
        let result = rpop(&mut ctx, &[bulk("k")]).unwrap();
        assert_eq!(result, bulk("only"));
        assert_eq!(llen(&mut ctx, &[bulk("k")]).unwrap(), RespValue::Integer(0));
    }

    #[test]
    fn test_rpop_expired_key_returns_null() {
        let mut ctx = make_ctx();
        set_expired_list(&mut ctx, "k", &["a", "b"]);
        let result = rpop(&mut ctx, &[bulk("k")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_rpop_wrong_type() {
        let mut ctx = make_ctx();
        set_string_key(&mut ctx, "s", "val");
        assert!(matches!(
            rpop(&mut ctx, &[bulk("s")]),
            Err(CommandError::WrongType)
        ));
    }

    #[test]
    fn test_rpop_wrong_arity() {
        let mut ctx = make_ctx();
        assert!(matches!(
            rpop(&mut ctx, &[]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_rpop_return_type_single_is_bulk_string() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a"]);
        let result = rpop(&mut ctx, &[bulk("k")]).unwrap();
        assert!(matches!(result, RespValue::BulkString(_)));
    }

    #[test]
    fn test_rpop_return_type_with_count_is_array() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b"]);
        let result = rpop(&mut ctx, &[bulk("k"), bulk("1")]).unwrap();
        assert!(matches!(result, RespValue::Array(_)));
    }

    #[test]
    fn test_rpop_special_characters() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["🦀", "hello\nworld"]);
        let result = rpop(&mut ctx, &[bulk("k")]).unwrap();
        assert_eq!(result, bulk("hello\nworld"));
        let result = rpop(&mut ctx, &[bulk("k")]).unwrap();
        assert_eq!(result, bulk("🦀"));
    }

    #[test]
    fn test_rpop_count_one() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b", "c"]);
        let result = rpop(&mut ctx, &[bulk("k"), bulk("1")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 1);
            assert_eq!(arr[0], bulk("c"));
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_rpop_drains_list_with_count() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b"]);
        let result = rpop(&mut ctx, &[bulk("k"), bulk("2")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], bulk("b"));
            assert_eq!(arr[1], bulk("a"));
        } else {
            panic!("Expected Array");
        }
        assert_eq!(llen(&mut ctx, &[bulk("k")]).unwrap(), RespValue::Integer(0));
    }

    #[test]
    fn test_rpop_nonexistent_key_returns_null() {
        let mut ctx = make_ctx();
        let result = rpop(&mut ctx, &[bulk("nokey")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_rpop_nonexistent_key_with_count_returns_null() {
        let mut ctx = make_ctx();
        let result = rpop(&mut ctx, &[bulk("nokey"), bulk("5")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_rpop_empty_string_element() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &[""]);
        let result = rpop(&mut ctx, &[bulk("k")]).unwrap();
        assert_eq!(result, bulk(""));
    }

    // ------------------------------------------------------------------
    // LRANGE additional tests (need 15 more to reach 20)
    // ------------------------------------------------------------------

    #[test]
    fn test_lrange_entire_list() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b", "c"]);
        let result = lrange(&mut ctx, &[bulk("k"), bulk("0"), bulk("-1")]).unwrap();
        if let RespValue::Array(arr) = result {
            let vals: Vec<&str> = arr.iter().map(|v| v.as_str().unwrap()).collect();
            assert_eq!(vals, vec!["a", "b", "c"]);
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_lrange_single_element_range() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b", "c"]);
        let result = lrange(&mut ctx, &[bulk("k"), bulk("1"), bulk("1")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 1);
            assert_eq!(arr[0], bulk("b"));
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_lrange_expired_key() {
        let mut ctx = make_ctx();
        set_expired_list(&mut ctx, "k", &["a", "b"]);
        let result = lrange(&mut ctx, &[bulk("k"), bulk("0"), bulk("-1")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![]));
    }

    #[test]
    fn test_lrange_nonexistent_key() {
        let mut ctx = make_ctx();
        let result = lrange(&mut ctx, &[bulk("nokey"), bulk("0"), bulk("-1")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![]));
    }

    #[test]
    fn test_lrange_wrong_type() {
        let mut ctx = make_ctx();
        set_string_key(&mut ctx, "s", "val");
        assert!(matches!(
            lrange(&mut ctx, &[bulk("s"), bulk("0"), bulk("-1")]),
            Err(CommandError::WrongType)
        ));
    }

    #[test]
    fn test_lrange_wrong_arity() {
        let mut ctx = make_ctx();
        assert!(matches!(
            lrange(&mut ctx, &[bulk("k")]),
            Err(CommandError::WrongArity(_))
        ));
        assert!(matches!(
            lrange(&mut ctx, &[bulk("k"), bulk("0")]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_lrange_start_greater_than_stop() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b", "c"]);
        let result = lrange(&mut ctx, &[bulk("k"), bulk("2"), bulk("0")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![]));
    }

    #[test]
    fn test_lrange_very_large_stop() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b"]);
        let result = lrange(&mut ctx, &[bulk("k"), bulk("0"), bulk("99999")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 2);
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_lrange_very_negative_start() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b", "c"]);
        // -100 normalizes to 0
        let result = lrange(&mut ctx, &[bulk("k"), bulk("-100"), bulk("-1")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 3);
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_lrange_return_type_is_array() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a"]);
        let result = lrange(&mut ctx, &[bulk("k"), bulk("0"), bulk("-1")]).unwrap();
        assert!(matches!(result, RespValue::Array(_)));
    }

    #[test]
    fn test_lrange_single_element_list() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["only"]);
        let result = lrange(&mut ctx, &[bulk("k"), bulk("0"), bulk("0")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 1);
            assert_eq!(arr[0], bulk("only"));
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_lrange_special_characters() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["🦀", "hello\nworld"]);
        let result = lrange(&mut ctx, &[bulk("k"), bulk("0"), bulk("-1")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr[0], bulk("🦀"));
            assert_eq!(arr[1], bulk("hello\nworld"));
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_lrange_negative_start_positive_stop() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b", "c", "d", "e"]);
        // -3 = index 2, stop = 3
        let result = lrange(&mut ctx, &[bulk("k"), bulk("-3"), bulk("3")]).unwrap();
        if let RespValue::Array(arr) = result {
            let vals: Vec<&str> = arr.iter().map(|v| v.as_str().unwrap()).collect();
            assert_eq!(vals, vec!["c", "d"]);
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_lrange_first_element_only() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b", "c"]);
        let result = lrange(&mut ctx, &[bulk("k"), bulk("0"), bulk("0")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 1);
            assert_eq!(arr[0], bulk("a"));
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_lrange_last_element_only() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b", "c"]);
        let result = lrange(&mut ctx, &[bulk("k"), bulk("-1"), bulk("-1")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 1);
            assert_eq!(arr[0], bulk("c"));
        } else {
            panic!("Expected Array");
        }
    }

    // ------------------------------------------------------------------
    // LINDEX additional tests (need 16 more to reach 20)
    // ------------------------------------------------------------------

    #[test]
    fn test_lindex_nonexistent_key() {
        let mut ctx = make_ctx();
        let result = lindex(&mut ctx, &[bulk("nokey"), bulk("0")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_lindex_expired_key() {
        let mut ctx = make_ctx();
        set_expired_list(&mut ctx, "k", &["a", "b"]);
        let result = lindex(&mut ctx, &[bulk("k"), bulk("0")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_lindex_wrong_type() {
        let mut ctx = make_ctx();
        set_string_key(&mut ctx, "s", "val");
        assert!(matches!(
            lindex(&mut ctx, &[bulk("s"), bulk("0")]),
            Err(CommandError::WrongType)
        ));
    }

    #[test]
    fn test_lindex_wrong_arity() {
        let mut ctx = make_ctx();
        assert!(matches!(
            lindex(&mut ctx, &[bulk("k")]),
            Err(CommandError::WrongArity(_))
        ));
        assert!(matches!(
            lindex(&mut ctx, &[]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_lindex_positive_out_of_range() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b"]);
        let result = lindex(&mut ctx, &[bulk("k"), bulk("5")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_lindex_negative_out_of_range() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b"]);
        let result = lindex(&mut ctx, &[bulk("k"), bulk("-5")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_lindex_first_element() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b", "c"]);
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("0")]).unwrap(),
            bulk("a")
        );
    }

    #[test]
    fn test_lindex_last_element_negative() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b", "c"]);
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("-1")]).unwrap(),
            bulk("c")
        );
    }

    #[test]
    fn test_lindex_single_element_list() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["only"]);
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("0")]).unwrap(),
            bulk("only")
        );
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("-1")]).unwrap(),
            bulk("only")
        );
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("1")]).unwrap(),
            RespValue::Null
        );
    }

    #[test]
    fn test_lindex_return_type_is_bulk_string() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a"]);
        let result = lindex(&mut ctx, &[bulk("k"), bulk("0")]).unwrap();
        assert!(matches!(result, RespValue::BulkString(_)));
    }

    #[test]
    fn test_lindex_special_characters() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["hello\nworld", "🦀"]);
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("0")]).unwrap(),
            bulk("hello\nworld")
        );
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("1")]).unwrap(),
            bulk("🦀")
        );
    }

    #[test]
    fn test_lindex_empty_string_element() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &[""]);
        assert_eq!(lindex(&mut ctx, &[bulk("k"), bulk("0")]).unwrap(), bulk(""));
    }

    #[test]
    fn test_lindex_large_list() {
        let mut ctx = make_ctx();
        let mut args = vec![bulk("k")];
        for i in 0..100 {
            args.push(bulk(&format!("e{i}")));
        }
        rpush(&mut ctx, &args).unwrap();
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("50")]).unwrap(),
            bulk("e50")
        );
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("99")]).unwrap(),
            bulk("e99")
        );
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("100")]).unwrap(),
            RespValue::Null
        );
    }

    #[test]
    fn test_lindex_negative_minus_len() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b", "c"]);
        // -3 should be first element
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("-3")]).unwrap(),
            bulk("a")
        );
    }

    #[test]
    fn test_lindex_zero_index() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["first", "second"]);
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("0")]).unwrap(),
            bulk("first")
        );
    }

    #[test]
    fn test_lindex_middle_element() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b", "c", "d", "e"]);
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("2")]).unwrap(),
            bulk("c")
        );
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("-3")]).unwrap(),
            bulk("c")
        );
    }

    // ------------------------------------------------------------------
    // LSET additional tests (need 16 more to reach 20)
    // ------------------------------------------------------------------

    #[test]
    fn test_lset_first_element() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b", "c"]);
        let result = lset(&mut ctx, &[bulk("k"), bulk("0"), bulk("X")]).unwrap();
        assert_eq!(result, RespValue::ok());
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("0")]).unwrap(),
            bulk("X")
        );
    }

    #[test]
    fn test_lset_last_element() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b", "c"]);
        let result = lset(&mut ctx, &[bulk("k"), bulk("2"), bulk("Z")]).unwrap();
        assert_eq!(result, RespValue::ok());
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("2")]).unwrap(),
            bulk("Z")
        );
    }

    #[test]
    fn test_lset_negative_index() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b", "c"]);
        lset(&mut ctx, &[bulk("k"), bulk("-1"), bulk("Z")]).unwrap();
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("2")]).unwrap(),
            bulk("Z")
        );
    }

    #[test]
    fn test_lset_negative_first() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b", "c"]);
        lset(&mut ctx, &[bulk("k"), bulk("-3"), bulk("Z")]).unwrap();
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("0")]).unwrap(),
            bulk("Z")
        );
    }

    #[test]
    fn test_lset_nonexistent_key() {
        let mut ctx = make_ctx();
        assert!(matches!(
            lset(&mut ctx, &[bulk("nokey"), bulk("0"), bulk("x")]),
            Err(CommandError::NoSuchKey)
        ));
    }

    #[test]
    fn test_lset_expired_key() {
        let mut ctx = make_ctx();
        set_expired_list(&mut ctx, "k", &["a"]);
        assert!(matches!(
            lset(&mut ctx, &[bulk("k"), bulk("0"), bulk("x")]),
            Err(CommandError::NoSuchKey)
        ));
    }

    #[test]
    fn test_lset_wrong_type() {
        let mut ctx = make_ctx();
        set_string_key(&mut ctx, "s", "val");
        assert!(matches!(
            lset(&mut ctx, &[bulk("s"), bulk("0"), bulk("x")]),
            Err(CommandError::WrongType)
        ));
    }

    #[test]
    fn test_lset_wrong_arity() {
        let mut ctx = make_ctx();
        assert!(matches!(
            lset(&mut ctx, &[bulk("k"), bulk("0")]),
            Err(CommandError::WrongArity(_))
        ));
        assert!(matches!(
            lset(&mut ctx, &[bulk("k")]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_lset_positive_out_of_range() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b"]);
        assert!(lset(&mut ctx, &[bulk("k"), bulk("5"), bulk("x")]).is_err());
    }

    #[test]
    fn test_lset_negative_out_of_range() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b"]);
        assert!(lset(&mut ctx, &[bulk("k"), bulk("-5"), bulk("x")]).is_err());
    }

    #[test]
    fn test_lset_return_type_is_ok() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a"]);
        let result = lset(&mut ctx, &[bulk("k"), bulk("0"), bulk("b")]).unwrap();
        assert_eq!(result, RespValue::ok());
    }

    #[test]
    fn test_lset_special_characters() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a"]);
        lset(&mut ctx, &[bulk("k"), bulk("0"), bulk("🦀hello\nworld")]).unwrap();
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("0")]).unwrap(),
            bulk("🦀hello\nworld")
        );
    }

    #[test]
    fn test_lset_empty_string() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a"]);
        lset(&mut ctx, &[bulk("k"), bulk("0"), bulk("")]).unwrap();
        assert_eq!(lindex(&mut ctx, &[bulk("k"), bulk("0")]).unwrap(), bulk(""));
    }

    #[test]
    fn test_lset_single_element_list() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["old"]);
        lset(&mut ctx, &[bulk("k"), bulk("0"), bulk("new")]).unwrap();
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("0")]).unwrap(),
            bulk("new")
        );
        assert_eq!(llen(&mut ctx, &[bulk("k")]).unwrap(), RespValue::Integer(1));
    }

    #[test]
    fn test_lset_does_not_change_length() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b", "c"]);
        lset(&mut ctx, &[bulk("k"), bulk("1"), bulk("X")]).unwrap();
        assert_eq!(llen(&mut ctx, &[bulk("k")]).unwrap(), RespValue::Integer(3));
    }

    #[test]
    fn test_lset_multiple_sets() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b", "c"]);
        lset(&mut ctx, &[bulk("k"), bulk("0"), bulk("X")]).unwrap();
        lset(&mut ctx, &[bulk("k"), bulk("1"), bulk("Y")]).unwrap();
        lset(&mut ctx, &[bulk("k"), bulk("2"), bulk("Z")]).unwrap();
        let result = lrange(&mut ctx, &[bulk("k"), bulk("0"), bulk("-1")]).unwrap();
        if let RespValue::Array(arr) = result {
            let vals: Vec<&str> = arr.iter().map(|v| v.as_str().unwrap()).collect();
            assert_eq!(vals, vec!["X", "Y", "Z"]);
        } else {
            panic!("Expected Array");
        }
    }

    // ------------------------------------------------------------------
    // LLEN additional tests (need 17 more to reach 20)
    // ------------------------------------------------------------------

    #[test]
    fn test_llen_nonexistent_key() {
        let mut ctx = make_ctx();
        assert_eq!(
            llen(&mut ctx, &[bulk("nokey")]).unwrap(),
            RespValue::Integer(0)
        );
    }

    #[test]
    fn test_llen_expired_key() {
        let mut ctx = make_ctx();
        set_expired_list(&mut ctx, "k", &["a", "b"]);
        assert_eq!(llen(&mut ctx, &[bulk("k")]).unwrap(), RespValue::Integer(0));
    }

    #[test]
    fn test_llen_wrong_type() {
        let mut ctx = make_ctx();
        set_string_key(&mut ctx, "s", "val");
        assert!(matches!(
            llen(&mut ctx, &[bulk("s")]),
            Err(CommandError::WrongType)
        ));
    }

    #[test]
    fn test_llen_wrong_arity() {
        let mut ctx = make_ctx();
        assert!(matches!(
            llen(&mut ctx, &[]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_llen_single_element() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a"]);
        assert_eq!(llen(&mut ctx, &[bulk("k")]).unwrap(), RespValue::Integer(1));
    }

    #[test]
    fn test_llen_multiple_elements() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b", "c", "d", "e"]);
        assert_eq!(llen(&mut ctx, &[bulk("k")]).unwrap(), RespValue::Integer(5));
    }

    #[test]
    fn test_llen_after_push_pop() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b", "c"]);
        lpop(&mut ctx, &[bulk("k")]).unwrap();
        assert_eq!(llen(&mut ctx, &[bulk("k")]).unwrap(), RespValue::Integer(2));
        rpop(&mut ctx, &[bulk("k")]).unwrap();
        assert_eq!(llen(&mut ctx, &[bulk("k")]).unwrap(), RespValue::Integer(1));
    }

    #[test]
    fn test_llen_return_type_is_integer() {
        let mut ctx = make_ctx();
        let result = llen(&mut ctx, &[bulk("nokey")]).unwrap();
        assert!(matches!(result, RespValue::Integer(_)));
    }

    #[test]
    fn test_llen_after_list_deletion() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a"]);
        lpop(&mut ctx, &[bulk("k")]).unwrap();
        assert_eq!(llen(&mut ctx, &[bulk("k")]).unwrap(), RespValue::Integer(0));
    }

    #[test]
    fn test_llen_large_list() {
        let mut ctx = make_ctx();
        let mut args = vec![bulk("k")];
        for i in 0..500 {
            args.push(bulk(&format!("e{i}")));
        }
        rpush(&mut ctx, &args).unwrap();
        assert_eq!(
            llen(&mut ctx, &[bulk("k")]).unwrap(),
            RespValue::Integer(500)
        );
    }

    #[test]
    fn test_llen_after_lrem() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b", "a", "c"]);
        lrem(&mut ctx, &[bulk("k"), bulk("0"), bulk("a")]).unwrap();
        assert_eq!(llen(&mut ctx, &[bulk("k")]).unwrap(), RespValue::Integer(2));
    }

    #[test]
    fn test_llen_after_ltrim() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b", "c", "d", "e"]);
        ltrim(&mut ctx, &[bulk("k"), bulk("1"), bulk("2")]).unwrap();
        assert_eq!(llen(&mut ctx, &[bulk("k")]).unwrap(), RespValue::Integer(2));
    }

    #[test]
    fn test_llen_after_linsert() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b"]);
        linsert(&mut ctx, &[bulk("k"), bulk("BEFORE"), bulk("b"), bulk("X")]).unwrap();
        assert_eq!(llen(&mut ctx, &[bulk("k")]).unwrap(), RespValue::Integer(3));
    }

    #[test]
    fn test_llen_after_lset_unchanged() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b"]);
        lset(&mut ctx, &[bulk("k"), bulk("0"), bulk("X")]).unwrap();
        assert_eq!(llen(&mut ctx, &[bulk("k")]).unwrap(), RespValue::Integer(2));
    }

    #[test]
    fn test_llen_different_keys() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k1", &["a"]);
        rpush_elements(&mut ctx, "k2", &["a", "b", "c"]);
        assert_eq!(
            llen(&mut ctx, &[bulk("k1")]).unwrap(),
            RespValue::Integer(1)
        );
        assert_eq!(
            llen(&mut ctx, &[bulk("k2")]).unwrap(),
            RespValue::Integer(3)
        );
    }

    #[test]
    fn test_llen_after_lpush() {
        let mut ctx = make_ctx();
        lpush_elements(&mut ctx, "k", &["a", "b", "c"]);
        assert_eq!(llen(&mut ctx, &[bulk("k")]).unwrap(), RespValue::Integer(3));
    }

    #[test]
    fn test_llen_after_lmove() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "src", &["a", "b", "c"]);
        lmove(
            &mut ctx,
            &[bulk("src"), bulk("dst"), bulk("LEFT"), bulk("RIGHT")],
        )
        .unwrap();
        assert_eq!(
            llen(&mut ctx, &[bulk("src")]).unwrap(),
            RespValue::Integer(2)
        );
        assert_eq!(
            llen(&mut ctx, &[bulk("dst")]).unwrap(),
            RespValue::Integer(1)
        );
    }

    // ------------------------------------------------------------------
    // LREM additional tests (need 14 more to reach 20)
    // ------------------------------------------------------------------

    #[test]
    fn test_lrem_nonexistent_key() {
        let mut ctx = make_ctx();
        let result = lrem(&mut ctx, &[bulk("nokey"), bulk("0"), bulk("a")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_lrem_expired_key() {
        let mut ctx = make_ctx();
        set_expired_list(&mut ctx, "k", &["a", "b"]);
        let result = lrem(&mut ctx, &[bulk("k"), bulk("0"), bulk("a")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_lrem_wrong_type() {
        let mut ctx = make_ctx();
        set_string_key(&mut ctx, "s", "val");
        assert!(matches!(
            lrem(&mut ctx, &[bulk("s"), bulk("0"), bulk("a")]),
            Err(CommandError::WrongType)
        ));
    }

    #[test]
    fn test_lrem_wrong_arity() {
        let mut ctx = make_ctx();
        assert!(matches!(
            lrem(&mut ctx, &[bulk("k"), bulk("0")]),
            Err(CommandError::WrongArity(_))
        ));
        assert!(matches!(
            lrem(&mut ctx, &[bulk("k")]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_lrem_element_not_found() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b", "c"]);
        let result = lrem(&mut ctx, &[bulk("k"), bulk("0"), bulk("z")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
        assert_eq!(llen(&mut ctx, &[bulk("k")]).unwrap(), RespValue::Integer(3));
    }

    #[test]
    fn test_lrem_return_type_is_integer() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a"]);
        let result = lrem(&mut ctx, &[bulk("k"), bulk("0"), bulk("a")]).unwrap();
        assert!(matches!(result, RespValue::Integer(_)));
    }

    #[test]
    fn test_lrem_positive_count_limited() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b", "a", "b", "a"]);
        // Remove first 1 occurrence of "a"
        let result = lrem(&mut ctx, &[bulk("k"), bulk("1"), bulk("a")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        let result = lrange(&mut ctx, &[bulk("k"), bulk("0"), bulk("-1")]).unwrap();
        if let RespValue::Array(arr) = result {
            let vals: Vec<&str> = arr.iter().map(|v| v.as_str().unwrap()).collect();
            assert_eq!(vals, vec!["b", "a", "b", "a"]);
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_lrem_negative_count_limited() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b", "a", "b", "a"]);
        // Remove last 1 occurrence of "a"
        let result = lrem(&mut ctx, &[bulk("k"), bulk("-1"), bulk("a")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        let result = lrange(&mut ctx, &[bulk("k"), bulk("0"), bulk("-1")]).unwrap();
        if let RespValue::Array(arr) = result {
            let vals: Vec<&str> = arr.iter().map(|v| v.as_str().unwrap()).collect();
            assert_eq!(vals, vec!["a", "b", "a", "b"]);
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_lrem_special_characters() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["🦀", "hello", "🦀"]);
        let result = lrem(&mut ctx, &[bulk("k"), bulk("0"), bulk("🦀")]).unwrap();
        assert_eq!(result, RespValue::Integer(2));
        assert_eq!(llen(&mut ctx, &[bulk("k")]).unwrap(), RespValue::Integer(1));
    }

    #[test]
    fn test_lrem_empty_string() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["", "a", ""]);
        let result = lrem(&mut ctx, &[bulk("k"), bulk("0"), bulk("")]).unwrap();
        assert_eq!(result, RespValue::Integer(2));
        assert_eq!(llen(&mut ctx, &[bulk("k")]).unwrap(), RespValue::Integer(1));
    }

    #[test]
    fn test_lrem_single_element_list() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a"]);
        let result = lrem(&mut ctx, &[bulk("k"), bulk("0"), bulk("a")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        assert_eq!(llen(&mut ctx, &[bulk("k")]).unwrap(), RespValue::Integer(0));
    }

    #[test]
    fn test_lrem_count_exceeds_occurrences() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b", "a"]);
        // count=10 but only 2 "a"s
        let result = lrem(&mut ctx, &[bulk("k"), bulk("10"), bulk("a")]).unwrap();
        assert_eq!(result, RespValue::Integer(2));
        assert_eq!(llen(&mut ctx, &[bulk("k")]).unwrap(), RespValue::Integer(1));
    }

    #[test]
    fn test_lrem_all_same_elements() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["x", "x", "x", "x", "x"]);
        let result = lrem(&mut ctx, &[bulk("k"), bulk("0"), bulk("x")]).unwrap();
        assert_eq!(result, RespValue::Integer(5));
        assert_eq!(llen(&mut ctx, &[bulk("k")]).unwrap(), RespValue::Integer(0));
    }

    #[test]
    fn test_lrem_preserves_other_elements_order() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b", "c", "b", "d"]);
        lrem(&mut ctx, &[bulk("k"), bulk("0"), bulk("b")]).unwrap();
        let result = lrange(&mut ctx, &[bulk("k"), bulk("0"), bulk("-1")]).unwrap();
        if let RespValue::Array(arr) = result {
            let vals: Vec<&str> = arr.iter().map(|v| v.as_str().unwrap()).collect();
            assert_eq!(vals, vec!["a", "c", "d"]);
        } else {
            panic!("Expected Array");
        }
    }

    // ------------------------------------------------------------------
    // LINSERT additional tests (need 14 more to reach 20)
    // ------------------------------------------------------------------

    #[test]
    fn test_linsert_nonexistent_key() {
        let mut ctx = make_ctx();
        let result = linsert(
            &mut ctx,
            &[bulk("nokey"), bulk("BEFORE"), bulk("a"), bulk("b")],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_linsert_expired_key() {
        let mut ctx = make_ctx();
        set_expired_list(&mut ctx, "k", &["a", "b"]);
        let result = linsert(&mut ctx, &[bulk("k"), bulk("BEFORE"), bulk("a"), bulk("X")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_linsert_wrong_type() {
        let mut ctx = make_ctx();
        set_string_key(&mut ctx, "s", "val");
        assert!(matches!(
            linsert(&mut ctx, &[bulk("s"), bulk("BEFORE"), bulk("a"), bulk("b")]),
            Err(CommandError::WrongType)
        ));
    }

    #[test]
    fn test_linsert_wrong_arity() {
        let mut ctx = make_ctx();
        assert!(matches!(
            linsert(&mut ctx, &[bulk("k"), bulk("BEFORE"), bulk("a")]),
            Err(CommandError::WrongArity(_))
        ));
        assert!(matches!(
            linsert(&mut ctx, &[bulk("k"), bulk("BEFORE")]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_linsert_invalid_position() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a"]);
        assert!(matches!(
            linsert(
                &mut ctx,
                &[bulk("k"), bulk("INVALID"), bulk("a"), bulk("b")]
            ),
            Err(CommandError::SyntaxError)
        ));
    }

    #[test]
    fn test_linsert_before_first_element() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b"]);
        let result = linsert(&mut ctx, &[bulk("k"), bulk("BEFORE"), bulk("a"), bulk("Z")]).unwrap();
        assert_eq!(result, RespValue::Integer(3));
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("0")]).unwrap(),
            bulk("Z")
        );
    }

    #[test]
    fn test_linsert_after_last_element() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b"]);
        let result = linsert(&mut ctx, &[bulk("k"), bulk("AFTER"), bulk("b"), bulk("Z")]).unwrap();
        assert_eq!(result, RespValue::Integer(3));
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("2")]).unwrap(),
            bulk("Z")
        );
    }

    #[test]
    fn test_linsert_return_type_is_integer() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a"]);
        let result = linsert(&mut ctx, &[bulk("k"), bulk("BEFORE"), bulk("a"), bulk("b")]).unwrap();
        assert!(matches!(result, RespValue::Integer(_)));
    }

    #[test]
    fn test_linsert_special_characters() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["🦀"]);
        let result = linsert(
            &mut ctx,
            &[bulk("k"), bulk("AFTER"), bulk("🦀"), bulk("hello\nworld")],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(2));
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("1")]).unwrap(),
            bulk("hello\nworld")
        );
    }

    #[test]
    fn test_linsert_empty_string_pivot() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &[""]);
        let result = linsert(&mut ctx, &[bulk("k"), bulk("BEFORE"), bulk(""), bulk("X")]).unwrap();
        assert_eq!(result, RespValue::Integer(2));
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("0")]).unwrap(),
            bulk("X")
        );
    }

    #[test]
    fn test_linsert_single_element_list_before() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["only"]);
        let result = linsert(
            &mut ctx,
            &[bulk("k"), bulk("BEFORE"), bulk("only"), bulk("new")],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(2));
        let result = lrange(&mut ctx, &[bulk("k"), bulk("0"), bulk("-1")]).unwrap();
        if let RespValue::Array(arr) = result {
            let vals: Vec<&str> = arr.iter().map(|v| v.as_str().unwrap()).collect();
            assert_eq!(vals, vec!["new", "only"]);
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_linsert_single_element_list_after() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["only"]);
        let result = linsert(
            &mut ctx,
            &[bulk("k"), bulk("AFTER"), bulk("only"), bulk("new")],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(2));
        let result = lrange(&mut ctx, &[bulk("k"), bulk("0"), bulk("-1")]).unwrap();
        if let RespValue::Array(arr) = result {
            let vals: Vec<&str> = arr.iter().map(|v| v.as_str().unwrap()).collect();
            assert_eq!(vals, vec!["only", "new"]);
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_linsert_duplicate_pivot_uses_first() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b", "a"]);
        let result = linsert(&mut ctx, &[bulk("k"), bulk("BEFORE"), bulk("a"), bulk("X")]).unwrap();
        assert_eq!(result, RespValue::Integer(4));
        let result = lrange(&mut ctx, &[bulk("k"), bulk("0"), bulk("-1")]).unwrap();
        if let RespValue::Array(arr) = result {
            let vals: Vec<&str> = arr.iter().map(|v| v.as_str().unwrap()).collect();
            assert_eq!(vals, vec!["X", "a", "b", "a"]);
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_linsert_multiple_inserts() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "c"]);
        linsert(&mut ctx, &[bulk("k"), bulk("BEFORE"), bulk("c"), bulk("b")]).unwrap();
        linsert(&mut ctx, &[bulk("k"), bulk("AFTER"), bulk("c"), bulk("d")]).unwrap();
        let result = lrange(&mut ctx, &[bulk("k"), bulk("0"), bulk("-1")]).unwrap();
        if let RespValue::Array(arr) = result {
            let vals: Vec<&str> = arr.iter().map(|v| v.as_str().unwrap()).collect();
            assert_eq!(vals, vec!["a", "b", "c", "d"]);
        } else {
            panic!("Expected Array");
        }
    }

    // ------------------------------------------------------------------
    // LTRIM additional tests (need 16 more to reach 20)
    // ------------------------------------------------------------------

    #[test]
    fn test_ltrim_nonexistent_key() {
        let mut ctx = make_ctx();
        let result = ltrim(&mut ctx, &[bulk("nokey"), bulk("0"), bulk("-1")]).unwrap();
        assert_eq!(result, RespValue::ok());
    }

    #[test]
    fn test_ltrim_expired_key() {
        let mut ctx = make_ctx();
        set_expired_list(&mut ctx, "k", &["a", "b"]);
        let result = ltrim(&mut ctx, &[bulk("k"), bulk("0"), bulk("-1")]).unwrap();
        assert_eq!(result, RespValue::ok());
    }

    #[test]
    fn test_ltrim_wrong_type() {
        let mut ctx = make_ctx();
        set_string_key(&mut ctx, "s", "val");
        assert!(matches!(
            ltrim(&mut ctx, &[bulk("s"), bulk("0"), bulk("-1")]),
            Err(CommandError::WrongType)
        ));
    }

    #[test]
    fn test_ltrim_wrong_arity() {
        let mut ctx = make_ctx();
        assert!(matches!(
            ltrim(&mut ctx, &[bulk("k"), bulk("0")]),
            Err(CommandError::WrongArity(_))
        ));
        assert!(matches!(
            ltrim(&mut ctx, &[bulk("k")]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_ltrim_keep_all() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b", "c"]);
        ltrim(&mut ctx, &[bulk("k"), bulk("0"), bulk("-1")]).unwrap();
        assert_eq!(llen(&mut ctx, &[bulk("k")]).unwrap(), RespValue::Integer(3));
    }

    #[test]
    fn test_ltrim_remove_all() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b", "c"]);
        ltrim(&mut ctx, &[bulk("k"), bulk("5"), bulk("10")]).unwrap();
        assert_eq!(llen(&mut ctx, &[bulk("k")]).unwrap(), RespValue::Integer(0));
    }

    #[test]
    fn test_ltrim_keep_first() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b", "c"]);
        ltrim(&mut ctx, &[bulk("k"), bulk("0"), bulk("0")]).unwrap();
        assert_eq!(llen(&mut ctx, &[bulk("k")]).unwrap(), RespValue::Integer(1));
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("0")]).unwrap(),
            bulk("a")
        );
    }

    #[test]
    fn test_ltrim_keep_last() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b", "c"]);
        ltrim(&mut ctx, &[bulk("k"), bulk("-1"), bulk("-1")]).unwrap();
        assert_eq!(llen(&mut ctx, &[bulk("k")]).unwrap(), RespValue::Integer(1));
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("0")]).unwrap(),
            bulk("c")
        );
    }

    #[test]
    fn test_ltrim_return_type_is_ok() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a"]);
        let result = ltrim(&mut ctx, &[bulk("k"), bulk("0"), bulk("-1")]).unwrap();
        assert_eq!(result, RespValue::ok());
    }

    #[test]
    fn test_ltrim_start_greater_than_end_deletes() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b", "c"]);
        ltrim(&mut ctx, &[bulk("k"), bulk("3"), bulk("1")]).unwrap();
        assert_eq!(llen(&mut ctx, &[bulk("k")]).unwrap(), RespValue::Integer(0));
    }

    #[test]
    fn test_ltrim_single_element_list_keep() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["only"]);
        ltrim(&mut ctx, &[bulk("k"), bulk("0"), bulk("0")]).unwrap();
        assert_eq!(llen(&mut ctx, &[bulk("k")]).unwrap(), RespValue::Integer(1));
        assert_eq!(
            lindex(&mut ctx, &[bulk("k"), bulk("0")]).unwrap(),
            bulk("only")
        );
    }

    #[test]
    fn test_ltrim_single_element_list_remove() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["only"]);
        ltrim(&mut ctx, &[bulk("k"), bulk("1"), bulk("2")]).unwrap();
        assert_eq!(llen(&mut ctx, &[bulk("k")]).unwrap(), RespValue::Integer(0));
    }

    #[test]
    fn test_ltrim_very_large_stop() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b"]);
        ltrim(&mut ctx, &[bulk("k"), bulk("0"), bulk("99999")]).unwrap();
        assert_eq!(llen(&mut ctx, &[bulk("k")]).unwrap(), RespValue::Integer(2));
    }

    #[test]
    fn test_ltrim_very_negative_start() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b", "c"]);
        ltrim(&mut ctx, &[bulk("k"), bulk("-100"), bulk("-1")]).unwrap();
        assert_eq!(llen(&mut ctx, &[bulk("k")]).unwrap(), RespValue::Integer(3));
    }

    #[test]
    fn test_ltrim_middle_range() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b", "c", "d", "e"]);
        ltrim(&mut ctx, &[bulk("k"), bulk("1"), bulk("3")]).unwrap();
        let result = lrange(&mut ctx, &[bulk("k"), bulk("0"), bulk("-1")]).unwrap();
        if let RespValue::Array(arr) = result {
            let vals: Vec<&str> = arr.iter().map(|v| v.as_str().unwrap()).collect();
            assert_eq!(vals, vec!["b", "c", "d"]);
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_ltrim_negative_start_negative_stop() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b", "c", "d", "e"]);
        // Keep elements from -4 to -2: [b, c, d]
        ltrim(&mut ctx, &[bulk("k"), bulk("-4"), bulk("-2")]).unwrap();
        let result = lrange(&mut ctx, &[bulk("k"), bulk("0"), bulk("-1")]).unwrap();
        if let RespValue::Array(arr) = result {
            let vals: Vec<&str> = arr.iter().map(|v| v.as_str().unwrap()).collect();
            assert_eq!(vals, vec!["b", "c", "d"]);
        } else {
            panic!("Expected Array");
        }
    }

    // ------------------------------------------------------------------
    // LPOS additional tests (need 10 more to reach 20)
    // ------------------------------------------------------------------

    #[test]
    fn test_lpos_expired_key() {
        let mut ctx = make_ctx();
        set_expired_list(&mut ctx, "k", &["a", "b"]);
        let result = lpos(&mut ctx, &[bulk("k"), bulk("a")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_lpos_expired_key_with_count() {
        let mut ctx = make_ctx();
        set_expired_list(&mut ctx, "k", &["a", "b"]);
        let result = lpos(&mut ctx, &[bulk("k"), bulk("a"), bulk("COUNT"), bulk("0")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![]));
    }

    #[test]
    fn test_lpos_wrong_type() {
        let mut ctx = make_ctx();
        set_string_key(&mut ctx, "s", "val");
        assert!(matches!(
            lpos(&mut ctx, &[bulk("s"), bulk("a")]),
            Err(CommandError::WrongType)
        ));
    }

    #[test]
    fn test_lpos_wrong_arity() {
        let mut ctx = make_ctx();
        assert!(matches!(
            lpos(&mut ctx, &[bulk("k")]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_lpos_single_element_list_found() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a"]);
        let result = lpos(&mut ctx, &[bulk("k"), bulk("a")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_lpos_single_element_list_not_found() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a"]);
        let result = lpos(&mut ctx, &[bulk("k"), bulk("z")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_lpos_return_type_without_count() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a"]);
        let result = lpos(&mut ctx, &[bulk("k"), bulk("a")]).unwrap();
        assert!(matches!(result, RespValue::Integer(_)));
    }

    #[test]
    fn test_lpos_return_type_with_count() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a"]);
        let result = lpos(&mut ctx, &[bulk("k"), bulk("a"), bulk("COUNT"), bulk("1")]).unwrap();
        assert!(matches!(result, RespValue::Array(_)));
    }

    #[test]
    fn test_lpos_special_characters() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["🦀", "hello", "🦀"]);
        let result = lpos(&mut ctx, &[bulk("k"), bulk("🦀"), bulk("COUNT"), bulk("0")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], RespValue::Integer(0));
            assert_eq!(arr[1], RespValue::Integer(2));
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_lpos_syntax_error_unknown_option() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a"]);
        assert!(matches!(
            lpos(&mut ctx, &[bulk("k"), bulk("a"), bulk("BADOPT"), bulk("1")]),
            Err(CommandError::SyntaxError)
        ));
    }

    // ------------------------------------------------------------------
    // LMOVE additional tests (need 13 more to reach 20)
    // ------------------------------------------------------------------

    #[test]
    fn test_lmove_nonexistent_source() {
        let mut ctx = make_ctx();
        let result = lmove(
            &mut ctx,
            &[bulk("nokey"), bulk("dst"), bulk("LEFT"), bulk("RIGHT")],
        )
        .unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_lmove_expired_source() {
        let mut ctx = make_ctx();
        set_expired_list(&mut ctx, "src", &["a", "b"]);
        let result = lmove(
            &mut ctx,
            &[bulk("src"), bulk("dst"), bulk("LEFT"), bulk("RIGHT")],
        )
        .unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_lmove_wrong_type_source() {
        let mut ctx = make_ctx();
        set_string_key(&mut ctx, "s", "val");
        assert!(matches!(
            lmove(
                &mut ctx,
                &[bulk("s"), bulk("dst"), bulk("LEFT"), bulk("RIGHT")]
            ),
            Err(CommandError::WrongType)
        ));
    }

    #[test]
    fn test_lmove_wrong_arity() {
        let mut ctx = make_ctx();
        assert!(matches!(
            lmove(&mut ctx, &[bulk("src"), bulk("dst"), bulk("LEFT")]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_lmove_invalid_direction_from() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "src", &["a"]);
        assert!(matches!(
            lmove(
                &mut ctx,
                &[bulk("src"), bulk("dst"), bulk("UP"), bulk("RIGHT")]
            ),
            Err(CommandError::SyntaxError)
        ));
    }

    #[test]
    fn test_lmove_invalid_direction_to() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "src", &["a"]);
        assert!(matches!(
            lmove(
                &mut ctx,
                &[bulk("src"), bulk("dst"), bulk("LEFT"), bulk("DOWN")]
            ),
            Err(CommandError::SyntaxError)
        ));
    }

    #[test]
    fn test_lmove_creates_destination() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "src", &["a", "b"]);
        let result = lmove(
            &mut ctx,
            &[bulk("src"), bulk("newdst"), bulk("LEFT"), bulk("RIGHT")],
        )
        .unwrap();
        assert_eq!(result, bulk("a"));
        assert_eq!(
            llen(&mut ctx, &[bulk("newdst")]).unwrap(),
            RespValue::Integer(1)
        );
    }

    #[test]
    fn test_lmove_return_type_is_bulk_string() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "src", &["a"]);
        let result = lmove(
            &mut ctx,
            &[bulk("src"), bulk("dst"), bulk("LEFT"), bulk("RIGHT")],
        )
        .unwrap();
        assert!(matches!(result, RespValue::BulkString(_)));
    }

    #[test]
    fn test_lmove_special_characters() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "src", &["🦀", "hello\nworld"]);
        let result = lmove(
            &mut ctx,
            &[bulk("src"), bulk("dst"), bulk("LEFT"), bulk("RIGHT")],
        )
        .unwrap();
        assert_eq!(result, bulk("🦀"));
    }

    #[test]
    fn test_lmove_single_element_source_deleted() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "src", &["only"]);
        lmove(
            &mut ctx,
            &[bulk("src"), bulk("dst"), bulk("LEFT"), bulk("RIGHT")],
        )
        .unwrap();
        assert_eq!(
            llen(&mut ctx, &[bulk("src")]).unwrap(),
            RespValue::Integer(0)
        );
        assert_eq!(
            llen(&mut ctx, &[bulk("dst")]).unwrap(),
            RespValue::Integer(1)
        );
    }

    #[test]
    fn test_lmove_same_key_rotate_right_to_left() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b", "c"]);
        let result = lmove(
            &mut ctx,
            &[bulk("k"), bulk("k"), bulk("RIGHT"), bulk("LEFT")],
        )
        .unwrap();
        assert_eq!(result, bulk("c"));
        let result = lrange(&mut ctx, &[bulk("k"), bulk("0"), bulk("-1")]).unwrap();
        if let RespValue::Array(arr) = result {
            let vals: Vec<&str> = arr.iter().map(|v| v.as_str().unwrap()).collect();
            assert_eq!(vals, vec!["c", "a", "b"]);
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_lmove_expired_destination() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "src", &["a"]);
        set_expired_list(&mut ctx, "dst", &["old"]);
        let result = lmove(
            &mut ctx,
            &[bulk("src"), bulk("dst"), bulk("LEFT"), bulk("RIGHT")],
        )
        .unwrap();
        assert_eq!(result, bulk("a"));
        assert_eq!(
            llen(&mut ctx, &[bulk("dst")]).unwrap(),
            RespValue::Integer(1)
        );
    }

    #[test]
    fn test_lmove_multiple_moves() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "src", &["a", "b", "c", "d"]);
        lmove(
            &mut ctx,
            &[bulk("src"), bulk("dst"), bulk("LEFT"), bulk("RIGHT")],
        )
        .unwrap();
        lmove(
            &mut ctx,
            &[bulk("src"), bulk("dst"), bulk("LEFT"), bulk("RIGHT")],
        )
        .unwrap();
        assert_eq!(
            llen(&mut ctx, &[bulk("src")]).unwrap(),
            RespValue::Integer(2)
        );
        assert_eq!(
            llen(&mut ctx, &[bulk("dst")]).unwrap(),
            RespValue::Integer(2)
        );
        let result = lrange(&mut ctx, &[bulk("dst"), bulk("0"), bulk("-1")]).unwrap();
        if let RespValue::Array(arr) = result {
            let vals: Vec<&str> = arr.iter().map(|v| v.as_str().unwrap()).collect();
            assert_eq!(vals, vec!["a", "b"]);
        } else {
            panic!("Expected Array");
        }
    }

    // ------------------------------------------------------------------
    // LMPOP additional tests (need 12 more to reach 20)
    // ------------------------------------------------------------------

    #[test]
    fn test_lmpop_expired_key_skipped() {
        let mut ctx = make_ctx();
        set_expired_list(&mut ctx, "exp", &["a", "b"]);
        rpush_elements(&mut ctx, "good", &["x", "y"]);
        let result = lmpop(
            &mut ctx,
            &[bulk("2"), bulk("exp"), bulk("good"), bulk("LEFT")],
        )
        .unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr[0], bulk("good"));
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_lmpop_wrong_arity_too_few() {
        let mut ctx = make_ctx();
        assert!(matches!(
            lmpop(&mut ctx, &[bulk("1")]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_lmpop_numkeys_zero() {
        let mut ctx = make_ctx();
        assert!(lmpop(&mut ctx, &[bulk("0"), bulk("LEFT")]).is_err());
    }

    #[test]
    fn test_lmpop_negative_numkeys() {
        let mut ctx = make_ctx();
        assert!(lmpop(&mut ctx, &[bulk("-1"), bulk("key"), bulk("LEFT")]).is_err());
    }

    #[test]
    fn test_lmpop_invalid_direction() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a"]);
        assert!(matches!(
            lmpop(&mut ctx, &[bulk("1"), bulk("k"), bulk("UP")]),
            Err(CommandError::SyntaxError)
        ));
    }

    #[test]
    fn test_lmpop_negative_count_error() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a"]);
        assert!(lmpop(
            &mut ctx,
            &[
                bulk("1"),
                bulk("k"),
                bulk("LEFT"),
                bulk("COUNT"),
                bulk("-1")
            ]
        )
        .is_err());
    }

    #[test]
    fn test_lmpop_zero_count_error() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a"]);
        assert!(lmpop(
            &mut ctx,
            &[bulk("1"), bulk("k"), bulk("LEFT"), bulk("COUNT"), bulk("0")]
        )
        .is_err());
    }

    #[test]
    fn test_lmpop_return_type_found() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a"]);
        let result = lmpop(&mut ctx, &[bulk("1"), bulk("k"), bulk("LEFT")]).unwrap();
        assert!(matches!(result, RespValue::Array(_)));
    }

    #[test]
    fn test_lmpop_return_type_not_found() {
        let mut ctx = make_ctx();
        let result = lmpop(&mut ctx, &[bulk("1"), bulk("nokey"), bulk("LEFT")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_lmpop_single_key_left() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a", "b", "c"]);
        let result = lmpop(
            &mut ctx,
            &[bulk("1"), bulk("k"), bulk("LEFT"), bulk("COUNT"), bulk("2")],
        )
        .unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr[0], bulk("k"));
            if let RespValue::Array(elements) = &arr[1] {
                assert_eq!(elements[0], bulk("a"));
                assert_eq!(elements[1], bulk("b"));
            } else {
                panic!("Expected inner Array");
            }
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_lmpop_deletes_empty_list() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "k", &["a"]);
        lmpop(&mut ctx, &[bulk("1"), bulk("k"), bulk("LEFT")]).unwrap();
        assert_eq!(llen(&mut ctx, &[bulk("k")]).unwrap(), RespValue::Integer(0));
    }

    #[test]
    fn test_lmpop_special_characters_in_key_and_value() {
        let mut ctx = make_ctx();
        rpush_elements(&mut ctx, "mykey", &["🦀", "hello\nworld"]);
        let result = lmpop(&mut ctx, &[bulk("1"), bulk("mykey"), bulk("LEFT")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr[0], bulk("mykey"));
            if let RespValue::Array(elements) = &arr[1] {
                assert_eq!(elements[0], bulk("🦀"));
            } else {
                panic!("Expected inner Array");
            }
        } else {
            panic!("Expected Array");
        }
    }
}

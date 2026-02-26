//! Hash commands: HGET, HSET, HSETNX, HDEL, HEXISTS, HGETALL, HMSET, HMGET,
//! HINCRBY, HINCRBYFLOAT, HKEYS, HVALS, HLEN, HSCAN, HSTRLEN, HRANDFIELD

use crate::{CommandContext, CommandError, CommandResult};
use bytes::Bytes;
use ferris_core::{Entry, RedisValue, UpdateAction};
use ferris_protocol::RespValue;
use std::collections::HashMap;

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
fn get_bytes(arg: &RespValue) -> Result<Bytes, CommandError> {
    arg.as_bytes()
        .cloned()
        .or_else(|| arg.as_str().map(|s| Bytes::from(s.to_owned())))
        .ok_or_else(|| CommandError::InvalidArgument("invalid argument".to_string()))
}

/// Extract a hash from the store, handling expiry and type checks.
/// Returns Ok(Some(hash)) if key exists with a hash value,
/// Ok(None) if key doesn't exist or is expired,
/// Err(WrongType) if key holds a different type.
fn get_hash_readonly(
    ctx: &CommandContext,
    key: &[u8],
) -> Result<Option<HashMap<Bytes, Bytes>>, CommandError> {
    let db = ctx.store().database(ctx.selected_db());
    match db.get(key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(key);
                Ok(None)
            } else {
                match entry.value {
                    RedisValue::Hash(h) => Ok(Some(h)),
                    _ => Err(CommandError::WrongType),
                }
            }
        }
        None => Ok(None),
    }
}

/// Extract a mutable hash from the store for write operations.
/// Returns the HashMap (empty if key doesn't exist or is expired).
/// Err(WrongType) if key holds a different type.
fn get_hash_for_write(
    ctx: &CommandContext,
    key: &[u8],
) -> Result<HashMap<Bytes, Bytes>, CommandError> {
    let db = ctx.store().database(ctx.selected_db());
    match db.get(key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(key);
                Ok(HashMap::new())
            } else {
                match entry.value {
                    RedisValue::Hash(h) => Ok(h),
                    _ => Err(CommandError::WrongType),
                }
            }
        }
        None => Ok(HashMap::new()),
    }
}

/// HGET key field
///
/// Returns the value associated with field in the hash stored at key.
///
/// Time complexity: O(1)
pub fn hget(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("HGET".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let field = get_bytes(&args[1])?;

    match get_hash_readonly(ctx, &key)? {
        Some(hash) => match hash.get(&field) {
            Some(val) => Ok(RespValue::BulkString(val.clone())),
            None => Ok(RespValue::Null),
        },
        None => Ok(RespValue::Null),
    }
}

/// HSET key field value [field value ...]
///
/// Sets field(s) in the hash stored at key. Returns the number of new fields added.
///
/// Time complexity: O(N) where N is the number of field-value pairs
pub fn hset(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 3 || (args.len() - 1) % 2 != 0 {
        return Err(CommandError::WrongArity("HSET".to_string()));
    }

    let key = get_bytes(&args[0])?;

    // Pre-parse all field-value pairs
    let pairs: Vec<(Bytes, Bytes)> = args[1..]
        .chunks(2)
        .map(|chunk| Ok((get_bytes(&chunk[0])?, get_bytes(&chunk[1])?)))
        .collect::<Result<_, CommandError>>()?;

    // Calculate memory delta: for each new field, we add (field_len + value_len + 48)
    // For existing fields, we add (new_value_len - old_value_len)
    // We'll track this inside the closure

    let db = ctx.store().database(ctx.selected_db());

    // Use atomic update to avoid separate get + set (single lock acquisition)
    let result = db.update(key.clone(), |entry| {
        match entry {
            Some(e) => {
                // Key exists - modify hash in place
                match &mut e.value {
                    RedisValue::Hash(hash) => {
                        let mut memory_delta: isize = 0;
                        // Track fields we've already counted as new in this call
                        let original_len = hash.len();

                        // Pre-allocate capacity for new fields to avoid reallocation
                        // We reserve space for the pairs we're adding, even if some might be updates
                        // This is a safe over-estimate that prevents mid-insert reallocation
                        hash.reserve(pairs.len());

                        for (field, value) in &pairs {
                            if let Some(old_value) = hash.get(field) {
                                // Field exists - calculate value size change
                                memory_delta += (value.len() as isize) - (old_value.len() as isize);
                            } else {
                                // New field - add full overhead
                                memory_delta += (field.len() + value.len() + 48) as isize;
                            }
                            hash.insert(field.clone(), value.clone());
                        }

                        // Count new fields as difference in hash size
                        let new_fields = (hash.len() - original_len) as i64;

                        (UpdateAction::KeepWithDelta(memory_delta), Ok(new_fields))
                    }
                    _ => (UpdateAction::Keep, Err(CommandError::WrongType)),
                }
            }
            None => {
                // Key doesn't exist - create new hash
                let mut hash = HashMap::with_capacity(pairs.len());
                for (field, value) in &pairs {
                    hash.insert(field.clone(), value.clone());
                }
                // Duplicate fields in args mean fewer actual new fields
                let new_fields = hash.len() as i64;
                (
                    UpdateAction::Set(Entry::new(RedisValue::Hash(hash))),
                    Ok(new_fields),
                )
            }
        }
    });

    let new_fields = result?;

    // Propagate to AOF and replication
    ctx.propagate_args(args);

    Ok(RespValue::Integer(new_fields))
}

/// HSETNX key field value
///
/// Sets field in the hash stored at key, only if field does not yet exist.
/// Returns 1 if field was set, 0 if field already existed.
///
/// Time complexity: O(1)
pub fn hsetnx(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongArity("HSETNX".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let field = get_bytes(&args[1])?;
    let value = get_bytes(&args[2])?;

    let db = ctx.store().database(ctx.selected_db());

    // Use atomic update
    let (was_set, result) = db.update(key.clone(), |entry| {
        match entry {
            Some(e) => match &mut e.value {
                RedisValue::Hash(hash) => {
                    if hash.contains_key(&field) {
                        (UpdateAction::Keep, (false, Ok(())))
                    } else {
                        let memory_delta = (field.len() + value.len() + 48) as isize;
                        hash.insert(field.clone(), value.clone());
                        (UpdateAction::KeepWithDelta(memory_delta), (true, Ok(())))
                    }
                }
                _ => (UpdateAction::Keep, (false, Err(CommandError::WrongType))),
            },
            None => {
                // Create new hash with single field
                let mut hash = HashMap::with_capacity(1);
                hash.insert(field.clone(), value.clone());
                (
                    UpdateAction::Set(Entry::new(RedisValue::Hash(hash))),
                    (true, Ok(())),
                )
            }
        }
    });

    result?;

    if was_set {
        ctx.propagate_args(args);
        Ok(RespValue::Integer(1))
    } else {
        Ok(RespValue::Integer(0))
    }
}

/// HDEL key field [field ...]
///
/// Removes the specified fields from the hash stored at key.
/// Returns the number of fields removed.
///
/// Time complexity: O(N) where N is the number of fields to remove
pub fn hdel(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("HDEL".to_string()));
    }

    let key = get_bytes(&args[0])?;

    // Pre-parse fields to delete
    let fields: Vec<Bytes> = args[1..].iter().map(get_bytes).collect::<Result<_, _>>()?;

    let db = ctx.store().database(ctx.selected_db());

    // Use atomic update
    let result: Result<i64, CommandError> = db.update(key.clone(), |entry| {
        match entry {
            Some(e) => match &mut e.value {
                RedisValue::Hash(hash) => {
                    if hash.is_empty() {
                        return (UpdateAction::Keep, Ok(0));
                    }

                    let mut removed = 0i64;
                    let mut memory_delta: isize = 0;

                    for field in &fields {
                        if let Some(old_value) = hash.remove(field) {
                            // Subtract memory for removed field
                            memory_delta -= (field.len() + old_value.len() + 48) as isize;
                            removed += 1;
                        }
                    }

                    let action = if hash.is_empty() {
                        UpdateAction::Delete
                    } else {
                        UpdateAction::KeepWithDelta(memory_delta)
                    };

                    (action, Ok(removed))
                }
                _ => (UpdateAction::Keep, Err(CommandError::WrongType)),
            },
            None => (UpdateAction::Keep, Ok(0)),
        }
    });

    let removed = result?;

    if removed > 0 {
        ctx.propagate_args(args);
    }

    Ok(RespValue::Integer(removed))
}

/// HEXISTS key field
///
/// Returns if field is an existing field in the hash stored at key.
/// Returns 1 if the field exists, 0 otherwise.
///
/// Time complexity: O(1)
pub fn hexists(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("HEXISTS".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let field = get_bytes(&args[1])?;

    match get_hash_readonly(ctx, &key)? {
        Some(hash) => {
            if hash.contains_key(&field) {
                Ok(RespValue::Integer(1))
            } else {
                Ok(RespValue::Integer(0))
            }
        }
        None => Ok(RespValue::Integer(0)),
    }
}

/// HGETALL key
///
/// Returns all fields and values of the hash stored at key.
/// Returns an array of alternating field, value pairs.
///
/// Time complexity: O(N) where N is the size of the hash
pub fn hgetall(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("HGETALL".to_string()));
    }

    let key = get_bytes(&args[0])?;

    match get_hash_readonly(ctx, &key)? {
        Some(hash) => {
            let mut result = Vec::with_capacity(hash.len() * 2);
            // Sort keys for deterministic output
            let mut keys: Vec<&Bytes> = hash.keys().collect();
            keys.sort();
            for k in keys {
                result.push(RespValue::BulkString(k.clone()));
                result.push(RespValue::BulkString(hash[k].clone()));
            }
            Ok(RespValue::Array(result))
        }
        None => Ok(RespValue::Array(vec![])),
    }
}

/// HMSET key field value [field value ...]
///
/// Sets the specified fields to their respective values in the hash stored at key.
/// Returns OK.
///
/// Time complexity: O(N) where N is the number of field-value pairs
pub fn hmset(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 3 || (args.len() - 1) % 2 != 0 {
        return Err(CommandError::WrongArity("HMSET".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let mut hash = get_hash_for_write(ctx, &key)?;

    let pairs = &args[1..];
    for chunk in pairs.chunks(2) {
        let field = get_bytes(&chunk[0])?;
        let value = get_bytes(&chunk[1])?;
        hash.insert(field, value);
    }

    let db = ctx.store().database(ctx.selected_db());
    db.set(key.clone(), Entry::new(RedisValue::Hash(hash)));

    // Propagate to AOF and replication
    ctx.propagate_args(args);

    Ok(RespValue::ok())
}

/// HMGET key field [field ...]
///
/// Returns the values associated with the specified fields in the hash.
/// For every field that does not exist, nil is returned.
///
/// Time complexity: O(N) where N is the number of fields
pub fn hmget(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("HMGET".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let hash = get_hash_readonly(ctx, &key)?;

    let mut results = Vec::with_capacity(args.len() - 1);
    for arg in &args[1..] {
        let field = get_bytes(arg)?;
        match &hash {
            Some(h) => match h.get(&field) {
                Some(val) => results.push(RespValue::BulkString(val.clone())),
                None => results.push(RespValue::Null),
            },
            None => results.push(RespValue::Null),
        }
    }

    Ok(RespValue::Array(results))
}

/// HINCRBY key field increment
///
/// Increments the number stored at field in the hash by increment.
/// If key does not exist, a new hash is created. If field does not exist,
/// the value is set to 0 before the operation.
///
/// Time complexity: O(1)
pub fn hincrby(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongArity("HINCRBY".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let field = get_bytes(&args[1])?;
    let increment = parse_int(&args[2])?;

    let mut hash = get_hash_for_write(ctx, &key)?;

    let current = match hash.get(&field) {
        Some(val) => {
            let s = std::str::from_utf8(val).map_err(|_| CommandError::NotAnInteger)?;
            s.parse::<i64>().map_err(|_| CommandError::NotAnInteger)?
        }
        None => 0,
    };

    let new_value = current
        .checked_add(increment)
        .ok_or(CommandError::NotAnInteger)?;

    hash.insert(field, Bytes::from(new_value.to_string()));

    let db = ctx.store().database(ctx.selected_db());
    db.set(key.clone(), Entry::new(RedisValue::Hash(hash)));

    // Propagate to AOF and replication
    ctx.propagate_args(args);

    Ok(RespValue::Integer(new_value))
}

/// HINCRBYFLOAT key field increment
///
/// Increment the specified field of a hash stored at key by the floating point
/// number stored at increment. If the field does not exist, it is set to 0
/// before performing the operation.
///
/// Time complexity: O(1)
pub fn hincrbyfloat(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongArity("HINCRBYFLOAT".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let field = get_bytes(&args[1])?;
    let increment = parse_float(&args[2])?;

    let mut hash = get_hash_for_write(ctx, &key)?;

    let current = match hash.get(&field) {
        Some(val) => {
            let s = std::str::from_utf8(val).map_err(|_| CommandError::NotAFloat)?;
            s.parse::<f64>().map_err(|_| CommandError::NotAFloat)?
        }
        None => 0.0,
    };

    let new_value = current + increment;
    if new_value.is_nan() || new_value.is_infinite() {
        return Err(CommandError::NotAFloat);
    }

    let formatted = if new_value.fract() == 0.0 {
        format!("{}", new_value as i64)
    } else {
        format!("{new_value}")
    };

    hash.insert(field, Bytes::from(formatted.clone()));

    let db = ctx.store().database(ctx.selected_db());
    db.set(key.clone(), Entry::new(RedisValue::Hash(hash)));

    // Propagate to AOF and replication
    ctx.propagate_args(args);

    Ok(RespValue::BulkString(Bytes::from(formatted)))
}

/// HKEYS key
///
/// Returns all field names in the hash stored at key.
///
/// Time complexity: O(N) where N is the size of the hash
pub fn hkeys(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("HKEYS".to_string()));
    }

    let key = get_bytes(&args[0])?;

    match get_hash_readonly(ctx, &key)? {
        Some(hash) => {
            let mut keys: Vec<&Bytes> = hash.keys().collect();
            keys.sort();
            let result: Vec<RespValue> = keys
                .into_iter()
                .map(|k| RespValue::BulkString(k.clone()))
                .collect();
            Ok(RespValue::Array(result))
        }
        None => Ok(RespValue::Array(vec![])),
    }
}

/// HVALS key
///
/// Returns all values in the hash stored at key.
///
/// Time complexity: O(N) where N is the size of the hash
pub fn hvals(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("HVALS".to_string()));
    }

    let key = get_bytes(&args[0])?;

    match get_hash_readonly(ctx, &key)? {
        Some(hash) => {
            // Sort by keys for deterministic output, then return values
            let mut keys: Vec<&Bytes> = hash.keys().collect();
            keys.sort();
            let result: Vec<RespValue> = keys
                .into_iter()
                .map(|k| RespValue::BulkString(hash[k].clone()))
                .collect();
            Ok(RespValue::Array(result))
        }
        None => Ok(RespValue::Array(vec![])),
    }
}

/// HLEN key
///
/// Returns the number of fields contained in the hash stored at key.
///
/// Time complexity: O(1)
pub fn hlen(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("HLEN".to_string()));
    }

    let key = get_bytes(&args[0])?;

    match get_hash_readonly(ctx, &key)? {
        Some(hash) => Ok(RespValue::Integer(hash.len() as i64)),
        None => Ok(RespValue::Integer(0)),
    }
}

/// HSCAN key cursor [MATCH pattern] [COUNT count]
///
/// Incrementally iterate hash fields and associated values.
///
/// Time complexity: O(1) for every call, O(N) for full iteration
pub fn hscan(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("HSCAN".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let cursor = parse_int(&args[1])? as usize;

    let mut pattern = "*";
    let mut count = 10usize;

    // Parse options
    let mut i = 2;
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
            _ => {}
        }
        i += 1;
    }

    match get_hash_readonly(ctx, &key)? {
        Some(hash) => {
            // Collect and sort fields for deterministic iteration
            let mut fields: Vec<(&Bytes, &Bytes)> = hash.iter().collect();
            fields.sort_by(|a, b| a.0.cmp(b.0));

            // Filter by pattern
            let filtered: Vec<(&Bytes, &Bytes)> = if pattern == "*" {
                fields
            } else {
                fields
                    .into_iter()
                    .filter(|(k, _)| {
                        let field_str = String::from_utf8_lossy(k);
                        glob_match::glob_match(pattern, &field_str)
                    })
                    .collect()
            };

            let total = filtered.len();
            let start = cursor.min(total);
            let end = (start + count).min(total);
            let next_cursor = if end >= total { 0 } else { end };

            let mut result_items = Vec::with_capacity((end - start) * 2);
            for (field, value) in filtered.into_iter().skip(start).take(count) {
                result_items.push(RespValue::BulkString(field.clone()));
                result_items.push(RespValue::BulkString(value.clone()));
            }

            Ok(RespValue::Array(vec![
                RespValue::BulkString(Bytes::from(next_cursor.to_string())),
                RespValue::Array(result_items),
            ]))
        }
        None => Ok(RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("0")),
            RespValue::Array(vec![]),
        ])),
    }
}

/// HSTRLEN key field
///
/// Returns the string length of the value associated with field in the hash
/// stored at key. If the key or field do not exist, 0 is returned.
///
/// Time complexity: O(1)
pub fn hstrlen(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("HSTRLEN".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let field = get_bytes(&args[1])?;

    match get_hash_readonly(ctx, &key)? {
        Some(hash) => match hash.get(&field) {
            Some(val) => Ok(RespValue::Integer(val.len() as i64)),
            None => Ok(RespValue::Integer(0)),
        },
        None => Ok(RespValue::Integer(0)),
    }
}

/// HRANDFIELD key [count [WITHVALUES]]
///
/// When called with just the key argument, return a random field from the hash.
/// With a positive count, return an array of distinct random fields (up to hash size).
/// With a negative count, return an array that may contain duplicate fields.
/// With WITHVALUES, include values after each field.
///
/// Time complexity: O(N) where N is the count
pub fn hrandfield(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("HRANDFIELD".to_string()));
    }

    let key = get_bytes(&args[0])?;

    let hash = match get_hash_readonly(ctx, &key)? {
        Some(h) => h,
        None => {
            // No count arg: return Null; with count arg: return empty array
            if args.len() >= 2 {
                return Ok(RespValue::Array(vec![]));
            }
            return Ok(RespValue::Null);
        }
    };

    if hash.is_empty() {
        if args.len() >= 2 {
            return Ok(RespValue::Array(vec![]));
        }
        return Ok(RespValue::Null);
    }

    // Collect fields in sorted order for reproducible random selection
    let mut fields: Vec<(&Bytes, &Bytes)> = hash.iter().collect();
    fields.sort_by(|a, b| a.0.cmp(b.0));

    // No count argument: return a single random field
    if args.len() < 2 {
        let idx = random_index(fields.len());
        return Ok(RespValue::BulkString(fields[idx].0.clone()));
    }

    let count = parse_int(&args[1])?;
    let with_values = if args.len() >= 3 {
        let opt = args[2].as_str().map(|s| s.to_uppercase());
        match opt.as_deref() {
            Some("WITHVALUES") => true,
            _ => return Err(CommandError::SyntaxError),
        }
    } else {
        false
    };

    if count == 0 {
        return Ok(RespValue::Array(vec![]));
    }

    if count > 0 {
        // Positive count: return unique fields (up to hash size)
        let n = (count as usize).min(fields.len());
        let selected = random_unique_indices(fields.len(), n);
        let mut result = Vec::with_capacity(if with_values { n * 2 } else { n });
        for idx in selected {
            result.push(RespValue::BulkString(fields[idx].0.clone()));
            if with_values {
                result.push(RespValue::BulkString(fields[idx].1.clone()));
            }
        }
        Ok(RespValue::Array(result))
    } else {
        // Negative count: may contain duplicates
        let n = count.unsigned_abs() as usize;
        let mut result = Vec::with_capacity(if with_values { n * 2 } else { n });
        for _ in 0..n {
            let idx = random_index(fields.len());
            result.push(RespValue::BulkString(fields[idx].0.clone()));
            if with_values {
                result.push(RespValue::BulkString(fields[idx].1.clone()));
            }
        }
        Ok(RespValue::Array(result))
    }
}

/// Generate a random index in range [0, len)
fn random_index(len: usize) -> usize {
    // Use a simple random source; rand::random gives uniform distribution
    rand::random::<usize>() % len
}

/// Select `n` unique random indices from [0, len)
fn random_unique_indices(len: usize, n: usize) -> Vec<usize> {
    if n >= len {
        return (0..len).collect();
    }

    // Fisher-Yates partial shuffle
    let mut indices: Vec<usize> = (0..len).collect();
    for i in 0..n {
        let j = i + (rand::random::<usize>() % (len - i));
        indices.swap(i, j);
    }
    indices.truncate(n);
    indices
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

    /// Helper: set up a hash with given field-value pairs
    fn setup_hash(ctx: &mut CommandContext, key: &str, pairs: &[(&str, &str)]) {
        let mut args = vec![bulk(key)];
        for (f, v) in pairs {
            args.push(bulk(f));
            args.push(bulk(v));
        }
        hset(ctx, &args).unwrap();
    }

    /// Helper: set up a string key (for wrong type tests)
    fn setup_string(ctx: &mut CommandContext, key: &str, val: &str) {
        let db = ctx.store().database(ctx.selected_db());
        db.set(
            Bytes::from(key.to_string()),
            Entry::new(RedisValue::String(Bytes::from(val.to_string()))),
        );
    }

    // ========================================================================
    // 1. test_hset_hget_basic
    // ========================================================================
    #[test]
    fn test_hset_hget_basic() {
        let mut ctx = make_ctx();

        let result = hset(&mut ctx, &[bulk("myhash"), bulk("field1"), bulk("Hello")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = hget(&mut ctx, &[bulk("myhash"), bulk("field1")]).unwrap();
        assert_eq!(result, bulk("Hello"));
    }

    // ========================================================================
    // 2. test_hset_multiple_fields
    // ========================================================================
    #[test]
    fn test_hset_multiple_fields() {
        let mut ctx = make_ctx();

        let result = hset(
            &mut ctx,
            &[
                bulk("myhash"),
                bulk("f1"),
                bulk("v1"),
                bulk("f2"),
                bulk("v2"),
                bulk("f3"),
                bulk("v3"),
            ],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(3));

        // Update existing + add new
        let result = hset(
            &mut ctx,
            &[
                bulk("myhash"),
                bulk("f1"),
                bulk("updated"),
                bulk("f4"),
                bulk("v4"),
            ],
        )
        .unwrap();
        // Only f4 is new
        assert_eq!(result, RespValue::Integer(1));

        let result = hget(&mut ctx, &[bulk("myhash"), bulk("f1")]).unwrap();
        assert_eq!(result, bulk("updated"));

        let result = hget(&mut ctx, &[bulk("myhash"), bulk("f4")]).unwrap();
        assert_eq!(result, bulk("v4"));
    }

    // ========================================================================
    // 3. test_hget_nonexistent_key
    // ========================================================================
    #[test]
    fn test_hget_nonexistent_key() {
        let mut ctx = make_ctx();
        let result = hget(&mut ctx, &[bulk("nokey"), bulk("field")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    // ========================================================================
    // 4. test_hget_nonexistent_field
    // ========================================================================
    #[test]
    fn test_hget_nonexistent_field() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "v1")]);

        let result = hget(&mut ctx, &[bulk("myhash"), bulk("nofield")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    // ========================================================================
    // 5. test_hsetnx_new_field
    // ========================================================================
    #[test]
    fn test_hsetnx_new_field() {
        let mut ctx = make_ctx();

        let result = hsetnx(&mut ctx, &[bulk("myhash"), bulk("field"), bulk("Hello")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = hget(&mut ctx, &[bulk("myhash"), bulk("field")]).unwrap();
        assert_eq!(result, bulk("Hello"));
    }

    // ========================================================================
    // 6. test_hsetnx_existing_field
    // ========================================================================
    #[test]
    fn test_hsetnx_existing_field() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("field", "Hello")]);

        let result = hsetnx(&mut ctx, &[bulk("myhash"), bulk("field"), bulk("World")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));

        // Value should remain unchanged
        let result = hget(&mut ctx, &[bulk("myhash"), bulk("field")]).unwrap();
        assert_eq!(result, bulk("Hello"));
    }

    // ========================================================================
    // 7. test_hdel_single
    // ========================================================================
    #[test]
    fn test_hdel_single() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "v1"), ("f2", "v2")]);

        let result = hdel(&mut ctx, &[bulk("myhash"), bulk("f1")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = hget(&mut ctx, &[bulk("myhash"), bulk("f1")]).unwrap();
        assert_eq!(result, RespValue::Null);

        // f2 should still exist
        let result = hget(&mut ctx, &[bulk("myhash"), bulk("f2")]).unwrap();
        assert_eq!(result, bulk("v2"));
    }

    // ========================================================================
    // 8. test_hdel_multiple
    // ========================================================================
    #[test]
    fn test_hdel_multiple() {
        let mut ctx = make_ctx();
        setup_hash(
            &mut ctx,
            "myhash",
            &[("f1", "v1"), ("f2", "v2"), ("f3", "v3")],
        );

        let result = hdel(&mut ctx, &[bulk("myhash"), bulk("f1"), bulk("f3")]).unwrap();
        assert_eq!(result, RespValue::Integer(2));

        let result = hlen(&mut ctx, &[bulk("myhash")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    // ========================================================================
    // 9. test_hdel_nonexistent
    // ========================================================================
    #[test]
    fn test_hdel_nonexistent() {
        let mut ctx = make_ctx();

        // Non-existent key
        let result = hdel(&mut ctx, &[bulk("nokey"), bulk("field")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));

        // Existing key, non-existent field
        setup_hash(&mut ctx, "myhash", &[("f1", "v1")]);
        let result = hdel(&mut ctx, &[bulk("myhash"), bulk("nofield")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    // ========================================================================
    // 10. test_hexists
    // ========================================================================
    #[test]
    fn test_hexists() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "v1")]);

        let result = hexists(&mut ctx, &[bulk("myhash"), bulk("f1")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = hexists(&mut ctx, &[bulk("myhash"), bulk("nofield")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));

        let result = hexists(&mut ctx, &[bulk("nokey"), bulk("f1")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    // ========================================================================
    // 11. test_hgetall
    // ========================================================================
    #[test]
    fn test_hgetall() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "v1"), ("f2", "v2")]);

        let result = hgetall(&mut ctx, &[bulk("myhash")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 4);
            // Sorted by field name: f1, v1, f2, v2
            assert_eq!(arr[0], bulk("f1"));
            assert_eq!(arr[1], bulk("v1"));
            assert_eq!(arr[2], bulk("f2"));
            assert_eq!(arr[3], bulk("v2"));
        } else {
            panic!("Expected array");
        }
    }

    // ========================================================================
    // 12. test_hgetall_empty
    // ========================================================================
    #[test]
    fn test_hgetall_empty() {
        let mut ctx = make_ctx();

        let result = hgetall(&mut ctx, &[bulk("nokey")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![]));
    }

    // ========================================================================
    // 13. test_hmset_hmget
    // ========================================================================
    #[test]
    fn test_hmset_hmget() {
        let mut ctx = make_ctx();

        let result = hmset(
            &mut ctx,
            &[
                bulk("myhash"),
                bulk("f1"),
                bulk("Hello"),
                bulk("f2"),
                bulk("World"),
            ],
        )
        .unwrap();
        assert_eq!(result, RespValue::ok());

        let result = hmget(&mut ctx, &[bulk("myhash"), bulk("f1"), bulk("f2")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], bulk("Hello"));
            assert_eq!(arr[1], bulk("World"));
        } else {
            panic!("Expected array");
        }
    }

    // ========================================================================
    // 14. test_hmget_missing_fields
    // ========================================================================
    #[test]
    fn test_hmget_missing_fields() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "v1")]);

        let result = hmget(
            &mut ctx,
            &[bulk("myhash"), bulk("f1"), bulk("nofield"), bulk("f1")],
        )
        .unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], bulk("v1"));
            assert_eq!(arr[1], RespValue::Null);
            assert_eq!(arr[2], bulk("v1"));
        } else {
            panic!("Expected array");
        }

        // Non-existent key
        let result = hmget(&mut ctx, &[bulk("nokey"), bulk("f1")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 1);
            assert_eq!(arr[0], RespValue::Null);
        } else {
            panic!("Expected array");
        }
    }

    // ========================================================================
    // 15. test_hincrby_new_field
    // ========================================================================
    #[test]
    fn test_hincrby_new_field() {
        let mut ctx = make_ctx();

        // HINCRBY on non-existent key and field
        let result = hincrby(&mut ctx, &[bulk("myhash"), bulk("counter"), bulk("5")]).unwrap();
        assert_eq!(result, RespValue::Integer(5));

        let result = hget(&mut ctx, &[bulk("myhash"), bulk("counter")]).unwrap();
        assert_eq!(result, bulk("5"));
    }

    // ========================================================================
    // 16. test_hincrby_existing
    // ========================================================================
    #[test]
    fn test_hincrby_existing() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("counter", "10")]);

        let result = hincrby(&mut ctx, &[bulk("myhash"), bulk("counter"), bulk("5")]).unwrap();
        assert_eq!(result, RespValue::Integer(15));

        let result = hincrby(&mut ctx, &[bulk("myhash"), bulk("counter"), bulk("-3")]).unwrap();
        assert_eq!(result, RespValue::Integer(12));
    }

    // ========================================================================
    // 17. test_hincrby_wrong_type
    // ========================================================================
    #[test]
    fn test_hincrby_wrong_type() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("field", "not_a_number")]);

        let result = hincrby(&mut ctx, &[bulk("myhash"), bulk("field"), bulk("1")]);
        assert!(matches!(result, Err(CommandError::NotAnInteger)));
    }

    // ========================================================================
    // 18. test_hincrbyfloat
    // ========================================================================
    #[test]
    fn test_hincrbyfloat() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("field", "10.5")]);

        let result = hincrbyfloat(&mut ctx, &[bulk("myhash"), bulk("field"), bulk("0.1")]).unwrap();
        if let RespValue::BulkString(b) = result {
            let val: f64 = std::str::from_utf8(&b).unwrap().parse().unwrap();
            assert!((val - 10.6).abs() < 0.0001);
        } else {
            panic!("Expected bulk string");
        }

        // HINCRBYFLOAT on new field
        let result =
            hincrbyfloat(&mut ctx, &[bulk("myhash"), bulk("newfield"), bulk("3.14")]).unwrap();
        if let RespValue::BulkString(b) = result {
            let val: f64 = std::str::from_utf8(&b).unwrap().parse().unwrap();
            assert!((val - 3.14).abs() < 0.0001);
        } else {
            panic!("Expected bulk string");
        }
    }

    // ========================================================================
    // 19. test_hkeys_hvals_hlen
    // ========================================================================
    #[test]
    fn test_hkeys_hvals_hlen() {
        let mut ctx = make_ctx();
        setup_hash(
            &mut ctx,
            "myhash",
            &[("f1", "v1"), ("f2", "v2"), ("f3", "v3")],
        );

        // HKEYS
        let result = hkeys(&mut ctx, &[bulk("myhash")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], bulk("f1"));
            assert_eq!(arr[1], bulk("f2"));
            assert_eq!(arr[2], bulk("f3"));
        } else {
            panic!("Expected array");
        }

        // HVALS (sorted by key)
        let result = hvals(&mut ctx, &[bulk("myhash")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], bulk("v1"));
            assert_eq!(arr[1], bulk("v2"));
            assert_eq!(arr[2], bulk("v3"));
        } else {
            panic!("Expected array");
        }

        // HLEN
        let result = hlen(&mut ctx, &[bulk("myhash")]).unwrap();
        assert_eq!(result, RespValue::Integer(3));

        // HLEN on nonexistent key
        let result = hlen(&mut ctx, &[bulk("nokey")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));

        // HKEYS on nonexistent key
        let result = hkeys(&mut ctx, &[bulk("nokey")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![]));

        // HVALS on nonexistent key
        let result = hvals(&mut ctx, &[bulk("nokey")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![]));
    }

    // ========================================================================
    // 20. test_hstrlen
    // ========================================================================
    #[test]
    fn test_hstrlen() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "HelloWorld")]);

        let result = hstrlen(&mut ctx, &[bulk("myhash"), bulk("f1")]).unwrap();
        assert_eq!(result, RespValue::Integer(10));

        // Non-existent field
        let result = hstrlen(&mut ctx, &[bulk("myhash"), bulk("nofield")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));

        // Non-existent key
        let result = hstrlen(&mut ctx, &[bulk("nokey"), bulk("f1")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    // ========================================================================
    // 21. test_hrandfield_basic
    // ========================================================================
    #[test]
    fn test_hrandfield_basic() {
        let mut ctx = make_ctx();
        setup_hash(
            &mut ctx,
            "myhash",
            &[("f1", "v1"), ("f2", "v2"), ("f3", "v3")],
        );

        // Without count: returns a single bulk string (one of the fields)
        let result = hrandfield(&mut ctx, &[bulk("myhash")]).unwrap();
        match &result {
            RespValue::BulkString(b) => {
                let s = std::str::from_utf8(b).unwrap();
                assert!(s == "f1" || s == "f2" || s == "f3", "Unexpected field: {s}");
            }
            _ => panic!("Expected bulk string, got {result:?}"),
        }

        // Non-existent key without count
        let result = hrandfield(&mut ctx, &[bulk("nokey")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    // ========================================================================
    // 22. test_hrandfield_with_count
    // ========================================================================
    #[test]
    fn test_hrandfield_with_count() {
        let mut ctx = make_ctx();
        setup_hash(
            &mut ctx,
            "myhash",
            &[("f1", "v1"), ("f2", "v2"), ("f3", "v3")],
        );

        // Positive count: unique fields
        let result = hrandfield(&mut ctx, &[bulk("myhash"), bulk("2")]).unwrap();
        if let RespValue::Array(arr) = &result {
            assert_eq!(arr.len(), 2);
            // All returned values should be valid fields
            for item in arr {
                if let RespValue::BulkString(b) = item {
                    let s = std::str::from_utf8(b).unwrap();
                    assert!(s == "f1" || s == "f2" || s == "f3");
                } else {
                    panic!("Expected bulk string in array");
                }
            }
            // Should be unique
            assert_ne!(arr[0], arr[1]);
        } else {
            panic!("Expected array");
        }

        // Positive count larger than hash size: returns all
        let result = hrandfield(&mut ctx, &[bulk("myhash"), bulk("10")]).unwrap();
        if let RespValue::Array(arr) = &result {
            assert_eq!(arr.len(), 3);
        } else {
            panic!("Expected array");
        }

        // Negative count: may have duplicates, returns abs(count) items
        let result = hrandfield(&mut ctx, &[bulk("myhash"), bulk("-5")]).unwrap();
        if let RespValue::Array(arr) = &result {
            assert_eq!(arr.len(), 5);
        } else {
            panic!("Expected array");
        }

        // Count 0: empty array
        let result = hrandfield(&mut ctx, &[bulk("myhash"), bulk("0")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![]));

        // WITHVALUES
        let result =
            hrandfield(&mut ctx, &[bulk("myhash"), bulk("2"), bulk("WITHVALUES")]).unwrap();
        if let RespValue::Array(arr) = &result {
            // 2 fields with values = 4 items
            assert_eq!(arr.len(), 4);
        } else {
            panic!("Expected array");
        }

        // Non-existent key with count
        let result = hrandfield(&mut ctx, &[bulk("nokey"), bulk("2")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![]));
    }

    // ========================================================================
    // 23. test_wrong_type_error
    // ========================================================================
    #[test]
    fn test_wrong_type_error() {
        let mut ctx = make_ctx();
        setup_string(&mut ctx, "mystring", "hello");

        assert!(matches!(
            hget(&mut ctx, &[bulk("mystring"), bulk("f1")]),
            Err(CommandError::WrongType)
        ));
        assert!(matches!(
            hset(&mut ctx, &[bulk("mystring"), bulk("f1"), bulk("v1")]),
            Err(CommandError::WrongType)
        ));
        assert!(matches!(
            hsetnx(&mut ctx, &[bulk("mystring"), bulk("f1"), bulk("v1")]),
            Err(CommandError::WrongType)
        ));
        assert!(matches!(
            hdel(&mut ctx, &[bulk("mystring"), bulk("f1")]),
            Err(CommandError::WrongType)
        ));
        assert!(matches!(
            hexists(&mut ctx, &[bulk("mystring"), bulk("f1")]),
            Err(CommandError::WrongType)
        ));
        assert!(matches!(
            hgetall(&mut ctx, &[bulk("mystring")]),
            Err(CommandError::WrongType)
        ));
        assert!(matches!(
            hmset(&mut ctx, &[bulk("mystring"), bulk("f1"), bulk("v1")]),
            Err(CommandError::WrongType)
        ));
        assert!(matches!(
            hmget(&mut ctx, &[bulk("mystring"), bulk("f1")]),
            Err(CommandError::WrongType)
        ));
        assert!(matches!(
            hincrby(&mut ctx, &[bulk("mystring"), bulk("f1"), bulk("1")]),
            Err(CommandError::WrongType)
        ));
        assert!(matches!(
            hincrbyfloat(&mut ctx, &[bulk("mystring"), bulk("f1"), bulk("1.0")]),
            Err(CommandError::WrongType)
        ));
        assert!(matches!(
            hkeys(&mut ctx, &[bulk("mystring")]),
            Err(CommandError::WrongType)
        ));
        assert!(matches!(
            hvals(&mut ctx, &[bulk("mystring")]),
            Err(CommandError::WrongType)
        ));
        assert!(matches!(
            hlen(&mut ctx, &[bulk("mystring")]),
            Err(CommandError::WrongType)
        ));
        assert!(matches!(
            hstrlen(&mut ctx, &[bulk("mystring"), bulk("f1")]),
            Err(CommandError::WrongType)
        ));
        assert!(matches!(
            hscan(&mut ctx, &[bulk("mystring"), bulk("0")]),
            Err(CommandError::WrongType)
        ));
        assert!(matches!(
            hrandfield(&mut ctx, &[bulk("mystring")]),
            Err(CommandError::WrongType)
        ));
    }

    // ========================================================================
    // 24. test_hscan_basic
    // ========================================================================
    #[test]
    fn test_hscan_basic() {
        let mut ctx = make_ctx();
        setup_hash(
            &mut ctx,
            "myhash",
            &[("f1", "v1"), ("f2", "v2"), ("f3", "v3"), ("other", "val")],
        );

        // Full scan with cursor 0
        let result = hscan(&mut ctx, &[bulk("myhash"), bulk("0")]).unwrap();
        if let RespValue::Array(outer) = &result {
            assert_eq!(outer.len(), 2);
            // cursor
            if let RespValue::BulkString(cursor) = &outer[0] {
                assert_eq!(cursor.as_ref(), b"0"); // All returned in one batch
            } else {
                panic!("Expected cursor as bulk string");
            }
            // items: alternating field, value
            if let RespValue::Array(items) = &outer[1] {
                assert_eq!(items.len(), 8); // 4 fields * 2
            } else {
                panic!("Expected items array");
            }
        } else {
            panic!("Expected outer array");
        }

        // HSCAN with MATCH pattern
        let result = hscan(
            &mut ctx,
            &[bulk("myhash"), bulk("0"), bulk("MATCH"), bulk("f*")],
        )
        .unwrap();
        if let RespValue::Array(outer) = &result {
            if let RespValue::Array(items) = &outer[1] {
                // f1, f2, f3 match "f*" -> 3 fields * 2 = 6 items
                assert_eq!(items.len(), 6);
            } else {
                panic!("Expected items array");
            }
        } else {
            panic!("Expected outer array");
        }

        // HSCAN with COUNT (small count to trigger pagination)
        let result = hscan(
            &mut ctx,
            &[bulk("myhash"), bulk("0"), bulk("COUNT"), bulk("2")],
        )
        .unwrap();
        if let RespValue::Array(outer) = &result {
            if let RespValue::BulkString(cursor) = &outer[0] {
                // With 4 items and count 2, cursor should be "2"
                assert_eq!(cursor.as_ref(), b"2");
            }
            if let RespValue::Array(items) = &outer[1] {
                assert_eq!(items.len(), 4); // 2 fields * 2
            } else {
                panic!("Expected items array");
            }
        } else {
            panic!("Expected outer array");
        }

        // HSCAN on non-existent key
        let result = hscan(&mut ctx, &[bulk("nokey"), bulk("0")]).unwrap();
        if let RespValue::Array(outer) = &result {
            assert_eq!(outer.len(), 2);
            if let RespValue::BulkString(cursor) = &outer[0] {
                assert_eq!(cursor.as_ref(), b"0");
            }
            if let RespValue::Array(items) = &outer[1] {
                assert!(items.is_empty());
            }
        } else {
            panic!("Expected outer array");
        }
    }

    // ========================================================================
    // Additional tests for comprehensive coverage
    // ========================================================================

    #[test]
    fn test_hset_wrong_arity() {
        let mut ctx = make_ctx();

        // Too few args
        assert!(matches!(
            hset(&mut ctx, &[bulk("key")]),
            Err(CommandError::WrongArity(_))
        ));

        // Odd number of field-value args
        assert!(matches!(
            hset(&mut ctx, &[bulk("key"), bulk("f1")]),
            Err(CommandError::WrongArity(_))
        ));

        assert!(matches!(
            hset(&mut ctx, &[bulk("key"), bulk("f1"), bulk("v1"), bulk("f2")]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_hdel_removes_empty_hash() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "v1")]);

        hdel(&mut ctx, &[bulk("myhash"), bulk("f1")]).unwrap();

        // After deleting the only field, the key should be removed
        let db = ctx.store().database(ctx.selected_db());
        assert!(db.get(&Bytes::from("myhash")).is_none());
    }

    #[test]
    fn test_hset_overwrite_existing() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "v1")]);

        // Overwrite: should return 0 (no new fields)
        let result = hset(&mut ctx, &[bulk("myhash"), bulk("f1"), bulk("new_value")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));

        let result = hget(&mut ctx, &[bulk("myhash"), bulk("f1")]).unwrap();
        assert_eq!(result, bulk("new_value"));
    }

    #[test]
    fn test_hmset_wrong_arity() {
        let mut ctx = make_ctx();
        assert!(matches!(
            hmset(&mut ctx, &[bulk("key")]),
            Err(CommandError::WrongArity(_))
        ));
        assert!(matches!(
            hmset(&mut ctx, &[bulk("key"), bulk("f1")]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_hmget_wrong_arity() {
        let mut ctx = make_ctx();
        assert!(matches!(
            hmget(&mut ctx, &[bulk("key")]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_hincrby_overflow() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("field", "9223372036854775807")]); // i64::MAX

        let result = hincrby(&mut ctx, &[bulk("myhash"), bulk("field"), bulk("1")]);
        assert!(matches!(result, Err(CommandError::NotAnInteger)));
    }

    #[test]
    fn test_hincrbyfloat_nan() {
        let mut ctx = make_ctx();
        // Infinity + (-Infinity) = NaN
        setup_hash(&mut ctx, "myhash", &[("field", "inf")]);

        let result = hincrbyfloat(&mut ctx, &[bulk("myhash"), bulk("field"), bulk("-inf")]);
        assert!(matches!(result, Err(CommandError::NotAFloat)));
    }

    #[test]
    fn test_hrandfield_empty_hash_no_count() {
        let mut ctx = make_ctx();
        // Create an empty hash by adding then removing a field
        setup_hash(&mut ctx, "myhash", &[("f1", "v1")]);
        hdel(&mut ctx, &[bulk("myhash"), bulk("f1")]).unwrap();

        // Key doesn't exist anymore (empty hash is deleted)
        let result = hrandfield(&mut ctx, &[bulk("myhash")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_hrandfield_negative_count_with_values() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "v1"), ("f2", "v2")]);

        let result =
            hrandfield(&mut ctx, &[bulk("myhash"), bulk("-3"), bulk("WITHVALUES")]).unwrap();
        if let RespValue::Array(arr) = &result {
            // -3 means 3 items (may repeat), with values = 6 elements
            assert_eq!(arr.len(), 6);
            // Verify structure: field, value, field, value, ...
            for i in (0..arr.len()).step_by(2) {
                if let RespValue::BulkString(field) = &arr[i] {
                    let s = std::str::from_utf8(field).unwrap();
                    assert!(s == "f1" || s == "f2");
                    // Next element should be the corresponding value
                    if let RespValue::BulkString(val) = &arr[i + 1] {
                        let v = std::str::from_utf8(val).unwrap();
                        if s == "f1" {
                            assert_eq!(v, "v1");
                        } else {
                            assert_eq!(v, "v2");
                        }
                    }
                }
            }
        } else {
            panic!("Expected array");
        }
    }

    // ========================================================================
    // HSCAN option tests
    // ========================================================================

    #[test]
    fn test_hscan_match_and_count_combined() {
        let mut ctx = make_ctx();
        setup_hash(
            &mut ctx,
            "myhash",
            &[
                ("alpha1", "a1"),
                ("alpha2", "a2"),
                ("alpha3", "a3"),
                ("beta1", "b1"),
                ("beta2", "b2"),
            ],
        );

        // HSCAN with MATCH alpha* and COUNT 2
        let result = hscan(
            &mut ctx,
            &[
                bulk("myhash"),
                bulk("0"),
                bulk("MATCH"),
                bulk("alpha*"),
                bulk("COUNT"),
                bulk("2"),
            ],
        )
        .unwrap();
        if let RespValue::Array(outer) = &result {
            assert_eq!(outer.len(), 2);
            // With COUNT 2 and 3 matching fields, first batch returns 2 fields
            if let RespValue::Array(items) = &outer[1] {
                assert_eq!(items.len(), 4); // 2 fields * 2 (field+value)
            } else {
                panic!("Expected items array");
            }
            // Cursor should be non-zero since there are more matching fields
            if let RespValue::BulkString(cursor) = &outer[0] {
                assert_eq!(cursor.as_ref(), b"2");
            } else {
                panic!("Expected cursor as bulk string");
            }
        } else {
            panic!("Expected outer array");
        }
    }

    #[test]
    fn test_hscan_full_iteration() {
        let mut ctx = make_ctx();
        setup_hash(
            &mut ctx,
            "myhash",
            &[
                ("f1", "v1"),
                ("f2", "v2"),
                ("f3", "v3"),
                ("f4", "v4"),
                ("f5", "v5"),
            ],
        );

        let mut all_fields: Vec<String> = Vec::new();
        let mut all_values: Vec<String> = Vec::new();
        let mut cursor = "0".to_string();

        // Iterate with COUNT 2 until cursor returns to 0
        loop {
            let result = hscan(
                &mut ctx,
                &[bulk("myhash"), bulk(&cursor), bulk("COUNT"), bulk("2")],
            )
            .unwrap();
            if let RespValue::Array(outer) = &result {
                if let RespValue::BulkString(c) = &outer[0] {
                    cursor = std::str::from_utf8(c).unwrap().to_string();
                }
                if let RespValue::Array(items) = &outer[1] {
                    for pair in items.chunks(2) {
                        if let (RespValue::BulkString(f), RespValue::BulkString(v)) =
                            (&pair[0], &pair[1])
                        {
                            all_fields.push(std::str::from_utf8(f).unwrap().to_string());
                            all_values.push(std::str::from_utf8(v).unwrap().to_string());
                        }
                    }
                }
            }
            if cursor == "0" {
                break;
            }
        }

        // Verify we collected all 5 field-value pairs
        assert_eq!(all_fields.len(), 5);
        assert_eq!(all_values.len(), 5);
        let mut sorted_fields = all_fields.clone();
        sorted_fields.sort();
        assert_eq!(sorted_fields, vec!["f1", "f2", "f3", "f4", "f5"]);
        let mut sorted_values = all_values.clone();
        sorted_values.sort();
        assert_eq!(sorted_values, vec!["v1", "v2", "v3", "v4", "v5"]);
    }

    #[test]
    fn test_hscan_nonexistent_key() {
        let mut ctx = make_ctx();

        let result = hscan(&mut ctx, &[bulk("nokey"), bulk("0")]).unwrap();
        if let RespValue::Array(outer) = &result {
            assert_eq!(outer.len(), 2);
            if let RespValue::BulkString(cursor) = &outer[0] {
                assert_eq!(cursor.as_ref(), b"0");
            } else {
                panic!("Expected cursor as bulk string");
            }
            if let RespValue::Array(items) = &outer[1] {
                assert!(items.is_empty());
            } else {
                panic!("Expected empty items array");
            }
        } else {
            panic!("Expected outer array");
        }
    }

    #[test]
    fn test_hscan_no_match_results() {
        let mut ctx = make_ctx();
        setup_hash(
            &mut ctx,
            "myhash",
            &[("f1", "v1"), ("f2", "v2"), ("f3", "v3")],
        );

        // MATCH with pattern that matches nothing
        let result = hscan(
            &mut ctx,
            &[bulk("myhash"), bulk("0"), bulk("MATCH"), bulk("nomatch*")],
        )
        .unwrap();
        if let RespValue::Array(outer) = &result {
            assert_eq!(outer.len(), 2);
            if let RespValue::BulkString(cursor) = &outer[0] {
                assert_eq!(cursor.as_ref(), b"0");
            } else {
                panic!("Expected cursor as bulk string");
            }
            if let RespValue::Array(items) = &outer[1] {
                assert!(items.is_empty());
            } else {
                panic!("Expected empty items array");
            }
        } else {
            panic!("Expected outer array");
        }
    }

    #[test]
    fn test_hscan_continued_cursor() {
        let mut ctx = make_ctx();
        setup_hash(
            &mut ctx,
            "myhash",
            &[("a", "1"), ("b", "2"), ("c", "3"), ("d", "4"), ("e", "5")],
        );

        // First call with COUNT 2
        let result = hscan(
            &mut ctx,
            &[bulk("myhash"), bulk("0"), bulk("COUNT"), bulk("2")],
        )
        .unwrap();
        let next_cursor;
        if let RespValue::Array(outer) = &result {
            if let RespValue::BulkString(c) = &outer[0] {
                next_cursor = std::str::from_utf8(c).unwrap().to_string();
                // Cursor should be > 0 since there are more items
                assert_ne!(next_cursor, "0");
            } else {
                panic!("Expected cursor as bulk string");
            }
            if let RespValue::Array(items) = &outer[1] {
                assert_eq!(items.len(), 4); // 2 fields * 2
            } else {
                panic!("Expected items array");
            }
        } else {
            panic!("Expected outer array");
        }

        // Feed cursor back for more results
        let result = hscan(
            &mut ctx,
            &[bulk("myhash"), bulk(&next_cursor), bulk("COUNT"), bulk("2")],
        )
        .unwrap();
        if let RespValue::Array(outer) = &result {
            if let RespValue::Array(items) = &outer[1] {
                assert_eq!(items.len(), 4); // 2 more fields * 2
            } else {
                panic!("Expected items array");
            }
        } else {
            panic!("Expected outer array");
        }
    }

    #[test]
    fn test_hscan_invalid_cursor() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "v1")]);

        // Non-numeric cursor should fail
        let result = hscan(&mut ctx, &[bulk("myhash"), bulk("notanumber")]);
        assert!(matches!(result, Err(CommandError::NotAnInteger)));
    }

    // ========================================================================
    // HRANDFIELD edge case tests
    // ========================================================================

    #[test]
    fn test_hrandfield_withvalues_without_count() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "v1"), ("f2", "v2")]);

        // HRANDFIELD key WITHVALUES (without count) — args[1] is "WITHVALUES"
        // which is not a valid integer, so parse_int should fail
        let result = hrandfield(&mut ctx, &[bulk("myhash"), bulk("WITHVALUES")]);
        assert!(matches!(result, Err(CommandError::NotAnInteger)));
    }

    #[test]
    fn test_hrandfield_count_larger_than_hash() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "v1"), ("f2", "v2")]);

        // Positive count larger than hash size returns all unique fields
        let result = hrandfield(&mut ctx, &[bulk("myhash"), bulk("100")]).unwrap();
        if let RespValue::Array(arr) = &result {
            assert_eq!(arr.len(), 2); // Only 2 fields exist
                                      // All returned should be valid fields
            let mut fields: Vec<String> = arr
                .iter()
                .map(|item| {
                    if let RespValue::BulkString(b) = item {
                        std::str::from_utf8(b).unwrap().to_string()
                    } else {
                        panic!("Expected bulk string");
                    }
                })
                .collect();
            fields.sort();
            assert_eq!(fields, vec!["f1", "f2"]);
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hrandfield_negative_count_size() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "v1"), ("f2", "v2")]);

        // Negative count: returns exactly |count| elements, may repeat
        let result = hrandfield(&mut ctx, &[bulk("myhash"), bulk("-10")]).unwrap();
        if let RespValue::Array(arr) = &result {
            assert_eq!(arr.len(), 10); // Exactly 10 elements
                                       // Each element must be a valid field
            for item in arr {
                if let RespValue::BulkString(b) = item {
                    let s = std::str::from_utf8(b).unwrap();
                    assert!(s == "f1" || s == "f2", "Unexpected field: {s}");
                } else {
                    panic!("Expected bulk string in array");
                }
            }
        } else {
            panic!("Expected array");
        }
    }

    // ========================================================================
    // HINCRBYFLOAT tests
    // ========================================================================

    #[test]
    fn test_hincrbyfloat_negative_increment() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("field", "10.5")]);

        let result =
            hincrbyfloat(&mut ctx, &[bulk("myhash"), bulk("field"), bulk("-2.3")]).unwrap();
        if let RespValue::BulkString(b) = result {
            let val: f64 = std::str::from_utf8(&b).unwrap().parse().unwrap();
            assert!((val - 8.2).abs() < 0.0001);
        } else {
            panic!("Expected bulk string");
        }

        // Verify stored value
        let result = hget(&mut ctx, &[bulk("myhash"), bulk("field")]).unwrap();
        if let RespValue::BulkString(b) = result {
            let val: f64 = std::str::from_utf8(&b).unwrap().parse().unwrap();
            assert!((val - 8.2).abs() < 0.0001);
        } else {
            panic!("Expected bulk string");
        }
    }

    #[test]
    fn test_hincrbyfloat_non_numeric_field() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("field", "not_a_number")]);

        let result = hincrbyfloat(&mut ctx, &[bulk("myhash"), bulk("field"), bulk("1.0")]);
        assert!(matches!(result, Err(CommandError::NotAFloat)));
    }

    #[test]
    fn test_hincrbyfloat_new_key() {
        let mut ctx = make_ctx();

        // HINCRBYFLOAT on nonexistent key creates the hash with field = increment
        let result =
            hincrbyfloat(&mut ctx, &[bulk("newkey"), bulk("field"), bulk("3.14")]).unwrap();
        if let RespValue::BulkString(b) = &result {
            let val: f64 = std::str::from_utf8(b).unwrap().parse().unwrap();
            assert!((val - 3.14).abs() < 0.0001);
        } else {
            panic!("Expected bulk string");
        }

        // Verify the key and field were created
        let result = hget(&mut ctx, &[bulk("newkey"), bulk("field")]).unwrap();
        if let RespValue::BulkString(b) = result {
            let val: f64 = std::str::from_utf8(&b).unwrap().parse().unwrap();
            assert!((val - 3.14).abs() < 0.0001);
        } else {
            panic!("Expected bulk string");
        }

        // Verify the hash has exactly one field
        let result = hlen(&mut ctx, &[bulk("newkey")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    // ========================================================================
    // HDEL/HSET edge tests (wrong arity)
    // ========================================================================

    #[test]
    fn test_hsetnx_wrong_arity() {
        let mut ctx = make_ctx();

        // Too few args: only key and field, no value
        assert!(matches!(
            hsetnx(&mut ctx, &[bulk("key"), bulk("field")]),
            Err(CommandError::WrongArity(_))
        ));

        // Too few args: only key
        assert!(matches!(
            hsetnx(&mut ctx, &[bulk("key")]),
            Err(CommandError::WrongArity(_))
        ));

        // No args at all
        assert!(matches!(
            hsetnx(&mut ctx, &[]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_hexists_wrong_arity() {
        let mut ctx = make_ctx();

        // Only key, no field
        assert!(matches!(
            hexists(&mut ctx, &[bulk("key")]),
            Err(CommandError::WrongArity(_))
        ));

        // No args at all
        assert!(matches!(
            hexists(&mut ctx, &[]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_hgetall_wrong_arity() {
        let mut ctx = make_ctx();

        // No args at all — HGETALL checks args.is_empty()
        assert!(matches!(
            hgetall(&mut ctx, &[]),
            Err(CommandError::WrongArity(_))
        ));
    }

    // ========================================================================
    // HRANDFIELD additional tests (13 more to reach 20 total)
    // ========================================================================

    #[test]
    fn test_hrandfield_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        assert!(matches!(
            hrandfield(&mut ctx, &[]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_hrandfield_nonexistent_key_no_count() {
        let mut ctx = make_ctx();
        // Nonexistent key without count returns Null
        let result = hrandfield(&mut ctx, &[bulk("nokey")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_hrandfield_nonexistent_key_with_count() {
        let mut ctx = make_ctx();
        // Nonexistent key with count returns empty array
        let result = hrandfield(&mut ctx, &[bulk("nokey"), bulk("5")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![]));
    }

    #[test]
    fn test_hrandfield_single_field_hash() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "one", &[("only", "val")]);

        // Without count: always returns the only field
        let result = hrandfield(&mut ctx, &[bulk("one")]).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from("only")));

        // With positive count: returns array with single element
        let result = hrandfield(&mut ctx, &[bulk("one"), bulk("1")]).unwrap();
        if let RespValue::Array(arr) = &result {
            assert_eq!(arr.len(), 1);
            assert_eq!(arr[0], RespValue::BulkString(Bytes::from("only")));
        } else {
            panic!("Expected array, got {result:?}");
        }
    }

    #[test]
    fn test_hrandfield_positive_count_returns_unique() {
        let mut ctx = make_ctx();
        setup_hash(
            &mut ctx,
            "myhash",
            &[("a", "1"), ("b", "2"), ("c", "3"), ("d", "4"), ("e", "5")],
        );

        // Positive count 3 => 3 unique fields
        let result = hrandfield(&mut ctx, &[bulk("myhash"), bulk("3")]).unwrap();
        if let RespValue::Array(arr) = &result {
            assert_eq!(arr.len(), 3);
            // All should be unique
            let fields: Vec<&[u8]> = arr
                .iter()
                .map(|r| {
                    if let RespValue::BulkString(b) = r {
                        b.as_ref()
                    } else {
                        panic!("Expected bulk string")
                    }
                })
                .collect();
            let mut deduped = fields.clone();
            deduped.sort();
            deduped.dedup();
            assert_eq!(deduped.len(), 3, "All fields must be unique");
            // All must be valid field names
            let valid = [b"a".as_ref(), b"b", b"c", b"d", b"e"];
            for f in &fields {
                assert!(valid.contains(f), "Unexpected field");
            }
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hrandfield_negative_count_allows_duplicates() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "v1")]);

        // Negative count -5 on hash with 1 field: all 5 are the same field
        let result = hrandfield(&mut ctx, &[bulk("myhash"), bulk("-5")]).unwrap();
        if let RespValue::Array(arr) = &result {
            assert_eq!(arr.len(), 5);
            for item in arr {
                assert_eq!(*item, RespValue::BulkString(Bytes::from("f1")));
            }
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hrandfield_with_values_positive_count() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("a", "1"), ("b", "2"), ("c", "3")]);

        let result =
            hrandfield(&mut ctx, &[bulk("myhash"), bulk("2"), bulk("WITHVALUES")]).unwrap();
        if let RespValue::Array(arr) = &result {
            // 2 fields * 2 (field + value) = 4 elements
            assert_eq!(arr.len(), 4);
            // Verify field-value pairing
            let valid: std::collections::HashMap<&str, &str> = [("a", "1"), ("b", "2"), ("c", "3")]
                .iter()
                .copied()
                .collect();
            for pair in arr.chunks(2) {
                if let (RespValue::BulkString(f), RespValue::BulkString(v)) = (&pair[0], &pair[1]) {
                    let fs = std::str::from_utf8(f).unwrap();
                    let vs = std::str::from_utf8(v).unwrap();
                    assert_eq!(valid.get(fs), Some(&vs), "Field {fs} should map to {vs}");
                } else {
                    panic!("Expected bulk strings");
                }
            }
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hrandfield_count_larger_than_hash_returns_all() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("x", "1"), ("y", "2")]);

        let result = hrandfield(&mut ctx, &[bulk("myhash"), bulk("999")]).unwrap();
        if let RespValue::Array(arr) = &result {
            assert_eq!(arr.len(), 2);
            let mut fields: Vec<String> = arr
                .iter()
                .map(|r| {
                    if let RespValue::BulkString(b) = r {
                        std::str::from_utf8(b).unwrap().to_string()
                    } else {
                        panic!("Expected bulk string")
                    }
                })
                .collect();
            fields.sort();
            assert_eq!(fields, vec!["x", "y"]);
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hrandfield_zero_count_returns_empty() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "v1"), ("f2", "v2")]);

        let result = hrandfield(&mut ctx, &[bulk("myhash"), bulk("0")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![]));
    }

    #[test]
    fn test_hrandfield_wrong_type_error() {
        let mut ctx = make_ctx();
        setup_string(&mut ctx, "mystring", "hello");

        assert!(matches!(
            hrandfield(&mut ctx, &[bulk("mystring")]),
            Err(CommandError::WrongType)
        ));
        assert!(matches!(
            hrandfield(&mut ctx, &[bulk("mystring"), bulk("2")]),
            Err(CommandError::WrongType)
        ));
    }

    #[test]
    fn test_hrandfield_expired_key() {
        use std::time::Instant;
        let mut ctx = make_ctx();

        // Manually insert an expired hash
        let mut hash = HashMap::new();
        hash.insert(Bytes::from("f1"), Bytes::from("v1"));
        let mut entry = Entry::new(RedisValue::Hash(hash));
        entry.expires_at = Some(Instant::now() - std::time::Duration::from_secs(1));
        let db = ctx.store().database(ctx.selected_db());
        db.set(Bytes::from("expired_hash"), entry);

        // Without count: expired key treated as non-existent → Null
        let result = hrandfield(&mut ctx, &[bulk("expired_hash")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_hrandfield_expired_key_with_count() {
        use std::time::Instant;
        let mut ctx = make_ctx();

        let mut hash = HashMap::new();
        hash.insert(Bytes::from("f1"), Bytes::from("v1"));
        let mut entry = Entry::new(RedisValue::Hash(hash));
        entry.expires_at = Some(Instant::now() - std::time::Duration::from_secs(1));
        let db = ctx.store().database(ctx.selected_db());
        db.set(Bytes::from("expired_hash"), entry);

        // With count: expired key treated as non-existent → empty array
        let result = hrandfield(&mut ctx, &[bulk("expired_hash"), bulk("3")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![]));
    }

    #[test]
    fn test_hrandfield_invalid_third_arg_syntax_error() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "v1")]);

        // Third arg that is not WITHVALUES should be a SyntaxError
        let result = hrandfield(&mut ctx, &[bulk("myhash"), bulk("2"), bulk("BOGUS")]);
        assert!(matches!(result, Err(CommandError::SyntaxError)));
    }

    #[test]
    fn test_hrandfield_withvalues_case_insensitive() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "v1")]);

        // "withvalues" in lowercase
        let result =
            hrandfield(&mut ctx, &[bulk("myhash"), bulk("1"), bulk("withvalues")]).unwrap();
        if let RespValue::Array(arr) = &result {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], RespValue::BulkString(Bytes::from("f1")));
            assert_eq!(arr[1], RespValue::BulkString(Bytes::from("v1")));
        } else {
            panic!("Expected array");
        }
    }

    // ========================================================================
    // HSCAN additional tests (13 more to reach 20 total)
    // ========================================================================

    #[test]
    fn test_hscan_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        assert!(matches!(
            hscan(&mut ctx, &[]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_hscan_wrong_arity_key_only() {
        let mut ctx = make_ctx();
        assert!(matches!(
            hscan(&mut ctx, &[bulk("myhash")]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_hscan_nonexistent_key_returns_zero_cursor_and_empty() {
        let mut ctx = make_ctx();
        let result = hscan(&mut ctx, &[bulk("nokey"), bulk("0")]).unwrap();
        if let RespValue::Array(outer) = &result {
            assert_eq!(outer.len(), 2);
            assert_eq!(outer[0], RespValue::BulkString(Bytes::from("0")));
            if let RespValue::Array(items) = &outer[1] {
                assert!(items.is_empty());
            } else {
                panic!("Expected empty array");
            }
        } else {
            panic!("Expected outer array");
        }
    }

    #[test]
    fn test_hscan_basic_returns_cursor_and_array_structure() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("a", "1"), ("b", "2")]);

        let result = hscan(&mut ctx, &[bulk("myhash"), bulk("0")]).unwrap();
        if let RespValue::Array(outer) = &result {
            assert_eq!(outer.len(), 2);
            // First element is cursor (BulkString)
            assert!(matches!(&outer[0], RespValue::BulkString(_)));
            // Second element is array of field-value pairs
            if let RespValue::Array(items) = &outer[1] {
                assert_eq!(items.len(), 4); // 2 fields * 2
            } else {
                panic!("Expected items array");
            }
        } else {
            panic!("Expected outer array");
        }
    }

    #[test]
    fn test_hscan_full_iteration_with_count_1() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("a", "1"), ("b", "2"), ("c", "3")]);

        let mut all_fields: Vec<String> = Vec::new();
        let mut cursor = "0".to_string();
        let mut iterations = 0;

        loop {
            let result = hscan(
                &mut ctx,
                &[bulk("myhash"), bulk(&cursor), bulk("COUNT"), bulk("1")],
            )
            .unwrap();
            iterations += 1;
            if let RespValue::Array(outer) = &result {
                if let RespValue::BulkString(c) = &outer[0] {
                    cursor = std::str::from_utf8(c).unwrap().to_string();
                }
                if let RespValue::Array(items) = &outer[1] {
                    for pair in items.chunks(2) {
                        if let RespValue::BulkString(f) = &pair[0] {
                            all_fields.push(std::str::from_utf8(f).unwrap().to_string());
                        }
                    }
                }
            }
            if cursor == "0" {
                break;
            }
        }

        // Should have iterated 3 times (COUNT 1 over 3 fields) + final
        assert!(
            iterations >= 3,
            "Expected at least 3 iterations, got {iterations}"
        );
        all_fields.sort();
        assert_eq!(all_fields, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_hscan_match_pattern_star() {
        let mut ctx = make_ctx();
        setup_hash(
            &mut ctx,
            "myhash",
            &[("user:1", "a"), ("user:2", "b"), ("post:1", "c")],
        );

        // MATCH user:* should only return user:1 and user:2
        let result = hscan(
            &mut ctx,
            &[bulk("myhash"), bulk("0"), bulk("MATCH"), bulk("user:*")],
        )
        .unwrap();
        if let RespValue::Array(outer) = &result {
            if let RespValue::Array(items) = &outer[1] {
                // 2 matching fields * 2 = 4
                assert_eq!(items.len(), 4);
                let fields: Vec<String> = items
                    .chunks(2)
                    .map(|pair| {
                        if let RespValue::BulkString(b) = &pair[0] {
                            std::str::from_utf8(b).unwrap().to_string()
                        } else {
                            panic!("Expected bulk string")
                        }
                    })
                    .collect();
                for f in &fields {
                    assert!(f.starts_with("user:"), "Expected user: prefix, got {f}");
                }
            } else {
                panic!("Expected items array");
            }
        } else {
            panic!("Expected outer array");
        }
    }

    #[test]
    fn test_hscan_count_option_limits_batch_size() {
        let mut ctx = make_ctx();
        setup_hash(
            &mut ctx,
            "myhash",
            &[
                ("a", "1"),
                ("b", "2"),
                ("c", "3"),
                ("d", "4"),
                ("e", "5"),
                ("f", "6"),
            ],
        );

        // COUNT 3 should return at most 3 fields in the first batch
        let result = hscan(
            &mut ctx,
            &[bulk("myhash"), bulk("0"), bulk("COUNT"), bulk("3")],
        )
        .unwrap();
        if let RespValue::Array(outer) = &result {
            if let RespValue::Array(items) = &outer[1] {
                assert_eq!(items.len(), 6); // 3 fields * 2
            } else {
                panic!("Expected items array");
            }
            // Cursor should be non-zero since there are more fields
            if let RespValue::BulkString(c) = &outer[0] {
                assert_ne!(c.as_ref(), b"0", "Cursor should be non-zero");
            }
        } else {
            panic!("Expected outer array");
        }
    }

    #[test]
    fn test_hscan_wrong_type_error() {
        let mut ctx = make_ctx();
        setup_string(&mut ctx, "mystring", "hello");

        assert!(matches!(
            hscan(&mut ctx, &[bulk("mystring"), bulk("0")]),
            Err(CommandError::WrongType)
        ));
    }

    #[test]
    fn test_hscan_expired_key() {
        use std::time::Instant;
        let mut ctx = make_ctx();

        let mut hash = HashMap::new();
        hash.insert(Bytes::from("f1"), Bytes::from("v1"));
        let mut entry = Entry::new(RedisValue::Hash(hash));
        entry.expires_at = Some(Instant::now() - std::time::Duration::from_secs(1));
        let db = ctx.store().database(ctx.selected_db());
        db.set(Bytes::from("expired_hash"), entry);

        // Expired hash treated as non-existent
        let result = hscan(&mut ctx, &[bulk("expired_hash"), bulk("0")]).unwrap();
        if let RespValue::Array(outer) = &result {
            assert_eq!(outer.len(), 2);
            assert_eq!(outer[0], RespValue::BulkString(Bytes::from("0")));
            if let RespValue::Array(items) = &outer[1] {
                assert!(items.is_empty());
            } else {
                panic!("Expected empty items array");
            }
        } else {
            panic!("Expected outer array");
        }
    }

    #[test]
    fn test_hscan_empty_hash() {
        let mut ctx = make_ctx();
        // Create then empty the hash
        setup_hash(&mut ctx, "myhash", &[("f1", "v1")]);
        hdel(&mut ctx, &[bulk("myhash"), bulk("f1")]).unwrap();

        // Empty/deleted hash → same as nonexistent
        let result = hscan(&mut ctx, &[bulk("myhash"), bulk("0")]).unwrap();
        if let RespValue::Array(outer) = &result {
            assert_eq!(outer.len(), 2);
            assert_eq!(outer[0], RespValue::BulkString(Bytes::from("0")));
            if let RespValue::Array(items) = &outer[1] {
                assert!(items.is_empty());
            } else {
                panic!("Expected empty items array");
            }
        } else {
            panic!("Expected outer array");
        }
    }

    #[test]
    fn test_hscan_match_no_results() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("aaa", "1"), ("bbb", "2")]);

        // Pattern that matches nothing
        let result = hscan(
            &mut ctx,
            &[bulk("myhash"), bulk("0"), bulk("MATCH"), bulk("zzz*")],
        )
        .unwrap();
        if let RespValue::Array(outer) = &result {
            assert_eq!(outer[0], RespValue::BulkString(Bytes::from("0")));
            if let RespValue::Array(items) = &outer[1] {
                assert!(items.is_empty());
            } else {
                panic!("Expected empty items array");
            }
        } else {
            panic!("Expected outer array");
        }
    }

    #[test]
    fn test_hscan_single_field() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("only", "val")]);

        let result = hscan(&mut ctx, &[bulk("myhash"), bulk("0")]).unwrap();
        if let RespValue::Array(outer) = &result {
            // Cursor should be 0 (all returned)
            assert_eq!(outer[0], RespValue::BulkString(Bytes::from("0")));
            if let RespValue::Array(items) = &outer[1] {
                assert_eq!(items.len(), 2);
                assert_eq!(items[0], RespValue::BulkString(Bytes::from("only")));
                assert_eq!(items[1], RespValue::BulkString(Bytes::from("val")));
            } else {
                panic!("Expected items array");
            }
        } else {
            panic!("Expected outer array");
        }
    }

    #[test]
    fn test_hscan_special_chars_in_fields() {
        let mut ctx = make_ctx();
        setup_hash(
            &mut ctx,
            "myhash",
            &[
                ("hello world", "v1"),
                ("foo:bar:baz", "v2"),
                ("key=val", "v3"),
            ],
        );

        let result = hscan(&mut ctx, &[bulk("myhash"), bulk("0")]).unwrap();
        if let RespValue::Array(outer) = &result {
            if let RespValue::Array(items) = &outer[1] {
                assert_eq!(items.len(), 6); // 3 fields * 2
                                            // Collect field names
                let fields: Vec<String> = items
                    .chunks(2)
                    .map(|pair| {
                        if let RespValue::BulkString(b) = &pair[0] {
                            std::str::from_utf8(b).unwrap().to_string()
                        } else {
                            panic!("Expected bulk string")
                        }
                    })
                    .collect();
                assert!(fields.contains(&"hello world".to_string()));
                assert!(fields.contains(&"foo:bar:baz".to_string()));
                assert!(fields.contains(&"key=val".to_string()));
            } else {
                panic!("Expected items array");
            }
        } else {
            panic!("Expected outer array");
        }
    }

    #[test]
    fn test_hscan_cursor_beyond_range() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "v1"), ("f2", "v2")]);

        // Cursor well past the end of the hash → returns empty results, cursor 0
        let result = hscan(&mut ctx, &[bulk("myhash"), bulk("999")]).unwrap();
        if let RespValue::Array(outer) = &result {
            assert_eq!(outer[0], RespValue::BulkString(Bytes::from("0")));
            if let RespValue::Array(items) = &outer[1] {
                assert!(items.is_empty());
            } else {
                panic!("Expected empty items array");
            }
        } else {
            panic!("Expected outer array");
        }
    }

    // ========================================================================
    // Helper: create an expired hash entry
    // ========================================================================
    fn setup_expired_hash(ctx: &mut CommandContext, key: &str, pairs: &[(&str, &str)]) {
        use std::time::{Duration, Instant};
        let db = ctx.store().database(ctx.selected_db());
        let mut map = HashMap::new();
        for (f, v) in pairs {
            map.insert(Bytes::from(f.to_string()), Bytes::from(v.to_string()));
        }
        let mut entry = Entry::new(RedisValue::Hash(map));
        entry.expires_at = Some(Instant::now() - Duration::from_secs(1));
        db.set(Bytes::from(key.to_owned()), entry);
    }

    // ========================================================================
    // HSET — additional tests (16 more to reach 20 total)
    // ========================================================================

    #[test]
    fn test_hset_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        assert!(matches!(
            hset(&mut ctx, &[]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_hset_wrong_arity_one_arg() {
        let mut ctx = make_ctx();
        assert!(matches!(
            hset(&mut ctx, &[bulk("key")]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_hset_single_field() {
        let mut ctx = make_ctx();
        let result = hset(&mut ctx, &[bulk("h"), bulk("f1"), bulk("v1")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let val = hget(&mut ctx, &[bulk("h"), bulk("f1")]).unwrap();
        assert_eq!(val, bulk("v1"));
    }

    #[test]
    fn test_hset_returns_new_count() {
        let mut ctx = make_ctx();
        // Create hash with 2 fields
        let r = hset(
            &mut ctx,
            &[bulk("h"), bulk("a"), bulk("1"), bulk("b"), bulk("2")],
        )
        .unwrap();
        assert_eq!(r, RespValue::Integer(2));

        // Overwrite 1 existing + add 1 new → returns 1 (only new)
        let r = hset(
            &mut ctx,
            &[bulk("h"), bulk("a"), bulk("X"), bulk("c"), bulk("3")],
        )
        .unwrap();
        assert_eq!(r, RespValue::Integer(1));
    }

    #[test]
    fn test_hset_wrong_type_string() {
        let mut ctx = make_ctx();
        setup_string(&mut ctx, "mystr", "hello");

        assert!(matches!(
            hset(&mut ctx, &[bulk("mystr"), bulk("f"), bulk("v")]),
            Err(CommandError::WrongType)
        ));
    }

    #[test]
    fn test_hset_creates_hash() {
        let mut ctx = make_ctx();
        // Key doesn't exist yet
        let result = hget(&mut ctx, &[bulk("newkey"), bulk("f1")]).unwrap();
        assert_eq!(result, RespValue::Null);

        // HSET creates the hash
        hset(&mut ctx, &[bulk("newkey"), bulk("f1"), bulk("v1")]).unwrap();
        let result = hget(&mut ctx, &[bulk("newkey"), bulk("f1")]).unwrap();
        assert_eq!(result, bulk("v1"));
    }

    #[test]
    fn test_hset_expired_key() {
        let mut ctx = make_ctx();
        setup_expired_hash(&mut ctx, "eh", &[("old", "data")]);

        // HSET on expired key should treat it as new (expired key is cleaned up)
        let result = hset(&mut ctx, &[bulk("eh"), bulk("f1"), bulk("v1")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // Old field should not be accessible
        let old = hget(&mut ctx, &[bulk("eh"), bulk("old")]).unwrap();
        assert_eq!(old, RespValue::Null);
    }

    #[test]
    fn test_hset_empty_value() {
        let mut ctx = make_ctx();
        let result = hset(&mut ctx, &[bulk("h"), bulk("f"), bulk("")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let val = hget(&mut ctx, &[bulk("h"), bulk("f")]).unwrap();
        assert_eq!(val, RespValue::BulkString(Bytes::from("")));
    }

    #[test]
    fn test_hset_empty_field() {
        let mut ctx = make_ctx();
        let result = hset(&mut ctx, &[bulk("h"), bulk(""), bulk("val")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let val = hget(&mut ctx, &[bulk("h"), bulk("")]).unwrap();
        assert_eq!(val, bulk("val"));
    }

    #[test]
    fn test_hset_binary_field() {
        let mut ctx = make_ctx();
        let binary_field = RespValue::BulkString(Bytes::from(vec![0x00, 0xFF, 0x80]));
        let result = hset(&mut ctx, &[bulk("h"), binary_field.clone(), bulk("val")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let val = hget(&mut ctx, &[bulk("h"), binary_field]).unwrap();
        assert_eq!(val, bulk("val"));
    }

    #[test]
    fn test_hset_special_chars() {
        let mut ctx = make_ctx();
        let result = hset(
            &mut ctx,
            &[
                bulk("h"),
                bulk("hello world!@#$%"),
                bulk("value\nwith\ttabs"),
            ],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let val = hget(&mut ctx, &[bulk("h"), bulk("hello world!@#$%")]).unwrap();
        assert_eq!(val, bulk("value\nwith\ttabs"));
    }

    #[test]
    fn test_hset_large_value() {
        let mut ctx = make_ctx();
        let large = "x".repeat(100_000);
        let result = hset(&mut ctx, &[bulk("h"), bulk("f"), bulk(&large)]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let val = hget(&mut ctx, &[bulk("h"), bulk("f")]).unwrap();
        assert_eq!(val, bulk(&large));
    }

    #[test]
    fn test_hset_many_fields() {
        let mut ctx = make_ctx();
        let mut args = vec![bulk("h")];
        for i in 0..100 {
            args.push(bulk(&format!("field{i}")));
            args.push(bulk(&format!("value{i}")));
        }
        let result = hset(&mut ctx, &args).unwrap();
        assert_eq!(result, RespValue::Integer(100));

        // Spot check a few fields
        let v0 = hget(&mut ctx, &[bulk("h"), bulk("field0")]).unwrap();
        assert_eq!(v0, bulk("value0"));
        let v99 = hget(&mut ctx, &[bulk("h"), bulk("field99")]).unwrap();
        assert_eq!(v99, bulk("value99"));
    }

    #[test]
    fn test_hset_odd_args_rejected() {
        let mut ctx = make_ctx();
        // key + 3 tokens = odd field-value args
        assert!(matches!(
            hset(&mut ctx, &[bulk("h"), bulk("f1"), bulk("v1"), bulk("f2")]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_hset_overwrite_field() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "h", &[("f1", "original")]);

        let result = hset(&mut ctx, &[bulk("h"), bulk("f1"), bulk("updated")]).unwrap();
        // Overwrite returns 0 new fields
        assert_eq!(result, RespValue::Integer(0));

        let val = hget(&mut ctx, &[bulk("h"), bulk("f1")]).unwrap();
        assert_eq!(val, bulk("updated"));
    }

    #[test]
    fn test_hset_multiple_fields_mixed_new_existing() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "h", &[("a", "1"), ("b", "2")]);

        // a is existing, c is new, b is existing, d is new
        let result = hset(
            &mut ctx,
            &[
                bulk("h"),
                bulk("a"),
                bulk("X"),
                bulk("c"),
                bulk("3"),
                bulk("b"),
                bulk("Y"),
                bulk("d"),
                bulk("4"),
            ],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(2)); // 2 new: c, d
    }

    #[test]
    fn test_hset_duplicate_field_in_args() {
        let mut ctx = make_ctx();
        // Same field twice in single HSET: last value wins, counted as 1 new
        let result = hset(
            &mut ctx,
            &[
                bulk("h"),
                bulk("f"),
                bulk("first"),
                bulk("f"),
                bulk("second"),
            ],
        )
        .unwrap();
        // First occurrence is new (counted), second is overwrite (not counted)
        assert_eq!(result, RespValue::Integer(1));

        let val = hget(&mut ctx, &[bulk("h"), bulk("f")]).unwrap();
        assert_eq!(val, bulk("second"));
    }

    // ========================================================================
    // HGET — additional tests (17 more to reach 20 total)
    // ========================================================================

    #[test]
    fn test_hget_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        assert!(matches!(
            hget(&mut ctx, &[]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_hget_wrong_arity_one_arg() {
        let mut ctx = make_ctx();
        assert!(matches!(
            hget(&mut ctx, &[bulk("key")]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_hget_existing_field() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "h", &[("f1", "v1"), ("f2", "v2")]);

        let result = hget(&mut ctx, &[bulk("h"), bulk("f1")]).unwrap();
        assert_eq!(result, bulk("v1"));
    }

    #[test]
    fn test_hget_nonexistent_field_in_existing_hash() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "h", &[("f1", "v1")]);

        let result = hget(&mut ctx, &[bulk("h"), bulk("nosuchfield")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_hget_nonexistent_key_returns_null() {
        let mut ctx = make_ctx();
        let result = hget(&mut ctx, &[bulk("nokey"), bulk("f")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_hget_wrong_type_string() {
        let mut ctx = make_ctx();
        setup_string(&mut ctx, "mystr", "hello");

        assert!(matches!(
            hget(&mut ctx, &[bulk("mystr"), bulk("f")]),
            Err(CommandError::WrongType)
        ));
    }

    #[test]
    fn test_hget_expired_key() {
        let mut ctx = make_ctx();
        setup_expired_hash(&mut ctx, "eh", &[("f1", "v1")]);

        // Expired key should behave as nonexistent
        let result = hget(&mut ctx, &[bulk("eh"), bulk("f1")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_hget_empty_field_name() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "h", &[("", "emptyfield")]);

        let result = hget(&mut ctx, &[bulk("h"), bulk("")]).unwrap();
        assert_eq!(result, bulk("emptyfield"));
    }

    #[test]
    fn test_hget_empty_value() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "h", &[("f", "")]);

        let result = hget(&mut ctx, &[bulk("h"), bulk("f")]).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from("")));
    }

    #[test]
    fn test_hget_binary_value() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut map = HashMap::new();
        map.insert(Bytes::from("f"), Bytes::from(vec![0x00, 0x01, 0xFF, 0x80]));
        db.set(Bytes::from("h"), Entry::new(RedisValue::Hash(map)));

        let result = hget(&mut ctx, &[bulk("h"), bulk("f")]).unwrap();
        assert_eq!(
            result,
            RespValue::BulkString(Bytes::from(vec![0x00, 0x01, 0xFF, 0x80]))
        );
    }

    #[test]
    fn test_hget_special_chars() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "h", &[("key\twith\nnewlines", "val!@#")]);

        let result = hget(&mut ctx, &[bulk("h"), bulk("key\twith\nnewlines")]).unwrap();
        assert_eq!(result, bulk("val!@#"));
    }

    #[test]
    fn test_hget_after_overwrite() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "h", &[("f", "old")]);
        hset(&mut ctx, &[bulk("h"), bulk("f"), bulk("new")]).unwrap();

        let result = hget(&mut ctx, &[bulk("h"), bulk("f")]).unwrap();
        assert_eq!(result, bulk("new"));
    }

    #[test]
    fn test_hget_after_hdel() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "h", &[("f1", "v1"), ("f2", "v2")]);
        hdel(&mut ctx, &[bulk("h"), bulk("f1")]).unwrap();

        let result = hget(&mut ctx, &[bulk("h"), bulk("f1")]).unwrap();
        assert_eq!(result, RespValue::Null);

        // Other field still accessible
        let result = hget(&mut ctx, &[bulk("h"), bulk("f2")]).unwrap();
        assert_eq!(result, bulk("v2"));
    }

    #[test]
    fn test_hget_returns_bulk_string() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "h", &[("f", "val")]);

        let result = hget(&mut ctx, &[bulk("h"), bulk("f")]).unwrap();
        // Ensure it's specifically a BulkString, not SimpleString
        assert!(matches!(result, RespValue::BulkString(_)));
        assert_eq!(result, bulk("val"));
    }

    #[test]
    fn test_hget_numeric_value() {
        let mut ctx = make_ctx();
        setup_hash(
            &mut ctx,
            "h",
            &[("count", "42"), ("neg", "-100"), ("float", "3.14")],
        );

        assert_eq!(
            hget(&mut ctx, &[bulk("h"), bulk("count")]).unwrap(),
            bulk("42")
        );
        assert_eq!(
            hget(&mut ctx, &[bulk("h"), bulk("neg")]).unwrap(),
            bulk("-100")
        );
        assert_eq!(
            hget(&mut ctx, &[bulk("h"), bulk("float")]).unwrap(),
            bulk("3.14")
        );
    }

    #[test]
    fn test_hget_large_value() {
        let mut ctx = make_ctx();
        let large = "y".repeat(100_000);
        setup_hash(&mut ctx, "h", &[("f", &large)]);

        let result = hget(&mut ctx, &[bulk("h"), bulk("f")]).unwrap();
        assert_eq!(result, bulk(&large));
    }

    #[test]
    fn test_hget_multiple_fields_independent() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "h", &[("a", "1"), ("b", "2"), ("c", "3")]);

        // Each HGET is independent
        assert_eq!(hget(&mut ctx, &[bulk("h"), bulk("a")]).unwrap(), bulk("1"));
        assert_eq!(hget(&mut ctx, &[bulk("h"), bulk("b")]).unwrap(), bulk("2"));
        assert_eq!(hget(&mut ctx, &[bulk("h"), bulk("c")]).unwrap(), bulk("3"));
        assert_eq!(
            hget(&mut ctx, &[bulk("h"), bulk("d")]).unwrap(),
            RespValue::Null
        );
    }

    // ========================================================================
    // HSETNX — additional tests (17 more to reach 20 total)
    // ========================================================================

    #[test]
    fn test_hsetnx_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        assert!(matches!(
            hsetnx(&mut ctx, &[]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_hsetnx_wrong_arity_two_args() {
        let mut ctx = make_ctx();
        assert!(matches!(
            hsetnx(&mut ctx, &[bulk("key"), bulk("field")]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_hsetnx_new_field_returns_1() {
        let mut ctx = make_ctx();
        let result = hsetnx(&mut ctx, &[bulk("h"), bulk("f"), bulk("v")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_hsetnx_existing_field_returns_0() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "h", &[("f", "original")]);

        let result = hsetnx(&mut ctx, &[bulk("h"), bulk("f"), bulk("new")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_hsetnx_wrong_type() {
        let mut ctx = make_ctx();
        setup_string(&mut ctx, "mystr", "hello");

        assert!(matches!(
            hsetnx(&mut ctx, &[bulk("mystr"), bulk("f"), bulk("v")]),
            Err(CommandError::WrongType)
        ));
    }

    #[test]
    fn test_hsetnx_expired_key() {
        let mut ctx = make_ctx();
        setup_expired_hash(&mut ctx, "eh", &[("f", "old")]);

        // Expired key treated as nonexistent, so field is "new"
        let result = hsetnx(&mut ctx, &[bulk("eh"), bulk("f"), bulk("new")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let val = hget(&mut ctx, &[bulk("eh"), bulk("f")]).unwrap();
        assert_eq!(val, bulk("new"));
    }

    #[test]
    fn test_hsetnx_empty_value() {
        let mut ctx = make_ctx();
        let result = hsetnx(&mut ctx, &[bulk("h"), bulk("f"), bulk("")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let val = hget(&mut ctx, &[bulk("h"), bulk("f")]).unwrap();
        assert_eq!(val, RespValue::BulkString(Bytes::from("")));
    }

    #[test]
    fn test_hsetnx_creates_hash() {
        let mut ctx = make_ctx();
        // Key doesn't exist
        let result = hsetnx(&mut ctx, &[bulk("newh"), bulk("f"), bulk("v")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // Verify hash was created
        let val = hget(&mut ctx, &[bulk("newh"), bulk("f")]).unwrap();
        assert_eq!(val, bulk("v"));
    }

    #[test]
    fn test_hsetnx_preserves_value() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "h", &[("f", "original")]);

        // HSETNX should not change existing field's value
        hsetnx(&mut ctx, &[bulk("h"), bulk("f"), bulk("ignored")]).unwrap();
        let val = hget(&mut ctx, &[bulk("h"), bulk("f")]).unwrap();
        assert_eq!(val, bulk("original"));
    }

    #[test]
    fn test_hsetnx_empty_field() {
        let mut ctx = make_ctx();
        let result = hsetnx(&mut ctx, &[bulk("h"), bulk(""), bulk("val")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let val = hget(&mut ctx, &[bulk("h"), bulk("")]).unwrap();
        assert_eq!(val, bulk("val"));
    }

    #[test]
    fn test_hsetnx_binary_data() {
        let mut ctx = make_ctx();
        let bin_field = RespValue::BulkString(Bytes::from(vec![0xDE, 0xAD]));
        let bin_val = RespValue::BulkString(Bytes::from(vec![0xBE, 0xEF]));

        let result = hsetnx(&mut ctx, &[bulk("h"), bin_field.clone(), bin_val.clone()]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let val = hget(&mut ctx, &[bulk("h"), bin_field]).unwrap();
        assert_eq!(val, bin_val);
    }

    #[test]
    fn test_hsetnx_special_chars() {
        let mut ctx = make_ctx();
        let result = hsetnx(
            &mut ctx,
            &[bulk("h"), bulk("key with spaces!"), bulk("val\nnewline")],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let val = hget(&mut ctx, &[bulk("h"), bulk("key with spaces!")]).unwrap();
        assert_eq!(val, bulk("val\nnewline"));
    }

    #[test]
    fn test_hsetnx_large_value() {
        let mut ctx = make_ctx();
        let large = "z".repeat(50_000);
        let result = hsetnx(&mut ctx, &[bulk("h"), bulk("f"), bulk(&large)]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let val = hget(&mut ctx, &[bulk("h"), bulk("f")]).unwrap();
        assert_eq!(val, bulk(&large));
    }

    #[test]
    fn test_hsetnx_after_hdel() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "h", &[("f", "v1"), ("g", "v2")]);
        hdel(&mut ctx, &[bulk("h"), bulk("f")]).unwrap();

        // Field "f" was deleted, so HSETNX should succeed
        let result = hsetnx(&mut ctx, &[bulk("h"), bulk("f"), bulk("v3")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let val = hget(&mut ctx, &[bulk("h"), bulk("f")]).unwrap();
        assert_eq!(val, bulk("v3"));
    }

    #[test]
    fn test_hsetnx_multiple_calls() {
        let mut ctx = make_ctx();

        // First call creates the hash and field
        let r1 = hsetnx(&mut ctx, &[bulk("h"), bulk("a"), bulk("1")]).unwrap();
        assert_eq!(r1, RespValue::Integer(1));

        // Second call adds a different field to same hash
        let r2 = hsetnx(&mut ctx, &[bulk("h"), bulk("b"), bulk("2")]).unwrap();
        assert_eq!(r2, RespValue::Integer(1));

        // Third call tries existing field
        let r3 = hsetnx(&mut ctx, &[bulk("h"), bulk("a"), bulk("X")]).unwrap();
        assert_eq!(r3, RespValue::Integer(0));

        // Values preserved correctly
        assert_eq!(hget(&mut ctx, &[bulk("h"), bulk("a")]).unwrap(), bulk("1"));
        assert_eq!(hget(&mut ctx, &[bulk("h"), bulk("b")]).unwrap(), bulk("2"));
    }

    #[test]
    fn test_hsetnx_nonexistent_key() {
        let mut ctx = make_ctx();
        // Confirm key doesn't exist
        let val = hget(&mut ctx, &[bulk("brand_new"), bulk("f")]).unwrap();
        assert_eq!(val, RespValue::Null);

        let result = hsetnx(&mut ctx, &[bulk("brand_new"), bulk("f"), bulk("v")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_hsetnx_does_not_overwrite_other_fields() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "h", &[("a", "1"), ("b", "2")]);

        // HSETNX on existing field "a" should not affect field "b"
        hsetnx(&mut ctx, &[bulk("h"), bulk("a"), bulk("X")]).unwrap();
        assert_eq!(hget(&mut ctx, &[bulk("h"), bulk("b")]).unwrap(), bulk("2"));
    }

    // ========================================================================
    // HDEL — additional tests (16 more to reach 20 total)
    // ========================================================================

    #[test]
    fn test_hdel_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        assert!(matches!(
            hdel(&mut ctx, &[]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_hdel_wrong_arity_one_arg() {
        let mut ctx = make_ctx();
        assert!(matches!(
            hdel(&mut ctx, &[bulk("key")]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_hdel_single_field_returns_1() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "h", &[("f1", "v1"), ("f2", "v2")]);

        let result = hdel(&mut ctx, &[bulk("h"), bulk("f1")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_hdel_multiple_fields_returns_count() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "h", &[("a", "1"), ("b", "2"), ("c", "3")]);

        let result = hdel(&mut ctx, &[bulk("h"), bulk("a"), bulk("c")]).unwrap();
        assert_eq!(result, RespValue::Integer(2));

        // Remaining field still accessible
        assert_eq!(hget(&mut ctx, &[bulk("h"), bulk("b")]).unwrap(), bulk("2"));
    }

    #[test]
    fn test_hdel_nonexistent_field_returns_0() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "h", &[("f1", "v1")]);

        let result = hdel(&mut ctx, &[bulk("h"), bulk("nosuch")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_hdel_nonexistent_key_returns_0() {
        let mut ctx = make_ctx();
        let result = hdel(&mut ctx, &[bulk("nokey"), bulk("f")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_hdel_wrong_type() {
        let mut ctx = make_ctx();
        setup_string(&mut ctx, "mystr", "hello");

        assert!(matches!(
            hdel(&mut ctx, &[bulk("mystr"), bulk("f")]),
            Err(CommandError::WrongType)
        ));
    }

    #[test]
    fn test_hdel_expired_key() {
        let mut ctx = make_ctx();
        setup_expired_hash(&mut ctx, "eh", &[("f1", "v1")]);

        // Expired key treated as nonexistent
        let result = hdel(&mut ctx, &[bulk("eh"), bulk("f1")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_hdel_returns_count() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "h", &[("a", "1"), ("b", "2"), ("c", "3")]);

        // Delete 2 existing + 1 nonexistent → returns 2
        let result = hdel(&mut ctx, &[bulk("h"), bulk("a"), bulk("b"), bulk("nope")]).unwrap();
        assert_eq!(result, RespValue::Integer(2));
    }

    #[test]
    fn test_hdel_removes_field() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "h", &[("f1", "v1"), ("f2", "v2")]);

        hdel(&mut ctx, &[bulk("h"), bulk("f1")]).unwrap();

        // f1 should be gone
        assert_eq!(
            hget(&mut ctx, &[bulk("h"), bulk("f1")]).unwrap(),
            RespValue::Null
        );
        // f2 should still be there
        assert_eq!(
            hget(&mut ctx, &[bulk("h"), bulk("f2")]).unwrap(),
            bulk("v2")
        );
    }

    #[test]
    fn test_hdel_last_field_removes_key() {
        // In Redis, deleting the last field removes the key entirely.
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "h", &[("only", "field")]);

        hdel(&mut ctx, &[bulk("h"), bulk("only")]).unwrap();

        // Key should be gone
        let db = ctx.store().database(ctx.selected_db());
        assert!(db.get(&Bytes::from("h")).is_none());
    }

    #[test]
    fn test_hdel_empty_field() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "h", &[("", "emptyfield"), ("other", "val")]);

        let result = hdel(&mut ctx, &[bulk("h"), bulk("")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        assert_eq!(
            hget(&mut ctx, &[bulk("h"), bulk("")]).unwrap(),
            RespValue::Null
        );
    }

    #[test]
    fn test_hdel_special_chars() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "h", &[("sp@cial!", "v"), ("normal", "v2")]);

        let result = hdel(&mut ctx, &[bulk("h"), bulk("sp@cial!")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_hdel_duplicate_fields() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "h", &[("f1", "v1")]);

        // Passing same field twice: first removes it, second is a no-op
        let result = hdel(&mut ctx, &[bulk("h"), bulk("f1"), bulk("f1")]).unwrap();
        assert_eq!(result, RespValue::Integer(1)); // only counted once
    }

    #[test]
    fn test_hdel_mixed_existing() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "h", &[("a", "1"), ("b", "2"), ("c", "3")]);

        // Mix of existing (a, c) and non-existing (x, y)
        let result = hdel(
            &mut ctx,
            &[bulk("h"), bulk("a"), bulk("x"), bulk("c"), bulk("y")],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(2));
    }

    #[test]
    fn test_hdel_all_fields() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "h", &[("a", "1"), ("b", "2"), ("c", "3")]);

        let result = hdel(&mut ctx, &[bulk("h"), bulk("a"), bulk("b"), bulk("c")]).unwrap();
        assert_eq!(result, RespValue::Integer(3));

        // All fields gone, key removed
        let db = ctx.store().database(ctx.selected_db());
        assert!(db.get(&Bytes::from("h")).is_none());
    }

    #[test]
    fn test_hdel_returns_integer() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "h", &[("f", "v")]);

        let result = hdel(&mut ctx, &[bulk("h"), bulk("f")]).unwrap();
        // Verify it's specifically an Integer response
        assert!(matches!(result, RespValue::Integer(1)));
    }

    // ========================================================================
    // HEXISTS — additional tests (18 more to reach 20)
    // ========================================================================

    #[test]
    fn test_hexists_nonexistent_key() {
        let mut ctx = make_ctx();
        let result = hexists(&mut ctx, &[bulk("nokey"), bulk("f1")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_hexists_nonexistent_field() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "v1")]);
        let result = hexists(&mut ctx, &[bulk("myhash"), bulk("nosuch")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_hexists_existing_field() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "v1")]);
        let result = hexists(&mut ctx, &[bulk("myhash"), bulk("f1")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_hexists_returns_integer_type() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "v1")]);
        let result = hexists(&mut ctx, &[bulk("myhash"), bulk("f1")]).unwrap();
        assert!(matches!(result, RespValue::Integer(1)));
        let result = hexists(&mut ctx, &[bulk("myhash"), bulk("no")]).unwrap();
        assert!(matches!(result, RespValue::Integer(0)));
    }

    #[test]
    fn test_hexists_wrong_type_string_key() {
        let mut ctx = make_ctx();
        setup_string(&mut ctx, "mystr", "hello");
        let result = hexists(&mut ctx, &[bulk("mystr"), bulk("f1")]);
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    #[test]
    fn test_hexists_expired_key() {
        use std::time::Instant;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut hash = HashMap::new();
        hash.insert(Bytes::from("f1"), Bytes::from("v1"));
        let mut entry = Entry::new(RedisValue::Hash(hash));
        entry.expires_at = Some(Instant::now() - std::time::Duration::from_secs(1));
        db.set(Bytes::from("myhash"), entry);

        let result = hexists(&mut ctx, &[bulk("myhash"), bulk("f1")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_hexists_multiple_fields() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("a", "1"), ("b", "2"), ("c", "3")]);

        assert_eq!(
            hexists(&mut ctx, &[bulk("myhash"), bulk("a")]).unwrap(),
            RespValue::Integer(1)
        );
        assert_eq!(
            hexists(&mut ctx, &[bulk("myhash"), bulk("b")]).unwrap(),
            RespValue::Integer(1)
        );
        assert_eq!(
            hexists(&mut ctx, &[bulk("myhash"), bulk("c")]).unwrap(),
            RespValue::Integer(1)
        );
        assert_eq!(
            hexists(&mut ctx, &[bulk("myhash"), bulk("d")]).unwrap(),
            RespValue::Integer(0)
        );
    }

    #[test]
    fn test_hexists_after_hdel() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "v1"), ("f2", "v2")]);
        assert_eq!(
            hexists(&mut ctx, &[bulk("myhash"), bulk("f1")]).unwrap(),
            RespValue::Integer(1)
        );

        hdel(&mut ctx, &[bulk("myhash"), bulk("f1")]).unwrap();
        assert_eq!(
            hexists(&mut ctx, &[bulk("myhash"), bulk("f1")]).unwrap(),
            RespValue::Integer(0)
        );
        assert_eq!(
            hexists(&mut ctx, &[bulk("myhash"), bulk("f2")]).unwrap(),
            RespValue::Integer(1)
        );
    }

    #[test]
    fn test_hexists_special_chars_field() {
        let mut ctx = make_ctx();
        setup_hash(
            &mut ctx,
            "myhash",
            &[("field with spaces", "v"), ("field\ttab", "v2")],
        );
        assert_eq!(
            hexists(&mut ctx, &[bulk("myhash"), bulk("field with spaces")]).unwrap(),
            RespValue::Integer(1)
        );
        assert_eq!(
            hexists(&mut ctx, &[bulk("myhash"), bulk("field\ttab")]).unwrap(),
            RespValue::Integer(1)
        );
    }

    #[test]
    fn test_hexists_empty_field_name() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("", "emptyfield")]);
        assert_eq!(
            hexists(&mut ctx, &[bulk("myhash"), bulk("")]).unwrap(),
            RespValue::Integer(1)
        );
    }

    #[test]
    fn test_hexists_empty_key_name() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "", &[("f1", "v1")]);
        assert_eq!(
            hexists(&mut ctx, &[bulk(""), bulk("f1")]).unwrap(),
            RespValue::Integer(1)
        );
    }

    #[test]
    fn test_hexists_binary_field() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut hash = HashMap::new();
        hash.insert(Bytes::from_static(b"\x00\x01\x02"), Bytes::from("binary"));
        db.set(Bytes::from("myhash"), Entry::new(RedisValue::Hash(hash)));

        let result = hexists(
            &mut ctx,
            &[
                RespValue::BulkString(Bytes::from_static(b"myhash")),
                RespValue::BulkString(Bytes::from_static(b"\x00\x01\x02")),
            ],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_hexists_after_overwrite_field() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "v1")]);
        hset(&mut ctx, &[bulk("myhash"), bulk("f1"), bulk("newval")]).unwrap();
        assert_eq!(
            hexists(&mut ctx, &[bulk("myhash"), bulk("f1")]).unwrap(),
            RespValue::Integer(1)
        );
    }

    #[test]
    fn test_hexists_case_sensitive_field() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("Field", "v1")]);
        assert_eq!(
            hexists(&mut ctx, &[bulk("myhash"), bulk("Field")]).unwrap(),
            RespValue::Integer(1)
        );
        assert_eq!(
            hexists(&mut ctx, &[bulk("myhash"), bulk("field")]).unwrap(),
            RespValue::Integer(0)
        );
        assert_eq!(
            hexists(&mut ctx, &[bulk("myhash"), bulk("FIELD")]).unwrap(),
            RespValue::Integer(0)
        );
    }

    #[test]
    fn test_hexists_after_all_fields_deleted() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "v1")]);
        hdel(&mut ctx, &[bulk("myhash"), bulk("f1")]).unwrap();
        assert_eq!(
            hexists(&mut ctx, &[bulk("myhash"), bulk("f1")]).unwrap(),
            RespValue::Integer(0)
        );
    }

    #[test]
    fn test_hexists_numeric_field() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("123", "numeric"), ("-1", "neg")]);
        assert_eq!(
            hexists(&mut ctx, &[bulk("myhash"), bulk("123")]).unwrap(),
            RespValue::Integer(1)
        );
        assert_eq!(
            hexists(&mut ctx, &[bulk("myhash"), bulk("-1")]).unwrap(),
            RespValue::Integer(1)
        );
    }

    #[test]
    fn test_hexists_large_number_of_fields() {
        let mut ctx = make_ctx();
        let pairs: Vec<(&str, &str)> = vec![
            ("f0", "v0"),
            ("f1", "v1"),
            ("f2", "v2"),
            ("f3", "v3"),
            ("f4", "v4"),
            ("f5", "v5"),
            ("f6", "v6"),
            ("f7", "v7"),
            ("f8", "v8"),
            ("f9", "v9"),
        ];
        setup_hash(&mut ctx, "myhash", &pairs);
        for i in 0..10 {
            let field = format!("f{i}");
            assert_eq!(
                hexists(&mut ctx, &[bulk("myhash"), bulk(&field)]).unwrap(),
                RespValue::Integer(1)
            );
        }
        assert_eq!(
            hexists(&mut ctx, &[bulk("myhash"), bulk("f10")]).unwrap(),
            RespValue::Integer(0)
        );
    }

    // ========================================================================
    // HGETALL — additional tests (17 more to reach 20)
    // ========================================================================

    #[test]
    fn test_hgetall_nonexistent_key() {
        let mut ctx = make_ctx();
        let result = hgetall(&mut ctx, &[bulk("nokey")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![]));
    }

    #[test]
    fn test_hgetall_single_field() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "v1")]);
        let result = hgetall(&mut ctx, &[bulk("myhash")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], bulk("f1"));
            assert_eq!(arr[1], bulk("v1"));
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hgetall_wrong_type_string_key() {
        let mut ctx = make_ctx();
        setup_string(&mut ctx, "mystr", "hello");
        let result = hgetall(&mut ctx, &[bulk("mystr")]);
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    #[test]
    fn test_hgetall_expired_key() {
        use std::time::Instant;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut hash = HashMap::new();
        hash.insert(Bytes::from("f1"), Bytes::from("v1"));
        let mut entry = Entry::new(RedisValue::Hash(hash));
        entry.expires_at = Some(Instant::now() - std::time::Duration::from_secs(1));
        db.set(Bytes::from("myhash"), entry);

        let result = hgetall(&mut ctx, &[bulk("myhash")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![]));
    }

    #[test]
    fn test_hgetall_returns_sorted_fields() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("z", "26"), ("a", "1"), ("m", "13")]);
        let result = hgetall(&mut ctx, &[bulk("myhash")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 6);
            assert_eq!(arr[0], bulk("a"));
            assert_eq!(arr[1], bulk("1"));
            assert_eq!(arr[2], bulk("m"));
            assert_eq!(arr[3], bulk("13"));
            assert_eq!(arr[4], bulk("z"));
            assert_eq!(arr[5], bulk("26"));
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hgetall_special_chars() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("key with spaces", "val with\ttab")]);
        let result = hgetall(&mut ctx, &[bulk("myhash")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], bulk("key with spaces"));
            assert_eq!(arr[1], bulk("val with\ttab"));
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hgetall_empty_values() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", ""), ("f2", "")]);
        let result = hgetall(&mut ctx, &[bulk("myhash")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 4);
            assert_eq!(arr[1], bulk(""));
            assert_eq!(arr[3], bulk(""));
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hgetall_after_field_update() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "old")]);
        hset(&mut ctx, &[bulk("myhash"), bulk("f1"), bulk("new")]).unwrap();
        let result = hgetall(&mut ctx, &[bulk("myhash")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[1], bulk("new"));
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hgetall_after_partial_hdel() {
        let mut ctx = make_ctx();
        setup_hash(
            &mut ctx,
            "myhash",
            &[("f1", "v1"), ("f2", "v2"), ("f3", "v3")],
        );
        hdel(&mut ctx, &[bulk("myhash"), bulk("f2")]).unwrap();
        let result = hgetall(&mut ctx, &[bulk("myhash")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 4);
            assert_eq!(arr[0], bulk("f1"));
            assert_eq!(arr[1], bulk("v1"));
            assert_eq!(arr[2], bulk("f3"));
            assert_eq!(arr[3], bulk("v3"));
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hgetall_binary_data() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut hash = HashMap::new();
        hash.insert(Bytes::from("binfield"), Bytes::from_static(b"\x00\xff\xfe"));
        db.set(Bytes::from("binhash"), Entry::new(RedisValue::Hash(hash)));

        let result = hgetall(&mut ctx, &[bulk("binhash")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], bulk("binfield"));
            assert_eq!(
                arr[1],
                RespValue::BulkString(Bytes::from_static(b"\x00\xff\xfe"))
            );
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hgetall_return_type_is_array() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "v1")]);
        let result = hgetall(&mut ctx, &[bulk("myhash")]).unwrap();
        assert!(matches!(result, RespValue::Array(_)));
    }

    #[test]
    fn test_hgetall_empty_key_returns_empty_array() {
        let mut ctx = make_ctx();
        let result = hgetall(&mut ctx, &[bulk("emptykey")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert!(arr.is_empty());
        } else {
            panic!("Expected empty array");
        }
    }

    #[test]
    fn test_hgetall_many_fields() {
        let mut ctx = make_ctx();
        let pairs: Vec<(&str, &str)> = vec![
            ("a", "1"),
            ("b", "2"),
            ("c", "3"),
            ("d", "4"),
            ("e", "5"),
            ("f", "6"),
            ("g", "7"),
            ("h", "8"),
            ("i", "9"),
            ("j", "10"),
        ];
        setup_hash(&mut ctx, "big", &pairs);
        let result = hgetall(&mut ctx, &[bulk("big")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 20);
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hgetall_numeric_fields_and_values() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("100", "200"), ("0", "-1")]);
        let result = hgetall(&mut ctx, &[bulk("myhash")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 4);
            assert_eq!(arr[0], bulk("0"));
            assert_eq!(arr[1], bulk("-1"));
            assert_eq!(arr[2], bulk("100"));
            assert_eq!(arr[3], bulk("200"));
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hgetall_empty_field_name() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("", "emptyname")]);
        let result = hgetall(&mut ctx, &[bulk("myhash")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], bulk(""));
            assert_eq!(arr[1], bulk("emptyname"));
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hgetall_after_hmset_overwrite() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "old1"), ("f2", "old2")]);
        hmset(
            &mut ctx,
            &[
                bulk("myhash"),
                bulk("f1"),
                bulk("new1"),
                bulk("f3"),
                bulk("new3"),
            ],
        )
        .unwrap();
        let result = hgetall(&mut ctx, &[bulk("myhash")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 6);
            assert_eq!(arr[0], bulk("f1"));
            assert_eq!(arr[1], bulk("new1"));
            assert_eq!(arr[2], bulk("f2"));
            assert_eq!(arr[3], bulk("old2"));
            assert_eq!(arr[4], bulk("f3"));
            assert_eq!(arr[5], bulk("new3"));
        } else {
            panic!("Expected array");
        }
    }

    // ========================================================================
    // HMSET — additional tests (18 more to reach 20)
    // ========================================================================

    #[test]
    fn test_hmset_basic_ok_response() {
        let mut ctx = make_ctx();
        let result = hmset(&mut ctx, &[bulk("myhash"), bulk("f1"), bulk("v1")]).unwrap();
        assert_eq!(result, RespValue::ok());
    }

    #[test]
    fn test_hmset_multiple_pairs() {
        let mut ctx = make_ctx();
        hmset(
            &mut ctx,
            &[
                bulk("myhash"),
                bulk("a"),
                bulk("1"),
                bulk("b"),
                bulk("2"),
                bulk("c"),
                bulk("3"),
            ],
        )
        .unwrap();
        assert_eq!(
            hget(&mut ctx, &[bulk("myhash"), bulk("a")]).unwrap(),
            bulk("1")
        );
        assert_eq!(
            hget(&mut ctx, &[bulk("myhash"), bulk("b")]).unwrap(),
            bulk("2")
        );
        assert_eq!(
            hget(&mut ctx, &[bulk("myhash"), bulk("c")]).unwrap(),
            bulk("3")
        );
    }

    #[test]
    fn test_hmset_overwrite_existing_fields() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "old1"), ("f2", "old2")]);
        hmset(&mut ctx, &[bulk("myhash"), bulk("f1"), bulk("new1")]).unwrap();
        assert_eq!(
            hget(&mut ctx, &[bulk("myhash"), bulk("f1")]).unwrap(),
            bulk("new1")
        );
        assert_eq!(
            hget(&mut ctx, &[bulk("myhash"), bulk("f2")]).unwrap(),
            bulk("old2")
        );
    }

    #[test]
    fn test_hmset_wrong_type_string_key() {
        let mut ctx = make_ctx();
        setup_string(&mut ctx, "mystr", "hello");
        let result = hmset(&mut ctx, &[bulk("mystr"), bulk("f1"), bulk("v1")]);
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    #[test]
    fn test_hmset_expired_key_creates_new() {
        use std::time::Instant;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut hash = HashMap::new();
        hash.insert(Bytes::from("old"), Bytes::from("val"));
        let mut entry = Entry::new(RedisValue::Hash(hash));
        entry.expires_at = Some(Instant::now() - std::time::Duration::from_secs(1));
        db.set(Bytes::from("myhash"), entry);

        hmset(&mut ctx, &[bulk("myhash"), bulk("new"), bulk("val")]).unwrap();
        assert_eq!(
            hget(&mut ctx, &[bulk("myhash"), bulk("old")]).unwrap(),
            RespValue::Null
        );
        assert_eq!(
            hget(&mut ctx, &[bulk("myhash"), bulk("new")]).unwrap(),
            bulk("val")
        );
    }

    #[test]
    fn test_hmset_empty_value() {
        let mut ctx = make_ctx();
        hmset(&mut ctx, &[bulk("myhash"), bulk("f1"), bulk("")]).unwrap();
        assert_eq!(
            hget(&mut ctx, &[bulk("myhash"), bulk("f1")]).unwrap(),
            bulk("")
        );
    }

    #[test]
    fn test_hmset_empty_field_name() {
        let mut ctx = make_ctx();
        hmset(&mut ctx, &[bulk("myhash"), bulk(""), bulk("val")]).unwrap();
        assert_eq!(
            hget(&mut ctx, &[bulk("myhash"), bulk("")]).unwrap(),
            bulk("val")
        );
    }

    #[test]
    fn test_hmset_special_chars() {
        let mut ctx = make_ctx();
        hmset(
            &mut ctx,
            &[bulk("myhash"), bulk("hello world"), bulk("foo\tbar")],
        )
        .unwrap();
        assert_eq!(
            hget(&mut ctx, &[bulk("myhash"), bulk("hello world")]).unwrap(),
            bulk("foo\tbar")
        );
    }

    #[test]
    fn test_hmset_duplicate_fields_last_wins() {
        let mut ctx = make_ctx();
        hmset(
            &mut ctx,
            &[
                bulk("myhash"),
                bulk("f1"),
                bulk("first"),
                bulk("f1"),
                bulk("second"),
            ],
        )
        .unwrap();
        assert_eq!(
            hget(&mut ctx, &[bulk("myhash"), bulk("f1")]).unwrap(),
            bulk("second")
        );
    }

    #[test]
    fn test_hmset_returns_ok_type() {
        let mut ctx = make_ctx();
        let result = hmset(&mut ctx, &[bulk("myhash"), bulk("f1"), bulk("v1")]).unwrap();
        assert!(matches!(result, RespValue::SimpleString(_)));
    }

    #[test]
    fn test_hmset_odd_pair_count_wrong_arity() {
        let mut ctx = make_ctx();
        assert!(matches!(
            hmset(&mut ctx, &[bulk("key"), bulk("f1"), bulk("v1"), bulk("f2")]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_hmset_no_args_wrong_arity() {
        let mut ctx = make_ctx();
        assert!(matches!(
            hmset(&mut ctx, &[]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_hmset_creates_new_key() {
        let mut ctx = make_ctx();
        hmset(&mut ctx, &[bulk("brand_new"), bulk("f1"), bulk("v1")]).unwrap();
        let result = hlen(&mut ctx, &[bulk("brand_new")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_hmset_preserves_unmentioned_fields() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("keep", "me"), ("update", "old")]);
        hmset(
            &mut ctx,
            &[
                bulk("myhash"),
                bulk("update"),
                bulk("new"),
                bulk("add"),
                bulk("val"),
            ],
        )
        .unwrap();
        assert_eq!(
            hget(&mut ctx, &[bulk("myhash"), bulk("keep")]).unwrap(),
            bulk("me")
        );
        assert_eq!(
            hget(&mut ctx, &[bulk("myhash"), bulk("update")]).unwrap(),
            bulk("new")
        );
        assert_eq!(
            hget(&mut ctx, &[bulk("myhash"), bulk("add")]).unwrap(),
            bulk("val")
        );
    }

    #[test]
    fn test_hmset_numeric_field_and_value() {
        let mut ctx = make_ctx();
        hmset(&mut ctx, &[bulk("myhash"), bulk("123"), bulk("456")]).unwrap();
        assert_eq!(
            hget(&mut ctx, &[bulk("myhash"), bulk("123")]).unwrap(),
            bulk("456")
        );
    }

    #[test]
    fn test_hmset_many_fields_at_once() {
        let mut ctx = make_ctx();
        let mut args = vec![bulk("myhash")];
        for i in 0..50 {
            args.push(bulk(&format!("field_{i}")));
            args.push(bulk(&format!("value_{i}")));
        }
        hmset(&mut ctx, &args).unwrap();
        assert_eq!(
            hlen(&mut ctx, &[bulk("myhash")]).unwrap(),
            RespValue::Integer(50)
        );
    }

    #[test]
    fn test_hmset_binary_data() {
        let mut ctx = make_ctx();
        let key = Bytes::from("myhash");
        let field = Bytes::from_static(b"\x00\x01");
        let value = Bytes::from_static(b"\xff\xfe");
        let result = hmset(
            &mut ctx,
            &[
                RespValue::BulkString(key.clone()),
                RespValue::BulkString(field.clone()),
                RespValue::BulkString(value.clone()),
            ],
        )
        .unwrap();
        assert_eq!(result, RespValue::ok());

        let db = ctx.store().database(ctx.selected_db());
        let stored = db.get(b"myhash").unwrap();
        if let RedisValue::Hash(h) = &stored.value {
            assert_eq!(h.get(&field), Some(&value));
        } else {
            panic!("Expected hash");
        }
    }

    // ========================================================================
    // HMGET — additional tests (18 more to reach 20)
    // ========================================================================

    #[test]
    fn test_hmget_basic() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "v1"), ("f2", "v2")]);
        let result = hmget(&mut ctx, &[bulk("myhash"), bulk("f1"), bulk("f2")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr, vec![bulk("v1"), bulk("v2")]);
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hmget_nonexistent_key() {
        let mut ctx = make_ctx();
        let result = hmget(&mut ctx, &[bulk("nokey"), bulk("f1"), bulk("f2")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr, vec![RespValue::Null, RespValue::Null]);
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hmget_wrong_type_string_key() {
        let mut ctx = make_ctx();
        setup_string(&mut ctx, "mystr", "hello");
        let result = hmget(&mut ctx, &[bulk("mystr"), bulk("f1")]);
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    #[test]
    fn test_hmget_expired_key() {
        use std::time::Instant;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut hash = HashMap::new();
        hash.insert(Bytes::from("f1"), Bytes::from("v1"));
        let mut entry = Entry::new(RedisValue::Hash(hash));
        entry.expires_at = Some(Instant::now() - std::time::Duration::from_secs(1));
        db.set(Bytes::from("myhash"), entry);

        let result = hmget(&mut ctx, &[bulk("myhash"), bulk("f1")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr, vec![RespValue::Null]);
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hmget_mix_existing_and_missing() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("a", "1"), ("c", "3")]);
        let result = hmget(&mut ctx, &[bulk("myhash"), bulk("a"), bulk("b"), bulk("c")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr, vec![bulk("1"), RespValue::Null, bulk("3")]);
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hmget_duplicate_fields() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "v1")]);
        let result = hmget(
            &mut ctx,
            &[bulk("myhash"), bulk("f1"), bulk("f1"), bulk("f1")],
        )
        .unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr, vec![bulk("v1"), bulk("v1"), bulk("v1")]);
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hmget_single_field() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "v1")]);
        let result = hmget(&mut ctx, &[bulk("myhash"), bulk("f1")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 1);
            assert_eq!(arr[0], bulk("v1"));
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hmget_returns_array_type() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "v1")]);
        let result = hmget(&mut ctx, &[bulk("myhash"), bulk("f1")]).unwrap();
        assert!(matches!(result, RespValue::Array(_)));
    }

    #[test]
    fn test_hmget_empty_value() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "")]);
        let result = hmget(&mut ctx, &[bulk("myhash"), bulk("f1")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr[0], bulk(""));
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hmget_special_chars_field() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("hello world", "val")]);
        let result = hmget(&mut ctx, &[bulk("myhash"), bulk("hello world")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr[0], bulk("val"));
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hmget_no_fields_wrong_arity() {
        let mut ctx = make_ctx();
        assert!(matches!(
            hmget(&mut ctx, &[]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_hmget_only_key_wrong_arity() {
        let mut ctx = make_ctx();
        assert!(matches!(
            hmget(&mut ctx, &[bulk("key")]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_hmget_many_fields() {
        let mut ctx = make_ctx();
        let pairs: Vec<(&str, &str)> = vec![
            ("f0", "v0"),
            ("f1", "v1"),
            ("f2", "v2"),
            ("f3", "v3"),
            ("f4", "v4"),
        ];
        setup_hash(&mut ctx, "myhash", &pairs);
        let result = hmget(
            &mut ctx,
            &[
                bulk("myhash"),
                bulk("f0"),
                bulk("f1"),
                bulk("f2"),
                bulk("f3"),
                bulk("f4"),
            ],
        )
        .unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 5);
            for i in 0..5 {
                assert_eq!(arr[i], bulk(&format!("v{i}")));
            }
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hmget_after_field_deleted() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "v1"), ("f2", "v2")]);
        hdel(&mut ctx, &[bulk("myhash"), bulk("f1")]).unwrap();
        let result = hmget(&mut ctx, &[bulk("myhash"), bulk("f1"), bulk("f2")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr[0], RespValue::Null);
            assert_eq!(arr[1], bulk("v2"));
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hmget_after_field_overwrite() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "old")]);
        hset(&mut ctx, &[bulk("myhash"), bulk("f1"), bulk("new")]).unwrap();
        let result = hmget(&mut ctx, &[bulk("myhash"), bulk("f1")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr[0], bulk("new"));
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hmget_case_sensitive() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("Field", "val")]);
        let result = hmget(
            &mut ctx,
            &[bulk("myhash"), bulk("Field"), bulk("field"), bulk("FIELD")],
        )
        .unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr[0], bulk("val"));
            assert_eq!(arr[1], RespValue::Null);
            assert_eq!(arr[2], RespValue::Null);
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hmget_numeric_fields() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("0", "zero"), ("1", "one")]);
        let result = hmget(&mut ctx, &[bulk("myhash"), bulk("0"), bulk("1"), bulk("2")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr, vec![bulk("zero"), bulk("one"), RespValue::Null]);
        } else {
            panic!("Expected array");
        }
    }

    // ========================================================================
    // HINCRBY — additional tests (16 more to reach 20)
    // ========================================================================

    #[test]
    fn test_hincrby_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        assert!(matches!(
            hincrby(&mut ctx, &[]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_hincrby_wrong_arity_missing_increment() {
        let mut ctx = make_ctx();
        assert!(matches!(
            hincrby(&mut ctx, &[bulk("key"), bulk("field")]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_hincrby_wrong_type_string_key() {
        let mut ctx = make_ctx();
        setup_string(&mut ctx, "mystr", "hello");
        let result = hincrby(&mut ctx, &[bulk("mystr"), bulk("f1"), bulk("1")]);
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    #[test]
    fn test_hincrby_non_integer_increment() {
        let mut ctx = make_ctx();
        let result = hincrby(&mut ctx, &[bulk("myhash"), bulk("f1"), bulk("notint")]);
        assert!(matches!(result, Err(CommandError::NotAnInteger)));
    }

    #[test]
    fn test_hincrby_float_increment_rejected() {
        let mut ctx = make_ctx();
        let result = hincrby(&mut ctx, &[bulk("myhash"), bulk("f1"), bulk("1.5")]);
        assert!(matches!(result, Err(CommandError::NotAnInteger)));
    }

    #[test]
    fn test_hincrby_expired_key() {
        use std::time::Instant;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut hash = HashMap::new();
        hash.insert(Bytes::from("counter"), Bytes::from("100"));
        let mut entry = Entry::new(RedisValue::Hash(hash));
        entry.expires_at = Some(Instant::now() - std::time::Duration::from_secs(1));
        db.set(Bytes::from("myhash"), entry);

        let result = hincrby(&mut ctx, &[bulk("myhash"), bulk("counter"), bulk("5")]).unwrap();
        assert_eq!(result, RespValue::Integer(5));
    }

    #[test]
    fn test_hincrby_negative_result() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("counter", "5")]);
        let result = hincrby(&mut ctx, &[bulk("myhash"), bulk("counter"), bulk("-10")]).unwrap();
        assert_eq!(result, RespValue::Integer(-5));
    }

    #[test]
    fn test_hincrby_zero_increment() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("counter", "42")]);
        let result = hincrby(&mut ctx, &[bulk("myhash"), bulk("counter"), bulk("0")]).unwrap();
        assert_eq!(result, RespValue::Integer(42));
    }

    #[test]
    fn test_hincrby_returns_integer_type() {
        let mut ctx = make_ctx();
        let result = hincrby(&mut ctx, &[bulk("myhash"), bulk("c"), bulk("7")]).unwrap();
        assert!(matches!(result, RespValue::Integer(7)));
    }

    #[test]
    fn test_hincrby_negative_overflow() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("field", "-9223372036854775808")]);
        let result = hincrby(&mut ctx, &[bulk("myhash"), bulk("field"), bulk("-1")]);
        assert!(matches!(result, Err(CommandError::NotAnInteger)));
    }

    #[test]
    fn test_hincrby_multiple_increments() {
        let mut ctx = make_ctx();
        hincrby(&mut ctx, &[bulk("myhash"), bulk("c"), bulk("1")]).unwrap();
        hincrby(&mut ctx, &[bulk("myhash"), bulk("c"), bulk("2")]).unwrap();
        hincrby(&mut ctx, &[bulk("myhash"), bulk("c"), bulk("3")]).unwrap();
        let result = hincrby(&mut ctx, &[bulk("myhash"), bulk("c"), bulk("4")]).unwrap();
        assert_eq!(result, RespValue::Integer(10));
    }

    #[test]
    fn test_hincrby_different_fields_same_hash() {
        let mut ctx = make_ctx();
        hincrby(&mut ctx, &[bulk("myhash"), bulk("a"), bulk("1")]).unwrap();
        hincrby(&mut ctx, &[bulk("myhash"), bulk("b"), bulk("2")]).unwrap();
        hincrby(&mut ctx, &[bulk("myhash"), bulk("c"), bulk("3")]).unwrap();

        assert_eq!(
            hget(&mut ctx, &[bulk("myhash"), bulk("a")]).unwrap(),
            bulk("1")
        );
        assert_eq!(
            hget(&mut ctx, &[bulk("myhash"), bulk("b")]).unwrap(),
            bulk("2")
        );
        assert_eq!(
            hget(&mut ctx, &[bulk("myhash"), bulk("c")]).unwrap(),
            bulk("3")
        );
    }

    #[test]
    fn test_hincrby_large_positive_increment() {
        let mut ctx = make_ctx();
        let result = hincrby(
            &mut ctx,
            &[bulk("myhash"), bulk("c"), bulk("9223372036854775806")],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(9223372036854775806));
        let result = hincrby(&mut ctx, &[bulk("myhash"), bulk("c"), bulk("1")]).unwrap();
        assert_eq!(result, RespValue::Integer(9223372036854775807));
    }

    #[test]
    fn test_hincrby_stores_as_string() {
        let mut ctx = make_ctx();
        hincrby(&mut ctx, &[bulk("myhash"), bulk("c"), bulk("42")]).unwrap();
        let result = hget(&mut ctx, &[bulk("myhash"), bulk("c")]).unwrap();
        assert_eq!(result, bulk("42"));
    }

    #[test]
    fn test_hincrby_field_with_leading_zeros() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("c", "007")]);
        let result = hincrby(&mut ctx, &[bulk("myhash"), bulk("c"), bulk("1")]).unwrap();
        assert_eq!(result, RespValue::Integer(8));
    }

    // ========================================================================
    // HINCRBYFLOAT — additional tests (15 more to reach 20)
    // ========================================================================

    #[test]
    fn test_hincrbyfloat_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        assert!(matches!(
            hincrbyfloat(&mut ctx, &[]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_hincrbyfloat_wrong_arity_missing_incr() {
        let mut ctx = make_ctx();
        assert!(matches!(
            hincrbyfloat(&mut ctx, &[bulk("key"), bulk("field")]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_hincrbyfloat_wrong_type_string_key() {
        let mut ctx = make_ctx();
        setup_string(&mut ctx, "mystr", "hello");
        let result = hincrbyfloat(&mut ctx, &[bulk("mystr"), bulk("f1"), bulk("1.0")]);
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    #[test]
    fn test_hincrbyfloat_expired_key() {
        use std::time::Instant;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut hash = HashMap::new();
        hash.insert(Bytes::from("f"), Bytes::from("100.5"));
        let mut entry = Entry::new(RedisValue::Hash(hash));
        entry.expires_at = Some(Instant::now() - std::time::Duration::from_secs(1));
        db.set(Bytes::from("myhash"), entry);

        let result = hincrbyfloat(&mut ctx, &[bulk("myhash"), bulk("f"), bulk("1.5")]).unwrap();
        if let RespValue::BulkString(b) = result {
            let val: f64 = std::str::from_utf8(&b).unwrap().parse().unwrap();
            assert!((val - 1.5).abs() < 0.0001);
        } else {
            panic!("Expected bulk string");
        }
    }

    #[test]
    fn test_hincrbyfloat_zero_increment() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f", "3.14")]);
        let result = hincrbyfloat(&mut ctx, &[bulk("myhash"), bulk("f"), bulk("0")]).unwrap();
        if let RespValue::BulkString(b) = result {
            let val: f64 = std::str::from_utf8(&b).unwrap().parse().unwrap();
            assert!((val - 3.14).abs() < 0.0001);
        } else {
            panic!("Expected bulk string");
        }
    }

    #[test]
    fn test_hincrbyfloat_integer_stored_value() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f", "10")]);
        let result = hincrbyfloat(&mut ctx, &[bulk("myhash"), bulk("f"), bulk("1.5")]).unwrap();
        if let RespValue::BulkString(b) = result {
            let val: f64 = std::str::from_utf8(&b).unwrap().parse().unwrap();
            assert!((val - 11.5).abs() < 0.0001);
        } else {
            panic!("Expected bulk string");
        }
    }

    #[test]
    fn test_hincrbyfloat_result_becomes_integer() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f", "1.5")]);
        let result = hincrbyfloat(&mut ctx, &[bulk("myhash"), bulk("f"), bulk("0.5")]).unwrap();
        if let RespValue::BulkString(b) = result {
            assert_eq!(std::str::from_utf8(&b).unwrap(), "2");
        } else {
            panic!("Expected bulk string");
        }
    }

    #[test]
    fn test_hincrbyfloat_returns_bulk_string() {
        let mut ctx = make_ctx();
        let result = hincrbyfloat(&mut ctx, &[bulk("myhash"), bulk("f"), bulk("1.5")]).unwrap();
        assert!(matches!(result, RespValue::BulkString(_)));
    }

    #[test]
    fn test_hincrbyfloat_infinity_rejected() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f", "1e308")]);
        let result = hincrbyfloat(&mut ctx, &[bulk("myhash"), bulk("f"), bulk("1e308")]);
        assert!(matches!(result, Err(CommandError::NotAFloat)));
    }

    #[test]
    fn test_hincrbyfloat_non_numeric_increment() {
        let mut ctx = make_ctx();
        let result = hincrbyfloat(&mut ctx, &[bulk("myhash"), bulk("f"), bulk("notfloat")]);
        assert!(matches!(result, Err(CommandError::NotAFloat)));
    }

    #[test]
    fn test_hincrbyfloat_multiple_increments() {
        let mut ctx = make_ctx();
        hincrbyfloat(&mut ctx, &[bulk("myhash"), bulk("f"), bulk("1.1")]).unwrap();
        hincrbyfloat(&mut ctx, &[bulk("myhash"), bulk("f"), bulk("2.2")]).unwrap();
        let result = hincrbyfloat(&mut ctx, &[bulk("myhash"), bulk("f"), bulk("3.3")]).unwrap();
        if let RespValue::BulkString(b) = result {
            let val: f64 = std::str::from_utf8(&b).unwrap().parse().unwrap();
            assert!((val - 6.6).abs() < 0.0001);
        } else {
            panic!("Expected bulk string");
        }
    }

    #[test]
    fn test_hincrbyfloat_large_negative() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f", "100.0")]);
        let result = hincrbyfloat(&mut ctx, &[bulk("myhash"), bulk("f"), bulk("-200.5")]).unwrap();
        if let RespValue::BulkString(b) = result {
            let val: f64 = std::str::from_utf8(&b).unwrap().parse().unwrap();
            assert!((val - (-100.5)).abs() < 0.0001);
        } else {
            panic!("Expected bulk string");
        }
    }

    #[test]
    fn test_hincrbyfloat_scientific_notation() {
        let mut ctx = make_ctx();
        let result = hincrbyfloat(&mut ctx, &[bulk("myhash"), bulk("f"), bulk("1.5e2")]).unwrap();
        if let RespValue::BulkString(b) = result {
            let val: f64 = std::str::from_utf8(&b).unwrap().parse().unwrap();
            assert!((val - 150.0).abs() < 0.0001);
        } else {
            panic!("Expected bulk string");
        }
    }

    #[test]
    fn test_hincrbyfloat_stored_value_verified() {
        let mut ctx = make_ctx();
        hincrbyfloat(&mut ctx, &[bulk("myhash"), bulk("f"), bulk("3.14")]).unwrap();
        let stored = hget(&mut ctx, &[bulk("myhash"), bulk("f")]).unwrap();
        if let RespValue::BulkString(b) = stored {
            let val: f64 = std::str::from_utf8(&b).unwrap().parse().unwrap();
            assert!((val - 3.14).abs() < 0.0001);
        } else {
            panic!("Expected bulk string");
        }
    }

    // ========================================================================
    // HKEYS — additional tests (19 more to reach 20)
    // ========================================================================

    #[test]
    fn test_hkeys_nonexistent_key() {
        let mut ctx = make_ctx();
        let result = hkeys(&mut ctx, &[bulk("nokey")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![]));
    }

    #[test]
    fn test_hkeys_wrong_arity() {
        let mut ctx = make_ctx();
        assert!(matches!(
            hkeys(&mut ctx, &[]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_hkeys_wrong_type_string_key() {
        let mut ctx = make_ctx();
        setup_string(&mut ctx, "mystr", "hello");
        let result = hkeys(&mut ctx, &[bulk("mystr")]);
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    #[test]
    fn test_hkeys_expired_key() {
        use std::time::Instant;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut hash = HashMap::new();
        hash.insert(Bytes::from("f1"), Bytes::from("v1"));
        let mut entry = Entry::new(RedisValue::Hash(hash));
        entry.expires_at = Some(Instant::now() - std::time::Duration::from_secs(1));
        db.set(Bytes::from("myhash"), entry);

        let result = hkeys(&mut ctx, &[bulk("myhash")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![]));
    }

    #[test]
    fn test_hkeys_single_field() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("only", "val")]);
        let result = hkeys(&mut ctx, &[bulk("myhash")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr, vec![bulk("only")]);
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hkeys_sorted_output() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("c", "3"), ("a", "1"), ("b", "2")]);
        let result = hkeys(&mut ctx, &[bulk("myhash")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr, vec![bulk("a"), bulk("b"), bulk("c")]);
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hkeys_returns_array_type() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "v1")]);
        let result = hkeys(&mut ctx, &[bulk("myhash")]).unwrap();
        assert!(matches!(result, RespValue::Array(_)));
    }

    #[test]
    fn test_hkeys_after_hdel() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("a", "1"), ("b", "2"), ("c", "3")]);
        hdel(&mut ctx, &[bulk("myhash"), bulk("b")]).unwrap();
        let result = hkeys(&mut ctx, &[bulk("myhash")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr, vec![bulk("a"), bulk("c")]);
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hkeys_after_hset_adds_new() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("a", "1")]);
        hset(&mut ctx, &[bulk("myhash"), bulk("b"), bulk("2")]).unwrap();
        let result = hkeys(&mut ctx, &[bulk("myhash")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr, vec![bulk("a"), bulk("b")]);
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hkeys_special_chars() {
        let mut ctx = make_ctx();
        setup_hash(
            &mut ctx,
            "myhash",
            &[("hello world", "v"), ("tab\there", "v2")],
        );
        let result = hkeys(&mut ctx, &[bulk("myhash")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 2);
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hkeys_empty_field_name() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("", "val"), ("a", "other")]);
        let result = hkeys(&mut ctx, &[bulk("myhash")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], bulk(""));
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hkeys_numeric_fields() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("100", "a"), ("20", "b"), ("3", "c")]);
        let result = hkeys(&mut ctx, &[bulk("myhash")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr, vec![bulk("100"), bulk("20"), bulk("3")]);
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hkeys_many_fields() {
        let mut ctx = make_ctx();
        let pairs: Vec<(&str, &str)> =
            vec![("e", "5"), ("d", "4"), ("c", "3"), ("b", "2"), ("a", "1")];
        setup_hash(&mut ctx, "myhash", &pairs);
        let result = hkeys(&mut ctx, &[bulk("myhash")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 5);
            assert_eq!(
                arr,
                vec![bulk("a"), bulk("b"), bulk("c"), bulk("d"), bulk("e")]
            );
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hkeys_does_not_return_values() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "v1"), ("f2", "v2")]);
        let result = hkeys(&mut ctx, &[bulk("myhash")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 2);
            assert!(!arr.contains(&bulk("v1")));
            assert!(!arr.contains(&bulk("v2")));
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hkeys_after_all_deleted_returns_empty() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "v1")]);
        hdel(&mut ctx, &[bulk("myhash"), bulk("f1")]).unwrap();
        let result = hkeys(&mut ctx, &[bulk("myhash")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![]));
    }

    #[test]
    fn test_hkeys_binary_field() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut hash = HashMap::new();
        hash.insert(Bytes::from_static(b"\x01\x02"), Bytes::from("binary"));
        db.set(Bytes::from("myhash"), Entry::new(RedisValue::Hash(hash)));

        let result = hkeys(&mut ctx, &[bulk("myhash")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 1);
            assert_eq!(
                arr[0],
                RespValue::BulkString(Bytes::from_static(b"\x01\x02"))
            );
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hkeys_consistency_with_hlen() {
        let mut ctx = make_ctx();
        setup_hash(
            &mut ctx,
            "myhash",
            &[("a", "1"), ("b", "2"), ("c", "3"), ("d", "4")],
        );
        let keys_result = hkeys(&mut ctx, &[bulk("myhash")]).unwrap();
        let len_result = hlen(&mut ctx, &[bulk("myhash")]).unwrap();
        if let (RespValue::Array(arr), RespValue::Integer(len)) = (keys_result, len_result) {
            assert_eq!(arr.len() as i64, len);
        } else {
            panic!("Unexpected types");
        }
    }

    #[test]
    fn test_hkeys_case_sensitive() {
        let mut ctx = make_ctx();
        setup_hash(
            &mut ctx,
            "myhash",
            &[("ABC", "1"), ("abc", "2"), ("Abc", "3")],
        );
        let result = hkeys(&mut ctx, &[bulk("myhash")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], bulk("ABC"));
            assert_eq!(arr[1], bulk("Abc"));
            assert_eq!(arr[2], bulk("abc"));
        } else {
            panic!("Expected array");
        }
    }

    // ========================================================================
    // HVALS — additional tests (19 more to reach 20)
    // ========================================================================

    #[test]
    fn test_hvals_nonexistent_key() {
        let mut ctx = make_ctx();
        let result = hvals(&mut ctx, &[bulk("nokey")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![]));
    }

    #[test]
    fn test_hvals_wrong_arity() {
        let mut ctx = make_ctx();
        assert!(matches!(
            hvals(&mut ctx, &[]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_hvals_wrong_type_string_key() {
        let mut ctx = make_ctx();
        setup_string(&mut ctx, "mystr", "hello");
        let result = hvals(&mut ctx, &[bulk("mystr")]);
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    #[test]
    fn test_hvals_expired_key() {
        use std::time::Instant;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut hash = HashMap::new();
        hash.insert(Bytes::from("f1"), Bytes::from("v1"));
        let mut entry = Entry::new(RedisValue::Hash(hash));
        entry.expires_at = Some(Instant::now() - std::time::Duration::from_secs(1));
        db.set(Bytes::from("myhash"), entry);

        let result = hvals(&mut ctx, &[bulk("myhash")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![]));
    }

    #[test]
    fn test_hvals_single_field() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "v1")]);
        let result = hvals(&mut ctx, &[bulk("myhash")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr, vec![bulk("v1")]);
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hvals_sorted_by_key() {
        let mut ctx = make_ctx();
        setup_hash(
            &mut ctx,
            "myhash",
            &[("c", "val_c"), ("a", "val_a"), ("b", "val_b")],
        );
        let result = hvals(&mut ctx, &[bulk("myhash")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr, vec![bulk("val_a"), bulk("val_b"), bulk("val_c")]);
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hvals_returns_array_type() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "v1")]);
        let result = hvals(&mut ctx, &[bulk("myhash")]).unwrap();
        assert!(matches!(result, RespValue::Array(_)));
    }

    #[test]
    fn test_hvals_empty_values() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("a", ""), ("b", "")]);
        let result = hvals(&mut ctx, &[bulk("myhash")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr, vec![bulk(""), bulk("")]);
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hvals_after_field_update() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "old")]);
        hset(&mut ctx, &[bulk("myhash"), bulk("f1"), bulk("new")]).unwrap();
        let result = hvals(&mut ctx, &[bulk("myhash")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr, vec![bulk("new")]);
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hvals_after_hdel() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("a", "1"), ("b", "2"), ("c", "3")]);
        hdel(&mut ctx, &[bulk("myhash"), bulk("b")]).unwrap();
        let result = hvals(&mut ctx, &[bulk("myhash")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr, vec![bulk("1"), bulk("3")]);
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hvals_special_chars() {
        let mut ctx = make_ctx();
        setup_hash(
            &mut ctx,
            "myhash",
            &[("f1", "hello world"), ("f2", "tab\there")],
        );
        let result = hvals(&mut ctx, &[bulk("myhash")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 2);
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hvals_duplicate_values() {
        let mut ctx = make_ctx();
        setup_hash(
            &mut ctx,
            "myhash",
            &[("a", "same"), ("b", "same"), ("c", "same")],
        );
        let result = hvals(&mut ctx, &[bulk("myhash")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr, vec![bulk("same"), bulk("same"), bulk("same")]);
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hvals_consistency_with_hlen() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("a", "1"), ("b", "2"), ("c", "3")]);
        let vals_result = hvals(&mut ctx, &[bulk("myhash")]).unwrap();
        let len_result = hlen(&mut ctx, &[bulk("myhash")]).unwrap();
        if let (RespValue::Array(arr), RespValue::Integer(len)) = (vals_result, len_result) {
            assert_eq!(arr.len() as i64, len);
        } else {
            panic!("Unexpected types");
        }
    }

    #[test]
    fn test_hvals_many_fields() {
        let mut ctx = make_ctx();
        let pairs: Vec<(&str, &str)> = vec![
            ("a", "1"),
            ("b", "2"),
            ("c", "3"),
            ("d", "4"),
            ("e", "5"),
            ("f", "6"),
            ("g", "7"),
            ("h", "8"),
            ("i", "9"),
            ("j", "10"),
        ];
        setup_hash(&mut ctx, "myhash", &pairs);
        let result = hvals(&mut ctx, &[bulk("myhash")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 10);
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hvals_after_all_deleted_returns_empty() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "v1")]);
        hdel(&mut ctx, &[bulk("myhash"), bulk("f1")]).unwrap();
        let result = hvals(&mut ctx, &[bulk("myhash")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![]));
    }

    #[test]
    fn test_hvals_binary_data() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut hash = HashMap::new();
        hash.insert(Bytes::from("f"), Bytes::from_static(b"\x00\xff"));
        db.set(Bytes::from("binhash"), Entry::new(RedisValue::Hash(hash)));

        let result = hvals(&mut ctx, &[bulk("binhash")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(
                arr[0],
                RespValue::BulkString(Bytes::from_static(b"\x00\xff"))
            );
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hvals_does_not_return_keys() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "v1"), ("f2", "v2")]);
        let result = hvals(&mut ctx, &[bulk("myhash")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 2);
            assert!(!arr.contains(&bulk("f1")));
            assert!(!arr.contains(&bulk("f2")));
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hvals_numeric_values() {
        let mut ctx = make_ctx();
        setup_hash(
            &mut ctx,
            "myhash",
            &[("a", "123"), ("b", "-456"), ("c", "0")],
        );
        let result = hvals(&mut ctx, &[bulk("myhash")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr, vec![bulk("123"), bulk("-456"), bulk("0")]);
        } else {
            panic!("Expected array");
        }
    }

    // ========================================================================
    // HLEN — additional tests (19 more to reach 20)
    // ========================================================================

    #[test]
    fn test_hlen_nonexistent_key() {
        let mut ctx = make_ctx();
        let result = hlen(&mut ctx, &[bulk("nokey")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_hlen_wrong_arity() {
        let mut ctx = make_ctx();
        assert!(matches!(
            hlen(&mut ctx, &[]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_hlen_wrong_type_string_key() {
        let mut ctx = make_ctx();
        setup_string(&mut ctx, "mystr", "hello");
        let result = hlen(&mut ctx, &[bulk("mystr")]);
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    #[test]
    fn test_hlen_expired_key() {
        use std::time::Instant;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut hash = HashMap::new();
        hash.insert(Bytes::from("f1"), Bytes::from("v1"));
        hash.insert(Bytes::from("f2"), Bytes::from("v2"));
        let mut entry = Entry::new(RedisValue::Hash(hash));
        entry.expires_at = Some(Instant::now() - std::time::Duration::from_secs(1));
        db.set(Bytes::from("myhash"), entry);

        let result = hlen(&mut ctx, &[bulk("myhash")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_hlen_single_field() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "v1")]);
        let result = hlen(&mut ctx, &[bulk("myhash")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_hlen_multiple_fields() {
        let mut ctx = make_ctx();
        setup_hash(
            &mut ctx,
            "myhash",
            &[("a", "1"), ("b", "2"), ("c", "3"), ("d", "4"), ("e", "5")],
        );
        let result = hlen(&mut ctx, &[bulk("myhash")]).unwrap();
        assert_eq!(result, RespValue::Integer(5));
    }

    #[test]
    fn test_hlen_returns_integer_type() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "v1")]);
        let result = hlen(&mut ctx, &[bulk("myhash")]).unwrap();
        assert!(matches!(result, RespValue::Integer(_)));
    }

    #[test]
    fn test_hlen_after_hset_increases() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "v1")]);
        assert_eq!(
            hlen(&mut ctx, &[bulk("myhash")]).unwrap(),
            RespValue::Integer(1)
        );
        hset(&mut ctx, &[bulk("myhash"), bulk("f2"), bulk("v2")]).unwrap();
        assert_eq!(
            hlen(&mut ctx, &[bulk("myhash")]).unwrap(),
            RespValue::Integer(2)
        );
    }

    #[test]
    fn test_hlen_after_hdel_decreases() {
        let mut ctx = make_ctx();
        setup_hash(
            &mut ctx,
            "myhash",
            &[("f1", "v1"), ("f2", "v2"), ("f3", "v3")],
        );
        assert_eq!(
            hlen(&mut ctx, &[bulk("myhash")]).unwrap(),
            RespValue::Integer(3)
        );
        hdel(&mut ctx, &[bulk("myhash"), bulk("f2")]).unwrap();
        assert_eq!(
            hlen(&mut ctx, &[bulk("myhash")]).unwrap(),
            RespValue::Integer(2)
        );
    }

    #[test]
    fn test_hlen_after_all_deleted() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "v1")]);
        hdel(&mut ctx, &[bulk("myhash"), bulk("f1")]).unwrap();
        assert_eq!(
            hlen(&mut ctx, &[bulk("myhash")]).unwrap(),
            RespValue::Integer(0)
        );
    }

    #[test]
    fn test_hlen_overwrite_does_not_change_count() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "v1")]);
        assert_eq!(
            hlen(&mut ctx, &[bulk("myhash")]).unwrap(),
            RespValue::Integer(1)
        );
        hset(&mut ctx, &[bulk("myhash"), bulk("f1"), bulk("new_val")]).unwrap();
        assert_eq!(
            hlen(&mut ctx, &[bulk("myhash")]).unwrap(),
            RespValue::Integer(1)
        );
    }

    #[test]
    fn test_hlen_after_hmset() {
        let mut ctx = make_ctx();
        hmset(
            &mut ctx,
            &[
                bulk("myhash"),
                bulk("a"),
                bulk("1"),
                bulk("b"),
                bulk("2"),
                bulk("c"),
                bulk("3"),
            ],
        )
        .unwrap();
        assert_eq!(
            hlen(&mut ctx, &[bulk("myhash")]).unwrap(),
            RespValue::Integer(3)
        );
    }

    #[test]
    fn test_hlen_after_hsetnx() {
        let mut ctx = make_ctx();
        hsetnx(&mut ctx, &[bulk("myhash"), bulk("f1"), bulk("v1")]).unwrap();
        assert_eq!(
            hlen(&mut ctx, &[bulk("myhash")]).unwrap(),
            RespValue::Integer(1)
        );
        hsetnx(&mut ctx, &[bulk("myhash"), bulk("f1"), bulk("v2")]).unwrap();
        assert_eq!(
            hlen(&mut ctx, &[bulk("myhash")]).unwrap(),
            RespValue::Integer(1)
        );
        hsetnx(&mut ctx, &[bulk("myhash"), bulk("f2"), bulk("v2")]).unwrap();
        assert_eq!(
            hlen(&mut ctx, &[bulk("myhash")]).unwrap(),
            RespValue::Integer(2)
        );
    }

    #[test]
    fn test_hlen_consistency_with_hkeys() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("a", "1"), ("b", "2"), ("c", "3")]);
        let len = hlen(&mut ctx, &[bulk("myhash")]).unwrap();
        let keys = hkeys(&mut ctx, &[bulk("myhash")]).unwrap();
        if let (RespValue::Integer(l), RespValue::Array(k)) = (len, keys) {
            assert_eq!(l, k.len() as i64);
        } else {
            panic!("Unexpected types");
        }
    }

    #[test]
    fn test_hlen_large_hash() {
        let mut ctx = make_ctx();
        let mut args = vec![bulk("myhash")];
        for i in 0..100 {
            args.push(bulk(&format!("f{i}")));
            args.push(bulk(&format!("v{i}")));
        }
        hset(&mut ctx, &args).unwrap();
        assert_eq!(
            hlen(&mut ctx, &[bulk("myhash")]).unwrap(),
            RespValue::Integer(100)
        );
    }

    #[test]
    fn test_hlen_after_hincrby_creates_field() {
        let mut ctx = make_ctx();
        hincrby(&mut ctx, &[bulk("myhash"), bulk("counter"), bulk("1")]).unwrap();
        assert_eq!(
            hlen(&mut ctx, &[bulk("myhash")]).unwrap(),
            RespValue::Integer(1)
        );
    }

    #[test]
    fn test_hlen_after_hincrbyfloat_creates_field() {
        let mut ctx = make_ctx();
        hincrbyfloat(&mut ctx, &[bulk("myhash"), bulk("counter"), bulk("1.5")]).unwrap();
        assert_eq!(
            hlen(&mut ctx, &[bulk("myhash")]).unwrap(),
            RespValue::Integer(1)
        );
    }

    #[test]
    fn test_hlen_empty_field_counts() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("", "val")]);
        assert_eq!(
            hlen(&mut ctx, &[bulk("myhash")]).unwrap(),
            RespValue::Integer(1)
        );
    }

    // ========================================================================
    // HSTRLEN — additional tests (19 more to reach 20)
    // ========================================================================

    #[test]
    fn test_hstrlen_nonexistent_key_returns_zero() {
        let mut ctx = make_ctx();
        let result = hstrlen(&mut ctx, &[bulk("nokey"), bulk("f1")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_hstrlen_nonexistent_field_returns_zero() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "v1")]);
        let result = hstrlen(&mut ctx, &[bulk("myhash"), bulk("nosuch")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_hstrlen_wrong_arity_no_args() {
        let mut ctx = make_ctx();
        assert!(matches!(
            hstrlen(&mut ctx, &[]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_hstrlen_wrong_arity_one_arg() {
        let mut ctx = make_ctx();
        assert!(matches!(
            hstrlen(&mut ctx, &[bulk("key")]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_hstrlen_wrong_type_string_key() {
        let mut ctx = make_ctx();
        setup_string(&mut ctx, "mystr", "hello");
        let result = hstrlen(&mut ctx, &[bulk("mystr"), bulk("f1")]);
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    #[test]
    fn test_hstrlen_expired_key() {
        use std::time::Instant;
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut hash = HashMap::new();
        hash.insert(Bytes::from("f1"), Bytes::from("value"));
        let mut entry = Entry::new(RedisValue::Hash(hash));
        entry.expires_at = Some(Instant::now() - std::time::Duration::from_secs(1));
        db.set(Bytes::from("myhash"), entry);

        let result = hstrlen(&mut ctx, &[bulk("myhash"), bulk("f1")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_hstrlen_empty_value() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "")]);
        let result = hstrlen(&mut ctx, &[bulk("myhash"), bulk("f1")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_hstrlen_single_char_value() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "x")]);
        let result = hstrlen(&mut ctx, &[bulk("myhash"), bulk("f1")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_hstrlen_returns_integer_type() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "hello")]);
        let result = hstrlen(&mut ctx, &[bulk("myhash"), bulk("f1")]).unwrap();
        assert!(matches!(result, RespValue::Integer(5)));
    }

    #[test]
    fn test_hstrlen_numeric_value() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "12345")]);
        let result = hstrlen(&mut ctx, &[bulk("myhash"), bulk("f1")]).unwrap();
        assert_eq!(result, RespValue::Integer(5));
    }

    #[test]
    fn test_hstrlen_long_value() {
        let mut ctx = make_ctx();
        let long_val = "a".repeat(1000);
        setup_hash(&mut ctx, "myhash", &[("f1", &long_val)]);
        let result = hstrlen(&mut ctx, &[bulk("myhash"), bulk("f1")]).unwrap();
        assert_eq!(result, RespValue::Integer(1000));
    }

    #[test]
    fn test_hstrlen_after_update() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "short")]);
        assert_eq!(
            hstrlen(&mut ctx, &[bulk("myhash"), bulk("f1")]).unwrap(),
            RespValue::Integer(5)
        );

        hset(
            &mut ctx,
            &[bulk("myhash"), bulk("f1"), bulk("much longer value here")],
        )
        .unwrap();
        assert_eq!(
            hstrlen(&mut ctx, &[bulk("myhash"), bulk("f1")]).unwrap(),
            RespValue::Integer(22)
        );
    }

    #[test]
    fn test_hstrlen_special_chars() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("f1", "hello\tworld\n")]);
        let result = hstrlen(&mut ctx, &[bulk("myhash"), bulk("f1")]).unwrap();
        assert_eq!(result, RespValue::Integer(12));
    }

    #[test]
    fn test_hstrlen_binary_value() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut hash = HashMap::new();
        hash.insert(Bytes::from("f1"), Bytes::from_static(b"\x00\x01\x02\x03"));
        db.set(Bytes::from("myhash"), Entry::new(RedisValue::Hash(hash)));

        let result = hstrlen(&mut ctx, &[bulk("myhash"), bulk("f1")]).unwrap();
        assert_eq!(result, RespValue::Integer(4));
    }

    #[test]
    fn test_hstrlen_multiple_fields_different_lengths() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("a", "x"), ("b", "xx"), ("c", "xxx")]);
        assert_eq!(
            hstrlen(&mut ctx, &[bulk("myhash"), bulk("a")]).unwrap(),
            RespValue::Integer(1)
        );
        assert_eq!(
            hstrlen(&mut ctx, &[bulk("myhash"), bulk("b")]).unwrap(),
            RespValue::Integer(2)
        );
        assert_eq!(
            hstrlen(&mut ctx, &[bulk("myhash"), bulk("c")]).unwrap(),
            RespValue::Integer(3)
        );
    }

    #[test]
    fn test_hstrlen_after_hincrby() {
        let mut ctx = make_ctx();
        hincrby(&mut ctx, &[bulk("myhash"), bulk("c"), bulk("42")]).unwrap();
        assert_eq!(
            hstrlen(&mut ctx, &[bulk("myhash"), bulk("c")]).unwrap(),
            RespValue::Integer(2)
        );
    }

    #[test]
    fn test_hstrlen_after_hincrbyfloat() {
        let mut ctx = make_ctx();
        hincrbyfloat(&mut ctx, &[bulk("myhash"), bulk("f"), bulk("3.14")]).unwrap();
        let result = hstrlen(&mut ctx, &[bulk("myhash"), bulk("f")]).unwrap();
        assert_eq!(result, RespValue::Integer(4));
    }

    #[test]
    fn test_hstrlen_empty_field_name() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("", "value")]);
        let result = hstrlen(&mut ctx, &[bulk("myhash"), bulk("")]).unwrap();
        assert_eq!(result, RespValue::Integer(5));
    }

    #[test]
    fn test_hstrlen_unicode_value() {
        let mut ctx = make_ctx();
        // "héllo" = 6 bytes in UTF-8 (é is 2 bytes)
        setup_hash(&mut ctx, "myhash", &[("f1", "héllo")]);
        let result = hstrlen(&mut ctx, &[bulk("myhash"), bulk("f1")]).unwrap();
        assert_eq!(result, RespValue::Integer(6));
    }

    // Extra tests to reach 20 per command

    #[test]
    fn test_hexists_unicode_field() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("café", "latte")]);
        assert_eq!(
            hexists(&mut ctx, &[bulk("myhash"), bulk("café")]).unwrap(),
            RespValue::Integer(1)
        );
        assert_eq!(
            hexists(&mut ctx, &[bulk("myhash"), bulk("cafe")]).unwrap(),
            RespValue::Integer(0)
        );
    }

    #[test]
    fn test_hgetall_case_sensitive_fields() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("ABC", "upper"), ("abc", "lower")]);
        let result = hgetall(&mut ctx, &[bulk("myhash")]).unwrap();
        if let RespValue::Array(arr) = result {
            assert_eq!(arr.len(), 4);
            // Lexicographic: "ABC" < "abc"
            assert_eq!(arr[0], bulk("ABC"));
            assert_eq!(arr[1], bulk("upper"));
            assert_eq!(arr[2], bulk("abc"));
            assert_eq!(arr[3], bulk("lower"));
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hincrby_on_nonexistent_key_and_field() {
        let mut ctx = make_ctx();
        // Both key and field don't exist yet
        let result = hincrby(&mut ctx, &[bulk("newkey"), bulk("newfield"), bulk("10")]).unwrap();
        assert_eq!(result, RespValue::Integer(10));
        // Verify the key and field were created
        assert_eq!(
            hget(&mut ctx, &[bulk("newkey"), bulk("newfield")]).unwrap(),
            bulk("10")
        );
        assert_eq!(
            hlen(&mut ctx, &[bulk("newkey")]).unwrap(),
            RespValue::Integer(1)
        );
    }

    #[test]
    fn test_hincrbyfloat_on_nonexistent_field_in_existing_hash() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("existing", "1.0")]);
        let result =
            hincrbyfloat(&mut ctx, &[bulk("myhash"), bulk("newfield"), bulk("2.5")]).unwrap();
        if let RespValue::BulkString(b) = result {
            let val: f64 = std::str::from_utf8(&b).unwrap().parse().unwrap();
            assert!((val - 2.5).abs() < 0.0001);
        } else {
            panic!("Expected bulk string");
        }
        // Existing field should be unchanged
        assert_eq!(
            hget(&mut ctx, &[bulk("myhash"), bulk("existing")]).unwrap(),
            bulk("1.0")
        );
    }

    #[test]
    fn test_hvals_after_hmset_update() {
        let mut ctx = make_ctx();
        setup_hash(&mut ctx, "myhash", &[("a", "old_a"), ("b", "old_b")]);
        hmset(
            &mut ctx,
            &[
                bulk("myhash"),
                bulk("a"),
                bulk("new_a"),
                bulk("c"),
                bulk("new_c"),
            ],
        )
        .unwrap();
        let result = hvals(&mut ctx, &[bulk("myhash")]).unwrap();
        if let RespValue::Array(arr) = result {
            // Sorted by keys: a, b, c
            assert_eq!(arr, vec![bulk("new_a"), bulk("old_b"), bulk("new_c")]);
        } else {
            panic!("Expected array");
        }
    }
}

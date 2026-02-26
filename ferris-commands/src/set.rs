//! Set commands: SADD, SREM, SMEMBERS, SISMEMBER, SMISMEMBER, SCARD,
//! SINTER, SINTERSTORE, SINTERCARD, SUNION, SUNIONSTORE, SDIFF, SDIFFSTORE,
//! SRANDMEMBER, SPOP, SMOVE, SSCAN

use crate::{CommandContext, CommandError, CommandResult};
use bytes::Bytes;
use ferris_core::{Entry, RedisValue};
use ferris_protocol::RespValue;
use std::collections::HashSet;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Parse an integer from a RESP value.
fn parse_int(arg: &RespValue) -> Result<i64, CommandError> {
    let s = arg.as_str().ok_or(CommandError::NotAnInteger)?;
    s.parse().map_err(|_| CommandError::NotAnInteger)
}

/// Extract `Bytes` from a RESP value (bulk string or simple string).
fn get_bytes(arg: &RespValue) -> Result<Bytes, CommandError> {
    arg.as_bytes()
        .cloned()
        .or_else(|| arg.as_str().map(|s| Bytes::from(s.to_owned())))
        .ok_or_else(|| CommandError::InvalidArgument("invalid argument".to_string()))
}

/// Retrieve the set stored at `key`, or an empty `HashSet` if the key does
/// not exist (or is expired). Returns `Err(WrongType)` when the value at
/// `key` is not a set.
fn get_set_or_empty(
    db: &ferris_core::Database,
    key: &[u8],
) -> Result<HashSet<Bytes>, CommandError> {
    match db.get(key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(key);
                return Ok(HashSet::new());
            }
            match entry.value {
                RedisValue::Set(s) => Ok(s),
                _ => Err(CommandError::WrongType),
            }
        }
        None => Ok(HashSet::new()),
    }
}

/// Store a set into the database. If the set is empty the key is deleted
/// (Redis semantics: empty aggregates are removed).
fn store_set(db: &ferris_core::Database, key: Bytes, set: HashSet<Bytes>) {
    if set.is_empty() {
        db.delete(&key);
    } else {
        db.set(key, Entry::new(RedisValue::Set(set)));
    }
}

/// Simple glob-style pattern match. Delegates to the `glob_match` crate
/// (already a dependency of ferris-commands).
fn pattern_matches(pattern: &str, value: &str) -> bool {
    glob_match::glob_match(pattern, value)
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

/// SADD key member [member ...]
///
/// Add the specified members to the set stored at key. Specified members that
/// are already a member of this set are ignored. If key does not exist, a new
/// set is created before adding the specified members.
///
/// Returns the number of elements that were added to the set (not including
/// elements already present).
///
/// Time complexity: O(1) for each element added
pub fn sadd(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("SADD".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let db = ctx.store().database(ctx.selected_db());
    let mut set = get_set_or_empty(db, &key)?;

    let mut added: i64 = 0;
    for arg in &args[1..] {
        let member = get_bytes(arg)?;
        if set.insert(member) {
            added += 1;
        }
    }

    store_set(db, key.clone(), set);

    if added > 0 {
        // Propagate to AOF and replication
        ctx.propagate_args(args);
    }

    Ok(RespValue::Integer(added))
}

/// SREM key member [member ...]
///
/// Remove the specified members from the set stored at key. Members that are
/// not in the set are ignored. If key does not exist, it is treated as an
/// empty set and this command returns 0.
///
/// Returns the number of members that were removed from the set.
///
/// Time complexity: O(N) where N is the number of members to be removed
pub fn srem(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("SREM".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let db = ctx.store().database(ctx.selected_db());
    let mut set = get_set_or_empty(db, &key)?;

    let mut removed: i64 = 0;
    for arg in &args[1..] {
        let member = get_bytes(arg)?;
        if set.remove(&member) {
            removed += 1;
        }
    }

    store_set(db, key.clone(), set);

    if removed > 0 {
        // Propagate to AOF and replication
        ctx.propagate_args(args);
    }

    Ok(RespValue::Integer(removed))
}

/// SMEMBERS key
///
/// Returns all the members of the set value stored at key.
///
/// Time complexity: O(N) where N is the set cardinality
pub fn smembers(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("SMEMBERS".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let db = ctx.store().database(ctx.selected_db());
    let set = get_set_or_empty(db, &key)?;

    // Pre-allocate Vec with exact capacity to avoid reallocation during iteration
    // This reduces allocation overhead for large sets
    let mut members = Vec::with_capacity(set.len());
    for member in set {
        members.push(RespValue::BulkString(member));
    }

    Ok(RespValue::Array(members))
}

/// SISMEMBER key member
///
/// Returns if member is a member of the set stored at key.
///
/// Time complexity: O(1)
pub fn sismember(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("SISMEMBER".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let member = get_bytes(&args[1])?;
    let db = ctx.store().database(ctx.selected_db());
    let set = get_set_or_empty(db, &key)?;

    Ok(RespValue::Integer(i64::from(set.contains(&member))))
}

/// SMISMEMBER key member [member ...]
///
/// Returns whether each member is a member of the set stored at key.
/// For each member the reply is 1 if present, 0 if not.
///
/// Time complexity: O(N) where N is the number of elements being checked
pub fn smismember(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("SMISMEMBER".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let db = ctx.store().database(ctx.selected_db());
    let set = get_set_or_empty(db, &key)?;

    let results: Vec<RespValue> = args[1..]
        .iter()
        .map(|arg| {
            let member = get_bytes(arg).unwrap_or_default();
            RespValue::Integer(i64::from(set.contains(&member)))
        })
        .collect();

    Ok(RespValue::Array(results))
}

/// SCARD key
///
/// Returns the set cardinality (number of elements) of the set stored at key.
///
/// Time complexity: O(1)
pub fn scard(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("SCARD".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let db = ctx.store().database(ctx.selected_db());
    let set = get_set_or_empty(db, &key)?;

    Ok(RespValue::Integer(set.len() as i64))
}

/// SINTER key [key ...]
///
/// Returns the members of the set resulting from the intersection of all the
/// given sets.
///
/// Time complexity: O(N*M) worst case where N is the cardinality of the
/// smallest set and M is the number of sets
pub fn sinter(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("SINTER".to_string()));
    }

    let db = ctx.store().database(ctx.selected_db());
    let result = compute_intersection(db, args)?;

    let members: Vec<RespValue> = result.into_iter().map(RespValue::BulkString).collect();

    Ok(RespValue::Array(members))
}

/// SINTERSTORE destination key [key ...]
///
/// This command is equal to SINTER, but instead of returning the resulting
/// set, it is stored in destination.
///
/// Returns the cardinality of the resulting set.
///
/// Time complexity: O(N*M) worst case
pub fn sinterstore(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("SINTERSTORE".to_string()));
    }

    let dest = get_bytes(&args[0])?;
    let db = ctx.store().database(ctx.selected_db());
    let result = compute_intersection(db, &args[1..])?;
    let count = result.len() as i64;

    store_set(db, dest.clone(), result);

    // Propagate to AOF and replication
    ctx.propagate_args(args);

    Ok(RespValue::Integer(count))
}

/// SINTERCARD numkeys key [key ...] [LIMIT limit]
///
/// Returns the cardinality of the intersection of the sets given by the
/// specified keys. Optionally, a LIMIT can be specified to stop counting
/// after reaching it.
///
/// Time complexity: O(N*M) worst case
pub fn sintercard(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("SINTERCARD".to_string()));
    }

    let numkeys = parse_int(&args[0])?;
    if numkeys <= 0 {
        return Err(CommandError::InvalidArgument(
            "numkeys can't be non-positive value".to_string(),
        ));
    }
    let numkeys = numkeys as usize;

    // We need at least numkeys key arguments after the numkeys arg
    if args.len() < 1 + numkeys {
        return Err(CommandError::WrongArity("SINTERCARD".to_string()));
    }

    let key_args = &args[1..1 + numkeys];

    // Parse optional LIMIT
    let mut limit: usize = 0; // 0 means no limit
    let mut i = 1 + numkeys;
    while i < args.len() {
        let opt = args[i].as_str().map(|s| s.to_uppercase());
        match opt.as_deref() {
            Some("LIMIT") => {
                i += 1;
                let l = parse_int(args.get(i).ok_or(CommandError::SyntaxError)?)?;
                if l < 0 {
                    return Err(CommandError::InvalidArgument(
                        "LIMIT can't be negative".to_string(),
                    ));
                }
                limit = l as usize;
            }
            _ => return Err(CommandError::SyntaxError),
        }
        i += 1;
    }

    let db = ctx.store().database(ctx.selected_db());
    let result = compute_intersection(db, key_args)?;

    let count = if limit > 0 {
        result.len().min(limit)
    } else {
        result.len()
    };

    Ok(RespValue::Integer(count as i64))
}

/// SUNION key [key ...]
///
/// Returns the members of the set resulting from the union of all the given
/// sets.
///
/// Time complexity: O(N) where N is the total number of elements in all
/// given sets
pub fn sunion(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("SUNION".to_string()));
    }

    let db = ctx.store().database(ctx.selected_db());
    let result = compute_union(db, args)?;

    let members: Vec<RespValue> = result.into_iter().map(RespValue::BulkString).collect();

    Ok(RespValue::Array(members))
}

/// SUNIONSTORE destination key [key ...]
///
/// This command is equal to SUNION, but instead of returning the resulting
/// set, it is stored in destination.
///
/// Returns the cardinality of the resulting set.
///
/// Time complexity: O(N)
pub fn sunionstore(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("SUNIONSTORE".to_string()));
    }

    let dest = get_bytes(&args[0])?;
    let db = ctx.store().database(ctx.selected_db());
    let result = compute_union(db, &args[1..])?;
    let count = result.len() as i64;

    store_set(db, dest.clone(), result);

    // Propagate to AOF and replication
    ctx.propagate_args(args);

    Ok(RespValue::Integer(count))
}

/// SDIFF key [key ...]
///
/// Returns the members of the set resulting from the difference between the
/// first set and all the successive sets.
///
/// Time complexity: O(N) where N is the total number of elements in all
/// given sets
pub fn sdiff(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("SDIFF".to_string()));
    }

    let db = ctx.store().database(ctx.selected_db());
    let result = compute_difference(db, args)?;

    let members: Vec<RespValue> = result.into_iter().map(RespValue::BulkString).collect();

    Ok(RespValue::Array(members))
}

/// SDIFFSTORE destination key [key ...]
///
/// This command is equal to SDIFF, but instead of returning the resulting
/// set, it is stored in destination.
///
/// Returns the cardinality of the resulting set.
///
/// Time complexity: O(N)
pub fn sdiffstore(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("SDIFFSTORE".to_string()));
    }

    let dest = get_bytes(&args[0])?;
    let db = ctx.store().database(ctx.selected_db());
    let result = compute_difference(db, &args[1..])?;
    let count = result.len() as i64;

    store_set(db, dest.clone(), result);

    // Propagate to AOF and replication
    ctx.propagate_args(args);
    Ok(RespValue::Integer(count))
}

/// SRANDMEMBER key [count]
///
/// When called with just the key argument, return a random element from the
/// set value stored at key. When called with the additional count argument,
/// return an array of count distinct elements if count is positive, or an
/// array of count elements (possibly repeating) if count is negative.
///
/// Time complexity: O(N) where N is the absolute value of the passed count
pub fn srandmember(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("SRANDMEMBER".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let db = ctx.store().database(ctx.selected_db());
    let set = get_set_or_empty(db, &key)?;

    if set.is_empty() {
        // With count argument: return empty array; without: return Null
        return if args.len() >= 2 {
            Ok(RespValue::Array(vec![]))
        } else {
            Ok(RespValue::Null)
        };
    }

    let members: Vec<Bytes> = set.into_iter().collect();

    if args.len() < 2 {
        // No count: return a single random member
        let idx = random_index(members.len());
        return Ok(RespValue::BulkString(members[idx].clone()));
    }

    let count = parse_int(&args[1])?;

    if count == 0 {
        return Ok(RespValue::Array(vec![]));
    }

    if count > 0 {
        // Positive count: unique elements, at most count (or set size)
        let take = (count as usize).min(members.len());
        let selected = random_sample_unique(&members, take);
        let result: Vec<RespValue> = selected
            .into_iter()
            .map(|b| RespValue::BulkString(b.clone()))
            .collect();
        Ok(RespValue::Array(result))
    } else {
        // Negative count: may repeat, exactly |count| elements
        let take = count.unsigned_abs() as usize;
        let mut result = Vec::with_capacity(take);
        for _ in 0..take {
            let idx = random_index(members.len());
            result.push(RespValue::BulkString(members[idx].clone()));
        }
        Ok(RespValue::Array(result))
    }
}

/// SPOP key [count]
///
/// Removes and returns one or more random members from the set value stored
/// at key.
///
/// Time complexity: O(1) without count, O(N) with count
pub fn spop(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("SPOP".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let db = ctx.store().database(ctx.selected_db());
    let mut set = get_set_or_empty(db, &key)?;

    if set.is_empty() {
        return if args.len() >= 2 {
            Ok(RespValue::Array(vec![]))
        } else {
            Ok(RespValue::Null)
        };
    }

    if args.len() < 2 {
        // No count: pop a single member
        let members: Vec<Bytes> = set.iter().cloned().collect();
        let idx = random_index(members.len());
        let popped = members[idx].clone();
        set.remove(&popped);
        store_set(db, key.clone(), set);

        // Propagate to AOF and replication
        ctx.propagate_args(args);

        return Ok(RespValue::BulkString(popped));
    }

    let count = parse_int(&args[1])?;
    if count < 0 {
        return Err(CommandError::InvalidArgument(
            "value is out of range, must be positive".to_string(),
        ));
    }
    let count = (count as usize).min(set.len());

    let members: Vec<Bytes> = set.iter().cloned().collect();
    let selected = random_sample_unique(&members, count);
    let mut result = Vec::with_capacity(count);
    for member in &selected {
        set.remove(*member);
        result.push(RespValue::BulkString((*member).clone()));
    }

    store_set(db, key.clone(), set);

    // Propagate to AOF and replication
    ctx.propagate_args(args);

    Ok(RespValue::Array(result))
}

/// SMOVE source destination member
///
/// Move member from the set at source to the set at destination. This
/// operation is atomic. Returns 1 if the element is moved, 0 if the element
/// is not a member of source and no operation was performed.
///
/// Time complexity: O(1)
pub fn smove(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongArity("SMOVE".to_string()));
    }

    let src_key = get_bytes(&args[0])?;
    let dst_key = get_bytes(&args[1])?;
    let member = get_bytes(&args[2])?;

    let db = ctx.store().database(ctx.selected_db());

    let mut src_set = get_set_or_empty(db, &src_key)?;
    if !src_set.remove(&member) {
        return Ok(RespValue::Integer(0));
    }
    store_set(db, src_key.clone(), src_set);

    let mut dst_set = get_set_or_empty(db, &dst_key)?;
    dst_set.insert(member);
    store_set(db, dst_key.clone(), dst_set);

    // Propagate to AOF and replication
    ctx.propagate_args(args);

    Ok(RespValue::Integer(1))
}

/// SSCAN key cursor [MATCH pattern] [COUNT count]
///
/// Incrementally iterate Set elements.
///
/// Time complexity: O(1) for every call, O(N) for a complete iteration
pub fn sscan(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("SSCAN".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let cursor = parse_int(&args[1])? as usize;

    let mut pattern = "*";
    let mut count: usize = 10;

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

    let db = ctx.store().database(ctx.selected_db());
    let set = get_set_or_empty(db, &key)?;

    // Collect all members that match the pattern
    let mut all_members: Vec<Bytes> = set
        .into_iter()
        .filter(|m| {
            let s = String::from_utf8_lossy(m);
            pattern_matches(pattern, &s)
        })
        .collect();

    // Sort for deterministic cursor-based iteration
    all_members.sort();

    let total = all_members.len();
    let start = cursor.min(total);
    let end = (start + count).min(total);
    let next_cursor = if end >= total { 0 } else { end };

    let page: Vec<RespValue> = all_members[start..end]
        .iter()
        .map(|m| RespValue::BulkString(m.clone()))
        .collect();

    Ok(RespValue::Array(vec![
        RespValue::BulkString(Bytes::from(next_cursor.to_string())),
        RespValue::Array(page),
    ]))
}

// ---------------------------------------------------------------------------
// Internal set-algebra helpers
// ---------------------------------------------------------------------------

/// Compute the intersection of the sets given by `key_args`.
fn compute_intersection(
    db: &ferris_core::Database,
    key_args: &[RespValue],
) -> Result<HashSet<Bytes>, CommandError> {
    let first_key = get_bytes(&key_args[0])?;
    let mut result = get_set_or_empty(db, &first_key)?;

    for arg in &key_args[1..] {
        let k = get_bytes(arg)?;
        let other = get_set_or_empty(db, &k)?;
        result.retain(|m| other.contains(m));
    }

    Ok(result)
}

/// Compute the union of the sets given by `key_args`.
fn compute_union(
    db: &ferris_core::Database,
    key_args: &[RespValue],
) -> Result<HashSet<Bytes>, CommandError> {
    let mut result: HashSet<Bytes> = HashSet::new();
    for arg in key_args {
        let k = get_bytes(arg)?;
        let s = get_set_or_empty(db, &k)?;
        result.extend(s);
    }
    Ok(result)
}

/// Compute the difference of the sets given by `key_args` (first - rest).
fn compute_difference(
    db: &ferris_core::Database,
    key_args: &[RespValue],
) -> Result<HashSet<Bytes>, CommandError> {
    let first_key = get_bytes(&key_args[0])?;
    let mut result = get_set_or_empty(db, &first_key)?;

    for arg in &key_args[1..] {
        let k = get_bytes(arg)?;
        let other = get_set_or_empty(db, &k)?;
        result.retain(|m| !other.contains(m));
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// Random selection helpers
// ---------------------------------------------------------------------------

/// Return a pseudo-random index in `[0, len)` using `rand::random`.
fn random_index(len: usize) -> usize {
    if len <= 1 {
        return 0;
    }
    rand::random::<usize>() % len
}

/// Return `take` unique elements sampled from `members` (Fisher-Yates /
/// Knuth shuffle on an index vector).
fn random_sample_unique<'a>(members: &'a [Bytes], take: usize) -> Vec<&'a Bytes> {
    let n = members.len();
    let take = take.min(n);
    let mut indices: Vec<usize> = (0..n).collect();

    // Partial Fisher-Yates shuffle for the first `take` positions
    for i in 0..take {
        let j = i + (rand::random::<usize>() % (n - i));
        indices.swap(i, j);
    }

    indices[..take].iter().map(|&idx| &members[idx]).collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

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

    /// Convenience: add members to a set directly through the store.
    fn insert_set(ctx: &mut CommandContext, key: &str, members: &[&str]) {
        let db = ctx.store().database(ctx.selected_db());
        let mut set: HashSet<Bytes> = HashSet::new();
        for m in members {
            set.insert(Bytes::from(String::from(*m)));
        }
        db.set(
            Bytes::from(key.to_owned()),
            Entry::new(RedisValue::Set(set)),
        );
    }

    /// Convenience: insert a string key (to test WRONGTYPE errors).
    fn insert_string(ctx: &mut CommandContext, key: &str, val: &str) {
        let db = ctx.store().database(ctx.selected_db());
        db.set(
            Bytes::from(key.to_owned()),
            Entry::new(RedisValue::String(Bytes::from(val.to_owned()))),
        );
    }

    /// Extract a `Vec<Bytes>` from a `RespValue::Array` of `BulkString`s and
    /// return it sorted for deterministic comparison.
    fn sorted_bytes_from_array(resp: &RespValue) -> Vec<Bytes> {
        match resp {
            RespValue::Array(arr) => {
                let mut v: Vec<Bytes> = arr
                    .iter()
                    .filter_map(|r| match r {
                        RespValue::BulkString(b) => Some(b.clone()),
                        _ => None,
                    })
                    .collect();
                v.sort();
                v
            }
            _ => panic!("expected Array, got {resp:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // 1. test_sadd_basic
    // -----------------------------------------------------------------------
    #[test]
    fn test_sadd_basic() {
        let mut ctx = make_ctx();
        let result = sadd(&mut ctx, &[bulk("myset"), bulk("a"), bulk("b"), bulk("c")]).unwrap();
        assert_eq!(result, RespValue::Integer(3));

        let members = smembers(&mut ctx, &[bulk("myset")]).unwrap();
        let sorted = sorted_bytes_from_array(&members);
        assert_eq!(sorted.len(), 3);
        assert!(sorted.contains(&Bytes::from("a")));
        assert!(sorted.contains(&Bytes::from("b")));
        assert!(sorted.contains(&Bytes::from("c")));
    }

    // -----------------------------------------------------------------------
    // 2. test_sadd_duplicates
    // -----------------------------------------------------------------------
    #[test]
    fn test_sadd_duplicates() {
        let mut ctx = make_ctx();
        sadd(&mut ctx, &[bulk("myset"), bulk("a"), bulk("b")]).unwrap();

        let result = sadd(&mut ctx, &[bulk("myset"), bulk("b"), bulk("c")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let card = scard(&mut ctx, &[bulk("myset")]).unwrap();
        assert_eq!(card, RespValue::Integer(3));
    }

    // -----------------------------------------------------------------------
    // 3. test_srem_basic
    // -----------------------------------------------------------------------
    #[test]
    fn test_srem_basic() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "myset", &["a", "b", "c"]);

        let result = srem(&mut ctx, &[bulk("myset"), bulk("a"), bulk("c")]).unwrap();
        assert_eq!(result, RespValue::Integer(2));

        let card = scard(&mut ctx, &[bulk("myset")]).unwrap();
        assert_eq!(card, RespValue::Integer(1));
    }

    // -----------------------------------------------------------------------
    // 4. test_srem_nonexistent
    // -----------------------------------------------------------------------
    #[test]
    fn test_srem_nonexistent() {
        let mut ctx = make_ctx();

        let result = srem(&mut ctx, &[bulk("nokey"), bulk("a")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));

        insert_set(&mut ctx, "myset", &["x"]);
        let result = srem(&mut ctx, &[bulk("myset"), bulk("nothere")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    // -----------------------------------------------------------------------
    // 5. test_smembers
    // -----------------------------------------------------------------------
    #[test]
    fn test_smembers() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "myset", &["x", "y", "z"]);

        let result = smembers(&mut ctx, &[bulk("myset")]).unwrap();
        let sorted = sorted_bytes_from_array(&result);
        assert_eq!(
            sorted,
            vec![Bytes::from("x"), Bytes::from("y"), Bytes::from("z")]
        );
    }

    // -----------------------------------------------------------------------
    // 6. test_smembers_empty
    // -----------------------------------------------------------------------
    #[test]
    fn test_smembers_empty() {
        let mut ctx = make_ctx();
        let result = smembers(&mut ctx, &[bulk("nokey")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![]));
    }

    // -----------------------------------------------------------------------
    // 7. test_sismember
    // -----------------------------------------------------------------------
    #[test]
    fn test_sismember() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "myset", &["hello", "world"]);

        let result = sismember(&mut ctx, &[bulk("myset"), bulk("hello")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = sismember(&mut ctx, &[bulk("myset"), bulk("nope")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));

        let result = sismember(&mut ctx, &[bulk("nokey"), bulk("x")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    // -----------------------------------------------------------------------
    // 8. test_smismember
    // -----------------------------------------------------------------------
    #[test]
    fn test_smismember() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "myset", &["a", "b", "c"]);

        let result =
            smismember(&mut ctx, &[bulk("myset"), bulk("a"), bulk("d"), bulk("c")]).unwrap();
        let expected = RespValue::Array(vec![
            RespValue::Integer(1),
            RespValue::Integer(0),
            RespValue::Integer(1),
        ]);
        assert_eq!(result, expected);
    }

    // -----------------------------------------------------------------------
    // 9. test_scard
    // -----------------------------------------------------------------------
    #[test]
    fn test_scard() {
        let mut ctx = make_ctx();

        let result = scard(&mut ctx, &[bulk("nokey")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));

        insert_set(&mut ctx, "myset", &["a", "b", "c"]);
        let result = scard(&mut ctx, &[bulk("myset")]).unwrap();
        assert_eq!(result, RespValue::Integer(3));
    }

    // -----------------------------------------------------------------------
    // 10. test_sinter_basic
    // -----------------------------------------------------------------------
    #[test]
    fn test_sinter_basic() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s1", &["a", "b", "c", "d"]);
        insert_set(&mut ctx, "s2", &["c", "d", "e"]);

        let result = sinter(&mut ctx, &[bulk("s1"), bulk("s2")]).unwrap();
        let sorted = sorted_bytes_from_array(&result);
        assert_eq!(sorted, vec![Bytes::from("c"), Bytes::from("d")]);
    }

    // -----------------------------------------------------------------------
    // 11. test_sinter_empty
    // -----------------------------------------------------------------------
    #[test]
    fn test_sinter_empty() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s1", &["a", "b"]);

        let result = sinter(&mut ctx, &[bulk("s1"), bulk("nokey")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![]));
    }

    // -----------------------------------------------------------------------
    // 12. test_sinterstore
    // -----------------------------------------------------------------------
    #[test]
    fn test_sinterstore() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s1", &["a", "b", "c"]);
        insert_set(&mut ctx, "s2", &["b", "c", "d"]);

        let result = sinterstore(&mut ctx, &[bulk("dest"), bulk("s1"), bulk("s2")]).unwrap();
        assert_eq!(result, RespValue::Integer(2));

        let members = smembers(&mut ctx, &[bulk("dest")]).unwrap();
        let sorted = sorted_bytes_from_array(&members);
        assert_eq!(sorted, vec![Bytes::from("b"), Bytes::from("c")]);
    }

    // -----------------------------------------------------------------------
    // 13. test_sintercard
    // -----------------------------------------------------------------------
    #[test]
    fn test_sintercard() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s1", &["a", "b", "c", "d"]);
        insert_set(&mut ctx, "s2", &["b", "c", "d", "e"]);

        let result = sintercard(&mut ctx, &[bulk("2"), bulk("s1"), bulk("s2")]).unwrap();
        assert_eq!(result, RespValue::Integer(3));

        let result = sintercard(
            &mut ctx,
            &[bulk("2"), bulk("s1"), bulk("s2"), bulk("LIMIT"), bulk("2")],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(2));
    }

    // -----------------------------------------------------------------------
    // 14. test_sunion_basic
    // -----------------------------------------------------------------------
    #[test]
    fn test_sunion_basic() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s1", &["a", "b"]);
        insert_set(&mut ctx, "s2", &["b", "c"]);

        let result = sunion(&mut ctx, &[bulk("s1"), bulk("s2")]).unwrap();
        let sorted = sorted_bytes_from_array(&result);
        assert_eq!(
            sorted,
            vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]
        );
    }

    // -----------------------------------------------------------------------
    // 15. test_sunionstore
    // -----------------------------------------------------------------------
    #[test]
    fn test_sunionstore() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s1", &["a"]);
        insert_set(&mut ctx, "s2", &["b"]);

        let result = sunionstore(&mut ctx, &[bulk("dest"), bulk("s1"), bulk("s2")]).unwrap();
        assert_eq!(result, RespValue::Integer(2));

        let members = smembers(&mut ctx, &[bulk("dest")]).unwrap();
        let sorted = sorted_bytes_from_array(&members);
        assert_eq!(sorted, vec![Bytes::from("a"), Bytes::from("b")]);
    }

    // -----------------------------------------------------------------------
    // 16. test_sdiff_basic
    // -----------------------------------------------------------------------
    #[test]
    fn test_sdiff_basic() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s1", &["a", "b", "c"]);
        insert_set(&mut ctx, "s2", &["b", "c", "d"]);

        let result = sdiff(&mut ctx, &[bulk("s1"), bulk("s2")]).unwrap();
        let sorted = sorted_bytes_from_array(&result);
        assert_eq!(sorted, vec![Bytes::from("a")]);
    }

    // -----------------------------------------------------------------------
    // 17. test_sdiffstore
    // -----------------------------------------------------------------------
    #[test]
    fn test_sdiffstore() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s1", &["a", "b", "c"]);
        insert_set(&mut ctx, "s2", &["b"]);

        let result = sdiffstore(&mut ctx, &[bulk("dest"), bulk("s1"), bulk("s2")]).unwrap();
        assert_eq!(result, RespValue::Integer(2));

        let members = smembers(&mut ctx, &[bulk("dest")]).unwrap();
        let sorted = sorted_bytes_from_array(&members);
        assert_eq!(sorted, vec![Bytes::from("a"), Bytes::from("c")]);
    }

    // -----------------------------------------------------------------------
    // 18. test_srandmember_single
    // -----------------------------------------------------------------------
    #[test]
    fn test_srandmember_single() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "myset", &["one", "two", "three"]);

        let result = srandmember(&mut ctx, &[bulk("myset")]).unwrap();
        match &result {
            RespValue::BulkString(b) => {
                let s = std::str::from_utf8(b).unwrap();
                assert!(
                    s == "one" || s == "two" || s == "three",
                    "unexpected member: {s}"
                );
            }
            _ => panic!("expected BulkString, got {result:?}"),
        }

        let result = srandmember(&mut ctx, &[bulk("nokey")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    // -----------------------------------------------------------------------
    // 19. test_srandmember_count
    // -----------------------------------------------------------------------
    #[test]
    fn test_srandmember_count() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "myset", &["a", "b", "c", "d", "e"]);

        // Positive count: unique members
        let result = srandmember(&mut ctx, &[bulk("myset"), bulk("3")]).unwrap();
        if let RespValue::Array(arr) = &result {
            assert_eq!(arr.len(), 3);
            let mut seen = HashSet::new();
            for item in arr {
                if let RespValue::BulkString(b) = item {
                    assert!(seen.insert(b.clone()), "duplicate found");
                }
            }
        } else {
            panic!("expected Array");
        }

        // Positive count > set size: return all
        let result = srandmember(&mut ctx, &[bulk("myset"), bulk("100")]).unwrap();
        if let RespValue::Array(arr) = &result {
            assert_eq!(arr.len(), 5);
        }

        // Negative count: may repeat, exactly |count|
        let result = srandmember(&mut ctx, &[bulk("myset"), bulk("-7")]).unwrap();
        if let RespValue::Array(arr) = &result {
            assert_eq!(arr.len(), 7);
        }

        // Count 0: empty array
        let result = srandmember(&mut ctx, &[bulk("myset"), bulk("0")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![]));
    }

    // -----------------------------------------------------------------------
    // 20. test_spop_basic
    // -----------------------------------------------------------------------
    #[test]
    fn test_spop_basic() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "myset", &["a", "b", "c"]);

        let result = spop(&mut ctx, &[bulk("myset")]).unwrap();
        match &result {
            RespValue::BulkString(b) => {
                let s = std::str::from_utf8(b).unwrap();
                assert!(s == "a" || s == "b" || s == "c");
            }
            _ => panic!("expected BulkString"),
        }

        let card = scard(&mut ctx, &[bulk("myset")]).unwrap();
        assert_eq!(card, RespValue::Integer(2));

        let result = spop(&mut ctx, &[bulk("nokey")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    // -----------------------------------------------------------------------
    // 21. test_spop_with_count
    // -----------------------------------------------------------------------
    #[test]
    fn test_spop_with_count() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "myset", &["a", "b", "c", "d", "e"]);

        let result = spop(&mut ctx, &[bulk("myset"), bulk("3")]).unwrap();
        if let RespValue::Array(arr) = &result {
            assert_eq!(arr.len(), 3);
        } else {
            panic!("expected Array");
        }

        let card = scard(&mut ctx, &[bulk("myset")]).unwrap();
        assert_eq!(card, RespValue::Integer(2));

        let result = spop(&mut ctx, &[bulk("myset"), bulk("100")]).unwrap();
        if let RespValue::Array(arr) = &result {
            assert_eq!(arr.len(), 2);
        }

        let card = scard(&mut ctx, &[bulk("myset")]).unwrap();
        assert_eq!(card, RespValue::Integer(0));
    }

    // -----------------------------------------------------------------------
    // 22. test_smove_basic
    // -----------------------------------------------------------------------
    #[test]
    fn test_smove_basic() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "src", &["a", "b", "c"]);
        insert_set(&mut ctx, "dst", &["x"]);

        let result = smove(&mut ctx, &[bulk("src"), bulk("dst"), bulk("b")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let src_has = sismember(&mut ctx, &[bulk("src"), bulk("b")]).unwrap();
        assert_eq!(src_has, RespValue::Integer(0));
        let dst_has = sismember(&mut ctx, &[bulk("dst"), bulk("b")]).unwrap();
        assert_eq!(dst_has, RespValue::Integer(1));
    }

    // -----------------------------------------------------------------------
    // 23. test_smove_nonexistent
    // -----------------------------------------------------------------------
    #[test]
    fn test_smove_nonexistent() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "src", &["a"]);

        let result = smove(&mut ctx, &[bulk("src"), bulk("dst"), bulk("z")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));

        let result = smove(&mut ctx, &[bulk("nokey"), bulk("dst"), bulk("a")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    // -----------------------------------------------------------------------
    // 24. test_sscan_basic
    // -----------------------------------------------------------------------
    #[test]
    fn test_sscan_basic() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "myset", &["alpha", "beta", "gamma", "delta"]);

        let result = sscan(&mut ctx, &[bulk("myset"), bulk("0")]).unwrap();
        if let RespValue::Array(outer) = &result {
            assert_eq!(outer.len(), 2);
            if let RespValue::BulkString(cursor) = &outer[0] {
                assert_eq!(cursor.as_ref(), b"0");
            }
            if let RespValue::Array(members) = &outer[1] {
                assert_eq!(members.len(), 4);
            }
        } else {
            panic!("expected Array");
        }

        // With MATCH
        let result = sscan(
            &mut ctx,
            &[bulk("myset"), bulk("0"), bulk("MATCH"), bulk("a*")],
        )
        .unwrap();
        if let RespValue::Array(outer) = &result {
            if let RespValue::Array(members) = &outer[1] {
                assert_eq!(members.len(), 1); // only "alpha"
            }
        }
    }

    // -----------------------------------------------------------------------
    // 25. test_wrong_type_error
    // -----------------------------------------------------------------------
    #[test]
    fn test_wrong_type_error() {
        let mut ctx = make_ctx();
        insert_string(&mut ctx, "strkey", "hello");

        let result = sadd(&mut ctx, &[bulk("strkey"), bulk("member")]);
        assert!(matches!(result, Err(CommandError::WrongType)));

        let result = scard(&mut ctx, &[bulk("strkey")]);
        assert!(matches!(result, Err(CommandError::WrongType)));

        let result = sismember(&mut ctx, &[bulk("strkey"), bulk("x")]);
        assert!(matches!(result, Err(CommandError::WrongType)));

        let result = smembers(&mut ctx, &[bulk("strkey")]);
        assert!(matches!(result, Err(CommandError::WrongType)));

        let result = sinter(&mut ctx, &[bulk("strkey")]);
        assert!(matches!(result, Err(CommandError::WrongType)));

        let result = spop(&mut ctx, &[bulk("strkey")]);
        assert!(matches!(result, Err(CommandError::WrongType)));

        let result = srem(&mut ctx, &[bulk("strkey"), bulk("x")]);
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    // -----------------------------------------------------------------------
    // Additional edge-case tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_srem_removes_key_when_empty() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "myset", &["a"]);

        srem(&mut ctx, &[bulk("myset"), bulk("a")]).unwrap();

        let db = ctx.store().database(ctx.selected_db());
        assert!(!db.exists(b"myset"));
    }

    #[test]
    fn test_sdiff_multiple_keys() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s1", &["a", "b", "c", "d"]);
        insert_set(&mut ctx, "s2", &["b"]);
        insert_set(&mut ctx, "s3", &["c"]);

        let result = sdiff(&mut ctx, &[bulk("s1"), bulk("s2"), bulk("s3")]).unwrap();
        let sorted = sorted_bytes_from_array(&result);
        assert_eq!(sorted, vec![Bytes::from("a"), Bytes::from("d")]);
    }

    #[test]
    fn test_sinter_single_key() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s1", &["a", "b"]);

        let result = sinter(&mut ctx, &[bulk("s1")]).unwrap();
        let sorted = sorted_bytes_from_array(&result);
        assert_eq!(sorted, vec![Bytes::from("a"), Bytes::from("b")]);
    }

    #[test]
    fn test_sunion_with_nonexistent() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s1", &["a"]);

        let result = sunion(&mut ctx, &[bulk("s1"), bulk("nokey")]).unwrap();
        let sorted = sorted_bytes_from_array(&result);
        assert_eq!(sorted, vec![Bytes::from("a")]);
    }

    #[test]
    fn test_smove_creates_destination() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "src", &["a", "b"]);

        let result = smove(&mut ctx, &[bulk("src"), bulk("dst"), bulk("a")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let members = smembers(&mut ctx, &[bulk("dst")]).unwrap();
        let sorted = sorted_bytes_from_array(&members);
        assert_eq!(sorted, vec![Bytes::from("a")]);
    }

    #[test]
    fn test_sintercard_limit_zero_means_no_limit() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s1", &["a", "b", "c"]);
        insert_set(&mut ctx, "s2", &["a", "b", "c"]);

        let result = sintercard(
            &mut ctx,
            &[bulk("2"), bulk("s1"), bulk("s2"), bulk("LIMIT"), bulk("0")],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(3));
    }

    #[test]
    fn test_spop_empty_set_with_count() {
        let mut ctx = make_ctx();
        let result = spop(&mut ctx, &[bulk("nokey"), bulk("5")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![]));
    }

    #[test]
    fn test_srandmember_empty_set_with_count() {
        let mut ctx = make_ctx();
        let result = srandmember(&mut ctx, &[bulk("nokey"), bulk("3")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![]));
    }

    #[test]
    fn test_sscan_with_count() {
        let mut ctx = make_ctx();
        let members: Vec<&str> = vec![
            "m01", "m02", "m03", "m04", "m05", "m06", "m07", "m08", "m09", "m10",
        ];
        insert_set(&mut ctx, "myset", &members);

        let result = sscan(
            &mut ctx,
            &[bulk("myset"), bulk("0"), bulk("COUNT"), bulk("3")],
        )
        .unwrap();
        if let RespValue::Array(outer) = &result {
            if let RespValue::BulkString(cursor_bytes) = &outer[0] {
                let cursor_str = std::str::from_utf8(cursor_bytes).unwrap();
                let cursor_val: usize = cursor_str.parse().unwrap();
                assert!(cursor_val > 0, "expected non-zero cursor for partial scan");
            }
            if let RespValue::Array(page) = &outer[1] {
                assert_eq!(page.len(), 3);
            }
        }
    }

    #[test]
    fn test_wrong_arity_errors() {
        let mut ctx = make_ctx();

        assert!(matches!(
            sadd(&mut ctx, &[bulk("key")]),
            Err(CommandError::WrongArity(_))
        ));
        assert!(matches!(
            srem(&mut ctx, &[bulk("key")]),
            Err(CommandError::WrongArity(_))
        ));
        assert!(matches!(
            smembers(&mut ctx, &[]),
            Err(CommandError::WrongArity(_))
        ));
        assert!(matches!(
            sismember(&mut ctx, &[bulk("key")]),
            Err(CommandError::WrongArity(_))
        ));
        assert!(matches!(
            smismember(&mut ctx, &[bulk("key")]),
            Err(CommandError::WrongArity(_))
        ));
        assert!(matches!(
            scard(&mut ctx, &[]),
            Err(CommandError::WrongArity(_))
        ));
        assert!(matches!(
            sinter(&mut ctx, &[]),
            Err(CommandError::WrongArity(_))
        ));
        assert!(matches!(
            sinterstore(&mut ctx, &[bulk("dest")]),
            Err(CommandError::WrongArity(_))
        ));
        assert!(matches!(
            sintercard(&mut ctx, &[bulk("1")]),
            Err(CommandError::WrongArity(_))
        ));
        assert!(matches!(
            sunion(&mut ctx, &[]),
            Err(CommandError::WrongArity(_))
        ));
        assert!(matches!(
            sunionstore(&mut ctx, &[bulk("dest")]),
            Err(CommandError::WrongArity(_))
        ));
        assert!(matches!(
            sdiff(&mut ctx, &[]),
            Err(CommandError::WrongArity(_))
        ));
        assert!(matches!(
            sdiffstore(&mut ctx, &[bulk("dest")]),
            Err(CommandError::WrongArity(_))
        ));
        assert!(matches!(
            srandmember(&mut ctx, &[]),
            Err(CommandError::WrongArity(_))
        ));
        assert!(matches!(
            spop(&mut ctx, &[]),
            Err(CommandError::WrongArity(_))
        ));
        assert!(matches!(
            smove(&mut ctx, &[bulk("src"), bulk("dst")]),
            Err(CommandError::WrongArity(_))
        ));
        assert!(matches!(
            sscan(&mut ctx, &[bulk("key")]),
            Err(CommandError::WrongArity(_))
        ));
    }

    // -----------------------------------------------------------------------
    // SINTERCARD option tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_sintercard_negative_limit() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "k1", &["a", "b"]);
        insert_set(&mut ctx, "k2", &["a", "b"]);

        let result = sintercard(
            &mut ctx,
            &[bulk("2"), bulk("k1"), bulk("k2"), bulk("LIMIT"), bulk("-1")],
        );
        assert!(matches!(result, Err(CommandError::InvalidArgument(_))));
    }

    #[test]
    fn test_sintercard_numkeys_zero() {
        let mut ctx = make_ctx();

        let result = sintercard(&mut ctx, &[bulk("0"), bulk("k1")]);
        assert!(matches!(result, Err(CommandError::InvalidArgument(_))));
    }

    #[test]
    fn test_sintercard_numkeys_mismatch() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "k1", &["a"]);
        insert_set(&mut ctx, "k2", &["a"]);

        // numkeys says 3, but only 2 key arguments provided
        let result = sintercard(&mut ctx, &[bulk("3"), bulk("k1"), bulk("k2")]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_sintercard_unknown_option() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "k1", &["a"]);
        insert_set(&mut ctx, "k2", &["a"]);

        let result = sintercard(
            &mut ctx,
            &[
                bulk("2"),
                bulk("k1"),
                bulk("k2"),
                bulk("NOTLIMIT"),
                bulk("5"),
            ],
        );
        assert!(matches!(result, Err(CommandError::SyntaxError)));
    }

    #[test]
    fn test_sintercard_limit_larger_than_intersection() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "k1", &["a", "b", "c"]);
        insert_set(&mut ctx, "k2", &["b", "c", "d"]);

        // Intersection is {b, c} (size 2), LIMIT 100 → returns 2
        let result = sintercard(
            &mut ctx,
            &[
                bulk("2"),
                bulk("k1"),
                bulk("k2"),
                bulk("LIMIT"),
                bulk("100"),
            ],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(2));
    }

    // -----------------------------------------------------------------------
    // SSCAN option tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_sscan_match_complex_pattern() {
        let mut ctx = make_ctx();
        insert_set(
            &mut ctx,
            "myset",
            &["apple", "apricot", "banana", "avocado"],
        );

        let result = sscan(
            &mut ctx,
            &[bulk("myset"), bulk("0"), bulk("MATCH"), bulk("a*")],
        )
        .unwrap();

        if let RespValue::Array(outer) = &result {
            if let RespValue::Array(members) = &outer[1] {
                // "apple", "apricot", "avocado" match "a*"
                assert_eq!(members.len(), 3);
                let sorted = sorted_bytes_from_array(&outer[1]);
                assert!(sorted.contains(&Bytes::from("apple")));
                assert!(sorted.contains(&Bytes::from("apricot")));
                assert!(sorted.contains(&Bytes::from("avocado")));
            } else {
                panic!("expected Array for members");
            }
        } else {
            panic!("expected Array for outer");
        }
    }

    #[test]
    fn test_sscan_full_iteration() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "myset", &["a", "b", "c", "d", "e", "f", "g"]);

        let mut all_found: HashSet<Bytes> = HashSet::new();
        let mut cursor: usize = 0;

        loop {
            let result = sscan(
                &mut ctx,
                &[
                    bulk("myset"),
                    bulk(&cursor.to_string()),
                    bulk("COUNT"),
                    bulk("3"),
                ],
            )
            .unwrap();

            if let RespValue::Array(outer) = &result {
                if let RespValue::BulkString(cursor_bytes) = &outer[0] {
                    cursor = std::str::from_utf8(cursor_bytes).unwrap().parse().unwrap();
                }
                if let RespValue::Array(members) = &outer[1] {
                    for m in members {
                        if let RespValue::BulkString(b) = m {
                            all_found.insert(b.clone());
                        }
                    }
                }
            }

            if cursor == 0 {
                break;
            }
        }

        assert_eq!(all_found.len(), 7);
        for expected in &["a", "b", "c", "d", "e", "f", "g"] {
            assert!(
                all_found.contains(&Bytes::from(*expected)),
                "missing member: {expected}"
            );
        }
    }

    #[test]
    fn test_sscan_nonexistent_key() {
        let mut ctx = make_ctx();

        let result = sscan(&mut ctx, &[bulk("nokey"), bulk("0")]).unwrap();

        if let RespValue::Array(outer) = &result {
            assert_eq!(outer.len(), 2);
            if let RespValue::BulkString(cursor_bytes) = &outer[0] {
                assert_eq!(cursor_bytes.as_ref(), b"0");
            } else {
                panic!("expected BulkString for cursor");
            }
            if let RespValue::Array(members) = &outer[1] {
                assert!(members.is_empty());
            } else {
                panic!("expected Array for members");
            }
        } else {
            panic!("expected Array for outer");
        }
    }

    #[test]
    fn test_sscan_match_and_count() {
        let mut ctx = make_ctx();
        insert_set(
            &mut ctx,
            "myset",
            &["foo1", "foo2", "foo3", "bar1", "bar2", "baz1"],
        );

        // MATCH foo* with COUNT 2 — first page should have at most 2 items
        let result = sscan(
            &mut ctx,
            &[
                bulk("myset"),
                bulk("0"),
                bulk("MATCH"),
                bulk("foo*"),
                bulk("COUNT"),
                bulk("2"),
            ],
        )
        .unwrap();

        if let RespValue::Array(outer) = &result {
            if let RespValue::Array(members) = &outer[1] {
                assert!(members.len() <= 2);
                // All returned members must match foo*
                for m in members {
                    if let RespValue::BulkString(b) = m {
                        let s = std::str::from_utf8(b).unwrap();
                        assert!(s.starts_with("foo"), "unexpected member: {s}");
                    }
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // SPOP edge case tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_spop_count_zero() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "myset", &["a", "b", "c"]);

        let result = spop(&mut ctx, &[bulk("myset"), bulk("0")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![]));

        // Set should be unchanged
        let card = scard(&mut ctx, &[bulk("myset")]).unwrap();
        assert_eq!(card, RespValue::Integer(3));
    }

    #[test]
    fn test_spop_negative_count() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "myset", &["a", "b"]);

        let result = spop(&mut ctx, &[bulk("myset"), bulk("-1")]);
        assert!(matches!(result, Err(CommandError::InvalidArgument(_))));
    }

    #[test]
    fn test_spop_removes_key_when_empty() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "myset", &["a", "b", "c"]);

        // Pop all elements using count
        spop(&mut ctx, &[bulk("myset"), bulk("3")]).unwrap();

        // Key should no longer exist
        let db = ctx.store().database(ctx.selected_db());
        assert!(!db.exists(b"myset"));
    }

    // -----------------------------------------------------------------------
    // SRANDMEMBER edge case tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_srandmember_negative_count_nonexistent() {
        let mut ctx = make_ctx();

        // SRANDMEMBER on a non-existent key with negative count → empty array
        let result = srandmember(&mut ctx, &[bulk("nokey"), bulk("-3")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![]));
    }

    // -----------------------------------------------------------------------
    // WRONGTYPE coverage
    // -----------------------------------------------------------------------

    #[test]
    fn test_wrongtype_smismember() {
        let mut ctx = make_ctx();
        insert_string(&mut ctx, "strkey", "hello");

        let result = smismember(&mut ctx, &[bulk("strkey"), bulk("a"), bulk("b")]);
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    #[test]
    fn test_wrongtype_sscan() {
        let mut ctx = make_ctx();
        insert_string(&mut ctx, "strkey", "hello");

        let result = sscan(&mut ctx, &[bulk("strkey"), bulk("0")]);
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    // ===================================================================
    // SADD — 18 new tests (existing: test_sadd_basic, test_sadd_duplicates)
    // ===================================================================

    #[test]
    fn test_sadd_wrong_arity_no_members() {
        let mut ctx = make_ctx();
        // SADD with key but no members
        let result = sadd(&mut ctx, &[bulk("key")]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_sadd_wrong_arity_empty() {
        let mut ctx = make_ctx();
        let result = sadd(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_sadd_wrong_type_on_string_key() {
        let mut ctx = make_ctx();
        insert_string(&mut ctx, "mystr", "hello");
        let result = sadd(&mut ctx, &[bulk("mystr"), bulk("a")]);
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    #[test]
    fn test_sadd_creates_new_set() {
        let mut ctx = make_ctx();
        let result = sadd(&mut ctx, &[bulk("newset"), bulk("x")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        let members = smembers(&mut ctx, &[bulk("newset")]).unwrap();
        let sorted = sorted_bytes_from_array(&members);
        assert_eq!(sorted, vec![Bytes::from("x")]);
    }

    #[test]
    fn test_sadd_single_member() {
        let mut ctx = make_ctx();
        let result = sadd(&mut ctx, &[bulk("s"), bulk("only")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        let card = scard(&mut ctx, &[bulk("s")]).unwrap();
        assert_eq!(card, RespValue::Integer(1));
    }

    #[test]
    fn test_sadd_many_members_at_once() {
        let mut ctx = make_ctx();
        let args: Vec<RespValue> = std::iter::once(bulk("big"))
            .chain((0..50).map(|i| bulk(&format!("m{i}"))))
            .collect();
        let result = sadd(&mut ctx, &args).unwrap();
        assert_eq!(result, RespValue::Integer(50));
        let card = scard(&mut ctx, &[bulk("big")]).unwrap();
        assert_eq!(card, RespValue::Integer(50));
    }

    #[test]
    fn test_sadd_all_duplicates_returns_zero() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "myset", &["a", "b"]);
        let result = sadd(&mut ctx, &[bulk("myset"), bulk("a"), bulk("b")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_sadd_mixed_new_and_existing() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "myset", &["a"]);
        let result = sadd(&mut ctx, &[bulk("myset"), bulk("a"), bulk("b"), bulk("c")]).unwrap();
        assert_eq!(result, RespValue::Integer(2));
        let card = scard(&mut ctx, &[bulk("myset")]).unwrap();
        assert_eq!(card, RespValue::Integer(3));
    }

    #[test]
    fn test_sadd_special_characters() {
        let mut ctx = make_ctx();
        let result = sadd(
            &mut ctx,
            &[
                bulk("s"),
                bulk("hello world"),
                bulk("foo\tbar"),
                bulk("a\nb"),
            ],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(3));
        let r = sismember(&mut ctx, &[bulk("s"), bulk("hello world")]).unwrap();
        assert_eq!(r, RespValue::Integer(1));
    }

    #[test]
    fn test_sadd_empty_string_member() {
        let mut ctx = make_ctx();
        let result = sadd(&mut ctx, &[bulk("s"), bulk("")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        let r = sismember(&mut ctx, &[bulk("s"), bulk("")]).unwrap();
        assert_eq!(r, RespValue::Integer(1));
    }

    #[test]
    fn test_sadd_binary_data() {
        let mut ctx = make_ctx();
        let bin = RespValue::BulkString(Bytes::from(vec![0u8, 1, 2, 255, 254]));
        let result = sadd(&mut ctx, &[bulk("s"), bin.clone()]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        let card = scard(&mut ctx, &[bulk("s")]).unwrap();
        assert_eq!(card, RespValue::Integer(1));
    }

    #[test]
    fn test_sadd_duplicate_args_in_same_call() {
        let mut ctx = make_ctx();
        let result = sadd(&mut ctx, &[bulk("s"), bulk("x"), bulk("x"), bulk("x")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        let card = scard(&mut ctx, &[bulk("s")]).unwrap();
        assert_eq!(card, RespValue::Integer(1));
    }

    #[test]
    fn test_sadd_on_expired_key() {
        use std::time::{Duration, Instant};
        let mut ctx = make_ctx();
        // Insert a set then expire it
        let db = ctx.store().database(ctx.selected_db());
        let mut set: HashSet<Bytes> = HashSet::new();
        set.insert(Bytes::from("old"));
        let mut entry = Entry::new(RedisValue::Set(set));
        entry.expires_at = Some(Instant::now() - Duration::from_secs(1));
        db.set(Bytes::from("s"), entry);

        // SADD on expired key should create fresh set
        let result = sadd(&mut ctx, &[bulk("s"), bulk("new")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        let members = smembers(&mut ctx, &[bulk("s")]).unwrap();
        let sorted = sorted_bytes_from_array(&members);
        assert_eq!(sorted, vec![Bytes::from("new")]);
    }

    #[test]
    fn test_sadd_returns_integer_type() {
        let mut ctx = make_ctx();
        let result = sadd(&mut ctx, &[bulk("s"), bulk("a")]).unwrap();
        assert!(matches!(result, RespValue::Integer(_)));
    }

    #[test]
    fn test_sadd_preserves_existing_members() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a", "b", "c"]);
        sadd(&mut ctx, &[bulk("s"), bulk("d")]).unwrap();
        let members = smembers(&mut ctx, &[bulk("s")]).unwrap();
        let sorted = sorted_bytes_from_array(&members);
        assert_eq!(
            sorted,
            vec![
                Bytes::from("a"),
                Bytes::from("b"),
                Bytes::from("c"),
                Bytes::from("d")
            ]
        );
    }

    #[test]
    fn test_sadd_incremental_adds() {
        let mut ctx = make_ctx();
        sadd(&mut ctx, &[bulk("s"), bulk("a")]).unwrap();
        sadd(&mut ctx, &[bulk("s"), bulk("b")]).unwrap();
        sadd(&mut ctx, &[bulk("s"), bulk("c")]).unwrap();
        let card = scard(&mut ctx, &[bulk("s")]).unwrap();
        assert_eq!(card, RespValue::Integer(3));
    }

    #[test]
    fn test_sadd_large_set() {
        let mut ctx = make_ctx();
        let args: Vec<RespValue> = std::iter::once(bulk("large"))
            .chain((0..1000).map(|i| bulk(&format!("member_{i}"))))
            .collect();
        let result = sadd(&mut ctx, &args).unwrap();
        assert_eq!(result, RespValue::Integer(1000));
        let card = scard(&mut ctx, &[bulk("large")]).unwrap();
        assert_eq!(card, RespValue::Integer(1000));
    }

    #[test]
    fn test_sadd_after_srem() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a", "b"]);
        srem(&mut ctx, &[bulk("s"), bulk("a")]).unwrap();
        let result = sadd(&mut ctx, &[bulk("s"), bulk("a")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        let card = scard(&mut ctx, &[bulk("s")]).unwrap();
        assert_eq!(card, RespValue::Integer(2));
    }

    // ===================================================================
    // SREM — 17 new tests (existing: test_srem_basic, test_srem_nonexistent,
    //        test_srem_removes_key_when_empty)
    // ===================================================================

    #[test]
    fn test_srem_wrong_arity_no_members() {
        let mut ctx = make_ctx();
        let result = srem(&mut ctx, &[bulk("key")]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_srem_wrong_arity_empty() {
        let mut ctx = make_ctx();
        let result = srem(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_srem_wrong_type_on_string() {
        let mut ctx = make_ctx();
        insert_string(&mut ctx, "mystr", "val");
        let result = srem(&mut ctx, &[bulk("mystr"), bulk("a")]);
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    #[test]
    fn test_srem_nonexistent_key_returns_zero() {
        let mut ctx = make_ctx();
        let result = srem(&mut ctx, &[bulk("nokey"), bulk("a"), bulk("b")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_srem_on_expired_key() {
        use std::time::{Duration, Instant};
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut set: HashSet<Bytes> = HashSet::new();
        set.insert(Bytes::from("a"));
        let mut entry = Entry::new(RedisValue::Set(set));
        entry.expires_at = Some(Instant::now() - Duration::from_secs(1));
        db.set(Bytes::from("s"), entry);

        let result = srem(&mut ctx, &[bulk("s"), bulk("a")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_srem_single_member() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a", "b", "c"]);
        let result = srem(&mut ctx, &[bulk("s"), bulk("b")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        let card = scard(&mut ctx, &[bulk("s")]).unwrap();
        assert_eq!(card, RespValue::Integer(2));
    }

    #[test]
    fn test_srem_multiple_members() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a", "b", "c", "d"]);
        let result = srem(&mut ctx, &[bulk("s"), bulk("a"), bulk("c"), bulk("d")]).unwrap();
        assert_eq!(result, RespValue::Integer(3));
        let members = smembers(&mut ctx, &[bulk("s")]).unwrap();
        let sorted = sorted_bytes_from_array(&members);
        assert_eq!(sorted, vec![Bytes::from("b")]);
    }

    #[test]
    fn test_srem_nonexistent_members() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a", "b"]);
        let result = srem(&mut ctx, &[bulk("s"), bulk("x"), bulk("y")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
        let card = scard(&mut ctx, &[bulk("s")]).unwrap();
        assert_eq!(card, RespValue::Integer(2));
    }

    #[test]
    fn test_srem_mixed_existing_and_missing() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a", "b", "c"]);
        let result = srem(&mut ctx, &[bulk("s"), bulk("a"), bulk("x"), bulk("c")]).unwrap();
        assert_eq!(result, RespValue::Integer(2));
        let card = scard(&mut ctx, &[bulk("s")]).unwrap();
        assert_eq!(card, RespValue::Integer(1));
    }

    #[test]
    fn test_srem_duplicate_args() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a", "b"]);
        let result = srem(&mut ctx, &[bulk("s"), bulk("a"), bulk("a"), bulk("a")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_srem_all_members_removes_key() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a", "b", "c"]);
        srem(&mut ctx, &[bulk("s"), bulk("a"), bulk("b"), bulk("c")]).unwrap();
        let db = ctx.store().database(ctx.selected_db());
        assert!(!db.exists(b"s"));
    }

    #[test]
    fn test_srem_returns_integer_type() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a"]);
        let result = srem(&mut ctx, &[bulk("s"), bulk("a")]).unwrap();
        assert!(matches!(result, RespValue::Integer(_)));
    }

    #[test]
    fn test_srem_special_characters() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["hello world", "foo\tbar"]);
        let result = srem(&mut ctx, &[bulk("s"), bulk("hello world")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
        let card = scard(&mut ctx, &[bulk("s")]).unwrap();
        assert_eq!(card, RespValue::Integer(1));
    }

    #[test]
    fn test_srem_empty_string_member() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &[""]);
        let result = srem(&mut ctx, &[bulk("s"), bulk("")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_srem_then_sadd_same_member() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a", "b"]);
        srem(&mut ctx, &[bulk("s"), bulk("a")]).unwrap();
        let r = sismember(&mut ctx, &[bulk("s"), bulk("a")]).unwrap();
        assert_eq!(r, RespValue::Integer(0));
        sadd(&mut ctx, &[bulk("s"), bulk("a")]).unwrap();
        let r = sismember(&mut ctx, &[bulk("s"), bulk("a")]).unwrap();
        assert_eq!(r, RespValue::Integer(1));
    }

    #[test]
    fn test_srem_large_set() {
        let mut ctx = make_ctx();
        let members: Vec<&str> = (0..100).map(|_| "x").collect();
        // Build a large set with unique members
        let db = ctx.store().database(ctx.selected_db());
        let mut set: HashSet<Bytes> = HashSet::new();
        for i in 0..100 {
            set.insert(Bytes::from(format!("m{i}")));
        }
        db.set(Bytes::from("s"), Entry::new(RedisValue::Set(set)));

        // Remove 50 members
        let args: Vec<RespValue> = std::iter::once(bulk("s"))
            .chain((0..50).map(|i| bulk(&format!("m{i}"))))
            .collect();
        let result = srem(&mut ctx, &args).unwrap();
        assert_eq!(result, RespValue::Integer(50));
        let card = scard(&mut ctx, &[bulk("s")]).unwrap();
        assert_eq!(card, RespValue::Integer(50));
    }

    #[test]
    fn test_srem_binary_data() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut set: HashSet<Bytes> = HashSet::new();
        set.insert(Bytes::from(vec![0u8, 1, 2, 255]));
        db.set(Bytes::from("s"), Entry::new(RedisValue::Set(set)));

        let bin = RespValue::BulkString(Bytes::from(vec![0u8, 1, 2, 255]));
        let result = srem(&mut ctx, &[bulk("s"), bin]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    // ===================================================================
    // SMEMBERS — 18 new tests (existing: test_smembers, test_smembers_empty)
    // ===================================================================

    #[test]
    fn test_smembers_wrong_arity_empty() {
        let mut ctx = make_ctx();
        let result = smembers(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_smembers_extra_args_still_works() {
        // SMEMBERS only checks args.is_empty(), extra args are ignored
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a"]);
        let result = smembers(&mut ctx, &[bulk("s"), bulk("extra")]).unwrap();
        let sorted = sorted_bytes_from_array(&result);
        assert_eq!(sorted, vec![Bytes::from("a")]);
    }

    #[test]
    fn test_smembers_wrong_type_on_string() {
        let mut ctx = make_ctx();
        insert_string(&mut ctx, "strkey", "val");
        let result = smembers(&mut ctx, &[bulk("strkey")]);
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    #[test]
    fn test_smembers_nonexistent_key() {
        let mut ctx = make_ctx();
        let result = smembers(&mut ctx, &[bulk("nokey")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![]));
    }

    #[test]
    fn test_smembers_on_expired_key() {
        use std::time::{Duration, Instant};
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut set: HashSet<Bytes> = HashSet::new();
        set.insert(Bytes::from("a"));
        let mut entry = Entry::new(RedisValue::Set(set));
        entry.expires_at = Some(Instant::now() - Duration::from_secs(1));
        db.set(Bytes::from("s"), entry);

        let result = smembers(&mut ctx, &[bulk("s")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![]));
    }

    #[test]
    fn test_smembers_single_member() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["only"]);
        let result = smembers(&mut ctx, &[bulk("s")]).unwrap();
        let sorted = sorted_bytes_from_array(&result);
        assert_eq!(sorted, vec![Bytes::from("only")]);
    }

    #[test]
    fn test_smembers_returns_array_type() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a"]);
        let result = smembers(&mut ctx, &[bulk("s")]).unwrap();
        assert!(matches!(result, RespValue::Array(_)));
    }

    #[test]
    fn test_smembers_returns_all_members() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a", "b", "c", "d", "e"]);
        let result = smembers(&mut ctx, &[bulk("s")]).unwrap();
        let sorted = sorted_bytes_from_array(&result);
        assert_eq!(sorted.len(), 5);
        assert_eq!(
            sorted,
            vec![
                Bytes::from("a"),
                Bytes::from("b"),
                Bytes::from("c"),
                Bytes::from("d"),
                Bytes::from("e"),
            ]
        );
    }

    #[test]
    fn test_smembers_special_characters() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["hello world", "tab\there", "nl\nnewline"]);
        let result = smembers(&mut ctx, &[bulk("s")]).unwrap();
        let sorted = sorted_bytes_from_array(&result);
        assert_eq!(sorted.len(), 3);
    }

    #[test]
    fn test_smembers_empty_string_member() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &[""]);
        let result = smembers(&mut ctx, &[bulk("s")]).unwrap();
        let sorted = sorted_bytes_from_array(&result);
        assert_eq!(sorted, vec![Bytes::from("")]);
    }

    #[test]
    fn test_smembers_binary_data() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut set: HashSet<Bytes> = HashSet::new();
        set.insert(Bytes::from(vec![0u8, 255, 128]));
        db.set(Bytes::from("s"), Entry::new(RedisValue::Set(set)));

        let result = smembers(&mut ctx, &[bulk("s")]).unwrap();
        if let RespValue::Array(arr) = &result {
            assert_eq!(arr.len(), 1);
        } else {
            panic!("expected Array");
        }
    }

    #[test]
    fn test_smembers_large_set() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut set: HashSet<Bytes> = HashSet::new();
        for i in 0..500 {
            set.insert(Bytes::from(format!("m{i}")));
        }
        db.set(Bytes::from("s"), Entry::new(RedisValue::Set(set)));

        let result = smembers(&mut ctx, &[bulk("s")]).unwrap();
        if let RespValue::Array(arr) = &result {
            assert_eq!(arr.len(), 500);
        } else {
            panic!("expected Array");
        }
    }

    #[test]
    fn test_smembers_after_sadd() {
        let mut ctx = make_ctx();
        sadd(&mut ctx, &[bulk("s"), bulk("a"), bulk("b")]).unwrap();
        let result = smembers(&mut ctx, &[bulk("s")]).unwrap();
        let sorted = sorted_bytes_from_array(&result);
        assert_eq!(sorted, vec![Bytes::from("a"), Bytes::from("b")]);
    }

    #[test]
    fn test_smembers_after_srem() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a", "b", "c"]);
        srem(&mut ctx, &[bulk("s"), bulk("b")]).unwrap();
        let result = smembers(&mut ctx, &[bulk("s")]).unwrap();
        let sorted = sorted_bytes_from_array(&result);
        assert_eq!(sorted, vec![Bytes::from("a"), Bytes::from("c")]);
    }

    #[test]
    fn test_smembers_does_not_modify_set() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a", "b"]);
        smembers(&mut ctx, &[bulk("s")]).unwrap();
        smembers(&mut ctx, &[bulk("s")]).unwrap();
        let card = scard(&mut ctx, &[bulk("s")]).unwrap();
        assert_eq!(card, RespValue::Integer(2));
    }

    #[test]
    fn test_smembers_each_element_is_bulk_string() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a", "b"]);
        let result = smembers(&mut ctx, &[bulk("s")]).unwrap();
        if let RespValue::Array(arr) = &result {
            for item in arr {
                assert!(matches!(item, RespValue::BulkString(_)));
            }
        } else {
            panic!("expected Array");
        }
    }

    #[test]
    fn test_smembers_after_removing_all_then_adding() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a"]);
        srem(&mut ctx, &[bulk("s"), bulk("a")]).unwrap();
        let result = smembers(&mut ctx, &[bulk("s")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![]));
        sadd(&mut ctx, &[bulk("s"), bulk("x")]).unwrap();
        let result = smembers(&mut ctx, &[bulk("s")]).unwrap();
        let sorted = sorted_bytes_from_array(&result);
        assert_eq!(sorted, vec![Bytes::from("x")]);
    }

    #[test]
    fn test_smembers_no_duplicates() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a", "b", "c"]);
        let result = smembers(&mut ctx, &[bulk("s")]).unwrap();
        let sorted = sorted_bytes_from_array(&result);
        let unique: HashSet<&Bytes> = sorted.iter().collect();
        assert_eq!(unique.len(), sorted.len());
    }

    // ===================================================================
    // SISMEMBER — 19 new tests (existing: test_sismember)
    // ===================================================================

    #[test]
    fn test_sismember_wrong_arity_empty() {
        let mut ctx = make_ctx();
        let result = sismember(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_sismember_wrong_arity_one_arg() {
        let mut ctx = make_ctx();
        let result = sismember(&mut ctx, &[bulk("key")]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_sismember_extra_args_still_works() {
        // SISMEMBER checks args.len() < 2, extra args are ignored
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a"]);
        let result = sismember(&mut ctx, &[bulk("s"), bulk("a"), bulk("extra")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_sismember_wrong_type_on_string() {
        let mut ctx = make_ctx();
        insert_string(&mut ctx, "strkey", "hello");
        let result = sismember(&mut ctx, &[bulk("strkey"), bulk("a")]);
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    #[test]
    fn test_sismember_nonexistent_key() {
        let mut ctx = make_ctx();
        let result = sismember(&mut ctx, &[bulk("nokey"), bulk("a")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_sismember_on_expired_key() {
        use std::time::{Duration, Instant};
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut set: HashSet<Bytes> = HashSet::new();
        set.insert(Bytes::from("a"));
        let mut entry = Entry::new(RedisValue::Set(set));
        entry.expires_at = Some(Instant::now() - Duration::from_secs(1));
        db.set(Bytes::from("s"), entry);

        let result = sismember(&mut ctx, &[bulk("s"), bulk("a")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_sismember_member_exists() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a", "b", "c"]);
        let result = sismember(&mut ctx, &[bulk("s"), bulk("b")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_sismember_member_not_exists() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a", "b"]);
        let result = sismember(&mut ctx, &[bulk("s"), bulk("z")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_sismember_returns_integer_type() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a"]);
        let result = sismember(&mut ctx, &[bulk("s"), bulk("a")]).unwrap();
        assert!(matches!(result, RespValue::Integer(_)));
    }

    #[test]
    fn test_sismember_special_characters() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["hello world", "tab\there"]);
        let r = sismember(&mut ctx, &[bulk("s"), bulk("hello world")]).unwrap();
        assert_eq!(r, RespValue::Integer(1));
        let r = sismember(&mut ctx, &[bulk("s"), bulk("tab\there")]).unwrap();
        assert_eq!(r, RespValue::Integer(1));
    }

    #[test]
    fn test_sismember_empty_string_member() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &[""]);
        let r = sismember(&mut ctx, &[bulk("s"), bulk("")]).unwrap();
        assert_eq!(r, RespValue::Integer(1));
    }

    #[test]
    fn test_sismember_binary_data() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut set: HashSet<Bytes> = HashSet::new();
        let bin_val = Bytes::from(vec![0u8, 1, 255]);
        set.insert(bin_val.clone());
        db.set(Bytes::from("s"), Entry::new(RedisValue::Set(set)));

        let bin = RespValue::BulkString(bin_val);
        let result = sismember(&mut ctx, &[bulk("s"), bin]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_sismember_after_sadd() {
        let mut ctx = make_ctx();
        sadd(&mut ctx, &[bulk("s"), bulk("x")]).unwrap();
        let r = sismember(&mut ctx, &[bulk("s"), bulk("x")]).unwrap();
        assert_eq!(r, RespValue::Integer(1));
    }

    #[test]
    fn test_sismember_after_srem() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a", "b"]);
        srem(&mut ctx, &[bulk("s"), bulk("a")]).unwrap();
        let r = sismember(&mut ctx, &[bulk("s"), bulk("a")]).unwrap();
        assert_eq!(r, RespValue::Integer(0));
        let r = sismember(&mut ctx, &[bulk("s"), bulk("b")]).unwrap();
        assert_eq!(r, RespValue::Integer(1));
    }

    #[test]
    fn test_sismember_case_sensitive() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["Hello"]);
        let r = sismember(&mut ctx, &[bulk("s"), bulk("Hello")]).unwrap();
        assert_eq!(r, RespValue::Integer(1));
        let r = sismember(&mut ctx, &[bulk("s"), bulk("hello")]).unwrap();
        assert_eq!(r, RespValue::Integer(0));
        let r = sismember(&mut ctx, &[bulk("s"), bulk("HELLO")]).unwrap();
        assert_eq!(r, RespValue::Integer(0));
    }

    #[test]
    fn test_sismember_large_set() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut set: HashSet<Bytes> = HashSet::new();
        for i in 0..1000 {
            set.insert(Bytes::from(format!("m{i}")));
        }
        db.set(Bytes::from("s"), Entry::new(RedisValue::Set(set)));

        let r = sismember(&mut ctx, &[bulk("s"), bulk("m500")]).unwrap();
        assert_eq!(r, RespValue::Integer(1));
        let r = sismember(&mut ctx, &[bulk("s"), bulk("m9999")]).unwrap();
        assert_eq!(r, RespValue::Integer(0));
    }

    #[test]
    fn test_sismember_does_not_modify_set() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a", "b"]);
        sismember(&mut ctx, &[bulk("s"), bulk("a")]).unwrap();
        sismember(&mut ctx, &[bulk("s"), bulk("z")]).unwrap();
        let card = scard(&mut ctx, &[bulk("s")]).unwrap();
        assert_eq!(card, RespValue::Integer(2));
    }

    #[test]
    fn test_sismember_numeric_strings() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["123", "0", "-1"]);
        let r = sismember(&mut ctx, &[bulk("s"), bulk("123")]).unwrap();
        assert_eq!(r, RespValue::Integer(1));
        let r = sismember(&mut ctx, &[bulk("s"), bulk("0")]).unwrap();
        assert_eq!(r, RespValue::Integer(1));
        let r = sismember(&mut ctx, &[bulk("s"), bulk("-1")]).unwrap();
        assert_eq!(r, RespValue::Integer(1));
        let r = sismember(&mut ctx, &[bulk("s"), bulk("456")]).unwrap();
        assert_eq!(r, RespValue::Integer(0));
    }

    #[test]
    fn test_sismember_empty_set_key_gone() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a"]);
        srem(&mut ctx, &[bulk("s"), bulk("a")]).unwrap();
        // Key is removed after set becomes empty
        let r = sismember(&mut ctx, &[bulk("s"), bulk("a")]).unwrap();
        assert_eq!(r, RespValue::Integer(0));
    }

    // ===================================================================
    // SMISMEMBER — 19 new tests (existing: test_smismember)
    // ===================================================================

    #[test]
    fn test_smismember_wrong_arity_empty() {
        let mut ctx = make_ctx();
        let result = smismember(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_smismember_wrong_arity_key_only() {
        let mut ctx = make_ctx();
        let result = smismember(&mut ctx, &[bulk("key")]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_smismember_wrong_type_on_string() {
        let mut ctx = make_ctx();
        insert_string(&mut ctx, "mystr", "val");
        let result = smismember(&mut ctx, &[bulk("mystr"), bulk("a")]);
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    #[test]
    fn test_smismember_nonexistent_key() {
        let mut ctx = make_ctx();
        let result = smismember(&mut ctx, &[bulk("nokey"), bulk("a"), bulk("b")]).unwrap();
        let expected = RespValue::Array(vec![RespValue::Integer(0), RespValue::Integer(0)]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_smismember_on_expired_key() {
        use std::time::{Duration, Instant};
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut set: HashSet<Bytes> = HashSet::new();
        set.insert(Bytes::from("a"));
        let mut entry = Entry::new(RedisValue::Set(set));
        entry.expires_at = Some(Instant::now() - Duration::from_secs(1));
        db.set(Bytes::from("s"), entry);

        let result = smismember(&mut ctx, &[bulk("s"), bulk("a")]).unwrap();
        let expected = RespValue::Array(vec![RespValue::Integer(0)]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_smismember_all_exist() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a", "b", "c"]);
        let result = smismember(&mut ctx, &[bulk("s"), bulk("a"), bulk("b"), bulk("c")]).unwrap();
        let expected = RespValue::Array(vec![
            RespValue::Integer(1),
            RespValue::Integer(1),
            RespValue::Integer(1),
        ]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_smismember_none_exist() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a", "b"]);
        let result = smismember(&mut ctx, &[bulk("s"), bulk("x"), bulk("y")]).unwrap();
        let expected = RespValue::Array(vec![RespValue::Integer(0), RespValue::Integer(0)]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_smismember_mixed() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a", "c", "e"]);
        let result = smismember(
            &mut ctx,
            &[
                bulk("s"),
                bulk("a"),
                bulk("b"),
                bulk("c"),
                bulk("d"),
                bulk("e"),
            ],
        )
        .unwrap();
        let expected = RespValue::Array(vec![
            RespValue::Integer(1),
            RespValue::Integer(0),
            RespValue::Integer(1),
            RespValue::Integer(0),
            RespValue::Integer(1),
        ]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_smismember_single_member_exists() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["hello"]);
        let result = smismember(&mut ctx, &[bulk("s"), bulk("hello")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![RespValue::Integer(1)]));
    }

    #[test]
    fn test_smismember_single_member_not_exists() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["hello"]);
        let result = smismember(&mut ctx, &[bulk("s"), bulk("world")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![RespValue::Integer(0)]));
    }

    #[test]
    fn test_smismember_returns_array_type() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a"]);
        let result = smismember(&mut ctx, &[bulk("s"), bulk("a")]).unwrap();
        assert!(matches!(result, RespValue::Array(_)));
    }

    #[test]
    fn test_smismember_duplicate_queries() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a"]);
        let result = smismember(&mut ctx, &[bulk("s"), bulk("a"), bulk("a"), bulk("a")]).unwrap();
        let expected = RespValue::Array(vec![
            RespValue::Integer(1),
            RespValue::Integer(1),
            RespValue::Integer(1),
        ]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_smismember_special_characters() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["hello world"]);
        let result = smismember(&mut ctx, &[bulk("s"), bulk("hello world"), bulk("nope")]).unwrap();
        let expected = RespValue::Array(vec![RespValue::Integer(1), RespValue::Integer(0)]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_smismember_empty_string() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &[""]);
        let result = smismember(&mut ctx, &[bulk("s"), bulk(""), bulk("notempty")]).unwrap();
        let expected = RespValue::Array(vec![RespValue::Integer(1), RespValue::Integer(0)]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_smismember_binary_data() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut set: HashSet<Bytes> = HashSet::new();
        let bin_val = Bytes::from(vec![0u8, 1, 255]);
        set.insert(bin_val.clone());
        db.set(Bytes::from("s"), Entry::new(RedisValue::Set(set)));

        let bin = RespValue::BulkString(bin_val);
        let other = RespValue::BulkString(Bytes::from(vec![99u8]));
        let result = smismember(&mut ctx, &[bulk("s"), bin, other]).unwrap();
        let expected = RespValue::Array(vec![RespValue::Integer(1), RespValue::Integer(0)]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_smismember_preserves_query_order() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["b"]);
        let result = smismember(&mut ctx, &[bulk("s"), bulk("a"), bulk("b"), bulk("c")]).unwrap();
        // Only "b" is a member, so [0, 1, 0] in order
        let expected = RespValue::Array(vec![
            RespValue::Integer(0),
            RespValue::Integer(1),
            RespValue::Integer(0),
        ]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_smismember_after_sadd() {
        let mut ctx = make_ctx();
        sadd(&mut ctx, &[bulk("s"), bulk("x"), bulk("y")]).unwrap();
        let result = smismember(&mut ctx, &[bulk("s"), bulk("x"), bulk("z")]).unwrap();
        let expected = RespValue::Array(vec![RespValue::Integer(1), RespValue::Integer(0)]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_smismember_after_srem() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a", "b", "c"]);
        srem(&mut ctx, &[bulk("s"), bulk("b")]).unwrap();
        let result = smismember(&mut ctx, &[bulk("s"), bulk("a"), bulk("b"), bulk("c")]).unwrap();
        let expected = RespValue::Array(vec![
            RespValue::Integer(1),
            RespValue::Integer(0),
            RespValue::Integer(1),
        ]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_smismember_large_query() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["m0", "m1", "m2"]);
        let args: Vec<RespValue> = std::iter::once(bulk("s"))
            .chain((0..100).map(|i| bulk(&format!("m{i}"))))
            .collect();
        let result = smismember(&mut ctx, &args).unwrap();
        if let RespValue::Array(arr) = &result {
            assert_eq!(arr.len(), 100);
            // m0, m1, m2 should be 1; rest should be 0
            assert_eq!(arr[0], RespValue::Integer(1));
            assert_eq!(arr[1], RespValue::Integer(1));
            assert_eq!(arr[2], RespValue::Integer(1));
            assert_eq!(arr[3], RespValue::Integer(0));
            assert_eq!(arr[99], RespValue::Integer(0));
        } else {
            panic!("expected Array");
        }
    }

    // ===================================================================
    // SCARD — 19 new tests (existing: test_scard)
    // ===================================================================

    #[test]
    fn test_scard_wrong_arity_empty() {
        let mut ctx = make_ctx();
        let result = scard(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_scard_extra_args_still_works() {
        // SCARD checks args.is_empty(), extra args are ignored
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a", "b"]);
        let result = scard(&mut ctx, &[bulk("s"), bulk("extra")]).unwrap();
        assert_eq!(result, RespValue::Integer(2));
    }

    #[test]
    fn test_scard_wrong_type_on_string() {
        let mut ctx = make_ctx();
        insert_string(&mut ctx, "mystr", "hello");
        let result = scard(&mut ctx, &[bulk("mystr")]);
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    #[test]
    fn test_scard_nonexistent_key() {
        let mut ctx = make_ctx();
        let result = scard(&mut ctx, &[bulk("nokey")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_scard_on_expired_key() {
        use std::time::{Duration, Instant};
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut set: HashSet<Bytes> = HashSet::new();
        set.insert(Bytes::from("a"));
        set.insert(Bytes::from("b"));
        let mut entry = Entry::new(RedisValue::Set(set));
        entry.expires_at = Some(Instant::now() - Duration::from_secs(1));
        db.set(Bytes::from("s"), entry);

        let result = scard(&mut ctx, &[bulk("s")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_scard_single_member() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["only"]);
        let result = scard(&mut ctx, &[bulk("s")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_scard_multiple_members() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a", "b", "c", "d", "e"]);
        let result = scard(&mut ctx, &[bulk("s")]).unwrap();
        assert_eq!(result, RespValue::Integer(5));
    }

    #[test]
    fn test_scard_returns_integer_type() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a"]);
        let result = scard(&mut ctx, &[bulk("s")]).unwrap();
        assert!(matches!(result, RespValue::Integer(_)));
    }

    #[test]
    fn test_scard_after_sadd() {
        let mut ctx = make_ctx();
        sadd(&mut ctx, &[bulk("s"), bulk("a"), bulk("b"), bulk("c")]).unwrap();
        let result = scard(&mut ctx, &[bulk("s")]).unwrap();
        assert_eq!(result, RespValue::Integer(3));
    }

    #[test]
    fn test_scard_after_srem() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a", "b", "c"]);
        srem(&mut ctx, &[bulk("s"), bulk("a")]).unwrap();
        let result = scard(&mut ctx, &[bulk("s")]).unwrap();
        assert_eq!(result, RespValue::Integer(2));
    }

    #[test]
    fn test_scard_after_removing_all() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a"]);
        srem(&mut ctx, &[bulk("s"), bulk("a")]).unwrap();
        let result = scard(&mut ctx, &[bulk("s")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_scard_does_not_modify_set() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a", "b"]);
        scard(&mut ctx, &[bulk("s")]).unwrap();
        scard(&mut ctx, &[bulk("s")]).unwrap();
        let members = smembers(&mut ctx, &[bulk("s")]).unwrap();
        let sorted = sorted_bytes_from_array(&members);
        assert_eq!(sorted.len(), 2);
    }

    #[test]
    fn test_scard_large_set() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut set: HashSet<Bytes> = HashSet::new();
        for i in 0..1000 {
            set.insert(Bytes::from(format!("m{i}")));
        }
        db.set(Bytes::from("s"), Entry::new(RedisValue::Set(set)));

        let result = scard(&mut ctx, &[bulk("s")]).unwrap();
        assert_eq!(result, RespValue::Integer(1000));
    }

    #[test]
    fn test_scard_with_empty_string_member() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &[""]);
        let result = scard(&mut ctx, &[bulk("s")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_scard_after_spop() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a", "b", "c"]);
        spop(&mut ctx, &[bulk("s")]).unwrap();
        let result = scard(&mut ctx, &[bulk("s")]).unwrap();
        assert_eq!(result, RespValue::Integer(2));
    }

    #[test]
    fn test_scard_after_smove_source() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "src", &["a", "b", "c"]);
        insert_set(&mut ctx, "dst", &["x"]);
        smove(&mut ctx, &[bulk("src"), bulk("dst"), bulk("a")]).unwrap();
        let result = scard(&mut ctx, &[bulk("src")]).unwrap();
        assert_eq!(result, RespValue::Integer(2));
        let result = scard(&mut ctx, &[bulk("dst")]).unwrap();
        assert_eq!(result, RespValue::Integer(2));
    }

    #[test]
    fn test_scard_incremental_adds() {
        let mut ctx = make_ctx();
        sadd(&mut ctx, &[bulk("s"), bulk("a")]).unwrap();
        assert_eq!(
            scard(&mut ctx, &[bulk("s")]).unwrap(),
            RespValue::Integer(1)
        );
        sadd(&mut ctx, &[bulk("s"), bulk("b")]).unwrap();
        assert_eq!(
            scard(&mut ctx, &[bulk("s")]).unwrap(),
            RespValue::Integer(2)
        );
        sadd(&mut ctx, &[bulk("s"), bulk("c")]).unwrap();
        assert_eq!(
            scard(&mut ctx, &[bulk("s")]).unwrap(),
            RespValue::Integer(3)
        );
    }

    #[test]
    fn test_scard_duplicate_add_no_change() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a", "b"]);
        sadd(&mut ctx, &[bulk("s"), bulk("a")]).unwrap();
        let result = scard(&mut ctx, &[bulk("s")]).unwrap();
        assert_eq!(result, RespValue::Integer(2));
    }

    #[test]
    fn test_scard_binary_members() {
        let mut ctx = make_ctx();
        let db = ctx.store().database(ctx.selected_db());
        let mut set: HashSet<Bytes> = HashSet::new();
        set.insert(Bytes::from(vec![0u8, 255]));
        set.insert(Bytes::from(vec![1u8, 254]));
        set.insert(Bytes::from(vec![2u8, 253]));
        db.set(Bytes::from("s"), Entry::new(RedisValue::Set(set)));

        let result = scard(&mut ctx, &[bulk("s")]).unwrap();
        assert_eq!(result, RespValue::Integer(3));
    }

    // ===================================================================
    // SRANDMEMBER — 16 new tests
    // ===================================================================

    #[test]
    fn test_srandmember_wrong_arity() {
        let mut ctx = make_ctx();
        let result = srandmember(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_srandmember_nonexistent_key_no_count() {
        let mut ctx = make_ctx();
        let result = srandmember(&mut ctx, &[bulk("nokey")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_srandmember_nonexistent_key_with_count() {
        let mut ctx = make_ctx();
        let result = srandmember(&mut ctx, &[bulk("nokey"), bulk("5")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![]));
    }

    #[test]
    fn test_srandmember_positive_count() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a", "b", "c", "d", "e"]);

        let result = srandmember(&mut ctx, &[bulk("s"), bulk("3")]).unwrap();
        if let RespValue::Array(arr) = &result {
            assert_eq!(arr.len(), 3);
            let mut seen = HashSet::new();
            for item in arr {
                if let RespValue::BulkString(b) = item {
                    assert!(seen.insert(b.clone()), "duplicate in positive count");
                }
            }
        } else {
            panic!("expected Array");
        }
    }

    #[test]
    fn test_srandmember_negative_count() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a", "b", "c"]);

        let result = srandmember(&mut ctx, &[bulk("s"), bulk("-5")]).unwrap();
        if let RespValue::Array(arr) = &result {
            assert_eq!(arr.len(), 5);
            for item in arr {
                if let RespValue::BulkString(b) = item {
                    let s = std::str::from_utf8(b).unwrap();
                    assert!(s == "a" || s == "b" || s == "c");
                }
            }
        } else {
            panic!("expected Array");
        }
    }

    #[test]
    fn test_srandmember_negative_count_duplicates() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["only"]);

        let result = srandmember(&mut ctx, &[bulk("s"), bulk("-10")]).unwrap();
        if let RespValue::Array(arr) = &result {
            assert_eq!(arr.len(), 10);
            for item in arr {
                assert_eq!(*item, RespValue::BulkString(Bytes::from("only")));
            }
        } else {
            panic!("expected Array");
        }
    }

    #[test]
    fn test_srandmember_count_zero() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a", "b"]);

        let result = srandmember(&mut ctx, &[bulk("s"), bulk("0")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![]));
    }

    #[test]
    fn test_srandmember_count_larger_than_set() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a", "b"]);

        let result = srandmember(&mut ctx, &[bulk("s"), bulk("100")]).unwrap();
        if let RespValue::Array(arr) = &result {
            assert_eq!(arr.len(), 2);
        } else {
            panic!("expected Array");
        }
    }

    #[test]
    fn test_srandmember_wrong_type() {
        let mut ctx = make_ctx();
        insert_string(&mut ctx, "str", "hello");

        let result = srandmember(&mut ctx, &[bulk("str")]);
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    #[test]
    fn test_srandmember_expired_key() {
        use std::time::Instant;
        let mut ctx = make_ctx();

        let mut set = HashSet::new();
        set.insert(Bytes::from("a"));
        let mut entry = Entry::new(RedisValue::Set(set));
        entry.expires_at = Some(Instant::now() - std::time::Duration::from_secs(1));
        let db = ctx.store().database(ctx.selected_db());
        db.set(Bytes::from("expired_set"), entry);

        let result = srandmember(&mut ctx, &[bulk("expired_set")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_srandmember_empty_set() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a"]);
        srem(&mut ctx, &[bulk("s"), bulk("a")]).unwrap();

        let result = srandmember(&mut ctx, &[bulk("s")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_srandmember_single_member_set() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["only"]);

        let result = srandmember(&mut ctx, &[bulk("s")]).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from("only")));
    }

    #[test]
    fn test_srandmember_returns_correct_type() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["x", "y"]);

        let result = srandmember(&mut ctx, &[bulk("s")]).unwrap();
        assert!(matches!(result, RespValue::BulkString(_)));

        let result = srandmember(&mut ctx, &[bulk("s"), bulk("1")]).unwrap();
        assert!(matches!(result, RespValue::Array(_)));
    }

    #[test]
    fn test_srandmember_special_chars() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["hello world", "foo\tbar", "a\nb"]);

        let result = srandmember(&mut ctx, &[bulk("s"), bulk("3")]).unwrap();
        if let RespValue::Array(arr) = &result {
            assert_eq!(arr.len(), 3);
        } else {
            panic!("expected Array");
        }
    }

    #[test]
    fn test_srandmember_with_count_returns_array() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a", "b", "c"]);

        let result = srandmember(&mut ctx, &[bulk("s"), bulk("2")]).unwrap();
        if let RespValue::Array(arr) = &result {
            assert_eq!(arr.len(), 2);
            let mut uniq = HashSet::new();
            for item in arr {
                if let RespValue::BulkString(b) = item {
                    uniq.insert(b.clone());
                }
            }
            assert_eq!(uniq.len(), 2, "positive count should return unique members");
        } else {
            panic!("expected Array");
        }
    }

    #[test]
    fn test_srandmember_basic_returns_member() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["alpha", "beta", "gamma"]);

        for _ in 0..20 {
            let result = srandmember(&mut ctx, &[bulk("s")]).unwrap();
            if let RespValue::BulkString(b) = &result {
                let s = std::str::from_utf8(b).unwrap();
                assert!(
                    s == "alpha" || s == "beta" || s == "gamma",
                    "unexpected: {s}"
                );
            } else {
                panic!("expected BulkString");
            }
        }
    }

    // ===================================================================
    // SPOP — 14 new tests
    // ===================================================================

    #[test]
    fn test_spop_wrong_arity() {
        let mut ctx = make_ctx();
        let result = spop(&mut ctx, &[]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_spop_nonexistent_key() {
        let mut ctx = make_ctx();
        let result = spop(&mut ctx, &[bulk("nokey")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_spop_basic_single() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a", "b", "c"]);

        let result = spop(&mut ctx, &[bulk("s")]).unwrap();
        if let RespValue::BulkString(b) = &result {
            let s = std::str::from_utf8(b).unwrap();
            assert!(s == "a" || s == "b" || s == "c");
        } else {
            panic!("expected BulkString");
        }

        let card = scard(&mut ctx, &[bulk("s")]).unwrap();
        assert_eq!(card, RespValue::Integer(2));
    }

    #[test]
    fn test_spop_with_count_new() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a", "b", "c", "d"]);

        let result = spop(&mut ctx, &[bulk("s"), bulk("2")]).unwrap();
        if let RespValue::Array(arr) = &result {
            assert_eq!(arr.len(), 2);
        } else {
            panic!("expected Array");
        }

        let card = scard(&mut ctx, &[bulk("s")]).unwrap();
        assert_eq!(card, RespValue::Integer(2));
    }

    #[test]
    fn test_spop_count_zero_new() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a", "b"]);

        let result = spop(&mut ctx, &[bulk("s"), bulk("0")]).unwrap();
        assert_eq!(result, RespValue::Array(vec![]));

        let card = scard(&mut ctx, &[bulk("s")]).unwrap();
        assert_eq!(card, RespValue::Integer(2));
    }

    #[test]
    fn test_spop_count_larger_than_set() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a", "b"]);

        let result = spop(&mut ctx, &[bulk("s"), bulk("100")]).unwrap();
        if let RespValue::Array(arr) = &result {
            assert_eq!(arr.len(), 2);
        } else {
            panic!("expected Array");
        }

        let card = scard(&mut ctx, &[bulk("s")]).unwrap();
        assert_eq!(card, RespValue::Integer(0));
    }

    #[test]
    fn test_spop_count_negative_error() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a"]);

        let result = spop(&mut ctx, &[bulk("s"), bulk("-1")]);
        assert!(matches!(result, Err(CommandError::InvalidArgument(_))));
    }

    #[test]
    fn test_spop_wrong_type() {
        let mut ctx = make_ctx();
        insert_string(&mut ctx, "str", "hello");

        let result = spop(&mut ctx, &[bulk("str")]);
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    #[test]
    fn test_spop_expired_key() {
        use std::time::Instant;
        let mut ctx = make_ctx();

        let mut set = HashSet::new();
        set.insert(Bytes::from("a"));
        let mut entry = Entry::new(RedisValue::Set(set));
        entry.expires_at = Some(Instant::now() - std::time::Duration::from_secs(1));
        let db = ctx.store().database(ctx.selected_db());
        db.set(Bytes::from("expired_set"), entry);

        let result = spop(&mut ctx, &[bulk("expired_set")]).unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_spop_actually_removes() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a", "b", "c"]);

        let result = spop(&mut ctx, &[bulk("s")]).unwrap();
        if let RespValue::BulkString(popped) = &result {
            let is_member = sismember(
                &mut ctx,
                &[bulk("s"), RespValue::BulkString(popped.clone())],
            )
            .unwrap();
            assert_eq!(is_member, RespValue::Integer(0));
        } else {
            panic!("expected BulkString");
        }
    }

    #[test]
    fn test_spop_empty_after_pop_all() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["x", "y"]);

        spop(&mut ctx, &[bulk("s")]).unwrap();
        spop(&mut ctx, &[bulk("s")]).unwrap();

        let db = ctx.store().database(ctx.selected_db());
        assert!(!db.exists(b"s"));
    }

    #[test]
    fn test_spop_single_member() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["only"]);

        let result = spop(&mut ctx, &[bulk("s")]).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from("only")));

        let card = scard(&mut ctx, &[bulk("s")]).unwrap();
        assert_eq!(card, RespValue::Integer(0));
    }

    #[test]
    fn test_spop_returns_correct_type() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a", "b", "c"]);

        let result = spop(&mut ctx, &[bulk("s")]).unwrap();
        assert!(matches!(result, RespValue::BulkString(_)));

        let result = spop(&mut ctx, &[bulk("s"), bulk("1")]).unwrap();
        assert!(matches!(result, RespValue::Array(_)));
    }

    #[test]
    fn test_spop_special_chars() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["hello world", "foo\tbar"]);

        let result = spop(&mut ctx, &[bulk("s"), bulk("2")]).unwrap();
        if let RespValue::Array(arr) = &result {
            assert_eq!(arr.len(), 2);
        } else {
            panic!("expected Array");
        }
    }

    // ===================================================================
    // SMOVE — 17 new tests
    // ===================================================================

    #[test]
    fn test_smove_wrong_arity() {
        let mut ctx = make_ctx();
        let result = smove(&mut ctx, &[bulk("src"), bulk("dst")]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_smove_nonexistent_source() {
        let mut ctx = make_ctx();
        let result = smove(&mut ctx, &[bulk("nokey"), bulk("dst"), bulk("x")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_smove_nonexistent_member() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "src", &["a", "b"]);

        let result = smove(&mut ctx, &[bulk("src"), bulk("dst"), bulk("z")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_smove_basic_move() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "src", &["a", "b", "c"]);
        insert_set(&mut ctx, "dst", &["x"]);

        let result = smove(&mut ctx, &[bulk("src"), bulk("dst"), bulk("a")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let in_src = sismember(&mut ctx, &[bulk("src"), bulk("a")]).unwrap();
        assert_eq!(in_src, RespValue::Integer(0));
        let in_dst = sismember(&mut ctx, &[bulk("dst"), bulk("a")]).unwrap();
        assert_eq!(in_dst, RespValue::Integer(1));
    }

    #[test]
    fn test_smove_wrong_type_source() {
        let mut ctx = make_ctx();
        insert_string(&mut ctx, "str", "hello");
        insert_set(&mut ctx, "dst", &["x"]);

        let result = smove(&mut ctx, &[bulk("str"), bulk("dst"), bulk("a")]);
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    #[test]
    fn test_smove_wrong_type_dest() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "src", &["a"]);
        insert_string(&mut ctx, "str", "hello");

        let result = smove(&mut ctx, &[bulk("src"), bulk("str"), bulk("a")]);
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    #[test]
    fn test_smove_member_already_in_dest() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "src", &["a", "b"]);
        insert_set(&mut ctx, "dst", &["a", "x"]);

        let result = smove(&mut ctx, &[bulk("src"), bulk("dst"), bulk("a")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let in_src = sismember(&mut ctx, &[bulk("src"), bulk("a")]).unwrap();
        assert_eq!(in_src, RespValue::Integer(0));

        let in_dst = sismember(&mut ctx, &[bulk("dst"), bulk("a")]).unwrap();
        assert_eq!(in_dst, RespValue::Integer(1));

        let card = scard(&mut ctx, &[bulk("dst")]).unwrap();
        assert_eq!(card, RespValue::Integer(2));
    }

    #[test]
    fn test_smove_creates_dest_new() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "src", &["a", "b"]);

        let result = smove(&mut ctx, &[bulk("src"), bulk("newdst"), bulk("a")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let members = smembers(&mut ctx, &[bulk("newdst")]).unwrap();
        let sorted = sorted_bytes_from_array(&members);
        assert_eq!(sorted, vec![Bytes::from("a")]);
    }

    #[test]
    fn test_smove_removes_from_source() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "src", &["a", "b", "c"]);
        insert_set(&mut ctx, "dst", &["x"]);

        smove(&mut ctx, &[bulk("src"), bulk("dst"), bulk("b")]).unwrap();

        let members = smembers(&mut ctx, &[bulk("src")]).unwrap();
        let sorted = sorted_bytes_from_array(&members);
        assert_eq!(sorted, vec![Bytes::from("a"), Bytes::from("c")]);
    }

    #[test]
    fn test_smove_returns_1_on_success() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "src", &["a"]);
        insert_set(&mut ctx, "dst", &["b"]);

        let result = smove(&mut ctx, &[bulk("src"), bulk("dst"), bulk("a")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_smove_returns_0_on_failure() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "src", &["a"]);

        let result = smove(&mut ctx, &[bulk("src"), bulk("dst"), bulk("missing")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_smove_expired_source() {
        use std::time::Instant;
        let mut ctx = make_ctx();

        let mut set = HashSet::new();
        set.insert(Bytes::from("a"));
        let mut entry = Entry::new(RedisValue::Set(set));
        entry.expires_at = Some(Instant::now() - std::time::Duration::from_secs(1));
        let db = ctx.store().database(ctx.selected_db());
        db.set(Bytes::from("expired_src"), entry);

        let result = smove(&mut ctx, &[bulk("expired_src"), bulk("dst"), bulk("a")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_smove_empty_source_after_move() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "src", &["only"]);
        insert_set(&mut ctx, "dst", &["x"]);

        smove(&mut ctx, &[bulk("src"), bulk("dst"), bulk("only")]).unwrap();

        let db = ctx.store().database(ctx.selected_db());
        assert!(!db.exists(b"src"));
    }

    #[test]
    fn test_smove_special_chars() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "src", &["hello world", "foo"]);
        insert_set(&mut ctx, "dst", &["bar"]);

        let result = smove(&mut ctx, &[bulk("src"), bulk("dst"), bulk("hello world")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let in_dst = sismember(&mut ctx, &[bulk("dst"), bulk("hello world")]).unwrap();
        assert_eq!(in_dst, RespValue::Integer(1));
    }

    #[test]
    fn test_smove_nonexistent_dest_key() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "src", &["a", "b"]);

        let result = smove(&mut ctx, &[bulk("src"), bulk("fresh"), bulk("a")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let card = scard(&mut ctx, &[bulk("fresh")]).unwrap();
        assert_eq!(card, RespValue::Integer(1));
    }

    #[test]
    fn test_smove_same_source_dest() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a", "b", "c"]);

        let result = smove(&mut ctx, &[bulk("s"), bulk("s"), bulk("a")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let in_s = sismember(&mut ctx, &[bulk("s"), bulk("a")]).unwrap();
        assert_eq!(in_s, RespValue::Integer(1));

        let card = scard(&mut ctx, &[bulk("s")]).unwrap();
        assert_eq!(card, RespValue::Integer(3));
    }

    #[test]
    fn test_smove_binary_member() {
        let mut ctx = make_ctx();

        let db = ctx.store().database(ctx.selected_db());
        let mut src_set: HashSet<Bytes> = HashSet::new();
        src_set.insert(Bytes::from_static(b"\x00\x01\x02"));
        src_set.insert(Bytes::from("normal"));
        db.set(Bytes::from("src"), Entry::new(RedisValue::Set(src_set)));

        let member = RespValue::BulkString(Bytes::from_static(b"\x00\x01\x02"));
        let result = smove(&mut ctx, &[bulk("src"), bulk("dst"), member]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let card = scard(&mut ctx, &[bulk("src")]).unwrap();
        assert_eq!(card, RespValue::Integer(1));
    }

    // ===================================================================
    // SSCAN — 14 new tests
    // ===================================================================

    #[test]
    fn test_sscan_wrong_arity() {
        let mut ctx = make_ctx();
        let result = sscan(&mut ctx, &[bulk("key")]);
        assert!(matches!(result, Err(CommandError::WrongArity(_))));
    }

    #[test]
    fn test_sscan_nonexistent_key_new() {
        let mut ctx = make_ctx();
        let result = sscan(&mut ctx, &[bulk("nokey"), bulk("0")]).unwrap();
        if let RespValue::Array(outer) = &result {
            assert_eq!(outer.len(), 2);
            assert_eq!(outer[0], RespValue::BulkString(Bytes::from("0")));
            if let RespValue::Array(members) = &outer[1] {
                assert!(members.is_empty());
            } else {
                panic!("expected inner Array");
            }
        } else {
            panic!("expected outer Array");
        }
    }

    #[test]
    fn test_sscan_basic_new() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a", "b", "c"]);

        let result = sscan(&mut ctx, &[bulk("s"), bulk("0")]).unwrap();
        if let RespValue::Array(outer) = &result {
            assert_eq!(outer.len(), 2);
            assert_eq!(outer[0], RespValue::BulkString(Bytes::from("0")));
            if let RespValue::Array(members) = &outer[1] {
                assert_eq!(members.len(), 3);
            } else {
                panic!("expected inner Array");
            }
        } else {
            panic!("expected outer Array");
        }
    }

    #[test]
    fn test_sscan_full_iteration_new() {
        let mut ctx = make_ctx();
        insert_set(
            &mut ctx,
            "s",
            &["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"],
        );

        let mut all_found: HashSet<Bytes> = HashSet::new();
        let mut cursor: usize = 0;

        loop {
            let result = sscan(
                &mut ctx,
                &[
                    bulk("s"),
                    bulk(&cursor.to_string()),
                    bulk("COUNT"),
                    bulk("3"),
                ],
            )
            .unwrap();

            if let RespValue::Array(outer) = &result {
                if let RespValue::BulkString(cb) = &outer[0] {
                    cursor = std::str::from_utf8(cb).unwrap().parse().unwrap();
                }
                if let RespValue::Array(members) = &outer[1] {
                    for m in members {
                        if let RespValue::BulkString(b) = m {
                            all_found.insert(b.clone());
                        }
                    }
                }
            }

            if cursor == 0 {
                break;
            }
        }

        assert_eq!(all_found.len(), 10);
    }

    #[test]
    fn test_sscan_match_pattern() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["apple", "apricot", "banana", "avocado"]);

        let result = sscan(&mut ctx, &[bulk("s"), bulk("0"), bulk("MATCH"), bulk("a*")]).unwrap();

        if let RespValue::Array(outer) = &result {
            if let RespValue::Array(members) = &outer[1] {
                assert_eq!(members.len(), 3);
                for m in members {
                    if let RespValue::BulkString(b) = m {
                        assert!(b.starts_with(b"a"));
                    }
                }
            } else {
                panic!("expected inner Array");
            }
        } else {
            panic!("expected outer Array");
        }
    }

    #[test]
    fn test_sscan_count_option() {
        let mut ctx = make_ctx();
        let members: Vec<&str> = vec![
            "m00", "m01", "m02", "m03", "m04", "m05", "m06", "m07", "m08", "m09", "m10", "m11",
            "m12", "m13", "m14", "m15", "m16", "m17", "m18", "m19",
        ];
        insert_set(&mut ctx, "s", &members);

        let result = sscan(&mut ctx, &[bulk("s"), bulk("0"), bulk("COUNT"), bulk("5")]).unwrap();

        if let RespValue::Array(outer) = &result {
            if let RespValue::BulkString(cb) = &outer[0] {
                let c: usize = std::str::from_utf8(cb).unwrap().parse().unwrap();
                assert!(c > 0, "cursor should be non-zero for partial scan");
            }
            if let RespValue::Array(page) = &outer[1] {
                assert_eq!(page.len(), 5);
            } else {
                panic!("expected inner Array");
            }
        } else {
            panic!("expected outer Array");
        }
    }

    #[test]
    fn test_sscan_wrong_type_new() {
        let mut ctx = make_ctx();
        insert_string(&mut ctx, "str", "hello");

        let result = sscan(&mut ctx, &[bulk("str"), bulk("0")]);
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    #[test]
    fn test_sscan_expired_key() {
        use std::time::Instant;
        let mut ctx = make_ctx();

        let mut set = HashSet::new();
        set.insert(Bytes::from("a"));
        let mut entry = Entry::new(RedisValue::Set(set));
        entry.expires_at = Some(Instant::now() - std::time::Duration::from_secs(1));
        let db = ctx.store().database(ctx.selected_db());
        db.set(Bytes::from("expired_set"), entry);

        let result = sscan(&mut ctx, &[bulk("expired_set"), bulk("0")]).unwrap();
        if let RespValue::Array(outer) = &result {
            assert_eq!(outer[0], RespValue::BulkString(Bytes::from("0")));
            if let RespValue::Array(members) = &outer[1] {
                assert!(members.is_empty());
            }
        }
    }

    #[test]
    fn test_sscan_empty_set() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a"]);
        srem(&mut ctx, &[bulk("s"), bulk("a")]).unwrap();

        let result = sscan(&mut ctx, &[bulk("s"), bulk("0")]).unwrap();
        if let RespValue::Array(outer) = &result {
            assert_eq!(outer[0], RespValue::BulkString(Bytes::from("0")));
            if let RespValue::Array(members) = &outer[1] {
                assert!(members.is_empty());
            }
        }
    }

    #[test]
    fn test_sscan_returns_cursor_and_array() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["a", "b"]);

        let result = sscan(&mut ctx, &[bulk("s"), bulk("0")]).unwrap();
        if let RespValue::Array(outer) = &result {
            assert_eq!(outer.len(), 2);
            assert!(matches!(&outer[0], RespValue::BulkString(_)));
            assert!(matches!(&outer[1], RespValue::Array(_)));
        } else {
            panic!("expected outer Array");
        }
    }

    #[test]
    fn test_sscan_match_no_results() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["apple", "banana", "cherry"]);

        let result = sscan(&mut ctx, &[bulk("s"), bulk("0"), bulk("MATCH"), bulk("z*")]).unwrap();

        if let RespValue::Array(outer) = &result {
            assert_eq!(outer[0], RespValue::BulkString(Bytes::from("0")));
            if let RespValue::Array(members) = &outer[1] {
                assert!(members.is_empty());
            }
        }
    }

    #[test]
    fn test_sscan_single_member() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["only"]);

        let result = sscan(&mut ctx, &[bulk("s"), bulk("0")]).unwrap();
        if let RespValue::Array(outer) = &result {
            assert_eq!(outer[0], RespValue::BulkString(Bytes::from("0")));
            if let RespValue::Array(members) = &outer[1] {
                assert_eq!(members.len(), 1);
                assert_eq!(members[0], RespValue::BulkString(Bytes::from("only")));
            }
        }
    }

    #[test]
    fn test_sscan_special_chars() {
        let mut ctx = make_ctx();
        insert_set(&mut ctx, "s", &["hello world", "foo\tbar", "a\nb"]);

        let result = sscan(&mut ctx, &[bulk("s"), bulk("0")]).unwrap();
        if let RespValue::Array(outer) = &result {
            assert_eq!(outer[0], RespValue::BulkString(Bytes::from("0")));
            if let RespValue::Array(members) = &outer[1] {
                assert_eq!(members.len(), 3);
            }
        }
    }

    #[test]
    fn test_sscan_large_set() {
        let mut ctx = make_ctx();
        let member_strs: Vec<String> = (0..100).map(|i| format!("member_{:03}", i)).collect();
        let member_refs: Vec<&str> = member_strs.iter().map(|s| s.as_str()).collect();
        insert_set(&mut ctx, "big", &member_refs);

        let mut all_found: HashSet<Bytes> = HashSet::new();
        let mut cursor: usize = 0;

        loop {
            let result = sscan(
                &mut ctx,
                &[
                    bulk("big"),
                    bulk(&cursor.to_string()),
                    bulk("COUNT"),
                    bulk("10"),
                ],
            )
            .unwrap();

            if let RespValue::Array(outer) = &result {
                if let RespValue::BulkString(cb) = &outer[0] {
                    cursor = std::str::from_utf8(cb).unwrap().parse().unwrap();
                }
                if let RespValue::Array(members) = &outer[1] {
                    for m in members {
                        if let RespValue::BulkString(b) = m {
                            all_found.insert(b.clone());
                        }
                    }
                }
            }

            if cursor == 0 {
                break;
            }
        }

        assert_eq!(all_found.len(), 100);
    }
}

//! Sorted Set commands: ZADD, ZREM, ZSCORE, ZRANK, ZREVRANK, ZCARD, ZCOUNT,
//! ZRANGE, ZREVRANGE, ZRANGEBYSCORE, ZREVRANGEBYSCORE, ZINCRBY, ZPOPMIN,
//! ZPOPMAX, ZRANGESTORE, ZUNIONSTORE, ZINTERSTORE, ZSCAN, ZMSCORE,
//! ZRANDMEMBER, ZLEXCOUNT, ZRANGEBYLEX, ZREVRANGEBYLEX, ZREMRANGEBYRANK,
//! ZREMRANGEBYSCORE, ZREMRANGEBYLEX, ZMPOP, ZUNION, ZINTER, ZDIFF, ZDIFFSTORE

use crate::{CommandContext, CommandError, CommandResult};
use bytes::Bytes;
use ferris_core::{Entry, RedisValue};
use ferris_protocol::RespValue;
use ordered_float::OrderedFloat;
use std::collections::{BTreeMap, HashMap};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Parse an integer from a `RespValue`.
fn parse_int(arg: &RespValue) -> Result<i64, CommandError> {
    let s = arg.as_str().ok_or(CommandError::NotAnInteger)?;
    s.parse().map_err(|_| CommandError::NotAnInteger)
}

/// Parse a float from a `RespValue`, supporting `+inf`, `-inf`, `inf`.
fn parse_float(arg: &RespValue) -> Result<f64, CommandError> {
    let s = arg.as_str().ok_or(CommandError::NotAFloat)?;
    match s {
        "+inf" | "inf" => Ok(f64::INFINITY),
        "-inf" => Ok(f64::NEG_INFINITY),
        _ => s.parse().map_err(|_| CommandError::NotAFloat),
    }
}

/// Extract `Bytes` from a `RespValue`.
fn get_bytes(arg: &RespValue) -> Result<Bytes, CommandError> {
    arg.as_bytes()
        .cloned()
        .or_else(|| arg.as_str().map(|s| Bytes::from(s.to_owned())))
        .ok_or_else(|| CommandError::InvalidArgument("invalid argument".to_string()))
}

/// Format a float score the way Redis does.
fn format_score(score: f64) -> String {
    if score.is_infinite() {
        if score.is_sign_positive() {
            "inf".to_string()
        } else {
            "-inf".to_string()
        }
    } else if score.fract() == 0.0 && score.abs() < (i64::MAX as f64) {
        #[allow(clippy::cast_possible_truncation)]
        let i = score as i64;
        i.to_string()
    } else {
        format!("{score}")
    }
}

// ---------------------------------------------------------------------------
// Sorted-set mutation helpers (dual-index bookkeeping)
// ---------------------------------------------------------------------------

/// Insert / update a member in the sorted set. Returns `true` if the member
/// was newly added (did not exist before).
fn zset_add(
    scores: &mut HashMap<Bytes, f64>,
    members: &mut BTreeMap<(OrderedFloat<f64>, Bytes), ()>,
    member: Bytes,
    score: f64,
) -> bool {
    if let Some(&old_score) = scores.get(&member) {
        members.remove(&(OrderedFloat(old_score), member.clone()));
        scores.insert(member.clone(), score);
        members.insert((OrderedFloat(score), member), ());
        false
    } else {
        scores.insert(member.clone(), score);
        members.insert((OrderedFloat(score), member), ());
        true
    }
}

/// Remove a member from the sorted set. Returns `true` if the member existed.
fn zset_remove(
    scores: &mut HashMap<Bytes, f64>,
    members: &mut BTreeMap<(OrderedFloat<f64>, Bytes), ()>,
    member: &[u8],
) -> bool {
    if let Some(score) = scores.remove(member) {
        members.remove(&(OrderedFloat(score), Bytes::copy_from_slice(member)));
        true
    } else {
        false
    }
}

// ---------------------------------------------------------------------------
// Score-range helpers
// ---------------------------------------------------------------------------

/// A bound for score-based range queries.
#[derive(Debug, Clone, Copy)]
struct ScoreBound {
    value: f64,
    exclusive: bool,
}

impl ScoreBound {
    fn includes(self, score: f64) -> bool {
        if self.exclusive {
            score > self.value
        } else {
            score >= self.value
        }
    }

    fn includes_upper(self, score: f64) -> bool {
        if self.exclusive {
            score < self.value
        } else {
            score <= self.value
        }
    }
}

fn parse_score_bound(arg: &RespValue) -> Result<ScoreBound, CommandError> {
    let s = arg.as_str().ok_or(CommandError::NotAFloat)?;
    if let Some(rest) = s.strip_prefix('(') {
        let value = match rest {
            "+inf" | "inf" => f64::INFINITY,
            "-inf" => f64::NEG_INFINITY,
            _ => rest.parse::<f64>().map_err(|_| CommandError::NotAFloat)?,
        };
        Ok(ScoreBound {
            value,
            exclusive: true,
        })
    } else {
        let value = match s {
            "+inf" | "inf" => f64::INFINITY,
            "-inf" => f64::NEG_INFINITY,
            _ => s.parse::<f64>().map_err(|_| CommandError::NotAFloat)?,
        };
        Ok(ScoreBound {
            value,
            exclusive: false,
        })
    }
}

// ---------------------------------------------------------------------------
// Lex-range helpers
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum LexBound {
    NegInf,
    PosInf,
    Inclusive(Bytes),
    Exclusive(Bytes),
}

fn parse_lex_bound(arg: &RespValue) -> Result<LexBound, CommandError> {
    let s = arg.as_str().ok_or_else(|| {
        CommandError::InvalidArgument("min or max is not valid string range item".to_string())
    })?;
    match s {
        "-" => Ok(LexBound::NegInf),
        "+" => Ok(LexBound::PosInf),
        _ if s.starts_with('[') => Ok(LexBound::Inclusive(Bytes::from(s[1..].to_owned()))),
        _ if s.starts_with('(') => Ok(LexBound::Exclusive(Bytes::from(s[1..].to_owned()))),
        _ => Err(CommandError::InvalidArgument(
            "min or max is not valid string range item".to_string(),
        )),
    }
}

/// Check whether `member` satisfies the lower lex bound.
fn lex_gte(member: &Bytes, bound: &LexBound) -> bool {
    match bound {
        LexBound::NegInf => true,
        LexBound::PosInf => false,
        LexBound::Inclusive(b) => member.as_ref() >= b.as_ref(),
        LexBound::Exclusive(b) => member.as_ref() > b.as_ref(),
    }
}

/// Check whether `member` satisfies the upper lex bound.
fn lex_lte(member: &Bytes, bound: &LexBound) -> bool {
    match bound {
        LexBound::NegInf => false,
        LexBound::PosInf => true,
        LexBound::Inclusive(b) => member.as_ref() <= b.as_ref(),
        LexBound::Exclusive(b) => member.as_ref() < b.as_ref(),
    }
}

// ---------------------------------------------------------------------------
// Aggregate helpers (for ZUNIONSTORE / ZINTERSTORE)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
enum Aggregate {
    Sum,
    Min,
    Max,
}

fn aggregate_scores(agg: Aggregate, a: f64, b: f64) -> f64 {
    match agg {
        Aggregate::Sum => a + b,
        Aggregate::Min => a.min(b),
        Aggregate::Max => a.max(b),
    }
}

/// Parse the shared tail of ZUNIONSTORE / ZINTERSTORE:
///   `[WEIGHTS w ...] [AGGREGATE SUM|MIN|MAX]`
fn parse_store_options(
    args: &[RespValue],
    start: usize,
    numkeys: usize,
) -> Result<(Vec<f64>, Aggregate), CommandError> {
    let mut weights: Vec<f64> = vec![1.0; numkeys];
    let mut agg = Aggregate::Sum;

    let mut i = start;
    while i < args.len() {
        let opt = args[i]
            .as_str()
            .map(|s| s.to_uppercase())
            .unwrap_or_default();
        match opt.as_str() {
            "WEIGHTS" => {
                for w in weights.iter_mut().take(numkeys) {
                    i += 1;
                    *w = parse_float(args.get(i).ok_or(CommandError::SyntaxError)?)?;
                }
            }
            "AGGREGATE" => {
                i += 1;
                let a = args
                    .get(i)
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_uppercase())
                    .ok_or(CommandError::SyntaxError)?;
                agg = match a.as_str() {
                    "SUM" => Aggregate::Sum,
                    "MIN" => Aggregate::Min,
                    "MAX" => Aggregate::Max,
                    _ => return Err(CommandError::SyntaxError),
                };
            }
            _ => return Err(CommandError::SyntaxError),
        }
        i += 1;
    }

    Ok((weights, agg))
}

/// Read a sorted set from the database. Returns empty maps if key doesn't
/// exist. Returns `Err(WrongType)` if the key holds a different type.
fn read_zset(
    ctx: &CommandContext,
    key: &[u8],
) -> Result<
    (
        HashMap<Bytes, f64>,
        BTreeMap<(OrderedFloat<f64>, Bytes), ()>,
    ),
    CommandError,
> {
    let db = ctx.store().database(ctx.selected_db());
    match db.get(key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(key);
                return Ok((HashMap::new(), BTreeMap::new()));
            }
            match entry.value {
                RedisValue::SortedSet { scores, members } => Ok((scores, members)),
                _ => Err(CommandError::WrongType),
            }
        }
        None => Ok((HashMap::new(), BTreeMap::new())),
    }
}

/// Persist a sorted set back to the database, deleting the key if empty.
fn write_zset(
    ctx: &CommandContext,
    key: Bytes,
    scores: HashMap<Bytes, f64>,
    members: BTreeMap<(OrderedFloat<f64>, Bytes), ()>,
) {
    let db = ctx.store().database(ctx.selected_db());
    if scores.is_empty() {
        db.delete(&key);
    } else {
        db.set(key, Entry::new(RedisValue::SortedSet { scores, members }));
    }
}

// ===================================================================
// COMMANDS
// ===================================================================

/// ZADD key [NX|XX] [GT|LT] [CH] score member [score member ...]
///
/// Add one or more members to a sorted set, or update the score if it
/// already exists.
///
/// Time complexity: O(log(N)) for each item added
pub fn zadd(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongArity("ZADD".to_string()));
    }

    let key = get_bytes(&args[0])?;

    // Parse flags
    let mut nx = false;
    let mut xx = false;
    let mut gt = false;
    let mut lt = false;
    let mut ch = false;
    let mut i = 1;

    while i < args.len() {
        let maybe_flag = args[i].as_str().map(|s| s.to_uppercase());
        match maybe_flag.as_deref() {
            Some("NX") => {
                nx = true;
                i += 1;
            }
            Some("XX") => {
                xx = true;
                i += 1;
            }
            Some("GT") => {
                gt = true;
                i += 1;
            }
            Some("LT") => {
                lt = true;
                i += 1;
            }
            Some("CH") => {
                ch = true;
                i += 1;
            }
            _ => break,
        }
    }

    // NX and XX are mutually exclusive
    if nx && xx {
        return Err(CommandError::InvalidArgument(
            "XX and NX options at the same time are not compatible".to_string(),
        ));
    }
    // NX and GT/LT are mutually exclusive
    if nx && (gt || lt) {
        return Err(CommandError::InvalidArgument(
            "GT, LT, and NX options at the same time are not compatible".to_string(),
        ));
    }

    // Remaining args must be score-member pairs
    let pair_args = &args[i..];
    if pair_args.is_empty() || pair_args.len() % 2 != 0 {
        return Err(CommandError::WrongArity("ZADD".to_string()));
    }

    let db = ctx.store().database(ctx.selected_db());

    // Fetch existing sorted set (or create new)
    let (mut scores, mut members) = match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                (HashMap::new(), BTreeMap::new())
            } else {
                match entry.value {
                    RedisValue::SortedSet { scores, members } => (scores, members),
                    _ => return Err(CommandError::WrongType),
                }
            }
        }
        None => (HashMap::new(), BTreeMap::new()),
    };

    let mut added: i64 = 0;
    let mut changed: i64 = 0;

    for pair in pair_args.chunks(2) {
        let score = parse_float(&pair[0])?;
        if score.is_nan() {
            return Err(CommandError::NotAFloat);
        }
        let member = get_bytes(&pair[1])?;

        let exists = scores.contains_key(&member);

        if nx && exists {
            continue;
        }
        if xx && !exists {
            continue;
        }

        if exists {
            let old_score = scores[&member];
            let should_update = if gt && lt {
                false
            } else if gt {
                score > old_score
            } else if lt {
                score < old_score
            } else {
                true
            };

            if should_update && (old_score - score).abs() > f64::EPSILON {
                zset_add(&mut scores, &mut members, member, score);
                changed += 1;
            }
        } else {
            zset_add(&mut scores, &mut members, member, score);
            added += 1;
            changed += 1;
        }
    }

    write_zset(ctx, key.clone(), scores, members);

    // Propagate to AOF and replication
    if added > 0 || changed > 0 {
        ctx.propagate_args(args);
    }

    // Notify any blocking waiters (BZPOPMIN, BZPOPMAX, etc.)
    if added > 0 || changed > 0 {
        ctx.blocking_registry().notify_key(ctx.selected_db(), &key);
    }

    if ch {
        Ok(RespValue::Integer(changed))
    } else {
        Ok(RespValue::Integer(added))
    }
}

/// ZREM key member [member ...]
///
/// Remove one or more members from a sorted set.
///
/// Time complexity: O(M*log(N))
pub fn zrem(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("ZREM".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let db = ctx.store().database(ctx.selected_db());

    let (mut scores, mut members) = match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                return Ok(RespValue::Integer(0));
            }
            match entry.value {
                RedisValue::SortedSet { scores, members } => (scores, members),
                _ => return Err(CommandError::WrongType),
            }
        }
        None => return Ok(RespValue::Integer(0)),
    };

    let mut removed: i64 = 0;
    for arg in &args[1..] {
        let member = get_bytes(arg)?;
        if zset_remove(&mut scores, &mut members, &member) {
            removed += 1;
        }
    }

    write_zset(ctx, key.clone(), scores, members);

    if removed > 0 {
        // Propagate to AOF and replication
        ctx.propagate_args(args);
    }

    Ok(RespValue::Integer(removed))
}

/// ZSCORE key member
///
/// Return the score of member in the sorted set.
///
/// Time complexity: O(1)
pub fn zscore(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("ZSCORE".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let member = get_bytes(&args[1])?;
    let db = ctx.store().database(ctx.selected_db());

    match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                return Ok(RespValue::Null);
            }
            match &entry.value {
                RedisValue::SortedSet { scores, .. } => match scores.get(&member) {
                    Some(&s) => Ok(RespValue::BulkString(Bytes::from(format_score(s)))),
                    None => Ok(RespValue::Null),
                },
                _ => Err(CommandError::WrongType),
            }
        }
        None => Ok(RespValue::Null),
    }
}

/// ZRANK key member [WITHSCORE]
///
/// Return the rank of member in the sorted set (0-based, ascending score).
/// When WITHSCORE is given, return an array of [rank, score] instead.
///
/// Time complexity: O(log(N))
pub fn zrank(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("ZRANK".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let member = get_bytes(&args[1])?;

    let withscore = if args.len() >= 3 {
        let opt = args[2].as_str().ok_or(CommandError::SyntaxError)?;
        if opt.eq_ignore_ascii_case("WITHSCORE") {
            true
        } else {
            return Err(CommandError::SyntaxError);
        }
    } else {
        false
    };

    let db = ctx.store().database(ctx.selected_db());

    match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                return Ok(RespValue::Null);
            }
            match &entry.value {
                RedisValue::SortedSet {
                    scores, members, ..
                } => {
                    let score = match scores.get(&member) {
                        Some(&s) => s,
                        None => return Ok(RespValue::Null),
                    };
                    let target = (OrderedFloat(score), member);
                    let rank = members.keys().position(|k| k == &target).ok_or_else(|| {
                        CommandError::Internal("sorted set index inconsistency".to_string())
                    })?;
                    if withscore {
                        Ok(RespValue::Array(vec![
                            RespValue::Integer(rank as i64),
                            RespValue::BulkString(Bytes::from(format_score(score))),
                        ]))
                    } else {
                        Ok(RespValue::Integer(rank as i64))
                    }
                }
                _ => Err(CommandError::WrongType),
            }
        }
        None => Ok(RespValue::Null),
    }
}

/// ZREVRANK key member [WITHSCORE]
///
/// Return the rank of member in the sorted set with scores ordered from
/// high to low. When WITHSCORE is given, return an array of [rank, score].
///
/// Time complexity: O(log(N))
pub fn zrevrank(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("ZREVRANK".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let member = get_bytes(&args[1])?;

    let withscore = if args.len() >= 3 {
        let opt = args[2].as_str().ok_or(CommandError::SyntaxError)?;
        if opt.eq_ignore_ascii_case("WITHSCORE") {
            true
        } else {
            return Err(CommandError::SyntaxError);
        }
    } else {
        false
    };

    let db = ctx.store().database(ctx.selected_db());

    match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                return Ok(RespValue::Null);
            }
            match &entry.value {
                RedisValue::SortedSet {
                    scores, members, ..
                } => {
                    let score = match scores.get(&member) {
                        Some(&s) => s,
                        None => return Ok(RespValue::Null),
                    };
                    let target = (OrderedFloat(score), member);
                    let rank = members.keys().position(|k| k == &target).ok_or_else(|| {
                        CommandError::Internal("sorted set index inconsistency".to_string())
                    })?;
                    let len = members.len();
                    let rev_rank = len - 1 - rank;
                    if withscore {
                        Ok(RespValue::Array(vec![
                            RespValue::Integer(rev_rank as i64),
                            RespValue::BulkString(Bytes::from(format_score(score))),
                        ]))
                    } else {
                        Ok(RespValue::Integer(rev_rank as i64))
                    }
                }
                _ => Err(CommandError::WrongType),
            }
        }
        None => Ok(RespValue::Null),
    }
}

/// ZCARD key
///
/// Return the number of elements in the sorted set.
///
/// Time complexity: O(1)
pub fn zcard(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("ZCARD".to_string()));
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
                RedisValue::SortedSet { scores, .. } => Ok(RespValue::Integer(scores.len() as i64)),
                _ => Err(CommandError::WrongType),
            }
        }
        None => Ok(RespValue::Integer(0)),
    }
}

/// ZCOUNT key min max
///
/// Count the members in a sorted set with scores within the given values.
///
/// Time complexity: O(log(N))
pub fn zcount(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongArity("ZCOUNT".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let min = parse_score_bound(&args[1])?;
    let max = parse_score_bound(&args[2])?;

    let db = ctx.store().database(ctx.selected_db());

    match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                return Ok(RespValue::Integer(0));
            }
            match &entry.value {
                RedisValue::SortedSet { members, .. } => {
                    let count = members
                        .keys()
                        .filter(|(s, _)| min.includes(s.0) && max.includes_upper(s.0))
                        .count();
                    Ok(RespValue::Integer(count as i64))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        None => Ok(RespValue::Integer(0)),
    }
}

/// ZRANGE key min max [BYSCORE|BYLEX] [REV] [LIMIT offset count] [WITHSCORES]
///
/// Versatile range query (Redis >= 6.2 unified ZRANGE).
///
/// Time complexity: O(log(N)+M)
pub fn zrange(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongArity("ZRANGE".to_string()));
    }

    let key = get_bytes(&args[0])?;

    // Detect optional flags
    let mut byscore = false;
    let mut bylex = false;
    let mut rev = false;
    let mut withscores = false;
    let mut limit_offset: Option<i64> = None;
    let mut limit_count: Option<i64> = None;

    let mut i = 3;
    while i < args.len() {
        let opt = args[i].as_str().map(|s| s.to_uppercase());
        match opt.as_deref() {
            Some("BYSCORE") => byscore = true,
            Some("BYLEX") => bylex = true,
            Some("REV") => rev = true,
            Some("WITHSCORES") => withscores = true,
            Some("LIMIT") => {
                i += 1;
                limit_offset = Some(parse_int(args.get(i).ok_or(CommandError::SyntaxError)?)?);
                i += 1;
                limit_count = Some(parse_int(args.get(i).ok_or(CommandError::SyntaxError)?)?);
            }
            _ => return Err(CommandError::SyntaxError),
        }
        i += 1;
    }

    let db = ctx.store().database(ctx.selected_db());

    let entry_opt = db.get(&key);
    let entry = match &entry_opt {
        Some(e) => {
            if e.is_expired() {
                db.delete(&key);
                return Ok(RespValue::Array(vec![]));
            }
            e
        }
        None => return Ok(RespValue::Array(vec![])),
    };

    let (_scores_map, members_map) = match &entry.value {
        RedisValue::SortedSet { scores, members } => (scores, members),
        _ => return Err(CommandError::WrongType),
    };

    // Collect results based on mode
    let results: Vec<(Bytes, f64)> = if byscore {
        let (min_arg, max_arg) = if rev {
            (&args[2], &args[1])
        } else {
            (&args[1], &args[2])
        };
        let min_bound = parse_score_bound(min_arg)?;
        let max_bound = parse_score_bound(max_arg)?;

        let all: Vec<(Bytes, f64)> = if rev {
            members_map
                .keys()
                .rev()
                .filter(|(s, _)| min_bound.includes(s.0) && max_bound.includes_upper(s.0))
                .map(|(s, m)| (m.clone(), s.0))
                .collect()
        } else {
            members_map
                .keys()
                .filter(|(s, _)| min_bound.includes(s.0) && max_bound.includes_upper(s.0))
                .map(|(s, m)| (m.clone(), s.0))
                .collect()
        };

        apply_limit(all, limit_offset, limit_count)
    } else if bylex {
        let (min_arg, max_arg) = if rev {
            (&args[2], &args[1])
        } else {
            (&args[1], &args[2])
        };
        let min_lex = parse_lex_bound(min_arg)?;
        let max_lex = parse_lex_bound(max_arg)?;

        let all: Vec<(Bytes, f64)> = if rev {
            members_map
                .keys()
                .rev()
                .filter(|(_, m)| lex_gte(m, &min_lex) && lex_lte(m, &max_lex))
                .map(|(s, m)| (m.clone(), s.0))
                .collect()
        } else {
            members_map
                .keys()
                .filter(|(_, m)| lex_gte(m, &min_lex) && lex_lte(m, &max_lex))
                .map(|(s, m)| (m.clone(), s.0))
                .collect()
        };

        apply_limit(all, limit_offset, limit_count)
    } else {
        // Default: by rank (integer indices)
        let start = parse_int(&args[1])?;
        let stop = parse_int(&args[2])?;
        let len = members_map.len() as i64;

        let start = normalize_index(start, len);
        let stop = normalize_index(stop, len);

        if start > stop || start >= len {
            return Ok(RespValue::Array(vec![]));
        }

        let start = start.max(0) as usize;
        let stop = stop.min(len - 1) as usize;

        if rev {
            members_map
                .keys()
                .rev()
                .skip(start)
                .take(stop - start + 1)
                .map(|(s, m)| (m.clone(), s.0))
                .collect()
        } else {
            members_map
                .keys()
                .skip(start)
                .take(stop - start + 1)
                .map(|(s, m)| (m.clone(), s.0))
                .collect()
        }
    };

    Ok(build_array_response(&results, withscores))
}

fn normalize_index(idx: i64, len: i64) -> i64 {
    if idx < 0 {
        (len + idx).max(0)
    } else {
        idx
    }
}

fn apply_limit(v: Vec<(Bytes, f64)>, offset: Option<i64>, count: Option<i64>) -> Vec<(Bytes, f64)> {
    let off = offset.unwrap_or(0).max(0) as usize;
    match count {
        Some(c) if c >= 0 => v.into_iter().skip(off).take(c as usize).collect(),
        Some(_) => v.into_iter().skip(off).collect(),
        None => v.into_iter().skip(off).collect(),
    }
}

fn build_array_response(items: &[(Bytes, f64)], withscores: bool) -> RespValue {
    let mut result = Vec::new();
    for (member, score) in items {
        result.push(RespValue::BulkString(member.clone()));
        if withscores {
            result.push(RespValue::BulkString(Bytes::from(format_score(*score))));
        }
    }
    RespValue::Array(result)
}

/// ZREVRANGE key start stop [WITHSCORES]
///
/// Return a range of members in a sorted set, by index, with scores
/// ordered from high to low (legacy).
///
/// Time complexity: O(log(N)+M)
pub fn zrevrange(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongArity("ZREVRANGE".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let start = parse_int(&args[1])?;
    let stop = parse_int(&args[2])?;
    let withscores = args
        .get(3)
        .and_then(|v| v.as_str())
        .map(|s| s.eq_ignore_ascii_case("WITHSCORES"))
        .unwrap_or(false);

    let db = ctx.store().database(ctx.selected_db());

    match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                return Ok(RespValue::Array(vec![]));
            }
            match &entry.value {
                RedisValue::SortedSet { members, .. } => {
                    let len = members.len() as i64;
                    let s = normalize_index(start, len);
                    let e = normalize_index(stop, len);

                    if s > e || s >= len {
                        return Ok(RespValue::Array(vec![]));
                    }
                    let s = s.max(0) as usize;
                    let e = e.min(len - 1) as usize;

                    let results: Vec<(Bytes, f64)> = members
                        .keys()
                        .rev()
                        .skip(s)
                        .take(e - s + 1)
                        .map(|(sc, m)| (m.clone(), sc.0))
                        .collect();

                    Ok(build_array_response(&results, withscores))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        None => Ok(RespValue::Array(vec![])),
    }
}

/// ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
///
/// Return a range of members in a sorted set, by score.
///
/// Time complexity: O(log(N)+M)
pub fn zrangebyscore(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongArity("ZRANGEBYSCORE".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let min = parse_score_bound(&args[1])?;
    let max = parse_score_bound(&args[2])?;

    let mut withscores = false;
    let mut limit_offset: Option<i64> = None;
    let mut limit_count: Option<i64> = None;

    let mut i = 3;
    while i < args.len() {
        let opt = args[i].as_str().map(|s| s.to_uppercase());
        match opt.as_deref() {
            Some("WITHSCORES") => withscores = true,
            Some("LIMIT") => {
                i += 1;
                limit_offset = Some(parse_int(args.get(i).ok_or(CommandError::SyntaxError)?)?);
                i += 1;
                limit_count = Some(parse_int(args.get(i).ok_or(CommandError::SyntaxError)?)?);
            }
            _ => return Err(CommandError::SyntaxError),
        }
        i += 1;
    }

    let db = ctx.store().database(ctx.selected_db());

    match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                return Ok(RespValue::Array(vec![]));
            }
            match &entry.value {
                RedisValue::SortedSet { members, .. } => {
                    let all: Vec<(Bytes, f64)> = members
                        .keys()
                        .filter(|(s, _)| min.includes(s.0) && max.includes_upper(s.0))
                        .map(|(s, m)| (m.clone(), s.0))
                        .collect();
                    let results = apply_limit(all, limit_offset, limit_count);
                    Ok(build_array_response(&results, withscores))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        None => Ok(RespValue::Array(vec![])),
    }
}

/// ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
///
/// Return a range of members in a sorted set, by score, with scores
/// ordered from high to low.
///
/// Time complexity: O(log(N)+M)
pub fn zrevrangebyscore(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongArity("ZREVRANGEBYSCORE".to_string()));
    }

    let key = get_bytes(&args[0])?;
    // Note: for ZREVRANGEBYSCORE the first bound is max, second is min
    let max = parse_score_bound(&args[1])?;
    let min = parse_score_bound(&args[2])?;

    let mut withscores = false;
    let mut limit_offset: Option<i64> = None;
    let mut limit_count: Option<i64> = None;

    let mut i = 3;
    while i < args.len() {
        let opt = args[i].as_str().map(|s| s.to_uppercase());
        match opt.as_deref() {
            Some("WITHSCORES") => withscores = true,
            Some("LIMIT") => {
                i += 1;
                limit_offset = Some(parse_int(args.get(i).ok_or(CommandError::SyntaxError)?)?);
                i += 1;
                limit_count = Some(parse_int(args.get(i).ok_or(CommandError::SyntaxError)?)?);
            }
            _ => return Err(CommandError::SyntaxError),
        }
        i += 1;
    }

    let db = ctx.store().database(ctx.selected_db());

    match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                return Ok(RespValue::Array(vec![]));
            }
            match &entry.value {
                RedisValue::SortedSet { members, .. } => {
                    let all: Vec<(Bytes, f64)> = members
                        .keys()
                        .rev()
                        .filter(|(s, _)| min.includes(s.0) && max.includes_upper(s.0))
                        .map(|(s, m)| (m.clone(), s.0))
                        .collect();
                    let results = apply_limit(all, limit_offset, limit_count);
                    Ok(build_array_response(&results, withscores))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        None => Ok(RespValue::Array(vec![])),
    }
}

/// ZINCRBY key increment member
///
/// Increment the score of a member in a sorted set.
///
/// Time complexity: O(log(N))
pub fn zincrby(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongArity("ZINCRBY".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let increment = parse_float(&args[1])?;
    let member = get_bytes(&args[2])?;

    if increment.is_nan() {
        return Err(CommandError::NotAFloat);
    }

    let db = ctx.store().database(ctx.selected_db());

    let (mut scores, mut members) = match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                (HashMap::new(), BTreeMap::new())
            } else {
                match entry.value {
                    RedisValue::SortedSet { scores, members } => (scores, members),
                    _ => return Err(CommandError::WrongType),
                }
            }
        }
        None => (HashMap::new(), BTreeMap::new()),
    };

    let new_score = scores.get(&member).copied().unwrap_or(0.0) + increment;
    zset_add(&mut scores, &mut members, member.clone(), new_score);

    write_zset(ctx, key.clone(), scores, members);

    // Propagate to AOF and replication
    ctx.propagate_args(args);

    Ok(RespValue::BulkString(Bytes::from(format_score(new_score))))
}

/// ZPOPMIN key [count]
///
/// Remove and return members with the lowest scores.
///
/// Time complexity: O(log(N)*M)
pub fn zpopmin(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("ZPOPMIN".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let count = if args.len() > 1 {
        parse_int(&args[1])?.max(0) as usize
    } else {
        1
    };

    let db = ctx.store().database(ctx.selected_db());

    let (mut scores, mut members) = match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                return Ok(RespValue::Array(vec![]));
            }
            match entry.value {
                RedisValue::SortedSet { scores, members } => (scores, members),
                _ => return Err(CommandError::WrongType),
            }
        }
        None => return Ok(RespValue::Array(vec![])),
    };

    let mut result = Vec::new();
    for _ in 0..count {
        let first = members.keys().next().cloned();
        match first {
            Some((score, member)) => {
                members.remove(&(score, member.clone()));
                scores.remove(&member);
                result.push(RespValue::BulkString(member));
                result.push(RespValue::BulkString(Bytes::from(format_score(score.0))));
            }
            None => break,
        }
    }

    write_zset(ctx, key.clone(), scores, members);

    if !result.is_empty() {
        // Propagate to AOF and replication
        ctx.propagate_args(args);
    }

    Ok(RespValue::Array(result))
}

/// ZPOPMAX key [count]
///
/// Remove and return members with the highest scores.
///
/// Time complexity: O(log(N)*M)
pub fn zpopmax(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("ZPOPMAX".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let count = if args.len() > 1 {
        parse_int(&args[1])?.max(0) as usize
    } else {
        1
    };

    let db = ctx.store().database(ctx.selected_db());

    let (mut scores, mut members) = match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                return Ok(RespValue::Array(vec![]));
            }
            match entry.value {
                RedisValue::SortedSet { scores, members } => (scores, members),
                _ => return Err(CommandError::WrongType),
            }
        }
        None => return Ok(RespValue::Array(vec![])),
    };

    let mut result = Vec::new();
    for _ in 0..count {
        let last = members.keys().next_back().cloned();
        match last {
            Some((score, member)) => {
                members.remove(&(score, member.clone()));
                scores.remove(&member);
                result.push(RespValue::BulkString(member));
                result.push(RespValue::BulkString(Bytes::from(format_score(score.0))));
            }
            None => break,
        }
    }

    write_zset(ctx, key.clone(), scores, members);

    if !result.is_empty() {
        // Propagate to AOF and replication
        ctx.propagate_args(args);
    }

    Ok(RespValue::Array(result))
}

/// ZRANGESTORE dst src min max [BYSCORE|BYLEX] [REV] [LIMIT offset count]
///
/// Store the result of a ZRANGE into a new sorted set.
///
/// Time complexity: O(log(N)+M)
pub fn zrangestore(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 4 {
        return Err(CommandError::WrongArity("ZRANGESTORE".to_string()));
    }

    let dst_key = get_bytes(&args[0])?;
    let src_key = get_bytes(&args[1])?;

    let db = ctx.store().database(ctx.selected_db());

    // Read source sorted set
    let (src_scores, src_members) = match db.get(&src_key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&src_key);
                (HashMap::new(), BTreeMap::new())
            } else {
                match entry.value {
                    RedisValue::SortedSet { scores, members } => (scores, members),
                    _ => return Err(CommandError::WrongType),
                }
            }
        }
        None => (HashMap::new(), BTreeMap::new()),
    };

    // Parse flags
    let mut byscore = false;
    let mut bylex = false;
    let mut rev = false;
    let mut limit_offset: Option<i64> = None;
    let mut limit_count: Option<i64> = None;

    let mut i = 4;
    while i < args.len() {
        let opt = args[i].as_str().map(|s| s.to_uppercase());
        match opt.as_deref() {
            Some("BYSCORE") => byscore = true,
            Some("BYLEX") => bylex = true,
            Some("REV") => rev = true,
            Some("LIMIT") => {
                i += 1;
                limit_offset = Some(parse_int(args.get(i).ok_or(CommandError::SyntaxError)?)?);
                i += 1;
                limit_count = Some(parse_int(args.get(i).ok_or(CommandError::SyntaxError)?)?);
            }
            _ => return Err(CommandError::SyntaxError),
        }
        i += 1;
    }

    let results: Vec<(Bytes, f64)> = if byscore {
        let (min_arg, max_arg) = if rev {
            (&args[3], &args[2])
        } else {
            (&args[2], &args[3])
        };
        let min_bound = parse_score_bound(min_arg)?;
        let max_bound = parse_score_bound(max_arg)?;
        let all: Vec<(Bytes, f64)> = if rev {
            src_members
                .keys()
                .rev()
                .filter(|(s, _)| min_bound.includes(s.0) && max_bound.includes_upper(s.0))
                .map(|(s, m)| (m.clone(), s.0))
                .collect()
        } else {
            src_members
                .keys()
                .filter(|(s, _)| min_bound.includes(s.0) && max_bound.includes_upper(s.0))
                .map(|(s, m)| (m.clone(), s.0))
                .collect()
        };
        apply_limit(all, limit_offset, limit_count)
    } else if bylex {
        let (min_arg, max_arg) = if rev {
            (&args[3], &args[2])
        } else {
            (&args[2], &args[3])
        };
        let min_lex = parse_lex_bound(min_arg)?;
        let max_lex = parse_lex_bound(max_arg)?;
        let all: Vec<(Bytes, f64)> = if rev {
            src_members
                .keys()
                .rev()
                .filter(|(_, m)| lex_gte(m, &min_lex) && lex_lte(m, &max_lex))
                .map(|(s, m)| (m.clone(), s.0))
                .collect()
        } else {
            src_members
                .keys()
                .filter(|(_, m)| lex_gte(m, &min_lex) && lex_lte(m, &max_lex))
                .map(|(s, m)| (m.clone(), s.0))
                .collect()
        };
        apply_limit(all, limit_offset, limit_count)
    } else {
        let start = parse_int(&args[2])?;
        let stop = parse_int(&args[3])?;
        let len = src_members.len() as i64;
        let s = normalize_index(start, len);
        let e = normalize_index(stop, len);
        if s > e || s >= len || len == 0 {
            vec![]
        } else {
            let s = s.max(0) as usize;
            let e = e.min(len - 1) as usize;
            if rev {
                src_members
                    .keys()
                    .rev()
                    .skip(s)
                    .take(e - s + 1)
                    .map(|(sc, m)| (m.clone(), sc.0))
                    .collect()
            } else {
                src_members
                    .keys()
                    .skip(s)
                    .take(e - s + 1)
                    .map(|(sc, m)| (m.clone(), sc.0))
                    .collect()
            }
        }
    };

    let _ = src_scores; // used implicitly via src_members

    let count = results.len() as i64;

    // Build destination sorted set
    let mut dst_scores = HashMap::new();
    let mut dst_members = BTreeMap::new();
    for (member, score) in results {
        zset_add(&mut dst_scores, &mut dst_members, member, score);
    }
    write_zset(ctx, dst_key.clone(), dst_scores, dst_members);

    // Propagate to AOF and replication
    ctx.propagate_args(args);

    Ok(RespValue::Integer(count))
}

/// ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS w ...] [AGGREGATE SUM|MIN|MAX]
///
/// Compute the union of sorted sets and store in destination.
///
/// Time complexity: O(N)+O(M log(M))
pub fn zunionstore(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongArity("ZUNIONSTORE".to_string()));
    }

    let dst = get_bytes(&args[0])?;
    let numkeys = parse_int(&args[1])? as usize;

    if numkeys == 0 {
        return Err(CommandError::InvalidArgument(
            "at least 1 input key is needed for 'zunionstore' command".to_string(),
        ));
    }
    if args.len() < 2 + numkeys {
        return Err(CommandError::SyntaxError);
    }

    let key_start = 2;
    let key_end = key_start + numkeys;
    let keys: Vec<Bytes> = args[key_start..key_end]
        .iter()
        .map(get_bytes)
        .collect::<Result<_, _>>()?;

    let (weights, agg) = parse_store_options(args, key_end, numkeys)?;

    // Collect union
    let mut union_scores: HashMap<Bytes, f64> = HashMap::new();

    for (idx, k) in keys.iter().enumerate() {
        let (zscores, _) = read_zset(ctx, k)?;
        let w = weights[idx];
        for (member, score) in &zscores {
            let weighted = *score * w;
            union_scores
                .entry(member.clone())
                .and_modify(|existing| *existing = aggregate_scores(agg, *existing, weighted))
                .or_insert(weighted);
        }
    }

    // Build destination
    let mut dst_scores = HashMap::new();
    let mut dst_members = BTreeMap::new();
    for (member, score) in union_scores {
        zset_add(&mut dst_scores, &mut dst_members, member, score);
    }

    let count = dst_scores.len() as i64;
    write_zset(ctx, dst.clone(), dst_scores, dst_members);

    // Propagate to AOF and replication
    ctx.propagate_args(args);

    Ok(RespValue::Integer(count))
}

/// ZINTERSTORE destination numkeys key [key ...] [WEIGHTS w ...] [AGGREGATE SUM|MIN|MAX]
///
/// Compute the intersection of sorted sets and store in destination.
///
/// Time complexity: O(N*K)+O(M*log(M))
pub fn zinterstore(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongArity("ZINTERSTORE".to_string()));
    }

    let dst = get_bytes(&args[0])?;
    let numkeys = parse_int(&args[1])? as usize;

    if numkeys == 0 {
        return Err(CommandError::InvalidArgument(
            "at least 1 input key is needed for 'zinterstore' command".to_string(),
        ));
    }
    if args.len() < 2 + numkeys {
        return Err(CommandError::SyntaxError);
    }

    let key_start = 2;
    let key_end = key_start + numkeys;
    let keys: Vec<Bytes> = args[key_start..key_end]
        .iter()
        .map(get_bytes)
        .collect::<Result<_, _>>()?;

    let (weights, agg) = parse_store_options(args, key_end, numkeys)?;

    // Start with the first key
    let (first_scores, _) = read_zset(ctx, &keys[0])?;

    let mut inter: HashMap<Bytes, f64> = HashMap::new();
    for (member, score) in &first_scores {
        inter.insert(member.clone(), *score * weights[0]);
    }

    // Intersect with remaining keys
    for (idx, k) in keys.iter().enumerate().skip(1) {
        let (zscores, _) = read_zset(ctx, k)?;
        let w = weights[idx];
        let mut new_inter: HashMap<Bytes, f64> = HashMap::new();
        for (member, current_score) in &inter {
            if let Some(&score) = zscores.get(member) {
                let weighted = score * w;
                new_inter.insert(
                    member.clone(),
                    aggregate_scores(agg, *current_score, weighted),
                );
            }
        }
        inter = new_inter;
    }

    // Build destination
    let mut dst_scores = HashMap::new();
    let mut dst_members = BTreeMap::new();
    for (member, score) in inter {
        zset_add(&mut dst_scores, &mut dst_members, member, score);
    }

    let count = dst_scores.len() as i64;
    write_zset(ctx, dst.clone(), dst_scores, dst_members);

    // Propagate to AOF and replication
    ctx.propagate_args(args);

    Ok(RespValue::Integer(count))
}

/// ZSCAN key cursor [MATCH pattern] [COUNT count]
///
/// Incrementally iterate sorted set elements and their scores.
///
/// Time complexity: O(1) per call, O(N) full iteration
pub fn zscan(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("ZSCAN".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let cursor = parse_int(&args[1])? as usize;

    let mut pattern = "*";
    let mut count = 10usize;

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

    match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                return Ok(RespValue::Array(vec![
                    RespValue::BulkString(Bytes::from("0")),
                    RespValue::Array(vec![]),
                ]));
            }
            match &entry.value {
                RedisValue::SortedSet { members, .. } => {
                    // Collect all members that match the pattern
                    let all: Vec<(&Bytes, f64)> = members
                        .keys()
                        .filter(|(_, m)| {
                            let member_str = String::from_utf8_lossy(m);
                            glob_match::glob_match(pattern, &member_str)
                        })
                        .map(|(s, m)| (m, s.0))
                        .collect();

                    let total = all.len();
                    let start = cursor.min(total);
                    let end = (start + count).min(total);
                    let next_cursor = if end >= total { 0 } else { end };

                    let mut elements = Vec::new();
                    for (member, score) in all.into_iter().skip(start).take(count) {
                        elements.push(RespValue::BulkString(member.clone()));
                        elements.push(RespValue::BulkString(Bytes::from(format_score(score))));
                    }

                    Ok(RespValue::Array(vec![
                        RespValue::BulkString(Bytes::from(next_cursor.to_string())),
                        RespValue::Array(elements),
                    ]))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        None => Ok(RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("0")),
            RespValue::Array(vec![]),
        ])),
    }
}

/// ZMSCORE key member [member ...]
///
/// Return the scores of multiple members.
///
/// Time complexity: O(N)
pub fn zmscore(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("ZMSCORE".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let db = ctx.store().database(ctx.selected_db());

    match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                let results: Vec<RespValue> = args[1..].iter().map(|_| RespValue::Null).collect();
                return Ok(RespValue::Array(results));
            }
            match &entry.value {
                RedisValue::SortedSet { scores, .. } => {
                    let results: Vec<RespValue> = args[1..]
                        .iter()
                        .map(|a| {
                            let member = get_bytes(a).ok();
                            match member {
                                Some(m) => match scores.get(&m) {
                                    Some(&s) => RespValue::BulkString(Bytes::from(format_score(s))),
                                    None => RespValue::Null,
                                },
                                None => RespValue::Null,
                            }
                        })
                        .collect();
                    Ok(RespValue::Array(results))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        None => {
            let results: Vec<RespValue> = args[1..].iter().map(|_| RespValue::Null).collect();
            Ok(RespValue::Array(results))
        }
    }
}

/// ZRANDMEMBER key [count [WITHSCORES]]
///
/// Return one or more random members.
///
/// Time complexity: O(N) when count is provided, O(1) otherwise
pub fn zrandmember(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("ZRANDMEMBER".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let db = ctx.store().database(ctx.selected_db());

    match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                if args.len() > 1 {
                    return Ok(RespValue::Array(vec![]));
                }
                return Ok(RespValue::Null);
            }
            match &entry.value {
                RedisValue::SortedSet {
                    scores, members, ..
                } => {
                    if scores.is_empty() {
                        if args.len() > 1 {
                            return Ok(RespValue::Array(vec![]));
                        }
                        return Ok(RespValue::Null);
                    }

                    if args.len() == 1 {
                        // Single random member
                        let idx = rand::random::<usize>() % members.len();
                        let member = members.keys().nth(idx).map(|(_, m)| m.clone());
                        match member {
                            Some(m) => Ok(RespValue::BulkString(m)),
                            None => Ok(RespValue::Null),
                        }
                    } else {
                        let count = parse_int(&args[1])?;
                        let withscores = args
                            .get(2)
                            .and_then(|v| v.as_str())
                            .map(|s| s.eq_ignore_ascii_case("WITHSCORES"))
                            .unwrap_or(false);

                        let allow_dupes = count < 0;
                        let abs_count = count.unsigned_abs() as usize;

                        let all_members: Vec<(&Bytes, f64)> =
                            members.keys().map(|(s, m)| (m, s.0)).collect();

                        let mut result = Vec::new();

                        if allow_dupes {
                            for _ in 0..abs_count {
                                let idx = rand::random::<usize>() % all_members.len();
                                let (m, s) = &all_members[idx];
                                result.push(RespValue::BulkString((*m).clone()));
                                if withscores {
                                    result
                                        .push(RespValue::BulkString(Bytes::from(format_score(*s))));
                                }
                            }
                        } else {
                            let take = abs_count.min(all_members.len());
                            // Fisher-Yates partial shuffle for truly random unique selection
                            let n = all_members.len();
                            let mut indices: Vec<usize> = (0..n).collect();
                            for i in 0..take {
                                let j = i + (rand::random::<usize>() % (n - i));
                                indices.swap(i, j);
                            }
                            for &idx in &indices[..take] {
                                let (m, s) = &all_members[idx];
                                result.push(RespValue::BulkString((*m).clone()));
                                if withscores {
                                    result
                                        .push(RespValue::BulkString(Bytes::from(format_score(*s))));
                                }
                            }
                        }

                        Ok(RespValue::Array(result))
                    }
                }
                _ => Err(CommandError::WrongType),
            }
        }
        None => {
            if args.len() > 1 {
                Ok(RespValue::Array(vec![]))
            } else {
                Ok(RespValue::Null)
            }
        }
    }
}

/// ZLEXCOUNT key min max
///
/// Count the number of members between a given lexicographical range.
///
/// Time complexity: O(log(N))
pub fn zlexcount(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongArity("ZLEXCOUNT".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let min = parse_lex_bound(&args[1])?;
    let max = parse_lex_bound(&args[2])?;

    let db = ctx.store().database(ctx.selected_db());

    match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                return Ok(RespValue::Integer(0));
            }
            match &entry.value {
                RedisValue::SortedSet { members, .. } => {
                    let count = members
                        .keys()
                        .filter(|(_, m)| lex_gte(m, &min) && lex_lte(m, &max))
                        .count();
                    Ok(RespValue::Integer(count as i64))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        None => Ok(RespValue::Integer(0)),
    }
}

/// ZRANGEBYLEX key min max [LIMIT offset count]
///
/// Return a range of members by lexicographical range.
///
/// Time complexity: O(log(N)+M)
pub fn zrangebylex(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongArity("ZRANGEBYLEX".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let min = parse_lex_bound(&args[1])?;
    let max = parse_lex_bound(&args[2])?;

    let mut limit_offset: Option<i64> = None;
    let mut limit_count: Option<i64> = None;

    let mut i = 3;
    while i < args.len() {
        let opt = args[i].as_str().map(|s| s.to_uppercase());
        match opt.as_deref() {
            Some("LIMIT") => {
                i += 1;
                limit_offset = Some(parse_int(args.get(i).ok_or(CommandError::SyntaxError)?)?);
                i += 1;
                limit_count = Some(parse_int(args.get(i).ok_or(CommandError::SyntaxError)?)?);
            }
            _ => return Err(CommandError::SyntaxError),
        }
        i += 1;
    }

    let db = ctx.store().database(ctx.selected_db());

    match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                return Ok(RespValue::Array(vec![]));
            }
            match &entry.value {
                RedisValue::SortedSet { members, .. } => {
                    let all: Vec<Bytes> = members
                        .keys()
                        .filter(|(_, m)| lex_gte(m, &min) && lex_lte(m, &max))
                        .map(|(_, m)| m.clone())
                        .collect();

                    let off = limit_offset.unwrap_or(0).max(0) as usize;
                    let results: Vec<RespValue> = match limit_count {
                        Some(c) if c >= 0 => all
                            .into_iter()
                            .skip(off)
                            .take(c as usize)
                            .map(RespValue::BulkString)
                            .collect(),
                        _ => all
                            .into_iter()
                            .skip(off)
                            .map(RespValue::BulkString)
                            .collect(),
                    };

                    Ok(RespValue::Array(results))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        None => Ok(RespValue::Array(vec![])),
    }
}

/// ZREVRANGEBYLEX key max min [LIMIT offset count]
///
/// Return a range of members by lexicographical range, ordered from
/// higher to lower strings.
///
/// Time complexity: O(log(N)+M)
pub fn zrevrangebylex(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongArity("ZREVRANGEBYLEX".to_string()));
    }

    let key = get_bytes(&args[0])?;
    // Note: for ZREVRANGEBYLEX first arg is max, second is min
    let max = parse_lex_bound(&args[1])?;
    let min = parse_lex_bound(&args[2])?;

    let mut limit_offset: Option<i64> = None;
    let mut limit_count: Option<i64> = None;

    let mut i = 3;
    while i < args.len() {
        let opt = args[i].as_str().map(|s| s.to_uppercase());
        match opt.as_deref() {
            Some("LIMIT") => {
                i += 1;
                limit_offset = Some(parse_int(args.get(i).ok_or(CommandError::SyntaxError)?)?);
                i += 1;
                limit_count = Some(parse_int(args.get(i).ok_or(CommandError::SyntaxError)?)?);
            }
            _ => return Err(CommandError::SyntaxError),
        }
        i += 1;
    }

    let db = ctx.store().database(ctx.selected_db());

    match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                return Ok(RespValue::Array(vec![]));
            }
            match &entry.value {
                RedisValue::SortedSet { members, .. } => {
                    let all: Vec<Bytes> = members
                        .keys()
                        .rev()
                        .filter(|(_, m)| lex_gte(m, &min) && lex_lte(m, &max))
                        .map(|(_, m)| m.clone())
                        .collect();

                    let off = limit_offset.unwrap_or(0).max(0) as usize;
                    let results: Vec<RespValue> = match limit_count {
                        Some(c) if c >= 0 => all
                            .into_iter()
                            .skip(off)
                            .take(c as usize)
                            .map(RespValue::BulkString)
                            .collect(),
                        _ => all
                            .into_iter()
                            .skip(off)
                            .map(RespValue::BulkString)
                            .collect(),
                    };

                    Ok(RespValue::Array(results))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        None => Ok(RespValue::Array(vec![])),
    }
}

/// ZREMRANGEBYRANK key start stop
///
/// Remove all members with rank between start and stop.
///
/// Time complexity: O(log(N)+M)
pub fn zremrangebyrank(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongArity("ZREMRANGEBYRANK".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let start = parse_int(&args[1])?;
    let stop = parse_int(&args[2])?;

    let db = ctx.store().database(ctx.selected_db());

    let (mut scores, mut members) = match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                return Ok(RespValue::Integer(0));
            }
            match entry.value {
                RedisValue::SortedSet { scores, members } => (scores, members),
                _ => return Err(CommandError::WrongType),
            }
        }
        None => return Ok(RespValue::Integer(0)),
    };

    let len = members.len() as i64;
    let s = normalize_index(start, len);
    let e = normalize_index(stop, len);

    if s > e || s >= len || len == 0 {
        write_zset(ctx, key, scores, members);
        return Ok(RespValue::Integer(0));
    }

    let s = s.max(0) as usize;
    let e = e.min(len - 1) as usize;

    // Collect members to remove
    let to_remove: Vec<(OrderedFloat<f64>, Bytes)> =
        members.keys().skip(s).take(e - s + 1).cloned().collect();

    let removed = to_remove.len() as i64;
    for (score, member) in to_remove {
        members.remove(&(score, member.clone()));
        scores.remove(&member);
    }

    write_zset(ctx, key, scores, members);
    Ok(RespValue::Integer(removed))
}

/// ZREMRANGEBYSCORE key min max
///
/// Remove all members with score between min and max.
///
/// Time complexity: O(log(N)+M)
pub fn zremrangebyscore(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongArity("ZREMRANGEBYSCORE".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let min = parse_score_bound(&args[1])?;
    let max = parse_score_bound(&args[2])?;

    let db = ctx.store().database(ctx.selected_db());

    let (mut scores, mut members) = match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                return Ok(RespValue::Integer(0));
            }
            match entry.value {
                RedisValue::SortedSet { scores, members } => (scores, members),
                _ => return Err(CommandError::WrongType),
            }
        }
        None => return Ok(RespValue::Integer(0)),
    };

    let to_remove: Vec<(OrderedFloat<f64>, Bytes)> = members
        .keys()
        .filter(|(s, _)| min.includes(s.0) && max.includes_upper(s.0))
        .cloned()
        .collect();

    let removed = to_remove.len() as i64;
    for (score, member) in to_remove {
        members.remove(&(score, member.clone()));
        scores.remove(&member);
    }

    write_zset(ctx, key, scores, members);
    Ok(RespValue::Integer(removed))
}

/// ZREMRANGEBYLEX key min max
///
/// Remove all members between the given lexicographical range.
///
/// Time complexity: O(log(N)+M)
pub fn zremrangebylex(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongArity("ZREMRANGEBYLEX".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let min = parse_lex_bound(&args[1])?;
    let max = parse_lex_bound(&args[2])?;

    let db = ctx.store().database(ctx.selected_db());

    let (mut scores, mut members) = match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                return Ok(RespValue::Integer(0));
            }
            match entry.value {
                RedisValue::SortedSet { scores, members } => (scores, members),
                _ => return Err(CommandError::WrongType),
            }
        }
        None => return Ok(RespValue::Integer(0)),
    };

    let to_remove: Vec<(OrderedFloat<f64>, Bytes)> = members
        .keys()
        .filter(|(_, m)| lex_gte(m, &min) && lex_lte(m, &max))
        .cloned()
        .collect();

    let removed = to_remove.len() as i64;
    for (score, member) in to_remove {
        members.remove(&(score, member.clone()));
        scores.remove(&member);
    }

    write_zset(ctx, key, scores, members);
    Ok(RespValue::Integer(removed))
}

/// ZMPOP numkeys key [key ...] MIN|MAX [COUNT count]
///
/// Remove and return members with the lowest/highest scores from the first
/// non-empty sorted set.
///
/// Time complexity: O(K) + O(M*log(N))
pub fn zmpop(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("ZMPOP".to_string()));
    }

    let numkeys = parse_int(&args[0])? as usize;
    if numkeys == 0 {
        return Err(CommandError::InvalidArgument(
            "numkeys can't be zero".to_string(),
        ));
    }
    if args.len() < 1 + numkeys + 1 {
        return Err(CommandError::SyntaxError);
    }

    let keys: Vec<Bytes> = args[1..1 + numkeys]
        .iter()
        .map(get_bytes)
        .collect::<Result<_, _>>()?;

    let direction_idx = 1 + numkeys;
    let direction = args[direction_idx]
        .as_str()
        .map(|s| s.to_uppercase())
        .ok_or(CommandError::SyntaxError)?;
    let pop_min = match direction.as_str() {
        "MIN" => true,
        "MAX" => false,
        _ => return Err(CommandError::SyntaxError),
    };

    let mut count = 1usize;
    let mut i = direction_idx + 1;
    while i < args.len() {
        let opt = args[i]
            .as_str()
            .map(|s| s.to_uppercase())
            .unwrap_or_default();
        match opt.as_str() {
            "COUNT" => {
                i += 1;
                count = parse_int(args.get(i).ok_or(CommandError::SyntaxError)?)? as usize;
            }
            _ => return Err(CommandError::SyntaxError),
        }
        i += 1;
    }

    let db = ctx.store().database(ctx.selected_db());

    for key in &keys {
        let (mut scores, mut members) = match db.get(key) {
            Some(entry) => {
                if entry.is_expired() {
                    db.delete(key);
                    continue;
                }
                match entry.value {
                    RedisValue::SortedSet { scores, members } => (scores, members),
                    _ => return Err(CommandError::WrongType),
                }
            }
            None => continue,
        };

        if scores.is_empty() {
            continue;
        }

        let mut result_elements = Vec::new();
        for _ in 0..count {
            let item = if pop_min {
                members.keys().next().cloned()
            } else {
                members.keys().next_back().cloned()
            };
            match item {
                Some((score, member)) => {
                    members.remove(&(score, member.clone()));
                    scores.remove(&member);
                    result_elements.push(RespValue::BulkString(member));
                    result_elements.push(RespValue::BulkString(Bytes::from(format_score(score.0))));
                }
                None => break,
            }
        }

        write_zset(ctx, key.clone(), scores, members);

        return Ok(RespValue::Array(vec![
            RespValue::BulkString(key.clone()),
            RespValue::Array(result_elements),
        ]));
    }

    Ok(RespValue::Null)
}

/// ZUNION numkeys key [key ...] [WEIGHTS w ...] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]
///
/// Return the union of multiple sorted sets (without storing).
///
/// Time complexity: O(N)+O(M*log(M))
pub fn zunion(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("ZUNION".to_string()));
    }

    let numkeys = parse_int(&args[0])? as usize;
    if numkeys == 0 {
        return Err(CommandError::InvalidArgument(
            "at least 1 input key is needed for 'zunion' command".to_string(),
        ));
    }
    if args.len() < 1 + numkeys {
        return Err(CommandError::SyntaxError);
    }

    let key_start = 1;
    let key_end = key_start + numkeys;
    let keys: Vec<Bytes> = args[key_start..key_end]
        .iter()
        .map(get_bytes)
        .collect::<Result<_, _>>()?;

    // Parse remaining args: WEIGHTS, AGGREGATE, WITHSCORES
    let mut weights: Vec<f64> = vec![1.0; numkeys];
    let mut agg = Aggregate::Sum;
    let mut withscores = false;

    let mut i = key_end;
    while i < args.len() {
        let opt = args[i]
            .as_str()
            .map(|s| s.to_uppercase())
            .unwrap_or_default();
        match opt.as_str() {
            "WEIGHTS" => {
                for w in weights.iter_mut().take(numkeys) {
                    i += 1;
                    *w = parse_float(args.get(i).ok_or(CommandError::SyntaxError)?)?;
                }
            }
            "AGGREGATE" => {
                i += 1;
                let a = args
                    .get(i)
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_uppercase())
                    .ok_or(CommandError::SyntaxError)?;
                agg = match a.as_str() {
                    "SUM" => Aggregate::Sum,
                    "MIN" => Aggregate::Min,
                    "MAX" => Aggregate::Max,
                    _ => return Err(CommandError::SyntaxError),
                };
            }
            "WITHSCORES" => withscores = true,
            _ => return Err(CommandError::SyntaxError),
        }
        i += 1;
    }

    // Collect union
    let mut union_scores: HashMap<Bytes, f64> = HashMap::new();
    for (idx, k) in keys.iter().enumerate() {
        let (zscores, _) = read_zset(ctx, k)?;
        let w = weights[idx];
        for (member, score) in &zscores {
            let weighted = *score * w;
            union_scores
                .entry(member.clone())
                .and_modify(|existing| *existing = aggregate_scores(agg, *existing, weighted))
                .or_insert(weighted);
        }
    }

    // Build result sorted by score
    let mut sorted: Vec<(Bytes, f64)> = union_scores.into_iter().collect();
    sorted.sort_by(|a, b| {
        OrderedFloat(a.1)
            .cmp(&OrderedFloat(b.1))
            .then_with(|| a.0.cmp(&b.0))
    });

    Ok(build_array_response(&sorted, withscores))
}

/// ZINTER numkeys key [key ...] [WEIGHTS w ...] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]
///
/// Return the intersection of multiple sorted sets (without storing).
///
/// Time complexity: O(N*K)+O(M*log(M))
pub fn zinter(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("ZINTER".to_string()));
    }

    let numkeys = parse_int(&args[0])? as usize;
    if numkeys == 0 {
        return Err(CommandError::InvalidArgument(
            "at least 1 input key is needed for 'zinter' command".to_string(),
        ));
    }
    if args.len() < 1 + numkeys {
        return Err(CommandError::SyntaxError);
    }

    let key_start = 1;
    let key_end = key_start + numkeys;
    let keys: Vec<Bytes> = args[key_start..key_end]
        .iter()
        .map(get_bytes)
        .collect::<Result<_, _>>()?;

    let mut weights: Vec<f64> = vec![1.0; numkeys];
    let mut agg = Aggregate::Sum;
    let mut withscores = false;

    let mut i = key_end;
    while i < args.len() {
        let opt = args[i]
            .as_str()
            .map(|s| s.to_uppercase())
            .unwrap_or_default();
        match opt.as_str() {
            "WEIGHTS" => {
                for w in weights.iter_mut().take(numkeys) {
                    i += 1;
                    *w = parse_float(args.get(i).ok_or(CommandError::SyntaxError)?)?;
                }
            }
            "AGGREGATE" => {
                i += 1;
                let a = args
                    .get(i)
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_uppercase())
                    .ok_or(CommandError::SyntaxError)?;
                agg = match a.as_str() {
                    "SUM" => Aggregate::Sum,
                    "MIN" => Aggregate::Min,
                    "MAX" => Aggregate::Max,
                    _ => return Err(CommandError::SyntaxError),
                };
            }
            "WITHSCORES" => withscores = true,
            _ => return Err(CommandError::SyntaxError),
        }
        i += 1;
    }

    // Start with the first key
    let (first_scores, _) = read_zset(ctx, &keys[0])?;
    let mut inter: HashMap<Bytes, f64> = HashMap::new();
    for (member, score) in &first_scores {
        inter.insert(member.clone(), *score * weights[0]);
    }

    // Intersect with remaining keys
    for (idx, k) in keys.iter().enumerate().skip(1) {
        let (zscores, _) = read_zset(ctx, k)?;
        let w = weights[idx];
        let mut new_inter: HashMap<Bytes, f64> = HashMap::new();
        for (member, current_score) in &inter {
            if let Some(&score) = zscores.get(member) {
                let weighted = score * w;
                new_inter.insert(
                    member.clone(),
                    aggregate_scores(agg, *current_score, weighted),
                );
            }
        }
        inter = new_inter;
    }

    // Build result sorted by score
    let mut sorted: Vec<(Bytes, f64)> = inter.into_iter().collect();
    sorted.sort_by(|a, b| {
        OrderedFloat(a.1)
            .cmp(&OrderedFloat(b.1))
            .then_with(|| a.0.cmp(&b.0))
    });

    Ok(build_array_response(&sorted, withscores))
}

/// ZINTERCARD numkeys key [key ...] [LIMIT limit]
///
/// Return the cardinality of the intersection of multiple sorted sets.
/// Optionally limit the number of matches to count.
///
/// Time complexity: O(N*K) where N is the size of smallest set and K is the number of sets
pub fn zintercard(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("ZINTERCARD".to_string()));
    }

    let numkeys = parse_int(&args[0])? as usize;
    if numkeys == 0 {
        return Err(CommandError::InvalidArgument(
            "at least 1 input key is needed for 'zintercard' command".to_string(),
        ));
    }
    if args.len() < 1 + numkeys {
        return Err(CommandError::SyntaxError);
    }

    let key_start = 1;
    let key_end = key_start + numkeys;
    let keys: Vec<Bytes> = args[key_start..key_end]
        .iter()
        .map(get_bytes)
        .collect::<Result<_, _>>()?;

    // Parse optional LIMIT
    let mut limit: Option<usize> = None;
    let mut i = key_end;
    while i < args.len() {
        let opt = args[i]
            .as_str()
            .map(|s| s.to_uppercase())
            .unwrap_or_default();
        match opt.as_str() {
            "LIMIT" => {
                i += 1;
                let limit_val = parse_int(args.get(i).ok_or(CommandError::SyntaxError)?)?;
                if limit_val < 0 {
                    return Err(CommandError::InvalidArgument(
                        "LIMIT can't be negative".to_string(),
                    ));
                }
                limit = Some(limit_val as usize);
            }
            _ => return Err(CommandError::SyntaxError),
        }
        i += 1;
    }

    // Find the smallest set first for optimization
    let mut set_sizes: Vec<(usize, usize)> = Vec::new();
    for (idx, key) in keys.iter().enumerate() {
        let (scores, _) = read_zset(ctx, key)?;
        set_sizes.push((idx, scores.len()));
    }
    set_sizes.sort_by_key(|(_, size)| *size);

    if set_sizes[0].1 == 0 {
        // Smallest set is empty, so intersection is empty
        return Ok(RespValue::Integer(0));
    }

    // Start with the smallest set
    let smallest_idx = set_sizes[0].0;
    let (first_scores, _) = read_zset(ctx, &keys[smallest_idx])?;

    let mut count = 0;
    let max_count = limit.unwrap_or(usize::MAX);

    // For each member in the smallest set, check if it exists in all other sets
    'outer: for member in first_scores.keys() {
        for (idx, key) in keys.iter().enumerate() {
            if idx == smallest_idx {
                continue; // Skip the smallest set itself
            }
            let (scores, _) = read_zset(ctx, key)?;
            if !scores.contains_key(member) {
                continue 'outer; // Not in all sets, skip this member
            }
        }
        // Member exists in all sets
        count += 1;
        if count >= max_count {
            break;
        }
    }

    Ok(RespValue::Integer(count as i64))
}

/// ZDIFF numkeys key [key ...] [WITHSCORES]
///
/// Return the difference of the first sorted set with all subsequent sorted sets.
///
/// Time complexity: O(L + (N-K)log(N))
pub fn zdiff(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("ZDIFF".to_string()));
    }

    let numkeys = parse_int(&args[0])? as usize;
    if numkeys == 0 {
        return Err(CommandError::InvalidArgument(
            "at least 1 input key is needed for 'zdiff' command".to_string(),
        ));
    }
    if args.len() < 1 + numkeys {
        return Err(CommandError::SyntaxError);
    }

    let key_start = 1;
    let key_end = key_start + numkeys;
    let keys: Vec<Bytes> = args[key_start..key_end]
        .iter()
        .map(get_bytes)
        .collect::<Result<_, _>>()?;

    let withscores = args
        .get(key_end)
        .and_then(|v| v.as_str())
        .map(|s| s.eq_ignore_ascii_case("WITHSCORES"))
        .unwrap_or(false);

    // Start with the first key
    let (first_scores, first_members) = read_zset(ctx, &keys[0])?;
    let mut diff: HashMap<Bytes, f64> = first_scores;

    // Remove members that exist in subsequent keys
    for k in keys.iter().skip(1) {
        let (zscores, _) = read_zset(ctx, k)?;
        for member in zscores.keys() {
            diff.remove(member);
        }
    }

    // Build result sorted by original score order using first_members BTreeMap
    let sorted: Vec<(Bytes, f64)> = first_members
        .keys()
        .filter(|(_, m)| diff.contains_key(m))
        .map(|(s, m)| (m.clone(), s.0))
        .collect();

    Ok(build_array_response(&sorted, withscores))
}

/// ZDIFFSTORE destination numkeys key [key ...]
///
/// Compute the difference between sorted sets and store in destination.
///
/// Time complexity: O(L + (N-K)log(N))
pub fn zdiffstore(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongArity("ZDIFFSTORE".to_string()));
    }

    let dst = get_bytes(&args[0])?;
    let numkeys = parse_int(&args[1])? as usize;

    if numkeys == 0 {
        return Err(CommandError::InvalidArgument(
            "at least 1 input key is needed for 'zdiffstore' command".to_string(),
        ));
    }
    if args.len() < 2 + numkeys {
        return Err(CommandError::SyntaxError);
    }

    let key_start = 2;
    let key_end = key_start + numkeys;
    let keys: Vec<Bytes> = args[key_start..key_end]
        .iter()
        .map(get_bytes)
        .collect::<Result<_, _>>()?;

    // Start with the first key
    let (first_scores, first_members) = read_zset(ctx, &keys[0])?;
    let mut diff: HashMap<Bytes, f64> = first_scores;

    // Remove members that exist in subsequent keys
    for k in keys.iter().skip(1) {
        let (zscores, _) = read_zset(ctx, k)?;
        for member in zscores.keys() {
            diff.remove(member);
        }
    }

    // Build destination sorted set, maintaining original order
    let mut dst_scores = HashMap::new();
    let mut dst_members = BTreeMap::new();
    for (score_of, member) in first_members.keys() {
        if let Some(&score) = diff.get(member) {
            let _ = score_of; // score from original order
            zset_add(&mut dst_scores, &mut dst_members, member.clone(), score);
        }
    }

    let count = dst_scores.len() as i64;
    write_zset(ctx, dst, dst_scores, dst_members);

    Ok(RespValue::Integer(count))
}

// ===================================================================
// BLOCKING COMMANDS
// ===================================================================

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

/// BZPOPMIN key [key ...] timeout
///
/// Blocking version of ZPOPMIN. Removes and returns the member with
/// the lowest score from the first non-empty sorted set among the
/// given keys. If all sorted sets are empty, blocks until an element
/// becomes available or the timeout expires.
///
/// Returns a three-element array [key, member, score] when data is
/// available, or Null on timeout.
///
/// Time complexity: O(log(N))
pub fn bzpopmin(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("BZPOPMIN".to_string()));
    }

    // Last argument is timeout
    let timeout_secs = parse_float_timeout(&args[args.len() - 1])?;
    let keys: Vec<Bytes> = args[..args.len() - 1]
        .iter()
        .map(get_bytes)
        .collect::<Result<_, _>>()?;

    if keys.is_empty() {
        return Err(CommandError::WrongArity("BZPOPMIN".to_string()));
    }

    let db = ctx.store().database(ctx.selected_db());

    for key in &keys {
        let (mut scores, mut members) = match db.get(key) {
            Some(entry) => {
                if entry.is_expired() {
                    db.delete(key);
                    continue;
                }
                match entry.value {
                    RedisValue::SortedSet { scores, members } if !scores.is_empty() => {
                        (scores, members)
                    }
                    RedisValue::SortedSet { .. } => continue,
                    _ => return Err(CommandError::WrongType),
                }
            }
            None => continue,
        };

        // Pop min
        let first = members.keys().next().cloned();
        if let Some((score, member)) = first {
            members.remove(&(score, member.clone()));
            scores.remove(&member);
            write_zset(ctx, key.clone(), scores, members);
            return Ok(RespValue::Array(vec![
                RespValue::BulkString(key.clone()),
                RespValue::BulkString(member),
                RespValue::BulkString(Bytes::from(format_score(score.0))),
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
        keys,
        timeout,
        command_name: "BZPOPMIN".to_string(),
        args: args.to_vec(),
    }))
}

/// BZPOPMAX key [key ...] timeout
///
/// Blocking version of ZPOPMAX. Removes and returns the member with
/// the highest score from the first non-empty sorted set among the
/// given keys. If all sorted sets are empty, blocks until an element
/// becomes available or the timeout expires.
///
/// Returns a three-element array [key, member, score] when data is
/// available, or Null on timeout.
///
/// Time complexity: O(log(N))
pub fn bzpopmax(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("BZPOPMAX".to_string()));
    }

    // Last argument is timeout
    let timeout_secs = parse_float_timeout(&args[args.len() - 1])?;
    let keys: Vec<Bytes> = args[..args.len() - 1]
        .iter()
        .map(get_bytes)
        .collect::<Result<_, _>>()?;

    if keys.is_empty() {
        return Err(CommandError::WrongArity("BZPOPMAX".to_string()));
    }

    let db = ctx.store().database(ctx.selected_db());

    for key in &keys {
        let (mut scores, mut members) = match db.get(key) {
            Some(entry) => {
                if entry.is_expired() {
                    db.delete(key);
                    continue;
                }
                match entry.value {
                    RedisValue::SortedSet { scores, members } if !scores.is_empty() => {
                        (scores, members)
                    }
                    RedisValue::SortedSet { .. } => continue,
                    _ => return Err(CommandError::WrongType),
                }
            }
            None => continue,
        };

        // Pop max
        let last = members.keys().next_back().cloned();
        if let Some((score, member)) = last {
            members.remove(&(score, member.clone()));
            scores.remove(&member);
            write_zset(ctx, key.clone(), scores, members);
            return Ok(RespValue::Array(vec![
                RespValue::BulkString(key.clone()),
                RespValue::BulkString(member),
                RespValue::BulkString(Bytes::from(format_score(score.0))),
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
        keys,
        timeout,
        command_name: "BZPOPMAX".to_string(),
        args: args.to_vec(),
    }))
}

/// BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT count]
///
/// Blocking version of ZMPOP. Pops one or more members from the first
/// non-empty sorted set among the given keys. If all sorted sets are empty,
/// blocks until data becomes available or the timeout expires.
///
/// Time complexity: O(K) + O(M*log(N))
pub fn bzmpop(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongArity("BZMPOP".to_string()));
    }

    // First argument is timeout (like BLMPOP)
    let timeout_secs = parse_float_timeout(&args[0])?;
    let numkeys = parse_int(&args[1])? as usize;

    if numkeys == 0 {
        return Err(CommandError::InvalidArgument(
            "numkeys can't be zero".to_string(),
        ));
    }
    if args.len() < 2 + numkeys + 1 {
        return Err(CommandError::SyntaxError);
    }

    let keys: Vec<Bytes> = args[2..2 + numkeys]
        .iter()
        .map(get_bytes)
        .collect::<Result<_, _>>()?;

    let direction_idx = 2 + numkeys;
    let direction = args[direction_idx]
        .as_str()
        .map(|s| s.to_uppercase())
        .ok_or(CommandError::SyntaxError)?;
    let pop_min = match direction.as_str() {
        "MIN" => true,
        "MAX" => false,
        _ => return Err(CommandError::SyntaxError),
    };

    let mut count = 1usize;
    let mut i = direction_idx + 1;
    while i < args.len() {
        let opt = args[i]
            .as_str()
            .map(|s| s.to_uppercase())
            .unwrap_or_default();
        match opt.as_str() {
            "COUNT" => {
                i += 1;
                count = parse_int(args.get(i).ok_or(CommandError::SyntaxError)?)? as usize;
            }
            _ => return Err(CommandError::SyntaxError),
        }
        i += 1;
    }

    let db = ctx.store().database(ctx.selected_db());

    for key in &keys {
        let (mut scores, mut members) = match db.get(key) {
            Some(entry) => {
                if entry.is_expired() {
                    db.delete(key);
                    continue;
                }
                match entry.value {
                    RedisValue::SortedSet { scores, members } if !scores.is_empty() => {
                        (scores, members)
                    }
                    RedisValue::SortedSet { .. } => continue,
                    _ => return Err(CommandError::WrongType),
                }
            }
            None => continue,
        };

        let mut result_elements = Vec::new();
        for _ in 0..count {
            let item = if pop_min {
                members.keys().next().cloned()
            } else {
                members.keys().next_back().cloned()
            };
            match item {
                Some((score, member)) => {
                    members.remove(&(score, member.clone()));
                    scores.remove(&member);
                    result_elements.push(RespValue::BulkString(member));
                    result_elements.push(RespValue::BulkString(Bytes::from(format_score(score.0))));
                }
                None => break,
            }
        }

        write_zset(ctx, key.clone(), scores, members);

        return Ok(RespValue::Array(vec![
            RespValue::BulkString(key.clone()),
            RespValue::Array(result_elements),
        ]));
    }

    // No data available — request blocking
    let timeout = if timeout_secs == 0.0 {
        None
    } else {
        Some(std::time::Duration::from_secs_f64(timeout_secs))
    };

    Err(CommandError::Block(crate::BlockingAction {
        db_index: ctx.selected_db(),
        keys,
        timeout,
        command_name: "BZMPOP".to_string(),
        args: args.to_vec(),
    }))
}

// ===================================================================
// TESTS
// ===================================================================

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

    /// Helper: insert a string key to test WRONGTYPE errors.
    fn set_string(ctx: &mut CommandContext, key: &str, val: &str) {
        let db = ctx.store().database(ctx.selected_db());
        db.set(
            Bytes::from(key.to_owned()),
            Entry::new(RedisValue::String(Bytes::from(val.to_owned()))),
        );
    }

    // ----- 1. test_zadd_basic -----
    #[test]
    fn test_zadd_basic() {
        let mut ctx = make_ctx();
        let result = zadd(
            &mut ctx,
            &[
                bulk("myzset"),
                bulk("1"),
                bulk("one"),
                bulk("2"),
                bulk("two"),
                bulk("3"),
                bulk("three"),
            ],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(3));

        let card = zcard(&mut ctx, &[bulk("myzset")]).unwrap();
        assert_eq!(card, RespValue::Integer(3));

        // Updating existing member returns 0 (no new member)
        let result = zadd(&mut ctx, &[bulk("myzset"), bulk("10"), bulk("one")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    // ----- 2. test_zadd_nx_xx -----
    #[test]
    fn test_zadd_nx_xx() {
        let mut ctx = make_ctx();
        zadd(&mut ctx, &[bulk("z"), bulk("1"), bulk("a")]).unwrap();

        // NX: don't update existing
        let r = zadd(&mut ctx, &[bulk("z"), bulk("NX"), bulk("5"), bulk("a")]).unwrap();
        assert_eq!(r, RespValue::Integer(0));
        let score = zscore(&mut ctx, &[bulk("z"), bulk("a")]).unwrap();
        assert_eq!(score, RespValue::BulkString(Bytes::from("1")));

        // NX: add new
        let r = zadd(&mut ctx, &[bulk("z"), bulk("NX"), bulk("2"), bulk("b")]).unwrap();
        assert_eq!(r, RespValue::Integer(1));

        // XX: only update existing
        let r = zadd(&mut ctx, &[bulk("z"), bulk("XX"), bulk("10"), bulk("a")]).unwrap();
        assert_eq!(r, RespValue::Integer(0));
        let score = zscore(&mut ctx, &[bulk("z"), bulk("a")]).unwrap();
        assert_eq!(score, RespValue::BulkString(Bytes::from("10")));

        // XX: don't add new
        let r = zadd(&mut ctx, &[bulk("z"), bulk("XX"), bulk("3"), bulk("c")]).unwrap();
        assert_eq!(r, RespValue::Integer(0));
        assert_eq!(
            zscore(&mut ctx, &[bulk("z"), bulk("c")]).unwrap(),
            RespValue::Null
        );
    }

    // ----- 3. test_zadd_gt_lt -----
    #[test]
    fn test_zadd_gt_lt() {
        let mut ctx = make_ctx();
        zadd(&mut ctx, &[bulk("z"), bulk("5"), bulk("a")]).unwrap();

        // GT: only update if new score > old
        zadd(&mut ctx, &[bulk("z"), bulk("GT"), bulk("3"), bulk("a")]).unwrap();
        assert_eq!(
            zscore(&mut ctx, &[bulk("z"), bulk("a")]).unwrap(),
            RespValue::BulkString(Bytes::from("5"))
        );

        zadd(&mut ctx, &[bulk("z"), bulk("GT"), bulk("10"), bulk("a")]).unwrap();
        assert_eq!(
            zscore(&mut ctx, &[bulk("z"), bulk("a")]).unwrap(),
            RespValue::BulkString(Bytes::from("10"))
        );

        // LT: only update if new score < old
        zadd(&mut ctx, &[bulk("z"), bulk("LT"), bulk("20"), bulk("a")]).unwrap();
        assert_eq!(
            zscore(&mut ctx, &[bulk("z"), bulk("a")]).unwrap(),
            RespValue::BulkString(Bytes::from("10"))
        );

        zadd(&mut ctx, &[bulk("z"), bulk("LT"), bulk("1"), bulk("a")]).unwrap();
        assert_eq!(
            zscore(&mut ctx, &[bulk("z"), bulk("a")]).unwrap(),
            RespValue::BulkString(Bytes::from("1"))
        );
    }

    // ----- 4. test_zadd_ch_flag -----
    #[test]
    fn test_zadd_ch_flag() {
        let mut ctx = make_ctx();
        zadd(&mut ctx, &[bulk("z"), bulk("1"), bulk("a")]).unwrap();

        // Without CH: returns count of NEW members only
        let r = zadd(
            &mut ctx,
            &[bulk("z"), bulk("5"), bulk("a"), bulk("2"), bulk("b")],
        )
        .unwrap();
        assert_eq!(r, RespValue::Integer(1));

        // With CH: returns count of CHANGED members (new + updated)
        let r = zadd(
            &mut ctx,
            &[
                bulk("z"),
                bulk("CH"),
                bulk("100"),
                bulk("a"),
                bulk("200"),
                bulk("c"),
            ],
        )
        .unwrap();
        assert_eq!(r, RespValue::Integer(2));
    }

    // ----- 5. test_zrem_basic -----
    #[test]
    fn test_zrem_basic() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("z"),
                bulk("1"),
                bulk("a"),
                bulk("2"),
                bulk("b"),
                bulk("3"),
                bulk("c"),
            ],
        )
        .unwrap();

        let r = zrem(
            &mut ctx,
            &[bulk("z"), bulk("a"), bulk("c"), bulk("nonexistent")],
        )
        .unwrap();
        assert_eq!(r, RespValue::Integer(2));

        assert_eq!(
            zcard(&mut ctx, &[bulk("z")]).unwrap(),
            RespValue::Integer(1)
        );

        // Remove last member – key should be deleted
        zrem(&mut ctx, &[bulk("z"), bulk("b")]).unwrap();
        assert_eq!(
            zcard(&mut ctx, &[bulk("z")]).unwrap(),
            RespValue::Integer(0)
        );
    }

    // ----- 6. test_zscore_basic -----
    #[test]
    fn test_zscore_basic() {
        let mut ctx = make_ctx();
        zadd(&mut ctx, &[bulk("z"), bulk("1.5"), bulk("a")]).unwrap();

        let r = zscore(&mut ctx, &[bulk("z"), bulk("a")]).unwrap();
        assert_eq!(r, RespValue::BulkString(Bytes::from("1.5")));
    }

    // ----- 7. test_zscore_nonexistent -----
    #[test]
    fn test_zscore_nonexistent() {
        let mut ctx = make_ctx();
        let r = zscore(&mut ctx, &[bulk("z"), bulk("a")]).unwrap();
        assert_eq!(r, RespValue::Null);

        zadd(&mut ctx, &[bulk("z"), bulk("1"), bulk("x")]).unwrap();
        let r = zscore(&mut ctx, &[bulk("z"), bulk("no")]).unwrap();
        assert_eq!(r, RespValue::Null);
    }

    // ----- 8. test_zrank_basic -----
    #[test]
    fn test_zrank_basic() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("z"),
                bulk("1"),
                bulk("a"),
                bulk("2"),
                bulk("b"),
                bulk("3"),
                bulk("c"),
            ],
        )
        .unwrap();

        assert_eq!(
            zrank(&mut ctx, &[bulk("z"), bulk("a")]).unwrap(),
            RespValue::Integer(0)
        );
        assert_eq!(
            zrank(&mut ctx, &[bulk("z"), bulk("b")]).unwrap(),
            RespValue::Integer(1)
        );
        assert_eq!(
            zrank(&mut ctx, &[bulk("z"), bulk("c")]).unwrap(),
            RespValue::Integer(2)
        );
        assert_eq!(
            zrank(&mut ctx, &[bulk("z"), bulk("d")]).unwrap(),
            RespValue::Null
        );
    }

    // ----- 9. test_zrevrank_basic -----
    #[test]
    fn test_zrevrank_basic() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("z"),
                bulk("1"),
                bulk("a"),
                bulk("2"),
                bulk("b"),
                bulk("3"),
                bulk("c"),
            ],
        )
        .unwrap();

        assert_eq!(
            zrevrank(&mut ctx, &[bulk("z"), bulk("a")]).unwrap(),
            RespValue::Integer(2)
        );
        assert_eq!(
            zrevrank(&mut ctx, &[bulk("z"), bulk("b")]).unwrap(),
            RespValue::Integer(1)
        );
        assert_eq!(
            zrevrank(&mut ctx, &[bulk("z"), bulk("c")]).unwrap(),
            RespValue::Integer(0)
        );
    }

    // ----- 10. test_zcard -----
    #[test]
    fn test_zcard() {
        let mut ctx = make_ctx();
        assert_eq!(
            zcard(&mut ctx, &[bulk("z")]).unwrap(),
            RespValue::Integer(0)
        );

        zadd(
            &mut ctx,
            &[bulk("z"), bulk("1"), bulk("a"), bulk("2"), bulk("b")],
        )
        .unwrap();
        assert_eq!(
            zcard(&mut ctx, &[bulk("z")]).unwrap(),
            RespValue::Integer(2)
        );
    }

    // ----- 11. test_zcount_basic -----
    #[test]
    fn test_zcount_basic() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("z"),
                bulk("1"),
                bulk("a"),
                bulk("2"),
                bulk("b"),
                bulk("3"),
                bulk("c"),
                bulk("4"),
                bulk("d"),
                bulk("5"),
                bulk("e"),
            ],
        )
        .unwrap();

        let r = zcount(&mut ctx, &[bulk("z"), bulk("-inf"), bulk("+inf")]).unwrap();
        assert_eq!(r, RespValue::Integer(5));

        let r = zcount(&mut ctx, &[bulk("z"), bulk("2"), bulk("4")]).unwrap();
        assert_eq!(r, RespValue::Integer(3));

        let r = zcount(&mut ctx, &[bulk("z"), bulk("(2"), bulk("(4")]).unwrap();
        assert_eq!(r, RespValue::Integer(1));
    }

    // ----- 12. test_zrange_by_rank -----
    #[test]
    fn test_zrange_by_rank() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("z"),
                bulk("1"),
                bulk("a"),
                bulk("2"),
                bulk("b"),
                bulk("3"),
                bulk("c"),
            ],
        )
        .unwrap();

        let r = zrange(&mut ctx, &[bulk("z"), bulk("0"), bulk("-1")]).unwrap();
        assert_eq!(r, RespValue::Array(vec![bulk("a"), bulk("b"), bulk("c"),]));

        let r = zrange(&mut ctx, &[bulk("z"), bulk("0"), bulk("1")]).unwrap();
        assert_eq!(r, RespValue::Array(vec![bulk("a"), bulk("b")]));
    }

    // ----- 13. test_zrange_withscores -----
    #[test]
    fn test_zrange_withscores() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[bulk("z"), bulk("1"), bulk("a"), bulk("2"), bulk("b")],
        )
        .unwrap();

        let r = zrange(
            &mut ctx,
            &[bulk("z"), bulk("0"), bulk("-1"), bulk("WITHSCORES")],
        )
        .unwrap();
        assert_eq!(
            r,
            RespValue::Array(vec![bulk("a"), bulk("1"), bulk("b"), bulk("2"),])
        );
    }

    // ----- 14. test_zrevrange_basic -----
    #[test]
    fn test_zrevrange_basic() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("z"),
                bulk("1"),
                bulk("a"),
                bulk("2"),
                bulk("b"),
                bulk("3"),
                bulk("c"),
            ],
        )
        .unwrap();

        let r = zrevrange(&mut ctx, &[bulk("z"), bulk("0"), bulk("-1")]).unwrap();
        assert_eq!(r, RespValue::Array(vec![bulk("c"), bulk("b"), bulk("a")]));
    }

    // ----- 15. test_zrangebyscore_basic -----
    #[test]
    fn test_zrangebyscore_basic() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("z"),
                bulk("1"),
                bulk("a"),
                bulk("2"),
                bulk("b"),
                bulk("3"),
                bulk("c"),
            ],
        )
        .unwrap();

        let r = zrangebyscore(&mut ctx, &[bulk("z"), bulk("-inf"), bulk("+inf")]).unwrap();
        assert_eq!(r, RespValue::Array(vec![bulk("a"), bulk("b"), bulk("c")]));

        let r = zrangebyscore(&mut ctx, &[bulk("z"), bulk("1"), bulk("2")]).unwrap();
        assert_eq!(r, RespValue::Array(vec![bulk("a"), bulk("b")]));
    }

    // ----- 16. test_zrangebyscore_exclusive -----
    #[test]
    fn test_zrangebyscore_exclusive() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("z"),
                bulk("1"),
                bulk("a"),
                bulk("2"),
                bulk("b"),
                bulk("3"),
                bulk("c"),
            ],
        )
        .unwrap();

        let r = zrangebyscore(&mut ctx, &[bulk("z"), bulk("(1"), bulk("(3")]).unwrap();
        assert_eq!(r, RespValue::Array(vec![bulk("b")]));
    }

    // ----- 17. test_zrangebyscore_inf -----
    #[test]
    fn test_zrangebyscore_inf() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("z"),
                bulk("1"),
                bulk("a"),
                bulk("2"),
                bulk("b"),
                bulk("3"),
                bulk("c"),
            ],
        )
        .unwrap();

        let r = zrangebyscore(&mut ctx, &[bulk("z"), bulk("-inf"), bulk("+inf")]).unwrap();
        if let RespValue::Array(arr) = r {
            assert_eq!(arr.len(), 3);
        } else {
            panic!("expected array");
        }
    }

    // ----- 18. test_zincrby_basic -----
    #[test]
    fn test_zincrby_basic() {
        let mut ctx = make_ctx();
        zadd(&mut ctx, &[bulk("z"), bulk("5"), bulk("a")]).unwrap();

        let r = zincrby(&mut ctx, &[bulk("z"), bulk("3"), bulk("a")]).unwrap();
        assert_eq!(r, RespValue::BulkString(Bytes::from("8")));

        let r = zincrby(&mut ctx, &[bulk("z"), bulk("10"), bulk("b")]).unwrap();
        assert_eq!(r, RespValue::BulkString(Bytes::from("10")));
    }

    // ----- 19. test_zpopmin_basic -----
    #[test]
    fn test_zpopmin_basic() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("z"),
                bulk("1"),
                bulk("a"),
                bulk("2"),
                bulk("b"),
                bulk("3"),
                bulk("c"),
            ],
        )
        .unwrap();

        let r = zpopmin(&mut ctx, &[bulk("z")]).unwrap();
        assert_eq!(r, RespValue::Array(vec![bulk("a"), bulk("1")]));

        let r = zpopmin(&mut ctx, &[bulk("z"), bulk("2")]).unwrap();
        assert_eq!(
            r,
            RespValue::Array(vec![bulk("b"), bulk("2"), bulk("c"), bulk("3")])
        );

        assert_eq!(
            zcard(&mut ctx, &[bulk("z")]).unwrap(),
            RespValue::Integer(0)
        );
    }

    // ----- 20. test_zpopmax_basic -----
    #[test]
    fn test_zpopmax_basic() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("z"),
                bulk("1"),
                bulk("a"),
                bulk("2"),
                bulk("b"),
                bulk("3"),
                bulk("c"),
            ],
        )
        .unwrap();

        let r = zpopmax(&mut ctx, &[bulk("z")]).unwrap();
        assert_eq!(r, RespValue::Array(vec![bulk("c"), bulk("3")]));

        let r = zpopmax(&mut ctx, &[bulk("z"), bulk("5")]).unwrap();
        assert_eq!(
            r,
            RespValue::Array(vec![bulk("b"), bulk("2"), bulk("a"), bulk("1")])
        );
    }

    // ----- 21. test_zunionstore_basic -----
    #[test]
    fn test_zunionstore_basic() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[bulk("z1"), bulk("1"), bulk("a"), bulk("2"), bulk("b")],
        )
        .unwrap();
        zadd(
            &mut ctx,
            &[bulk("z2"), bulk("3"), bulk("b"), bulk("4"), bulk("c")],
        )
        .unwrap();

        let r = zunionstore(&mut ctx, &[bulk("dest"), bulk("2"), bulk("z1"), bulk("z2")]).unwrap();
        assert_eq!(r, RespValue::Integer(3));

        assert_eq!(
            zscore(&mut ctx, &[bulk("dest"), bulk("b")]).unwrap(),
            RespValue::BulkString(Bytes::from("5"))
        );
        assert_eq!(
            zscore(&mut ctx, &[bulk("dest"), bulk("a")]).unwrap(),
            RespValue::BulkString(Bytes::from("1"))
        );
        assert_eq!(
            zscore(&mut ctx, &[bulk("dest"), bulk("c")]).unwrap(),
            RespValue::BulkString(Bytes::from("4"))
        );
    }

    // ----- 22. test_zinterstore_basic -----
    #[test]
    fn test_zinterstore_basic() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[bulk("z1"), bulk("1"), bulk("a"), bulk("2"), bulk("b")],
        )
        .unwrap();
        zadd(
            &mut ctx,
            &[bulk("z2"), bulk("3"), bulk("b"), bulk("4"), bulk("c")],
        )
        .unwrap();

        let r = zinterstore(&mut ctx, &[bulk("dest"), bulk("2"), bulk("z1"), bulk("z2")]).unwrap();
        assert_eq!(r, RespValue::Integer(1));

        assert_eq!(
            zscore(&mut ctx, &[bulk("dest"), bulk("b")]).unwrap(),
            RespValue::BulkString(Bytes::from("5"))
        );
    }

    // ----- 23. test_zmscore_basic -----
    #[test]
    fn test_zmscore_basic() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[bulk("z"), bulk("1"), bulk("a"), bulk("2"), bulk("b")],
        )
        .unwrap();

        let r = zmscore(&mut ctx, &[bulk("z"), bulk("a"), bulk("b"), bulk("c")]).unwrap();
        assert_eq!(
            r,
            RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("1")),
                RespValue::BulkString(Bytes::from("2")),
                RespValue::Null,
            ])
        );
    }

    // ----- 24. test_zrandmember_basic -----
    #[test]
    fn test_zrandmember_basic() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("z"),
                bulk("1"),
                bulk("a"),
                bulk("2"),
                bulk("b"),
                bulk("3"),
                bulk("c"),
            ],
        )
        .unwrap();

        let r = zrandmember(&mut ctx, &[bulk("z")]).unwrap();
        match r {
            RespValue::BulkString(b) => {
                let s = String::from_utf8_lossy(&b).to_string();
                assert!(s == "a" || s == "b" || s == "c");
            }
            _ => panic!("Expected bulk string"),
        }

        let r = zrandmember(&mut ctx, &[bulk("z"), bulk("2")]).unwrap();
        if let RespValue::Array(arr) = r {
            assert_eq!(arr.len(), 2);
        } else {
            panic!("Expected array");
        }

        assert_eq!(
            zrandmember(&mut ctx, &[bulk("nope")]).unwrap(),
            RespValue::Null
        );
    }

    // ----- 25. test_zrangebylex_basic -----
    #[test]
    fn test_zrangebylex_basic() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("z"),
                bulk("0"),
                bulk("a"),
                bulk("0"),
                bulk("b"),
                bulk("0"),
                bulk("c"),
                bulk("0"),
                bulk("d"),
                bulk("0"),
                bulk("e"),
            ],
        )
        .unwrap();

        let r = zrangebylex(&mut ctx, &[bulk("z"), bulk("-"), bulk("+")]).unwrap();
        assert_eq!(
            r,
            RespValue::Array(vec![bulk("a"), bulk("b"), bulk("c"), bulk("d"), bulk("e")])
        );

        let r = zrangebylex(&mut ctx, &[bulk("z"), bulk("[b"), bulk("[d")]).unwrap();
        assert_eq!(r, RespValue::Array(vec![bulk("b"), bulk("c"), bulk("d")]));

        let r = zrangebylex(&mut ctx, &[bulk("z"), bulk("(a"), bulk("(d")]).unwrap();
        assert_eq!(r, RespValue::Array(vec![bulk("b"), bulk("c")]));
    }

    // ----- 26. test_zlexcount_basic -----
    #[test]
    fn test_zlexcount_basic() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("z"),
                bulk("0"),
                bulk("a"),
                bulk("0"),
                bulk("b"),
                bulk("0"),
                bulk("c"),
                bulk("0"),
                bulk("d"),
            ],
        )
        .unwrap();

        let r = zlexcount(&mut ctx, &[bulk("z"), bulk("-"), bulk("+")]).unwrap();
        assert_eq!(r, RespValue::Integer(4));

        let r = zlexcount(&mut ctx, &[bulk("z"), bulk("[b"), bulk("[c")]).unwrap();
        assert_eq!(r, RespValue::Integer(2));

        let r = zlexcount(&mut ctx, &[bulk("z"), bulk("(a"), bulk("(d")]).unwrap();
        assert_eq!(r, RespValue::Integer(2));
    }

    // ----- 27. test_zremrangebyrank -----
    #[test]
    fn test_zremrangebyrank() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("z"),
                bulk("1"),
                bulk("a"),
                bulk("2"),
                bulk("b"),
                bulk("3"),
                bulk("c"),
                bulk("4"),
                bulk("d"),
            ],
        )
        .unwrap();

        let r = zremrangebyrank(&mut ctx, &[bulk("z"), bulk("1"), bulk("2")]).unwrap();
        assert_eq!(r, RespValue::Integer(2));

        assert_eq!(
            zcard(&mut ctx, &[bulk("z")]).unwrap(),
            RespValue::Integer(2)
        );
        assert_eq!(
            zscore(&mut ctx, &[bulk("z"), bulk("b")]).unwrap(),
            RespValue::Null
        );
        assert_eq!(
            zscore(&mut ctx, &[bulk("z"), bulk("c")]).unwrap(),
            RespValue::Null
        );
    }

    // ----- 28. test_zremrangebyscore -----
    #[test]
    fn test_zremrangebyscore() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("z"),
                bulk("1"),
                bulk("a"),
                bulk("2"),
                bulk("b"),
                bulk("3"),
                bulk("c"),
                bulk("4"),
                bulk("d"),
            ],
        )
        .unwrap();

        let r = zremrangebyscore(&mut ctx, &[bulk("z"), bulk("2"), bulk("3")]).unwrap();
        assert_eq!(r, RespValue::Integer(2));

        assert_eq!(
            zcard(&mut ctx, &[bulk("z")]).unwrap(),
            RespValue::Integer(2)
        );
        assert_eq!(
            zscore(&mut ctx, &[bulk("z"), bulk("a")]).unwrap(),
            RespValue::BulkString(Bytes::from("1"))
        );
        assert_eq!(
            zscore(&mut ctx, &[bulk("z"), bulk("d")]).unwrap(),
            RespValue::BulkString(Bytes::from("4"))
        );
    }

    // ----- 29. test_zscan_basic -----
    #[test]
    fn test_zscan_basic() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[bulk("z"), bulk("1"), bulk("alpha"), bulk("2"), bulk("beta")],
        )
        .unwrap();

        let r = zscan(&mut ctx, &[bulk("z"), bulk("0")]).unwrap();
        if let RespValue::Array(outer) = r {
            assert_eq!(outer.len(), 2);
            if let RespValue::BulkString(c) = &outer[0] {
                assert_eq!(c.as_ref(), b"0");
            }
            if let RespValue::Array(elements) = &outer[1] {
                assert_eq!(elements.len(), 4);
            } else {
                panic!("expected inner array");
            }
        } else {
            panic!("expected array");
        }

        // ZSCAN with MATCH
        let r = zscan(
            &mut ctx,
            &[bulk("z"), bulk("0"), bulk("MATCH"), bulk("al*")],
        )
        .unwrap();
        if let RespValue::Array(outer) = r {
            if let RespValue::Array(elements) = &outer[1] {
                assert_eq!(elements.len(), 2);
            }
        }
    }

    // ----- 30. test_wrong_type_error -----
    #[test]
    fn test_wrong_type_error() {
        let mut ctx = make_ctx();
        set_string(&mut ctx, "mykey", "hello");

        assert!(matches!(
            zadd(&mut ctx, &[bulk("mykey"), bulk("1"), bulk("a")]),
            Err(CommandError::WrongType)
        ));
        assert!(matches!(
            zrem(&mut ctx, &[bulk("mykey"), bulk("a")]),
            Err(CommandError::WrongType)
        ));
        assert!(matches!(
            zscore(&mut ctx, &[bulk("mykey"), bulk("a")]),
            Err(CommandError::WrongType)
        ));
        assert!(matches!(
            zrank(&mut ctx, &[bulk("mykey"), bulk("a")]),
            Err(CommandError::WrongType)
        ));
        assert!(matches!(
            zrevrank(&mut ctx, &[bulk("mykey"), bulk("a")]),
            Err(CommandError::WrongType)
        ));
        assert!(matches!(
            zcard(&mut ctx, &[bulk("mykey")]),
            Err(CommandError::WrongType)
        ));
        assert!(matches!(
            zcount(&mut ctx, &[bulk("mykey"), bulk("0"), bulk("1")]),
            Err(CommandError::WrongType)
        ));
        assert!(matches!(
            zrange(&mut ctx, &[bulk("mykey"), bulk("0"), bulk("-1")]),
            Err(CommandError::WrongType)
        ));
        assert!(matches!(
            zpopmin(&mut ctx, &[bulk("mykey")]),
            Err(CommandError::WrongType)
        ));
        assert!(matches!(
            zpopmax(&mut ctx, &[bulk("mykey")]),
            Err(CommandError::WrongType)
        ));
    }

    // ----- Additional tests -----

    #[test]
    fn test_zrevrangebyscore() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("z"),
                bulk("1"),
                bulk("a"),
                bulk("2"),
                bulk("b"),
                bulk("3"),
                bulk("c"),
            ],
        )
        .unwrap();

        let r = zrevrangebyscore(&mut ctx, &[bulk("z"), bulk("+inf"), bulk("-inf")]).unwrap();
        assert_eq!(r, RespValue::Array(vec![bulk("c"), bulk("b"), bulk("a")]));
    }

    #[test]
    fn test_zrangestore_basic() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("src"),
                bulk("1"),
                bulk("a"),
                bulk("2"),
                bulk("b"),
                bulk("3"),
                bulk("c"),
            ],
        )
        .unwrap();

        let r = zrangestore(&mut ctx, &[bulk("dst"), bulk("src"), bulk("0"), bulk("1")]).unwrap();
        assert_eq!(r, RespValue::Integer(2));

        assert_eq!(
            zcard(&mut ctx, &[bulk("dst")]).unwrap(),
            RespValue::Integer(2)
        );
    }

    #[test]
    fn test_zunionstore_with_weights() {
        let mut ctx = make_ctx();
        zadd(&mut ctx, &[bulk("z1"), bulk("1"), bulk("a")]).unwrap();
        zadd(&mut ctx, &[bulk("z2"), bulk("2"), bulk("a")]).unwrap();

        zunionstore(
            &mut ctx,
            &[
                bulk("out"),
                bulk("2"),
                bulk("z1"),
                bulk("z2"),
                bulk("WEIGHTS"),
                bulk("2"),
                bulk("3"),
            ],
        )
        .unwrap();

        assert_eq!(
            zscore(&mut ctx, &[bulk("out"), bulk("a")]).unwrap(),
            RespValue::BulkString(Bytes::from("8"))
        );
    }

    #[test]
    fn test_zinterstore_with_aggregate_min() {
        let mut ctx = make_ctx();
        zadd(&mut ctx, &[bulk("z1"), bulk("5"), bulk("a")]).unwrap();
        zadd(&mut ctx, &[bulk("z2"), bulk("3"), bulk("a")]).unwrap();

        zinterstore(
            &mut ctx,
            &[
                bulk("out"),
                bulk("2"),
                bulk("z1"),
                bulk("z2"),
                bulk("AGGREGATE"),
                bulk("MIN"),
            ],
        )
        .unwrap();

        assert_eq!(
            zscore(&mut ctx, &[bulk("out"), bulk("a")]).unwrap(),
            RespValue::BulkString(Bytes::from("3"))
        );
    }

    #[test]
    fn test_zremrangebylex() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("z"),
                bulk("0"),
                bulk("a"),
                bulk("0"),
                bulk("b"),
                bulk("0"),
                bulk("c"),
                bulk("0"),
                bulk("d"),
            ],
        )
        .unwrap();

        let r = zremrangebylex(&mut ctx, &[bulk("z"), bulk("[b"), bulk("[c")]).unwrap();
        assert_eq!(r, RespValue::Integer(2));
        assert_eq!(
            zcard(&mut ctx, &[bulk("z")]).unwrap(),
            RespValue::Integer(2)
        );
    }

    #[test]
    fn test_zrevrangebylex() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("z"),
                bulk("0"),
                bulk("a"),
                bulk("0"),
                bulk("b"),
                bulk("0"),
                bulk("c"),
            ],
        )
        .unwrap();

        let r = zrevrangebylex(&mut ctx, &[bulk("z"), bulk("+"), bulk("-")]).unwrap();
        assert_eq!(r, RespValue::Array(vec![bulk("c"), bulk("b"), bulk("a")]));
    }

    #[test]
    fn test_zadd_wrong_arity() {
        let mut ctx = make_ctx();
        assert!(matches!(
            zadd(&mut ctx, &[bulk("z")]),
            Err(CommandError::WrongArity(_))
        ));
        assert!(matches!(
            zadd(&mut ctx, &[bulk("z"), bulk("1")]),
            Err(CommandError::WrongArity(_))
        ));
    }

    #[test]
    fn test_zpopmin_empty() {
        let mut ctx = make_ctx();
        let r = zpopmin(&mut ctx, &[bulk("z")]).unwrap();
        assert_eq!(r, RespValue::Array(vec![]));
    }

    #[test]
    fn test_zrange_byscore() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("z"),
                bulk("1"),
                bulk("a"),
                bulk("2"),
                bulk("b"),
                bulk("3"),
                bulk("c"),
            ],
        )
        .unwrap();

        let r = zrange(
            &mut ctx,
            &[bulk("z"), bulk("1"), bulk("2"), bulk("BYSCORE")],
        )
        .unwrap();
        assert_eq!(r, RespValue::Array(vec![bulk("a"), bulk("b")]));
    }

    #[test]
    fn test_zrangebyscore_with_limit() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("z"),
                bulk("1"),
                bulk("a"),
                bulk("2"),
                bulk("b"),
                bulk("3"),
                bulk("c"),
                bulk("4"),
                bulk("d"),
            ],
        )
        .unwrap();

        let r = zrangebyscore(
            &mut ctx,
            &[
                bulk("z"),
                bulk("-inf"),
                bulk("+inf"),
                bulk("LIMIT"),
                bulk("1"),
                bulk("2"),
            ],
        )
        .unwrap();
        assert_eq!(r, RespValue::Array(vec![bulk("b"), bulk("c")]));
    }

    #[test]
    fn test_zmscore_nonexistent_key() {
        let mut ctx = make_ctx();
        let r = zmscore(&mut ctx, &[bulk("nokey"), bulk("a"), bulk("b")]).unwrap();
        assert_eq!(r, RespValue::Array(vec![RespValue::Null, RespValue::Null]));
    }

    #[test]
    fn test_zincrby_creates_key() {
        let mut ctx = make_ctx();
        let r = zincrby(&mut ctx, &[bulk("newz"), bulk("5"), bulk("m")]).unwrap();
        assert_eq!(r, RespValue::BulkString(Bytes::from("5")));
        assert_eq!(
            zcard(&mut ctx, &[bulk("newz")]).unwrap(),
            RespValue::Integer(1)
        );
    }

    // ===== ZMPOP tests =====

    #[test]
    fn test_zmpop_min_basic() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("z"),
                bulk("1"),
                bulk("a"),
                bulk("2"),
                bulk("b"),
                bulk("3"),
                bulk("c"),
            ],
        )
        .unwrap();

        let r = zmpop(&mut ctx, &[bulk("1"), bulk("z"), bulk("MIN")]).unwrap();
        if let RespValue::Array(outer) = r {
            assert_eq!(outer[0], RespValue::BulkString(Bytes::from("z")));
            if let RespValue::Array(inner) = &outer[1] {
                assert_eq!(inner[0], bulk("a"));
                assert_eq!(inner[1], bulk("1"));
            } else {
                panic!("expected inner array");
            }
        } else {
            panic!("expected array");
        }
        assert_eq!(
            zcard(&mut ctx, &[bulk("z")]).unwrap(),
            RespValue::Integer(2)
        );
    }

    #[test]
    fn test_zmpop_max_with_count() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("z"),
                bulk("1"),
                bulk("a"),
                bulk("2"),
                bulk("b"),
                bulk("3"),
                bulk("c"),
            ],
        )
        .unwrap();

        let r = zmpop(
            &mut ctx,
            &[bulk("1"), bulk("z"), bulk("MAX"), bulk("COUNT"), bulk("2")],
        )
        .unwrap();
        if let RespValue::Array(outer) = r {
            if let RespValue::Array(inner) = &outer[1] {
                assert_eq!(inner.len(), 4); // 2 member-score pairs
                assert_eq!(inner[0], bulk("c"));
                assert_eq!(inner[1], bulk("3"));
                assert_eq!(inner[2], bulk("b"));
                assert_eq!(inner[3], bulk("2"));
            } else {
                panic!("expected inner array");
            }
        } else {
            panic!("expected array");
        }
        assert_eq!(
            zcard(&mut ctx, &[bulk("z")]).unwrap(),
            RespValue::Integer(1)
        );
    }

    #[test]
    fn test_zmpop_empty_keys() {
        let mut ctx = make_ctx();
        let r = zmpop(&mut ctx, &[bulk("1"), bulk("empty"), bulk("MIN")]).unwrap();
        assert_eq!(r, RespValue::Null);
    }

    #[test]
    fn test_zmpop_multiple_keys_first_nonempty() {
        let mut ctx = make_ctx();
        // z1 is empty (doesn't exist), z2 has data
        zadd(&mut ctx, &[bulk("z2"), bulk("5"), bulk("x")]).unwrap();

        let r = zmpop(&mut ctx, &[bulk("2"), bulk("z1"), bulk("z2"), bulk("MIN")]).unwrap();
        if let RespValue::Array(outer) = r {
            assert_eq!(outer[0], RespValue::BulkString(Bytes::from("z2")));
        } else {
            panic!("expected array");
        }
    }

    #[test]
    fn test_zmpop_wrong_type() {
        let mut ctx = make_ctx();
        set_string(&mut ctx, "s", "hello");
        assert!(matches!(
            zmpop(&mut ctx, &[bulk("1"), bulk("s"), bulk("MIN")]),
            Err(CommandError::WrongType)
        ));
    }

    #[test]
    fn test_zmpop_wrong_arity() {
        let mut ctx = make_ctx();
        assert!(matches!(zmpop(&mut ctx, &[bulk("1")]), Err(_)));
    }

    // ===== ZUNION tests =====

    #[test]
    fn test_zunion_basic() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[bulk("z1"), bulk("1"), bulk("a"), bulk("2"), bulk("b")],
        )
        .unwrap();
        zadd(
            &mut ctx,
            &[bulk("z2"), bulk("3"), bulk("b"), bulk("4"), bulk("c")],
        )
        .unwrap();

        let r = zunion(&mut ctx, &[bulk("2"), bulk("z1"), bulk("z2")]).unwrap();
        if let RespValue::Array(arr) = r {
            // a(1), b(5), c(4) sorted by score: a(1), c(4), b(5)
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], bulk("a"));
            assert_eq!(arr[1], bulk("c"));
            assert_eq!(arr[2], bulk("b"));
        } else {
            panic!("expected array");
        }
    }

    #[test]
    fn test_zunion_withscores() {
        let mut ctx = make_ctx();
        zadd(&mut ctx, &[bulk("z1"), bulk("1"), bulk("a")]).unwrap();
        zadd(&mut ctx, &[bulk("z2"), bulk("2"), bulk("a")]).unwrap();

        let r = zunion(
            &mut ctx,
            &[bulk("2"), bulk("z1"), bulk("z2"), bulk("WITHSCORES")],
        )
        .unwrap();
        if let RespValue::Array(arr) = r {
            assert_eq!(arr.len(), 2); // 1 member-score pair
            assert_eq!(arr[0], bulk("a"));
            assert_eq!(arr[1], bulk("3")); // 1+2=3
        } else {
            panic!("expected array");
        }
    }

    #[test]
    fn test_zunion_weights() {
        let mut ctx = make_ctx();
        zadd(&mut ctx, &[bulk("z1"), bulk("1"), bulk("a")]).unwrap();
        zadd(&mut ctx, &[bulk("z2"), bulk("2"), bulk("a")]).unwrap();

        let r = zunion(
            &mut ctx,
            &[
                bulk("2"),
                bulk("z1"),
                bulk("z2"),
                bulk("WEIGHTS"),
                bulk("2"),
                bulk("3"),
                bulk("WITHSCORES"),
            ],
        )
        .unwrap();
        if let RespValue::Array(arr) = r {
            assert_eq!(arr[0], bulk("a"));
            assert_eq!(arr[1], bulk("8")); // 1*2 + 2*3 = 8
        } else {
            panic!("expected array");
        }
    }

    #[test]
    fn test_zunion_aggregate_max() {
        let mut ctx = make_ctx();
        zadd(&mut ctx, &[bulk("z1"), bulk("5"), bulk("a")]).unwrap();
        zadd(&mut ctx, &[bulk("z2"), bulk("3"), bulk("a")]).unwrap();

        let r = zunion(
            &mut ctx,
            &[
                bulk("2"),
                bulk("z1"),
                bulk("z2"),
                bulk("AGGREGATE"),
                bulk("MAX"),
                bulk("WITHSCORES"),
            ],
        )
        .unwrap();
        if let RespValue::Array(arr) = r {
            assert_eq!(arr[1], bulk("5")); // max(5,3) = 5
        } else {
            panic!("expected array");
        }
    }

    #[test]
    fn test_zunion_empty_key() {
        let mut ctx = make_ctx();
        zadd(&mut ctx, &[bulk("z1"), bulk("1"), bulk("a")]).unwrap();

        let r = zunion(&mut ctx, &[bulk("2"), bulk("z1"), bulk("empty")]).unwrap();
        if let RespValue::Array(arr) = r {
            assert_eq!(arr.len(), 1);
            assert_eq!(arr[0], bulk("a"));
        } else {
            panic!("expected array");
        }
    }

    // ===== ZINTER tests =====

    #[test]
    fn test_zinter_basic() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[bulk("z1"), bulk("1"), bulk("a"), bulk("2"), bulk("b")],
        )
        .unwrap();
        zadd(
            &mut ctx,
            &[bulk("z2"), bulk("3"), bulk("b"), bulk("4"), bulk("c")],
        )
        .unwrap();

        let r = zinter(&mut ctx, &[bulk("2"), bulk("z1"), bulk("z2")]).unwrap();
        if let RespValue::Array(arr) = r {
            assert_eq!(arr.len(), 1); // only "b" is in both
            assert_eq!(arr[0], bulk("b"));
        } else {
            panic!("expected array");
        }
    }

    #[test]
    fn test_zinter_withscores() {
        let mut ctx = make_ctx();
        zadd(&mut ctx, &[bulk("z1"), bulk("1"), bulk("a")]).unwrap();
        zadd(&mut ctx, &[bulk("z2"), bulk("2"), bulk("a")]).unwrap();

        let r = zinter(
            &mut ctx,
            &[bulk("2"), bulk("z1"), bulk("z2"), bulk("WITHSCORES")],
        )
        .unwrap();
        if let RespValue::Array(arr) = r {
            assert_eq!(arr[0], bulk("a"));
            assert_eq!(arr[1], bulk("3")); // 1+2=3
        } else {
            panic!("expected array");
        }
    }

    #[test]
    fn test_zinter_no_intersection() {
        let mut ctx = make_ctx();
        zadd(&mut ctx, &[bulk("z1"), bulk("1"), bulk("a")]).unwrap();
        zadd(&mut ctx, &[bulk("z2"), bulk("2"), bulk("b")]).unwrap();

        let r = zinter(&mut ctx, &[bulk("2"), bulk("z1"), bulk("z2")]).unwrap();
        assert_eq!(r, RespValue::Array(vec![]));
    }

    #[test]
    fn test_zinter_aggregate_min() {
        let mut ctx = make_ctx();
        zadd(&mut ctx, &[bulk("z1"), bulk("5"), bulk("a")]).unwrap();
        zadd(&mut ctx, &[bulk("z2"), bulk("3"), bulk("a")]).unwrap();

        let r = zinter(
            &mut ctx,
            &[
                bulk("2"),
                bulk("z1"),
                bulk("z2"),
                bulk("AGGREGATE"),
                bulk("MIN"),
                bulk("WITHSCORES"),
            ],
        )
        .unwrap();
        if let RespValue::Array(arr) = r {
            assert_eq!(arr[1], bulk("3")); // min(5,3) = 3
        } else {
            panic!("expected array");
        }
    }

    #[test]
    fn test_zinter_empty_key() {
        let mut ctx = make_ctx();
        zadd(&mut ctx, &[bulk("z1"), bulk("1"), bulk("a")]).unwrap();

        let r = zinter(&mut ctx, &[bulk("2"), bulk("z1"), bulk("empty")]).unwrap();
        assert_eq!(r, RespValue::Array(vec![]));
    }

    // ===== ZDIFF tests =====

    #[test]
    fn test_zdiff_basic() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("z1"),
                bulk("1"),
                bulk("a"),
                bulk("2"),
                bulk("b"),
                bulk("3"),
                bulk("c"),
            ],
        )
        .unwrap();
        zadd(&mut ctx, &[bulk("z2"), bulk("2"), bulk("b")]).unwrap();

        let r = zdiff(&mut ctx, &[bulk("2"), bulk("z1"), bulk("z2")]).unwrap();
        if let RespValue::Array(arr) = r {
            assert_eq!(arr.len(), 2); // a and c remain
            assert_eq!(arr[0], bulk("a"));
            assert_eq!(arr[1], bulk("c"));
        } else {
            panic!("expected array");
        }
    }

    #[test]
    fn test_zdiff_withscores() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[bulk("z1"), bulk("1"), bulk("a"), bulk("2"), bulk("b")],
        )
        .unwrap();
        zadd(&mut ctx, &[bulk("z2"), bulk("2"), bulk("b")]).unwrap();

        let r = zdiff(
            &mut ctx,
            &[bulk("2"), bulk("z1"), bulk("z2"), bulk("WITHSCORES")],
        )
        .unwrap();
        if let RespValue::Array(arr) = r {
            assert_eq!(arr.len(), 2); // a + score
            assert_eq!(arr[0], bulk("a"));
            assert_eq!(arr[1], bulk("1"));
        } else {
            panic!("expected array");
        }
    }

    #[test]
    fn test_zdiff_no_difference() {
        let mut ctx = make_ctx();
        zadd(&mut ctx, &[bulk("z1"), bulk("1"), bulk("a")]).unwrap();
        zadd(&mut ctx, &[bulk("z2"), bulk("2"), bulk("a")]).unwrap();

        let r = zdiff(&mut ctx, &[bulk("2"), bulk("z1"), bulk("z2")]).unwrap();
        assert_eq!(r, RespValue::Array(vec![]));
    }

    #[test]
    fn test_zdiff_nonexistent_second_key() {
        let mut ctx = make_ctx();
        zadd(&mut ctx, &[bulk("z1"), bulk("1"), bulk("a")]).unwrap();

        let r = zdiff(&mut ctx, &[bulk("2"), bulk("z1"), bulk("empty")]).unwrap();
        if let RespValue::Array(arr) = r {
            assert_eq!(arr.len(), 1);
            assert_eq!(arr[0], bulk("a"));
        } else {
            panic!("expected array");
        }
    }

    #[test]
    fn test_zdiff_nonexistent_first_key() {
        let mut ctx = make_ctx();
        zadd(&mut ctx, &[bulk("z2"), bulk("1"), bulk("a")]).unwrap();

        let r = zdiff(&mut ctx, &[bulk("2"), bulk("empty"), bulk("z2")]).unwrap();
        assert_eq!(r, RespValue::Array(vec![]));
    }

    // ===== ZDIFFSTORE tests =====

    #[test]
    fn test_zdiffstore_basic() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("z1"),
                bulk("1"),
                bulk("a"),
                bulk("2"),
                bulk("b"),
                bulk("3"),
                bulk("c"),
            ],
        )
        .unwrap();
        zadd(&mut ctx, &[bulk("z2"), bulk("2"), bulk("b")]).unwrap();

        let r = zdiffstore(&mut ctx, &[bulk("dest"), bulk("2"), bulk("z1"), bulk("z2")]).unwrap();
        assert_eq!(r, RespValue::Integer(2));
        assert_eq!(
            zcard(&mut ctx, &[bulk("dest")]).unwrap(),
            RespValue::Integer(2)
        );
        assert_eq!(
            zscore(&mut ctx, &[bulk("dest"), bulk("a")]).unwrap(),
            RespValue::BulkString(Bytes::from("1"))
        );
        assert_eq!(
            zscore(&mut ctx, &[bulk("dest"), bulk("c")]).unwrap(),
            RespValue::BulkString(Bytes::from("3"))
        );
    }

    #[test]
    fn test_zdiffstore_empty_result() {
        let mut ctx = make_ctx();
        zadd(&mut ctx, &[bulk("z1"), bulk("1"), bulk("a")]).unwrap();
        zadd(&mut ctx, &[bulk("z2"), bulk("2"), bulk("a")]).unwrap();

        let r = zdiffstore(&mut ctx, &[bulk("dest"), bulk("2"), bulk("z1"), bulk("z2")]).unwrap();
        assert_eq!(r, RespValue::Integer(0));
    }

    #[test]
    fn test_zdiffstore_wrong_type() {
        let mut ctx = make_ctx();
        set_string(&mut ctx, "s", "hello");
        assert!(matches!(
            zdiffstore(&mut ctx, &[bulk("dest"), bulk("1"), bulk("s")]),
            Err(CommandError::WrongType)
        ));
    }

    #[test]
    fn test_zdiffstore_wrong_arity() {
        let mut ctx = make_ctx();
        assert!(matches!(
            zdiffstore(&mut ctx, &[bulk("dest"), bulk("1")]),
            Err(CommandError::WrongArity(_))
        ));
        // numkeys says 2 but only 1 key provided
        assert!(matches!(
            zdiffstore(&mut ctx, &[bulk("dest"), bulk("2"), bulk("z1")]),
            Err(CommandError::SyntaxError)
        ));
    }

    // ===== ZRANGE options tests =====

    // 1. test_zrange_bylex
    #[test]
    fn test_zrange_bylex() {
        let mut ctx = make_ctx();
        // All members with same score for lex ordering
        zadd(
            &mut ctx,
            &[
                bulk("z"),
                bulk("0"),
                bulk("a"),
                bulk("0"),
                bulk("b"),
                bulk("0"),
                bulk("c"),
                bulk("0"),
                bulk("d"),
                bulk("0"),
                bulk("e"),
            ],
        )
        .unwrap();

        let r = zrange(
            &mut ctx,
            &[bulk("z"), bulk("[b"), bulk("[d"), bulk("BYLEX")],
        )
        .unwrap();
        assert_eq!(r, RespValue::Array(vec![bulk("b"), bulk("c"), bulk("d")]));
    }

    // 2. test_zrange_rev
    #[test]
    fn test_zrange_rev() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("z"),
                bulk("1"),
                bulk("a"),
                bulk("2"),
                bulk("b"),
                bulk("3"),
                bulk("c"),
            ],
        )
        .unwrap();

        let r = zrange(&mut ctx, &[bulk("z"), bulk("0"), bulk("-1"), bulk("REV")]).unwrap();
        assert_eq!(r, RespValue::Array(vec![bulk("c"), bulk("b"), bulk("a")]));
    }

    // 3. test_zrange_byscore_with_limit
    #[test]
    fn test_zrange_byscore_with_limit() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("z"),
                bulk("1"),
                bulk("a"),
                bulk("2"),
                bulk("b"),
                bulk("3"),
                bulk("c"),
                bulk("4"),
                bulk("d"),
                bulk("5"),
                bulk("e"),
            ],
        )
        .unwrap();

        let r = zrange(
            &mut ctx,
            &[
                bulk("z"),
                bulk("0"),
                bulk("100"),
                bulk("BYSCORE"),
                bulk("LIMIT"),
                bulk("1"),
                bulk("2"),
            ],
        )
        .unwrap();
        assert_eq!(r, RespValue::Array(vec![bulk("b"), bulk("c")]));
    }

    // 4. test_zrange_bylex_rev
    #[test]
    fn test_zrange_bylex_rev() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("z"),
                bulk("0"),
                bulk("a"),
                bulk("0"),
                bulk("b"),
                bulk("0"),
                bulk("c"),
                bulk("0"),
                bulk("d"),
            ],
        )
        .unwrap();

        // ZRANGE key + - BYLEX REV → reversed lex order
        let r = zrange(
            &mut ctx,
            &[bulk("z"), bulk("+"), bulk("-"), bulk("BYLEX"), bulk("REV")],
        )
        .unwrap();
        assert_eq!(
            r,
            RespValue::Array(vec![bulk("d"), bulk("c"), bulk("b"), bulk("a")])
        );
    }

    // 5. test_zrange_byscore_rev
    #[test]
    fn test_zrange_byscore_rev() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("z"),
                bulk("1"),
                bulk("a"),
                bulk("2"),
                bulk("b"),
                bulk("3"),
                bulk("c"),
            ],
        )
        .unwrap();

        // ZRANGE key 100 0 BYSCORE REV → reverse score order
        // With REV + BYSCORE, args are swapped internally: min=args[2]=0, max=args[1]=100
        let r = zrange(
            &mut ctx,
            &[
                bulk("z"),
                bulk("100"),
                bulk("0"),
                bulk("BYSCORE"),
                bulk("REV"),
            ],
        )
        .unwrap();
        assert_eq!(r, RespValue::Array(vec![bulk("c"), bulk("b"), bulk("a")]));
    }

    // 6. test_zrange_rev_with_limit
    #[test]
    fn test_zrange_rev_with_limit() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("z"),
                bulk("1"),
                bulk("a"),
                bulk("2"),
                bulk("b"),
                bulk("3"),
                bulk("c"),
                bulk("4"),
                bulk("d"),
            ],
        )
        .unwrap();

        // ZRANGE key 0 -1 REV → d,c,b,a; but LIMIT only works with BYSCORE/BYLEX,
        // so we use BYSCORE to test REV with LIMIT
        let r = zrange(
            &mut ctx,
            &[
                bulk("z"),
                bulk("+inf"),
                bulk("-inf"),
                bulk("BYSCORE"),
                bulk("REV"),
                bulk("LIMIT"),
                bulk("0"),
                bulk("2"),
            ],
        )
        .unwrap();
        assert_eq!(r, RespValue::Array(vec![bulk("d"), bulk("c")]));
    }

    // 7. test_zrange_empty_result
    #[test]
    fn test_zrange_empty_result() {
        let mut ctx = make_ctx();
        // Nonexistent key
        let r = zrange(&mut ctx, &[bulk("nokey"), bulk("0"), bulk("-1")]).unwrap();
        assert_eq!(r, RespValue::Array(vec![]));
    }

    // 8. test_zrange_bylex_withscores
    #[test]
    fn test_zrange_bylex_withscores() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("z"),
                bulk("0"),
                bulk("a"),
                bulk("0"),
                bulk("b"),
                bulk("0"),
                bulk("c"),
            ],
        )
        .unwrap();

        let r = zrange(
            &mut ctx,
            &[
                bulk("z"),
                bulk("[a"),
                bulk("[c"),
                bulk("BYLEX"),
                bulk("WITHSCORES"),
            ],
        )
        .unwrap();
        assert_eq!(
            r,
            RespValue::Array(vec![
                bulk("a"),
                bulk("0"),
                bulk("b"),
                bulk("0"),
                bulk("c"),
                bulk("0"),
            ])
        );
    }

    // ===== ZRANGESTORE options tests =====

    // 9. test_zrangestore_byscore
    #[test]
    fn test_zrangestore_byscore() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("src"),
                bulk("1"),
                bulk("a"),
                bulk("2"),
                bulk("b"),
                bulk("3"),
                bulk("c"),
                bulk("4"),
                bulk("d"),
            ],
        )
        .unwrap();

        let r = zrangestore(
            &mut ctx,
            &[
                bulk("dst"),
                bulk("src"),
                bulk("1"),
                bulk("3"),
                bulk("BYSCORE"),
            ],
        )
        .unwrap();
        assert_eq!(r, RespValue::Integer(3));
        assert_eq!(
            zscore(&mut ctx, &[bulk("dst"), bulk("a")]).unwrap(),
            RespValue::BulkString(Bytes::from("1"))
        );
        assert_eq!(
            zscore(&mut ctx, &[bulk("dst"), bulk("b")]).unwrap(),
            RespValue::BulkString(Bytes::from("2"))
        );
        assert_eq!(
            zscore(&mut ctx, &[bulk("dst"), bulk("c")]).unwrap(),
            RespValue::BulkString(Bytes::from("3"))
        );
        assert_eq!(
            zscore(&mut ctx, &[bulk("dst"), bulk("d")]).unwrap(),
            RespValue::Null
        );
    }

    // 10. test_zrangestore_bylex
    #[test]
    fn test_zrangestore_bylex() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("src"),
                bulk("0"),
                bulk("a"),
                bulk("0"),
                bulk("b"),
                bulk("0"),
                bulk("c"),
                bulk("0"),
                bulk("d"),
            ],
        )
        .unwrap();

        let r = zrangestore(
            &mut ctx,
            &[
                bulk("dst"),
                bulk("src"),
                bulk("[a"),
                bulk("[c"),
                bulk("BYLEX"),
            ],
        )
        .unwrap();
        assert_eq!(r, RespValue::Integer(3));
        assert_eq!(
            zcard(&mut ctx, &[bulk("dst")]).unwrap(),
            RespValue::Integer(3)
        );
        // d should not be present
        assert_eq!(
            zscore(&mut ctx, &[bulk("dst"), bulk("d")]).unwrap(),
            RespValue::Null
        );
    }

    // 11. test_zrangestore_rev
    #[test]
    fn test_zrangestore_rev() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("src"),
                bulk("1"),
                bulk("a"),
                bulk("2"),
                bulk("b"),
                bulk("3"),
                bulk("c"),
            ],
        )
        .unwrap();

        // REV by rank: 0 1 → from the end: c, b
        let r = zrangestore(
            &mut ctx,
            &[bulk("dst"), bulk("src"), bulk("0"), bulk("1"), bulk("REV")],
        )
        .unwrap();
        assert_eq!(r, RespValue::Integer(2));
        assert_eq!(
            zscore(&mut ctx, &[bulk("dst"), bulk("c")]).unwrap(),
            RespValue::BulkString(Bytes::from("3"))
        );
        assert_eq!(
            zscore(&mut ctx, &[bulk("dst"), bulk("b")]).unwrap(),
            RespValue::BulkString(Bytes::from("2"))
        );
    }

    // 12. test_zrangestore_limit
    #[test]
    fn test_zrangestore_limit() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("src"),
                bulk("1"),
                bulk("a"),
                bulk("2"),
                bulk("b"),
                bulk("3"),
                bulk("c"),
                bulk("4"),
                bulk("d"),
            ],
        )
        .unwrap();

        let r = zrangestore(
            &mut ctx,
            &[
                bulk("dst"),
                bulk("src"),
                bulk("0"),
                bulk("100"),
                bulk("BYSCORE"),
                bulk("LIMIT"),
                bulk("0"),
                bulk("2"),
            ],
        )
        .unwrap();
        assert_eq!(r, RespValue::Integer(2));
        assert_eq!(
            zcard(&mut ctx, &[bulk("dst")]).unwrap(),
            RespValue::Integer(2)
        );
    }

    // 13. test_zrangestore_empty_source
    #[test]
    fn test_zrangestore_empty_source() {
        let mut ctx = make_ctx();
        let r = zrangestore(
            &mut ctx,
            &[bulk("dst"), bulk("nosrc"), bulk("0"), bulk("-1")],
        )
        .unwrap();
        assert_eq!(r, RespValue::Integer(0));
    }

    // ===== ZREVRANGE options tests =====

    // 14. test_zrevrange_withscores
    #[test]
    fn test_zrevrange_withscores() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("z"),
                bulk("1"),
                bulk("a"),
                bulk("2"),
                bulk("b"),
                bulk("3"),
                bulk("c"),
            ],
        )
        .unwrap();

        let r = zrevrange(
            &mut ctx,
            &[bulk("z"), bulk("0"), bulk("-1"), bulk("WITHSCORES")],
        )
        .unwrap();
        assert_eq!(
            r,
            RespValue::Array(vec![
                bulk("c"),
                bulk("3"),
                bulk("b"),
                bulk("2"),
                bulk("a"),
                bulk("1"),
            ])
        );
    }

    // 15. test_zrevrange_partial
    #[test]
    fn test_zrevrange_partial() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("z"),
                bulk("1"),
                bulk("a"),
                bulk("2"),
                bulk("b"),
                bulk("3"),
                bulk("c"),
            ],
        )
        .unwrap();

        // ZREVRANGE key 0 1 → top 2 by score desc: c, b
        let r = zrevrange(&mut ctx, &[bulk("z"), bulk("0"), bulk("1")]).unwrap();
        assert_eq!(r, RespValue::Array(vec![bulk("c"), bulk("b")]));
    }

    // 16. test_zrevrange_empty
    #[test]
    fn test_zrevrange_empty() {
        let mut ctx = make_ctx();
        let r = zrevrange(&mut ctx, &[bulk("nokey"), bulk("0"), bulk("-1")]).unwrap();
        assert_eq!(r, RespValue::Array(vec![]));
    }

    // ===== ZREVRANGEBYSCORE options tests =====

    // 17. test_zrevrangebyscore_withscores
    #[test]
    fn test_zrevrangebyscore_withscores() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("z"),
                bulk("1"),
                bulk("a"),
                bulk("2"),
                bulk("b"),
                bulk("3"),
                bulk("c"),
            ],
        )
        .unwrap();

        let r = zrevrangebyscore(
            &mut ctx,
            &[bulk("z"), bulk("+inf"), bulk("-inf"), bulk("WITHSCORES")],
        )
        .unwrap();
        assert_eq!(
            r,
            RespValue::Array(vec![
                bulk("c"),
                bulk("3"),
                bulk("b"),
                bulk("2"),
                bulk("a"),
                bulk("1"),
            ])
        );
    }

    // 18. test_zrevrangebyscore_limit
    #[test]
    fn test_zrevrangebyscore_limit() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("z"),
                bulk("1"),
                bulk("a"),
                bulk("2"),
                bulk("b"),
                bulk("3"),
                bulk("c"),
                bulk("4"),
                bulk("d"),
            ],
        )
        .unwrap();

        // ZREVRANGEBYSCORE z +inf -inf LIMIT 1 2 → skip 1, take 2 from [d,c,b,a]
        let r = zrevrangebyscore(
            &mut ctx,
            &[
                bulk("z"),
                bulk("+inf"),
                bulk("-inf"),
                bulk("LIMIT"),
                bulk("1"),
                bulk("2"),
            ],
        )
        .unwrap();
        assert_eq!(r, RespValue::Array(vec![bulk("c"), bulk("b")]));
    }

    // 19. test_zrevrangebyscore_exclusive
    #[test]
    fn test_zrevrangebyscore_exclusive() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("z"),
                bulk("1"),
                bulk("a"),
                bulk("2"),
                bulk("b"),
                bulk("3"),
                bulk("c"),
            ],
        )
        .unwrap();

        // ZREVRANGEBYSCORE z (3 (1 → exclusive both ends, only b (score 2)
        let r = zrevrangebyscore(&mut ctx, &[bulk("z"), bulk("(3"), bulk("(1")]).unwrap();
        assert_eq!(r, RespValue::Array(vec![bulk("b")]));
    }

    // 20. test_zrevrangebyscore_empty_range
    #[test]
    fn test_zrevrangebyscore_empty_range() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[bulk("z"), bulk("1"), bulk("a"), bulk("2"), bulk("b")],
        )
        .unwrap();

        // Range with no matches: scores 10-20 but members only have 1-2
        let r = zrevrangebyscore(&mut ctx, &[bulk("z"), bulk("20"), bulk("10")]).unwrap();
        assert_eq!(r, RespValue::Array(vec![]));
    }

    // ===== ZUNIONSTORE options tests =====

    // 21. test_zunionstore_aggregate_min
    #[test]
    fn test_zunionstore_aggregate_min() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[bulk("z1"), bulk("5"), bulk("a"), bulk("10"), bulk("b")],
        )
        .unwrap();
        zadd(
            &mut ctx,
            &[bulk("z2"), bulk("3"), bulk("a"), bulk("20"), bulk("b")],
        )
        .unwrap();

        zunionstore(
            &mut ctx,
            &[
                bulk("out"),
                bulk("2"),
                bulk("z1"),
                bulk("z2"),
                bulk("AGGREGATE"),
                bulk("MIN"),
            ],
        )
        .unwrap();

        assert_eq!(
            zscore(&mut ctx, &[bulk("out"), bulk("a")]).unwrap(),
            RespValue::BulkString(Bytes::from("3"))
        );
        assert_eq!(
            zscore(&mut ctx, &[bulk("out"), bulk("b")]).unwrap(),
            RespValue::BulkString(Bytes::from("10"))
        );
    }

    // 22. test_zunionstore_aggregate_max
    #[test]
    fn test_zunionstore_aggregate_max() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[bulk("z1"), bulk("5"), bulk("a"), bulk("10"), bulk("b")],
        )
        .unwrap();
        zadd(
            &mut ctx,
            &[bulk("z2"), bulk("3"), bulk("a"), bulk("20"), bulk("b")],
        )
        .unwrap();

        zunionstore(
            &mut ctx,
            &[
                bulk("out"),
                bulk("2"),
                bulk("z1"),
                bulk("z2"),
                bulk("AGGREGATE"),
                bulk("MAX"),
            ],
        )
        .unwrap();

        assert_eq!(
            zscore(&mut ctx, &[bulk("out"), bulk("a")]).unwrap(),
            RespValue::BulkString(Bytes::from("5"))
        );
        assert_eq!(
            zscore(&mut ctx, &[bulk("out"), bulk("b")]).unwrap(),
            RespValue::BulkString(Bytes::from("20"))
        );
    }

    // 23. test_zunionstore_weights_and_aggregate
    #[test]
    fn test_zunionstore_weights_and_aggregate() {
        let mut ctx = make_ctx();
        zadd(&mut ctx, &[bulk("z1"), bulk("2"), bulk("a")]).unwrap();
        zadd(&mut ctx, &[bulk("z2"), bulk("3"), bulk("a")]).unwrap();

        // WEIGHTS 2 3, AGGREGATE MAX → max(2*2, 3*3) = max(4, 9) = 9
        zunionstore(
            &mut ctx,
            &[
                bulk("out"),
                bulk("2"),
                bulk("z1"),
                bulk("z2"),
                bulk("WEIGHTS"),
                bulk("2"),
                bulk("3"),
                bulk("AGGREGATE"),
                bulk("MAX"),
            ],
        )
        .unwrap();

        assert_eq!(
            zscore(&mut ctx, &[bulk("out"), bulk("a")]).unwrap(),
            RespValue::BulkString(Bytes::from("9"))
        );
    }

    // 24. test_zunionstore_three_keys
    #[test]
    fn test_zunionstore_three_keys() {
        let mut ctx = make_ctx();
        zadd(&mut ctx, &[bulk("z1"), bulk("1"), bulk("a")]).unwrap();
        zadd(&mut ctx, &[bulk("z2"), bulk("2"), bulk("b")]).unwrap();
        zadd(&mut ctx, &[bulk("z3"), bulk("3"), bulk("c")]).unwrap();

        let r = zunionstore(
            &mut ctx,
            &[bulk("out"), bulk("3"), bulk("z1"), bulk("z2"), bulk("z3")],
        )
        .unwrap();
        assert_eq!(r, RespValue::Integer(3));
        assert_eq!(
            zscore(&mut ctx, &[bulk("out"), bulk("a")]).unwrap(),
            RespValue::BulkString(Bytes::from("1"))
        );
        assert_eq!(
            zscore(&mut ctx, &[bulk("out"), bulk("b")]).unwrap(),
            RespValue::BulkString(Bytes::from("2"))
        );
        assert_eq!(
            zscore(&mut ctx, &[bulk("out"), bulk("c")]).unwrap(),
            RespValue::BulkString(Bytes::from("3"))
        );
    }

    // ===== ZINTERSTORE options tests =====

    // 25. test_zinterstore_weights
    #[test]
    fn test_zinterstore_weights() {
        let mut ctx = make_ctx();
        zadd(&mut ctx, &[bulk("z1"), bulk("2"), bulk("a")]).unwrap();
        zadd(&mut ctx, &[bulk("z2"), bulk("3"), bulk("a")]).unwrap();

        // WEIGHTS 2 3 → 2*2 + 3*3 = 4 + 9 = 13
        zinterstore(
            &mut ctx,
            &[
                bulk("out"),
                bulk("2"),
                bulk("z1"),
                bulk("z2"),
                bulk("WEIGHTS"),
                bulk("2"),
                bulk("3"),
            ],
        )
        .unwrap();

        assert_eq!(
            zscore(&mut ctx, &[bulk("out"), bulk("a")]).unwrap(),
            RespValue::BulkString(Bytes::from("13"))
        );
    }

    // 26. test_zinterstore_aggregate_max
    #[test]
    fn test_zinterstore_aggregate_max() {
        let mut ctx = make_ctx();
        zadd(&mut ctx, &[bulk("z1"), bulk("5"), bulk("a")]).unwrap();
        zadd(&mut ctx, &[bulk("z2"), bulk("8"), bulk("a")]).unwrap();

        zinterstore(
            &mut ctx,
            &[
                bulk("out"),
                bulk("2"),
                bulk("z1"),
                bulk("z2"),
                bulk("AGGREGATE"),
                bulk("MAX"),
            ],
        )
        .unwrap();

        assert_eq!(
            zscore(&mut ctx, &[bulk("out"), bulk("a")]).unwrap(),
            RespValue::BulkString(Bytes::from("8"))
        );
    }

    // 27. test_zinterstore_empty_intersection
    #[test]
    fn test_zinterstore_empty_intersection() {
        let mut ctx = make_ctx();
        zadd(&mut ctx, &[bulk("z1"), bulk("1"), bulk("a")]).unwrap();
        zadd(&mut ctx, &[bulk("z2"), bulk("2"), bulk("b")]).unwrap();

        let r = zinterstore(&mut ctx, &[bulk("out"), bulk("2"), bulk("z1"), bulk("z2")]).unwrap();
        assert_eq!(r, RespValue::Integer(0));
    }

    // 28. test_zinterstore_weights_and_aggregate
    #[test]
    fn test_zinterstore_weights_and_aggregate() {
        let mut ctx = make_ctx();
        zadd(&mut ctx, &[bulk("z1"), bulk("4"), bulk("a")]).unwrap();
        zadd(&mut ctx, &[bulk("z2"), bulk("6"), bulk("a")]).unwrap();

        // WEIGHTS 2 3, AGGREGATE MIN → min(4*2, 6*3) = min(8, 18) = 8
        zinterstore(
            &mut ctx,
            &[
                bulk("out"),
                bulk("2"),
                bulk("z1"),
                bulk("z2"),
                bulk("WEIGHTS"),
                bulk("2"),
                bulk("3"),
                bulk("AGGREGATE"),
                bulk("MIN"),
            ],
        )
        .unwrap();

        assert_eq!(
            zscore(&mut ctx, &[bulk("out"), bulk("a")]).unwrap(),
            RespValue::BulkString(Bytes::from("8"))
        );
    }

    // ===== ZRANDMEMBER options tests =====

    // 29. test_zrandmember_negative_count
    #[test]
    fn test_zrandmember_negative_count() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[bulk("z"), bulk("1"), bulk("a"), bulk("2"), bulk("b")],
        )
        .unwrap();

        // Negative count allows duplicates, returns exactly |count| elements
        let r = zrandmember(&mut ctx, &[bulk("z"), bulk("-5")]).unwrap();
        if let RespValue::Array(arr) = r {
            assert_eq!(arr.len(), 5);
            // Each element must be either "a" or "b"
            for item in &arr {
                if let RespValue::BulkString(b) = item {
                    let s = String::from_utf8_lossy(b).to_string();
                    assert!(s == "a" || s == "b");
                } else {
                    panic!("Expected BulkString");
                }
            }
        } else {
            panic!("Expected array");
        }
    }

    // 30. test_zrandmember_withscores
    #[test]
    fn test_zrandmember_withscores() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("z"),
                bulk("1"),
                bulk("a"),
                bulk("2"),
                bulk("b"),
                bulk("3"),
                bulk("c"),
            ],
        )
        .unwrap();

        let r = zrandmember(&mut ctx, &[bulk("z"), bulk("2"), bulk("WITHSCORES")]).unwrap();
        if let RespValue::Array(arr) = r {
            // 2 members with scores → 4 elements (member, score, member, score)
            assert_eq!(arr.len(), 4);
            // Verify each pair: odd indices should be score strings
            for pair in arr.chunks(2) {
                if let RespValue::BulkString(member) = &pair[0] {
                    let m = String::from_utf8_lossy(member).to_string();
                    assert!(m == "a" || m == "b" || m == "c");
                }
                if let RespValue::BulkString(score) = &pair[1] {
                    let s = String::from_utf8_lossy(score).to_string();
                    assert!(s == "1" || s == "2" || s == "3");
                }
            }
        } else {
            panic!("Expected array");
        }
    }

    // 31. test_zrandmember_count_greater_than_size
    #[test]
    fn test_zrandmember_count_greater_than_size() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[bulk("z"), bulk("1"), bulk("a"), bulk("2"), bulk("b")],
        )
        .unwrap();

        // Positive count > size → returns all unique members
        let r = zrandmember(&mut ctx, &[bulk("z"), bulk("10")]).unwrap();
        if let RespValue::Array(arr) = r {
            assert_eq!(arr.len(), 2); // Only 2 members exist
        } else {
            panic!("Expected array");
        }
    }

    // 32. test_zrandmember_count_zero
    #[test]
    fn test_zrandmember_count_zero() {
        let mut ctx = make_ctx();
        zadd(&mut ctx, &[bulk("z"), bulk("1"), bulk("a")]).unwrap();

        let r = zrandmember(&mut ctx, &[bulk("z"), bulk("0")]).unwrap();
        assert_eq!(r, RespValue::Array(vec![]));
    }

    // 33. test_zrandmember_empty_set
    #[test]
    fn test_zrandmember_empty_set() {
        let mut ctx = make_ctx();

        // No count arg → Null
        let r = zrandmember(&mut ctx, &[bulk("nokey")]).unwrap();
        assert_eq!(r, RespValue::Null);

        // With count arg → empty array
        let r = zrandmember(&mut ctx, &[bulk("nokey"), bulk("3")]).unwrap();
        assert_eq!(r, RespValue::Array(vec![]));
    }

    // 34. test_zrandmember_negative_withscores
    #[test]
    fn test_zrandmember_negative_withscores() {
        let mut ctx = make_ctx();
        zadd(&mut ctx, &[bulk("z"), bulk("10"), bulk("x")]).unwrap();

        // Negative count with WITHSCORES: returns |count| pairs
        let r = zrandmember(&mut ctx, &[bulk("z"), bulk("-3"), bulk("WITHSCORES")]).unwrap();
        if let RespValue::Array(arr) = r {
            // 3 pairs of (member, score) = 6 elements
            assert_eq!(arr.len(), 6);
            for pair in arr.chunks(2) {
                assert_eq!(pair[0], bulk("x"));
                assert_eq!(pair[1], bulk("10"));
            }
        } else {
            panic!("Expected array");
        }
    }

    // ===== ZSCAN options tests =====

    // 35. test_zscan_count_option
    #[test]
    fn test_zscan_count_option() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("z"),
                bulk("1"),
                bulk("a"),
                bulk("2"),
                bulk("b"),
                bulk("3"),
                bulk("c"),
                bulk("4"),
                bulk("d"),
                bulk("5"),
                bulk("e"),
            ],
        )
        .unwrap();

        // ZSCAN with COUNT 2 → should return at most 2 elements per iteration
        let r = zscan(&mut ctx, &[bulk("z"), bulk("0"), bulk("COUNT"), bulk("2")]).unwrap();
        if let RespValue::Array(outer) = r {
            assert_eq!(outer.len(), 2);
            if let RespValue::Array(elements) = &outer[1] {
                // 2 members × 2 (member+score) = 4
                assert_eq!(elements.len(), 4);
            } else {
                panic!("expected inner array");
            }
            // cursor should be non-zero since there are more elements
            if let RespValue::BulkString(cursor) = &outer[0] {
                assert_eq!(cursor.as_ref(), b"2");
            }
        } else {
            panic!("expected array");
        }
    }

    // 36. test_zscan_match_and_iterate
    #[test]
    fn test_zscan_match_and_iterate() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("z"),
                bulk("1"),
                bulk("apple"),
                bulk("2"),
                bulk("apricot"),
                bulk("3"),
                bulk("banana"),
                bulk("4"),
                bulk("avocado"),
            ],
        )
        .unwrap();

        // Iterate with MATCH ap* until cursor returns to 0
        let mut cursor = "0".to_string();
        let mut collected = Vec::new();
        loop {
            let r = zscan(
                &mut ctx,
                &[
                    bulk("z"),
                    bulk(&cursor),
                    bulk("MATCH"),
                    bulk("ap*"),
                    bulk("COUNT"),
                    bulk("100"),
                ],
            )
            .unwrap();
            if let RespValue::Array(outer) = r {
                if let RespValue::BulkString(c) = &outer[0] {
                    cursor = String::from_utf8_lossy(c).to_string();
                }
                if let RespValue::Array(elements) = &outer[1] {
                    for chunk in elements.chunks(2) {
                        if let RespValue::BulkString(m) = &chunk[0] {
                            collected.push(String::from_utf8_lossy(m).to_string());
                        }
                    }
                }
            }
            if cursor == "0" {
                break;
            }
        }

        collected.sort();
        assert_eq!(collected, vec!["apple", "apricot"]);
    }

    // 37. test_zscan_nonexistent_key
    #[test]
    fn test_zscan_nonexistent_key() {
        let mut ctx = make_ctx();
        let r = zscan(&mut ctx, &[bulk("nokey"), bulk("0")]).unwrap();
        if let RespValue::Array(outer) = r {
            assert_eq!(outer.len(), 2);
            assert_eq!(outer[0], RespValue::BulkString(Bytes::from("0")));
            assert_eq!(outer[1], RespValue::Array(vec![]));
        } else {
            panic!("expected array");
        }
    }

    // 38. test_zscan_no_match
    #[test]
    fn test_zscan_no_match() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[bulk("z"), bulk("1"), bulk("alpha"), bulk("2"), bulk("beta")],
        )
        .unwrap();

        let r = zscan(
            &mut ctx,
            &[
                bulk("z"),
                bulk("0"),
                bulk("MATCH"),
                bulk("nomatch*"),
                bulk("COUNT"),
                bulk("100"),
            ],
        )
        .unwrap();
        if let RespValue::Array(outer) = r {
            assert_eq!(outer[0], RespValue::BulkString(Bytes::from("0")));
            assert_eq!(outer[1], RespValue::Array(vec![]));
        } else {
            panic!("expected array");
        }
    }

    // ===== ZRANK/ZREVRANK WITHSCORE tests =====

    // 39. test_zrank_withscore
    #[test]
    fn test_zrank_withscore() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("z"),
                bulk("10"),
                bulk("a"),
                bulk("20"),
                bulk("b"),
                bulk("30"),
                bulk("c"),
            ],
        )
        .unwrap();

        let r = zrank(&mut ctx, &[bulk("z"), bulk("b"), bulk("WITHSCORE")]).unwrap();
        assert_eq!(
            r,
            RespValue::Array(vec![
                RespValue::Integer(1),
                RespValue::BulkString(Bytes::from("20")),
            ])
        );
    }

    // 40. test_zrank_withscore_nonexistent
    #[test]
    fn test_zrank_withscore_nonexistent() {
        let mut ctx = make_ctx();
        zadd(&mut ctx, &[bulk("z"), bulk("1"), bulk("a")]).unwrap();

        let r = zrank(&mut ctx, &[bulk("z"), bulk("noexist"), bulk("WITHSCORE")]).unwrap();
        assert_eq!(r, RespValue::Null);
    }

    // 41. test_zrevrank_withscore
    #[test]
    fn test_zrevrank_withscore() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("z"),
                bulk("10"),
                bulk("a"),
                bulk("20"),
                bulk("b"),
                bulk("30"),
                bulk("c"),
            ],
        )
        .unwrap();

        // c has rank 2 in ascending, revrank 0; b has rank 1, revrank 1; a has rank 0, revrank 2
        let r = zrevrank(&mut ctx, &[bulk("z"), bulk("a"), bulk("WITHSCORE")]).unwrap();
        assert_eq!(
            r,
            RespValue::Array(vec![
                RespValue::Integer(2),
                RespValue::BulkString(Bytes::from("10")),
            ])
        );

        let r = zrevrank(&mut ctx, &[bulk("z"), bulk("c"), bulk("WITHSCORE")]).unwrap();
        assert_eq!(
            r,
            RespValue::Array(vec![
                RespValue::Integer(0),
                RespValue::BulkString(Bytes::from("30")),
            ])
        );
    }

    // 42. test_zrank_invalid_option
    #[test]
    fn test_zrank_invalid_option() {
        let mut ctx = make_ctx();
        zadd(&mut ctx, &[bulk("z"), bulk("1"), bulk("a")]).unwrap();

        let r = zrank(&mut ctx, &[bulk("z"), bulk("a"), bulk("INVALID")]);
        assert!(matches!(r, Err(CommandError::SyntaxError)));
    }

    // ===== ZRANGEBYLEX/ZREVRANGEBYLEX LIMIT tests =====

    // 43. test_zrangebylex_with_limit
    #[test]
    fn test_zrangebylex_with_limit() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("z"),
                bulk("0"),
                bulk("a"),
                bulk("0"),
                bulk("b"),
                bulk("0"),
                bulk("c"),
                bulk("0"),
                bulk("d"),
                bulk("0"),
                bulk("e"),
            ],
        )
        .unwrap();

        // ZRANGEBYLEX z - + LIMIT 1 2 → skip "a", take "b" and "c"
        let r = zrangebylex(
            &mut ctx,
            &[
                bulk("z"),
                bulk("-"),
                bulk("+"),
                bulk("LIMIT"),
                bulk("1"),
                bulk("2"),
            ],
        )
        .unwrap();
        assert_eq!(r, RespValue::Array(vec![bulk("b"), bulk("c")]));
    }

    // 44. test_zrevrangebylex_with_limit
    #[test]
    fn test_zrevrangebylex_with_limit() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("z"),
                bulk("0"),
                bulk("a"),
                bulk("0"),
                bulk("b"),
                bulk("0"),
                bulk("c"),
                bulk("0"),
                bulk("d"),
                bulk("0"),
                bulk("e"),
            ],
        )
        .unwrap();

        // ZREVRANGEBYLEX z + - LIMIT 1 2 → reversed: e,d,c,b,a; skip 1 → d,c,b,a; take 2 → d,c
        let r = zrevrangebylex(
            &mut ctx,
            &[
                bulk("z"),
                bulk("+"),
                bulk("-"),
                bulk("LIMIT"),
                bulk("1"),
                bulk("2"),
            ],
        )
        .unwrap();
        assert_eq!(r, RespValue::Array(vec![bulk("d"), bulk("c")]));
    }

    // 45. test_zrangebylex_empty_result
    #[test]
    fn test_zrangebylex_empty_result() {
        let mut ctx = make_ctx();
        zadd(
            &mut ctx,
            &[
                bulk("z"),
                bulk("0"),
                bulk("a"),
                bulk("0"),
                bulk("b"),
                bulk("0"),
                bulk("c"),
            ],
        )
        .unwrap();

        // Range [x to [z has no matches since members are a, b, c
        let r = zrangebylex(&mut ctx, &[bulk("z"), bulk("[x"), bulk("[z")]).unwrap();
        assert_eq!(r, RespValue::Array(vec![]));
    }

    // ZINTERCARD tests
    #[test]
    fn test_zintercard_basic() {
        let mut ctx = make_ctx();

        zadd(
            &mut ctx,
            &[bulk("z1"), bulk("1"), bulk("a"), bulk("2"), bulk("b")],
        )
        .unwrap();
        zadd(
            &mut ctx,
            &[bulk("z2"), bulk("1"), bulk("b"), bulk("2"), bulk("c")],
        )
        .unwrap();

        // Intersection is just "b"
        let result = zintercard(&mut ctx, &[bulk("2"), bulk("z1"), bulk("z2")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_zintercard_multiple_sets() {
        let mut ctx = make_ctx();

        zadd(
            &mut ctx,
            &[
                bulk("z1"),
                bulk("1"),
                bulk("a"),
                bulk("2"),
                bulk("b"),
                bulk("3"),
                bulk("c"),
            ],
        )
        .unwrap();
        zadd(
            &mut ctx,
            &[
                bulk("z2"),
                bulk("1"),
                bulk("b"),
                bulk("2"),
                bulk("c"),
                bulk("3"),
                bulk("d"),
            ],
        )
        .unwrap();
        zadd(
            &mut ctx,
            &[bulk("z3"), bulk("1"), bulk("c"), bulk("2"), bulk("d")],
        )
        .unwrap();

        // Intersection of all three is just "c"
        let result =
            zintercard(&mut ctx, &[bulk("3"), bulk("z1"), bulk("z2"), bulk("z3")]).unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_zintercard_no_intersection() {
        let mut ctx = make_ctx();

        zadd(
            &mut ctx,
            &[bulk("z1"), bulk("1"), bulk("a"), bulk("2"), bulk("b")],
        )
        .unwrap();
        zadd(
            &mut ctx,
            &[bulk("z2"), bulk("1"), bulk("c"), bulk("2"), bulk("d")],
        )
        .unwrap();

        let result = zintercard(&mut ctx, &[bulk("2"), bulk("z1"), bulk("z2")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_zintercard_empty_set() {
        let mut ctx = make_ctx();

        zadd(
            &mut ctx,
            &[bulk("z1"), bulk("1"), bulk("a"), bulk("2"), bulk("b")],
        )
        .unwrap();

        // z2 doesn't exist (empty set)
        let result = zintercard(&mut ctx, &[bulk("2"), bulk("z1"), bulk("z2")]).unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_zintercard_with_limit() {
        let mut ctx = make_ctx();

        zadd(
            &mut ctx,
            &[
                bulk("z1"),
                bulk("1"),
                bulk("a"),
                bulk("2"),
                bulk("b"),
                bulk("3"),
                bulk("c"),
            ],
        )
        .unwrap();
        zadd(
            &mut ctx,
            &[
                bulk("z2"),
                bulk("1"),
                bulk("a"),
                bulk("2"),
                bulk("b"),
                bulk("3"),
                bulk("c"),
            ],
        )
        .unwrap();

        // Intersection has 3 members, but limit to 2
        let result = zintercard(
            &mut ctx,
            &[bulk("2"), bulk("z1"), bulk("z2"), bulk("LIMIT"), bulk("2")],
        )
        .unwrap();
        assert_eq!(result, RespValue::Integer(2));
    }

    #[test]
    fn test_zintercard_single_set() {
        let mut ctx = make_ctx();

        zadd(
            &mut ctx,
            &[
                bulk("z1"),
                bulk("1"),
                bulk("a"),
                bulk("2"),
                bulk("b"),
                bulk("3"),
                bulk("c"),
            ],
        )
        .unwrap();

        // Single set: cardinality is the set size
        let result = zintercard(&mut ctx, &[bulk("1"), bulk("z1")]).unwrap();
        assert_eq!(result, RespValue::Integer(3));
    }

    #[test]
    fn test_zintercard_all_same_members() {
        let mut ctx = make_ctx();

        zadd(
            &mut ctx,
            &[bulk("z1"), bulk("1"), bulk("a"), bulk("2"), bulk("b")],
        )
        .unwrap();
        zadd(
            &mut ctx,
            &[bulk("z2"), bulk("3"), bulk("a"), bulk("4"), bulk("b")],
        )
        .unwrap();
        zadd(
            &mut ctx,
            &[bulk("z3"), bulk("5"), bulk("a"), bulk("6"), bulk("b")],
        )
        .unwrap();

        // All sets have same members
        let result =
            zintercard(&mut ctx, &[bulk("3"), bulk("z1"), bulk("z2"), bulk("z3")]).unwrap();
        assert_eq!(result, RespValue::Integer(2));
    }
}

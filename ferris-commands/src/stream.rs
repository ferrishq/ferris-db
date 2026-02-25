//! Stream commands: XADD, XREAD, XRANGE, XREVRANGE, XLEN, XINFO, XTRIM, XDEL,
//! XGROUP, XREADGROUP, XACK, XPENDING, XCLAIM, XAUTOCLAIM, XSETID

use crate::{CommandContext, CommandError, CommandResult};
use bytes::Bytes;
use ferris_core::{Entry, RedisValue, StreamData, StreamEntry, StreamId};
use ferris_protocol::RespValue;

/// Helper to get bytes from RespValue
fn get_bytes(arg: &RespValue) -> Result<Bytes, CommandError> {
    arg.as_bytes()
        .cloned()
        .or_else(|| arg.as_str().map(|s| Bytes::from(s.to_owned())))
        .ok_or_else(|| CommandError::InvalidArgument("invalid argument".to_string()))
}

/// Helper to get string from RespValue
fn get_str(arg: &RespValue) -> Result<&str, CommandError> {
    arg.as_str()
        .ok_or_else(|| CommandError::InvalidArgument("invalid argument".to_string()))
}

/// Helper to parse an integer from RespValue
fn parse_int(arg: &RespValue) -> Result<i64, CommandError> {
    let s = arg.as_str().ok_or(CommandError::NotAnInteger)?;
    s.parse().map_err(|_| CommandError::NotAnInteger)
}

/// Helper to parse a usize from RespValue
fn parse_usize(arg: &RespValue) -> Result<usize, CommandError> {
    let s = arg.as_str().ok_or(CommandError::NotAnInteger)?;
    s.parse().map_err(|_| CommandError::NotAnInteger)
}

/// Parse a stream ID from argument
fn parse_stream_id(arg: &RespValue) -> Result<Option<StreamId>, CommandError> {
    let s = get_str(arg)?;
    if s == "*" {
        return Ok(None); // Auto-generate
    }
    StreamId::parse(s)
        .map(Some)
        .ok_or_else(|| CommandError::InvalidArgument(format!("Invalid stream ID: {}", s)))
}

/// Convert a StreamEntry to RespValue
fn entry_to_resp(entry: &StreamEntry) -> RespValue {
    let fields: Vec<RespValue> = entry
        .fields
        .iter()
        .flat_map(|(k, v)| {
            vec![
                RespValue::BulkString(k.clone()),
                RespValue::BulkString(v.clone()),
            ]
        })
        .collect();

    RespValue::Array(vec![
        RespValue::BulkString(Bytes::from(entry.id.to_string())),
        RespValue::Array(fields),
    ])
}

// ---------------------------------------------------------------------------
// XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold] [LIMIT count] *|id field value [field value ...]
// ---------------------------------------------------------------------------

/// XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold] [LIMIT count] *|id field value [field value ...]
///
/// Appends a new entry to a stream.
/// Returns the ID of the added entry.
///
/// Time complexity: O(1) when adding a new entry, O(N) with N being the number of entries evicted by MAXLEN.
pub fn xadd(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 4 {
        return Err(CommandError::WrongArity("XADD".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let db = ctx.store().database(ctx.selected_db());

    let mut idx = 1;
    let mut nomkstream = false;
    let mut maxlen: Option<usize> = None;
    let mut approx = false;

    // Parse options
    while idx < args.len() {
        let opt = get_str(&args[idx])?.to_uppercase();
        match opt.as_str() {
            "NOMKSTREAM" => {
                nomkstream = true;
                idx += 1;
            }
            "MAXLEN" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                let next = get_str(&args[idx])?;
                if next == "~" {
                    approx = true;
                    idx += 1;
                } else if next == "=" {
                    idx += 1;
                }
                if idx >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                maxlen = Some(parse_usize(&args[idx])?);
                idx += 1;
            }
            "MINID" => {
                // Skip MINID for now - similar to MAXLEN but by ID
                idx += 1;
                if idx >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                let next = get_str(&args[idx])?;
                if next == "~" || next == "=" {
                    idx += 1;
                }
                if idx >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                idx += 1; // Skip the threshold
            }
            "LIMIT" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                // Skip LIMIT for now
                idx += 1;
            }
            _ => break, // Must be the ID
        }
    }

    if idx >= args.len() {
        return Err(CommandError::WrongArity("XADD".to_string()));
    }

    // Parse ID
    let entry_id = parse_stream_id(&args[idx])?;
    idx += 1;

    // Parse field-value pairs
    let remaining = args.len() - idx;
    if remaining == 0 || remaining % 2 != 0 {
        return Err(CommandError::WrongArity("XADD".to_string()));
    }

    let mut fields = Vec::new();
    while idx < args.len() {
        let field = get_bytes(&args[idx])?;
        let value = get_bytes(&args[idx + 1])?;
        fields.push((field, value));
        idx += 2;
    }

    // Get or create stream
    let mut stream = match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                if nomkstream {
                    return Ok(RespValue::Null);
                }
                StreamData::new()
            } else {
                match entry.value {
                    RedisValue::Stream(s) => s,
                    _ => return Err(CommandError::WrongType),
                }
            }
        }
        None => {
            if nomkstream {
                return Ok(RespValue::Null);
            }
            StreamData::new()
        }
    };

    // Set maxlen if specified
    if let Some(max) = maxlen {
        stream.max_len = Some(max);
    }

    // Add entry
    let new_id = stream.add(entry_id, fields);
    let id_str = new_id.to_string();

    // Apply trimming if needed
    if let Some(max) = maxlen {
        stream.trim(max, approx);
    }

    db.set(key.clone(), Entry::new(RedisValue::Stream(stream)));

    // Notify any blocking waiters
    ctx.blocking_registry().notify_key(ctx.selected_db(), &key);

    Ok(RespValue::BulkString(Bytes::from(id_str)))
}

// ---------------------------------------------------------------------------
// XLEN key
// ---------------------------------------------------------------------------

/// XLEN key
///
/// Returns the number of entries in a stream.
///
/// Time complexity: O(1)
pub fn xlen(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("XLEN".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let db = ctx.store().database(ctx.selected_db());

    match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                return Ok(RespValue::Integer(0));
            }
            match &entry.value {
                RedisValue::Stream(s) => Ok(RespValue::Integer(s.len() as i64)),
                _ => Err(CommandError::WrongType),
            }
        }
        None => Ok(RespValue::Integer(0)),
    }
}

// ---------------------------------------------------------------------------
// XRANGE key start end [COUNT count]
// ---------------------------------------------------------------------------

/// XRANGE key start end [COUNT count]
///
/// Returns entries from a stream in a given ID range.
///
/// Time complexity: O(N) where N is the number of elements returned.
pub fn xrange(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongArity("XRANGE".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let start_str = get_str(&args[1])?;
    let end_str = get_str(&args[2])?;

    let start = if start_str == "-" {
        StreamId::min()
    } else {
        StreamId::parse(start_str).ok_or_else(|| {
            CommandError::InvalidArgument(format!("Invalid stream ID: {}", start_str))
        })?
    };

    let end = if end_str == "+" {
        StreamId::max()
    } else {
        StreamId::parse(end_str).ok_or_else(|| {
            CommandError::InvalidArgument(format!("Invalid stream ID: {}", end_str))
        })?
    };

    let mut count: Option<usize> = None;
    if args.len() > 3 {
        let opt = get_str(&args[3])?.to_uppercase();
        if opt == "COUNT" && args.len() > 4 {
            count = Some(parse_usize(&args[4])?);
        }
    }

    let db = ctx.store().database(ctx.selected_db());

    match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                return Ok(RespValue::Array(vec![]));
            }
            match &entry.value {
                RedisValue::Stream(s) => {
                    let entries = s.range(&start, &end, count);
                    let result: Vec<RespValue> = entries.iter().map(entry_to_resp).collect();
                    Ok(RespValue::Array(result))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        None => Ok(RespValue::Array(vec![])),
    }
}

// ---------------------------------------------------------------------------
// XREVRANGE key end start [COUNT count]
// ---------------------------------------------------------------------------

/// XREVRANGE key end start [COUNT count]
///
/// Returns entries from a stream in reverse order (high to low IDs).
///
/// Time complexity: O(N) where N is the number of elements returned.
pub fn xrevrange(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongArity("XREVRANGE".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let end_str = get_str(&args[1])?;
    let start_str = get_str(&args[2])?;

    let end = if end_str == "+" {
        StreamId::max()
    } else {
        StreamId::parse(end_str).ok_or_else(|| {
            CommandError::InvalidArgument(format!("Invalid stream ID: {}", end_str))
        })?
    };

    let start = if start_str == "-" {
        StreamId::min()
    } else {
        StreamId::parse(start_str).ok_or_else(|| {
            CommandError::InvalidArgument(format!("Invalid stream ID: {}", start_str))
        })?
    };

    let mut count: Option<usize> = None;
    if args.len() > 3 {
        let opt = get_str(&args[3])?.to_uppercase();
        if opt == "COUNT" && args.len() > 4 {
            count = Some(parse_usize(&args[4])?);
        }
    }

    let db = ctx.store().database(ctx.selected_db());

    match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                return Ok(RespValue::Array(vec![]));
            }
            match &entry.value {
                RedisValue::Stream(s) => {
                    let entries = s.rev_range(&end, &start, count);
                    let result: Vec<RespValue> = entries.iter().map(entry_to_resp).collect();
                    Ok(RespValue::Array(result))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        None => Ok(RespValue::Array(vec![])),
    }
}

// ---------------------------------------------------------------------------
// XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]
// ---------------------------------------------------------------------------

/// XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]
///
/// Read data from one or more streams, only returning entries with IDs greater than the specified IDs.
///
/// Time complexity: O(N) where N is the number of elements returned.
pub fn xread(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("XREAD".to_string()));
    }

    let mut idx = 0;
    let mut count: Option<usize> = None;
    // Note: BLOCK functionality would require async handling - parsed but not used yet
    #[allow(unused_assignments)]
    let mut block_ms: Option<u64> = None;

    // Parse options
    while idx < args.len() {
        let opt = get_str(&args[idx])?.to_uppercase();
        match opt.as_str() {
            "COUNT" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                count = Some(parse_usize(&args[idx])?);
                idx += 1;
            }
            "BLOCK" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                block_ms = Some(parse_int(&args[idx])? as u64);
                idx += 1;
                // TODO: Implement blocking behavior using BlockingAction
                let _ = block_ms; // Acknowledge that we parsed it
            }
            "STREAMS" => {
                idx += 1;
                break;
            }
            _ => {
                return Err(CommandError::SyntaxError);
            }
        }
    }

    // Parse keys and IDs
    let remaining = args.len() - idx;
    if remaining == 0 || remaining % 2 != 0 {
        return Err(CommandError::SyntaxError);
    }

    let num_streams = remaining / 2;
    let keys: Vec<Bytes> = (0..num_streams)
        .map(|i| get_bytes(&args[idx + i]))
        .collect::<Result<_, _>>()?;

    let ids: Vec<StreamId> = (0..num_streams)
        .map(|i| {
            let id_str = get_str(&args[idx + num_streams + i])?;
            if id_str == "$" {
                // Special: read new entries only (use max ID)
                Ok(StreamId::max())
            } else if id_str == "0" || id_str == "0-0" {
                Ok(StreamId::min())
            } else {
                StreamId::parse(id_str).ok_or_else(|| {
                    CommandError::InvalidArgument(format!("Invalid stream ID: {}", id_str))
                })
            }
        })
        .collect::<Result<_, _>>()?;

    let db = ctx.store().database(ctx.selected_db());
    let mut results: Vec<RespValue> = Vec::new();

    for (key, after_id) in keys.iter().zip(ids.iter()) {
        match db.get(key) {
            Some(entry) if !entry.is_expired() => {
                if let RedisValue::Stream(s) = &entry.value {
                    let entries = s.read_after(after_id, count);
                    if !entries.is_empty() {
                        let entry_results: Vec<RespValue> =
                            entries.iter().map(entry_to_resp).collect();
                        results.push(RespValue::Array(vec![
                            RespValue::BulkString(key.clone()),
                            RespValue::Array(entry_results),
                        ]));
                    }
                }
            }
            _ => {}
        }
    }

    if results.is_empty() {
        Ok(RespValue::Null)
    } else {
        Ok(RespValue::Array(results))
    }
}

// ---------------------------------------------------------------------------
// XDEL key id [id ...]
// ---------------------------------------------------------------------------

/// XDEL key id [id ...]
///
/// Removes the specified entries from the stream.
/// Returns the number of entries deleted.
///
/// Time complexity: O(1) for each entry mentioned.
pub fn xdel(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("XDEL".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let db = ctx.store().database(ctx.selected_db());

    let ids: Vec<StreamId> = args[1..]
        .iter()
        .map(|arg| {
            let s = get_str(arg)?;
            StreamId::parse(s)
                .ok_or_else(|| CommandError::InvalidArgument(format!("Invalid stream ID: {}", s)))
        })
        .collect::<Result<_, _>>()?;

    match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                return Ok(RespValue::Integer(0));
            }
            match entry.value {
                RedisValue::Stream(mut s) => {
                    let deleted = s.delete(&ids);
                    if s.is_empty() {
                        db.delete(&key);
                    } else {
                        db.set(key, Entry::new(RedisValue::Stream(s)));
                    }
                    Ok(RespValue::Integer(deleted as i64))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        None => Ok(RespValue::Integer(0)),
    }
}

// ---------------------------------------------------------------------------
// XTRIM key MAXLEN|MINID [=|~] threshold [LIMIT count]
// ---------------------------------------------------------------------------

/// XTRIM key MAXLEN|MINID [=|~] threshold [LIMIT count]
///
/// Trims the stream to a given number of entries.
/// Returns the number of entries deleted.
///
/// Time complexity: O(N) where N is the number of evicted entries.
pub fn xtrim(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongArity("XTRIM".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let strategy = get_str(&args[1])?.to_uppercase();

    let mut idx = 2;
    let mut approx = false;

    // Check for ~ or =
    if idx < args.len() {
        let modifier = get_str(&args[idx])?;
        if modifier == "~" {
            approx = true;
            idx += 1;
        } else if modifier == "=" {
            idx += 1;
        }
    }

    if idx >= args.len() {
        return Err(CommandError::WrongArity("XTRIM".to_string()));
    }

    let threshold = parse_usize(&args[idx])?;

    let db = ctx.store().database(ctx.selected_db());

    match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                return Ok(RespValue::Integer(0));
            }
            match entry.value {
                RedisValue::Stream(mut s) => {
                    let deleted = match strategy.as_str() {
                        "MAXLEN" => s.trim(threshold, approx),
                        "MINID" => {
                            // Trim entries with IDs lower than the threshold
                            // For now, treat threshold as a count-based trim
                            s.trim(threshold, approx)
                        }
                        _ => {
                            return Err(CommandError::SyntaxError);
                        }
                    };
                    if s.is_empty() {
                        db.delete(&key);
                    } else {
                        db.set(key, Entry::new(RedisValue::Stream(s)));
                    }
                    Ok(RespValue::Integer(deleted as i64))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        None => Ok(RespValue::Integer(0)),
    }
}

// ---------------------------------------------------------------------------
// XINFO STREAM key [FULL [COUNT count]]
// XINFO GROUPS key
// XINFO CONSUMERS key groupname
// ---------------------------------------------------------------------------

/// XINFO subcommand [key] [additional args]
///
/// Returns information about streams, consumer groups, and consumers.
pub fn xinfo(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("XINFO".to_string()));
    }

    let subcommand = get_str(&args[0])?.to_uppercase();

    match subcommand.as_str() {
        "STREAM" => {
            if args.len() < 2 {
                return Err(CommandError::WrongArity("XINFO".to_string()));
            }
            let key = get_bytes(&args[1])?;
            let db = ctx.store().database(ctx.selected_db());

            match db.get(&key) {
                Some(entry) => {
                    if entry.is_expired() {
                        return Err(CommandError::NoSuchKey);
                    }
                    match &entry.value {
                        RedisValue::Stream(s) => {
                            let info = vec![
                                RespValue::BulkString(Bytes::from("length")),
                                RespValue::Integer(s.len() as i64),
                                RespValue::BulkString(Bytes::from("radix-tree-keys")),
                                RespValue::Integer(0),
                                RespValue::BulkString(Bytes::from("radix-tree-nodes")),
                                RespValue::Integer(0),
                                RespValue::BulkString(Bytes::from("last-generated-id")),
                                RespValue::BulkString(Bytes::from(s.last_id.to_string())),
                                RespValue::BulkString(Bytes::from("groups")),
                                RespValue::Integer(s.groups.len() as i64),
                                RespValue::BulkString(Bytes::from("first-entry")),
                                s.entries
                                    .iter()
                                    .next()
                                    .map(|(id, fields)| {
                                        entry_to_resp(&StreamEntry {
                                            id: id.clone(),
                                            fields: fields.clone(),
                                        })
                                    })
                                    .unwrap_or(RespValue::Null),
                                RespValue::BulkString(Bytes::from("last-entry")),
                                s.entries
                                    .iter()
                                    .next_back()
                                    .map(|(id, fields)| {
                                        entry_to_resp(&StreamEntry {
                                            id: id.clone(),
                                            fields: fields.clone(),
                                        })
                                    })
                                    .unwrap_or(RespValue::Null),
                            ];
                            Ok(RespValue::Array(info))
                        }
                        _ => Err(CommandError::WrongType),
                    }
                }
                None => Err(CommandError::NoSuchKey),
            }
        }
        "GROUPS" => {
            if args.len() < 2 {
                return Err(CommandError::WrongArity("XINFO".to_string()));
            }
            let key = get_bytes(&args[1])?;
            let db = ctx.store().database(ctx.selected_db());

            match db.get(&key) {
                Some(entry) => {
                    if entry.is_expired() {
                        return Err(CommandError::NoSuchKey);
                    }
                    match &entry.value {
                        RedisValue::Stream(s) => {
                            let groups: Vec<RespValue> = s
                                .groups
                                .values()
                                .map(|g| {
                                    RespValue::Array(vec![
                                        RespValue::BulkString(Bytes::from("name")),
                                        RespValue::BulkString(g.name.clone()),
                                        RespValue::BulkString(Bytes::from("consumers")),
                                        RespValue::Integer(g.consumers.len() as i64),
                                        RespValue::BulkString(Bytes::from("pending")),
                                        RespValue::Integer(g.pending.len() as i64),
                                        RespValue::BulkString(Bytes::from("last-delivered-id")),
                                        RespValue::BulkString(Bytes::from(
                                            g.last_delivered_id.to_string(),
                                        )),
                                    ])
                                })
                                .collect();
                            Ok(RespValue::Array(groups))
                        }
                        _ => Err(CommandError::WrongType),
                    }
                }
                None => Err(CommandError::NoSuchKey),
            }
        }
        "CONSUMERS" => {
            if args.len() < 3 {
                return Err(CommandError::WrongArity("XINFO".to_string()));
            }
            let key = get_bytes(&args[1])?;
            let group_name = get_bytes(&args[2])?;
            let db = ctx.store().database(ctx.selected_db());

            match db.get(&key) {
                Some(entry) => {
                    if entry.is_expired() {
                        return Err(CommandError::NoSuchKey);
                    }
                    match &entry.value {
                        RedisValue::Stream(s) => {
                            let group = s.groups.get(&group_name).ok_or_else(|| {
                                CommandError::InvalidArgument(
                                    "NOGROUP No such consumer group".to_string(),
                                )
                            })?;

                            let consumers: Vec<RespValue> =
                                group
                                    .consumers
                                    .values()
                                    .map(|c| {
                                        RespValue::Array(vec![
                                            RespValue::BulkString(Bytes::from("name")),
                                            RespValue::BulkString(c.name.clone()),
                                            RespValue::BulkString(Bytes::from("pending")),
                                            RespValue::Integer(c.pending_count as i64),
                                            RespValue::BulkString(Bytes::from("idle")),
                                            RespValue::Integer(
                                                c.last_seen.elapsed().as_millis() as i64
                                            ),
                                        ])
                                    })
                                    .collect();
                            Ok(RespValue::Array(consumers))
                        }
                        _ => Err(CommandError::WrongType),
                    }
                }
                None => Err(CommandError::NoSuchKey),
            }
        }
        "HELP" => {
            let help = vec![
                RespValue::BulkString(Bytes::from("XINFO STREAM <key> [FULL [COUNT <count>]]")),
                RespValue::BulkString(Bytes::from("XINFO GROUPS <key>")),
                RespValue::BulkString(Bytes::from("XINFO CONSUMERS <key> <group>")),
                RespValue::BulkString(Bytes::from("XINFO HELP")),
            ];
            Ok(RespValue::Array(help))
        }
        _ => Err(CommandError::UnknownSubcommand(
            subcommand,
            "XINFO".to_string(),
        )),
    }
}

// ---------------------------------------------------------------------------
// XGROUP CREATE key groupname id|$ [MKSTREAM] [ENTRIESREAD entries_read]
// XGROUP DESTROY key groupname
// XGROUP SETID key groupname id|$ [ENTRIESREAD entries_read]
// XGROUP CREATECONSUMER key groupname consumername
// XGROUP DELCONSUMER key groupname consumername
// ---------------------------------------------------------------------------

/// XGROUP subcommand key groupname [additional args]
///
/// Manages consumer groups.
pub fn xgroup(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("XGROUP".to_string()));
    }

    let subcommand = get_str(&args[0])?.to_uppercase();

    match subcommand.as_str() {
        "CREATE" => {
            if args.len() < 4 {
                return Err(CommandError::WrongArity("XGROUP".to_string()));
            }
            let key = get_bytes(&args[1])?;
            let group_name = get_bytes(&args[2])?;
            let id_str = get_str(&args[3])?;

            let mut mkstream = false;
            for arg in &args[4..] {
                if get_str(arg)?.to_uppercase() == "MKSTREAM" {
                    mkstream = true;
                }
            }

            let db = ctx.store().database(ctx.selected_db());

            let start_id = if id_str == "$" {
                // Start from the last ID
                match db.get(&key) {
                    Some(entry) => {
                        if let RedisValue::Stream(s) = &entry.value {
                            s.last_id.clone()
                        } else {
                            return Err(CommandError::WrongType);
                        }
                    }
                    None => {
                        if mkstream {
                            StreamId::new(0, 0)
                        } else {
                            return Err(CommandError::NoSuchKey);
                        }
                    }
                }
            } else if id_str == "0" {
                StreamId::new(0, 0)
            } else {
                StreamId::parse(id_str).ok_or_else(|| {
                    CommandError::InvalidArgument(format!("Invalid stream ID: {}", id_str))
                })?
            };

            let mut stream = match db.get(&key) {
                Some(entry) => {
                    if entry.is_expired() {
                        db.delete(&key);
                        if mkstream {
                            StreamData::new()
                        } else {
                            return Err(CommandError::NoSuchKey);
                        }
                    } else {
                        match entry.value {
                            RedisValue::Stream(s) => s,
                            _ => return Err(CommandError::WrongType),
                        }
                    }
                }
                None => {
                    if mkstream {
                        StreamData::new()
                    } else {
                        return Err(CommandError::NoSuchKey);
                    }
                }
            };

            stream
                .create_group(group_name, start_id, mkstream)
                .map_err(|e| CommandError::InvalidArgument(e.to_string()))?;

            db.set(key, Entry::new(RedisValue::Stream(stream)));

            Ok(RespValue::ok())
        }
        "DESTROY" => {
            if args.len() < 3 {
                return Err(CommandError::WrongArity("XGROUP".to_string()));
            }
            let key = get_bytes(&args[1])?;
            let group_name = get_bytes(&args[2])?;

            let db = ctx.store().database(ctx.selected_db());

            match db.get(&key) {
                Some(entry) => {
                    if entry.is_expired() {
                        return Ok(RespValue::Integer(0));
                    }
                    match entry.value {
                        RedisValue::Stream(mut s) => {
                            let removed = s.groups.remove(&group_name).is_some();
                            db.set(key, Entry::new(RedisValue::Stream(s)));
                            Ok(RespValue::Integer(if removed { 1 } else { 0 }))
                        }
                        _ => Err(CommandError::WrongType),
                    }
                }
                None => Ok(RespValue::Integer(0)),
            }
        }
        "SETID" => {
            if args.len() < 4 {
                return Err(CommandError::WrongArity("XGROUP".to_string()));
            }
            let key = get_bytes(&args[1])?;
            let group_name = get_bytes(&args[2])?;
            let id_str = get_str(&args[3])?;

            let db = ctx.store().database(ctx.selected_db());

            let new_id = if id_str == "$" {
                match db.get(&key) {
                    Some(entry) => {
                        if let RedisValue::Stream(s) = &entry.value {
                            s.last_id.clone()
                        } else {
                            return Err(CommandError::WrongType);
                        }
                    }
                    None => return Err(CommandError::NoSuchKey),
                }
            } else {
                StreamId::parse(id_str).ok_or_else(|| {
                    CommandError::InvalidArgument(format!("Invalid stream ID: {}", id_str))
                })?
            };

            match db.get(&key) {
                Some(entry) => {
                    if entry.is_expired() {
                        return Err(CommandError::NoSuchKey);
                    }
                    match entry.value {
                        RedisValue::Stream(mut s) => {
                            let group = s.groups.get_mut(&group_name).ok_or_else(|| {
                                CommandError::InvalidArgument(
                                    "NOGROUP No such consumer group".to_string(),
                                )
                            })?;
                            group.last_delivered_id = new_id;
                            db.set(key, Entry::new(RedisValue::Stream(s)));
                            Ok(RespValue::ok())
                        }
                        _ => Err(CommandError::WrongType),
                    }
                }
                None => Err(CommandError::NoSuchKey),
            }
        }
        "CREATECONSUMER" => {
            if args.len() < 4 {
                return Err(CommandError::WrongArity("XGROUP".to_string()));
            }
            let key = get_bytes(&args[1])?;
            let group_name = get_bytes(&args[2])?;
            let consumer_name = get_bytes(&args[3])?;

            let db = ctx.store().database(ctx.selected_db());

            match db.get(&key) {
                Some(entry) => {
                    if entry.is_expired() {
                        return Err(CommandError::NoSuchKey);
                    }
                    match entry.value {
                        RedisValue::Stream(mut s) => {
                            let group = s.groups.get_mut(&group_name).ok_or_else(|| {
                                CommandError::InvalidArgument(
                                    "NOGROUP No such consumer group".to_string(),
                                )
                            })?;

                            let created = if group.consumers.contains_key(&consumer_name) {
                                0
                            } else {
                                group.consumers.insert(
                                    consumer_name.clone(),
                                    ferris_core::Consumer {
                                        name: consumer_name,
                                        pending_count: 0,
                                        last_seen: std::time::Instant::now(),
                                    },
                                );
                                1
                            };

                            db.set(key, Entry::new(RedisValue::Stream(s)));
                            Ok(RespValue::Integer(created))
                        }
                        _ => Err(CommandError::WrongType),
                    }
                }
                None => Err(CommandError::NoSuchKey),
            }
        }
        "DELCONSUMER" => {
            if args.len() < 4 {
                return Err(CommandError::WrongArity("XGROUP".to_string()));
            }
            let key = get_bytes(&args[1])?;
            let group_name = get_bytes(&args[2])?;
            let consumer_name = get_bytes(&args[3])?;

            let db = ctx.store().database(ctx.selected_db());

            match db.get(&key) {
                Some(entry) => {
                    if entry.is_expired() {
                        return Err(CommandError::NoSuchKey);
                    }
                    match entry.value {
                        RedisValue::Stream(mut s) => {
                            let group = s.groups.get_mut(&group_name).ok_or_else(|| {
                                CommandError::InvalidArgument(
                                    "NOGROUP No such consumer group".to_string(),
                                )
                            })?;

                            // Count pending entries for this consumer
                            let pending_count = group
                                .pending
                                .values()
                                .filter(|p| p.consumer == consumer_name)
                                .count();

                            group.consumers.remove(&consumer_name);
                            db.set(key, Entry::new(RedisValue::Stream(s)));
                            Ok(RespValue::Integer(pending_count as i64))
                        }
                        _ => Err(CommandError::WrongType),
                    }
                }
                None => Err(CommandError::NoSuchKey),
            }
        }
        "HELP" => {
            let help = vec![
                RespValue::BulkString(Bytes::from(
                    "XGROUP CREATE <key> <group> <id|$> [MKSTREAM] [ENTRIESREAD <n>]",
                )),
                RespValue::BulkString(Bytes::from("XGROUP DESTROY <key> <group>")),
                RespValue::BulkString(Bytes::from(
                    "XGROUP SETID <key> <group> <id|$> [ENTRIESREAD <n>]",
                )),
                RespValue::BulkString(Bytes::from(
                    "XGROUP CREATECONSUMER <key> <group> <consumer>",
                )),
                RespValue::BulkString(Bytes::from("XGROUP DELCONSUMER <key> <group> <consumer>")),
                RespValue::BulkString(Bytes::from("XGROUP HELP")),
            ];
            Ok(RespValue::Array(help))
        }
        _ => Err(CommandError::UnknownSubcommand(
            subcommand,
            "XGROUP".to_string(),
        )),
    }
}

// ---------------------------------------------------------------------------
// XSETID key last-id [ENTRIESADDED entries_added] [MAXDELETEDID max_deleted_id]
// ---------------------------------------------------------------------------

/// XSETID key last-id
///
/// Sets the last ID of a stream.
/// Internal command used for replication.
pub fn xsetid(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("XSETID".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let id_str = get_str(&args[1])?;

    let new_id = StreamId::parse(id_str)
        .ok_or_else(|| CommandError::InvalidArgument(format!("Invalid stream ID: {}", id_str)))?;

    let db = ctx.store().database(ctx.selected_db());

    match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                return Err(CommandError::NoSuchKey);
            }
            match entry.value {
                RedisValue::Stream(mut s) => {
                    s.last_id = new_id;
                    db.set(key, Entry::new(RedisValue::Stream(s)));
                    Ok(RespValue::ok())
                }
                _ => Err(CommandError::WrongType),
            }
        }
        None => Err(CommandError::NoSuchKey),
    }
}

// ---------------------------------------------------------------------------
// XACK key group id [id ...]
// ---------------------------------------------------------------------------

/// XACK key group id [id ...]
///
/// Acknowledges message(s) as processed by a consumer group.
/// Returns the number of messages acknowledged.
pub fn xack(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongArity("XACK".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let group_name = get_bytes(&args[1])?;

    let ids: Vec<StreamId> = args[2..]
        .iter()
        .map(|arg| {
            let s = get_str(arg)?;
            StreamId::parse(s)
                .ok_or_else(|| CommandError::InvalidArgument(format!("Invalid stream ID: {}", s)))
        })
        .collect::<Result<_, _>>()?;

    let db = ctx.store().database(ctx.selected_db());

    match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                return Ok(RespValue::Integer(0));
            }
            match entry.value {
                RedisValue::Stream(mut s) => {
                    let group = s.groups.get_mut(&group_name).ok_or_else(|| {
                        CommandError::InvalidArgument("NOGROUP No such consumer group".to_string())
                    })?;

                    let mut acked = 0;
                    for id in &ids {
                        if group.pending.remove(id).is_some() {
                            acked += 1;
                        }
                    }

                    db.set(key, Entry::new(RedisValue::Stream(s)));
                    Ok(RespValue::Integer(acked))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        None => Ok(RespValue::Integer(0)),
    }
}

// ---------------------------------------------------------------------------
// XPENDING key group [[IDLE min-idle-time] start end count [consumer]]
// ---------------------------------------------------------------------------

/// XPENDING key group [[IDLE min-idle-time] start end count [consumer]]
///
/// Returns information about pending messages in a consumer group.
pub fn xpending(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("XPENDING".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let group_name = get_bytes(&args[1])?;

    let db = ctx.store().database(ctx.selected_db());

    match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                return Err(CommandError::NoSuchKey);
            }
            match &entry.value {
                RedisValue::Stream(s) => {
                    let group = s.groups.get(&group_name).ok_or_else(|| {
                        CommandError::InvalidArgument("NOGROUP No such consumer group".to_string())
                    })?;

                    if args.len() == 2 {
                        // Summary form
                        let pending_count = group.pending.len();
                        if pending_count == 0 {
                            return Ok(RespValue::Array(vec![
                                RespValue::Integer(0),
                                RespValue::Null,
                                RespValue::Null,
                                RespValue::Null,
                            ]));
                        }

                        let min_id = group.pending.keys().min().map(|id| id.to_string());
                        let max_id = group.pending.keys().max().map(|id| id.to_string());

                        // Count by consumer
                        let mut consumer_counts: std::collections::HashMap<&Bytes, i64> =
                            std::collections::HashMap::new();
                        for pe in group.pending.values() {
                            *consumer_counts.entry(&pe.consumer).or_insert(0) += 1;
                        }

                        let consumers: Vec<RespValue> = consumer_counts
                            .into_iter()
                            .map(|(name, count)| {
                                RespValue::Array(vec![
                                    RespValue::BulkString(name.clone()),
                                    RespValue::BulkString(Bytes::from(count.to_string())),
                                ])
                            })
                            .collect();

                        Ok(RespValue::Array(vec![
                            RespValue::Integer(pending_count as i64),
                            min_id
                                .map(|s| RespValue::BulkString(Bytes::from(s)))
                                .unwrap_or(RespValue::Null),
                            max_id
                                .map(|s| RespValue::BulkString(Bytes::from(s)))
                                .unwrap_or(RespValue::Null),
                            RespValue::Array(consumers),
                        ]))
                    } else {
                        // Extended form with range
                        // For now, return empty array - full implementation would parse range args
                        Ok(RespValue::Array(vec![]))
                    }
                }
                _ => Err(CommandError::WrongType),
            }
        }
        None => Err(CommandError::NoSuchKey),
    }
}

// ---------------------------------------------------------------------------
// XREADGROUP GROUP group consumer [COUNT count] [BLOCK milliseconds] [NOACK] STREAMS key [key ...] id [id ...]
// ---------------------------------------------------------------------------

/// XREADGROUP GROUP group consumer [COUNT count] [BLOCK milliseconds] [NOACK] STREAMS key [key ...] id [id ...]
///
/// Read data from a stream via a consumer group.
///
/// Time complexity: O(N) with N being the number of elements returned.
#[allow(clippy::too_many_lines)]
pub fn xreadgroup(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    use std::time::Instant;

    if args.len() < 6 {
        return Err(CommandError::WrongArity("XREADGROUP".to_string()));
    }

    // Parse GROUP group consumer
    if get_str(&args[0])?.to_uppercase() != "GROUP" {
        return Err(CommandError::SyntaxError);
    }

    let group_name = get_bytes(&args[1])?;
    let consumer_name = get_bytes(&args[2])?;

    let mut idx = 3;
    let mut count: Option<usize> = None;
    #[allow(clippy::collection_is_never_read)]
    let mut _block: Option<u64> = None; // TODO: implement blocking
    let mut noack = false;

    // Parse options
    while idx < args.len() {
        let opt = get_str(&args[idx])?.to_uppercase();
        match opt.as_str() {
            "COUNT" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                count = Some(parse_usize(&args[idx])?);
                idx += 1;
            }
            "BLOCK" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                _block = Some(parse_int(&args[idx])? as u64);
                idx += 1;
            }
            "NOACK" => {
                noack = true;
                idx += 1;
            }
            "STREAMS" => {
                idx += 1;
                break;
            }
            _ => return Err(CommandError::SyntaxError),
        }
    }

    if idx >= args.len() {
        return Err(CommandError::SyntaxError);
    }

    // Parse STREAMS keys and IDs
    let remaining = args.len() - idx;
    if remaining % 2 != 0 {
        return Err(CommandError::SyntaxError);
    }

    let num_streams = remaining / 2;
    let keys = &args[idx..idx + num_streams];
    let ids = &args[idx + num_streams..];

    let db = ctx.store().database(ctx.selected_db());
    let mut results = Vec::new();

    for i in 0..num_streams {
        let key = get_bytes(&keys[i])?;
        let id_str = get_str(&ids[i])?;

        // Special ID ">" means "never delivered to this consumer group"
        let start_id = if id_str == ">" {
            None // Will use last_delivered_id
        } else {
            Some(StreamId::parse(id_str).ok_or_else(|| {
                CommandError::InvalidArgument(format!("Invalid stream ID: {}", id_str))
            })?)
        };

        #[allow(clippy::single_match)]
        match db.get(&key) {
            Some(entry) => match &entry.value {
                RedisValue::Stream(stream) => {
                    // Get or create consumer group
                    if !stream.groups.contains_key(&group_name) {
                        return Err(CommandError::InvalidArgument(format!(
                            "NOGROUP No such consumer group '{}' for key '{}'",
                            String::from_utf8_lossy(&group_name),
                            String::from_utf8_lossy(&key)
                        )));
                    }

                    let mut stream = stream.clone();
                    let group = stream.groups.get_mut(&group_name).unwrap(); // Safe: we just checked existence above

                    // Ensure consumer exists
                    if !group.consumers.contains_key(&consumer_name) {
                        use ferris_core::Consumer;
                        group.consumers.insert(
                            consumer_name.clone(),
                            Consumer {
                                name: consumer_name.clone(),
                                pending_count: 0,
                                last_seen: Instant::now(),
                            },
                        );
                    }

                    // Update consumer last seen
                    if let Some(consumer) = group.consumers.get_mut(&consumer_name) {
                        consumer.last_seen = Instant::now();
                    }

                    // Determine starting point
                    let actual_start = start_id.unwrap_or_else(|| group.last_delivered_id.clone());

                    // Get entries after start_id
                    let entries: Vec<_> = stream
                        .entries
                        .range((
                            std::ops::Bound::Excluded(actual_start),
                            std::ops::Bound::Unbounded,
                        ))
                        .take(count.unwrap_or(usize::MAX))
                        .map(|(id, fields)| {
                            let fields_vec: Vec<(Bytes, Bytes)> = fields.clone();
                            (id.clone(), fields_vec)
                        })
                        .collect();

                    if !entries.is_empty() {
                        // Add to pending list (unless NOACK)
                        if !noack {
                            use ferris_core::PendingEntry;
                            for (id, _) in &entries {
                                group.pending.insert(
                                    id.clone(),
                                    PendingEntry {
                                        consumer: consumer_name.clone(),
                                        delivery_time: Instant::now(),
                                        delivery_count: 1,
                                    },
                                );
                                if let Some(consumer) = group.consumers.get_mut(&consumer_name) {
                                    consumer.pending_count += 1;
                                }
                            }
                        }

                        // Update last_delivered_id
                        if let Some((last_id, _)) = entries.last() {
                            group.last_delivered_id = last_id.clone();
                        }

                        // Save back to database
                        db.set(
                            key.clone(),
                            Entry {
                                value: RedisValue::Stream(stream),
                                expires_at: entry.expires_at,
                                version: entry.version + 1,
                                last_access: Instant::now(),
                                lfu_counter: entry.lfu_counter,
                            },
                        );

                        // Format response
                        let stream_entries: Vec<RespValue> = entries
                            .into_iter()
                            .map(|(id, fields)| {
                                let fields_resp: Vec<RespValue> = fields
                                    .into_iter()
                                    .flat_map(|(k, v)| {
                                        vec![RespValue::BulkString(k), RespValue::BulkString(v)]
                                    })
                                    .collect();

                                RespValue::Array(vec![
                                    RespValue::BulkString(Bytes::from(id.to_string())),
                                    RespValue::Array(fields_resp),
                                ])
                            })
                            .collect();

                        results.push(RespValue::Array(vec![
                            RespValue::BulkString(key),
                            RespValue::Array(stream_entries),
                        ]));
                    }
                }
                _ => return Err(CommandError::WrongType),
            },
            None => {} // Key doesn't exist, skip
        }
    }

    if results.is_empty() {
        Ok(RespValue::Null)
    } else {
        Ok(RespValue::Array(results))
    }
}

// ---------------------------------------------------------------------------
// XCLAIM key group consumer min-idle-time id [id ...] [IDLE ms] [TIME unix-ms] [RETRYCOUNT count] [FORCE] [JUSTID]
// ---------------------------------------------------------------------------

/// XCLAIM key group consumer min-idle-time id [id ...] [IDLE ms] [TIME unix-ms] [RETRYCOUNT count] [FORCE] [JUSTID]
///
/// Changes (or acquires) ownership of messages in a consumer group.
///
/// Time complexity: O(log N) with N being the number of messages in the pending entries list.
#[allow(clippy::too_many_lines)]
pub fn xclaim(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    use std::time::{Duration, Instant};

    if args.len() < 5 {
        return Err(CommandError::WrongArity("XCLAIM".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let group_name = get_bytes(&args[1])?;
    let consumer_name = get_bytes(&args[2])?;
    let min_idle_ms = parse_int(&args[3])?;

    let mut idx = 4;
    let mut ids = Vec::new();

    // Parse IDs
    while idx < args.len() {
        let arg_str = get_str(&args[idx])?;
        if arg_str
            .to_uppercase()
            .chars()
            .all(|c| c.is_ascii_alphabetic())
        {
            // This is an option
            break;
        }
        let id = StreamId::parse(arg_str).ok_or_else(|| {
            CommandError::InvalidArgument(format!("Invalid stream ID: {}", arg_str))
        })?;
        ids.push(id);
        idx += 1;
    }

    if ids.is_empty() {
        return Err(CommandError::SyntaxError);
    }

    // Parse options
    let mut idle: Option<u64> = None;
    let mut time: Option<u64> = None;
    let mut retry_count: Option<u64> = None;
    let mut force = false;
    let mut justid = false;

    while idx < args.len() {
        let opt = get_str(&args[idx])?.to_uppercase();
        match opt.as_str() {
            "IDLE" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                idle = Some(parse_int(&args[idx])? as u64);
                idx += 1;
            }
            "TIME" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                time = Some(parse_int(&args[idx])? as u64);
                idx += 1;
            }
            "RETRYCOUNT" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                retry_count = Some(parse_int(&args[idx])? as u64);
                idx += 1;
            }
            "FORCE" => {
                force = true;
                idx += 1;
            }
            "JUSTID" => {
                justid = true;
                idx += 1;
            }
            _ => return Err(CommandError::SyntaxError),
        }
    }

    let db = ctx.store().database(ctx.selected_db());

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::Stream(stream) => {
                let mut stream = stream.clone();

                // Check if group exists
                if !stream.groups.contains_key(&group_name) {
                    return Err(CommandError::InvalidArgument(format!(
                        "NOGROUP No such consumer group '{}'",
                        String::from_utf8_lossy(&group_name)
                    )));
                }

                let group = stream.groups.get_mut(&group_name).unwrap(); // Safe: we just checked existence above

                // Ensure consumer exists
                if !group.consumers.contains_key(&consumer_name) {
                    use ferris_core::Consumer;
                    group.consumers.insert(
                        consumer_name.clone(),
                        Consumer {
                            name: consumer_name.clone(),
                            pending_count: 0,
                            last_seen: Instant::now(),
                        },
                    );
                }

                let mut claimed = Vec::new();

                for id in ids {
                    // Check if entry exists in pending
                    if let Some(pending) = group.pending.get_mut(&id) {
                        // Check if idle time is sufficient
                        let elapsed = pending.delivery_time.elapsed().as_millis() as i64;
                        if elapsed < min_idle_ms && !force {
                            continue;
                        }

                        // Decrement old consumer's pending count
                        if let Some(old_consumer) = group.consumers.get_mut(&pending.consumer) {
                            old_consumer.pending_count =
                                old_consumer.pending_count.saturating_sub(1);
                        }

                        // Update pending entry
                        pending.consumer = consumer_name.clone();
                        if let Some(idle_ms) = idle {
                            pending.delivery_time = Instant::now()
                                .checked_sub(Duration::from_millis(idle_ms))
                                .unwrap_or_else(Instant::now);
                        } else if let Some(time_ms) = time {
                            // Convert Unix timestamp to Instant (approximate)
                            let now = Instant::now();
                            let current_ms = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis() as u64;
                            if time_ms < current_ms {
                                pending.delivery_time = now
                                    .checked_sub(Duration::from_millis(current_ms - time_ms))
                                    .unwrap_or(now);
                            }
                        } else {
                            pending.delivery_time = Instant::now();
                        }

                        if let Some(count) = retry_count {
                            pending.delivery_count = count;
                        } else {
                            pending.delivery_count += 1;
                        }

                        // Increment new consumer's pending count
                        if let Some(new_consumer) = group.consumers.get_mut(&consumer_name) {
                            new_consumer.pending_count += 1;
                            new_consumer.last_seen = Instant::now();
                        }

                        // Get entry data if needed
                        if !justid {
                            if let Some(fields) = stream.entries.get(&id) {
                                claimed.push((id, fields.clone()));
                            }
                        } else {
                            claimed.push((id, vec![]));
                        }
                    } else if force {
                        // FORCE: Claim even if not in pending
                        use ferris_core::PendingEntry;
                        group.pending.insert(
                            id.clone(),
                            PendingEntry {
                                consumer: consumer_name.clone(),
                                delivery_time: idle
                                    .and_then(|ms| {
                                        Instant::now().checked_sub(Duration::from_millis(ms))
                                    })
                                    .unwrap_or_else(Instant::now),
                                delivery_count: retry_count.unwrap_or(1),
                            },
                        );

                        if let Some(consumer) = group.consumers.get_mut(&consumer_name) {
                            consumer.pending_count += 1;
                        }

                        if !justid {
                            if let Some(fields) = stream.entries.get(&id) {
                                claimed.push((id, fields.clone()));
                            }
                        } else {
                            claimed.push((id, vec![]));
                        }
                    }
                }

                // Save back to database
                db.set(
                    key,
                    Entry {
                        value: RedisValue::Stream(stream),
                        expires_at: entry.expires_at,
                        version: entry.version + 1,
                        last_access: Instant::now(),
                        lfu_counter: entry.lfu_counter,
                    },
                );

                // Format response
                let result: Vec<RespValue> = claimed
                    .into_iter()
                    .map(|(id, fields)| {
                        if justid {
                            RespValue::BulkString(Bytes::from(id.to_string()))
                        } else {
                            let fields_resp: Vec<RespValue> = fields
                                .into_iter()
                                .flat_map(|(k, v)| {
                                    vec![RespValue::BulkString(k), RespValue::BulkString(v)]
                                })
                                .collect();

                            RespValue::Array(vec![
                                RespValue::BulkString(Bytes::from(id.to_string())),
                                RespValue::Array(fields_resp),
                            ])
                        }
                    })
                    .collect();

                Ok(RespValue::Array(result))
            }
            _ => Err(CommandError::WrongType),
        },
        None => Err(CommandError::NoSuchKey),
    }
}

// ---------------------------------------------------------------------------
// XAUTOCLAIM key group consumer min-idle-time start [COUNT count] [JUSTID]
// ---------------------------------------------------------------------------

/// XAUTOCLAIM key group consumer min-idle-time start [COUNT count] [JUSTID]
///
/// Automatically claims idle messages from a consumer group (Redis 6.2+).
///
/// Time complexity: O(1) if COUNT is small, O(N) with N being the number of messages claimed.
pub fn xautoclaim(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    use std::time::Instant;

    if args.len() < 5 {
        return Err(CommandError::WrongArity("XAUTOCLAIM".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let group_name = get_bytes(&args[1])?;
    let consumer_name = get_bytes(&args[2])?;
    let min_idle_ms = parse_int(&args[3])?;
    let start_id = get_str(&args[4])?;

    let mut idx = 5;
    let mut count: Option<usize> = None;
    let mut justid = false;

    // Parse options
    while idx < args.len() {
        let opt = get_str(&args[idx])?.to_uppercase();
        match opt.as_str() {
            "COUNT" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                count = Some(parse_usize(&args[idx])?);
                idx += 1;
            }
            "JUSTID" => {
                justid = true;
                idx += 1;
            }
            _ => return Err(CommandError::SyntaxError),
        }
    }

    let start = if start_id == "0-0" {
        StreamId::new(0, 0)
    } else {
        StreamId::parse(start_id).ok_or_else(|| {
            CommandError::InvalidArgument(format!("Invalid stream ID: {}", start_id))
        })?
    };

    let db = ctx.store().database(ctx.selected_db());

    match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::Stream(stream) => {
                let mut stream = stream.clone();

                // Check if group exists
                if !stream.groups.contains_key(&group_name) {
                    return Err(CommandError::InvalidArgument(format!(
                        "NOGROUP No such consumer group '{}'",
                        String::from_utf8_lossy(&group_name)
                    )));
                }

                let group = stream.groups.get_mut(&group_name).unwrap(); // Safe: we just checked existence above

                // Ensure consumer exists
                if !group.consumers.contains_key(&consumer_name) {
                    use ferris_core::Consumer;
                    group.consumers.insert(
                        consumer_name.clone(),
                        Consumer {
                            name: consumer_name.clone(),
                            pending_count: 0,
                            last_seen: Instant::now(),
                        },
                    );
                }

                let mut claimed = Vec::new();
                let mut next_id = start.clone();
                let max_count = count.unwrap_or(100); // Default to 100 like Redis
                let start_clone = start.clone();

                // Iterate through pending entries starting from start_id
                for (id, pending) in group
                    .pending
                    .iter_mut()
                    .filter(move |(id, _)| **id >= start_clone)
                    .take(max_count * 2)
                {
                    // Check if idle time is sufficient
                    let elapsed = pending.delivery_time.elapsed().as_millis() as i64;
                    if elapsed >= min_idle_ms {
                        // Decrement old consumer's pending count
                        if let Some(old_consumer) = group.consumers.get_mut(&pending.consumer) {
                            old_consumer.pending_count =
                                old_consumer.pending_count.saturating_sub(1);
                        }

                        // Claim for new consumer
                        pending.consumer = consumer_name.clone();
                        pending.delivery_time = Instant::now();
                        pending.delivery_count += 1;

                        // Increment new consumer's pending count
                        if let Some(new_consumer) = group.consumers.get_mut(&consumer_name) {
                            new_consumer.pending_count += 1;
                            new_consumer.last_seen = Instant::now();
                        }

                        // Add to claimed list
                        if !justid {
                            if let Some(fields) = stream.entries.get(id) {
                                claimed.push((id.clone(), fields.clone()));
                            }
                        } else {
                            claimed.push((id.clone(), vec![]));
                        }

                        next_id = id.clone();

                        if claimed.len() >= max_count {
                            break;
                        }
                    }

                    next_id = id.clone();
                }

                // Calculate next start cursor
                let next_cursor = if claimed.len() >= max_count {
                    // More entries might be available
                    Bytes::from(next_id.to_string())
                } else {
                    // No more entries
                    Bytes::from("0-0")
                };

                // Save back to database
                db.set(
                    key,
                    Entry {
                        value: RedisValue::Stream(stream),
                        expires_at: entry.expires_at,
                        version: entry.version + 1,
                        last_access: Instant::now(),
                        lfu_counter: entry.lfu_counter,
                    },
                );

                // Format response: [next_id, [claimed entries]]
                let entries_resp: Vec<RespValue> = claimed
                    .into_iter()
                    .map(|(id, fields)| {
                        if justid {
                            RespValue::BulkString(Bytes::from(id.to_string()))
                        } else {
                            let fields_resp: Vec<RespValue> = fields
                                .into_iter()
                                .flat_map(|(k, v)| {
                                    vec![RespValue::BulkString(k), RespValue::BulkString(v)]
                                })
                                .collect();

                            RespValue::Array(vec![
                                RespValue::BulkString(Bytes::from(id.to_string())),
                                RespValue::Array(fields_resp),
                            ])
                        }
                    })
                    .collect();

                Ok(RespValue::Array(vec![
                    RespValue::BulkString(next_cursor),
                    RespValue::Array(entries_resp),
                ]))
            }
            _ => Err(CommandError::WrongType),
        },
        None => Err(CommandError::NoSuchKey),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_id_parse() {
        assert_eq!(StreamId::parse("1234-5"), Some(StreamId::new(1234, 5)));
        assert_eq!(StreamId::parse("1234"), Some(StreamId::new(1234, 0)));
        assert_eq!(StreamId::parse("-"), Some(StreamId::min()));
        assert_eq!(StreamId::parse("+"), Some(StreamId::max()));
        assert_eq!(StreamId::parse("*"), None);
    }

    #[test]
    fn test_stream_id_display() {
        assert_eq!(StreamId::new(1234, 5).to_string(), "1234-5");
    }

    #[test]
    fn test_stream_id_ordering() {
        let id1 = StreamId::new(100, 0);
        let id2 = StreamId::new(100, 1);
        let id3 = StreamId::new(101, 0);

        assert!(id1 < id2);
        assert!(id2 < id3);
        assert!(id1 < id3);
    }
}

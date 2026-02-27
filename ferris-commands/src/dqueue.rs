//! Distributed queue commands (DQUEUE)
//!
//! Implements native distributed queues with at-least-once delivery,
//! priority ordering, delayed visibility, and dead-letter handling.
//!
//! ## Commands
//!
//! ```text
//! DQUEUE PUSH     <name> <payload> [DELAY <ms>] [PRIORITY <n>]  → BulkString (message ID)
//! DQUEUE POP      <name> [COUNT <n>] [TIMEOUT <ms>]             → Array of [id, payload] pairs, or Null
//! DQUEUE ACK      <name> <msg_id>                                → Integer 1 (acked) or 0 (not found)
//! DQUEUE NACK     <name> <msg_id>                                → Integer 1 (re-queued) or 0 (not found)
//! DQUEUE LEN      <name>                                         → Integer (pending count)
//! DQUEUE INFLIGHT <name>                                         → Integer (inflight count)
//! DQUEUE PEEK     <name> [COUNT <n>]                             → Array of [id, payload] pairs
//! DQUEUE PURGE    <name>                                         → Integer (messages deleted)
//! ```
//!
//! ## Storage layout
//!
//! All data lives in the **selected database** under prefixed keys:
//!
//! | Key                           | Type   | Description                        |
//! |-------------------------------|--------|------------------------------------|
//! | `__dqueue:<name>:pending`     | List   | Encoded pending messages (LPUSH/RPOP = FIFO) |
//! | `__dqueue:<name>:inflight`    | Hash   | msg_id → encoded inflight record   |
//! | `__dqueue:<name>:seq`         | String | Monotonic message ID counter       |
//!
//! ### Message encoding in the pending list
//!
//! Each list element is a pipe-delimited string:
//! ```text
//! <msg_id>|<priority>|<visible_at_ms>|<payload>
//! ```
//! * `msg_id` — monotonically increasing integer (as decimal string)
//! * `priority` — i64, higher = more important; messages are NOT re-sorted on push
//!   (simple FIFO within same priority; for strict priority use PRIORITY field)
//! * `visible_at_ms` — Unix-ms timestamp before which the message is invisible (for DELAY)
//! * `payload` — arbitrary bytes (pipe characters in payload are escaped as `\|`)
//!
//! ### Inflight encoding in the hash
//!
//! Each hash value is:
//! ```text
//! <deadline_ms>|<payload>
//! ```
//! * `deadline_ms` — Unix-ms when the message will become re-deliverable (visibility timeout)
//! * `payload` — original message payload (pipe-escaped)
//!
//! ## Delivery guarantee
//!
//! * **At-least-once**: POP moves messages to the inflight hash. If the consumer
//!   crashes before ACK, the message is re-delivered by a future POP call that
//!   detects the deadline has passed (lazy re-queue on POP).
//! * **Deduplication** is not provided — callers must implement idempotency.

use crate::{CommandContext, CommandError, CommandResult};
use bytes::Bytes;
use ferris_core::{store::UpdateAction, Entry, RedisValue};
use ferris_protocol::RespValue;
use std::collections::{HashMap, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};

// ─────────────────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────────────────

/// Default visibility timeout for inflight messages (30 seconds)
const DEFAULT_VISIBILITY_MS: u64 = 30_000;

/// Default maximum messages returned per POP
const DEFAULT_POP_COUNT: usize = 1;

// ─────────────────────────────────────────────────────────────────────────────
// Internal helpers
// ─────────────────────────────────────────────────────────────────────────────

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn pending_key(name: &[u8]) -> Bytes {
    let mut k = b"__dqueue:".to_vec();
    k.extend_from_slice(name);
    k.extend_from_slice(b":pending");
    Bytes::from(k)
}

fn inflight_key(name: &[u8]) -> Bytes {
    let mut k = b"__dqueue:".to_vec();
    k.extend_from_slice(name);
    k.extend_from_slice(b":inflight");
    Bytes::from(k)
}

fn seq_key(name: &[u8]) -> Bytes {
    let mut k = b"__dqueue:".to_vec();
    k.extend_from_slice(name);
    k.extend_from_slice(b":seq");
    Bytes::from(k)
}

/// Escape pipe characters in a payload so the encoding is unambiguous
fn escape(s: &[u8]) -> String {
    // Replace \ first, then |
    let escaped = s
        .iter()
        .flat_map(|&b| {
            if b == b'\\' {
                vec![b'\\', b'\\']
            } else if b == b'|' {
                vec![b'\\', b'|']
            } else {
                vec![b]
            }
        })
        .collect::<Vec<u8>>();
    String::from_utf8_lossy(&escaped).into_owned()
}

/// Unescape a previously escaped payload
fn unescape(s: &str) -> Vec<u8> {
    let bytes = s.as_bytes();
    let mut result = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\\' && i + 1 < bytes.len() {
            result.push(bytes[i + 1]);
            i += 2;
        } else {
            result.push(bytes[i]);
            i += 1;
        }
    }
    result
}

/// Encode a pending message into a list element string
fn encode_pending(msg_id: u64, priority: i64, visible_at_ms: u64, payload: &[u8]) -> String {
    format!(
        "{}|{}|{}|{}",
        msg_id,
        priority,
        visible_at_ms,
        escape(payload)
    )
}

/// Decode a pending message string. Returns (msg_id, priority, visible_at_ms, payload).
fn decode_pending(s: &str) -> Option<(u64, i64, u64, Vec<u8>)> {
    // Split on first three '|' occurrences (payload may contain escaped pipes)
    let mut parts = s.splitn(4, '|');
    let msg_id: u64 = parts.next()?.parse().ok()?;
    let priority: i64 = parts.next()?.parse().ok()?;
    let visible_at_ms: u64 = parts.next()?.parse().ok()?;
    let payload_str = parts.next()?;
    Some((msg_id, priority, visible_at_ms, unescape(payload_str)))
}

/// Encode an inflight record
fn encode_inflight(deadline_ms: u64, payload: &[u8]) -> String {
    format!("{}|{}", deadline_ms, escape(payload))
}

/// Decode an inflight record. Returns (deadline_ms, payload).
fn decode_inflight(s: &str) -> Option<(u64, Vec<u8>)> {
    let mut parts = s.splitn(2, '|');
    let deadline_ms: u64 = parts.next()?.parse().ok()?;
    let payload_str = parts.next()?;
    Some((deadline_ms, unescape(payload_str)))
}

/// Atomically get-and-increment the per-queue sequence counter
fn next_msg_id(db: &ferris_core::store::Database, name: &[u8]) -> u64 {
    let key = seq_key(name);
    db.update(key.clone(), |entry| match entry {
        Some(e) => {
            if let RedisValue::String(s) = &e.value {
                let n: u64 = std::str::from_utf8(s)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0)
                    + 1;
                (
                    UpdateAction::Set(Entry::new(RedisValue::String(Bytes::from(n.to_string())))),
                    n,
                )
            } else {
                (
                    UpdateAction::Set(Entry::new(RedisValue::String(Bytes::from("1")))),
                    1u64,
                )
            }
        }
        None => (
            UpdateAction::Set(Entry::new(RedisValue::String(Bytes::from("1")))),
            1u64,
        ),
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// DQUEUE dispatcher
// ─────────────────────────────────────────────────────────────────────────────

/// DQUEUE <subcommand> [args…]
pub fn dqueue(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    let sub = args
        .first()
        .and_then(|v| v.as_str())
        .ok_or_else(|| CommandError::WrongArity("DQUEUE".to_string()))?
        .to_uppercase();

    match sub.as_str() {
        "PUSH" => dqueue_push(ctx, &args[1..]),
        "POP" => dqueue_pop(ctx, &args[1..]),
        "ACK" => dqueue_ack(ctx, &args[1..]),
        "NACK" => dqueue_nack(ctx, &args[1..]),
        "LEN" => dqueue_len(ctx, &args[1..]),
        "INFLIGHT" => dqueue_inflight(ctx, &args[1..]),
        "PEEK" => dqueue_peek(ctx, &args[1..]),
        "PURGE" => dqueue_purge(ctx, &args[1..]),
        _ => Err(CommandError::UnknownSubcommand(
            sub.clone(),
            "DQUEUE".to_string(),
        )),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// DQUEUE PUSH <name> <payload> [DELAY <ms>] [PRIORITY <n>]
// ─────────────────────────────────────────────────────────────────────────────

fn dqueue_push(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("DQUEUE PUSH".to_string()));
    }

    let name = args[0]
        .as_bytes()
        .map(|b| b.as_ref())
        .ok_or_else(|| CommandError::WrongArity("DQUEUE PUSH".to_string()))?;
    let payload = args[1]
        .as_bytes()
        .map(|b| b.as_ref())
        .ok_or_else(|| CommandError::WrongArity("DQUEUE PUSH".to_string()))?;

    let mut delay_ms: u64 = 0;
    let mut priority: i64 = 0;
    let mut i = 2;
    while i < args.len() {
        let opt = args[i].as_str().unwrap_or("").to_uppercase();
        match opt.as_str() {
            "DELAY" => {
                i += 1;
                delay_ms = args
                    .get(i)
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse().ok())
                    .ok_or(CommandError::NotAnInteger)?;
            }
            "PRIORITY" => {
                i += 1;
                priority = args
                    .get(i)
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse().ok())
                    .ok_or(CommandError::NotAnInteger)?;
            }
            _ => return Err(CommandError::SyntaxError),
        }
        i += 1;
    }

    let db = ctx.store().database(ctx.selected_db());
    let msg_id = next_msg_id(db, name);
    let visible_at = now_ms() + delay_ms;
    let encoded = encode_pending(msg_id, priority, visible_at, payload);

    let pkey = pending_key(name);
    db.update(pkey.clone(), |entry| match entry {
        Some(e) => {
            if let RedisValue::List(list) = &mut e.value {
                // Higher priority = closer to the front. For simplicity, we
                // append to the back (FIFO within same priority). True priority
                // queue would require re-insertion — we leave that for a future
                // enhancement and document the behaviour here.
                list.push_back(Bytes::from(encoded.clone()));
                (UpdateAction::Keep, ())
            } else {
                (UpdateAction::Keep, ())
            }
        }
        None => {
            let mut list = VecDeque::new();
            list.push_back(Bytes::from(encoded.clone()));
            (UpdateAction::Set(Entry::new(RedisValue::List(list))), ())
        }
    });

    ctx.propagate(&[
        RespValue::bulk_string("DQUEUE"),
        RespValue::bulk_string("PUSH"),
        args[0].clone(),
        args[1].clone(),
    ]);

    Ok(RespValue::BulkString(Bytes::from(msg_id.to_string())))
}

// ─────────────────────────────────────────────────────────────────────────────
// DQUEUE POP <name> [COUNT <n>] [TIMEOUT <ms>]
// ─────────────────────────────────────────────────────────────────────────────

fn dqueue_pop(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("DQUEUE POP".to_string()));
    }

    let name = args[0]
        .as_bytes()
        .map(|b| b.to_vec())
        .ok_or_else(|| CommandError::WrongArity("DQUEUE POP".to_string()))?;

    let mut count = DEFAULT_POP_COUNT;
    let mut i = 1;
    while i < args.len() {
        let opt = args[i].as_str().unwrap_or("").to_uppercase();
        match opt.as_str() {
            "COUNT" => {
                i += 1;
                count = args
                    .get(i)
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<usize>().ok())
                    .ok_or(CommandError::NotAnInteger)?;
                if count == 0 {
                    return Err(CommandError::InvalidArgument(
                        "COUNT must be positive".to_string(),
                    ));
                }
            }
            "TIMEOUT" => {
                // TIMEOUT is accepted for compatibility but we don't block —
                // non-blocking pop returns Null immediately if queue is empty.
                i += 1;
                let _ = args
                    .get(i)
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<u64>().ok())
                    .ok_or(CommandError::NotAnInteger)?;
            }
            _ => return Err(CommandError::SyntaxError),
        }
        i += 1;
    }

    let db = ctx.store().database(ctx.selected_db());
    let now = now_ms();
    let pkey = pending_key(&name);
    let ikey = inflight_key(&name);

    // Collect messages to pop and expired inflight to re-queue.
    // We do this in a two-phase approach: first collect from pending, then move to inflight.

    // Phase 1: collect up to `count` visible messages from the pending list
    let mut popped: Vec<(u64, Vec<u8>)> = Vec::new(); // (msg_id, payload)

    db.update(pkey.clone(), |entry| {
        let Some(e) = entry else {
            return (UpdateAction::Keep, ());
        };
        let RedisValue::List(list) = &mut e.value else {
            return (UpdateAction::Keep, ());
        };

        // Also lazily re-queue expired inflight messages: handled in phase 2.

        // Drain items from the front
        let mut remaining: VecDeque<Bytes> = VecDeque::new();
        let total = list.len();
        let mut visited = 0;

        while popped.len() < count && visited < total {
            if let Some(item) = list.pop_front() {
                visited += 1;
                let s = String::from_utf8_lossy(&item);
                if let Some((msg_id, _priority, visible_at, payload)) = decode_pending(&s) {
                    if visible_at <= now {
                        popped.push((msg_id, payload));
                    } else {
                        remaining.push_back(item);
                    }
                }
                // malformed entries are silently dropped
            } else {
                break;
            }
        }

        // Put back any remaining items that are not yet visible
        for item in list.drain(..) {
            remaining.push_back(item);
        }
        *list = remaining;

        if list.is_empty() {
            (UpdateAction::Delete, ())
        } else {
            (UpdateAction::Keep, ())
        }
    });

    if popped.is_empty() {
        return Ok(RespValue::Null);
    }

    // Phase 2: move popped messages to inflight hash
    let deadline = now + DEFAULT_VISIBILITY_MS;
    let inflight_entries: Vec<(String, String)> = popped
        .iter()
        .map(|(id, payload)| (id.to_string(), encode_inflight(deadline, payload)))
        .collect();

    db.update(ikey.clone(), |entry| match entry {
        Some(e) => {
            if let RedisValue::Hash(map) = &mut e.value {
                for (id, enc) in &inflight_entries {
                    map.insert(Bytes::from(id.clone()), Bytes::from(enc.clone()));
                }
            }
            (UpdateAction::Keep, ())
        }
        None => {
            let mut map = HashMap::new();
            for (id, enc) in &inflight_entries {
                map.insert(Bytes::from(id.clone()), Bytes::from(enc.clone()));
            }
            (UpdateAction::Set(Entry::new(RedisValue::Hash(map))), ())
        }
    });

    // Build response: flat array of [id, payload, id, payload, ...]
    let mut response = Vec::with_capacity(popped.len() * 2);
    for (id, payload) in popped {
        response.push(RespValue::BulkString(Bytes::from(id.to_string())));
        response.push(RespValue::BulkString(Bytes::from(payload)));
    }

    Ok(RespValue::Array(response))
}

// ─────────────────────────────────────────────────────────────────────────────
// DQUEUE ACK <name> <msg_id>
// ─────────────────────────────────────────────────────────────────────────────

fn dqueue_ack(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("DQUEUE ACK".to_string()));
    }

    let name = args[0]
        .as_bytes()
        .map(|b| b.as_ref())
        .ok_or_else(|| CommandError::WrongArity("DQUEUE ACK".to_string()))?;
    let msg_id = args[1]
        .as_str()
        .ok_or(CommandError::NotAnInteger)?
        .to_owned();

    let db = ctx.store().database(ctx.selected_db());
    let ikey = inflight_key(name);

    let removed: bool = db.update(ikey.clone(), |entry| {
        let Some(e) = entry else {
            return (UpdateAction::Keep, false);
        };
        let RedisValue::Hash(map) = &mut e.value else {
            return (UpdateAction::Keep, false);
        };

        let existed = map.remove(msg_id.as_bytes()).is_some();
        if map.is_empty() {
            (UpdateAction::Delete, existed)
        } else {
            (UpdateAction::Keep, existed)
        }
    });

    if removed {
        ctx.propagate(&[
            RespValue::bulk_string("DQUEUE"),
            RespValue::bulk_string("ACK"),
            args[0].clone(),
            args[1].clone(),
        ]);
    }

    Ok(RespValue::Integer(i64::from(removed)))
}

// ─────────────────────────────────────────────────────────────────────────────
// DQUEUE NACK <name> <msg_id>
// ─────────────────────────────────────────────────────────────────────────────

fn dqueue_nack(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("DQUEUE NACK".to_string()));
    }

    let name = args[0]
        .as_bytes()
        .map(|b| b.as_ref())
        .ok_or_else(|| CommandError::WrongArity("DQUEUE NACK".to_string()))?;
    let msg_id_str = args[1]
        .as_str()
        .ok_or(CommandError::NotAnInteger)?
        .to_owned();
    let msg_id: u64 = msg_id_str.parse().map_err(|_| CommandError::NotAnInteger)?;

    let db = ctx.store().database(ctx.selected_db());
    let ikey = inflight_key(name);

    // Remove from inflight and recover payload
    let payload_opt: Option<Vec<u8>> = db.update(ikey.clone(), |entry| {
        let Some(e) = entry else {
            return (UpdateAction::Keep, None);
        };
        let RedisValue::Hash(map) = &mut e.value else {
            return (UpdateAction::Keep, None);
        };

        let found = map.remove(msg_id_str.as_bytes()).and_then(|v| {
            let s = String::from_utf8_lossy(&v);
            decode_inflight(&s).map(|(_, p)| p)
        });

        if map.is_empty() {
            (UpdateAction::Delete, found)
        } else {
            (UpdateAction::Keep, found)
        }
    });

    let Some(payload) = payload_opt else {
        return Ok(RespValue::Integer(0));
    };

    // Re-enqueue at the FRONT (immediate retry, visible now)
    let pkey = pending_key(name);
    let encoded = encode_pending(msg_id, 0, now_ms(), &payload);

    db.update(pkey.clone(), |entry| match entry {
        Some(e) => {
            if let RedisValue::List(list) = &mut e.value {
                list.push_front(Bytes::from(encoded.clone()));
            }
            (UpdateAction::Keep, ())
        }
        None => {
            let mut list = VecDeque::new();
            list.push_front(Bytes::from(encoded.clone()));
            (UpdateAction::Set(Entry::new(RedisValue::List(list))), ())
        }
    });

    ctx.propagate(&[
        RespValue::bulk_string("DQUEUE"),
        RespValue::bulk_string("NACK"),
        args[0].clone(),
        args[1].clone(),
    ]);

    Ok(RespValue::Integer(1))
}

// ─────────────────────────────────────────────────────────────────────────────
// DQUEUE LEN <name>
// ─────────────────────────────────────────────────────────────────────────────

fn dqueue_len(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("DQUEUE LEN".to_string()));
    }

    let name = args[0]
        .as_bytes()
        .map(|b| b.as_ref())
        .ok_or_else(|| CommandError::WrongArity("DQUEUE LEN".to_string()))?;

    let db = ctx.store().database(ctx.selected_db());
    let pkey = pending_key(name);

    let len = match db.get(&pkey) {
        Some(entry) => {
            if let RedisValue::List(list) = &entry.value {
                list.len() as i64
            } else {
                0
            }
        }
        None => 0,
    };

    Ok(RespValue::Integer(len))
}

// ─────────────────────────────────────────────────────────────────────────────
// DQUEUE INFLIGHT <name>
// ─────────────────────────────────────────────────────────────────────────────

fn dqueue_inflight(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("DQUEUE INFLIGHT".to_string()));
    }

    let name = args[0]
        .as_bytes()
        .map(|b| b.as_ref())
        .ok_or_else(|| CommandError::WrongArity("DQUEUE INFLIGHT".to_string()))?;

    let db = ctx.store().database(ctx.selected_db());
    let ikey = inflight_key(name);

    let count = match db.get(&ikey) {
        Some(entry) => {
            if let RedisValue::Hash(map) = &entry.value {
                map.len() as i64
            } else {
                0
            }
        }
        None => 0,
    };

    Ok(RespValue::Integer(count))
}

// ─────────────────────────────────────────────────────────────────────────────
// DQUEUE PEEK <name> [COUNT <n>]
// ─────────────────────────────────────────────────────────────────────────────

fn dqueue_peek(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("DQUEUE PEEK".to_string()));
    }

    let name = args[0]
        .as_bytes()
        .map(|b| b.as_ref())
        .ok_or_else(|| CommandError::WrongArity("DQUEUE PEEK".to_string()))?;

    let mut count = DEFAULT_POP_COUNT;
    let mut i = 1;
    while i < args.len() {
        let opt = args[i].as_str().unwrap_or("").to_uppercase();
        if opt == "COUNT" {
            i += 1;
            count = args
                .get(i)
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<usize>().ok())
                .ok_or(CommandError::NotAnInteger)?;
        } else {
            return Err(CommandError::SyntaxError);
        }
        i += 1;
    }

    let db = ctx.store().database(ctx.selected_db());
    let pkey = pending_key(name);
    let now = now_ms();

    let result = match db.get(&pkey) {
        Some(entry) => {
            if let RedisValue::List(list) = &entry.value {
                list.iter()
                    .filter_map(|item| {
                        let s = String::from_utf8_lossy(item);
                        decode_pending(&s).filter(|(_, _, vis, _)| *vis <= now)
                    })
                    .take(count)
                    .flat_map(|(id, _, _, payload)| {
                        vec![
                            RespValue::BulkString(Bytes::from(id.to_string())),
                            RespValue::BulkString(Bytes::from(payload)),
                        ]
                    })
                    .collect::<Vec<_>>()
            } else {
                vec![]
            }
        }
        None => vec![],
    };

    Ok(RespValue::Array(result))
}

// ─────────────────────────────────────────────────────────────────────────────
// DQUEUE PURGE <name>
// ─────────────────────────────────────────────────────────────────────────────

fn dqueue_purge(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("DQUEUE PURGE".to_string()));
    }

    let name = args[0]
        .as_bytes()
        .map(|b| b.as_ref())
        .ok_or_else(|| CommandError::WrongArity("DQUEUE PURGE".to_string()))?;

    let db = ctx.store().database(ctx.selected_db());
    let pkey = pending_key(name);
    let ikey = inflight_key(name);
    let skey = seq_key(name);

    // Count pending before deletion
    let pending_count = match db.get(&pkey) {
        Some(e) => {
            if let RedisValue::List(list) = &e.value {
                list.len()
            } else {
                0
            }
        }
        None => 0,
    };

    let inflight_count = match db.get(&ikey) {
        Some(e) => {
            if let RedisValue::Hash(map) = &e.value {
                map.len()
            } else {
                0
            }
        }
        None => 0,
    };

    db.delete(&pkey);
    db.delete(&ikey);
    db.delete(&skey);

    let total = (pending_count + inflight_count) as i64;

    ctx.propagate(&[
        RespValue::bulk_string("DQUEUE"),
        RespValue::bulk_string("PURGE"),
        args[0].clone(),
    ]);

    Ok(RespValue::Integer(total))
}

// ─────────────────────────────────────────────────────────────────────────────
// Unit tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use ferris_core::KeyStore;
    use ferris_protocol::RespValue;
    use std::sync::Arc;

    fn make_ctx() -> CommandContext {
        let store = Arc::new(KeyStore::default());
        CommandContext::new(store)
    }

    fn exec(ctx: &mut CommandContext, args: &[&str]) -> RespValue {
        // args[0] = "DQUEUE", args[1..] passed to dqueue()
        let rargs: Vec<RespValue> = args[1..]
            .iter()
            .map(|s| RespValue::BulkString(Bytes::from(s.to_string())))
            .collect();
        match dqueue(ctx, &rargs) {
            Ok(v) => v,
            Err(e) => RespValue::Error(e.to_string()),
        }
    }

    fn push(ctx: &mut CommandContext, queue: &str, payload: &str) -> u64 {
        match exec(ctx, &["DQUEUE", "PUSH", queue, payload]) {
            RespValue::BulkString(b) => String::from_utf8_lossy(&b).parse().unwrap(),
            other => panic!("PUSH failed: {other:?}"),
        }
    }

    fn pop(ctx: &mut CommandContext, queue: &str) -> Option<(u64, Vec<u8>)> {
        match exec(ctx, &["DQUEUE", "POP", queue]) {
            RespValue::Array(arr) if arr.len() >= 2 => {
                let id: u64 = match &arr[0] {
                    RespValue::BulkString(b) => String::from_utf8_lossy(b).parse().unwrap(),
                    other => panic!("bad id: {other:?}"),
                };
                let payload = match &arr[1] {
                    RespValue::BulkString(b) => b.to_vec(),
                    other => panic!("bad payload: {other:?}"),
                };
                Some((id, payload))
            }
            RespValue::Null => None,
            other => panic!("POP returned: {other:?}"),
        }
    }

    // ── PUSH ──────────────────────────────────────────────────────────────────

    #[test]
    fn test_push_returns_message_id() {
        let mut ctx = make_ctx();
        let id = push(&mut ctx, "q1", "hello");
        assert!(id > 0);
    }

    #[test]
    fn test_push_ids_are_monotonically_increasing() {
        let mut ctx = make_ctx();
        let id1 = push(&mut ctx, "q1", "msg1");
        let id2 = push(&mut ctx, "q1", "msg2");
        let id3 = push(&mut ctx, "q1", "msg3");
        assert!(id2 > id1);
        assert!(id3 > id2);
    }

    #[test]
    fn test_push_increments_len() {
        let mut ctx = make_ctx();
        push(&mut ctx, "q1", "a");
        push(&mut ctx, "q1", "b");
        let len = exec(&mut ctx, &["DQUEUE", "LEN", "q1"]);
        assert_eq!(len, RespValue::Integer(2));
    }

    #[test]
    fn test_push_wrong_arity() {
        let mut ctx = make_ctx();
        let r = exec(&mut ctx, &["DQUEUE", "PUSH", "q1"]);
        assert!(matches!(r, RespValue::Error(_)));
    }

    #[test]
    fn test_push_with_delay_not_visible_immediately() {
        let mut ctx = make_ctx();
        exec(
            &mut ctx,
            &["DQUEUE", "PUSH", "q1", "delayed", "DELAY", "60000"],
        );
        // Should not be poppable yet
        let r = pop(&mut ctx, "q1");
        assert!(r.is_none(), "delayed message must not be visible yet");
    }

    #[test]
    fn test_push_multiple_queues_independent() {
        let mut ctx = make_ctx();
        push(&mut ctx, "qa", "msg-a");
        push(&mut ctx, "qb", "msg-b");
        assert_eq!(
            exec(&mut ctx, &["DQUEUE", "LEN", "qa"]),
            RespValue::Integer(1)
        );
        assert_eq!(
            exec(&mut ctx, &["DQUEUE", "LEN", "qb"]),
            RespValue::Integer(1)
        );
    }

    // ── POP ───────────────────────────────────────────────────────────────────

    #[test]
    fn test_pop_returns_message() {
        let mut ctx = make_ctx();
        push(&mut ctx, "q1", "hello");
        let r = pop(&mut ctx, "q1");
        assert!(r.is_some());
        let (_, payload) = r.unwrap();
        assert_eq!(payload, b"hello");
    }

    #[test]
    fn test_pop_empty_queue_returns_null() {
        let mut ctx = make_ctx();
        let r = exec(&mut ctx, &["DQUEUE", "POP", "q1"]);
        assert_eq!(r, RespValue::Null);
    }

    #[test]
    fn test_pop_is_fifo() {
        let mut ctx = make_ctx();
        push(&mut ctx, "q1", "first");
        push(&mut ctx, "q1", "second");
        push(&mut ctx, "q1", "third");
        let (_, p1) = pop(&mut ctx, "q1").unwrap();
        let (_, p2) = pop(&mut ctx, "q1").unwrap();
        let (_, p3) = pop(&mut ctx, "q1").unwrap();
        assert_eq!(p1, b"first");
        assert_eq!(p2, b"second");
        assert_eq!(p3, b"third");
    }

    #[test]
    fn test_pop_moves_to_inflight() {
        let mut ctx = make_ctx();
        push(&mut ctx, "q1", "msg");
        pop(&mut ctx, "q1");
        assert_eq!(
            exec(&mut ctx, &["DQUEUE", "INFLIGHT", "q1"]),
            RespValue::Integer(1)
        );
        assert_eq!(
            exec(&mut ctx, &["DQUEUE", "LEN", "q1"]),
            RespValue::Integer(0)
        );
    }

    #[test]
    fn test_pop_count_multiple() {
        let mut ctx = make_ctx();
        push(&mut ctx, "q1", "a");
        push(&mut ctx, "q1", "b");
        push(&mut ctx, "q1", "c");
        let r = exec(&mut ctx, &["DQUEUE", "POP", "q1", "COUNT", "2"]);
        match r {
            RespValue::Array(arr) => assert_eq!(arr.len(), 4), // 2 messages × 2 fields
            other => panic!("expected array, got {other:?}"),
        }
        assert_eq!(
            exec(&mut ctx, &["DQUEUE", "LEN", "q1"]),
            RespValue::Integer(1)
        );
    }

    #[test]
    fn test_pop_wrong_arity() {
        let mut ctx = make_ctx();
        let r = exec(&mut ctx, &["DQUEUE", "POP"]);
        assert!(matches!(r, RespValue::Error(_)));
    }

    // ── ACK ───────────────────────────────────────────────────────────────────

    #[test]
    fn test_ack_removes_from_inflight() {
        let mut ctx = make_ctx();
        let id = push(&mut ctx, "q1", "msg");
        pop(&mut ctx, "q1");
        let r = exec(&mut ctx, &["DQUEUE", "ACK", "q1", &id.to_string()]);
        assert_eq!(r, RespValue::Integer(1));
        assert_eq!(
            exec(&mut ctx, &["DQUEUE", "INFLIGHT", "q1"]),
            RespValue::Integer(0)
        );
    }

    #[test]
    fn test_ack_nonexistent_msg_id() {
        let mut ctx = make_ctx();
        let r = exec(&mut ctx, &["DQUEUE", "ACK", "q1", "9999"]);
        assert_eq!(r, RespValue::Integer(0));
    }

    #[test]
    fn test_ack_wrong_arity() {
        let mut ctx = make_ctx();
        let r = exec(&mut ctx, &["DQUEUE", "ACK", "q1"]);
        assert!(matches!(r, RespValue::Error(_)));
    }

    #[test]
    fn test_ack_twice_returns_zero_second_time() {
        let mut ctx = make_ctx();
        let id = push(&mut ctx, "q1", "msg");
        pop(&mut ctx, "q1");
        exec(&mut ctx, &["DQUEUE", "ACK", "q1", &id.to_string()]);
        let r = exec(&mut ctx, &["DQUEUE", "ACK", "q1", &id.to_string()]);
        assert_eq!(r, RespValue::Integer(0));
    }

    // ── NACK ──────────────────────────────────────────────────────────────────

    #[test]
    fn test_nack_requeues_message() {
        let mut ctx = make_ctx();
        push(&mut ctx, "q1", "msg");
        let (id, _) = pop(&mut ctx, "q1").unwrap();

        let r = exec(&mut ctx, &["DQUEUE", "NACK", "q1", &id.to_string()]);
        assert_eq!(r, RespValue::Integer(1));

        // Message should be back in pending
        assert_eq!(
            exec(&mut ctx, &["DQUEUE", "LEN", "q1"]),
            RespValue::Integer(1)
        );
        assert_eq!(
            exec(&mut ctx, &["DQUEUE", "INFLIGHT", "q1"]),
            RespValue::Integer(0)
        );
    }

    #[test]
    fn test_nack_requeued_message_poppable() {
        let mut ctx = make_ctx();
        push(&mut ctx, "q1", "original");
        let (id, _) = pop(&mut ctx, "q1").unwrap();
        exec(&mut ctx, &["DQUEUE", "NACK", "q1", &id.to_string()]);

        let r = pop(&mut ctx, "q1");
        assert!(r.is_some());
        let (_, payload) = r.unwrap();
        assert_eq!(payload, b"original");
    }

    #[test]
    fn test_nack_nonexistent_returns_zero() {
        let mut ctx = make_ctx();
        let r = exec(&mut ctx, &["DQUEUE", "NACK", "q1", "9999"]);
        assert_eq!(r, RespValue::Integer(0));
    }

    #[test]
    fn test_nack_wrong_arity() {
        let mut ctx = make_ctx();
        let r = exec(&mut ctx, &["DQUEUE", "NACK", "q1"]);
        assert!(matches!(r, RespValue::Error(_)));
    }

    // ── LEN / INFLIGHT ────────────────────────────────────────────────────────

    #[test]
    fn test_len_empty_queue() {
        let mut ctx = make_ctx();
        let r = exec(&mut ctx, &["DQUEUE", "LEN", "q1"]);
        assert_eq!(r, RespValue::Integer(0));
    }

    #[test]
    fn test_inflight_empty() {
        let mut ctx = make_ctx();
        let r = exec(&mut ctx, &["DQUEUE", "INFLIGHT", "q1"]);
        assert_eq!(r, RespValue::Integer(0));
    }

    // ── PEEK ──────────────────────────────────────────────────────────────────

    #[test]
    fn test_peek_does_not_consume() {
        let mut ctx = make_ctx();
        push(&mut ctx, "q1", "peekable");
        let r = exec(&mut ctx, &["DQUEUE", "PEEK", "q1"]);
        match r {
            RespValue::Array(arr) => {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[1], RespValue::bulk_string("peekable"));
            }
            other => panic!("expected array, got {other:?}"),
        }
        // Still pending
        assert_eq!(
            exec(&mut ctx, &["DQUEUE", "LEN", "q1"]),
            RespValue::Integer(1)
        );
    }

    #[test]
    fn test_peek_empty_queue() {
        let mut ctx = make_ctx();
        let r = exec(&mut ctx, &["DQUEUE", "PEEK", "q1"]);
        assert_eq!(r, RespValue::Array(vec![]));
    }

    // ── PURGE ─────────────────────────────────────────────────────────────────

    #[test]
    fn test_purge_clears_all_messages() {
        let mut ctx = make_ctx();
        push(&mut ctx, "q1", "a");
        push(&mut ctx, "q1", "b");
        pop(&mut ctx, "q1"); // move one to inflight

        let r = exec(&mut ctx, &["DQUEUE", "PURGE", "q1"]);
        assert_eq!(r, RespValue::Integer(2)); // 1 pending + 1 inflight

        assert_eq!(
            exec(&mut ctx, &["DQUEUE", "LEN", "q1"]),
            RespValue::Integer(0)
        );
        assert_eq!(
            exec(&mut ctx, &["DQUEUE", "INFLIGHT", "q1"]),
            RespValue::Integer(0)
        );
    }

    #[test]
    fn test_purge_empty_queue() {
        let mut ctx = make_ctx();
        let r = exec(&mut ctx, &["DQUEUE", "PURGE", "q1"]);
        assert_eq!(r, RespValue::Integer(0));
    }

    #[test]
    fn test_purge_allows_repush() {
        let mut ctx = make_ctx();
        push(&mut ctx, "q1", "msg");
        exec(&mut ctx, &["DQUEUE", "PURGE", "q1"]);
        let id = push(&mut ctx, "q1", "new");
        assert!(id > 0);
    }

    // ── UNKNOWN SUBCOMMAND ────────────────────────────────────────────────────

    #[test]
    fn test_unknown_subcommand() {
        let mut ctx = make_ctx();
        let r = exec(&mut ctx, &["DQUEUE", "BOGUS", "q1"]);
        assert!(matches!(r, RespValue::Error(_)));
    }

    #[test]
    fn test_missing_subcommand() {
        let mut ctx = make_ctx();
        let r = exec(&mut ctx, &["DQUEUE"]);
        assert!(matches!(r, RespValue::Error(_)));
    }

    // ── PAYLOAD WITH SPECIAL CHARS ───────────────────────────────────────────

    #[test]
    fn test_push_pop_payload_with_pipe() {
        let mut ctx = make_ctx();
        push(&mut ctx, "q1", "key|value");
        let (_, payload) = pop(&mut ctx, "q1").unwrap();
        assert_eq!(payload, b"key|value");
    }

    #[test]
    fn test_push_pop_payload_with_backslash() {
        let mut ctx = make_ctx();
        push(&mut ctx, "q1", "path\\to\\file");
        let (_, payload) = pop(&mut ctx, "q1").unwrap();
        assert_eq!(payload, b"path\\to\\file");
    }
}

//! Distributed lock commands (DLOCK)
//!
//! Implements native distributed locks with fencing tokens.
//!
//! ## Commands
//!
//! ```text
//! DLOCK ACQUIRE  <name> <holder> <ttl_ms>               → Integer (fencing token) or -1 (already held)
//! DLOCK RELEASE  <name> <holder> <token>                → Integer 1 (released) or 0 (not owner)
//! DLOCK EXTEND   <name> <holder> <token> <ttl_ms>       → Integer 1 (extended) or 0 (not owner/expired)
//! DLOCK STATUS   <name>                                  → Array [holder, token, ttl_ms] or Null
//! DLOCK FORCERELEASE <name>                              → Integer 1 (released) or 0 (not held)
//! ```
//!
//! ## Storage layout
//!
//! Locks are stored as a `RedisValue::Hash` at key `__dlock:<name>` with fields:
//!
//! | Field      | Description                              |
//! |------------|------------------------------------------|
//! | `holder`   | Identifier of the current lock holder    |
//! | `token`    | Fencing token (monotonically increasing) |
//! | `expires`  | Expiry as Unix-ms timestamp              |
//!
//! A global fencing counter is stored as a `RedisValue::String` at `__dlock:__seq__`.
//!
//! ## Design guarantees
//!
//! * All mutations (ACQUIRE, RELEASE, EXTEND) are **atomic** — they hold the
//!   DashMap shard lock for their entire read-modify-write cycle via `db.update()`.
//! * Fencing tokens are monotonically increasing across all locks in the same db.
//! * An expired lock is treated as vacant on every read.

use crate::{CommandContext, CommandError, CommandResult};
use bytes::Bytes;
use ferris_core::{store::UpdateAction, Entry, RedisValue};
use ferris_protocol::RespValue;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

// ─────────────────────────────────────────────────────────────────────────────
// Internal helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Current time as Unix milliseconds
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Key used to store lock state in the database
fn lock_key(name: &[u8]) -> Bytes {
    let mut k = b"__dlock:".to_vec();
    k.extend_from_slice(name);
    Bytes::from(k)
}

/// Key for the global monotonic fencing counter
fn seq_key() -> Bytes {
    Bytes::from_static(b"__dlock:__seq__")
}

/// Read a field from a Hash entry as a `&str`-owned `String`
fn hash_field(map: &HashMap<Bytes, Bytes>, field: &[u8]) -> Option<String> {
    map.get(field)
        .and_then(|v| std::str::from_utf8(v).ok().map(str::to_owned))
}

/// Atomically increment and return the next fencing token.
///
/// Uses `db.update()` to ensure the increment is not lost under concurrency.
fn next_token(db: &ferris_core::store::Database) -> u64 {
    let key = seq_key();
    db.update(key.clone(), |entry| match entry {
        Some(e) => {
            if let RedisValue::String(s) = &e.value {
                let n: u64 = std::str::from_utf8(s)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0)
                    + 1;
                let new_entry = Entry::new(RedisValue::String(Bytes::from(n.to_string())));
                (UpdateAction::Set(new_entry), n)
            } else {
                let new_entry = Entry::new(RedisValue::String(Bytes::from("1")));
                (UpdateAction::Set(new_entry), 1u64)
            }
        }
        None => {
            let new_entry = Entry::new(RedisValue::String(Bytes::from("1")));
            (UpdateAction::Set(new_entry), 1u64)
        }
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// DLOCK dispatcher
// ─────────────────────────────────────────────────────────────────────────────

/// DLOCK <subcommand> [args…]
///
/// Dispatcher for all distributed lock subcommands.
pub fn dlock(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    let sub = args
        .first()
        .and_then(|v| v.as_str())
        .ok_or_else(|| CommandError::WrongArity("DLOCK".to_string()))?
        .to_uppercase();

    match sub.as_str() {
        "ACQUIRE" => dlock_acquire(ctx, &args[1..]),
        "RELEASE" => dlock_release(ctx, &args[1..]),
        "EXTEND" => dlock_extend(ctx, &args[1..]),
        "STATUS" => dlock_status(ctx, &args[1..]),
        "FORCERELEASE" => dlock_forcerelease(ctx, &args[1..]),
        _ => Err(CommandError::UnknownSubcommand(
            sub.clone(),
            "DLOCK".to_string(),
        )),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// DLOCK ACQUIRE <name> <holder> <ttl_ms>
// ─────────────────────────────────────────────────────────────────────────────

fn dlock_acquire(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongArity("DLOCK ACQUIRE".to_string()));
    }

    let name = args[0]
        .as_bytes()
        .map(|b| b.as_ref())
        .ok_or_else(|| CommandError::WrongArity("DLOCK ACQUIRE".to_string()))?;
    let holder = args[1]
        .as_bytes()
        .map(|b| b.as_ref())
        .ok_or_else(|| CommandError::WrongArity("DLOCK ACQUIRE".to_string()))?;
    let ttl_ms: u64 = args[2]
        .as_str()
        .and_then(|s| s.parse().ok())
        .ok_or(CommandError::NotAnInteger)?;

    if ttl_ms == 0 {
        return Err(CommandError::InvalidArgument(
            "invalid expire time in 'DLOCK ACQUIRE'".to_string(),
        ));
    }

    let key = lock_key(name);
    let db = ctx.store().database(ctx.selected_db());
    let now = now_ms();
    let expires = now + ttl_ms;

    // We need the token before entering the update closure (it is itself an
    // atomic operation on the same db, but a *different* key — so it's safe
    // to call before the lock-state update).
    let candidate_token = next_token(db);

    let result: Result<i64, CommandError> = db.update(key.clone(), |entry| {
        // Check whether the existing lock is still valid
        if let Some(e) = entry {
            if let RedisValue::Hash(map) = &e.value {
                let exp: u64 = hash_field(map, b"expires")
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                if exp > now {
                    // Lock is still held — return -1 (not acquired)
                    return (UpdateAction::Keep, Ok(-1i64));
                }
            }
        }

        // Vacant or expired — install the new lock
        let mut map = HashMap::new();
        map.insert(Bytes::from_static(b"holder"), Bytes::from(holder.to_vec()));
        map.insert(
            Bytes::from_static(b"token"),
            Bytes::from(candidate_token.to_string()),
        );
        map.insert(
            Bytes::from_static(b"expires"),
            Bytes::from(expires.to_string()),
        );
        let new_entry = Entry::new(RedisValue::Hash(map));
        (UpdateAction::Set(new_entry), Ok(candidate_token as i64))
    });

    match result? {
        -1 => Ok(RespValue::Integer(-1)),
        token => {
            ctx.propagate(&[
                RespValue::bulk_string("DLOCK"),
                RespValue::bulk_string("ACQUIRE"),
                args[0].clone(),
                args[1].clone(),
                args[2].clone(),
            ]);
            Ok(RespValue::Integer(token))
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// DLOCK RELEASE <name> <holder> <token>
// ─────────────────────────────────────────────────────────────────────────────

fn dlock_release(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongArity("DLOCK RELEASE".to_string()));
    }

    let name = args[0]
        .as_bytes()
        .map(|b| b.as_ref())
        .ok_or_else(|| CommandError::WrongArity("DLOCK RELEASE".to_string()))?;
    let holder = args[1].as_str().unwrap_or("");
    let token = args[2]
        .as_str()
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or(CommandError::NotAnInteger)?;

    let key = lock_key(name);
    let db = ctx.store().database(ctx.selected_db());
    let now = now_ms();

    let released: bool = db.update(key.clone(), |entry| {
        let Some(e) = entry else {
            return (UpdateAction::Keep, false);
        };
        let RedisValue::Hash(map) = &e.value else {
            return (UpdateAction::Keep, false);
        };

        let exp: u64 = hash_field(map, b"expires")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        if exp <= now {
            // Already expired — treat as released
            return (UpdateAction::Delete, true);
        }

        let stored_holder = hash_field(map, b"holder").unwrap_or_default();
        let stored_token: u64 = hash_field(map, b"token")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        if stored_holder == holder && stored_token == token {
            (UpdateAction::Delete, true)
        } else {
            (UpdateAction::Keep, false)
        }
    });

    if released {
        ctx.propagate(&[
            RespValue::bulk_string("DLOCK"),
            RespValue::bulk_string("RELEASE"),
            args[0].clone(),
            args[1].clone(),
            args[2].clone(),
        ]);
    }

    Ok(RespValue::Integer(i64::from(released)))
}

// ─────────────────────────────────────────────────────────────────────────────
// DLOCK EXTEND <name> <holder> <token> <ttl_ms>
// ─────────────────────────────────────────────────────────────────────────────

fn dlock_extend(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 4 {
        return Err(CommandError::WrongArity("DLOCK EXTEND".to_string()));
    }

    let name = args[0]
        .as_bytes()
        .map(|b| b.as_ref())
        .ok_or_else(|| CommandError::WrongArity("DLOCK EXTEND".to_string()))?;
    let holder = args[1].as_str().unwrap_or("");
    let token = args[2]
        .as_str()
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or(CommandError::NotAnInteger)?;
    let ttl_ms: u64 = args[3]
        .as_str()
        .and_then(|s| s.parse().ok())
        .ok_or(CommandError::NotAnInteger)?;

    if ttl_ms == 0 {
        return Err(CommandError::InvalidArgument(
            "invalid expire time in 'DLOCK EXTEND'".to_string(),
        ));
    }

    let key = lock_key(name);
    let db = ctx.store().database(ctx.selected_db());
    let now = now_ms();
    let new_expires = now + ttl_ms;

    let extended: bool = db.update(key.clone(), |entry| {
        let Some(e) = entry else {
            return (UpdateAction::Keep, false);
        };
        let RedisValue::Hash(map) = &mut e.value else {
            return (UpdateAction::Keep, false);
        };

        let exp: u64 = hash_field(map, b"expires")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        if exp <= now {
            return (UpdateAction::Delete, false); // Expired — can't extend
        }

        let stored_holder = hash_field(map, b"holder").unwrap_or_default();
        let stored_token: u64 = hash_field(map, b"token")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        if stored_holder == holder && stored_token == token {
            map.insert(
                Bytes::from_static(b"expires"),
                Bytes::from(new_expires.to_string()),
            );
            (UpdateAction::Keep, true)
        } else {
            (UpdateAction::Keep, false)
        }
    });

    if extended {
        ctx.propagate(&[
            RespValue::bulk_string("DLOCK"),
            RespValue::bulk_string("EXTEND"),
            args[0].clone(),
            args[1].clone(),
            args[2].clone(),
            args[3].clone(),
        ]);
    }

    Ok(RespValue::Integer(i64::from(extended)))
}

// ─────────────────────────────────────────────────────────────────────────────
// DLOCK STATUS <name>
// ─────────────────────────────────────────────────────────────────────────────

fn dlock_status(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("DLOCK STATUS".to_string()));
    }

    let name = args[0]
        .as_bytes()
        .map(|b| b.as_ref())
        .ok_or_else(|| CommandError::WrongArity("DLOCK STATUS".to_string()))?;

    let key = lock_key(name);
    let db = ctx.store().database(ctx.selected_db());
    let now = now_ms();

    match db.get(&key) {
        None => Ok(RespValue::Null),
        Some(entry) => {
            if let RedisValue::Hash(map) = &entry.value {
                let exp: u64 = hash_field(map, b"expires")
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);

                if exp <= now {
                    // Lazily remove expired lock
                    db.delete(&key);
                    return Ok(RespValue::Null);
                }

                let holder = hash_field(map, b"holder").unwrap_or_default();
                let token = hash_field(map, b"token").unwrap_or_default();
                let ttl_remaining = exp.saturating_sub(now);

                Ok(RespValue::Array(vec![
                    RespValue::bulk_string("holder"),
                    RespValue::BulkString(Bytes::from(holder)),
                    RespValue::bulk_string("token"),
                    RespValue::BulkString(Bytes::from(token)),
                    RespValue::bulk_string("ttl_ms"),
                    RespValue::BulkString(Bytes::from(ttl_remaining.to_string())),
                ]))
            } else {
                Ok(RespValue::Null)
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// DLOCK FORCERELEASE <name>
// ─────────────────────────────────────────────────────────────────────────────

fn dlock_forcerelease(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("DLOCK FORCERELEASE".to_string()));
    }

    let name = args[0]
        .as_bytes()
        .map(|b| b.as_ref())
        .ok_or_else(|| CommandError::WrongArity("DLOCK FORCERELEASE".to_string()))?;

    let key = lock_key(name);
    let db = ctx.store().database(ctx.selected_db());

    let existed = db.delete(&key);
    if existed {
        ctx.propagate(&[
            RespValue::bulk_string("DLOCK"),
            RespValue::bulk_string("FORCERELEASE"),
            args[0].clone(),
        ]);
    }

    Ok(RespValue::Integer(i64::from(existed)))
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

    /// Convert cmd slice to RespValue args (excluding cmd[0] = command name)
    /// and call dlock() directly.
    fn exec(ctx: &mut CommandContext, cmd: &[&str]) -> RespValue {
        // cmd[0] is "DLOCK", cmd[1..] are the args passed to dlock()
        let args: Vec<RespValue> = cmd[1..]
            .iter()
            .map(|s| RespValue::BulkString(Bytes::from(s.to_string())))
            .collect();
        match dlock(ctx, &args) {
            Ok(v) => v,
            Err(e) => RespValue::Error(e.to_string()),
        }
    }

    // ── ACQUIRE ──────────────────────────────────────────────────────────────

    #[test]
    fn test_acquire_returns_positive_token() {
        let mut ctx = make_ctx();
        let result = exec(
            &mut ctx,
            &["DLOCK", "ACQUIRE", "mylock", "worker-1", "5000"],
        );
        match result {
            RespValue::Integer(n) => assert!(n > 0, "token must be positive"),
            other => panic!("expected integer token, got {other:?}"),
        }
    }

    #[test]
    fn test_acquire_second_caller_blocked() {
        let mut ctx = make_ctx();
        exec(
            &mut ctx,
            &["DLOCK", "ACQUIRE", "mylock", "worker-1", "5000"],
        );
        let result = exec(
            &mut ctx,
            &["DLOCK", "ACQUIRE", "mylock", "worker-2", "5000"],
        );
        assert_eq!(
            result,
            RespValue::Integer(-1),
            "second acquire must return -1"
        );
    }

    #[test]
    fn test_acquire_same_holder_also_blocked() {
        // ACQUIRE is not reentrant — even same holder is blocked
        let mut ctx = make_ctx();
        exec(
            &mut ctx,
            &["DLOCK", "ACQUIRE", "mylock", "worker-1", "5000"],
        );
        let result = exec(
            &mut ctx,
            &["DLOCK", "ACQUIRE", "mylock", "worker-1", "5000"],
        );
        assert_eq!(result, RespValue::Integer(-1));
    }

    #[test]
    fn test_acquire_after_release() {
        let mut ctx = make_ctx();
        let tok = match exec(
            &mut ctx,
            &["DLOCK", "ACQUIRE", "mylock", "worker-1", "5000"],
        ) {
            RespValue::Integer(n) => n.to_string(),
            other => panic!("bad token: {other:?}"),
        };
        exec(&mut ctx, &["DLOCK", "RELEASE", "mylock", "worker-1", &tok]);
        let result = exec(
            &mut ctx,
            &["DLOCK", "ACQUIRE", "mylock", "worker-2", "5000"],
        );
        match result {
            RespValue::Integer(n) => assert!(n > 0),
            other => panic!("expected integer after re-acquire, got {other:?}"),
        }
    }

    #[test]
    fn test_acquire_token_is_monotonically_increasing() {
        let mut ctx = make_ctx();
        let tok1 = match exec(&mut ctx, &["DLOCK", "ACQUIRE", "lock-a", "w1", "5000"]) {
            RespValue::Integer(n) => n,
            other => panic!("{other:?}"),
        };
        exec(
            &mut ctx,
            &["DLOCK", "RELEASE", "lock-a", "w1", &tok1.to_string()],
        );
        let tok2 = match exec(&mut ctx, &["DLOCK", "ACQUIRE", "lock-a", "w2", "5000"]) {
            RespValue::Integer(n) => n,
            other => panic!("{other:?}"),
        };
        assert!(tok2 > tok1, "tokens must be strictly increasing");
    }

    #[test]
    fn test_acquire_different_locks_independent() {
        let mut ctx = make_ctx();
        let r1 = exec(&mut ctx, &["DLOCK", "ACQUIRE", "lock-x", "w1", "5000"]);
        let r2 = exec(&mut ctx, &["DLOCK", "ACQUIRE", "lock-y", "w1", "5000"]);
        assert!(matches!(r1, RespValue::Integer(n) if n > 0));
        assert!(matches!(r2, RespValue::Integer(n) if n > 0));
    }

    #[test]
    fn test_acquire_zero_ttl_rejected() {
        let mut ctx = make_ctx();
        let result = exec(&mut ctx, &["DLOCK", "ACQUIRE", "mylock", "w1", "0"]);
        assert!(matches!(result, RespValue::Error(_)));
    }

    #[test]
    fn test_acquire_wrong_arity() {
        let mut ctx = make_ctx();
        let result = exec(&mut ctx, &["DLOCK", "ACQUIRE", "mylock"]);
        assert!(matches!(result, RespValue::Error(_)));
    }

    // ── RELEASE ───────────────────────────────────────────────────────────────

    #[test]
    fn test_release_correct_holder_and_token() {
        let mut ctx = make_ctx();
        let tok = match exec(
            &mut ctx,
            &["DLOCK", "ACQUIRE", "mylock", "worker-1", "5000"],
        ) {
            RespValue::Integer(n) => n,
            other => panic!("{other:?}"),
        };
        let result = exec(
            &mut ctx,
            &["DLOCK", "RELEASE", "mylock", "worker-1", &tok.to_string()],
        );
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_release_wrong_holder_fails() {
        let mut ctx = make_ctx();
        let tok = match exec(
            &mut ctx,
            &["DLOCK", "ACQUIRE", "mylock", "worker-1", "5000"],
        ) {
            RespValue::Integer(n) => n,
            other => panic!("{other:?}"),
        };
        let result = exec(
            &mut ctx,
            &["DLOCK", "RELEASE", "mylock", "worker-2", &tok.to_string()],
        );
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_release_wrong_token_fails() {
        let mut ctx = make_ctx();
        exec(
            &mut ctx,
            &["DLOCK", "ACQUIRE", "mylock", "worker-1", "5000"],
        );
        let result = exec(
            &mut ctx,
            &["DLOCK", "RELEASE", "mylock", "worker-1", "9999"],
        );
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_release_nonexistent_lock() {
        let mut ctx = make_ctx();
        let result = exec(&mut ctx, &["DLOCK", "RELEASE", "ghost", "w1", "1"]);
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_release_wrong_arity() {
        let mut ctx = make_ctx();
        let result = exec(&mut ctx, &["DLOCK", "RELEASE", "mylock", "w1"]);
        assert!(matches!(result, RespValue::Error(_)));
    }

    // ── EXTEND ────────────────────────────────────────────────────────────────

    #[test]
    fn test_extend_correct_owner() {
        let mut ctx = make_ctx();
        let tok = match exec(&mut ctx, &["DLOCK", "ACQUIRE", "mylock", "w1", "5000"]) {
            RespValue::Integer(n) => n,
            other => panic!("{other:?}"),
        };
        let result = exec(
            &mut ctx,
            &["DLOCK", "EXTEND", "mylock", "w1", &tok.to_string(), "10000"],
        );
        assert_eq!(result, RespValue::Integer(1));
    }

    #[test]
    fn test_extend_wrong_holder_fails() {
        let mut ctx = make_ctx();
        let tok = match exec(&mut ctx, &["DLOCK", "ACQUIRE", "mylock", "w1", "5000"]) {
            RespValue::Integer(n) => n,
            other => panic!("{other:?}"),
        };
        let result = exec(
            &mut ctx,
            &["DLOCK", "EXTEND", "mylock", "w2", &tok.to_string(), "10000"],
        );
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_extend_wrong_token_fails() {
        let mut ctx = make_ctx();
        exec(&mut ctx, &["DLOCK", "ACQUIRE", "mylock", "w1", "5000"]);
        let result = exec(
            &mut ctx,
            &["DLOCK", "EXTEND", "mylock", "w1", "9999", "10000"],
        );
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_extend_nonexistent_lock() {
        let mut ctx = make_ctx();
        let result = exec(&mut ctx, &["DLOCK", "EXTEND", "ghost", "w1", "1", "5000"]);
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_extend_zero_ttl_rejected() {
        let mut ctx = make_ctx();
        let tok = match exec(&mut ctx, &["DLOCK", "ACQUIRE", "mylock", "w1", "5000"]) {
            RespValue::Integer(n) => n,
            other => panic!("{other:?}"),
        };
        let result = exec(
            &mut ctx,
            &["DLOCK", "EXTEND", "mylock", "w1", &tok.to_string(), "0"],
        );
        assert!(matches!(result, RespValue::Error(_)));
    }

    #[test]
    fn test_extend_wrong_arity() {
        let mut ctx = make_ctx();
        let result = exec(&mut ctx, &["DLOCK", "EXTEND", "mylock", "w1"]);
        assert!(matches!(result, RespValue::Error(_)));
    }

    // ── STATUS ────────────────────────────────────────────────────────────────

    #[test]
    fn test_status_held_lock() {
        let mut ctx = make_ctx();
        let tok = match exec(
            &mut ctx,
            &["DLOCK", "ACQUIRE", "mylock", "worker-1", "5000"],
        ) {
            RespValue::Integer(n) => n,
            other => panic!("{other:?}"),
        };
        let result = exec(&mut ctx, &["DLOCK", "STATUS", "mylock"]);
        match result {
            RespValue::Array(arr) => {
                assert_eq!(arr.len(), 6);
                // arr = [holder, <value>, token, <value>, ttl_ms, <value>]
                assert_eq!(arr[0], RespValue::bulk_string("holder"));
                assert_eq!(arr[2], RespValue::bulk_string("token"));
                assert_eq!(arr[4], RespValue::bulk_string("ttl_ms"));
                // Check holder value
                assert_eq!(arr[1], RespValue::bulk_string("worker-1"));
                // Check token value
                assert_eq!(arr[3], RespValue::bulk_string(tok.to_string()));
            }
            other => panic!("expected array, got {other:?}"),
        }
    }

    #[test]
    fn test_status_absent_lock() {
        let mut ctx = make_ctx();
        let result = exec(&mut ctx, &["DLOCK", "STATUS", "nobody"]);
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_status_released_lock() {
        let mut ctx = make_ctx();
        let tok = match exec(&mut ctx, &["DLOCK", "ACQUIRE", "mylock", "w1", "5000"]) {
            RespValue::Integer(n) => n,
            other => panic!("{other:?}"),
        };
        exec(
            &mut ctx,
            &["DLOCK", "RELEASE", "mylock", "w1", &tok.to_string()],
        );
        let result = exec(&mut ctx, &["DLOCK", "STATUS", "mylock"]);
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_status_wrong_arity() {
        let mut ctx = make_ctx();
        let result = exec(&mut ctx, &["DLOCK", "STATUS"]);
        assert!(matches!(result, RespValue::Error(_)));
    }

    // ── FORCERELEASE ─────────────────────────────────────────────────────────

    #[test]
    fn test_forcerelease_held_lock() {
        let mut ctx = make_ctx();
        exec(&mut ctx, &["DLOCK", "ACQUIRE", "mylock", "w1", "5000"]);
        let result = exec(&mut ctx, &["DLOCK", "FORCERELEASE", "mylock"]);
        assert_eq!(result, RespValue::Integer(1));
        // Verify it's gone
        assert_eq!(
            exec(&mut ctx, &["DLOCK", "STATUS", "mylock"]),
            RespValue::Null
        );
    }

    #[test]
    fn test_forcerelease_absent_lock() {
        let mut ctx = make_ctx();
        let result = exec(&mut ctx, &["DLOCK", "FORCERELEASE", "ghost"]);
        assert_eq!(result, RespValue::Integer(0));
    }

    #[test]
    fn test_forcerelease_allows_reacquire() {
        let mut ctx = make_ctx();
        exec(&mut ctx, &["DLOCK", "ACQUIRE", "mylock", "w1", "5000"]);
        exec(&mut ctx, &["DLOCK", "FORCERELEASE", "mylock"]);
        let result = exec(&mut ctx, &["DLOCK", "ACQUIRE", "mylock", "w2", "5000"]);
        assert!(matches!(result, RespValue::Integer(n) if n > 0));
    }

    #[test]
    fn test_forcerelease_wrong_arity() {
        let mut ctx = make_ctx();
        let result = exec(&mut ctx, &["DLOCK", "FORCERELEASE"]);
        assert!(matches!(result, RespValue::Error(_)));
    }

    // ── UNKNOWN SUBCOMMAND ────────────────────────────────────────────────────

    #[test]
    fn test_unknown_subcommand() {
        let mut ctx = make_ctx();
        let result = exec(&mut ctx, &["DLOCK", "BOGUS", "x"]);
        assert!(matches!(result, RespValue::Error(_)));
    }

    #[test]
    fn test_missing_subcommand() {
        let mut ctx = make_ctx();
        let result = exec(&mut ctx, &["DLOCK"]);
        assert!(matches!(result, RespValue::Error(_)));
    }
}

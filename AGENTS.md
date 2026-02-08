# ferris-db Development Guidelines

> **Purpose**: This document provides guidelines for AI agents and human contributors working on ferris-db. It ensures consistency across the codebase and documents architectural decisions that must be preserved.

---

## Implementation Checklist (Mandatory)

Every implementation task **MUST** satisfy all of the following:

1. **Tests are mandatory -- 100% code coverage with 10-20 tests per command minimum.**
   - Every command must have at least 10-20 tests covering: happy path (3-5), edge cases (5-10), error cases (3-5), and concurrency (2-3).
   - Tests must be written alongside (or before) the implementation.
   - Coverage must be 100% line coverage target, 95% minimum. No exceptions.
   - Use `cargo tarpaulin` or `cargo llvm-cov` to verify.
   - **20 unique test scenarios per command**: For each command, understand its Redis semantics fully, review the implementation, and combine both understandings to derive 20 unique test scenarios. Each test must exercise a distinct behavior or code path. Duplicate or near-duplicate tests are not acceptable.

2. **Implementation MUST be multi-threaded.**
   - Use `DashMap`-based sharded storage (key-level locking, NOT global locks).
   - Single-key operations lock only the relevant shard.
   - Multi-key operations sort shards by index before locking (prevents deadlocks).
   - Blocking operations use `tokio::sync::Notify` per key.
   - Never use `std::sync::Mutex` or any global lock that blocks unrelated keys.

3. **Follow the design in DESIGN.md strictly.**
   - All data types use the `RedisValue` enum defined in `ferris-core`.
   - All commands follow the handler pattern: `fn(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult`.
   - Commands must be registered in `registry.rs` with correct arity and flags.
   - RESP protocol compatibility must be maintained -- standard Redis clients must work.
   - Cross-platform code only (no platform-specific code without `cfg` fallbacks).
   - No `unsafe` code (`#![deny(unsafe_code)]` in all crates).
   - Use `thiserror` for error types, return `Result<T, E>`, never `panic!()` or `unwrap()`.

---

## Table of Contents

1. [Project Context](#project-context)
2. [Development Philosophy](#development-philosophy)
3. [Crate Responsibilities](#crate-responsibilities)
4. [How to Add a New Command](#how-to-add-a-new-command)
5. [How to Add a New Data Type](#how-to-add-a-new-data-type)
6. [How to Add a New CRDT Type](#how-to-add-a-new-crdt-type)
7. [Architecture Invariants](#architecture-invariants)
8. [Coding Conventions](#coding-conventions)
9. [Testing Requirements](#testing-requirements)
10. [Document Maintenance](#document-maintenance)
11. [Common Pitfalls](#common-pitfalls)

---

## Project Context

ferris-db is a Redis-compatible distributed key-value store in Rust with these key differentiators:

- **Multi-threaded** with key-level synchronization (not global locks)
- **Native distributed locks** with fencing tokens
- **Native distributed queues** with at-least-once delivery
- **CRDT support** for active/active replication
- **Configurable consistency** (async/semi-sync/sync)

**Default port**: 6380 (to avoid conflict with Redis on 6379)

When implementing features, always refer to:
- `DESIGN.md` for architectural decisions
- `ROADMAP.md` for current progress and next tasks
- Redis documentation for command semantics (https://redis.io/commands/)

---

## Development Philosophy

### Test-Driven Development (TDD) is Mandatory

**Every feature must be developed test-first.** This is non-negotiable.

```
1. Write failing tests that define expected behavior
2. Implement the minimum code to make tests pass
3. Refactor while keeping tests green
4. Add more edge case tests
5. Verify coverage >= 95%
```

### Test Requirements Per Feature

| Category | Minimum Tests | Description |
|----------|--------------|-------------|
| Happy path | 3-5 | Basic functionality works |
| Edge cases | 5-10 | Boundary conditions, empty inputs, large inputs |
| Error cases | 3-5 | Invalid input, type errors, missing keys |
| Concurrency | 2-3 | Race conditions, parallel access |
| Integration | 2-3 | Over-the-wire RESP protocol |

**Total: 15-26 tests per command, 10-20 per feature module**

### Coverage Requirements

- **Line coverage**: >= 95% (target 100%)
- **Branch coverage**: >= 90%
- No PR is merged if coverage drops
- Use `cargo tarpaulin` or `cargo llvm-cov` to measure

---

## Crate Responsibilities

### ferris-server
**Purpose**: Binary entry point, CLI, server bootstrap

**Contains**:
- `main.rs` - CLI parsing, config loading, server start
- `config.rs` - Configuration struct and loading logic

**Does NOT contain**: Business logic, command implementations

### ferris-core
**Purpose**: Core data structures and storage engine

**Contains**:
- `KeyStore` - Sharded DashMap for concurrent access
- `RedisValue` - Enum for all Redis data types
- `Entry` - Value + metadata (TTL, version, etc.)
- Expiry management (TTL background task)
- Memory tracking and eviction

**Does NOT contain**: Command parsing, networking, RESP protocol

### ferris-protocol
**Purpose**: RESP2 and RESP3 codec

**Contains**:
- RESP value types
- RESP2 parser and serializer
- RESP3 parser and serializer
- Tokio codec (Decoder/Encoder)
- Command parsing (name + arguments)

**Does NOT contain**: Command execution, business logic

### ferris-commands
**Purpose**: Command implementations

**Contains**:
- Command registry (dispatch table)
- Individual command handlers (one file per category)
- Execution context (connection state)

**Does NOT contain**: Storage implementation, networking

### ferris-network
**Purpose**: TCP/TLS listener and connection handling

**Contains**:
- TCP listener with Tokio
- TLS configuration with rustls
- Connection lifecycle management
- Graceful shutdown

**Does NOT contain**: Command logic, storage

### ferris-persistence
**Purpose**: AOF persistence

**Contains**:
- AOF writer (async buffered)
- AOF reader (replay on startup)
- AOF rewriter (background compaction)
- Platform-specific fsync

**Does NOT contain**: Networking, command logic

### ferris-replication
**Purpose**: Replication and cluster

**Contains**:
- Leader/follower replication
- Replication backlog
- Cluster topology and gossip
- Slot migration
- Failover logic

### ferris-crdt
**Purpose**: Conflict-free replicated data types

**Contains**:
- Hybrid Logical Clock (HLC)
- CRDT implementations (counter, register, set, map)
- Merge protocol

### ferris-queue
**Purpose**: Distributed queue implementation

**Contains**:
- Queue abstraction
- Message model
- Scheduling (delay, priority)
- Dead letter queue

### ferris-lock
**Purpose**: Distributed lock implementation

**Contains**:
- Lock state machine
- Fencing token generation
- Wait queue for blocking acquire

### ferris-test-utils
**Purpose**: Shared test utilities

**Contains**:
- `TestServer` - Spawn server on random port
- `TestClient` - Simple RESP client for tests
- PropTest generators
- Redis compatibility helpers

---

## How to Add a New Command

Follow these steps exactly:

### Step 1: Write Tests First

Create tests in `ferris-commands/tests/` or the integration test directory:

```rust
// tests/integration/commands/string_test.rs

#[tokio::test]
async fn test_getset_returns_old_value() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Set initial value
    client.cmd_ok(&["SET", "mykey", "Hello"]).await;

    // GETSET should return old value
    let result = client.cmd(&["GETSET", "mykey", "World"]).await;
    assert_eq!(result, RespValue::bulk_string("Hello"));

    // New value should be set
    let result = client.cmd(&["GET", "mykey"]).await;
    assert_eq!(result, RespValue::bulk_string("World"));

    server.stop().await;
}

#[tokio::test]
async fn test_getset_on_nonexistent_key_returns_nil() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["GETSET", "newkey", "value"]).await;
    assert_eq!(result, RespValue::Null);

    let result = client.cmd(&["GET", "newkey"]).await;
    assert_eq!(result, RespValue::bulk_string("value"));

    server.stop().await;
}

#[tokio::test]
async fn test_getset_on_wrong_type_returns_error() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Create a list
    client.cmd_ok(&["LPUSH", "mylist", "item"]).await;

    // GETSET on list should error
    let result = client.cmd(&["GETSET", "mylist", "value"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    server.stop().await;
}

// Add 10+ more tests for edge cases...
```

### Step 2: Add Command Handler

```rust
// ferris-commands/src/string.rs

use crate::{CommandContext, CommandResult, RespValue};
use ferris_core::{KeyStore, RedisValue, Entry};

/// GETSET key value
/// 
/// Atomically sets key to value and returns the old value stored at key.
/// Returns nil if key did not exist.
/// 
/// Time complexity: O(1)
pub fn getset(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    // Validate arguments
    if args.len() != 2 {
        return Err(CommandError::WrongArity("GETSET"));
    }

    let key = args[0].as_bytes()?;
    let value = args[1].as_bytes()?;

    // Get the store
    let store = ctx.store();
    let db = ctx.selected_db();

    // Atomically get old and set new
    let old_value = store.get_and_set(db, key.clone(), |entry| {
        // Type check
        if let Some(e) = entry {
            match &e.value {
                RedisValue::String(old) => {
                    let result = old.clone();
                    e.value = RedisValue::String(value.clone());
                    e.increment_version();
                    Ok(Some(result))
                }
                _ => Err(CommandError::WrongType),
            }
        } else {
            // Key doesn't exist, create it
            Ok(None) // Return None, but set the value
        }
    })?;

    // Queue for AOF/replication
    ctx.propagate(&["GETSET", &key, &value]);

    Ok(match old_value {
        Some(v) => RespValue::BulkString(v),
        None => RespValue::Null,
    })
}
```

### Step 3: Register Command

```rust
// ferris-commands/src/registry.rs

pub fn register_commands(registry: &mut CommandRegistry) {
    // ... existing commands ...
    
    registry.register("GETSET", CommandSpec {
        handler: string::getset,
        arity: 3,  // command name + 2 args
        flags: CommandFlags::WRITE | CommandFlags::DENYOOM,
        first_key: 1,
        last_key: 1,
        step: 1,
    });
}
```

### Step 4: Verify Tests Pass

```bash
cargo test --package ferris-commands -- getset
cargo test --test integration -- getset
```

### Step 5: Check Coverage

```bash
cargo tarpaulin --package ferris-commands --out Html
# Open tarpaulin-report.html and verify >= 95% coverage
```

### Step 6: Update ROADMAP.md

Check off the command in ROADMAP.md:
```markdown
- [x] GETSET
```

---

## How to Add a New Data Type

### Step 1: Define the Type in ferris-core

```rust
// ferris-core/src/value/redis_value.rs

#[derive(Clone, Debug)]
pub enum RedisValue {
    String(Bytes),
    List(VecDeque<Bytes>),
    Set(HashSet<Bytes>),
    Hash(HashMap<Bytes, Bytes>),
    SortedSet { ... },
    // NEW TYPE
    Stream(StreamData),  // Add new variant
}

// ferris-core/src/value/stream.rs
#[derive(Clone, Debug)]
pub struct StreamData {
    pub entries: BTreeMap<StreamId, HashMap<Bytes, Bytes>>,
    pub last_id: StreamId,
    // ... stream-specific fields
}
```

### Step 2: Add Type-Specific Operations

```rust
// ferris-core/src/value/stream.rs

impl StreamData {
    pub fn new() -> Self { ... }
    pub fn add(&mut self, id: StreamId, fields: HashMap<Bytes, Bytes>) -> StreamId { ... }
    pub fn range(&self, start: StreamId, end: StreamId) -> Vec<...> { ... }
    // ... all operations needed by commands
}
```

### Step 3: Update TYPE Command

```rust
// ferris-commands/src/key.rs

pub fn type_cmd(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    // ... 
    let type_name = match &entry.value {
        RedisValue::String(_) => "string",
        RedisValue::List(_) => "list",
        RedisValue::Set(_) => "set",
        RedisValue::Hash(_) => "hash",
        RedisValue::SortedSet { .. } => "zset",
        RedisValue::Stream(_) => "stream",  // Add new type
    };
    // ...
}
```

### Step 4: Implement Commands

Create `ferris-commands/src/stream.rs` with all XADD, XREAD, etc.

### Step 5: Write Tests (before implementation!)

Minimum 10-20 tests per new type covering:
- Creation and basic operations
- Edge cases (empty, large)
- Type errors (wrong type access)
- Memory usage
- Serialization (for AOF)

---

## How to Add a New CRDT Type

### Step 1: Define CRDT in ferris-crdt

```rust
// ferris-crdt/src/my_crdt.rs

use crate::hlc::HLC;

#[derive(Clone, Debug)]
pub struct MyCRDT {
    // State
    value: ...,
    // Metadata
    hlc: HLC,
    node_id: u32,
}

impl MyCRDT {
    /// Create new instance
    pub fn new(node_id: u32) -> Self { ... }

    /// Apply local mutation
    pub fn mutate(&mut self, ...) -> Delta { ... }

    /// Merge remote delta
    pub fn merge(&mut self, delta: Delta) { ... }

    /// Get current value
    pub fn value(&self) -> ... { ... }
}
```

### Step 2: Define Delta Type

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Delta {
    pub hlc: HLC,
    pub node_id: u32,
    pub operation: Operation,
}
```

### Step 3: Write Convergence Tests

**These are critical for CRDTs:**

```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn test_commutativity(ops1 in vec_of_ops(), ops2 in vec_of_ops()) {
        // Apply ops1 then ops2
        let mut crdt1 = MyCRDT::new(1);
        for op in &ops1 { crdt1.apply(op); }
        for op in &ops2 { crdt1.apply(op); }

        // Apply ops2 then ops1
        let mut crdt2 = MyCRDT::new(1);
        for op in &ops2 { crdt2.apply(op); }
        for op in &ops1 { crdt2.apply(op); }

        // Must converge to same value
        assert_eq!(crdt1.value(), crdt2.value());
    }

    #[test]
    fn test_idempotency(ops in vec_of_ops()) {
        let mut crdt1 = MyCRDT::new(1);
        for op in &ops { crdt1.apply(op); }
        let value1 = crdt1.value();

        // Apply same ops again
        for op in &ops { crdt1.apply(op); }

        // Value must not change
        assert_eq!(crdt1.value(), value1);
    }
}
```

### Step 4: Integrate with RedisValue

Add CRDT metadata to Entry when in active/active mode.

---

## Architecture Invariants

These rules **MUST NOT** be violated:

### 1. Key-Level Locking Only

```
CORRECT: Lock only the shard(s) containing accessed keys
WRONG:   Any global lock that blocks unrelated keys
```

### 2. Multi-Key Locking Order

```
CORRECT: Sort shards by index, lock in order (prevents deadlocks)
WRONG:   Lock shards in arbitrary order
```

### 3. Async AOF Channel

```
CORRECT: Commands sent to channel, writer task flushes async
WRONG:   Synchronous disk writes in command execution path
```

### 4. RESP Protocol Compatibility

```
CORRECT: Standard Redis clients can connect and work
WRONG:   Breaking RESP protocol or changing command semantics
```

### 5. Cross-Platform Code

```
CORRECT: Use std/tokio abstractions, cfg attributes for platform-specific code
WRONG:   Unix-only syscalls without Windows/macOS alternatives
```

### 6. No Unsafe Code

```
CORRECT: #![deny(unsafe_code)] in all crates
WRONG:   Using unsafe without extremely strong justification
```

### 7. Error Handling

```
CORRECT: Return Result<T, E>, use thiserror for error types
WRONG:   panic!(), unwrap() on fallible operations, expect() without justification
```

---

## Coding Conventions

### Naming

```rust
// Types: PascalCase
struct KeyStore { }
enum RedisValue { }

// Functions/methods: snake_case
fn get_and_set() { }

// Constants: SCREAMING_SNAKE_CASE
const DEFAULT_PORT: u16 = 6380;

// Modules: snake_case
mod sorted_set;
```

### Error Handling

```rust
// Use thiserror for error types
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CommandError {
    #[error("wrong number of arguments for '{0}' command")]
    WrongArity(&'static str),

    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,

    #[error("value is not an integer or out of range")]
    NotAnInteger,
}

// Return Result, never panic
pub fn my_command(...) -> Result<RespValue, CommandError> {
    let value = args.get(0).ok_or(CommandError::WrongArity("MYCOMMAND"))?;
    // ...
}
```

### Logging

```rust
use tracing::{debug, info, warn, error, instrument};

#[instrument(skip(store))]
pub fn execute_command(cmd: &Command, store: &KeyStore) -> Result<...> {
    debug!(command = %cmd.name, "Executing command");
    // ...
    if let Err(e) = result {
        warn!(error = %e, "Command failed");
    }
}
```

### Documentation

```rust
/// Sets `key` to hold the string `value`.
///
/// If `key` already holds a value, it is overwritten, regardless of its type.
/// Any previous time to live associated with the key is discarded.
///
/// # Arguments
///
/// * `ctx` - Command execution context
/// * `args` - Command arguments: `key value [EX seconds] [PX milliseconds] [NX|XX]`
///
/// # Returns
///
/// * `OK` - if SET was executed correctly
/// * `Null` - if SET was not performed because of NX/XX condition
///
/// # Errors
///
/// * `WrongArity` - if wrong number of arguments
/// * `SyntaxError` - if invalid options
///
/// # Examples
///
/// ```
/// SET mykey "Hello"
/// SET mykey "World" XX  // Only if exists
/// SET mykey "New" NX    // Only if not exists
/// ```
pub fn set(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    // ...
}
```

---

## Testing Requirements

### Unit Tests

Location: `src/` alongside code (mod tests) or `tests/unit/`

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_string() {
        let input = b"+OK\r\n";
        let result = parse_resp2(input).unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".into()));
    }
}
```

### Integration Tests

Location: `tests/integration/`

```rust
#[tokio::test]
async fn test_set_get_over_tcp() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd_ok(&["SET", "key", "value"]).await;
    let result = client.cmd(&["GET", "key"]).await;
    assert_eq!(result, RespValue::bulk_string("value"));
}
```

### Property Tests

Location: `tests/property/`

```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn test_sorted_set_ordering(scores in vec(any::<f64>(), 0..100)) {
        let mut zset = SortedSet::new();
        for (i, score) in scores.iter().enumerate() {
            zset.add(format!("member{}", i).into(), *score);
        }

        // Verify ordering invariant
        let members = zset.range(..);
        for window in members.windows(2) {
            assert!(window[0].0 <= window[1].0);
        }
    }
}
```

### Stress Tests

Location: `tests/stress/`

```rust
#[tokio::test]
async fn test_concurrent_incr() {
    let server = TestServer::spawn().await;

    let tasks: Vec<_> = (0..100)
        .map(|_| {
            let addr = server.addr;
            tokio::spawn(async move {
                let mut client = TestClient::connect(addr).await;
                for _ in 0..1000 {
                    client.cmd(&["INCR", "counter"]).await;
                }
            })
        })
        .collect();

    for task in tasks {
        task.await.unwrap();
    }

    let mut client = server.client().await;
    let result = client.cmd(&["GET", "counter"]).await;
    assert_eq!(result, RespValue::bulk_string("100000"));
}
```

---

## Document Maintenance

### When to Update ROADMAP.md

- **Completing a task**: Check the box `- [ ]` -> `- [x]`
- **Completing a phase**: Update the Status field
- **Adding new tasks**: Add them to appropriate phase
- **Milestone reached**: Note in milestone criteria

### When to Update DESIGN.md

- **Changing architecture**: Update relevant section
- **Adding new crate**: Add to project structure
- **New data structure**: Document in Data Model
- **Protocol changes**: Update RESP section

### When to Update AGENTS.md

- **New conventions**: Add to Coding Conventions
- **New patterns**: Add as how-to guide
- **New invariants**: Add to Architecture Invariants
- **Common mistakes**: Add to Common Pitfalls

---

## Common Pitfalls

### Deadlock in Multi-Key Operations

```rust
// WRONG: Locks in arbitrary order
fn mset(keys: &[Key]) {
    for key in keys {
        let shard = get_shard(key);
        shard.lock();  // Can deadlock!
    }
}

// CORRECT: Sort shards first
fn mset(keys: &[Key]) {
    let mut shards: Vec<_> = keys.iter().map(get_shard_index).collect();
    shards.sort();
    shards.dedup();
    for shard_idx in shards {
        get_shard(shard_idx).lock();
    }
}
```

### Blocking in Async Context

```rust
// WRONG: Blocking call in async function
async fn load_config() {
    let content = std::fs::read_to_string("config.toml");  // Blocks!
}

// CORRECT: Use async I/O
async fn load_config() {
    let content = tokio::fs::read_to_string("config.toml").await;
}
```

### Platform-Specific Code Without Fallback

```rust
// WRONG: Unix-only
fn sync_file(f: &File) {
    nix::unistd::fdatasync(f.as_raw_fd());
}

// CORRECT: Cross-platform
fn sync_file(f: &File) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        nix::unistd::fdatasync(f.as_raw_fd())?;
    }
    #[cfg(target_os = "macos")]
    {
        use std::os::unix::io::AsRawFd;
        // F_FULLFSYNC for macOS
        unsafe { libc::fcntl(f.as_raw_fd(), libc::F_FULLFSYNC) };
    }
    #[cfg(target_os = "windows")]
    {
        f.sync_all()?;
    }
    Ok(())
}
```

### Forgetting to Propagate to AOF/Replication

```rust
// WRONG: Mutates state but doesn't propagate
fn set(ctx: &mut CommandContext, key: &[u8], value: &[u8]) -> CommandResult {
    ctx.store().set(key, value);
    Ok(RespValue::ok())
}

// CORRECT: Always propagate write commands
fn set(ctx: &mut CommandContext, key: &[u8], value: &[u8]) -> CommandResult {
    ctx.store().set(key, value);
    ctx.propagate(&["SET", key, value]);  // For AOF and replication
    Ok(RespValue::ok())
}
```

### Tests Without Cleanup

```rust
// WRONG: Test leaves server running
#[tokio::test]
async fn test_something() {
    let server = TestServer::spawn().await;
    // ... test logic ...
    // Server keeps running, port stays bound!
}

// CORRECT: Always stop server
#[tokio::test]
async fn test_something() {
    let server = TestServer::spawn().await;
    // ... test logic ...
    server.stop().await;  // Cleanup
}
```

---

## Quick Reference

### Default Configuration

| Setting | Value |
|---------|-------|
| Port | 6380 |
| Max connections | 10000 |
| Max databases | 16 |
| Timeout | 0 (none) |
| AOF | enabled |
| appendfsync | everysec |

### Command Template

```rust
/// COMMAND_NAME arg1 arg2 [OPT1] [OPT2 value]
///
/// Description of what the command does.
///
/// Time complexity: O(...)
pub fn command_name(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    // 1. Validate argument count
    if args.len() < 2 {
        return Err(CommandError::WrongArity("COMMAND_NAME"));
    }

    // 2. Parse arguments
    let key = args[0].as_bytes()?;
    let value = args[1].as_bytes()?;

    // 3. Type check if needed
    // 4. Execute operation
    // 5. Propagate for AOF/replication (if write)
    // 6. Return result
    Ok(RespValue::ok())
}
```

### Test Template

```rust
#[tokio::test]
async fn test_command_basic() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Setup
    // Action
    // Assert
    
    server.stop().await;
}

#[tokio::test]
async fn test_command_edge_case() { ... }

#[tokio::test]
async fn test_command_error_case() { ... }

#[tokio::test]
async fn test_command_concurrent() { ... }
```

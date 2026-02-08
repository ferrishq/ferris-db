# 🚀 Phase 2: Transactions, Pub/Sub, and Persistence

**Status**: In Progress  
**Start Date**: February 7, 2026  
**Goal**: Add ACID-like transactions, real-time messaging, and data durability

---

## 📋 Phase 2 Overview

Phase 2 adds three critical features to ferris-db:

1. **Transactions (MULTI/EXEC/WATCH)** - ACID-like command batching with optimistic locking
2. **Pub/Sub** - Real-time publish/subscribe messaging
3. **AOF Persistence** - Append-Only File for crash recovery

---

## 🎯 Implementation Strategy: Parallel Basics First

We'll implement **basic versions of all three features in parallel**, then enhance:

### Sprint 1: Basic Implementations (Current)
- ✅ **Transactions**: MULTI/EXEC/DISCARD (no WATCH yet)
- ✅ **Pub/Sub**: SUBSCRIBE/PUBLISH/UNSUBSCRIBE (no patterns yet)
- ✅ **AOF**: Write and replay (everysec mode only)

### Sprint 2: Enhanced Features (Next)
- **Transactions**: Add WATCH/UNWATCH for optimistic locking
- **Pub/Sub**: Add PSUBSCRIBE for pattern matching, PUBSUB introspection
- **AOF**: Add BGREWRITEAOF for compaction, fsync modes (always/no)

### Sprint 3: Production Hardening (Final)
- Performance optimization
- Edge case testing
- Integration testing
- Documentation

---

## 🔧 Feature 1: Transactions (MULTI/EXEC)

### Design: Transaction State Machine

Each connection tracks transaction state:

```rust
pub enum TransactionState {
    None,                          // Not in transaction
    Queuing(Vec<QueuedCommand>),  // In MULTI, queueing commands
    Executing,                     // Executing EXEC
}

pub struct QueuedCommand {
    name: String,
    args: Vec<RespValue>,
}
```

### Command Flow

```
Client: MULTI
Server: OK (state → Queuing)

Client: SET key1 value1
Server: QUEUED (command saved)

Client: INCR counter
Server: QUEUED (command saved)

Client: EXEC
Server: [OK, 1] (execute all, return results array)
       (state → None)
```

### Implementation Tasks

1. **Add transaction state to CommandContext**
   - `transaction_state: TransactionState`
   - Methods: `start_multi()`, `queue_command()`, `exec()`, `discard()`

2. **Implement MULTI command**
   - Check not already in transaction
   - Set state to `Queuing(vec![])`
   - Return OK

3. **Modify command execution**
   - If in `Queuing` state, save command instead of executing
   - Return QUEUED
   - Exception: MULTI, EXEC, DISCARD, WATCH execute immediately

4. **Implement EXEC command**
   - Check in transaction
   - Execute all queued commands sequentially
   - Collect results into array
   - Reset state to None
   - Return array of results

5. **Implement DISCARD command**
   - Check in transaction
   - Clear queued commands
   - Reset state to None
   - Return OK

### Key Edge Cases

- **Nested MULTI**: Return error, don't allow
- **EXEC without MULTI**: Return error
- **Syntax error during queue**: Queue the error, return QUEUED
- **Runtime error during exec**: Execute partial transaction, return error in results array
- **Connection close**: Clear transaction state

### Tests to Write (minimum 15)

1. Basic MULTI/EXEC with SET/GET
2. Multiple commands in transaction
3. DISCARD clears queue
4. EXEC returns array of results
5. Error during queueing (syntax)
6. Error during execution (WRONGTYPE)
7. Nested MULTI rejected
8. EXEC without MULTI rejected
9. DISCARD without MULTI rejected
10. Transaction with INCR shows correct final value
11. Transaction is atomic (concurrent observer sees all or nothing)
12. Empty transaction (MULTI, EXEC)
13. Transaction with blocking commands (should error)
14. Connection close clears transaction
15. Large transaction (100+ commands)

---

## 📡 Feature 2: Pub/Sub

### Design: Subscriber Registry

Global shared registry:

```rust
pub struct PubSubRegistry {
    // channel_name -> set of subscriber client IDs
    subscribers: DashMap<Bytes, HashSet<u64>>,
    
    // pattern -> set of subscriber client IDs
    pattern_subscribers: DashMap<Bytes, HashSet<u64>>,
}
```

Per-connection state:

```rust
pub struct PubSubState {
    mode: PubSubMode,
    subscribed_channels: HashSet<Bytes>,
    subscribed_patterns: HashSet<Bytes>,
}

pub enum PubSubMode {
    Normal,
    PubSub,  // Only SUBSCRIBE/UNSUBSCRIBE/PUBLISH/PING/QUIT allowed
}
```

### Command Flow

```
Client1: SUBSCRIBE news
Server: [subscribe, news, 1]
       (enter PubSub mode)

Client2: PUBLISH news "Breaking: Rust is awesome"
Server: 1 (number of subscribers)

Client1 receives: [message, news, "Breaking: Rust is awesome"]

Client1: UNSUBSCRIBE news
Server: [unsubscribe, news, 0]
       (exit PubSub mode if no subscriptions)
```

### Implementation Tasks

1. **Create ferris-pubsub crate** (or add to ferris-core)
   - PubSubRegistry struct
   - Methods: `subscribe()`, `unsubscribe()`, `publish()`, `get_subscribers()`

2. **Add PubSub state to CommandContext**
   - `pubsub_state: PubSubState`
   - `pubsub_registry: Arc<PubSubRegistry>`

3. **Implement SUBSCRIBE command**
   - Enter PubSub mode
   - Add channel to subscriptions
   - Register in global registry
   - Return [subscribe, channel, total_count]

4. **Implement PUBLISH command**
   - Get all subscribers for channel
   - Send message to each subscriber
   - Return subscriber count

5. **Implement UNSUBSCRIBE command**
   - Remove channel from subscriptions
   - Unregister from global registry
   - If no subscriptions left, exit PubSub mode
   - Return [unsubscribe, channel, remaining_count]

6. **Modify command executor**
   - In PubSub mode, only allow: SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, PING, QUIT
   - Return error for other commands

7. **Message delivery**
   - Use tokio channel per connection
   - Send messages as RESP arrays: [message, channel, data]

### Key Edge Cases

- **Subscribe to same channel twice**: Idempotent, don't duplicate
- **Publish to channel with no subscribers**: Return 0
- **Client disconnect**: Remove from all channels
- **PubSub mode restrictions**: Non-pubsub commands return error

### Tests to Write (minimum 12)

1. Basic SUBSCRIBE/PUBLISH/UNSUBSCRIBE
2. Multiple subscribers receive same message
3. PUBLISH with no subscribers returns 0
4. Multiple channels per subscriber
5. Unsubscribe from one channel, still get messages on others
6. PubSub mode blocks normal commands (SET should error)
7. Subscriber disconnect removes from channels
8. SUBSCRIBE to same channel twice (idempotent)
9. High-volume publishing (1000 messages)
10. Multiple publishers to same channel
11. PING/QUIT allowed in PubSub mode
12. Unsubscribe from all channels exits PubSub mode

---

## 💾 Feature 3: AOF Persistence

### Design: Async AOF Writer

```rust
pub struct AofWriter {
    // Async channel for write commands
    tx: mpsc::Sender<AofEntry>,
    // Background task handle
    task_handle: JoinHandle<()>,
}

pub struct AofEntry {
    command: Vec<RespValue>,
    timestamp: Instant,
}
```

Background writer task:

```rust
async fn aof_writer_task(
    mut rx: mpsc::Receiver<AofEntry>,
    file_path: PathBuf,
) {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(file_path)?;
    
    loop {
        match rx.recv().await {
            Some(entry) => {
                // Serialize command to RESP
                let bytes = serialize_resp(&entry.command);
                file.write_all(&bytes)?;
                
                // fsync every second (everysec mode)
                if should_fsync() {
                    file.sync_all()?;
                }
            }
            None => break, // Channel closed
        }
    }
}
```

### Implementation Tasks

1. **Create AOF writer in ferris-persistence**
   - AofWriter struct with async channel
   - Background task for writing
   - RESP serialization

2. **Integrate with CommandExecutor**
   - After executing write command, send to AOF channel
   - Only write commands (SET, DEL, INCR, etc.), not reads

3. **Implement AOF replay**
   - On startup, read AOF file
   - Parse RESP commands
   - Execute each command
   - Log progress every 10k commands

4. **Add configuration**
   - `appendonly yes/no`
   - `appendfilename "ferris.aof"`
   - `appendfsync everysec/always/no`

5. **Handle fsync modes**
   - `always`: fsync after every write
   - `everysec`: fsync every second (background timer)
   - `no`: Let OS handle fsync

### Key Edge Cases

- **AOF disabled**: Don't send to channel
- **Disk full**: Log error, continue serving (Redis behavior)
- **Corrupted AOF**: Skip bad command, log warning, continue
- **Large AOF**: Stream parse, don't load all into memory

### Tests to Write (minimum 10)

1. AOF file created on first write
2. AOF contains valid RESP commands
3. Server restart recovers data from AOF
4. Multiple commands written to AOF
5. AOF disabled doesn't create file
6. Truncated AOF handled gracefully
7. Concurrent writes don't corrupt AOF
8. Large dataset recovery (10k commands)
9. appendfsync everysec batches writes
10. Read commands not written to AOF

---

## 📐 Architecture Changes

### CommandContext Enhancements

```rust
pub struct CommandContext {
    // Existing
    store: Arc<KeyStore>,
    blocking_registry: Arc<BlockingRegistry>,
    selected_db: usize,
    client_id: u64,
    client_name: Option<String>,
    
    // NEW: Transaction state
    transaction_state: TransactionState,
    watch_keys: Option<HashMap<Bytes, u64>>, // key -> version
    
    // NEW: Pub/Sub state
    pubsub_state: PubSubState,
    pubsub_registry: Arc<PubSubRegistry>,
    
    // NEW: AOF writer
    aof_writer: Option<Arc<AofWriter>>,
}
```

### Shared State

```rust
// Server now holds
pub struct Server {
    // Existing
    store: Arc<KeyStore>,
    blocking_registry: Arc<BlockingRegistry>,
    
    // NEW
    pubsub_registry: Arc<PubSubRegistry>,
    aof_writer: Option<Arc<AofWriter>>,
}
```

---

## 🧪 Testing Strategy

### Unit Tests (per feature)
- **Transactions**: 15+ tests in ferris-commands
- **Pub/Sub**: 12+ tests in ferris-pubsub
- **AOF**: 10+ tests in ferris-persistence

### Integration Tests
- **Transactions**: Multi-connection atomicity tests
- **Pub/Sub**: Multi-subscriber message delivery
- **AOF**: Crash recovery simulation

### End-to-End Tests
- Combined scenario: Transaction + AOF ensures durability
- Combined scenario: Pub/Sub + Transactions for event sourcing
- Combined scenario: Full stack with all three features

### Target Coverage
- **Phase 2 code coverage**: >= 95%
- **Total tests**: 2,268 (Phase 1) + ~150 (Phase 2) = ~2,420 tests

---

## 📅 Sprint Timeline

### Sprint 1: Basic Implementations (Week 1)
**Days 1-2**: Transactions (MULTI/EXEC/DISCARD)
- Implement transaction state machine
- Add commands
- Write 15 tests

**Days 3-4**: Pub/Sub (SUBSCRIBE/PUBLISH)
- Implement subscriber registry
- Add commands
- Write 12 tests

**Days 5-6**: AOF Persistence
- Implement AOF writer
- Implement AOF replay
- Write 10 tests

**Day 7**: Integration & Testing
- Run full test suite
- Fix issues
- Document progress

---

## 🎯 Success Criteria for Sprint 1

✅ **Transactions**:
- MULTI/EXEC/DISCARD work correctly
- Commands queued and executed atomically
- All 15 tests pass

✅ **Pub/Sub**:
- SUBSCRIBE/PUBLISH/UNSUBSCRIBE work
- Messages delivered to all subscribers
- All 12 tests pass

✅ **AOF**:
- Write commands persisted to AOF
- Server restart recovers data
- All 10 tests pass

✅ **Integration**:
- All Phase 1 tests still pass (2,268 tests)
- New Phase 2 tests pass (~37 tests)
- Total: ~2,305 tests passing

---

## 📖 Implementation Order

Based on complexity and dependencies:

1. **Start with**: AOF Writer (most independent)
2. **Then**: Transactions (needs CommandContext changes)
3. **Finally**: Pub/Sub (needs connection message routing)

OR implement all three in parallel if working solo.

---

## 🚀 Let's Begin!

Ready to start Phase 2! Which feature would you like to tackle first?

1. **AOF Persistence** - Most independent, good foundation
2. **Transactions** - Most complex, best to start fresh
3. **Pub/Sub** - Most fun, visible results quickly

Let me know and we'll dive in! 🦀✨

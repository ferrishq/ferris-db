# ferris-db TODO List

> **Last Updated**: 2026-02-08  
> **Current Status**: 191/230 commands (83% complete)

---

## High Priority: Complete Phase 2 (39 commands remaining)

### 🔴 **CRITICAL: Streams Implementation (25 commands)**

**Effort**: 6-8 hours | **Priority**: HIGH | **Complexity**: HIGH

Streams are one of Redis's most powerful features for event sourcing, messaging, and time-series data.

#### **Step 1: Add Stream Data Type (2 hours)**

**File**: `ferris-core/src/value.rs`

```rust
// Add to RedisValue enum
Stream {
    entries: BTreeMap<StreamId, HashMap<Bytes, Bytes>>,
    length: usize,
    last_generated_id: StreamId,
    // Consumer group state
    groups: HashMap<Bytes, ConsumerGroup>,
}

// New types to add
pub struct StreamId {
    timestamp_ms: u64,
    sequence: u64,
}

pub struct ConsumerGroup {
    name: Bytes,
    last_delivered_id: StreamId,
    pending: BTreeMap<StreamId, PendingEntry>,
    consumers: HashMap<Bytes, Consumer>,
}

pub struct PendingEntry {
    id: StreamId,
    consumer: Bytes,
    delivery_time: Instant,
    delivery_count: usize,
}

pub struct Consumer {
    name: Bytes,
    pending: HashSet<StreamId>,
    idle_time: Duration,
}
```

**Tests to Write** (20+ tests):
- StreamId parsing and generation
- StreamId auto-generation (timestamp + sequence)
- StreamId comparison and ordering
- Entry insertion and range queries
- Consumer group creation and management
- Pending entry tracking

#### **Step 2: Core Stream Commands (3 hours)**

**File**: `ferris-commands/src/stream.rs` (NEW)

Commands to implement:
1. ✅ **XADD** - Add entry to stream
   - Syntax: `XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold [LIMIT count]] *|ID field value [field value ...]`
   - Auto-generate ID if `*`
   - Enforce MAXLEN trimming
   - Return generated stream ID

2. ✅ **XLEN** - Get stream length
   - Simple counter

3. ✅ **XRANGE** - Range query
   - Syntax: `XRANGE key start end [COUNT count]`
   - Support `-` for minimum, `+` for maximum
   - Return entries in time order

4. ✅ **XREVRANGE** - Reverse range query
   - Same as XRANGE but reversed

5. ✅ **XREAD** - Read from streams (blocking optional)
   - Syntax: `XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] ID [ID ...]`
   - Support blocking with timeout
   - Multiple streams

6. ✅ **XDEL** - Delete entries
   - Mark entries as deleted
   - Don't actually remove (maintain ID sequence)

7. ✅ **XTRIM** - Trim stream
   - MAXLEN or MINID
   - Approximate trimming (~)
   - LIMIT for gradual trimming

8. ✅ **XINFO STREAM** - Stream info
   - Return stream metadata

**Tests**: 30+ tests covering all operations and edge cases

#### **Step 3: Consumer Group Commands (3 hours)**

Commands to implement:
9. ✅ **XGROUP CREATE** - Create consumer group
10. ✅ **XGROUP DESTROY** - Delete consumer group
11. ✅ **XGROUP SETID** - Set group last delivered ID
12. ✅ **XGROUP CREATECONSUMER** - Create consumer in group
13. ✅ **XGROUP DELCONSUMER** - Remove consumer from group
14. ✅ **XREADGROUP** - Read as consumer
    - Deliver messages to consumer
    - Track in pending list (PEL)
15. ✅ **XACK** - Acknowledge messages
    - Remove from pending list
16. ✅ **XPENDING** - Query pending messages
17. ✅ **XCLAIM** - Claim pending messages from idle consumer
18. ✅ **XAUTOCLAIM** - Auto-claim idle messages
19. ✅ **XINFO GROUPS** - List consumer groups
20. ✅ **XINFO CONSUMERS** - List consumers in group

**Tests**: 25+ tests covering consumer group operations

#### **Validation Checklist**
- [ ] All 25 stream commands implemented
- [ ] 50+ tests passing
- [ ] StreamId auto-generation works correctly
- [ ] Range queries handle edge cases (-, +, empty streams)
- [ ] Consumer groups track pending entries correctly
- [ ] XACK removes from pending list
- [ ] XCLAIM transfers ownership properly
- [ ] Blocking XREAD with timeout works
- [ ] MAXLEN trimming preserves most recent entries
- [ ] Integration test with redis-cli works

---

### 🟡 **MEDIUM: Lua Scripting (6 commands)**

**Effort**: 4-6 hours | **Priority**: MEDIUM | **Complexity**: MEDIUM

Scripting allows complex operations with atomicity guarantees.

#### **Step 1: Add mlua Dependency (15 minutes)**

**File**: `ferris-commands/Cargo.toml`

```toml
[dependencies]
mlua = { version = "0.9", features = ["lua54", "async", "serialize"] }
sha1 = "0.10"
```

#### **Step 2: Script Cache Infrastructure (1 hour)**

**File**: `ferris-commands/src/script.rs` (NEW)

```rust
pub struct ScriptCache {
    scripts: Arc<DashMap<String, String>>, // SHA1 -> script source
}

impl ScriptCache {
    pub fn load(&self, script: &str) -> String {
        let sha1 = compute_sha1(script);
        self.scripts.insert(sha1.clone(), script.to_string());
        sha1
    }

    pub fn exists(&self, sha1: &str) -> bool {
        self.scripts.contains_key(sha1)
    }

    pub fn get(&self, sha1: &str) -> Option<String> {
        self.scripts.get(sha1).map(|s| s.clone())
    }

    pub fn flush(&self) {
        self.scripts.clear();
    }
}
```

#### **Step 3: Lua Environment Setup (1 hour)**

Create sandboxed Lua environment with Redis API:

```rust
pub fn create_lua_env() -> Result<Lua, mlua::Error> {
    let lua = Lua::new();
    
    // Add redis table
    let redis = lua.create_table()?;
    redis.set("call", lua.create_function(redis_call)?)?;
    redis.set("pcall", lua.create_function(redis_pcall)?)?;
    lua.globals().set("redis", redis)?;
    
    // Disable dangerous functions
    lua.globals().set("os", lua.null())?;
    lua.globals().set("io", lua.null())?;
    lua.globals().set("dofile", lua.null())?;
    lua.globals().set("loadfile", lua.null())?;
    
    Ok(lua)
}
```

#### **Step 4: Implement Commands (2 hours)**

1. ✅ **EVAL** - Execute Lua script
2. ✅ **EVALSHA** - Execute cached script
3. ✅ **SCRIPT LOAD** - Cache script, return SHA1
4. ✅ **SCRIPT EXISTS** - Check if scripts exist
5. ✅ **SCRIPT FLUSH** - Clear script cache
6. ✅ **SCRIPT KILL** - Kill running script (if not written yet)

**Tests**: 20+ tests covering:
- Basic script execution
- KEYS and ARGV arguments
- redis.call() vs redis.pcall()
- Error handling
- Script caching
- SHA1 computation
- Timeout handling

#### **Validation Checklist**
- [ ] All 6 script commands implemented
- [ ] 20+ tests passing
- [ ] Lua environment properly sandboxed
- [ ] redis.call() executes commands correctly
- [ ] redis.pcall() catches errors
- [ ] KEYS and ARGV passed correctly
- [ ] Script cache works (SHA1 keys)
- [ ] EVALSHA retrieves cached scripts
- [ ] Integration test with complex Lua works

---

### 🟢 **LOW: Minor Missing Commands (8 commands)**

**Effort**: 2-3 hours | **Priority**: LOW | **Complexity**: LOW

#### **Quick Wins**
1. [ ] **SINTERCARD** - Set intersection cardinality (check if exists)
2. [ ] **ZMPOP** - Pop from sorted set (Redis 7.0+)
3. [ ] **BZMPOP** - Blocking pop from sorted set (Redis 7.0+)
4. [ ] **LMPOP** - Pop from list (check if exists)
5. [ ] **SPUBLISH** - Shard pub/sub (Redis 7.0+)
6. [ ] **SSUBSCRIBE** - Shard subscribe (Redis 7.0+)
7. [ ] **PUBSUB SHARDCHANNELS** - List shard channels (Redis 7.0+)
8. [ ] **PUBSUB SHARDNUMSUB** - Shard subscriber counts (Redis 7.0+)

Most of these are Redis 7.0+ features and can be skipped for now.

**Validation**: 10+ tests per command

---

## Medium Priority: Production Readiness

### 📊 **Performance & Benchmarking (4-6 hours)**

#### **Step 1: Benchmark Suite**

**File**: `benches/command_bench.rs` (NEW)

```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn bench_set(c: &mut Criterion) {
    // Benchmark SET command throughput
}

fn bench_get(c: &mut Criterion) {
    // Benchmark GET command throughput
}

fn bench_incr(c: &mut Criterion) {
    // Benchmark INCR command throughput
}

criterion_group!(benches, bench_set, bench_get, bench_incr);
criterion_main!(benches);
```

Tasks:
- [ ] Add criterion benchmark suite
- [ ] Benchmark all command categories
- [ ] Compare with Redis baseline
- [ ] Document results in README

#### **Step 2: Integration Tests**

**File**: `tests/integration/` (various)

- [ ] Test with redis-cli
- [ ] Test with redis-py (Python client)
- [ ] Test with Jedis (Java client)
- [ ] Test with StackExchange.Redis (.NET client)
- [ ] Test with node-redis (Node.js client)

#### **Step 3: Stress Tests**

- [ ] Multi-client concurrent access
- [ ] Large dataset operations (millions of keys)
- [ ] Memory limit enforcement
- [ ] AOF under heavy load
- [ ] Pub/Sub with many subscribers

---

### 📚 **Documentation (2-4 hours)**

#### **User Documentation**
- [ ] README.md with features and quick start
- [ ] INSTALL.md with detailed setup
- [ ] CONFIGURATION.md with all options
- [ ] API compatibility matrix (what works vs Redis)

#### **Developer Documentation**
- [ ] Architecture overview
- [ ] Data flow diagrams
- [ ] Concurrency model explanation
- [ ] How to add new commands
- [ ] How to add new data types

#### **Operations Documentation**
- [ ] Deployment guide
- [ ] Monitoring and metrics
- [ ] Backup and restore procedures
- [ ] Troubleshooting guide
- [ ] Performance tuning guide

---

### 🐳 **Deployment (2-3 hours)**

#### **Docker**
- [ ] Create Dockerfile
- [ ] Create docker-compose.yml for testing
- [ ] Publish to Docker Hub
- [ ] Add health checks

#### **Kubernetes**
- [ ] Create Helm chart
- [ ] Add StatefulSet for persistence
- [ ] Add Service and ConfigMap
- [ ] Add monitoring (Prometheus metrics)

---

## Low Priority: Phase 3 Features (Future)

### 🌐 **Cluster Mode (20-40 hours)**

**Very complex feature - needs dedicated sprint**

Components needed:
- Consistent hashing (16384 slots)
- Slot migration protocol
- Gossip protocol
- Automatic failover
- MOVED/ASK redirections
- Multi-key command routing

**40+ CLUSTER commands to implement**

### 🔐 **ACL System (8-12 hours)**

Components needed:
- User database
- Permission system (commands, keys, channels)
- ACL file format
- Authentication integration

**12 ACL commands to implement**

### 🔄 **Replication (12-16 hours)**

Components needed:
- Replication protocol
- Incremental sync
- Full sync with RDB
- Replication backlog
- Automatic reconnection

**8+ replication commands**

---

## Quick Reference: Command Coverage

### ✅ Fully Complete (100%)
- Hash commands (16/16)
- Transactions (5/5)
- HyperLogLog (3/3)
- Geo (6/6)

### 🟢 Near Complete (90%+)
- String (28/29 - 97%)
- Key management (26/27 - 96%)
- Sorted Set (35/37 - 95%)
- List (17/18 - 94%)
- Set (18/19 - 95%)

### 🟡 Mostly Complete (60-89%)
- Server/Admin (25/30 - 83%)
- Connection (6/8 - 75%)
- Pub/Sub (6/10 - 60%)

### 🔴 Not Started (0%)
- Streams (0/25)
- Scripting (0/6)
- Cluster (0/40+)
- ACL (0/12)

---

## Development Workflow

### Before Starting Work
1. Read [AGENTS.md](./AGENTS.md) for guidelines
2. Check current test coverage: `cargo tarpaulin --workspace`
3. Review related Redis documentation

### While Working
1. Write tests first (TDD)
2. Implement minimum code to pass tests
3. Refactor while keeping tests green
4. Add more edge case tests
5. Verify coverage >= 95%

### Before Committing
1. Run full test suite: `cargo test --workspace`
2. Check formatting: `cargo fmt --check`
3. Run clippy: `cargo clippy -- -D warnings`
4. Build release: `cargo build --release`
5. Test with redis-cli if applicable

---

## Notes for Future Contributors

### Easy Entry Points (1-2 hours each)
- Missing small commands (SINTERCARD, ZMPOP, etc.)
- Additional tests for existing commands
- Documentation improvements
- Benchmark suite setup

### Medium Challenges (4-8 hours each)
- Lua scripting implementation
- Advanced pub/sub features
- Performance optimizations
- Integration test suite

### Large Projects (20+ hours each)
- Streams implementation
- Cluster mode
- ACL system
- Full replication support

---

## Contact & Support

For questions or discussions:
- Open an issue on GitHub
- Check [AGENTS.md](./AGENTS.md) for development guidelines
- Review [DESIGN.md](./DESIGN.md) for architecture details

---

**Last Updated**: 2026-02-08  
**Next Review**: When Streams or Scripting is completed

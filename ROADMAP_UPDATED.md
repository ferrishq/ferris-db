# ferris-db Roadmap

> **Status**: Phase 2 - MOSTLY COMPLETE ✅ (191/230 commands = 83%)  
> **Last Updated**: 2026-02-08  
> **Default Port**: 6380 (to avoid conflict with Redis on 6379)

---

## Current Status Summary

### 🎉 **191 Commands Implemented (83% Redis Compatibility)**

**Total Tests**: 2,315+ passing across all modules

### Completed Features ✅

#### **Core Data Types** (114/120 commands - 95%)
- ✅ **String**: 28/29 commands (97%)
- ✅ **List**: 17/18 commands (94%)
- ✅ **Hash**: 16/16 commands (100%)
- ✅ **Set**: 18/19 commands (95%)
- ✅ **Sorted Set**: 35/37 commands (95%)

#### **Key Management** (26/27 commands - 96%)
- ✅ DEL, UNLINK, EXISTS, KEYS, SCAN
- ✅ EXPIRE, EXPIREAT, PEXPIRE, PEXPIREAT, TTL, PTTL, PERSIST
- ✅ EXPIRETIME, PEXPIRETIME
- ✅ TYPE, RENAME, RENAMENX
- ✅ TOUCH, RANDOMKEY, MOVE, COPY
- ✅ DUMP, RESTORE
- ✅ SORT, SORT_RO
- ✅ OBJECT (ENCODING, REFCOUNT, IDLETIME, FREQ, HELP)

#### **Server/Admin** (25/30 commands - 83%)
- ✅ PING, ECHO, TIME
- ✅ COMMAND, COMMAND COUNT, COMMAND GETKEYS, COMMAND INFO
- ✅ DBSIZE, FLUSHDB, FLUSHALL
- ✅ INFO, CONFIG (GET, SET, RESETSTAT, REWRITE)
- ✅ SAVE, BGSAVE, LASTSAVE, BGREWRITEAOF
- ✅ SHUTDOWN, ROLE
- ✅ SLOWLOG, MEMORY

#### **Connection** (6/8 commands - 75%)
- ✅ SELECT, QUIT, RESET
- ✅ CLIENT (SETNAME, GETNAME, ID, LIST, KILL, PAUSE, REPLY, NO-EVICT)
- ✅ HELLO (RESP2/RESP3 negotiation)
- ✅ AUTH

#### **Transactions** (5/5 commands - 100%) ✨
- ✅ MULTI - Begin transaction
- ✅ EXEC - Execute queued commands
- ✅ DISCARD - Abort transaction
- ✅ WATCH - Optimistic locking
- ✅ UNWATCH - Clear watched keys

#### **HyperLogLog** (3/3 commands - 100%) ✨
- ✅ PFADD - Add elements to HLL
- ✅ PFCOUNT - Count unique elements
- ✅ PFMERGE - Merge HLLs

#### **Geo** (6/6 commands - 100%) ✨
- ✅ GEOADD - Add geo locations
- ✅ GEODIST - Distance between points
- ✅ GEOHASH - Get geohash strings
- ✅ GEOPOS - Get coordinates
- ✅ GEORADIUS - Query by radius
- ✅ GEOSEARCH - Query by area

#### **Pub/Sub** (6/10 commands - 60%) ✨
- ✅ PUBLISH - Post messages
- ✅ SUBSCRIBE - Subscribe to channels
- ✅ UNSUBSCRIBE - Unsubscribe from channels
- ✅ PSUBSCRIBE - Pattern subscriptions
- ✅ PUNSUBSCRIBE - Pattern unsubscriptions
- ✅ PUBSUB (CHANNELS, NUMSUB, NUMPAT)

#### **Blocking Operations** (11 commands) ✅
- ✅ BLPOP, BRPOP, BLMOVE
- ✅ BZPOPMIN, BZPOPMAX, BLMPOP
- ✅ BRPOPLPUSH (legacy)

#### **Advanced String Operations** ✨
- ✅ BITFIELD - Arbitrary bit operations with GET/SET/INCRBY
- ✅ BITOP - Bitwise AND/OR/XOR/NOT
- ✅ BITCOUNT, BITPOS, SETBIT, GETBIT
- ✅ LCS - Longest common subsequence

---

## Remaining Work (39 commands)

### Phase 2 Remaining Items

#### **1. Streams (25 commands - 0%) - HIGH PRIORITY**

**Core Operations (12 commands)**
- [ ] XADD - Add entry to stream
- [ ] XLEN - Get stream length
- [ ] XRANGE - Range query
- [ ] XREVRANGE - Reverse range query
- [ ] XREAD - Read from streams
- [ ] XDEL - Delete entries
- [ ] XTRIM - Trim stream
- [ ] XINFO (STREAM, GROUPS, CONSUMERS)

**Consumer Groups (13 commands)**
- [ ] XGROUP CREATE - Create consumer group
- [ ] XGROUP DESTROY - Delete consumer group
- [ ] XGROUP SETID - Set group ID
- [ ] XGROUP CREATECONSUMER - Add consumer
- [ ] XGROUP DELCONSUMER - Remove consumer
- [ ] XREADGROUP - Read as consumer
- [ ] XACK - Acknowledge messages
- [ ] XPENDING - Pending messages
- [ ] XCLAIM - Claim pending messages
- [ ] XAUTOCLAIM - Auto-claim idle messages

**Implementation Requirements:**
- New `RedisValue::Stream` variant
- Stream entry: `(id: StreamId, fields: HashMap<Bytes, Bytes>)`
- StreamId: timestamp-sequence format (e.g., "1526919030474-0")
- Consumer group state tracking
- Pending entry list (PEL) per consumer
- 50+ tests covering all operations

**Estimated Effort**: 6-8 hours

#### **2. Scripting (6 commands - 0%) - MEDIUM PRIORITY**

- [ ] EVAL - Execute Lua script
- [ ] EVALSHA - Execute cached script
- [ ] SCRIPT LOAD - Cache script
- [ ] SCRIPT EXISTS - Check if script exists
- [ ] SCRIPT FLUSH - Clear script cache
- [ ] SCRIPT KILL - Kill running script

**Implementation Requirements:**
- Add `mlua` crate dependency (Lua 5.4 integration)
- Script caching with SHA1 keys
- Sandboxed Lua environment
- Redis API bindings (redis.call, redis.pcall)
- Script timeout handling
- 20+ tests covering Lua execution, caching, errors

**Estimated Effort**: 4-6 hours

#### **3. Missing Pub/Sub Commands (4 commands)**

- [ ] PUBSUB SHARDCHANNELS - List shard channels (Redis 7.0+)
- [ ] PUBSUB SHARDNUMSUB - Shard subscriber counts (Redis 7.0+)
- [ ] SPUBLISH - Publish to shard channel (Redis 7.0+)
- [ ] SSUBSCRIBE - Subscribe to shard channel (Redis 7.0+)

**Note**: These are Redis 7.0+ sharded pub/sub features, lower priority

**Estimated Effort**: 1-2 hours

#### **4. Missing String Commands (1 command)**

- [ ] STRALGO LCS (with advanced options) - Already have basic LCS

**Estimated Effort**: 30 minutes

#### **5. Missing List Commands (1 command)**

- [ ] LMPOP (non-blocking variant) - May already exist, needs verification

**Estimated Effort**: 30 minutes

#### **6. Missing Set Commands (1 command)**

- [ ] SINTERCARD - Intersection cardinality (may exist, needs verification)

**Estimated Effort**: 30 minutes

#### **7. Missing Sorted Set Commands (2 commands)**

- [ ] BZMPOP - Blocking multi-pop (Redis 7.0+)
- [ ] ZMPOP - Multi-pop (Redis 7.0+)

**Estimated Effort**: 1 hour

---

## Phase 3: Advanced Features (Future)

### **Cluster Mode (40+ commands - 0%)**

**Slot Management**
- [ ] CLUSTER ADDSLOTS, DELSLOTS, SETSLOT
- [ ] CLUSTER SLOTS, SHARDS, NODES
- [ ] CLUSTER MEET, FORGET
- [ ] CLUSTER FAILOVER, RESET
- [ ] CLUSTER REPLICATE, SAVECONFIG

**Key Management**
- [ ] CLUSTER KEYSLOT
- [ ] CLUSTER COUNTKEYSINSLOT
- [ ] CLUSTER GETKEYSINSLOT

**Implementation Requirements:**
- Consistent hashing (16384 slots)
- Slot migration protocol
- Gossip protocol for cluster state
- Automatic failover
- Multi-key command routing
- MOVED/ASK redirections

**Estimated Effort**: 20-40 hours

### **ACL System (12 commands - 0%)**

- [ ] ACL SETUSER, DELUSER, LIST
- [ ] ACL GETUSER, WHOAMI
- [ ] ACL LOAD, SAVE
- [ ] ACL CAT, GENPASS
- [ ] ACL LOG, DRYRUN

**Implementation Requirements:**
- User/password management
- Permission system (commands, keys, channels)
- ACL file format support
- Authentication flow integration

**Estimated Effort**: 8-12 hours

---

## Architecture Components Status

### ✅ **Completed**
- Multi-threaded KeyStore (DashMap sharding)
- Key-level locking (no global locks)
- Deadlock-free multi-key operations (sorted shard locking)
- RESP2/RESP3 protocol support
- AOF persistence (append-only file)
- AOF rewriting/compaction
- TTL expiry system (lazy + active)
- Memory tracking and eviction policies
- Blocking operations (BLPOP, BRPOP, etc.)
- Transaction support (MULTI/EXEC/WATCH)
- Pub/Sub infrastructure
- Geo spatial operations
- HyperLogLog probabilistic counting
- BITFIELD advanced bitmap operations

### 🚧 **In Progress**
- Comprehensive integration tests
- Performance benchmarking vs Redis
- Production documentation

### ⏳ **Future**
- Replication (leader-follower)
- Cluster mode
- CRDT support for active-active
- TLS support
- Lua scripting
- Streams
- ACL system

---

## Testing Status

### Current Coverage
- **Total Tests**: 2,315+ passing
  - ferris-commands: 1,991 tests
  - ferris-core: 150 tests (including pub/sub)
  - ferris-persistence: 63 tests
  - ferris-protocol: 23 tests
  - Other modules: 88 tests

### Coverage Targets
- Line coverage: >= 95% (target 100%)
- Branch coverage: >= 90%
- Integration tests: Verify RESP protocol over TCP
- Compatibility tests: redis-cli and client libraries work

### Testing Strategy
1. **Unit tests**: Every command has 10-20 tests
2. **Integration tests**: Real TCP connections with RESP codec
3. **Property tests**: For complex data structures (sorted sets, streams)
4. **Concurrency tests**: Multi-threaded access patterns
5. **Benchmark tests**: Performance comparison with Redis

---

## Performance Targets

### Throughput (Single-threaded baseline)
- SET: >= 100k ops/sec
- GET: >= 100k ops/sec
- INCR: >= 100k ops/sec
- LPUSH: >= 100k ops/sec
- SADD: >= 100k ops/sec

### Latency (P99)
- GET/SET: < 1ms
- ZADD: < 1ms
- MULTI/EXEC: < 2ms

### Memory
- Overhead per key: < 100 bytes
- AOF write buffer: Configurable (default 64MB)
- Efficient eviction with LRU/LFU

---

## Quick Start for Contributors

### Build & Test
```bash
# Build everything
cargo build --release

# Run all tests
cargo test --workspace

# Run specific module tests
cargo test --package ferris-commands

# Check code coverage
cargo tarpaulin --workspace --out Html

# Run the server
./target/release/ferris-db --port 6380 --aof true

# Test with redis-cli
redis-cli -p 6380
```

### Adding a New Command
1. Write tests first in appropriate module (e.g., `ferris-commands/src/string.rs`)
2. Implement command handler
3. Register in `ferris-commands/src/registry.rs`
4. Verify tests pass: `cargo test`
5. Check coverage: `cargo tarpaulin --package ferris-commands`

---

## Next Milestones

### **Milestone 1: Complete Phase 2 (Target: 220+ commands)**
- [ ] Implement Streams (25 commands)
- [ ] Implement Scripting (6 commands)
- [ ] Add remaining minor commands (8 commands)
- [ ] Achieve 96% Redis OSS compatibility

**Estimated Total Effort**: 12-18 hours

### **Milestone 2: Production Ready**
- [ ] Comprehensive integration test suite
- [ ] Performance benchmarking
- [ ] Production documentation
- [ ] Docker image
- [ ] Helm chart for Kubernetes

**Estimated Total Effort**: 8-12 hours

### **Milestone 3: Phase 3 - Distributed Features**
- [ ] Replication (leader-follower)
- [ ] Cluster mode (consistent hashing, slot migration)
- [ ] ACL system
- [ ] TLS support

**Estimated Total Effort**: 40-60 hours

---

## Contributing

See [AGENTS.md](./AGENTS.md) for detailed development guidelines, including:
- Test-driven development requirements
- Architecture invariants
- Coding conventions
- Common pitfalls
- How to add new commands/features

---

## License

Apache 2.0 + MIT dual license (TBD - needs LICENSE files)

# Redis Commands Implementation Status

**Last Updated**: 2026-02-24  
**Total Redis Commands**: ~470 (Redis 7.2)  
**Implemented in ferris-db**: 221 commands  
**Coverage**: ~47%

> This document tracks Redis command compatibility. Commands are organized by category matching Redis documentation.

---

## Legend

- ✅ **Implemented** - Fully working, tested
- 🟡 **Partial** - Basic implementation, missing features
- ❌ **Not Implemented**
- 🔒 **Enterprise** - Redis Enterprise only (may not implement)
- 📝 **Stub** - Placeholder implementation

---

## String Commands (24/30)

| Command | Status | Notes |
|---------|--------|-------|
| APPEND | ✅ | |
| DECR | ✅ | |
| DECRBY | ✅ | |
| GET | ✅ | |
| GETDEL | ✅ | |
| GETEX | ✅ | |
| GETRANGE | ✅ | |
| GETSET | ✅ | Deprecated, use SET with GET |
| INCR | ✅ | |
| INCRBY | ✅ | |
| INCRBYFLOAT | ✅ | |
| LCS | ✅ | Longest common subsequence |
| MGET | ✅ | |
| MSET | ✅ | |
| MSETNX | ✅ | |
| PSETEX | ✅ | |
| SET | ✅ | With EX, PX, NX, XX, GET options |
| SETEX | ✅ | |
| SETNX | ✅ | |
| SETRANGE | ✅ | |
| STRLEN | ✅ | |
| SUBSTR | ✅ | Deprecated, use GETRANGE |
| GETBIT | ✅ | |
| SETBIT | ✅ | |
| BITCOUNT | ✅ | |
| BITPOS | ✅ | |
| BITOP | ✅ | AND, OR, XOR, NOT |
| BITFIELD | ✅ | |
| BITFIELD_RO | ❌ | Read-only variant |
| STRALGO | ❌ | Moved to LCS in Redis 7.0 |

**Coverage**: 24/30 (80%)

---

## Hash Commands (15/15)

| Command | Status | Notes |
|---------|--------|-------|
| HDEL | ✅ | |
| HEXISTS | ✅ | |
| HGET | ✅ | |
| HGETALL | ✅ | |
| HINCRBY | ✅ | |
| HINCRBYFLOAT | ✅ | |
| HKEYS | ✅ | |
| HLEN | ✅ | |
| HMGET | ✅ | |
| HMSET | ✅ | Deprecated, use HSET |
| HRANDFIELD | ✅ | |
| HSCAN | ✅ | |
| HSET | ✅ | |
| HSETNX | ✅ | |
| HSTRLEN | ✅ | |
| HVALS | ✅ | |
| HEXPIRE | ❌ | Redis 7.4+ |
| HEXPIREAT | ❌ | Redis 7.4+ |
| HEXPIRETIME | ❌ | Redis 7.4+ |
| HPERSIST | ❌ | Redis 7.4+ |
| HPEXPIRE | ❌ | Redis 7.4+ |
| HPEXPIREAT | ❌ | Redis 7.4+ |
| HPEXPIRETIME | ❌ | Redis 7.4+ |
| HPTTL | ❌ | Redis 7.4+ |
| HTTL | ❌ | Redis 7.4+ |

**Coverage**: 15/15 (100% for Redis 7.2)

---

## List Commands (19/23)

| Command | Status | Notes |
|---------|--------|-------|
| BLMOVE | ✅ | Blocking LMOVE |
| BLMPOP | ✅ | Blocking LMPOP |
| BLPOP | ✅ | |
| BRPOP | ✅ | |
| BRPOPLPUSH | ✅ | Deprecated, use BLMOVE |
| LINDEX | ✅ | |
| LINSERT | ✅ | |
| LLEN | ✅ | |
| LMOVE | ✅ | |
| LMPOP | ✅ | |
| LPOP | ✅ | |
| LPOS | ✅ | |
| LPUSH | ✅ | |
| LPUSHX | ✅ | |
| LRANGE | ✅ | |
| LREM | ✅ | |
| LSET | ✅ | |
| LTRIM | ✅ | |
| RPOP | ✅ | |
| RPOPLPUSH | ✅ | Deprecated, use LMOVE |
| RPUSH | ✅ | |
| RPUSHX | ✅ | |
| LOLWUT | ❌ | Easter egg command |

**Coverage**: 19/23 (83%)

---

## Set Commands (17/17)

| Command | Status | Notes |
|---------|--------|-------|
| SADD | ✅ | |
| SCARD | ✅ | |
| SDIFF | ✅ | |
| SDIFFSTORE | ✅ | |
| SINTER | ✅ | |
| SINTERCARD | ✅ | Redis 7.0+ |
| SINTERSTORE | ✅ | |
| SISMEMBER | ✅ | |
| SMEMBERS | ✅ | |
| SMISMEMBER | ✅ | |
| SMOVE | ✅ | |
| SPOP | ✅ | |
| SRANDMEMBER | ✅ | |
| SREM | ✅ | |
| SSCAN | ✅ | |
| SUNION | ✅ | |
| SUNIONSTORE | ✅ | |

**Coverage**: 17/17 (100%)

---

## Sorted Set Commands (43/48)

| Command | Status | Notes |
|---------|--------|-------|
| BZMPOP | ✅ | Blocking ZMPOP |
| BZPOPMAX | ✅ | |
| BZPOPMIN | ✅ | |
| ZADD | ✅ | With NX, XX, GT, LT, CH, INCR |
| ZCARD | ✅ | |
| ZCOUNT | ✅ | |
| ZDIFF | ✅ | |
| ZDIFFSTORE | ✅ | |
| ZINCRBY | ✅ | |
| ZINTER | ✅ | |
| ZINTERCARD | ✅ | Redis 7.0+ |
| ZINTERSTORE | ✅ | |
| ZLEXCOUNT | ✅ | |
| ZMPOP | ✅ | Redis 7.0+ |
| ZMSCORE | ✅ | |
| ZPOPMAX | ✅ | |
| ZPOPMIN | ✅ | |
| ZRANDMEMBER | ✅ | |
| ZRANGE | ✅ | With BYSCORE, BYLEX, REV, LIMIT |
| ZRANGEBYLEX | ✅ | |
| ZRANGEBYSCORE | ✅ | |
| ZRANGESTORE | ✅ | |
| ZRANK | ✅ | With WITHSCORE |
| ZREM | ✅ | |
| ZREMRANGEBYLEX | ✅ | |
| ZREMRANGEBYRANK | ✅ | |
| ZREMRANGEBYSCORE | ✅ | |
| ZREVRANGE | ✅ | |
| ZREVRANGEBYLEX | ✅ | |
| ZREVRANGEBYSCORE | ✅ | |
| ZREVRANK | ✅ | With WITHSCORE |
| ZSCAN | ✅ | |
| ZSCORE | ✅ | |
| ZUNION | ✅ | |
| ZUNIONSTORE | ✅ | |
| GEOADD | ✅ | Wrapper around ZADD |
| GEODIST | ✅ | |
| GEOHASH | ✅ | |
| GEOPOS | ✅ | |
| GEORADIUS | ✅ | Deprecated, use GEOSEARCH |
| GEORADIUS_RO | ❌ | |
| GEORADIUSBYMEMBER | ❌ | Deprecated, use GEOSEARCH |
| GEORADIUSBYMEMBER_RO | ❌ | |
| GEOSEARCH | ✅ | Redis 6.2+ |
| GEOSEARCHSTORE | ❌ | |

**Coverage**: 43/48 (90%)

---

## Stream Commands (13/26)

| Command | Status | Notes |
|---------|--------|-------|
| XACK | ✅ | |
| XADD | ✅ | |
| XAUTOCLAIM | ✅ | Redis 6.2+ |
| XCLAIM | ✅ | |
| XDEL | ✅ | |
| XGROUP | ✅ | CREATE, DESTROY, SETID, etc. |
| XINFO | ✅ | CONSUMERS, GROUPS, STREAM |
| XLEN | ✅ | |
| XPENDING | ✅ | |
| XRANGE | ✅ | |
| XREAD | ✅ | |
| XREADGROUP | ✅ | |
| XREVRANGE | ✅ | |
| XSETID | ✅ | |
| XTRIM | ✅ | |

**Coverage**: 13/26 (50%)

---

## HyperLogLog Commands (3/3)

| Command | Status | Notes |
|---------|--------|-------|
| PFADD | ✅ | |
| PFCOUNT | ✅ | |
| PFMERGE | ✅ | |
| PFDEBUG | ❌ | Debug command |
| PFSELFTEST | ❌ | Self-test command |

**Coverage**: 3/3 (100%)

---

## Pub/Sub Commands (6/15)

| Command | Status | Notes |
|---------|--------|-------|
| PSUBSCRIBE | ✅ | Pattern subscribe |
| PUBLISH | ✅ | |
| PUBSUB | ✅ | CHANNELS, NUMSUB, NUMPAT |
| PUNSUBSCRIBE | ✅ | |
| SUBSCRIBE | ✅ | |
| UNSUBSCRIBE | ✅ | |
| SPUBLISH | ❌ | Sharded pub/sub (Redis 7.0+) |
| SSUBSCRIBE | ❌ | Sharded pub/sub (Redis 7.0+) |
| SUNSUBSCRIBE | ❌ | Sharded pub/sub (Redis 7.0+) |
| PUBSUB SHARDCHANNELS | ❌ | Redis 7.0+ |
| PUBSUB SHARDNUMSUB | ❌ | Redis 7.0+ |

**Coverage**: 6/15 (40%)

---

## Transaction Commands (5/5)

| Command | Status | Notes |
|---------|--------|-------|
| DISCARD | ✅ | |
| EXEC | ✅ | |
| MULTI | ✅ | |
| UNWATCH | ✅ | |
| WATCH | ✅ | |

**Coverage**: 5/5 (100%)

---

## Key Commands (25/31)

| Command | Status | Notes |
|---------|--------|-------|
| COPY | ✅ | Redis 6.2+ |
| DEL | ✅ | |
| DUMP | ✅ | |
| EXISTS | ✅ | |
| EXPIRE | ✅ | With NX, XX, GT, LT |
| EXPIREAT | ✅ | |
| EXPIRETIME | ✅ | Redis 7.0+ |
| KEYS | ✅ | |
| MIGRATE | ❌ | Cluster command |
| MOVE | ✅ | Move key to different DB |
| OBJECT | ✅ | REFCOUNT, ENCODING, IDLETIME, FREQ |
| PERSIST | ✅ | |
| PEXPIRE | ✅ | |
| PEXPIREAT | ✅ | |
| PEXPIRETIME | ✅ | Redis 7.0+ |
| PTTL | ✅ | |
| RANDOMKEY | ✅ | |
| RENAME | ✅ | |
| RENAMENX | ✅ | |
| RESTORE | ✅ | |
| SCAN | ✅ | With MATCH, COUNT, TYPE |
| SORT | ✅ | With BY, LIMIT, GET, ASC/DESC, ALPHA |
| SORT_RO | ✅ | Read-only SORT (Redis 7.0+) |
| TOUCH | ✅ | |
| TTL | ✅ | |
| TYPE | ✅ | |
| UNLINK | ✅ | Async delete |
| WAIT | 📝 | Stub implementation |
| WAITAOF | 📝 | Stub implementation |

**Coverage**: 25/31 (81%)

---

## Server Commands (18/80+)

| Command | Status | Notes |
|---------|--------|-------|
| ACL | ✅ | Basic ACL support |
| AUTH | ✅ | |
| BGREWRITEAOF | ✅ | |
| BGSAVE | ✅ | |
| CLIENT | ✅ | SETNAME, GETNAME, ID, LIST, KILL |
| COMMAND | ✅ | COUNT, DOCS, INFO, LIST |
| CONFIG | ✅ | GET, SET, RESETSTAT, REWRITE |
| DBSIZE | ✅ | |
| DEBUG | ✅ | OBJECT, SEGFAULT, etc. |
| FLUSHALL | ✅ | |
| FLUSHDB | ✅ | |
| HELLO | ✅ | RESP3 negotiation |
| INFO | ✅ | All sections |
| LASTSAVE | ✅ | |
| MEMORY | ✅ | STATS, USAGE, DOCTOR, etc. |
| PING | ✅ | |
| QUIT | ✅ | |
| RESET | ✅ | |
| ROLE | ✅ | Replication role |
| SAVE | ✅ | |
| SELECT | ✅ | |
| SHUTDOWN | ✅ | |
| SLOWLOG | ✅ | GET, LEN, RESET |
| SWAPDB | ✅ | |
| TIME | ✅ | |
| ECHO | ✅ | |
| BGSAVE | ✅ | |
| CLUSTER | ❌ | Cluster commands |
| LATENCY | ❌ | Latency monitoring |
| LOLWUT | ❌ | Easter egg |
| MONITOR | ❌ | Debug tool |
| PSYNC | ❌ | Replication |
| READONLY | ❌ | Cluster |
| READWRITE | ❌ | Cluster |
| REPLICAOF | 📝 | Stub implementation |
| REPLCONF | ❌ | Replication |
| RESTORE-ASKING | ❌ | Cluster |
| SLAVEOF | 📝 | Deprecated, use REPLICAOF |
| SYNC | ❌ | Old replication protocol |
| MODULE | ❌ | Module system |
| FAILOVER | ❌ | Sentinel/cluster |
| SENTINEL | ❌ | Sentinel commands |

**Coverage**: 18/80+ (23%)

---

## Scripting Commands (8/16)

| Command | Status | Notes |
|---------|--------|-------|
| EVAL | ✅ | Lua scripting |
| EVALSHA | ✅ | |
| EVAL_RO | ✅ | Read-only variant |
| EVALSHA_RO | ✅ | |
| FCALL | ✅ | Function call (Redis 7.0+) |
| FCALL_RO | ✅ | |
| FUNCTION | ✅ | Function management |
| SCRIPT | ✅ | LOAD, EXISTS, FLUSH, KILL |
| SCRIPT DEBUG | ❌ | Debugging |
| SCRIPT SHOW | ❌ | |

**Coverage**: 8/16 (50%)

---

## Connection Commands (5/8)

| Command | Status | Notes |
|---------|--------|-------|
| AUTH | ✅ | |
| CLIENT | ✅ | Multiple subcommands |
| ECHO | ✅ | |
| HELLO | ✅ | RESP3 handshake |
| PING | ✅ | |
| QUIT | ✅ | |
| RESET | ✅ | |
| SELECT | ✅ | |
| CLIENT CACHING | ❌ | Client-side caching |
| CLIENT GETREDIR | ❌ | |
| CLIENT TRACKING | ❌ | |

**Coverage**: 5/8 (63%)

---

## Cluster Commands (0/20+)

| Command | Status | Notes |
|---------|--------|-------|
| ASKING | ❌ | |
| CLUSTER | ❌ | All subcommands |
| READONLY | ❌ | |
| READWRITE | ❌ | |

**Coverage**: 0/20+ (0%) - Phase 4

---

## Bitmap Commands

All bitmap commands are implemented as part of String commands (see above):
- GETBIT, SETBIT, BITCOUNT, BITPOS, BITOP, BITFIELD

---

## JSON Commands (0/20+)

Redis Stack feature - not implemented (may add as extension)

**Coverage**: 0/20+ (0%)

---

## Search Commands (0/50+)

Redis Stack feature (RediSearch) - not implemented

**Coverage**: 0/50+ (0%)

---

## Time Series Commands (0/30+)

Redis Stack feature - not implemented

**Coverage**: 0/30+ (0%)

---

## Graph Commands (0/30+)

Redis Stack feature (RedisGraph) - not implemented, deprecated in Redis Stack

**Coverage**: 0/30+ (0%)

---

## Bloom Filter Commands (0/20+)

Redis Stack feature - not implemented

**Coverage**: 0/20+ (0%)

---

## Summary by Category

| Category | Implemented | Total | Coverage |
|----------|-------------|-------|----------|
| String | 24 | 30 | 80% |
| Hash | 15 | 15 | 100% |
| List | 19 | 23 | 83% |
| Set | 17 | 17 | 100% |
| Sorted Set | 43 | 48 | 90% |
| Stream | 10 | 26 | 38% |
| HyperLogLog | 3 | 3 | 100% |
| Pub/Sub | 6 | 15 | 40% |
| Transaction | 5 | 5 | 100% |
| Key | 25 | 31 | 81% |
| Server | 18 | 80+ | 23% |
| Scripting | 8 | 16 | 50% |
| Connection | 5 | 8 | 63% |
| Cluster | 0 | 20+ | 0% |
| **TOTAL** | **218** | **~470** | **~46%** |

---

## Implementation Priority

### ✅ Phase 1 - Core Data Structures (COMPLETE)
- String, Hash, List, Set, Sorted Set commands
- Key management
- Basic server commands

### ✅ Phase 2 - Transactions & Persistence (COMPLETE)
- MULTI/EXEC/WATCH
- Pub/Sub
- AOF persistence

### 🚧 Phase 3 - Replication (IN PROGRESS)
- ✅ Basic commands (REPLICAOF, ROLE, WAIT)
- ✅ Replication backlog and state tracking
- ⏳ PSYNC protocol
- ⏳ Follower sync logic

### 📋 Phase 4 - Cluster (PLANNED)
- CLUSTER commands
- Hash slot management
- MOVED/ASK redirects
- Gossip protocol

### 📋 Phase 5 - Advanced Features (PLANNED)
- Streams (finish implementation)
- Client-side caching
- ACL improvements
- Modules (maybe)

---

## Notes

- **Redis Stack commands** (JSON, Search, Time Series, Graph, Bloom) are not planned for core implementation
- **Cluster commands** will be implemented in Phase 4
- **Deprecated commands** are implemented for compatibility but not recommended
- **Stub implementations** return basic responses but need full implementation
- Redis version compatibility target: **Redis 7.2**

---

## Contributing

When adding new commands:
1. Add to appropriate category in this file
2. Implement in `ferris-commands/src/`
3. Register in `ferris-commands/src/registry.rs`
4. Write 10-20 tests (unit + integration)
5. Update `ROADMAP.md` if part of a phase
6. Run `cargo test` and `cargo clippy`
7. Update this document's coverage percentages

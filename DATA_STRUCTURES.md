# ferris-db Data Structures and Algorithms

> **Purpose**: This document provides detailed information about the data structures, algorithms, and implementation details for all commands and features in ferris-db.

---

## Table of Contents

1. [Core Storage Engine](#core-storage-engine)
2. [String Commands](#string-commands)
3. [List Commands](#list-commands)
4. [Set Commands](#set-commands)
5. [Hash Commands](#hash-commands)
6. [Sorted Set Commands](#sorted-set-commands)
7. [Cluster Implementation](#cluster-implementation)
8. [Persistence (AOF)](#persistence-aof)
9. [Replication](#replication)
10. [Pub/Sub](#pubsub)
11. [Transactions](#transactions)

---

## Core Storage Engine

### KeyStore Architecture

**Data Structure**: Sharded `DashMap<Bytes, Entry>` with 256 shards

**Implementation**:
```rust
pub struct KeyStore {
    databases: Vec<Arc<DashMap<Bytes, Entry>>>,
    num_shards: usize, // 256 by default
}
```

**Shard Selection**:
- Uses FNV hash of key to determine shard: `hash(key) % num_shards`
- Each shard is independently lockable for high concurrency

**Time Complexity**:
- GET/SET: O(1) average, O(log n) worst case (DashMap internal)
- DEL: O(1) average
- Concurrent operations: Lock-free reads, fine-grained write locks

**Memory Model**:
- Each `Entry` contains:
  - `value`: `RedisValue` enum (actual data)
  - `expires_at`: `Option<Instant>` for TTL
  - `version`: `u64` for optimistic locking
  - `last_accessed`: `Instant` for LRU/LFU
  - `ref_count`: For copy-on-write optimizations

**Key Benefits**:
- No global locks - operations on different keys never block
- Multi-key operations lock shards in sorted order (deadlock prevention)
- Memory-efficient: shared references for large values

---

## String Commands

### GET / SET

**Data Structure**: `RedisValue::String(Bytes)`

**Algorithm**:
```
GET key:
  1. Calculate shard = hash(key) % 256
  2. Lock shard for read
  3. Check if key exists
  4. If expired, delete and return nil
  5. Return value

SET key value [EX seconds] [PX milliseconds] [NX|XX]:
  1. Calculate shard = hash(key) % 256
  2. Lock shard for write
  3. If NX and key exists: fail
  4. If XX and key doesn't exist: fail
  5. Create Entry with value and TTL
  6. Insert into shard
  7. Propagate to AOF/replication
```

**Time Complexity**:
- GET: O(1)
- SET: O(1)

### INCR / DECR / INCRBY / DECRBY

**Algorithm**:
```
INCR key:
  1. GET current value
  2. Parse as i64 (error if not integer)
  3. Add 1 (check overflow)
  4. SET new value
  5. Return new value
```

**Time Complexity**: O(1)

**Edge Cases**:
- Overflow: Returns error `-ERR increment or decrement would overflow`
- Non-integer: Returns error `-ERR value is not an integer`
- Missing key: Treats as 0

### APPEND

**Algorithm**:
```
APPEND key value:
  1. GET current value (or empty string)
  2. Concatenate with new value
  3. SET concatenated value
  4. Return new length
```

**Time Complexity**: O(n) where n is new string length

**Optimization**: Uses `Bytes` which supports efficient concatenation via copy-on-write

### GETRANGE / SETRANGE

**Algorithm**:
```
GETRANGE key start end:
  1. GET value
  2. Calculate actual indices (handle negative indices)
  3. Slice Bytes [start..=end]
  4. Return slice

SETRANGE key offset value:
  1. GET current value (or create empty)
  2. If offset > len, pad with null bytes
  3. Replace bytes at offset
  4. SET modified value
  5. Return new length
```

**Time Complexity**:
- GETRANGE: O(k) where k is range size
- SETRANGE: O(n) where n is final string length

### MGET / MSET

**Data Structure**: Batch operations over multiple keys

**Algorithm**:
```
MGET key1 key2 ... keyN:
  1. Validate all keys in same slot (cluster mode)
  2. For each key:
     a. GET value
     b. Add to result array (nil if missing)
  3. Return array

MSET key1 value1 key2 value2 ... keyN valueN:
  1. Validate all keys in same slot (cluster mode)
  2. Calculate shards for all keys
  3. Sort shards to prevent deadlock
  4. Lock all shards in order
  5. SET each key-value pair
  6. Unlock shards
  7. Propagate to AOF/replication
```

**Time Complexity**:
- MGET: O(n) where n is number of keys
- MSET: O(n + s log s) where s is number of unique shards

**Multi-Key Locking Strategy**:
1. Collect all shard indices
2. Sort and deduplicate
3. Lock in ascending order (prevents deadlocks)
4. Execute operations
5. Unlock in reverse order

---

## List Commands

### Data Structure

**Implementation**: `RedisValue::List(VecDeque<Bytes>)`

**Why VecDeque**:
- O(1) push/pop at both ends
- Efficient for LPUSH/RPUSH/LPOP/RPOP
- Contiguous memory for better cache locality

### LPUSH / RPUSH / LPOP / RPOP

**Algorithm**:
```
LPUSH key element [element ...]:
  1. GET list (or create empty)
  2. For each element:
     a. Push to front of VecDeque
  3. SET modified list
  4. Return new length

LPOP key [count]:
  1. GET list
  2. Check if empty
  3. Pop count elements from front
  4. If list empty after pop, delete key
  5. Return popped elements
```

**Time Complexity**:
- LPUSH/RPUSH: O(n) where n is number of elements
- LPOP/RPOP: O(n) where n is count (amortized O(1) per element)

### LINDEX / LSET

**Algorithm**:
```
LINDEX key index:
  1. GET list
  2. Convert negative index: if index < 0: index = len + index
  3. Bounds check
  4. Return list[index]

LSET key index element:
  1. GET list
  2. Convert negative index
  3. Bounds check
  4. Set list[index] = element
  5. SET modified list
```

**Time Complexity**: O(n) where n is index (VecDeque indexing)

**Optimization Opportunity**: For large lists with frequent random access, could use Vec instead

### LRANGE

**Algorithm**:
```
LRANGE key start stop:
  1. GET list
  2. Convert negative indices
  3. Clamp to valid range [0, len)
  4. Slice list[start..=stop]
  5. Return slice as array
```

**Time Complexity**: O(s + n) where s is start and n is range size

### LTRIM

**Algorithm**:
```
LTRIM key start stop:
  1. GET list
  2. Convert negative indices
  3. Create new VecDeque with elements [start..=stop]
  4. SET trimmed list (or delete if empty)
```

**Time Complexity**: O(n) where n is list size

**Memory**: Creates new VecDeque - could optimize with drain() for in-place

### LINSERT

**Algorithm**:
```
LINSERT key BEFORE|AFTER pivot element:
  1. GET list
  2. Linear search for pivot
  3. If found:
     a. Insert element at position
     b. SET modified list
     c. Return new length
  4. If not found: return -1
```

**Time Complexity**: O(n) where n is list size

**Implementation Detail**: Uses `VecDeque::insert()` which may require shifting elements

### LPOS

**Algorithm**:
```
LPOS key element [RANK rank] [COUNT num] [MAXLEN len]:
  1. GET list
  2. Iterate through list (up to MAXLEN)
  3. Track matches
  4. Apply RANK (nth occurrence)
  5. Return indices based on COUNT
```

**Time Complexity**: O(n) where n is MAXLEN or list size

### Blocking Operations (BLPOP / BRPOP)

**Data Structure**: `BlockingRegistry` with per-key wait queues

**Algorithm**:
```
BLPOP key1 key2 ... keyN timeout:
  1. Try immediate pop from each key
  2. If any succeeds, return result
  3. Register wait with BlockingRegistry:
     a. Create tokio::sync::Notify for this connection
     b. Add to wait queue for each key
  4. Wait with timeout:
     a. tokio::select! on notify or timeout
  5. On wakeup:
     a. Try pop again
     b. If success, return
     c. If fail, re-register or timeout
```

**Time Complexity**:
- Registration: O(k) where k is number of keys
- Wakeup: O(w) where w is waiters on that key

**Concurrency**:
- Uses `tokio::sync::Notify` for async wakeups
- Fair ordering: FIFO queue per key
- Automatic cleanup on timeout

**Memory Management**:
- Weak references to avoid memory leaks
- Periodic cleanup of stale registrations

---

## Set Commands

### Data Structure

**Implementation**: `RedisValue::Set(HashSet<Bytes>)`

**Why HashSet**:
- O(1) add/remove/contains
- No ordering needed for sets
- Efficient for membership tests

### SADD / SREM

**Algorithm**:
```
SADD key member [member ...]:
  1. GET set (or create empty)
  2. For each member:
     a. Insert into HashSet
     b. Track if new (not already present)
  3. SET modified set
  4. Return count of new members

SREM key member [member ...]:
  1. GET set
  2. For each member:
     a. Remove from HashSet
     b. Track if existed
  3. If set empty, delete key
  4. SET modified set
  5. Return count of removed members
```

**Time Complexity**:
- SADD: O(n) where n is number of members
- SREM: O(n) where n is number of members

### SISMEMBER / SMISMEMBER

**Algorithm**:
```
SISMEMBER key member:
  1. GET set
  2. Check HashSet::contains(member)
  3. Return 1 or 0

SMISMEMBER key member1 ... memberN:
  1. GET set
  2. For each member:
     a. Check contains
     b. Add result to array
  3. Return array of 1/0
```

**Time Complexity**:
- SISMEMBER: O(1)
- SMISMEMBER: O(n) where n is number of members

### SINTER / SUNION / SDIFF

**Algorithm**:
```
SINTER key1 key2 ... keyN:
  1. Validate all keys in same slot (cluster mode)
  2. GET all sets
  3. Start with smallest set as base
  4. For each element in base:
     a. Check if exists in ALL other sets
     b. If yes, add to result
  5. Return result

SUNION key1 key2 ... keyN:
  1. Validate all keys in same slot (cluster mode)
  2. GET all sets
  3. Create result = HashSet::new()
  4. For each set:
     a. Insert all members into result
  5. Return result

SDIFF key1 key2 ... keyN:
  1. Validate all keys in same slot (cluster mode)
  2. GET all sets
  3. Start with first set as base
  4. For each subsequent set:
     a. Remove those members from base
  5. Return result
```

**Time Complexity**:
- SINTER: O(n * m) where n is smallest set size, m is number of sets
- SUNION: O(n) where n is total elements across all sets
- SDIFF: O(n) where n is size of first set

**Optimization**: SINTER starts with smallest set to minimize iterations

### SCARD / SMEMBERS

**Algorithm**:
```
SCARD key:
  1. GET set
  2. Return HashSet::len()

SMEMBERS key:
  1. GET set
  2. Collect all members into array
  3. Return array
```

**Time Complexity**:
- SCARD: O(1)
- SMEMBERS: O(n) where n is set size

### SPOP / SRANDMEMBER

**Algorithm**:
```
SPOP key [count]:
  1. GET set
  2. Randomly select count members:
     a. Use HashSet iterator with random skip
     b. Or collect to Vec and shuffle
  3. Remove selected members
  4. SET modified set (or delete if empty)
  5. Return removed members

SRANDMEMBER key [count]:
  Similar but doesn't remove members
```

**Time Complexity**:
- SPOP: O(n) where n is count
- SRANDMEMBER: O(n) where n is count

**Randomness**: Uses thread-local RNG for performance

### SMOVE

**Algorithm**:
```
SMOVE source destination member:
  1. Validate source and dest in same slot (cluster mode)
  2. Calculate shards for both keys
  3. Lock shards in order
  4. GET source set
  5. Check if member exists
  6. If exists:
     a. Remove from source
     b. GET destination set (or create)
     c. Add to destination
     d. SET both sets
     e. Return 1
  7. If not exists: return 0
```

**Time Complexity**: O(1) average for hash operations, O(log s) for shard locking

---

## Hash Commands

### Data Structure

**Implementation**: `RedisValue::Hash(HashMap<Bytes, Bytes>)`

**Why HashMap**:
- O(1) field access
- Efficient for key-value pairs within a hash
- Good memory locality

### HSET / HGET / HDEL

**Algorithm**:
```
HSET key field value [field value ...]:
  1. GET hash (or create empty)
  2. For each field-value pair:
     a. Insert into HashMap
     b. Track if new (not updating)
  3. SET modified hash
  4. Return count of new fields

HGET key field:
  1. GET hash
  2. Look up HashMap::get(field)
  3. Return value or nil

HDEL key field [field ...]:
  1. GET hash
  2. For each field:
     a. Remove from HashMap
     b. Track if existed
  3. If hash empty, delete key
  4. SET modified hash
  5. Return count of deleted fields
```

**Time Complexity**:
- HSET: O(n) where n is number of fields
- HGET: O(1)
- HDEL: O(n) where n is number of fields

### HINCRBY / HINCRBYFLOAT

**Algorithm**:
```
HINCRBY key field increment:
  1. GET hash (or create empty)
  2. Get current value (or "0")
  3. Parse as i64
  4. Add increment (check overflow)
  5. Store as string
  6. SET modified hash
  7. Return new value

HINCRBYFLOAT: Same but with f64
```

**Time Complexity**: O(1)

**Edge Cases**:
- Non-numeric value: error
- Overflow (HINCRBY): error
- Infinity/NaN (HINCRBYFLOAT): error

### HGETALL / HKEYS / HVALS

**Algorithm**:
```
HGETALL key:
  1. GET hash
  2. Iterate HashMap
  3. Build array: [field1, value1, field2, value2, ...]
  4. Return array

HKEYS key:
  Similar but only returns fields

HVALS key:
  Similar but only returns values
```

**Time Complexity**: O(n) where n is number of fields

**Output Format**: HGETALL returns flat array for RESP efficiency

### HEXISTS / HLEN

**Algorithm**:
```
HEXISTS key field:
  1. GET hash
  2. Return HashMap::contains_key(field)

HLEN key:
  1. GET hash
  2. Return HashMap::len()
```

**Time Complexity**:
- HEXISTS: O(1)
- HLEN: O(1)

### HSCAN

**Algorithm**:
```
HSCAN key cursor [MATCH pattern] [COUNT hint]:
  1. GET hash
  2. Parse cursor (0 = start)
  3. Collect keys into Vec
  4. Iterate from cursor position
  5. Apply MATCH filter if specified
  6. Collect up to COUNT fields
  7. Return new cursor and results
```

**Time Complexity**: O(n) where n is COUNT

**Cursor Implementation**:
- Cursor is just an offset into collected keys
- Not stable across modifications
- Full scan guaranteed if cursor reaches end

**MATCH Pattern**: Uses glob pattern matching (*, ?, [...])

---

## Sorted Set Commands

### Data Structure

**Implementation**: `RedisValue::SortedSet { members: HashMap<Bytes, f64>, scores: BTreeMap<(OrderedFloat<f64>, Bytes), ()> }`

**Dual Index Approach**:
1. **members**: `HashMap<Bytes, f64>` - Fast score lookup by member (O(1))
2. **scores**: `BTreeMap<(OrderedFloat<f64>, Bytes), ()>` - Ordered by score for range queries (O(log n))

**Why This Design**:
- Need both fast member lookup AND ordered range queries
- `OrderedFloat` wrapper makes f64 implement Ord (handles NaN/infinity)
- Tuple key `(score, member)` ensures stable ordering when scores are equal
- Maintaining both indexes on every operation is acceptable cost for query performance

### ZADD

**Algorithm**:
```
ZADD key [NX|XX] [GT|LT] [CH] score member [score member ...]:
  1. GET sorted set (or create empty)
  2. For each score-member pair:
     a. Check flags (NX/XX/GT/LT)
     b. If updating existing:
        - Remove old (score, member) from BTreeMap
        - Update score in HashMap
        - Insert new (score, member) into BTreeMap
     c. If adding new:
        - Insert into HashMap
        - Insert into BTreeMap
     d. Track changes for return value
  3. SET modified sorted set
  4. Return count (based on CH flag)
```

**Time Complexity**: O(n log m) where n is number of elements to add, m is total members

**NX/XX/GT/LT Logic**:
- NX: Only add new members (don't update)
- XX: Only update existing members (don't add new)
- GT: Only update if new score > current score
- LT: Only update if new score < current score

### ZRANGE / ZREVRANGE

**Algorithm**:
```
ZRANGE key start stop [WITHSCORES]:
  1. GET sorted set
  2. Convert negative indices
  3. Iterate BTreeMap from start to stop
  4. Collect members (and scores if WITHSCORES)
  5. Return array
```

**Time Complexity**: O(log n + m) where n is total members, m is range size

**BTreeMap Range Queries**: Very efficient - O(log n) to find start, then O(m) to collect m elements

### ZRANGEBYSCORE / ZREVRANGEBYSCORE

**Algorithm**:
```
ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]:
  1. GET sorted set
  2. Parse min/max (handle -inf, +inf, exclusive)
  3. Use BTreeMap::range((min, ..)..(max, ..))
  4. Apply LIMIT offset and count
  5. Collect results
  6. Return array
```

**Time Complexity**: O(log n + m) where n is total members, m is result size

**Exclusive Ranges**: `-inf`, `(5` means exclusive, `5` or `[5` means inclusive

### ZRANK / ZREVRANK

**Algorithm**:
```
ZRANK key member:
  1. GET sorted set
  2. Look up score in HashMap - O(1)
  3. If not found: return nil
  4. Iterate BTreeMap from start until finding member
  5. Return index (rank)

ZREVRANK: Same but iterate from end
```

**Time Complexity**: O(n) where n is rank

**Optimization Opportunity**: Could add rank cache, but adds memory overhead

### ZREM / ZREMRANGEBYRANK / ZREMRANGEBYSCORE

**Algorithm**:
```
ZREM key member [member ...]:
  1. GET sorted set
  2. For each member:
     a. Look up score in HashMap
     b. Remove from HashMap
     c. Remove (score, member) from BTreeMap
     d. Track if existed
  3. If sorted set empty, delete key
  4. SET modified sorted set
  5. Return count of removed members

ZREMRANGEBYRANK key start stop:
  1. GET sorted set
  2. Collect members in range [start, stop]
  3. Remove each member (HashMap + BTreeMap)
  4. SET modified sorted set
  5. Return count removed

ZREMRANGEBYSCORE: Similar but by score range
```

**Time Complexity**:
- ZREM: O(n log m) where n is number to remove, m is total members
- ZREMRANGEBYRANK: O(r log m) where r is range size
- ZREMRANGEBYSCORE: O(r log m) where r is result size

### ZCARD / ZCOUNT / ZLEXCOUNT

**Algorithm**:
```
ZCARD key:
  1. GET sorted set
  2. Return HashMap::len() - O(1)

ZCOUNT key min max:
  1. GET sorted set
  2. Use BTreeMap::range(min..max)
  3. Count elements
  4. Return count

ZLEXCOUNT key min max:
  Similar but for lexicographic range (when all scores are same)
```

**Time Complexity**:
- ZCARD: O(1)
- ZCOUNT: O(log n + m) where m is count
- ZLEXCOUNT: O(log n + m)

### ZINCRBY

**Algorithm**:
```
ZINCRBY key increment member:
  1. GET sorted set (or create empty)
  2. Look up current score (or 0.0)
  3. Add increment
  4. Remove old (score, member) from BTreeMap
  5. Update HashMap
  6. Insert new (score, member) into BTreeMap
  7. SET modified sorted set
  8. Return new score
```

**Time Complexity**: O(log n) where n is total members

### ZSCORE / ZMSCORE

**Algorithm**:
```
ZSCORE key member:
  1. GET sorted set
  2. Return HashMap::get(member) - O(1)

ZMSCORE key member [member ...]:
  1. GET sorted set
  2. For each member:
     a. Look up in HashMap
     b. Add to result array
  3. Return array
```

**Time Complexity**:
- ZSCORE: O(1)
- ZMSCORE: O(n) where n is number of members

### Set Operations (ZUNION / ZINTER / ZDIFF)

**Algorithm**:
```
ZUNION numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX]:
  1. Validate all keys in same slot (cluster mode)
  2. GET all sorted sets
  3. For each member across all sets:
     a. Collect all scores
     b. Apply weights
     c. Apply aggregate function (SUM/MIN/MAX)
     d. Store in result sorted set
  4. Return result

ZINTER: Similar but only includes members in ALL sets
ZDIFF: Similar but subtracts subsequent sets from first
```

**Time Complexity**: O(n * k + m log m) where n is total members, k is number of sets, m is result size

**AGGREGATE Options**:
- SUM (default): Add weighted scores
- MIN: Take minimum weighted score
- MAX: Take maximum weighted score

---

## Cluster Implementation

### Hash Slot Calculation

**Algorithm**: CRC16 with Redis-compatible lookup table

```
key_hash_slot(key):
  1. Extract hash tag if present: {...}
  2. Calculate CRC16 of key (or tag)
  3. Return CRC16 % 16384
```

**CRC16 Implementation**:
- Uses CCITT polynomial (0x1021)
- Pre-computed 256-entry lookup table
- Matches Redis exactly for compatibility

**Hash Tag Support**:
```
{user123}:profile -> hash("user123")
{user123}:settings -> hash("user123")  // Same slot!
user123:profile -> hash("user123:profile")  // Different slot
```

**Time Complexity**: O(n) where n is key length (or tag length)

### Cross-Slot Validation

**Data Structure**: Multi-key command validation

**Algorithm**:
```
validate_same_slot(ctx, keys):
  1. If cluster mode disabled: return Ok
  2. If 0 or 1 keys: return Ok
  3. Calculate first_slot = key_hash_slot(keys[0])
  4. For each remaining key:
     a. Calculate slot = key_hash_slot(key)
     b. If slot != first_slot: return CROSSSLOT error
  5. Return Ok
```

**Time Complexity**: O(k * n) where k is number of keys, n is average key length

**Commands Using Cross-Slot Validation**:
- String: MGET, MSET, MSETNX
- Key: DEL, EXISTS, TOUCH, UNLINK
- Set: SINTER, SUNION, SDIFF, SMOVE
- Sorted Set: ZUNION, ZINTER, ZDIFF
- All multi-key operations

### Cluster Redirects (MOVED / ASK)

**Data Structure**: `ClusterState` with slot-to-node mapping

**Algorithm**:
```
check_cluster_redirect(ctx, command, args):
  1. If cluster mode disabled: return Ok
  2. If command doesn't access keys: return Ok
  3. If ASKING flag set: return Ok (bypass)
  4. Extract first key from command
  5. Calculate slot = key_hash_slot(key)
  6. Check slot ownership:
     a. Lock cluster state (read)
     b. Look up slot_map[slot]
     c. If owned by this node: return Ok
     d. If owned by other node: return MOVED error
     e. If being migrated: return ASK error
```

**Time Complexity**: O(1) for slot lookup

**MOVED vs ASK**:
- **MOVED**: Permanent redirect (slot fully migrated)
  - Client updates slot map
  - All future commands for that slot go to new node
- **ASK**: Temporary redirect (slot being migrated)
  - Client sends ASKING command
  - Sends original command
  - Does NOT update slot map

**Key Extraction**:
- Most commands: first argument is key
- EVAL/EVALSHA: key at position `2 + numkeys`
- Special handling per command type

---

## Persistence (AOF)

### Data Structure

**Implementation**: Async channel + background writer task

```rust
pub struct AofWriter {
    sender: mpsc::UnboundedSender<AofEntry>,
    writer_task: JoinHandle<()>,
}

pub struct AofEntry {
    command: Vec<RespValue>,
    db: usize,
}
```

### Write Algorithm

**Main Thread** (non-blocking):
```
propagate_to_aof(command):
  1. Create AofEntry { command, db }
  2. Try to send to channel (try_send)
  3. If channel full: silently drop (prioritize latency)
  4. Return immediately
```

**Writer Task** (async background):
```
aof_writer_loop():
  1. Receive batch of entries from channel
  2. Serialize to RESP protocol
  3. Write to buffer
  4. When buffer reaches threshold OR timer:
     a. Flush buffer to file
     b. Call fsync() based on appendfsync:
        - always: fsync after every write
        - everysec: fsync once per second
        - no: let OS handle flushing
```

**Time Complexity**: O(1) for propagation (non-blocking send)

**Fsync Options**:
- `always`: Durability but slow (fsync per write)
- `everysec`: Good balance (fsync every second)
- `no`: Fast but less durable (OS buffering)

### AOF Replay Algorithm

**On Startup**:
```
replay_aof(file):
  1. Open AOF file
  2. Parse RESP commands one by one
  3. For each command:
     a. If SELECT: change database
     b. Execute command directly (bypass auth/propagation)
     c. Update progress counter
  4. Close file
  5. Resume normal operations
```

**Time Complexity**: O(n) where n is number of commands in AOF

**Error Handling**:
- Corrupted entry: Log warning, skip to next
- Partial entry at end: Truncate and continue
- Invalid command: Skip and log

### AOF Rewrite Algorithm

**Background Rewrite**:
```
rewrite_aof():
  1. Create temporary file
  2. For each database:
     a. Snapshot all keys
     b. For each key:
        - Serialize current state as single command
        - Example: Instead of multiple INCRs, write final SET
  3. Atomic rename temp file to AOF
  4. Old AOF is replaced
```

**Time Complexity**: O(n * m) where n is number of keys, m is average serialization time

**Benefits**:
- Compact: Only final state, not history
- Faster startup: Fewer commands to replay
- Reduced disk usage

**Optimization**: Writes synthetic commands:
- String: SET key value
- List: RPUSH key elem1 elem2 ... (batched)
- Set: SADD key member1 member2 ... (batched)
- Hash: HSET key field1 value1 field2 value2 ... (batched)
- Sorted Set: ZADD key score1 member1 ... (batched)

---

## Replication

### Data Structure

```rust
pub struct ReplicationManager {
    state: Arc<RwLock<ReplicationState>>,
    backlog: Arc<RwLock<ReplicationBacklog>>,
    followers: Arc<RwLock<Vec<FollowerInfo>>>,
}

pub struct ReplicationBacklog {
    buffer: VecDeque<BacklogEntry>,
    max_size: usize,
    offset: u64,
}

pub struct BacklogEntry {
    offset: u64,
    command: Vec<RespValue>,
    db: usize,
}
```

### Replication Protocol

**PSYNC (Partial Sync)**:
```
Follower connects:
  1. Send PING
  2. Send REPLCONF listening-port <port>
  3. Send REPLCONF capa eof capa psync2
  4. Send PSYNC <replication_id> <offset>

Leader responds:
  1. If first sync OR offset too old:
     a. Send FULLRESYNC <replication_id> <offset>
     b. Generate RDB snapshot
     c. Send RDB over socket
     d. Switch to streaming mode
  2. If offset in backlog:
     a. Send CONTINUE
     b. Stream commands from offset
```

**Streaming Mode**:
```
Leader (on every write):
  1. Execute command locally
  2. Add to backlog
  3. For each follower:
     a. Send command over socket
     b. Update follower's offset
```

**Time Complexity**: O(f) where f is number of followers per write

### Backlog Management

**Algorithm**:
```
append_command(command, db):
  1. Lock backlog
  2. Create entry with current offset
  3. Push to VecDeque
  4. If size > max_size:
     a. Pop oldest entries
  5. Increment offset
  6. Unlock backlog
```

**Time Complexity**: O(1) amortized (VecDeque operations)

**Circular Buffer**: Fixed-size backlog, old entries dropped when full

**Offset Tracking**:
- Each command has unique offset
- Followers track their replication offset
- Allows partial resync if follower lags

### Consistency Modes

**Async (Default)**:
```
write_command():
  1. Execute command locally
  2. Propagate to followers (non-blocking)
  3. Return immediately to client
```

**Semi-Sync**:
```
write_command():
  1. Execute command locally
  2. Propagate to followers
  3. Wait for min_replicas to ACK
  4. If timeout: fail or continue (configurable)
  5. Return to client
```

**Sync (Strongest)**:
```
write_command():
  1. Execute command locally
  2. Propagate to ALL followers
  3. Wait for ALL to ACK
  4. Return to client
```

**WAIT Command**:
```
WAIT numreplicas timeout:
  1. Get current offset
  2. Send REPLCONF GETACK * to followers
  3. Count how many ACKed >= offset
  4. If >= numreplicas: return count
  5. If timeout: return current count
```

**Time Complexity**: O(f) where f is number of followers

---

## Pub/Sub

### Data Structure

```rust
pub struct PubSubRegistry {
    channels: DashMap<Bytes, Vec<SubscriberId>>,
    patterns: DashMap<Bytes, Vec<SubscriberId>>,
    subscribers: DashMap<SubscriberId, mpsc::UnboundedSender<PubSubMessage>>,
}
```

### Subscribe Algorithm

**SUBSCRIBE**:
```
SUBSCRIBE channel [channel ...]:
  1. For each channel:
     a. Get or create subscriber list
     b. Add this connection's subscriber_id
     c. Send confirmation message
```

**PSUBSCRIBE** (Pattern Subscribe):
```
PSUBSCRIBE pattern [pattern ...]:
  1. For each pattern:
     a. Add to patterns map
     b. Store connection's subscriber_id
     c. Send confirmation message
```

**Time Complexity**:
- SUBSCRIBE: O(n) where n is number of channels
- PSUBSCRIBE: O(n) where n is number of patterns

### Publish Algorithm

**PUBLISH**:
```
PUBLISH channel message:
  1. Look up exact channel subscribers
  2. For each subscriber:
     a. Send message to their channel
  3. Look up all patterns
  4. For each pattern:
     a. Check if pattern matches channel
     b. If match, send message to pattern subscribers
  5. Return total subscriber count
```

**Time Complexity**: O(s + p * m) where s is channel subscribers, p is patterns, m is match cost

**Pattern Matching**: Glob-style (* and ?)

### Unsubscribe Algorithm

**UNSUBSCRIBE**:
```
UNSUBSCRIBE [channel ...]:
  1. If no channels: unsubscribe from all
  2. For each channel:
     a. Remove subscriber_id from channel list
     b. If list empty, remove channel entry
     c. Send confirmation message
```

**Time Complexity**: O(n) where n is number of channels

### Message Delivery

**Async Non-Blocking**:
- Uses `mpsc::unbounded_channel` per subscriber
- Publisher never blocks waiting for subscriber
- If subscriber's channel full, message dropped

**Connection Handling**:
```
Connection loop:
  1. tokio::select! {
       command = read_command() => handle command
       message = pubsub_rx.recv() => send to client
       shutdown = shutdown_signal => cleanup
     }
```

**Time Complexity**: O(1) per message to each subscriber

---

## Transactions

### Data Structure

```rust
pub struct TransactionState {
    in_transaction: bool,
    queued_commands: Vec<QueuedCommand>,
    watched_keys: HashMap<Bytes, u64>, // key -> version
}

pub struct QueuedCommand {
    name: String,
    args: Vec<RespValue>,
}
```

### MULTI / EXEC / DISCARD

**MULTI**:
```
MULTI:
  1. Set in_transaction = true
  2. Clear queued_commands
  3. Return OK
```

**Command Queueing**:
```
Any command after MULTI:
  1. Parse command
  2. Add to queued_commands
  3. Return "QUEUED"
```

**EXEC**:
```
EXEC:
  1. Check WATCH keys (version check)
  2. If any watched key modified:
     a. Clear transaction
     b. Return nil
  3. Execute each queued command in order:
     a. Don't propagate to AOF yet
     b. Collect results
  4. If all succeed:
     a. Propagate as atomic batch to AOF
     b. Return results
  5. If any fails:
     a. Abort (implementation-specific)
     b. Return partial results
```

**DISCARD**:
```
DISCARD:
  1. Clear queued_commands
  2. Clear watched_keys
  3. Set in_transaction = false
  4. Return OK
```

**Time Complexity**:
- MULTI: O(1)
- Queue: O(1) per command
- EXEC: O(n) where n is number of queued commands
- DISCARD: O(1)

### WATCH

**Algorithm**:
```
WATCH key [key ...]:
  1. For each key:
     a. Get current Entry
     b. Record key -> version in watched_keys
  2. Return OK

On key modification:
  1. Increment Entry.version
  2. Transaction will fail at EXEC if version mismatch
```

**Optimistic Locking**:
- No locks held during transaction
- Version check at EXEC time
- Retryable on conflict

**Time Complexity**: O(n) where n is number of keys

### Transaction Semantics

**Atomicity**:
- All commands execute or none do (EXEC-level)
- Commands execute in order
- No other commands interleaved

**Isolation**:
- Other clients see intermediate states
- WATCH provides optimistic concurrency control

**No Rollback**:
- If command fails mid-transaction, partial results returned
- This matches Redis behavior

---

## Performance Characteristics Summary

| Operation | Data Structure | Time Complexity | Space Complexity |
|-----------|---------------|-----------------|------------------|
| GET/SET | DashMap | O(1) avg | O(1) |
| LPUSH/RPUSH | VecDeque | O(1) amortized | O(1) |
| LINDEX | VecDeque | O(n) | O(1) |
| SADD/SREM | HashSet | O(1) avg | O(1) |
| SINTER | HashSet | O(n * m) | O(min(sets)) |
| HSET/HGET | HashMap | O(1) avg | O(1) |
| ZADD | HashMap + BTreeMap | O(log n) | O(1) |
| ZRANGE | BTreeMap | O(log n + m) | O(m) |
| ZRANK | BTreeMap | O(n) | O(1) |
| PUBLISH | DashMap | O(s + p * m) | O(1) |
| EXEC | Vec | O(n) | O(n) |

**Legend**:
- n: Number of elements
- m: Range/result size
- s: Number of subscribers
- p: Number of patterns
- k: Number of keys

---

## Memory Management

### Copy-on-Write (COW)

**Bytes Type**: Uses `bytes::Bytes` which supports zero-copy slicing and cloning

**Benefits**:
- Cloning a `Bytes` is O(1) and doesn't copy data
- Slicing is O(1) and shares underlying buffer
- Only copies when mutation needed

**Example**:
```rust
let original = Bytes::from("hello world");
let slice1 = original.slice(0..5);  // "hello" - no copy
let slice2 = original.slice(6..11); // "world" - no copy
```

### Entry Version Tracking

**Purpose**: Optimistic locking for WATCH

**Implementation**:
```rust
pub struct Entry {
    value: RedisValue,
    expires_at: Option<Instant>,
    version: AtomicU64,  // Incremented on every write
    // ...
}
```

**Usage**:
- WATCH records version at start
- EXEC checks if version changed
- If changed: transaction aborts

### Reference Counting

**Large Values**: Could implement RC for values > threshold

**Current**: Direct ownership in Entry

**Future Optimization**: Arc<RedisValue> for large values shared across operations

---

## Benchmarking Notes

### Expected Performance

Based on architecture:

1. **Single-threaded ops**: ~1M ops/sec (GET/SET)
2. **Multi-threaded**: Scales linearly with cores (DashMap)
3. **List ops**: ~500K ops/sec (VecDeque overhead)
4. **Sorted set ops**: ~200K ops/sec (BTreeMap)
5. **Pub/Sub**: ~1M messages/sec (async channels)

### Bottlenecks

1. **Sorted Set**: BTreeMap insert O(log n) - could use skip list
2. **LINDEX/LSET**: VecDeque indexing O(n) - could use chunked list
3. **ZRANK**: Linear scan - could add rank cache
4. **Pattern matching**: O(p) patterns checked - could use trie

### Optimization Opportunities

1. **Hot keys**: Cache frequently accessed keys
2. **Rank cache**: Store rank in sorted set nodes
3. **Chunked lists**: Better indexing for large lists
4. **Bloom filters**: Fast negative lookups
5. **Batch operations**: Amortize locking overhead

---

## Next Steps

This document will be continuously updated as new features are added. Key areas for expansion:

1. **Scripting (Lua)**: When implemented
2. **Streams**: When implemented
3. **Geospatial**: When implemented
4. **HyperLogLog**: Detailed algorithm explanation
5. **Bitmap**: Bitfield operations
6. **CRDTs**: Conflict-free replicated data types (Phase 6)

---

**Document Version**: 1.0  
**Last Updated**: 2026-02-26  
**Author**: ferris-db team

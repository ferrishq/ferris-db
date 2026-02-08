# ferris-db Technical Design Document

> **Version**: 1.0  
> **Last Updated**: 2026-02-06

---

## Table of Contents

1. [Overview](#overview)
2. [Requirements](#requirements)
3. [Project Structure](#project-structure)
4. [Concurrency Model](#concurrency-model)
5. [Data Model](#data-model)
6. [RESP Protocol](#resp-protocol)
7. [Networking Layer](#networking-layer)
8. [Persistence (AOF)](#persistence-aof)
9. [Replication](#replication)
10. [Cluster](#cluster)
11. [CRDTs](#crdts)
12. [Distributed Locks](#distributed-locks)
13. [Distributed Queues](#distributed-queues)
14. [Testing Architecture](#testing-architecture)
15. [Cross-Platform Strategy](#cross-platform-strategy)
16. [Performance Targets](#performance-targets)
17. [Dependencies](#dependencies)
18. [Design Principles](#design-principles)

---

## Overview

ferris-db is a high-performance, multi-threaded, Redis-compatible distributed key-value store written in Rust. It aims to be a drop-in replacement for Redis with enhanced features for distributed systems.

### Key Differentiators from Redis

| Feature | Redis | ferris-db |
|---------|-------|-----------|
| Threading | Single-threaded (mostly) | Fully multi-threaded |
| Lock granularity | Global | Per-key |
| Distributed locks | Client-side (Redlock) | Native server-side |
| Distributed queues | DIY with lists | Native with at-least-once delivery |
| Active/Active | Redis Enterprise only | Built-in with CRDTs |
| Consistency | Async only | Configurable (async/semi-sync/sync) |

---

## Requirements

1. **Cross-platform**: Linux, macOS, Windows
2. **Multi-threaded**: Key-level synchronization only
3. **High performance**: >1M ops/sec, <1ms p99 latency
4. **AOF persistence**: Async disk writes, same stream for replication
5. **Cluster support**: Redis Cluster compatible (16384 hash slots)
6. **Replication**: Configurable consistency (async/semi-sync/sync)
7. **Active/Active**: CRDT-based conflict resolution
8. **Distributed locks**: Native, with fencing tokens
9. **Distributed queues**: Orkes Queues inspired, at-least-once delivery
10. **Strong consistency**: At node level, configurable across nodes

---

## Project Structure

```
ferris-db/
├── Cargo.toml                    # Workspace root
│
├── ferris-server/                # Main server binary
│   ├── Cargo.toml
│   └── src/
│       ├── main.rs               # Entry point, CLI parsing
│       ├── config.rs             # Configuration loading (file + env + CLI)
│       └── server.rs             # Server bootstrap and lifecycle
│
├── ferris-core/                  # Core data structures & storage engine
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs
│       ├── store/
│       │   ├── mod.rs
│       │   ├── keystore.rs       # Sharded DashMap storage
│       │   ├── entry.rs          # Entry struct (value + metadata)
│       │   └── database.rs       # Database (SELECT N) abstraction
│       ├── value/
│       │   ├── mod.rs
│       │   ├── redis_value.rs    # RedisValue enum
│       │   ├── string.rs         # String operations
│       │   ├── list.rs           # List operations (VecDeque)
│       │   ├── set.rs            # Set operations (HashSet)
│       │   ├── hash.rs           # Hash operations (HashMap)
│       │   └── sorted_set.rs     # Sorted set (HashMap + BTreeMap)
│       ├── expiry/
│       │   ├── mod.rs
│       │   ├── manager.rs        # TTL manager with background task
│       │   └── wheel.rs          # Timing wheel for efficient expiry
│       └── memory/
│           ├── mod.rs
│           ├── tracker.rs        # Memory usage tracking
│           └── eviction.rs       # Eviction policies (LRU, LFU, etc.)
│
├── ferris-protocol/              # RESP2 + RESP3 codec
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs
│       ├── types.rs              # RESP value types
│       ├── resp2/
│       │   ├── mod.rs
│       │   ├── parser.rs         # RESP2 parser
│       │   └── serializer.rs     # RESP2 serializer
│       ├── resp3/
│       │   ├── mod.rs
│       │   ├── parser.rs         # RESP3 parser
│       │   └── serializer.rs     # RESP3 serializer
│       ├── codec.rs              # Tokio codec (Decoder/Encoder)
│       └── command.rs            # Command parsing and representation
│
├── ferris-commands/              # Command implementations
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs                # Command registry and dispatch
│       ├── registry.rs           # Command table (name -> handler)
│       ├── context.rs            # Execution context (connection state)
│       ├── error.rs              # Command error types
│       ├── string.rs             # GET, SET, INCR, etc.
│       ├── hash.rs               # HGET, HSET, etc.
│       ├── list.rs               # LPUSH, RPUSH, BLPOP, etc.
│       ├── set.rs                # SADD, SREM, SINTER, etc.
│       ├── sorted_set.rs         # ZADD, ZRANGE, etc.
│       ├── key.rs                # DEL, EXISTS, EXPIRE, etc.
│       ├── server.rs             # PING, INFO, CONFIG, etc.
│       ├── connection.rs         # CLIENT, HELLO, QUIT
│       ├── transaction.rs        # MULTI, EXEC, WATCH
│       ├── pubsub.rs             # SUBSCRIBE, PUBLISH
│       ├── dlock.rs              # DLOCK.* commands
│       └── dqueue.rs             # DQUEUE.* commands
│
├── ferris-network/               # Networking layer
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs
│       ├── listener.rs           # TCP listener
│       ├── connection.rs         # Per-connection handler
│       ├── tls.rs                # TLS configuration
│       └── shutdown.rs           # Graceful shutdown
│
├── ferris-persistence/           # AOF persistence
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs
│       ├── aof/
│       │   ├── mod.rs
│       │   ├── writer.rs         # Async AOF writer
│       │   ├── reader.rs         # AOF replay
│       │   └── rewriter.rs       # Background AOF compaction
│       └── fsync.rs              # Platform-specific fsync
│
├── ferris-replication/           # Replication & cluster
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs
│       ├── leader.rs             # Leader replication logic
│       ├── follower.rs           # Follower sync logic
│       ├── backlog.rs            # Replication backlog (ring buffer)
│       ├── consistency.rs        # Consistency mode handling
│       ├── cluster/
│       │   ├── mod.rs
│       │   ├── topology.rs       # Cluster topology
│       │   ├── slots.rs          # Hash slot management
│       │   ├── gossip.rs         # Gossip protocol
│       │   ├── migration.rs      # Slot migration
│       │   └── failover.rs       # Automatic failover
│       └── commands.rs           # REPLICAOF, CLUSTER, WAIT
│
├── ferris-crdt/                  # Conflict-free replicated data types
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs
│       ├── hlc.rs                # Hybrid Logical Clock
│       ├── counter.rs            # G-Counter, PN-Counter
│       ├── register.rs           # LWW-Register
│       ├── set.rs                # OR-Set
│       ├── map.rs                # LWW-Map
│       ├── sorted_set.rs         # LWW-Element-Set
│       └── merge.rs              # Merge protocol
│
├── ferris-queue/                 # Distributed queue
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs
│       ├── queue.rs              # Queue abstraction
│       ├── message.rs            # QueueMessage struct
│       ├── scheduler.rs          # Delayed/priority scheduling
│       └── dlq.rs                # Dead letter queue
│
├── ferris-lock/                  # Distributed lock
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs
│       ├── lock.rs               # Lock state machine
│       ├── fencing.rs            # Fencing token generation
│       └── waiter.rs             # Lock wait queue
│
└── ferris-test-utils/            # Shared test utilities
    ├── Cargo.toml
    └── src/
        ├── lib.rs
        ├── server.rs             # TestServer (spawn on random port)
        ├── client.rs             # TestClient (simple RESP client)
        ├── compat.rs             # Redis compatibility testing
        └── generators.rs         # PropTest generators
```

---

## Concurrency Model

### Key-Level Locking with Sharded DashMap

The core innovation of ferris-db is moving from Redis's global single-threaded model to per-key synchronization.

```
┌─────────────────────────────────────────────────────────────┐
│                        KeyStore                              │
│                                                             │
│   ┌─────────┐  ┌─────────┐  ┌─────────┐      ┌─────────┐   │
│   │ Shard 0 │  │ Shard 1 │  │ Shard 2 │ .... │Shard N-1│   │
│   │ RwLock  │  │ RwLock  │  │ RwLock  │      │ RwLock  │   │
│   │ HashMap │  │ HashMap │  │ HashMap │      │ HashMap │   │
│   └─────────┘  └─────────┘  └─────────┘      └─────────┘   │
│                                                             │
│   N = num_cpus * 16 (default, tunable)                      │
│   Shard selection: hash(key) % N                            │
└─────────────────────────────────────────────────────────────┘
```

### Concurrency Rules

1. **Single-key operations** (GET, SET, HGET, etc.):
   - Hash the key to determine shard
   - Acquire read or write lock on that shard only
   - All other shards remain unlocked and fully concurrent

2. **Multi-key operations** (MGET, MSET, RENAME, set operations):
   - Collect all keys involved
   - Determine unique shards needed
   - **Sort shards by index** (deterministic order prevents deadlocks)
   - Acquire locks in sorted order
   - Execute operation
   - Release locks in reverse order

3. **MULTI/EXEC transactions**:
   - During MULTI: queue commands, no locks held
   - On EXEC: collect all keys from queued commands + WATCH keys
   - Lock shards in sorted order
   - Execute all commands atomically
   - Release locks

4. **Blocking operations** (BLPOP, BRPOP, BLMOVE):
   - Use `tokio::sync::Notify` per key
   - Waiter registers interest, releases shard lock, awaits notify
   - Writer notifies after pushing to list
   - Waiter re-acquires lock, pops element

### DashMap Implementation

```rust
use dashmap::DashMap;

pub struct KeyStore {
    // DashMap is internally sharded with RwLocks
    databases: Vec<DashMap<Bytes, Entry>>,
}

impl KeyStore {
    pub fn get(&self, db: usize, key: &[u8]) -> Option<Entry> {
        self.databases[db].get(key).map(|r| r.clone())
    }

    pub fn set(&self, db: usize, key: Bytes, value: Entry) {
        self.databases[db].insert(key, value);
    }

    // Multi-key: lock in deterministic order
    pub fn mget(&self, db: usize, keys: &[Bytes]) -> Vec<Option<Entry>> {
        // DashMap handles internal locking per shard
        keys.iter()
            .map(|k| self.databases[db].get(k).map(|r| r.clone()))
            .collect()
    }
}
```

---

## Data Model

### RedisValue Enum

```rust
use bytes::Bytes;
use std::collections::{HashMap, HashSet, VecDeque, BTreeMap};
use ordered_float::OrderedFloat;

#[derive(Clone, Debug)]
pub enum RedisValue {
    /// String or binary data
    String(Bytes),

    /// Doubly-ended queue for list operations
    List(VecDeque<Bytes>),

    /// Unordered set of unique elements
    Set(HashSet<Bytes>),

    /// Field-value map
    Hash(HashMap<Bytes, Bytes>),

    /// Sorted set: member -> score mapping with ordered index
    SortedSet {
        /// Member to score lookup
        scores: HashMap<Bytes, f64>,
        /// (score, member) for range queries - BTreeMap maintains order
        members: BTreeMap<(OrderedFloat<f64>, Bytes), ()>,
    },
}

impl RedisValue {
    pub fn type_name(&self) -> &'static str {
        match self {
            RedisValue::String(_) => "string",
            RedisValue::List(_) => "list",
            RedisValue::Set(_) => "set",
            RedisValue::Hash(_) => "hash",
            RedisValue::SortedSet { .. } => "zset",
        }
    }

    pub fn memory_usage(&self) -> usize {
        // Approximate memory usage for MEMORY USAGE command
        match self {
            RedisValue::String(s) => s.len() + 24, // Bytes overhead
            RedisValue::List(l) => l.iter().map(|e| e.len() + 24).sum::<usize>() + 64,
            RedisValue::Set(s) => s.iter().map(|e| e.len() + 32).sum::<usize>() + 64,
            RedisValue::Hash(h) => h.iter().map(|(k, v)| k.len() + v.len() + 48).sum::<usize>() + 64,
            RedisValue::SortedSet { scores, .. } => {
                scores.iter().map(|(k, _)| k.len() + 8 + 48).sum::<usize>() + 128
            }
        }
    }
}
```

### Entry Struct

```rust
use std::time::Instant;

#[derive(Clone, Debug)]
pub struct Entry {
    /// The actual value
    pub value: RedisValue,

    /// Optional expiration time
    pub expires_at: Option<Instant>,

    /// Monotonic version for optimistic locking (WATCH)
    pub version: u64,

    /// Last access time for LRU eviction
    pub last_access: Instant,

    /// Access frequency counter for LFU eviction (Morris counter)
    pub lfu_counter: u8,

    /// CRDT metadata (only in active/active mode)
    pub crdt_meta: Option<CrdtMetadata>,
}

impl Entry {
    pub fn new(value: RedisValue) -> Self {
        Self {
            value,
            expires_at: None,
            version: 0,
            last_access: Instant::now(),
            lfu_counter: 5, // Initial LFU counter value (like Redis)
            crdt_meta: None,
        }
    }

    pub fn is_expired(&self) -> bool {
        self.expires_at.map_or(false, |exp| Instant::now() >= exp)
    }

    pub fn touch(&mut self) {
        self.last_access = Instant::now();
        // LFU: probabilistic increment (Morris counter)
        if self.lfu_counter < 255 {
            let r: f64 = rand::random();
            let p = 1.0 / (self.lfu_counter as f64 * 10.0 + 1.0);
            if r < p {
                self.lfu_counter += 1;
            }
        }
    }

    pub fn increment_version(&mut self) {
        self.version = self.version.wrapping_add(1);
    }
}
```

---

## RESP Protocol

### RESP2 Types

| Type | Prefix | Example |
|------|--------|---------|
| Simple String | `+` | `+OK\r\n` |
| Error | `-` | `-ERR unknown command\r\n` |
| Integer | `:` | `:1000\r\n` |
| Bulk String | `$` | `$6\r\nfoobar\r\n` |
| Array | `*` | `*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n` |
| Null Bulk | `$-1` | `$-1\r\n` |
| Null Array | `*-1` | `*-1\r\n` |

### RESP3 Additional Types

| Type | Prefix | Example |
|------|--------|---------|
| Map | `%` | `%2\r\n+key1\r\n:1\r\n+key2\r\n:2\r\n` |
| Set | `~` | `~3\r\n+a\r\n+b\r\n+c\r\n` |
| Double | `,` | `,3.14159\r\n` |
| Boolean | `#` | `#t\r\n` or `#f\r\n` |
| Null | `_` | `_\r\n` |
| Big Number | `(` | `(3492890328409238509324850943850943825024385\r\n` |
| Verbatim String | `=` | `=15\r\ntxt:Hello world\r\n` |
| Push | `>` | `>3\r\n+message\r\n+channel\r\n+payload\r\n` |

### Codec Design

```rust
use tokio_util::codec::{Decoder, Encoder};
use bytes::{BytesMut, Bytes};

pub struct RespCodec {
    protocol_version: ProtocolVersion,
}

#[derive(Clone, Copy, PartialEq)]
pub enum ProtocolVersion {
    Resp2,
    Resp3,
}

impl Decoder for RespCodec {
    type Item = RespValue;
    type Error = RespError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self.protocol_version {
            ProtocolVersion::Resp2 => resp2::parse(src),
            ProtocolVersion::Resp3 => resp3::parse(src),
        }
    }
}

impl Encoder<RespValue> for RespCodec {
    type Error = RespError;

    fn encode(&mut self, item: RespValue, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match self.protocol_version {
            ProtocolVersion::Resp2 => resp2::serialize(&item, dst),
            ProtocolVersion::Resp3 => resp3::serialize(&item, dst),
        }
    }
}
```

---

## Networking Layer

### Architecture

```
                    ┌──────────────────────────────┐
                    │         TCP Listener         │
                    │    (bind 0.0.0.0:6380)       │
                    │    + optional TLS listener   │
                    └──────────────┬───────────────┘
                                   │
                    ┌──────────────┴───────────────┐
                    │                              │
         ┌──────────▼─────────┐         ┌──────────▼─────────┐
         │  Connection Task   │         │  Connection Task   │
         │  (Tokio spawn)     │         │  (Tokio spawn)     │
         │                    │         │                    │
         │  ┌──────────────┐  │         │  ┌──────────────┐  │
         │  │ RESP Codec   │  │         │  │ RESP Codec   │  │
         │  │ (framing)    │  │         │  │ (framing)    │  │
         │  └──────┬───────┘  │         │  └──────┬───────┘  │
         │         │          │         │         │          │
         │  ┌──────▼───────┐  │         │  ┌──────▼───────┐  │
         │  │ Command      │  │         │  │ Command      │  │
         │  │ Dispatcher   │  │         │  │ Dispatcher   │  │
         │  └──────┬───────┘  │         │  └──────┬───────┘  │
         └─────────┼──────────┘         └─────────┼──────────┘
                   │                              │
                   ▼                              ▼
         ┌────────────────────────────────────────────────────┐
         │              Shared KeyStore (Arc)                  │
         │             (DashMap, concurrent access)            │
         └────────────────────────────────────────────────────┘
                                   │
                   ┌───────────────┴───────────────┐
                   ▼                               ▼
         ┌─────────────────┐             ┌─────────────────┐
         │   AOF Channel   │             │  Pub/Sub Hub    │
         │ (tokio mpsc)    │             │  (broadcast)    │
         └─────────────────┘             └─────────────────┘
```

### Connection Lifecycle

```rust
pub async fn handle_connection(
    stream: TcpStream,
    shared: Arc<SharedState>,
) -> Result<(), ConnectionError> {
    let (reader, writer) = stream.into_split();
    let mut reader = FramedRead::new(reader, RespCodec::new());
    let mut writer = FramedWrite::new(writer, RespCodec::new());

    let mut conn_state = ConnectionState::new();

    loop {
        tokio::select! {
            // Read commands from client
            Some(result) = reader.next() => {
                let command = result?;
                let response = execute_command(&command, &shared, &mut conn_state).await?;
                writer.send(response).await?;
            }

            // Handle pub/sub messages (if subscribed)
            Some(msg) = conn_state.pubsub_rx.recv(), if conn_state.is_subscribed() => {
                writer.send(msg).await?;
            }

            // Graceful shutdown signal
            _ = shared.shutdown.notified() => {
                break;
            }
        }
    }

    Ok(())
}
```

---

## Persistence (AOF)

### Unified Command Stream

The same command stream feeds both AOF persistence and replication:

```
   Client Write
        │
        ▼
   ┌─────────────┐
   │  Execute    │ ──────────► Response to Client
   │  Command    │
   └──────┬──────┘
          │
          ▼ (send to channel)
   ┌─────────────────────────────────────────┐
   │         AOF/Replication Channel          │
   │         (tokio mpsc, bounded)            │
   └────────────────┬────────────────────────┘
                    │
          ┌─────────┴─────────┐
          │                   │
          ▼                   ▼
   ┌─────────────┐     ┌─────────────┐
   │ AOF Writer  │     │ Replication │
   │ Task        │     │ Broadcaster │
   └──────┬──────┘     └─────────────┘
          │
          ▼
   ┌─────────────┐
   │  AOF File   │
   └─────────────┘
```

### AOF Format

AOF uses RESP format (human-readable, same as Redis):

```
*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
*2\r\n$3\r\nDEL\r\n$3\r\nfoo\r\n
```

### Fsync Policies

| Mode | Behavior | Durability | Performance |
|------|----------|------------|-------------|
| `always` | fsync after every command | Highest | Lowest |
| `everysec` | fsync once per second | High | Medium |
| `no` | Let OS decide when to flush | Lowest | Highest |

### AOF Rewrite

Background compaction that creates a minimal AOF representing current state:

1. Fork a snapshot of current state (conceptually)
2. Serialize as minimal commands (SET, HSET, ZADD, etc.)
3. Capture new writes during rewrite into a delta buffer
4. When base rewrite is complete, append delta
5. Atomically rename new AOF to replace old

---

## Replication

### Leader/Follower Architecture

```
  ┌─────────────┐                          ┌─────────────┐
  │   Leader    │      Replication         │  Follower   │
  │  (Master)   │      Stream (TCP)        │  (Replica)  │
  │             │ ────────────────────────►│             │
  │             │                          │             │
  │             │      ACK offsets         │             │
  │             │ ◄────────────────────────│             │
  └──────┬──────┘                          └─────────────┘
         │
         ▼
  ┌─────────────┐
  │ Replication │  Circular buffer
  │   Backlog   │  (default 1MB)
  └─────────────┘
```

### Consistency Modes

| Mode | Behavior | Use Case |
|------|----------|----------|
| `async` | Return immediately, replicate in background | Maximum performance |
| `semi-sync` | Wait for N replicas to ACK (configurable) | Balanced |
| `sync` | Wait for ALL replicas to ACK | Maximum durability |

### PSYNC Protocol

1. Follower connects, sends: `PSYNC <replication_id> <offset>`
2. Leader checks if offset is within backlog
3. If yes: `+CONTINUE` and stream delta from offset
4. If no: `+FULLRESYNC <new_id> <offset>` and send full snapshot

---

## Cluster

### Hash Slot Partitioning

```
┌────────────────────────────────────────────────────────────┐
│                   16384 Hash Slots                          │
│                                                            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │   Node A     │  │   Node B     │  │   Node C     │     │
│  │ Slots 0-5460 │  │Slots 5461-   │  │Slots 10923-  │     │
│  │              │  │    10922     │  │    16383     │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
│                                                            │
│  slot = CRC16(key) % 16384                                 │
│                                                            │
│  Hash tags: {user:123}:profile -> slot based on "user:123" │
└────────────────────────────────────────────────────────────┘
```

### Gossip Protocol

- Nodes exchange PING/PONG messages every 100ms (randomized subset)
- Messages contain: node ID, IP, port, flags, slots, epoch
- Failure detection: PFAIL (node perspective) -> FAIL (quorum agrees)

### Client Redirects

```
Client: GET user:123
Server: -MOVED 12539 192.168.1.2:6380

Client: (during migration) GET user:123
Server: -ASK 12539 192.168.1.3:6380
Client: ASKING
Client: GET user:123
```

---

## CRDTs

### Type Mapping

| Redis Type | CRDT Type | Resolution Strategy |
|-----------|-----------|---------------------|
| String (counter) | PN-Counter | Sum increments across nodes |
| String (value) | LWW-Register | Highest HLC timestamp wins |
| Hash | LWW-Map | Per-field LWW |
| Set | OR-Set | Add wins over concurrent remove |
| Sorted Set | LWW-Element-Set | Per-element LWW for scores |

### Hybrid Logical Clock (HLC)

```rust
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct HLC {
    /// Physical time (milliseconds since epoch)
    pub physical: u64,
    /// Logical counter (increments when physical time doesn't advance)
    pub logical: u32,
    /// Node ID (tiebreaker)
    pub node_id: u32,
}

impl HLC {
    pub fn tick(&mut self) {
        let now = current_time_millis();
        if now > self.physical {
            self.physical = now;
            self.logical = 0;
        } else {
            self.logical += 1;
        }
    }

    pub fn merge(&mut self, other: &HLC) {
        let now = current_time_millis();
        if now > self.physical && now > other.physical {
            self.physical = now;
            self.logical = 0;
        } else if self.physical > other.physical {
            self.logical += 1;
        } else if other.physical > self.physical {
            self.physical = other.physical;
            self.logical = other.logical + 1;
        } else {
            self.logical = std::cmp::max(self.logical, other.logical) + 1;
        }
    }
}
```

---

## Distributed Locks

### Commands

```
DLOCK.ACQUIRE <lock_name> <holder_id> <ttl_ms> [WAIT timeout_ms]
  Returns: fencing_token (integer) or nil

DLOCK.RELEASE <lock_name> <holder_id> <fencing_token>
  Returns: OK or error

DLOCK.EXTEND <lock_name> <holder_id> <fencing_token> <additional_ttl_ms>
  Returns: OK or error

DLOCK.INFO <lock_name>
  Returns: holder, fencing_token, remaining_ttl (or nil if not held)
```

### Fencing Tokens

Fencing tokens are monotonically increasing integers that prevent stale lock holders from making writes:

```
Lock acquire #1: fencing_token = 1
Lock expires (holder crashed)
Lock acquire #2: fencing_token = 2

If holder #1 wakes up and tries to write with token=1,
the system can reject it because token=1 < current_token=2
```

---

## Distributed Queues

### Message Model

```rust
pub struct QueueMessage {
    /// Unique message identifier
    pub id: String,

    /// Message payload (opaque bytes)
    pub payload: Bytes,

    /// Priority (0 = highest)
    pub priority: i32,

    /// Delay before message becomes visible (milliseconds)
    pub timeout: u64,

    /// Message expiry time (milliseconds from now, 0 = never)
    pub expiry: u64,

    /// Number of times this message has been delivered
    pub deliver_count: u32,
}
```

### Internal Storage

Each queue uses three data structures:

1. **Pending ZSET**: `ZSET(score=priority_timestamp, member=message_id)`
2. **Unacked ZSET**: `ZSET(score=unack_deadline, member=message_id)`
3. **Message HASH**: `HASH(message_id -> serialized QueueMessage)`

### Commands

```
DQUEUE.CREATE <queue> [UNACK_TIMEOUT ms] [MAX_RETRIES n] [DLQ dlq_name]
DQUEUE.PUSH <queue> <id> <payload> [PRIORITY p] [DELAY ms] [EXPIRY ms]
DQUEUE.POP <queue> <count> [WAIT timeout_ms]
DQUEUE.ACK <queue> <message_id>
DQUEUE.NACK <queue> <message_id>
DQUEUE.SET_UNACK_TIMEOUT <queue> <message_id> <timeout_ms>
DQUEUE.SIZE <queue>
DQUEUE.INFO <queue>
DQUEUE.GET <queue> <message_id>
DQUEUE.REMOVE <queue> <message_id>
DQUEUE.FLUSH <queue>
```

### Delivery Semantics: At-Least-Once

1. `POP` removes from pending, adds to unacked with deadline
2. If `ACK` received before deadline: message consumed
3. If deadline passes: message returns to pending (redelivery)
4. After `MAX_RETRIES`: message moves to dead letter queue

---

## Testing Architecture

### Test Categories

```
tests/
├── unit/                    # Fast, isolated unit tests
│   ├── protocol/            # RESP parsing/serialization
│   ├── store/               # KeyStore operations
│   └── commands/            # Individual command logic
│
├── integration/             # Tests over real TCP
│   ├── commands/            # Each command over network
│   ├── transactions/        # MULTI/EXEC scenarios
│   ├── pubsub/              # Pub/Sub message delivery
│   └── persistence/         # AOF write/replay
│
├── compatibility/           # Redis behavior matching
│   ├── redis_cli/           # redis-cli compatibility
│   └── clients/             # Client library tests
│
├── property/                # Property-based (proptest)
│   ├── sorted_set/          # Score ordering invariants
│   ├── crdt/                # CRDT convergence properties
│   └── protocol/            # RESP roundtrip properties
│
├── stress/                  # Concurrent/load tests
│   ├── concurrent/          # Race condition detection
│   └── benchmark/           # Performance regression
│
└── fuzz/                    # Fuzzing (cargo-fuzz)
    └── resp/                # RESP parser fuzzing
```

### Test Utilities

```rust
// ferris-test-utils/src/server.rs
pub struct TestServer {
    pub port: u16,
    pub addr: SocketAddr,
    shutdown: Sender<()>,
    handle: JoinHandle<()>,
}

impl TestServer {
    /// Spawn a ferris-db server on a random available port
    pub async fn spawn() -> Self { ... }

    /// Connect a test client to this server
    pub async fn client(&self) -> TestClient { ... }

    /// Stop the server
    pub async fn stop(self) { ... }
}

// ferris-test-utils/src/client.rs
pub struct TestClient {
    stream: TcpStream,
    codec: RespCodec,
}

impl TestClient {
    pub async fn cmd(&mut self, args: &[&str]) -> RespValue { ... }
    pub async fn cmd_ok(&mut self, args: &[&str]) { ... }
    pub async fn cmd_err(&mut self, args: &[&str]) -> String { ... }
}
```

### Coverage Requirements

- **Line coverage**: >= 95% (target 100%)
- **Branch coverage**: >= 90%
- **Tool**: `cargo-tarpaulin` or `cargo-llvm-cov`
- **CI enforcement**: PRs blocked if coverage drops

---

## Cross-Platform Strategy

| Concern | Solution |
|---------|----------|
| Async I/O | Tokio (epoll/kqueue/IOCP) |
| TLS | `tokio-rustls` (pure Rust, no OpenSSL) |
| File I/O | `tokio::fs` + `std::fs` |
| fsync | Conditional: `fdatasync` (Linux), `fcntl F_FULLFSYNC` (macOS), `FlushFileBuffers` (Windows) |
| Time | `std::time::Instant` (monotonic) |
| Signals | `tokio::signal` (Unix signals, Ctrl+C on Windows) |

---

## Performance Targets

| Metric | Target | Strategy |
|--------|--------|----------|
| Throughput | >1M ops/sec | Multi-threaded, zero-copy |
| Latency (p99) | <1ms | Key-level locking |
| Memory overhead | <50 bytes/key | Compact structs |
| Connections | >10K concurrent | Tokio async tasks |
| AOF impact | <5% throughput | Async buffered writes |

---

## Dependencies

```toml
[workspace.dependencies]
# Async runtime
tokio = { version = "1", features = ["full"] }
tokio-util = { version = "0.7", features = ["codec"] }
tokio-rustls = "0.26"

# Data structures
bytes = "1"
dashmap = "6"
parking_lot = "0.12"
crossbeam = "0.8"
ordered-float = "4"

# Hashing
crc16 = "0.4"

# Serialization
serde = { version = "1", features = ["derive"] }
serde_json = "1"

# Logging
tracing = "0.1"
tracing-subscriber = "0.3"

# CLI
clap = { version = "4", features = ["derive"] }

# Error handling
thiserror = "2"

# Utilities
rand = "0.8"
uuid = { version = "1", features = ["v4"] }
glob-match = "0.2"

# Testing
proptest = "1"
criterion = "0.5"
```

---

## Design Principles

1. **Compatibility First**: Drop-in replacement for Redis clients
2. **Concurrency by Design**: Key-level locking, no global contention
3. **Unified Command Stream**: AOF and replication share the same channel
4. **Configurable Consistency**: From eventual to strong, user's choice
5. **CRDTs are Opt-in**: Zero overhead in single-master mode
6. **Native Distributed Primitives**: Locks and queues are first-class
7. **Cross-Platform Always**: Pure Rust dependencies, no platform-specific code
8. **Test Everything**: TDD, 100% coverage target, no exceptions

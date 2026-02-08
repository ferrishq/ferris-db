<p align="center">
  <img src="https://raw.githubusercontent.com/rust-lang/rust-artwork/master/logo/rust-logo-512x512.png" width="120" alt="Ferris">
</p>

<h1 align="center">ferris-db</h1>

<p align="center">
  <strong>A blazingly fast, Redis-compatible database written in Rust</strong>
</p>

<p align="center">
  <a href="#features">Features</a> •
  <a href="#quick-start">Quick Start</a> •
  <a href="#why-ferris-db">Why ferris-db?</a> •
  <a href="#documentation">Docs</a> •
  <a href="#contributing">Contributing</a>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/commands-191-blue?style=flat-square" alt="Commands">
  <img src="https://img.shields.io/badge/tests-2315%2B%20passing-brightgreen?style=flat-square" alt="Tests">
  <img src="https://img.shields.io/badge/Redis%20compatible-83%25-orange?style=flat-square" alt="Compatibility">
  <img src="https://img.shields.io/badge/license-Apache%202.0%20%2F%20MIT-blue?style=flat-square" alt="License">
</p>

---

## Why ferris-db?

**ferris-db** brings the power of Redis to the Rust ecosystem with a focus on performance, safety, and modern architecture.

| Feature | Redis | ferris-db |
|---------|-------|-----------|
| Language | C | Rust (memory-safe) |
| Threading | Single-threaded* | Multi-threaded with key-level locking |
| Protocol | RESP2/RESP3 | RESP2/RESP3 |
| Persistence | RDB + AOF | AOF with rewriting |
| Memory Safety | Manual | Guaranteed (no `unsafe`) |

*Redis 6+ has I/O threads but command execution remains single-threaded.

### Key Differentiators

- **True Multi-threading**: Concurrent command execution with sharded storage (DashMap)
- **Zero `unsafe` Code**: Memory safety guaranteed by the Rust compiler
- **Drop-in Replacement**: Works with `redis-cli`, redis-py, Jedis, and all standard clients
- **Modern Codebase**: Clean, well-documented Rust with 2,315+ tests

---

## Features

### Data Structures
- **Strings** - GET, SET, INCR, APPEND, BITFIELD, and 25+ more
- **Lists** - LPUSH, RPOP, LRANGE, blocking operations (BLPOP, BRPOP)
- **Hashes** - HSET, HGET, HINCRBY, HSCAN, and all hash commands
- **Sets** - SADD, SINTER, SUNION, SDIFF, SMEMBERS, and more
- **Sorted Sets** - ZADD, ZRANGE, ZRANK, ZINCRBY, with full scoring support
- **HyperLogLog** - PFADD, PFCOUNT, PFMERGE for cardinality estimation
- **Geo** - GEOADD, GEODIST, GEORADIUS for location-based queries

### Advanced Features
- **Transactions** - MULTI/EXEC with WATCH for optimistic locking
- **Pub/Sub** - Real-time messaging with pattern subscriptions
- **Persistence** - AOF with background rewriting
- **Expiration** - Millisecond-precision TTL with lazy + active expiry
- **Eviction** - LRU, LFU, Random, and TTL-based policies
- **Blocking Ops** - BLPOP, BRPOP, BLMOVE, BZPOPMIN, BZPOPMAX

---

## Quick Start

### Installation

```bash
git clone https://github.com/anomalyco/ferris-db.git
cd ferris-db
cargo build --release
```

### Run the Server

```bash
# Start on port 6380 (default, avoids conflict with Redis)
./target/release/ferris-db

# Or with custom options
./target/release/ferris-db --port 6380 --aof true
```

### Connect & Use

```bash
$ redis-cli -p 6380

127.0.0.1:6380> SET hello "world"
OK
127.0.0.1:6380> GET hello
"world"
127.0.0.1:6380> INCR counter
(integer) 1
127.0.0.1:6380> ZADD leaderboard 100 "alice" 200 "bob"
(integer) 2
127.0.0.1:6380> ZRANGE leaderboard 0 -1 WITHSCORES
1) "alice"
2) "100"
3) "bob"
4) "200"
```

### Use with Your Favorite Client

```python
# Python
import redis
r = redis.Redis(host='localhost', port=6380)
r.set('foo', 'bar')
print(r.get('foo'))  # b'bar'
```

```javascript
// Node.js
const Redis = require('ioredis');
const redis = new Redis(6380);
await redis.set('foo', 'bar');
console.log(await redis.get('foo'));  // 'bar'
```

```java
// Java (Jedis)
Jedis jedis = new Jedis("localhost", 6380);
jedis.set("foo", "bar");
System.out.println(jedis.get("foo"));  // "bar"
```

---

## Command Coverage

**191 commands implemented** (83% Redis OSS compatibility)

| Category | Commands | Coverage |
|----------|----------|----------|
| String | 28 | 97% |
| List | 17 | 94% |
| Hash | 16 | **100%** |
| Set | 18 | 95% |
| Sorted Set | 35 | 95% |
| Key | 26 | 96% |
| Server | 25 | 83% |
| Transactions | 5 | **100%** |
| Pub/Sub | 6 | 60% |
| HyperLogLog | 3 | **100%** |
| Geo | 6 | **100%** |

See the full [command list](ROADMAP_UPDATED.md) for details.

---

## Performance

Benchmarked with `redis-benchmark` on Apple M1:

```
SET:     ~89,000 ops/sec
GET:     ~93,000 ops/sec
INCR:    ~83,000 ops/sec
LPUSH:   ~85,000 ops/sec
SADD:    ~97,000 ops/sec
ZADD:    ~93,000 ops/sec
```

*Performance varies based on hardware, data size, and workload.*

---

## Documentation

| Document | Description |
|----------|-------------|
| [ROADMAP_UPDATED.md](ROADMAP_UPDATED.md) | Implementation status and future plans |
| [DESIGN.md](DESIGN.md) | Architecture and technical decisions |
| [TODO.md](TODO.md) | Detailed task list for contributors |
| [AGENTS.md](AGENTS.md) | Development guidelines and conventions |
| [STATUS.md](STATUS.md) | Current status and quick reference |

---

## Project Structure

```
ferris-db/
├── ferris-server/      # Server binary and CLI
├── ferris-core/        # Storage engine, data structures
├── ferris-protocol/    # RESP2/RESP3 codec
├── ferris-commands/    # 191 command implementations
├── ferris-network/     # TCP/TLS networking
├── ferris-persistence/ # AOF persistence
└── ferris-test-utils/  # Test helpers
```

---

## Contributing

We welcome contributions! ferris-db is built with love by the community.

### How to Contribute

1. **Star this repo** - Show your support!
2. **Fork & Clone** - `git clone https://github.com/YOUR_USERNAME/ferris-db.git`
3. **Pick an issue** - Check [TODO.md](TODO.md) for tasks
4. **Write tests first** - We practice TDD (see [AGENTS.md](AGENTS.md))
5. **Submit a PR** - We review promptly!

### Good First Issues

- Add missing commands (SINTERCARD, ZMPOP, BZMPOP)
- Write additional tests for edge cases
- Improve documentation
- Set up benchmarking suite

### Larger Projects

- **Streams** (25 commands) - Event sourcing and messaging
- **Lua Scripting** (6 commands) - Complex atomic operations
- **Cluster Mode** - Horizontal scaling

See [TODO.md](TODO.md) for detailed implementation guides.

---

## Running Tests

```bash
# All tests
cargo test --workspace

# Specific module
cargo test --package ferris-commands

# With coverage (requires cargo-tarpaulin)
cargo tarpaulin --workspace --out Html
```

**Current status**: 2,315+ tests passing with 95%+ coverage.

---

## Roadmap

### Phase 2 (Current - 83% Complete)
- [x] Core data types (String, List, Hash, Set, Sorted Set)
- [x] Transactions (MULTI/EXEC/WATCH)
- [x] Pub/Sub messaging
- [x] HyperLogLog & Geo
- [ ] Streams (25 commands)
- [ ] Lua scripting (6 commands)

### Phase 3 (Future)
- [ ] Cluster mode
- [ ] Replication
- [ ] ACL system

See [ROADMAP_UPDATED.md](ROADMAP_UPDATED.md) for the complete roadmap.

---

## Community

- **Issues**: [GitHub Issues](https://github.com/anomalyco/ferris-db/issues)
- **Discussions**: Coming soon
- **Twitter**: Follow for updates

---

## License

Dual-licensed under [Apache 2.0](LICENSE-APACHE) and [MIT](LICENSE-MIT).

---

<p align="center">
  <strong>If ferris-db helps you, please consider giving it a star!</strong>
  <br>
  <br>
  <a href="https://github.com/anomalyco/ferris-db">
    <img src="https://img.shields.io/github/stars/anomalyco/ferris-db?style=social" alt="GitHub stars">
  </a>
</p>

<p align="center">
  Built with 🦀 by the Rust community
</p>

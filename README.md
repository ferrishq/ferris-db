# ferris-db

A high-performance, multi-threaded, Redis-compatible distributed key-value store written in Rust.

[![Tests](https://img.shields.io/badge/tests-2242%20passing-brightgreen)]()
[![Commands](https://img.shields.io/badge/commands-135%2B-blue)]()
[![License](https://img.shields.io/badge/license-Apache%202.0%20%2F%20MIT-blue)]()

## Features

- **Redis Compatible**: Drop-in replacement for Redis, works with `redis-cli` and standard client libraries
- **Multi-threaded**: Key-level synchronization with sharded storage (no global locks)
- **RESP2 & RESP3**: Full protocol support for both Redis protocol versions
- **Memory Management**: All 8 eviction policies (LRU, LFU, Random, TTL-based)
- **Pipeline Support**: Multiple commands per connection with batched responses
- **Blocking Operations**: BLPOP, BRPOP, BLMOVE, BLMPOP, BZPOPMIN, BZPOPMAX, BZMPOP
- **135+ Commands**: Complete implementations of String, List, Set, Hash, Sorted Set, Key, Server, and Connection commands
- **Comprehensive Testing**: 2,242 tests with 95%+ code coverage
- **Cross-Platform**: Runs on Linux, macOS, and Windows

## Quick Start

### Running the Server

```bash
# Default port is 6380 (to avoid conflict with Redis on 6379)
cargo run --release -- --port 6380

# With authentication
cargo run --release -- --port 6380 --requirepass mysecretpassword

# With custom bind address
cargo run --release -- --bind 0.0.0.0 --port 6380
```

### Connecting with redis-cli

```bash
redis-cli -p 6380
127.0.0.1:6380> PING
PONG
127.0.0.1:6380> SET mykey "Hello, ferris-db!"
OK
127.0.0.1:6380> GET mykey
"Hello, ferris-db!"
```

## Building

```bash
# Build all crates
cargo build --workspace

# Run tests
cargo test --workspace

# Run with optimizations
cargo build --release
```

## Project Structure

```
ferris-db/
├── ferris-server/     # Server binary
├── ferris-core/       # Core data structures and storage
├── ferris-protocol/   # RESP2/RESP3 protocol codec
├── ferris-commands/   # Command implementations
├── ferris-network/    # Networking layer
├── ferris-persistence/# AOF persistence
├── ferris-replication/# Replication & cluster
├── ferris-crdt/       # CRDT implementations
├── ferris-queue/      # Distributed queues
├── ferris-lock/       # Distributed locks
└── ferris-test-utils/ # Test utilities
```

## Documentation

- [DESIGN.md](DESIGN.md) - Technical architecture and design decisions
- [ROADMAP.md](ROADMAP.md) - Implementation progress and milestones
- [AGENTS.md](AGENTS.md) - Development guidelines

## Development

### Prerequisites

- Rust 1.75 or later
- Cargo

### Running Tests

```bash
# Run all tests across the entire workspace
cargo test --workspace

# Run all tests including doc-tests
cargo test --workspace --all-targets
cargo test --workspace --doc

# Run tests for a specific crate
cargo test --package ferris-commands
cargo test --package ferris-core
cargo test --package ferris-protocol

# Run tests for a specific module
cargo test --package ferris-commands -- key::tests
cargo test --package ferris-commands -- string::tests
cargo test --package ferris-commands -- list::tests
cargo test --package ferris-commands -- hash::tests
cargo test --package ferris-commands -- set::tests
cargo test --package ferris-commands -- sorted_set::tests

# Run a specific test by name
cargo test --package ferris-commands -- key::tests::test_dump_basic

# Run tests matching a pattern
cargo test --package ferris-commands -- test_restore

# Show stdout/stderr output from passing tests
cargo test --workspace -- --nocapture

# Run tests with a specific number of threads
cargo test --workspace -- --test-threads=1
```

### Code Coverage

```bash
# Install cargo-tarpaulin (one-time setup)
cargo install cargo-tarpaulin

# Run coverage for entire workspace (HTML report)
cargo tarpaulin --workspace --out Html
# Open tarpaulin-report.html in your browser

# Run coverage with XML output (for CI/Codecov)
cargo tarpaulin --workspace --out Xml --out Html --output-dir coverage

# Run coverage for a specific crate
cargo tarpaulin --package ferris-commands --out Html

# Target: >= 95% line coverage, 100% preferred
```

### Code Quality

```bash
# Format code
cargo fmt --all

# Check formatting without modifying files
cargo fmt --all -- --check

# Run linter
cargo clippy --workspace --all-targets -- -D warnings

# Build documentation and check for warnings
cargo doc --workspace --no-deps
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps

# Check for security vulnerabilities
cargo audit

# Full pre-push check (mirrors CI)
cargo fmt --all -- --check \
  && cargo clippy --workspace --all-targets -- -D warnings \
  && cargo test --workspace --all-targets \
  && cargo test --workspace --doc \
  && cargo doc --workspace --no-deps
```

### Benchmarks

Performance tested with `redis-benchmark` on macOS:

```bash
redis-benchmark -p 6380 -t set,get,lpush,sadd,hset,zadd -n 10000 -q
```

| Command | Requests/sec |
|---------|--------------|
| SET     | ~89,000      |
| GET     | ~93,000      |
| INCR    | ~83,000      |
| LPUSH   | ~14,000      |
| SADD    | ~97,000      |
| HSET    | ~94,000      |
| ZADD    | ~93,000      |

## Contributing

Please read [AGENTS.md](AGENTS.md) for development guidelines and coding conventions.

## License

This project is dual-licensed under MIT and Apache 2.0. See LICENSE-MIT and LICENSE-APACHE for details.

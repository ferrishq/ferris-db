# ferris-db Performance

> **Last Updated**: February 2026  
> **Benchmark Version**: 1.0  
> **Test Environment**: macOS, Apple Silicon, single-threaded client

This document tracks performance benchmarks comparing ferris-db against Redis. All benchmarks use the same test methodology for fair comparison.

## Executive Summary

| Metric | ferris-db vs Redis |
|--------|-------------------|
| **Average Throughput** | **4.4x faster** |
| **Average P50 Latency** | **4.5x lower** |
| **Average P99 Latency** | **3.6x lower** |

ferris-db outperforms Redis across all tested operations, with throughput improvements ranging from 3.5x to 5.5x depending on the operation type.

---

## Benchmark Results

### Basic Operations

| Operation | Redis ops/s | ferris-db ops/s | Speedup | P50 Latency Ratio |
|-----------|-------------|-----------------|---------|-------------------|
| PING | 6,981 | 38,136 | **5.46x** | 5.08x lower |
| SET | 8,252 | 35,301 | **4.28x** | 4.42x lower |
| GET | 8,294 | 38,465 | **4.64x** | 4.75x lower |
| INCR | 8,272 | 37,711 | **4.56x** | 4.75x lower |

### Multi-Key Operations

| Operation | Redis ops/s | ferris-db ops/s | Speedup | P50 Latency Ratio |
|-----------|-------------|-----------------|---------|-------------------|
| MSET (5 keys) | 8,064 | 31,285 | **3.88x** | 3.93x lower |
| MGET (5 keys) | 8,256 | 38,519 | **4.67x** | 4.79x lower |

### List Operations

| Operation | Redis ops/s | ferris-db ops/s | Speedup | P50 Latency Ratio |
|-----------|-------------|-----------------|---------|-------------------|
| LPUSH | 8,270 | 29,136 | **3.52x** | 3.71x lower |
| RPUSH | 8,148 | 29,084 | **3.57x** | 3.59x lower |
| LPOP | 8,408 | 38,724 | **4.61x** | 4.75x lower |
| LRANGE (50 items) | 8,545 | 40,750 | **4.77x** | 4.96x lower |

### Hash Operations

| Operation | Redis ops/s | ferris-db ops/s | Speedup | P50 Latency Ratio |
|-----------|-------------|-----------------|---------|-------------------|
| HSET | 8,250 | 35,358 | **4.29x** | 4.42x lower |
| HGETALL (50 fields) | 8,284 | 38,858 | **4.69x** | 4.96x lower |

### Set Operations

| Operation | Redis ops/s | ferris-db ops/s | Speedup | P50 Latency Ratio |
|-----------|-------------|-----------------|---------|-------------------|
| SADD | 8,237 | 39,020 | **4.74x** | 4.96x lower |
| SMEMBERS (100 members) | 8,317 | 37,303 | **4.49x** | 4.56x lower |

### Sorted Set Operations

| Operation | Redis ops/s | ferris-db ops/s | Speedup | P50 Latency Ratio |
|-----------|-------------|-----------------|---------|-------------------|
| ZADD | 8,277 | 38,376 | **4.64x** | 4.75x lower |
| ZRANGE (50 items) | 8,231 | 37,115 | **4.51x** | 4.75x lower |

### Large Value Operations

| Operation | Redis ops/s | ferris-db ops/s | Speedup | P50 Latency Ratio |
|-----------|-------------|-----------------|---------|-------------------|
| SET (10KB value) | 8,041 | 29,575 | **3.68x** | 3.90x lower |
| GET (10KB value) | 8,307 | 39,092 | **4.71x** | 4.75x lower |

---

## Performance Analysis

### Strengths

1. **Read Operations Excel**: GET, MGET, LRANGE, HGETALL, SMEMBERS, ZRANGE all show 4.5-5x improvement
2. **Consistent Low Latency**: P50 latencies are consistently 4-5x lower across all operations
3. **Bulk Reads**: Operations returning multiple elements (LRANGE, HGETALL, SMEMBERS) are particularly fast

### Areas for Optimization

1. **List Writes (LPUSH/RPUSH)**: Smallest advantage at ~3.5x
   - Under sustained load, P99 latency can occasionally exceed Redis
   - Potential optimization target for future releases

2. **Large Value Writes**: 10KB SET shows 3.68x (vs 4.28x for small values)
   - Memory allocation overhead for large values
   - Still significantly faster than Redis

3. **Multi-Key Writes (MSET)**: 3.88x advantage
   - Slightly less than single-key operations
   - Lock acquisition overhead for multiple keys

### Latency Characteristics

| Percentile | Typical ferris-db | Typical Redis | Ratio |
|------------|-------------------|---------------|-------|
| P50 | 23-31 μs | 114-122 μs | ~4.5x lower |
| P99 | 50-97 μs | 185-280 μs | ~3.5x lower |
| Max | Varies | Varies | Occasional spikes in both |

**Note**: Both Redis and ferris-db can experience occasional latency spikes (up to several milliseconds) under load. ferris-db's max latency is generally comparable to or better than Redis.

---

## Why ferris-db is Faster

1. **Multi-threaded Architecture**: ferris-db uses DashMap-based sharded storage allowing concurrent access to different keys without global locks

2. **Rust's Zero-Cost Abstractions**: No garbage collection pauses, predictable memory layout

3. **Optimized Data Structures**: Purpose-built for Redis-compatible operations

4. **Efficient RESP Protocol Implementation**: Minimal allocation during parsing/serialization

---

## Running Benchmarks

```bash
# Quick benchmark (500 iterations per operation)
./target/release/parity-tests --bench-only --quick

# Default benchmark (1000 iterations)
./target/release/parity-tests --bench-only

# Thorough benchmark (10000 iterations)
./target/release/parity-tests --bench-only --thorough

# Run parity tests + benchmarks
./target/release/parity-tests --benchmark
```

### Benchmark Output

Results are saved to `benchmark-report.json` with detailed metrics:
- Operations per second
- P50, P99 latency
- Min/Max latency
- Comparison ratios

---

## Test Methodology

- **Iterations**: 1000 operations per benchmark (default mode)
- **Warmup**: 100 iterations before measurement
- **Connection**: Single TCP connection per server
- **Value Size**: 100 bytes (default), 10KB (large value tests)
- **Measurement**: Wall-clock time with microsecond precision

### Environment Requirements

- Redis server running on port 6379
- ferris-db server running on port 6380
- Both servers on localhost to minimize network variance

---

## Version History

| Date | Version | Notes |
|------|---------|-------|
| Feb 2026 | 1.0 | Initial benchmark suite with 18 operations |

---

## Future Benchmarks

Planned additions:
- [ ] Concurrent client benchmarks (multiple connections)
- [ ] Pipeline benchmarks
- [ ] Transaction (MULTI/EXEC) benchmarks
- [ ] Pub/Sub latency benchmarks
- [ ] Memory efficiency comparisons
- [ ] Persistence overhead benchmarks (AOF enabled vs disabled)

# Redis Parity Test Results

> **Last Run**: February 2026  
> **Overall Status**: PASSED  
> **Pass Rate**: 100% (166/166 tests)

## Summary

This document tracks the parity between ferris-db and Redis. All commands listed below have been tested to produce identical results to Redis.

## Test Results by Category

### String Commands (35/35 - 100%)

| Command | Tests | Status |
|---------|-------|--------|
| SET | Basic, overwrite, EX, PX, NX, XX, NX+XX conflict | ✅ |
| GET | Basic, nonexistent key, wrong type | ✅ |
| SETNX | New key, existing key | ✅ |
| SETEX | With TTL | ✅ |
| PSETEX | With millisecond TTL | ✅ |
| GETEX | With EX, with PERSIST | ✅ |
| GETDEL | Delete after get | ✅ |
| MSET | Multiple keys | ✅ |
| MGET | All exist, partial exist | ✅ |
| MSETNX | All new, some exist | ✅ |
| INCR | Existing, new key, non-integer error | ✅ |
| INCRBY | Positive, negative increment | ✅ |
| INCRBYFLOAT | Float increment (with tolerance) | ✅ |
| DECR | Basic decrement | ✅ |
| DECRBY | Decrement by value | ✅ |
| APPEND | Existing key, new key | ✅ |
| STRLEN | Existing, nonexistent | ✅ |
| GETRANGE | Positive indices, negative indices | ✅ |
| SETRANGE | Basic, with padding | ✅ |

### List Commands (29/29 - 100%)

| Command | Tests | Status |
|---------|-------|--------|
| LPUSH | Single, multiple elements | ✅ |
| RPUSH | Single, multiple elements | ✅ |
| LPOP | Single, with count, empty list | ✅ |
| RPOP | Single, with count, empty list | ✅ |
| LRANGE | Positive indices, negative indices, out of bounds | ✅ |
| LLEN | Existing, nonexistent | ✅ |
| LINDEX | Positive, negative, out of bounds | ✅ |
| LSET | Basic, out of bounds error | ✅ |
| LREM | Positive count, negative count, zero (all) | ✅ |
| LINSERT | BEFORE, AFTER, pivot not found | ✅ |
| LTRIM | Positive indices, negative indices | ✅ |
| LPOS | Basic, not found, with RANK | ✅ |
| LMOVE | Between lists | ✅ |
| LPUSHX | Nonexistent, existing | ✅ |
| RPUSHX | Nonexistent, existing | ✅ |

### Set Commands (20/20 - 100%)

| Command | Tests | Status |
|---------|-------|--------|
| SADD | Single, multiple, duplicates | ✅ |
| SREM | Existing, nonexistent member | ✅ |
| SMEMBERS | All members (unordered) | ✅ |
| SISMEMBER | Exists, not exists | ✅ |
| SMISMEMBER | Multiple members | ✅ |
| SCARD | Existing, nonexistent | ✅ |
| SINTER | Intersection of sets | ✅ |
| SUNION | Union of sets | ✅ |
| SDIFF | Difference of sets | ✅ |
| SINTERSTORE | Store intersection | ✅ |
| SUNIONSTORE | Store union | ✅ |
| SDIFFSTORE | Store difference | ✅ |
| SPOP | Random pop (count verified) | ✅ |
| SRANDMEMBER | Random member (type verified) | ✅ |
| SMOVE | Move between sets, nonexistent | ✅ |

### Hash Commands (20/20 - 100%)

| Command | Tests | Status |
|---------|-------|--------|
| HSET | Single field, multiple fields | ✅ |
| HGET | Existing, nonexistent field/key | ✅ |
| HMSET | Multiple fields | ✅ |
| HMGET | All exist, some missing | ✅ |
| HGETALL | All fields (length verified) | ✅ |
| HDEL | Single, multiple fields | ✅ |
| HEXISTS | Exists, not exists | ✅ |
| HLEN | Existing, nonexistent | ✅ |
| HKEYS | All keys (unordered) | ✅ |
| HVALS | All values (unordered) | ✅ |
| HINCRBY | Existing, new field | ✅ |
| HINCRBYFLOAT | Float increment (with tolerance) | ✅ |
| HSETNX | New field, existing field | ✅ |
| HSTRLEN | Existing, nonexistent field | ✅ |

### Sorted Set Commands (23/23 - 100%)

| Command | Tests | Status |
|---------|-------|--------|
| ZADD | Single, multiple, NX, XX, GT/LT | ✅ |
| ZREM | Existing, nonexistent | ✅ |
| ZSCORE | Existing, nonexistent | ✅ |
| ZRANK | All positions, nonexistent | ✅ |
| ZREVRANK | Reverse positions | ✅ |
| ZRANGE | Basic, WITHSCORES | ✅ |
| ZREVRANGE | Reverse range | ✅ |
| ZRANGEBYSCORE | Range, exclusive, -inf/+inf | ✅ |
| ZCARD | Existing, nonexistent | ✅ |
| ZCOUNT | Score range | ✅ |
| ZINCRBY | Existing, new member | ✅ |
| ZPOPMIN | Pop minimum | ✅ |
| ZPOPMAX | Pop maximum | ✅ |

### Key Commands (29/29 - 100%)

| Command | Tests | Status |
|---------|-------|--------|
| DEL | Single, multiple, nonexistent | ✅ |
| EXISTS | Single, multiple keys | ✅ |
| EXPIRE | Basic, nonexistent key | ✅ |
| TTL | With expire, no expire, nonexistent | ✅ |
| PEXPIRE | Millisecond expiry | ✅ |
| PTTL | Millisecond TTL | ✅ |
| EXPIREAT | Unix timestamp | ✅ |
| PERSIST | Remove TTL, no TTL | ✅ |
| TYPE | string, list, set, hash, zset, nonexistent | ✅ |
| KEYS | All keys, pattern matching | ✅ |
| RENAME | Basic, overwrite, nonexistent error | ✅ |
| RENAMENX | New name, existing name | ✅ |
| COPY | Basic, with REPLACE | ✅ |
| UNLINK | Async delete | ✅ |
| TOUCH | Update access time | ✅ |

### Server Commands (10/10 - 100%)

| Command | Tests | Status |
|---------|-------|--------|
| PING | No args, with message | ✅ |
| ECHO | Echo message | ✅ |
| SELECT | Valid DB, invalid DB | ✅ |
| DBSIZE | With keys, empty | ✅ |
| FLUSHDB | Clear database | ✅ |
| TIME | Returns array of 2 | ✅ |
| DEBUG SLEEP | Handled gracefully | ✅ |

## Commands Not Yet Tested

The following command categories are implemented in ferris-db but not yet covered by parity tests:

- **HyperLogLog**: PFADD, PFCOUNT, PFMERGE
- **Pub/Sub**: PUBLISH, SUBSCRIBE, PSUBSCRIBE, UNSUBSCRIBE
- **Transactions**: MULTI, EXEC, DISCARD, WATCH, UNWATCH
- **Scripting**: EVAL, EVALSHA, SCRIPT
- **Streams**: XADD, XREAD, XRANGE, XLEN, etc.
- **Geo**: GEOADD, GEODIST, GEOHASH, GEOPOS, etc.
- **Cluster**: CLUSTER commands

## Running the Tests

```bash
# Option 1: Automated script
./scripts/run_parity_tests.sh

# Option 2: Manual
# Terminal 1: Start Redis
redis-server --port 6379

# Terminal 2: Start ferris-db  
cargo run --release -p ferris-server -- --port 6380

# Terminal 3: Run tests
cargo run --release -p parity-tests
```

## Notes

1. **Float Comparisons**: INCRBYFLOAT and HINCRBYFLOAT use numeric tolerance (0.0001) rather than exact string matching due to floating-point representation differences.

2. **Unordered Results**: Commands like SMEMBERS, KEYS, HKEYS, HVALS use unordered comparison since Redis doesn't guarantee order.

3. **Random Operations**: SPOP and SRANDMEMBER verify that results are valid (correct type and count) rather than exact values.

4. **TTL Tolerance**: TTL/PTTL comparisons allow ±2 seconds variance for timing-sensitive operations.

//! Comprehensive end-to-end tests for ferris-db
//!
//! These 20 tests verify that all major features work correctly together
//! over real TCP connections using the full stack.

use ferris_protocol::RespValue;
use ferris_test_utils::TestServer;

// ============================================================
// 1. STRING OPERATIONS
// ============================================================

#[tokio::test]
async fn test_e2e_string_basic_operations() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // SET and GET
    client.cmd_ok(&["SET", "name", "ferris-db"]).await;
    let result = client.cmd(&["GET", "name"]).await;
    assert_eq!(result, RespValue::bulk_string("ferris-db"));

    // INCR
    client.cmd_ok(&["SET", "counter", "10"]).await;
    let result = client.cmd(&["INCR", "counter"]).await;
    assert_eq!(result, RespValue::Integer(11));

    // APPEND
    let result = client.cmd(&["APPEND", "name", "-v1"]).await;
    assert_eq!(result, RespValue::Integer(12)); // "ferris-db-v1" = 9 + 3

    // STRLEN
    let result = client.cmd(&["STRLEN", "name"]).await;
    assert_eq!(result, RespValue::Integer(12));

    // MSET and MGET
    client.cmd_ok(&["MSET", "k1", "v1", "k2", "v2", "k3", "v3"]).await;
    let result = client.cmd(&["MGET", "k1", "k2", "k3"]).await;
    assert_eq!(
        result,
        RespValue::Array(vec![
            RespValue::bulk_string("v1"),
            RespValue::bulk_string("v2"),
            RespValue::bulk_string("v3"),
        ])
    );

    server.stop().await;
}

#[tokio::test]
async fn test_e2e_string_advanced_operations() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // GETSET
    client.cmd_ok(&["SET", "key", "old"]).await;
    let result = client.cmd(&["GETSET", "key", "new"]).await;
    assert_eq!(result, RespValue::bulk_string("old"));
    let result = client.cmd(&["GET", "key"]).await;
    assert_eq!(result, RespValue::bulk_string("new"));

    // GETDEL
    let result = client.cmd(&["GETDEL", "key"]).await;
    assert_eq!(result, RespValue::bulk_string("new"));
    let result = client.cmd(&["GET", "key"]).await;
    assert_eq!(result, RespValue::Null);

    // SETRANGE and GETRANGE
    client.cmd_ok(&["SET", "msg", "Hello World"]).await;
    client.cmd(&["SETRANGE", "msg", "6", "Redis"]).await;
    let result = client.cmd(&["GET", "msg"]).await;
    assert_eq!(result, RespValue::bulk_string("Hello Redis"));

    let result = client.cmd(&["GETRANGE", "msg", "0", "4"]).await;
    assert_eq!(result, RespValue::bulk_string("Hello"));

    server.stop().await;
}

// ============================================================
// 2. LIST OPERATIONS
// ============================================================

#[tokio::test]
async fn test_e2e_list_basic_operations() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // LPUSH and LRANGE
    let result = client.cmd(&["LPUSH", "mylist", "world", "hello"]).await;
    assert_eq!(result, RespValue::Integer(2));

    let result = client.cmd(&["LRANGE", "mylist", "0", "-1"]).await;
    assert_eq!(
        result,
        RespValue::Array(vec![
            RespValue::bulk_string("hello"),
            RespValue::bulk_string("world"),
        ])
    );

    // RPUSH
    let result = client.cmd(&["RPUSH", "mylist", "!"]).await;
    assert_eq!(result, RespValue::Integer(3));

    // LLEN
    let result = client.cmd(&["LLEN", "mylist"]).await;
    assert_eq!(result, RespValue::Integer(3));

    // LPOP and RPOP
    let result = client.cmd(&["LPOP", "mylist"]).await;
    assert_eq!(result, RespValue::bulk_string("hello"));

    let result = client.cmd(&["RPOP", "mylist"]).await;
    assert_eq!(result, RespValue::bulk_string("!"));

    // LINDEX
    let result = client.cmd(&["LINDEX", "mylist", "0"]).await;
    assert_eq!(result, RespValue::bulk_string("world"));

    server.stop().await;
}

#[tokio::test]
async fn test_e2e_list_blocking_operations() {
    let server = TestServer::spawn().await;
    let mut client1 = server.client().await;
    let mut client2 = server.client().await;

    // Client 1 starts blocking on empty list
    let client1_handle = tokio::spawn(async move {
        let result = client1
            .cmd_timeout(&["BLPOP", "queue", "2"], tokio::time::Duration::from_secs(3))
            .await;
        (client1, result)
    });

    // Wait a bit to ensure client1 is blocking
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Client 2 pushes to the list
    client2.cmd(&["LPUSH", "queue", "item1"]).await;

    // Client 1 should receive the item
    let (client1, result) = client1_handle.await.unwrap();
    assert!(result.is_some());
    if let Some(RespValue::Array(arr)) = result {
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0], RespValue::bulk_string("queue"));
        assert_eq!(arr[1], RespValue::bulk_string("item1"));
    } else {
        panic!("Expected array response");
    }

    drop(client1);
    server.stop().await;
}

// ============================================================
// 3. HASH OPERATIONS
// ============================================================

#[tokio::test]
async fn test_e2e_hash_operations() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // HSET and HGET
    let result = client.cmd(&["HSET", "user:1", "name", "Alice"]).await;
    assert_eq!(result, RespValue::Integer(1));

    let result = client.cmd(&["HGET", "user:1", "name"]).await;
    assert_eq!(result, RespValue::bulk_string("Alice"));

    // HMSET and HMGET
    client
        .cmd_ok(&["HMSET", "user:1", "age", "30", "city", "NYC"])
        .await;
    let result = client.cmd(&["HMGET", "user:1", "name", "age", "city"]).await;
    assert_eq!(
        result,
        RespValue::Array(vec![
            RespValue::bulk_string("Alice"),
            RespValue::bulk_string("30"),
            RespValue::bulk_string("NYC"),
        ])
    );

    // HGETALL
    let result = client.cmd(&["HGETALL", "user:1"]).await;
    if let RespValue::Array(arr) = result {
        assert_eq!(arr.len(), 6); // 3 field-value pairs
    } else {
        panic!("Expected array");
    }

    // HINCRBY
    let result = client.cmd(&["HINCRBY", "user:1", "age", "5"]).await;
    assert_eq!(result, RespValue::Integer(35));

    // HDEL
    let result = client.cmd(&["HDEL", "user:1", "city"]).await;
    assert_eq!(result, RespValue::Integer(1));

    // HEXISTS
    let result = client.cmd(&["HEXISTS", "user:1", "city"]).await;
    assert_eq!(result, RespValue::Integer(0));

    // HLEN
    let result = client.cmd(&["HLEN", "user:1"]).await;
    assert_eq!(result, RespValue::Integer(2));

    server.stop().await;
}

// ============================================================
// 4. SET OPERATIONS
// ============================================================

#[tokio::test]
async fn test_e2e_set_operations() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // SADD and SMEMBERS
    let result = client.cmd(&["SADD", "myset", "a", "b", "c"]).await;
    assert_eq!(result, RespValue::Integer(3));

    let result = client.cmd(&["SMEMBERS", "myset"]).await;
    if let RespValue::Array(arr) = result {
        assert_eq!(arr.len(), 3);
    } else {
        panic!("Expected array");
    }

    // SISMEMBER
    let result = client.cmd(&["SISMEMBER", "myset", "b"]).await;
    assert_eq!(result, RespValue::Integer(1));

    let result = client.cmd(&["SISMEMBER", "myset", "z"]).await;
    assert_eq!(result, RespValue::Integer(0));

    // SCARD
    let result = client.cmd(&["SCARD", "myset"]).await;
    assert_eq!(result, RespValue::Integer(3));

    // SREM
    let result = client.cmd(&["SREM", "myset", "b"]).await;
    assert_eq!(result, RespValue::Integer(1));

    // SINTER, SUNION, SDIFF
    client.cmd(&["SADD", "set1", "a", "b", "c"]).await;
    client.cmd(&["SADD", "set2", "b", "c", "d"]).await;

    let result = client.cmd(&["SINTER", "set1", "set2"]).await;
    if let RespValue::Array(arr) = result {
        assert_eq!(arr.len(), 2); // b, c
    } else {
        panic!("Expected array");
    }

    let result = client.cmd(&["SUNION", "set1", "set2"]).await;
    if let RespValue::Array(arr) = result {
        assert_eq!(arr.len(), 4); // a, b, c, d
    } else {
        panic!("Expected array");
    }

    server.stop().await;
}

// ============================================================
// 5. SORTED SET OPERATIONS
// ============================================================

#[tokio::test]
async fn test_e2e_sorted_set_operations() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // ZADD
    let result = client
        .cmd(&["ZADD", "leaderboard", "100", "alice", "200", "bob", "150", "charlie"])
        .await;
    assert_eq!(result, RespValue::Integer(3));

    // ZCARD
    let result = client.cmd(&["ZCARD", "leaderboard"]).await;
    assert_eq!(result, RespValue::Integer(3));

    // ZRANGE with WITHSCORES
    let result = client
        .cmd(&["ZRANGE", "leaderboard", "0", "-1", "WITHSCORES"])
        .await;
    if let RespValue::Array(arr) = result {
        assert_eq!(arr.len(), 6); // 3 members * 2 (member + score)
        assert_eq!(arr[0], RespValue::bulk_string("alice"));
        assert_eq!(arr[1], RespValue::bulk_string("100"));
    } else {
        panic!("Expected array");
    }

    // ZRANK
    let result = client.cmd(&["ZRANK", "leaderboard", "charlie"]).await;
    assert_eq!(result, RespValue::Integer(1)); // 0-indexed

    // ZSCORE
    let result = client.cmd(&["ZSCORE", "leaderboard", "bob"]).await;
    assert_eq!(result, RespValue::bulk_string("200"));

    // ZINCRBY
    let result = client.cmd(&["ZINCRBY", "leaderboard", "50", "alice"]).await;
    assert_eq!(result, RespValue::bulk_string("150"));

    // ZPOPMAX
    let result = client.cmd(&["ZPOPMAX", "leaderboard"]).await;
    if let RespValue::Array(arr) = result {
        assert_eq!(arr[0], RespValue::bulk_string("bob"));
    } else {
        panic!("Expected array");
    }

    server.stop().await;
}

// ============================================================
// 6. KEY MANAGEMENT
// ============================================================

#[tokio::test]
async fn test_e2e_key_operations() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // SET multiple keys
    client.cmd_ok(&["SET", "key1", "value1"]).await;
    client.cmd_ok(&["SET", "key2", "value2"]).await;
    client.cmd_ok(&["SET", "key3", "value3"]).await;

    // EXISTS
    let result = client.cmd(&["EXISTS", "key1", "key2", "key99"]).await;
    assert_eq!(result, RespValue::Integer(2));

    // TYPE
    let result = client.cmd(&["TYPE", "key1"]).await;
    assert_eq!(result, RespValue::SimpleString("string".to_string()));

    client.cmd(&["LPUSH", "mylist", "item"]).await;
    let result = client.cmd(&["TYPE", "mylist"]).await;
    assert_eq!(result, RespValue::SimpleString("list".to_string()));

    // DEL
    let result = client.cmd(&["DEL", "key1", "key2"]).await;
    assert_eq!(result, RespValue::Integer(2));

    // RENAME
    client.cmd_ok(&["SET", "oldname", "data"]).await;
    client.cmd_ok(&["RENAME", "oldname", "newname"]).await;
    let result = client.cmd(&["GET", "newname"]).await;
    assert_eq!(result, RespValue::bulk_string("data"));

    // COPY
    client.cmd_ok(&["SET", "source", "copied"]).await;
    let result = client.cmd(&["COPY", "source", "dest"]).await;
    assert_eq!(result, RespValue::Integer(1));
    let result = client.cmd(&["GET", "dest"]).await;
    assert_eq!(result, RespValue::bulk_string("copied"));

    server.stop().await;
}

// ============================================================
// 7. TTL AND EXPIRY
// ============================================================

#[tokio::test]
async fn test_e2e_ttl_operations() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // SET with EX
    client.cmd_ok(&["SET", "tempkey", "value", "EX", "2"]).await;

    // TTL should be around 2 seconds
    let result = client.cmd(&["TTL", "tempkey"]).await;
    if let RespValue::Integer(ttl) = result {
        assert!(ttl >= 1 && ttl <= 2);
    } else {
        panic!("Expected integer TTL");
    }

    // EXPIRE
    client.cmd_ok(&["SET", "key", "value"]).await;
    let result = client.cmd(&["EXPIRE", "key", "5"]).await;
    assert_eq!(result, RespValue::Integer(1));

    let result = client.cmd(&["TTL", "key"]).await;
    if let RespValue::Integer(ttl) = result {
        assert!(ttl >= 4 && ttl <= 5);
    } else {
        panic!("Expected integer TTL");
    }

    // PERSIST
    let result = client.cmd(&["PERSIST", "key"]).await;
    assert_eq!(result, RespValue::Integer(1));

    let result = client.cmd(&["TTL", "key"]).await;
    assert_eq!(result, RespValue::Integer(-1)); // No expiry

    // Wait for expiry
    tokio::time::sleep(tokio::time::Duration::from_millis(2100)).await;
    let result = client.cmd(&["GET", "tempkey"]).await;
    assert_eq!(result, RespValue::Null); // Key expired

    server.stop().await;
}

#[tokio::test]
async fn test_e2e_pexpire_operations() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // PSETEX - set with millisecond expiry
    client.cmd_ok(&["SET", "fastkey", "value"]).await;
    client.cmd(&["PEXPIRE", "fastkey", "500"]).await;

    // PTTL
    let result = client.cmd(&["PTTL", "fastkey"]).await;
    if let RespValue::Integer(pttl) = result {
        assert!(pttl > 0 && pttl <= 500);
    } else {
        panic!("Expected integer PTTL");
    }

    // Wait for expiry
    tokio::time::sleep(tokio::time::Duration::from_millis(600)).await;
    let result = client.cmd(&["GET", "fastkey"]).await;
    assert_eq!(result, RespValue::Null);

    server.stop().await;
}

// ============================================================
// 8. DATABASE OPERATIONS
// ============================================================

#[tokio::test]
async fn test_e2e_database_operations() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Default DB is 0
    client.cmd_ok(&["SET", "key", "db0"]).await;

    // SELECT different database
    client.cmd_ok(&["SELECT", "1"]).await;
    let result = client.cmd(&["GET", "key"]).await;
    assert_eq!(result, RespValue::Null); // Different DB

    client.cmd_ok(&["SET", "key", "db1"]).await;

    // SWAPDB
    client.cmd_ok(&["SWAPDB", "0", "1"]).await;

    client.cmd_ok(&["SELECT", "0"]).await;
    let result = client.cmd(&["GET", "key"]).await;
    assert_eq!(result, RespValue::bulk_string("db1"));

    // DBSIZE
    client.cmd_ok(&["SELECT", "0"]).await;
    client.cmd_ok(&["SET", "k1", "v1"]).await;
    client.cmd_ok(&["SET", "k2", "v2"]).await;
    let result = client.cmd(&["DBSIZE"]).await;
    if let RespValue::Integer(size) = result {
        assert!(size >= 2);
    } else {
        panic!("Expected integer");
    }

    // FLUSHDB
    client.cmd_ok(&["FLUSHDB"]).await;
    let result = client.cmd(&["DBSIZE"]).await;
    assert_eq!(result, RespValue::Integer(0));

    server.stop().await;
}

// ============================================================
// 9. SERVER COMMANDS
// ============================================================

#[tokio::test]
async fn test_e2e_server_commands() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // PING
    let result = client.cmd(&["PING"]).await;
    assert_eq!(result, RespValue::SimpleString("PONG".to_string()));

    let result = client.cmd(&["PING", "hello"]).await;
    assert_eq!(result, RespValue::bulk_string("hello"));

    // ECHO
    let result = client.cmd(&["ECHO", "test message"]).await;
    assert_eq!(result, RespValue::bulk_string("test message"));

    // TIME
    let result = client.cmd(&["TIME"]).await;
    if let RespValue::Array(arr) = result {
        assert_eq!(arr.len(), 2); // [seconds, microseconds]
    } else {
        panic!("Expected array");
    }

    // INFO
    let result = client.cmd(&["INFO", "server"]).await;
    if let RespValue::BulkString(info) = result {
        let info_str = String::from_utf8_lossy(&info);
        assert!(info_str.contains("redis_version"));
        assert!(info_str.contains("ferris_version"));
    } else {
        panic!("Expected bulk string");
    }

    // CONFIG GET
    let result = client.cmd(&["CONFIG", "GET", "maxmemory"]).await;
    if let RespValue::Array(arr) = result {
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0], RespValue::bulk_string("maxmemory"));
    } else {
        panic!("Expected array");
    }

    // CONFIG SET
    client
        .cmd_ok(&["CONFIG", "SET", "maxmemory", "1000000"])
        .await;
    let result = client.cmd(&["CONFIG", "GET", "maxmemory"]).await;
    if let RespValue::Array(arr) = result {
        assert_eq!(arr[1], RespValue::bulk_string("1000000"));
    } else {
        panic!("Expected array");
    }

    server.stop().await;
}

// ============================================================
// 10. MEMORY MANAGEMENT
// ============================================================

#[tokio::test]
async fn test_e2e_memory_operations() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Set a small maxmemory limit
    client.cmd_ok(&["CONFIG", "SET", "maxmemory", "50000"]).await;
    client
        .cmd_ok(&["CONFIG", "SET", "maxmemory-policy", "allkeys-lru"])
        .await;

    // Add keys
    for i in 0..10 {
        client
            .cmd_ok(&["SET", &format!("key{}", i), "x".repeat(1000).as_str()])
            .await;
    }

    // MEMORY STATS
    let result = client.cmd(&["MEMORY", "STATS"]).await;
    if let RespValue::Array(arr) = result {
        assert!(arr.len() > 0);
        // Should contain stats like peak.allocated, total.allocated, etc.
    } else {
        panic!("Expected array");
    }

    // MEMORY USAGE for a specific key
    let result = client.cmd(&["MEMORY", "USAGE", "key0"]).await;
    if let RespValue::Integer(usage) = result {
        assert!(usage > 0);
    } else {
        panic!("Expected integer");
    }

    server.stop().await;
}

// ============================================================
// 11. PIPELINE OPERATIONS
// ============================================================

#[tokio::test]
async fn test_e2e_pipeline_operations() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Send multiple commands in a pipeline
    let responses = client
        .pipeline(&[
            &["SET", "p1", "v1"],
            &["SET", "p2", "v2"],
            &["SET", "p3", "v3"],
            &["GET", "p1"],
            &["GET", "p2"],
            &["GET", "p3"],
            &["MGET", "p1", "p2", "p3"],
        ])
        .await;

    assert_eq!(responses.len(), 7);
    assert_eq!(responses[0], RespValue::SimpleString("OK".to_string()));
    assert_eq!(responses[3], RespValue::bulk_string("v1"));
    assert_eq!(responses[4], RespValue::bulk_string("v2"));
    assert_eq!(responses[5], RespValue::bulk_string("v3"));

    if let RespValue::Array(arr) = &responses[6] {
        assert_eq!(arr.len(), 3);
    } else {
        panic!("Expected array");
    }

    server.stop().await;
}

// ============================================================
// 12. CONNECTION MANAGEMENT
// ============================================================

#[tokio::test]
async fn test_e2e_connection_commands() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // CLIENT ID
    let result = client.cmd(&["CLIENT", "ID"]).await;
    if let RespValue::Integer(id) = result {
        assert!(id > 0);
    } else {
        panic!("Expected integer");
    }

    // CLIENT SETNAME and GETNAME
    client.cmd_ok(&["CLIENT", "SETNAME", "test-client"]).await;
    let result = client.cmd(&["CLIENT", "GETNAME"]).await;
    assert_eq!(result, RespValue::bulk_string("test-client"));

    // CLIENT LIST
    let result = client.cmd(&["CLIENT", "LIST"]).await;
    if let RespValue::BulkString(list) = result {
        let list_str = String::from_utf8_lossy(&list);
        assert!(list_str.contains("name=test-client"));
    } else {
        panic!("Expected bulk string");
    }

    server.stop().await;
}

// ============================================================
// 13. MULTI-TYPE OPERATIONS
// ============================================================

#[tokio::test]
async fn test_e2e_cross_type_operations() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Create different types with same key prefix
    client.cmd_ok(&["SET", "data:string", "text"]).await;
    client.cmd(&["LPUSH", "data:list", "item"]).await;
    client.cmd(&["SADD", "data:set", "member"]).await;
    client.cmd(&["HSET", "data:hash", "field", "value"]).await;
    client.cmd(&["ZADD", "data:zset", "1.0", "member"]).await;

    // Verify types
    let result = client.cmd(&["TYPE", "data:string"]).await;
    assert_eq!(result, RespValue::SimpleString("string".to_string()));

    let result = client.cmd(&["TYPE", "data:list"]).await;
    assert_eq!(result, RespValue::SimpleString("list".to_string()));

    let result = client.cmd(&["TYPE", "data:set"]).await;
    assert_eq!(result, RespValue::SimpleString("set".to_string()));

    let result = client.cmd(&["TYPE", "data:hash"]).await;
    assert_eq!(result, RespValue::SimpleString("hash".to_string()));

    let result = client.cmd(&["TYPE", "data:zset"]).await;
    assert_eq!(result, RespValue::SimpleString("zset".to_string()));

    // Try wrong-type operations (should error)
    let result = client.cmd(&["LPUSH", "data:string", "item"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    let result = client.cmd(&["SADD", "data:list", "member"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    server.stop().await;
}

// ============================================================
// 14. CONCURRENT OPERATIONS
// ============================================================

#[tokio::test]
async fn test_e2e_concurrent_operations() {
    let server = TestServer::spawn().await;

    // Initialize counter
    let mut init_client = server.client().await;
    init_client.cmd_ok(&["SET", "counter", "0"]).await;
    drop(init_client);

    // Spawn 10 concurrent clients, each incrementing 10 times
    let mut handles = vec![];
    for _ in 0..10 {
        let addr = server.addr;
        let handle = tokio::spawn(async move {
            let mut client = ferris_test_utils::TestClient::connect(addr).await;
            for _ in 0..10 {
                client.cmd(&["INCR", "counter"]).await;
            }
        });
        handles.push(handle);
    }

    // Wait for all to complete
    for handle in handles {
        handle.await.unwrap();
    }

    // Verify final count
    let mut client = server.client().await;
    let result = client.cmd(&["GET", "counter"]).await;
    assert_eq!(result, RespValue::bulk_string("100"));

    server.stop().await;
}

// ============================================================
// 15. TRANSACTION-LIKE OPERATIONS (without MULTI/EXEC)
// ============================================================

#[tokio::test]
async fn test_e2e_atomic_operations() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // GETSET is atomic
    client.cmd_ok(&["SET", "atomic", "1"]).await;
    let result = client.cmd(&["GETSET", "atomic", "2"]).await;
    assert_eq!(result, RespValue::bulk_string("1"));

    // INCR is atomic
    let mut handles = vec![];
    for _ in 0..5 {
        let addr = server.addr;
        let handle = tokio::spawn(async move {
            let mut c = ferris_test_utils::TestClient::connect(addr).await;
            for _ in 0..20 {
                c.cmd(&["INCR", "atomic"]).await;
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let result = client.cmd(&["GET", "atomic"]).await;
    assert_eq!(result, RespValue::bulk_string("102")); // 2 + 100

    server.stop().await;
}

// ============================================================
// 16. SCAN OPERATIONS
// ============================================================

#[tokio::test]
async fn test_e2e_scan_operations() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Create many keys
    for i in 0..50 {
        client
            .cmd_ok(&["SET", &format!("scankey:{}", i), "value"])
            .await;
    }

    // SCAN
    let result = client.cmd(&["SCAN", "0", "COUNT", "10"]).await;
    if let RespValue::Array(arr) = result {
        assert_eq!(arr.len(), 2); // [cursor, keys]
        if let RespValue::Array(keys) = &arr[1] {
            assert!(keys.len() > 0);
        }
    } else {
        panic!("Expected array");
    }

    // HSCAN
    client
        .cmd(&["HSET", "hscan_test", "f1", "v1", "f2", "v2", "f3", "v3"])
        .await;
    let result = client.cmd(&["HSCAN", "hscan_test", "0"]).await;
    if let RespValue::Array(arr) = result {
        assert_eq!(arr.len(), 2);
    } else {
        panic!("Expected array");
    }

    // SSCAN
    client.cmd(&["SADD", "sscan_test", "m1", "m2", "m3"]).await;
    let result = client.cmd(&["SSCAN", "sscan_test", "0"]).await;
    if let RespValue::Array(arr) = result {
        assert_eq!(arr.len(), 2);
    } else {
        panic!("Expected array");
    }

    // ZSCAN
    client
        .cmd(&["ZADD", "zscan_test", "1", "m1", "2", "m2", "3", "m3"])
        .await;
    let result = client.cmd(&["ZSCAN", "zscan_test", "0"]).await;
    if let RespValue::Array(arr) = result {
        assert_eq!(arr.len(), 2);
    } else {
        panic!("Expected array");
    }

    server.stop().await;
}

// ============================================================
// 17. EDGE CASES
// ============================================================

#[tokio::test]
async fn test_e2e_edge_cases() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Empty string value
    client.cmd_ok(&["SET", "empty", ""]).await;
    let result = client.cmd(&["GET", "empty"]).await;
    assert_eq!(result, RespValue::bulk_string(""));

    // Very long key name
    let long_key = "k".repeat(1000);
    client.cmd_ok(&["SET", &long_key, "value"]).await;
    let result = client.cmd(&["GET", &long_key]).await;
    assert_eq!(result, RespValue::bulk_string("value"));

    // Very long value
    let long_value = "x".repeat(10000);
    client.cmd_ok(&["SET", "longval", &long_value]).await;
    let result = client.cmd(&["GET", "longval"]).await;
    if let RespValue::BulkString(data) = result {
        assert_eq!(data.len(), 10000);
    } else {
        panic!("Expected bulk string");
    }

    // Negative indices in lists
    client.cmd(&["LPUSH", "neglist", "a", "b", "c"]).await;
    let result = client.cmd(&["LINDEX", "neglist", "-1"]).await;
    assert_eq!(result, RespValue::bulk_string("a"));

    // Operations on non-existent keys
    let result = client.cmd(&["GET", "nonexistent"]).await;
    assert_eq!(result, RespValue::Null);

    let result = client.cmd(&["LPOP", "nonexistent"]).await;
    assert_eq!(result, RespValue::Null);

    server.stop().await;
}

// ============================================================
// 18. ERROR HANDLING
// ============================================================

#[tokio::test]
async fn test_e2e_error_handling() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Wrong number of arguments
    let result = client.cmd(&["SET"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    let result = client.cmd(&["GET"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    // Invalid integer
    // First set a non-numeric value
    client.cmd_ok(&["SET", "notanumber", "abc"]).await;
    let result = client.cmd(&["INCR", "notanumber"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    // Invalid database number
    let result = client.cmd(&["SELECT", "999"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    let result = client.cmd(&["SELECT", "abc"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    // Unknown command
    let result = client.cmd(&["NOTACOMMAND", "arg"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    server.stop().await;
}

// ============================================================
// 19. SPECIAL VALUES
// ============================================================

#[tokio::test]
async fn test_e2e_special_values() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Binary data (non-UTF8)
    let binary_data = vec![0xFF, 0xFE, 0x00, 0x01, 0x02];
    let result = client
        .cmd(&[
            "SET",
            "binary",
            &String::from_utf8_lossy(&binary_data),
        ])
        .await;
    assert!(matches!(
        result,
        RespValue::SimpleString(_) | RespValue::Error(_)
    ));

    // Unicode strings
    client.cmd_ok(&["SET", "unicode", "Hello 世界 🦀"]).await;
    let result = client.cmd(&["GET", "unicode"]).await;
    assert_eq!(result, RespValue::bulk_string("Hello 世界 🦀"));

    // Numbers as strings
    client.cmd_ok(&["SET", "numstr", "12345"]).await;
    let result = client.cmd(&["GET", "numstr"]).await;
    assert_eq!(result, RespValue::bulk_string("12345"));

    // Floating point in sorted sets
    client
        .cmd(&["ZADD", "floatset", "3.14159", "pi", "-2.718", "e"])
        .await;
    let result = client.cmd(&["ZSCORE", "floatset", "pi"]).await;
    assert_eq!(result, RespValue::bulk_string("3.14159"));

    server.stop().await;
}

// ============================================================
// 20. FULL WORKFLOW TEST
// ============================================================

#[tokio::test]
async fn test_e2e_full_workflow() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Simulate a real application workflow: user session management

    // 1. User registers
    let user_id = "user:1001";
    client
        .cmd_ok(&[
            "HMSET",
            user_id,
            "username",
            "alice",
            "email",
            "alice@example.com",
            "created",
            "2024-01-01",
        ])
        .await;

    // 2. User logs in - create session
    let session_id = "session:abc123";
    client
        .cmd_ok(&["SET", session_id, user_id, "EX", "3600"])
        .await; // 1 hour TTL

    // 3. User views products
    client
        .cmd(&["ZADD", "products:viewed:1001", "1", "prod:1", "2", "prod:2"])
        .await;

    // 4. User adds to cart
    client
        .cmd(&["SADD", "cart:1001", "prod:1", "prod:3"])
        .await;

    // 5. User visits count
    client.cmd(&["INCR", "user:1001:visits"]).await;

    // 6. Activity log
    client
        .cmd(&["LPUSH", "activity:1001", "viewed prod:1", "added to cart"])
        .await;

    // Verify the workflow
    let user_info = client.cmd(&["HGETALL", user_id]).await;
    assert!(matches!(user_info, RespValue::Array(_)));

    let session = client.cmd(&["GET", session_id]).await;
    assert_eq!(session, RespValue::bulk_string(user_id));

    let cart = client.cmd(&["SMEMBERS", "cart:1001"]).await;
    if let RespValue::Array(items) = cart {
        assert_eq!(items.len(), 2);
    } else {
        panic!("Expected array");
    }

    let visits = client.cmd(&["GET", "user:1001:visits"]).await;
    assert_eq!(visits, RespValue::bulk_string("1"));

    // Pipeline cleanup
    let responses = client
        .pipeline(&[
            &["DEL", user_id],
            &["DEL", session_id],
            &["DEL", "products:viewed:1001"],
            &["DEL", "cart:1001"],
            &["DEL", "user:1001:visits"],
            &["DEL", "activity:1001"],
        ])
        .await;

    assert_eq!(responses.len(), 6);

    server.stop().await;
}

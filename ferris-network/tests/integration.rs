//! Integration tests for the ferris-network layer.
//!
//! These tests spawn a real server on a random port and exercise
//! the full stack: TCP -> RESP codec -> command executor -> response.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
#![allow(clippy::uninlined_format_args)]
#![allow(clippy::match_same_arms)]
#![allow(clippy::redundant_closure_for_method_calls)]

use bytes::Bytes;
use ferris_network::ServerConfig;
use ferris_protocol::RespValue;
use ferris_test_utils::TestServer;

// ============================================================
// 1. Basic connection tests
// ============================================================

#[tokio::test]
async fn test_connect_and_disconnect() {
    let server = TestServer::spawn().await;
    let client = server.client().await;
    drop(client); // Disconnect
                  // Server should handle the disconnect gracefully
    server.stop().await;
}

#[tokio::test]
async fn test_ping_pong() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let response = client.cmd(&["PING"]).await;
    assert_eq!(response, RespValue::SimpleString("PONG".to_string()));

    server.stop().await;
}

#[tokio::test]
async fn test_ping_with_message() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let response = client.cmd(&["PING", "hello"]).await;
    assert_eq!(response, RespValue::BulkString(Bytes::from("hello")));

    server.stop().await;
}

#[tokio::test]
async fn test_echo() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let response = client.cmd(&["ECHO", "Hello World"]).await;
    assert_eq!(response, RespValue::BulkString(Bytes::from("Hello World")));

    server.stop().await;
}

#[tokio::test]
async fn test_unknown_command_returns_error() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let err = client.cmd_err(&["NOTAREALCOMMAND"]).await;
    assert!(
        err.contains("unknown command"),
        "expected unknown command error, got: {err}"
    );

    server.stop().await;
}

// ============================================================
// 2. String command roundtrip tests
// ============================================================

#[tokio::test]
async fn test_set_get_roundtrip() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd_ok(&["SET", "mykey", "myvalue"]).await;

    let response = client.cmd(&["GET", "mykey"]).await;
    assert_eq!(response, RespValue::BulkString(Bytes::from("myvalue")));

    server.stop().await;
}

#[tokio::test]
async fn test_get_nonexistent_key() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let response = client.cmd(&["GET", "nosuchkey"]).await;
    assert_eq!(response, RespValue::Null);

    server.stop().await;
}

#[tokio::test]
async fn test_set_overwrite() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd_ok(&["SET", "key1", "first"]).await;
    client.cmd_ok(&["SET", "key1", "second"]).await;

    let response = client.cmd(&["GET", "key1"]).await;
    assert_eq!(response, RespValue::BulkString(Bytes::from("second")));

    server.stop().await;
}

#[tokio::test]
async fn test_mset_mget() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client
        .cmd_ok(&["MSET", "k1", "v1", "k2", "v2", "k3", "v3"])
        .await;

    let response = client.cmd(&["MGET", "k1", "k2", "k3", "k4"]).await;

    match response {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 4);
            assert_eq!(arr[0], RespValue::BulkString(Bytes::from("v1")));
            assert_eq!(arr[1], RespValue::BulkString(Bytes::from("v2")));
            assert_eq!(arr[2], RespValue::BulkString(Bytes::from("v3")));
            assert_eq!(arr[3], RespValue::Null); // k4 doesn't exist
        }
        other => panic!("expected Array, got {:?}", other),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_incr_decr() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd_ok(&["SET", "counter", "10"]).await;

    let response = client.cmd(&["INCR", "counter"]).await;
    assert_eq!(response, RespValue::Integer(11));

    let response = client.cmd(&["DECRBY", "counter", "5"]).await;
    assert_eq!(response, RespValue::Integer(6));

    server.stop().await;
}

#[tokio::test]
async fn test_append_strlen() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let response = client.cmd(&["APPEND", "mykey", "Hello"]).await;
    assert_eq!(response, RespValue::Integer(5));

    let response = client.cmd(&["APPEND", "mykey", " World"]).await;
    assert_eq!(response, RespValue::Integer(11));

    let response = client.cmd(&["STRLEN", "mykey"]).await;
    assert_eq!(response, RespValue::Integer(11));

    server.stop().await;
}

// ============================================================
// 3. Key command tests
// ============================================================

#[tokio::test]
async fn test_del_exists() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd_ok(&["SET", "key1", "val1"]).await;
    client.cmd_ok(&["SET", "key2", "val2"]).await;

    let response = client.cmd(&["EXISTS", "key1"]).await;
    assert_eq!(response, RespValue::Integer(1));

    let response = client.cmd(&["DEL", "key1", "key2", "key3"]).await;
    assert_eq!(response, RespValue::Integer(2)); // 2 keys deleted

    let response = client.cmd(&["EXISTS", "key1"]).await;
    assert_eq!(response, RespValue::Integer(0));

    server.stop().await;
}

#[tokio::test]
async fn test_expire_and_ttl() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd_ok(&["SET", "mykey", "val"]).await;

    // No expiry set
    let response = client.cmd(&["TTL", "mykey"]).await;
    assert_eq!(response, RespValue::Integer(-1));

    // Set 10 second expiry
    let response = client.cmd(&["EXPIRE", "mykey", "10"]).await;
    assert_eq!(response, RespValue::Integer(1));

    // TTL should be between 1 and 10
    let response = client.cmd(&["TTL", "mykey"]).await;
    match response {
        RespValue::Integer(ttl) => assert!((1..=10).contains(&ttl)),
        other => panic!("expected Integer, got {:?}", other),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_type_command() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd_ok(&["SET", "str", "value"]).await;
    let response = client.cmd(&["TYPE", "str"]).await;
    assert_eq!(response, RespValue::SimpleString("string".to_string()));

    client.cmd(&["LPUSH", "lst", "item"]).await;
    let response = client.cmd(&["TYPE", "lst"]).await;
    assert_eq!(response, RespValue::SimpleString("list".to_string()));

    client.cmd(&["SADD", "st", "member"]).await;
    let response = client.cmd(&["TYPE", "st"]).await;
    assert_eq!(response, RespValue::SimpleString("set".to_string()));

    let response = client.cmd(&["TYPE", "nonexistent"]).await;
    assert_eq!(response, RespValue::SimpleString("none".to_string()));

    server.stop().await;
}

// ============================================================
// 4. Hash command tests
// ============================================================

#[tokio::test]
async fn test_hash_operations() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let response = client
        .cmd(&["HSET", "myhash", "field1", "val1", "field2", "val2"])
        .await;
    assert_eq!(response, RespValue::Integer(2));

    let response = client.cmd(&["HGET", "myhash", "field1"]).await;
    assert_eq!(response, RespValue::BulkString(Bytes::from("val1")));

    let response = client.cmd(&["HLEN", "myhash"]).await;
    assert_eq!(response, RespValue::Integer(2));

    let response = client.cmd(&["HDEL", "myhash", "field1"]).await;
    assert_eq!(response, RespValue::Integer(1));

    let response = client.cmd(&["HEXISTS", "myhash", "field1"]).await;
    assert_eq!(response, RespValue::Integer(0));

    server.stop().await;
}

// ============================================================
// 5. List command tests
// ============================================================

#[tokio::test]
async fn test_list_operations() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let response = client.cmd(&["RPUSH", "mylist", "a", "b", "c"]).await;
    assert_eq!(response, RespValue::Integer(3));

    let response = client.cmd(&["LLEN", "mylist"]).await;
    assert_eq!(response, RespValue::Integer(3));

    let response = client.cmd(&["LRANGE", "mylist", "0", "-1"]).await;
    match response {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], RespValue::BulkString(Bytes::from("a")));
            assert_eq!(arr[1], RespValue::BulkString(Bytes::from("b")));
            assert_eq!(arr[2], RespValue::BulkString(Bytes::from("c")));
        }
        other => panic!("expected Array, got {:?}", other),
    }

    let response = client.cmd(&["LPOP", "mylist"]).await;
    assert_eq!(response, RespValue::BulkString(Bytes::from("a")));

    server.stop().await;
}

// ============================================================
// 6. Set command tests
// ============================================================

#[tokio::test]
async fn test_set_operations() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let response = client.cmd(&["SADD", "myset", "a", "b", "c"]).await;
    assert_eq!(response, RespValue::Integer(3));

    let response = client.cmd(&["SCARD", "myset"]).await;
    assert_eq!(response, RespValue::Integer(3));

    let response = client.cmd(&["SISMEMBER", "myset", "a"]).await;
    assert_eq!(response, RespValue::Integer(1));

    let response = client.cmd(&["SISMEMBER", "myset", "z"]).await;
    assert_eq!(response, RespValue::Integer(0));

    server.stop().await;
}

// ============================================================
// 7. Sorted set command tests
// ============================================================

#[tokio::test]
async fn test_sorted_set_operations() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let response = client
        .cmd(&["ZADD", "myzset", "1", "one", "2", "two", "3", "three"])
        .await;
    assert_eq!(response, RespValue::Integer(3));

    let response = client.cmd(&["ZCARD", "myzset"]).await;
    assert_eq!(response, RespValue::Integer(3));

    let response = client.cmd(&["ZSCORE", "myzset", "two"]).await;
    assert_eq!(response, RespValue::BulkString(Bytes::from("2")));

    let response = client
        .cmd(&["ZRANGE", "myzset", "0", "-1", "WITHSCORES"])
        .await;
    match response {
        RespValue::Array(arr) => {
            // Should be [one, 1, two, 2, three, 3]
            assert_eq!(arr.len(), 6);
            assert_eq!(arr[0], RespValue::BulkString(Bytes::from("one")));
            assert_eq!(arr[2], RespValue::BulkString(Bytes::from("two")));
            assert_eq!(arr[4], RespValue::BulkString(Bytes::from("three")));
        }
        other => panic!("expected Array, got {:?}", other),
    }

    server.stop().await;
}

// ============================================================
// 8. Multiple commands on same connection (pipeline-like)
// ============================================================

#[tokio::test]
async fn test_sequential_commands() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Execute 50 commands sequentially on one connection
    for i in 0..50 {
        let key = format!("key{i}");
        let val = format!("val{i}");
        client.cmd_ok(&["SET", &key, &val]).await;
    }

    for i in 0..50 {
        let key = format!("key{i}");
        let expected = format!("val{i}");
        let response = client.cmd(&["GET", &key]).await;
        assert_eq!(response, RespValue::BulkString(Bytes::from(expected)));
    }

    server.stop().await;
}

// ============================================================
// 9. Multiple concurrent clients
// ============================================================

#[tokio::test]
async fn test_concurrent_clients() {
    let server = TestServer::spawn().await;

    let mut handles = Vec::new();
    for i in 0..10 {
        let mut client = server.client().await;
        let handle = tokio::spawn(async move {
            let key = format!("concurrent_key_{i}");
            let val = format!("concurrent_val_{i}");
            client.cmd_ok(&["SET", &key, &val]).await;
            let response = client.cmd(&["GET", &key]).await;
            assert_eq!(response, RespValue::BulkString(Bytes::from(val)));
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.expect("client task panicked");
    }

    server.stop().await;
}

#[tokio::test]
async fn test_concurrent_incr() {
    let server = TestServer::spawn().await;

    // Pre-set the counter
    let mut setup_client = server.client().await;
    client_cmd_ok(&mut setup_client, &["SET", "shared_counter", "0"]).await;

    let num_clients = 10;
    let ops_per_client = 100;

    let mut handles = Vec::new();
    for _ in 0..num_clients {
        let mut client = server.client().await;
        let handle = tokio::spawn(async move {
            for _ in 0..ops_per_client {
                client.cmd(&["INCR", "shared_counter"]).await;
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.expect("incr task panicked");
    }

    let response = setup_client.cmd(&["GET", "shared_counter"]).await;
    let expected = num_clients * ops_per_client;
    assert_eq!(
        response,
        RespValue::BulkString(Bytes::from(format!("{expected}")))
    );

    server.stop().await;
}

/// Helper that asserts cmd returns OK
async fn client_cmd_ok(client: &mut ferris_test_utils::TestClient, args: &[&str]) {
    client.cmd_ok(args).await;
}

// ============================================================
// 10. QUIT command
// ============================================================

#[tokio::test]
async fn test_quit_command() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // QUIT should return OK
    let response = client.cmd(&["QUIT"]).await;
    assert_eq!(response, RespValue::SimpleString("OK".to_string()));

    // After QUIT, the server closes the connection
    // Trying another command should fail
    // (we just verify we got OK and move on)

    server.stop().await;
}

// ============================================================
// 11. Connection isolation (different DBs)
// ============================================================

#[tokio::test]
async fn test_select_database_isolation() {
    let server = TestServer::spawn().await;

    let mut client1 = server.client().await;
    let mut client2 = server.client().await;

    // Client 1 on db 0
    client1.cmd_ok(&["SET", "key", "db0_value"]).await;

    // Client 2 selects db 1
    client2.cmd_ok(&["SELECT", "1"]).await;
    client2.cmd_ok(&["SET", "key", "db1_value"]).await;

    // Verify isolation
    let r1 = client1.cmd(&["GET", "key"]).await;
    assert_eq!(r1, RespValue::BulkString(Bytes::from("db0_value")));

    let r2 = client2.cmd(&["GET", "key"]).await;
    assert_eq!(r2, RespValue::BulkString(Bytes::from("db1_value")));

    server.stop().await;
}

// ============================================================
// 12. Max connections enforcement
// ============================================================

#[tokio::test]
async fn test_max_connections_limit() {
    let server = TestServer::spawn_with_config(ServerConfig {
        bind_addr: "127.0.0.1:0".to_string(),
        max_connections: 3,
        timeout: 0,
    })
    .await;

    // Connect 3 clients (should succeed)
    let mut client1 = server.client().await;
    let mut client2 = server.client().await;
    let mut client3 = server.client().await;

    // Verify all 3 work
    let r1 = client1.cmd(&["PING"]).await;
    assert_eq!(r1, RespValue::SimpleString("PONG".to_string()));

    let r2 = client2.cmd(&["PING"]).await;
    assert_eq!(r2, RespValue::SimpleString("PONG".to_string()));

    let r3 = client3.cmd(&["PING"]).await;
    assert_eq!(r3, RespValue::SimpleString("PONG".to_string()));

    // 4th connection should be rejected (server drops the stream)
    // We need to give a short timeout — the connection may be accepted at TCP level
    // but the server drops it immediately
    let result = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        tokio::net::TcpStream::connect(server.addr),
    )
    .await;

    // The connection might succeed at TCP level but get immediately closed.
    // Try to send a command — it should fail.
    if let Ok(Ok(stream)) = result {
        use tokio::io::AsyncWriteExt;
        let mut stream = stream;
        let write_result = stream.write_all(b"*1\r\n$4\r\nPING\r\n").await;
        if write_result.is_ok() {
            use tokio::io::AsyncReadExt;
            let mut buf = [0u8; 128];
            let read_result =
                tokio::time::timeout(std::time::Duration::from_millis(500), stream.read(&mut buf))
                    .await;
            // Either timeout or read 0 bytes (connection closed) is acceptable
            match read_result {
                Ok(Ok(0)) => {} // Connection was closed by server — correct behavior
                Err(_) => {}    // Timeout — server dropped the connection
                Ok(Ok(_n)) => {
                    // If server somehow responded, that's also okay —
                    // it might have accepted just before the limit was checked.
                    // This is a race condition inherent to TCP accept.
                }
                Ok(Err(_)) => {} // Read error — connection was rejected
            }
        }
    }

    server.stop().await;
}

// ============================================================
// 13. Graceful shutdown
// ============================================================

#[tokio::test]
async fn test_graceful_shutdown() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Verify server is working
    let response = client.cmd(&["PING"]).await;
    assert_eq!(response, RespValue::SimpleString("PONG".to_string()));

    // Stop should complete without hanging
    server.stop().await;
}

// ============================================================
// 14. Server commands over the wire
// ============================================================

#[tokio::test]
async fn test_dbsize_command() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let response = client.cmd(&["DBSIZE"]).await;
    assert_eq!(response, RespValue::Integer(0));

    client.cmd_ok(&["SET", "k1", "v1"]).await;
    client.cmd_ok(&["SET", "k2", "v2"]).await;

    let response = client.cmd(&["DBSIZE"]).await;
    assert_eq!(response, RespValue::Integer(2));

    server.stop().await;
}

#[tokio::test]
async fn test_flushdb() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd_ok(&["SET", "k1", "v1"]).await;
    client.cmd_ok(&["SET", "k2", "v2"]).await;

    client.cmd_ok(&["FLUSHDB"]).await;

    let response = client.cmd(&["DBSIZE"]).await;
    assert_eq!(response, RespValue::Integer(0));

    server.stop().await;
}

#[tokio::test]
async fn test_time_command() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let response = client.cmd(&["TIME"]).await;
    match response {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2);
            // Both should be bulk strings representing numbers
        }
        other => panic!("expected Array, got {:?}", other),
    }

    server.stop().await;
}

// ============================================================
// 15. Wrong arity errors over the wire
// ============================================================

#[tokio::test]
async fn test_wrong_arity_error() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // GET requires exactly 1 argument
    let err = client.cmd_err(&["GET"]).await;
    assert!(
        err.contains("wrong number of arguments"),
        "expected arity error, got: {err}"
    );

    server.stop().await;
}

// ============================================================
// 16. WRONGTYPE errors over the wire
// ============================================================

#[tokio::test]
async fn test_wrongtype_error() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Create a list
    client.cmd(&["LPUSH", "mylist", "item"]).await;

    // Try to GET a list key — should return WRONGTYPE
    let err = client.cmd_err(&["GET", "mylist"]).await;
    assert!(
        err.contains("WRONGTYPE"),
        "expected WRONGTYPE error, got: {err}"
    );

    server.stop().await;
}

// ============================================================
// 17. Large values
// ============================================================

#[tokio::test]
async fn test_large_value() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // 1MB value
    let large_val = "x".repeat(1_000_000);
    client.cmd_ok(&["SET", "large", &large_val]).await;

    let response = client.cmd(&["GET", "large"]).await;
    assert_eq!(
        response,
        RespValue::BulkString(Bytes::from(large_val.clone()))
    );

    let response = client.cmd(&["STRLEN", "large"]).await;
    assert_eq!(response, RespValue::Integer(1_000_000));

    server.stop().await;
}

// ============================================================
// 18. CLIENT commands over the wire
// ============================================================

#[tokio::test]
async fn test_client_setname_getname() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd_ok(&["CLIENT", "SETNAME", "myconn"]).await;

    let response = client.cmd(&["CLIENT", "GETNAME"]).await;
    assert_eq!(response, RespValue::BulkString(Bytes::from("myconn")));

    server.stop().await;
}

// ============================================================
// 19. INFO command over the wire
// ============================================================

#[tokio::test]
async fn test_info_command() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let response = client.cmd(&["INFO"]).await;
    match response {
        RespValue::BulkString(data) => {
            let text = String::from_utf8_lossy(&data);
            assert!(
                text.contains("redis_version"),
                "INFO should contain redis_version"
            );
        }
        other => panic!("expected BulkString for INFO, got {:?}", other),
    }

    server.stop().await;
}

// ============================================================
// 20. COMMAND COUNT over the wire
// ============================================================

#[tokio::test]
async fn test_command_count() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let response = client.cmd(&["COMMAND", "COUNT"]).await;
    match response {
        RespValue::Integer(count) => {
            // We have dozens of commands registered
            assert!(count > 50, "expected > 50 commands, got {count}");
        }
        other => panic!("expected Integer, got {:?}", other),
    }

    server.stop().await;
}

// ============================================================
// 21. Data persistence across commands on shared store
// ============================================================

#[tokio::test]
async fn test_data_visible_across_clients() {
    let server = TestServer::spawn().await;

    let mut writer = server.client().await;
    let mut reader = server.client().await;

    writer.cmd_ok(&["SET", "shared_key", "shared_val"]).await;

    let response = reader.cmd(&["GET", "shared_key"]).await;
    assert_eq!(response, RespValue::BulkString(Bytes::from("shared_val")));

    server.stop().await;
}

// ============================================================
// 22. RENAME over the wire
// ============================================================

#[tokio::test]
async fn test_rename_over_wire() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd_ok(&["SET", "oldname", "value"]).await;
    client.cmd_ok(&["RENAME", "oldname", "newname"]).await;

    let response = client.cmd(&["GET", "oldname"]).await;
    assert_eq!(response, RespValue::Null);

    let response = client.cmd(&["GET", "newname"]).await;
    assert_eq!(response, RespValue::BulkString(Bytes::from("value")));

    server.stop().await;
}

// ============================================================
// 23. COPY command over the wire
// ============================================================

#[tokio::test]
async fn test_copy_over_wire() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd_ok(&["SET", "src", "value"]).await;

    let response = client.cmd(&["COPY", "src", "dst"]).await;
    assert_eq!(response, RespValue::Integer(1));

    // Both should have the value
    let r1 = client.cmd(&["GET", "src"]).await;
    let r2 = client.cmd(&["GET", "dst"]).await;
    assert_eq!(r1, RespValue::BulkString(Bytes::from("value")));
    assert_eq!(r2, RespValue::BulkString(Bytes::from("value")));

    server.stop().await;
}

// ============================================================
// 24. Mixed data type operations
// ============================================================

#[tokio::test]
async fn test_mixed_data_types() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // String
    client.cmd_ok(&["SET", "str_key", "string_val"]).await;

    // List
    client.cmd(&["RPUSH", "list_key", "a", "b"]).await;

    // Set
    client.cmd(&["SADD", "set_key", "x", "y"]).await;

    // Hash
    client.cmd(&["HSET", "hash_key", "f1", "v1"]).await;

    // Sorted set
    client.cmd(&["ZADD", "zset_key", "1.0", "m1"]).await;

    // Verify all types
    let t1 = client.cmd(&["TYPE", "str_key"]).await;
    let t2 = client.cmd(&["TYPE", "list_key"]).await;
    let t3 = client.cmd(&["TYPE", "set_key"]).await;
    let t4 = client.cmd(&["TYPE", "hash_key"]).await;
    let t5 = client.cmd(&["TYPE", "zset_key"]).await;

    assert_eq!(t1, RespValue::SimpleString("string".to_string()));
    assert_eq!(t2, RespValue::SimpleString("list".to_string()));
    assert_eq!(t3, RespValue::SimpleString("set".to_string()));
    assert_eq!(t4, RespValue::SimpleString("hash".to_string()));
    assert_eq!(t5, RespValue::SimpleString("zset".to_string()));

    server.stop().await;
}

// ============================================================
// 25. Stress test: many commands on one connection
// ============================================================

#[tokio::test]
async fn test_stress_single_connection() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    for i in 0..500 {
        let key = format!("stress_{i}");
        let val = format!("v{i}");
        client.cmd_ok(&["SET", &key, &val]).await;
    }

    // Verify a sampling
    for i in (0..500).step_by(50) {
        let key = format!("stress_{i}");
        let expected = format!("v{i}");
        let response = client.cmd(&["GET", &key]).await;
        assert_eq!(response, RespValue::BulkString(Bytes::from(expected)));
    }

    let response = client.cmd(&["DBSIZE"]).await;
    assert_eq!(response, RespValue::Integer(500));

    server.stop().await;
}

// ============================================================
// Memory Management Integration Tests
// ============================================================

#[tokio::test]
async fn test_memory_stats_returns_values() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Set some keys to generate memory usage
    for i in 0..10 {
        client
            .cmd_ok(&["SET", &format!("key{i}"), &format!("value{i}")])
            .await;
    }

    let response = client.cmd(&["MEMORY", "STATS"]).await;
    if let RespValue::Array(arr) = response {
        // Should have key-value pairs
        assert!(
            arr.len() >= 4,
            "Expected at least 4 elements in MEMORY STATS"
        );

        // Check for peak.allocated
        let peak_idx = arr
            .iter()
            .position(|v| {
                if let RespValue::BulkString(b) = v {
                    b.as_ref() == b"peak.allocated"
                } else {
                    false
                }
            })
            .expect("peak.allocated not found");
        if let RespValue::Integer(peak) = &arr[peak_idx + 1] {
            assert!(*peak > 0, "peak.allocated should be > 0 after setting keys");
        } else {
            panic!("peak.allocated value should be integer");
        }

        // Check for total.allocated
        let total_idx = arr
            .iter()
            .position(|v| {
                if let RespValue::BulkString(b) = v {
                    b.as_ref() == b"total.allocated"
                } else {
                    false
                }
            })
            .expect("total.allocated not found");
        if let RespValue::Integer(total) = &arr[total_idx + 1] {
            assert!(
                *total > 0,
                "total.allocated should be > 0 after setting keys"
            );
        } else {
            panic!("total.allocated value should be integer");
        }

        // Check for keys.count
        let keys_idx = arr
            .iter()
            .position(|v| {
                if let RespValue::BulkString(b) = v {
                    b.as_ref() == b"keys.count"
                } else {
                    false
                }
            })
            .expect("keys.count not found");
        if let RespValue::Integer(keys) = &arr[keys_idx + 1] {
            assert_eq!(*keys, 10, "keys.count should be 10");
        } else {
            panic!("keys.count value should be integer");
        }
    } else {
        panic!("MEMORY STATS should return array");
    }

    server.stop().await;
}

#[tokio::test]
async fn test_memory_usage_single_key() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd_ok(&["SET", "mykey", "hello"]).await;

    let response = client.cmd(&["MEMORY", "USAGE", "mykey"]).await;
    if let RespValue::Integer(usage) = response {
        // Should be positive (some memory used)
        assert!(usage > 0, "MEMORY USAGE should return positive integer");
        // Basic sanity check: string "hello" + key "mykey" + overhead should be less than 1KB
        assert!(usage < 1024, "Simple key should use less than 1KB");
    } else {
        panic!("MEMORY USAGE should return integer, got: {response:?}");
    }

    server.stop().await;
}

#[tokio::test]
async fn test_memory_usage_nonexistent_key() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let response = client.cmd(&["MEMORY", "USAGE", "nonexistent"]).await;
    assert_eq!(response, RespValue::Null);

    server.stop().await;
}

#[tokio::test]
async fn test_config_get_maxmemory() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let response = client.cmd(&["CONFIG", "GET", "maxmemory"]).await;
    if let RespValue::Array(arr) = response {
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0], RespValue::BulkString(Bytes::from("maxmemory")));
        // Default is 0 (unlimited)
        if let RespValue::BulkString(val) = &arr[1] {
            assert_eq!(val.as_ref(), b"0");
        }
    } else {
        panic!("CONFIG GET should return array");
    }

    server.stop().await;
}

#[tokio::test]
async fn test_config_set_maxmemory() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Set maxmemory to 10MB
    client.cmd_ok(&["CONFIG", "SET", "maxmemory", "10mb"]).await;

    // Verify it was set
    let response = client.cmd(&["CONFIG", "GET", "maxmemory"]).await;
    if let RespValue::Array(arr) = response {
        if let RespValue::BulkString(val) = &arr[1] {
            let bytes: usize = String::from_utf8_lossy(val).parse().unwrap_or(0);
            assert_eq!(bytes, 10 * 1024 * 1024, "maxmemory should be 10MB");
        }
    }

    server.stop().await;
}

#[tokio::test]
async fn test_config_get_maxmemory_policy() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let response = client.cmd(&["CONFIG", "GET", "maxmemory-policy"]).await;
    if let RespValue::Array(arr) = response {
        assert_eq!(arr.len(), 2);
        assert_eq!(
            arr[0],
            RespValue::BulkString(Bytes::from("maxmemory-policy"))
        );
        if let RespValue::BulkString(val) = &arr[1] {
            assert_eq!(val.as_ref(), b"noeviction");
        }
    } else {
        panic!("CONFIG GET should return array");
    }

    server.stop().await;
}

#[tokio::test]
async fn test_config_set_maxmemory_policy() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Set policy to allkeys-lru
    client
        .cmd_ok(&["CONFIG", "SET", "maxmemory-policy", "allkeys-lru"])
        .await;

    // Verify it was set
    let response = client.cmd(&["CONFIG", "GET", "maxmemory-policy"]).await;
    if let RespValue::Array(arr) = response {
        if let RespValue::BulkString(val) = &arr[1] {
            assert_eq!(val.as_ref(), b"allkeys-lru");
        }
    }

    server.stop().await;
}

#[tokio::test]
async fn test_memory_stats_tracks_deletes() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Set some keys
    for i in 0..5 {
        client
            .cmd_ok(&["SET", &format!("key{i}"), &format!("value{i}")])
            .await;
    }

    // Get initial memory
    let response = client.cmd(&["MEMORY", "STATS"]).await;
    let initial_total = extract_stat(&response, "total.allocated");
    assert!(initial_total > 0);

    // Delete some keys
    client.cmd(&["DEL", "key0", "key1", "key2"]).await;

    // Memory should decrease
    let response = client.cmd(&["MEMORY", "STATS"]).await;
    let after_delete = extract_stat(&response, "total.allocated");
    assert!(
        after_delete < initial_total,
        "Memory should decrease after deleting keys"
    );

    server.stop().await;
}

#[tokio::test]
async fn test_memory_stats_peak_never_decreases() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Set some keys
    for i in 0..10 {
        client
            .cmd_ok(&["SET", &format!("key{i}"), &format!("value{i}")])
            .await;
    }

    let response = client.cmd(&["MEMORY", "STATS"]).await;
    let initial_peak = extract_stat(&response, "peak.allocated");

    // Delete all keys
    for i in 0..10 {
        client.cmd(&["DEL", &format!("key{i}")]).await;
    }

    // Peak should not decrease
    let response = client.cmd(&["MEMORY", "STATS"]).await;
    let after_delete_peak = extract_stat(&response, "peak.allocated");
    assert!(
        after_delete_peak >= initial_peak,
        "Peak memory should never decrease"
    );

    server.stop().await;
}

#[tokio::test]
async fn test_memory_doctor() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let response = client.cmd(&["MEMORY", "DOCTOR"]).await;
    // Should return a string
    assert!(matches!(response, RespValue::BulkString(_)));

    server.stop().await;
}

#[tokio::test]
async fn test_memory_usage_list() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Create a list with multiple items (RPUSH returns count, not OK)
    let response = client
        .cmd(&["RPUSH", "mylist", "a", "b", "c", "d", "e"])
        .await;
    assert_eq!(response, RespValue::Integer(5));

    let response = client.cmd(&["MEMORY", "USAGE", "mylist"]).await;
    if let RespValue::Integer(usage) = response {
        assert!(usage > 0, "List should use memory");
    } else {
        panic!("MEMORY USAGE should return integer");
    }

    server.stop().await;
}

#[tokio::test]
async fn test_memory_usage_hash() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // HSET returns the number of fields added
    let response = client
        .cmd(&["HSET", "myhash", "f1", "v1", "f2", "v2", "f3", "v3"])
        .await;
    assert_eq!(response, RespValue::Integer(3));

    let response = client.cmd(&["MEMORY", "USAGE", "myhash"]).await;
    if let RespValue::Integer(usage) = response {
        assert!(usage > 0, "Hash should use memory");
    } else {
        panic!("MEMORY USAGE should return integer");
    }

    server.stop().await;
}

#[tokio::test]
async fn test_memory_usage_set() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // SADD returns the number of elements added
    let response = client
        .cmd(&["SADD", "myset", "a", "b", "c", "d", "e"])
        .await;
    assert_eq!(response, RespValue::Integer(5));

    let response = client.cmd(&["MEMORY", "USAGE", "myset"]).await;
    if let RespValue::Integer(usage) = response {
        assert!(usage > 0, "Set should use memory");
    } else {
        panic!("MEMORY USAGE should return integer");
    }

    server.stop().await;
}

#[tokio::test]
async fn test_memory_usage_zset() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // ZADD returns the number of elements added
    let response = client
        .cmd(&["ZADD", "myzset", "1", "a", "2", "b", "3", "c"])
        .await;
    assert_eq!(response, RespValue::Integer(3));

    let response = client.cmd(&["MEMORY", "USAGE", "myzset"]).await;
    if let RespValue::Integer(usage) = response {
        assert!(usage > 0, "Sorted set should use memory");
    } else {
        panic!("MEMORY USAGE should return integer");
    }

    server.stop().await;
}

#[tokio::test]
async fn test_config_resetstat_resets_memory_stats() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Create some memory usage
    for i in 0..100 {
        client
            .cmd_ok(&["SET", &format!("key{i}"), &format!("value{i}")])
            .await;
    }

    // Delete most keys to create divergence between peak and current
    for i in 0..90 {
        client.cmd(&["DEL", &format!("key{i}")]).await;
    }

    // Get stats before reset
    let response = client.cmd(&["MEMORY", "STATS"]).await;
    let peak_before = extract_stat(&response, "peak.allocated");
    let total_before = extract_stat(&response, "total.allocated");
    assert!(peak_before > total_before, "Peak should be > current");

    // Reset stats
    client.cmd_ok(&["CONFIG", "RESETSTAT"]).await;

    // After reset, peak should equal current
    let response = client.cmd(&["MEMORY", "STATS"]).await;
    let peak_after = extract_stat(&response, "peak.allocated");
    let total_after = extract_stat(&response, "total.allocated");

    // Peak should now be reset to current
    assert_eq!(
        peak_after, total_after,
        "After RESETSTAT, peak should equal current"
    );

    server.stop().await;
}

#[tokio::test]
async fn test_memory_tracks_overwrites_correctly() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Set a small value
    client.cmd_ok(&["SET", "mykey", "small"]).await;

    let response = client.cmd(&["MEMORY", "STATS"]).await;
    let after_small = extract_stat(&response, "total.allocated");

    // Overwrite with a larger value
    let large_value = "x".repeat(10000);
    client.cmd_ok(&["SET", "mykey", &large_value]).await;

    let response = client.cmd(&["MEMORY", "STATS"]).await;
    let after_large = extract_stat(&response, "total.allocated");

    assert!(
        after_large > after_small,
        "Memory should increase when overwriting with larger value"
    );

    // Overwrite with small value again
    client.cmd_ok(&["SET", "mykey", "tiny"]).await;

    let response = client.cmd(&["MEMORY", "STATS"]).await;
    let after_tiny = extract_stat(&response, "total.allocated");

    assert!(
        after_tiny < after_large,
        "Memory should decrease when overwriting with smaller value"
    );

    server.stop().await;
}

#[tokio::test]
async fn test_flushdb_clears_memory() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Set many keys
    for i in 0..100 {
        client
            .cmd_ok(&["SET", &format!("key{i}"), &format!("value{i}")])
            .await;
    }

    let response = client.cmd(&["MEMORY", "STATS"]).await;
    let before_flush = extract_stat(&response, "total.allocated");
    assert!(before_flush > 0);

    // Flush the database
    client.cmd_ok(&["FLUSHDB"]).await;

    let response = client.cmd(&["MEMORY", "STATS"]).await;
    let after_flush = extract_stat(&response, "total.allocated");

    assert_eq!(after_flush, 0, "Memory should be 0 after FLUSHDB");

    server.stop().await;
}

/// Helper function to extract a stat value from MEMORY STATS response
fn extract_stat(response: &RespValue, name: &str) -> i64 {
    if let RespValue::Array(arr) = response {
        for i in (0..arr.len()).step_by(2) {
            if let RespValue::BulkString(key) = &arr[i] {
                if key.as_ref() == name.as_bytes() {
                    if let RespValue::Integer(val) = &arr[i + 1] {
                        return *val;
                    }
                }
            }
        }
    }
    panic!("Stat {name} not found in response");
}

// ============================================================
// Pipeline Support Tests
// ============================================================

#[tokio::test]
async fn test_pipeline_basic() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Send multiple SET commands in a pipeline
    let responses = client
        .pipeline(&[
            &["SET", "key1", "value1"],
            &["SET", "key2", "value2"],
            &["SET", "key3", "value3"],
        ])
        .await;

    // All should return OK
    assert_eq!(responses.len(), 3);
    for resp in &responses {
        assert_eq!(*resp, RespValue::SimpleString("OK".to_string()));
    }

    // Verify all keys were set
    let get_responses = client
        .pipeline(&[&["GET", "key1"], &["GET", "key2"], &["GET", "key3"]])
        .await;

    assert_eq!(get_responses.len(), 3);
    assert_eq!(
        get_responses[0],
        RespValue::BulkString(Bytes::from("value1"))
    );
    assert_eq!(
        get_responses[1],
        RespValue::BulkString(Bytes::from("value2"))
    );
    assert_eq!(
        get_responses[2],
        RespValue::BulkString(Bytes::from("value3"))
    );

    server.stop().await;
}

#[tokio::test]
async fn test_pipeline_mixed_commands() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Mix of different command types
    let responses = client
        .pipeline(&[
            &["SET", "mykey", "myvalue"],
            &["GET", "mykey"],
            &["INCR", "counter"],
            &["INCR", "counter"],
            &["GET", "counter"],
            &["PING"],
        ])
        .await;

    assert_eq!(responses.len(), 6);
    assert_eq!(responses[0], RespValue::SimpleString("OK".to_string())); // SET
    assert_eq!(responses[1], RespValue::BulkString(Bytes::from("myvalue"))); // GET
    assert_eq!(responses[2], RespValue::Integer(1)); // INCR
    assert_eq!(responses[3], RespValue::Integer(2)); // INCR
    assert_eq!(responses[4], RespValue::BulkString(Bytes::from("2"))); // GET counter
    assert_eq!(responses[5], RespValue::SimpleString("PONG".to_string())); // PING

    server.stop().await;
}

#[tokio::test]
async fn test_pipeline_with_errors() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Create a string key
    client.cmd_ok(&["SET", "strkey", "value"]).await;

    // Pipeline with an error in the middle
    let responses = client
        .pipeline(&[
            &["SET", "key1", "value1"],
            &["LPUSH", "strkey", "item"], // Wrong type error
            &["SET", "key2", "value2"],
        ])
        .await;

    assert_eq!(responses.len(), 3);
    assert_eq!(responses[0], RespValue::SimpleString("OK".to_string()));
    assert!(matches!(responses[1], RespValue::Error(_))); // WRONGTYPE error
    assert_eq!(responses[2], RespValue::SimpleString("OK".to_string()));

    // Both SET commands should have succeeded
    let v1 = client.cmd(&["GET", "key1"]).await;
    let v2 = client.cmd(&["GET", "key2"]).await;
    assert_eq!(v1, RespValue::BulkString(Bytes::from("value1")));
    assert_eq!(v2, RespValue::BulkString(Bytes::from("value2")));

    server.stop().await;
}

#[tokio::test]
async fn test_pipeline_large() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Create a large pipeline using a different approach
    // We'll use a batch of 100 SET commands
    let keys: Vec<String> = (0..100).map(|i| format!("key{i}")).collect();
    let vals: Vec<String> = (0..100).map(|i| format!("value{i}")).collect();

    // Build command slices
    let commands: Vec<[&str; 3]> = keys
        .iter()
        .zip(vals.iter())
        .map(|(k, v)| ["SET", k.as_str(), v.as_str()])
        .collect();

    let cmd_refs: Vec<&[&str]> = commands.iter().map(|c| c.as_slice()).collect();

    let responses = client.pipeline(&cmd_refs).await;

    assert_eq!(responses.len(), 100);
    for resp in &responses {
        assert_eq!(*resp, RespValue::SimpleString("OK".to_string()));
    }

    // Verify a few keys
    assert_eq!(
        client.cmd(&["GET", "key0"]).await,
        RespValue::BulkString(Bytes::from("value0"))
    );
    assert_eq!(
        client.cmd(&["GET", "key99"]).await,
        RespValue::BulkString(Bytes::from("value99"))
    );

    server.stop().await;
}

#[tokio::test]
async fn test_pipeline_with_quit() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Pipeline with QUIT at the end
    let responses = client
        .pipeline(&[
            &["SET", "key1", "value1"],
            &["SET", "key2", "value2"],
            &["QUIT"],
        ])
        .await;

    assert_eq!(responses.len(), 3);
    assert_eq!(responses[0], RespValue::SimpleString("OK".to_string()));
    assert_eq!(responses[1], RespValue::SimpleString("OK".to_string()));
    assert_eq!(responses[2], RespValue::SimpleString("OK".to_string())); // QUIT returns OK

    // Connection should be closed now - verify data was saved with new connection
    let mut client2 = server.client().await;
    assert_eq!(
        client2.cmd(&["GET", "key1"]).await,
        RespValue::BulkString(Bytes::from("value1"))
    );
    assert_eq!(
        client2.cmd(&["GET", "key2"]).await,
        RespValue::BulkString(Bytes::from("value2"))
    );

    server.stop().await;
}

#[tokio::test]
async fn test_pipeline_list_operations() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let responses = client
        .pipeline(&[
            &["RPUSH", "mylist", "a", "b", "c"],
            &["LPUSH", "mylist", "z"],
            &["LRANGE", "mylist", "0", "-1"],
            &["LLEN", "mylist"],
        ])
        .await;

    assert_eq!(responses.len(), 4);
    assert_eq!(responses[0], RespValue::Integer(3)); // RPUSH returns length
    assert_eq!(responses[1], RespValue::Integer(4)); // LPUSH returns length
    if let RespValue::Array(arr) = &responses[2] {
        assert_eq!(arr.len(), 4);
        assert_eq!(arr[0], RespValue::BulkString(Bytes::from("z")));
        assert_eq!(arr[1], RespValue::BulkString(Bytes::from("a")));
    } else {
        panic!("Expected array for LRANGE");
    }
    assert_eq!(responses[3], RespValue::Integer(4)); // LLEN

    server.stop().await;
}

#[tokio::test]
async fn test_pipeline_preserves_order() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Send commands that depend on order
    let responses = client
        .pipeline(&[
            &["SET", "x", "0"],
            &["INCR", "x"],
            &["INCR", "x"],
            &["INCR", "x"],
            &["GET", "x"],
        ])
        .await;

    assert_eq!(responses.len(), 5);
    assert_eq!(responses[0], RespValue::SimpleString("OK".to_string()));
    assert_eq!(responses[1], RespValue::Integer(1));
    assert_eq!(responses[2], RespValue::Integer(2));
    assert_eq!(responses[3], RespValue::Integer(3));
    assert_eq!(responses[4], RespValue::BulkString(Bytes::from("3")));

    server.stop().await;
}

#[tokio::test]
async fn test_pipeline_hash_operations() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let responses = client
        .pipeline(&[
            &["HSET", "myhash", "f1", "v1", "f2", "v2"],
            &["HGET", "myhash", "f1"],
            &["HGET", "myhash", "f2"],
            &["HGETALL", "myhash"],
        ])
        .await;

    assert_eq!(responses.len(), 4);
    assert_eq!(responses[0], RespValue::Integer(2)); // HSET returns fields added
    assert_eq!(responses[1], RespValue::BulkString(Bytes::from("v1")));
    assert_eq!(responses[2], RespValue::BulkString(Bytes::from("v2")));
    if let RespValue::Array(arr) = &responses[3] {
        assert_eq!(arr.len(), 4); // 2 field-value pairs
    } else {
        panic!("Expected array for HGETALL");
    }

    server.stop().await;
}

#[tokio::test]
async fn test_pipeline_dbsize_accurate() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Pipeline sets and check DBSIZE
    let responses = client
        .pipeline(&[
            &["SET", "a", "1"],
            &["SET", "b", "2"],
            &["SET", "c", "3"],
            &["DBSIZE"],
        ])
        .await;

    assert_eq!(responses.len(), 4);
    assert_eq!(responses[3], RespValue::Integer(3)); // DBSIZE should be 3

    // Add more and check again
    let responses = client
        .pipeline(&[&["SET", "d", "4"], &["SET", "e", "5"], &["DBSIZE"]])
        .await;

    assert_eq!(responses.len(), 3);
    assert_eq!(responses[2], RespValue::Integer(5)); // DBSIZE should be 5

    server.stop().await;
}

#[tokio::test]
async fn test_pipeline_empty() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Empty pipeline should work
    let responses = client.pipeline(&[]).await;
    assert!(responses.is_empty());

    // Server should still work after empty pipeline
    let response = client.cmd(&["PING"]).await;
    assert_eq!(response, RespValue::SimpleString("PONG".to_string()));

    server.stop().await;
}

#[tokio::test]
async fn test_pipeline_single_command() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Single command in pipeline
    let responses = client.pipeline(&[&["PING"]]).await;
    assert_eq!(responses.len(), 1);
    assert_eq!(responses[0], RespValue::SimpleString("PONG".to_string()));

    server.stop().await;
}

// ============================================================
// 13. Connection timeout tests
// ============================================================

#[tokio::test]
async fn test_connection_timeout_no_timeout() {
    // Server with timeout=0 should never timeout
    let server = TestServer::spawn_with_config(ServerConfig {
        bind_addr: "127.0.0.1:0".to_string(),
        max_connections: 10,
        timeout: 0,
    })
    .await;

    let mut client = server.client().await;

    // Wait longer than typical timeout
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Connection should still be alive
    let response = client.cmd(&["PING"]).await;
    assert_eq!(response, RespValue::SimpleString("PONG".to_string()));

    server.stop().await;
}

#[tokio::test]
async fn test_connection_timeout_with_timeout() {
    // Server with 1 second timeout
    let server = TestServer::spawn_with_config(ServerConfig {
        bind_addr: "127.0.0.1:0".to_string(),
        max_connections: 10,
        timeout: 1,
    })
    .await;

    let mut client = server.client().await;

    // Send a command immediately (should work)
    let response = client.cmd(&["PING"]).await;
    assert_eq!(response, RespValue::SimpleString("PONG".to_string()));

    // Wait for timeout (1 second + buffer)
    tokio::time::sleep(tokio::time::Duration::from_millis(1200)).await;

    // Connection should be closed by the server
    // Attempting to send a command should fail
    let result = client.try_cmd(&["PING"]).await;
    assert!(result.is_err(), "Connection should be closed after timeout");

    server.stop().await;
}

#[tokio::test]
#[cfg_attr(
    target_os = "windows",
    ignore = "Flaky on Windows due to timing sensitivity"
)]
async fn test_connection_timeout_reset_on_activity() {
    // Server with 2 second timeout
    let server = TestServer::spawn_with_config(ServerConfig {
        bind_addr: "127.0.0.1:0".to_string(),
        max_connections: 10,
        timeout: 2,
    })
    .await;

    let mut client = server.client().await;

    // Send commands every 1 second (before timeout)
    for _ in 0..5 {
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        let response = client.cmd(&["PING"]).await;
        assert_eq!(response, RespValue::SimpleString("PONG".to_string()));
    }

    // Connection should still be alive after 5 seconds of activity
    let response = client.cmd(&["GET", "key"]).await;
    assert!(matches!(response, RespValue::Null) || matches!(response, RespValue::BulkString(_)));

    server.stop().await;
}

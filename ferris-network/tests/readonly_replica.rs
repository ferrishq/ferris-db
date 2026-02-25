//! Tests for read-only replica mode
//!
//! Verifies that replicas reject write commands and allow read commands.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
#![allow(clippy::uninlined_format_args)]

use ferris_protocol::RespValue;
use ferris_test_utils::TestServer;

// ============================================================
// READ-ONLY REPLICA TESTS
// ============================================================

#[tokio::test]
async fn test_replica_rejects_set() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Initially a master, SET should work
    let result = client.cmd(&["SET", "key", "value"]).await;
    assert!(matches!(result, RespValue::SimpleString(_)));

    // Become a replica (even though we can't actually connect to a master)
    // This just sets the replica state
    client.cmd(&["REPLICAOF", "127.0.0.1", "9999"]).await;

    // Give the async task time to set replica state
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Now SET should be rejected
    let result = client.cmd(&["SET", "key2", "value2"]).await;
    match result {
        RespValue::Error(e) => {
            assert!(e.contains("READONLY"), "Expected READONLY error, got: {}", e);
        }
        _ => panic!("Expected READONLY error, got: {:?}", result),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_replica_allows_get() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Set a value while master
    client.cmd(&["SET", "mykey", "myvalue"]).await;

    // Become a replica
    client.cmd(&["REPLICAOF", "127.0.0.1", "9999"]).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // GET should still work
    let result = client.cmd(&["GET", "mykey"]).await;
    assert_eq!(result, RespValue::bulk_string("myvalue"));

    server.stop().await;
}

#[tokio::test]
async fn test_replica_rejects_del() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Set a value while master
    client.cmd(&["SET", "key", "value"]).await;

    // Become a replica
    client.cmd(&["REPLICAOF", "127.0.0.1", "9999"]).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // DEL should be rejected
    let result = client.cmd(&["DEL", "key"]).await;
    match result {
        RespValue::Error(e) => {
            assert!(e.contains("READONLY"), "Expected READONLY error");
        }
        _ => panic!("Expected READONLY error, got: {:?}", result),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_replica_rejects_hset() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Become a replica
    client.cmd(&["REPLICAOF", "127.0.0.1", "9999"]).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // HSET should be rejected
    let result = client.cmd(&["HSET", "myhash", "field", "value"]).await;
    match result {
        RespValue::Error(e) => {
            assert!(e.contains("READONLY"), "Expected READONLY error");
        }
        _ => panic!("Expected READONLY error, got: {:?}", result),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_replica_allows_hget() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Set a hash while master
    client.cmd(&["HSET", "myhash", "field1", "value1"]).await;

    // Become a replica
    client.cmd(&["REPLICAOF", "127.0.0.1", "9999"]).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // HGET should still work
    let result = client.cmd(&["HGET", "myhash", "field1"]).await;
    assert_eq!(result, RespValue::bulk_string("value1"));

    server.stop().await;
}

#[tokio::test]
async fn test_replica_rejects_lpush() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Become a replica
    client.cmd(&["REPLICAOF", "127.0.0.1", "9999"]).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // LPUSH should be rejected
    let result = client.cmd(&["LPUSH", "mylist", "item"]).await;
    match result {
        RespValue::Error(e) => {
            assert!(e.contains("READONLY"), "Expected READONLY error");
        }
        _ => panic!("Expected READONLY error, got: {:?}", result),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_replica_allows_lrange() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Create a list while master
    client.cmd(&["LPUSH", "mylist", "item1", "item2"]).await;

    // Become a replica
    client.cmd(&["REPLICAOF", "127.0.0.1", "9999"]).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // LRANGE should still work
    let result = client.cmd(&["LRANGE", "mylist", "0", "-1"]).await;
    assert!(matches!(result, RespValue::Array(_)));

    server.stop().await;
}

#[tokio::test]
async fn test_replica_rejects_zadd() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Become a replica
    client.cmd(&["REPLICAOF", "127.0.0.1", "9999"]).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // ZADD should be rejected
    let result = client.cmd(&["ZADD", "myzset", "1.0", "member"]).await;
    match result {
        RespValue::Error(e) => {
            assert!(e.contains("READONLY"), "Expected READONLY error");
        }
        _ => panic!("Expected READONLY error, got: {:?}", result),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_replica_allows_zrange() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Create a sorted set while master
    client.cmd(&["ZADD", "myzset", "1.0", "member1"]).await;

    // Become a replica
    client.cmd(&["REPLICAOF", "127.0.0.1", "9999"]).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // ZRANGE should still work
    let result = client.cmd(&["ZRANGE", "myzset", "0", "-1"]).await;
    assert!(matches!(result, RespValue::Array(_)));

    server.stop().await;
}

#[tokio::test]
async fn test_replicaof_no_one_allows_writes() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Become a replica
    client.cmd(&["REPLICAOF", "127.0.0.1", "9999"]).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // SET should be rejected
    let result = client.cmd(&["SET", "key", "value"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    // Promote back to master
    client.cmd(&["REPLICAOF", "NO", "ONE"]).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Now SET should work again
    let result = client.cmd(&["SET", "key", "value"]).await;
    assert!(matches!(result, RespValue::SimpleString(_)));

    server.stop().await;
}

#[tokio::test]
async fn test_replica_rejects_incr() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Become a replica
    client.cmd(&["REPLICAOF", "127.0.0.1", "9999"]).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // INCR should be rejected (it's a write command)
    let result = client.cmd(&["INCR", "counter"]).await;
    match result {
        RespValue::Error(e) => {
            assert!(e.contains("READONLY"), "Expected READONLY error");
        }
        _ => panic!("Expected READONLY error, got: {:?}", result),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_replica_allows_exists() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Set a key while master
    client.cmd(&["SET", "mykey", "value"]).await;

    // Become a replica
    client.cmd(&["REPLICAOF", "127.0.0.1", "9999"]).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // EXISTS should still work (read-only command)
    let result = client.cmd(&["EXISTS", "mykey"]).await;
    assert_eq!(result, RespValue::Integer(1));

    server.stop().await;
}

#[tokio::test]
async fn test_replica_rejects_sadd() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Become a replica
    client.cmd(&["REPLICAOF", "127.0.0.1", "9999"]).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // SADD should be rejected
    let result = client.cmd(&["SADD", "myset", "member"]).await;
    match result {
        RespValue::Error(e) => {
            assert!(e.contains("READONLY"), "Expected READONLY error");
        }
        _ => panic!("Expected READONLY error, got: {:?}", result),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_replica_allows_smembers() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Create a set while master
    client.cmd(&["SADD", "myset", "member1", "member2"]).await;

    // Become a replica
    client.cmd(&["REPLICAOF", "127.0.0.1", "9999"]).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // SMEMBERS should still work
    let result = client.cmd(&["SMEMBERS", "myset"]).await;
    assert!(matches!(result, RespValue::Array(_)));

    server.stop().await;
}

//! Integration tests for replication consistency framework
//!
//! Tests that the `propagate_args` consistency waiting framework is in place
//! and working correctly. The actual consistency mode configuration will be
//! tested via server configuration when that feature is added.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
#![allow(clippy::uninlined_format_args)]

use bytes::Bytes;
use ferris_protocol::RespValue;
use ferris_test_utils::TestServer;
use std::time::Duration;
use tokio::time::sleep;

/// Helper to convert string to `RespValue`
fn resp_bulk(s: &str) -> RespValue {
    RespValue::BulkString(Bytes::from(s.to_string()))
}

/// Test that writes complete successfully (async mode is default)
#[tokio::test]
async fn test_write_completes_successfully() {
    let leader = TestServer::spawn().await;
    let mut leader_client = leader.client().await;

    // Write should complete successfully
    leader_client.cmd_ok(&["SET", "key1", "value1"]).await;

    // Verify value was set
    let result = leader_client.cmd(&["GET", "key1"]).await;
    assert_eq!(result, resp_bulk("value1"));

    leader.stop().await;
}

/// Test writes with replication established
#[tokio::test]
async fn test_writes_with_replication() {
    let leader = TestServer::spawn().await;
    let mut leader_client = leader.client().await;

    // Start a follower
    let follower = TestServer::spawn().await;
    let mut follower_client = follower.client().await;

    // Connect follower to leader
    let leader_addr = format!("127.0.0.1:{}", leader.port);
    let parts: Vec<&str> = leader_addr.split(':').collect();

    follower_client
        .cmd_ok(&["REPLICAOF", parts[0], parts[1]])
        .await;

    // Give replication time to establish
    sleep(Duration::from_millis(100)).await;

    // Write on leader should complete
    leader_client.cmd_ok(&["SET", "key2", "value2"]).await;

    // Verify value was set on leader
    let result = leader_client.cmd(&["GET", "key2"]).await;
    assert_eq!(result, resp_bulk("value2"));

    // Eventually, follower should also have the value
    sleep(Duration::from_millis(200)).await;
    let result = follower_client.cmd(&["GET", "key2"]).await;
    assert_eq!(result, resp_bulk("value2"));

    follower.stop().await;
    leader.stop().await;
}

/// Test multiple writes are all replicated
#[tokio::test]
async fn test_multiple_writes_replicated() {
    let leader = TestServer::spawn().await;
    let mut leader_client = leader.client().await;

    // Start a follower
    let follower = TestServer::spawn().await;
    let mut follower_client = follower.client().await;

    // Connect follower to leader
    let leader_addr = format!("127.0.0.1:{}", leader.port);
    let parts: Vec<&str> = leader_addr.split(':').collect();

    follower_client
        .cmd_ok(&["REPLICAOF", parts[0], parts[1]])
        .await;

    // Give replication time to establish
    sleep(Duration::from_millis(200)).await;

    // Perform multiple writes
    for i in 0..10 {
        let key = format!("key{}", i);
        let value = format!("value{}", i);

        leader_client.cmd_ok(&["SET", &key, &value]).await;
    }

    // Wait for replication
    sleep(Duration::from_millis(300)).await;

    // Verify all values on leader
    for i in 0..10 {
        let key = format!("key{}", i);
        let value = format!("value{}", i);

        let result = leader_client.cmd(&["GET", &key]).await;
        assert_eq!(result, resp_bulk(&value));
    }

    // Verify all values on follower
    for i in 0..10 {
        let key = format!("key{}", i);
        let value = format!("value{}", i);

        let result = follower_client.cmd(&["GET", &key]).await;
        assert_eq!(result, resp_bulk(&value));
    }

    follower.stop().await;
    leader.stop().await;
}

/// Test writes with various command types
#[tokio::test]
async fn test_various_commands_replicated() {
    let leader = TestServer::spawn().await;
    let mut leader_client = leader.client().await;

    // Start a follower
    let follower = TestServer::spawn().await;
    let mut follower_client = follower.client().await;

    // Connect follower to leader
    let leader_addr = format!("127.0.0.1:{}", leader.port);
    let parts: Vec<&str> = leader_addr.split(':').collect();

    follower_client
        .cmd_ok(&["REPLICAOF", parts[0], parts[1]])
        .await;

    // Give replication time to establish
    sleep(Duration::from_millis(200)).await;

    // Test various write commands
    leader_client.cmd_ok(&["SET", "string_key", "value"]).await;
    leader_client
        .cmd(&["LPUSH", "list_key", "item1", "item2"])
        .await; // Returns count
    leader_client
        .cmd(&["SADD", "set_key", "member1", "member2"])
        .await; // Returns count
    leader_client
        .cmd(&["HSET", "hash_key", "field1", "value1"])
        .await; // Returns count
    leader_client
        .cmd(&["ZADD", "zset_key", "1.0", "member1"])
        .await; // Returns count

    // Wait for replication
    sleep(Duration::from_millis(300)).await;

    // Verify all on follower
    let result = follower_client.cmd(&["GET", "string_key"]).await;
    assert_eq!(result, resp_bulk("value"));

    let result = follower_client.cmd(&["LLEN", "list_key"]).await;
    assert_eq!(result, RespValue::Integer(2));

    let result = follower_client.cmd(&["SCARD", "set_key"]).await;
    assert_eq!(result, RespValue::Integer(2));

    let result = follower_client.cmd(&["HGET", "hash_key", "field1"]).await;
    assert_eq!(result, resp_bulk("value1"));

    let result = follower_client.cmd(&["ZCARD", "zset_key"]).await;
    assert_eq!(result, RespValue::Integer(1));

    follower.stop().await;
    leader.stop().await;
}

/// Test that read commands work normally and don't affect replication
#[tokio::test]
async fn test_read_commands_work_normally() {
    let leader = TestServer::spawn().await;
    let mut leader_client = leader.client().await;

    // Write a key
    leader_client.cmd_ok(&["SET", "readkey", "readvalue"]).await;

    // Read should complete immediately
    let start = std::time::Instant::now();
    let result = leader_client.cmd(&["GET", "readkey"]).await;
    let elapsed = start.elapsed();

    assert_eq!(result, resp_bulk("readvalue"));
    assert!(elapsed < Duration::from_millis(100), "Read should be fast");

    leader.stop().await;
}

/// Test writes with two followers
#[tokio::test]
async fn test_writes_with_multiple_followers() {
    let leader = TestServer::spawn().await;
    let mut leader_client = leader.client().await;

    // Start two followers
    let follower1 = TestServer::spawn().await;
    let mut follower1_client = follower1.client().await;

    let follower2 = TestServer::spawn().await;
    let mut follower2_client = follower2.client().await;

    // Connect followers to leader
    let leader_addr = format!("127.0.0.1:{}", leader.port);
    let parts: Vec<&str> = leader_addr.split(':').collect();

    follower1_client
        .cmd_ok(&["REPLICAOF", parts[0], parts[1]])
        .await;

    follower2_client
        .cmd_ok(&["REPLICAOF", parts[0], parts[1]])
        .await;

    // Give replication time to establish
    sleep(Duration::from_millis(200)).await;

    // Write on leader
    leader_client.cmd_ok(&["SET", "key3", "value3"]).await;

    // Wait for replication
    sleep(Duration::from_millis(300)).await;

    // Verify value on leader
    let result = leader_client.cmd(&["GET", "key3"]).await;
    assert_eq!(result, resp_bulk("value3"));

    // Both followers should have the value
    let result = follower1_client.cmd(&["GET", "key3"]).await;
    assert_eq!(result, resp_bulk("value3"));

    let result = follower2_client.cmd(&["GET", "key3"]).await;
    assert_eq!(result, resp_bulk("value3"));

    follower2.stop().await;
    follower1.stop().await;
    leader.stop().await;
}

/// Test writes complete even with slow/disconnected followers
#[tokio::test]
async fn test_writes_complete_without_followers() {
    let leader = TestServer::spawn().await;
    let mut leader_client = leader.client().await;

    // Write should complete successfully even with no followers
    let start = std::time::Instant::now();
    leader_client.cmd_ok(&["SET", "key4", "value4"]).await;
    let elapsed = start.elapsed();

    // Should complete quickly in async mode (default)
    assert!(
        elapsed < Duration::from_millis(500),
        "Write should complete quickly: {:?}",
        elapsed
    );

    // Verify value was set
    let result = leader_client.cmd(&["GET", "key4"]).await;
    assert_eq!(result, resp_bulk("value4"));

    leader.stop().await;
}

/// Test that consistency framework doesn't break normal operations
#[tokio::test]
async fn test_consistency_framework_transparent() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Perform a series of mixed operations
    client.cmd_ok(&["SET", "k1", "v1"]).await;
    client.cmd(&["LPUSH", "list", "a"]).await; // Returns count
    client.cmd(&["SADD", "set", "x"]).await; // Returns count

    let r1 = client.cmd(&["GET", "k1"]).await;
    assert_eq!(r1, resp_bulk("v1"));

    let r2 = client.cmd(&["LLEN", "list"]).await;
    assert_eq!(r2, RespValue::Integer(1));

    let r3 = client.cmd(&["SCARD", "set"]).await;
    assert_eq!(r3, RespValue::Integer(1));

    server.stop().await;
}

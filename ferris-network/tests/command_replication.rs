//! Integration tests for command replication
//!
//! These tests verify that commands executed on a leader are replicated
//! to followers and applied correctly.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
#![allow(clippy::uninlined_format_args)]

use ferris_protocol::RespValue;
use ferris_test_utils::TestServer;
use tokio::time::{sleep, Duration};

/// Helper to wait for replication to propagate
async fn wait_for_replication() {
    sleep(Duration::from_millis(1000)).await;
}

#[tokio::test]
async fn test_simple_command_replication() {
    // Spawn leader
    let leader = TestServer::spawn().await;
    let mut leader_client = leader.client().await;

    // Spawn follower
    let follower = TestServer::spawn().await;
    let mut follower_client = follower.client().await;

    // Make follower replicate from leader
    let result = follower_client
        .cmd(&["REPLICAOF", "127.0.0.1", &leader.port.to_string()])
        .await;
    assert_eq!(result, RespValue::SimpleString("OK".to_string()));

    // Wait for replication to start
    wait_for_replication().await;

    // Write on leader
    leader_client.cmd(&["SET", "key1", "value1"]).await;

    // Wait for replication
    wait_for_replication().await;

    // Read from follower (should have the value)
    let result = follower_client.cmd(&["GET", "key1"]).await;
    assert_eq!(result, RespValue::bulk_string("value1"));

    // Cleanup
    follower.stop().await;
    leader.stop().await;
}

#[tokio::test]
async fn test_multiple_commands_replication() {
    let leader = TestServer::spawn().await;
    let mut leader_client = leader.client().await;

    let follower = TestServer::spawn().await;
    let mut follower_client = follower.client().await;

    // Setup replication
    follower_client
        .cmd(&["REPLICAOF", "127.0.0.1", &leader.port.to_string()])
        .await;
    wait_for_replication().await;

    // Execute multiple commands on leader
    leader_client.cmd(&["SET", "key1", "value1"]).await;
    leader_client.cmd(&["SET", "key2", "value2"]).await;
    leader_client.cmd(&["SET", "key3", "value3"]).await;

    wait_for_replication().await;

    // Verify all commands were replicated
    assert_eq!(
        follower_client.cmd(&["GET", "key1"]).await,
        RespValue::bulk_string("value1")
    );
    assert_eq!(
        follower_client.cmd(&["GET", "key2"]).await,
        RespValue::bulk_string("value2")
    );
    assert_eq!(
        follower_client.cmd(&["GET", "key3"]).await,
        RespValue::bulk_string("value3")
    );

    follower.stop().await;
    leader.stop().await;
}

#[tokio::test]
async fn test_del_command_replication() {
    let leader = TestServer::spawn().await;
    let mut leader_client = leader.client().await;

    let follower = TestServer::spawn().await;
    let mut follower_client = follower.client().await;

    // Setup replication
    follower_client
        .cmd(&["REPLICAOF", "127.0.0.1", &leader.port.to_string()])
        .await;
    wait_for_replication().await;

    // Set and delete on leader
    leader_client.cmd(&["SET", "key1", "value1"]).await;
    wait_for_replication().await;

    // Verify key exists on follower
    assert_eq!(
        follower_client.cmd(&["GET", "key1"]).await,
        RespValue::bulk_string("value1")
    );

    // Delete on leader
    leader_client.cmd(&["DEL", "key1"]).await;
    wait_for_replication().await;

    // Verify key is deleted on follower
    assert_eq!(follower_client.cmd(&["GET", "key1"]).await, RespValue::Null);

    follower.stop().await;
    leader.stop().await;
}

#[tokio::test]
async fn test_incr_command_replication() {
    let leader = TestServer::spawn().await;
    let mut leader_client = leader.client().await;

    let follower = TestServer::spawn().await;
    let mut follower_client = follower.client().await;

    // Setup replication
    follower_client
        .cmd(&["REPLICAOF", "127.0.0.1", &leader.port.to_string()])
        .await;
    wait_for_replication().await;

    // INCR multiple times on leader
    leader_client.cmd(&["INCR", "counter"]).await;
    leader_client.cmd(&["INCR", "counter"]).await;
    leader_client.cmd(&["INCR", "counter"]).await;

    wait_for_replication().await;

    // Verify counter value on follower
    assert_eq!(
        follower_client.cmd(&["GET", "counter"]).await,
        RespValue::bulk_string("3")
    );

    follower.stop().await;
    leader.stop().await;
}

#[tokio::test]
async fn test_list_commands_replication() {
    let leader = TestServer::spawn().await;
    let mut leader_client = leader.client().await;

    let follower = TestServer::spawn().await;
    let mut follower_client = follower.client().await;

    // Setup replication
    follower_client
        .cmd(&["REPLICAOF", "127.0.0.1", &leader.port.to_string()])
        .await;
    wait_for_replication().await;

    // Execute list commands on leader
    leader_client.cmd(&["LPUSH", "mylist", "a"]).await;
    leader_client.cmd(&["LPUSH", "mylist", "b"]).await;
    leader_client.cmd(&["LPUSH", "mylist", "c"]).await;

    wait_for_replication().await;

    // Verify list on follower
    let result = follower_client.cmd(&["LRANGE", "mylist", "0", "-1"]).await;
    if let RespValue::Array(items) = result {
        assert_eq!(items.len(), 3);
        assert_eq!(items[0], RespValue::bulk_string("c"));
        assert_eq!(items[1], RespValue::bulk_string("b"));
        assert_eq!(items[2], RespValue::bulk_string("a"));
    } else {
        panic!("Expected array response");
    }

    follower.stop().await;
    leader.stop().await;
}

#[tokio::test]
async fn test_hash_commands_replication() {
    let leader = TestServer::spawn().await;
    let mut leader_client = leader.client().await;

    let follower = TestServer::spawn().await;
    let mut follower_client = follower.client().await;

    // Setup replication
    follower_client
        .cmd(&["REPLICAOF", "127.0.0.1", &leader.port.to_string()])
        .await;
    wait_for_replication().await;

    // Execute hash commands on leader
    leader_client
        .cmd(&["HSET", "myhash", "field1", "value1"])
        .await;
    leader_client
        .cmd(&["HSET", "myhash", "field2", "value2"])
        .await;

    wait_for_replication().await;

    // Verify hash on follower
    assert_eq!(
        follower_client.cmd(&["HGET", "myhash", "field1"]).await,
        RespValue::bulk_string("value1")
    );
    assert_eq!(
        follower_client.cmd(&["HGET", "myhash", "field2"]).await,
        RespValue::bulk_string("value2")
    );

    follower.stop().await;
    leader.stop().await;
}

#[tokio::test]
async fn test_set_commands_replication() {
    let leader = TestServer::spawn().await;
    let mut leader_client = leader.client().await;

    let follower = TestServer::spawn().await;
    let mut follower_client = follower.client().await;

    // Setup replication
    follower_client
        .cmd(&["REPLICAOF", "127.0.0.1", &leader.port.to_string()])
        .await;
    wait_for_replication().await;

    // Execute set commands on leader
    leader_client.cmd(&["SADD", "myset", "a", "b", "c"]).await;

    wait_for_replication().await;

    // Verify set on follower
    let result = follower_client.cmd(&["SMEMBERS", "myset"]).await;
    if let RespValue::Array(items) = result {
        assert_eq!(items.len(), 3);
        // Order is not guaranteed, just check presence
        assert!(items.contains(&RespValue::bulk_string("a")));
        assert!(items.contains(&RespValue::bulk_string("b")));
        assert!(items.contains(&RespValue::bulk_string("c")));
    } else {
        panic!("Expected array response");
    }

    follower.stop().await;
    leader.stop().await;
}

#[tokio::test]
async fn test_sorted_set_commands_replication() {
    let leader = TestServer::spawn().await;
    let mut leader_client = leader.client().await;

    let follower = TestServer::spawn().await;
    let mut follower_client = follower.client().await;

    // Setup replication
    follower_client
        .cmd(&["REPLICAOF", "127.0.0.1", &leader.port.to_string()])
        .await;
    wait_for_replication().await;

    // Execute sorted set commands on leader
    leader_client
        .cmd(&["ZADD", "myzset", "1", "one", "2", "two", "3", "three"])
        .await;

    wait_for_replication().await;

    // Verify sorted set on follower
    let result = follower_client.cmd(&["ZRANGE", "myzset", "0", "-1"]).await;
    if let RespValue::Array(items) = result {
        assert_eq!(items.len(), 3);
        assert_eq!(items[0], RespValue::bulk_string("one"));
        assert_eq!(items[1], RespValue::bulk_string("two"));
        assert_eq!(items[2], RespValue::bulk_string("three"));
    } else {
        panic!("Expected array response");
    }

    follower.stop().await;
    leader.stop().await;
}

#[tokio::test]
async fn test_expire_command_replication() {
    let leader = TestServer::spawn().await;
    let mut leader_client = leader.client().await;

    let follower = TestServer::spawn().await;
    let mut follower_client = follower.client().await;

    // Setup replication
    follower_client
        .cmd(&["REPLICAOF", "127.0.0.1", &leader.port.to_string()])
        .await;
    wait_for_replication().await;

    // Set key with expiry on leader
    leader_client.cmd(&["SET", "expiring", "value"]).await;
    leader_client.cmd(&["EXPIRE", "expiring", "1"]).await;

    wait_for_replication().await;

    // Verify key exists on follower
    assert_eq!(
        follower_client.cmd(&["GET", "expiring"]).await,
        RespValue::bulk_string("value")
    );

    // Wait for expiry
    sleep(Duration::from_secs(2)).await;

    // Verify key expired on both leader and follower
    assert_eq!(
        leader_client.cmd(&["GET", "expiring"]).await,
        RespValue::Null
    );
    assert_eq!(
        follower_client.cmd(&["GET", "expiring"]).await,
        RespValue::Null
    );

    follower.stop().await;
    leader.stop().await;
}

#[tokio::test]
async fn test_database_selection_replication() {
    let leader = TestServer::spawn().await;
    let mut leader_client = leader.client().await;

    let follower = TestServer::spawn().await;
    let mut follower_client = follower.client().await;

    // Setup replication
    follower_client
        .cmd(&["REPLICAOF", "127.0.0.1", &leader.port.to_string()])
        .await;
    wait_for_replication().await;

    // Set key in database 0 on leader
    leader_client.cmd(&["SET", "key0", "value0"]).await;

    // Switch to database 1 and set key
    leader_client.cmd(&["SELECT", "1"]).await;
    leader_client.cmd(&["SET", "key1", "value1"]).await;

    wait_for_replication().await;

    // Verify on follower - database 0
    assert_eq!(
        follower_client.cmd(&["GET", "key0"]).await,
        RespValue::bulk_string("value0")
    );

    // Switch to database 1 on follower
    follower_client.cmd(&["SELECT", "1"]).await;
    assert_eq!(
        follower_client.cmd(&["GET", "key1"]).await,
        RespValue::bulk_string("value1")
    );

    // Verify database 0 key is not in database 1
    assert_eq!(follower_client.cmd(&["GET", "key0"]).await, RespValue::Null);

    follower.stop().await;
    leader.stop().await;
}

#[tokio::test]
async fn test_replication_after_follower_has_data() {
    let leader = TestServer::spawn().await;
    let mut leader_client = leader.client().await;

    let follower = TestServer::spawn().await;
    let mut follower_client = follower.client().await;

    // Set data on follower before replication
    follower_client
        .cmd(&["SET", "follower_key", "follower_value"])
        .await;

    // Setup replication
    follower_client
        .cmd(&["REPLICAOF", "127.0.0.1", &leader.port.to_string()])
        .await;
    wait_for_replication().await;

    // Write on leader
    leader_client
        .cmd(&["SET", "leader_key", "leader_value"])
        .await;
    wait_for_replication().await;

    // Verify leader's key is replicated
    assert_eq!(
        follower_client.cmd(&["GET", "leader_key"]).await,
        RespValue::bulk_string("leader_value")
    );

    // Follower's original data should still exist (not cleared by replication)
    assert_eq!(
        follower_client.cmd(&["GET", "follower_key"]).await,
        RespValue::bulk_string("follower_value")
    );

    follower.stop().await;
    leader.stop().await;
}

#[tokio::test]
async fn test_replicaof_no_one_allows_writes() {
    let leader = TestServer::spawn().await;
    let _leader_client = leader.client().await;

    let follower = TestServer::spawn().await;
    let mut follower_client = follower.client().await;

    // Setup replication
    follower_client
        .cmd(&["REPLICAOF", "127.0.0.1", &leader.port.to_string()])
        .await;
    wait_for_replication().await;

    // Write should fail on follower
    let result = follower_client.cmd(&["SET", "key", "value"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    // Promote follower to master
    follower_client.cmd(&["REPLICAOF", "NO", "ONE"]).await;
    wait_for_replication().await;

    // Now writes should work
    let result = follower_client.cmd(&["SET", "key", "value"]).await;
    assert_eq!(result, RespValue::ok());

    // Verify data
    assert_eq!(
        follower_client.cmd(&["GET", "key"]).await,
        RespValue::bulk_string("value")
    );

    follower.stop().await;
    leader.stop().await;
}

#[tokio::test]
async fn test_multiple_followers() {
    let leader = TestServer::spawn().await;
    let mut leader_client = leader.client().await;

    let follower1 = TestServer::spawn().await;
    let mut follower1_client = follower1.client().await;

    let follower2 = TestServer::spawn().await;
    let mut follower2_client = follower2.client().await;

    // Setup replication for both followers
    follower1_client
        .cmd(&["REPLICAOF", "127.0.0.1", &leader.port.to_string()])
        .await;
    follower2_client
        .cmd(&["REPLICAOF", "127.0.0.1", &leader.port.to_string()])
        .await;
    wait_for_replication().await;

    // Write on leader
    leader_client
        .cmd(&["SET", "shared_key", "shared_value"])
        .await;
    wait_for_replication().await;

    // Verify both followers received the data
    assert_eq!(
        follower1_client.cmd(&["GET", "shared_key"]).await,
        RespValue::bulk_string("shared_value")
    );
    assert_eq!(
        follower2_client.cmd(&["GET", "shared_key"]).await,
        RespValue::bulk_string("shared_value")
    );

    follower2.stop().await;
    follower1.stop().await;
    leader.stop().await;
}

#[tokio::test]
async fn test_replication_with_pipelined_commands() {
    let leader = TestServer::spawn().await;
    let mut leader_client = leader.client().await;

    let follower = TestServer::spawn().await;
    let mut follower_client = follower.client().await;

    // Setup replication
    follower_client
        .cmd(&["REPLICAOF", "127.0.0.1", &leader.port.to_string()])
        .await;
    wait_for_replication().await;

    // Execute many commands quickly (simulating pipelining)
    for i in 0..10 {
        leader_client
            .cmd(&["SET", &format!("key{}", i), &format!("value{}", i)])
            .await;
    }

    wait_for_replication().await;

    // Verify all commands were replicated
    for i in 0..10 {
        let expected_value = format!("value{}", i);
        assert_eq!(
            follower_client.cmd(&["GET", &format!("key{}", i)]).await,
            RespValue::bulk_string(expected_value)
        );
    }

    follower.stop().await;
    leader.stop().await;
}

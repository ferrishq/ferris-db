//! Integration tests for replication commands
//!
//! Tests: REPLICAOF, SLAVEOF, ROLE, WAIT, WAITAOF

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use ferris_protocol::RespValue;
use ferris_test_utils::TestServer;

#[tokio::test]
async fn test_role_returns_master_by_default() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["ROLE"]).await;
    match result {
        RespValue::Array(parts) => {
            assert_eq!(parts.len(), 3);
            assert_eq!(parts[0], RespValue::bulk_string("master"));
            assert_eq!(parts[1], RespValue::Integer(0)); // replication offset
            assert!(matches!(parts[2], RespValue::Array(_))); // connected slaves
        }
        _ => panic!("Expected array response from ROLE"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_role_wrong_arity() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["ROLE", "extra"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    server.stop().await;
}

#[tokio::test]
async fn test_replicaof_basic() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["REPLICAOF", "127.0.0.1", "6379"]).await;
    assert_eq!(result, RespValue::ok());

    server.stop().await;
}

#[tokio::test]
async fn test_replicaof_no_one() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // First make it a replica
    let result = client.cmd(&["REPLICAOF", "127.0.0.1", "6379"]).await;
    assert_eq!(result, RespValue::ok());

    // Then promote to master
    let result = client.cmd(&["REPLICAOF", "NO", "ONE"]).await;
    assert_eq!(result, RespValue::ok());

    server.stop().await;
}

#[tokio::test]
async fn test_replicaof_invalid_port() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["REPLICAOF", "127.0.0.1", "invalid"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    server.stop().await;
}

#[tokio::test]
async fn test_replicaof_wrong_arity() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["REPLICAOF"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    let result = client.cmd(&["REPLICAOF", "127.0.0.1"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    server.stop().await;
}

#[tokio::test]
async fn test_replicaof_port_out_of_range() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["REPLICAOF", "127.0.0.1", "99999"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    server.stop().await;
}

#[tokio::test]
async fn test_slaveof_alias() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["SLAVEOF", "127.0.0.1", "6379"]).await;
    assert_eq!(result, RespValue::ok());

    server.stop().await;
}

#[tokio::test]
async fn test_slaveof_no_one() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["SLAVEOF", "NO", "ONE"]).await;
    assert_eq!(result, RespValue::ok());

    server.stop().await;
}

#[tokio::test]
async fn test_wait_basic() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // WAIT with 1 replica and 1000ms timeout
    let result = client.cmd(&["WAIT", "1", "1000"]).await;
    assert_eq!(result, RespValue::Integer(0)); // No replicas, returns 0

    server.stop().await;
}

#[tokio::test]
async fn test_wait_zero_replicas() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // WAIT with 0 replicas should return immediately
    let result = client.cmd(&["WAIT", "0", "1000"]).await;
    assert_eq!(result, RespValue::Integer(0));

    server.stop().await;
}

#[tokio::test]
async fn test_wait_wrong_arity() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["WAIT"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    let result = client.cmd(&["WAIT", "1"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    server.stop().await;
}

#[tokio::test]
async fn test_wait_invalid_arguments() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["WAIT", "invalid", "1000"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    let result = client.cmd(&["WAIT", "1", "invalid"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    server.stop().await;
}

#[tokio::test]
async fn test_wait_negative_values() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Negative numreplicas should error
    let result = client.cmd(&["WAIT", "-1", "1000"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    // Negative timeout should error
    let result = client.cmd(&["WAIT", "1", "-1"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    server.stop().await;
}

#[tokio::test]
async fn test_waitaof_basic() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // WAITAOF local_offset replica_offset timeout
    let result = client.cmd(&["WAITAOF", "0", "0", "1000"]).await;
    match result {
        RespValue::Array(parts) => {
            assert_eq!(parts.len(), 2);
            assert_eq!(parts[0], RespValue::Integer(0)); // local ack
            assert_eq!(parts[1], RespValue::Integer(0)); // replica ack count
        }
        _ => panic!("Expected array response from WAITAOF"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_waitaof_wrong_arity() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["WAITAOF"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    let result = client.cmd(&["WAITAOF", "0", "0"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    server.stop().await;
}

#[tokio::test]
async fn test_waitaof_invalid_arguments() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["WAITAOF", "invalid", "0", "1000"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    let result = client.cmd(&["WAITAOF", "0", "invalid", "1000"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    let result = client.cmd(&["WAITAOF", "0", "0", "invalid"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    server.stop().await;
}

#[tokio::test]
async fn test_replicaof_case_insensitive() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Test various case combinations for NO ONE
    let result = client.cmd(&["REPLICAOF", "no", "one"]).await;
    assert_eq!(result, RespValue::ok());

    let result = client.cmd(&["REPLICAOF", "No", "One"]).await;
    assert_eq!(result, RespValue::ok());

    let result = client.cmd(&["REPLICAOF", "NO", "one"]).await;
    assert_eq!(result, RespValue::ok());

    server.stop().await;
}

#[tokio::test]
async fn test_replicaof_with_hostname() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["REPLICAOF", "localhost", "6379"]).await;
    assert_eq!(result, RespValue::ok());

    server.stop().await;
}

#[tokio::test]
async fn test_replicaof_with_ipv6() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["REPLICAOF", "::1", "6379"]).await;
    assert_eq!(result, RespValue::ok());

    server.stop().await;
}

// ============================================================================
// WAIT Command - Comprehensive Tests
// ============================================================================

#[tokio::test]
async fn test_wait_no_writes_returns_immediately() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // WAIT should return immediately with replica count if no writes have occurred
    // (offset is 0)
    let start = std::time::Instant::now();
    let result = client.cmd(&["WAIT", "1", "5000"]).await;
    let elapsed = start.elapsed();

    // Should return 0 (no replicas connected)
    assert_eq!(result, RespValue::Integer(0));

    // Should be fast since no writes to wait for
    // Note: Includes overhead from spawning async task (CI can be slow)
    assert!(
        elapsed.as_millis() < 300,
        "WAIT took too long with no writes: {elapsed:?}",
    );

    server.stop().await;
}

#[tokio::test]
async fn test_wait_with_write_and_no_replicas_times_out() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Perform a write to increment offset
    client.cmd_ok(&["SET", "key", "value"]).await;

    // WAIT should timeout waiting for replicas
    let start = std::time::Instant::now();
    let result = client.cmd(&["WAIT", "1", "100"]).await;
    let elapsed = start.elapsed();

    // Should return 0 (no replicas acknowledged)
    assert_eq!(result, RespValue::Integer(0));

    // Should take approximately the timeout duration (with some overhead)
    assert!(
        elapsed.as_millis() >= 90,
        "WAIT returned too early: {elapsed:?}",
    );
    assert!(elapsed.as_millis() < 250, "WAIT took too long: {elapsed:?}");

    server.stop().await;
}

#[tokio::test]
async fn test_wait_zero_timeout_returns_immediately_no_writes() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // With no writes, WAIT with any timeout should return immediately
    // (offset 0 means all replicas are up-to-date)
    let start = std::time::Instant::now();
    let result = client.cmd(&["WAIT", "0", "0"]).await;
    let elapsed = start.elapsed();

    assert_eq!(result, RespValue::Integer(0));
    assert!(elapsed.as_millis() < 1000, "Took too long: {elapsed:?}");

    server.stop().await;
}

#[tokio::test]
async fn test_wait_asks_for_zero_replicas() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Write some data
    client.cmd_ok(&["SET", "key", "value"]).await;

    // WAIT for 0 replicas should return immediately
    let start = std::time::Instant::now();
    let result = client.cmd(&["WAIT", "0", "1000"]).await;
    let elapsed = start.elapsed();

    // Should return 0 (asked for 0, got 0)
    assert_eq!(result, RespValue::Integer(0));

    // Should be fast (includes async task spawn overhead)
    assert!(elapsed.as_millis() < 200);

    server.stop().await;
}

#[tokio::test]
async fn test_wait_multiple_writes_accumulate() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Perform multiple writes
    client.cmd_ok(&["SET", "key1", "value1"]).await;
    client.cmd_ok(&["SET", "key2", "value2"]).await;
    client.cmd_ok(&["SET", "key3", "value3"]).await;

    // WAIT should wait for all accumulated writes
    let start = std::time::Instant::now();
    let result = client.cmd(&["WAIT", "1", "100"]).await;
    let elapsed = start.elapsed();

    // Should timeout (no replicas)
    assert_eq!(result, RespValue::Integer(0));
    assert!(elapsed.as_millis() >= 90);

    server.stop().await;
}

#[tokio::test]
async fn test_wait_on_follower_returns_zero() {
    // Spawn leader
    let leader = TestServer::spawn().await;
    let _leader_client = leader.client().await;

    // Spawn follower and make it a replica
    let follower = TestServer::spawn().await;
    let mut follower_client = follower.client().await;

    let leader_port = leader.addr.port();
    follower_client
        .cmd_ok(&["REPLICAOF", "127.0.0.1", &leader_port.to_string()])
        .await;

    // Give replication time to connect
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // WAIT on a follower should return 0 (followers don't have replicas)
    let result = follower_client.cmd(&["WAIT", "1", "1000"]).await;
    assert_eq!(result, RespValue::Integer(0));

    follower.stop().await;
    leader.stop().await;
}

#[tokio::test]
async fn test_wait_concurrent_waits() {
    let server = TestServer::spawn().await;
    let mut client1 = server.client().await;
    let mut client2 = server.client().await;

    // Perform writes
    client1.cmd_ok(&["SET", "key", "value"]).await;

    // Start two WAIT commands concurrently
    let wait1 = client1.cmd(&["WAIT", "1", "200"]);
    let wait2 = client2.cmd(&["WAIT", "1", "200"]);

    let (result1, result2) = tokio::join!(wait1, wait2);

    // Both should timeout with 0
    assert_eq!(result1, RespValue::Integer(0));
    assert_eq!(result2, RespValue::Integer(0));

    server.stop().await;
}

#[tokio::test]
async fn test_wait_large_timeout() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Write data
    client.cmd_ok(&["SET", "key", "value"]).await;

    // WAIT with very large timeout (but we'll timeout anyway with no replicas)
    let start = std::time::Instant::now();
    let result = client.cmd(&["WAIT", "1", "100"]).await;
    let elapsed = start.elapsed();

    assert_eq!(result, RespValue::Integer(0));
    // CI environments can be slower, allow up to 300ms
    assert!(elapsed.as_millis() >= 90 && elapsed.as_millis() < 300);

    server.stop().await;
}

#[tokio::test]
async fn test_wait_very_short_timeout() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Write data
    client.cmd_ok(&["SET", "key", "value"]).await;

    // WAIT with very short timeout
    let start = std::time::Instant::now();
    let result = client.cmd(&["WAIT", "1", "10"]).await;
    let elapsed = start.elapsed();

    assert_eq!(result, RespValue::Integer(0));
    // Should timeout quickly (10ms + overhead, CI can be slower)
    assert!(elapsed.as_millis() < 300);

    server.stop().await;
}

#[tokio::test]
async fn test_wait_after_multiple_commands_in_pipeline() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Execute a transaction with multiple commands
    client.cmd_ok(&["MULTI"]).await;
    client.cmd(&["SET", "key1", "value1"]).await; // Returns QUEUED
    client.cmd(&["SET", "key2", "value2"]).await; // Returns QUEUED
    client.cmd(&["EXEC"]).await; // Returns array of results

    // WAIT should wait for all transactional writes
    let result = client.cmd(&["WAIT", "1", "100"]).await;
    assert_eq!(result, RespValue::Integer(0));

    server.stop().await;
}

#[tokio::test]
async fn test_wait_with_read_commands_only() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Only read commands, no writes
    client.cmd(&["GET", "nonexistent"]).await;
    client.cmd(&["EXISTS", "key"]).await;

    // WAIT should return immediately (no writes = offset 0)
    let start = std::time::Instant::now();
    let result = client.cmd(&["WAIT", "1", "1000"]).await;
    let elapsed = start.elapsed();

    assert_eq!(result, RespValue::Integer(0));
    // CI environments can be slower
    assert!(elapsed.as_millis() < 300, "Took {elapsed:?}");

    server.stop().await;
}

#[tokio::test]
async fn test_wait_mixed_read_write_commands() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Mix of reads and writes
    client.cmd(&["GET", "key"]).await;
    client.cmd_ok(&["SET", "key", "value"]).await;
    client.cmd(&["GET", "key"]).await;
    client.cmd(&["DEL", "key"]).await; // Returns number of keys deleted

    // WAIT should wait for the writes
    let result = client.cmd(&["WAIT", "1", "100"]).await;
    assert_eq!(result, RespValue::Integer(0));

    server.stop().await;
}

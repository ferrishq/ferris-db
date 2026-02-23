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

    // Negative timeout should be valid (means infinite wait)
    let result = client.cmd(&["WAIT", "1", "-1"]).await;
    assert_eq!(result, RespValue::Integer(0));

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

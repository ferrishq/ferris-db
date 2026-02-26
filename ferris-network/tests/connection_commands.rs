#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

//! Integration tests for connection commands

use ferris_protocol::RespValue;
use ferris_test_utils::TestServer;

#[tokio::test]
async fn test_readonly_command() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // READONLY should return OK
    let result = client.cmd(&["READONLY"]).await;
    assert_eq!(result, RespValue::SimpleString("OK".to_string()));

    server.stop().await;
}

#[tokio::test]
async fn test_readwrite_command() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // READWRITE should return OK
    let result = client.cmd(&["READWRITE"]).await;
    assert_eq!(result, RespValue::SimpleString("OK".to_string()));

    server.stop().await;
}

#[tokio::test]
async fn test_readonly_wrong_arity() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // READONLY with arguments should error
    let result = client.cmd(&["READONLY", "extra"]).await;
    match result {
        RespValue::Error(msg) => {
            assert!(msg.contains("wrong number") || msg.contains("arity"));
        }
        _ => panic!("Expected error for wrong arity"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_command_count() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // COMMAND COUNT should return number of commands
    let result = client.cmd(&["COMMAND", "COUNT"]).await;
    match result {
        RespValue::Integer(count) => {
            // Should be > 200 commands
            assert!(count > 200, "Expected > 200 commands, got {count}");
            assert!(count < 300, "Expected < 300 commands, got {count}");
        }
        _ => panic!("Expected integer, got {result:?}"),
    }

    server.stop().await;
}

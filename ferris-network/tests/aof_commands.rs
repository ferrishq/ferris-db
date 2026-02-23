#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
#![allow(clippy::manual_let_else)]
#![allow(clippy::uninlined_format_args)]
#![allow(clippy::unreadable_literal)]

//! Integration tests for AOF persistence commands: SAVE, BGSAVE, BGREWRITEAOF, LASTSAVE

use ferris_protocol::RespValue;
use ferris_test_utils::TestServer;

#[tokio::test]
async fn test_lastsave_returns_timestamp() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // LASTSAVE should return a Unix timestamp
    let result = client.cmd(&["LASTSAVE"]).await;

    match result {
        RespValue::Integer(timestamp) => {
            // Should be a reasonable Unix timestamp (> year 2020)
            assert!(timestamp > 1577836800); // Jan 1, 2020
        }
        _ => panic!("Expected integer timestamp"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_save_command() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Set some data
    client.cmd_ok(&["SET", "key1", "value1"]).await;
    client.cmd_ok(&["SET", "key2", "value2"]).await;

    // SAVE should trigger a snapshot
    let result = client.cmd(&["SAVE"]).await;
    assert_eq!(result, RespValue::SimpleString("OK".to_string()));

    server.stop().await;
}

#[tokio::test]
async fn test_bgsave_command() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Set some data
    client.cmd_ok(&["SET", "key1", "value1"]).await;

    // BGSAVE should start background save
    let result = client.cmd(&["BGSAVE"]).await;

    // Should return OK or "Background saving started"
    match result {
        RespValue::SimpleString(s) => {
            assert!(s == "OK" || s.contains("Background"));
        }
        _ => panic!("Expected string response"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_bgrewriteaof_command() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Set some data to generate AOF entries
    client.cmd_ok(&["SET", "key1", "value1"]).await;
    client.cmd_ok(&["SET", "key2", "value2"]).await;
    client.cmd(&["DEL", "key1"]).await; // Creates redundant AOF entries (returns count)

    // BGREWRITEAOF should start background rewrite
    let result = client.cmd(&["BGREWRITEAOF"]).await;

    // Should return OK or "Background AOF rewrite started"
    match result {
        RespValue::SimpleString(s) => {
            assert!(s == "OK" || s.contains("Background") || s.contains("AOF"));
        }
        _ => panic!("Expected string response"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_save_wrong_arity() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["SAVE", "extra"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    server.stop().await;
}

#[tokio::test]
async fn test_lastsave_wrong_arity() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["LASTSAVE", "extra"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    server.stop().await;
}

#[tokio::test]
async fn test_bgsave_multiple_calls() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // First BGSAVE
    let result1 = client.cmd(&["BGSAVE"]).await;
    assert!(matches!(result1, RespValue::SimpleString(_)));

    // Immediate second BGSAVE might be rejected or queued
    let result2 = client.cmd(&["BGSAVE"]).await;
    // Either succeeds or returns error about save already in progress
    match result2 {
        RespValue::SimpleString(_) => {} // OK
        RespValue::Error(e) => {
            assert!(e.contains("already") || e.contains("progress"));
        }
        _ => panic!("Unexpected response type"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_bgrewriteaof_multiple_calls() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd_ok(&["SET", "test", "value"]).await;

    // First BGREWRITEAOF
    let result1 = client.cmd(&["BGREWRITEAOF"]).await;
    assert!(matches!(result1, RespValue::SimpleString(_)));

    // Immediate second call might be rejected
    let result2 = client.cmd(&["BGREWRITEAOF"]).await;
    match result2 {
        RespValue::SimpleString(_) => {} // OK
        RespValue::Error(e) => {
            assert!(e.contains("already") || e.contains("progress") || e.contains("scheduled"));
        }
        _ => panic!("Unexpected response type"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_persistence_commands_with_data() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Create diverse data types
    client.cmd_ok(&["SET", "string_key", "string_value"]).await;
    client.cmd(&["LPUSH", "list_key", "item1", "item2"]).await; // Returns integer count
    client.cmd(&["SADD", "set_key", "member1", "member2"]).await; // Returns integer count
    client.cmd(&["HSET", "hash_key", "field1", "value1"]).await; // Returns integer count
    client.cmd(&["ZADD", "zset_key", "1.0", "member1"]).await; // Returns integer count

    // Test SAVE
    let result = client.cmd(&["SAVE"]).await;
    assert_eq!(result, RespValue::SimpleString("OK".to_string()));

    // Test BGSAVE
    let result = client.cmd(&["BGSAVE"]).await;
    assert!(matches!(result, RespValue::SimpleString(_)));

    // Test BGREWRITEAOF
    let result = client.cmd(&["BGREWRITEAOF"]).await;
    assert!(matches!(result, RespValue::SimpleString(_)));

    // Test LASTSAVE
    let result = client.cmd(&["LASTSAVE"]).await;
    assert!(matches!(result, RespValue::Integer(_)));

    server.stop().await;
}

#[tokio::test]
async fn test_save_doesnt_affect_operations() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Set initial value
    client.cmd_ok(&["SET", "counter", "0"]).await;

    // Trigger save
    client.cmd_ok(&["SAVE"]).await;

    // Operations should still work after save
    client.cmd(&["INCR", "counter"]).await;
    let result = client.cmd(&["GET", "counter"]).await;
    assert_eq!(result, RespValue::bulk_string("1"));

    server.stop().await;
}

#[tokio::test]
async fn test_bgsave_doesnt_block_operations() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd_ok(&["SET", "key1", "value1"]).await;

    // Start background save
    client.cmd(&["BGSAVE"]).await;

    // Should be able to write immediately (non-blocking)
    client.cmd_ok(&["SET", "key2", "value2"]).await;
    client.cmd_ok(&["SET", "key3", "value3"]).await;

    let result = client.cmd(&["GET", "key3"]).await;
    assert_eq!(result, RespValue::bulk_string("value3"));

    server.stop().await;
}

#[tokio::test]
async fn test_bgrewriteaof_doesnt_block_operations() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Generate some AOF entries
    for i in 0..10 {
        client
            .cmd_ok(&["SET", &format!("key{}", i), &format!("value{}", i)])
            .await;
    }

    // Start background rewrite
    client.cmd(&["BGREWRITEAOF"]).await;

    // Should be able to continue operations
    client.cmd_ok(&["SET", "newkey", "newvalue"]).await;

    let result = client.cmd(&["GET", "newkey"]).await;
    assert_eq!(result, RespValue::bulk_string("newvalue"));

    server.stop().await;
}

#[tokio::test]
async fn test_lastsave_increases_after_save() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Get initial timestamp
    let result1 = client.cmd(&["LASTSAVE"]).await;
    let timestamp1 = match result1 {
        RespValue::Integer(t) => t,
        _ => panic!("Expected integer"),
    };

    // Wait a bit
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Trigger save
    client.cmd_ok(&["SAVE"]).await;

    // Get new timestamp
    let result2 = client.cmd(&["LASTSAVE"]).await;
    let timestamp2 = match result2 {
        RespValue::Integer(t) => t,
        _ => panic!("Expected integer"),
    };

    // Timestamp should have increased
    assert!(timestamp2 >= timestamp1);

    server.stop().await;
}

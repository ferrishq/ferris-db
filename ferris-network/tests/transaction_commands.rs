#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

//! Integration tests for transaction commands: MULTI, EXEC, DISCARD, WATCH, UNWATCH

use ferris_protocol::RespValue;
use ferris_test_utils::TestServer;

#[tokio::test]
async fn test_multi_exec_basic() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Start transaction
    let result = client.cmd(&["MULTI"]).await;
    assert_eq!(result, RespValue::SimpleString("OK".to_string()));

    // Queue commands
    let result = client.cmd(&["SET", "key1", "value1"]).await;
    assert_eq!(result, RespValue::SimpleString("QUEUED".to_string()));

    let result = client.cmd(&["SET", "key2", "value2"]).await;
    assert_eq!(result, RespValue::SimpleString("QUEUED".to_string()));

    let result = client.cmd(&["GET", "key1"]).await;
    assert_eq!(result, RespValue::SimpleString("QUEUED".to_string()));

    // Execute transaction
    let result = client.cmd(&["EXEC"]).await;
    match result {
        RespValue::Array(results) => {
            assert_eq!(results.len(), 3);
            assert_eq!(results[0], RespValue::SimpleString("OK".to_string()));
            assert_eq!(results[1], RespValue::SimpleString("OK".to_string()));
            assert_eq!(results[2], RespValue::bulk_string("value1"));
        }
        _ => panic!("Expected array response"),
    }

    // Verify keys were set
    let result = client.cmd(&["GET", "key2"]).await;
    assert_eq!(result, RespValue::bulk_string("value2"));

    server.stop().await;
}

#[tokio::test]
async fn test_multi_discard() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Start transaction
    client.cmd_ok(&["MULTI"]).await;

    // Queue commands
    let result = client.cmd(&["SET", "key", "value"]).await;
    assert_eq!(result, RespValue::SimpleString("QUEUED".to_string()));

    // Discard transaction
    let result = client.cmd(&["DISCARD"]).await;
    assert_eq!(result, RespValue::SimpleString("OK".to_string()));

    // Key should not be set
    let result = client.cmd(&["GET", "key"]).await;
    assert_eq!(result, RespValue::Null);

    server.stop().await;
}

#[tokio::test]
async fn test_multi_nested_error() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd_ok(&["MULTI"]).await;

    // Try to nest MULTI
    let result = client.cmd(&["MULTI"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    server.stop().await;
}

#[tokio::test]
async fn test_exec_without_multi() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["EXEC"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    server.stop().await;
}

#[tokio::test]
async fn test_discard_without_multi() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["DISCARD"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    server.stop().await;
}

#[tokio::test]
async fn test_multi_exec_with_error() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd_ok(&["MULTI"]).await;

    // Queue valid command
    let result = client.cmd(&["SET", "key", "value"]).await;
    assert_eq!(result, RespValue::SimpleString("QUEUED".to_string()));

    // Queue command that will error (wrong type)
    let result = client.cmd(&["LPUSH", "key", "item"]).await;
    assert_eq!(result, RespValue::SimpleString("QUEUED".to_string()));

    // Execute - should return mixed results
    let result = client.cmd(&["EXEC"]).await;
    match result {
        RespValue::Array(results) => {
            assert_eq!(results.len(), 2);
            assert_eq!(results[0], RespValue::SimpleString("OK".to_string()));
            // Second command should error (wrong type)
            assert!(matches!(results[1], RespValue::Error(_)));
        }
        _ => panic!("Expected array response"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_multi_exec_empty() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd_ok(&["MULTI"]).await;

    // Execute without queuing any commands
    let result = client.cmd(&["EXEC"]).await;
    match result {
        RespValue::Array(results) => {
            assert_eq!(results.len(), 0);
        }
        _ => panic!("Expected empty array"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_watch_basic() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Watch a key
    let result = client.cmd(&["WATCH", "mykey"]).await;
    assert_eq!(result, RespValue::SimpleString("OK".to_string()));

    // Start transaction
    client.cmd_ok(&["MULTI"]).await;
    let result = client.cmd(&["SET", "mykey", "value"]).await;
    assert_eq!(result, RespValue::SimpleString("QUEUED".to_string()));

    // Execute - should succeed (key was not modified)
    let result = client.cmd(&["EXEC"]).await;
    match result {
        RespValue::Array(results) => {
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], RespValue::SimpleString("OK".to_string()));
        }
        _ => panic!("Expected array response"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_watch_abort_on_modification() {
    let server = TestServer::spawn().await;
    let mut client1 = server.client().await;
    let mut client2 = server.client().await;

    // Client 1: Watch a key
    client1.cmd_ok(&["WATCH", "mykey"]).await;

    // Client 2: Modify the watched key
    client2.cmd_ok(&["SET", "mykey", "changed"]).await;

    // Client 1: Start transaction and try to modify
    client1.cmd_ok(&["MULTI"]).await;
    let result = client1.cmd(&["SET", "mykey", "value"]).await;
    assert_eq!(result, RespValue::SimpleString("QUEUED".to_string()));

    // Execute - should return nil (transaction aborted)
    let result = client1.cmd(&["EXEC"]).await;
    assert_eq!(result, RespValue::Null);

    // Verify the key still has the value from client2
    let result = client1.cmd(&["GET", "mykey"]).await;
    assert_eq!(result, RespValue::bulk_string("changed"));

    server.stop().await;
}

#[tokio::test]
async fn test_watch_multiple_keys() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Watch multiple keys
    let result = client.cmd(&["WATCH", "key1", "key2", "key3"]).await;
    assert_eq!(result, RespValue::SimpleString("OK".to_string()));

    client.cmd_ok(&["MULTI"]).await;
    let result = client.cmd(&["SET", "key1", "value1"]).await;
    assert_eq!(result, RespValue::SimpleString("QUEUED".to_string()));

    // Execute - should succeed (no keys modified)
    let result = client.cmd(&["EXEC"]).await;
    assert!(matches!(result, RespValue::Array(_)));

    server.stop().await;
}

#[tokio::test]
async fn test_watch_nonexistent_key_then_created() {
    let server = TestServer::spawn().await;
    let mut client1 = server.client().await;
    let mut client2 = server.client().await;

    // Watch a non-existent key
    client1.cmd_ok(&["WATCH", "newkey"]).await;

    // Another client creates the key
    client2.cmd_ok(&["SET", "newkey", "value"]).await;

    // Try to execute transaction
    client1.cmd_ok(&["MULTI"]).await;
    let result = client1.cmd(&["SET", "newkey", "myvalue"]).await;
    assert_eq!(result, RespValue::SimpleString("QUEUED".to_string()));

    // Should abort because watched key was created
    let result = client1.cmd(&["EXEC"]).await;
    assert_eq!(result, RespValue::Null);

    server.stop().await;
}

#[tokio::test]
async fn test_unwatch() {
    let server = TestServer::spawn().await;
    let mut client1 = server.client().await;
    let mut client2 = server.client().await;

    // Watch a key
    client1.cmd_ok(&["WATCH", "mykey"]).await;

    // Unwatch
    let result = client1.cmd(&["UNWATCH"]).await;
    assert_eq!(result, RespValue::SimpleString("OK".to_string()));

    // Another client modifies the key
    client2.cmd_ok(&["SET", "mykey", "changed"]).await;

    // Transaction should succeed (not watching anymore)
    client1.cmd_ok(&["MULTI"]).await;
    let result = client1.cmd(&["SET", "mykey", "value"]).await;
    assert_eq!(result, RespValue::SimpleString("QUEUED".to_string()));

    let result = client1.cmd(&["EXEC"]).await;
    assert!(matches!(result, RespValue::Array(_)));

    server.stop().await;
}

#[tokio::test]
async fn test_watch_inside_multi_error() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd_ok(&["MULTI"]).await;

    // WATCH inside MULTI should error
    let result = client.cmd(&["WATCH", "key"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    server.stop().await;
}

#[tokio::test]
async fn test_watch_clears_on_exec() {
    let server = TestServer::spawn().await;
    let mut client1 = server.client().await;
    let mut client2 = server.client().await;

    // Watch and execute transaction
    client1.cmd_ok(&["WATCH", "mykey"]).await;
    client1.cmd_ok(&["MULTI"]).await;
    let result = client1.cmd(&["SET", "mykey", "value"]).await;
    assert_eq!(result, RespValue::SimpleString("QUEUED".to_string()));
    client1.cmd(&["EXEC"]).await;

    // Now modify the key (watches should be cleared)
    client2.cmd_ok(&["SET", "mykey", "changed"]).await;

    // New transaction should succeed (no watches active)
    client1.cmd_ok(&["MULTI"]).await;
    let result = client1.cmd(&["GET", "mykey"]).await;
    assert_eq!(result, RespValue::SimpleString("QUEUED".to_string()));

    let result = client1.cmd(&["EXEC"]).await;
    assert!(matches!(result, RespValue::Array(_)));

    server.stop().await;
}

#[tokio::test]
async fn test_watch_clears_on_discard() {
    let server = TestServer::spawn().await;
    let mut client1 = server.client().await;
    let mut client2 = server.client().await;

    // Watch and discard transaction
    client1.cmd_ok(&["WATCH", "mykey"]).await;
    client1.cmd_ok(&["MULTI"]).await;
    let result = client1.cmd(&["SET", "mykey", "value"]).await;
    assert_eq!(result, RespValue::SimpleString("QUEUED".to_string()));
    client1.cmd_ok(&["DISCARD"]).await;

    // Modify the key
    client2.cmd_ok(&["SET", "mykey", "changed"]).await;

    // New transaction should FAIL (watches NOT cleared on discard, still watching)
    client1.cmd_ok(&["MULTI"]).await;
    let result = client1.cmd(&["SET", "mykey", "newvalue"]).await;
    assert_eq!(result, RespValue::SimpleString("QUEUED".to_string()));

    // Should abort because key was modified while being watched
    let result = client1.cmd(&["EXEC"]).await;
    assert_eq!(result, RespValue::Null);

    server.stop().await;
}

#[tokio::test]
async fn test_multi_exec_incr_atomicity() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Set initial counter
    client.cmd_ok(&["SET", "counter", "0"]).await;

    // Transaction with multiple increments
    client.cmd_ok(&["MULTI"]).await;
    let result = client.cmd(&["INCR", "counter"]).await;
    assert_eq!(result, RespValue::SimpleString("QUEUED".to_string()));
    let result = client.cmd(&["INCR", "counter"]).await;
    assert_eq!(result, RespValue::SimpleString("QUEUED".to_string()));
    let result = client.cmd(&["INCR", "counter"]).await;
    assert_eq!(result, RespValue::SimpleString("QUEUED".to_string()));

    let result = client.cmd(&["EXEC"]).await;
    match result {
        RespValue::Array(results) => {
            assert_eq!(results.len(), 3);
            assert_eq!(results[0], RespValue::Integer(1));
            assert_eq!(results[1], RespValue::Integer(2));
            assert_eq!(results[2], RespValue::Integer(3));
        }
        _ => panic!("Expected array response"),
    }

    // Final value should be 3
    let result = client.cmd(&["GET", "counter"]).await;
    assert_eq!(result, RespValue::bulk_string("3"));

    server.stop().await;
}

#[tokio::test]
async fn test_multi_exec_mset_mget() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd_ok(&["MULTI"]).await;
    let result = client
        .cmd(&["MSET", "key1", "val1", "key2", "val2", "key3", "val3"])
        .await;
    assert_eq!(result, RespValue::SimpleString("QUEUED".to_string()));
    let result = client.cmd(&["MGET", "key1", "key2", "key3"]).await;
    assert_eq!(result, RespValue::SimpleString("QUEUED".to_string()));

    let result = client.cmd(&["EXEC"]).await;
    match result {
        RespValue::Array(results) => {
            assert_eq!(results.len(), 2);
            assert_eq!(results[0], RespValue::SimpleString("OK".to_string()));
            match &results[1] {
                RespValue::Array(values) => {
                    assert_eq!(values.len(), 3);
                    assert_eq!(values[0], RespValue::bulk_string("val1"));
                    assert_eq!(values[1], RespValue::bulk_string("val2"));
                    assert_eq!(values[2], RespValue::bulk_string("val3"));
                }
                _ => panic!("Expected array"),
            }
        }
        _ => panic!("Expected array response"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_multi_exec_list_operations() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd_ok(&["MULTI"]).await;
    let result = client.cmd(&["LPUSH", "mylist", "a", "b", "c"]).await;
    assert_eq!(result, RespValue::SimpleString("QUEUED".to_string()));
    let result = client.cmd(&["LRANGE", "mylist", "0", "-1"]).await;
    assert_eq!(result, RespValue::SimpleString("QUEUED".to_string()));
    let result = client.cmd(&["LLEN", "mylist"]).await;
    assert_eq!(result, RespValue::SimpleString("QUEUED".to_string()));

    let result = client.cmd(&["EXEC"]).await;
    match result {
        RespValue::Array(results) => {
            assert_eq!(results.len(), 3);
            assert_eq!(results[0], RespValue::Integer(3));
            match &results[1] {
                RespValue::Array(items) => {
                    assert_eq!(items.len(), 3);
                }
                _ => panic!("Expected array"),
            }
            assert_eq!(results[2], RespValue::Integer(3));
        }
        _ => panic!("Expected array response"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_watch_with_del() {
    let server = TestServer::spawn().await;
    let mut client1 = server.client().await;
    let mut client2 = server.client().await;

    // Set a key and watch it
    client1.cmd_ok(&["SET", "mykey", "value"]).await;
    client1.cmd_ok(&["WATCH", "mykey"]).await;

    // Another client deletes it
    client2.cmd(&["DEL", "mykey"]).await;

    // Transaction should abort
    client1.cmd_ok(&["MULTI"]).await;
    let result = client1.cmd(&["SET", "mykey", "newvalue"]).await;
    assert_eq!(result, RespValue::SimpleString("QUEUED".to_string()));

    let result = client1.cmd(&["EXEC"]).await;
    assert_eq!(result, RespValue::Null);

    server.stop().await;
}

#[tokio::test]
async fn test_watch_with_expire() {
    let server = TestServer::spawn().await;
    let mut client1 = server.client().await;
    let mut client2 = server.client().await;

    // Set a key and watch it
    client1.cmd_ok(&["SET", "mykey", "value"]).await;
    client1.cmd_ok(&["WATCH", "mykey"]).await;

    // Another client sets expiry
    client2.cmd(&["EXPIRE", "mykey", "10"]).await;

    // Transaction should abort (EXPIRE modifies key metadata)
    client1.cmd_ok(&["MULTI"]).await;
    let result = client1.cmd(&["SET", "mykey", "newvalue"]).await;
    assert_eq!(result, RespValue::SimpleString("QUEUED".to_string()));

    let result = client1.cmd(&["EXEC"]).await;
    assert_eq!(result, RespValue::Null);

    server.stop().await;
}

#[tokio::test]
async fn test_multi_exec_across_databases() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Start in DB 0
    client.cmd_ok(&["SELECT", "0"]).await;
    client.cmd_ok(&["SET", "key", "db0"]).await;

    // Transaction spanning database switch (not typical, but should work)
    client.cmd_ok(&["MULTI"]).await;
    let result = client.cmd(&["GET", "key"]).await;
    assert_eq!(result, RespValue::SimpleString("QUEUED".to_string()));
    let result = client.cmd(&["SELECT", "1"]).await;
    assert_eq!(result, RespValue::SimpleString("QUEUED".to_string()));
    let result = client.cmd(&["SET", "key", "db1"]).await;
    assert_eq!(result, RespValue::SimpleString("QUEUED".to_string()));

    let result = client.cmd(&["EXEC"]).await;
    match result {
        RespValue::Array(results) => {
            assert_eq!(results.len(), 3);
            assert_eq!(results[0], RespValue::bulk_string("db0"));
            assert_eq!(results[1], RespValue::SimpleString("OK".to_string()));
            assert_eq!(results[2], RespValue::SimpleString("OK".to_string()));
        }
        _ => panic!("Expected array response"),
    }

    // Should now be in DB 1
    let result = client.cmd(&["GET", "key"]).await;
    assert_eq!(result, RespValue::bulk_string("db1"));

    server.stop().await;
}

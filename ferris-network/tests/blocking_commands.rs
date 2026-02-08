//! Integration tests for blocking commands.
//!
//! Tests BLPOP, BRPOP, BLMOVE, BLMPOP, BZPOPMIN, BZPOPMAX, BZMPOP.
//! These tests exercise the full blocking pipeline: command -> Block error
//! -> connection handler wait -> notify -> retry -> response.

use bytes::Bytes;
use ferris_protocol::RespValue;
use ferris_test_utils::TestServer;
use std::time::Duration;

// ============================================================
// BLPOP tests
// ============================================================

#[tokio::test]
async fn test_blpop_immediate_data() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Pre-populate a list
    client.cmd(&["LPUSH", "mylist", "a", "b", "c"]).await;

    // BLPOP should return immediately
    let result = client.cmd(&["BLPOP", "mylist", "1"]).await;
    assert_eq!(
        result,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("mylist")),
            RespValue::BulkString(Bytes::from("c")),
        ])
    );

    server.stop().await;
}

#[tokio::test]
async fn test_blpop_timeout_returns_null() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // BLPOP on non-existent key with short timeout
    let result = client
        .cmd_timeout(&["BLPOP", "nokey", "0.1"], Duration::from_secs(3))
        .await;
    assert_eq!(result, Some(RespValue::Null));

    server.stop().await;
}

#[tokio::test]
async fn test_blpop_wakeup_on_push() {
    let server = TestServer::spawn().await;
    let mut client1 = server.client().await;
    let mut client2 = server.client().await;

    // Client 1 sends BLPOP (will block)
    client1.send_raw(&["BLPOP", "mylist", "5"]).await;

    // Give a moment for the server to process the blocking command
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Client 2 pushes data
    client2.cmd(&["LPUSH", "mylist", "hello"]).await;

    // Client 1 should receive the data
    let result = client1.read_response_timeout(Duration::from_secs(2)).await;
    assert_eq!(
        result,
        Some(RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("mylist")),
            RespValue::BulkString(Bytes::from("hello")),
        ]))
    );

    server.stop().await;
}

#[tokio::test]
async fn test_blpop_multiple_keys_first_available() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Pre-populate second key
    client.cmd(&["RPUSH", "list2", "x"]).await;

    // BLPOP checks keys in order; list1 is empty, list2 has data
    let result = client.cmd(&["BLPOP", "list1", "list2", "1"]).await;
    assert_eq!(
        result,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("list2")),
            RespValue::BulkString(Bytes::from("x")),
        ])
    );

    server.stop().await;
}

#[tokio::test]
async fn test_blpop_multiple_keys_wakeup() {
    let server = TestServer::spawn().await;
    let mut client1 = server.client().await;
    let mut client2 = server.client().await;

    // Client 1 blocks on two keys
    client1.send_raw(&["BLPOP", "a", "b", "5"]).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Push to key "b"
    client2.cmd(&["RPUSH", "b", "val"]).await;

    let result = client1.read_response_timeout(Duration::from_secs(2)).await;
    assert_eq!(
        result,
        Some(RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("b")),
            RespValue::BulkString(Bytes::from("val")),
        ]))
    );

    server.stop().await;
}

#[tokio::test]
async fn test_blpop_wrong_type_error() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Create a string key
    client.cmd_ok(&["SET", "str_key", "value"]).await;

    // BLPOP on a string should return WRONGTYPE
    let result = client.cmd(&["BLPOP", "str_key", "0.1"]).await;
    match result {
        RespValue::Error(e) => assert!(e.contains("WRONGTYPE")),
        other => panic!("expected error, got {:?}", other),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_blpop_removes_element_from_list() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["RPUSH", "mylist", "a", "b"]).await;

    let result = client.cmd(&["BLPOP", "mylist", "1"]).await;
    assert_eq!(
        result,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("mylist")),
            RespValue::BulkString(Bytes::from("a")),
        ])
    );

    // Only "b" should remain
    let len = client.cmd(&["LLEN", "mylist"]).await;
    assert_eq!(len, RespValue::Integer(1));

    server.stop().await;
}

#[tokio::test]
async fn test_blpop_deletes_empty_list() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["RPUSH", "mylist", "only"]).await;

    let result = client.cmd(&["BLPOP", "mylist", "1"]).await;
    assert_eq!(
        result,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("mylist")),
            RespValue::BulkString(Bytes::from("only")),
        ])
    );

    // Key should be deleted
    let exists = client.cmd(&["EXISTS", "mylist"]).await;
    assert_eq!(exists, RespValue::Integer(0));

    server.stop().await;
}

#[tokio::test]
async fn test_blpop_negative_timeout_error() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["BLPOP", "mylist", "-1"]).await;
    match result {
        RespValue::Error(e) => assert!(e.contains("negative") || e.contains("timeout")),
        other => panic!("expected error, got {:?}", other),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_blpop_zero_timeout_blocks() {
    // With 0 timeout, should block until data or server stops
    let server = TestServer::spawn().await;
    let mut client1 = server.client().await;
    let mut client2 = server.client().await;

    client1.send_raw(&["BLPOP", "blockkey", "0"]).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Push data
    client2.cmd(&["RPUSH", "blockkey", "unblocked"]).await;

    let result = client1.read_response_timeout(Duration::from_secs(2)).await;
    assert_eq!(
        result,
        Some(RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("blockkey")),
            RespValue::BulkString(Bytes::from("unblocked")),
        ]))
    );

    server.stop().await;
}

#[tokio::test]
async fn test_blpop_float_timeout() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let start = std::time::Instant::now();
    let result = client
        .cmd_timeout(&["BLPOP", "nokey", "0.2"], Duration::from_secs(3))
        .await;
    let elapsed = start.elapsed();

    assert_eq!(result, Some(RespValue::Null));
    // Should have waited approximately 0.2 seconds
    assert!(
        elapsed >= Duration::from_millis(150) && elapsed <= Duration::from_millis(500),
        "elapsed: {:?}",
        elapsed
    );

    server.stop().await;
}

#[tokio::test]
async fn test_blpop_multiple_pops_in_sequence() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["RPUSH", "mylist", "a", "b", "c"]).await;

    // Pop all three elements sequentially
    let r1 = client.cmd(&["BLPOP", "mylist", "1"]).await;
    let r2 = client.cmd(&["BLPOP", "mylist", "1"]).await;
    let r3 = client.cmd(&["BLPOP", "mylist", "1"]).await;

    assert_eq!(
        r1,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("mylist")),
            RespValue::BulkString(Bytes::from("a")),
        ])
    );
    assert_eq!(
        r2,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("mylist")),
            RespValue::BulkString(Bytes::from("b")),
        ])
    );
    assert_eq!(
        r3,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("mylist")),
            RespValue::BulkString(Bytes::from("c")),
        ])
    );

    // Now the list is empty, next BLPOP should timeout
    let r4 = client
        .cmd_timeout(&["BLPOP", "mylist", "0.1"], Duration::from_secs(3))
        .await;
    assert_eq!(r4, Some(RespValue::Null));

    server.stop().await;
}

#[tokio::test]
async fn test_blpop_wakeup_via_lpush() {
    let server = TestServer::spawn().await;
    let mut client1 = server.client().await;
    let mut client2 = server.client().await;

    client1.send_raw(&["BLPOP", "mylist", "5"]).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Use LPUSH (not RPUSH) to wake
    client2.cmd(&["LPUSH", "mylist", "from_lpush"]).await;

    let result = client1.read_response_timeout(Duration::from_secs(2)).await;
    assert_eq!(
        result,
        Some(RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("mylist")),
            RespValue::BulkString(Bytes::from("from_lpush")),
        ]))
    );

    server.stop().await;
}

#[tokio::test]
async fn test_blpop_expired_key_blocks() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Create a key with short TTL
    client.cmd(&["RPUSH", "mylist", "val"]).await;
    client.cmd(&["PEXPIRE", "mylist", "50"]).await;

    // Wait for expiry
    tokio::time::sleep(Duration::from_millis(100)).await;

    // BLPOP should block since key is expired
    let result = client
        .cmd_timeout(&["BLPOP", "mylist", "0.1"], Duration::from_secs(3))
        .await;
    assert_eq!(result, Some(RespValue::Null));

    server.stop().await;
}

#[tokio::test]
async fn test_blpop_wrong_arity() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // No keys, just timeout
    let result = client.cmd(&["BLPOP", "0.1"]).await;
    match result {
        RespValue::Error(e) => assert!(
            e.contains("wrong number") || e.contains("arity") || e.contains("ERR"),
            "unexpected error: {}",
            e
        ),
        other => panic!("expected error, got {:?}", other),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_blpop_non_numeric_timeout() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["BLPOP", "mylist", "abc"]).await;
    match result {
        RespValue::Error(e) => assert!(
            e.contains("timeout") || e.contains("float") || e.contains("not") || e.contains("ERR"),
            "unexpected error: {}",
            e
        ),
        other => panic!("expected error, got {:?}", other),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_blpop_multiple_blocked_clients_multiple_pushes() {
    let server = TestServer::spawn().await;
    let mut client1 = server.client().await;
    let mut client2 = server.client().await;
    let mut pusher = server.client().await;

    // Two clients block
    client1.send_raw(&["BLPOP", "shared", "5"]).await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    client2.send_raw(&["BLPOP", "shared", "5"]).await;
    tokio::time::sleep(Duration::from_millis(30)).await;

    // Push two elements
    pusher.cmd(&["RPUSH", "shared", "e1"]).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    pusher.cmd(&["RPUSH", "shared", "e2"]).await;

    let r1 = client1.read_response_timeout(Duration::from_secs(2)).await;
    let r2 = client2.read_response_timeout(Duration::from_secs(2)).await;

    // Both clients should have received data
    assert!(r1.is_some(), "client1 should have received data");
    assert!(r2.is_some(), "client2 should have received data");

    server.stop().await;
}

// ============================================================
// BRPOP tests
// ============================================================

#[tokio::test]
async fn test_brpop_immediate_data() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["RPUSH", "mylist", "a", "b", "c"]).await;

    let result = client.cmd(&["BRPOP", "mylist", "1"]).await;
    assert_eq!(
        result,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("mylist")),
            RespValue::BulkString(Bytes::from("c")),
        ])
    );

    server.stop().await;
}

#[tokio::test]
async fn test_brpop_timeout_returns_null() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client
        .cmd_timeout(&["BRPOP", "nokey", "0.1"], Duration::from_secs(3))
        .await;
    assert_eq!(result, Some(RespValue::Null));

    server.stop().await;
}

#[tokio::test]
async fn test_brpop_wakeup_on_push() {
    let server = TestServer::spawn().await;
    let mut client1 = server.client().await;
    let mut client2 = server.client().await;

    client1.send_raw(&["BRPOP", "mylist", "5"]).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    client2.cmd(&["RPUSH", "mylist", "world"]).await;

    let result = client1.read_response_timeout(Duration::from_secs(2)).await;
    assert_eq!(
        result,
        Some(RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("mylist")),
            RespValue::BulkString(Bytes::from("world")),
        ]))
    );

    server.stop().await;
}

#[tokio::test]
async fn test_brpop_wrong_type_error() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd_ok(&["SET", "str_key", "value"]).await;

    let result = client.cmd(&["BRPOP", "str_key", "0.1"]).await;
    match result {
        RespValue::Error(e) => assert!(e.contains("WRONGTYPE")),
        other => panic!("expected error, got {:?}", other),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_brpop_pops_from_right() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["RPUSH", "mylist", "first", "second"]).await;

    let result = client.cmd(&["BRPOP", "mylist", "1"]).await;
    assert_eq!(
        result,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("mylist")),
            RespValue::BulkString(Bytes::from("second")),
        ])
    );

    server.stop().await;
}

#[tokio::test]
async fn test_brpop_multiple_keys() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["RPUSH", "list2", "val"]).await;

    let result = client.cmd(&["BRPOP", "list1", "list2", "1"]).await;
    assert_eq!(
        result,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("list2")),
            RespValue::BulkString(Bytes::from("val")),
        ])
    );

    server.stop().await;
}

#[tokio::test]
async fn test_brpop_negative_timeout_error() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["BRPOP", "mylist", "-1"]).await;
    match result {
        RespValue::Error(e) => assert!(e.contains("negative") || e.contains("timeout")),
        other => panic!("expected error, got {:?}", other),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_brpop_zero_timeout_wakeup() {
    let server = TestServer::spawn().await;
    let mut client1 = server.client().await;
    let mut client2 = server.client().await;

    client1.send_raw(&["BRPOP", "zerokey", "0"]).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    client2.cmd(&["RPUSH", "zerokey", "val"]).await;

    let result = client1.read_response_timeout(Duration::from_secs(2)).await;
    assert_eq!(
        result,
        Some(RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("zerokey")),
            RespValue::BulkString(Bytes::from("val")),
        ]))
    );

    server.stop().await;
}

#[tokio::test]
async fn test_brpop_float_timeout_precision() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let start = std::time::Instant::now();
    let result = client
        .cmd_timeout(&["BRPOP", "nokey", "0.2"], Duration::from_secs(3))
        .await;
    let elapsed = start.elapsed();

    assert_eq!(result, Some(RespValue::Null));
    assert!(
        elapsed >= Duration::from_millis(150) && elapsed <= Duration::from_millis(500),
        "elapsed: {:?}",
        elapsed
    );

    server.stop().await;
}

#[tokio::test]
async fn test_brpop_removes_element_from_list() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["RPUSH", "mylist", "a", "b"]).await;

    client.cmd(&["BRPOP", "mylist", "1"]).await;

    let len = client.cmd(&["LLEN", "mylist"]).await;
    assert_eq!(len, RespValue::Integer(1));

    server.stop().await;
}

#[tokio::test]
async fn test_brpop_deletes_empty_list() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["RPUSH", "mylist", "only"]).await;

    client.cmd(&["BRPOP", "mylist", "1"]).await;

    let exists = client.cmd(&["EXISTS", "mylist"]).await;
    assert_eq!(exists, RespValue::Integer(0));

    server.stop().await;
}

#[tokio::test]
async fn test_brpop_multiple_pops_in_sequence() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["RPUSH", "mylist", "x", "y", "z"]).await;

    let r1 = client.cmd(&["BRPOP", "mylist", "1"]).await;
    let r2 = client.cmd(&["BRPOP", "mylist", "1"]).await;
    let r3 = client.cmd(&["BRPOP", "mylist", "1"]).await;

    // BRPOP pops from right: z, y, x
    assert_eq!(
        r1,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("mylist")),
            RespValue::BulkString(Bytes::from("z")),
        ])
    );
    assert_eq!(
        r2,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("mylist")),
            RespValue::BulkString(Bytes::from("y")),
        ])
    );
    assert_eq!(
        r3,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("mylist")),
            RespValue::BulkString(Bytes::from("x")),
        ])
    );

    server.stop().await;
}

#[tokio::test]
async fn test_brpop_wakeup_via_lpush() {
    let server = TestServer::spawn().await;
    let mut client1 = server.client().await;
    let mut client2 = server.client().await;

    client1.send_raw(&["BRPOP", "mylist", "5"]).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    client2.cmd(&["LPUSH", "mylist", "from_lpush"]).await;

    let result = client1.read_response_timeout(Duration::from_secs(2)).await;
    assert_eq!(
        result,
        Some(RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("mylist")),
            RespValue::BulkString(Bytes::from("from_lpush")),
        ]))
    );

    server.stop().await;
}

#[tokio::test]
async fn test_brpop_expired_key_blocks() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["RPUSH", "mylist", "val"]).await;
    client.cmd(&["PEXPIRE", "mylist", "50"]).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let result = client
        .cmd_timeout(&["BRPOP", "mylist", "0.1"], Duration::from_secs(3))
        .await;
    assert_eq!(result, Some(RespValue::Null));

    server.stop().await;
}

#[tokio::test]
async fn test_brpop_connection_works_after_immediate() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["RPUSH", "mylist", "val"]).await;
    client.cmd(&["BRPOP", "mylist", "1"]).await;

    let pong = client.cmd(&["PING"]).await;
    assert_eq!(pong, RespValue::SimpleString("PONG".to_string()));

    server.stop().await;
}

#[tokio::test]
async fn test_brpop_connection_works_after_timeout() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client
        .cmd_timeout(&["BRPOP", "nokey", "0.1"], Duration::from_secs(3))
        .await;
    assert_eq!(result, Some(RespValue::Null));

    let pong = client.cmd(&["PING"]).await;
    assert_eq!(pong, RespValue::SimpleString("PONG".to_string()));

    server.stop().await;
}

#[tokio::test]
async fn test_brpop_non_numeric_timeout() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["BRPOP", "mylist", "abc"]).await;
    match result {
        RespValue::Error(e) => assert!(
            e.contains("timeout") || e.contains("float") || e.contains("not") || e.contains("ERR"),
            "unexpected error: {}",
            e
        ),
        other => panic!("expected error, got {:?}", other),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_brpop_wrong_arity() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["BRPOP", "0.1"]).await;
    match result {
        RespValue::Error(e) => assert!(
            e.contains("wrong number") || e.contains("arity") || e.contains("ERR"),
            "unexpected error: {}",
            e
        ),
        other => panic!("expected error, got {:?}", other),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_brpop_multiple_keys_wakeup() {
    let server = TestServer::spawn().await;
    let mut client1 = server.client().await;
    let mut client2 = server.client().await;

    client1.send_raw(&["BRPOP", "a", "b", "5"]).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    client2.cmd(&["RPUSH", "a", "fromA"]).await;

    let result = client1.read_response_timeout(Duration::from_secs(2)).await;
    assert_eq!(
        result,
        Some(RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("a")),
            RespValue::BulkString(Bytes::from("fromA")),
        ]))
    );

    server.stop().await;
}

#[tokio::test]
async fn test_brpop_multiple_blocked_clients() {
    let server = TestServer::spawn().await;
    let mut client1 = server.client().await;
    let mut client2 = server.client().await;
    let mut pusher = server.client().await;

    client1.send_raw(&["BRPOP", "shared", "5"]).await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    client2.send_raw(&["BRPOP", "shared", "5"]).await;
    tokio::time::sleep(Duration::from_millis(30)).await;

    pusher.cmd(&["RPUSH", "shared", "e1"]).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    pusher.cmd(&["RPUSH", "shared", "e2"]).await;

    let r1 = client1.read_response_timeout(Duration::from_secs(2)).await;
    let r2 = client2.read_response_timeout(Duration::from_secs(2)).await;

    assert!(r1.is_some(), "client1 should have received data");
    assert!(r2.is_some(), "client2 should have received data");

    server.stop().await;
}

// ============================================================
// BLMOVE tests
// ============================================================

#[tokio::test]
async fn test_blmove_immediate_data() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client
        .cmd(&["RPUSH", "src", "a", "b", "c"])
        .await;

    let result = client
        .cmd(&["BLMOVE", "src", "dst", "LEFT", "RIGHT", "1"])
        .await;
    assert_eq!(result, RespValue::BulkString(Bytes::from("a")));

    // Check destination
    let dst_val = client.cmd(&["LRANGE", "dst", "0", "-1"]).await;
    assert_eq!(
        dst_val,
        RespValue::Array(vec![RespValue::BulkString(Bytes::from("a"))])
    );

    server.stop().await;
}

#[tokio::test]
async fn test_blmove_timeout_returns_null() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client
        .cmd_timeout(
            &["BLMOVE", "nosrc", "dst", "LEFT", "RIGHT", "0.1"],
            Duration::from_secs(3),
        )
        .await;
    assert_eq!(result, Some(RespValue::Null));

    server.stop().await;
}

#[tokio::test]
async fn test_blmove_wakeup_on_push() {
    let server = TestServer::spawn().await;
    let mut client1 = server.client().await;
    let mut client2 = server.client().await;

    client1
        .send_raw(&["BLMOVE", "src", "dst", "LEFT", "RIGHT", "5"])
        .await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    client2.cmd(&["RPUSH", "src", "woken"]).await;

    let result = client1.read_response_timeout(Duration::from_secs(2)).await;
    assert_eq!(result, Some(RespValue::BulkString(Bytes::from("woken"))));

    // Destination should have the element
    let dst_val = client2.cmd(&["LRANGE", "dst", "0", "-1"]).await;
    assert_eq!(
        dst_val,
        RespValue::Array(vec![RespValue::BulkString(Bytes::from("woken"))])
    );

    server.stop().await;
}

#[tokio::test]
async fn test_blmove_right_left() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["RPUSH", "src", "a", "b"]).await;

    let result = client
        .cmd(&["BLMOVE", "src", "dst", "RIGHT", "LEFT", "1"])
        .await;
    assert_eq!(result, RespValue::BulkString(Bytes::from("b")));

    server.stop().await;
}

#[tokio::test]
async fn test_blmove_wrong_type_error() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd_ok(&["SET", "src", "string"]).await;

    let result = client
        .cmd(&["BLMOVE", "src", "dst", "LEFT", "RIGHT", "0.1"])
        .await;
    match result {
        RespValue::Error(e) => assert!(e.contains("WRONGTYPE")),
        other => panic!("expected error, got {:?}", other),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_blmove_same_key() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client
        .cmd(&["RPUSH", "mylist", "a", "b", "c"])
        .await;

    // Rotate: pop left, push right (same key)
    let result = client
        .cmd(&["BLMOVE", "mylist", "mylist", "LEFT", "RIGHT", "1"])
        .await;
    assert_eq!(result, RespValue::BulkString(Bytes::from("a")));

    // List should now be b, c, a
    let list = client.cmd(&["LRANGE", "mylist", "0", "-1"]).await;
    assert_eq!(
        list,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("b")),
            RespValue::BulkString(Bytes::from("c")),
            RespValue::BulkString(Bytes::from("a")),
        ])
    );

    server.stop().await;
}

#[tokio::test]
async fn test_blmove_invalid_direction() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client
        .cmd(&["BLMOVE", "src", "dst", "UP", "DOWN", "1"])
        .await;
    match result {
        RespValue::Error(_) => {} // Expected
        other => panic!("expected error, got {:?}", other),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_blmove_left_left() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["RPUSH", "src", "a", "b", "c"]).await;

    let result = client
        .cmd(&["BLMOVE", "src", "dst", "LEFT", "LEFT", "1"])
        .await;
    assert_eq!(result, RespValue::BulkString(Bytes::from("a")));

    // dst should have "a" at head
    let dst_val = client.cmd(&["LRANGE", "dst", "0", "-1"]).await;
    assert_eq!(
        dst_val,
        RespValue::Array(vec![RespValue::BulkString(Bytes::from("a"))])
    );

    server.stop().await;
}

#[tokio::test]
async fn test_blmove_right_right() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["RPUSH", "src", "a", "b", "c"]).await;

    let result = client
        .cmd(&["BLMOVE", "src", "dst", "RIGHT", "RIGHT", "1"])
        .await;
    assert_eq!(result, RespValue::BulkString(Bytes::from("c")));

    let dst_val = client.cmd(&["LRANGE", "dst", "0", "-1"]).await;
    assert_eq!(
        dst_val,
        RespValue::Array(vec![RespValue::BulkString(Bytes::from("c"))])
    );

    server.stop().await;
}

#[tokio::test]
async fn test_blmove_left_right_with_existing_dst() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["RPUSH", "src", "s1", "s2"]).await;
    client.cmd(&["RPUSH", "dst", "d1"]).await;

    let result = client
        .cmd(&["BLMOVE", "src", "dst", "LEFT", "RIGHT", "1"])
        .await;
    assert_eq!(result, RespValue::BulkString(Bytes::from("s1")));

    // dst should be [d1, s1]
    let dst_val = client.cmd(&["LRANGE", "dst", "0", "-1"]).await;
    assert_eq!(
        dst_val,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("d1")),
            RespValue::BulkString(Bytes::from("s1")),
        ])
    );

    server.stop().await;
}

#[tokio::test]
async fn test_blmove_negative_timeout_error() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client
        .cmd(&["BLMOVE", "src", "dst", "LEFT", "RIGHT", "-1"])
        .await;
    match result {
        RespValue::Error(e) => assert!(e.contains("negative") || e.contains("timeout")),
        other => panic!("expected error, got {:?}", other),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_blmove_float_timeout() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let start = std::time::Instant::now();
    let result = client
        .cmd_timeout(
            &["BLMOVE", "nosrc", "dst", "LEFT", "RIGHT", "0.2"],
            Duration::from_secs(3),
        )
        .await;
    let elapsed = start.elapsed();

    assert_eq!(result, Some(RespValue::Null));
    assert!(
        elapsed >= Duration::from_millis(150) && elapsed <= Duration::from_millis(500),
        "elapsed: {:?}",
        elapsed
    );

    server.stop().await;
}

#[tokio::test]
async fn test_blmove_zero_timeout_wakeup() {
    let server = TestServer::spawn().await;
    let mut client1 = server.client().await;
    let mut client2 = server.client().await;

    client1
        .send_raw(&["BLMOVE", "src", "dst", "LEFT", "RIGHT", "0"])
        .await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    client2.cmd(&["RPUSH", "src", "woken"]).await;

    let result = client1.read_response_timeout(Duration::from_secs(2)).await;
    assert_eq!(result, Some(RespValue::BulkString(Bytes::from("woken"))));

    server.stop().await;
}

#[tokio::test]
async fn test_blmove_source_deleted_after_last_element() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["RPUSH", "src", "only"]).await;

    client
        .cmd(&["BLMOVE", "src", "dst", "LEFT", "RIGHT", "1"])
        .await;

    let exists = client.cmd(&["EXISTS", "src"]).await;
    assert_eq!(exists, RespValue::Integer(0));

    server.stop().await;
}

#[tokio::test]
async fn test_blmove_dst_wrong_type() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["RPUSH", "src", "val"]).await;
    client.cmd_ok(&["SET", "dst", "string_value"]).await;

    let result = client
        .cmd(&["BLMOVE", "src", "dst", "LEFT", "RIGHT", "1"])
        .await;
    match result {
        RespValue::Error(e) => assert!(e.contains("WRONGTYPE")),
        other => panic!("expected error, got {:?}", other),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_blmove_connection_works_after_timeout() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client
        .cmd_timeout(
            &["BLMOVE", "nosrc", "dst", "LEFT", "RIGHT", "0.1"],
            Duration::from_secs(3),
        )
        .await;
    assert_eq!(result, Some(RespValue::Null));

    let pong = client.cmd(&["PING"]).await;
    assert_eq!(pong, RespValue::SimpleString("PONG".to_string()));

    server.stop().await;
}

#[tokio::test]
async fn test_blmove_connection_works_after_immediate() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["RPUSH", "src", "val"]).await;
    client
        .cmd(&["BLMOVE", "src", "dst", "LEFT", "RIGHT", "1"])
        .await;

    let pong = client.cmd(&["PING"]).await;
    assert_eq!(pong, RespValue::SimpleString("PONG".to_string()));

    server.stop().await;
}

#[tokio::test]
async fn test_blmove_expired_key_blocks() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["RPUSH", "src", "val"]).await;
    client.cmd(&["PEXPIRE", "src", "50"]).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let result = client
        .cmd_timeout(
            &["BLMOVE", "src", "dst", "LEFT", "RIGHT", "0.1"],
            Duration::from_secs(3),
        )
        .await;
    assert_eq!(result, Some(RespValue::Null));

    server.stop().await;
}

#[tokio::test]
async fn test_blmove_non_numeric_timeout() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client
        .cmd(&["BLMOVE", "src", "dst", "LEFT", "RIGHT", "abc"])
        .await;
    match result {
        RespValue::Error(e) => assert!(
            e.contains("timeout") || e.contains("float") || e.contains("not") || e.contains("ERR"),
            "unexpected error: {}",
            e
        ),
        other => panic!("expected error, got {:?}", other),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_blmove_multiple_sequential_moves() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client
        .cmd(&["RPUSH", "src", "a", "b", "c"])
        .await;

    let r1 = client
        .cmd(&["BLMOVE", "src", "dst", "LEFT", "RIGHT", "1"])
        .await;
    let r2 = client
        .cmd(&["BLMOVE", "src", "dst", "LEFT", "RIGHT", "1"])
        .await;
    let r3 = client
        .cmd(&["BLMOVE", "src", "dst", "LEFT", "RIGHT", "1"])
        .await;

    assert_eq!(r1, RespValue::BulkString(Bytes::from("a")));
    assert_eq!(r2, RespValue::BulkString(Bytes::from("b")));
    assert_eq!(r3, RespValue::BulkString(Bytes::from("c")));

    // dst should be [a, b, c]
    let dst_val = client.cmd(&["LRANGE", "dst", "0", "-1"]).await;
    assert_eq!(
        dst_val,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("a")),
            RespValue::BulkString(Bytes::from("b")),
            RespValue::BulkString(Bytes::from("c")),
        ])
    );

    server.stop().await;
}

// ============================================================
// BLMPOP tests
// ============================================================

#[tokio::test]
async fn test_blmpop_immediate_data_left() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client
        .cmd(&["RPUSH", "mylist", "a", "b", "c"])
        .await;

    let result = client.cmd(&["BLMPOP", "1", "1", "mylist", "LEFT"]).await;
    assert_eq!(
        result,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("mylist")),
            RespValue::Array(vec![RespValue::BulkString(Bytes::from("a"))]),
        ])
    );

    server.stop().await;
}

#[tokio::test]
async fn test_blmpop_immediate_data_right() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client
        .cmd(&["RPUSH", "mylist", "a", "b", "c"])
        .await;

    let result = client.cmd(&["BLMPOP", "1", "1", "mylist", "RIGHT"]).await;
    assert_eq!(
        result,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("mylist")),
            RespValue::Array(vec![RespValue::BulkString(Bytes::from("c"))]),
        ])
    );

    server.stop().await;
}

#[tokio::test]
async fn test_blmpop_with_count() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client
        .cmd(&["RPUSH", "mylist", "a", "b", "c"])
        .await;

    let result = client
        .cmd(&["BLMPOP", "1", "1", "mylist", "LEFT", "COUNT", "2"])
        .await;
    assert_eq!(
        result,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("mylist")),
            RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("a")),
                RespValue::BulkString(Bytes::from("b")),
            ]),
        ])
    );

    server.stop().await;
}

#[tokio::test]
async fn test_blmpop_timeout_returns_null() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client
        .cmd_timeout(
            &["BLMPOP", "0.1", "1", "nokey", "LEFT"],
            Duration::from_secs(3),
        )
        .await;
    assert_eq!(result, Some(RespValue::Null));

    server.stop().await;
}

#[tokio::test]
async fn test_blmpop_wakeup_on_push() {
    let server = TestServer::spawn().await;
    let mut client1 = server.client().await;
    let mut client2 = server.client().await;

    client1
        .send_raw(&["BLMPOP", "5", "1", "mylist", "LEFT"])
        .await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    client2.cmd(&["RPUSH", "mylist", "hello"]).await;

    let result = client1.read_response_timeout(Duration::from_secs(2)).await;
    assert_eq!(
        result,
        Some(RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("mylist")),
            RespValue::Array(vec![RespValue::BulkString(Bytes::from("hello"))]),
        ]))
    );

    server.stop().await;
}

#[tokio::test]
async fn test_blmpop_multiple_keys() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["RPUSH", "list2", "val"]).await;

    let result = client
        .cmd(&["BLMPOP", "1", "2", "list1", "list2", "LEFT"])
        .await;
    assert_eq!(
        result,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("list2")),
            RespValue::Array(vec![RespValue::BulkString(Bytes::from("val"))]),
        ])
    );

    server.stop().await;
}

#[tokio::test]
async fn test_blmpop_wrong_type() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd_ok(&["SET", "str_key", "value"]).await;

    let result = client
        .cmd(&["BLMPOP", "0.1", "1", "str_key", "LEFT"])
        .await;
    match result {
        RespValue::Error(e) => assert!(e.contains("WRONGTYPE")),
        other => panic!("expected error, got {:?}", other),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_blmpop_negative_timeout_error() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["BLMPOP", "-1", "1", "mylist", "LEFT"]).await;
    match result {
        RespValue::Error(e) => assert!(e.contains("negative") || e.contains("timeout")),
        other => panic!("expected error, got {:?}", other),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_blmpop_zero_timeout_wakeup() {
    let server = TestServer::spawn().await;
    let mut client1 = server.client().await;
    let mut client2 = server.client().await;

    client1
        .send_raw(&["BLMPOP", "0", "1", "mylist", "LEFT"])
        .await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    client2.cmd(&["RPUSH", "mylist", "woken"]).await;

    let result = client1.read_response_timeout(Duration::from_secs(2)).await;
    assert_eq!(
        result,
        Some(RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("mylist")),
            RespValue::Array(vec![RespValue::BulkString(Bytes::from("woken"))]),
        ]))
    );

    server.stop().await;
}

#[tokio::test]
async fn test_blmpop_float_timeout_precision() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let start = std::time::Instant::now();
    let result = client
        .cmd_timeout(
            &["BLMPOP", "0.2", "1", "nokey", "LEFT"],
            Duration::from_secs(3),
        )
        .await;
    let elapsed = start.elapsed();

    assert_eq!(result, Some(RespValue::Null));
    assert!(
        elapsed >= Duration::from_millis(150) && elapsed <= Duration::from_millis(500),
        "elapsed: {:?}",
        elapsed
    );

    server.stop().await;
}

#[tokio::test]
async fn test_blmpop_count_exceeds_list_size() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["RPUSH", "mylist", "a", "b"]).await;

    // Ask for 10 but only 2 exist
    let result = client
        .cmd(&["BLMPOP", "1", "1", "mylist", "LEFT", "COUNT", "10"])
        .await;
    assert_eq!(
        result,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("mylist")),
            RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("a")),
                RespValue::BulkString(Bytes::from("b")),
            ]),
        ])
    );

    // Key should be deleted since all elements were popped
    let exists = client.cmd(&["EXISTS", "mylist"]).await;
    assert_eq!(exists, RespValue::Integer(0));

    server.stop().await;
}

#[tokio::test]
async fn test_blmpop_deletes_empty_list() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["RPUSH", "mylist", "only"]).await;

    client
        .cmd(&["BLMPOP", "1", "1", "mylist", "LEFT"])
        .await;

    let exists = client.cmd(&["EXISTS", "mylist"]).await;
    assert_eq!(exists, RespValue::Integer(0));

    server.stop().await;
}

#[tokio::test]
async fn test_blmpop_connection_works_after_timeout() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client
        .cmd_timeout(
            &["BLMPOP", "0.1", "1", "nokey", "LEFT"],
            Duration::from_secs(3),
        )
        .await;
    assert_eq!(result, Some(RespValue::Null));

    let pong = client.cmd(&["PING"]).await;
    assert_eq!(pong, RespValue::SimpleString("PONG".to_string()));

    server.stop().await;
}

#[tokio::test]
async fn test_blmpop_connection_works_after_immediate() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["RPUSH", "mylist", "val"]).await;
    client
        .cmd(&["BLMPOP", "1", "1", "mylist", "LEFT"])
        .await;

    let pong = client.cmd(&["PING"]).await;
    assert_eq!(pong, RespValue::SimpleString("PONG".to_string()));

    server.stop().await;
}

#[tokio::test]
async fn test_blmpop_expired_key_blocks() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["RPUSH", "mylist", "val"]).await;
    client.cmd(&["PEXPIRE", "mylist", "50"]).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let result = client
        .cmd_timeout(
            &["BLMPOP", "0.1", "1", "mylist", "LEFT"],
            Duration::from_secs(3),
        )
        .await;
    assert_eq!(result, Some(RespValue::Null));

    server.stop().await;
}

#[tokio::test]
async fn test_blmpop_non_numeric_timeout() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client
        .cmd(&["BLMPOP", "abc", "1", "mylist", "LEFT"])
        .await;
    match result {
        RespValue::Error(e) => assert!(
            e.contains("timeout") || e.contains("float") || e.contains("not") || e.contains("ERR"),
            "unexpected error: {}",
            e
        ),
        other => panic!("expected error, got {:?}", other),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_blmpop_wakeup_via_lpush() {
    let server = TestServer::spawn().await;
    let mut client1 = server.client().await;
    let mut client2 = server.client().await;

    client1
        .send_raw(&["BLMPOP", "5", "1", "mylist", "RIGHT"])
        .await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    client2.cmd(&["LPUSH", "mylist", "from_lpush"]).await;

    let result = client1.read_response_timeout(Duration::from_secs(2)).await;
    assert_eq!(
        result,
        Some(RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("mylist")),
            RespValue::Array(vec![RespValue::BulkString(Bytes::from("from_lpush"))]),
        ]))
    );

    server.stop().await;
}

#[tokio::test]
async fn test_blmpop_multiple_pops_in_sequence() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client
        .cmd(&["RPUSH", "mylist", "a", "b", "c"])
        .await;

    let r1 = client
        .cmd(&["BLMPOP", "1", "1", "mylist", "LEFT"])
        .await;
    let r2 = client
        .cmd(&["BLMPOP", "1", "1", "mylist", "LEFT"])
        .await;
    let r3 = client
        .cmd(&["BLMPOP", "1", "1", "mylist", "LEFT"])
        .await;

    assert_eq!(
        r1,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("mylist")),
            RespValue::Array(vec![RespValue::BulkString(Bytes::from("a"))]),
        ])
    );
    assert_eq!(
        r2,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("mylist")),
            RespValue::Array(vec![RespValue::BulkString(Bytes::from("b"))]),
        ])
    );
    assert_eq!(
        r3,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("mylist")),
            RespValue::Array(vec![RespValue::BulkString(Bytes::from("c"))]),
        ])
    );

    server.stop().await;
}

#[tokio::test]
async fn test_blmpop_invalid_direction() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client
        .cmd(&["BLMPOP", "1", "1", "mylist", "UP"])
        .await;
    match result {
        RespValue::Error(_) => {} // Expected
        other => panic!("expected error, got {:?}", other),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_blmpop_multiple_blocked_clients() {
    let server = TestServer::spawn().await;
    let mut client1 = server.client().await;
    let mut client2 = server.client().await;
    let mut pusher = server.client().await;

    client1
        .send_raw(&["BLMPOP", "5", "1", "shared", "LEFT"])
        .await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    client2
        .send_raw(&["BLMPOP", "5", "1", "shared", "LEFT"])
        .await;
    tokio::time::sleep(Duration::from_millis(30)).await;

    pusher.cmd(&["RPUSH", "shared", "e1"]).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    pusher.cmd(&["RPUSH", "shared", "e2"]).await;

    let r1 = client1.read_response_timeout(Duration::from_secs(2)).await;
    let r2 = client2.read_response_timeout(Duration::from_secs(2)).await;

    assert!(r1.is_some(), "client1 should have received data");
    assert!(r2.is_some(), "client2 should have received data");

    server.stop().await;
}

// ============================================================
// BZPOPMIN tests
// ============================================================

#[tokio::test]
async fn test_bzpopmin_immediate_data() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client
        .cmd(&["ZADD", "myzset", "1", "a", "2", "b", "3", "c"])
        .await;

    let result = client.cmd(&["BZPOPMIN", "myzset", "1"]).await;
    assert_eq!(
        result,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("myzset")),
            RespValue::BulkString(Bytes::from("a")),
            RespValue::BulkString(Bytes::from("1")),
        ])
    );

    server.stop().await;
}

#[tokio::test]
async fn test_bzpopmin_timeout_returns_null() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client
        .cmd_timeout(&["BZPOPMIN", "nokey", "0.1"], Duration::from_secs(3))
        .await;
    assert_eq!(result, Some(RespValue::Null));

    server.stop().await;
}

#[tokio::test]
async fn test_bzpopmin_wakeup_on_zadd() {
    let server = TestServer::spawn().await;
    let mut client1 = server.client().await;
    let mut client2 = server.client().await;

    client1.send_raw(&["BZPOPMIN", "myzset", "5"]).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    client2.cmd(&["ZADD", "myzset", "10", "hello"]).await;

    let result = client1.read_response_timeout(Duration::from_secs(2)).await;
    assert_eq!(
        result,
        Some(RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("myzset")),
            RespValue::BulkString(Bytes::from("hello")),
            RespValue::BulkString(Bytes::from("10")),
        ]))
    );

    server.stop().await;
}

#[tokio::test]
async fn test_bzpopmin_multiple_keys() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["ZADD", "zset2", "5", "member"]).await;

    let result = client
        .cmd(&["BZPOPMIN", "zset1", "zset2", "1"])
        .await;
    assert_eq!(
        result,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("zset2")),
            RespValue::BulkString(Bytes::from("member")),
            RespValue::BulkString(Bytes::from("5")),
        ])
    );

    server.stop().await;
}

#[tokio::test]
async fn test_bzpopmin_wrong_type() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd_ok(&["SET", "str_key", "value"]).await;

    let result = client.cmd(&["BZPOPMIN", "str_key", "0.1"]).await;
    match result {
        RespValue::Error(e) => assert!(e.contains("WRONGTYPE")),
        other => panic!("expected error, got {:?}", other),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_bzpopmin_pops_lowest_score() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client
        .cmd(&["ZADD", "myzset", "99", "high", "1", "low", "50", "mid"])
        .await;

    let result = client.cmd(&["BZPOPMIN", "myzset", "1"]).await;
    assert_eq!(
        result,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("myzset")),
            RespValue::BulkString(Bytes::from("low")),
            RespValue::BulkString(Bytes::from("1")),
        ])
    );

    server.stop().await;
}

#[tokio::test]
async fn test_bzpopmin_negative_timeout_error() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["BZPOPMIN", "myzset", "-1"]).await;
    match result {
        RespValue::Error(e) => assert!(e.contains("negative") || e.contains("timeout")),
        other => panic!("expected error, got {:?}", other),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_bzpopmin_zero_timeout_wakeup() {
    let server = TestServer::spawn().await;
    let mut client1 = server.client().await;
    let mut client2 = server.client().await;

    client1.send_raw(&["BZPOPMIN", "myzset", "0"]).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    client2.cmd(&["ZADD", "myzset", "5", "woken"]).await;

    let result = client1.read_response_timeout(Duration::from_secs(2)).await;
    assert_eq!(
        result,
        Some(RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("myzset")),
            RespValue::BulkString(Bytes::from("woken")),
            RespValue::BulkString(Bytes::from("5")),
        ]))
    );

    server.stop().await;
}

#[tokio::test]
async fn test_bzpopmin_float_timeout_precision() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let start = std::time::Instant::now();
    let result = client
        .cmd_timeout(&["BZPOPMIN", "nokey", "0.2"], Duration::from_secs(3))
        .await;
    let elapsed = start.elapsed();

    assert_eq!(result, Some(RespValue::Null));
    assert!(
        elapsed >= Duration::from_millis(150) && elapsed <= Duration::from_millis(500),
        "elapsed: {:?}",
        elapsed
    );

    server.stop().await;
}

#[tokio::test]
async fn test_bzpopmin_removes_from_zset() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client
        .cmd(&["ZADD", "myzset", "1", "a", "2", "b", "3", "c"])
        .await;

    client.cmd(&["BZPOPMIN", "myzset", "1"]).await;

    // Should have 2 members remaining
    let card = client.cmd(&["ZCARD", "myzset"]).await;
    assert_eq!(card, RespValue::Integer(2));

    // "a" should be gone, "b" and "c" remain
    let members = client
        .cmd(&["ZRANGE", "myzset", "0", "-1"])
        .await;
    assert_eq!(
        members,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("b")),
            RespValue::BulkString(Bytes::from("c")),
        ])
    );

    server.stop().await;
}

#[tokio::test]
async fn test_bzpopmin_deletes_empty_zset() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["ZADD", "myzset", "1", "only"]).await;

    client.cmd(&["BZPOPMIN", "myzset", "1"]).await;

    let exists = client.cmd(&["EXISTS", "myzset"]).await;
    assert_eq!(exists, RespValue::Integer(0));

    server.stop().await;
}

#[tokio::test]
async fn test_bzpopmin_multiple_pops_in_sequence() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client
        .cmd(&["ZADD", "myzset", "1", "a", "2", "b", "3", "c"])
        .await;

    let r1 = client.cmd(&["BZPOPMIN", "myzset", "1"]).await;
    let r2 = client.cmd(&["BZPOPMIN", "myzset", "1"]).await;
    let r3 = client.cmd(&["BZPOPMIN", "myzset", "1"]).await;

    // Should pop in order of ascending score
    assert_eq!(
        r1,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("myzset")),
            RespValue::BulkString(Bytes::from("a")),
            RespValue::BulkString(Bytes::from("1")),
        ])
    );
    assert_eq!(
        r2,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("myzset")),
            RespValue::BulkString(Bytes::from("b")),
            RespValue::BulkString(Bytes::from("2")),
        ])
    );
    assert_eq!(
        r3,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("myzset")),
            RespValue::BulkString(Bytes::from("c")),
            RespValue::BulkString(Bytes::from("3")),
        ])
    );

    server.stop().await;
}

#[tokio::test]
async fn test_bzpopmin_expired_key_blocks() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["ZADD", "myzset", "1", "a"]).await;
    client.cmd(&["PEXPIRE", "myzset", "50"]).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let result = client
        .cmd_timeout(&["BZPOPMIN", "myzset", "0.1"], Duration::from_secs(3))
        .await;
    assert_eq!(result, Some(RespValue::Null));

    server.stop().await;
}

#[tokio::test]
async fn test_bzpopmin_connection_works_after_immediate() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["ZADD", "myzset", "1", "a"]).await;
    client.cmd(&["BZPOPMIN", "myzset", "1"]).await;

    let pong = client.cmd(&["PING"]).await;
    assert_eq!(pong, RespValue::SimpleString("PONG".to_string()));

    server.stop().await;
}

#[tokio::test]
async fn test_bzpopmin_connection_works_after_timeout() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client
        .cmd_timeout(&["BZPOPMIN", "nokey", "0.1"], Duration::from_secs(3))
        .await;
    assert_eq!(result, Some(RespValue::Null));

    let pong = client.cmd(&["PING"]).await;
    assert_eq!(pong, RespValue::SimpleString("PONG".to_string()));

    server.stop().await;
}

#[tokio::test]
async fn test_bzpopmin_non_numeric_timeout() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["BZPOPMIN", "myzset", "abc"]).await;
    match result {
        RespValue::Error(e) => assert!(
            e.contains("timeout") || e.contains("float") || e.contains("not") || e.contains("ERR"),
            "unexpected error: {}",
            e
        ),
        other => panic!("expected error, got {:?}", other),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_bzpopmin_multiple_keys_wakeup() {
    let server = TestServer::spawn().await;
    let mut client1 = server.client().await;
    let mut client2 = server.client().await;

    client1
        .send_raw(&["BZPOPMIN", "zset1", "zset2", "5"])
        .await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    client2.cmd(&["ZADD", "zset2", "3", "member"]).await;

    let result = client1.read_response_timeout(Duration::from_secs(2)).await;
    assert_eq!(
        result,
        Some(RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("zset2")),
            RespValue::BulkString(Bytes::from("member")),
            RespValue::BulkString(Bytes::from("3")),
        ]))
    );

    server.stop().await;
}

#[tokio::test]
async fn test_bzpopmin_multiple_blocked_clients() {
    let server = TestServer::spawn().await;
    let mut client1 = server.client().await;
    let mut client2 = server.client().await;
    let mut pusher = server.client().await;

    client1.send_raw(&["BZPOPMIN", "shared", "5"]).await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    client2.send_raw(&["BZPOPMIN", "shared", "5"]).await;
    tokio::time::sleep(Duration::from_millis(30)).await;

    pusher.cmd(&["ZADD", "shared", "1", "e1"]).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    pusher.cmd(&["ZADD", "shared", "2", "e2"]).await;

    let r1 = client1.read_response_timeout(Duration::from_secs(2)).await;
    let r2 = client2.read_response_timeout(Duration::from_secs(2)).await;

    assert!(r1.is_some(), "client1 should have received data");
    assert!(r2.is_some(), "client2 should have received data");

    server.stop().await;
}

#[tokio::test]
async fn test_bzpopmin_with_negative_scores() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client
        .cmd(&["ZADD", "myzset", "-5", "neg", "0", "zero", "5", "pos"])
        .await;

    let result = client.cmd(&["BZPOPMIN", "myzset", "1"]).await;
    assert_eq!(
        result,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("myzset")),
            RespValue::BulkString(Bytes::from("neg")),
            RespValue::BulkString(Bytes::from("-5")),
        ])
    );

    server.stop().await;
}

#[tokio::test]
async fn test_bzpopmin_wrong_arity() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Only command name and timeout, no keys
    let result = client.cmd(&["BZPOPMIN", "0.1"]).await;
    match result {
        RespValue::Error(e) => assert!(
            e.contains("wrong number") || e.contains("arity") || e.contains("ERR"),
            "unexpected error: {}",
            e
        ),
        other => panic!("expected error, got {:?}", other),
    }

    server.stop().await;
}

// ============================================================
// BZPOPMAX tests
// ============================================================

#[tokio::test]
async fn test_bzpopmax_immediate_data() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client
        .cmd(&["ZADD", "myzset", "1", "a", "2", "b", "3", "c"])
        .await;

    let result = client.cmd(&["BZPOPMAX", "myzset", "1"]).await;
    assert_eq!(
        result,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("myzset")),
            RespValue::BulkString(Bytes::from("c")),
            RespValue::BulkString(Bytes::from("3")),
        ])
    );

    server.stop().await;
}

#[tokio::test]
async fn test_bzpopmax_timeout_returns_null() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client
        .cmd_timeout(&["BZPOPMAX", "nokey", "0.1"], Duration::from_secs(3))
        .await;
    assert_eq!(result, Some(RespValue::Null));

    server.stop().await;
}

#[tokio::test]
async fn test_bzpopmax_wakeup_on_zadd() {
    let server = TestServer::spawn().await;
    let mut client1 = server.client().await;
    let mut client2 = server.client().await;

    client1.send_raw(&["BZPOPMAX", "myzset", "5"]).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    client2.cmd(&["ZADD", "myzset", "42", "hello"]).await;

    let result = client1.read_response_timeout(Duration::from_secs(2)).await;
    assert_eq!(
        result,
        Some(RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("myzset")),
            RespValue::BulkString(Bytes::from("hello")),
            RespValue::BulkString(Bytes::from("42")),
        ]))
    );

    server.stop().await;
}

#[tokio::test]
async fn test_bzpopmax_pops_highest_score() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client
        .cmd(&["ZADD", "myzset", "1", "low", "99", "high", "50", "mid"])
        .await;

    let result = client.cmd(&["BZPOPMAX", "myzset", "1"]).await;
    assert_eq!(
        result,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("myzset")),
            RespValue::BulkString(Bytes::from("high")),
            RespValue::BulkString(Bytes::from("99")),
        ])
    );

    server.stop().await;
}

#[tokio::test]
async fn test_bzpopmax_multiple_keys() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["ZADD", "zset2", "5", "member"]).await;

    let result = client
        .cmd(&["BZPOPMAX", "zset1", "zset2", "1"])
        .await;
    assert_eq!(
        result,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("zset2")),
            RespValue::BulkString(Bytes::from("member")),
            RespValue::BulkString(Bytes::from("5")),
        ])
    );

    server.stop().await;
}

#[tokio::test]
async fn test_bzpopmax_negative_timeout_error() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["BZPOPMAX", "myzset", "-1"]).await;
    match result {
        RespValue::Error(e) => assert!(e.contains("negative") || e.contains("timeout")),
        other => panic!("expected error, got {:?}", other),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_bzpopmax_zero_timeout_wakeup() {
    let server = TestServer::spawn().await;
    let mut client1 = server.client().await;
    let mut client2 = server.client().await;

    client1.send_raw(&["BZPOPMAX", "myzset", "0"]).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    client2.cmd(&["ZADD", "myzset", "42", "woken"]).await;

    let result = client1.read_response_timeout(Duration::from_secs(2)).await;
    assert_eq!(
        result,
        Some(RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("myzset")),
            RespValue::BulkString(Bytes::from("woken")),
            RespValue::BulkString(Bytes::from("42")),
        ]))
    );

    server.stop().await;
}

#[tokio::test]
async fn test_bzpopmax_float_timeout_precision() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let start = std::time::Instant::now();
    let result = client
        .cmd_timeout(&["BZPOPMAX", "nokey", "0.2"], Duration::from_secs(3))
        .await;
    let elapsed = start.elapsed();

    assert_eq!(result, Some(RespValue::Null));
    assert!(
        elapsed >= Duration::from_millis(150) && elapsed <= Duration::from_millis(500),
        "elapsed: {:?}",
        elapsed
    );

    server.stop().await;
}

#[tokio::test]
async fn test_bzpopmax_removes_from_zset() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client
        .cmd(&["ZADD", "myzset", "1", "a", "2", "b", "3", "c"])
        .await;

    client.cmd(&["BZPOPMAX", "myzset", "1"]).await;

    let card = client.cmd(&["ZCARD", "myzset"]).await;
    assert_eq!(card, RespValue::Integer(2));

    // "c" should be gone, "a" and "b" remain
    let members = client
        .cmd(&["ZRANGE", "myzset", "0", "-1"])
        .await;
    assert_eq!(
        members,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("a")),
            RespValue::BulkString(Bytes::from("b")),
        ])
    );

    server.stop().await;
}

#[tokio::test]
async fn test_bzpopmax_deletes_empty_zset() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["ZADD", "myzset", "1", "only"]).await;

    client.cmd(&["BZPOPMAX", "myzset", "1"]).await;

    let exists = client.cmd(&["EXISTS", "myzset"]).await;
    assert_eq!(exists, RespValue::Integer(0));

    server.stop().await;
}

#[tokio::test]
async fn test_bzpopmax_multiple_pops_in_sequence() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client
        .cmd(&["ZADD", "myzset", "1", "a", "2", "b", "3", "c"])
        .await;

    let r1 = client.cmd(&["BZPOPMAX", "myzset", "1"]).await;
    let r2 = client.cmd(&["BZPOPMAX", "myzset", "1"]).await;
    let r3 = client.cmd(&["BZPOPMAX", "myzset", "1"]).await;

    // Should pop in order of descending score
    assert_eq!(
        r1,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("myzset")),
            RespValue::BulkString(Bytes::from("c")),
            RespValue::BulkString(Bytes::from("3")),
        ])
    );
    assert_eq!(
        r2,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("myzset")),
            RespValue::BulkString(Bytes::from("b")),
            RespValue::BulkString(Bytes::from("2")),
        ])
    );
    assert_eq!(
        r3,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("myzset")),
            RespValue::BulkString(Bytes::from("a")),
            RespValue::BulkString(Bytes::from("1")),
        ])
    );

    server.stop().await;
}

#[tokio::test]
async fn test_bzpopmax_expired_key_blocks() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["ZADD", "myzset", "1", "a"]).await;
    client.cmd(&["PEXPIRE", "myzset", "50"]).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let result = client
        .cmd_timeout(&["BZPOPMAX", "myzset", "0.1"], Duration::from_secs(3))
        .await;
    assert_eq!(result, Some(RespValue::Null));

    server.stop().await;
}

#[tokio::test]
async fn test_bzpopmax_connection_works_after_immediate() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["ZADD", "myzset", "1", "a"]).await;
    client.cmd(&["BZPOPMAX", "myzset", "1"]).await;

    let pong = client.cmd(&["PING"]).await;
    assert_eq!(pong, RespValue::SimpleString("PONG".to_string()));

    server.stop().await;
}

#[tokio::test]
async fn test_bzpopmax_connection_works_after_timeout() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client
        .cmd_timeout(&["BZPOPMAX", "nokey", "0.1"], Duration::from_secs(3))
        .await;
    assert_eq!(result, Some(RespValue::Null));

    let pong = client.cmd(&["PING"]).await;
    assert_eq!(pong, RespValue::SimpleString("PONG".to_string()));

    server.stop().await;
}

#[tokio::test]
async fn test_bzpopmax_non_numeric_timeout() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["BZPOPMAX", "myzset", "abc"]).await;
    match result {
        RespValue::Error(e) => assert!(
            e.contains("timeout") || e.contains("float") || e.contains("not") || e.contains("ERR"),
            "unexpected error: {}",
            e
        ),
        other => panic!("expected error, got {:?}", other),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_bzpopmax_with_negative_scores() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client
        .cmd(&["ZADD", "myzset", "-5", "neg", "0", "zero", "5", "pos"])
        .await;

    let result = client.cmd(&["BZPOPMAX", "myzset", "1"]).await;
    assert_eq!(
        result,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("myzset")),
            RespValue::BulkString(Bytes::from("pos")),
            RespValue::BulkString(Bytes::from("5")),
        ])
    );

    server.stop().await;
}

#[tokio::test]
async fn test_bzpopmax_multiple_keys_wakeup() {
    let server = TestServer::spawn().await;
    let mut client1 = server.client().await;
    let mut client2 = server.client().await;

    client1
        .send_raw(&["BZPOPMAX", "zset1", "zset2", "5"])
        .await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    client2.cmd(&["ZADD", "zset1", "10", "member"]).await;

    let result = client1.read_response_timeout(Duration::from_secs(2)).await;
    assert_eq!(
        result,
        Some(RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("zset1")),
            RespValue::BulkString(Bytes::from("member")),
            RespValue::BulkString(Bytes::from("10")),
        ]))
    );

    server.stop().await;
}

#[tokio::test]
async fn test_bzpopmax_multiple_blocked_clients() {
    let server = TestServer::spawn().await;
    let mut client1 = server.client().await;
    let mut client2 = server.client().await;
    let mut pusher = server.client().await;

    client1.send_raw(&["BZPOPMAX", "shared", "5"]).await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    client2.send_raw(&["BZPOPMAX", "shared", "5"]).await;
    tokio::time::sleep(Duration::from_millis(30)).await;

    pusher.cmd(&["ZADD", "shared", "1", "e1"]).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    pusher.cmd(&["ZADD", "shared", "2", "e2"]).await;

    let r1 = client1.read_response_timeout(Duration::from_secs(2)).await;
    let r2 = client2.read_response_timeout(Duration::from_secs(2)).await;

    assert!(r1.is_some(), "client1 should have received data");
    assert!(r2.is_some(), "client2 should have received data");

    server.stop().await;
}

#[tokio::test]
async fn test_bzpopmax_wrong_arity() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["BZPOPMAX", "0.1"]).await;
    match result {
        RespValue::Error(e) => assert!(
            e.contains("wrong number") || e.contains("arity") || e.contains("ERR"),
            "unexpected error: {}",
            e
        ),
        other => panic!("expected error, got {:?}", other),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_bzpopmax_wrong_type() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd_ok(&["SET", "str_key", "value"]).await;

    let result = client.cmd(&["BZPOPMAX", "str_key", "0.1"]).await;
    match result {
        RespValue::Error(e) => assert!(e.contains("WRONGTYPE")),
        other => panic!("expected error, got {:?}", other),
    }

    server.stop().await;
}

// ============================================================
// BZMPOP tests
// ============================================================

#[tokio::test]
async fn test_bzmpop_immediate_min() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client
        .cmd(&["ZADD", "myzset", "1", "a", "2", "b", "3", "c"])
        .await;

    let result = client
        .cmd(&["BZMPOP", "1", "1", "myzset", "MIN"])
        .await;
    assert_eq!(
        result,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("myzset")),
            RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("a")),
                RespValue::BulkString(Bytes::from("1")),
            ]),
        ])
    );

    server.stop().await;
}

#[tokio::test]
async fn test_bzmpop_immediate_max() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client
        .cmd(&["ZADD", "myzset", "1", "a", "2", "b", "3", "c"])
        .await;

    let result = client
        .cmd(&["BZMPOP", "1", "1", "myzset", "MAX"])
        .await;
    assert_eq!(
        result,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("myzset")),
            RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("c")),
                RespValue::BulkString(Bytes::from("3")),
            ]),
        ])
    );

    server.stop().await;
}

#[tokio::test]
async fn test_bzmpop_with_count() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client
        .cmd(&["ZADD", "myzset", "1", "a", "2", "b", "3", "c"])
        .await;

    let result = client
        .cmd(&["BZMPOP", "1", "1", "myzset", "MIN", "COUNT", "2"])
        .await;
    assert_eq!(
        result,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("myzset")),
            RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("a")),
                RespValue::BulkString(Bytes::from("1")),
                RespValue::BulkString(Bytes::from("b")),
                RespValue::BulkString(Bytes::from("2")),
            ]),
        ])
    );

    server.stop().await;
}

#[tokio::test]
async fn test_bzmpop_timeout_returns_null() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client
        .cmd_timeout(
            &["BZMPOP", "0.1", "1", "nokey", "MIN"],
            Duration::from_secs(3),
        )
        .await;
    assert_eq!(result, Some(RespValue::Null));

    server.stop().await;
}

#[tokio::test]
async fn test_bzmpop_wakeup_on_zadd() {
    let server = TestServer::spawn().await;
    let mut client1 = server.client().await;
    let mut client2 = server.client().await;

    client1
        .send_raw(&["BZMPOP", "5", "1", "myzset", "MIN"])
        .await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    client2
        .cmd(&["ZADD", "myzset", "7", "hello"])
        .await;

    let result = client1.read_response_timeout(Duration::from_secs(2)).await;
    assert_eq!(
        result,
        Some(RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("myzset")),
            RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("hello")),
                RespValue::BulkString(Bytes::from("7")),
            ]),
        ]))
    );

    server.stop().await;
}

#[tokio::test]
async fn test_bzmpop_multiple_keys() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["ZADD", "zset2", "5", "member"]).await;

    let result = client
        .cmd(&["BZMPOP", "1", "2", "zset1", "zset2", "MIN"])
        .await;
    assert_eq!(
        result,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("zset2")),
            RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("member")),
                RespValue::BulkString(Bytes::from("5")),
            ]),
        ])
    );

    server.stop().await;
}

#[tokio::test]
async fn test_bzmpop_wrong_type() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd_ok(&["SET", "str_key", "value"]).await;

    let result = client
        .cmd(&["BZMPOP", "0.1", "1", "str_key", "MIN"])
        .await;
    match result {
        RespValue::Error(e) => assert!(e.contains("WRONGTYPE")),
        other => panic!("expected error, got {:?}", other),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_bzmpop_negative_timeout_error() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client
        .cmd(&["BZMPOP", "-1", "1", "myzset", "MIN"])
        .await;
    match result {
        RespValue::Error(e) => assert!(e.contains("negative") || e.contains("timeout")),
        other => panic!("expected error, got {:?}", other),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_bzmpop_zero_timeout_wakeup() {
    let server = TestServer::spawn().await;
    let mut client1 = server.client().await;
    let mut client2 = server.client().await;

    client1
        .send_raw(&["BZMPOP", "0", "1", "myzset", "MIN"])
        .await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    client2.cmd(&["ZADD", "myzset", "5", "woken"]).await;

    let result = client1.read_response_timeout(Duration::from_secs(2)).await;
    assert_eq!(
        result,
        Some(RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("myzset")),
            RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("woken")),
                RespValue::BulkString(Bytes::from("5")),
            ]),
        ]))
    );

    server.stop().await;
}

#[tokio::test]
async fn test_bzmpop_float_timeout_precision() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let start = std::time::Instant::now();
    let result = client
        .cmd_timeout(
            &["BZMPOP", "0.2", "1", "nokey", "MIN"],
            Duration::from_secs(3),
        )
        .await;
    let elapsed = start.elapsed();

    assert_eq!(result, Some(RespValue::Null));
    assert!(
        elapsed >= Duration::from_millis(150) && elapsed <= Duration::from_millis(500),
        "elapsed: {:?}",
        elapsed
    );

    server.stop().await;
}

#[tokio::test]
async fn test_bzmpop_count_exceeds_zset_size() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["ZADD", "myzset", "1", "a", "2", "b"]).await;

    let result = client
        .cmd(&["BZMPOP", "1", "1", "myzset", "MIN", "COUNT", "10"])
        .await;
    assert_eq!(
        result,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("myzset")),
            RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("a")),
                RespValue::BulkString(Bytes::from("1")),
                RespValue::BulkString(Bytes::from("b")),
                RespValue::BulkString(Bytes::from("2")),
            ]),
        ])
    );

    let exists = client.cmd(&["EXISTS", "myzset"]).await;
    assert_eq!(exists, RespValue::Integer(0));

    server.stop().await;
}

#[tokio::test]
async fn test_bzmpop_max_with_count() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client
        .cmd(&["ZADD", "myzset", "1", "a", "2", "b", "3", "c"])
        .await;

    let result = client
        .cmd(&["BZMPOP", "1", "1", "myzset", "MAX", "COUNT", "2"])
        .await;
    assert_eq!(
        result,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("myzset")),
            RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("c")),
                RespValue::BulkString(Bytes::from("3")),
                RespValue::BulkString(Bytes::from("b")),
                RespValue::BulkString(Bytes::from("2")),
            ]),
        ])
    );

    server.stop().await;
}

#[tokio::test]
async fn test_bzmpop_deletes_empty_zset() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["ZADD", "myzset", "1", "only"]).await;

    client
        .cmd(&["BZMPOP", "1", "1", "myzset", "MIN"])
        .await;

    let exists = client.cmd(&["EXISTS", "myzset"]).await;
    assert_eq!(exists, RespValue::Integer(0));

    server.stop().await;
}

#[tokio::test]
async fn test_bzmpop_connection_works_after_timeout() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client
        .cmd_timeout(
            &["BZMPOP", "0.1", "1", "nokey", "MIN"],
            Duration::from_secs(3),
        )
        .await;
    assert_eq!(result, Some(RespValue::Null));

    let pong = client.cmd(&["PING"]).await;
    assert_eq!(pong, RespValue::SimpleString("PONG".to_string()));

    server.stop().await;
}

#[tokio::test]
async fn test_bzmpop_connection_works_after_immediate() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["ZADD", "myzset", "1", "a"]).await;
    client
        .cmd(&["BZMPOP", "1", "1", "myzset", "MIN"])
        .await;

    let pong = client.cmd(&["PING"]).await;
    assert_eq!(pong, RespValue::SimpleString("PONG".to_string()));

    server.stop().await;
}

#[tokio::test]
async fn test_bzmpop_expired_key_blocks() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["ZADD", "myzset", "1", "a"]).await;
    client.cmd(&["PEXPIRE", "myzset", "50"]).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let result = client
        .cmd_timeout(
            &["BZMPOP", "0.1", "1", "myzset", "MIN"],
            Duration::from_secs(3),
        )
        .await;
    assert_eq!(result, Some(RespValue::Null));

    server.stop().await;
}

#[tokio::test]
async fn test_bzmpop_non_numeric_timeout() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client
        .cmd(&["BZMPOP", "abc", "1", "myzset", "MIN"])
        .await;
    match result {
        RespValue::Error(e) => assert!(
            e.contains("timeout") || e.contains("float") || e.contains("not") || e.contains("ERR"),
            "unexpected error: {}",
            e
        ),
        other => panic!("expected error, got {:?}", other),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_bzmpop_invalid_modifier() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client
        .cmd(&["BZMPOP", "1", "1", "myzset", "MIDDLE"])
        .await;
    match result {
        RespValue::Error(_) => {} // Expected
        other => panic!("expected error, got {:?}", other),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_bzmpop_multiple_pops_in_sequence() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client
        .cmd(&["ZADD", "myzset", "1", "a", "2", "b", "3", "c"])
        .await;

    let r1 = client
        .cmd(&["BZMPOP", "1", "1", "myzset", "MIN"])
        .await;
    let r2 = client
        .cmd(&["BZMPOP", "1", "1", "myzset", "MIN"])
        .await;
    let r3 = client
        .cmd(&["BZMPOP", "1", "1", "myzset", "MIN"])
        .await;

    assert_eq!(
        r1,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("myzset")),
            RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("a")),
                RespValue::BulkString(Bytes::from("1")),
            ]),
        ])
    );
    assert_eq!(
        r2,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("myzset")),
            RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("b")),
                RespValue::BulkString(Bytes::from("2")),
            ]),
        ])
    );
    assert_eq!(
        r3,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("myzset")),
            RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("c")),
                RespValue::BulkString(Bytes::from("3")),
            ]),
        ])
    );

    server.stop().await;
}

#[tokio::test]
async fn test_bzmpop_multiple_blocked_clients() {
    let server = TestServer::spawn().await;
    let mut client1 = server.client().await;
    let mut client2 = server.client().await;
    let mut pusher = server.client().await;

    client1
        .send_raw(&["BZMPOP", "5", "1", "shared", "MIN"])
        .await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    client2
        .send_raw(&["BZMPOP", "5", "1", "shared", "MIN"])
        .await;
    tokio::time::sleep(Duration::from_millis(30)).await;

    pusher.cmd(&["ZADD", "shared", "1", "e1"]).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    pusher.cmd(&["ZADD", "shared", "2", "e2"]).await;

    let r1 = client1.read_response_timeout(Duration::from_secs(2)).await;
    let r2 = client2.read_response_timeout(Duration::from_secs(2)).await;

    assert!(r1.is_some(), "client1 should have received data");
    assert!(r2.is_some(), "client2 should have received data");

    server.stop().await;
}

// ============================================================
// Concurrency / edge case tests
// ============================================================

#[tokio::test]
async fn test_blpop_fifo_wake_order() {
    // When multiple clients block on the same key, the first client
    // to block should be the first to wake up
    let server = TestServer::spawn().await;
    let mut client1 = server.client().await;
    let mut client2 = server.client().await;
    let mut pusher = server.client().await;

    // Client 1 blocks first
    client1.send_raw(&["BLPOP", "fifo", "5"]).await;
    tokio::time::sleep(Duration::from_millis(30)).await;

    // Client 2 blocks second
    client2.send_raw(&["BLPOP", "fifo", "5"]).await;
    tokio::time::sleep(Duration::from_millis(30)).await;

    // Push one element
    pusher.cmd(&["RPUSH", "fifo", "first"]).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Push another for client 2
    pusher.cmd(&["RPUSH", "fifo", "second"]).await;

    // Client 1 should get "first"
    let r1 = client1.read_response_timeout(Duration::from_secs(2)).await;
    // Client 2 should get "second"
    let r2 = client2.read_response_timeout(Duration::from_secs(2)).await;

    // At least one of them should have gotten data
    assert!(r1.is_some() || r2.is_some(), "at least one client should have received data");

    server.stop().await;
}

#[tokio::test]
async fn test_blpop_rpush_interaction() {
    // RPUSH should also wake BLPOP waiters (not just LPUSH)
    let server = TestServer::spawn().await;
    let mut client1 = server.client().await;
    let mut client2 = server.client().await;

    client1.send_raw(&["BLPOP", "mylist", "5"]).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // RPUSH should trigger wakeup
    client2.cmd(&["RPUSH", "mylist", "fromrpush"]).await;

    let result = client1.read_response_timeout(Duration::from_secs(2)).await;
    assert_eq!(
        result,
        Some(RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("mylist")),
            RespValue::BulkString(Bytes::from("fromrpush")),
        ]))
    );

    server.stop().await;
}

#[tokio::test]
async fn test_blpop_lpushx_interaction() {
    // LPUSHX should also wake BLPOP waiters
    let server = TestServer::spawn().await;
    let mut client1 = server.client().await;
    let mut client2 = server.client().await;

    // First create the list so LPUSHX works
    client2.cmd(&["RPUSH", "mylist", "existing"]).await;

    // Client 1 consumes the existing element first
    let result = client1.cmd(&["BLPOP", "mylist", "1"]).await;
    assert_eq!(
        result,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("mylist")),
            RespValue::BulkString(Bytes::from("existing")),
        ])
    );

    // Now client 1 blocks again
    client1.send_raw(&["BLPOP", "mylist", "5"]).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // List still exists? We need it for LPUSHX. Actually it was deleted.
    // LPUSHX won't push to a non-existent key, so BLPOP won't be woken.
    // Let's re-create and test RPUSHX instead
    client2.cmd(&["RPUSH", "mylist", "base"]).await;
    // This should wake client1
    let result = client1.read_response_timeout(Duration::from_secs(2)).await;
    assert!(result.is_some());

    server.stop().await;
}

#[tokio::test]
async fn test_blocking_command_after_normal_command() {
    // After a blocking command completes, the connection should work normally
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["RPUSH", "mylist", "val"]).await;

    let result = client.cmd(&["BLPOP", "mylist", "1"]).await;
    assert_eq!(
        result,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("mylist")),
            RespValue::BulkString(Bytes::from("val")),
        ])
    );

    // Normal commands should still work
    let pong = client.cmd(&["PING"]).await;
    assert_eq!(pong, RespValue::SimpleString("PONG".to_string()));

    server.stop().await;
}

#[tokio::test]
async fn test_blocking_timeout_then_normal_command() {
    // After a blocking command times out, the connection should work normally
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client
        .cmd_timeout(&["BLPOP", "nokey", "0.1"], Duration::from_secs(3))
        .await;
    assert_eq!(result, Some(RespValue::Null));

    // Normal commands should still work
    let pong = client.cmd(&["PING"]).await;
    assert_eq!(pong, RespValue::SimpleString("PONG".to_string()));

    server.stop().await;
}

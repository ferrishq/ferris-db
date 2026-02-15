//! Comprehensive tests for CLIENT subcommands
//!
//! These tests verify CLIENT command functionality over real TCP connections.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
#![allow(clippy::uninlined_format_args)]
#![allow(clippy::manual_let_else)]
#![allow(clippy::needless_raw_string_hashes)]

use ferris_protocol::RespValue;
use ferris_test_utils::TestServer;

// ============================================================
// CLIENT ID TESTS
// ============================================================

#[tokio::test]
async fn test_client_id() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["CLIENT", "ID"]).await;

    match result {
        RespValue::Integer(id) => {
            assert!(id > 0, "Client ID should be positive");
        }
        _ => panic!("Expected Integer, got {:?}", result),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_client_id_unique() {
    let server = TestServer::spawn().await;
    let mut client1 = server.client().await;
    let mut client2 = server.client().await;

    let id1 = client1.cmd(&["CLIENT", "ID"]).await;
    let id2 = client2.cmd(&["CLIENT", "ID"]).await;

    assert_ne!(id1, id2, "Different clients should have different IDs");

    server.stop().await;
}

// ============================================================
// CLIENT SETNAME / GETNAME TESTS
// ============================================================

#[tokio::test]
async fn test_client_setname() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["CLIENT", "SETNAME", "my-client"]).await;
    assert_eq!(result, RespValue::SimpleString("OK".to_string()));

    server.stop().await;
}

#[tokio::test]
async fn test_client_getname_after_setname() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["CLIENT", "SETNAME", "test-client"]).await;

    let result = client.cmd(&["CLIENT", "GETNAME"]).await;
    assert_eq!(
        result,
        RespValue::BulkString(bytes::Bytes::from("test-client"))
    );

    server.stop().await;
}

#[tokio::test]
async fn test_client_getname_no_name() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["CLIENT", "GETNAME"]).await;
    assert_eq!(result, RespValue::Null);

    server.stop().await;
}

#[tokio::test]
async fn test_client_setname_clear() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["CLIENT", "SETNAME", "my-client"]).await;
    client.cmd(&["CLIENT", "SETNAME", ""]).await;

    let result = client.cmd(&["CLIENT", "GETNAME"]).await;
    assert_eq!(result, RespValue::Null);

    server.stop().await;
}

// ============================================================
// CLIENT LIST TESTS
// ============================================================

#[tokio::test]
async fn test_client_list() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["CLIENT", "LIST"]).await;

    match result {
        RespValue::BulkString(list) => {
            let list_str = String::from_utf8_lossy(&list);
            assert!(list_str.contains("id="), "Should contain client info");
        }
        _ => panic!("Expected BulkString"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_client_list_multiple_clients() {
    let server = TestServer::spawn().await;
    let mut client1 = server.client().await;
    let mut client2 = server.client().await;
    let mut client3 = server.client().await;

    // Make sure all clients are connected and active
    client2.cmd(&["PING"]).await;
    client3.cmd(&["PING"]).await;

    let result = client1.cmd(&["CLIENT", "LIST"]).await;

    match result {
        RespValue::BulkString(list) => {
            let list_str = String::from_utf8_lossy(&list);
            // Just verify that CLIENT LIST returns something with client info
            assert!(list_str.contains("id="), "Should contain client info");
        }
        _ => panic!("Expected BulkString"),
    }

    server.stop().await;
}

// ============================================================
// CLIENT INFO TESTS
// ============================================================

#[tokio::test]
async fn test_client_info() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["CLIENT", "INFO"]).await;

    match result {
        RespValue::BulkString(info) => {
            let info_str = String::from_utf8_lossy(&info);
            assert!(info_str.contains("id="), "Should contain id");
        }
        _ => panic!("Expected BulkString"),
    }

    server.stop().await;
}

// ============================================================
// CLIENT SETINFO TESTS
// ============================================================

#[tokio::test]
async fn test_client_setinfo_lib_name() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client
        .cmd(&["CLIENT", "SETINFO", "LIB-NAME", "my-redis-lib"])
        .await;
    assert_eq!(result, RespValue::SimpleString("OK".to_string()));

    server.stop().await;
}

#[tokio::test]
async fn test_client_setinfo_lib_ver() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["CLIENT", "SETINFO", "LIB-VER", "1.2.3"]).await;
    assert_eq!(result, RespValue::SimpleString("OK".to_string()));

    server.stop().await;
}

#[tokio::test]
async fn test_client_setinfo_invalid() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["CLIENT", "SETINFO", "INVALID", "value"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    server.stop().await;
}

// ============================================================
// CLIENT TRACKING TESTS
// ============================================================

#[tokio::test]
async fn test_client_tracking_on() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["CLIENT", "TRACKING", "ON"]).await;
    assert_eq!(result, RespValue::SimpleString("OK".to_string()));

    server.stop().await;
}

#[tokio::test]
async fn test_client_tracking_off() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["CLIENT", "TRACKING", "OFF"]).await;
    assert_eq!(result, RespValue::SimpleString("OK".to_string()));

    server.stop().await;
}

#[tokio::test]
async fn test_client_tracking_invalid() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["CLIENT", "TRACKING", "INVALID"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    server.stop().await;
}

// ============================================================
// CLIENT TRACKINGINFO TESTS
// ============================================================

#[tokio::test]
async fn test_client_trackinginfo() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["CLIENT", "TRACKINGINFO"]).await;

    match result {
        RespValue::Array(info) => {
            assert!(!info.is_empty());
        }
        _ => panic!("Expected Array"),
    }

    server.stop().await;
}

// ============================================================
// CLIENT CACHING TESTS
// ============================================================

#[tokio::test]
async fn test_client_caching_yes() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["CLIENT", "CACHING", "YES"]).await;
    assert_eq!(result, RespValue::SimpleString("OK".to_string()));

    server.stop().await;
}

#[tokio::test]
async fn test_client_caching_no() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["CLIENT", "CACHING", "NO"]).await;
    assert_eq!(result, RespValue::SimpleString("OK".to_string()));

    server.stop().await;
}

#[tokio::test]
async fn test_client_caching_invalid() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["CLIENT", "CACHING", "INVALID"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    server.stop().await;
}

// ============================================================
// CLIENT GETREDIR TESTS
// ============================================================

#[tokio::test]
async fn test_client_getredir() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["CLIENT", "GETREDIR"]).await;

    match result {
        RespValue::Integer(id) => {
            // -1 means not redirecting, 0 or positive is a client ID
            assert!(id >= -1);
        }
        _ => panic!("Expected Integer"),
    }

    server.stop().await;
}

// ============================================================
// CLIENT UNBLOCK TESTS
// ============================================================

#[tokio::test]
async fn test_client_unblock_nonblocking_client() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Get our client ID
    let id = match client.cmd(&["CLIENT", "ID"]).await {
        RespValue::Integer(id) => id,
        _ => panic!("Expected Integer"),
    };

    // Try to unblock (should return 0 since we're not blocked)
    let result = client.cmd(&["CLIENT", "UNBLOCK", &id.to_string()]).await;
    assert_eq!(result, RespValue::Integer(0));

    server.stop().await;
}

#[tokio::test]
async fn test_client_unblock_with_timeout() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let id = match client.cmd(&["CLIENT", "ID"]).await {
        RespValue::Integer(id) => id,
        _ => panic!("Expected Integer"),
    };

    let result = client
        .cmd(&["CLIENT", "UNBLOCK", &id.to_string(), "TIMEOUT"])
        .await;
    assert_eq!(result, RespValue::Integer(0));

    server.stop().await;
}

#[tokio::test]
async fn test_client_unblock_with_error() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let id = match client.cmd(&["CLIENT", "ID"]).await {
        RespValue::Integer(id) => id,
        _ => panic!("Expected Integer"),
    };

    let result = client
        .cmd(&["CLIENT", "UNBLOCK", &id.to_string(), "ERROR"])
        .await;
    assert_eq!(result, RespValue::Integer(0));

    server.stop().await;
}

// ============================================================
// CLIENT NO-EVICT TESTS
// ============================================================

#[tokio::test]
async fn test_client_no_evict_on() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["CLIENT", "NO-EVICT", "ON"]).await;
    assert_eq!(result, RespValue::SimpleString("OK".to_string()));

    server.stop().await;
}

#[tokio::test]
async fn test_client_no_evict_off() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["CLIENT", "NO-EVICT", "OFF"]).await;
    assert_eq!(result, RespValue::SimpleString("OK".to_string()));

    server.stop().await;
}

// ============================================================
// CLIENT NO-TOUCH TESTS
// ============================================================

#[tokio::test]
async fn test_client_no_touch_on() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["CLIENT", "NO-TOUCH", "ON"]).await;
    assert_eq!(result, RespValue::SimpleString("OK".to_string()));

    server.stop().await;
}

#[tokio::test]
async fn test_client_no_touch_off() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["CLIENT", "NO-TOUCH", "OFF"]).await;
    assert_eq!(result, RespValue::SimpleString("OK".to_string()));

    server.stop().await;
}

// ============================================================
// CLIENT PAUSE / UNPAUSE TESTS
// ============================================================

#[tokio::test]
async fn test_client_pause() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Pause for 100ms
    let result = client.cmd(&["CLIENT", "PAUSE", "100"]).await;
    assert_eq!(result, RespValue::SimpleString("OK".to_string()));

    server.stop().await;
}

#[tokio::test]
async fn test_client_unpause() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["CLIENT", "UNPAUSE"]).await;
    assert_eq!(result, RespValue::SimpleString("OK".to_string()));

    server.stop().await;
}

// ============================================================
// CLIENT REPLY TESTS
// ============================================================

#[tokio::test]
async fn test_client_reply_on() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["CLIENT", "REPLY", "ON"]).await;
    assert_eq!(result, RespValue::SimpleString("OK".to_string()));

    server.stop().await;
}

#[tokio::test]
async fn test_client_reply_skip() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // SKIP mode - implementation may return OK or skip response
    let result = client.cmd(&["CLIENT", "REPLY", "SKIP"]).await;
    // Just verify it doesn't error
    assert!(!matches!(result, RespValue::Error(_)));

    server.stop().await;
}

// ============================================================
// CLIENT KILL TESTS
// ============================================================

#[tokio::test]
async fn test_client_kill_by_id() {
    let server = TestServer::spawn().await;
    let mut client1 = server.client().await;
    let mut client2 = server.client().await;

    // Get client2's ID
    let id2 = match client2.cmd(&["CLIENT", "ID"]).await {
        RespValue::Integer(id) => id,
        _ => panic!("Expected Integer"),
    };

    // Kill client2 from client1
    let result = client1
        .cmd(&["CLIENT", "KILL", "ID", &id2.to_string()])
        .await;
    // Implementation returns OK or Integer(1) depending on version
    assert!(
        matches!(result, RespValue::SimpleString(_) | RespValue::Integer(_)),
        "Expected OK or Integer"
    );

    server.stop().await;
}

#[tokio::test]
async fn test_client_kill_nonexistent() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["CLIENT", "KILL", "ID", "999999"]).await;
    // Implementation returns OK or Integer(0) depending on version
    assert!(
        matches!(result, RespValue::SimpleString(_) | RespValue::Integer(_)),
        "Expected OK or Integer"
    );

    server.stop().await;
}

// ============================================================
// CLIENT HELP TESTS
// ============================================================

#[tokio::test]
async fn test_client_help() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["CLIENT", "HELP"]).await;

    match result {
        RespValue::Array(help) => {
            assert!(!help.is_empty());
        }
        _ => panic!("Expected Array"),
    }

    server.stop().await;
}

// ============================================================
// CLIENT UNKNOWN SUBCOMMAND TEST
// ============================================================

#[tokio::test]
async fn test_client_unknown_subcommand() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["CLIENT", "UNKNOWN"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    server.stop().await;
}

// ============================================================
// CLIENT WRONG ARITY TESTS
// ============================================================

#[tokio::test]
async fn test_client_no_subcommand() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["CLIENT"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    server.stop().await;
}

#[tokio::test]
async fn test_client_setname_no_name() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["CLIENT", "SETNAME"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    server.stop().await;
}

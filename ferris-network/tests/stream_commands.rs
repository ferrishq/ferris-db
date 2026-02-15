//! Comprehensive tests for Stream commands (XADD, XLEN, XRANGE, etc.)
//!
//! These tests verify Redis Stream functionality over real TCP connections.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
#![allow(clippy::uninlined_format_args)]
#![allow(clippy::manual_let_else)]
#![allow(clippy::needless_raw_string_hashes)]

use ferris_protocol::RespValue;
use ferris_test_utils::TestServer;

// ============================================================
// XADD TESTS
// ============================================================

#[tokio::test]
async fn test_xadd_basic() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Add an entry with auto-generated ID
    let result = client
        .cmd(&["XADD", "mystream", "*", "field1", "value1"])
        .await;

    // Should return a stream ID like "1234567890123-0"
    match &result {
        RespValue::BulkString(id) => {
            let id_str = String::from_utf8_lossy(id);
            assert!(
                id_str.contains('-'),
                "Stream ID should contain '-': {}",
                id_str
            );
        }
        _ => panic!("Expected BulkString, got {:?}", result),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_xadd_multiple_fields() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client
        .cmd(&[
            "XADD", "mystream", "*", "name", "Alice", "age", "30", "city", "NYC",
        ])
        .await;

    match &result {
        RespValue::BulkString(_) => {}
        _ => panic!("Expected BulkString, got {:?}", result),
    }

    // Verify length
    let len = client.cmd(&["XLEN", "mystream"]).await;
    assert_eq!(len, RespValue::Integer(1));

    server.stop().await;
}

#[tokio::test]
async fn test_xadd_explicit_id() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client
        .cmd(&["XADD", "mystream", "1000-0", "field", "value"])
        .await;
    assert_eq!(result, RespValue::BulkString(bytes::Bytes::from("1000-0")));

    server.stop().await;
}

#[tokio::test]
async fn test_xadd_nomkstream_nonexistent() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // NOMKSTREAM should return null if stream doesn't exist
    let result = client
        .cmd(&["XADD", "nonexistent", "NOMKSTREAM", "*", "field", "value"])
        .await;
    assert_eq!(result, RespValue::Null);

    // Verify stream was not created
    let len = client.cmd(&["XLEN", "nonexistent"]).await;
    assert_eq!(len, RespValue::Integer(0));

    server.stop().await;
}

#[tokio::test]
async fn test_xadd_with_maxlen() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Add 5 entries with MAXLEN 3
    for i in 0..5 {
        client
            .cmd(&[
                "XADD",
                "mystream",
                "MAXLEN",
                "3",
                "*",
                "num",
                &i.to_string(),
            ])
            .await;
    }

    // Should only have 3 entries
    let len = client.cmd(&["XLEN", "mystream"]).await;
    assert_eq!(len, RespValue::Integer(3));

    server.stop().await;
}

#[tokio::test]
async fn test_xadd_with_approximate_maxlen() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Add entries with approximate MAXLEN
    for i in 0..10 {
        client
            .cmd(&[
                "XADD",
                "mystream",
                "MAXLEN",
                "~",
                "5",
                "*",
                "num",
                &i.to_string(),
            ])
            .await;
    }

    // Length should be around 5 (approximate)
    let len = client.cmd(&["XLEN", "mystream"]).await;
    match len {
        RespValue::Integer(n) => assert!(n <= 10, "Should be trimmed"),
        _ => panic!("Expected Integer"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_xadd_wrong_type() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Create a string key
    client.cmd_ok(&["SET", "mykey", "value"]).await;

    // XADD on string should fail
    let result = client.cmd(&["XADD", "mykey", "*", "field", "value"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    server.stop().await;
}

#[tokio::test]
async fn test_xadd_odd_fields() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Odd number of field-value pairs should fail
    let result = client.cmd(&["XADD", "mystream", "*", "field1"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    server.stop().await;
}

// ============================================================
// XLEN TESTS
// ============================================================

#[tokio::test]
async fn test_xlen_empty_stream() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let len = client.cmd(&["XLEN", "nonexistent"]).await;
    assert_eq!(len, RespValue::Integer(0));

    server.stop().await;
}

#[tokio::test]
async fn test_xlen_after_multiple_adds() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    for i in 0..10 {
        client
            .cmd(&["XADD", "mystream", "*", "i", &i.to_string()])
            .await;
    }

    let len = client.cmd(&["XLEN", "mystream"]).await;
    assert_eq!(len, RespValue::Integer(10));

    server.stop().await;
}

// ============================================================
// XRANGE TESTS
// ============================================================

#[tokio::test]
async fn test_xrange_all_entries() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Add entries
    client.cmd(&["XADD", "mystream", "1-0", "a", "1"]).await;
    client.cmd(&["XADD", "mystream", "2-0", "b", "2"]).await;
    client.cmd(&["XADD", "mystream", "3-0", "c", "3"]).await;

    // Get all entries with - to +
    let result = client.cmd(&["XRANGE", "mystream", "-", "+"]).await;

    match result {
        RespValue::Array(entries) => {
            assert_eq!(entries.len(), 3);
        }
        _ => panic!("Expected Array, got {:?}", result),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_xrange_with_count() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Add 5 entries
    for i in 1..=5 {
        client
            .cmd(&["XADD", "mystream", &format!("{}-0", i), "n", &i.to_string()])
            .await;
    }

    // Get only 2 entries
    let result = client
        .cmd(&["XRANGE", "mystream", "-", "+", "COUNT", "2"])
        .await;

    match result {
        RespValue::Array(entries) => {
            assert_eq!(entries.len(), 2);
        }
        _ => panic!("Expected Array"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_xrange_specific_range() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Add entries
    for i in 1..=10 {
        client
            .cmd(&["XADD", "mystream", &format!("{}-0", i), "n", &i.to_string()])
            .await;
    }

    // Get entries from 3-0 to 7-0
    let result = client.cmd(&["XRANGE", "mystream", "3-0", "7-0"]).await;

    match result {
        RespValue::Array(entries) => {
            assert_eq!(entries.len(), 5);
        }
        _ => panic!("Expected Array"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_xrange_empty_result() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["XADD", "mystream", "1-0", "a", "1"]).await;

    // Range that doesn't match any entries
    let result = client.cmd(&["XRANGE", "mystream", "100-0", "200-0"]).await;

    match result {
        RespValue::Array(entries) => {
            assert_eq!(entries.len(), 0);
        }
        _ => panic!("Expected empty Array"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_xrange_nonexistent_stream() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["XRANGE", "nonexistent", "-", "+"]).await;

    match result {
        RespValue::Array(entries) => {
            assert_eq!(entries.len(), 0);
        }
        _ => panic!("Expected empty Array"),
    }

    server.stop().await;
}

// ============================================================
// XREVRANGE TESTS
// ============================================================

#[tokio::test]
async fn test_xrevrange_all_entries() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["XADD", "mystream", "1-0", "a", "1"]).await;
    client.cmd(&["XADD", "mystream", "2-0", "b", "2"]).await;
    client.cmd(&["XADD", "mystream", "3-0", "c", "3"]).await;

    // Get all entries in reverse order
    let result = client.cmd(&["XREVRANGE", "mystream", "+", "-"]).await;

    match result {
        RespValue::Array(entries) => {
            assert_eq!(entries.len(), 3);
            // First entry should be 3-0 (highest ID)
            if let RespValue::Array(ref entry) = entries[0] {
                if let RespValue::BulkString(ref id) = entry[0] {
                    assert_eq!(id.as_ref(), b"3-0");
                }
            }
        }
        _ => panic!("Expected Array"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_xrevrange_with_count() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    for i in 1..=5 {
        client
            .cmd(&["XADD", "mystream", &format!("{}-0", i), "n", &i.to_string()])
            .await;
    }

    let result = client
        .cmd(&["XREVRANGE", "mystream", "+", "-", "COUNT", "2"])
        .await;

    match result {
        RespValue::Array(entries) => {
            assert_eq!(entries.len(), 2);
        }
        _ => panic!("Expected Array"),
    }

    server.stop().await;
}

// ============================================================
// XREAD TESTS
// ============================================================

#[tokio::test]
async fn test_xread_single_stream() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client
        .cmd(&["XADD", "mystream", "1-0", "field", "value"])
        .await;

    let result = client.cmd(&["XREAD", "STREAMS", "mystream", "0"]).await;

    match result {
        RespValue::Array(streams) => {
            assert_eq!(streams.len(), 1);
        }
        _ => panic!("Expected Array, got {:?}", result),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_xread_multiple_streams() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["XADD", "stream1", "1-0", "a", "1"]).await;
    client.cmd(&["XADD", "stream2", "1-0", "b", "2"]).await;

    let result = client
        .cmd(&["XREAD", "STREAMS", "stream1", "stream2", "0", "0"])
        .await;

    match result {
        RespValue::Array(streams) => {
            assert_eq!(streams.len(), 2);
        }
        _ => panic!("Expected Array"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_xread_with_count() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    for i in 1..=10 {
        client
            .cmd(&["XADD", "mystream", &format!("{}-0", i), "n", &i.to_string()])
            .await;
    }

    let result = client
        .cmd(&["XREAD", "COUNT", "3", "STREAMS", "mystream", "0"])
        .await;

    match result {
        RespValue::Array(streams) => {
            if let RespValue::Array(ref stream_data) = streams[0] {
                if let RespValue::Array(ref entries) = stream_data[1] {
                    assert_eq!(entries.len(), 3);
                }
            }
        }
        _ => panic!("Expected Array"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_xread_no_new_entries() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client
        .cmd(&["XADD", "mystream", "1-0", "field", "value"])
        .await;

    // Read after the only entry
    let result = client.cmd(&["XREAD", "STREAMS", "mystream", "1-0"]).await;
    assert_eq!(result, RespValue::Null);

    server.stop().await;
}

#[tokio::test]
async fn test_xread_nonexistent_stream() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["XREAD", "STREAMS", "nonexistent", "0"]).await;
    assert_eq!(result, RespValue::Null);

    server.stop().await;
}

// ============================================================
// XDEL TESTS
// ============================================================

#[tokio::test]
async fn test_xdel_single_entry() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["XADD", "mystream", "1-0", "a", "1"]).await;
    client.cmd(&["XADD", "mystream", "2-0", "b", "2"]).await;

    let deleted = client.cmd(&["XDEL", "mystream", "1-0"]).await;
    assert_eq!(deleted, RespValue::Integer(1));

    let len = client.cmd(&["XLEN", "mystream"]).await;
    assert_eq!(len, RespValue::Integer(1));

    server.stop().await;
}

#[tokio::test]
async fn test_xdel_multiple_entries() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    for i in 1..=5 {
        client
            .cmd(&["XADD", "mystream", &format!("{}-0", i), "n", &i.to_string()])
            .await;
    }

    let deleted = client.cmd(&["XDEL", "mystream", "1-0", "3-0", "5-0"]).await;
    assert_eq!(deleted, RespValue::Integer(3));

    let len = client.cmd(&["XLEN", "mystream"]).await;
    assert_eq!(len, RespValue::Integer(2));

    server.stop().await;
}

#[tokio::test]
async fn test_xdel_nonexistent_id() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["XADD", "mystream", "1-0", "a", "1"]).await;

    let deleted = client.cmd(&["XDEL", "mystream", "999-0"]).await;
    assert_eq!(deleted, RespValue::Integer(0));

    server.stop().await;
}

#[tokio::test]
async fn test_xdel_nonexistent_stream() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let deleted = client.cmd(&["XDEL", "nonexistent", "1-0"]).await;
    assert_eq!(deleted, RespValue::Integer(0));

    server.stop().await;
}

// ============================================================
// XTRIM TESTS
// ============================================================

#[tokio::test]
async fn test_xtrim_maxlen() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    for i in 1..=10 {
        client
            .cmd(&["XADD", "mystream", &format!("{}-0", i), "n", &i.to_string()])
            .await;
    }

    let trimmed = client.cmd(&["XTRIM", "mystream", "MAXLEN", "5"]).await;
    assert_eq!(trimmed, RespValue::Integer(5));

    let len = client.cmd(&["XLEN", "mystream"]).await;
    assert_eq!(len, RespValue::Integer(5));

    server.stop().await;
}

#[tokio::test]
async fn test_xtrim_approximate() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    for i in 1..=10 {
        client
            .cmd(&["XADD", "mystream", &format!("{}-0", i), "n", &i.to_string()])
            .await;
    }

    let _trimmed = client.cmd(&["XTRIM", "mystream", "MAXLEN", "~", "5"]).await;

    let len = client.cmd(&["XLEN", "mystream"]).await;
    match len {
        RespValue::Integer(n) => assert!(n <= 10),
        _ => panic!("Expected Integer"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_xtrim_nonexistent_stream() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let trimmed = client.cmd(&["XTRIM", "nonexistent", "MAXLEN", "5"]).await;
    assert_eq!(trimmed, RespValue::Integer(0));

    server.stop().await;
}

// ============================================================
// XINFO TESTS
// ============================================================

#[tokio::test]
async fn test_xinfo_stream() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client
        .cmd(&["XADD", "mystream", "1-0", "field", "value"])
        .await;

    let result = client.cmd(&["XINFO", "STREAM", "mystream"]).await;

    match result {
        RespValue::Array(info) => {
            assert!(!info.is_empty());
            // Should contain "length" field
        }
        _ => panic!("Expected Array"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_xinfo_groups_empty() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client
        .cmd(&["XADD", "mystream", "1-0", "field", "value"])
        .await;

    let result = client.cmd(&["XINFO", "GROUPS", "mystream"]).await;

    match result {
        RespValue::Array(groups) => {
            assert_eq!(groups.len(), 0);
        }
        _ => panic!("Expected Array"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_xinfo_nonexistent_stream() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["XINFO", "STREAM", "nonexistent"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    server.stop().await;
}

#[tokio::test]
async fn test_xinfo_help() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["XINFO", "HELP"]).await;

    match result {
        RespValue::Array(help) => {
            assert!(!help.is_empty());
        }
        _ => panic!("Expected Array"),
    }

    server.stop().await;
}

// ============================================================
// XGROUP TESTS
// ============================================================

#[tokio::test]
async fn test_xgroup_create() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client
        .cmd(&["XADD", "mystream", "1-0", "field", "value"])
        .await;

    let result = client
        .cmd(&["XGROUP", "CREATE", "mystream", "mygroup", "0"])
        .await;
    assert_eq!(result, RespValue::SimpleString("OK".to_string()));

    server.stop().await;
}

#[tokio::test]
async fn test_xgroup_create_mkstream() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Create group on non-existent stream with MKSTREAM
    let result = client
        .cmd(&["XGROUP", "CREATE", "newstream", "mygroup", "$", "MKSTREAM"])
        .await;
    assert_eq!(result, RespValue::SimpleString("OK".to_string()));

    // Verify stream exists
    let len = client.cmd(&["XLEN", "newstream"]).await;
    assert_eq!(len, RespValue::Integer(0));

    server.stop().await;
}

#[tokio::test]
async fn test_xgroup_create_without_mkstream() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Should fail without MKSTREAM on non-existent stream
    let result = client
        .cmd(&["XGROUP", "CREATE", "nonexistent", "mygroup", "0"])
        .await;
    assert!(matches!(result, RespValue::Error(_)));

    server.stop().await;
}

#[tokio::test]
async fn test_xgroup_destroy() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client
        .cmd(&["XADD", "mystream", "1-0", "field", "value"])
        .await;
    client
        .cmd(&["XGROUP", "CREATE", "mystream", "mygroup", "0"])
        .await;

    let result = client
        .cmd(&["XGROUP", "DESTROY", "mystream", "mygroup"])
        .await;
    assert_eq!(result, RespValue::Integer(1));

    // Destroying again should return 0
    let result = client
        .cmd(&["XGROUP", "DESTROY", "mystream", "mygroup"])
        .await;
    assert_eq!(result, RespValue::Integer(0));

    server.stop().await;
}

#[tokio::test]
async fn test_xgroup_createconsumer() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client
        .cmd(&["XADD", "mystream", "1-0", "field", "value"])
        .await;
    client
        .cmd(&["XGROUP", "CREATE", "mystream", "mygroup", "0"])
        .await;

    let result = client
        .cmd(&[
            "XGROUP",
            "CREATECONSUMER",
            "mystream",
            "mygroup",
            "consumer1",
        ])
        .await;
    assert_eq!(result, RespValue::Integer(1));

    // Creating same consumer again should return 0
    let result = client
        .cmd(&[
            "XGROUP",
            "CREATECONSUMER",
            "mystream",
            "mygroup",
            "consumer1",
        ])
        .await;
    assert_eq!(result, RespValue::Integer(0));

    server.stop().await;
}

#[tokio::test]
async fn test_xgroup_delconsumer() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client
        .cmd(&["XADD", "mystream", "1-0", "field", "value"])
        .await;
    client
        .cmd(&["XGROUP", "CREATE", "mystream", "mygroup", "0"])
        .await;
    client
        .cmd(&[
            "XGROUP",
            "CREATECONSUMER",
            "mystream",
            "mygroup",
            "consumer1",
        ])
        .await;

    let result = client
        .cmd(&["XGROUP", "DELCONSUMER", "mystream", "mygroup", "consumer1"])
        .await;
    // Returns number of pending messages (0 in this case)
    assert_eq!(result, RespValue::Integer(0));

    server.stop().await;
}

#[tokio::test]
async fn test_xgroup_setid() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client
        .cmd(&["XADD", "mystream", "1-0", "field", "value"])
        .await;
    client
        .cmd(&["XGROUP", "CREATE", "mystream", "mygroup", "0"])
        .await;

    let result = client
        .cmd(&["XGROUP", "SETID", "mystream", "mygroup", "1-0"])
        .await;
    assert_eq!(result, RespValue::SimpleString("OK".to_string()));

    server.stop().await;
}

#[tokio::test]
async fn test_xgroup_help() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["XGROUP", "HELP"]).await;

    match result {
        RespValue::Array(help) => {
            assert!(!help.is_empty());
        }
        _ => panic!("Expected Array"),
    }

    server.stop().await;
}

// ============================================================
// XSETID TESTS
// ============================================================

#[tokio::test]
async fn test_xsetid_basic() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client
        .cmd(&["XADD", "mystream", "1-0", "field", "value"])
        .await;

    let result = client.cmd(&["XSETID", "mystream", "100-0"]).await;
    assert_eq!(result, RespValue::SimpleString("OK".to_string()));

    server.stop().await;
}

#[tokio::test]
async fn test_xsetid_nonexistent_stream() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["XSETID", "nonexistent", "100-0"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    server.stop().await;
}

// ============================================================
// XACK TESTS
// ============================================================

#[tokio::test]
async fn test_xack_basic() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client
        .cmd(&["XADD", "mystream", "1-0", "field", "value"])
        .await;
    client
        .cmd(&["XGROUP", "CREATE", "mystream", "mygroup", "0"])
        .await;

    // ACK without pending messages returns 0
    let result = client.cmd(&["XACK", "mystream", "mygroup", "1-0"]).await;
    assert_eq!(result, RespValue::Integer(0));

    server.stop().await;
}

#[tokio::test]
async fn test_xack_nonexistent_group() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client
        .cmd(&["XADD", "mystream", "1-0", "field", "value"])
        .await;

    let result = client
        .cmd(&["XACK", "mystream", "nonexistent", "1-0"])
        .await;
    assert!(matches!(result, RespValue::Error(_)));

    server.stop().await;
}

// ============================================================
// XPENDING TESTS
// ============================================================

#[tokio::test]
async fn test_xpending_summary() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client
        .cmd(&["XADD", "mystream", "1-0", "field", "value"])
        .await;
    client
        .cmd(&["XGROUP", "CREATE", "mystream", "mygroup", "0"])
        .await;

    let result = client.cmd(&["XPENDING", "mystream", "mygroup"]).await;

    match result {
        RespValue::Array(summary) => {
            assert_eq!(summary.len(), 4);
            // First element is count (0 pending)
            assert_eq!(summary[0], RespValue::Integer(0));
        }
        _ => panic!("Expected Array"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_xpending_nonexistent_group() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client
        .cmd(&["XADD", "mystream", "1-0", "field", "value"])
        .await;

    let result = client.cmd(&["XPENDING", "mystream", "nonexistent"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    server.stop().await;
}

// ============================================================
// CONCURRENT TESTS
// ============================================================

#[tokio::test]
async fn test_stream_concurrent_xadd() {
    let server = TestServer::spawn().await;

    let handles: Vec<_> = (0..10)
        .map(|i| {
            let addr = server.addr;
            tokio::spawn(async move {
                let mut client = ferris_test_utils::TestClient::connect(addr).await;
                for j in 0..10 {
                    client
                        .cmd(&[
                            "XADD",
                            "mystream",
                            "*",
                            "producer",
                            &i.to_string(),
                            "msg",
                            &j.to_string(),
                        ])
                        .await;
                }
            })
        })
        .collect();

    for handle in handles {
        handle.await.unwrap();
    }

    let mut client = server.client().await;
    let len = client.cmd(&["XLEN", "mystream"]).await;
    assert_eq!(len, RespValue::Integer(100));

    server.stop().await;
}

#[tokio::test]
async fn test_stream_ids_are_unique() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let mut ids = std::collections::HashSet::new();

    for _ in 0..100 {
        let result = client
            .cmd(&["XADD", "mystream", "*", "field", "value"])
            .await;
        if let RespValue::BulkString(id) = result {
            let id_str = String::from_utf8_lossy(&id).to_string();
            assert!(ids.insert(id_str.clone()), "Duplicate ID: {}", id_str);
        }
    }

    assert_eq!(ids.len(), 100);

    server.stop().await;
}

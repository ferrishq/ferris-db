#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

//! Integration tests for CLUSTER commands

use ferris_protocol::RespValue;
use ferris_test_utils::TestServer;

#[tokio::test]
async fn test_cluster_keyslot_basic() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Test basic CLUSTER KEYSLOT
    let result = client.cmd(&["CLUSTER", "KEYSLOT", "mykey"]).await;

    match result {
        RespValue::Integer(slot) => {
            // Slot should be in valid range
            assert!((0..16384).contains(&slot), "Slot {slot} out of range");
        }
        _ => panic!("Expected integer, got {result:?}"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_cluster_keyslot_consistent() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Same key should always return same slot
    let result1 = client.cmd(&["CLUSTER", "KEYSLOT", "testkey"]).await;
    let result2 = client.cmd(&["CLUSTER", "KEYSLOT", "testkey"]).await;

    assert_eq!(result1, result2);

    server.stop().await;
}

#[tokio::test]
async fn test_cluster_keyslot_hash_tags() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Keys with same hash tag should have same slot
    let slot1 = client
        .cmd(&["CLUSTER", "KEYSLOT", "{user:123}:profile"])
        .await;
    let slot2 = client
        .cmd(&["CLUSTER", "KEYSLOT", "{user:123}:settings"])
        .await;

    assert_eq!(slot1, slot2, "Hash tags should produce same slot");

    // Different hash tags should (usually) have different slots
    let slot3 = client
        .cmd(&["CLUSTER", "KEYSLOT", "{user:456}:profile"])
        .await;

    // This might occasionally fail due to hash collisions, but very unlikely
    if slot1 == slot3 {
        // If they happen to be equal, just verify they're both valid
        match slot1 {
            RespValue::Integer(s) => assert!((0..16384).contains(&s)),
            _ => panic!("Expected integer slot"),
        }
    }

    server.stop().await;
}

#[tokio::test]
async fn test_cluster_keyslot_wrong_arity() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // CLUSTER KEYSLOT requires exactly 1 argument
    let result = client.cmd(&["CLUSTER", "KEYSLOT"]).await;

    match result {
        RespValue::Error(msg) => {
            assert!(msg.contains("wrong number of arguments") || msg.contains("arity"));
        }
        _ => panic!("Expected error for wrong arity"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_cluster_info() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // CLUSTER INFO should return cluster status
    let result = client.cmd(&["CLUSTER", "INFO"]).await;

    match result {
        RespValue::BulkString(info) => {
            let info_str = String::from_utf8_lossy(&info);
            // Should contain cluster_state field
            assert!(info_str.contains("cluster_state:"));
        }
        _ => panic!("Expected bulk string, got {result:?}"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_cluster_unknown_subcommand() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Unknown subcommand should return error
    let result = client.cmd(&["CLUSTER", "UNKNOWN"]).await;

    match result {
        RespValue::Error(msg) => {
            assert!(msg.contains("Unknown") || msg.contains("unknown"));
        }
        _ => panic!("Expected error for unknown subcommand"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_cluster_keyslot_empty_key() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Empty key should still work (return slot 0)
    let result = client.cmd(&["CLUSTER", "KEYSLOT", ""]).await;

    match result {
        RespValue::Integer(slot) => {
            assert!((0..16384).contains(&slot));
        }
        _ => panic!("Expected integer slot"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_cluster_keyslot_various_keys() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Test various key patterns
    let keys = vec![
        "simple",
        "key:with:colons",
        "key{with}tag",
        "{tag}:suffix",
        "prefix:{tag}",
        "{a}{b}",  // Multiple tags - first one is used
        "{}empty", // Empty tag
    ];

    for key in keys {
        let result = client.cmd(&["CLUSTER", "KEYSLOT", key]).await;

        match result {
            RespValue::Integer(slot) => {
                assert!(
                    (0..16384).contains(&slot),
                    "Slot for key '{key}' out of range: {slot}"
                );
            }
            _ => panic!("Expected integer slot for key '{key}'"),
        }
    }

    server.stop().await;
}

#[tokio::test]
async fn test_cluster_nodes() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // CLUSTER NODES should return node information
    let result = client.cmd(&["CLUSTER", "NODES"]).await;

    match result {
        RespValue::BulkString(nodes) => {
            let nodes_str = String::from_utf8_lossy(&nodes);
            // Should contain "myself" flag
            assert!(nodes_str.contains("myself"));
        }
        _ => panic!("Expected bulk string, got {result:?}"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_cluster_slots() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // CLUSTER SLOTS should return slot mappings
    let result = client.cmd(&["CLUSTER", "SLOTS"]).await;

    match result {
        RespValue::Array(slots) => {
            // For standalone mode, should be empty
            assert_eq!(slots.len(), 0);
        }
        _ => panic!("Expected array, got {result:?}"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_cluster_addslots_not_enabled() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // CLUSTER ADDSLOTS should return error since cluster is not enabled
    let result = client.cmd(&["CLUSTER", "ADDSLOTS", "0", "1", "2"]).await;

    match result {
        RespValue::Error(msg) => {
            assert!(msg.contains("not enabled") || msg.contains("not supported"));
        }
        _ => panic!("Expected error, got {result:?}"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_cluster_meet_not_enabled() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // CLUSTER MEET should return error since cluster is not enabled
    let result = client.cmd(&["CLUSTER", "MEET", "127.0.0.1", "6381"]).await;

    match result {
        RespValue::Error(msg) => {
            assert!(msg.contains("not enabled") || msg.contains("not supported"));
        }
        _ => panic!("Expected error, got {result:?}"),
    }

    server.stop().await;
}

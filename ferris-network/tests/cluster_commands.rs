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

// ============================================================================
// CLUSTER SETSLOT Command Tests
// ============================================================================

#[tokio::test]
async fn test_cluster_setslot_not_enabled() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // CLUSTER SETSLOT should return error since cluster is not enabled
    let result = client
        .cmd(&["CLUSTER", "SETSLOT", "100", "MIGRATING", "node2"])
        .await;

    match result {
        RespValue::Error(msg) => {
            assert!(
                msg.contains("not enabled") || msg.contains("Cluster"),
                "Expected cluster not enabled error, got: {msg}"
            );
        }
        _ => panic!("Expected error, got {result:?}"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_cluster_setslot_migrating_wrong_arity() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Too few arguments
    let result = client.cmd(&["CLUSTER", "SETSLOT", "100"]).await;

    match result {
        RespValue::Error(msg) => {
            assert!(
                msg.contains("wrong number of arguments") || msg.contains("arity"),
                "Expected arity error, got: {msg}"
            );
        }
        _ => panic!("Expected error for wrong arity, got {result:?}"),
    }

    // Missing node-id for MIGRATING
    let result = client
        .cmd(&["CLUSTER", "SETSLOT", "100", "MIGRATING"])
        .await;

    match result {
        RespValue::Error(msg) => {
            assert!(
                msg.contains("wrong number of arguments") || msg.contains("arity"),
                "Expected arity error, got: {msg}"
            );
        }
        _ => panic!("Expected error for missing node-id, got {result:?}"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_cluster_setslot_importing_wrong_arity() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Missing node-id for IMPORTING
    let result = client
        .cmd(&["CLUSTER", "SETSLOT", "100", "IMPORTING"])
        .await;

    match result {
        RespValue::Error(msg) => {
            assert!(
                msg.contains("wrong number of arguments") || msg.contains("arity"),
                "Expected arity error, got: {msg}"
            );
        }
        _ => panic!("Expected error for missing node-id, got {result:?}"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_cluster_setslot_stable_wrong_arity() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // STABLE doesn't take node-id, extra arguments should error
    let result = client
        .cmd(&["CLUSTER", "SETSLOT", "100", "STABLE", "extra"])
        .await;

    match result {
        RespValue::Error(msg) => {
            assert!(
                msg.contains("wrong number of arguments") || msg.contains("arity"),
                "Expected arity error, got: {msg}"
            );
        }
        _ => panic!("Expected error for extra argument, got {result:?}"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_cluster_setslot_node_wrong_arity() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Missing node-id for NODE
    let result = client.cmd(&["CLUSTER", "SETSLOT", "100", "NODE"]).await;

    match result {
        RespValue::Error(msg) => {
            assert!(
                msg.contains("wrong number of arguments") || msg.contains("arity"),
                "Expected arity error, got: {msg}"
            );
        }
        _ => panic!("Expected error for missing node-id, got {result:?}"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_cluster_setslot_invalid_slot_number() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Slot number too large (>= 16384)
    let result = client
        .cmd(&["CLUSTER", "SETSLOT", "99999", "MIGRATING", "node2"])
        .await;

    match result {
        RespValue::Error(msg) => {
            assert!(
                msg.contains("invalid") || msg.contains("slot") || msg.contains("range"),
                "Expected invalid slot error, got: {msg}"
            );
        }
        _ => panic!("Expected error for invalid slot, got {result:?}"),
    }

    // Non-numeric slot
    let result = client
        .cmd(&["CLUSTER", "SETSLOT", "invalid", "MIGRATING", "node2"])
        .await;

    match result {
        RespValue::Error(msg) => {
            assert!(
                msg.contains("invalid") || msg.contains("integer") || msg.contains("slot"),
                "Expected invalid slot error, got: {msg}"
            );
        }
        _ => panic!("Expected error for non-numeric slot, got {result:?}"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_cluster_setslot_invalid_subcommand() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Invalid subcommand
    let result = client.cmd(&["CLUSTER", "SETSLOT", "100", "INVALID"]).await;

    match result {
        RespValue::Error(msg) => {
            assert!(
                msg.contains("Unknown") || msg.contains("invalid") || msg.contains("subcommand"),
                "Expected invalid subcommand error, got: {msg}"
            );
        }
        _ => panic!("Expected error for invalid subcommand, got {result:?}"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_cluster_setslot_negative_slot() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Negative slot number
    let result = client
        .cmd(&["CLUSTER", "SETSLOT", "-1", "MIGRATING", "node2"])
        .await;

    match result {
        RespValue::Error(msg) => {
            assert!(
                msg.contains("invalid") || msg.contains("slot") || msg.contains("range"),
                "Expected invalid slot error, got: {msg}"
            );
        }
        _ => panic!("Expected error for negative slot, got {result:?}"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_cluster_setslot_zero_slot() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Slot 0 is valid (should error because cluster not enabled, not because of slot)
    let result = client
        .cmd(&["CLUSTER", "SETSLOT", "0", "MIGRATING", "node2"])
        .await;

    match result {
        RespValue::Error(msg) => {
            assert!(
                msg.contains("not enabled") || msg.contains("Cluster"),
                "Expected cluster not enabled error, got: {msg}"
            );
        }
        _ => panic!("Expected error, got {result:?}"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_cluster_setslot_max_slot() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Slot 16383 is the maximum valid slot (should error because cluster not enabled)
    let result = client
        .cmd(&["CLUSTER", "SETSLOT", "16383", "MIGRATING", "node2"])
        .await;

    match result {
        RespValue::Error(msg) => {
            assert!(
                msg.contains("not enabled") || msg.contains("Cluster"),
                "Expected cluster not enabled error, got: {msg}"
            );
        }
        _ => panic!("Expected error, got {result:?}"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_cluster_setslot_at_boundary() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Slot 16384 is just beyond the maximum (should be rejected)
    let result = client
        .cmd(&["CLUSTER", "SETSLOT", "16384", "MIGRATING", "node2"])
        .await;

    match result {
        RespValue::Error(msg) => {
            assert!(
                msg.contains("invalid") || msg.contains("slot") || msg.contains("range"),
                "Expected invalid slot error, got: {msg}"
            );
        }
        _ => panic!("Expected error for out-of-range slot, got {result:?}"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_cluster_setslot_migrating_subcommand() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Test MIGRATING subcommand (will fail because cluster not enabled, but should parse correctly)
    let result = client
        .cmd(&["CLUSTER", "SETSLOT", "100", "MIGRATING", "abc123"])
        .await;

    match result {
        RespValue::Error(msg) => {
            // Should fail because cluster is not enabled, not because of syntax
            assert!(
                msg.contains("not enabled") || msg.contains("Cluster"),
                "Expected cluster not enabled error, got: {msg}"
            );
        }
        _ => panic!("Expected error, got {result:?}"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_cluster_setslot_importing_subcommand() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Test IMPORTING subcommand
    let result = client
        .cmd(&["CLUSTER", "SETSLOT", "100", "IMPORTING", "abc123"])
        .await;

    match result {
        RespValue::Error(msg) => {
            assert!(
                msg.contains("not enabled") || msg.contains("Cluster"),
                "Expected cluster not enabled error, got: {msg}"
            );
        }
        _ => panic!("Expected error, got {result:?}"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_cluster_setslot_stable_subcommand() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Test STABLE subcommand
    let result = client.cmd(&["CLUSTER", "SETSLOT", "100", "STABLE"]).await;

    match result {
        RespValue::Error(msg) => {
            assert!(
                msg.contains("not enabled") || msg.contains("Cluster"),
                "Expected cluster not enabled error, got: {msg}"
            );
        }
        _ => panic!("Expected error, got {result:?}"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_cluster_setslot_node_subcommand() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Test NODE subcommand
    let result = client
        .cmd(&["CLUSTER", "SETSLOT", "100", "NODE", "abc123"])
        .await;

    match result {
        RespValue::Error(msg) => {
            assert!(
                msg.contains("not enabled") || msg.contains("Cluster"),
                "Expected cluster not enabled error, got: {msg}"
            );
        }
        _ => panic!("Expected error, got {result:?}"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_cluster_setslot_case_sensitivity() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Test that subcommands are case-insensitive (or case-sensitive as designed)
    let result = client
        .cmd(&["CLUSTER", "SETSLOT", "100", "migrating", "node2"])
        .await;

    if let RespValue::Error(msg) = result {
        // Could fail due to case sensitivity or cluster not enabled
        // We're just verifying it doesn't crash
        assert!(
            msg.contains("not enabled")
                || msg.contains("Cluster")
                || msg.contains("Unknown")
                || msg.contains("invalid"),
            "Expected reasonable error message, got: {msg}"
        );
    }
    // If lowercase is accepted, that's also OK (depends on implementation)

    server.stop().await;
}

#[tokio::test]
async fn test_cluster_setslot_empty_node_id() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Empty node ID should be rejected
    let result = client
        .cmd(&["CLUSTER", "SETSLOT", "100", "MIGRATING", ""])
        .await;

    match result {
        RespValue::Error(msg) => {
            // Should fail (either cluster not enabled or invalid node id)
            assert!(!msg.is_empty(), "Error message should not be empty");
        }
        _ => panic!("Expected error for empty node ID, got {result:?}"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_cluster_setslot_whitespace_slot() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Whitespace in slot number should be rejected
    let result = client
        .cmd(&["CLUSTER", "SETSLOT", " 100 ", "MIGRATING", "node2"])
        .await;

    match result {
        RespValue::Error(msg) => {
            assert!(
                msg.contains("invalid") || msg.contains("integer") || msg.contains("not enabled"),
                "Expected error, got: {msg}"
            );
        }
        _ => panic!("Expected error for whitespace in slot, got {result:?}"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_cluster_setslot_multiple_subcommands() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Multiple subcommands should be rejected (only one allowed)
    let result = client
        .cmd(&["CLUSTER", "SETSLOT", "100", "MIGRATING", "node2", "STABLE"])
        .await;

    match result {
        RespValue::Error(msg) => {
            assert!(
                msg.contains("wrong number of arguments") || msg.contains("arity"),
                "Expected arity error, got: {msg}"
            );
        }
        _ => panic!("Expected error for multiple subcommands, got {result:?}"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_cluster_setslot_long_node_id() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Very long node ID (Redis uses 40-character hex strings)
    let long_node_id = "a".repeat(100);
    let result = client
        .cmd(&["CLUSTER", "SETSLOT", "100", "MIGRATING", &long_node_id])
        .await;

    match result {
        RespValue::Error(msg) => {
            // Should fail (cluster not enabled or node validation)
            assert!(!msg.is_empty());
        }
        _ => panic!("Expected error, got {result:?}"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_cluster_setslot_special_chars_in_node_id() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Node ID with special characters
    let result = client
        .cmd(&["CLUSTER", "SETSLOT", "100", "MIGRATING", "node@#$%"])
        .await;

    match result {
        RespValue::Error(msg) => {
            assert!(!msg.is_empty());
        }
        _ => panic!("Expected error, got {result:?}"),
    }

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

//! Cluster commands for Redis Cluster protocol compatibility
//!
//! Redis Cluster provides a way to run a Redis installation where data is
//! automatically sharded across multiple Redis nodes.

use crate::{CommandContext, CommandError, CommandResult};
use ferris_protocol::RespValue;

/// Number of hash slots in Redis Cluster
pub const CLUSTER_SLOTS: u16 = 16384;

/// Calculate the hash slot for a given key using CRC16
///
/// Redis uses CRC16 and then takes modulo 16384.
/// Hash tags are supported: {user:123}:profile uses "user:123" for hashing.
///
/// # Arguments
///
/// * `key` - The key to hash
///
/// # Returns
///
/// The slot number (0-16383)
pub fn key_hash_slot(key: &[u8]) -> u16 {
    // Extract hash tag if present: {...}
    let hash_key = extract_hash_tag(key).unwrap_or(key);

    // Calculate CRC16 and take modulo
    let crc = crc16(hash_key);
    crc % CLUSTER_SLOTS
}

/// Extract hash tag from key if present
///
/// Hash tags are denoted by curly braces: {tag}
/// The part between the first { and first } after it is used for hashing.
/// If no valid tag is found, returns None.
fn extract_hash_tag(key: &[u8]) -> Option<&[u8]> {
    // Find first '{'
    let start = key.iter().position(|&b| b == b'{')?;

    // Find first '}' after '{'
    let end = key[start + 1..].iter().position(|&b| b == b'}')?;

    // Must have at least one character between braces
    if end == 0 {
        return None;
    }

    Some(&key[start + 1..start + 1 + end])
}

/// CRC16 implementation (CCITT polynomial)
///
/// This is the same CRC16 algorithm used by Redis for cluster slot calculation.
fn crc16(data: &[u8]) -> u16 {
    const CRC16_TAB: [u16; 256] = [
        0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7, 0x8108, 0x9129, 0xa14a,
        0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef, 0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294,
        0x72f7, 0x62d6, 0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de, 0x2462,
        0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485, 0xa56a, 0xb54b, 0x8528, 0x9509,
        0xe5ee, 0xf5cf, 0xc5ac, 0xd58d, 0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695,
        0x46b4, 0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc, 0x48c4, 0x58e5,
        0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823, 0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948,
        0x9969, 0xa90a, 0xb92b, 0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12,
        0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a, 0x6ca6, 0x7c87, 0x4ce4,
        0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41, 0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b,
        0x8d68, 0x9d49, 0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70, 0xff9f,
        0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78, 0x9188, 0x81a9, 0xb1ca, 0xa1eb,
        0xd10c, 0xc12d, 0xf14e, 0xe16f, 0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046,
        0x6067, 0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e, 0x02b1, 0x1290,
        0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256, 0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e,
        0xe54f, 0xd52c, 0xc50d, 0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
        0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c, 0x26d3, 0x36f2, 0x0691,
        0x16b0, 0x6657, 0x7676, 0x4615, 0x5634, 0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9,
        0xb98a, 0xa9ab, 0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3, 0xcb7d,
        0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a, 0x4a75, 0x5a54, 0x6a37, 0x7a16,
        0x0af1, 0x1ad0, 0x2ab3, 0x3a92, 0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8,
        0x8dc9, 0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1, 0xef1f, 0xff3e,
        0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8, 0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93,
        0x3eb2, 0x0ed1, 0x1ef0,
    ];

    let mut crc: u16 = 0;
    for &byte in data {
        let idx = ((crc >> 8) as u8 ^ byte) as usize;
        crc = (crc << 8) ^ CRC16_TAB[idx];
    }
    crc
}

/// CLUSTER KEYSLOT key
///
/// Returns the hash slot for the given key.
///
/// Time complexity: O(N) where N is the length of the key
pub fn cluster_keyslot(_ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() != 1 {
        return Err(CommandError::WrongArity("CLUSTER KEYSLOT".to_string()));
    }

    let key = args[0]
        .as_bytes()
        .ok_or_else(|| CommandError::InvalidArgument("invalid key".to_string()))?;

    let slot = key_hash_slot(key);

    Ok(RespValue::Integer(i64::from(slot)))
}

/// CLUSTER INFO
///
/// Provides info about cluster state.
/// For now, we return a basic response indicating cluster is disabled.
pub fn cluster_info(_ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if !args.is_empty() {
        return Err(CommandError::WrongArity("CLUSTER INFO".to_string()));
    }

    // For now, cluster mode is not enabled
    let info = "cluster_state:fail\r\n\
                cluster_slots_assigned:0\r\n\
                cluster_slots_ok:0\r\n\
                cluster_slots_pfail:0\r\n\
                cluster_slots_fail:0\r\n\
                cluster_known_nodes:1\r\n\
                cluster_size:0\r\n\
                cluster_current_epoch:0\r\n\
                cluster_my_epoch:0\r\n\
                cluster_stats_messages_sent:0\r\n\
                cluster_stats_messages_received:0\r\n";

    Ok(RespValue::BulkString(info.into()))
}

/// CLUSTER dispatch command
///
/// Routes to appropriate subcommand.
pub fn cluster(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("CLUSTER".to_string()));
    }

    let subcommand = args[0]
        .as_str()
        .ok_or_else(|| CommandError::InvalidArgument("invalid subcommand".to_string()))?
        .to_uppercase();

    cluster_dispatch(ctx, &subcommand, &args[1..])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc16() {
        // Test vector from Redis
        assert_eq!(crc16(b"123456789"), 0x31C3);
    }

    #[test]
    fn test_key_hash_slot_basic() {
        // Test basic key hashing
        let slot = key_hash_slot(b"mykey");
        assert!(slot < CLUSTER_SLOTS);
    }

    #[test]
    fn test_key_hash_slot_same_key() {
        // Same key should always hash to same slot
        let slot1 = key_hash_slot(b"mykey");
        let slot2 = key_hash_slot(b"mykey");
        assert_eq!(slot1, slot2);
    }

    #[test]
    fn test_hash_tag_extraction() {
        // Test hash tag extraction
        assert_eq!(extract_hash_tag(b"user:123"), None);
        assert_eq!(extract_hash_tag(b"{user:123}"), Some(b"user:123" as &[u8]));
        assert_eq!(
            extract_hash_tag(b"{user:123}:profile"),
            Some(b"user:123" as &[u8])
        );
        assert_eq!(
            extract_hash_tag(b"prefix:{user:123}:suffix"),
            Some(b"user:123" as &[u8])
        );
        assert_eq!(extract_hash_tag(b"{}"), None); // Empty tag
        assert_eq!(extract_hash_tag(b"{"), None); // No closing brace
    }

    #[test]
    fn test_hash_tag_same_slot() {
        // Keys with same hash tag should go to same slot
        let slot1 = key_hash_slot(b"{user:123}:profile");
        let slot2 = key_hash_slot(b"{user:123}:settings");
        assert_eq!(slot1, slot2);

        let slot3 = key_hash_slot(b"{order:456}:items");
        let slot4 = key_hash_slot(b"{order:456}:total");
        assert_eq!(slot3, slot4);

        // Different tags should (usually) have different slots
        assert_ne!(slot1, slot3);
    }

    #[test]
    fn test_hash_tag_vs_no_tag() {
        // Key with tag should hash based on tag content
        let slot_with_tag = key_hash_slot(b"{abc}def");
        let slot_tag_content = key_hash_slot(b"abc");
        assert_eq!(slot_with_tag, slot_tag_content);
    }

    #[test]
    fn test_cluster_slots_constant() {
        assert_eq!(CLUSTER_SLOTS, 16384);
    }

    #[test]
    fn test_slot_distribution() {
        // Ensure slots are distributed across the range
        let mut slots = std::collections::HashSet::new();
        for i in 0..1000 {
            let key = format!("key{i}");
            let slot = key_hash_slot(key.as_bytes());
            assert!(slot < CLUSTER_SLOTS);
            slots.insert(slot);
        }
        // Should have good distribution (at least 500 unique slots for 1000 keys)
        assert!(slots.len() > 500);
    }
}

/// CLUSTER NODES
///
/// Returns information about all nodes in the cluster.
/// For now, returns just this node since cluster is not enabled.
pub fn cluster_nodes(_ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if !args.is_empty() {
        return Err(CommandError::WrongArity("CLUSTER NODES".to_string()));
    }

    // Format: <id> <ip:port@cport> <flags> <master> <ping-sent> <pong-recv> <config-epoch> <link-state> <slot> <slot> ... <slot>
    // For single-node mode, return just this node
    let node_id = "0000000000000000000000000000000000000000"; // Placeholder ID
    let info = format!(
        "{} 127.0.0.1:6380@16380 myself,master - 0 0 0 connected\n",
        node_id
    );

    Ok(RespValue::BulkString(info.into()))
}

/// CLUSTER SLOTS
///
/// Returns array of slot ranges and their assigned nodes.
/// For now, returns empty since cluster is not enabled.
pub fn cluster_slots(_ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if !args.is_empty() {
        return Err(CommandError::WrongArity("CLUSTER SLOTS".to_string()));
    }

    // Return empty array since no slots are assigned
    Ok(RespValue::Array(vec![]))
}

/// CLUSTER ADDSLOTS slot [slot ...]
///
/// Assign hash slots to this node.
/// Stub implementation - returns error since cluster mode is not enabled.
pub fn cluster_addslots(_ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("CLUSTER ADDSLOTS".to_string()));
    }

    // For now, cluster mode is not fully implemented
    Err(CommandError::InvalidArgument(
        "Cluster mode is not enabled. Use standalone or replication mode.".to_string(),
    ))
}

/// CLUSTER MEET ip port
///
/// Connect a node to the cluster.
/// Stub implementation - returns error since cluster mode is not enabled.
pub fn cluster_meet(_ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongArity("CLUSTER MEET".to_string()));
    }

    // For now, cluster mode is not fully implemented
    Err(CommandError::InvalidArgument(
        "Cluster mode is not enabled. Use standalone or replication mode.".to_string(),
    ))
}

/// Update cluster dispatch to handle new subcommands
pub fn cluster_dispatch(
    ctx: &mut CommandContext,
    subcommand: &str,
    args: &[RespValue],
) -> CommandResult {
    match subcommand {
        "KEYSLOT" => cluster_keyslot(ctx, args),
        "INFO" => cluster_info(ctx, args),
        "NODES" => cluster_nodes(ctx, args),
        "SLOTS" => cluster_slots(ctx, args),
        "ADDSLOTS" => cluster_addslots(ctx, args),
        "MEET" => cluster_meet(ctx, args),
        _ => Err(CommandError::InvalidArgument(format!(
            "Unknown CLUSTER subcommand '{subcommand}'"
        ))),
    }
}

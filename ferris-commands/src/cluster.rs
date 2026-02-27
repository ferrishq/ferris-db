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
pub fn cluster_info(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if !args.is_empty() {
        return Err(CommandError::WrongArity("CLUSTER INFO".to_string()));
    }

    if let Some(cluster_mgr) = ctx.cluster_manager() {
        // Use async block to get cluster state
        let info = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let state = cluster_mgr.state().await;
                state.info_string()
            })
        });
        Ok(RespValue::BulkString(info.into()))
    } else {
        // Cluster mode not enabled
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
    use crate::CommandContext;
    use ferris_core::KeyStore;
    use std::sync::Arc;

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

    #[test]
    fn test_asking_command() {
        let store = Arc::new(KeyStore::new(16));
        let mut ctx = CommandContext::new(store);

        // Initially, asking flag should be false
        assert!(!ctx.is_asking());

        // Execute ASKING command
        let result = asking(&mut ctx, &[]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), RespValue::ok());

        // After ASKING, flag should be set
        assert!(ctx.is_asking());
    }

    #[test]
    fn test_asking_wrong_arity() {
        let store = Arc::new(KeyStore::new(16));
        let mut ctx = CommandContext::new(store);

        // ASKING should not accept arguments
        let result = asking(&mut ctx, &[RespValue::BulkString("extra".into())]);
        assert!(result.is_err());
    }

    #[test]
    fn test_asking_flag_cleared() {
        let store = Arc::new(KeyStore::new(16));
        let mut ctx = CommandContext::new(store);

        // Set asking flag
        ctx.set_asking(true);
        assert!(ctx.is_asking());

        // Clear asking flag
        ctx.clear_asking();
        assert!(!ctx.is_asking());
    }

    #[test]
    fn test_migrate_basic_parsing() {
        let store = Arc::new(KeyStore::new(16));
        let mut ctx = CommandContext::new(store);

        // Basic MIGRATE command: key does not exist → returns NOKEY
        let result = migrate(
            &mut ctx,
            &[
                RespValue::BulkString("127.0.0.1".into()),
                RespValue::BulkString("6380".into()),
                RespValue::BulkString("mykey".into()),
                RespValue::BulkString("0".into()),
                RespValue::BulkString("5000".into()),
            ],
        );

        // Key doesn't exist, so MIGRATE returns +NOKEY
        assert_eq!(
            result.unwrap(),
            RespValue::SimpleString("NOKEY".to_string())
        );
    }

    #[test]
    fn test_migrate_wrong_arity() {
        let store = Arc::new(KeyStore::new(16));
        let mut ctx = CommandContext::new(store);

        // Too few arguments
        let result = migrate(&mut ctx, &[RespValue::BulkString("host".into())]);
        assert!(result.is_err());
        match result {
            Err(CommandError::WrongArity(_)) => (),
            _ => panic!("Expected WrongArity error"),
        }
    }

    #[test]
    fn test_migrate_with_copy_flag() {
        let store = Arc::new(KeyStore::new(16));
        let mut ctx = CommandContext::new(store);

        let result = migrate(
            &mut ctx,
            &[
                RespValue::BulkString("127.0.0.1".into()),
                RespValue::BulkString("6380".into()),
                RespValue::BulkString("mykey".into()),
                RespValue::BulkString("0".into()),
                RespValue::BulkString("5000".into()),
                RespValue::BulkString("COPY".into()),
            ],
        );

        // Key doesn't exist → NOKEY (COPY flag accepted, parsing correct)
        assert_eq!(
            result.unwrap(),
            RespValue::SimpleString("NOKEY".to_string())
        );
    }

    #[test]
    fn test_migrate_with_replace_flag() {
        let store = Arc::new(KeyStore::new(16));
        let mut ctx = CommandContext::new(store);

        let result = migrate(
            &mut ctx,
            &[
                RespValue::BulkString("127.0.0.1".into()),
                RespValue::BulkString("6380".into()),
                RespValue::BulkString("mykey".into()),
                RespValue::BulkString("0".into()),
                RespValue::BulkString("5000".into()),
                RespValue::BulkString("REPLACE".into()),
            ],
        );

        // Key doesn't exist → NOKEY (REPLACE flag accepted, parsing correct)
        assert_eq!(
            result.unwrap(),
            RespValue::SimpleString("NOKEY".to_string())
        );
    }

    #[test]
    fn test_migrate_invalid_port() {
        let store = Arc::new(KeyStore::new(16));
        let mut ctx = CommandContext::new(store);

        let result = migrate(
            &mut ctx,
            &[
                RespValue::BulkString("127.0.0.1".into()),
                RespValue::BulkString("invalid".into()), // Invalid port
                RespValue::BulkString("mykey".into()),
                RespValue::BulkString("0".into()),
                RespValue::BulkString("5000".into()),
            ],
        );

        assert!(matches!(result, Err(CommandError::InvalidArgument(_))));
    }

    #[test]
    fn test_migrate_invalid_db() {
        let store = Arc::new(KeyStore::new(16));
        let mut ctx = CommandContext::new(store);

        let result = migrate(
            &mut ctx,
            &[
                RespValue::BulkString("127.0.0.1".into()),
                RespValue::BulkString("6380".into()),
                RespValue::BulkString("mykey".into()),
                RespValue::BulkString("invalid".into()), // Invalid DB
                RespValue::BulkString("5000".into()),
            ],
        );

        assert!(matches!(result, Err(CommandError::InvalidArgument(_))));
    }

    #[test]
    fn test_migrate_with_keys_option() {
        let store = Arc::new(KeyStore::new(16));
        let mut ctx = CommandContext::new(store);

        let result = migrate(
            &mut ctx,
            &[
                RespValue::BulkString("127.0.0.1".into()),
                RespValue::BulkString("6380".into()),
                RespValue::BulkString("".into()), // Empty key when using KEYS option
                RespValue::BulkString("0".into()),
                RespValue::BulkString("5000".into()),
                RespValue::BulkString("KEYS".into()),
                RespValue::BulkString("key1".into()),
                RespValue::BulkString("key2".into()),
                RespValue::BulkString("key3".into()),
            ],
        );

        // Keys don't exist → NOKEY (KEYS option accepted, parsing correct)
        assert_eq!(
            result.unwrap(),
            RespValue::SimpleString("NOKEY".to_string())
        );
    }

    // Tests for redirect logic
    #[test]
    fn test_should_check_redirect_cluster_commands() {
        // Cluster management commands should not be checked
        assert!(!should_check_redirect("CLUSTER"));
        assert!(!should_check_redirect("ASKING"));
        assert!(!should_check_redirect("MIGRATE"));
        assert!(!should_check_redirect("READONLY"));
        assert!(!should_check_redirect("READWRITE"));
    }

    #[test]
    fn test_should_check_redirect_connection_commands() {
        // Connection commands without keys should not be checked
        assert!(!should_check_redirect("AUTH"));
        assert!(!should_check_redirect("ECHO"));
        assert!(!should_check_redirect("PING"));
        assert!(!should_check_redirect("QUIT"));
        assert!(!should_check_redirect("SELECT"));
        assert!(!should_check_redirect("INFO"));
    }

    #[test]
    fn test_should_check_redirect_key_commands() {
        // Commands that access keys should be checked
        assert!(should_check_redirect("GET"));
        assert!(should_check_redirect("SET"));
        assert!(should_check_redirect("DEL"));
        assert!(should_check_redirect("INCR"));
        assert!(should_check_redirect("LPUSH"));
        assert!(should_check_redirect("HGET"));
        assert!(should_check_redirect("SADD"));
        assert!(should_check_redirect("ZADD"));
    }

    #[test]
    fn test_extract_first_key_simple_commands() {
        // Simple key commands
        let args = [bytes::Bytes::from("mykey")];
        let key = extract_first_key("GET", &args);
        assert_eq!(key, Some(b"mykey" as &[u8]));

        let args = [bytes::Bytes::from("mykey"), bytes::Bytes::from("value")];
        let key = extract_first_key("SET", &args);
        assert_eq!(key, Some(b"mykey" as &[u8]));
    }

    #[test]
    fn test_extract_first_key_no_key_commands() {
        // Commands with no keys
        let key = extract_first_key("PING", &[]);
        assert_eq!(key, None);

        let args = [bytes::Bytes::from("server")];
        let key = extract_first_key("INFO", &args);
        assert_eq!(key, None);
    }

    #[test]
    fn test_extract_first_key_eval() {
        // EVAL script numkeys key [key ...] arg [arg ...]
        let args = [
            bytes::Bytes::from("return redis.call('GET', KEYS[1])"),
            bytes::Bytes::from("1"), // numkeys
            bytes::Bytes::from("mykey"),
        ];
        let key = extract_first_key("EVAL", &args);
        assert_eq!(key, Some(b"mykey" as &[u8]));

        // EVAL with numkeys=0
        let args = [bytes::Bytes::from("return 42"), bytes::Bytes::from("0")];
        let key = extract_first_key("EVAL", &args);
        assert_eq!(key, None);
    }

    #[test]
    fn test_check_cluster_redirect_no_cluster() {
        // Without cluster mode, should always return Ok(())
        let store = Arc::new(KeyStore::new(16));
        let ctx = CommandContext::new(store);

        let args = [bytes::Bytes::from("mykey")];
        let result = check_cluster_redirect(&ctx, "GET", &args);
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_cluster_redirect_asking_flag() {
        // With ASKING flag set, should skip redirect check
        let store = Arc::new(KeyStore::new(16));
        let mut ctx = CommandContext::new(store);
        ctx.set_asking(true);

        let args = [bytes::Bytes::from("mykey")];
        let result = check_cluster_redirect(&ctx, "GET", &args);
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_cluster_redirect_no_key() {
        // Commands with no keys should not redirect
        let store = Arc::new(KeyStore::new(16));
        let ctx = CommandContext::new(store);

        let result = check_cluster_redirect(&ctx, "PING", &[]);
        assert!(result.is_ok());

        let args = [bytes::Bytes::from("server")];
        let result = check_cluster_redirect(&ctx, "INFO", &args);
        assert!(result.is_ok());
    }

    #[test]
    fn test_key_extraction_empty_args() {
        // Empty args should return None
        let key = extract_first_key("GET", &[]);
        assert_eq!(key, None);
    }

    #[test]
    fn test_key_extraction_hash_commands() {
        // Hash commands: key is first arg
        let args = [bytes::Bytes::from("myhash"), bytes::Bytes::from("field")];
        let key = extract_first_key("HGET", &args);
        assert_eq!(key, Some(b"myhash" as &[u8]));

        let args = [
            bytes::Bytes::from("myhash"),
            bytes::Bytes::from("field"),
            bytes::Bytes::from("value"),
        ];
        let key = extract_first_key("HSET", &args);
        assert_eq!(key, Some(b"myhash" as &[u8]));
    }

    #[test]
    fn test_key_extraction_list_commands() {
        // List commands: key is first arg
        let args = [bytes::Bytes::from("mylist"), bytes::Bytes::from("element")];
        let key = extract_first_key("LPUSH", &args);
        assert_eq!(key, Some(b"mylist" as &[u8]));

        let args = [bytes::Bytes::from("mylist")];
        let key = extract_first_key("RPOP", &args);
        assert_eq!(key, Some(b"mylist" as &[u8]));
    }

    #[test]
    fn test_key_extraction_set_commands() {
        // Set commands: key is first arg
        let args = [bytes::Bytes::from("myset"), bytes::Bytes::from("member")];
        let key = extract_first_key("SADD", &args);
        assert_eq!(key, Some(b"myset" as &[u8]));

        let args = [bytes::Bytes::from("myset")];
        let key = extract_first_key("SMEMBERS", &args);
        assert_eq!(key, Some(b"myset" as &[u8]));
    }

    #[test]
    fn test_key_extraction_sorted_set_commands() {
        // Sorted set commands: key is first arg
        let args = [
            bytes::Bytes::from("myzset"),
            bytes::Bytes::from("1"),
            bytes::Bytes::from("member"),
        ];
        let key = extract_first_key("ZADD", &args);
        assert_eq!(key, Some(b"myzset" as &[u8]));

        let args = [bytes::Bytes::from("myzset")];
        let key = extract_first_key("ZCARD", &args);
        assert_eq!(key, Some(b"myzset" as &[u8]));
    }

    // Tests for cross-slot validation
    #[test]
    fn test_validate_same_slot_no_cluster() {
        // Without cluster mode, should always succeed
        let store = Arc::new(KeyStore::new(16));
        let ctx = CommandContext::new(store);

        let keys = vec![b"key1" as &[u8], b"key2", b"key3"];
        let result = validate_same_slot(&ctx, &keys);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_same_slot_empty() {
        // Empty key list should always succeed
        let store = Arc::new(KeyStore::new(16));
        let ctx = CommandContext::new(store);

        let keys: Vec<&[u8]> = vec![];
        let result = validate_same_slot(&ctx, &keys);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_same_slot_single_key() {
        // Single key should always succeed
        let store = Arc::new(KeyStore::new(16));
        let ctx = CommandContext::new(store);

        let keys = vec![b"key1" as &[u8]];
        let result = validate_same_slot(&ctx, &keys);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_same_slot_same_hash_tag() {
        // Keys with same hash tag should be in same slot
        let store = Arc::new(KeyStore::new(16));
        let ctx = CommandContext::new(store);

        let keys = vec![
            b"{user123}:profile" as &[u8],
            b"{user123}:settings",
            b"{user123}:preferences",
        ];
        let result = validate_same_slot(&ctx, &keys);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_same_slot_different_keys_same_slot() {
        // These keys happen to hash to the same slot
        // We need to find keys that hash to the same slot
        let store = Arc::new(KeyStore::new(16));
        let ctx = CommandContext::new(store);

        // Use hash tags to ensure same slot
        let keys = vec![b"{a}key1" as &[u8], b"{a}key2", b"{a}key3"];
        let result = validate_same_slot(&ctx, &keys);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_same_slot_two_keys_same() {
        // Two keys with same hash tag
        let store = Arc::new(KeyStore::new(16));
        let ctx = CommandContext::new(store);

        let keys = vec![b"{foo}bar" as &[u8], b"{foo}baz"];
        let result = validate_same_slot(&ctx, &keys);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_same_slot_many_keys() {
        // Many keys with same hash tag
        let store = Arc::new(KeyStore::new(16));
        let ctx = CommandContext::new(store);

        let keys = vec![
            b"{test}1" as &[u8],
            b"{test}2",
            b"{test}3",
            b"{test}4",
            b"{test}5",
        ];
        let result = validate_same_slot(&ctx, &keys);
        assert!(result.is_ok());
    }

    // Tests for cross-slot errors in commands
    #[test]
    fn test_del_crossslot_without_cluster() {
        // DEL with different keys should work without cluster mode
        use crate::key::del;
        let store = Arc::new(KeyStore::new(16));
        let mut ctx = CommandContext::new(store.clone());

        // Set some keys
        let db = store.database(0);
        db.set(
            bytes::Bytes::from("key1"),
            ferris_core::Entry::new(ferris_core::RedisValue::String(bytes::Bytes::from(
                "value1",
            ))),
        );
        db.set(
            bytes::Bytes::from("key2"),
            ferris_core::Entry::new(ferris_core::RedisValue::String(bytes::Bytes::from(
                "value2",
            ))),
        );

        // DEL should work even with keys in different slots
        let args = vec![
            RespValue::BulkString(bytes::Bytes::from("key1")),
            RespValue::BulkString(bytes::Bytes::from("key2")),
        ];
        let result = del(&mut ctx, &args);
        assert!(result.is_ok());
    }

    #[test]
    fn test_mget_same_hash_tag() {
        // MGET with same hash tag should work
        use crate::string::mget;
        let store = Arc::new(KeyStore::new(16));
        let mut ctx = CommandContext::new(store);

        let args = vec![
            RespValue::BulkString(bytes::Bytes::from("{user}:name")),
            RespValue::BulkString(bytes::Bytes::from("{user}:email")),
            RespValue::BulkString(bytes::Bytes::from("{user}:age")),
        ];
        let result = mget(&mut ctx, &args);
        assert!(result.is_ok());
    }

    #[test]
    fn test_mset_same_hash_tag() {
        // MSET with same hash tag should work
        use crate::string::mset;
        let store = Arc::new(KeyStore::new(16));
        let mut ctx = CommandContext::new(store);

        let args = vec![
            RespValue::BulkString(bytes::Bytes::from("{user}:name")),
            RespValue::BulkString(bytes::Bytes::from("Alice")),
            RespValue::BulkString(bytes::Bytes::from("{user}:email")),
            RespValue::BulkString(bytes::Bytes::from("alice@example.com")),
        ];
        let result = mset(&mut ctx, &args);
        assert!(result.is_ok());
    }

    #[test]
    fn test_exists_same_hash_tag() {
        // EXISTS with same hash tag should work
        use crate::key::exists;
        let store = Arc::new(KeyStore::new(16));
        let mut ctx = CommandContext::new(store);

        let args = vec![
            RespValue::BulkString(bytes::Bytes::from("{order}:123")),
            RespValue::BulkString(bytes::Bytes::from("{order}:124")),
            RespValue::BulkString(bytes::Bytes::from("{order}:125")),
        ];
        let result = exists(&mut ctx, &args);
        assert!(result.is_ok());
    }

    #[test]
    fn test_touch_same_hash_tag() {
        // TOUCH with same hash tag should work
        use crate::key::touch;
        let store = Arc::new(KeyStore::new(16));
        let mut ctx = CommandContext::new(store);

        let args = vec![
            RespValue::BulkString(bytes::Bytes::from("{session}:1")),
            RespValue::BulkString(bytes::Bytes::from("{session}:2")),
        ];
        let result = touch(&mut ctx, &args);
        assert!(result.is_ok());
    }
}

/// CLUSTER NODES
///
/// Returns information about all nodes in the cluster.
pub fn cluster_nodes(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if !args.is_empty() {
        return Err(CommandError::WrongArity("CLUSTER NODES".to_string()));
    }

    if let Some(cluster_mgr) = ctx.cluster_manager() {
        let nodes_str = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let state = cluster_mgr.state().await;
                state.nodes_string()
            })
        });
        Ok(RespValue::BulkString(nodes_str.into()))
    } else {
        // Cluster mode not enabled, return single node
        let node_id = "0000000000000000000000000000000000000000";
        let info = format!(
            "{} 127.0.0.1:6380@16380 myself,master - 0 0 0 connected\n",
            node_id
        );
        Ok(RespValue::BulkString(info.into()))
    }
}

/// CLUSTER SLOTS
///
/// Returns array of slot ranges and their assigned nodes.
pub fn cluster_slots(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    use bytes::Bytes;

    if !args.is_empty() {
        return Err(CommandError::WrongArity("CLUSTER SLOTS".to_string()));
    }

    if let Some(cluster_mgr) = ctx.cluster_manager() {
        let ranges = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let state = cluster_mgr.state().await;
                state.slots_array()
            })
        });

        // Convert to RESP format
        let mut result = Vec::new();
        for (start, end, nodes) in ranges {
            let mut range_info = vec![
                RespValue::Integer(i64::from(start)),
                RespValue::Integer(i64::from(end)),
            ];

            // Add node info (for now just placeholder)
            for _node_id in nodes {
                range_info.push(RespValue::Array(vec![
                    RespValue::BulkString(Bytes::from("127.0.0.1")),
                    RespValue::Integer(6380),
                ]));
            }

            result.push(RespValue::Array(range_info));
        }

        Ok(RespValue::Array(result))
    } else {
        Ok(RespValue::Array(vec![]))
    }
}

/// CLUSTER ADDSLOTS slot [slot ...]
///
/// Assign hash slots to this node.
pub fn cluster_addslots(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("CLUSTER ADDSLOTS".to_string()));
    }

    let Some(cluster_mgr) = ctx.cluster_manager() else {
        return Err(CommandError::InvalidArgument(
            "Cluster mode is not enabled. Use standalone or replication mode.".to_string(),
        ));
    };

    // Parse slot numbers
    let mut slots = Vec::new();
    for arg in args {
        let slot_str = arg
            .as_str()
            .ok_or_else(|| CommandError::InvalidArgument("invalid slot number".to_string()))?;
        let slot: u16 = slot_str
            .parse()
            .map_err(|_| CommandError::InvalidArgument("invalid slot number".to_string()))?;

        if slot >= ferris_replication::CLUSTER_SLOTS {
            return Err(CommandError::InvalidArgument(format!(
                "Invalid slot {slot} (must be 0-16383)"
            )));
        }

        slots.push(slot);
    }

    // Assign slots to this node
    tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(async {
            let mut state = cluster_mgr.state_mut().await;
            let my_id = state.my_id.clone();
            state.assign_slots(&my_id, &slots)
        })
    })
    .map_err(|e| CommandError::InvalidArgument(e))?;

    Ok(RespValue::ok())
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

/// ASKING
///
/// Tells the cluster node that the client is about to query a slot that is being migrated.
/// This is used during slot migration to allow temporary access to a slot that is being moved.
///
/// The ASKING flag is cleared automatically after the next command.
///
/// Time complexity: O(1)
pub fn asking(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if !args.is_empty() {
        return Err(CommandError::WrongArity("ASKING".to_string()));
    }

    // Set the ASKING flag on this connection
    ctx.set_asking(true);

    Ok(RespValue::ok())
}

/// MIGRATE host port key|"" destination-db timeout [COPY] [REPLACE] [AUTH password] [AUTH2 username password] [KEYS key [key ...]]
///
/// Atomically transfer one or more keys from this instance to a destination
/// instance using DUMP + RESTORE over a raw TCP connection.
///
/// Flow:
/// 1. Serialize each key with `serialize()`.
/// 2. Open a TCP connection to `host:port`.
/// 3. Optionally authenticate with AUTH / AUTH2.
/// 4. Send a pipelined RESTORE command for each key.
/// 5. Unless COPY is set, delete the keys from this node.
///
/// Returns `+OK` on success, or an error on any failure.
///
/// Time complexity: O(N·M) where N is the number of keys and M is value size.
pub fn migrate(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    // Minimum: MIGRATE host port key destination-db timeout
    if args.len() < 5 {
        return Err(CommandError::WrongArity("MIGRATE".to_string()));
    }

    let host = args[0]
        .as_str()
        .ok_or_else(|| CommandError::InvalidArgument("invalid host".to_string()))?
        .to_string();

    let port_str = args[1]
        .as_str()
        .ok_or_else(|| CommandError::InvalidArgument("invalid port".to_string()))?;
    let port: u16 = port_str
        .parse()
        .map_err(|_| CommandError::InvalidArgument("invalid port number".to_string()))?;

    let key_arg = args[2]
        .as_str()
        .ok_or_else(|| CommandError::InvalidArgument("invalid key".to_string()))?
        .to_string();

    let db_str = args[3]
        .as_str()
        .ok_or_else(|| CommandError::InvalidArgument("invalid destination-db".to_string()))?;
    let destination_db: usize = db_str
        .parse()
        .map_err(|_| CommandError::InvalidArgument("invalid destination-db number".to_string()))?;

    let timeout_str = args[4]
        .as_str()
        .ok_or_else(|| CommandError::InvalidArgument("invalid timeout".to_string()))?;
    let timeout_ms: u64 = timeout_str
        .parse()
        .map_err(|_| CommandError::InvalidArgument("invalid timeout number".to_string()))?;

    // Parse optional flags
    let mut copy = false;
    let mut replace = false;
    let mut auth_password: Option<String> = None;
    let mut auth2_user: Option<String> = None;
    let mut auth2_pass: Option<String> = None;
    let mut keys_to_migrate: Vec<bytes::Bytes> = Vec::new();

    let mut i = 5;
    while i < args.len() {
        let flag = args[i]
            .as_str()
            .ok_or_else(|| CommandError::InvalidArgument("invalid argument".to_string()))?
            .to_uppercase();

        match flag.as_str() {
            "COPY" => {
                copy = true;
                i += 1;
            }
            "REPLACE" => {
                replace = true;
                i += 1;
            }
            "AUTH" => {
                if i + 1 >= args.len() {
                    return Err(CommandError::Syntax);
                }
                auth_password = Some(
                    args[i + 1]
                        .as_str()
                        .ok_or_else(|| {
                            CommandError::InvalidArgument("invalid password".to_string())
                        })?
                        .to_string(),
                );
                i += 2;
            }
            "AUTH2" => {
                if i + 2 >= args.len() {
                    return Err(CommandError::Syntax);
                }
                auth2_user = Some(
                    args[i + 1]
                        .as_str()
                        .ok_or_else(|| {
                            CommandError::InvalidArgument("invalid username".to_string())
                        })?
                        .to_string(),
                );
                auth2_pass = Some(
                    args[i + 2]
                        .as_str()
                        .ok_or_else(|| {
                            CommandError::InvalidArgument("invalid password".to_string())
                        })?
                        .to_string(),
                );
                i += 3;
            }
            "KEYS" => {
                i += 1;
                while i < args.len() {
                    let key = args[i]
                        .as_bytes()
                        .ok_or_else(|| CommandError::InvalidArgument("invalid key".to_string()))?
                        .clone();
                    keys_to_migrate.push(key);
                    i += 1;
                }
            }
            _ => {
                return Err(CommandError::InvalidArgument(format!(
                    "unknown option '{flag}'"
                )));
            }
        }
    }

    // If no KEYS option, use the single key argument
    if keys_to_migrate.is_empty() {
        if key_arg.is_empty() {
            return Err(CommandError::InvalidArgument(
                "empty key with no KEYS option".to_string(),
            ));
        }
        keys_to_migrate.push(bytes::Bytes::from(key_arg));
    }

    // ── Serialize all keys from the local store ────────────────────────────
    let src_db = ctx.store().database(ctx.selected_db());
    let timeout = std::time::Duration::from_millis(timeout_ms.max(1));

    // Collect (key, serialized_payload, ttl_ms) for each existing key
    let mut payloads: Vec<(bytes::Bytes, bytes::Bytes, u64)> = Vec::new();
    for key in &keys_to_migrate {
        if let Some(entry) = src_db.get(key) {
            if entry.is_expired() {
                src_db.delete(key);
                continue;
            }
            let ttl_ms = entry.ttl_millis().unwrap_or(0);
            let payload = ferris_core::serialize(&entry.value);
            payloads.push((key.clone(), payload, ttl_ms));
        }
        // If key doesn't exist, we silently skip it (NOKEY is not an error for MIGRATE)
    }

    if payloads.is_empty() {
        return Ok(RespValue::SimpleString("NOKEY".to_string()));
    }

    // ── Connect to destination and send RESTORE commands ──────────────────
    // MIGRATE needs async TCP I/O from a synchronous command handler.
    //
    // `block_in_place` releases the current Tokio worker thread to the
    // scheduler while we create a new single-thread runtime for the
    // migration TCP session. This requires a multi-thread Tokio runtime
    // (the production server and TestServer both use one).
    let addr = format!("{host}:{port}");
    let result = tokio::task::block_in_place(|| {
        let addr2 = addr.clone();
        let payloads2 = payloads.clone();
        let auth_password2 = auth_password.clone();
        let auth2_user2 = auth2_user.clone();
        let auth2_pass2 = auth2_pass.clone();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build();
        match rt {
            Ok(rt) => rt.block_on(async move {
                tokio::time::timeout(
                    timeout,
                    do_migrate(
                        &addr2,
                        destination_db,
                        &payloads2,
                        replace,
                        auth_password2.as_deref(),
                        auth2_user2.as_deref(),
                        auth2_pass2.as_deref(),
                        timeout,
                    ),
                )
                .await
                .unwrap_or_else(|_| Err(format!("operation timed out after {timeout:?}")))
            }),
            Err(e) => Err(format!("runtime error: {e}")),
        }
    });

    match result {
        Ok(()) => {
            // ── Delete source keys unless COPY ─────────────────────────────
            if !copy {
                for (key, _, _) in &payloads {
                    src_db.delete(key);
                }
            }
            Ok(RespValue::ok())
        }
        Err(e) => Err(CommandError::InvalidArgument(format!(
            "MIGRATE failed: {e}"
        ))),
    }
}

/// Inner async helper that performs the actual TCP migration.
///
/// Uses the RESP2 wire protocol — all commands are sent as RESP arrays and
/// responses are parsed with the ferris-protocol codec so we handle both
/// simple-string (`+OK`) and error (`-ERR …`) replies correctly.
#[allow(clippy::too_many_arguments)]
async fn do_migrate(
    addr: &str,
    destination_db: usize,
    payloads: &[(bytes::Bytes, bytes::Bytes, u64)],
    replace: bool,
    auth_password: Option<&str>,
    auth2_user: Option<&str>,
    auth2_pass: Option<&str>,
    timeout: std::time::Duration,
) -> Result<(), String> {
    use bytes::BytesMut;
    use ferris_protocol::{resp2, RespValue};
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;

    let stream = tokio::time::timeout(timeout, TcpStream::connect(addr))
        .await
        .map_err(|_| format!("connection to {addr} timed out"))?
        .map_err(|e| format!("cannot connect to {addr}: {e}"))?;

    stream
        .set_nodelay(true)
        .map_err(|e| format!("setsockopt: {e}"))?;

    let (mut reader, mut writer) = tokio::io::split(stream);
    let mut read_buf = BytesMut::with_capacity(4096);

    // ── Optional AUTH ──────────────────────────────────────────────────────
    if let Some(pass) = auth_password {
        let cmd = RespValue::Array(vec![
            RespValue::BulkString(bytes::Bytes::from_static(b"AUTH")),
            RespValue::BulkString(bytes::Bytes::copy_from_slice(pass.as_bytes())),
        ]);
        let mut out = BytesMut::new();
        resp2::serialize(&cmd, &mut out);
        writer
            .write_all(&out)
            .await
            .map_err(|e| format!("write error: {e}"))?;
        let reply = recv_resp(&mut reader, &mut read_buf).await?;
        if matches!(reply, RespValue::Error(_)) {
            return Err(format!("AUTH failed: {reply:?}"));
        }
    } else if let (Some(user), Some(pass)) = (auth2_user, auth2_pass) {
        let cmd = RespValue::Array(vec![
            RespValue::BulkString(bytes::Bytes::from_static(b"AUTH")),
            RespValue::BulkString(bytes::Bytes::copy_from_slice(user.as_bytes())),
            RespValue::BulkString(bytes::Bytes::copy_from_slice(pass.as_bytes())),
        ]);
        let mut out = BytesMut::new();
        resp2::serialize(&cmd, &mut out);
        writer
            .write_all(&out)
            .await
            .map_err(|e| format!("write error: {e}"))?;
        let reply = recv_resp(&mut reader, &mut read_buf).await?;
        if matches!(reply, RespValue::Error(_)) {
            return Err(format!("AUTH2 failed: {reply:?}"));
        }
    }

    // ── SELECT destination database ───────────────────────────────────────
    if destination_db != 0 {
        let cmd = RespValue::Array(vec![
            RespValue::BulkString(bytes::Bytes::from_static(b"SELECT")),
            RespValue::BulkString(bytes::Bytes::copy_from_slice(
                destination_db.to_string().as_bytes(),
            )),
        ]);
        let mut out = BytesMut::new();
        resp2::serialize(&cmd, &mut out);
        writer
            .write_all(&out)
            .await
            .map_err(|e| format!("write error: {e}"))?;
        let reply = recv_resp(&mut reader, &mut read_buf).await?;
        if matches!(reply, RespValue::Error(_)) {
            return Err(format!("SELECT {destination_db} failed: {reply:?}"));
        }
    }

    // ── Pipeline RESTORE commands ─────────────────────────────────────────
    // Build and flush the entire pipeline in one write, then read N replies.
    let mut pipeline_buf = BytesMut::new();
    for (key, payload, ttl_ms) in payloads {
        let mut restore_args = vec![
            RespValue::BulkString(bytes::Bytes::from_static(b"RESTORE")),
            RespValue::BulkString(key.clone()),
            RespValue::BulkString(bytes::Bytes::copy_from_slice(ttl_ms.to_string().as_bytes())),
            RespValue::BulkString(payload.clone()),
        ];
        if replace {
            restore_args.push(RespValue::BulkString(bytes::Bytes::from_static(b"REPLACE")));
        }
        resp2::serialize(&RespValue::Array(restore_args), &mut pipeline_buf);
    }

    writer
        .write_all(&pipeline_buf)
        .await
        .map_err(|e| format!("pipeline write error: {e}"))?;
    writer
        .flush()
        .await
        .map_err(|e| format!("flush error: {e}"))?;

    // Collect one reply per key
    for (key, _, _) in payloads {
        let reply = recv_resp(&mut reader, &mut read_buf).await?;
        if let RespValue::Error(msg) = reply {
            let key_str = String::from_utf8_lossy(key);
            return Err(format!("RESTORE for '{key_str}' failed: {msg}"));
        }
    }

    Ok(())
}

/// Read exactly one RESP2 value from `reader`, buffering into `buf`.
async fn recv_resp<R>(
    reader: &mut R,
    buf: &mut bytes::BytesMut,
) -> Result<ferris_protocol::RespValue, String>
where
    R: tokio::io::AsyncReadExt + Unpin,
{
    use ferris_protocol::resp2;
    loop {
        if let Some(val) = resp2::parse(buf).map_err(|e| format!("RESP parse error: {e}"))? {
            return Ok(val);
        }
        let n = reader
            .read_buf(buf)
            .await
            .map_err(|e| format!("read error: {e}"))?;
        if n == 0 {
            return Err("connection closed unexpectedly".to_string());
        }
    }
}

/// CLUSTER SETSLOT slot MIGRATING|IMPORTING|STABLE|NODE node-id
///
/// Manages slot migration states in cluster mode.
///
/// Subcommands:
/// - MIGRATING node-id: Mark slot as migrating to target node
/// - IMPORTING node-id: Mark slot as importing from source node
/// - STABLE: Clear any migration state for the slot
/// - NODE node-id: Assign slot to node and clear migration state
///
/// Time complexity: O(1)
pub fn cluster_setslot(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    // Minimum: CLUSTER SETSLOT slot subcommand
    if args.len() < 2 {
        return Err(CommandError::WrongArity("CLUSTER SETSLOT".to_string()));
    }

    // Parse slot number
    let slot_str = args[0]
        .as_str()
        .ok_or_else(|| CommandError::InvalidArgument("invalid slot number".to_string()))?;
    let slot: u16 = slot_str
        .parse()
        .map_err(|_| CommandError::InvalidArgument(format!("invalid slot number: {slot_str}")))?;

    // Validate slot range (0-16383)
    if slot >= ferris_replication::cluster::CLUSTER_SLOTS {
        return Err(CommandError::InvalidArgument(format!(
            "invalid slot number: {slot} (must be 0-16383)"
        )));
    }

    // Parse subcommand
    let subcommand = args[1]
        .as_str()
        .ok_or_else(|| CommandError::InvalidArgument("invalid subcommand".to_string()))?
        .to_uppercase();

    // Validate arity based on subcommand (before checking cluster mode)
    match subcommand.as_str() {
        "MIGRATING" | "IMPORTING" | "NODE" => {
            if args.len() != 3 {
                return Err(CommandError::WrongArity(format!(
                    "CLUSTER SETSLOT {subcommand}"
                )));
            }
        }
        "STABLE" => {
            if args.len() != 2 {
                return Err(CommandError::WrongArity(
                    "CLUSTER SETSLOT STABLE".to_string(),
                ));
            }
        }
        _ => {
            return Err(CommandError::InvalidArgument(format!(
                "Unknown CLUSTER SETSLOT subcommand '{subcommand}'. Expected MIGRATING, IMPORTING, STABLE, or NODE"
            )));
        }
    }

    // Get cluster manager (after arity validation)
    let Some(cluster_mgr) = ctx.cluster_manager() else {
        return Err(CommandError::InvalidArgument(
            "Cluster mode is not enabled".to_string(),
        ));
    };

    match subcommand.as_str() {
        "MIGRATING" => {
            let target_node = args[2]
                .as_str()
                .ok_or_else(|| CommandError::InvalidArgument("invalid node id".to_string()))?
                .to_string();

            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    let mut state = cluster_mgr.state_mut().await;
                    state.set_slot_migrating(slot, target_node)
                })
            })
            .map_err(CommandError::InvalidArgument)?;

            Ok(RespValue::ok())
        }
        "IMPORTING" => {
            let source_node = args[2]
                .as_str()
                .ok_or_else(|| CommandError::InvalidArgument("invalid node id".to_string()))?
                .to_string();

            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    let mut state = cluster_mgr.state_mut().await;
                    state.set_slot_importing(slot, source_node)
                })
            })
            .map_err(CommandError::InvalidArgument)?;

            Ok(RespValue::ok())
        }
        "STABLE" => {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    let mut state = cluster_mgr.state_mut().await;
                    state.set_slot_stable(slot);
                });
            });

            Ok(RespValue::ok())
        }
        "NODE" => {
            let node_id = args[2]
                .as_str()
                .ok_or_else(|| CommandError::InvalidArgument("invalid node id".to_string()))?
                .to_string();

            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    let mut state = cluster_mgr.state_mut().await;
                    state.set_slot_node(slot, &node_id)
                })
            })
            .map_err(CommandError::InvalidArgument)?;

            Ok(RespValue::ok())
        }
        _ => unreachable!("Subcommand validation should have caught invalid subcommands"),
    }
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
        "SETSLOT" => cluster_setslot(ctx, args),
        "MEET" => cluster_meet(ctx, args),
        _ => Err(CommandError::InvalidArgument(format!(
            "Unknown CLUSTER subcommand '{subcommand}'"
        ))),
    }
}

/// Check if this node owns a given slot
///
/// Returns Ok(true) if this node owns the slot
/// Returns Ok(false) if slot is not assigned or assigned to another node
/// Returns Err if cluster manager is not available
pub async fn check_slot_ownership(ctx: &CommandContext, slot: u16) -> Result<bool, CommandError> {
    let Some(cluster_mgr) = ctx.cluster_manager() else {
        // Cluster mode not enabled - treat as owning all slots
        return Ok(true);
    };

    let state = cluster_mgr.state().await;
    if let Some(owner_id) = state.get_slot_node(slot) {
        Ok(owner_id == &state.my_id)
    } else {
        // Slot not assigned to anyone
        Ok(false)
    }
}

/// Check if a slot is currently being migrated
///
/// TODO: Implement slot migration tracking
/// For now, always returns false (no migration in progress)
#[allow(clippy::unused_async)]
pub fn is_slot_migrating(_ctx: &CommandContext, _slot: u16) -> bool {
    // TODO: Implement migration state tracking
    // This will require adding migration state to ClusterState:
    // - migrating_slots: HashMap<u16, NodeId> (slot -> target node)
    // - importing_slots: HashMap<u16, NodeId> (slot -> source node)
    false
}

/// Get the redirect information for a slot
///
/// Returns None if this node owns the slot or cluster mode is disabled
/// Returns Some((host, port, is_migration)) where is_migration indicates
/// whether this is a temporary ASK redirect (true) or permanent MOVED (false)
pub async fn get_slot_redirect(
    ctx: &CommandContext,
    slot: u16,
) -> Result<Option<(String, u16, bool)>, CommandError> {
    let Some(cluster_mgr) = ctx.cluster_manager() else {
        // Cluster mode not enabled - no redirects
        return Ok(None);
    };

    let state = cluster_mgr.state().await;

    // Check if slot is assigned
    let Some(owner_id) = state.get_slot_node(slot) else {
        // Slot not assigned to anyone - this is an error in production
        // but for now we'll accept it as belonging to this node
        return Ok(None);
    };

    // Check if we own this slot
    if owner_id == &state.my_id {
        // Check if slot is being migrated away
        if is_slot_migrating(ctx, slot) {
            // TODO: Get target node info and return ASK redirect
            // For now, we own the slot
            return Ok(None);
        }
        return Ok(None);
    }

    // Slot belongs to another node - get node info for redirect
    let Some(node) = state.nodes.get(owner_id) else {
        // Node not found - this shouldn't happen
        return Err(CommandError::Internal(format!(
            "Slot {slot} assigned to unknown node {owner_id}"
        )));
    };

    // Return MOVED redirect (permanent)
    let host = node.addr.ip().to_string();
    let port = node.addr.port();
    Ok(Some((host, port, false)))
}

/// Check if a command needs cluster redirect checking
///
/// Returns true for commands that access keys and should be checked
/// Returns false for commands that don't access keys or are cluster-related
pub fn should_check_redirect(command_name: &str) -> bool {
    // Don't check cluster management commands
    if matches!(
        command_name,
        "CLUSTER" | "ASKING" | "MIGRATE" | "READONLY" | "READWRITE"
    ) {
        return false;
    }

    // Don't check connection/server commands that don't access keys
    if matches!(
        command_name,
        "AUTH"
            | "ECHO"
            | "PING"
            | "QUIT"
            | "SELECT"
            | "INFO"
            | "CONFIG"
            | "CLIENT"
            | "COMMAND"
            | "HELLO"
            | "RESET"
            | "SHUTDOWN"
            | "TIME"
            | "DBSIZE"
            | "FLUSHDB"
            | "FLUSHALL"
            | "SAVE"
            | "BGSAVE"
            | "LASTSAVE"
            | "ROLE"
            | "REPLICAOF"
            | "SLAVEOF"
            | "PSYNC"
            | "REPLCONF"
            | "WAIT"
            | "MULTI"
            | "EXEC"
            | "DISCARD"
            | "WATCH"
            | "UNWATCH"
            | "SUBSCRIBE"
            | "UNSUBSCRIBE"
            | "PSUBSCRIBE"
            | "PUNSUBSCRIBE"
            | "PUBLISH"
            | "PUBSUB"
    ) {
        return false;
    }

    // All other commands access keys and should be checked
    true
}

/// Extract the first key from command arguments
///
/// This is a simplified implementation that works for most single-key commands.
/// Multi-key commands may need special handling.
///
/// Returns None if the command has no keys or the key cannot be extracted
pub fn extract_first_key<'a>(command_name: &str, args: &'a [bytes::Bytes]) -> Option<&'a [u8]> {
    // Most commands have the key as the first argument
    // Special cases:
    match command_name {
        // Commands with no keys
        "PING" | "ECHO" | "QUIT" | "SELECT" | "AUTH" | "INFO" => None,

        // EVAL and EVALSHA: EVAL script numkeys key [key ...] arg [arg ...]
        // Key is at position 2 if numkeys > 0
        "EVAL" | "EVALSHA" => {
            if args.len() >= 3 {
                // Try to parse numkeys
                if let Ok(numkeys_str) = std::str::from_utf8(&args[1]) {
                    if let Ok(numkeys) = numkeys_str.parse::<usize>() {
                        if numkeys > 0 && args.len() > 2 {
                            return Some(&args[2]);
                        }
                    }
                }
            }
            None
        }

        // Default: first argument is the key
        _ => args.first().map(|b| b.as_ref()),
    }
}

/// Validate that all keys in a multi-key command hash to the same slot
///
/// In cluster mode, multi-key commands must operate on keys that hash to the
/// same slot. This function checks that all provided keys hash to the same slot.
///
/// Returns:
/// - Ok(()) if all keys are in the same slot or cluster mode is disabled
/// - Err(CommandError::CrossSlot) if keys are in different slots
pub fn validate_same_slot(ctx: &CommandContext, keys: &[&[u8]]) -> Result<(), CommandError> {
    // Skip validation if cluster mode is not enabled
    if ctx.cluster_manager().is_none() {
        return Ok(());
    }

    // No keys or single key - always valid
    if keys.len() <= 1 {
        return Ok(());
    }

    // Calculate slot for first key
    let first_slot = key_hash_slot(keys[0]);

    // Check all other keys hash to the same slot
    for key in keys.iter().skip(1) {
        let slot = key_hash_slot(key);
        if slot != first_slot {
            return Err(CommandError::CrossSlot);
        }
    }

    Ok(())
}

/// Check for cluster redirect before executing a command (synchronous wrapper)
///
/// This function is called before command execution to check if the command
/// should be redirected to another node. It uses `block_in_place` to make
/// async cluster lookups work in a sync context.
///
/// Returns:
/// - Ok(None) if no redirect needed (this node owns the slot or cluster mode disabled)
/// - Err(CommandError::Moved/Ask) if redirect needed
pub fn check_cluster_redirect(
    ctx: &CommandContext,
    command_name: &str,
    args: &[bytes::Bytes],
) -> Result<(), CommandError> {
    // Skip redirect check if:
    // 1. Cluster mode not enabled
    // 2. Command doesn't access keys
    // 3. ASKING flag is set (allows access to migrating slots)

    if ctx.cluster_manager().is_none() {
        return Ok(());
    }

    if !should_check_redirect(command_name) {
        return Ok(());
    }

    if ctx.is_asking() {
        // ASKING flag allows access to slots being migrated
        // The flag will be cleared after this command
        return Ok(());
    }

    // Extract the key from the command
    let Some(key) = extract_first_key(command_name, args) else {
        // No key to check
        return Ok(());
    };

    // Calculate the slot for this key
    let slot = key_hash_slot(key);

    // Check if we need to redirect using block_in_place for async cluster lookup
    let redirect_result = tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(get_slot_redirect(ctx, slot))
    })?;

    if let Some((host, port, is_migration)) = redirect_result {
        if is_migration {
            // ASK redirect (temporary - slot being migrated)
            return Err(CommandError::Ask { slot, host, port });
        }
        // MOVED redirect (permanent - slot belongs to another node)
        return Err(CommandError::Moved { slot, host, port });
    }

    Ok(())
}

//! Edge case and boundary condition parity tests
//!
//! Tests for: empty values, null handling, binary data, special characters,
//! wrong arity, wrong types, overflow, and other boundary conditions.

#![allow(clippy::large_stack_frames)]

use crate::report::{CategoryReport, TestResult};
use crate::run_parity_test;
use crate::DualClient;

/// Run all edge case tests
#[allow(clippy::large_stack_frames)]
pub async fn run_all(client: &mut DualClient) -> CategoryReport {
    let mut report = CategoryReport::new("Edge Cases & Boundary Conditions");

    // Empty and null value handling
    report.add_result(test_empty_string_value(client).await);
    report.add_result(test_empty_key(client).await);
    report.add_result(test_whitespace_key(client).await);
    report.add_result(test_whitespace_value(client).await);

    // Special characters in keys/values
    report.add_result(test_special_chars_in_key(client).await);
    report.add_result(test_special_chars_in_value(client).await);
    report.add_result(test_unicode_key(client).await);
    report.add_result(test_unicode_value(client).await);
    report.add_result(test_newline_in_value(client).await);
    report.add_result(test_binary_safe_value(client).await);

    // Numeric boundaries
    report.add_result(test_incr_overflow(client).await);
    report.add_result(test_incr_underflow(client).await);
    report.add_result(test_large_integer(client).await);
    report.add_result(test_negative_integer(client).await);
    report.add_result(test_float_special_values(client).await);

    // Wrong arity errors
    report.add_result(test_get_wrong_arity(client).await);
    report.add_result(test_set_wrong_arity(client).await);
    report.add_result(test_lpush_wrong_arity(client).await);
    report.add_result(test_hset_wrong_arity(client).await);
    report.add_result(test_zadd_wrong_arity(client).await);

    // Wrong type errors (WRONGTYPE)
    report.add_result(test_string_op_on_list(client).await);
    report.add_result(test_string_op_on_set(client).await);
    report.add_result(test_string_op_on_hash(client).await);
    report.add_result(test_string_op_on_zset(client).await);
    report.add_result(test_list_op_on_string(client).await);
    report.add_result(test_set_op_on_string(client).await);
    report.add_result(test_hash_op_on_string(client).await);
    report.add_result(test_zset_op_on_string(client).await);

    // Invalid argument errors
    report.add_result(test_expire_invalid_time(client).await);
    report.add_result(test_setex_invalid_time(client).await);
    report.add_result(test_incr_on_non_integer(client).await);
    report.add_result(test_incrbyfloat_on_non_float(client).await);
    report.add_result(test_zadd_invalid_score(client).await);
    report.add_result(test_lindex_invalid_index(client).await);

    // Large data handling
    report.add_result(test_large_value(client).await);
    report.add_result(test_many_list_elements(client).await);
    report.add_result(test_many_set_members(client).await);
    report.add_result(test_many_hash_fields(client).await);
    report.add_result(test_many_zset_members(client).await);

    // Expiry edge cases
    report.add_result(test_expire_zero(client).await);
    report.add_result(test_expire_negative(client).await);
    report.add_result(test_pexpire_small(client).await);

    // Key patterns
    report.add_result(test_keys_empty_pattern(client).await);
    report.add_result(test_keys_complex_pattern(client).await);

    // Concurrent-like scenarios
    report.add_result(test_delete_while_iterating(client).await);
    report.add_result(test_overwrite_different_type(client).await);

    report
}

// ============================================================================
// Empty and null value handling
// ============================================================================

async fn test_empty_string_value(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "Empty string value", {
        c.assert_parity(&["SET", "key", ""]).await?;
        c.assert_parity(&["GET", "key"]).await?;
        c.assert_parity(&["STRLEN", "key"]).await?;
        Ok(())
    })
}

async fn test_empty_key(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "Empty key name", {
        // Empty key should work in Redis
        c.assert_parity(&["SET", "", "value"]).await?;
        c.assert_parity(&["GET", ""]).await?;
        c.assert_parity(&["DEL", ""]).await?;
        Ok(())
    })
}

async fn test_whitespace_key(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "Whitespace key", {
        c.assert_parity(&["SET", "   ", "value"]).await?;
        c.assert_parity(&["GET", "   "]).await?;
        c.assert_parity(&["SET", "\t\n", "value2"]).await?;
        c.assert_parity(&["GET", "\t\n"]).await?;
        Ok(())
    })
}

async fn test_whitespace_value(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "Whitespace value", {
        c.assert_parity(&["SET", "key", "   "]).await?;
        c.assert_parity(&["GET", "key"]).await?;
        c.assert_parity(&["STRLEN", "key"]).await?;
        Ok(())
    })
}

// ============================================================================
// Special characters in keys/values
// ============================================================================

async fn test_special_chars_in_key(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "Special chars in key", {
        c.assert_parity(&["SET", "key:with:colons", "v1"]).await?;
        c.assert_parity(&["GET", "key:with:colons"]).await?;
        c.assert_parity(&["SET", "key.with.dots", "v2"]).await?;
        c.assert_parity(&["GET", "key.with.dots"]).await?;
        c.assert_parity(&["SET", "key-with-dashes", "v3"]).await?;
        c.assert_parity(&["GET", "key-with-dashes"]).await?;
        c.assert_parity(&["SET", "key_with_underscores", "v4"])
            .await?;
        c.assert_parity(&["GET", "key_with_underscores"]).await?;
        Ok(())
    })
}

async fn test_special_chars_in_value(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "Special chars in value", {
        c.assert_parity(&["SET", "k1", "value with spaces"]).await?;
        c.assert_parity(&["GET", "k1"]).await?;
        c.assert_parity(&["SET", "k2", "value\twith\ttabs"]).await?;
        c.assert_parity(&["GET", "k2"]).await?;
        c.assert_parity(&["SET", "k3", "!@#$%^&*()[]{}|;':\",./<>?"])
            .await?;
        c.assert_parity(&["GET", "k3"]).await?;
        Ok(())
    })
}

async fn test_unicode_key(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "Unicode key", {
        c.assert_parity(&["SET", "键", "value"]).await?;
        c.assert_parity(&["GET", "键"]).await?;
        c.assert_parity(&["SET", "مفتاح", "value2"]).await?;
        c.assert_parity(&["GET", "مفتاح"]).await?;
        c.assert_parity(&["SET", "🔑", "emoji key"]).await?;
        c.assert_parity(&["GET", "🔑"]).await?;
        Ok(())
    })
}

async fn test_unicode_value(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "Unicode value", {
        c.assert_parity(&["SET", "k1", "你好世界"]).await?;
        c.assert_parity(&["GET", "k1"]).await?;
        c.assert_parity(&["SET", "k2", "مرحبا بالعالم"]).await?;
        c.assert_parity(&["GET", "k2"]).await?;
        c.assert_parity(&["SET", "k3", "🎉🎊🎁"]).await?;
        c.assert_parity(&["GET", "k3"]).await?;
        Ok(())
    })
}

async fn test_newline_in_value(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "Newline in value", {
        c.assert_parity(&["SET", "key", "line1\nline2\nline3"])
            .await?;
        c.assert_parity(&["GET", "key"]).await?;
        c.assert_parity(&["SET", "key2", "line1\r\nline2\r\n"])
            .await?;
        c.assert_parity(&["GET", "key2"]).await?;
        Ok(())
    })
}

async fn test_binary_safe_value(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "Binary-safe value", {
        // Test with null bytes and other binary data
        c.assert_parity(&["SET", "key", "before\x00after"]).await?;
        c.assert_parity(&["GET", "key"]).await?;
        c.assert_parity(&["STRLEN", "key"]).await?;
        Ok(())
    })
}

// ============================================================================
// Numeric boundaries
// ============================================================================

async fn test_incr_overflow(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "INCR overflow", {
        // Set to near max i64
        c.assert_parity(&["SET", "counter", "9223372036854775807"])
            .await?;
        // This should error - overflow
        c.assert_both_error(&["INCR", "counter"]).await?;
        Ok(())
    })
}

async fn test_incr_underflow(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "DECR underflow", {
        // Set to near min i64
        c.assert_parity(&["SET", "counter", "-9223372036854775808"])
            .await?;
        // This should error - underflow
        c.assert_both_error(&["DECR", "counter"]).await?;
        Ok(())
    })
}

async fn test_large_integer(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "Large integer operations", {
        c.assert_parity(&["SET", "num", "9223372036854775800"])
            .await?;
        c.assert_parity(&["GET", "num"]).await?;
        c.assert_parity(&["INCRBY", "num", "5"]).await?;
        c.assert_parity(&["GET", "num"]).await?;
        Ok(())
    })
}

async fn test_negative_integer(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "Negative integer operations", {
        c.assert_parity(&["SET", "num", "-100"]).await?;
        c.assert_parity(&["INCR", "num"]).await?;
        c.assert_parity(&["DECRBY", "num", "200"]).await?;
        c.assert_parity(&["GET", "num"]).await?;
        Ok(())
    })
}

async fn test_float_special_values(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "Float special values", {
        // Test inf in sorted sets
        c.assert_parity(&["ZADD", "zs", "inf", "infinite"]).await?;
        c.assert_parity(&["ZADD", "zs", "-inf", "neg_infinite"])
            .await?;
        c.assert_parity(&["ZRANGE", "zs", "0", "-1", "WITHSCORES"])
            .await?;
        Ok(())
    })
}

// ============================================================================
// Wrong arity errors
// ============================================================================

async fn test_get_wrong_arity(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "GET wrong arity", {
        c.assert_both_error(&["GET"]).await?;
        c.assert_both_error(&["GET", "key1", "key2"]).await?;
        Ok(())
    })
}

async fn test_set_wrong_arity(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "SET wrong arity", {
        c.assert_both_error(&["SET"]).await?;
        c.assert_both_error(&["SET", "key"]).await?;
        Ok(())
    })
}

async fn test_lpush_wrong_arity(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "LPUSH wrong arity", {
        c.assert_both_error(&["LPUSH"]).await?;
        c.assert_both_error(&["LPUSH", "key"]).await?;
        Ok(())
    })
}

async fn test_hset_wrong_arity(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "HSET wrong arity", {
        c.assert_both_error(&["HSET"]).await?;
        c.assert_both_error(&["HSET", "key"]).await?;
        c.assert_both_error(&["HSET", "key", "field"]).await?;
        Ok(())
    })
}

async fn test_zadd_wrong_arity(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZADD wrong arity", {
        c.assert_both_error(&["ZADD"]).await?;
        c.assert_both_error(&["ZADD", "key"]).await?;
        c.assert_both_error(&["ZADD", "key", "1"]).await?;
        Ok(())
    })
}

// ============================================================================
// Wrong type errors (WRONGTYPE)
// ============================================================================

async fn test_string_op_on_list(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "String op on list", {
        c.assert_parity(&["LPUSH", "mylist", "item"]).await?;
        c.assert_both_error(&["GET", "mylist"]).await?;
        c.assert_both_error(&["APPEND", "mylist", "data"]).await?;
        c.assert_both_error(&["INCR", "mylist"]).await?;
        Ok(())
    })
}

async fn test_string_op_on_set(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "String op on set", {
        c.assert_parity(&["SADD", "myset", "member"]).await?;
        c.assert_both_error(&["GET", "myset"]).await?;
        c.assert_both_error(&["STRLEN", "myset"]).await?;
        Ok(())
    })
}

async fn test_string_op_on_hash(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "String op on hash", {
        c.assert_parity(&["HSET", "myhash", "f", "v"]).await?;
        c.assert_both_error(&["GET", "myhash"]).await?;
        c.assert_both_error(&["SETRANGE", "myhash", "0", "x"])
            .await?;
        Ok(())
    })
}

async fn test_string_op_on_zset(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "String op on zset", {
        c.assert_parity(&["ZADD", "myzset", "1", "m"]).await?;
        c.assert_both_error(&["GET", "myzset"]).await?;
        c.assert_both_error(&["GETRANGE", "myzset", "0", "10"])
            .await?;
        Ok(())
    })
}

async fn test_list_op_on_string(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "List op on string", {
        c.assert_parity(&["SET", "mystr", "value"]).await?;
        c.assert_both_error(&["LPUSH", "mystr", "item"]).await?;
        c.assert_both_error(&["LRANGE", "mystr", "0", "-1"]).await?;
        c.assert_both_error(&["LLEN", "mystr"]).await?;
        Ok(())
    })
}

async fn test_set_op_on_string(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "Set op on string", {
        c.assert_parity(&["SET", "mystr", "value"]).await?;
        c.assert_both_error(&["SADD", "mystr", "member"]).await?;
        c.assert_both_error(&["SMEMBERS", "mystr"]).await?;
        c.assert_both_error(&["SCARD", "mystr"]).await?;
        Ok(())
    })
}

async fn test_hash_op_on_string(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "Hash op on string", {
        c.assert_parity(&["SET", "mystr", "value"]).await?;
        c.assert_both_error(&["HSET", "mystr", "field", "val"])
            .await?;
        c.assert_both_error(&["HGET", "mystr", "field"]).await?;
        c.assert_both_error(&["HGETALL", "mystr"]).await?;
        Ok(())
    })
}

async fn test_zset_op_on_string(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "Zset op on string", {
        c.assert_parity(&["SET", "mystr", "value"]).await?;
        c.assert_both_error(&["ZADD", "mystr", "1", "member"])
            .await?;
        c.assert_both_error(&["ZRANGE", "mystr", "0", "-1"]).await?;
        c.assert_both_error(&["ZCARD", "mystr"]).await?;
        Ok(())
    })
}

// ============================================================================
// Invalid argument errors
// ============================================================================

async fn test_expire_invalid_time(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "EXPIRE invalid time", {
        c.assert_parity(&["SET", "key", "value"]).await?;
        c.assert_both_error(&["EXPIRE", "key", "notanumber"])
            .await?;
        c.assert_both_error(&["EXPIRE", "key", "1.5"]).await?;
        Ok(())
    })
}

async fn test_setex_invalid_time(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "SETEX invalid time", {
        c.assert_both_error(&["SETEX", "key", "notanumber", "value"])
            .await?;
        c.assert_both_error(&["SETEX", "key", "0", "value"]).await?;
        c.assert_both_error(&["SETEX", "key", "-1", "value"])
            .await?;
        Ok(())
    })
}

async fn test_incr_on_non_integer(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "INCR on non-integer string", {
        c.assert_parity(&["SET", "key", "hello"]).await?;
        c.assert_both_error(&["INCR", "key"]).await?;
        c.assert_parity(&["SET", "key2", "10.5"]).await?;
        c.assert_both_error(&["INCR", "key2"]).await?;
        c.assert_parity(&["SET", "key3", "10abc"]).await?;
        c.assert_both_error(&["INCR", "key3"]).await?;
        Ok(())
    })
}

async fn test_incrbyfloat_on_non_float(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "INCRBYFLOAT on non-float", {
        c.assert_parity(&["SET", "key", "hello"]).await?;
        c.assert_both_error(&["INCRBYFLOAT", "key", "1.0"]).await?;
        Ok(())
    })
}

async fn test_zadd_invalid_score(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZADD invalid score", {
        c.assert_both_error(&["ZADD", "zs", "notanumber", "member"])
            .await?;
        // NaN should also error
        c.assert_both_error(&["ZADD", "zs", "nan", "member"])
            .await?;
        Ok(())
    })
}

async fn test_lindex_invalid_index(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "LINDEX invalid index", {
        c.assert_parity(&["RPUSH", "list", "a", "b", "c"]).await?;
        c.assert_both_error(&["LINDEX", "list", "notanumber"])
            .await?;
        Ok(())
    })
}

// ============================================================================
// Large data handling
// ============================================================================

async fn test_large_value(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "Large value (1MB)", {
        let large_value = "x".repeat(1024 * 1024); // 1MB
        c.assert_parity(&["SET", "largekey", &large_value]).await?;
        c.assert_parity(&["STRLEN", "largekey"]).await?;
        Ok(())
    })
}

async fn test_many_list_elements(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "Many list elements", {
        // Add 1000 elements
        for i in 0..100 {
            c.assert_parity(&["RPUSH", "biglist", &format!("item{}", i)])
                .await?;
        }
        c.assert_parity(&["LLEN", "biglist"]).await?;
        c.assert_parity(&["LRANGE", "biglist", "0", "10"]).await?;
        c.assert_parity(&["LRANGE", "biglist", "-10", "-1"]).await?;
        Ok(())
    })
}

async fn test_many_set_members(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "Many set members", {
        let mut args = vec!["SADD".to_string(), "bigset".to_string()];
        for i in 0..100 {
            args.push(format!("member{}", i));
        }
        let args_ref: Vec<&str> = args.iter().map(String::as_str).collect();
        c.assert_parity(&args_ref).await?;
        c.assert_parity(&["SCARD", "bigset"]).await?;
        Ok(())
    })
}

async fn test_many_hash_fields(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "Many hash fields", {
        let mut args = vec!["HSET".to_string(), "bighash".to_string()];
        for i in 0..100 {
            args.push(format!("field{}", i));
            args.push(format!("value{}", i));
        }
        let args_ref: Vec<&str> = args.iter().map(String::as_str).collect();
        c.assert_parity(&args_ref).await?;
        c.assert_parity(&["HLEN", "bighash"]).await?;
        Ok(())
    })
}

async fn test_many_zset_members(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "Many zset members", {
        let mut args = vec!["ZADD".to_string(), "bigzset".to_string()];
        for i in 0..100 {
            args.push(format!("{}", i));
            args.push(format!("member{}", i));
        }
        let args_ref: Vec<&str> = args.iter().map(String::as_str).collect();
        c.assert_parity(&args_ref).await?;
        c.assert_parity(&["ZCARD", "bigzset"]).await?;
        c.assert_parity(&["ZRANGE", "bigzset", "0", "10"]).await?;
        Ok(())
    })
}

// ============================================================================
// Expiry edge cases
// ============================================================================

async fn test_expire_zero(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "EXPIRE with zero", {
        c.assert_parity(&["SET", "key", "value"]).await?;
        c.assert_parity(&["EXPIRE", "key", "0"]).await?;
        // Key should be deleted immediately (or very soon)
        // Give a tiny delay then check
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        c.assert_parity(&["EXISTS", "key"]).await?;
        Ok(())
    })
}

async fn test_expire_negative(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "EXPIRE with negative", {
        c.assert_parity(&["SET", "key", "value"]).await?;
        c.assert_parity(&["EXPIRE", "key", "-1"]).await?;
        // Key should be deleted
        c.assert_parity(&["EXISTS", "key"]).await?;
        Ok(())
    })
}

async fn test_pexpire_small(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "PEXPIRE with small value", {
        c.assert_parity(&["SET", "key", "value"]).await?;
        c.assert_parity(&["PEXPIRE", "key", "1"]).await?;
        // Wait for expiry
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        c.assert_parity(&["EXISTS", "key"]).await?;
        Ok(())
    })
}

// ============================================================================
// Key patterns
// ============================================================================

async fn test_keys_empty_pattern(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "KEYS with empty result", {
        // No keys match this pattern
        c.assert_parity(&["KEYS", "nonexistent_pattern_*"]).await?;
        Ok(())
    })
}

async fn test_keys_complex_pattern(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "KEYS complex pattern", {
        c.assert_parity(&["SET", "user:1:name", "alice"]).await?;
        c.assert_parity(&["SET", "user:2:name", "bob"]).await?;
        c.assert_parity(&["SET", "user:1:email", "a@b.c"]).await?;
        c.assert_parity(&["SET", "post:1", "content"]).await?;
        c.assert_parity_unordered(&["KEYS", "user:*:name"]).await?;
        c.assert_parity_unordered(&["KEYS", "user:1:*"]).await?;
        c.assert_parity_unordered(&["KEYS", "*:1*"]).await?;
        Ok(())
    })
}

// ============================================================================
// Concurrent-like scenarios
// ============================================================================

async fn test_delete_while_iterating(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "Delete during operations", {
        c.assert_parity(&["SET", "key", "value"]).await?;
        c.assert_parity(&["DEL", "key"]).await?;
        c.assert_parity(&["GET", "key"]).await?;
        // Re-create with different type
        c.assert_parity(&["LPUSH", "key", "item"]).await?;
        c.assert_parity(&["TYPE", "key"]).await?;
        Ok(())
    })
}

async fn test_overwrite_different_type(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "Overwrite with different type", {
        // Create as string
        c.assert_parity(&["SET", "key", "string_value"]).await?;
        c.assert_parity(&["TYPE", "key"]).await?;

        // DEL and recreate as list
        c.assert_parity(&["DEL", "key"]).await?;
        c.assert_parity(&["LPUSH", "key", "list_item"]).await?;
        c.assert_parity(&["TYPE", "key"]).await?;

        // DEL and recreate as set
        c.assert_parity(&["DEL", "key"]).await?;
        c.assert_parity(&["SADD", "key", "set_member"]).await?;
        c.assert_parity(&["TYPE", "key"]).await?;

        // DEL and recreate as hash
        c.assert_parity(&["DEL", "key"]).await?;
        c.assert_parity(&["HSET", "key", "field", "value"]).await?;
        c.assert_parity(&["TYPE", "key"]).await?;

        // DEL and recreate as zset
        c.assert_parity(&["DEL", "key"]).await?;
        c.assert_parity(&["ZADD", "key", "1", "member"]).await?;
        c.assert_parity(&["TYPE", "key"]).await?;

        Ok(())
    })
}

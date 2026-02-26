//! Key command parity tests
//!
//! Tests for: DEL, EXISTS, EXPIRE, EXPIREAT, TTL, PTTL, PERSIST, TYPE,
//! KEYS, SCAN, RENAME, RENAMENX, COPY, UNLINK, TOUCH, OBJECT

use crate::report::{CategoryReport, TestResult};
use crate::run_parity_test;
use crate::DualClient;

/// Run all key command tests
pub async fn run_all(client: &mut DualClient) -> CategoryReport {
    let mut report = CategoryReport::new("Key Commands");

    // DEL
    report.add_result(test_del(client).await);
    report.add_result(test_del_multiple(client).await);
    report.add_result(test_del_nonexistent(client).await);

    // EXISTS
    report.add_result(test_exists(client).await);
    report.add_result(test_exists_multiple(client).await);

    // EXPIRE, TTL
    report.add_result(test_expire_ttl(client).await);
    report.add_result(test_expire_nonexistent(client).await);
    report.add_result(test_ttl_no_expire(client).await);

    // PEXPIRE, PTTL
    report.add_result(test_pexpire_pttl(client).await);

    // EXPIREAT, EXPIRETIME
    report.add_result(test_expireat(client).await);

    // PERSIST
    report.add_result(test_persist(client).await);
    report.add_result(test_persist_no_ttl(client).await);

    // TYPE
    report.add_result(test_type_string(client).await);
    report.add_result(test_type_list(client).await);
    report.add_result(test_type_set(client).await);
    report.add_result(test_type_hash(client).await);
    report.add_result(test_type_zset(client).await);
    report.add_result(test_type_nonexistent(client).await);

    // KEYS
    report.add_result(test_keys_all(client).await);
    report.add_result(test_keys_pattern(client).await);

    // RENAME
    report.add_result(test_rename(client).await);
    report.add_result(test_rename_overwrite(client).await);
    report.add_result(test_rename_nonexistent(client).await);

    // RENAMENX
    report.add_result(test_renamenx(client).await);
    report.add_result(test_renamenx_exists(client).await);

    // COPY
    report.add_result(test_copy(client).await);
    report.add_result(test_copy_replace(client).await);

    // UNLINK
    report.add_result(test_unlink(client).await);

    // TOUCH
    report.add_result(test_touch(client).await);

    report
}

// ============================================================================
// DEL
// ============================================================================

async fn test_del(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "DEL", {
        c.assert_parity(&["SET", "key", "value"]).await?;
        c.assert_parity(&["DEL", "key"]).await?;
        c.assert_parity(&["GET", "key"]).await?;
        Ok(())
    })
}

async fn test_del_multiple(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "DEL multiple", {
        c.assert_parity(&["SET", "k1", "v1"]).await?;
        c.assert_parity(&["SET", "k2", "v2"]).await?;
        c.assert_parity(&["SET", "k3", "v3"]).await?;
        c.assert_parity(&["DEL", "k1", "k2", "k3"]).await?;
        Ok(())
    })
}

async fn test_del_nonexistent(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "DEL nonexistent", {
        c.assert_parity(&["DEL", "nonexistent"]).await?;
        Ok(())
    })
}

// ============================================================================
// EXISTS
// ============================================================================

async fn test_exists(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "EXISTS", {
        c.assert_parity(&["SET", "key", "value"]).await?;
        c.assert_parity(&["EXISTS", "key"]).await?;
        c.assert_parity(&["EXISTS", "nonexistent"]).await?;
        Ok(())
    })
}

async fn test_exists_multiple(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "EXISTS multiple", {
        c.assert_parity(&["SET", "k1", "v1"]).await?;
        c.assert_parity(&["SET", "k2", "v2"]).await?;
        c.assert_parity(&["EXISTS", "k1", "k2", "k3"]).await?;
        Ok(())
    })
}

// ============================================================================
// EXPIRE, TTL
// ============================================================================

async fn test_expire_ttl(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "EXPIRE/TTL", {
        c.assert_parity(&["SET", "key", "value"]).await?;
        c.assert_parity(&["EXPIRE", "key", "100"]).await?;

        // TTL should be approximately 100
        let redis_ttl = c.redis(&["TTL", "key"]).await?;
        let ferris_ttl = c.ferris(&["TTL", "key"]).await?;

        if let (ferris_protocol::RespValue::Integer(r), ferris_protocol::RespValue::Integer(f)) =
            (&redis_ttl, &ferris_ttl)
        {
            if (*r - *f).abs() > 2 {
                anyhow::bail!("TTL mismatch: Redis={}, Ferris={}", r, f);
            }
        }
        Ok(())
    })
}

async fn test_expire_nonexistent(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "EXPIRE nonexistent", {
        c.assert_parity(&["EXPIRE", "nonexistent", "100"]).await?;
        Ok(())
    })
}

async fn test_ttl_no_expire(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "TTL no expire", {
        c.assert_parity(&["SET", "key", "value"]).await?;
        c.assert_parity(&["TTL", "key"]).await?; // Should be -1
        c.assert_parity(&["TTL", "nonexistent"]).await?; // Should be -2
        Ok(())
    })
}

// ============================================================================
// PEXPIRE, PTTL
// ============================================================================

async fn test_pexpire_pttl(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "PEXPIRE/PTTL", {
        c.assert_parity(&["SET", "key", "value"]).await?;
        c.assert_parity(&["PEXPIRE", "key", "100000"]).await?;

        // PTTL should be approximately 100000
        let redis_pttl = c.redis(&["PTTL", "key"]).await?;
        let ferris_pttl = c.ferris(&["PTTL", "key"]).await?;

        if let (ferris_protocol::RespValue::Integer(r), ferris_protocol::RespValue::Integer(f)) =
            (&redis_pttl, &ferris_pttl)
        {
            if (*r - *f).abs() > 1000 {
                anyhow::bail!("PTTL mismatch: Redis={}, Ferris={}", r, f);
            }
        }
        Ok(())
    })
}

// ============================================================================
// EXPIREAT
// ============================================================================

async fn test_expireat(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "EXPIREAT", {
        c.assert_parity(&["SET", "key", "value"]).await?;
        // Set expire to far future (year 2030)
        c.assert_parity(&["EXPIREAT", "key", "1893456000"]).await?;

        // Both should have TTL > 0
        let redis_ttl = c.redis(&["TTL", "key"]).await?;
        let ferris_ttl = c.ferris(&["TTL", "key"]).await?;

        if let (ferris_protocol::RespValue::Integer(r), ferris_protocol::RespValue::Integer(f)) =
            (&redis_ttl, &ferris_ttl)
        {
            if *r <= 0 || *f <= 0 {
                anyhow::bail!("TTL should be positive: Redis={}, Ferris={}", r, f);
            }
        }
        Ok(())
    })
}

// ============================================================================
// PERSIST
// ============================================================================

async fn test_persist(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "PERSIST", {
        c.assert_parity(&["SET", "key", "value", "EX", "100"])
            .await?;
        c.assert_parity(&["PERSIST", "key"]).await?;
        c.assert_parity(&["TTL", "key"]).await?; // Should be -1
        Ok(())
    })
}

async fn test_persist_no_ttl(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "PERSIST no TTL", {
        c.assert_parity(&["SET", "key", "value"]).await?;
        c.assert_parity(&["PERSIST", "key"]).await?; // Should return 0
        Ok(())
    })
}

// ============================================================================
// TYPE
// ============================================================================

async fn test_type_string(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "TYPE string", {
        c.assert_parity(&["SET", "key", "value"]).await?;
        c.assert_parity(&["TYPE", "key"]).await?;
        Ok(())
    })
}

async fn test_type_list(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "TYPE list", {
        c.assert_parity(&["LPUSH", "key", "value"]).await?;
        c.assert_parity(&["TYPE", "key"]).await?;
        Ok(())
    })
}

async fn test_type_set(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "TYPE set", {
        c.assert_parity(&["SADD", "key", "member"]).await?;
        c.assert_parity(&["TYPE", "key"]).await?;
        Ok(())
    })
}

async fn test_type_hash(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "TYPE hash", {
        c.assert_parity(&["HSET", "key", "field", "value"]).await?;
        c.assert_parity(&["TYPE", "key"]).await?;
        Ok(())
    })
}

async fn test_type_zset(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "TYPE zset", {
        c.assert_parity(&["ZADD", "key", "1", "member"]).await?;
        c.assert_parity(&["TYPE", "key"]).await?;
        Ok(())
    })
}

async fn test_type_nonexistent(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "TYPE nonexistent", {
        c.assert_parity(&["TYPE", "nonexistent"]).await?;
        Ok(())
    })
}

// ============================================================================
// KEYS
// ============================================================================

async fn test_keys_all(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "KEYS *", {
        c.assert_parity(&["SET", "k1", "v1"]).await?;
        c.assert_parity(&["SET", "k2", "v2"]).await?;
        c.assert_parity(&["SET", "k3", "v3"]).await?;
        c.assert_parity_unordered(&["KEYS", "*"]).await?;
        Ok(())
    })
}

async fn test_keys_pattern(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "KEYS pattern", {
        c.assert_parity(&["SET", "user:1", "a"]).await?;
        c.assert_parity(&["SET", "user:2", "b"]).await?;
        c.assert_parity(&["SET", "post:1", "c"]).await?;
        c.assert_parity_unordered(&["KEYS", "user:*"]).await?;
        Ok(())
    })
}

// ============================================================================
// RENAME
// ============================================================================

async fn test_rename(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "RENAME", {
        c.assert_parity(&["SET", "oldkey", "value"]).await?;
        c.assert_parity(&["RENAME", "oldkey", "newkey"]).await?;
        c.assert_parity(&["GET", "oldkey"]).await?; // Should be nil
        c.assert_parity(&["GET", "newkey"]).await?;
        Ok(())
    })
}

async fn test_rename_overwrite(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "RENAME overwrite", {
        c.assert_parity(&["SET", "key1", "value1"]).await?;
        c.assert_parity(&["SET", "key2", "value2"]).await?;
        c.assert_parity(&["RENAME", "key1", "key2"]).await?;
        c.assert_parity(&["GET", "key2"]).await?; // Should be value1
        Ok(())
    })
}

async fn test_rename_nonexistent(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "RENAME nonexistent", {
        c.assert_both_error(&["RENAME", "nonexistent", "newkey"])
            .await?;
        Ok(())
    })
}

// ============================================================================
// RENAMENX
// ============================================================================

async fn test_renamenx(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "RENAMENX", {
        c.assert_parity(&["SET", "oldkey", "value"]).await?;
        c.assert_parity(&["RENAMENX", "oldkey", "newkey"]).await?;
        c.assert_parity(&["GET", "newkey"]).await?;
        Ok(())
    })
}

async fn test_renamenx_exists(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "RENAMENX exists", {
        c.assert_parity(&["SET", "key1", "value1"]).await?;
        c.assert_parity(&["SET", "key2", "value2"]).await?;
        c.assert_parity(&["RENAMENX", "key1", "key2"]).await?; // Should return 0
        c.assert_parity(&["GET", "key1"]).await?; // Should still exist
        c.assert_parity(&["GET", "key2"]).await?; // Should still be value2
        Ok(())
    })
}

// ============================================================================
// COPY
// ============================================================================

async fn test_copy(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "COPY", {
        c.assert_parity(&["SET", "src", "value"]).await?;
        c.assert_parity(&["COPY", "src", "dst"]).await?;
        c.assert_parity(&["GET", "src"]).await?;
        c.assert_parity(&["GET", "dst"]).await?;
        Ok(())
    })
}

async fn test_copy_replace(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "COPY REPLACE", {
        c.assert_parity(&["SET", "src", "value1"]).await?;
        c.assert_parity(&["SET", "dst", "value2"]).await?;
        c.assert_parity(&["COPY", "src", "dst", "REPLACE"]).await?;
        c.assert_parity(&["GET", "dst"]).await?; // Should be value1
        Ok(())
    })
}

// ============================================================================
// UNLINK
// ============================================================================

async fn test_unlink(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "UNLINK", {
        c.assert_parity(&["SET", "k1", "v1"]).await?;
        c.assert_parity(&["SET", "k2", "v2"]).await?;
        c.assert_parity(&["UNLINK", "k1", "k2"]).await?;
        c.assert_parity(&["EXISTS", "k1", "k2"]).await?;
        Ok(())
    })
}

// ============================================================================
// TOUCH
// ============================================================================

async fn test_touch(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "TOUCH", {
        c.assert_parity(&["SET", "k1", "v1"]).await?;
        c.assert_parity(&["SET", "k2", "v2"]).await?;
        c.assert_parity(&["TOUCH", "k1", "k2", "k3"]).await?;
        Ok(())
    })
}

//! String command parity tests
//!
//! Tests for: GET, SET, MGET, MSET, SETNX, SETEX, GETEX, GETDEL,
//! INCR, INCRBY, INCRBYFLOAT, DECR, DECRBY, APPEND, STRLEN, GETRANGE, SETRANGE

use crate::report::{CategoryReport, TestResult};
use crate::run_parity_test;
use crate::DualClient;

/// Run all string command tests
pub async fn run_all(client: &mut DualClient) -> CategoryReport {
    let mut report = CategoryReport::new("String Commands");

    // Basic GET/SET
    report.add_result(test_set_get_basic(client).await);
    report.add_result(test_get_nonexistent(client).await);
    report.add_result(test_set_overwrite(client).await);

    // SET options
    report.add_result(test_set_ex(client).await);
    report.add_result(test_set_px(client).await);
    report.add_result(test_set_nx(client).await);
    report.add_result(test_set_xx(client).await);
    report.add_result(test_set_nx_xx_conflict(client).await);

    // SETNX, SETEX, PSETEX
    report.add_result(test_setnx(client).await);
    report.add_result(test_setex(client).await);
    report.add_result(test_psetex(client).await);

    // GETEX, GETDEL
    report.add_result(test_getex_ex(client).await);
    report.add_result(test_getex_persist(client).await);
    report.add_result(test_getdel(client).await);

    // MGET, MSET, MSETNX
    report.add_result(test_mset_mget(client).await);
    report.add_result(test_mget_partial(client).await);
    report.add_result(test_msetnx_success(client).await);
    report.add_result(test_msetnx_fail(client).await);

    // INCR, DECR family
    report.add_result(test_incr(client).await);
    report.add_result(test_incr_new_key(client).await);
    report.add_result(test_incr_not_integer(client).await);
    report.add_result(test_incrby(client).await);
    report.add_result(test_incrbyfloat(client).await);
    report.add_result(test_decr(client).await);
    report.add_result(test_decrby(client).await);

    // APPEND, STRLEN
    report.add_result(test_append(client).await);
    report.add_result(test_append_new_key(client).await);
    report.add_result(test_strlen(client).await);
    report.add_result(test_strlen_nonexistent(client).await);

    // GETRANGE, SETRANGE
    report.add_result(test_getrange(client).await);
    report.add_result(test_getrange_negative(client).await);
    report.add_result(test_setrange(client).await);
    report.add_result(test_setrange_padding(client).await);

    // Type errors
    report.add_result(test_get_wrong_type(client).await);
    report.add_result(test_incr_wrong_type(client).await);

    report
}

// ============================================================================
// Basic GET/SET tests
// ============================================================================

async fn test_set_get_basic(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "SET/GET basic", {
        c.assert_parity(&["SET", "mykey", "myvalue"]).await?;
        c.assert_parity(&["GET", "mykey"]).await?;
        Ok(())
    })
}

async fn test_get_nonexistent(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "GET nonexistent key", {
        c.assert_parity(&["GET", "nonexistent"]).await?;
        Ok(())
    })
}

async fn test_set_overwrite(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "SET overwrite", {
        c.assert_parity(&["SET", "key", "value1"]).await?;
        c.assert_parity(&["SET", "key", "value2"]).await?;
        c.assert_parity(&["GET", "key"]).await?;
        Ok(())
    })
}

// ============================================================================
// SET with options
// ============================================================================

async fn test_set_ex(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "SET EX", {
        c.assert_parity(&["SET", "key", "value", "EX", "100"])
            .await?;
        // TTL should be approximately 100
        let redis_ttl = c.redis(&["TTL", "key"]).await?;
        let ferris_ttl = c.ferris(&["TTL", "key"]).await?;

        // Allow some variance
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

async fn test_set_px(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "SET PX", {
        c.assert_parity(&["SET", "key", "value", "PX", "100000"])
            .await?;
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

async fn test_set_nx(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "SET NX", {
        // NX on new key should succeed
        c.assert_parity(&["SET", "newkey", "value", "NX"]).await?;
        c.assert_parity(&["GET", "newkey"]).await?;

        // NX on existing key should return nil
        c.assert_parity(&["SET", "newkey", "value2", "NX"]).await?;
        // Value should still be original
        c.assert_parity(&["GET", "newkey"]).await?;
        Ok(())
    })
}

async fn test_set_xx(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "SET XX", {
        // XX on nonexistent key should return nil
        c.assert_parity(&["SET", "newkey", "value", "XX"]).await?;
        c.assert_parity(&["GET", "newkey"]).await?;

        // Create key
        c.assert_parity(&["SET", "newkey", "value1"]).await?;

        // XX on existing key should succeed
        c.assert_parity(&["SET", "newkey", "value2", "XX"]).await?;
        c.assert_parity(&["GET", "newkey"]).await?;
        Ok(())
    })
}

async fn test_set_nx_xx_conflict(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "SET NX XX conflict", {
        // NX and XX together should error
        c.assert_both_error(&["SET", "key", "value", "NX", "XX"])
            .await?;
        Ok(())
    })
}

// ============================================================================
// SETNX, SETEX, PSETEX
// ============================================================================

async fn test_setnx(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "SETNX", {
        c.assert_parity(&["SETNX", "key", "value"]).await?;
        c.assert_parity(&["SETNX", "key", "value2"]).await?;
        c.assert_parity(&["GET", "key"]).await?;
        Ok(())
    })
}

async fn test_setex(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "SETEX", {
        c.assert_parity(&["SETEX", "key", "100", "value"]).await?;
        c.assert_parity(&["GET", "key"]).await?;
        Ok(())
    })
}

async fn test_psetex(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "PSETEX", {
        c.assert_parity(&["PSETEX", "key", "100000", "value"])
            .await?;
        c.assert_parity(&["GET", "key"]).await?;
        Ok(())
    })
}

// ============================================================================
// GETEX, GETDEL
// ============================================================================

async fn test_getex_ex(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "GETEX EX", {
        c.assert_parity(&["SET", "key", "value"]).await?;
        c.assert_parity(&["GETEX", "key", "EX", "100"]).await?;
        // Should have TTL now
        let redis_ttl = c.redis(&["TTL", "key"]).await?;
        let ferris_ttl = c.ferris(&["TTL", "key"]).await?;

        if let (ferris_protocol::RespValue::Integer(r), ferris_protocol::RespValue::Integer(f)) =
            (&redis_ttl, &ferris_ttl)
        {
            if *r < 0 || *f < 0 {
                anyhow::bail!("Key should have TTL: Redis={}, Ferris={}", r, f);
            }
        }
        Ok(())
    })
}

async fn test_getex_persist(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "GETEX PERSIST", {
        c.assert_parity(&["SET", "key", "value", "EX", "100"])
            .await?;
        c.assert_parity(&["GETEX", "key", "PERSIST"]).await?;
        // Should have no TTL now
        c.assert_parity(&["TTL", "key"]).await?;
        Ok(())
    })
}

async fn test_getdel(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "GETDEL", {
        c.assert_parity(&["SET", "key", "value"]).await?;
        c.assert_parity(&["GETDEL", "key"]).await?;
        c.assert_parity(&["GET", "key"]).await?; // Should be nil
        Ok(())
    })
}

// ============================================================================
// MGET, MSET, MSETNX
// ============================================================================

async fn test_mset_mget(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "MSET/MGET", {
        c.assert_parity(&["MSET", "k1", "v1", "k2", "v2", "k3", "v3"])
            .await?;
        c.assert_parity(&["MGET", "k1", "k2", "k3"]).await?;
        Ok(())
    })
}

async fn test_mget_partial(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "MGET with missing keys", {
        c.assert_parity(&["SET", "k1", "v1"]).await?;
        c.assert_parity(&["SET", "k3", "v3"]).await?;
        c.assert_parity(&["MGET", "k1", "k2", "k3"]).await?; // k2 should be nil
        Ok(())
    })
}

async fn test_msetnx_success(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "MSETNX success", {
        c.assert_parity(&["MSETNX", "k1", "v1", "k2", "v2"]).await?;
        c.assert_parity(&["MGET", "k1", "k2"]).await?;
        Ok(())
    })
}

async fn test_msetnx_fail(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "MSETNX fail (key exists)", {
        c.assert_parity(&["SET", "k1", "existing"]).await?;
        c.assert_parity(&["MSETNX", "k1", "v1", "k2", "v2"]).await?;
        c.assert_parity(&["GET", "k1"]).await?; // Should still be "existing"
        c.assert_parity(&["GET", "k2"]).await?; // Should be nil
        Ok(())
    })
}

// ============================================================================
// INCR, DECR family
// ============================================================================

async fn test_incr(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "INCR", {
        c.assert_parity(&["SET", "counter", "10"]).await?;
        c.assert_parity(&["INCR", "counter"]).await?;
        c.assert_parity(&["GET", "counter"]).await?;
        Ok(())
    })
}

async fn test_incr_new_key(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "INCR new key", {
        c.assert_parity(&["INCR", "newcounter"]).await?;
        c.assert_parity(&["GET", "newcounter"]).await?;
        Ok(())
    })
}

async fn test_incr_not_integer(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "INCR on non-integer", {
        c.assert_parity(&["SET", "key", "notanumber"]).await?;
        c.assert_both_error(&["INCR", "key"]).await?;
        Ok(())
    })
}

async fn test_incrby(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "INCRBY", {
        c.assert_parity(&["SET", "counter", "10"]).await?;
        c.assert_parity(&["INCRBY", "counter", "5"]).await?;
        c.assert_parity(&["INCRBY", "counter", "-3"]).await?;
        c.assert_parity(&["GET", "counter"]).await?;
        Ok(())
    })
}

async fn test_incrbyfloat(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "INCRBYFLOAT", {
        c.assert_parity(&["SET", "counter", "10.5"]).await?;
        // Use float tolerance comparison - string representation may differ slightly
        c.assert_parity_float(&["INCRBYFLOAT", "counter", "0.1"])
            .await?;
        Ok(())
    })
}

async fn test_decr(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "DECR", {
        c.assert_parity(&["SET", "counter", "10"]).await?;
        c.assert_parity(&["DECR", "counter"]).await?;
        c.assert_parity(&["GET", "counter"]).await?;
        Ok(())
    })
}

async fn test_decrby(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "DECRBY", {
        c.assert_parity(&["SET", "counter", "10"]).await?;
        c.assert_parity(&["DECRBY", "counter", "3"]).await?;
        c.assert_parity(&["GET", "counter"]).await?;
        Ok(())
    })
}

// ============================================================================
// APPEND, STRLEN
// ============================================================================

async fn test_append(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "APPEND", {
        c.assert_parity(&["SET", "key", "Hello"]).await?;
        c.assert_parity(&["APPEND", "key", " World"]).await?;
        c.assert_parity(&["GET", "key"]).await?;
        Ok(())
    })
}

async fn test_append_new_key(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "APPEND to new key", {
        c.assert_parity(&["APPEND", "newkey", "value"]).await?;
        c.assert_parity(&["GET", "newkey"]).await?;
        Ok(())
    })
}

async fn test_strlen(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "STRLEN", {
        c.assert_parity(&["SET", "key", "Hello World"]).await?;
        c.assert_parity(&["STRLEN", "key"]).await?;
        Ok(())
    })
}

async fn test_strlen_nonexistent(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "STRLEN nonexistent", {
        c.assert_parity(&["STRLEN", "nonexistent"]).await?;
        Ok(())
    })
}

// ============================================================================
// GETRANGE, SETRANGE
// ============================================================================

async fn test_getrange(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "GETRANGE", {
        c.assert_parity(&["SET", "key", "Hello World"]).await?;
        c.assert_parity(&["GETRANGE", "key", "0", "4"]).await?;
        c.assert_parity(&["GETRANGE", "key", "6", "10"]).await?;
        Ok(())
    })
}

async fn test_getrange_negative(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "GETRANGE negative indices", {
        c.assert_parity(&["SET", "key", "Hello World"]).await?;
        c.assert_parity(&["GETRANGE", "key", "-5", "-1"]).await?;
        Ok(())
    })
}

async fn test_setrange(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "SETRANGE", {
        c.assert_parity(&["SET", "key", "Hello World"]).await?;
        c.assert_parity(&["SETRANGE", "key", "6", "Redis"]).await?;
        c.assert_parity(&["GET", "key"]).await?;
        Ok(())
    })
}

async fn test_setrange_padding(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "SETRANGE with padding", {
        c.assert_parity(&["SET", "key", "Hello"]).await?;
        c.assert_parity(&["SETRANGE", "key", "10", "World"]).await?;
        c.assert_parity(&["GET", "key"]).await?;
        Ok(())
    })
}

// ============================================================================
// Type errors
// ============================================================================

async fn test_get_wrong_type(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "GET on wrong type", {
        c.assert_parity(&["LPUSH", "mylist", "item"]).await?;
        c.assert_both_error(&["GET", "mylist"]).await?;
        Ok(())
    })
}

async fn test_incr_wrong_type(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "INCR on wrong type", {
        c.assert_parity(&["LPUSH", "mylist", "item"]).await?;
        c.assert_both_error(&["INCR", "mylist"]).await?;
        Ok(())
    })
}

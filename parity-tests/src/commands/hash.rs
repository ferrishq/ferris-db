//! Hash command parity tests
//!
//! Tests for: HSET, HGET, HMSET, HMGET, HGETALL, HDEL, HEXISTS, HLEN,
//! HKEYS, HVALS, HINCRBY, HINCRBYFLOAT, HSETNX, HSTRLEN

use crate::report::{CategoryReport, TestResult};
use crate::run_parity_test;
use crate::DualClient;

/// Run all hash command tests
pub async fn run_all(client: &mut DualClient) -> CategoryReport {
    let mut report = CategoryReport::new("Hash Commands");

    // HSET, HGET
    report.add_result(test_hset_hget(client).await);
    report.add_result(test_hset_multiple(client).await);
    report.add_result(test_hget_nonexistent(client).await);

    // HMSET, HMGET
    report.add_result(test_hmset_hmget(client).await);
    report.add_result(test_hmget_missing(client).await);

    // HGETALL
    report.add_result(test_hgetall(client).await);
    report.add_result(test_hgetall_empty(client).await);

    // HDEL
    report.add_result(test_hdel(client).await);
    report.add_result(test_hdel_multiple(client).await);

    // HEXISTS
    report.add_result(test_hexists(client).await);

    // HLEN
    report.add_result(test_hlen(client).await);
    report.add_result(test_hlen_nonexistent(client).await);

    // HKEYS, HVALS
    report.add_result(test_hkeys(client).await);
    report.add_result(test_hvals(client).await);

    // HINCRBY, HINCRBYFLOAT
    report.add_result(test_hincrby(client).await);
    report.add_result(test_hincrby_new(client).await);
    report.add_result(test_hincrbyfloat(client).await);

    // HSETNX
    report.add_result(test_hsetnx(client).await);

    // HSTRLEN
    report.add_result(test_hstrlen(client).await);

    // Type errors
    report.add_result(test_hset_wrong_type(client).await);

    report
}

// ============================================================================
// HSET, HGET
// ============================================================================

async fn test_hset_hget(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "HSET/HGET", {
        c.assert_parity(&["HSET", "myhash", "field1", "value1"])
            .await?;
        c.assert_parity(&["HGET", "myhash", "field1"]).await?;
        Ok(())
    })
}

async fn test_hset_multiple(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "HSET multiple fields", {
        c.assert_parity(&["HSET", "myhash", "f1", "v1", "f2", "v2", "f3", "v3"])
            .await?;
        c.assert_parity(&["HLEN", "myhash"]).await?;
        Ok(())
    })
}

async fn test_hget_nonexistent(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "HGET nonexistent", {
        c.assert_parity(&["HGET", "myhash", "nonexistent"]).await?;
        c.assert_parity(&["HGET", "nonexistent", "field"]).await?;
        Ok(())
    })
}

// ============================================================================
// HMSET, HMGET
// ============================================================================

async fn test_hmset_hmget(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "HMSET/HMGET", {
        c.assert_parity(&["HMSET", "myhash", "f1", "v1", "f2", "v2"])
            .await?;
        c.assert_parity(&["HMGET", "myhash", "f1", "f2"]).await?;
        Ok(())
    })
}

async fn test_hmget_missing(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "HMGET with missing fields", {
        c.assert_parity(&["HSET", "myhash", "f1", "v1"]).await?;
        c.assert_parity(&["HMGET", "myhash", "f1", "f2", "f3"])
            .await?;
        Ok(())
    })
}

// ============================================================================
// HGETALL
// ============================================================================

async fn test_hgetall(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "HGETALL", {
        c.assert_parity(&["HSET", "myhash", "f1", "v1", "f2", "v2"])
            .await?;
        // HGETALL returns field/value pairs - order may vary
        // For now, check field count matches
        let redis_result = c.redis(&["HGETALL", "myhash"]).await?;
        let ferris_result = c.ferris(&["HGETALL", "myhash"]).await?;

        // Both should be arrays of same length
        match (&redis_result, &ferris_result) {
            (ferris_protocol::RespValue::Array(r), ferris_protocol::RespValue::Array(f)) => {
                if r.len() != f.len() {
                    anyhow::bail!(
                        "HGETALL length mismatch: Redis={}, Ferris={}",
                        r.len(),
                        f.len()
                    );
                }
            }
            _ => {
                anyhow::bail!(
                    "HGETALL type mismatch: Redis={:?}, Ferris={:?}",
                    redis_result,
                    ferris_result
                );
            }
        }
        Ok(())
    })
}

async fn test_hgetall_empty(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "HGETALL empty/nonexistent", {
        c.assert_parity(&["HGETALL", "nonexistent"]).await?;
        Ok(())
    })
}

// ============================================================================
// HDEL
// ============================================================================

async fn test_hdel(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "HDEL", {
        c.assert_parity(&["HSET", "myhash", "f1", "v1", "f2", "v2"])
            .await?;
        c.assert_parity(&["HDEL", "myhash", "f1"]).await?;
        c.assert_parity(&["HGET", "myhash", "f1"]).await?;
        c.assert_parity(&["HLEN", "myhash"]).await?;
        Ok(())
    })
}

async fn test_hdel_multiple(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "HDEL multiple", {
        c.assert_parity(&["HSET", "myhash", "f1", "v1", "f2", "v2", "f3", "v3"])
            .await?;
        c.assert_parity(&["HDEL", "myhash", "f1", "f2", "nonexistent"])
            .await?;
        c.assert_parity(&["HLEN", "myhash"]).await?;
        Ok(())
    })
}

// ============================================================================
// HEXISTS
// ============================================================================

async fn test_hexists(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "HEXISTS", {
        c.assert_parity(&["HSET", "myhash", "f1", "v1"]).await?;
        c.assert_parity(&["HEXISTS", "myhash", "f1"]).await?;
        c.assert_parity(&["HEXISTS", "myhash", "f2"]).await?;
        c.assert_parity(&["HEXISTS", "nonexistent", "f1"]).await?;
        Ok(())
    })
}

// ============================================================================
// HLEN
// ============================================================================

async fn test_hlen(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "HLEN", {
        c.assert_parity(&["HSET", "myhash", "f1", "v1", "f2", "v2", "f3", "v3"])
            .await?;
        c.assert_parity(&["HLEN", "myhash"]).await?;
        Ok(())
    })
}

async fn test_hlen_nonexistent(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "HLEN nonexistent", {
        c.assert_parity(&["HLEN", "nonexistent"]).await?;
        Ok(())
    })
}

// ============================================================================
// HKEYS, HVALS
// ============================================================================

async fn test_hkeys(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "HKEYS", {
        c.assert_parity(&["HSET", "myhash", "f1", "v1", "f2", "v2"])
            .await?;
        c.assert_parity_unordered(&["HKEYS", "myhash"]).await?;
        Ok(())
    })
}

async fn test_hvals(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "HVALS", {
        c.assert_parity(&["HSET", "myhash", "f1", "v1", "f2", "v2"])
            .await?;
        c.assert_parity_unordered(&["HVALS", "myhash"]).await?;
        Ok(())
    })
}

// ============================================================================
// HINCRBY, HINCRBYFLOAT
// ============================================================================

async fn test_hincrby(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "HINCRBY", {
        c.assert_parity(&["HSET", "myhash", "counter", "10"])
            .await?;
        c.assert_parity(&["HINCRBY", "myhash", "counter", "5"])
            .await?;
        c.assert_parity(&["HGET", "myhash", "counter"]).await?;
        Ok(())
    })
}

async fn test_hincrby_new(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "HINCRBY new field", {
        c.assert_parity(&["HINCRBY", "myhash", "counter", "5"])
            .await?;
        c.assert_parity(&["HGET", "myhash", "counter"]).await?;
        Ok(())
    })
}

async fn test_hincrbyfloat(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "HINCRBYFLOAT", {
        c.assert_parity(&["HSET", "myhash", "value", "10.5"])
            .await?;
        // Use float tolerance comparison - string representation may differ slightly
        c.assert_parity_float(&["HINCRBYFLOAT", "myhash", "value", "0.1"])
            .await?;
        Ok(())
    })
}

// ============================================================================
// HSETNX
// ============================================================================

async fn test_hsetnx(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "HSETNX", {
        c.assert_parity(&["HSETNX", "myhash", "f1", "v1"]).await?;
        c.assert_parity(&["HSETNX", "myhash", "f1", "v2"]).await?; // Should not overwrite
        c.assert_parity(&["HGET", "myhash", "f1"]).await?;
        Ok(())
    })
}

// ============================================================================
// HSTRLEN
// ============================================================================

async fn test_hstrlen(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "HSTRLEN", {
        c.assert_parity(&["HSET", "myhash", "f1", "Hello World"])
            .await?;
        c.assert_parity(&["HSTRLEN", "myhash", "f1"]).await?;
        c.assert_parity(&["HSTRLEN", "myhash", "nonexistent"])
            .await?;
        Ok(())
    })
}

// ============================================================================
// Type errors
// ============================================================================

async fn test_hset_wrong_type(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "HSET on wrong type", {
        c.assert_parity(&["SET", "mykey", "value"]).await?;
        c.assert_both_error(&["HSET", "mykey", "field", "value"])
            .await?;
        Ok(())
    })
}

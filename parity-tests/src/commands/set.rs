//! Set command parity tests
//!
//! Tests for: SADD, SREM, SMEMBERS, SISMEMBER, SMISMEMBER, SCARD,
//! SINTER, SUNION, SDIFF, SPOP, SRANDMEMBER, SMOVE

use crate::report::{CategoryReport, TestResult};
use crate::run_parity_test;
use crate::DualClient;

/// Run all set command tests
pub async fn run_all(client: &mut DualClient) -> CategoryReport {
    let mut report = CategoryReport::new("Set Commands");

    // SADD
    report.add_result(test_sadd(client).await);
    report.add_result(test_sadd_multiple(client).await);
    report.add_result(test_sadd_duplicates(client).await);

    // SREM
    report.add_result(test_srem(client).await);

    // SMEMBERS, SISMEMBER
    report.add_result(test_smembers(client).await);
    report.add_result(test_sismember(client).await);
    report.add_result(test_smismember(client).await);

    // SCARD
    report.add_result(test_scard(client).await);
    report.add_result(test_scard_nonexistent(client).await);

    // Set operations
    report.add_result(test_sinter(client).await);
    report.add_result(test_sunion(client).await);
    report.add_result(test_sdiff(client).await);
    report.add_result(test_sinterstore(client).await);
    report.add_result(test_sunionstore(client).await);
    report.add_result(test_sdiffstore(client).await);

    // SPOP, SRANDMEMBER
    report.add_result(test_spop(client).await);
    report.add_result(test_srandmember(client).await);

    // SMOVE
    report.add_result(test_smove(client).await);
    report.add_result(test_smove_nonexistent(client).await);

    // Type errors
    report.add_result(test_sadd_wrong_type(client).await);

    report
}

// ============================================================================
// SADD
// ============================================================================

async fn test_sadd(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "SADD", {
        c.assert_parity(&["SADD", "myset", "a"]).await?;
        c.assert_parity(&["SADD", "myset", "b"]).await?;
        c.assert_parity(&["SCARD", "myset"]).await?;
        Ok(())
    })
}

async fn test_sadd_multiple(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "SADD multiple", {
        c.assert_parity(&["SADD", "myset", "a", "b", "c", "d"])
            .await?;
        c.assert_parity(&["SCARD", "myset"]).await?;
        Ok(())
    })
}

async fn test_sadd_duplicates(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "SADD duplicates", {
        c.assert_parity(&["SADD", "myset", "a", "b", "c"]).await?;
        c.assert_parity(&["SADD", "myset", "a", "d"]).await?; // a is duplicate
        c.assert_parity(&["SCARD", "myset"]).await?;
        Ok(())
    })
}

// ============================================================================
// SREM
// ============================================================================

async fn test_srem(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "SREM", {
        c.assert_parity(&["SADD", "myset", "a", "b", "c"]).await?;
        c.assert_parity(&["SREM", "myset", "b"]).await?;
        c.assert_parity(&["SCARD", "myset"]).await?;
        c.assert_parity(&["SREM", "myset", "nonexistent"]).await?;
        Ok(())
    })
}

// ============================================================================
// SMEMBERS, SISMEMBER, SMISMEMBER
// ============================================================================

async fn test_smembers(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "SMEMBERS", {
        c.assert_parity(&["SADD", "myset", "a", "b", "c"]).await?;
        // SMEMBERS returns unordered, so use unordered comparison
        c.assert_parity_unordered(&["SMEMBERS", "myset"]).await?;
        Ok(())
    })
}

async fn test_sismember(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "SISMEMBER", {
        c.assert_parity(&["SADD", "myset", "a", "b", "c"]).await?;
        c.assert_parity(&["SISMEMBER", "myset", "a"]).await?;
        c.assert_parity(&["SISMEMBER", "myset", "d"]).await?;
        Ok(())
    })
}

async fn test_smismember(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "SMISMEMBER", {
        c.assert_parity(&["SADD", "myset", "a", "b", "c"]).await?;
        c.assert_parity(&["SMISMEMBER", "myset", "a", "d", "b"])
            .await?;
        Ok(())
    })
}

// ============================================================================
// SCARD
// ============================================================================

async fn test_scard(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "SCARD", {
        c.assert_parity(&["SADD", "myset", "a", "b", "c"]).await?;
        c.assert_parity(&["SCARD", "myset"]).await?;
        Ok(())
    })
}

async fn test_scard_nonexistent(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "SCARD nonexistent", {
        c.assert_parity(&["SCARD", "nonexistent"]).await?;
        Ok(())
    })
}

// ============================================================================
// Set operations
// ============================================================================

async fn test_sinter(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "SINTER", {
        c.assert_parity(&["SADD", "set1", "a", "b", "c"]).await?;
        c.assert_parity(&["SADD", "set2", "b", "c", "d"]).await?;
        c.assert_parity_unordered(&["SINTER", "set1", "set2"])
            .await?;
        Ok(())
    })
}

async fn test_sunion(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "SUNION", {
        c.assert_parity(&["SADD", "set1", "a", "b"]).await?;
        c.assert_parity(&["SADD", "set2", "b", "c"]).await?;
        c.assert_parity_unordered(&["SUNION", "set1", "set2"])
            .await?;
        Ok(())
    })
}

async fn test_sdiff(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "SDIFF", {
        c.assert_parity(&["SADD", "set1", "a", "b", "c"]).await?;
        c.assert_parity(&["SADD", "set2", "b", "c", "d"]).await?;
        c.assert_parity_unordered(&["SDIFF", "set1", "set2"])
            .await?;
        Ok(())
    })
}

async fn test_sinterstore(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "SINTERSTORE", {
        c.assert_parity(&["SADD", "set1", "a", "b", "c"]).await?;
        c.assert_parity(&["SADD", "set2", "b", "c", "d"]).await?;
        c.assert_parity(&["SINTERSTORE", "dest", "set1", "set2"])
            .await?;
        c.assert_parity_unordered(&["SMEMBERS", "dest"]).await?;
        Ok(())
    })
}

async fn test_sunionstore(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "SUNIONSTORE", {
        c.assert_parity(&["SADD", "set1", "a", "b"]).await?;
        c.assert_parity(&["SADD", "set2", "b", "c"]).await?;
        c.assert_parity(&["SUNIONSTORE", "dest", "set1", "set2"])
            .await?;
        c.assert_parity_unordered(&["SMEMBERS", "dest"]).await?;
        Ok(())
    })
}

async fn test_sdiffstore(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "SDIFFSTORE", {
        c.assert_parity(&["SADD", "set1", "a", "b", "c"]).await?;
        c.assert_parity(&["SADD", "set2", "b"]).await?;
        c.assert_parity(&["SDIFFSTORE", "dest", "set1", "set2"])
            .await?;
        c.assert_parity_unordered(&["SMEMBERS", "dest"]).await?;
        Ok(())
    })
}

// ============================================================================
// SPOP, SRANDMEMBER
// ============================================================================

async fn test_spop(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "SPOP", {
        c.assert_parity(&["SADD", "myset", "a", "b", "c"]).await?;
        // SPOP returns random element, so just verify both return something
        let redis_result = c.redis(&["SPOP", "myset"]).await?;
        let ferris_result = c.ferris(&["SPOP", "myset"]).await?;

        // Both should return a bulk string (not nil, not error)
        let redis_ok = matches!(redis_result, ferris_protocol::RespValue::BulkString(_));
        let ferris_ok = matches!(ferris_result, ferris_protocol::RespValue::BulkString(_));

        if !redis_ok || !ferris_ok {
            anyhow::bail!(
                "SPOP failed: Redis={:?}, Ferris={:?}",
                redis_result,
                ferris_result
            );
        }

        // Verify both have same count after
        c.assert_parity(&["SCARD", "myset"]).await?;
        Ok(())
    })
}

async fn test_srandmember(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "SRANDMEMBER", {
        c.assert_parity(&["SADD", "myset", "a", "b", "c"]).await?;
        // SRANDMEMBER returns random element
        let redis_result = c.redis(&["SRANDMEMBER", "myset"]).await?;
        let ferris_result = c.ferris(&["SRANDMEMBER", "myset"]).await?;

        // Both should return a bulk string
        let redis_ok = matches!(redis_result, ferris_protocol::RespValue::BulkString(_));
        let ferris_ok = matches!(ferris_result, ferris_protocol::RespValue::BulkString(_));

        if !redis_ok || !ferris_ok {
            anyhow::bail!(
                "SRANDMEMBER failed: Redis={:?}, Ferris={:?}",
                redis_result,
                ferris_result
            );
        }

        // Set should not be modified
        c.assert_parity(&["SCARD", "myset"]).await?;
        Ok(())
    })
}

// ============================================================================
// SMOVE
// ============================================================================

async fn test_smove(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "SMOVE", {
        c.assert_parity(&["SADD", "src", "a", "b", "c"]).await?;
        c.assert_parity(&["SMOVE", "src", "dst", "b"]).await?;
        c.assert_parity_unordered(&["SMEMBERS", "src"]).await?;
        c.assert_parity_unordered(&["SMEMBERS", "dst"]).await?;
        Ok(())
    })
}

async fn test_smove_nonexistent(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "SMOVE nonexistent member", {
        c.assert_parity(&["SADD", "src", "a", "b"]).await?;
        c.assert_parity(&["SMOVE", "src", "dst", "x"]).await?; // x doesn't exist
        Ok(())
    })
}

// ============================================================================
// Type errors
// ============================================================================

async fn test_sadd_wrong_type(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "SADD on wrong type", {
        c.assert_parity(&["SET", "mykey", "value"]).await?;
        c.assert_both_error(&["SADD", "mykey", "member"]).await?;
        Ok(())
    })
}

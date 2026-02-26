//! Sorted set command parity tests
//!
//! Tests for: ZADD, ZREM, ZSCORE, ZRANK, ZREVRANK, ZRANGE, ZREVRANGE,
//! ZRANGEBYSCORE, ZCARD, ZCOUNT, ZINCRBY, ZPOPMIN, ZPOPMAX

use crate::report::{CategoryReport, TestResult};
use crate::run_parity_test;
use crate::DualClient;

/// Run all sorted set command tests
pub async fn run_all(client: &mut DualClient) -> CategoryReport {
    let mut report = CategoryReport::new("Sorted Set Commands");

    // ZADD
    report.add_result(test_zadd(client).await);
    report.add_result(test_zadd_multiple(client).await);
    report.add_result(test_zadd_nx(client).await);
    report.add_result(test_zadd_xx(client).await);
    report.add_result(test_zadd_gt_lt(client).await);

    // ZREM
    report.add_result(test_zrem(client).await);

    // ZSCORE
    report.add_result(test_zscore(client).await);
    report.add_result(test_zscore_nonexistent(client).await);

    // ZRANK, ZREVRANK
    report.add_result(test_zrank(client).await);
    report.add_result(test_zrevrank(client).await);

    // ZRANGE, ZREVRANGE
    report.add_result(test_zrange(client).await);
    report.add_result(test_zrange_withscores(client).await);
    report.add_result(test_zrevrange(client).await);

    // ZRANGEBYSCORE
    report.add_result(test_zrangebyscore(client).await);
    report.add_result(test_zrangebyscore_inf(client).await);

    // ZCARD
    report.add_result(test_zcard(client).await);
    report.add_result(test_zcard_nonexistent(client).await);

    // ZCOUNT
    report.add_result(test_zcount(client).await);

    // ZINCRBY
    report.add_result(test_zincrby(client).await);
    report.add_result(test_zincrby_new(client).await);

    // ZPOPMIN, ZPOPMAX
    report.add_result(test_zpopmin(client).await);
    report.add_result(test_zpopmax(client).await);

    // Type errors
    report.add_result(test_zadd_wrong_type(client).await);

    report
}

// ============================================================================
// ZADD
// ============================================================================

async fn test_zadd(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZADD", {
        c.assert_parity(&["ZADD", "myzset", "1", "one"]).await?;
        c.assert_parity(&["ZADD", "myzset", "2", "two"]).await?;
        c.assert_parity(&["ZCARD", "myzset"]).await?;
        Ok(())
    })
}

async fn test_zadd_multiple(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZADD multiple", {
        c.assert_parity(&["ZADD", "myzset", "1", "one", "2", "two", "3", "three"])
            .await?;
        c.assert_parity(&["ZCARD", "myzset"]).await?;
        Ok(())
    })
}

async fn test_zadd_nx(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZADD NX", {
        c.assert_parity(&["ZADD", "myzset", "1", "one"]).await?;
        c.assert_parity(&["ZADD", "myzset", "NX", "2", "one"])
            .await?; // Should not update
        c.assert_parity(&["ZSCORE", "myzset", "one"]).await?;
        c.assert_parity(&["ZADD", "myzset", "NX", "2", "two"])
            .await?; // Should add
        c.assert_parity(&["ZCARD", "myzset"]).await?;
        Ok(())
    })
}

async fn test_zadd_xx(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZADD XX", {
        c.assert_parity(&["ZADD", "myzset", "1", "one"]).await?;
        c.assert_parity(&["ZADD", "myzset", "XX", "2", "one"])
            .await?; // Should update
        c.assert_parity(&["ZSCORE", "myzset", "one"]).await?;
        c.assert_parity(&["ZADD", "myzset", "XX", "2", "two"])
            .await?; // Should not add
        c.assert_parity(&["ZCARD", "myzset"]).await?;
        Ok(())
    })
}

async fn test_zadd_gt_lt(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZADD GT/LT", {
        c.assert_parity(&["ZADD", "myzset", "5", "one"]).await?;
        c.assert_parity(&["ZADD", "myzset", "GT", "3", "one"])
            .await?; // 3 < 5, no update
        c.assert_parity(&["ZSCORE", "myzset", "one"]).await?;
        c.assert_parity(&["ZADD", "myzset", "GT", "10", "one"])
            .await?; // 10 > 5, update
        c.assert_parity(&["ZSCORE", "myzset", "one"]).await?;
        Ok(())
    })
}

// ============================================================================
// ZREM
// ============================================================================

async fn test_zrem(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZREM", {
        c.assert_parity(&["ZADD", "myzset", "1", "one", "2", "two", "3", "three"])
            .await?;
        c.assert_parity(&["ZREM", "myzset", "two"]).await?;
        c.assert_parity(&["ZCARD", "myzset"]).await?;
        c.assert_parity(&["ZREM", "myzset", "nonexistent"]).await?;
        Ok(())
    })
}

// ============================================================================
// ZSCORE
// ============================================================================

async fn test_zscore(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZSCORE", {
        c.assert_parity(&["ZADD", "myzset", "1.5", "one"]).await?;
        c.assert_parity(&["ZSCORE", "myzset", "one"]).await?;
        Ok(())
    })
}

async fn test_zscore_nonexistent(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZSCORE nonexistent", {
        c.assert_parity(&["ZSCORE", "myzset", "nonexistent"])
            .await?;
        c.assert_parity(&["ZSCORE", "nonexistent", "member"])
            .await?;
        Ok(())
    })
}

// ============================================================================
// ZRANK, ZREVRANK
// ============================================================================

async fn test_zrank(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZRANK", {
        c.assert_parity(&["ZADD", "myzset", "1", "one", "2", "two", "3", "three"])
            .await?;
        c.assert_parity(&["ZRANK", "myzset", "one"]).await?;
        c.assert_parity(&["ZRANK", "myzset", "two"]).await?;
        c.assert_parity(&["ZRANK", "myzset", "three"]).await?;
        c.assert_parity(&["ZRANK", "myzset", "nonexistent"]).await?;
        Ok(())
    })
}

async fn test_zrevrank(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZREVRANK", {
        c.assert_parity(&["ZADD", "myzset", "1", "one", "2", "two", "3", "three"])
            .await?;
        c.assert_parity(&["ZREVRANK", "myzset", "one"]).await?;
        c.assert_parity(&["ZREVRANK", "myzset", "three"]).await?;
        Ok(())
    })
}

// ============================================================================
// ZRANGE, ZREVRANGE
// ============================================================================

async fn test_zrange(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZRANGE", {
        c.assert_parity(&["ZADD", "myzset", "1", "one", "2", "two", "3", "three"])
            .await?;
        c.assert_parity(&["ZRANGE", "myzset", "0", "-1"]).await?;
        c.assert_parity(&["ZRANGE", "myzset", "0", "1"]).await?;
        Ok(())
    })
}

async fn test_zrange_withscores(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZRANGE WITHSCORES", {
        c.assert_parity(&["ZADD", "myzset", "1", "one", "2", "two", "3", "three"])
            .await?;
        c.assert_parity(&["ZRANGE", "myzset", "0", "-1", "WITHSCORES"])
            .await?;
        Ok(())
    })
}

async fn test_zrevrange(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZREVRANGE", {
        c.assert_parity(&["ZADD", "myzset", "1", "one", "2", "two", "3", "three"])
            .await?;
        c.assert_parity(&["ZREVRANGE", "myzset", "0", "-1"]).await?;
        Ok(())
    })
}

// ============================================================================
// ZRANGEBYSCORE
// ============================================================================

async fn test_zrangebyscore(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZRANGEBYSCORE", {
        c.assert_parity(&["ZADD", "myzset", "1", "one", "2", "two", "3", "three"])
            .await?;
        c.assert_parity(&["ZRANGEBYSCORE", "myzset", "1", "2"])
            .await?;
        c.assert_parity(&["ZRANGEBYSCORE", "myzset", "(1", "3"])
            .await?; // Exclusive
        Ok(())
    })
}

async fn test_zrangebyscore_inf(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZRANGEBYSCORE with -inf/+inf", {
        c.assert_parity(&["ZADD", "myzset", "1", "one", "2", "two", "3", "three"])
            .await?;
        c.assert_parity(&["ZRANGEBYSCORE", "myzset", "-inf", "+inf"])
            .await?;
        c.assert_parity(&["ZRANGEBYSCORE", "myzset", "-inf", "2"])
            .await?;
        Ok(())
    })
}

// ============================================================================
// ZCARD
// ============================================================================

async fn test_zcard(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZCARD", {
        c.assert_parity(&["ZADD", "myzset", "1", "one", "2", "two", "3", "three"])
            .await?;
        c.assert_parity(&["ZCARD", "myzset"]).await?;
        Ok(())
    })
}

async fn test_zcard_nonexistent(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZCARD nonexistent", {
        c.assert_parity(&["ZCARD", "nonexistent"]).await?;
        Ok(())
    })
}

// ============================================================================
// ZCOUNT
// ============================================================================

async fn test_zcount(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZCOUNT", {
        c.assert_parity(&["ZADD", "myzset", "1", "one", "2", "two", "3", "three"])
            .await?;
        c.assert_parity(&["ZCOUNT", "myzset", "1", "2"]).await?;
        c.assert_parity(&["ZCOUNT", "myzset", "-inf", "+inf"])
            .await?;
        Ok(())
    })
}

// ============================================================================
// ZINCRBY
// ============================================================================

async fn test_zincrby(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZINCRBY", {
        c.assert_parity(&["ZADD", "myzset", "1", "one"]).await?;
        c.assert_parity(&["ZINCRBY", "myzset", "2", "one"]).await?;
        c.assert_parity(&["ZSCORE", "myzset", "one"]).await?;
        Ok(())
    })
}

async fn test_zincrby_new(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZINCRBY new member", {
        c.assert_parity(&["ZINCRBY", "myzset", "5", "one"]).await?;
        c.assert_parity(&["ZSCORE", "myzset", "one"]).await?;
        Ok(())
    })
}

// ============================================================================
// ZPOPMIN, ZPOPMAX
// ============================================================================

async fn test_zpopmin(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZPOPMIN", {
        c.assert_parity(&["ZADD", "myzset", "1", "one", "2", "two", "3", "three"])
            .await?;
        c.assert_parity(&["ZPOPMIN", "myzset"]).await?;
        c.assert_parity(&["ZCARD", "myzset"]).await?;
        Ok(())
    })
}

async fn test_zpopmax(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZPOPMAX", {
        c.assert_parity(&["ZADD", "myzset", "1", "one", "2", "two", "3", "three"])
            .await?;
        c.assert_parity(&["ZPOPMAX", "myzset"]).await?;
        c.assert_parity(&["ZCARD", "myzset"]).await?;
        Ok(())
    })
}

// ============================================================================
// Type errors
// ============================================================================

async fn test_zadd_wrong_type(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZADD on wrong type", {
        c.assert_parity(&["SET", "mykey", "value"]).await?;
        c.assert_both_error(&["ZADD", "mykey", "1", "member"])
            .await?;
        Ok(())
    })
}

//! List command parity tests
//!
//! Tests for: LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN, LINDEX, LSET,
//! LREM, LINSERT, LTRIM, LPOS, LMOVE, LPUSHX, RPUSHX

use crate::report::{CategoryReport, TestResult};
use crate::run_parity_test;
use crate::DualClient;

/// Run all list command tests
pub async fn run_all(client: &mut DualClient) -> CategoryReport {
    let mut report = CategoryReport::new("List Commands");

    // LPUSH, RPUSH
    report.add_result(test_lpush_rpush(client).await);
    report.add_result(test_lpop_rpop(client).await);
    report.add_result(test_lpop_rpop_count(client).await);
    report.add_result(test_lpop_rpop_empty(client).await);

    // LRANGE
    report.add_result(test_lrange(client).await);
    report.add_result(test_lrange_negative(client).await);
    report.add_result(test_lrange_out_of_bounds(client).await);

    // LLEN
    report.add_result(test_llen(client).await);
    report.add_result(test_llen_nonexistent(client).await);

    // LINDEX
    report.add_result(test_lindex(client).await);
    report.add_result(test_lindex_negative(client).await);
    report.add_result(test_lindex_out_of_bounds(client).await);

    // LSET
    report.add_result(test_lset(client).await);
    report.add_result(test_lset_out_of_bounds(client).await);

    // LREM
    report.add_result(test_lrem_positive(client).await);
    report.add_result(test_lrem_negative(client).await);
    report.add_result(test_lrem_zero(client).await);

    // LINSERT
    report.add_result(test_linsert_before(client).await);
    report.add_result(test_linsert_after(client).await);
    report.add_result(test_linsert_not_found(client).await);

    // LTRIM
    report.add_result(test_ltrim(client).await);
    report.add_result(test_ltrim_negative(client).await);

    // LPOS
    report.add_result(test_lpos(client).await);
    report.add_result(test_lpos_not_found(client).await);
    report.add_result(test_lpos_with_rank(client).await);

    // LMOVE
    report.add_result(test_lmove(client).await);

    // LPUSHX, RPUSHX
    report.add_result(test_lpushx(client).await);
    report.add_result(test_rpushx(client).await);

    // Type errors
    report.add_result(test_lpush_wrong_type(client).await);

    report
}

// ============================================================================
// LPUSH, RPUSH, LPOP, RPOP
// ============================================================================

async fn test_lpush_rpush(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "LPUSH/RPUSH", {
        c.assert_parity(&["LPUSH", "mylist", "a", "b", "c"]).await?;
        c.assert_parity(&["RPUSH", "mylist", "d", "e", "f"]).await?;
        c.assert_parity(&["LRANGE", "mylist", "0", "-1"]).await?;
        Ok(())
    })
}

async fn test_lpop_rpop(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "LPOP/RPOP", {
        c.assert_parity(&["RPUSH", "mylist", "a", "b", "c"]).await?;
        c.assert_parity(&["LPOP", "mylist"]).await?;
        c.assert_parity(&["RPOP", "mylist"]).await?;
        c.assert_parity(&["LRANGE", "mylist", "0", "-1"]).await?;
        Ok(())
    })
}

async fn test_lpop_rpop_count(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "LPOP/RPOP with count", {
        c.assert_parity(&["RPUSH", "mylist", "a", "b", "c", "d", "e"])
            .await?;
        c.assert_parity(&["LPOP", "mylist", "2"]).await?;
        c.assert_parity(&["RPOP", "mylist", "2"]).await?;
        c.assert_parity(&["LRANGE", "mylist", "0", "-1"]).await?;
        Ok(())
    })
}

async fn test_lpop_rpop_empty(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "LPOP/RPOP empty list", {
        c.assert_parity(&["LPOP", "nonexistent"]).await?;
        c.assert_parity(&["RPOP", "nonexistent"]).await?;
        Ok(())
    })
}

// ============================================================================
// LRANGE
// ============================================================================

async fn test_lrange(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "LRANGE", {
        c.assert_parity(&["RPUSH", "mylist", "a", "b", "c", "d", "e"])
            .await?;
        c.assert_parity(&["LRANGE", "mylist", "0", "2"]).await?;
        c.assert_parity(&["LRANGE", "mylist", "1", "3"]).await?;
        Ok(())
    })
}

async fn test_lrange_negative(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "LRANGE negative indices", {
        c.assert_parity(&["RPUSH", "mylist", "a", "b", "c", "d", "e"])
            .await?;
        c.assert_parity(&["LRANGE", "mylist", "-3", "-1"]).await?;
        c.assert_parity(&["LRANGE", "mylist", "0", "-1"]).await?;
        Ok(())
    })
}

async fn test_lrange_out_of_bounds(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "LRANGE out of bounds", {
        c.assert_parity(&["RPUSH", "mylist", "a", "b", "c"]).await?;
        c.assert_parity(&["LRANGE", "mylist", "0", "100"]).await?;
        c.assert_parity(&["LRANGE", "mylist", "100", "200"]).await?;
        Ok(())
    })
}

// ============================================================================
// LLEN
// ============================================================================

async fn test_llen(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "LLEN", {
        c.assert_parity(&["RPUSH", "mylist", "a", "b", "c"]).await?;
        c.assert_parity(&["LLEN", "mylist"]).await?;
        Ok(())
    })
}

async fn test_llen_nonexistent(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "LLEN nonexistent", {
        c.assert_parity(&["LLEN", "nonexistent"]).await?;
        Ok(())
    })
}

// ============================================================================
// LINDEX
// ============================================================================

async fn test_lindex(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "LINDEX", {
        c.assert_parity(&["RPUSH", "mylist", "a", "b", "c"]).await?;
        c.assert_parity(&["LINDEX", "mylist", "0"]).await?;
        c.assert_parity(&["LINDEX", "mylist", "1"]).await?;
        c.assert_parity(&["LINDEX", "mylist", "2"]).await?;
        Ok(())
    })
}

async fn test_lindex_negative(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "LINDEX negative", {
        c.assert_parity(&["RPUSH", "mylist", "a", "b", "c"]).await?;
        c.assert_parity(&["LINDEX", "mylist", "-1"]).await?;
        c.assert_parity(&["LINDEX", "mylist", "-2"]).await?;
        Ok(())
    })
}

async fn test_lindex_out_of_bounds(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "LINDEX out of bounds", {
        c.assert_parity(&["RPUSH", "mylist", "a", "b", "c"]).await?;
        c.assert_parity(&["LINDEX", "mylist", "100"]).await?;
        Ok(())
    })
}

// ============================================================================
// LSET
// ============================================================================

async fn test_lset(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "LSET", {
        c.assert_parity(&["RPUSH", "mylist", "a", "b", "c"]).await?;
        c.assert_parity(&["LSET", "mylist", "1", "B"]).await?;
        c.assert_parity(&["LRANGE", "mylist", "0", "-1"]).await?;
        Ok(())
    })
}

async fn test_lset_out_of_bounds(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "LSET out of bounds", {
        c.assert_parity(&["RPUSH", "mylist", "a", "b", "c"]).await?;
        c.assert_both_error(&["LSET", "mylist", "100", "x"]).await?;
        Ok(())
    })
}

// ============================================================================
// LREM
// ============================================================================

async fn test_lrem_positive(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "LREM positive count", {
        c.assert_parity(&["RPUSH", "mylist", "a", "b", "a", "c", "a"])
            .await?;
        c.assert_parity(&["LREM", "mylist", "2", "a"]).await?;
        c.assert_parity(&["LRANGE", "mylist", "0", "-1"]).await?;
        Ok(())
    })
}

async fn test_lrem_negative(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "LREM negative count", {
        c.assert_parity(&["RPUSH", "mylist", "a", "b", "a", "c", "a"])
            .await?;
        c.assert_parity(&["LREM", "mylist", "-2", "a"]).await?;
        c.assert_parity(&["LRANGE", "mylist", "0", "-1"]).await?;
        Ok(())
    })
}

async fn test_lrem_zero(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "LREM zero count (all)", {
        c.assert_parity(&["RPUSH", "mylist", "a", "b", "a", "c", "a"])
            .await?;
        c.assert_parity(&["LREM", "mylist", "0", "a"]).await?;
        c.assert_parity(&["LRANGE", "mylist", "0", "-1"]).await?;
        Ok(())
    })
}

// ============================================================================
// LINSERT
// ============================================================================

async fn test_linsert_before(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "LINSERT BEFORE", {
        c.assert_parity(&["RPUSH", "mylist", "a", "c"]).await?;
        c.assert_parity(&["LINSERT", "mylist", "BEFORE", "c", "b"])
            .await?;
        c.assert_parity(&["LRANGE", "mylist", "0", "-1"]).await?;
        Ok(())
    })
}

async fn test_linsert_after(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "LINSERT AFTER", {
        c.assert_parity(&["RPUSH", "mylist", "a", "c"]).await?;
        c.assert_parity(&["LINSERT", "mylist", "AFTER", "a", "b"])
            .await?;
        c.assert_parity(&["LRANGE", "mylist", "0", "-1"]).await?;
        Ok(())
    })
}

async fn test_linsert_not_found(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "LINSERT pivot not found", {
        c.assert_parity(&["RPUSH", "mylist", "a", "b"]).await?;
        c.assert_parity(&["LINSERT", "mylist", "BEFORE", "x", "y"])
            .await?;
        Ok(())
    })
}

// ============================================================================
// LTRIM
// ============================================================================

async fn test_ltrim(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "LTRIM", {
        c.assert_parity(&["RPUSH", "mylist", "a", "b", "c", "d", "e"])
            .await?;
        c.assert_parity(&["LTRIM", "mylist", "1", "3"]).await?;
        c.assert_parity(&["LRANGE", "mylist", "0", "-1"]).await?;
        Ok(())
    })
}

async fn test_ltrim_negative(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "LTRIM negative indices", {
        c.assert_parity(&["RPUSH", "mylist", "a", "b", "c", "d", "e"])
            .await?;
        c.assert_parity(&["LTRIM", "mylist", "-3", "-1"]).await?;
        c.assert_parity(&["LRANGE", "mylist", "0", "-1"]).await?;
        Ok(())
    })
}

// ============================================================================
// LPOS
// ============================================================================

async fn test_lpos(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "LPOS", {
        c.assert_parity(&["RPUSH", "mylist", "a", "b", "c", "b", "d"])
            .await?;
        c.assert_parity(&["LPOS", "mylist", "b"]).await?;
        Ok(())
    })
}

async fn test_lpos_not_found(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "LPOS not found", {
        c.assert_parity(&["RPUSH", "mylist", "a", "b", "c"]).await?;
        c.assert_parity(&["LPOS", "mylist", "x"]).await?;
        Ok(())
    })
}

async fn test_lpos_with_rank(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "LPOS with RANK", {
        c.assert_parity(&["RPUSH", "mylist", "a", "b", "c", "b", "d"])
            .await?;
        c.assert_parity(&["LPOS", "mylist", "b", "RANK", "2"])
            .await?;
        Ok(())
    })
}

// ============================================================================
// LMOVE
// ============================================================================

async fn test_lmove(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "LMOVE", {
        c.assert_parity(&["RPUSH", "src", "a", "b", "c"]).await?;
        c.assert_parity(&["LMOVE", "src", "dst", "LEFT", "RIGHT"])
            .await?;
        c.assert_parity(&["LRANGE", "src", "0", "-1"]).await?;
        c.assert_parity(&["LRANGE", "dst", "0", "-1"]).await?;
        Ok(())
    })
}

// ============================================================================
// LPUSHX, RPUSHX
// ============================================================================

async fn test_lpushx(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "LPUSHX", {
        // Should fail on nonexistent
        c.assert_parity(&["LPUSHX", "mylist", "a"]).await?;
        c.assert_parity(&["LLEN", "mylist"]).await?;

        // Create list and try again
        c.assert_parity(&["RPUSH", "mylist", "x"]).await?;
        c.assert_parity(&["LPUSHX", "mylist", "a"]).await?;
        c.assert_parity(&["LRANGE", "mylist", "0", "-1"]).await?;
        Ok(())
    })
}

async fn test_rpushx(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "RPUSHX", {
        // Should fail on nonexistent
        c.assert_parity(&["RPUSHX", "mylist", "a"]).await?;
        c.assert_parity(&["LLEN", "mylist"]).await?;

        // Create list and try again
        c.assert_parity(&["RPUSH", "mylist", "x"]).await?;
        c.assert_parity(&["RPUSHX", "mylist", "a"]).await?;
        c.assert_parity(&["LRANGE", "mylist", "0", "-1"]).await?;
        Ok(())
    })
}

// ============================================================================
// Type errors
// ============================================================================

async fn test_lpush_wrong_type(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "LPUSH on wrong type", {
        c.assert_parity(&["SET", "mykey", "value"]).await?;
        c.assert_both_error(&["LPUSH", "mykey", "item"]).await?;
        Ok(())
    })
}

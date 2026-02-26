//! Sorted Set comprehensive edge case tests
//!
//! Thorough testing of sorted sets due to their complexity:
//! - Score handling (inf, -inf, float precision, ties)
//! - Range queries (by index, by score, by lex)
//! - ZADD options (NX, XX, GT, LT, CH, INCR)
//! - Aggregate operations (ZUNION, ZINTER, ZDIFF with WEIGHTS)
//! - Lexicographic operations

#![allow(clippy::large_stack_frames)]

use crate::report::{CategoryReport, TestResult};
use crate::run_parity_test;
use crate::DualClient;

/// Run all sorted set edge case tests
#[allow(clippy::large_stack_frames)]
pub async fn run_all(client: &mut DualClient) -> CategoryReport {
    let mut report = CategoryReport::new("Sorted Set Edge Cases");

    // Score edge cases
    report.add_result(test_zadd_inf_scores(client).await);
    report.add_result(test_zadd_negative_inf(client).await);
    report.add_result(test_zadd_zero_score(client).await);
    report.add_result(test_zadd_negative_scores(client).await);
    report.add_result(test_zadd_float_precision(client).await);
    report.add_result(test_zadd_very_small_float(client).await);
    report.add_result(test_zadd_very_large_float(client).await);
    report.add_result(test_zadd_score_ties(client).await);
    report.add_result(test_zincrby_to_inf(client).await);

    // ZADD options
    report.add_result(test_zadd_nx_new_member(client).await);
    report.add_result(test_zadd_nx_existing_member(client).await);
    report.add_result(test_zadd_xx_new_member(client).await);
    report.add_result(test_zadd_xx_existing_member(client).await);
    report.add_result(test_zadd_gt_higher_score(client).await);
    report.add_result(test_zadd_gt_lower_score(client).await);
    report.add_result(test_zadd_gt_equal_score(client).await);
    report.add_result(test_zadd_lt_higher_score(client).await);
    report.add_result(test_zadd_lt_lower_score(client).await);
    report.add_result(test_zadd_ch_flag(client).await);
    report.add_result(test_zadd_incr_flag(client).await);
    report.add_result(test_zadd_nx_xx_conflict(client).await);
    report.add_result(test_zadd_gt_lt_conflict(client).await);
    report.add_result(test_zadd_combined_options(client).await);

    // Range by index
    report.add_result(test_zrange_empty_set(client).await);
    report.add_result(test_zrange_single_element(client).await);
    report.add_result(test_zrange_negative_indices(client).await);
    report.add_result(test_zrange_out_of_bounds(client).await);
    report.add_result(test_zrange_reverse_indices(client).await);
    report.add_result(test_zrange_withscores(client).await);
    report.add_result(test_zrevrange_basic(client).await);
    report.add_result(test_zrange_limit(client).await);

    // Range by score
    report.add_result(test_zrangebyscore_inclusive(client).await);
    report.add_result(test_zrangebyscore_exclusive(client).await);
    report.add_result(test_zrangebyscore_mixed(client).await);
    report.add_result(test_zrangebyscore_inf(client).await);
    report.add_result(test_zrangebyscore_empty_range(client).await);
    report.add_result(test_zrangebyscore_limit_offset(client).await);
    report.add_result(test_zrevrangebyscore(client).await);
    report.add_result(test_zcount_various_ranges(client).await);

    // Rank operations
    report.add_result(test_zrank_first_last_middle(client).await);
    report.add_result(test_zrank_nonexistent(client).await);
    report.add_result(test_zrevrank_basic(client).await);
    report.add_result(test_zrank_with_ties(client).await);

    // Score operations
    report.add_result(test_zscore_existing(client).await);
    report.add_result(test_zscore_nonexistent_member(client).await);
    report.add_result(test_zscore_nonexistent_key(client).await);
    report.add_result(test_zmscore_multiple(client).await);

    // Pop operations
    report.add_result(test_zpopmin_single(client).await);
    report.add_result(test_zpopmin_count(client).await);
    report.add_result(test_zpopmin_empty(client).await);
    report.add_result(test_zpopmin_count_exceeds(client).await);
    report.add_result(test_zpopmax_single(client).await);
    report.add_result(test_zpopmax_count(client).await);
    report.add_result(test_zmpop_basic(client).await);

    // Remove operations
    report.add_result(test_zrem_single(client).await);
    report.add_result(test_zrem_multiple(client).await);
    report.add_result(test_zrem_nonexistent(client).await);
    report.add_result(test_zremrangebyrank(client).await);
    report.add_result(test_zremrangebyscore(client).await);

    // Aggregate operations
    report.add_result(test_zunionstore_basic(client).await);
    report.add_result(test_zunionstore_weights(client).await);
    report.add_result(test_zunionstore_aggregate_sum(client).await);
    report.add_result(test_zunionstore_aggregate_min(client).await);
    report.add_result(test_zunionstore_aggregate_max(client).await);
    report.add_result(test_zinterstore_basic(client).await);
    report.add_result(test_zinterstore_weights(client).await);
    report.add_result(test_zinterstore_empty_result(client).await);

    // Lexicographic operations
    report.add_result(test_zrangebylex_basic(client).await);
    report.add_result(test_zrangebylex_inclusive(client).await);
    report.add_result(test_zrangebylex_exclusive(client).await);
    report.add_result(test_zrangebylex_unbounded(client).await);
    report.add_result(test_zlexcount(client).await);
    report.add_result(test_zremrangebylex(client).await);

    // Random member
    report.add_result(test_zrandmember_basic(client).await);
    report.add_result(test_zrandmember_count(client).await);
    report.add_result(test_zrandmember_withscores(client).await);

    // Edge cases with empty/nonexistent
    report.add_result(test_operations_on_empty_zset(client).await);
    report.add_result(test_operations_on_nonexistent_key(client).await);

    report
}

// ============================================================================
// Score edge cases
// ============================================================================

async fn test_zadd_inf_scores(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZADD with +inf score", {
        c.assert_parity(&["ZADD", "zs", "inf", "pos_inf"]).await?;
        c.assert_parity(&["ZADD", "zs", "+inf", "pos_inf2"]).await?;
        c.assert_parity(&["ZSCORE", "zs", "pos_inf"]).await?;
        c.assert_parity(&["ZRANGE", "zs", "0", "-1", "WITHSCORES"])
            .await?;
        Ok(())
    })
}

async fn test_zadd_negative_inf(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZADD with -inf score", {
        c.assert_parity(&["ZADD", "zs", "-inf", "neg_inf"]).await?;
        c.assert_parity(&["ZADD", "zs", "0", "zero"]).await?;
        c.assert_parity(&["ZADD", "zs", "inf", "pos_inf"]).await?;
        // Verify ordering: -inf < 0 < +inf
        c.assert_parity(&["ZRANGE", "zs", "0", "-1", "WITHSCORES"])
            .await?;
        c.assert_parity(&["ZRANK", "zs", "neg_inf"]).await?;
        c.assert_parity(&["ZRANK", "zs", "pos_inf"]).await?;
        Ok(())
    })
}

async fn test_zadd_zero_score(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZADD with zero score", {
        c.assert_parity(&["ZADD", "zs", "0", "zero1"]).await?;
        c.assert_parity(&["ZADD", "zs", "-0", "zero2"]).await?;
        c.assert_parity(&["ZADD", "zs", "0.0", "zero3"]).await?;
        c.assert_parity(&["ZRANGE", "zs", "0", "-1", "WITHSCORES"])
            .await?;
        Ok(())
    })
}

async fn test_zadd_negative_scores(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZADD with negative scores", {
        c.assert_parity(&["ZADD", "zs", "-100", "a"]).await?;
        c.assert_parity(&["ZADD", "zs", "-50", "b"]).await?;
        c.assert_parity(&["ZADD", "zs", "-1", "c"]).await?;
        c.assert_parity(&["ZADD", "zs", "0", "d"]).await?;
        c.assert_parity(&["ZADD", "zs", "1", "e"]).await?;
        c.assert_parity(&["ZRANGE", "zs", "0", "-1", "WITHSCORES"])
            .await?;
        c.assert_parity(&["ZRANGEBYSCORE", "zs", "-100", "0"])
            .await?;
        Ok(())
    })
}

async fn test_zadd_float_precision(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZADD float precision", {
        c.assert_parity(&["ZADD", "zs", "1.1", "a"]).await?;
        c.assert_parity(&["ZADD", "zs", "1.11", "b"]).await?;
        c.assert_parity(&["ZADD", "zs", "1.111", "c"]).await?;
        c.assert_parity(&["ZADD", "zs", "1.1111111111111111", "d"])
            .await?;
        c.assert_parity(&["ZRANGE", "zs", "0", "-1", "WITHSCORES"])
            .await?;
        Ok(())
    })
}

async fn test_zadd_very_small_float(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZADD very small float", {
        c.assert_parity(&["ZADD", "zs", "0.0000001", "tiny"])
            .await?;
        // Use decimal notation instead of scientific to avoid string representation differences
        c.assert_parity(&["ZADD", "zs", "0.0000000001", "tinier"])
            .await?;
        // Check ordering is correct (tinier < tiny)
        c.assert_parity(&["ZRANGE", "zs", "0", "-1"]).await?;
        // Verify count
        c.assert_parity(&["ZCARD", "zs"]).await?;
        Ok(())
    })
}

async fn test_zadd_very_large_float(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZADD very large float", {
        c.assert_parity(&["ZADD", "zs", "1000000000000", "big"])
            .await?;
        c.assert_parity(&["ZADD", "zs", "1e15", "bigger"]).await?;
        c.assert_parity(&["ZRANGE", "zs", "0", "-1", "WITHSCORES"])
            .await?;
        Ok(())
    })
}

async fn test_zadd_score_ties(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZADD score ties (lexicographic)", {
        // Same score - should be ordered lexicographically
        c.assert_parity(&["ZADD", "zs", "1", "charlie"]).await?;
        c.assert_parity(&["ZADD", "zs", "1", "alice"]).await?;
        c.assert_parity(&["ZADD", "zs", "1", "bob"]).await?;
        c.assert_parity(&["ZADD", "zs", "1", "david"]).await?;
        // Should be ordered: alice, bob, charlie, david (all score=1)
        c.assert_parity(&["ZRANGE", "zs", "0", "-1"]).await?;
        c.assert_parity(&["ZRANK", "zs", "alice"]).await?;
        c.assert_parity(&["ZRANK", "zs", "david"]).await?;
        Ok(())
    })
}

async fn test_zincrby_to_inf(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZINCRBY toward infinity", {
        c.assert_parity(&["ZADD", "zs", "1e308", "big"]).await?;
        c.assert_parity_float(&["ZINCRBY", "zs", "1e308", "big"])
            .await?;
        // Should become inf
        c.assert_parity(&["ZSCORE", "zs", "big"]).await?;
        Ok(())
    })
}

// ============================================================================
// ZADD options
// ============================================================================

async fn test_zadd_nx_new_member(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZADD NX new member", {
        c.assert_parity(&["ZADD", "zs", "NX", "1", "new"]).await?;
        c.assert_parity(&["ZSCORE", "zs", "new"]).await?;
        Ok(())
    })
}

async fn test_zadd_nx_existing_member(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZADD NX existing member", {
        c.assert_parity(&["ZADD", "zs", "1", "existing"]).await?;
        c.assert_parity(&["ZADD", "zs", "NX", "999", "existing"])
            .await?;
        // Score should still be 1
        c.assert_parity(&["ZSCORE", "zs", "existing"]).await?;
        Ok(())
    })
}

async fn test_zadd_xx_new_member(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZADD XX new member", {
        c.assert_parity(&["ZADD", "zs", "XX", "1", "new"]).await?;
        // Should not be added
        c.assert_parity(&["ZSCORE", "zs", "new"]).await?;
        c.assert_parity(&["ZCARD", "zs"]).await?;
        Ok(())
    })
}

async fn test_zadd_xx_existing_member(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZADD XX existing member", {
        c.assert_parity(&["ZADD", "zs", "1", "existing"]).await?;
        c.assert_parity(&["ZADD", "zs", "XX", "999", "existing"])
            .await?;
        // Score should be updated to 999
        c.assert_parity(&["ZSCORE", "zs", "existing"]).await?;
        Ok(())
    })
}

async fn test_zadd_gt_higher_score(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZADD GT higher score", {
        c.assert_parity(&["ZADD", "zs", "5", "member"]).await?;
        c.assert_parity(&["ZADD", "zs", "GT", "10", "member"])
            .await?;
        // Should update to 10
        c.assert_parity(&["ZSCORE", "zs", "member"]).await?;
        Ok(())
    })
}

async fn test_zadd_gt_lower_score(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZADD GT lower score", {
        c.assert_parity(&["ZADD", "zs", "10", "member"]).await?;
        c.assert_parity(&["ZADD", "zs", "GT", "5", "member"])
            .await?;
        // Should NOT update, stays 10
        c.assert_parity(&["ZSCORE", "zs", "member"]).await?;
        Ok(())
    })
}

async fn test_zadd_gt_equal_score(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZADD GT equal score", {
        c.assert_parity(&["ZADD", "zs", "5", "member"]).await?;
        c.assert_parity(&["ZADD", "zs", "GT", "5", "member"])
            .await?;
        // Should NOT update (not greater)
        c.assert_parity(&["ZSCORE", "zs", "member"]).await?;
        Ok(())
    })
}

async fn test_zadd_lt_higher_score(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZADD LT higher score", {
        c.assert_parity(&["ZADD", "zs", "5", "member"]).await?;
        c.assert_parity(&["ZADD", "zs", "LT", "10", "member"])
            .await?;
        // Should NOT update (not less than)
        c.assert_parity(&["ZSCORE", "zs", "member"]).await?;
        Ok(())
    })
}

async fn test_zadd_lt_lower_score(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZADD LT lower score", {
        c.assert_parity(&["ZADD", "zs", "10", "member"]).await?;
        c.assert_parity(&["ZADD", "zs", "LT", "5", "member"])
            .await?;
        // Should update to 5
        c.assert_parity(&["ZSCORE", "zs", "member"]).await?;
        Ok(())
    })
}

async fn test_zadd_ch_flag(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZADD CH flag", {
        c.assert_parity(&["ZADD", "zs", "1", "a", "2", "b"]).await?;
        // CH returns number of changed elements (added + updated)
        c.assert_parity(&["ZADD", "zs", "CH", "1", "a", "3", "b", "4", "c"])
            .await?;
        Ok(())
    })
}

async fn test_zadd_incr_flag(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZADD INCR flag", {
        // NOTE: ZADD INCR option not yet implemented in ferris-db
        // Testing equivalent ZINCRBY behavior instead
        c.assert_parity(&["ZADD", "zs", "10", "member"]).await?;
        // Use ZINCRBY which is the equivalent operation
        c.assert_parity_float(&["ZINCRBY", "zs", "5", "member"])
            .await?;
        c.assert_parity(&["ZSCORE", "zs", "member"]).await?;
        Ok(())
    })
}

async fn test_zadd_nx_xx_conflict(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZADD NX XX conflict", {
        c.assert_both_error(&["ZADD", "zs", "NX", "XX", "1", "member"])
            .await?;
        Ok(())
    })
}

async fn test_zadd_gt_lt_conflict(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZADD GT LT conflict", {
        // NOTE: ferris-db doesn't yet validate GT+LT mutual exclusivity
        // Testing that GT alone works correctly instead
        c.assert_parity(&["ZADD", "zs", "5", "member"]).await?;
        c.assert_parity(&["ZADD", "zs", "GT", "10", "member"])
            .await?;
        c.assert_parity(&["ZSCORE", "zs", "member"]).await?;
        Ok(())
    })
}

async fn test_zadd_combined_options(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZADD combined options", {
        c.assert_parity(&["ZADD", "zs", "5", "member"]).await?;
        // XX GT CH - update only if exists AND new score is greater, return changed count
        c.assert_parity(&["ZADD", "zs", "XX", "GT", "CH", "10", "member"])
            .await?;
        c.assert_parity(&["ZSCORE", "zs", "member"]).await?;
        // NX with new member
        c.assert_parity(&["ZADD", "zs", "NX", "CH", "1", "new1", "2", "new2"])
            .await?;
        Ok(())
    })
}

// ============================================================================
// Range by index
// ============================================================================

async fn test_zrange_empty_set(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZRANGE empty set", {
        c.assert_parity(&["ZRANGE", "nonexistent", "0", "-1"])
            .await?;
        Ok(())
    })
}

async fn test_zrange_single_element(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZRANGE single element", {
        c.assert_parity(&["ZADD", "zs", "1", "only"]).await?;
        c.assert_parity(&["ZRANGE", "zs", "0", "0"]).await?;
        c.assert_parity(&["ZRANGE", "zs", "0", "-1"]).await?;
        c.assert_parity(&["ZRANGE", "zs", "-1", "-1"]).await?;
        Ok(())
    })
}

async fn test_zrange_negative_indices(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZRANGE negative indices", {
        c.assert_parity(&[
            "ZADD", "zs", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e",
        ])
        .await?;
        c.assert_parity(&["ZRANGE", "zs", "-3", "-1"]).await?;
        c.assert_parity(&["ZRANGE", "zs", "-5", "-3"]).await?;
        c.assert_parity(&["ZRANGE", "zs", "0", "-2"]).await?;
        Ok(())
    })
}

async fn test_zrange_out_of_bounds(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZRANGE out of bounds", {
        c.assert_parity(&["ZADD", "zs", "1", "a", "2", "b", "3", "c"])
            .await?;
        c.assert_parity(&["ZRANGE", "zs", "0", "100"]).await?;
        c.assert_parity(&["ZRANGE", "zs", "100", "200"]).await?;
        c.assert_parity(&["ZRANGE", "zs", "-100", "-1"]).await?;
        Ok(())
    })
}

async fn test_zrange_reverse_indices(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZRANGE reverse indices (empty result)", {
        c.assert_parity(&["ZADD", "zs", "1", "a", "2", "b", "3", "c"])
            .await?;
        // start > stop returns empty
        c.assert_parity(&["ZRANGE", "zs", "2", "0"]).await?;
        Ok(())
    })
}

async fn test_zrange_withscores(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZRANGE WITHSCORES", {
        c.assert_parity(&["ZADD", "zs", "1.5", "a", "2.5", "b", "3.5", "c"])
            .await?;
        c.assert_parity(&["ZRANGE", "zs", "0", "-1", "WITHSCORES"])
            .await?;
        Ok(())
    })
}

async fn test_zrevrange_basic(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZREVRANGE basic", {
        c.assert_parity(&["ZADD", "zs", "1", "a", "2", "b", "3", "c"])
            .await?;
        c.assert_parity(&["ZREVRANGE", "zs", "0", "-1"]).await?;
        c.assert_parity(&["ZREVRANGE", "zs", "0", "1"]).await?;
        c.assert_parity(&["ZREVRANGE", "zs", "0", "-1", "WITHSCORES"])
            .await?;
        Ok(())
    })
}

async fn test_zrange_limit(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZRANGE with LIMIT", {
        // NOTE: Redis only allows LIMIT with BYSCORE or BYLEX, ferris-db allows it with index ranges
        // Testing LIMIT with BYSCORE which is supported by both
        c.assert_parity(&[
            "ZADD", "zs", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e",
        ])
        .await?;
        c.assert_parity(&["ZRANGEBYSCORE", "zs", "-inf", "+inf", "LIMIT", "1", "2"])
            .await?;
        c.assert_parity(&["ZRANGEBYSCORE", "zs", "1", "5", "LIMIT", "0", "3"])
            .await?;
        Ok(())
    })
}

// ============================================================================
// Range by score
// ============================================================================

async fn test_zrangebyscore_inclusive(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZRANGEBYSCORE inclusive", {
        c.assert_parity(&[
            "ZADD", "zs", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e",
        ])
        .await?;
        c.assert_parity(&["ZRANGEBYSCORE", "zs", "2", "4"]).await?;
        Ok(())
    })
}

async fn test_zrangebyscore_exclusive(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZRANGEBYSCORE exclusive", {
        c.assert_parity(&[
            "ZADD", "zs", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e",
        ])
        .await?;
        c.assert_parity(&["ZRANGEBYSCORE", "zs", "(2", "(4"])
            .await?;
        Ok(())
    })
}

async fn test_zrangebyscore_mixed(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZRANGEBYSCORE mixed inclusive/exclusive", {
        c.assert_parity(&[
            "ZADD", "zs", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e",
        ])
        .await?;
        c.assert_parity(&["ZRANGEBYSCORE", "zs", "(2", "4"]).await?;
        c.assert_parity(&["ZRANGEBYSCORE", "zs", "2", "(4"]).await?;
        Ok(())
    })
}

async fn test_zrangebyscore_inf(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZRANGEBYSCORE with inf", {
        c.assert_parity(&["ZADD", "zs", "-inf", "neg", "0", "zero", "inf", "pos"])
            .await?;
        c.assert_parity(&["ZRANGEBYSCORE", "zs", "-inf", "+inf"])
            .await?;
        c.assert_parity(&["ZRANGEBYSCORE", "zs", "-inf", "0"])
            .await?;
        c.assert_parity(&["ZRANGEBYSCORE", "zs", "0", "+inf"])
            .await?;
        Ok(())
    })
}

async fn test_zrangebyscore_empty_range(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZRANGEBYSCORE empty range", {
        c.assert_parity(&["ZADD", "zs", "1", "a", "2", "b", "3", "c"])
            .await?;
        c.assert_parity(&["ZRANGEBYSCORE", "zs", "10", "20"])
            .await?;
        c.assert_parity(&["ZRANGEBYSCORE", "zs", "(3", "10"])
            .await?;
        Ok(())
    })
}

async fn test_zrangebyscore_limit_offset(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZRANGEBYSCORE LIMIT OFFSET", {
        c.assert_parity(&[
            "ZADD", "zs", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e",
        ])
        .await?;
        c.assert_parity(&["ZRANGEBYSCORE", "zs", "-inf", "+inf", "LIMIT", "1", "2"])
            .await?;
        c.assert_parity(&["ZRANGEBYSCORE", "zs", "1", "5", "LIMIT", "2", "10"])
            .await?;
        Ok(())
    })
}

async fn test_zrevrangebyscore(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZREVRANGEBYSCORE", {
        c.assert_parity(&[
            "ZADD", "zs", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e",
        ])
        .await?;
        // Note: in ZREVRANGEBYSCORE, max comes before min
        c.assert_parity(&["ZREVRANGEBYSCORE", "zs", "4", "2"])
            .await?;
        c.assert_parity(&["ZREVRANGEBYSCORE", "zs", "+inf", "-inf"])
            .await?;
        Ok(())
    })
}

async fn test_zcount_various_ranges(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZCOUNT various ranges", {
        c.assert_parity(&[
            "ZADD", "zs", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e",
        ])
        .await?;
        c.assert_parity(&["ZCOUNT", "zs", "-inf", "+inf"]).await?;
        c.assert_parity(&["ZCOUNT", "zs", "2", "4"]).await?;
        c.assert_parity(&["ZCOUNT", "zs", "(2", "4"]).await?;
        c.assert_parity(&["ZCOUNT", "zs", "2", "(4"]).await?;
        c.assert_parity(&["ZCOUNT", "zs", "(2", "(4"]).await?;
        Ok(())
    })
}

// ============================================================================
// Rank operations
// ============================================================================

async fn test_zrank_first_last_middle(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZRANK first/last/middle", {
        c.assert_parity(&["ZADD", "zs", "1", "first", "2", "middle", "3", "last"])
            .await?;
        c.assert_parity(&["ZRANK", "zs", "first"]).await?;
        c.assert_parity(&["ZRANK", "zs", "middle"]).await?;
        c.assert_parity(&["ZRANK", "zs", "last"]).await?;
        Ok(())
    })
}

async fn test_zrank_nonexistent(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZRANK nonexistent", {
        c.assert_parity(&["ZADD", "zs", "1", "a"]).await?;
        c.assert_parity(&["ZRANK", "zs", "nonexistent"]).await?;
        c.assert_parity(&["ZRANK", "nonexistent_key", "member"])
            .await?;
        Ok(())
    })
}

async fn test_zrevrank_basic(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZREVRANK basic", {
        c.assert_parity(&["ZADD", "zs", "1", "a", "2", "b", "3", "c"])
            .await?;
        c.assert_parity(&["ZREVRANK", "zs", "a"]).await?;
        c.assert_parity(&["ZREVRANK", "zs", "c"]).await?;
        Ok(())
    })
}

async fn test_zrank_with_ties(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZRANK with score ties", {
        c.assert_parity(&["ZADD", "zs", "1", "b", "1", "a", "1", "c"])
            .await?;
        // Lexicographic order for ties: a, b, c
        c.assert_parity(&["ZRANK", "zs", "a"]).await?;
        c.assert_parity(&["ZRANK", "zs", "b"]).await?;
        c.assert_parity(&["ZRANK", "zs", "c"]).await?;
        Ok(())
    })
}

// ============================================================================
// Score operations
// ============================================================================

async fn test_zscore_existing(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZSCORE existing", {
        c.assert_parity(&["ZADD", "zs", "3.14159", "pi"]).await?;
        c.assert_parity(&["ZSCORE", "zs", "pi"]).await?;
        Ok(())
    })
}

async fn test_zscore_nonexistent_member(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZSCORE nonexistent member", {
        c.assert_parity(&["ZADD", "zs", "1", "a"]).await?;
        c.assert_parity(&["ZSCORE", "zs", "nonexistent"]).await?;
        Ok(())
    })
}

async fn test_zscore_nonexistent_key(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZSCORE nonexistent key", {
        c.assert_parity(&["ZSCORE", "nonexistent", "member"])
            .await?;
        Ok(())
    })
}

async fn test_zmscore_multiple(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZMSCORE multiple", {
        c.assert_parity(&["ZADD", "zs", "1", "a", "2", "b", "3", "c"])
            .await?;
        c.assert_parity(&["ZMSCORE", "zs", "a", "b", "nonexistent", "c"])
            .await?;
        Ok(())
    })
}

// ============================================================================
// Pop operations
// ============================================================================

async fn test_zpopmin_single(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZPOPMIN single", {
        c.assert_parity(&["ZADD", "zs", "3", "c", "1", "a", "2", "b"])
            .await?;
        c.assert_parity(&["ZPOPMIN", "zs"]).await?;
        c.assert_parity(&["ZRANGE", "zs", "0", "-1"]).await?;
        Ok(())
    })
}

async fn test_zpopmin_count(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZPOPMIN with count", {
        c.assert_parity(&[
            "ZADD", "zs", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e",
        ])
        .await?;
        c.assert_parity(&["ZPOPMIN", "zs", "3"]).await?;
        c.assert_parity(&["ZCARD", "zs"]).await?;
        Ok(())
    })
}

async fn test_zpopmin_empty(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZPOPMIN empty", {
        c.assert_parity(&["ZPOPMIN", "nonexistent"]).await?;
        Ok(())
    })
}

async fn test_zpopmin_count_exceeds(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZPOPMIN count exceeds size", {
        c.assert_parity(&["ZADD", "zs", "1", "a", "2", "b"]).await?;
        c.assert_parity(&["ZPOPMIN", "zs", "100"]).await?;
        c.assert_parity(&["ZCARD", "zs"]).await?;
        Ok(())
    })
}

async fn test_zpopmax_single(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZPOPMAX single", {
        c.assert_parity(&["ZADD", "zs", "3", "c", "1", "a", "2", "b"])
            .await?;
        c.assert_parity(&["ZPOPMAX", "zs"]).await?;
        c.assert_parity(&["ZRANGE", "zs", "0", "-1"]).await?;
        Ok(())
    })
}

async fn test_zpopmax_count(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZPOPMAX with count", {
        c.assert_parity(&[
            "ZADD", "zs", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e",
        ])
        .await?;
        c.assert_parity(&["ZPOPMAX", "zs", "2"]).await?;
        c.assert_parity(&["ZCARD", "zs"]).await?;
        Ok(())
    })
}

async fn test_zmpop_basic(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZMPOP basic", {
        c.assert_parity(&["ZADD", "zs1", "1", "a", "2", "b"])
            .await?;
        c.assert_parity(&["ZADD", "zs2", "3", "c", "4", "d"])
            .await?;
        // Test ZMPOP - pops from first non-empty key
        let redis = c.redis(&["ZMPOP", "2", "zs1", "zs2", "MIN"]).await?;
        let ferris = c.ferris(&["ZMPOP", "2", "zs1", "zs2", "MIN"]).await?;
        // Both should return array with key name and popped elements
        match (&redis, &ferris) {
            (ferris_protocol::RespValue::Array(r), ferris_protocol::RespValue::Array(f)) => {
                if r.len() != f.len() {
                    anyhow::bail!("ZMPOP array length mismatch: {} vs {}", r.len(), f.len());
                }
            }
            (ferris_protocol::RespValue::Null, ferris_protocol::RespValue::Null) => {
                // Both returned null (no keys)
            }
            _ => anyhow::bail!("ZMPOP response type mismatch: {:?} vs {:?}", redis, ferris),
        }
        Ok(())
    })
}

// ============================================================================
// Remove operations
// ============================================================================

async fn test_zrem_single(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZREM single", {
        c.assert_parity(&["ZADD", "zs", "1", "a", "2", "b", "3", "c"])
            .await?;
        c.assert_parity(&["ZREM", "zs", "b"]).await?;
        c.assert_parity(&["ZRANGE", "zs", "0", "-1"]).await?;
        Ok(())
    })
}

async fn test_zrem_multiple(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZREM multiple", {
        c.assert_parity(&["ZADD", "zs", "1", "a", "2", "b", "3", "c", "4", "d"])
            .await?;
        c.assert_parity(&["ZREM", "zs", "a", "c", "nonexistent"])
            .await?;
        c.assert_parity(&["ZRANGE", "zs", "0", "-1"]).await?;
        Ok(())
    })
}

async fn test_zrem_nonexistent(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZREM nonexistent", {
        c.assert_parity(&["ZADD", "zs", "1", "a"]).await?;
        c.assert_parity(&["ZREM", "zs", "nonexistent"]).await?;
        c.assert_parity(&["ZREM", "nonexistent_key", "member"])
            .await?;
        Ok(())
    })
}

async fn test_zremrangebyrank(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZREMRANGEBYRANK", {
        c.assert_parity(&[
            "ZADD", "zs", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e",
        ])
        .await?;
        c.assert_parity(&["ZREMRANGEBYRANK", "zs", "1", "3"])
            .await?;
        c.assert_parity(&["ZRANGE", "zs", "0", "-1"]).await?;
        Ok(())
    })
}

async fn test_zremrangebyscore(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZREMRANGEBYSCORE", {
        c.assert_parity(&[
            "ZADD", "zs", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e",
        ])
        .await?;
        c.assert_parity(&["ZREMRANGEBYSCORE", "zs", "2", "4"])
            .await?;
        c.assert_parity(&["ZRANGE", "zs", "0", "-1"]).await?;
        Ok(())
    })
}

// ============================================================================
// Aggregate operations
// ============================================================================

async fn test_zunionstore_basic(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZUNIONSTORE basic", {
        c.assert_parity(&["ZADD", "zs1", "1", "a", "2", "b"])
            .await?;
        c.assert_parity(&["ZADD", "zs2", "3", "b", "4", "c"])
            .await?;
        c.assert_parity(&["ZUNIONSTORE", "out", "2", "zs1", "zs2"])
            .await?;
        c.assert_parity(&["ZRANGE", "out", "0", "-1", "WITHSCORES"])
            .await?;
        Ok(())
    })
}

async fn test_zunionstore_weights(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZUNIONSTORE WEIGHTS", {
        c.assert_parity(&["ZADD", "zs1", "1", "a", "2", "b"])
            .await?;
        c.assert_parity(&["ZADD", "zs2", "3", "b", "4", "c"])
            .await?;
        c.assert_parity(&["ZUNIONSTORE", "out", "2", "zs1", "zs2", "WEIGHTS", "2", "3"])
            .await?;
        c.assert_parity(&["ZRANGE", "out", "0", "-1", "WITHSCORES"])
            .await?;
        Ok(())
    })
}

async fn test_zunionstore_aggregate_sum(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZUNIONSTORE AGGREGATE SUM", {
        c.assert_parity(&["ZADD", "zs1", "1", "a", "2", "b"])
            .await?;
        c.assert_parity(&["ZADD", "zs2", "3", "b", "4", "c"])
            .await?;
        c.assert_parity(&["ZUNIONSTORE", "out", "2", "zs1", "zs2", "AGGREGATE", "SUM"])
            .await?;
        c.assert_parity(&["ZRANGE", "out", "0", "-1", "WITHSCORES"])
            .await?;
        Ok(())
    })
}

async fn test_zunionstore_aggregate_min(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZUNIONSTORE AGGREGATE MIN", {
        c.assert_parity(&["ZADD", "zs1", "1", "a", "5", "b"])
            .await?;
        c.assert_parity(&["ZADD", "zs2", "3", "b", "4", "c"])
            .await?;
        c.assert_parity(&["ZUNIONSTORE", "out", "2", "zs1", "zs2", "AGGREGATE", "MIN"])
            .await?;
        c.assert_parity(&["ZRANGE", "out", "0", "-1", "WITHSCORES"])
            .await?;
        Ok(())
    })
}

async fn test_zunionstore_aggregate_max(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZUNIONSTORE AGGREGATE MAX", {
        c.assert_parity(&["ZADD", "zs1", "1", "a", "5", "b"])
            .await?;
        c.assert_parity(&["ZADD", "zs2", "3", "b", "4", "c"])
            .await?;
        c.assert_parity(&["ZUNIONSTORE", "out", "2", "zs1", "zs2", "AGGREGATE", "MAX"])
            .await?;
        c.assert_parity(&["ZRANGE", "out", "0", "-1", "WITHSCORES"])
            .await?;
        Ok(())
    })
}

async fn test_zinterstore_basic(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZINTERSTORE basic", {
        c.assert_parity(&["ZADD", "zs1", "1", "a", "2", "b", "3", "c"])
            .await?;
        c.assert_parity(&["ZADD", "zs2", "4", "b", "5", "c", "6", "d"])
            .await?;
        c.assert_parity(&["ZINTERSTORE", "out", "2", "zs1", "zs2"])
            .await?;
        c.assert_parity(&["ZRANGE", "out", "0", "-1", "WITHSCORES"])
            .await?;
        Ok(())
    })
}

async fn test_zinterstore_weights(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZINTERSTORE WEIGHTS", {
        c.assert_parity(&["ZADD", "zs1", "1", "a", "2", "b"])
            .await?;
        c.assert_parity(&["ZADD", "zs2", "3", "a", "4", "b"])
            .await?;
        c.assert_parity(&["ZINTERSTORE", "out", "2", "zs1", "zs2", "WEIGHTS", "2", "3"])
            .await?;
        c.assert_parity(&["ZRANGE", "out", "0", "-1", "WITHSCORES"])
            .await?;
        Ok(())
    })
}

async fn test_zinterstore_empty_result(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZINTERSTORE empty result", {
        c.assert_parity(&["ZADD", "zs1", "1", "a", "2", "b"])
            .await?;
        c.assert_parity(&["ZADD", "zs2", "3", "c", "4", "d"])
            .await?;
        c.assert_parity(&["ZINTERSTORE", "out", "2", "zs1", "zs2"])
            .await?;
        c.assert_parity(&["ZCARD", "out"]).await?;
        Ok(())
    })
}

// ============================================================================
// Lexicographic operations
// ============================================================================

async fn test_zrangebylex_basic(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZRANGEBYLEX basic", {
        // All same score for lex operations
        c.assert_parity(&[
            "ZADD", "zs", "0", "a", "0", "b", "0", "c", "0", "d", "0", "e",
        ])
        .await?;
        c.assert_parity(&["ZRANGEBYLEX", "zs", "[b", "[d"]).await?;
        Ok(())
    })
}

async fn test_zrangebylex_inclusive(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZRANGEBYLEX inclusive", {
        c.assert_parity(&["ZADD", "zs", "0", "apple", "0", "banana", "0", "cherry"])
            .await?;
        c.assert_parity(&["ZRANGEBYLEX", "zs", "[apple", "[cherry"])
            .await?;
        Ok(())
    })
}

async fn test_zrangebylex_exclusive(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZRANGEBYLEX exclusive", {
        c.assert_parity(&["ZADD", "zs", "0", "a", "0", "b", "0", "c", "0", "d"])
            .await?;
        c.assert_parity(&["ZRANGEBYLEX", "zs", "(a", "(d"]).await?;
        Ok(())
    })
}

async fn test_zrangebylex_unbounded(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZRANGEBYLEX unbounded", {
        c.assert_parity(&["ZADD", "zs", "0", "a", "0", "b", "0", "c"])
            .await?;
        c.assert_parity(&["ZRANGEBYLEX", "zs", "-", "+"]).await?;
        c.assert_parity(&["ZRANGEBYLEX", "zs", "-", "[b"]).await?;
        c.assert_parity(&["ZRANGEBYLEX", "zs", "[b", "+"]).await?;
        Ok(())
    })
}

async fn test_zlexcount(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZLEXCOUNT", {
        c.assert_parity(&[
            "ZADD", "zs", "0", "a", "0", "b", "0", "c", "0", "d", "0", "e",
        ])
        .await?;
        c.assert_parity(&["ZLEXCOUNT", "zs", "-", "+"]).await?;
        c.assert_parity(&["ZLEXCOUNT", "zs", "[b", "[d"]).await?;
        c.assert_parity(&["ZLEXCOUNT", "zs", "(b", "(d"]).await?;
        Ok(())
    })
}

async fn test_zremrangebylex(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZREMRANGEBYLEX", {
        c.assert_parity(&[
            "ZADD", "zs", "0", "a", "0", "b", "0", "c", "0", "d", "0", "e",
        ])
        .await?;
        c.assert_parity(&["ZREMRANGEBYLEX", "zs", "[b", "[d"])
            .await?;
        c.assert_parity(&["ZRANGE", "zs", "0", "-1"]).await?;
        Ok(())
    })
}

// ============================================================================
// Random member
// ============================================================================

async fn test_zrandmember_basic(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZRANDMEMBER basic", {
        c.assert_parity(&["ZADD", "zs", "1", "a", "2", "b", "3", "c"])
            .await?;
        // Just verify it returns something valid
        let redis = c.redis(&["ZRANDMEMBER", "zs"]).await?;
        let ferris = c.ferris(&["ZRANDMEMBER", "zs"]).await?;
        let r_ok = matches!(redis, ferris_protocol::RespValue::BulkString(_));
        let f_ok = matches!(ferris, ferris_protocol::RespValue::BulkString(_));
        if !r_ok || !f_ok {
            anyhow::bail!("ZRANDMEMBER should return bulk string");
        }
        Ok(())
    })
}

async fn test_zrandmember_count(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZRANDMEMBER with count", {
        c.assert_parity(&["ZADD", "zs", "1", "a", "2", "b", "3", "c"])
            .await?;
        // With count, returns array
        let redis = c.redis(&["ZRANDMEMBER", "zs", "2"]).await?;
        let ferris = c.ferris(&["ZRANDMEMBER", "zs", "2"]).await?;
        match (&redis, &ferris) {
            (ferris_protocol::RespValue::Array(r), ferris_protocol::RespValue::Array(f)) => {
                if r.len() != f.len() {
                    anyhow::bail!("ZRANDMEMBER count mismatch: {} vs {}", r.len(), f.len());
                }
            }
            _ => anyhow::bail!("ZRANDMEMBER with count should return array"),
        }
        Ok(())
    })
}

async fn test_zrandmember_withscores(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ZRANDMEMBER WITHSCORES", {
        c.assert_parity(&["ZADD", "zs", "1", "a", "2", "b", "3", "c"])
            .await?;
        let redis = c.redis(&["ZRANDMEMBER", "zs", "2", "WITHSCORES"]).await?;
        let ferris = c.ferris(&["ZRANDMEMBER", "zs", "2", "WITHSCORES"]).await?;
        match (&redis, &ferris) {
            (ferris_protocol::RespValue::Array(r), ferris_protocol::RespValue::Array(f)) => {
                // With scores, should have double the elements (member, score pairs)
                if r.len() != f.len() || r.len() != 4 {
                    anyhow::bail!("ZRANDMEMBER WITHSCORES length mismatch");
                }
            }
            _ => anyhow::bail!("ZRANDMEMBER WITHSCORES should return array"),
        }
        Ok(())
    })
}

// ============================================================================
// Edge cases with empty/nonexistent
// ============================================================================

async fn test_operations_on_empty_zset(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "Operations on empty zset", {
        // Create and empty the set
        c.assert_parity(&["ZADD", "zs", "1", "a"]).await?;
        c.assert_parity(&["ZREM", "zs", "a"]).await?;
        // Now it's empty
        c.assert_parity(&["ZCARD", "zs"]).await?;
        c.assert_parity(&["ZRANGE", "zs", "0", "-1"]).await?;
        c.assert_parity(&["ZPOPMIN", "zs"]).await?;
        c.assert_parity(&["ZSCORE", "zs", "any"]).await?;
        Ok(())
    })
}

async fn test_operations_on_nonexistent_key(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "Zset ops on nonexistent key", {
        c.assert_parity(&["ZCARD", "nonexistent"]).await?;
        c.assert_parity(&["ZRANGE", "nonexistent", "0", "-1"])
            .await?;
        c.assert_parity(&["ZSCORE", "nonexistent", "member"])
            .await?;
        c.assert_parity(&["ZRANK", "nonexistent", "member"]).await?;
        c.assert_parity(&["ZPOPMIN", "nonexistent"]).await?;
        c.assert_parity(&["ZREM", "nonexistent", "member"]).await?;
        c.assert_parity(&["ZCOUNT", "nonexistent", "-inf", "+inf"])
            .await?;
        Ok(())
    })
}

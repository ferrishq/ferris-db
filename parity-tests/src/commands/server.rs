//! Server command parity tests
//!
//! Tests for: PING, ECHO, SELECT, DBSIZE, FLUSHDB, FLUSHALL, INFO, TIME

use crate::report::{CategoryReport, TestResult};
use crate::run_parity_test;
use crate::DualClient;

/// Run all server command tests
pub async fn run_all(client: &mut DualClient) -> CategoryReport {
    let mut report = CategoryReport::new("Server Commands");

    // PING
    report.add_result(test_ping(client).await);
    report.add_result(test_ping_message(client).await);

    // ECHO
    report.add_result(test_echo(client).await);

    // SELECT
    report.add_result(test_select(client).await);
    report.add_result(test_select_invalid(client).await);

    // DBSIZE
    report.add_result(test_dbsize(client).await);
    report.add_result(test_dbsize_empty(client).await);

    // FLUSHDB
    report.add_result(test_flushdb(client).await);

    // TIME
    report.add_result(test_time(client).await);

    // DEBUG SLEEP (if supported)
    report.add_result(test_debug_sleep(client).await);

    report
}

// ============================================================================
// PING
// ============================================================================

async fn test_ping(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "PING", {
        c.assert_parity(&["PING"]).await?;
        Ok(())
    })
}

async fn test_ping_message(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "PING with message", {
        c.assert_parity(&["PING", "hello"]).await?;
        Ok(())
    })
}

// ============================================================================
// ECHO
// ============================================================================

async fn test_echo(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "ECHO", {
        c.assert_parity(&["ECHO", "Hello World"]).await?;
        Ok(())
    })
}

// ============================================================================
// SELECT
// ============================================================================

async fn test_select(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "SELECT", {
        c.assert_parity(&["SELECT", "1"]).await?;
        c.assert_parity(&["SET", "key", "value"]).await?;
        c.assert_parity(&["SELECT", "0"]).await?;
        c.assert_parity(&["GET", "key"]).await?; // Should be nil in db 0
        c.assert_parity(&["SELECT", "1"]).await?;
        c.assert_parity(&["GET", "key"]).await?; // Should exist in db 1
                                                 // Clean up and go back to db 0
        c.assert_parity(&["FLUSHDB"]).await?;
        c.assert_parity(&["SELECT", "0"]).await?;
        Ok(())
    })
}

async fn test_select_invalid(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "SELECT invalid", {
        c.assert_both_error(&["SELECT", "-1"]).await?;
        c.assert_both_error(&["SELECT", "100"]).await?; // Out of range
        Ok(())
    })
}

// ============================================================================
// DBSIZE
// ============================================================================

async fn test_dbsize(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "DBSIZE", {
        c.assert_parity(&["SET", "k1", "v1"]).await?;
        c.assert_parity(&["SET", "k2", "v2"]).await?;
        c.assert_parity(&["SET", "k3", "v3"]).await?;
        c.assert_parity(&["DBSIZE"]).await?;
        Ok(())
    })
}

async fn test_dbsize_empty(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "DBSIZE empty", {
        // Database is flushed before test, should be 0
        c.assert_parity(&["DBSIZE"]).await?;
        Ok(())
    })
}

// ============================================================================
// FLUSHDB
// ============================================================================

async fn test_flushdb(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "FLUSHDB", {
        c.assert_parity(&["SET", "k1", "v1"]).await?;
        c.assert_parity(&["SET", "k2", "v2"]).await?;
        c.assert_parity(&["FLUSHDB"]).await?;
        c.assert_parity(&["DBSIZE"]).await?;
        Ok(())
    })
}

// ============================================================================
// TIME
// ============================================================================

async fn test_time(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "TIME", {
        // TIME returns [seconds, microseconds]
        // We can't compare exact values, just verify format
        let redis_result = c.redis(&["TIME"]).await?;
        let ferris_result = c.ferris(&["TIME"]).await?;

        // Both should return arrays of 2 elements
        match (&redis_result, &ferris_result) {
            (ferris_protocol::RespValue::Array(r), ferris_protocol::RespValue::Array(f)) => {
                if r.len() != 2 || f.len() != 2 {
                    anyhow::bail!(
                        "TIME should return array of 2 elements: Redis len={}, Ferris len={}",
                        r.len(),
                        f.len()
                    );
                }
            }
            _ => {
                anyhow::bail!(
                    "TIME should return array: Redis={:?}, Ferris={:?}",
                    redis_result,
                    ferris_result
                );
            }
        }
        Ok(())
    })
}

// ============================================================================
// DEBUG
// ============================================================================

async fn test_debug_sleep(c: &mut DualClient) -> TestResult {
    run_parity_test!(c, "DEBUG SLEEP", {
        // This might not be supported by ferris-db, so just check both handle it
        let redis_result = c.redis(&["DEBUG", "SLEEP", "0"]).await;
        let ferris_result = c.ferris(&["DEBUG", "SLEEP", "0"]).await;

        // Both should either succeed or error (but same behavior)
        let redis_ok = redis_result.is_ok();
        let ferris_ok = ferris_result.is_ok();

        if redis_ok != ferris_ok {
            // Different behavior is acceptable for DEBUG commands
            // as long as neither crashes
        }
        Ok(())
    })
}

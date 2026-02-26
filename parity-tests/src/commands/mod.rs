//! Command-specific parity tests
//!
//! Each submodule contains tests for a category of Redis commands.

pub mod hash;
pub mod key;
pub mod list;
pub mod server;
pub mod set;
pub mod sorted_set;
pub mod string;

use crate::report::{CategoryReport, TestResult};
use crate::DualClient;

/// Trait for command test categories
#[async_trait::async_trait]
pub trait CommandTests {
    /// Get the category name
    fn category_name() -> &'static str;

    /// Run all tests in this category
    async fn run_all(client: &mut DualClient) -> CategoryReport;
}

/// Helper struct for running tests with timing and isolation
pub struct TestRunner<'a> {
    client: &'a mut DualClient,
}

impl<'a> TestRunner<'a> {
    pub fn new(client: &'a mut DualClient) -> Self {
        Self { client }
    }

    /// Run a test with setup (flush) and timing
    pub async fn run(&mut self, name: &str, result: anyhow::Result<()>) -> TestResult {
        TestResult {
            name: name.to_string(),
            passed: result.is_ok(),
            error: result.err().map(|e| e.to_string()),
            duration_ms: 0, // Timing handled externally if needed
        }
    }

    /// Flush both databases for test isolation
    pub async fn flush(&mut self) -> anyhow::Result<()> {
        self.client.flush_both().await
    }

    /// Get the client
    pub fn client(&mut self) -> &mut DualClient {
        self.client
    }
}

/// Macro for simpler test execution with auto-flush
#[macro_export]
macro_rules! run_parity_test {
    ($client:expr, $name:expr, $body:block) => {{
        let start = std::time::Instant::now();
        let _ = $client.flush_both().await;
        let result: anyhow::Result<()> = async { $body }.await;
        let duration = start.elapsed();
        $crate::report::TestResult {
            name: $name.to_string(),
            passed: result.is_ok(),
            error: result.err().map(|e| e.to_string()),
            duration_ms: duration.as_millis() as u64,
        }
    }};
}

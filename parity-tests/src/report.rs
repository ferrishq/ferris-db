//! Test report generation for parity tests

use colored::Colorize;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Result of a single test
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestResult {
    /// Test name
    pub name: String,
    /// Whether the test passed
    pub passed: bool,
    /// Error message if failed
    pub error: Option<String>,
    /// Duration of the test
    pub duration_ms: u64,
}

/// Report for a category of tests
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CategoryReport {
    /// Category name
    pub name: String,
    /// Individual test results
    pub tests: Vec<TestResult>,
    /// Number of tests passed
    pub passed: usize,
    /// Number of tests failed
    pub failed: usize,
    /// Total duration
    pub duration_ms: u64,
}

impl CategoryReport {
    /// Create a new empty category report
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            tests: Vec::new(),
            passed: 0,
            failed: 0,
            duration_ms: 0,
        }
    }

    /// Add a test result
    pub fn add_result(&mut self, result: TestResult) {
        if result.passed {
            self.passed += 1;
        } else {
            self.failed += 1;
        }
        self.duration_ms += result.duration_ms;
        self.tests.push(result);
    }

    /// Get pass rate as percentage
    pub fn pass_rate(&self) -> f64 {
        let total = self.passed + self.failed;
        if total == 0 {
            100.0
        } else {
            (self.passed as f64 / total as f64) * 100.0
        }
    }
}

/// Performance comparison data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceReport {
    /// Operations per second for Redis
    pub redis_ops_per_sec: f64,
    /// Operations per second for ferris-db
    pub ferris_ops_per_sec: f64,
    /// Ratio (ferris / redis, >1.0 means faster)
    pub ratio: f64,
    /// P50 latency for Redis (microseconds)
    pub redis_p50_us: f64,
    /// P50 latency for ferris-db (microseconds)
    pub ferris_p50_us: f64,
    /// P99 latency for Redis (microseconds)
    pub redis_p99_us: f64,
    /// P99 latency for ferris-db (microseconds)
    pub ferris_p99_us: f64,
}

/// Complete parity test report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParityReport {
    /// Timestamp of the report
    pub timestamp: String,
    /// Total tests run
    pub total_tests: usize,
    /// Total tests passed
    pub passed: usize,
    /// Total tests failed
    pub failed: usize,
    /// Category reports
    pub categories: HashMap<String, CategoryReport>,
    /// Performance report (if benchmarks were run)
    pub performance: Option<PerformanceReport>,
    /// Total duration
    pub duration_ms: u64,
}

impl ParityReport {
    /// Create a new empty report
    pub fn new() -> Self {
        Self {
            timestamp: chrono_lite_timestamp(),
            total_tests: 0,
            passed: 0,
            failed: 0,
            categories: HashMap::new(),
            performance: None,
            duration_ms: 0,
        }
    }

    /// Add a category report
    pub fn add_category(&mut self, category: CategoryReport) {
        self.total_tests += category.passed + category.failed;
        self.passed += category.passed;
        self.failed += category.failed;
        self.duration_ms += category.duration_ms;
        self.categories.insert(category.name.clone(), category);
    }

    /// Get overall pass rate
    pub fn pass_rate(&self) -> f64 {
        if self.total_tests == 0 {
            100.0
        } else {
            (self.passed as f64 / self.total_tests as f64) * 100.0
        }
    }

    /// Print a summary to console
    pub fn print_summary(&self) {
        println!("\n{}", "═".repeat(60).blue());
        println!("{}", " Redis Parity Test Report ".blue().bold());
        println!("{}", "═".repeat(60).blue());
        println!();

        // Overall summary
        let status = if self.failed == 0 {
            "PASSED".green().bold()
        } else {
            "FAILED".red().bold()
        };
        println!("Overall Status: {}", status);
        println!(
            "Pass Rate: {:.1}% ({}/{} tests)",
            self.pass_rate(),
            self.passed,
            self.total_tests
        );
        println!("Duration: {:.2}s", self.duration_ms as f64 / 1000.0);
        println!();

        // Category breakdown
        println!("{}", "Category Results:".bold());
        println!("{}", "─".repeat(60));

        let mut categories: Vec<_> = self.categories.values().collect();
        categories.sort_by(|a, b| a.name.cmp(&b.name));

        for cat in categories {
            let status = if cat.failed == 0 {
                "✓".green()
            } else {
                "✗".red()
            };
            println!(
                "  {} {:20} {:>3}/{:>3} ({:>5.1}%)",
                status,
                cat.name,
                cat.passed,
                cat.passed + cat.failed,
                cat.pass_rate()
            );

            // Show failed tests
            for test in &cat.tests {
                if !test.passed {
                    println!("      {} {}", "✗".red(), test.name);
                    if let Some(ref err) = test.error {
                        // Truncate long error messages
                        let err_short = if err.len() > 100 {
                            format!("{}...", &err[..97])
                        } else {
                            err.clone()
                        };
                        println!("        {}", err_short.dimmed());
                    }
                }
            }
        }

        // Performance summary (if available)
        if let Some(ref perf) = self.performance {
            println!();
            println!("{}", "Performance Comparison:".bold());
            println!("{}", "─".repeat(60));
            println!(
                "  Throughput: Redis {:.0} ops/s, ferris-db {:.0} ops/s ({:.1}x)",
                perf.redis_ops_per_sec, perf.ferris_ops_per_sec, perf.ratio
            );
            println!(
                "  Latency P50: Redis {:.0}μs, ferris-db {:.0}μs",
                perf.redis_p50_us, perf.ferris_p50_us
            );
            println!(
                "  Latency P99: Redis {:.0}μs, ferris-db {:.0}μs",
                perf.redis_p99_us, perf.ferris_p99_us
            );
        }

        println!();
        println!("{}", "═".repeat(60).blue());
    }

    /// Save report to JSON file
    pub fn save_json(&self, path: &str) -> anyhow::Result<()> {
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(path, json)?;
        Ok(())
    }
}

impl Default for ParityReport {
    fn default() -> Self {
        Self::new()
    }
}

/// Simple timestamp without external crate
fn chrono_lite_timestamp() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = duration.as_secs();
    // Simple ISO-like format
    format!("{}", secs)
}

/// Helper macro for running a parity test and recording results
#[macro_export]
macro_rules! parity_test {
    ($category:expr, $name:expr, $client:expr, $body:block) => {{
        let start = std::time::Instant::now();
        let result = (|| async { $body })().await;
        let duration = start.elapsed();

        let test_result = match result {
            Ok(()) => $crate::report::TestResult {
                name: $name.to_string(),
                passed: true,
                error: None,
                duration_ms: duration.as_millis() as u64,
            },
            Err(e) => $crate::report::TestResult {
                name: $name.to_string(),
                passed: false,
                error: Some(e.to_string()),
                duration_ms: duration.as_millis() as u64,
            },
        };

        test_result
    }};
}

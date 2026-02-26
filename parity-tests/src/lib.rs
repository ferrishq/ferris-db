//! Redis Parity Test Suite for ferris-db
//!
//! This crate provides comprehensive parity testing between Redis and ferris-db,
//! ensuring command-level compatibility and performance parity.
//!
//! # Usage
//!
//! ```bash
//! # Start Redis on port 6379
//! redis-server --port 6379
//!
//! # Start ferris-db on port 6380
//! cargo run --release -- --port 6380
//!
//! # Run parity tests
//! cargo test -p parity-tests
//! ```

// Allow common test patterns that would otherwise trigger clippy warnings
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::missing_const_for_fn)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::uninlined_format_args)]
#![allow(clippy::match_same_arms)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::must_use_candidate)]
#![allow(clippy::return_self_not_must_use)]
#![allow(clippy::unused_async)]

pub mod benchmark;
pub mod client;
pub mod commands;
pub mod comparison;
pub mod report;

pub use benchmark::{
    print_benchmark_summary, run_all_benchmarks, BenchmarkComparison, BenchmarkConfig,
    BenchmarkResult, Benchmarker,
};
pub use client::{DualClient, ParityClient, ServerType};
pub use comparison::{compare_responses, CompareMode, ParityResult};
pub use report::{CategoryReport, ParityReport, PerformanceReport, TestResult};

/// Default Redis port
pub const REDIS_PORT: u16 = 6379;

/// Default ferris-db port
pub const FERRIS_PORT: u16 = 6380;

/// Configuration for parity tests
#[derive(Debug, Clone)]
pub struct ParityConfig {
    /// Redis server address
    pub redis_addr: String,
    /// ferris-db server address
    pub ferris_addr: String,
    /// Whether to flush databases before each test
    pub flush_before_test: bool,
    /// Key prefix to use for isolation
    pub key_prefix: String,
    /// Timeout for operations in milliseconds
    pub timeout_ms: u64,
}

impl Default for ParityConfig {
    fn default() -> Self {
        Self {
            redis_addr: format!("127.0.0.1:{REDIS_PORT}"),
            ferris_addr: format!("127.0.0.1:{FERRIS_PORT}"),
            flush_before_test: true,
            key_prefix: String::new(),
            timeout_ms: 5000,
        }
    }
}

impl ParityConfig {
    /// Create config with custom ports
    pub fn with_ports(redis_port: u16, ferris_port: u16) -> Self {
        Self {
            redis_addr: format!("127.0.0.1:{redis_port}"),
            ferris_addr: format!("127.0.0.1:{ferris_port}"),
            ..Default::default()
        }
    }

    /// Set a key prefix for test isolation
    pub fn with_prefix(mut self, prefix: &str) -> Self {
        self.key_prefix = prefix.to_string();
        self
    }
}

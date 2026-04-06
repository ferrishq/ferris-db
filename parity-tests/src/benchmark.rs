//! Performance benchmarking for Redis vs ferris-db comparison
//!
//! Provides tools to measure and compare throughput and latency between
//! Redis and ferris-db for various operations.

use crate::client::{ParityClient, ServerType};
use crate::report::PerformanceReport;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Configuration for benchmark runs
#[derive(Debug, Clone)]
pub struct BenchmarkConfig {
    /// Number of warmup iterations (not counted)
    pub warmup_iterations: usize,
    /// Number of iterations to measure
    pub iterations: usize,
    /// Value size in bytes for SET/GET tests
    pub value_size: usize,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            warmup_iterations: 100,
            iterations: 1000,
            value_size: 100,
        }
    }
}

impl BenchmarkConfig {
    /// Quick benchmark configuration (fewer iterations)
    pub fn quick() -> Self {
        Self {
            warmup_iterations: 50,
            iterations: 500,
            value_size: 100,
        }
    }

    /// Thorough benchmark configuration (more iterations)
    pub fn thorough() -> Self {
        Self {
            warmup_iterations: 500,
            iterations: 10000,
            value_size: 100,
        }
    }
}

/// Result from a single benchmark run
#[derive(Debug)]
pub struct BenchmarkResult {
    /// Server type that was benchmarked
    pub server: ServerType,
    /// Operation name
    pub operation: String,
    /// Total operations completed
    pub total_ops: usize,
    /// Total time for all operations
    pub total_time: Duration,
    /// Individual operation latencies (microseconds)
    pub latencies_us: Vec<u64>,
    /// Cached sorted latencies (lazily computed)
    #[allow(clippy::vec_box)]
    sorted_latencies: std::cell::OnceCell<Vec<u64>>,
}

impl Clone for BenchmarkResult {
    fn clone(&self) -> Self {
        // When cloning, we create a new empty cache rather than trying to clone
        // the OnceCell, which would lose the cached data anyway
        Self {
            server: self.server,
            operation: self.operation.clone(),
            total_ops: self.total_ops,
            total_time: self.total_time,
            latencies_us: self.latencies_us.clone(),
            sorted_latencies: std::cell::OnceCell::new(),
        }
    }
}

impl BenchmarkResult {
    /// Calculate operations per second
    pub fn ops_per_sec(&self) -> f64 {
        self.total_ops as f64 / self.total_time.as_secs_f64()
    }

    /// Calculate mean latency in microseconds
    pub fn mean_latency_us(&self) -> f64 {
        if self.latencies_us.is_empty() {
            return 0.0;
        }
        let sum: u64 = self.latencies_us.iter().sum();
        sum as f64 / self.latencies_us.len() as f64
    }

    /// Get sorted latencies (cached)
    fn sorted_latencies(&self) -> &[u64] {
        self.sorted_latencies.get_or_init(|| {
            let mut sorted = self.latencies_us.clone();
            sorted.sort_unstable();
            sorted
        })
    }

    /// Calculate percentile latency in microseconds
    #[allow(clippy::cast_sign_loss)]
    pub fn percentile_latency_us(&self, p: f64) -> f64 {
        if self.latencies_us.is_empty() {
            return 0.0;
        }
        let sorted = self.sorted_latencies();
        let idx = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
        sorted[idx.min(sorted.len() - 1)] as f64
    }

    /// P50 latency
    pub fn p50_us(&self) -> f64 {
        self.percentile_latency_us(50.0)
    }

    /// P99 latency
    pub fn p99_us(&self) -> f64 {
        self.percentile_latency_us(99.0)
    }

    /// P99.9 latency
    pub fn p999_us(&self) -> f64 {
        self.percentile_latency_us(99.9)
    }

    /// Min latency
    pub fn min_latency_us(&self) -> f64 {
        self.latencies_us.iter().min().copied().unwrap_or(0) as f64
    }

    /// Max latency
    pub fn max_latency_us(&self) -> f64 {
        self.latencies_us.iter().max().copied().unwrap_or(0) as f64
    }
}

/// Comparison of benchmark results between Redis and ferris-db
#[derive(Debug, Clone)]
pub struct BenchmarkComparison {
    /// Operation name
    pub operation: String,
    /// Redis results
    pub redis: BenchmarkResult,
    /// ferris-db results
    pub ferris: BenchmarkResult,
}

impl BenchmarkComparison {
    /// Create a new comparison from two results
    pub fn new(redis: BenchmarkResult, ferris: BenchmarkResult) -> Self {
        assert_eq!(redis.operation, ferris.operation);
        Self {
            operation: redis.operation.clone(),
            redis,
            ferris,
        }
    }

    /// Throughput ratio (ferris / redis). >1.0 means ferris is faster
    pub fn throughput_ratio(&self) -> f64 {
        self.ferris.ops_per_sec() / self.redis.ops_per_sec()
    }

    /// Latency ratio for P50 (redis / ferris). >1.0 means ferris is faster (lower latency)
    pub fn latency_p50_ratio(&self) -> f64 {
        if self.ferris.p50_us() == 0.0 {
            return 1.0;
        }
        self.redis.p50_us() / self.ferris.p50_us()
    }

    /// Latency ratio for P99 (redis / ferris). >1.0 means ferris is faster
    pub fn latency_p99_ratio(&self) -> f64 {
        if self.ferris.p99_us() == 0.0 {
            return 1.0;
        }
        self.redis.p99_us() / self.ferris.p99_us()
    }

    /// Convert to performance report format
    pub fn to_performance_report(&self) -> PerformanceReport {
        PerformanceReport {
            redis_ops_per_sec: self.redis.ops_per_sec(),
            ferris_ops_per_sec: self.ferris.ops_per_sec(),
            ratio: self.throughput_ratio(),
            redis_p50_us: self.redis.p50_us(),
            ferris_p50_us: self.ferris.p50_us(),
            redis_p99_us: self.redis.p99_us(),
            ferris_p99_us: self.ferris.p99_us(),
        }
    }

    /// Print a formatted comparison
    pub fn print(&self) {
        use colored::Colorize;

        println!("\n{} Benchmark:", self.operation.bold());
        println!("{}", "─".repeat(60));

        // Throughput
        let ratio = self.throughput_ratio();
        let ratio_str = if ratio >= 1.0 {
            format!("{:.2}x faster", ratio).green()
        } else {
            format!("{:.2}x slower", 1.0 / ratio).red()
        };
        println!(
            "  Throughput:  Redis {:>10.0} ops/s  |  ferris-db {:>10.0} ops/s  ({})",
            self.redis.ops_per_sec(),
            self.ferris.ops_per_sec(),
            ratio_str
        );

        // P50 Latency
        let p50_ratio = self.latency_p50_ratio();
        let p50_str = if p50_ratio >= 1.0 {
            format!("{:.2}x lower", p50_ratio).green()
        } else {
            format!("{:.2}x higher", 1.0 / p50_ratio).red()
        };
        println!(
            "  Latency P50: Redis {:>10.1} μs   |  ferris-db {:>10.1} μs   ({})",
            self.redis.p50_us(),
            self.ferris.p50_us(),
            p50_str
        );

        // P99 Latency
        let p99_ratio = self.latency_p99_ratio();
        let p99_str = if p99_ratio >= 1.0 {
            format!("{:.2}x lower", p99_ratio).green()
        } else {
            format!("{:.2}x higher", 1.0 / p99_ratio).red()
        };
        println!(
            "  Latency P99: Redis {:>10.1} μs   |  ferris-db {:>10.1} μs   ({})",
            self.redis.p99_us(),
            self.ferris.p99_us(),
            p99_str
        );

        // Min/Max
        println!(
            "  Latency Min: Redis {:>10.1} μs   |  ferris-db {:>10.1} μs",
            self.redis.min_latency_us(),
            self.ferris.min_latency_us()
        );
        println!(
            "  Latency Max: Redis {:>10.1} μs   |  ferris-db {:>10.1} μs",
            self.redis.max_latency_us(),
            self.ferris.max_latency_us()
        );
    }
}

/// Benchmark runner for comparing Redis and ferris-db
pub struct Benchmarker {
    config: BenchmarkConfig,
}

impl Benchmarker {
    /// Create a new benchmarker with the given configuration
    pub fn new(config: BenchmarkConfig) -> Self {
        Self { config }
    }

    /// Run a benchmark on a single server
    async fn run_single(
        &self,
        client: &mut ParityClient,
        operation: &str,
        commands: &[Vec<&str>],
    ) -> anyhow::Result<BenchmarkResult> {
        let server = client.server_type();

        // Warmup
        let mut warmup_errors = 0;
        let total_warmup_attempts = self.config.warmup_iterations * commands.len();
        for _ in 0..self.config.warmup_iterations {
            for cmd in commands {
                if let Err(e) = client.cmd(cmd).await {
                    warmup_errors += 1;
                    tracing::warn!("Warmup command failed for {}: {:?}", operation, e);
                }
            }
        }

        // If all warmup attempts failed, abort before running the actual benchmark
        if warmup_errors == total_warmup_attempts {
            anyhow::bail!(
                "All {} warmup attempts failed for {}. Aborting benchmark.",
                total_warmup_attempts,
                operation
            );
        }

        if warmup_errors > 0 {
            tracing::warn!(
                "Warmup had {} errors out of {} iterations for {}",
                warmup_errors,
                total_warmup_attempts,
                operation
            );
        }

        // Actual benchmark
        let mut latencies = Vec::with_capacity(self.config.iterations * commands.len());
        let overall_start = Instant::now();

        for _ in 0..self.config.iterations {
            for cmd in commands {
                let start = Instant::now();
                let _ = client.cmd(cmd).await?;
                let elapsed = start.elapsed();
                latencies.push(elapsed.as_micros() as u64);
            }
        }

        let total_time = overall_start.elapsed();
        let total_ops = self.config.iterations * commands.len();

        Ok(BenchmarkResult {
            server,
            operation: operation.to_string(),
            total_ops,
            total_time,
            latencies_us: latencies,
            sorted_latencies: std::cell::OnceCell::new(),
        })
    }

    /// Run a benchmark comparing Redis and ferris-db
    pub async fn run_comparison(
        &self,
        redis: &mut ParityClient,
        ferris: &mut ParityClient,
        operation: &str,
        commands: &[Vec<&str>],
    ) -> anyhow::Result<BenchmarkComparison> {
        // Flush both databases before benchmark
        let _ = redis.cmd(&["FLUSHDB"]).await;
        let _ = ferris.cmd(&["FLUSHDB"]).await;

        // Allow time for background cleanup after FLUSHDB
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Run Redis benchmark
        let redis_result = self.run_single(redis, operation, commands).await?;

        // Flush again and run ferris-db benchmark
        let _ = redis.cmd(&["FLUSHDB"]).await;
        let _ = ferris.cmd(&["FLUSHDB"]).await;

        // Allow time for background cleanup after FLUSHDB
        tokio::time::sleep(Duration::from_millis(100)).await;

        let ferris_result = self.run_single(ferris, operation, commands).await?;

        Ok(BenchmarkComparison::new(redis_result, ferris_result))
    }

    /// Run SET benchmark
    pub async fn bench_set(
        &self,
        redis: &mut ParityClient,
        ferris: &mut ParityClient,
    ) -> anyhow::Result<BenchmarkComparison> {
        let value = "x".repeat(self.config.value_size);
        let commands: Vec<Vec<&str>> = (0..10)
            .map(|i| {
                let key = format!("bench:set:{}", i);
                // We need to leak these strings to get &str with 'static lifetime
                // This is fine for benchmarks as they're short-lived
                vec!["SET", Box::leak(key.into_boxed_str()), &value]
            })
            .collect();

        // Convert to the format expected
        let cmd_refs: Vec<Vec<&str>> = commands.iter().map(Vec::clone).collect();

        self.run_comparison(redis, ferris, "SET", &cmd_refs).await
    }

    /// Run GET benchmark (requires keys to exist)
    pub async fn bench_get(
        &self,
        redis: &mut ParityClient,
        ferris: &mut ParityClient,
    ) -> anyhow::Result<BenchmarkComparison> {
        let value = "x".repeat(self.config.value_size);

        // Setup: create keys in both databases
        for i in 0..10 {
            let key = format!("bench:get:{}", i);
            let _ = redis.cmd(&["SET", &key, &value]).await;
            let _ = ferris.cmd(&["SET", &key, &value]).await;
        }

        let commands: Vec<Vec<&str>> = (0..10)
            .map(|i| {
                let key = format!("bench:get:{}", i);
                vec!["GET", Box::leak(key.into_boxed_str())]
            })
            .collect();

        let cmd_refs: Vec<Vec<&str>> = commands.iter().map(Vec::clone).collect();

        self.run_comparison(redis, ferris, "GET", &cmd_refs).await
    }

    /// Run INCR benchmark
    pub async fn bench_incr(
        &self,
        redis: &mut ParityClient,
        ferris: &mut ParityClient,
    ) -> anyhow::Result<BenchmarkComparison> {
        let commands: Vec<Vec<&str>> = (0..10)
            .map(|i| {
                let key = format!("bench:incr:{}", i);
                vec!["INCR", Box::leak(key.into_boxed_str())]
            })
            .collect();

        let cmd_refs: Vec<Vec<&str>> = commands.iter().map(Vec::clone).collect();

        self.run_comparison(redis, ferris, "INCR", &cmd_refs).await
    }

    /// Run LPUSH benchmark
    pub async fn bench_lpush(
        &self,
        redis: &mut ParityClient,
        ferris: &mut ParityClient,
    ) -> anyhow::Result<BenchmarkComparison> {
        let value = "x".repeat(self.config.value_size);
        let commands: Vec<Vec<&str>> = (0..10)
            .map(|i| {
                let key = format!("bench:lpush:{}", i);
                vec!["LPUSH", Box::leak(key.into_boxed_str()), &value]
            })
            .collect();

        let cmd_refs: Vec<Vec<&str>> = commands.iter().map(Vec::clone).collect();

        self.run_comparison(redis, ferris, "LPUSH", &cmd_refs).await
    }

    /// Run HSET benchmark
    pub async fn bench_hset(
        &self,
        redis: &mut ParityClient,
        ferris: &mut ParityClient,
    ) -> anyhow::Result<BenchmarkComparison> {
        let value = "x".repeat(self.config.value_size);
        let commands: Vec<Vec<&str>> = (0..10)
            .map(|i| {
                let key = format!("bench:hset:{}", i);
                let field = format!("field{}", i);
                vec![
                    "HSET",
                    Box::leak(key.into_boxed_str()),
                    Box::leak(field.into_boxed_str()),
                    &value,
                ]
            })
            .collect();

        let cmd_refs: Vec<Vec<&str>> = commands.iter().map(Vec::clone).collect();

        self.run_comparison(redis, ferris, "HSET", &cmd_refs).await
    }

    /// Run ZADD benchmark
    pub async fn bench_zadd(
        &self,
        redis: &mut ParityClient,
        ferris: &mut ParityClient,
    ) -> anyhow::Result<BenchmarkComparison> {
        let commands: Vec<Vec<&str>> = (0..10)
            .map(|i| {
                let key = format!("bench:zadd:{}", i);
                let score = format!("{}", i);
                let member = format!("member{}", i);
                vec![
                    "ZADD",
                    Box::leak(key.into_boxed_str()),
                    Box::leak(score.into_boxed_str()),
                    Box::leak(member.into_boxed_str()),
                ]
            })
            .collect();

        let cmd_refs: Vec<Vec<&str>> = commands.iter().map(Vec::clone).collect();

        self.run_comparison(redis, ferris, "ZADD", &cmd_refs).await
    }

    /// Run SADD benchmark
    pub async fn bench_sadd(
        &self,
        redis: &mut ParityClient,
        ferris: &mut ParityClient,
    ) -> anyhow::Result<BenchmarkComparison> {
        let commands: Vec<Vec<&str>> = (0..10)
            .map(|i| {
                let key = format!("bench:sadd:{}", i);
                let member = format!("member{}", i);
                vec![
                    "SADD",
                    Box::leak(key.into_boxed_str()),
                    Box::leak(member.into_boxed_str()),
                ]
            })
            .collect();

        let cmd_refs: Vec<Vec<&str>> = commands.iter().map(Vec::clone).collect();

        self.run_comparison(redis, ferris, "SADD", &cmd_refs).await
    }

    /// Run PING benchmark (minimal operation overhead)
    pub async fn bench_ping(
        &self,
        redis: &mut ParityClient,
        ferris: &mut ParityClient,
    ) -> anyhow::Result<BenchmarkComparison> {
        let commands = vec![vec!["PING"]];
        self.run_comparison(redis, ferris, "PING", &commands).await
    }

    /// Run MSET benchmark (multi-key write)
    pub async fn bench_mset(
        &self,
        redis: &mut ParityClient,
        ferris: &mut ParityClient,
    ) -> anyhow::Result<BenchmarkComparison> {
        let value = "x".repeat(self.config.value_size);
        // MSET with 5 keys at once
        let commands: Vec<Vec<&str>> = vec![vec![
            "MSET",
            Box::leak("mset:k1".to_string().into_boxed_str()),
            Box::leak(value.clone().into_boxed_str()),
            Box::leak("mset:k2".to_string().into_boxed_str()),
            Box::leak(value.clone().into_boxed_str()),
            Box::leak("mset:k3".to_string().into_boxed_str()),
            Box::leak(value.clone().into_boxed_str()),
            Box::leak("mset:k4".to_string().into_boxed_str()),
            Box::leak(value.clone().into_boxed_str()),
            Box::leak("mset:k5".to_string().into_boxed_str()),
            Box::leak(value.clone().into_boxed_str()),
        ]];
        self.run_comparison(redis, ferris, "MSET (5 keys)", &commands)
            .await
    }

    /// Run MGET benchmark (multi-key read)
    pub async fn bench_mget(
        &self,
        redis: &mut ParityClient,
        ferris: &mut ParityClient,
    ) -> anyhow::Result<BenchmarkComparison> {
        let value = "x".repeat(self.config.value_size);

        // Setup: create keys
        for i in 0..5 {
            let key = format!("mget:k{}", i);
            let _ = redis.cmd(&["SET", &key, &value]).await;
            let _ = ferris.cmd(&["SET", &key, &value]).await;
        }

        let commands: Vec<Vec<&str>> = vec![vec![
            "MGET",
            Box::leak("mget:k0".to_string().into_boxed_str()),
            Box::leak("mget:k1".to_string().into_boxed_str()),
            Box::leak("mget:k2".to_string().into_boxed_str()),
            Box::leak("mget:k3".to_string().into_boxed_str()),
            Box::leak("mget:k4".to_string().into_boxed_str()),
        ]];
        self.run_comparison(redis, ferris, "MGET (5 keys)", &commands)
            .await
    }

    /// Run LRANGE benchmark (list read)
    pub async fn bench_lrange(
        &self,
        redis: &mut ParityClient,
        ferris: &mut ParityClient,
    ) -> anyhow::Result<BenchmarkComparison> {
        // Setup: create a list with 100 elements
        for i in 0..100 {
            let val = format!("item{}", i);
            let _ = redis.cmd(&["RPUSH", "bench:lrange", &val]).await;
            let _ = ferris.cmd(&["RPUSH", "bench:lrange", &val]).await;
        }

        let commands: Vec<Vec<&str>> = vec![vec!["LRANGE", "bench:lrange", "0", "49"]];
        self.run_comparison(redis, ferris, "LRANGE (50 items)", &commands)
            .await
    }

    /// Run ZRANGE benchmark (sorted set read)
    pub async fn bench_zrange(
        &self,
        redis: &mut ParityClient,
        ferris: &mut ParityClient,
    ) -> anyhow::Result<BenchmarkComparison> {
        // Setup: create a sorted set with 100 elements
        for i in 0..100 {
            let score = format!("{}", i);
            let member = format!("member{}", i);
            let _ = redis.cmd(&["ZADD", "bench:zrange", &score, &member]).await;
            let _ = ferris.cmd(&["ZADD", "bench:zrange", &score, &member]).await;
        }

        let commands: Vec<Vec<&str>> = vec![vec!["ZRANGE", "bench:zrange", "0", "49"]];
        self.run_comparison(redis, ferris, "ZRANGE (50 items)", &commands)
            .await
    }

    /// Run HGETALL benchmark (hash read all fields)
    pub async fn bench_hgetall(
        &self,
        redis: &mut ParityClient,
        ferris: &mut ParityClient,
    ) -> anyhow::Result<BenchmarkComparison> {
        let value = "x".repeat(self.config.value_size);

        // Setup: create a hash with 50 fields
        for i in 0..50 {
            let field = format!("field{}", i);
            let _ = redis.cmd(&["HSET", "bench:hgetall", &field, &value]).await;
            let _ = ferris.cmd(&["HSET", "bench:hgetall", &field, &value]).await;
        }

        let commands: Vec<Vec<&str>> = vec![vec!["HGETALL", "bench:hgetall"]];
        self.run_comparison(redis, ferris, "HGETALL (50 fields)", &commands)
            .await
    }

    /// Run SMEMBERS benchmark (set read all members)
    pub async fn bench_smembers(
        &self,
        redis: &mut ParityClient,
        ferris: &mut ParityClient,
    ) -> anyhow::Result<BenchmarkComparison> {
        // Setup: create a set with 100 members
        for i in 0..100 {
            let member = format!("member{}", i);
            let _ = redis.cmd(&["SADD", "bench:smembers", &member]).await;
            let _ = ferris.cmd(&["SADD", "bench:smembers", &member]).await;
        }

        let commands: Vec<Vec<&str>> = vec![vec!["SMEMBERS", "bench:smembers"]];
        self.run_comparison(redis, ferris, "SMEMBERS (100 members)", &commands)
            .await
    }

    /// Run large value SET benchmark
    pub async fn bench_set_large(
        &self,
        redis: &mut ParityClient,
        ferris: &mut ParityClient,
    ) -> anyhow::Result<BenchmarkComparison> {
        // 10KB value
        let large_value = "x".repeat(10 * 1024);
        let commands: Vec<Vec<&str>> = vec![vec![
            "SET",
            Box::leak("bench:large".to_string().into_boxed_str()),
            Box::leak(large_value.into_boxed_str()),
        ]];
        self.run_comparison(redis, ferris, "SET (10KB value)", &commands)
            .await
    }

    /// Run large value GET benchmark
    pub async fn bench_get_large(
        &self,
        redis: &mut ParityClient,
        ferris: &mut ParityClient,
    ) -> anyhow::Result<BenchmarkComparison> {
        // Setup: create large value
        let large_value = "x".repeat(10 * 1024);
        let _ = redis.cmd(&["SET", "bench:getlarge", &large_value]).await;
        let _ = ferris.cmd(&["SET", "bench:getlarge", &large_value]).await;

        let commands: Vec<Vec<&str>> = vec![vec!["GET", "bench:getlarge"]];
        self.run_comparison(redis, ferris, "GET (10KB value)", &commands)
            .await
    }

    /// Run RPUSH benchmark (list append - different from LPUSH)
    pub async fn bench_rpush(
        &self,
        redis: &mut ParityClient,
        ferris: &mut ParityClient,
    ) -> anyhow::Result<BenchmarkComparison> {
        let value = "x".repeat(self.config.value_size);
        let commands: Vec<Vec<&str>> = (0..10)
            .map(|i| {
                let key = format!("bench:rpush:{}", i);
                vec!["RPUSH", Box::leak(key.into_boxed_str()), &value]
            })
            .collect();

        let cmd_refs: Vec<Vec<&str>> = commands.iter().map(Vec::clone).collect();

        self.run_comparison(redis, ferris, "RPUSH", &cmd_refs).await
    }

    /// Run LPOP benchmark
    pub async fn bench_lpop(
        &self,
        redis: &mut ParityClient,
        ferris: &mut ParityClient,
    ) -> anyhow::Result<BenchmarkComparison> {
        // Setup: create lists with items
        for i in 0..10 {
            let key = format!("bench:lpop:{}", i);
            for j in 0..100 {
                let val = format!("item{}", j);
                let _ = redis.cmd(&["RPUSH", &key, &val]).await;
                let _ = ferris.cmd(&["RPUSH", &key, &val]).await;
            }
        }

        let commands: Vec<Vec<&str>> = (0..10)
            .map(|i| {
                let key = format!("bench:lpop:{}", i);
                vec!["LPOP", Box::leak(key.into_boxed_str())]
            })
            .collect();

        let cmd_refs: Vec<Vec<&str>> = commands.iter().map(Vec::clone).collect();

        self.run_comparison(redis, ferris, "LPOP", &cmd_refs).await
    }
}

/// Run all standard benchmarks and return comparisons
pub async fn run_all_benchmarks(
    redis: &mut ParityClient,
    ferris: &mut ParityClient,
    config: BenchmarkConfig,
) -> anyhow::Result<Vec<BenchmarkComparison>> {
    let benchmarker = Benchmarker::new(config);
    let mut results = Vec::new();

    // Basic operations
    results.push(benchmarker.bench_ping(redis, ferris).await?);
    results.push(benchmarker.bench_set(redis, ferris).await?);
    results.push(benchmarker.bench_get(redis, ferris).await?);
    results.push(benchmarker.bench_incr(redis, ferris).await?);

    // Multi-key operations
    results.push(benchmarker.bench_mset(redis, ferris).await?);
    results.push(benchmarker.bench_mget(redis, ferris).await?);

    // List operations
    results.push(benchmarker.bench_lpush(redis, ferris).await?);
    results.push(benchmarker.bench_rpush(redis, ferris).await?);
    results.push(benchmarker.bench_lpop(redis, ferris).await?);
    results.push(benchmarker.bench_lrange(redis, ferris).await?);

    // Hash operations
    results.push(benchmarker.bench_hset(redis, ferris).await?);
    results.push(benchmarker.bench_hgetall(redis, ferris).await?);

    // Set operations
    results.push(benchmarker.bench_sadd(redis, ferris).await?);
    results.push(benchmarker.bench_smembers(redis, ferris).await?);

    // Sorted set operations
    results.push(benchmarker.bench_zadd(redis, ferris).await?);
    results.push(benchmarker.bench_zrange(redis, ferris).await?);

    // Large value operations
    results.push(benchmarker.bench_set_large(redis, ferris).await?);
    results.push(benchmarker.bench_get_large(redis, ferris).await?);

    Ok(results)
}

/// Print a summary of all benchmark results
pub fn print_benchmark_summary(results: &[BenchmarkComparison]) {
    use colored::Colorize;

    println!("\n{}", "═".repeat(70).cyan());
    println!(
        "{}",
        " Performance Benchmark: Redis vs ferris-db ".cyan().bold()
    );
    println!("{}", "═".repeat(70).cyan());

    for result in results {
        result.print();
    }

    // Overall summary
    println!("\n{}", "═".repeat(70).cyan());
    println!("{}", " Summary ".cyan().bold());
    println!("{}", "─".repeat(70));

    let avg_throughput_ratio: f64 = results
        .iter()
        .map(BenchmarkComparison::throughput_ratio)
        .sum::<f64>()
        / results.len() as f64;
    let avg_p50_ratio: f64 = results
        .iter()
        .map(BenchmarkComparison::latency_p50_ratio)
        .sum::<f64>()
        / results.len() as f64;
    let avg_p99_ratio: f64 = results
        .iter()
        .map(BenchmarkComparison::latency_p99_ratio)
        .sum::<f64>()
        / results.len() as f64;

    let throughput_str = if avg_throughput_ratio >= 1.0 {
        format!(
            "ferris-db is {:.2}x faster on average",
            avg_throughput_ratio
        )
        .green()
    } else {
        format!(
            "ferris-db is {:.2}x slower on average",
            1.0 / avg_throughput_ratio
        )
        .red()
    };
    println!("  Throughput: {}", throughput_str);

    let p50_str = if avg_p50_ratio >= 1.0 {
        format!(
            "ferris-db has {:.2}x lower P50 latency on average",
            avg_p50_ratio
        )
        .green()
    } else {
        format!(
            "ferris-db has {:.2}x higher P50 latency on average",
            1.0 / avg_p50_ratio
        )
        .red()
    };
    println!("  Latency P50: {}", p50_str);

    let p99_str = if avg_p99_ratio >= 1.0 {
        format!(
            "ferris-db has {:.2}x lower P99 latency on average",
            avg_p99_ratio
        )
        .green()
    } else {
        format!(
            "ferris-db has {:.2}x higher P99 latency on average",
            1.0 / avg_p99_ratio
        )
        .red()
    };
    println!("  Latency P99: {}", p99_str);

    println!("{}", "═".repeat(70).cyan());
}

// ============================================================================
// Multi-threaded/Concurrent Benchmarks
// ============================================================================

/// Configuration for concurrent benchmark runs
#[derive(Debug, Clone)]
pub struct ConcurrentBenchmarkConfig {
    /// Number of concurrent clients
    pub num_clients: usize,
    /// Number of operations per client
    pub operations_per_client: usize,
    /// Duration to run the benchmark (alternative to operations_per_client)
    pub duration_secs: Option<u64>,
    /// Value size in bytes for SET/GET tests
    pub value_size: usize,
}

impl Default for ConcurrentBenchmarkConfig {
    fn default() -> Self {
        Self {
            num_clients: 10,
            operations_per_client: 1000,
            duration_secs: None,
            value_size: 100,
        }
    }
}

impl ConcurrentBenchmarkConfig {
    /// Light load configuration (10 clients, 1000 ops each)
    pub fn light() -> Self {
        Self {
            num_clients: 10,
            operations_per_client: 1000,
            duration_secs: None,
            value_size: 100,
        }
    }

    /// Medium load configuration (50 clients, 1000 ops each)
    pub fn medium() -> Self {
        Self {
            num_clients: 50,
            operations_per_client: 1000,
            duration_secs: None,
            value_size: 100,
        }
    }

    /// Heavy load configuration (100 clients, 1000 ops each)
    pub fn heavy() -> Self {
        Self {
            num_clients: 100,
            operations_per_client: 1000,
            duration_secs: None,
            value_size: 100,
        }
    }

    /// Time-based benchmark (runs for specified duration)
    pub fn timed(num_clients: usize, duration_secs: u64) -> Self {
        Self {
            num_clients,
            operations_per_client: usize::MAX, // Ignored when duration is set
            duration_secs: Some(duration_secs),
            value_size: 100,
        }
    }
}

/// Result from a concurrent benchmark run
#[derive(Debug, Clone)]
pub struct ConcurrentBenchmarkResult {
    /// Server type that was benchmarked
    pub server: ServerType,
    /// Operation name
    pub operation: String,
    /// Total operations completed across all clients
    pub total_ops: usize,
    /// Total time for the benchmark
    pub total_time: Duration,
    /// Number of concurrent clients
    pub num_clients: usize,
    /// Individual operation latencies (microseconds) from all clients
    pub latencies_us: Vec<u64>,
    /// Cached sorted latencies (thread-safe)
    sorted_latencies: std::sync::OnceLock<Vec<u64>>,
}

impl ConcurrentBenchmarkResult {
    /// Calculate operations per second (aggregate throughput)
    pub fn ops_per_sec(&self) -> f64 {
        self.total_ops as f64 / self.total_time.as_secs_f64()
    }

    /// Calculate mean latency in microseconds
    pub fn mean_latency_us(&self) -> f64 {
        if self.latencies_us.is_empty() {
            return 0.0;
        }
        let sum: u64 = self.latencies_us.iter().sum();
        sum as f64 / self.latencies_us.len() as f64
    }

    /// Get sorted latencies (cached, thread-safe)
    fn sorted_latencies(&self) -> &[u64] {
        self.sorted_latencies.get_or_init(|| {
            let mut sorted = self.latencies_us.clone();
            sorted.sort_unstable();
            sorted
        })
    }

    /// Calculate percentile latency in microseconds
    #[allow(clippy::cast_sign_loss)]
    pub fn percentile_latency_us(&self, p: f64) -> f64 {
        if self.latencies_us.is_empty() {
            return 0.0;
        }
        let sorted = self.sorted_latencies();
        let idx = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
        sorted[idx.min(sorted.len() - 1)] as f64
    }

    /// P50 latency
    pub fn p50_us(&self) -> f64 {
        self.percentile_latency_us(50.0)
    }

    /// P99 latency
    pub fn p99_us(&self) -> f64 {
        self.percentile_latency_us(99.0)
    }

    /// P99.9 latency
    pub fn p999_us(&self) -> f64 {
        self.percentile_latency_us(99.9)
    }

    /// Min latency
    pub fn min_latency_us(&self) -> f64 {
        self.latencies_us.iter().min().copied().unwrap_or(0) as f64
    }

    /// Max latency
    pub fn max_latency_us(&self) -> f64 {
        self.latencies_us.iter().max().copied().unwrap_or(0) as f64
    }
}

/// Comparison of concurrent benchmark results
#[derive(Debug, Clone)]
pub struct ConcurrentBenchmarkComparison {
    /// Operation name
    pub operation: String,
    /// Redis results
    pub redis: ConcurrentBenchmarkResult,
    /// ferris-db results
    pub ferris: ConcurrentBenchmarkResult,
}

impl ConcurrentBenchmarkComparison {
    /// Create a new comparison
    pub fn new(
        redis: ConcurrentBenchmarkResult,
        ferris: ConcurrentBenchmarkResult,
    ) -> Self {
        assert_eq!(redis.operation, ferris.operation);
        Self {
            operation: redis.operation.clone(),
            redis,
            ferris,
        }
    }

    /// Throughput ratio (ferris / redis). >1.0 means ferris is faster
    pub fn throughput_ratio(&self) -> f64 {
        self.ferris.ops_per_sec() / self.redis.ops_per_sec()
    }

    /// Latency ratio for P50 (redis / ferris). >1.0 means ferris is faster
    pub fn latency_p50_ratio(&self) -> f64 {
        if self.ferris.p50_us() == 0.0 {
            return 1.0;
        }
        self.redis.p50_us() / self.ferris.p50_us()
    }

    /// Latency ratio for P99 (redis / ferris). >1.0 means ferris is faster
    pub fn latency_p99_ratio(&self) -> f64 {
        if self.ferris.p99_us() == 0.0 {
            return 1.0;
        }
        self.redis.p99_us() / self.ferris.p99_us()
    }

    /// Print a formatted comparison
    pub fn print(&self) {
        use colored::Colorize;

        println!("\n{} ({}× concurrent clients):", self.operation.bold(), self.redis.num_clients);
        println!("{}", "─".repeat(70));

        // Aggregate Throughput
        let ratio = self.throughput_ratio();
        let ratio_str = if ratio >= 1.0 {
            format!("{:.2}x faster", ratio).green()
        } else {
            format!("{:.2}x slower", 1.0 / ratio).red()
        };
        println!(
            "  Throughput:  Redis {:>12.0} ops/s  |  ferris-db {:>12.0} ops/s  ({})",
            self.redis.ops_per_sec(),
            self.ferris.ops_per_sec(),
            ratio_str
        );

        // P50 Latency
        let p50_ratio = self.latency_p50_ratio();
        let p50_str = if p50_ratio >= 1.0 {
            format!("{:.2}x lower", p50_ratio).green()
        } else {
            format!("{:.2}x higher", 1.0 / p50_ratio).red()
        };
        println!(
            "  Latency P50: Redis {:>10.1} μs     |  ferris-db {:>10.1} μs     ({})",
            self.redis.p50_us(),
            self.ferris.p50_us(),
            p50_str
        );

        // P99 Latency
        let p99_ratio = self.latency_p99_ratio();
        let p99_str = if p99_ratio >= 1.0 {
            format!("{:.2}x lower", p99_ratio).green()
        } else {
            format!("{:.2}x higher", 1.0 / p99_ratio).red()
        };
        println!(
            "  Latency P99: Redis {:>10.1} μs     |  ferris-db {:>10.1} μs     ({})",
            self.redis.p99_us(),
            self.ferris.p99_us(),
            p99_str
        );

        // P99.9 Latency (important for concurrent workloads)
        let p99_9_ratio = if self.ferris.p999_us() == 0.0 {
            1.0
        } else {
            self.redis.p999_us() / self.ferris.p999_us()
        };
        let p99_9_display = if p99_9_ratio >= 1.0 {
            format!("{:.2}x lower", p99_9_ratio).green()
        } else {
            format!("{:.2}x higher", 1.0 / p99_9_ratio).red()
        };
        println!(
            "  Latency P99.9: Redis {:>8.1} μs     |  ferris-db {:>10.1} μs     ({})",
            self.redis.p999_us(),
            self.ferris.p999_us(),
            p99_9_display
        );
    }
}

/// Concurrent benchmark runner
pub struct ConcurrentBenchmarker {
    config: ConcurrentBenchmarkConfig,
}

impl ConcurrentBenchmarker {
    /// Create a new concurrent benchmarker
    pub fn new(config: ConcurrentBenchmarkConfig) -> Self {
        Self { config }
    }

    /// Run a concurrent benchmark on a single server
    async fn run_single(
        &self,
        server_type: ServerType,
        operation: &str,
        command_generator: impl Fn(usize) -> Vec<String> + Send + Sync + 'static,
    ) -> anyhow::Result<ConcurrentBenchmarkResult> {
        use std::sync::Arc;
        use tokio::sync::Mutex;

        let num_clients = self.config.num_clients;
        let ops_per_client = self.config.operations_per_client;

        // Shared result collection (thread-safe)
        let all_latencies = Arc::new(Mutex::new(Vec::new()));
        let total_ops = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        // Wrap command generator in Arc for sharing
        let command_gen = Arc::new(command_generator);

        let overall_start = Instant::now();

        // Spawn concurrent client tasks
        let mut tasks = Vec::new();

        for client_id in 0..num_clients {
            let latencies = Arc::clone(&all_latencies);
            let ops_counter = Arc::clone(&total_ops);
            let command_gen = Arc::clone(&command_gen);
            let duration_secs = self.config.duration_secs;

            let task = tokio::spawn(async move {
                // Each client gets its own connection
                let mut client = match server_type {
                    ServerType::Redis => ParityClient::connect_redis().await,
                    ServerType::Ferris => ParityClient::connect_ferris().await,
                }?;

                let mut client_latencies = Vec::new();
                let mut ops = 0;

                let start_time = Instant::now();

                loop {
                    // Check if we should stop (either by ops count or duration)
                    if let Some(duration) = duration_secs {
                        if start_time.elapsed().as_secs() >= duration {
                            break;
                        }
                    } else if ops >= ops_per_client {
                        break;
                    }

                    // Generate command for this client
                    let cmd_strings = command_gen(client_id);
                    let cmd: Vec<&str> = cmd_strings.iter().map(String::as_str).collect();

                    // Execute and measure
                    let op_start = Instant::now();
                    if client.cmd(&cmd).await.is_ok() {
                        let latency_us = op_start.elapsed().as_micros() as u64;
                        client_latencies.push(latency_us);
                        ops += 1;
                    }
                    // Skip failed operations
                }

                // Merge this client's results into shared results
                latencies.lock().await.extend(client_latencies);
                ops_counter.fetch_add(ops, std::sync::atomic::Ordering::Relaxed);

                Ok::<(), anyhow::Error>(())
            });

            tasks.push(task);
        }

        // Wait for all clients to complete
        for task in tasks {
            task.await??;
        }

        let total_time = overall_start.elapsed();
        let latencies = all_latencies.lock().await.clone();
        let total_ops = total_ops.load(std::sync::atomic::Ordering::Relaxed);

        Ok(ConcurrentBenchmarkResult {
            server: server_type,
            operation: operation.to_string(),
            total_ops,
            total_time,
            num_clients,
            latencies_us: latencies,
            sorted_latencies: std::sync::OnceLock::new(),
        })
    }

    /// Run concurrent SET benchmark
    pub async fn bench_set_concurrent(
        &self,
    ) -> anyhow::Result<ConcurrentBenchmarkComparison> {
        let value = "x".repeat(self.config.value_size);
        let value = Arc::new(value);

        let command_gen = {
            let value = Arc::clone(&value);
            move |client_id: usize| {
                let key = format!("bench:concurrent:set:{}:{}", client_id, rand::random::<u32>());
                vec!["SET".to_string(), key, value.as_ref().clone()]
            }
        };

        let redis_result = self
            .run_single(ServerType::Redis, "SET (concurrent)", command_gen.clone())
            .await?;

        let ferris_result = self
            .run_single(ServerType::Ferris, "SET (concurrent)", command_gen)
            .await?;

        Ok(ConcurrentBenchmarkComparison::new(
            redis_result,
            ferris_result,
        ))
    }

    /// Run concurrent GET benchmark
    pub async fn bench_get_concurrent(
        &self,
    ) -> anyhow::Result<ConcurrentBenchmarkComparison> {
        // Pre-populate keys for GET
        let num_keys = 1000;
        let value = "x".repeat(self.config.value_size);

        // Setup keys in both servers
        let mut redis = ParityClient::connect_redis().await?;
        let mut ferris = ParityClient::connect_ferris().await?;

        for i in 0..num_keys {
            let key = format!("bench:concurrent:get:{}", i);
            let _ = redis.cmd(&["SET", &key, &value]).await;
            let _ = ferris.cmd(&["SET", &key, &value]).await;
        }

        let command_gen = move |_client_id: usize| {
            let key_id = rand::random::<usize>() % num_keys;
            let key = format!("bench:concurrent:get:{}", key_id);
            vec!["GET".to_string(), key]
        };

        let redis_result = self
            .run_single(ServerType::Redis, "GET (concurrent)", command_gen)
            .await?;

        let ferris_result = self
            .run_single(ServerType::Ferris, "GET (concurrent)", command_gen)
            .await?;

        Ok(ConcurrentBenchmarkComparison::new(
            redis_result,
            ferris_result,
        ))
    }

    /// Run concurrent INCR benchmark
    pub async fn bench_incr_concurrent(
        &self,
    ) -> anyhow::Result<ConcurrentBenchmarkComparison> {
        let num_counters = 100;

        let command_gen = move |_client_id: usize| {
            let counter_id = rand::random::<usize>() % num_counters;
            let key = format!("bench:concurrent:incr:{}", counter_id);
            vec!["INCR".to_string(), key]
        };

        let redis_result = self
            .run_single(ServerType::Redis, "INCR (concurrent)", command_gen)
            .await?;

        let ferris_result = self
            .run_single(ServerType::Ferris, "INCR (concurrent)", command_gen)
            .await?;

        Ok(ConcurrentBenchmarkComparison::new(
            redis_result,
            ferris_result,
        ))
    }

    /// Run concurrent mixed workload (50% GET, 50% SET)
    pub async fn bench_mixed_concurrent(
        &self,
    ) -> anyhow::Result<ConcurrentBenchmarkComparison> {
        let num_keys = 1000;
        let value = Arc::new("x".repeat(self.config.value_size));

        // Pre-populate some keys
        let mut redis = ParityClient::connect_redis().await?;
        let mut ferris = ParityClient::connect_ferris().await?;

        for i in 0..num_keys {
            let key = format!("bench:concurrent:mixed:{}", i);
            let _ = redis.cmd(&["SET", &key, value.as_ref()]).await;
            let _ = ferris.cmd(&["SET", &key, value.as_ref()]).await;
        }

        let command_gen = {
            let value = Arc::clone(&value);
            move |_client_id: usize| {
                let key_id = rand::random::<usize>() % num_keys;
                let key = format!("bench:concurrent:mixed:{}", key_id);

                // 50% GET, 50% SET
                if rand::random::<bool>() {
                    vec!["GET".to_string(), key]
                } else {
                    vec!["SET".to_string(), key, value.as_ref().clone()]
                }
            }
        };

        let redis_result = self
            .run_single(ServerType::Redis, "Mixed 50/50 (concurrent)", command_gen.clone())
            .await?;

        let ferris_result = self
            .run_single(ServerType::Ferris, "Mixed 50/50 (concurrent)", command_gen)
            .await?;

        Ok(ConcurrentBenchmarkComparison::new(
            redis_result,
            ferris_result,
        ))
    }
}

/// Run all concurrent benchmarks
pub async fn run_all_concurrent_benchmarks(
    config: ConcurrentBenchmarkConfig,
) -> anyhow::Result<Vec<ConcurrentBenchmarkComparison>> {
    let benchmarker = ConcurrentBenchmarker::new(config);
    let mut results = Vec::new();

    println!("  Running SET (concurrent)...");
    results.push(benchmarker.bench_set_concurrent().await?);

    println!("  Running GET (concurrent)...");
    results.push(benchmarker.bench_get_concurrent().await?);

    println!("  Running INCR (concurrent)...");
    results.push(benchmarker.bench_incr_concurrent().await?);

    println!("  Running Mixed 50/50 (concurrent)...");
    results.push(benchmarker.bench_mixed_concurrent().await?);

    Ok(results)
}

/// Print summary of concurrent benchmark results
pub fn print_concurrent_benchmark_summary(results: &[ConcurrentBenchmarkComparison]) {
    use colored::Colorize;

    println!("\n{}", "═".repeat(70).cyan());
    println!(
        "{}",
        " Concurrent Performance Benchmark: Redis vs ferris-db "
            .cyan()
            .bold()
    );
    println!("{}", "═".repeat(70).cyan());

    for result in results {
        result.print();
    }

    // Overall summary
    println!("\n{}", "═".repeat(70).cyan());
    println!("{}", " Summary ".cyan().bold());
    println!("{}", "─".repeat(70));

    let avg_throughput_ratio: f64 = results
        .iter()
        .map(ConcurrentBenchmarkComparison::throughput_ratio)
        .sum::<f64>()
        / results.len() as f64;
    let avg_p50_ratio: f64 = results
        .iter()
        .map(ConcurrentBenchmarkComparison::latency_p50_ratio)
        .sum::<f64>()
        / results.len() as f64;
    let avg_p99_ratio: f64 = results
        .iter()
        .map(ConcurrentBenchmarkComparison::latency_p99_ratio)
        .sum::<f64>()
        / results.len() as f64;

    let throughput_str = if avg_throughput_ratio >= 1.0 {
        format!(
            "ferris-db is {:.2}x faster on average",
            avg_throughput_ratio
        )
        .green()
    } else {
        format!(
            "ferris-db is {:.2}x slower on average",
            1.0 / avg_throughput_ratio
        )
        .red()
    };
    println!("  Aggregate Throughput: {}", throughput_str);

    let p50_str = if avg_p50_ratio >= 1.0 {
        format!(
            "ferris-db has {:.2}x lower P50 latency on average",
            avg_p50_ratio
        )
        .green()
    } else {
        format!(
            "ferris-db has {:.2}x higher P50 latency on average",
            1.0 / avg_p50_ratio
        )
        .red()
    };
    println!("  Latency P50: {}", p50_str);

    let p99_str = if avg_p99_ratio >= 1.0 {
        format!(
            "ferris-db has {:.2}x lower P99 latency on average",
            avg_p99_ratio
        )
        .green()
    } else {
        format!(
            "ferris-db has {:.2}x higher P99 latency on average",
            1.0 / avg_p99_ratio
        )
        .red()
    };
    println!("  Latency P99: {}", p99_str);

    println!("{}", "═".repeat(70).cyan());
}

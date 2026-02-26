//! Performance benchmarking for Redis vs ferris-db comparison
//!
//! Provides tools to measure and compare throughput and latency between
//! Redis and ferris-db for various operations.

use crate::client::{ParityClient, ServerType};
use crate::report::PerformanceReport;
use std::time::{Duration, Instant};

/// Configuration for benchmark runs
#[derive(Debug, Clone)]
pub struct BenchmarkConfig {
    /// Number of warmup iterations (not counted)
    pub warmup_iterations: usize,
    /// Number of iterations to measure
    pub iterations: usize,
    /// Number of parallel connections (for throughput tests)
    pub parallelism: usize,
    /// Value size in bytes for SET/GET tests
    pub value_size: usize,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            warmup_iterations: 100,
            iterations: 1000,
            parallelism: 1,
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
            parallelism: 1,
            value_size: 100,
        }
    }

    /// Thorough benchmark configuration (more iterations)
    pub fn thorough() -> Self {
        Self {
            warmup_iterations: 500,
            iterations: 10000,
            parallelism: 1,
            value_size: 100,
        }
    }
}

/// Result from a single benchmark run
#[derive(Debug, Clone)]
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

    /// Calculate percentile latency in microseconds
    #[allow(clippy::cast_sign_loss)]
    pub fn percentile_latency_us(&self, p: f64) -> f64 {
        if self.latencies_us.is_empty() {
            return 0.0;
        }
        let mut sorted = self.latencies_us.clone();
        sorted.sort_unstable();
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
        for _ in 0..self.config.warmup_iterations {
            for cmd in commands {
                let _ = client.cmd(cmd).await;
            }
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

        // Run Redis benchmark
        let redis_result = self.run_single(redis, operation, commands).await?;

        // Flush again and run ferris-db benchmark
        let _ = redis.cmd(&["FLUSHDB"]).await;
        let _ = ferris.cmd(&["FLUSHDB"]).await;

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
        // MSET with 10 keys at once
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

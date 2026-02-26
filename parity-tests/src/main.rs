//! Redis Parity Test Runner
//!
//! This binary runs comprehensive parity tests between Redis and ferris-db.
//!
//! Usage:
//!   parity-tests              # Run parity tests only
//!   parity-tests --benchmark  # Run parity tests + performance benchmarks
//!   parity-tests --bench-only # Run performance benchmarks only
//!   parity-tests --quick      # Run quick benchmarks (fewer iterations)
//!   parity-tests --thorough   # Run thorough benchmarks (more iterations)

#![allow(clippy::too_many_lines)]
#![allow(clippy::uninlined_format_args)]
#![allow(clippy::cast_possible_truncation)]

use parity_tests::benchmark::{print_benchmark_summary, run_all_benchmarks, BenchmarkConfig};
use parity_tests::commands::{
    edge_cases, hash, key, list, server, set, sorted_set, sorted_set_edge, string,
};
use parity_tests::report::ParityReport;
use parity_tests::{DualClient, ParityClient};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunMode {
    ParityOnly,
    ParityAndBenchmark,
    BenchmarkOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BenchmarkMode {
    Default,
    Quick,
    Thorough,
}

fn parse_args() -> (RunMode, BenchmarkMode) {
    let args: Vec<String> = std::env::args().collect();

    let mut run_mode = RunMode::ParityOnly;
    let mut bench_mode = BenchmarkMode::Default;

    for arg in &args[1..] {
        match arg.as_str() {
            "--benchmark" | "-b" => run_mode = RunMode::ParityAndBenchmark,
            "--bench-only" => run_mode = RunMode::BenchmarkOnly,
            "--quick" | "-q" => {
                bench_mode = BenchmarkMode::Quick;
                if run_mode == RunMode::ParityOnly {
                    run_mode = RunMode::ParityAndBenchmark;
                }
            }
            "--thorough" | "-t" => {
                bench_mode = BenchmarkMode::Thorough;
                if run_mode == RunMode::ParityOnly {
                    run_mode = RunMode::ParityAndBenchmark;
                }
            }
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            _ => {
                eprintln!("Unknown argument: {}", arg);
                print_help();
                std::process::exit(1);
            }
        }
    }

    (run_mode, bench_mode)
}

fn print_help() {
    println!("Redis Parity Test Suite for ferris-db");
    println!();
    println!("Usage: parity-tests [OPTIONS]");
    println!();
    println!("Options:");
    println!("  --benchmark, -b   Run parity tests followed by performance benchmarks");
    println!("  --bench-only      Run performance benchmarks only (skip parity tests)");
    println!("  --quick, -q       Run quick benchmarks (500 iterations)");
    println!("  --thorough, -t    Run thorough benchmarks (10000 iterations)");
    println!("  --help, -h        Show this help message");
    println!();
    println!("Examples:");
    println!("  parity-tests                    # Run parity tests only");
    println!("  parity-tests --benchmark        # Run parity tests + benchmarks");
    println!("  parity-tests --bench-only -q    # Run quick benchmarks only");
    println!("  parity-tests -t                 # Run parity + thorough benchmarks");
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (run_mode, bench_mode) = parse_args();

    println!("Redis Parity Test Suite for ferris-db");
    println!("=====================================");
    println!();

    // Try to connect to both servers
    println!("Connecting to servers...");
    let mut client = match DualClient::connect().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to connect to servers: {}", e);
            eprintln!();
            eprintln!("Make sure both servers are running:");
            eprintln!("  Redis:     redis-server --port 6379");
            eprintln!("  ferris-db: cargo run --release -p ferris-server -- --port 6380");
            std::process::exit(1);
        }
    };

    // Verify connectivity
    match client.ping_both().await {
        Ok((true, true)) => {
            println!("  Redis:     Connected on port 6379");
            println!("  ferris-db: Connected on port 6380");
        }
        Ok((redis, ferris)) => {
            eprintln!("Ping failed - Redis: {}, ferris-db: {}", redis, ferris);
            std::process::exit(1);
        }
        Err(e) => {
            eprintln!("Failed to ping servers: {}", e);
            std::process::exit(1);
        }
    }
    println!();

    // Run parity tests if requested
    let parity_failed = if run_mode == RunMode::BenchmarkOnly {
        false
    } else {
        run_parity_tests(&mut client).await
    };

    // Run benchmarks if requested
    if run_mode != RunMode::ParityOnly {
        run_benchmarks(bench_mode).await?;
    }

    // Exit with appropriate code
    if parity_failed {
        std::process::exit(1);
    }

    Ok(())
}

async fn run_parity_tests(client: &mut DualClient) -> bool {
    let mut report = ParityReport::new();
    let start = std::time::Instant::now();

    // Run all command category tests
    println!("Running parity tests...");
    println!();

    // String commands
    print!("  String commands... ");
    let string_report = string::run_all(client).await;
    println!(
        "{}/{} passed",
        string_report.passed,
        string_report.passed + string_report.failed
    );
    report.add_category(string_report);

    // List commands
    print!("  List commands... ");
    let list_report = list::run_all(client).await;
    println!(
        "{}/{} passed",
        list_report.passed,
        list_report.passed + list_report.failed
    );
    report.add_category(list_report);

    // Set commands
    print!("  Set commands... ");
    let set_report = set::run_all(client).await;
    println!(
        "{}/{} passed",
        set_report.passed,
        set_report.passed + set_report.failed
    );
    report.add_category(set_report);

    // Hash commands
    print!("  Hash commands... ");
    let hash_report = hash::run_all(client).await;
    println!(
        "{}/{} passed",
        hash_report.passed,
        hash_report.passed + hash_report.failed
    );
    report.add_category(hash_report);

    // Sorted set commands
    print!("  Sorted Set commands... ");
    let zset_report = sorted_set::run_all(client).await;
    println!(
        "{}/{} passed",
        zset_report.passed,
        zset_report.passed + zset_report.failed
    );
    report.add_category(zset_report);

    // Key commands
    print!("  Key commands... ");
    let key_report = key::run_all(client).await;
    println!(
        "{}/{} passed",
        key_report.passed,
        key_report.passed + key_report.failed
    );
    report.add_category(key_report);

    // Server commands
    print!("  Server commands... ");
    let server_report = server::run_all(client).await;
    println!(
        "{}/{} passed",
        server_report.passed,
        server_report.passed + server_report.failed
    );
    report.add_category(server_report);

    // Edge cases
    print!("  Edge Cases... ");
    let edge_report = edge_cases::run_all(client).await;
    println!(
        "{}/{} passed",
        edge_report.passed,
        edge_report.passed + edge_report.failed
    );
    report.add_category(edge_report);

    // Sorted Set Edge Cases
    print!("  Sorted Set Edge Cases... ");
    let zset_edge_report = sorted_set_edge::run_all(client).await;
    println!(
        "{}/{} passed",
        zset_edge_report.passed,
        zset_edge_report.passed + zset_edge_report.failed
    );
    report.add_category(zset_edge_report);

    // Update total duration
    report.duration_ms = start.elapsed().as_millis() as u64;

    // Print summary
    report.print_summary();

    // Save JSON report
    let report_path = "parity-report.json";
    if let Err(e) = report.save_json(report_path) {
        eprintln!("Warning: Failed to save report to {}: {}", report_path, e);
    } else {
        println!("Report saved to: {}", report_path);
    }

    report.failed > 0
}

async fn run_benchmarks(mode: BenchmarkMode) -> anyhow::Result<()> {
    println!();
    println!("Running performance benchmarks...");

    let config = match mode {
        BenchmarkMode::Quick => {
            println!("  Mode: Quick (500 iterations)");
            BenchmarkConfig::quick()
        }
        BenchmarkMode::Default => {
            println!("  Mode: Default (1000 iterations)");
            BenchmarkConfig::default()
        }
        BenchmarkMode::Thorough => {
            println!("  Mode: Thorough (10000 iterations)");
            BenchmarkConfig::thorough()
        }
    };

    // Create fresh connections for benchmarking
    let mut redis = ParityClient::connect_redis().await?;
    let mut ferris = ParityClient::connect_ferris().await?;

    println!("  Running benchmarks...");

    let results = run_all_benchmarks(&mut redis, &mut ferris, config).await?;

    // Print results
    print_benchmark_summary(&results);

    // Save benchmark results to JSON
    let bench_report: Vec<_> = results
        .iter()
        .map(|r| {
            serde_json::json!({
                "operation": r.operation,
                "redis_ops_per_sec": r.redis.ops_per_sec(),
                "ferris_ops_per_sec": r.ferris.ops_per_sec(),
                "throughput_ratio": r.throughput_ratio(),
                "redis_p50_us": r.redis.p50_us(),
                "ferris_p50_us": r.ferris.p50_us(),
                "latency_p50_ratio": r.latency_p50_ratio(),
                "redis_p99_us": r.redis.p99_us(),
                "ferris_p99_us": r.ferris.p99_us(),
                "latency_p99_ratio": r.latency_p99_ratio(),
            })
        })
        .collect();

    let json = serde_json::to_string_pretty(&bench_report)?;
    std::fs::write("benchmark-report.json", json)?;
    println!("\nBenchmark report saved to: benchmark-report.json");

    Ok(())
}

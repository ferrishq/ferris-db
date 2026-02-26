//! Redis Parity Test Runner
//!
//! This binary runs comprehensive parity tests between Redis and ferris-db.

#![allow(clippy::too_many_lines)]
#![allow(clippy::uninlined_format_args)]
#![allow(clippy::cast_possible_truncation)]

use parity_tests::commands::{
    edge_cases, hash, key, list, server, set, sorted_set, sorted_set_edge, string,
};
use parity_tests::report::ParityReport;
use parity_tests::DualClient;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
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

    let mut report = ParityReport::new();
    let start = std::time::Instant::now();

    // Run all command category tests
    println!("Running parity tests...");
    println!();

    // String commands
    print!("  String commands... ");
    let string_report = string::run_all(&mut client).await;
    println!(
        "{}/{} passed",
        string_report.passed,
        string_report.passed + string_report.failed
    );
    report.add_category(string_report);

    // List commands
    print!("  List commands... ");
    let list_report = list::run_all(&mut client).await;
    println!(
        "{}/{} passed",
        list_report.passed,
        list_report.passed + list_report.failed
    );
    report.add_category(list_report);

    // Set commands
    print!("  Set commands... ");
    let set_report = set::run_all(&mut client).await;
    println!(
        "{}/{} passed",
        set_report.passed,
        set_report.passed + set_report.failed
    );
    report.add_category(set_report);

    // Hash commands
    print!("  Hash commands... ");
    let hash_report = hash::run_all(&mut client).await;
    println!(
        "{}/{} passed",
        hash_report.passed,
        hash_report.passed + hash_report.failed
    );
    report.add_category(hash_report);

    // Sorted set commands
    print!("  Sorted Set commands... ");
    let zset_report = sorted_set::run_all(&mut client).await;
    println!(
        "{}/{} passed",
        zset_report.passed,
        zset_report.passed + zset_report.failed
    );
    report.add_category(zset_report);

    // Key commands
    print!("  Key commands... ");
    let key_report = key::run_all(&mut client).await;
    println!(
        "{}/{} passed",
        key_report.passed,
        key_report.passed + key_report.failed
    );
    report.add_category(key_report);

    // Server commands
    print!("  Server commands... ");
    let server_report = server::run_all(&mut client).await;
    println!(
        "{}/{} passed",
        server_report.passed,
        server_report.passed + server_report.failed
    );
    report.add_category(server_report);

    // Edge cases
    print!("  Edge Cases... ");
    let edge_report = edge_cases::run_all(&mut client).await;
    println!(
        "{}/{} passed",
        edge_report.passed,
        edge_report.passed + edge_report.failed
    );
    report.add_category(edge_report);

    // Sorted Set Edge Cases
    print!("  Sorted Set Edge Cases... ");
    let zset_edge_report = sorted_set_edge::run_all(&mut client).await;
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

    // Exit with appropriate code
    if report.failed > 0 {
        std::process::exit(1);
    }

    Ok(())
}

//! ferris-db server binary
//!
//! A high-performance, multi-threaded, Redis-compatible distributed key-value store.

#![deny(unsafe_code)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::match_same_arms)]
#![allow(clippy::unnecessary_wraps)]

// Use jemalloc as the global allocator for better performance on large allocations
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use bytes::Bytes;
use clap::Parser;
use ferris_core::{Entry, ExpiryManager, KeyStore, RedisValue};
use ferris_network::{Server, ServerConfig};
use ferris_persistence::{AofConfig, AofReader, AofWriter, FsyncMode};
use ferris_protocol::RespValue;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{info, warn, Level};
use tracing_subscriber::FmtSubscriber;

/// ferris-db: A Redis-compatible distributed key-value store
#[derive(Parser, Debug)]
#[command(name = "ferris-db")]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Port to listen on
    #[arg(short, long, default_value_t = 6380)]
    port: u16,

    /// Bind address
    #[arg(short, long, default_value = "127.0.0.1")]
    bind: String,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Configuration file path
    #[arg(short, long)]
    config: Option<String>,

    /// Enable AOF persistence
    #[arg(long, default_value_t = true)]
    aof: bool,

    /// AOF file path
    #[arg(long, default_value = "ferris-db.aof")]
    aof_file: String,

    /// Require password for connections
    #[arg(long)]
    requirepass: Option<String>,

    /// Maximum memory in bytes (0 = unlimited)
    #[arg(long, default_value_t = 0)]
    maxmemory: usize,

    /// Maximum number of concurrent connections
    #[arg(long, default_value_t = 10000)]
    maxclients: usize,

    /// Connection idle timeout in seconds (0 = no timeout)
    #[arg(long, default_value_t = 0)]
    timeout: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Set up logging
    let log_level = match args.log_level.to_lowercase().as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };

    let subscriber = FmtSubscriber::builder()
        .with_max_level(log_level)
        .with_target(false)
        .with_thread_ids(true)
        .finish();

    tracing::subscriber::set_global_default(subscriber)?;

    let bind_addr = format!("{}:{}", args.bind, args.port);

    info!("Starting ferris-db server v{}", env!("CARGO_PKG_VERSION"));
    info!("Bind address: {}", bind_addr);
    info!(
        "AOF persistence: {}",
        if args.aof { "enabled" } else { "disabled" }
    );

    // Initialize the key store
    let store = Arc::new(KeyStore::default());

    // AOF: Replay commands if AOF is enabled
    if args.aof {
        let aof_reader = AofReader::new(&args.aof_file);
        if aof_reader.exists() {
            info!(path = %args.aof_file, "Replaying AOF file");
            let store_clone = Arc::clone(&store);
            let command_count = aof_reader.replay(move |db, command| {
                // Execute command on the store
                // For replay, we just need to recreate the data structures
                // We don't need full command execution
                replay_command(&store_clone, db, &command)
            })?;
            info!(commands = command_count, "AOF replay complete");
        } else {
            info!("No AOF file found, starting with empty database");
        }
    }

    // AOF: Initialize writer if enabled
    let aof_writer = if args.aof {
        let aof_config = AofConfig {
            file_path: PathBuf::from(&args.aof_file),
            fsync_mode: FsyncMode::EverySecond,
            buffer_size: 64 * 1024,
            channel_capacity: 1024,
        };
        match AofWriter::new(aof_config) {
            Ok(writer) => {
                info!(path = %args.aof_file, "AOF writer initialized");
                Some(Arc::new(writer))
            }
            Err(e) => {
                warn!(error = %e, "Failed to initialize AOF writer, continuing without persistence");
                None
            }
        }
    } else {
        None
    };

    // Initialize the expiry manager and spawn the background task
    let expiry_manager = Arc::new(ExpiryManager::new());
    let expiry_store = store.clone();
    let expiry_handle = expiry_manager.clone();
    let expiry_task = tokio::spawn(async move {
        expiry_handle.run(expiry_store).await;
    });

    // Configure and create the server
    let server_config = ServerConfig {
        bind_addr,
        max_connections: args.maxclients,
        timeout: args.timeout,
    };
    let server = if let Some(ref aof) = aof_writer {
        Server::with_aof(server_config, store, Arc::clone(aof))
    } else {
        Server::new(server_config, store)
    };
    let shutdown_tx = server.shutdown_handle();

    info!("Server ready to accept connections");

    // Spawn the server in a task so we can listen for signals
    let server_task = tokio::spawn(async move {
        if let Err(e) = server.run().await {
            tracing::error!(error = %e, "Server error");
        }
    });

    // Wait for Ctrl+C (SIGINT) or SIGTERM
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = signal(SignalKind::terminate())?;
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Received SIGINT (Ctrl+C), shutting down...");
            }
            _ = sigterm.recv() => {
                info!("Received SIGTERM, shutting down...");
            }
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await?;
        info!("Received Ctrl+C, shutting down...");
    }

    // Send shutdown signal to the server (connections will drain)
    let _ = shutdown_tx.send(());

    // Shutdown the expiry manager
    expiry_manager.shutdown();

    // Shutdown AOF writer
    if let Some(aof) = aof_writer {
        info!("Shutting down AOF writer...");
        if let Ok(writer) = Arc::try_unwrap(aof) {
            if let Err(e) = writer.shutdown().await {
                warn!(error = %e, "Error shutting down AOF writer");
            }
        } else {
            warn!("Could not shutdown AOF writer (still has references)");
        }
    }

    // Wait for tasks to complete (with timeout)
    let _ = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let _ = server_task.await;
        let _ = expiry_task.await;
    })
    .await;

    info!("ferris-db server stopped. Goodbye!");

    Ok(())
}

/// Replay a command from AOF file
///
/// This is a simplified command execution for AOF replay.
/// We only need to recreate the data structures, not run full command logic.
fn replay_command(
    store: &KeyStore,
    db_index: usize,
    command: &[RespValue],
) -> Result<(), ferris_persistence::PersistenceError> {
    // Parse command name
    let cmd_name = match command.first() {
        Some(RespValue::BulkString(name)) => String::from_utf8_lossy(name).to_uppercase(),
        _ => return Ok(()), // Skip invalid commands
    };

    let db = store.database(db_index);

    // Handle common write commands for replay
    match cmd_name.as_str() {
        "SET" => {
            if command.len() >= 3 {
                if let (Some(RespValue::BulkString(key)), Some(RespValue::BulkString(value))) =
                    (command.get(1), command.get(2))
                {
                    db.set(key.clone(), Entry::new(RedisValue::String(value.clone())));
                }
            }
        }
        "DEL" => {
            for arg in command.iter().skip(1) {
                if let RespValue::BulkString(key) = arg {
                    db.delete(key);
                }
            }
        }
        "LPUSH" | "RPUSH" => {
            if command.len() >= 3 {
                if let Some(RespValue::BulkString(key)) = command.get(1) {
                    // Get or create list
                    let values: Vec<Bytes> = command
                        .iter()
                        .skip(2)
                        .filter_map(|v| {
                            if let RespValue::BulkString(b) = v {
                                Some(b.clone())
                            } else {
                                None
                            }
                        })
                        .collect();

                    if !values.is_empty() {
                        let mut list = db
                            .get(key)
                            .and_then(|entry| {
                                if let RedisValue::List(l) = &entry.value {
                                    Some(l.clone())
                                } else {
                                    None
                                }
                            })
                            .unwrap_or_default();

                        for value in values {
                            if cmd_name == "LPUSH" {
                                list.push_front(value);
                            } else {
                                list.push_back(value);
                            }
                        }
                        db.set(key.clone(), Entry::new(RedisValue::List(list)));
                    }
                }
            }
        }
        "SADD" => {
            if command.len() >= 3 {
                if let Some(RespValue::BulkString(key)) = command.get(1) {
                    let mut set = db
                        .get(key)
                        .and_then(|entry| {
                            if let RedisValue::Set(s) = &entry.value {
                                Some(s.clone())
                            } else {
                                None
                            }
                        })
                        .unwrap_or_default();

                    for arg in command.iter().skip(2) {
                        if let RespValue::BulkString(member) = arg {
                            set.insert(member.clone());
                        }
                    }
                    db.set(key.clone(), Entry::new(RedisValue::Set(set)));
                }
            }
        }
        "HSET" => {
            if command.len() >= 4 && command.len() % 2 == 0 {
                if let Some(RespValue::BulkString(key)) = command.get(1) {
                    let mut hash = db
                        .get(key)
                        .and_then(|entry| {
                            if let RedisValue::Hash(h) = &entry.value {
                                Some(h.clone())
                            } else {
                                None
                            }
                        })
                        .unwrap_or_default();

                    let mut i = 2;
                    while i + 1 < command.len() {
                        if let (
                            Some(RespValue::BulkString(field)),
                            Some(RespValue::BulkString(value)),
                        ) = (command.get(i), command.get(i + 1))
                        {
                            hash.insert(field.clone(), value.clone());
                        }
                        i += 2;
                    }
                    db.set(key.clone(), Entry::new(RedisValue::Hash(hash)));
                }
            }
        }
        // Add more commands as needed for complete replay
        _ => {
            // For other commands, we can ignore during replay
            // or implement them as needed
        }
    }

    Ok(())
}

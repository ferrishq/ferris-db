//! TCP server listener
//!
//! Binds to a port, accepts connections, and spawns per-connection tasks.

use crate::connection::handle_connection;
use crate::shutdown::Shutdown;
use ferris_commands::CommandExecutor;
use ferris_core::{BlockingRegistry, KeyStore};
use ferris_persistence::AofWriter;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tracing::{error, info, warn};

/// Errors that can occur when running the server.
#[derive(Error, Debug)]
pub enum ServerError {
    #[error("Failed to bind to {addr}: {source}")]
    Bind {
        addr: String,
        source: std::io::Error,
    },

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Configuration for the server.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Address to bind to (e.g., "0.0.0.0:6380")
    pub bind_addr: String,
    /// Maximum number of concurrent connections
    pub max_connections: usize,
    /// Connection idle timeout in seconds (0 = no timeout)
    pub timeout: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: format!("0.0.0.0:{}", crate::DEFAULT_PORT),
            max_connections: crate::DEFAULT_MAX_CONNECTIONS,
            timeout: 0, // No timeout by default
        }
    }
}

/// The main TCP server.
pub struct Server {
    /// Server configuration
    config: ServerConfig,
    /// Shared key store
    store: Arc<KeyStore>,
    /// Shared command executor
    executor: Arc<CommandExecutor>,
    /// Shared blocking registry for blocking commands
    blocking_registry: Arc<BlockingRegistry>,
    /// Optional AOF writer for persistence
    aof_writer: Option<Arc<AofWriter>>,
    /// Current number of active connections
    active_connections: Arc<AtomicUsize>,
    /// Broadcast sender for shutdown signal
    shutdown_tx: broadcast::Sender<()>,
}

impl Server {
    /// Create a new server with the given configuration and store.
    pub fn new(config: ServerConfig, store: Arc<KeyStore>) -> Self {
        let executor = Arc::new(CommandExecutor::new());
        let blocking_registry = Arc::new(BlockingRegistry::new());
        let (shutdown_tx, _) = broadcast::channel(1);

        Self {
            config,
            store,
            executor,
            blocking_registry,
            aof_writer: None,
            active_connections: Arc::new(AtomicUsize::new(0)),
            shutdown_tx,
        }
    }

    /// Create a new server with AOF writer
    pub fn with_aof(config: ServerConfig, store: Arc<KeyStore>, aof_writer: Arc<AofWriter>) -> Self {
        let executor = Arc::new(CommandExecutor::new());
        let blocking_registry = Arc::new(BlockingRegistry::new());
        let (shutdown_tx, _) = broadcast::channel(1);

        Self {
            config,
            store,
            executor,
            blocking_registry,
            aof_writer: Some(aof_writer),
            active_connections: Arc::new(AtomicUsize::new(0)),
            shutdown_tx,
        }
    }

    /// Create a server with default configuration.
    pub fn with_defaults(store: Arc<KeyStore>) -> Self {
        Self::new(ServerConfig::default(), store)
    }

    /// Get the shutdown sender (for external shutdown control).
    pub fn shutdown_handle(&self) -> broadcast::Sender<()> {
        self.shutdown_tx.clone()
    }

    /// Get the current number of active connections.
    pub fn active_connections(&self) -> usize {
        self.active_connections.load(Ordering::Relaxed)
    }

    /// Run the server, accepting connections until shutdown is signaled.
    ///
    /// This method will bind to the configured address and accept connections.
    /// Each connection is handled in its own tokio task.
    ///
    /// The server shuts down when the shutdown sender is dropped or sends a message.
    pub async fn run(&self) -> Result<(), ServerError> {
        let listener = TcpListener::bind(&self.config.bind_addr)
            .await
            .map_err(|e| ServerError::Bind {
                addr: self.config.bind_addr.clone(),
                source: e,
            })?;

        info!(addr = %self.config.bind_addr, "Server listening");

        let mut shutdown_rx = self.shutdown_tx.subscribe();

        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((stream, addr)) => {
                            let current = self.active_connections.load(Ordering::Relaxed);
                            if current >= self.config.max_connections {
                                warn!(
                                    addr = %addr,
                                    max = self.config.max_connections,
                                    "Max connections reached, rejecting"
                                );
                                // Drop the stream to reject the connection
                                drop(stream);
                                continue;
                            }

                            self.active_connections.fetch_add(1, Ordering::Relaxed);

                            let executor = self.executor.clone();
                            let store = self.store.clone();
                            let blocking_registry = self.blocking_registry.clone();
                            let aof_writer = self.aof_writer.clone();
                            let shutdown = Shutdown::new(self.shutdown_tx.subscribe());
                            let active_connections = self.active_connections.clone();
                            let timeout = self.config.timeout;

                            tokio::spawn(async move {
                                handle_connection(stream, executor, store, blocking_registry, aof_writer, shutdown, timeout).await;
                                active_connections.fetch_sub(1, Ordering::Relaxed);
                            });
                        }
                        Err(e) => {
                            error!(error = %e, "Failed to accept connection");
                        }
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!("Shutdown signal received, stopping listener");
                    break;
                }
            }
        }

        // Wait briefly for active connections to drain
        let timeout = tokio::time::sleep(std::time::Duration::from_secs(5));
        tokio::pin!(timeout);

        loop {
            if self.active_connections.load(Ordering::Relaxed) == 0 {
                info!("All connections closed");
                break;
            }

            tokio::select! {
                () = &mut timeout => {
                    let remaining = self.active_connections.load(Ordering::Relaxed);
                    warn!(remaining, "Shutdown timeout, forcefully closing connections");
                    break;
                }
                () = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                    // Check again
                }
            }
        }

        info!("Server stopped");
        Ok(())
    }

    /// Run the server on a specific `TcpListener` (useful for tests).
    pub async fn run_on_listener(&self, listener: TcpListener) -> Result<(), ServerError> {
        let addr = listener.local_addr()?;
        info!(addr = %addr, "Server listening (on provided listener)");

        let mut shutdown_rx = self.shutdown_tx.subscribe();

        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((stream, _addr)) => {
                            let current = self.active_connections.load(Ordering::Relaxed);
                            if current >= self.config.max_connections {
                                drop(stream);
                                continue;
                            }

                            self.active_connections.fetch_add(1, Ordering::Relaxed);

                            let executor = self.executor.clone();
                            let store = self.store.clone();
                            let blocking_registry = self.blocking_registry.clone();
                            let aof_writer = self.aof_writer.clone();
                            let shutdown = Shutdown::new(self.shutdown_tx.subscribe());
                            let active_connections = self.active_connections.clone();
                            let timeout = self.config.timeout;

                            tokio::spawn(async move {
                                handle_connection(stream, executor, store, blocking_registry, aof_writer, shutdown, timeout).await;
                                active_connections.fetch_sub(1, Ordering::Relaxed);
                            });
                        }
                        Err(e) => {
                            error!(error = %e, "Failed to accept connection");
                        }
                    }
                }
                _ = shutdown_rx.recv() => {
                    break;
                }
            }
        }

        Ok(())
    }
}

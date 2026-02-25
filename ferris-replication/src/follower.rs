//! Follower (replica) implementation
//!
//! Handles connection to leader, synchronization, and command replication.

#![allow(clippy::module_name_repetitions)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::ignored_unit_patterns)]
#![allow(clippy::uninlined_format_args)]
#![allow(clippy::missing_const_for_fn)]
#![allow(clippy::single_match)]
#![allow(clippy::single_match_else)]

use bytes::Bytes;
use ferris_protocol::RespValue;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, RwLock};
use tokio::time::sleep;

/// Follower connection state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FollowerState {
    /// Not connected to any leader
    Disconnected,
    /// Connecting to leader
    Connecting,
    /// Sending handshake (PING, REPLCONF)
    Handshaking,
    /// Waiting for PSYNC response
    WaitingSync,
    /// Receiving full sync (RDB transfer)
    FullSync,
    /// Receiving incremental updates
    Streaming,
    /// Connection error, will retry
    Error,
}

/// Configuration for follower
#[derive(Debug, Clone)]
pub struct FollowerConfig {
    /// Leader host
    pub host: String,
    /// Leader port
    pub port: u16,
    /// Our listening port (for REPLCONF)
    pub listening_port: u16,
    /// Connection retry delay (milliseconds)
    pub retry_delay_ms: u64,
    /// Maximum retry delay (milliseconds)
    pub max_retry_delay_ms: u64,
}

impl FollowerConfig {
    /// Create a new follower configuration
    #[must_use]
    pub fn new(host: String, port: u16, listening_port: u16) -> Self {
        Self {
            host,
            port,
            listening_port,
            retry_delay_ms: 1000,      // Start with 1 second
            max_retry_delay_ms: 60000, // Max 60 seconds
        }
    }
}

/// Commands received from leader
#[derive(Debug, Clone)]
pub enum ReplicationCommand {
    /// A write command to apply
    Command(Vec<Bytes>),
    /// Full sync started with this replication ID and offset
    FullSyncStart { repl_id: String, offset: u64 },
    /// Partial sync continuation
    PartialSyncStart { repl_id: String },
}

/// Follower manager
///
/// Manages connection to leader and handles replication stream.
#[derive(Debug)]
pub struct Follower {
    /// Configuration
    config: Arc<RwLock<Option<FollowerConfig>>>,
    /// Current state
    state: Arc<RwLock<FollowerState>>,
    /// Channel to send commands to main server for execution
    command_tx: mpsc::UnboundedSender<ReplicationCommand>,
    /// Shutdown signal
    shutdown: Arc<tokio::sync::Notify>,
}

impl Follower {
    /// Create a new follower manager
    ///
    /// Returns the follower and a receiver for replication commands
    #[must_use]
    pub fn new() -> (Self, mpsc::UnboundedReceiver<ReplicationCommand>) {
        let (command_tx, command_rx) = mpsc::unbounded_channel();

        (
            Self {
                config: Arc::new(RwLock::new(None)),
                state: Arc::new(RwLock::new(FollowerState::Disconnected)),
                command_tx,
                shutdown: Arc::new(tokio::sync::Notify::new()),
            },
            command_rx,
        )
    }

    /// Start replication with a leader
    ///
    /// This spawns a background task that maintains the connection.
    pub async fn start(&self, config: FollowerConfig) {
        *self.config.write().await = Some(config.clone());
        *self.state.write().await = FollowerState::Connecting;

        let config_clone = self.config.clone();
        let state_clone = self.state.clone();
        let command_tx = self.command_tx.clone();
        let shutdown = self.shutdown.clone();

        tokio::spawn(async move {
            Self::replication_loop(config_clone, state_clone, command_tx, shutdown).await;
        });
    }

    /// Stop replication
    pub async fn stop(&self) {
        *self.config.write().await = None;
        *self.state.write().await = FollowerState::Disconnected;
        self.shutdown.notify_waiters();
    }

    /// Get current state
    pub async fn state(&self) -> FollowerState {
        *self.state.read().await
    }

    /// Main replication loop with retry logic
    async fn replication_loop(
        config: Arc<RwLock<Option<FollowerConfig>>>,
        state: Arc<RwLock<FollowerState>>,
        command_tx: mpsc::UnboundedSender<ReplicationCommand>,
        shutdown: Arc<tokio::sync::Notify>,
    ) {
        let mut retry_delay = 1000u64; // Start with 1 second

        loop {
            // Check if we should stop
            let config_guard = config.read().await;
            let current_config = match config_guard.as_ref() {
                Some(c) => c.clone(),
                None => {
                    // No config, stop replication
                    *state.write().await = FollowerState::Disconnected;
                    return;
                }
            };
            drop(config_guard);

            // Try to connect and replicate
            *state.write().await = FollowerState::Connecting;

            match Self::connect_and_replicate(
                &current_config,
                state.clone(),
                command_tx.clone(),
                shutdown.clone(),
            )
            .await
            {
                Ok(()) => {
                    // Successful connection, reset retry delay
                    retry_delay = current_config.retry_delay_ms;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Replication connection failed, will retry");
                    *state.write().await = FollowerState::Error;

                    // Exponential backoff
                    tokio::select! {
                        _ = sleep(Duration::from_millis(retry_delay)) => {}
                        _ = shutdown.notified() => {
                            *state.write().await = FollowerState::Disconnected;
                            return;
                        }
                    }

                    retry_delay = std::cmp::min(retry_delay * 2, current_config.max_retry_delay_ms);
                }
            }
        }
    }

    /// Connect to leader and start replication
    async fn connect_and_replicate(
        config: &FollowerConfig,
        state: Arc<RwLock<FollowerState>>,
        command_tx: mpsc::UnboundedSender<ReplicationCommand>,
        shutdown: Arc<tokio::sync::Notify>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Connect to leader
        let addr = format!("{}:{}", config.host, config.port);
        tracing::info!("Connecting to leader at {}", addr);

        let mut stream = TcpStream::connect(&addr).await?;

        // Perform handshake
        *state.write().await = FollowerState::Handshaking;
        Self::perform_handshake(&mut stream, config).await?;

        // Send PSYNC to request synchronization
        *state.write().await = FollowerState::WaitingSync;
        Self::send_psync(&mut stream).await?;

        // Read PSYNC response
        let response = Self::read_resp(&mut stream).await?;
        tracing::debug!("PSYNC response: {:?}", response);

        match &response {
            RespValue::SimpleString(s) if s.starts_with("FULLRESYNC") => {
                // Full resync: +FULLRESYNC <repl_id> <offset>
                let parts: Vec<&str> = s.split_whitespace().collect();
                if parts.len() >= 3 {
                    let repl_id = parts[1].to_string();
                    let offset: u64 = parts[2].parse().unwrap_or(0);

                    tracing::info!(
                        "Starting full sync with repl_id={}, offset={}",
                        repl_id,
                        offset
                    );

                    let _ = command_tx.send(ReplicationCommand::FullSyncStart { repl_id, offset });

                    *state.write().await = FollowerState::FullSync;

                    // For now, we skip RDB transfer (will implement in next iteration)
                    // In real Redis, leader sends RDB snapshot here

                    *state.write().await = FollowerState::Streaming;
                }
            }
            RespValue::SimpleString(s) if s.starts_with("CONTINUE") => {
                // Partial resync: +CONTINUE <repl_id>
                let parts: Vec<&str> = s.split_whitespace().collect();
                if parts.len() >= 2 {
                    let repl_id = parts[1].to_string();

                    tracing::info!("Starting partial sync with repl_id={}", repl_id);

                    let _ = command_tx.send(ReplicationCommand::PartialSyncStart { repl_id });

                    *state.write().await = FollowerState::Streaming;
                }
            }
            _ => {
                return Err(format!("Unexpected PSYNC response: {:?}", response).into());
            }
        }

        // Stream commands from leader
        Self::stream_commands(&mut stream, command_tx, shutdown).await?;

        Ok(())
    }

    /// Perform handshake with leader
    async fn perform_handshake(
        stream: &mut TcpStream,
        config: &FollowerConfig,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Send PING
        Self::send_command(stream, &["PING"]).await?;
        let response = Self::read_resp(stream).await?;
        tracing::debug!("PING response: {:?}", response);

        // Send REPLCONF listening-port
        Self::send_command(
            stream,
            &["REPLCONF", "listening-port", &config.listening_port.to_string()],
        )
        .await?;
        let response = Self::read_resp(stream).await?;
        tracing::debug!("REPLCONF listening-port response: {:?}", response);

        // Send REPLCONF capa eof capa psync2
        Self::send_command(stream, &["REPLCONF", "capa", "eof", "capa", "psync2"]).await?;
        let response = Self::read_resp(stream).await?;
        tracing::debug!("REPLCONF capa response: {:?}", response);

        Ok(())
    }

    /// Send PSYNC command
    async fn send_psync(
        stream: &mut TcpStream,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // First time sync: PSYNC ? -1
        // TODO: Track repl_id and offset for partial resync
        Self::send_command(stream, &["PSYNC", "?", "-1"]).await?;
        Ok(())
    }

    /// Stream commands from leader
    async fn stream_commands(
        stream: &mut TcpStream,
        command_tx: mpsc::UnboundedSender<ReplicationCommand>,
        shutdown: Arc<tokio::sync::Notify>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        loop {
            tokio::select! {
                result = Self::read_resp(stream) => {
                    let cmd = result?;

                    // Parse command array
                    if let RespValue::Array(parts) = cmd {
                        let bytes_parts: Vec<Bytes> = parts
                            .into_iter()
                            .filter_map(|v| match v {
                                RespValue::BulkString(b) => Some(b),
                                _ => None,
                            })
                            .collect();

                        if !bytes_parts.is_empty() {
                            let _ = command_tx.send(ReplicationCommand::Command(bytes_parts));
                        }
                    }
                }
                _ = shutdown.notified() => {
                    tracing::info!("Replication shutdown requested");
                    return Ok(());
                }
            }
        }
    }

    /// Send a RESP command
    async fn send_command(
        stream: &mut TcpStream,
        args: &[&str],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Build RESP array
        let mut buf = Vec::new();
        buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());

        for arg in args {
            buf.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            buf.extend_from_slice(arg.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }

        stream.write_all(&buf).await?;
        stream.flush().await?;

        Ok(())
    }

    /// Read a RESP value from stream
    async fn read_resp(
        stream: &mut TcpStream,
    ) -> Result<RespValue, Box<dyn std::error::Error + Send + Sync>> {
        // Simple RESP parser for reading responses
        // For production, we should use ferris-protocol codec

        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte).await?;

        match byte[0] {
            b'+' => {
                // Simple string
                let line = Self::read_line(stream).await?;
                Ok(RespValue::SimpleString(line))
            }
            b'-' => {
                // Error
                let line = Self::read_line(stream).await?;
                Ok(RespValue::Error(line))
            }
            b':' => {
                // Integer
                let line = Self::read_line(stream).await?;
                let num: i64 = line.parse()?;
                Ok(RespValue::Integer(num))
            }
            b'$' => {
                // Bulk string
                let len_str = Self::read_line(stream).await?;
                let len: i64 = len_str.parse()?;

                if len < 0 {
                    return Ok(RespValue::Null);
                }

                let mut data = vec![0u8; len as usize];
                stream.read_exact(&mut data).await?;

                // Read trailing \r\n
                let mut crlf = [0u8; 2];
                stream.read_exact(&mut crlf).await?;

                Ok(RespValue::BulkString(Bytes::from(data)))
            }
            b'*' => {
                // Array
                let count_str = Self::read_line(stream).await?;
                let count: i64 = count_str.parse()?;

                if count < 0 {
                    return Ok(RespValue::Null);
                }

                let mut elements = Vec::new();
                for _ in 0..count {
                    elements.push(Box::pin(Self::read_resp(stream)).await?);
                }

                Ok(RespValue::Array(elements))
            }
            _ => Err(format!("Unknown RESP type: {}", char::from(byte[0])).into()),
        }
    }

    /// Read a line (until \r\n)
    async fn read_line(
        stream: &mut TcpStream,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let mut line = Vec::new();
        let mut byte = [0u8; 1];

        loop {
            stream.read_exact(&mut byte).await?;

            if byte[0] == b'\r' {
                stream.read_exact(&mut byte).await?;
                if byte[0] == b'\n' {
                    break;
                }
                line.push(b'\r');
            }

            line.push(byte[0]);
        }

        Ok(String::from_utf8(line)?)
    }
}

impl Default for Follower {
    fn default() -> Self {
        Self::new().0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_follower_config_new() {
        let config = FollowerConfig::new("localhost".to_string(), 6379, 6380);
        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 6379);
        assert_eq!(config.listening_port, 6380);
    }

    #[test]
    fn test_follower_state_transitions() {
        assert_eq!(
            FollowerState::Disconnected,
            FollowerState::Disconnected
        );
        assert_ne!(FollowerState::Connecting, FollowerState::Streaming);
    }

    #[tokio::test]
    async fn test_follower_creation() {
        let (follower, mut rx) = Follower::new();
        assert_eq!(follower.state().await, FollowerState::Disconnected);

        // Channel should be empty
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_follower_stop() {
        let (follower, _rx) = Follower::new();

        follower.stop().await;

        assert_eq!(follower.state().await, FollowerState::Disconnected);
    }
}

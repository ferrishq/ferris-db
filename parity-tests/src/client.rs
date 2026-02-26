//! Client implementations for parity testing
//!
//! Provides both single-server and dual-server clients for testing.

use bytes::BytesMut;
use ferris_protocol::{resp2, ProtocolError, RespValue};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

use crate::comparison::{compare_responses, CompareMode, ParityResult};
use crate::ParityConfig;

/// Server type identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerType {
    Redis,
    Ferris,
}

impl std::fmt::Display for ServerType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Redis => write!(f, "Redis"),
            Self::Ferris => write!(f, "ferris-db"),
        }
    }
}

/// A client that can connect to either Redis or ferris-db
#[derive(Debug)]
pub struct ParityClient {
    stream: TcpStream,
    read_buf: BytesMut,
    server_type: ServerType,
    timeout: Duration,
}

impl ParityClient {
    /// Connect to a server
    pub async fn connect(addr: &str, server_type: ServerType) -> anyhow::Result<Self> {
        let socket_addr: SocketAddr = addr.parse()?;
        let stream = TcpStream::connect(socket_addr).await?;

        Ok(Self {
            stream,
            read_buf: BytesMut::with_capacity(8192),
            server_type,
            timeout: Duration::from_secs(5),
        })
    }

    /// Connect to Redis on default port
    pub async fn connect_redis() -> anyhow::Result<Self> {
        Self::connect(
            &format!("127.0.0.1:{}", crate::REDIS_PORT),
            ServerType::Redis,
        )
        .await
    }

    /// Connect to ferris-db on default port
    pub async fn connect_ferris() -> anyhow::Result<Self> {
        Self::connect(
            &format!("127.0.0.1:{}", crate::FERRIS_PORT),
            ServerType::Ferris,
        )
        .await
    }

    /// Get the server type
    pub fn server_type(&self) -> ServerType {
        self.server_type
    }

    /// Send a command and receive the response
    pub async fn cmd(&mut self, args: &[&str]) -> anyhow::Result<RespValue> {
        self.send_command(args).await?;
        self.read_response().await
    }

    /// Send a command without reading response
    pub async fn send_command(&mut self, args: &[&str]) -> anyhow::Result<()> {
        let cmd = Self::encode_command(args);
        self.stream.write_all(&cmd).await?;
        self.stream.flush().await?;
        Ok(())
    }

    /// Read a response from the server
    pub async fn read_response(&mut self) -> anyhow::Result<RespValue> {
        match timeout(self.timeout, self.read_response_inner()).await {
            Ok(result) => result,
            Err(_) => anyhow::bail!("Timeout reading response from {}", self.server_type),
        }
    }

    async fn read_response_inner(&mut self) -> anyhow::Result<RespValue> {
        loop {
            // Try to parse existing buffer
            if !self.read_buf.is_empty() {
                match resp2::parse(&mut self.read_buf) {
                    Ok(Some(value)) => {
                        return Ok(value);
                    }
                    Ok(None) => {
                        // Need more data (incomplete)
                    }
                    Err(ProtocolError::Incomplete) => {
                        // Need more data
                    }
                    Err(e) => return Err(e.into()),
                }
            }

            // Read more data
            let mut buf = [0u8; 4096];
            let n = self.stream.read(&mut buf).await?;
            if n == 0 {
                anyhow::bail!("Connection closed by {}", self.server_type);
            }
            self.read_buf.extend_from_slice(&buf[..n]);
        }
    }

    /// Encode a command as RESP array
    fn encode_command(args: &[&str]) -> Vec<u8> {
        let mut buf = Vec::new();

        // Array header
        buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());

        // Each argument as bulk string
        for arg in args {
            buf.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            buf.extend_from_slice(arg.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }

        buf
    }

    /// Execute FLUSHDB to clear the database
    pub async fn flushdb(&mut self) -> anyhow::Result<()> {
        let result = self.cmd(&["FLUSHDB"]).await?;
        match result {
            RespValue::SimpleString(s) if s == "OK" => Ok(()),
            RespValue::Error(e) => anyhow::bail!("FLUSHDB failed: {}", e),
            other => anyhow::bail!("Unexpected FLUSHDB response: {:?}", other),
        }
    }

    /// Execute PING to check connectivity
    pub async fn ping(&mut self) -> anyhow::Result<bool> {
        let result = self.cmd(&["PING"]).await?;
        Ok(matches!(result, RespValue::SimpleString(s) if s == "PONG"))
    }
}

/// A dual client that sends commands to both Redis and ferris-db
pub struct DualClient {
    redis: ParityClient,
    ferris: ParityClient,
    config: ParityConfig,
    test_id: u64,
}

impl DualClient {
    /// Connect to both Redis and ferris-db using default configuration
    pub async fn connect() -> anyhow::Result<Self> {
        Self::connect_with_config(ParityConfig::default()).await
    }

    /// Connect with custom configuration
    pub async fn connect_with_config(config: ParityConfig) -> anyhow::Result<Self> {
        let redis = ParityClient::connect(&config.redis_addr, ServerType::Redis).await?;
        let ferris = ParityClient::connect(&config.ferris_addr, ServerType::Ferris).await?;

        Ok(Self {
            redis,
            ferris,
            config,
            test_id: 0,
        })
    }

    /// Flush both databases
    pub async fn flush_both(&mut self) -> anyhow::Result<()> {
        self.redis.flushdb().await?;
        self.ferris.flushdb().await?;
        Ok(())
    }

    /// Ping both servers to check connectivity
    pub async fn ping_both(&mut self) -> anyhow::Result<(bool, bool)> {
        let redis_ok = self.redis.ping().await?;
        let ferris_ok = self.ferris.ping().await?;
        Ok((redis_ok, ferris_ok))
    }

    /// Generate a unique key with optional prefix
    pub fn unique_key(&mut self, base: &str) -> String {
        self.test_id += 1;
        if self.config.key_prefix.is_empty() {
            format!("{}:{}", base, self.test_id)
        } else {
            format!("{}:{}:{}", self.config.key_prefix, base, self.test_id)
        }
    }

    /// Execute a command on both servers and compare results
    pub async fn cmd(&mut self, args: &[&str]) -> anyhow::Result<ParityResult> {
        let redis_result = self.redis.cmd(args).await;
        let ferris_result = self.ferris.cmd(args).await;

        let command = args.join(" ");

        match (redis_result, ferris_result) {
            (Ok(redis), Ok(ferris)) => {
                let matches = compare_responses(&redis, &ferris, CompareMode::Strict);
                Ok(ParityResult {
                    command,
                    redis: Some(redis),
                    ferris: Some(ferris),
                    redis_error: None,
                    ferris_error: None,
                    matches,
                })
            }
            (Ok(redis), Err(e)) => Ok(ParityResult {
                command,
                redis: Some(redis),
                ferris: None,
                redis_error: None,
                ferris_error: Some(e.to_string()),
                matches: false,
            }),
            (Err(e), Ok(ferris)) => Ok(ParityResult {
                command,
                redis: None,
                ferris: Some(ferris),
                redis_error: Some(e.to_string()),
                ferris_error: None,
                matches: false,
            }),
            (Err(re), Err(fe)) => Ok(ParityResult {
                command,
                redis: None,
                ferris: None,
                redis_error: Some(re.to_string()),
                ferris_error: Some(fe.to_string()),
                matches: false,
            }),
        }
    }

    /// Execute a command and assert both return the same result
    pub async fn assert_parity(&mut self, args: &[&str]) -> anyhow::Result<()> {
        let result = self.cmd(args).await?;
        if !result.matches {
            anyhow::bail!(
                "Parity mismatch for '{}'\n  Redis:  {:?}\n  Ferris: {:?}",
                result.command,
                result.redis,
                result.ferris
            );
        }
        Ok(())
    }

    /// Execute a command with unordered comparison (for sets, etc.)
    pub async fn assert_parity_unordered(&mut self, args: &[&str]) -> anyhow::Result<()> {
        let redis_result = self.redis.cmd(args).await?;
        let ferris_result = self.ferris.cmd(args).await?;

        let matches = compare_responses(&redis_result, &ferris_result, CompareMode::UnorderedArray);
        if !matches {
            anyhow::bail!(
                "Parity mismatch (unordered) for '{}'\n  Redis:  {:?}\n  Ferris: {:?}",
                args.join(" "),
                redis_result,
                ferris_result
            );
        }
        Ok(())
    }

    /// Execute command on Redis only
    pub async fn redis(&mut self, args: &[&str]) -> anyhow::Result<RespValue> {
        self.redis.cmd(args).await
    }

    /// Execute command on ferris-db only
    pub async fn ferris(&mut self, args: &[&str]) -> anyhow::Result<RespValue> {
        self.ferris.cmd(args).await
    }

    /// Execute a command that should return an error on both
    pub async fn assert_both_error(&mut self, args: &[&str]) -> anyhow::Result<()> {
        let redis_result = self.redis.cmd(args).await?;
        let ferris_result = self.ferris.cmd(args).await?;

        let redis_is_error = matches!(redis_result, RespValue::Error(_));
        let ferris_is_error = matches!(ferris_result, RespValue::Error(_));

        if !redis_is_error || !ferris_is_error {
            anyhow::bail!(
                "Expected error from both for '{}'\n  Redis:  {:?} (is_error={})\n  Ferris: {:?} (is_error={})",
                args.join(" "),
                redis_result,
                redis_is_error,
                ferris_result,
                ferris_is_error
            );
        }
        Ok(())
    }

    /// Execute a command and compare results with float tolerance
    /// Used for INCRBYFLOAT, HINCRBYFLOAT where string representation may differ slightly
    pub async fn assert_parity_float(&mut self, args: &[&str]) -> anyhow::Result<()> {
        let redis_result = self.redis.cmd(args).await?;
        let ferris_result = self.ferris.cmd(args).await?;

        // Parse both as floats and compare with tolerance
        let redis_float = Self::parse_float_response(&redis_result);
        let ferris_float = Self::parse_float_response(&ferris_result);

        match (redis_float, ferris_float) {
            (Some(r), Some(f)) => {
                // Allow small floating point differences (0.0001 tolerance)
                if (r - f).abs() > 0.0001 {
                    anyhow::bail!(
                        "Float parity mismatch for '{}'\n  Redis:  {} ({:?})\n  Ferris: {} ({:?})",
                        args.join(" "),
                        r,
                        redis_result,
                        f,
                        ferris_result
                    );
                }
            }
            (None, None) => {
                // Both failed to parse - check if they're both errors
                let both_errors = matches!(redis_result, RespValue::Error(_))
                    && matches!(ferris_result, RespValue::Error(_));
                if !both_errors {
                    anyhow::bail!(
                        "Float parse failed for '{}'\n  Redis:  {:?}\n  Ferris: {:?}",
                        args.join(" "),
                        redis_result,
                        ferris_result
                    );
                }
            }
            _ => {
                anyhow::bail!(
                    "Float type mismatch for '{}'\n  Redis:  {:?}\n  Ferris: {:?}",
                    args.join(" "),
                    redis_result,
                    ferris_result
                );
            }
        }
        Ok(())
    }

    /// Parse a RESP value as a float (handles BulkString responses)
    fn parse_float_response(value: &RespValue) -> Option<f64> {
        match value {
            RespValue::BulkString(b) => std::str::from_utf8(b).ok()?.parse::<f64>().ok(),
            RespValue::Double(d) => Some(*d),
            RespValue::Integer(i) => Some(*i as f64),
            _ => None,
        }
    }

    /// Get access to the Redis client directly
    pub fn redis_client(&mut self) -> &mut ParityClient {
        &mut self.redis
    }

    /// Get access to the ferris-db client directly
    pub fn ferris_client(&mut self) -> &mut ParityClient {
        &mut self.ferris
    }
}

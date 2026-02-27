//! Test client for integration tests

use bytes::{Bytes, BytesMut};
use ferris_protocol::{resp2, RespValue};
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// A simple test client for sending commands and receiving responses
#[derive(Debug)]
pub struct TestClient {
    stream: TcpStream,
    read_buf: BytesMut,
}

impl TestClient {
    /// Connect to a ferris-db server
    pub async fn connect(addr: SocketAddr) -> Self {
        let stream = TcpStream::connect(addr)
            .await
            .expect("failed to connect to test server");

        Self {
            stream,
            read_buf: BytesMut::with_capacity(4096),
        }
    }

    /// Send a command and receive the response
    pub async fn cmd(&mut self, args: &[&str]) -> RespValue {
        self.send_command(args).await;
        self.read_response().await
    }

    /// Try to send a command and receive the response, returning an error if the connection is closed
    pub async fn try_cmd(&mut self, args: &[&str]) -> std::io::Result<RespValue> {
        self.try_send_command(args).await?;
        self.try_read_response().await
    }

    /// Send a command and assert it returns OK
    pub async fn cmd_ok(&mut self, args: &[&str]) {
        let response = self.cmd(args).await;
        assert!(
            matches!(response, RespValue::SimpleString(ref s) if s == "OK"),
            "expected OK, got {:?}",
            response
        );
    }

    /// Send a command and assert it returns an error
    pub async fn cmd_err(&mut self, args: &[&str]) -> String {
        let response = self.cmd(args).await;
        match response {
            RespValue::Error(e) => e,
            other => panic!("expected error, got {:?}", other),
        }
    }

    /// Send a command and wait for response with a timeout.
    /// Returns `None` if the timeout expires (the command is still blocking).
    pub async fn cmd_timeout(
        &mut self,
        args: &[&str],
        timeout: std::time::Duration,
    ) -> Option<RespValue> {
        self.send_raw(args).await;
        self.read_response_timeout(timeout).await
    }

    /// Send a command where arguments may contain arbitrary binary data.
    ///
    /// Unlike `cmd`, which takes `&[&str]`, this accepts `&[&[u8]]` so that
    /// binary payloads (e.g. DUMP output) can be sent without UTF-8 conversion.
    pub async fn cmd_raw(&mut self, args: &[&[u8]]) -> RespValue {
        let cmd = RespValue::Array(
            args.iter()
                .map(|b| RespValue::BulkString(Bytes::copy_from_slice(b)))
                .collect(),
        );

        let mut buf = BytesMut::new();
        resp2::serialize(&cmd, &mut buf);

        self.stream
            .write_all(&buf)
            .await
            .expect("failed to write raw command");

        self.read_response().await
    }

    /// Send a raw command without reading the response.
    /// Useful for blocking commands where you want to send from another client first.
    pub async fn send_raw(&mut self, args: &[&str]) {
        self.send_command(args).await;
    }

    /// Try to read a response with a timeout.
    /// Returns `None` if the timeout expires.
    pub async fn read_response_timeout(
        &mut self,
        timeout: std::time::Duration,
    ) -> Option<RespValue> {
        match tokio::time::timeout(timeout, self.read_response()).await {
            Ok(resp) => Some(resp),
            Err(_) => None,
        }
    }

    /// Send a raw command
    async fn send_command(&mut self, args: &[&str]) {
        let cmd = RespValue::Array(
            args.iter()
                .map(|s| RespValue::BulkString(Bytes::from(s.to_string())))
                .collect(),
        );

        let mut buf = BytesMut::new();
        resp2::serialize(&cmd, &mut buf);

        self.stream
            .write_all(&buf)
            .await
            .expect("failed to write command");
    }

    /// Try to send a raw command, returning an error if the connection is closed
    async fn try_send_command(&mut self, args: &[&str]) -> std::io::Result<()> {
        let cmd = RespValue::Array(
            args.iter()
                .map(|s| RespValue::BulkString(Bytes::from(s.to_string())))
                .collect(),
        );

        let mut buf = BytesMut::new();
        resp2::serialize(&cmd, &mut buf);

        self.stream.write_all(&buf).await
    }

    /// Send multiple commands at once (pipelining) and read all responses
    ///
    /// This sends all commands in a single write, then reads all responses.
    /// This is the core of Redis pipelining support.
    pub async fn pipeline(&mut self, commands: &[&[&str]]) -> Vec<RespValue> {
        // Serialize all commands into a single buffer
        let mut buf = BytesMut::new();
        for args in commands {
            let cmd = RespValue::Array(
                args.iter()
                    .map(|s| RespValue::BulkString(Bytes::from(s.to_string())))
                    .collect(),
            );
            resp2::serialize(&cmd, &mut buf);
        }

        // Send all commands at once
        self.stream
            .write_all(&buf)
            .await
            .expect("failed to write pipelined commands");

        // Read all responses
        let mut responses = Vec::with_capacity(commands.len());
        for _ in 0..commands.len() {
            responses.push(self.read_response().await);
        }
        responses
    }

    /// Send multiple commands at once without waiting for responses
    /// Use `read_responses` to get the responses later
    pub async fn send_pipeline(&mut self, commands: &[&[&str]]) {
        let mut buf = BytesMut::new();
        for args in commands {
            let cmd = RespValue::Array(
                args.iter()
                    .map(|s| RespValue::BulkString(Bytes::from(s.to_string())))
                    .collect(),
            );
            resp2::serialize(&cmd, &mut buf);
        }

        self.stream
            .write_all(&buf)
            .await
            .expect("failed to write pipelined commands");
    }

    /// Read multiple responses (used with `send_pipeline`)
    pub async fn read_responses(&mut self, count: usize) -> Vec<RespValue> {
        let mut responses = Vec::with_capacity(count);
        for _ in 0..count {
            responses.push(self.read_response().await);
        }
        responses
    }

    /// Read a response from the server
    ///
    /// This is useful for reading pub/sub messages asynchronously
    pub async fn read_response(&mut self) -> RespValue {
        self.try_read_response()
            .await
            .expect("failed to read response")
    }

    /// Try to read a response from the server, returning an error if the connection is closed
    async fn try_read_response(&mut self) -> std::io::Result<RespValue> {
        loop {
            // Try to parse from existing buffer
            if let Some(value) = resp2::parse(&mut self.read_buf)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?
            {
                return Ok(value);
            }

            // Need more data
            let n = self.stream.read_buf(&mut self.read_buf).await?;

            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "connection closed by server",
                ));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // These tests require a running server, so they're marked as ignored
    // They will be run when we have the actual server implementation

    #[tokio::test]
    #[ignore]
    async fn test_client_ping() {
        let mut client = TestClient::connect("127.0.0.1:6380".parse().unwrap()).await;
        let response = client.cmd(&["PING"]).await;
        assert_eq!(response, RespValue::SimpleString("PONG".to_string()));
    }
}

//! Test server for integration tests
//!
//! Spawns a real ferris-db server on a random port for integration testing.

use crate::client::TestClient;
use ferris_core::KeyStore;
use ferris_network::{Server, ServerConfig};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::broadcast;

/// A test server instance that can be spawned for integration tests.
///
/// The server runs on a random available port and can be stopped
/// when the test is done.
pub struct TestServer {
    /// The address the server is listening on
    pub addr: SocketAddr,
    /// The port number
    pub port: u16,
    /// Shutdown signal sender
    shutdown_tx: Option<broadcast::Sender<()>>,
    /// Join handle for the server task
    server_handle: Option<tokio::task::JoinHandle<()>>,
    /// The shared key store (for direct inspection in tests)
    pub store: Arc<KeyStore>,
}

impl TestServer {
    /// Spawn a new test server on a random available port.
    ///
    /// This starts a real ferris-db server with full command execution,
    /// RESP protocol handling, and key store.
    ///
    /// # Panics
    /// Panics if the server fails to start.
    pub async fn spawn() -> Self {
        Self::spawn_with_config(ServerConfig {
            bind_addr: "127.0.0.1:0".to_string(),
            max_connections: 100,
            timeout: 0, // No timeout for tests
        })
        .await
    }

    /// Spawn a server with custom configuration.
    ///
    /// The `bind_addr` in config is ignored — a random port is always used.
    /// The `max_connections` value is respected.
    ///
    /// # Panics
    /// Panics if the server fails to start.
    pub async fn spawn_with_config(mut config: ServerConfig) -> Self {
        // Bind to a random port
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("failed to bind to random port");
        let addr = listener.local_addr().expect("failed to get local addr");
        let port = addr.port();

        config.bind_addr = addr.to_string();

        let store = Arc::new(KeyStore::default());
        let server = Server::new(config, store.clone());
        let shutdown_tx = server.shutdown_handle();

        // Spawn the server task
        let server_handle = tokio::spawn(async move {
            if let Err(e) = server.run_on_listener(listener).await {
                tracing::error!(error = %e, "Test server error");
            }
        });

        // Give the server a moment to start accepting connections
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        Self {
            addr,
            port,
            shutdown_tx: Some(shutdown_tx),
            server_handle: Some(server_handle),
            store,
        }
    }

    /// Connect a test client to this server.
    pub async fn client(&self) -> TestClient {
        TestClient::connect(self.addr).await
    }

    /// Stop the test server and wait for it to shut down.
    pub async fn stop(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.server_handle.take() {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), handle).await;
        }
    }

    /// Get the connection string for this server.
    #[must_use]
    pub fn connection_string(&self) -> String {
        format!("redis://127.0.0.1:{}", self.port)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_server_spawn() {
        let server = TestServer::spawn().await;
        assert!(server.port > 0);
        assert!(server.addr.port() > 0);
        server.stop().await;
    }

    #[tokio::test]
    async fn test_server_accepts_client() {
        let server = TestServer::spawn().await;
        let _client = server.client().await;
        server.stop().await;
    }

    #[tokio::test]
    async fn test_server_ping_pong() {
        let server = TestServer::spawn().await;
        let mut client = server.client().await;

        let response = client.cmd(&["PING"]).await;
        assert_eq!(
            response,
            ferris_protocol::RespValue::SimpleString("PONG".to_string())
        );

        server.stop().await;
    }

    #[tokio::test]
    async fn test_server_multiple_clients() {
        let server = TestServer::spawn().await;

        let mut client1 = server.client().await;
        let mut client2 = server.client().await;

        let r1 = client1.cmd(&["PING"]).await;
        let r2 = client2.cmd(&["PING"]).await;

        assert_eq!(
            r1,
            ferris_protocol::RespValue::SimpleString("PONG".to_string())
        );
        assert_eq!(
            r2,
            ferris_protocol::RespValue::SimpleString("PONG".to_string())
        );

        server.stop().await;
    }
}

//! Graceful shutdown coordination
//!
//! Uses a broadcast channel to signal all connection tasks to stop.

use tokio::sync::broadcast;

/// Listens for the server shutdown signal.
///
/// Each connection task holds a `Shutdown` instance. When the server
/// decides to shut down, it drops the broadcast `Sender`, causing all
/// receivers to return an error, which the connection tasks interpret
/// as "time to stop".
#[derive(Debug)]
pub struct Shutdown {
    /// The receive half of the broadcast channel.
    receiver: broadcast::Receiver<()>,
    /// True once the shutdown signal has been received.
    is_shutdown: bool,
}

impl Shutdown {
    /// Create a new `Shutdown` backed by the given `broadcast::Receiver`.
    pub fn new(receiver: broadcast::Receiver<()>) -> Self {
        Self {
            receiver,
            is_shutdown: false,
        }
    }

    /// Returns `true` if the shutdown signal has been received.
    pub fn is_shutdown(&self) -> bool {
        self.is_shutdown
    }

    /// Receive the shutdown notice, waiting if necessary.
    pub async fn recv(&mut self) {
        if self.is_shutdown {
            return;
        }

        // The sender was dropped (or sent a message) — either way, shut down.
        let _ = self.receiver.recv().await;
        self.is_shutdown = true;
    }
}

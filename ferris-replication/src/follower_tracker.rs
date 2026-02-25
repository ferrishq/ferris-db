//! Follower connection tracking for leader
//!
//! Tracks connected followers and sends replication commands to them.

use bytes::Bytes;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Notify, RwLock};
use tracing::{debug, error, warn};

/// A connected follower
#[derive(Debug)]
pub struct FollowerConnection {
    /// Unique ID for this follower
    pub id: u64,
    /// Follower's listening port (from REPLCONF)
    pub listening_port: Option<u16>,
    /// Channel to send commands to this follower's writer task
    tx: mpsc::UnboundedSender<Bytes>,
    /// The follower's last acknowledged offset
    offset: Arc<AtomicU64>,
    /// Notifier to wake up WAIT commands when offset changes
    /// Note: Stored but not directly accessed (used implicitly by Notify mechanism)
    #[allow(dead_code)]
    notify: Arc<Notify>,
}

impl FollowerConnection {
    /// Create a new follower connection
    ///
    /// Spawns a background task that writes commands to the TCP stream.
    pub fn new(id: u64, stream: TcpStream, notify: Arc<Notify>) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let offset = Arc::new(AtomicU64::new(0));

        // Spawn writer task
        let offset_clone = offset.clone();
        let notify_clone = notify.clone();
        tokio::spawn(async move {
            if let Err(e) = Self::writer_task(stream, rx, offset_clone, notify_clone).await {
                error!(follower_id = id, error = %e, "Follower writer task failed");
            }
        });

        Self {
            id,
            listening_port: None,
            tx,
            offset,
            notify,
        }
    }

    /// Send a command to this follower
    ///
    /// # Errors
    ///
    /// Returns error if the follower's writer task has disconnected
    pub fn send_command(&self, data: Bytes) -> Result<(), mpsc::error::SendError<Bytes>> {
        self.tx.send(data)
    }

    /// Get the follower's current offset
    #[must_use]
    pub fn offset(&self) -> u64 {
        self.offset.load(Ordering::Relaxed)
    }

    /// Set the follower's listening port
    pub fn set_listening_port(&mut self, port: u16) {
        self.listening_port = Some(port);
    }

    /// Writer task that sends commands to the follower's TCP stream
    async fn writer_task(
        mut stream: TcpStream,
        mut rx: mpsc::UnboundedReceiver<Bytes>,
        offset: Arc<AtomicU64>,
        notify: Arc<Notify>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        while let Some(data) = rx.recv().await {
            // Write data to stream
            stream.write_all(&data).await?;
            stream.flush().await?;

            // Update offset
            offset.fetch_add(data.len() as u64, Ordering::Relaxed);

            // Notify any waiting WAIT commands
            notify.notify_waiters();

            debug!(bytes = data.len(), "Sent command to follower");
        }

        Ok(())
    }
}

/// Global counter for follower IDs
static NEXT_FOLLOWER_ID: AtomicU64 = AtomicU64::new(1);

/// Tracks all connected followers
#[derive(Debug)]
pub struct FollowerTracker {
    followers: Arc<RwLock<HashMap<u64, FollowerConnection>>>,
    /// Notifier for when follower offsets are updated (for WAIT command)
    offset_notify: Arc<Notify>,
}

impl Default for FollowerTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl FollowerTracker {
    /// Create a new follower tracker
    #[must_use]
    pub fn new() -> Self {
        Self {
            followers: Arc::new(RwLock::new(HashMap::new())),
            offset_notify: Arc::new(Notify::new()),
        }
    }

    /// Register a new follower connection
    ///
    /// Returns the follower ID
    pub async fn register_follower(&self, stream: TcpStream) -> u64 {
        let id = NEXT_FOLLOWER_ID.fetch_add(1, Ordering::Relaxed);
        let follower = FollowerConnection::new(id, stream, self.offset_notify.clone());

        self.followers.write().await.insert(id, follower);

        debug!(follower_id = id, "Registered new follower");
        id
    }

    /// Update follower's listening port
    pub async fn set_follower_port(&self, id: u64, port: u16) {
        let mut followers = self.followers.write().await;
        if let Some(follower) = followers.get_mut(&id) {
            follower.set_listening_port(port);
        }
    }

    /// Send a command to all followers
    pub async fn broadcast_command(&self, data: Bytes) {
        let followers = self.followers.read().await;

        for (id, follower) in followers.iter() {
            if let Err(e) = follower.send_command(data.clone()) {
                warn!(follower_id = id, error = %e, "Failed to send command to follower");
                // Follower disconnected, we'll clean it up later
            }
        }
    }

    /// Send a command to a specific follower
    pub async fn send_to_follower(&self, id: u64, data: Bytes) -> bool {
        let followers = self.followers.read().await;

        followers
            .get(&id)
            .is_some_and(|follower| match follower.send_command(data) {
                Ok(()) => true,
                Err(e) => {
                    warn!(follower_id = id, error = %e, "Failed to send to follower");
                    false
                }
            })
    }

    /// Remove a follower (when disconnected)
    pub async fn remove_follower(&self, id: u64) {
        let mut followers = self.followers.write().await;
        if followers.remove(&id).is_some() {
            debug!(follower_id = id, "Removed follower");
        }
    }

    /// Get the number of connected followers
    pub async fn follower_count(&self) -> usize {
        self.followers.read().await.len()
    }

    /// Get all follower IDs
    pub async fn follower_ids(&self) -> Vec<u64> {
        self.followers.read().await.keys().copied().collect()
    }

    /// Wait for at least `num_replicas` followers to reach the given offset
    ///
    /// Returns the number of replicas that reached the offset within the timeout.
    /// If timeout is None, waits indefinitely.
    ///
    /// # Arguments
    ///
    /// * `num_replicas` - Minimum number of replicas to wait for
    /// * `offset` - The replication offset to wait for
    /// * `timeout` - Maximum time to wait (None = wait forever)
    pub async fn wait_for_offset(
        &self,
        num_replicas: usize,
        offset: u64,
        timeout: Option<Duration>,
    ) -> usize {
        // Quick check: if we have no followers, return immediately
        if self.follower_count().await == 0 {
            return 0;
        }

        // Quick check: if already enough replicas at offset, return immediately
        let ready = self.count_replicas_at_offset(offset).await;
        if ready >= num_replicas {
            return ready;
        }

        // Need to wait - set up timeout if specified
        let wait_fut = async {
            loop {
                // Wait for notification
                self.offset_notify.notified().await;

                // Check how many replicas have reached the offset
                let ready = self.count_replicas_at_offset(offset).await;
                if ready >= num_replicas {
                    return ready;
                }

                // If we have no more followers, return what we have
                if self.follower_count().await == 0 {
                    return 0;
                }
            }
        };

        // Apply timeout if specified
        match timeout {
            Some(duration) => {
                match tokio::time::timeout(duration, wait_fut).await {
                    Ok(count) => count,
                    Err(_) => {
                        // Timeout - return current count
                        self.count_replicas_at_offset(offset).await
                    }
                }
            }
            None => wait_fut.await,
        }
    }

    /// Count how many followers have reached at least the given offset
    pub async fn count_replicas_at_offset(&self, offset: u64) -> usize {
        let followers = self.followers.read().await;
        followers.values().filter(|f| f.offset() >= offset).count()
    }

    /// Notify waiting WAIT commands that offsets have been updated
    pub fn notify_offset_updated(&self) {
        self.offset_notify.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_follower_tracker_new() {
        let tracker = FollowerTracker::new();
        assert_eq!(tracker.follower_count().await, 0);
    }

    #[tokio::test]
    async fn test_follower_count() {
        let tracker = FollowerTracker::new();
        assert_eq!(tracker.follower_count().await, 0);
    }

    #[tokio::test]
    async fn test_follower_ids_empty() {
        let tracker = FollowerTracker::new();
        assert!(tracker.follower_ids().await.is_empty());
    }

    #[tokio::test]
    async fn test_remove_nonexistent_follower() {
        let tracker = FollowerTracker::new();
        tracker.remove_follower(999).await;
        // Should not panic
    }
}

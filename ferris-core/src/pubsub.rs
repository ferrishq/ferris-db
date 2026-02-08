//! Pub/Sub registry for managing subscriptions
//!
//! Redis Pub/Sub allows clients to subscribe to channels and receive messages
//! published to those channels. This module provides the infrastructure for
//! tracking subscriptions and routing messages.

use bytes::Bytes;
use dashmap::DashMap;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::mpsc;

/// Message sent to a subscriber
#[derive(Debug, Clone)]
pub enum PubSubMessage {
    /// Message from PUBLISH command
    Message { channel: Bytes, message: Bytes },
    /// Pattern message from PSUBSCRIBE
    PatternMessage {
        pattern: Bytes,
        channel: Bytes,
        message: Bytes,
    },
    /// Subscription confirmation
    Subscribe { channel: Bytes, count: usize },
    /// Unsubscription confirmation
    Unsubscribe { channel: Bytes, count: usize },
    /// Pattern subscription confirmation
    PSubscribe { pattern: Bytes, count: usize },
    /// Pattern unsubscription confirmation
    PUnsubscribe { pattern: Bytes, count: usize },
}

/// Unique subscriber ID
pub type SubscriberId = u64;

/// Registry for managing pub/sub subscriptions
pub struct PubSubRegistry {
    /// Channel name -> Set of subscriber IDs
    channels: Arc<DashMap<Bytes, HashSet<SubscriberId>>>,
    /// Pattern -> Set of subscriber IDs
    patterns: Arc<DashMap<Bytes, HashSet<SubscriberId>>>,
    /// Subscriber ID -> Message sender
    subscribers: Arc<DashMap<SubscriberId, mpsc::UnboundedSender<PubSubMessage>>>,
    /// Subscriber ID -> Subscribed channels
    subscriber_channels: Arc<DashMap<SubscriberId, HashSet<Bytes>>>,
    /// Subscriber ID -> Subscribed patterns
    subscriber_patterns: Arc<DashMap<SubscriberId, HashSet<Bytes>>>,
    /// Next subscriber ID
    next_id: Arc<std::sync::atomic::AtomicU64>,
}

impl Default for PubSubRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl PubSubRegistry {
    /// Create a new pub/sub registry
    pub fn new() -> Self {
        Self {
            channels: Arc::new(DashMap::new()),
            patterns: Arc::new(DashMap::new()),
            subscribers: Arc::new(DashMap::new()),
            subscriber_channels: Arc::new(DashMap::new()),
            subscriber_patterns: Arc::new(DashMap::new()),
            next_id: Arc::new(std::sync::atomic::AtomicU64::new(1)),
        }
    }

    /// Register a new subscriber and return their ID and receiver
    pub fn register_subscriber(&self) -> (SubscriberId, mpsc::UnboundedReceiver<PubSubMessage>) {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (tx, rx) = mpsc::unbounded_channel();
        self.subscribers.insert(id, tx);
        self.subscriber_channels.insert(id, HashSet::new());
        self.subscriber_patterns.insert(id, HashSet::new());
        (id, rx)
    }

    /// Unregister a subscriber
    pub fn unregister_subscriber(&self, subscriber_id: SubscriberId) {
        // Remove from all channels
        if let Some((_, channels)) = self.subscriber_channels.remove(&subscriber_id) {
            for channel in channels {
                if let Some(mut subs) = self.channels.get_mut(&channel) {
                    subs.remove(&subscriber_id);
                }
            }
        }

        // Remove from all patterns
        if let Some((_, patterns)) = self.subscriber_patterns.remove(&subscriber_id) {
            for pattern in patterns {
                if let Some(mut subs) = self.patterns.get_mut(&pattern) {
                    subs.remove(&subscriber_id);
                }
            }
        }

        // Remove subscriber
        self.subscribers.remove(&subscriber_id);
    }

    /// Subscribe to a channel
    pub fn subscribe(&self, subscriber_id: SubscriberId, channel: Bytes) -> usize {
        // Add to channel subscriptions
        self.channels
            .entry(channel.clone())
            .or_insert_with(HashSet::new)
            .insert(subscriber_id);

        // Track for subscriber
        let mut sub_channels = self.subscriber_channels.entry(subscriber_id).or_default();
        sub_channels.insert(channel.clone());

        // Send confirmation
        let count = sub_channels.len()
            + self
                .subscriber_patterns
                .get(&subscriber_id)
                .map_or(0, |p| p.len());

        if let Some(tx) = self.subscribers.get(&subscriber_id) {
            let _ = tx.send(PubSubMessage::Subscribe { channel, count });
        }

        count
    }

    /// Unsubscribe from a channel
    pub fn unsubscribe(&self, subscriber_id: SubscriberId, channel: Bytes) -> usize {
        // Remove from channel
        if let Some(mut subs) = self.channels.get_mut(&channel) {
            subs.remove(&subscriber_id);
        }

        // Remove from subscriber tracking
        if let Some(mut sub_channels) = self.subscriber_channels.get_mut(&subscriber_id) {
            sub_channels.remove(&channel);
        }

        // Send confirmation
        let count = self
            .subscriber_channels
            .get(&subscriber_id)
            .map_or(0, |c| c.len())
            + self
                .subscriber_patterns
                .get(&subscriber_id)
                .map_or(0, |p| p.len());

        if let Some(tx) = self.subscribers.get(&subscriber_id) {
            let _ = tx.send(PubSubMessage::Unsubscribe { channel, count });
        }

        count
    }

    /// Unsubscribe from all channels
    pub fn unsubscribe_all(&self, subscriber_id: SubscriberId) -> usize {
        let channels: Vec<Bytes> = self
            .subscriber_channels
            .get(&subscriber_id)
            .map(|c| c.iter().cloned().collect())
            .unwrap_or_default();

        for channel in channels {
            self.unsubscribe(subscriber_id, channel);
        }

        self.subscriber_patterns
            .get(&subscriber_id)
            .map_or(0, |p| p.len())
    }

    /// Subscribe to a pattern
    pub fn psubscribe(&self, subscriber_id: SubscriberId, pattern: Bytes) -> usize {
        // Add to pattern subscriptions
        self.patterns
            .entry(pattern.clone())
            .or_insert_with(HashSet::new)
            .insert(subscriber_id);

        // Track for subscriber
        let mut sub_patterns = self.subscriber_patterns.entry(subscriber_id).or_default();
        sub_patterns.insert(pattern.clone());

        // Send confirmation
        let count = self
            .subscriber_channels
            .get(&subscriber_id)
            .map_or(0, |c| c.len())
            + sub_patterns.len();

        if let Some(tx) = self.subscribers.get(&subscriber_id) {
            let _ = tx.send(PubSubMessage::PSubscribe { pattern, count });
        }

        count
    }

    /// Unsubscribe from a pattern
    pub fn punsubscribe(&self, subscriber_id: SubscriberId, pattern: Bytes) -> usize {
        // Remove from pattern
        if let Some(mut subs) = self.patterns.get_mut(&pattern) {
            subs.remove(&subscriber_id);
        }

        // Remove from subscriber tracking
        if let Some(mut sub_patterns) = self.subscriber_patterns.get_mut(&subscriber_id) {
            sub_patterns.remove(&pattern);
        }

        // Send confirmation
        let count = self
            .subscriber_channels
            .get(&subscriber_id)
            .map_or(0, |c| c.len())
            + self
                .subscriber_patterns
                .get(&subscriber_id)
                .map_or(0, |p| p.len());

        if let Some(tx) = self.subscribers.get(&subscriber_id) {
            let _ = tx.send(PubSubMessage::PUnsubscribe { pattern, count });
        }

        count
    }

    /// Unsubscribe from all patterns
    pub fn punsubscribe_all(&self, subscriber_id: SubscriberId) -> usize {
        let patterns: Vec<Bytes> = self
            .subscriber_patterns
            .get(&subscriber_id)
            .map(|p| p.iter().cloned().collect())
            .unwrap_or_default();

        for pattern in patterns {
            self.punsubscribe(subscriber_id, pattern);
        }

        self.subscriber_channels
            .get(&subscriber_id)
            .map_or(0, |c| c.len())
    }

    /// Publish a message to a channel
    /// Returns the number of subscribers that received the message
    pub fn publish(&self, channel: &[u8], message: Bytes) -> usize {
        let mut count = 0;

        // Send to direct channel subscribers
        if let Some(subs) = self.channels.get(channel) {
            for sub_id in subs.iter() {
                if let Some(tx) = self.subscribers.get(sub_id) {
                    let msg = PubSubMessage::Message {
                        channel: Bytes::copy_from_slice(channel),
                        message: message.clone(),
                    };
                    if tx.send(msg).is_ok() {
                        count += 1;
                    }
                }
            }
        }

        // Send to pattern subscribers
        for pattern_entry in self.patterns.iter() {
            if pattern_matches(pattern_entry.key(), channel) {
                for sub_id in pattern_entry.value().iter() {
                    if let Some(tx) = self.subscribers.get(sub_id) {
                        let msg = PubSubMessage::PatternMessage {
                            pattern: pattern_entry.key().clone(),
                            channel: Bytes::copy_from_slice(channel),
                            message: message.clone(),
                        };
                        if tx.send(msg).is_ok() {
                            count += 1;
                        }
                    }
                }
            }
        }

        count
    }

    /// Get list of active channels (with at least one subscriber)
    pub fn channels(&self, pattern: Option<&[u8]>) -> Vec<Bytes> {
        if let Some(pat) = pattern {
            self.channels
                .iter()
                .filter(|entry| !entry.value().is_empty() && pattern_matches(pat, entry.key()))
                .map(|entry| entry.key().clone())
                .collect()
        } else {
            self.channels
                .iter()
                .filter(|entry| !entry.value().is_empty())
                .map(|entry| entry.key().clone())
                .collect()
        }
    }

    /// Get number of subscribers for specific channels
    pub fn numsub(&self, channels: &[Bytes]) -> HashMap<Bytes, usize> {
        channels
            .iter()
            .map(|channel| {
                let count = self.channels.get(channel).map_or(0, |subs| subs.len());
                (channel.clone(), count)
            })
            .collect()
    }

    /// Get number of subscribed patterns
    pub fn numpat(&self) -> usize {
        self.patterns
            .iter()
            .filter(|entry| !entry.value().is_empty())
            .count()
    }
}

/// Check if a pattern matches a channel name
/// Redis glob-style pattern matching: * matches any sequence, ? matches single char
fn pattern_matches(pattern: &[u8], channel: &[u8]) -> bool {
    let mut p_idx = 0;
    let mut c_idx = 0;
    let mut star_idx = None;
    let mut match_idx = 0;

    while c_idx < channel.len() {
        if p_idx < pattern.len() {
            match pattern[p_idx] {
                b'*' => {
                    star_idx = Some(p_idx);
                    match_idx = c_idx;
                    p_idx += 1;
                    continue;
                }
                b'?' => {
                    p_idx += 1;
                    c_idx += 1;
                    continue;
                }
                c if c == channel[c_idx] => {
                    p_idx += 1;
                    c_idx += 1;
                    continue;
                }
                _ => {}
            }
        }

        // Mismatch - backtrack to last star if exists
        if let Some(s_idx) = star_idx {
            p_idx = s_idx + 1;
            match_idx += 1;
            c_idx = match_idx;
        } else {
            return false;
        }
    }

    // Consume remaining stars in pattern
    while p_idx < pattern.len() && pattern[p_idx] == b'*' {
        p_idx += 1;
    }

    p_idx == pattern.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pattern_matches() {
        assert!(pattern_matches(b"*", b"anything"));
        assert!(pattern_matches(b"h*llo", b"hello"));
        assert!(pattern_matches(b"h*llo", b"hallo"));
        assert!(pattern_matches(b"h?llo", b"hello"));
        assert!(!pattern_matches(b"h?llo", b"hllo"));
        assert!(pattern_matches(b"news.*", b"news.music"));
        assert!(pattern_matches(b"news.*", b"news.sports"));
        assert!(!pattern_matches(b"news.*", b"sport.tennis"));
    }

    #[test]
    fn test_register_subscriber() {
        let registry = PubSubRegistry::new();
        let (id1, _rx1) = registry.register_subscriber();
        let (id2, _rx2) = registry.register_subscriber();
        assert_ne!(id1, id2);
        assert!(registry.subscribers.contains_key(&id1));
        assert!(registry.subscribers.contains_key(&id2));
    }

    #[test]
    fn test_subscribe_unsubscribe() {
        let registry = PubSubRegistry::new();
        let (id, mut rx) = registry.register_subscriber();

        let count = registry.subscribe(id, Bytes::from("channel1"));
        assert_eq!(count, 1);

        // Should receive subscribe confirmation
        let msg = rx.try_recv().unwrap();
        match msg {
            PubSubMessage::Subscribe { channel, count } => {
                assert_eq!(channel, Bytes::from("channel1"));
                assert_eq!(count, 1);
            }
            _ => panic!("Expected Subscribe message"),
        }

        let count = registry.unsubscribe(id, Bytes::from("channel1"));
        assert_eq!(count, 0);
    }

    #[test]
    fn test_publish() {
        let registry = PubSubRegistry::new();
        let (id, mut rx) = registry.register_subscriber();

        registry.subscribe(id, Bytes::from("news"));
        rx.try_recv().unwrap(); // Consume subscribe confirmation

        let count = registry.publish(b"news", Bytes::from("Hello!"));
        assert_eq!(count, 1);

        let msg = rx.try_recv().unwrap();
        match msg {
            PubSubMessage::Message { channel, message } => {
                assert_eq!(channel, Bytes::from("news"));
                assert_eq!(message, Bytes::from("Hello!"));
            }
            _ => panic!("Expected Message"),
        }
    }

    #[test]
    fn test_pattern_subscribe() {
        let registry = PubSubRegistry::new();
        let (id, mut rx) = registry.register_subscriber();

        registry.psubscribe(id, Bytes::from("news.*"));
        rx.try_recv().unwrap(); // Consume psubscribe confirmation

        let count = registry.publish(b"news.sports", Bytes::from("Goal!"));
        assert_eq!(count, 1);

        let msg = rx.try_recv().unwrap();
        match msg {
            PubSubMessage::PatternMessage {
                pattern,
                channel,
                message,
            } => {
                assert_eq!(pattern, Bytes::from("news.*"));
                assert_eq!(channel, Bytes::from("news.sports"));
                assert_eq!(message, Bytes::from("Goal!"));
            }
            _ => panic!("Expected PatternMessage"),
        }
    }

    #[test]
    fn test_channels() {
        let registry = PubSubRegistry::new();
        let (id, _rx) = registry.register_subscriber();

        registry.subscribe(id, Bytes::from("news"));
        registry.subscribe(id, Bytes::from("sports"));

        let channels = registry.channels(None);
        assert_eq!(channels.len(), 2);
        assert!(channels.contains(&Bytes::from("news")));
        assert!(channels.contains(&Bytes::from("sports")));
    }

    #[test]
    fn test_numsub() {
        let registry = PubSubRegistry::new();
        let (id1, _rx1) = registry.register_subscriber();
        let (id2, _rx2) = registry.register_subscriber();

        registry.subscribe(id1, Bytes::from("news"));
        registry.subscribe(id2, Bytes::from("news"));

        let counts = registry.numsub(&[Bytes::from("news"), Bytes::from("sports")]);
        assert_eq!(counts.get(&Bytes::from("news")), Some(&2));
        assert_eq!(counts.get(&Bytes::from("sports")), Some(&0));
    }

    #[test]
    fn test_numpat() {
        let registry = PubSubRegistry::new();
        let (id, _rx) = registry.register_subscriber();

        registry.psubscribe(id, Bytes::from("news.*"));
        registry.psubscribe(id, Bytes::from("sports.*"));

        assert_eq!(registry.numpat(), 2);
    }
}

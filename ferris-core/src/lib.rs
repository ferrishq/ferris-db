//! ferris-core: Core data structures and storage engine for ferris-db
//!
//! This crate provides the fundamental building blocks:
//! - `KeyStore`: Sharded concurrent key-value storage
//! - `RedisValue`: Enum representing all Redis data types
//! - `Entry`: Value with metadata (TTL, version, etc.)
//! - Expiry management for key TTLs
//! - Memory tracking and eviction policies

#![deny(unsafe_code)]
#![warn(clippy::all, clippy::pedantic, clippy::nursery)]
#![allow(clippy::module_name_repetitions)]

pub mod store;
pub mod value;
pub mod expiry;
pub mod memory;
pub mod blocking;
pub mod pubsub;

pub use store::{KeyStore, Database};
pub use value::{RedisValue, Entry};
pub use expiry::ExpiryManager;
pub use memory::{MemoryTracker, EvictionPolicy};
pub use blocking::BlockingRegistry;
pub use pubsub::{PubSubRegistry, PubSubMessage, SubscriberId};

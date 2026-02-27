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
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::must_use_candidate)]
#![allow(clippy::redundant_closure_for_method_calls)]
#![allow(clippy::use_self)]
#![allow(clippy::missing_const_for_fn)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_lossless)]
#![allow(clippy::uninlined_format_args)]
#![allow(clippy::significant_drop_tightening)]
#![allow(clippy::option_if_let_else)]
#![allow(clippy::redundant_pub_crate)]
#![allow(clippy::unchecked_time_subtraction)]
#![allow(clippy::suboptimal_flops)]
#![allow(clippy::unused_self)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::derivable_impls)]
#![allow(clippy::should_implement_trait)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::unwrap_or_default)]
#![allow(clippy::needless_pass_by_value)]
#![allow(clippy::explicit_iter_loop)]
#![allow(clippy::map_unwrap_or)]
#![allow(clippy::zero_sized_map_values)]
#![allow(clippy::iter_kv_map)]
#![allow(clippy::unnecessary_map_or)]
// Allow unwrap/expect in tests
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
#![cfg_attr(test, allow(clippy::used_underscore_binding))]
#![cfg_attr(test, allow(clippy::redundant_clone))]
#![cfg_attr(test, allow(unused_must_use, unused_mut))]

pub mod blocking;
pub mod expiry;
pub mod memory;
pub mod pubsub;
pub mod serialization;
pub mod store;
pub mod value;
pub mod watch_registry;

pub use blocking::BlockingRegistry;
pub use expiry::ExpiryManager;
pub use memory::{EvictionPolicy, MemoryTracker};
pub use pubsub::{PubSubMessage, PubSubRegistry, SubscriberId};
pub use serialization::{deserialize, serialize, SerdeError};
pub use store::{Database, KeyStore, UpdateAction};
pub use value::{
    Consumer, ConsumerGroup, Entry, PendingEntry, RedisValue, StreamData, StreamEntry, StreamId,
};
pub use watch_registry::{ClientId, WatchRegistry};

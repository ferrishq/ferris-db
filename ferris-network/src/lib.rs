//! ferris-network: Networking layer for ferris-db
//!
//! Handles TCP connections, RESP protocol framing, and connection lifecycle.
//! Each incoming connection is handled in its own async task.

#![deny(unsafe_code)]
#![warn(clippy::all, clippy::pedantic, clippy::nursery)]
#![allow(clippy::module_name_repetitions)]

pub mod listener;
pub mod connection;
pub mod shutdown;

pub use listener::{Server, ServerConfig, ServerError};
pub use shutdown::Shutdown;

/// Default port for ferris-db (6380 to avoid conflict with Redis)
pub const DEFAULT_PORT: u16 = 6380;

/// Default maximum number of concurrent connections
pub const DEFAULT_MAX_CONNECTIONS: usize = 10_000;

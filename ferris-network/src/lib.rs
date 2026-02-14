//! ferris-network: Networking layer for ferris-db
//!
//! Handles TCP connections, RESP protocol framing, and connection lifecycle.
//! Each incoming connection is handled in its own async task.

#![deny(unsafe_code)]
#![warn(clippy::all, clippy::pedantic, clippy::nursery)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::must_use_candidate)]
#![allow(clippy::redundant_closure_for_method_calls)]
#![allow(clippy::use_self)]
#![allow(clippy::missing_const_for_fn)]
#![allow(clippy::uninlined_format_args)]
#![allow(clippy::significant_drop_tightening)]
#![allow(clippy::option_if_let_else)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::unused_async)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::map_unwrap_or)]
#![allow(clippy::semicolon_if_nothing_returned)]
#![allow(clippy::iter_with_drain)]
#![allow(clippy::needless_continue)]
#![allow(clippy::ignored_unit_patterns)]
#![allow(clippy::inefficient_to_string)]
#![allow(clippy::similar_names)]
// Allow unwrap/expect in tests
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
#![cfg_attr(test, allow(unused_must_use, unused_mut))]

pub mod connection;
pub mod listener;
pub mod shutdown;

pub use listener::{Server, ServerConfig, ServerError};
pub use shutdown::Shutdown;

/// Default port for ferris-db (6380 to avoid conflict with Redis)
pub const DEFAULT_PORT: u16 = 6380;

/// Default maximum number of concurrent connections
pub const DEFAULT_MAX_CONNECTIONS: usize = 10_000;

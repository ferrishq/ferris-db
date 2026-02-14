//! ferris-test-utils: Test utilities for ferris-db
//!
//! Provides helpers for testing ferris-db:
//! - `TestServer`: Spawn a ferris-db server on a random port
//! - `TestClient`: Simple RESP client for test assertions
//! - PropTest generators for fuzzing

#![deny(unsafe_code)]
#![warn(clippy::all, clippy::pedantic, clippy::nursery)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::must_use_candidate)]
#![allow(clippy::use_self)]
#![allow(clippy::missing_const_for_fn)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::redundant_closure)]
#![allow(clippy::uninlined_format_args)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::significant_drop_tightening)]
// Allow unwrap/expect in tests (this is a test utility crate)
#![allow(clippy::unwrap_used, clippy::expect_used)]
#![allow(clippy::inefficient_to_string)]
#![allow(clippy::unreadable_literal)]
#![allow(clippy::ignore_without_reason)]
#![allow(clippy::manual_ok_err)]
#![allow(clippy::option_if_let_else)]

pub mod client;
pub mod generators;
pub mod server;

pub use client::TestClient;
pub use server::TestServer;

//! ferris-test-utils: Test utilities for ferris-db
//!
//! Provides helpers for testing ferris-db:
//! - `TestServer`: Spawn a ferris-db server on a random port
//! - `TestClient`: Simple RESP client for test assertions
//! - PropTest generators for fuzzing

#![deny(unsafe_code)]
#![warn(clippy::all, clippy::pedantic, clippy::nursery)]
#![allow(clippy::module_name_repetitions)]

pub mod server;
pub mod client;
pub mod generators;

pub use server::TestServer;
pub use client::TestClient;

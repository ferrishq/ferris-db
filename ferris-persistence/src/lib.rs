//! ferris-persistence: AOF persistence engine for ferris-db
//!
//! Provides append-only file persistence for durability.

#![deny(unsafe_code)]
#![warn(clippy::all, clippy::pedantic, clippy::nursery)]
#![allow(clippy::module_name_repetitions)]

pub mod aof;
pub mod error;
pub mod fsync;

pub use aof::{AofReader, AofWriter, AofConfig, FsyncMode};
pub use error::{PersistenceError, PersistenceResult};

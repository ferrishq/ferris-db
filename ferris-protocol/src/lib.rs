//! ferris-protocol: RESP2 and RESP3 protocol codec for ferris-db
//!
//! This crate implements the Redis Serialization Protocol (RESP) versions 2 and 3.
//! It provides:
//! - Parsers for decoding RESP data from bytes
//! - Serializers for encoding RESP data to bytes
//! - Tokio codec integration for framed I/O
//! - Command parsing utilities

#![deny(unsafe_code)]
#![warn(clippy::all, clippy::pedantic, clippy::nursery)]
#![allow(clippy::module_name_repetitions)]

pub mod codec;
pub mod command;
pub mod error;
pub mod resp2;
pub mod resp3;
pub mod types;

pub use codec::{ProtocolVersion, RespCodec};
pub use command::Command;
pub use error::ProtocolError;
pub use types::RespValue;

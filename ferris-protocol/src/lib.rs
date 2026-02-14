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
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::must_use_candidate)]
#![allow(clippy::redundant_closure_for_method_calls)]
#![allow(clippy::match_same_arms)]
#![allow(clippy::use_self)]
#![allow(clippy::missing_const_for_fn)]
#![allow(clippy::manual_let_else)]
#![allow(clippy::option_if_let_else)]
#![allow(clippy::manual_find)]
#![allow(clippy::significant_drop_tightening)]
#![allow(clippy::single_match_else)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::approx_constant)]
#![allow(clippy::inefficient_to_string)]
// Allow unwrap/expect in tests
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

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

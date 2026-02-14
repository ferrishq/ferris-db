//! ferris-persistence: AOF persistence engine for ferris-db
//!
//! Provides append-only file persistence for durability.

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
#![allow(clippy::uninlined_format_args)]
#![allow(clippy::significant_drop_tightening)]
#![allow(clippy::suspicious_open_options)]
#![allow(clippy::match_same_arms)]
#![allow(clippy::if_not_else)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::option_if_let_else)]
#![allow(clippy::manual_let_else)]
#![allow(clippy::unnecessary_wraps)]
#![allow(clippy::cognitive_complexity)]
#![allow(clippy::ignored_unit_patterns)]
#![allow(clippy::single_match_else)]
// Allow unwrap/expect in tests
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
#![cfg_attr(test, allow(unused_must_use, unused_mut))]

pub mod aof;
pub mod error;
pub mod fsync;

pub use aof::{AofConfig, AofReader, AofWriter, FsyncMode};
pub use error::{PersistenceError, PersistenceResult};

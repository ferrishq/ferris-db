//! ferris-commands: Command implementations for ferris-db
//!
//! This crate contains all Redis command handlers organized by category.

#![deny(unsafe_code)]
#![warn(clippy::all, clippy::pedantic, clippy::nursery)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::similar_names)]

pub mod context;
pub mod error;
pub mod executor;
pub mod registry;

// Command modules
pub mod connection;
pub mod geo;
pub mod hash;
pub mod hyperloglog;
pub mod key;
pub mod list;
pub mod pubsub;
pub mod server;
pub mod set;
pub mod sorted_set;
pub mod string;
pub mod transaction;

pub use context::CommandContext;
pub use error::CommandError;
pub use executor::CommandExecutor;
pub use registry::{CommandFlags, CommandRegistry, CommandSpec};

use bytes::Bytes;
use ferris_protocol::RespValue;
use std::time::Duration;

/// Result type for command execution
pub type CommandResult = Result<RespValue, CommandError>;

/// Command handler function type
pub type CommandHandler = fn(&mut CommandContext, &[RespValue]) -> CommandResult;

/// Describes a blocking action that the connection handler should perform.
///
/// When a blocking command (BLPOP, BRPOP, etc.) finds no data immediately,
/// it returns `BlockingAction` to tell the connection handler to:
/// 1. Register for notifications on the specified keys
/// 2. Wait until notified (or timeout)
/// 3. Re-execute the retry function to try popping again
///
/// The synchronous handler parses arguments and does the first try;
/// if data exists, it returns a normal `RespValue`.
/// If no data exists, it returns `Err(CommandError::Block(action))`.
#[derive(Debug, Clone)]
pub struct BlockingAction {
    /// Database index the command operates on
    pub db_index: usize,
    /// Keys to watch for notifications
    pub keys: Vec<Bytes>,
    /// Timeout duration (None = block forever / 0 timeout)
    pub timeout: Option<Duration>,
    /// The original command and args to re-execute on wakeup
    pub command_name: String,
    /// The original arguments to replay
    pub args: Vec<RespValue>,
}

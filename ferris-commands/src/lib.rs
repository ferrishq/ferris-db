//! ferris-commands: Command implementations for ferris-db
//!
//! This crate contains all Redis command handlers organized by category.

// Allow unescaped brackets in doc comments (Redis command syntax uses [optional] notation)
#![allow(rustdoc::broken_intra_doc_links)]
#![allow(rustdoc::invalid_html_tags)]
#![deny(unsafe_code)]
#![warn(clippy::all, clippy::pedantic, clippy::nursery)]
#![allow(clippy::large_stack_arrays)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::similar_names)]
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
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::uninlined_format_args)]
#![allow(clippy::significant_drop_tightening)]
#![allow(clippy::option_if_let_else)]
#![allow(clippy::redundant_pub_crate)]
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
#![allow(clippy::manual_let_else)]
#![allow(clippy::redundant_closure)]
#![allow(clippy::needless_pass_by_ref_mut)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::match_same_arms)]
#![allow(clippy::single_match_else)]
#![allow(clippy::items_after_statements)]
#![allow(clippy::cognitive_complexity)]
#![allow(clippy::redundant_clone)]
#![allow(clippy::iter_cloned_collect)]
#![allow(clippy::manual_div_ceil)]
#![allow(clippy::if_not_else)]
#![allow(clippy::match_wildcard_for_single_variants)]
#![allow(clippy::struct_field_names)]
#![allow(clippy::needless_for_each)]
#![allow(clippy::unnecessary_wraps)]
#![allow(clippy::ref_option)]
#![allow(clippy::unnecessary_map_or)]
#![allow(clippy::let_underscore_untyped)]
#![allow(clippy::zero_sized_map_values)]
#![allow(clippy::unnecessary_unwrap)]
#![allow(clippy::unwrap_used)]
#![allow(clippy::if_same_then_else)]
#![allow(clippy::needless_range_loop)]
#![allow(clippy::collapsible_else_if)]
#![allow(clippy::inefficient_to_string)]
#![allow(clippy::iter_kv_map)]
#![allow(clippy::range_plus_one)]
#![allow(clippy::needless_lifetimes)]
#![allow(clippy::too_long_first_doc_paragraph)]
#![allow(clippy::type_complexity)]
#![allow(clippy::branches_sharing_code)]
#![allow(clippy::comparison_chain)]
#![allow(clippy::explicit_counter_loop)]
#![allow(clippy::single_char_pattern)]
#![allow(clippy::format_push_string)]
#![allow(clippy::manual_string_new)]
#![allow(clippy::struct_excessive_bools)]
#![allow(clippy::needless_return)]
#![allow(clippy::new_without_default)]
#![allow(clippy::naive_bytecount)]
#![allow(clippy::bool_to_int_with_if)]
#![allow(clippy::manual_range_contains)]
#![allow(clippy::unreadable_literal)]
#![allow(clippy::suspicious_operation_groupings)]
#![allow(clippy::missing_fields_in_debug)]
#![allow(clippy::items_after_test_module)]
// Allow various lints in tests
#![cfg_attr(test, allow(clippy::expect_used))]
#![cfg_attr(test, allow(clippy::used_underscore_binding))]
#![cfg_attr(test, allow(unused_must_use, unused_mut, unused_variables))]
#![cfg_attr(test, allow(clippy::default_trait_access))]
#![cfg_attr(test, allow(clippy::approx_constant))]
#![cfg_attr(test, allow(clippy::unchecked_time_subtraction))]
#![cfg_attr(test, allow(clippy::len_zero))]
#![cfg_attr(test, allow(clippy::redundant_pattern_matching))]

pub mod context;
pub mod error;
pub mod executor;
pub mod registry;

// Command modules
pub mod acl;
pub mod cluster;
pub mod connection;
pub mod geo;
pub mod hash;
pub mod hyperloglog;
pub mod key;
pub mod list;
pub mod pubsub;
pub mod replication;
pub mod scripting;
pub mod server;
pub mod set;
pub mod sorted_set;
pub mod stream;
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

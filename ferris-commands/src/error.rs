//! Command error types

use crate::BlockingAction;
use thiserror::Error;

/// Errors that can occur during command execution
#[derive(Error, Debug)]
pub enum CommandError {
    /// Wrong number of arguments for command
    #[error("ERR wrong number of arguments for '{0}' command")]
    WrongArity(String),

    /// Operation against wrong type
    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,

    /// Value is not an integer or out of range
    #[error("ERR value is not an integer or out of range")]
    NotAnInteger,

    /// Value is not a valid float
    #[error("ERR value is not a valid float")]
    NotAFloat,

    /// Syntax error
    #[error("ERR syntax error")]
    SyntaxError,

    /// Unknown command
    #[error("ERR unknown command '{0}'")]
    UnknownCommand(String),

    /// Invalid argument
    #[error("ERR {0}")]
    InvalidArgument(String),

    /// Authentication required
    #[error("NOAUTH Authentication required")]
    NoAuth,

    /// Invalid password
    #[error("WRONGPASS invalid username-password pair")]
    WrongPass,

    /// Invalid database index
    #[error("ERR invalid DB index")]
    InvalidDbIndex,

    /// Out of memory
    #[error("OOM command not allowed when used memory > 'maxmemory'")]
    OutOfMemory,

    /// Key not found (for commands that require existing key)
    #[error("ERR no such key")]
    NoSuchKey,

    /// Unknown subcommand
    #[error("ERR unknown subcommand '{0}'. Try {1} HELP.")]
    UnknownSubcommand(String, String),

    /// Internal error
    #[error("ERR {0}")]
    Internal(String),

    /// Blocking command needs to wait for data.
    /// This is not a real error — it signals the connection handler
    /// to block until data becomes available or timeout expires.
    #[error("BLOCK")]
    Block(BlockingAction),
}

impl CommandError {
    /// Convert error to a RESP error string
    #[must_use]
    pub fn to_resp_string(&self) -> String {
        self.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_messages() {
        let err = CommandError::WrongArity("GET".to_string());
        assert!(err.to_string().contains("wrong number of arguments"));
        assert!(err.to_string().contains("GET"));

        let err = CommandError::WrongType;
        assert!(err.to_string().contains("WRONGTYPE"));

        let err = CommandError::UnknownCommand("FOO".to_string());
        assert!(err.to_string().contains("unknown command"));
        assert!(err.to_string().contains("FOO"));
    }
}

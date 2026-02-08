//! Protocol error types

use thiserror::Error;

/// Errors that can occur during protocol parsing or serialization
#[derive(Error, Debug)]
pub enum ProtocolError {
    /// Incomplete data - need more bytes to parse
    #[error("incomplete data, need more bytes")]
    Incomplete,

    /// Invalid RESP data format
    #[error("invalid RESP format: {0}")]
    InvalidFormat(String),

    /// Invalid UTF-8 in string data
    #[error("invalid UTF-8: {0}")]
    InvalidUtf8(#[from] std::string::FromUtf8Error),

    /// Invalid integer format
    #[error("invalid integer: {0}")]
    InvalidInteger(#[from] std::num::ParseIntError),

    /// Invalid float format
    #[error("invalid float: {0}")]
    InvalidFloat(#[from] std::num::ParseFloatError),

    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Unknown RESP type prefix
    #[error("unknown RESP type prefix: {0}")]
    UnknownType(char),
}

//! Error types for persistence operations

use thiserror::Error;

/// Result type alias for persistence operations
pub type PersistenceResult<T> = Result<T, PersistenceError>;

/// Errors that can occur during persistence operations
#[derive(Error, Debug)]
pub enum PersistenceError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Failed to serialize command: {0}")]
    Serialization(String),

    #[error("Failed to parse AOF file: {0}")]
    Parse(String),

    #[error("AOF writer channel closed")]
    ChannelClosed,

    #[error("AOF writer task panicked")]
    TaskPanicked,

    #[error("Invalid AOF configuration: {0}")]
    InvalidConfig(String),
}

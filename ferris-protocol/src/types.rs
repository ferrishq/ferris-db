//! RESP value types
//!
//! Represents all possible values in RESP2 and RESP3 protocols.

use bytes::Bytes;
use std::collections::HashMap;

/// A value in the RESP protocol
#[derive(Clone, Debug, PartialEq)]
pub enum RespValue {
    /// Simple string: +OK\r\n
    SimpleString(String),

    /// Error: -ERR message\r\n
    Error(String),

    /// Integer: :1000\r\n
    Integer(i64),

    /// Bulk string: $6\r\nfoobar\r\n
    BulkString(Bytes),

    /// Array: *2\r\n...
    Array(Vec<RespValue>),

    /// Null (RESP2: $-1\r\n or *-1\r\n, RESP3: _\r\n)
    Null,

    // RESP3 types below
    /// Double: ,3.14\r\n (RESP3)
    Double(f64),

    /// Boolean: #t\r\n or #f\r\n (RESP3)
    Boolean(bool),

    /// Big number: (12345...\r\n (RESP3)
    BigNumber(String),

    /// Verbatim string: =15\r\ntxt:Hello world\r\n (RESP3)
    VerbatimString { encoding: String, data: Bytes },

    /// Map: %2\r\n... (RESP3)
    Map(HashMap<String, RespValue>),

    /// Set: ~3\r\n... (RESP3)
    Set(Vec<RespValue>),

    /// Push: >3\r\n... (RESP3) - for pub/sub and client tracking
    Push(Vec<RespValue>),
}

impl RespValue {
    /// Create a simple "OK" response
    #[must_use]
    pub fn ok() -> Self {
        Self::SimpleString("OK".to_string())
    }

    /// Create an error response
    #[must_use]
    pub fn error(msg: impl Into<String>) -> Self {
        Self::Error(msg.into())
    }

    /// Create a bulk string from bytes
    #[must_use]
    pub fn bulk_string(data: impl Into<Bytes>) -> Self {
        Self::BulkString(data.into())
    }

    /// Create a bulk string from a string
    #[must_use]
    pub fn bulk_string_from_str(s: &str) -> Self {
        Self::BulkString(Bytes::from(s.to_owned()))
    }

    /// Create an integer response
    #[must_use]
    pub const fn integer(n: i64) -> Self {
        Self::Integer(n)
    }

    /// Create an array response
    #[must_use]
    pub fn array(items: Vec<RespValue>) -> Self {
        Self::Array(items)
    }

    /// Try to get this value as bytes
    #[must_use]
    pub fn as_bytes(&self) -> Option<&Bytes> {
        match self {
            Self::BulkString(b) => Some(b),
            _ => None,
        }
    }

    /// Try to get this value as a string
    #[must_use]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::SimpleString(s) | Self::Error(s) => Some(s),
            Self::BulkString(b) => std::str::from_utf8(b).ok(),
            _ => None,
        }
    }

    /// Try to get this value as an integer
    #[must_use]
    pub const fn as_integer(&self) -> Option<i64> {
        match self {
            Self::Integer(n) => Some(*n),
            _ => None,
        }
    }

    /// Try to get this value as an array
    #[must_use]
    pub fn as_array(&self) -> Option<&[RespValue]> {
        match self {
            Self::Array(arr) => Some(arr),
            _ => None,
        }
    }

    /// Check if this is a null value
    #[must_use]
    pub const fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Check if this is an error
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Error(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ok_response() {
        let ok = RespValue::ok();
        assert_eq!(ok, RespValue::SimpleString("OK".to_string()));
    }

    #[test]
    fn test_error_response() {
        let err = RespValue::error("something went wrong");
        assert!(err.is_error());
        assert_eq!(err.as_str(), Some("something went wrong"));
    }

    #[test]
    fn test_bulk_string() {
        let bulk = RespValue::bulk_string_from_str("hello");
        assert_eq!(bulk.as_str(), Some("hello"));
        assert!(bulk.as_bytes().is_some());
    }

    #[test]
    fn test_integer() {
        let int = RespValue::integer(42);
        assert_eq!(int.as_integer(), Some(42));
    }

    #[test]
    fn test_array() {
        let arr = RespValue::array(vec![
            RespValue::bulk_string_from_str("foo"),
            RespValue::bulk_string_from_str("bar"),
        ]);
        assert!(arr.as_array().is_some());
        assert_eq!(arr.as_array().map(|a| a.len()), Some(2));
    }

    #[test]
    fn test_null() {
        let null = RespValue::Null;
        assert!(null.is_null());
        assert!(!null.is_error());
    }
}

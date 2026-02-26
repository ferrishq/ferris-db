//! Response comparison logic for parity testing

use ferris_protocol::RespValue;
use std::collections::HashSet;

/// Comparison mode for responses
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareMode {
    /// Exact match required (order matters for arrays)
    Strict,
    /// Arrays can be in any order (for SMEMBERS, KEYS, etc.)
    UnorderedArray,
    /// Error type matches but message can differ
    ErrorTypeOnly,
    /// Numeric comparison with tolerance
    NumericTolerance,
}

/// Result of comparing Redis and ferris-db responses
#[derive(Debug, Clone)]
pub struct ParityResult {
    /// The command that was executed
    pub command: String,
    /// Redis response (if successful)
    pub redis: Option<RespValue>,
    /// ferris-db response (if successful)
    pub ferris: Option<RespValue>,
    /// Redis connection/parse error (if any)
    pub redis_error: Option<String>,
    /// ferris-db connection/parse error (if any)
    pub ferris_error: Option<String>,
    /// Whether the responses match
    pub matches: bool,
}

impl ParityResult {
    /// Check if both servers returned a response
    pub fn both_responded(&self) -> bool {
        self.redis.is_some() && self.ferris.is_some()
    }

    /// Get a short description of the result
    pub fn summary(&self) -> String {
        if self.matches {
            format!("{}: PASS", self.command)
        } else if let (Some(r), Some(f)) = (&self.redis, &self.ferris) {
            format!(
                "{}: FAIL\n  Redis:  {}\n  Ferris: {}",
                self.command,
                format_resp_short(r),
                format_resp_short(f)
            )
        } else {
            format!(
                "{}: ERROR\n  Redis error:  {:?}\n  Ferris error: {:?}",
                self.command, self.redis_error, self.ferris_error
            )
        }
    }
}

/// Compare two RESP values
pub fn compare_responses(redis: &RespValue, ferris: &RespValue, mode: CompareMode) -> bool {
    match mode {
        CompareMode::Strict => compare_strict(redis, ferris),
        CompareMode::UnorderedArray => compare_unordered(redis, ferris),
        CompareMode::ErrorTypeOnly => compare_error_type(redis, ferris),
        CompareMode::NumericTolerance => compare_numeric(redis, ferris),
    }
}

/// Strict comparison - exact match required
fn compare_strict(redis: &RespValue, ferris: &RespValue) -> bool {
    match (redis, ferris) {
        (RespValue::SimpleString(a), RespValue::SimpleString(b)) => a == b,
        (RespValue::Integer(a), RespValue::Integer(b)) => a == b,
        (RespValue::BulkString(a), RespValue::BulkString(b)) => a == b,
        (RespValue::Null, RespValue::Null) => true,

        // Both should be errors (message content may differ slightly)
        (RespValue::Error(a), RespValue::Error(b)) => {
            // Check if error types match (e.g., both start with "WRONGTYPE" or "ERR")
            let a_type = a.split_whitespace().next().unwrap_or("");
            let b_type = b.split_whitespace().next().unwrap_or("");
            a_type == b_type
        }

        // Arrays must match element by element
        (RespValue::Array(a), RespValue::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| compare_strict(x, y))
        }

        // RESP3 types
        (RespValue::Double(a), RespValue::Double(b)) => (a - b).abs() < f64::EPSILON,
        (RespValue::Boolean(a), RespValue::Boolean(b)) => a == b,
        (RespValue::Map(a), RespValue::Map(b)) => {
            a.len() == b.len()
                && a.iter()
                    .all(|(k, v)| b.get(k).is_some_and(|bv| compare_strict(v, bv)))
        }
        (RespValue::Set(a), RespValue::Set(b)) => compare_unordered_arrays(a, b),

        // Type mismatch
        _ => false,
    }
}

/// Unordered comparison - for commands like SMEMBERS, KEYS
fn compare_unordered(redis: &RespValue, ferris: &RespValue) -> bool {
    match (redis, ferris) {
        (RespValue::Array(a), RespValue::Array(b)) => compare_unordered_arrays(a, b),
        // Fall back to strict for non-arrays
        _ => compare_strict(redis, ferris),
    }
}

/// Compare two arrays ignoring order
fn compare_unordered_arrays(a: &[RespValue], b: &[RespValue]) -> bool {
    if a.len() != b.len() {
        return false;
    }

    // Convert to comparable strings for simple comparison
    let a_set: HashSet<String> = a.iter().map(format_resp_comparable).collect();
    let b_set: HashSet<String> = b.iter().map(format_resp_comparable).collect();

    a_set == b_set
}

/// Error type comparison - only checks error prefix matches
fn compare_error_type(redis: &RespValue, ferris: &RespValue) -> bool {
    match (redis, ferris) {
        (RespValue::Error(a), RespValue::Error(b)) => {
            let a_type = a.split_whitespace().next().unwrap_or("");
            let b_type = b.split_whitespace().next().unwrap_or("");
            a_type == b_type
        }
        // Non-errors use strict comparison
        _ => compare_strict(redis, ferris),
    }
}

/// Numeric comparison with tolerance (for TTL, scores, etc.)
fn compare_numeric(redis: &RespValue, ferris: &RespValue) -> bool {
    match (redis, ferris) {
        (RespValue::Integer(a), RespValue::Integer(b)) => {
            // Allow ±1 for timing-sensitive values
            (*a - *b).abs() <= 1
        }
        (RespValue::Double(a), RespValue::Double(b)) => {
            // Allow small floating point differences
            (*a - *b).abs() < 0.0001
        }
        // Fall back to strict for non-numeric
        _ => compare_strict(redis, ferris),
    }
}

/// Format a RespValue for comparison (used in unordered set comparison)
fn format_resp_comparable(value: &RespValue) -> String {
    match value {
        RespValue::SimpleString(s) => format!("S:{s}"),
        RespValue::Error(s) => format!("E:{s}"),
        RespValue::Integer(i) => format!("I:{i}"),
        RespValue::BulkString(b) => {
            format!("B:{}", String::from_utf8_lossy(b))
        }
        RespValue::Null => "N".to_string(),
        RespValue::Array(arr) => {
            let items: Vec<String> = arr.iter().map(format_resp_comparable).collect();
            format!("A:[{}]", items.join(","))
        }
        RespValue::Double(d) => format!("D:{d}"),
        RespValue::Boolean(b) => format!("O:{b}"),
        RespValue::Map(m) => {
            let mut items: Vec<String> = m
                .iter()
                .map(|(k, v)| format!("{}={}", k, format_resp_comparable(v)))
                .collect();
            items.sort();
            format!("M:{{{}}}", items.join(","))
        }
        RespValue::Set(s) => {
            let mut items: Vec<String> = s.iter().map(format_resp_comparable).collect();
            items.sort();
            format!("T:[{}]", items.join(","))
        }
        RespValue::Push(p) => {
            let items: Vec<String> = p.iter().map(format_resp_comparable).collect();
            format!("P:[{}]", items.join(","))
        }
        RespValue::BigNumber(b) => format!("N:{b}"),
        RespValue::VerbatimString { encoding, data } => {
            format!("V:{}:{}", encoding, String::from_utf8_lossy(data))
        }
    }
}

/// Format a RespValue for short display
fn format_resp_short(value: &RespValue) -> String {
    match value {
        RespValue::SimpleString(s) => format!("+{s}"),
        RespValue::Error(s) => format!("-{s}"),
        RespValue::Integer(i) => format!(":{i}"),
        RespValue::BulkString(b) => {
            let s = String::from_utf8_lossy(b);
            if s.len() > 50 {
                format!("${}... ({} bytes)", &s[..47], b.len())
            } else {
                format!("${s}")
            }
        }
        RespValue::Null => "(nil)".to_string(),
        RespValue::Array(arr) => {
            if arr.len() > 5 {
                format!("[{} items]", arr.len())
            } else {
                let items: Vec<String> = arr.iter().map(format_resp_short).collect();
                format!("[{}]", items.join(", "))
            }
        }
        RespValue::Double(d) => format!(",{d}"),
        RespValue::Boolean(b) => format!("#{b}"),
        _ => format!("{value:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_compare_simple_string() {
        let a = RespValue::SimpleString("OK".to_string());
        let b = RespValue::SimpleString("OK".to_string());
        assert!(compare_responses(&a, &b, CompareMode::Strict));

        let c = RespValue::SimpleString("PONG".to_string());
        assert!(!compare_responses(&a, &c, CompareMode::Strict));
    }

    #[test]
    fn test_compare_integer() {
        let a = RespValue::Integer(42);
        let b = RespValue::Integer(42);
        assert!(compare_responses(&a, &b, CompareMode::Strict));

        let c = RespValue::Integer(43);
        assert!(!compare_responses(&a, &c, CompareMode::Strict));

        // With tolerance
        assert!(compare_responses(&a, &c, CompareMode::NumericTolerance));
    }

    #[test]
    fn test_compare_bulk_string() {
        let a = RespValue::BulkString(Bytes::from("hello"));
        let b = RespValue::BulkString(Bytes::from("hello"));
        assert!(compare_responses(&a, &b, CompareMode::Strict));

        let c = RespValue::BulkString(Bytes::from("world"));
        assert!(!compare_responses(&a, &c, CompareMode::Strict));
    }

    #[test]
    fn test_compare_array_ordered() {
        let a = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("a")),
            RespValue::BulkString(Bytes::from("b")),
        ]);
        let b = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("a")),
            RespValue::BulkString(Bytes::from("b")),
        ]);
        assert!(compare_responses(&a, &b, CompareMode::Strict));

        let c = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("b")),
            RespValue::BulkString(Bytes::from("a")),
        ]);
        assert!(!compare_responses(&a, &c, CompareMode::Strict));
    }

    #[test]
    fn test_compare_array_unordered() {
        let a = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("a")),
            RespValue::BulkString(Bytes::from("b")),
        ]);
        let b = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("b")),
            RespValue::BulkString(Bytes::from("a")),
        ]);
        assert!(compare_responses(&a, &b, CompareMode::UnorderedArray));
    }

    #[test]
    fn test_compare_error_type() {
        let a = RespValue::Error("WRONGTYPE Operation against a key".to_string());
        let b = RespValue::Error("WRONGTYPE Key holds wrong type".to_string());
        assert!(compare_responses(&a, &b, CompareMode::Strict));
        assert!(compare_responses(&a, &b, CompareMode::ErrorTypeOnly));

        let c = RespValue::Error("ERR syntax error".to_string());
        assert!(!compare_responses(&a, &c, CompareMode::Strict));
    }

    #[test]
    fn test_compare_null() {
        let a = RespValue::Null;
        let b = RespValue::Null;
        assert!(compare_responses(&a, &b, CompareMode::Strict));
    }
}

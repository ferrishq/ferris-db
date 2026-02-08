//! PropTest generators for fuzzing

use bytes::Bytes;
use proptest::prelude::*;

/// Generate a random key (alphanumeric, 1-100 chars)
pub fn key() -> impl Strategy<Value = Bytes> {
    "[a-zA-Z0-9:_-]{1,100}".prop_map(|s| Bytes::from(s))
}

/// Generate a random string value (any bytes, 0-1000 bytes)
pub fn value() -> impl Strategy<Value = Bytes> {
    prop::collection::vec(any::<u8>(), 0..1000).prop_map(Bytes::from)
}

/// Generate a random integer
pub fn integer() -> impl Strategy<Value = i64> {
    any::<i64>()
}

/// Generate a random positive integer
pub fn positive_integer() -> impl Strategy<Value = i64> {
    1i64..=i64::MAX
}

/// Generate a random score for sorted sets
pub fn score() -> impl Strategy<Value = f64> {
    prop_oneof![
        // Normal range
        -1e10f64..1e10f64,
        // Edge cases
        Just(0.0),
        Just(-0.0),
        Just(f64::INFINITY),
        Just(f64::NEG_INFINITY),
    ]
}

/// Generate a random field name for hashes
pub fn field() -> impl Strategy<Value = Bytes> {
    "[a-zA-Z][a-zA-Z0-9_]{0,50}".prop_map(|s| Bytes::from(s))
}

/// Generate a random TTL in seconds (1 second to 1 year)
pub fn ttl_seconds() -> impl Strategy<Value = u64> {
    1u64..31536000u64
}

/// Generate a random TTL in milliseconds
pub fn ttl_millis() -> impl Strategy<Value = u64> {
    1u64..31536000000u64
}

#[cfg(test)]
mod tests {
    use super::*;

    proptest! {
        #[test]
        fn test_key_generator(k in key()) {
            assert!(!k.is_empty());
            assert!(k.len() <= 100);
        }

        #[test]
        fn test_value_generator(v in value()) {
            assert!(v.len() <= 1000);
        }

        #[test]
        fn test_score_generator(s in score()) {
            // Score should be a valid f64 (not NaN for comparison purposes)
            assert!(!s.is_nan());
        }

        #[test]
        fn test_ttl_generator(t in ttl_seconds()) {
            assert!(t >= 1);
            assert!(t <= 31536000);
        }
    }
}

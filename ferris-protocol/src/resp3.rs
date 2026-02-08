//! RESP3 protocol parser and serializer
//!
//! RESP3 adds new types: Map, Set, Double, Boolean, BigNumber, VerbatimString, Push

use crate::error::ProtocolError;
use crate::types::RespValue;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::collections::HashMap;

/// Parse a RESP3 value from the buffer
pub fn parse(buf: &mut BytesMut) -> Result<Option<RespValue>, ProtocolError> {
    if buf.is_empty() {
        return Ok(None);
    }

    match buf[0] {
        // RESP2 types
        b'+' => parse_simple_string(buf),
        b'-' => parse_error(buf),
        b':' => parse_integer(buf),
        b'$' => parse_bulk_string(buf),
        b'*' => parse_array(buf),
        // RESP3 types
        b'_' => parse_null(buf),
        b',' => parse_double(buf),
        b'#' => parse_boolean(buf),
        b'(' => parse_big_number(buf),
        b'=' => parse_verbatim_string(buf),
        b'%' => parse_map(buf),
        b'~' => parse_set(buf),
        b'>' => parse_push(buf),
        other => Err(ProtocolError::UnknownType(other as char)),
    }
}

/// Serialize a RESP3 value to bytes
pub fn serialize(value: &RespValue, buf: &mut BytesMut) {
    match value {
        RespValue::SimpleString(s) => {
            buf.put_u8(b'+');
            buf.put_slice(s.as_bytes());
            buf.put_slice(b"\r\n");
        }
        RespValue::Error(s) => {
            buf.put_u8(b'-');
            buf.put_slice(s.as_bytes());
            buf.put_slice(b"\r\n");
        }
        RespValue::Integer(n) => {
            buf.put_u8(b':');
            buf.put_slice(n.to_string().as_bytes());
            buf.put_slice(b"\r\n");
        }
        RespValue::BulkString(data) => {
            buf.put_u8(b'$');
            buf.put_slice(data.len().to_string().as_bytes());
            buf.put_slice(b"\r\n");
            buf.put_slice(data);
            buf.put_slice(b"\r\n");
        }
        RespValue::Null => {
            buf.put_slice(b"_\r\n");
        }
        RespValue::Array(items) => {
            buf.put_u8(b'*');
            buf.put_slice(items.len().to_string().as_bytes());
            buf.put_slice(b"\r\n");
            for item in items {
                serialize(item, buf);
            }
        }
        RespValue::Double(d) => {
            buf.put_u8(b',');
            if d.is_infinite() {
                if d.is_sign_positive() {
                    buf.put_slice(b"inf");
                } else {
                    buf.put_slice(b"-inf");
                }
            } else if d.is_nan() {
                buf.put_slice(b"nan");
            } else {
                buf.put_slice(d.to_string().as_bytes());
            }
            buf.put_slice(b"\r\n");
        }
        RespValue::Boolean(b) => {
            buf.put_u8(b'#');
            buf.put_u8(if *b { b't' } else { b'f' });
            buf.put_slice(b"\r\n");
        }
        RespValue::BigNumber(s) => {
            buf.put_u8(b'(');
            buf.put_slice(s.as_bytes());
            buf.put_slice(b"\r\n");
        }
        RespValue::VerbatimString { encoding, data } => {
            buf.put_u8(b'=');
            let total_len = 3 + 1 + data.len(); // encoding (3) + : + data
            buf.put_slice(total_len.to_string().as_bytes());
            buf.put_slice(b"\r\n");
            buf.put_slice(encoding.as_bytes());
            buf.put_u8(b':');
            buf.put_slice(data);
            buf.put_slice(b"\r\n");
        }
        RespValue::Map(map) => {
            buf.put_u8(b'%');
            buf.put_slice(map.len().to_string().as_bytes());
            buf.put_slice(b"\r\n");
            for (k, v) in map {
                serialize(&RespValue::BulkString(Bytes::from(k.clone())), buf);
                serialize(v, buf);
            }
        }
        RespValue::Set(items) => {
            buf.put_u8(b'~');
            buf.put_slice(items.len().to_string().as_bytes());
            buf.put_slice(b"\r\n");
            for item in items {
                serialize(item, buf);
            }
        }
        RespValue::Push(items) => {
            buf.put_u8(b'>');
            buf.put_slice(items.len().to_string().as_bytes());
            buf.put_slice(b"\r\n");
            for item in items {
                serialize(item, buf);
            }
        }
    }
}

// Reuse RESP2 parsers for common types
fn parse_simple_string(buf: &mut BytesMut) -> Result<Option<RespValue>, ProtocolError> {
    crate::resp2::parse(buf)
}

fn parse_error(buf: &mut BytesMut) -> Result<Option<RespValue>, ProtocolError> {
    crate::resp2::parse(buf)
}

fn parse_integer(buf: &mut BytesMut) -> Result<Option<RespValue>, ProtocolError> {
    crate::resp2::parse(buf)
}

fn parse_bulk_string(buf: &mut BytesMut) -> Result<Option<RespValue>, ProtocolError> {
    crate::resp2::parse(buf)
}

fn parse_array(buf: &mut BytesMut) -> Result<Option<RespValue>, ProtocolError> {
    // For arrays, we need to use RESP3 parsing for nested elements
    if let Some(end) = find_crlf(buf, 1) {
        let len_str = std::str::from_utf8(&buf[1..end])
            .map_err(|e| ProtocolError::InvalidFormat(e.to_string()))?;
        let len: i64 = len_str.parse()?;

        if len == -1 {
            buf.advance(end + 2);
            return Ok(Some(RespValue::Null));
        }

        let len = len as usize;
        let saved = buf.clone();
        buf.advance(end + 2);

        let mut items = Vec::with_capacity(len);
        for _ in 0..len {
            match parse(buf)? {
                Some(item) => items.push(item),
                None => {
                    *buf = saved;
                    return Ok(None);
                }
            }
        }

        Ok(Some(RespValue::Array(items)))
    } else {
        Ok(None)
    }
}

fn parse_null(buf: &mut BytesMut) -> Result<Option<RespValue>, ProtocolError> {
    if buf.len() >= 3 && &buf[..3] == b"_\r\n" {
        buf.advance(3);
        Ok(Some(RespValue::Null))
    } else if buf.len() < 3 {
        Ok(None)
    } else {
        Err(ProtocolError::InvalidFormat("invalid null".to_string()))
    }
}

fn parse_double(buf: &mut BytesMut) -> Result<Option<RespValue>, ProtocolError> {
    if let Some(end) = find_crlf(buf, 1) {
        let s = std::str::from_utf8(&buf[1..end])
            .map_err(|e| ProtocolError::InvalidFormat(e.to_string()))?;

        let d = match s {
            "inf" => f64::INFINITY,
            "-inf" => f64::NEG_INFINITY,
            "nan" => f64::NAN,
            _ => s.parse()?,
        };

        buf.advance(end + 2);
        Ok(Some(RespValue::Double(d)))
    } else {
        Ok(None)
    }
}

fn parse_boolean(buf: &mut BytesMut) -> Result<Option<RespValue>, ProtocolError> {
    if buf.len() >= 4 {
        let b = match buf[1] {
            b't' => true,
            b'f' => false,
            _ => return Err(ProtocolError::InvalidFormat("invalid boolean".to_string())),
        };
        buf.advance(4);
        Ok(Some(RespValue::Boolean(b)))
    } else {
        Ok(None)
    }
}

fn parse_big_number(buf: &mut BytesMut) -> Result<Option<RespValue>, ProtocolError> {
    if let Some(end) = find_crlf(buf, 1) {
        let s = String::from_utf8(buf[1..end].to_vec())?;
        buf.advance(end + 2);
        Ok(Some(RespValue::BigNumber(s)))
    } else {
        Ok(None)
    }
}

fn parse_verbatim_string(buf: &mut BytesMut) -> Result<Option<RespValue>, ProtocolError> {
    if let Some(end) = find_crlf(buf, 1) {
        let len_str = std::str::from_utf8(&buf[1..end])
            .map_err(|e| ProtocolError::InvalidFormat(e.to_string()))?;
        let len: usize = len_str.parse()?;

        let total_len = end + 2 + len + 2;
        if buf.len() < total_len {
            return Ok(None);
        }

        buf.advance(end + 2);
        let content = buf.split_to(len);
        buf.advance(2);

        if content.len() < 4 || content[3] != b':' {
            return Err(ProtocolError::InvalidFormat(
                "invalid verbatim string".to_string(),
            ));
        }

        let encoding = String::from_utf8(content[..3].to_vec())?;
        let data = Bytes::copy_from_slice(&content[4..]);

        Ok(Some(RespValue::VerbatimString { encoding, data }))
    } else {
        Ok(None)
    }
}

fn parse_map(buf: &mut BytesMut) -> Result<Option<RespValue>, ProtocolError> {
    if let Some(end) = find_crlf(buf, 1) {
        let len_str = std::str::from_utf8(&buf[1..end])
            .map_err(|e| ProtocolError::InvalidFormat(e.to_string()))?;
        let len: usize = len_str.parse()?;

        let saved = buf.clone();
        buf.advance(end + 2);

        let mut map = HashMap::with_capacity(len);
        for _ in 0..len {
            let key = match parse(buf)? {
                Some(RespValue::BulkString(b)) => String::from_utf8(b.to_vec())
                    .map_err(|e| ProtocolError::InvalidFormat(e.to_string()))?,
                Some(RespValue::SimpleString(s)) => s,
                Some(_) => {
                    return Err(ProtocolError::InvalidFormat(
                        "map key must be string".to_string(),
                    ))
                }
                None => {
                    *buf = saved;
                    return Ok(None);
                }
            };
            let value = match parse(buf)? {
                Some(v) => v,
                None => {
                    *buf = saved;
                    return Ok(None);
                }
            };
            map.insert(key, value);
        }

        Ok(Some(RespValue::Map(map)))
    } else {
        Ok(None)
    }
}

fn parse_set(buf: &mut BytesMut) -> Result<Option<RespValue>, ProtocolError> {
    if let Some(end) = find_crlf(buf, 1) {
        let len_str = std::str::from_utf8(&buf[1..end])
            .map_err(|e| ProtocolError::InvalidFormat(e.to_string()))?;
        let len: usize = len_str.parse()?;

        let saved = buf.clone();
        buf.advance(end + 2);

        let mut items = Vec::with_capacity(len);
        for _ in 0..len {
            match parse(buf)? {
                Some(item) => items.push(item),
                None => {
                    *buf = saved;
                    return Ok(None);
                }
            }
        }

        Ok(Some(RespValue::Set(items)))
    } else {
        Ok(None)
    }
}

fn parse_push(buf: &mut BytesMut) -> Result<Option<RespValue>, ProtocolError> {
    if let Some(end) = find_crlf(buf, 1) {
        let len_str = std::str::from_utf8(&buf[1..end])
            .map_err(|e| ProtocolError::InvalidFormat(e.to_string()))?;
        let len: usize = len_str.parse()?;

        let saved = buf.clone();
        buf.advance(end + 2);

        let mut items = Vec::with_capacity(len);
        for _ in 0..len {
            match parse(buf)? {
                Some(item) => items.push(item),
                None => {
                    *buf = saved;
                    return Ok(None);
                }
            }
        }

        Ok(Some(RespValue::Push(items)))
    } else {
        Ok(None)
    }
}

fn find_crlf(buf: &[u8], start: usize) -> Option<usize> {
    for i in start..buf.len().saturating_sub(1) {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Some(i);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_null() {
        let mut buf = BytesMut::from("_\r\n");
        let result = parse(&mut buf).unwrap().unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_parse_double() {
        let mut buf = BytesMut::from(",3.14159\r\n");
        let result = parse(&mut buf).unwrap().unwrap();
        if let RespValue::Double(d) = result {
            assert!((d - 3.14159).abs() < 0.0001);
        } else {
            panic!("expected Double");
        }
    }

    #[test]
    fn test_parse_double_infinity() {
        let mut buf = BytesMut::from(",inf\r\n");
        let result = parse(&mut buf).unwrap().unwrap();
        assert_eq!(result, RespValue::Double(f64::INFINITY));

        let mut buf = BytesMut::from(",-inf\r\n");
        let result = parse(&mut buf).unwrap().unwrap();
        assert_eq!(result, RespValue::Double(f64::NEG_INFINITY));
    }

    #[test]
    fn test_parse_boolean() {
        let mut buf = BytesMut::from("#t\r\n");
        let result = parse(&mut buf).unwrap().unwrap();
        assert_eq!(result, RespValue::Boolean(true));

        let mut buf = BytesMut::from("#f\r\n");
        let result = parse(&mut buf).unwrap().unwrap();
        assert_eq!(result, RespValue::Boolean(false));
    }

    #[test]
    fn test_parse_big_number() {
        let mut buf = BytesMut::from("(3492890328409238509324850943850943825024385\r\n");
        let result = parse(&mut buf).unwrap().unwrap();
        assert_eq!(
            result,
            RespValue::BigNumber("3492890328409238509324850943850943825024385".to_string())
        );
    }

    #[test]
    fn test_parse_set() {
        let mut buf = BytesMut::from("~3\r\n+a\r\n+b\r\n+c\r\n");
        let result = parse(&mut buf).unwrap().unwrap();
        assert_eq!(
            result,
            RespValue::Set(vec![
                RespValue::SimpleString("a".to_string()),
                RespValue::SimpleString("b".to_string()),
                RespValue::SimpleString("c".to_string()),
            ])
        );
    }

    #[test]
    fn test_serialize_null() {
        let mut buf = BytesMut::new();
        serialize(&RespValue::Null, &mut buf);
        assert_eq!(&buf[..], b"_\r\n");
    }

    #[test]
    fn test_serialize_double() {
        let mut buf = BytesMut::new();
        serialize(&RespValue::Double(3.14), &mut buf);
        assert!(buf.starts_with(b","));
        assert!(buf.ends_with(b"\r\n"));
    }

    #[test]
    fn test_serialize_boolean() {
        let mut buf = BytesMut::new();
        serialize(&RespValue::Boolean(true), &mut buf);
        assert_eq!(&buf[..], b"#t\r\n");

        let mut buf = BytesMut::new();
        serialize(&RespValue::Boolean(false), &mut buf);
        assert_eq!(&buf[..], b"#f\r\n");
    }
}

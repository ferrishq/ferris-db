//! RESP2 protocol parser and serializer

use crate::error::ProtocolError;
use crate::types::RespValue;
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Parse a RESP2 value from the buffer
///
/// Returns `Ok(Some(value))` if a complete value was parsed,
/// `Ok(None)` if more data is needed,
/// `Err(e)` if the data is invalid.
///
/// This parser also supports Redis inline commands (plain text format)
/// for compatibility with tools like redis-benchmark. Inline commands
/// are text lines like "PING\r\n" or "SET key value\r\n".
pub fn parse(buf: &mut BytesMut) -> Result<Option<RespValue>, ProtocolError> {
    if buf.is_empty() {
        return Ok(None);
    }

    match buf[0] {
        b'+' => parse_simple_string(buf),
        b'-' => parse_error(buf),
        b':' => parse_integer(buf),
        b'$' => parse_bulk_string(buf),
        b'*' => parse_array(buf),
        _ => {
            // Try to parse as inline command (plain text format)
            // This is for compatibility with redis-benchmark and telnet
            parse_inline_command(buf)
        }
    }
}

/// Serialize a RESP value to bytes
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
            buf.put_slice(b"$-1\r\n");
        }
        RespValue::Array(items) => {
            buf.put_u8(b'*');
            buf.put_slice(items.len().to_string().as_bytes());
            buf.put_slice(b"\r\n");
            for item in items {
                serialize(item, buf);
            }
        }
        // RESP3 types serialized as RESP2 equivalents
        RespValue::Double(d) => {
            let s = d.to_string();
            buf.put_u8(b'$');
            buf.put_slice(s.len().to_string().as_bytes());
            buf.put_slice(b"\r\n");
            buf.put_slice(s.as_bytes());
            buf.put_slice(b"\r\n");
        }
        RespValue::Boolean(b) => {
            if *b {
                buf.put_slice(b":1\r\n");
            } else {
                buf.put_slice(b":0\r\n");
            }
        }
        RespValue::Map(map) => {
            buf.put_u8(b'*');
            buf.put_slice((map.len() * 2).to_string().as_bytes());
            buf.put_slice(b"\r\n");
            for (k, v) in map {
                serialize(&RespValue::BulkString(Bytes::from(k.clone())), buf);
                serialize(v, buf);
            }
        }
        RespValue::Set(items) | RespValue::Push(items) => {
            buf.put_u8(b'*');
            buf.put_slice(items.len().to_string().as_bytes());
            buf.put_slice(b"\r\n");
            for item in items {
                serialize(item, buf);
            }
        }
        RespValue::BigNumber(s) => {
            // Serialize as bulk string for RESP2 compatibility
            buf.put_u8(b'$');
            buf.put_slice(s.len().to_string().as_bytes());
            buf.put_slice(b"\r\n");
            buf.put_slice(s.as_bytes());
            buf.put_slice(b"\r\n");
        }
        RespValue::VerbatimString { data, .. } => {
            // Serialize as bulk string for RESP2 compatibility
            buf.put_u8(b'$');
            buf.put_slice(data.len().to_string().as_bytes());
            buf.put_slice(b"\r\n");
            buf.put_slice(data);
            buf.put_slice(b"\r\n");
        }
    }
}

fn parse_simple_string(buf: &mut BytesMut) -> Result<Option<RespValue>, ProtocolError> {
    if let Some(end) = find_crlf(buf, 1) {
        let line = buf.split_to(end + 2);
        let s = String::from_utf8(line[1..end].to_vec())?;
        Ok(Some(RespValue::SimpleString(s)))
    } else {
        Ok(None)
    }
}

fn parse_error(buf: &mut BytesMut) -> Result<Option<RespValue>, ProtocolError> {
    if let Some(end) = find_crlf(buf, 1) {
        let line = buf.split_to(end + 2);
        let s = String::from_utf8(line[1..end].to_vec())?;
        Ok(Some(RespValue::Error(s)))
    } else {
        Ok(None)
    }
}

fn parse_integer(buf: &mut BytesMut) -> Result<Option<RespValue>, ProtocolError> {
    if let Some(end) = find_crlf(buf, 1) {
        let line = buf.split_to(end + 2);
        let s = std::str::from_utf8(&line[1..end])
            .map_err(|e| ProtocolError::InvalidFormat(e.to_string()))?;
        let n: i64 = s.parse()?;
        Ok(Some(RespValue::Integer(n)))
    } else {
        Ok(None)
    }
}

fn parse_bulk_string(buf: &mut BytesMut) -> Result<Option<RespValue>, ProtocolError> {
    if let Some(end) = find_crlf(buf, 1) {
        let len_str = std::str::from_utf8(&buf[1..end])
            .map_err(|e| ProtocolError::InvalidFormat(e.to_string()))?;
        let len: i64 = len_str.parse()?;

        if len == -1 {
            buf.advance(end + 2);
            return Ok(Some(RespValue::Null));
        }

        let len = len as usize;
        let total_len = end + 2 + len + 2; // prefix + CRLF + data + CRLF

        if buf.len() < total_len {
            return Ok(None); // Need more data
        }

        buf.advance(end + 2); // Skip length line
        let data = buf.split_to(len).freeze();
        buf.advance(2); // Skip trailing CRLF

        Ok(Some(RespValue::BulkString(data)))
    } else {
        Ok(None)
    }
}

fn parse_array(buf: &mut BytesMut) -> Result<Option<RespValue>, ProtocolError> {
    if let Some(end) = find_crlf(buf, 1) {
        let len_str = std::str::from_utf8(&buf[1..end])
            .map_err(|e| ProtocolError::InvalidFormat(e.to_string()))?;
        let len: i64 = len_str.parse()?;

        if len == -1 {
            buf.advance(end + 2);
            return Ok(Some(RespValue::Null));
        }

        let len = len as usize;

        // Save the current position
        let saved = buf.clone();
        buf.advance(end + 2); // Skip length line

        let mut items = Vec::with_capacity(len);
        for _ in 0..len {
            match parse(buf)? {
                Some(item) => items.push(item),
                None => {
                    // Restore buffer and return incomplete
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

/// Parse an inline command (plain text format used by redis-benchmark and telnet)
///
/// Inline commands are simple text lines terminated by \r\n or \n:
/// - "PING\r\n" -> Array([BulkString("PING")])
/// - "SET key value\r\n" -> Array([BulkString("SET"), BulkString("key"), BulkString("value")])
///
/// Arguments can be:
/// - Space-separated tokens
/// - Quoted strings with spaces (using double quotes)
fn parse_inline_command(buf: &mut BytesMut) -> Result<Option<RespValue>, ProtocolError> {
    // Find line ending - either \r\n or just \n
    let line_end = if let Some(pos) = find_crlf(buf, 0) {
        Some((pos, 2)) // Found \r\n, consume 2 bytes
    } else if let Some(pos) = buf.iter().position(|&b| b == b'\n') {
        Some((pos, 1)) // Found \n, consume 1 byte
    } else {
        None
    };

    let (end, consume) = match line_end {
        Some(x) => x,
        None => return Ok(None), // Need more data
    };

    // Extract the line
    let line = buf.split_to(end + consume);
    let line_str = std::str::from_utf8(&line[..end])
        .map_err(|e| ProtocolError::InvalidFormat(e.to_string()))?;

    // Parse the line into tokens (space-separated, with support for quoted strings)
    let tokens = parse_inline_tokens(line_str)?;

    if tokens.is_empty() {
        // Empty line - return an error
        return Err(ProtocolError::InvalidFormat("Empty inline command".to_string()));
    }

    // Convert tokens to RESP Array of BulkStrings (same as RESP protocol commands)
    let args: Vec<RespValue> = tokens
        .into_iter()
        .map(|s| RespValue::BulkString(Bytes::from(s)))
        .collect();

    Ok(Some(RespValue::Array(args)))
}

/// Parse inline command tokens, supporting quoted strings
fn parse_inline_tokens(line: &str) -> Result<Vec<String>, ProtocolError> {
    let mut tokens = Vec::new();
    let mut current_token = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '"' if !in_quotes => {
                // Start of quoted string
                in_quotes = true;
            }
            '"' if in_quotes => {
                // End of quoted string
                in_quotes = false;
            }
            ' ' | '\t' if !in_quotes => {
                // Whitespace outside quotes - token separator
                if !current_token.is_empty() {
                    tokens.push(current_token.clone());
                    current_token.clear();
                }
            }
            '\\' if in_quotes => {
                // Escape sequence in quoted string
                if let Some(next_ch) = chars.next() {
                    match next_ch {
                        'n' => current_token.push('\n'),
                        'r' => current_token.push('\r'),
                        't' => current_token.push('\t'),
                        '\\' => current_token.push('\\'),
                        '"' => current_token.push('"'),
                        _ => {
                            current_token.push('\\');
                            current_token.push(next_ch);
                        }
                    }
                } else {
                    return Err(ProtocolError::InvalidFormat(
                        "Incomplete escape sequence".to_string(),
                    ));
                }
            }
            _ => {
                // Regular character
                current_token.push(ch);
            }
        }
    }

    // Add the last token if any
    if !current_token.is_empty() {
        tokens.push(current_token);
    }

    if in_quotes {
        return Err(ProtocolError::InvalidFormat(
            "Unclosed quoted string".to_string(),
        ));
    }

    Ok(tokens)
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
    fn test_parse_simple_string() {
        let mut buf = BytesMut::from("+OK\r\n");
        let result = parse(&mut buf).unwrap().unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
        assert!(buf.is_empty());
    }

    #[test]
    fn test_parse_error() {
        let mut buf = BytesMut::from("-ERR something went wrong\r\n");
        let result = parse(&mut buf).unwrap().unwrap();
        assert_eq!(
            result,
            RespValue::Error("ERR something went wrong".to_string())
        );
    }

    #[test]
    fn test_parse_integer() {
        let mut buf = BytesMut::from(":1000\r\n");
        let result = parse(&mut buf).unwrap().unwrap();
        assert_eq!(result, RespValue::Integer(1000));

        let mut buf = BytesMut::from(":-42\r\n");
        let result = parse(&mut buf).unwrap().unwrap();
        assert_eq!(result, RespValue::Integer(-42));
    }

    #[test]
    fn test_parse_bulk_string() {
        let mut buf = BytesMut::from("$6\r\nfoobar\r\n");
        let result = parse(&mut buf).unwrap().unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from("foobar")));
    }

    #[test]
    fn test_parse_null_bulk_string() {
        let mut buf = BytesMut::from("$-1\r\n");
        let result = parse(&mut buf).unwrap().unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_parse_array() {
        let mut buf = BytesMut::from("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        let result = parse(&mut buf).unwrap().unwrap();
        assert_eq!(
            result,
            RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("foo")),
                RespValue::BulkString(Bytes::from("bar")),
            ])
        );
    }

    #[test]
    fn test_parse_empty_array() {
        let mut buf = BytesMut::from("*0\r\n");
        let result = parse(&mut buf).unwrap().unwrap();
        assert_eq!(result, RespValue::Array(vec![]));
    }

    #[test]
    fn test_parse_null_array() {
        let mut buf = BytesMut::from("*-1\r\n");
        let result = parse(&mut buf).unwrap().unwrap();
        assert_eq!(result, RespValue::Null);
    }

    #[test]
    fn test_parse_incomplete() {
        let mut buf = BytesMut::from("+OK\r");
        let result = parse(&mut buf).unwrap();
        assert!(result.is_none());

        let mut buf = BytesMut::from("$6\r\nfoo");
        let result = parse(&mut buf).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_serialize_simple_string() {
        let mut buf = BytesMut::new();
        serialize(&RespValue::SimpleString("OK".to_string()), &mut buf);
        assert_eq!(&buf[..], b"+OK\r\n");
    }

    #[test]
    fn test_serialize_error() {
        let mut buf = BytesMut::new();
        serialize(&RespValue::Error("ERR test".to_string()), &mut buf);
        assert_eq!(&buf[..], b"-ERR test\r\n");
    }

    #[test]
    fn test_serialize_integer() {
        let mut buf = BytesMut::new();
        serialize(&RespValue::Integer(42), &mut buf);
        assert_eq!(&buf[..], b":42\r\n");
    }

    #[test]
    fn test_serialize_bulk_string() {
        let mut buf = BytesMut::new();
        serialize(&RespValue::BulkString(Bytes::from("hello")), &mut buf);
        assert_eq!(&buf[..], b"$5\r\nhello\r\n");
    }

    #[test]
    fn test_serialize_null() {
        let mut buf = BytesMut::new();
        serialize(&RespValue::Null, &mut buf);
        assert_eq!(&buf[..], b"$-1\r\n");
    }

    #[test]
    fn test_serialize_array() {
        let mut buf = BytesMut::new();
        serialize(
            &RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("foo")),
                RespValue::Integer(42),
            ]),
            &mut buf,
        );
        assert_eq!(&buf[..], b"*2\r\n$3\r\nfoo\r\n:42\r\n");
    }

    #[test]
    fn test_roundtrip() {
        let values = vec![
            RespValue::SimpleString("OK".to_string()),
            RespValue::Error("ERR test".to_string()),
            RespValue::Integer(12345),
            RespValue::BulkString(Bytes::from("hello world")),
            RespValue::Null,
            RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("SET")),
                RespValue::BulkString(Bytes::from("key")),
                RespValue::BulkString(Bytes::from("value")),
            ]),
        ];

        for original in values {
            let mut buf = BytesMut::new();
            serialize(&original, &mut buf);
            let parsed = parse(&mut buf).unwrap().unwrap();
            assert_eq!(original, parsed);
        }
    }

    #[test]
    fn test_parse_inline_command_simple() {
        let mut buf = BytesMut::from("PING\r\n");
        let result = parse(&mut buf).unwrap().unwrap();
        assert_eq!(
            result,
            RespValue::Array(vec![RespValue::BulkString(Bytes::from("PING"))])
        );
        assert!(buf.is_empty());
    }

    #[test]
    fn test_parse_inline_command_with_args() {
        let mut buf = BytesMut::from("SET key value\r\n");
        let result = parse(&mut buf).unwrap().unwrap();
        assert_eq!(
            result,
            RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("SET")),
                RespValue::BulkString(Bytes::from("key")),
                RespValue::BulkString(Bytes::from("value")),
            ])
        );
    }

    #[test]
    fn test_parse_inline_command_lf_only() {
        // Support \n without \r (common in telnet)
        let mut buf = BytesMut::from("PING\n");
        let result = parse(&mut buf).unwrap().unwrap();
        assert_eq!(
            result,
            RespValue::Array(vec![RespValue::BulkString(Bytes::from("PING"))])
        );
    }

    #[test]
    fn test_parse_inline_command_with_quotes() {
        let mut buf = BytesMut::from("SET key \"value with spaces\"\r\n");
        let result = parse(&mut buf).unwrap().unwrap();
        assert_eq!(
            result,
            RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("SET")),
                RespValue::BulkString(Bytes::from("key")),
                RespValue::BulkString(Bytes::from("value with spaces")),
            ])
        );
    }

    #[test]
    fn test_parse_inline_command_multiple_spaces() {
        let mut buf = BytesMut::from("SET   key    value\r\n");
        let result = parse(&mut buf).unwrap().unwrap();
        assert_eq!(
            result,
            RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("SET")),
                RespValue::BulkString(Bytes::from("key")),
                RespValue::BulkString(Bytes::from("value")),
            ])
        );
    }

    #[test]
    fn test_parse_inline_command_incomplete() {
        let mut buf = BytesMut::from("PING");
        let result = parse(&mut buf).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_inline_tokens() {
        assert_eq!(
            parse_inline_tokens("PING").unwrap(),
            vec!["PING".to_string()]
        );
        assert_eq!(
            parse_inline_tokens("SET key value").unwrap(),
            vec!["SET".to_string(), "key".to_string(), "value".to_string()]
        );
        assert_eq!(
            parse_inline_tokens("SET key \"value with spaces\"").unwrap(),
            vec![
                "SET".to_string(),
                "key".to_string(),
                "value with spaces".to_string()
            ]
        );
    }
}

//! Tokio codec for RESP protocol framing

use crate::error::ProtocolError;
use crate::types::RespValue;
use crate::{resp2, resp3};
use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};

/// Protocol version for RESP
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProtocolVersion {
    /// RESP2 (Redis 2.0+)
    #[default]
    Resp2,
    /// RESP3 (Redis 6.0+)
    Resp3,
}

/// Codec for encoding/decoding RESP protocol messages
#[derive(Debug, Clone)]
pub struct RespCodec {
    /// Current protocol version
    version: ProtocolVersion,
}

impl Default for RespCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl RespCodec {
    /// Create a new RESP codec (defaults to RESP2)
    #[must_use]
    pub fn new() -> Self {
        Self {
            version: ProtocolVersion::Resp2,
        }
    }

    /// Create a new RESP codec with a specific version
    #[must_use]
    pub const fn with_version(version: ProtocolVersion) -> Self {
        Self { version }
    }

    /// Get the current protocol version
    #[must_use]
    pub const fn version(&self) -> ProtocolVersion {
        self.version
    }

    /// Set the protocol version
    pub fn set_version(&mut self, version: ProtocolVersion) {
        self.version = version;
    }
}

impl Decoder for RespCodec {
    type Item = RespValue;
    type Error = ProtocolError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        match self.version {
            ProtocolVersion::Resp2 => resp2::parse(src),
            ProtocolVersion::Resp3 => resp3::parse(src),
        }
    }
}

impl Encoder<RespValue> for RespCodec {
    type Error = ProtocolError;

    fn encode(&mut self, item: RespValue, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match self.version {
            ProtocolVersion::Resp2 => resp2::serialize(&item, dst),
            ProtocolVersion::Resp3 => resp3::serialize(&item, dst),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_codec_default_version() {
        let codec = RespCodec::new();
        assert_eq!(codec.version(), ProtocolVersion::Resp2);
    }

    #[test]
    fn test_codec_with_version() {
        let codec = RespCodec::with_version(ProtocolVersion::Resp3);
        assert_eq!(codec.version(), ProtocolVersion::Resp3);
    }

    #[test]
    fn test_codec_decode_resp2() {
        let mut codec = RespCodec::new();
        let mut buf = BytesMut::from("+OK\r\n");
        let result = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
    }

    #[test]
    fn test_codec_encode_resp2() {
        let mut codec = RespCodec::new();
        let mut buf = BytesMut::new();
        codec
            .encode(RespValue::SimpleString("OK".to_string()), &mut buf)
            .unwrap();
        assert_eq!(&buf[..], b"+OK\r\n");
    }

    #[test]
    fn test_codec_decode_resp3() {
        let mut codec = RespCodec::with_version(ProtocolVersion::Resp3);
        let mut buf = BytesMut::from("#t\r\n");
        let result = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(result, RespValue::Boolean(true));
    }

    #[test]
    fn test_codec_encode_resp3() {
        let mut codec = RespCodec::with_version(ProtocolVersion::Resp3);
        let mut buf = BytesMut::new();
        codec.encode(RespValue::Boolean(true), &mut buf).unwrap();
        assert_eq!(&buf[..], b"#t\r\n");
    }

    #[test]
    fn test_codec_roundtrip() {
        let mut codec = RespCodec::new();
        let original = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("SET")),
            RespValue::BulkString(Bytes::from("key")),
            RespValue::BulkString(Bytes::from("value")),
        ]);

        let mut buf = BytesMut::new();
        codec.encode(original.clone(), &mut buf).unwrap();
        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(original, decoded);
    }
}

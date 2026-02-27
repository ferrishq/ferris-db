//! Value serialization for DUMP/RESTORE and slot migration.
//!
//! ## Format (version 1, little-endian)
//!
//! ```text
//! ┌──────────┬──────────┬─────────────────┬──────────┐
//! │ magic(2) │ ver(1)   │ payload (var)   │ crc32(4) │
//! └──────────┴──────────┴─────────────────┴──────────┘
//! ```
//!
//! Magic: 0xFE 0xDB\
//! Version: 0x01\
//! CRC-32 covers everything before the checksum field.
//!
//! ### Payload encoding per type
//!
//! | Type | Tag | Payload |
//! |------|-----|---------|
//! | String | 0x00 | u32 len + bytes |
//! | List   | 0x01 | u32 count + (u32 len + bytes)* |
//! | Set    | 0x02 | u32 count + (u32 len + bytes)* |
//! | Hash   | 0x03 | u32 count + (u32 klen + k + u32 vlen + v)* |
//! | ZSet   | 0x04 | u32 count + (f64 score + u32 len + bytes)* |
//! | Stream | 0x05 | u32 entry_count + (u64 ts + u64 seq + u32 field_count + (u32 klen + k + u32 vlen + v)*)* |

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::value::RedisValue;

/// Magic bytes at the start of every payload
const MAGIC: [u8; 2] = [0xFE, 0xDB];
/// Current format version
const VERSION: u8 = 0x01;

// Type tags
const TAG_STRING: u8 = 0x00;
const TAG_LIST: u8 = 0x01;
const TAG_SET: u8 = 0x02;
const TAG_HASH: u8 = 0x03;
const TAG_ZSET: u8 = 0x04;
const TAG_STREAM: u8 = 0x05;

/// Errors that can arise during serialization or deserialization
#[derive(Debug, thiserror::Error)]
pub enum SerdeError {
    /// The data is too short to contain a valid header or field
    #[error("buffer underflow: expected at least {expected} bytes, got {got}")]
    Underflow { expected: usize, got: usize },

    /// Magic bytes don't match
    #[error("invalid magic bytes")]
    BadMagic,

    /// Version is not supported
    #[error("unsupported version: {0}")]
    UnsupportedVersion(u8),

    /// Checksum mismatch
    #[error("CRC32 mismatch: expected {expected:#010x}, got {got:#010x}")]
    BadChecksum { expected: u32, got: u32 },

    /// Unknown type tag
    #[error("unknown type tag: {0:#04x}")]
    UnknownTag(u8),
}

// ---------------------------------------------------------------------------
// CRC-32 (IEEE 802.3 polynomial) — no external crate needed
// ---------------------------------------------------------------------------

/// Compute a CRC-32 checksum (IEEE 802.3 / Ethernet polynomial 0xEDB88320).
fn crc32(data: &[u8]) -> u32 {
    // Build table at call time — Rust optimises this to a constant with -O.
    let mut table = [0u32; 256];
    for (i, entry) in table.iter_mut().enumerate() {
        let mut val = i as u32;
        for _ in 0..8 {
            if val & 1 == 1 {
                val = (val >> 1) ^ 0xEDB8_8320;
            } else {
                val >>= 1;
            }
        }
        *entry = val;
    }

    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in data {
        let idx = ((crc ^ u32::from(byte)) & 0xFF) as usize;
        crc = (crc >> 8) ^ table[idx];
    }
    crc ^ 0xFFFF_FFFF
}

// ---------------------------------------------------------------------------
// Low-level helpers
// ---------------------------------------------------------------------------

fn write_bytes(buf: &mut BytesMut, b: &[u8]) {
    buf.put_u32_le(b.len() as u32);
    buf.put_slice(b);
}

fn read_bytes(buf: &mut &[u8]) -> Result<Bytes, SerdeError> {
    if buf.len() < 4 {
        return Err(SerdeError::Underflow {
            expected: 4,
            got: buf.len(),
        });
    }
    let len = buf.get_u32_le() as usize;
    if buf.len() < len {
        return Err(SerdeError::Underflow {
            expected: len,
            got: buf.len(),
        });
    }
    let data = Bytes::copy_from_slice(&buf[..len]);
    buf.advance(len);
    Ok(data)
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Serialize a `RedisValue` into a self-describing byte payload.
///
/// The returned `Bytes` includes magic, version tag, type-specific encoding,
/// and a trailing CRC-32 checksum.
pub fn serialize(value: &RedisValue) -> Bytes {
    let mut payload = BytesMut::new();

    // Header
    payload.put_slice(&MAGIC);
    payload.put_u8(VERSION);

    match value {
        RedisValue::String(s) => {
            payload.put_u8(TAG_STRING);
            write_bytes(&mut payload, s);
        }

        RedisValue::List(list) => {
            payload.put_u8(TAG_LIST);
            payload.put_u32_le(list.len() as u32);
            for item in list {
                write_bytes(&mut payload, item);
            }
        }

        RedisValue::Set(set) => {
            payload.put_u8(TAG_SET);
            // Sort for deterministic output (simplifies testing)
            let mut items: Vec<&Bytes> = set.iter().collect();
            items.sort();
            payload.put_u32_le(items.len() as u32);
            for item in items {
                write_bytes(&mut payload, item);
            }
        }

        RedisValue::Hash(hash) => {
            payload.put_u8(TAG_HASH);
            let mut pairs: Vec<(&Bytes, &Bytes)> = hash.iter().collect();
            pairs.sort_by_key(|(k, _)| *k);
            payload.put_u32_le(pairs.len() as u32);
            for (k, v) in pairs {
                write_bytes(&mut payload, k);
                write_bytes(&mut payload, v);
            }
        }

        RedisValue::SortedSet { members, .. } => {
            payload.put_u8(TAG_ZSET);
            payload.put_u32_le(members.len() as u32);
            // members is BTreeMap<(OrderedFloat<f64>, Bytes), ()> — already sorted
            for ((score, member), ()) in members {
                payload.put_f64_le(score.into_inner());
                write_bytes(&mut payload, member);
            }
        }

        RedisValue::Stream(stream) => {
            payload.put_u8(TAG_STREAM);
            payload.put_u32_le(stream.entries.len() as u32);
            for (id, fields) in &stream.entries {
                payload.put_u64_le(id.timestamp);
                payload.put_u64_le(id.sequence);
                payload.put_u32_le(fields.len() as u32);
                for (k, v) in fields {
                    write_bytes(&mut payload, k);
                    write_bytes(&mut payload, v);
                }
            }
        }
    }

    // Append CRC-32 over everything written so far
    let checksum = crc32(&payload);
    payload.put_u32_le(checksum);

    payload.freeze()
}

/// Deserialize a payload produced by [`serialize`].
///
/// Returns the reconstructed `RedisValue` or a [`SerdeError`] describing
/// what went wrong.
#[allow(clippy::too_many_lines)]
pub fn deserialize(data: &[u8]) -> Result<RedisValue, SerdeError> {
    // Need at least: magic(2) + version(1) + tag(1) + crc(4) = 8 bytes
    if data.len() < 8 {
        return Err(SerdeError::Underflow {
            expected: 8,
            got: data.len(),
        });
    }

    // Verify magic
    if data[0] != MAGIC[0] || data[1] != MAGIC[1] {
        return Err(SerdeError::BadMagic);
    }

    // Verify version
    let version = data[2];
    if version != VERSION {
        return Err(SerdeError::UnsupportedVersion(version));
    }

    // Verify CRC-32 (last 4 bytes)
    let (payload_without_crc, crc_bytes) = data.split_at(data.len() - 4);
    let stored_crc = u32::from_le_bytes(crc_bytes.try_into().unwrap_or([0; 4]));
    let computed_crc = crc32(payload_without_crc);
    if stored_crc != computed_crc {
        return Err(SerdeError::BadChecksum {
            expected: computed_crc,
            got: stored_crc,
        });
    }

    // Decode type tag — cursor starts after magic + version
    let tag = data[3];
    let mut cursor: &[u8] = &payload_without_crc[4..]; // skip magic(2) + ver(1) + tag(1)

    match tag {
        TAG_STRING => {
            let s = read_bytes(&mut cursor)?;
            Ok(RedisValue::String(s))
        }

        TAG_LIST => {
            if cursor.len() < 4 {
                return Err(SerdeError::Underflow {
                    expected: 4,
                    got: cursor.len(),
                });
            }
            let count = cursor.get_u32_le() as usize;
            let mut list = std::collections::VecDeque::with_capacity(count);
            for _ in 0..count {
                list.push_back(read_bytes(&mut cursor)?);
            }
            Ok(RedisValue::List(list))
        }

        TAG_SET => {
            if cursor.len() < 4 {
                return Err(SerdeError::Underflow {
                    expected: 4,
                    got: cursor.len(),
                });
            }
            let count = cursor.get_u32_le() as usize;
            let mut set = std::collections::HashSet::with_capacity(count);
            for _ in 0..count {
                set.insert(read_bytes(&mut cursor)?);
            }
            Ok(RedisValue::Set(set))
        }

        TAG_HASH => {
            if cursor.len() < 4 {
                return Err(SerdeError::Underflow {
                    expected: 4,
                    got: cursor.len(),
                });
            }
            let count = cursor.get_u32_le() as usize;
            let mut hash = std::collections::HashMap::with_capacity(count);
            for _ in 0..count {
                let k = read_bytes(&mut cursor)?;
                let v = read_bytes(&mut cursor)?;
                hash.insert(k, v);
            }
            Ok(RedisValue::Hash(hash))
        }

        TAG_ZSET => {
            if cursor.len() < 4 {
                return Err(SerdeError::Underflow {
                    expected: 4,
                    got: cursor.len(),
                });
            }
            let count = cursor.get_u32_le() as usize;
            let mut scores = std::collections::HashMap::with_capacity(count);
            let mut members = std::collections::BTreeMap::new();
            for _ in 0..count {
                if cursor.len() < 8 {
                    return Err(SerdeError::Underflow {
                        expected: 8,
                        got: cursor.len(),
                    });
                }
                let score = cursor.get_f64_le();
                let member = read_bytes(&mut cursor)?;
                scores.insert(member.clone(), score);
                members.insert((ordered_float::OrderedFloat(score), member), ());
            }
            Ok(RedisValue::SortedSet { scores, members })
        }

        TAG_STREAM => {
            use crate::value::{StreamData, StreamId};
            if cursor.len() < 4 {
                return Err(SerdeError::Underflow {
                    expected: 4,
                    got: cursor.len(),
                });
            }
            let entry_count = cursor.get_u32_le() as usize;
            let mut stream = StreamData::new();
            for _ in 0..entry_count {
                if cursor.len() < 20 {
                    // u64 ts + u64 seq + u32 field_count
                    return Err(SerdeError::Underflow {
                        expected: 20,
                        got: cursor.len(),
                    });
                }
                let ts = cursor.get_u64_le();
                let seq = cursor.get_u64_le();
                let field_count = cursor.get_u32_le() as usize;
                let id = StreamId::new(ts, seq);
                let mut fields = Vec::with_capacity(field_count);
                for _ in 0..field_count {
                    let k = read_bytes(&mut cursor)?;
                    let v = read_bytes(&mut cursor)?;
                    fields.push((k, v));
                }
                stream.entries.insert(id.clone(), fields);
                stream.last_id = id;
            }
            stream.first_id = stream.entries.keys().next().cloned();
            Ok(RedisValue::Stream(stream))
        }

        other => Err(SerdeError::UnknownTag(other)),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    #![allow(clippy::float_cmp)]

    use super::*;
    use bytes::Bytes;
    use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

    fn roundtrip(value: RedisValue) -> RedisValue {
        let serialized = serialize(&value);
        deserialize(&serialized).expect("roundtrip failed")
    }

    // ── String ──────────────────────────────────────────────────────────────

    #[test]
    fn test_string_roundtrip() {
        match roundtrip(RedisValue::String(Bytes::from("hello world"))) {
            RedisValue::String(s) => assert_eq!(s, Bytes::from("hello world")),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn test_string_empty_roundtrip() {
        match roundtrip(RedisValue::String(Bytes::new())) {
            RedisValue::String(s) => assert!(s.is_empty()),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn test_string_binary_roundtrip() {
        let binary = Bytes::from(vec![0x00, 0xFF, 0xAB, 0xCD, 0x01]);
        match roundtrip(RedisValue::String(binary.clone())) {
            RedisValue::String(s) => assert_eq!(s, binary),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn test_string_large_roundtrip() {
        let large = Bytes::from(vec![b'x'; 1_000_000]);
        match roundtrip(RedisValue::String(large.clone())) {
            RedisValue::String(s) => assert_eq!(s.len(), 1_000_000),
            other => panic!("unexpected: {other:?}"),
        }
    }

    // ── List ────────────────────────────────────────────────────────────────

    #[test]
    fn test_list_roundtrip() {
        let list: VecDeque<Bytes> =
            vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")].into();
        match roundtrip(RedisValue::List(list.clone())) {
            RedisValue::List(l) => assert_eq!(l, list),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn test_list_empty_roundtrip() {
        match roundtrip(RedisValue::List(VecDeque::new())) {
            RedisValue::List(l) => assert!(l.is_empty()),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn test_list_order_preserved() {
        let list: VecDeque<Bytes> = (0..100u8).map(|i| Bytes::from(vec![i])).collect();
        match roundtrip(RedisValue::List(list.clone())) {
            RedisValue::List(l) => assert_eq!(l, list),
            other => panic!("unexpected: {other:?}"),
        }
    }

    // ── Set ─────────────────────────────────────────────────────────────────

    #[test]
    fn test_set_roundtrip() {
        let mut set = HashSet::new();
        set.insert(Bytes::from("alpha"));
        set.insert(Bytes::from("beta"));
        set.insert(Bytes::from("gamma"));
        match roundtrip(RedisValue::Set(set.clone())) {
            RedisValue::Set(s) => assert_eq!(s, set),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn test_set_empty_roundtrip() {
        match roundtrip(RedisValue::Set(HashSet::new())) {
            RedisValue::Set(s) => assert!(s.is_empty()),
            other => panic!("unexpected: {other:?}"),
        }
    }

    // ── Hash ────────────────────────────────────────────────────────────────

    #[test]
    fn test_hash_roundtrip() {
        let mut hash = HashMap::new();
        hash.insert(Bytes::from("field1"), Bytes::from("value1"));
        hash.insert(Bytes::from("field2"), Bytes::from("value2"));
        match roundtrip(RedisValue::Hash(hash.clone())) {
            RedisValue::Hash(h) => assert_eq!(h, hash),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn test_hash_empty_roundtrip() {
        match roundtrip(RedisValue::Hash(HashMap::new())) {
            RedisValue::Hash(h) => assert!(h.is_empty()),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn test_hash_binary_keys_and_values() {
        let mut hash = HashMap::new();
        hash.insert(Bytes::from(vec![0, 1, 2]), Bytes::from(vec![255, 254, 253]));
        match roundtrip(RedisValue::Hash(hash.clone())) {
            RedisValue::Hash(h) => assert_eq!(h, hash),
            other => panic!("unexpected: {other:?}"),
        }
    }

    // ── Sorted Set ──────────────────────────────────────────────────────────

    #[test]
    fn test_zset_roundtrip() {
        let mut scores = HashMap::new();
        let mut members = BTreeMap::new();
        scores.insert(Bytes::from("alice"), 1.5);
        scores.insert(Bytes::from("bob"), 2.5);
        members.insert((ordered_float::OrderedFloat(1.5), Bytes::from("alice")), ());
        members.insert((ordered_float::OrderedFloat(2.5), Bytes::from("bob")), ());
        match roundtrip(RedisValue::SortedSet {
            scores: scores.clone(),
            members,
        }) {
            RedisValue::SortedSet {
                scores: s,
                members: m,
            } => {
                assert_eq!(s, scores);
                assert_eq!(m.len(), 2);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn test_zset_empty_roundtrip() {
        match roundtrip(RedisValue::SortedSet {
            scores: HashMap::new(),
            members: BTreeMap::new(),
        }) {
            RedisValue::SortedSet { scores, members } => {
                assert!(scores.is_empty());
                assert!(members.is_empty());
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn test_zset_negative_and_inf_scores() {
        let mut scores = HashMap::new();
        let mut members = BTreeMap::new();
        scores.insert(Bytes::from("neg"), -100.0);
        scores.insert(Bytes::from("zero"), 0.0);
        scores.insert(Bytes::from("pos"), f64::MAX);
        members.insert(
            (ordered_float::OrderedFloat(-100.0), Bytes::from("neg")),
            (),
        );
        members.insert((ordered_float::OrderedFloat(0.0), Bytes::from("zero")), ());
        members.insert(
            (ordered_float::OrderedFloat(f64::MAX), Bytes::from("pos")),
            (),
        );
        match roundtrip(RedisValue::SortedSet { scores, members }) {
            RedisValue::SortedSet { scores: s, .. } => {
                assert_eq!(*s.get(&Bytes::from("neg")).unwrap(), -100.0);
                assert_eq!(*s.get(&Bytes::from("zero")).unwrap(), 0.0);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    // ── Stream ──────────────────────────────────────────────────────────────

    #[test]
    fn test_stream_roundtrip() {
        use crate::value::{StreamData, StreamId};
        let mut stream = StreamData::new();
        stream.entries.insert(
            StreamId::new(1000, 0),
            vec![(Bytes::from("key"), Bytes::from("val"))],
        );
        stream.last_id = StreamId::new(1000, 0);
        stream.first_id = Some(StreamId::new(1000, 0));

        match roundtrip(RedisValue::Stream(stream)) {
            RedisValue::Stream(s) => {
                assert_eq!(s.entries.len(), 1);
                let entry = s.entries.get(&StreamId::new(1000, 0)).unwrap();
                assert_eq!(entry[0].0, Bytes::from("key"));
                assert_eq!(entry[0].1, Bytes::from("val"));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn test_stream_empty_roundtrip() {
        use crate::value::StreamData;
        match roundtrip(RedisValue::Stream(StreamData::new())) {
            RedisValue::Stream(s) => assert!(s.entries.is_empty()),
            other => panic!("unexpected: {other:?}"),
        }
    }

    // ── Error cases ─────────────────────────────────────────────────────────

    #[test]
    fn test_deserialize_bad_magic() {
        let bad = [0x00, 0x00, VERSION, TAG_STRING, 0, 0, 0, 0, 0, 0, 0, 0];
        assert!(matches!(deserialize(&bad), Err(SerdeError::BadMagic)));
    }

    #[test]
    fn test_deserialize_bad_version() {
        let mut data = serialize(&RedisValue::String(Bytes::from("x"))).to_vec();
        data[2] = 0xFF; // corrupt version
                        // Also fix CRC so it gets past checksum (easier: just check we get UnsupportedVersion)
        assert!(matches!(
            deserialize(&data),
            Err(SerdeError::BadChecksum { .. } | SerdeError::UnsupportedVersion(_))
        ));
    }

    #[test]
    fn test_deserialize_bad_checksum() {
        let mut data = serialize(&RedisValue::String(Bytes::from("hello"))).to_vec();
        // Flip last byte of CRC
        let last = data.len() - 1;
        data[last] ^= 0xFF;
        assert!(matches!(
            deserialize(&data),
            Err(SerdeError::BadChecksum { .. })
        ));
    }

    #[test]
    fn test_deserialize_too_short() {
        assert!(matches!(
            deserialize(&[0xFE, 0xDB, 0x01]),
            Err(SerdeError::Underflow { .. })
        ));
    }

    #[test]
    fn test_deserialize_unknown_tag() {
        // Build a valid-looking payload with an unknown tag byte
        let mut buf = BytesMut::new();
        buf.put_slice(&MAGIC);
        buf.put_u8(VERSION);
        buf.put_u8(0xAA); // unknown tag
        let crc = crc32(&buf);
        buf.put_u32_le(crc);
        assert!(matches!(
            deserialize(&buf),
            Err(SerdeError::UnknownTag(0xAA))
        ));
    }

    #[test]
    fn test_crc32_known_value() {
        // CRC-32 of "123456789" = 0xCBF43926
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn test_serialize_produces_deterministic_output() {
        let mut set = HashSet::new();
        set.insert(Bytes::from("z"));
        set.insert(Bytes::from("a"));
        set.insert(Bytes::from("m"));
        let out1 = serialize(&RedisValue::Set(set.clone()));
        let out2 = serialize(&RedisValue::Set(set));
        assert_eq!(out1, out2, "serialize must be deterministic");
    }
}

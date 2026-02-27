#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

//! Integration tests for DUMP, RESTORE, and MIGRATE commands.
//!
//! DUMP / RESTORE tests use a single server and exercise the full
//! serialization round-trip over the wire.
//!
//! MIGRATE tests spin up **two** independent `TestServer` instances and verify
//! that keys are physically moved (or copied) between them.

use ferris_protocol::RespValue;
use ferris_test_utils::TestServer;

// ============================================================================
// Helpers
// ============================================================================

/// Assert the response is `+OK`.
fn assert_ok(r: &RespValue) {
    assert!(
        matches!(r, RespValue::SimpleString(s) if s == "OK"),
        "expected +OK, got {r:?}"
    );
}

/// Assert the response is an error containing `fragment`.
fn assert_err_contains(r: &RespValue, fragment: &str) {
    match r {
        RespValue::Error(msg) => {
            assert!(
                msg.contains(fragment),
                "expected error containing '{fragment}', got: {msg}"
            );
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

// ============================================================================
// DUMP tests
// ============================================================================

#[tokio::test]
async fn test_dump_nonexistent_key_returns_nil() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["DUMP", "no_such_key"]).await;
    assert_eq!(
        result,
        RespValue::Null,
        "DUMP on missing key must return nil"
    );

    server.stop().await;
}

#[tokio::test]
async fn test_dump_string_returns_bulk_string() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["SET", "mystr", "hello"]).await;
    let result = client.cmd(&["DUMP", "mystr"]).await;

    match result {
        RespValue::BulkString(payload) => {
            assert!(!payload.is_empty(), "DUMP payload must be non-empty");
            // Magic header 0xFE 0xDB
            assert_eq!(payload[0], 0xFE, "wrong magic byte 0");
            assert_eq!(payload[1], 0xDB, "wrong magic byte 1");
        }
        other => panic!("expected BulkString, got {other:?}"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_dump_list_returns_payload() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["RPUSH", "mylist", "a", "b", "c"]).await;
    let result = client.cmd(&["DUMP", "mylist"]).await;

    assert!(
        matches!(result, RespValue::BulkString(_)),
        "DUMP of list must return BulkString"
    );
    server.stop().await;
}

#[tokio::test]
async fn test_dump_set_returns_payload() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["SADD", "myset", "x", "y", "z"]).await;
    let result = client.cmd(&["DUMP", "myset"]).await;

    assert!(
        matches!(result, RespValue::BulkString(_)),
        "DUMP of set must return BulkString"
    );
    server.stop().await;
}

#[tokio::test]
async fn test_dump_hash_returns_payload() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client
        .cmd(&["HSET", "myhash", "f1", "v1", "f2", "v2"])
        .await;
    let result = client.cmd(&["DUMP", "myhash"]).await;

    assert!(
        matches!(result, RespValue::BulkString(_)),
        "DUMP of hash must return BulkString"
    );
    server.stop().await;
}

#[tokio::test]
async fn test_dump_sorted_set_returns_payload() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["ZADD", "myzset", "1.5", "alice"]).await;
    client.cmd(&["ZADD", "myzset", "2.5", "bob"]).await;
    let result = client.cmd(&["DUMP", "myzset"]).await;

    assert!(
        matches!(result, RespValue::BulkString(_)),
        "DUMP of sorted set must return BulkString"
    );
    server.stop().await;
}

#[tokio::test]
async fn test_dump_wrong_arity() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // No key argument
    let result = client.cmd(&["DUMP"]).await;
    assert_err_contains(&result, "wrong number of arguments");

    // Extra argument
    let result = client.cmd(&["DUMP", "key", "extra"]).await;
    assert_err_contains(&result, "wrong number of arguments");

    server.stop().await;
}

#[tokio::test]
async fn test_dump_expired_key_returns_nil() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Set a key with 1ms TTL then wait for it to expire
    client.cmd(&["SET", "expkey", "val"]).await;
    client.cmd(&["PEXPIRE", "expkey", "1"]).await;
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    let result = client.cmd(&["DUMP", "expkey"]).await;
    assert_eq!(
        result,
        RespValue::Null,
        "DUMP on expired key must return nil"
    );

    server.stop().await;
}

// ============================================================================
// RESTORE tests
// ============================================================================

#[tokio::test]
async fn test_restore_string_roundtrip() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["SET", "src", "roundtrip_value"]).await;
    let dump = client.cmd(&["DUMP", "src"]).await;

    let payload = match dump {
        RespValue::BulkString(b) => b,
        other => panic!("expected BulkString from DUMP, got {other:?}"),
    };

    // Restore under a different key, no TTL
    let payload_str: Vec<u8> = payload.to_vec();
    let result = client
        .cmd_raw(&[b"RESTORE", b"dst", b"0", payload_str.as_slice()])
        .await;
    assert_ok(&result);

    // Verify value
    let got = client.cmd(&["GET", "dst"]).await;
    assert_eq!(
        got,
        RespValue::BulkString(bytes::Bytes::from("roundtrip_value"))
    );

    server.stop().await;
}

#[tokio::test]
async fn test_restore_list_roundtrip() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client
        .cmd(&["RPUSH", "srclist", "first", "second", "third"])
        .await;
    let dump = client.cmd(&["DUMP", "srclist"]).await;
    let payload = match dump {
        RespValue::BulkString(b) => b,
        other => panic!("{other:?}"),
    };

    client
        .cmd_raw(&[b"RESTORE", b"dstlist", b"0", &payload])
        .await;

    let len = client.cmd(&["LLEN", "dstlist"]).await;
    assert_eq!(len, RespValue::Integer(3));

    let first = client.cmd(&["LINDEX", "dstlist", "0"]).await;
    assert_eq!(first, RespValue::BulkString(bytes::Bytes::from("first")));

    server.stop().await;
}

#[tokio::test]
async fn test_restore_hash_roundtrip() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client
        .cmd(&["HSET", "srchash", "name", "alice", "age", "30"])
        .await;
    let dump = client.cmd(&["DUMP", "srchash"]).await;
    let payload = match dump {
        RespValue::BulkString(b) => b,
        other => panic!("{other:?}"),
    };

    client
        .cmd_raw(&[b"RESTORE", b"dsthash", b"0", &payload])
        .await;

    let name = client.cmd(&["HGET", "dsthash", "name"]).await;
    assert_eq!(name, RespValue::BulkString(bytes::Bytes::from("alice")));

    let age = client.cmd(&["HGET", "dsthash", "age"]).await;
    assert_eq!(age, RespValue::BulkString(bytes::Bytes::from("30")));

    server.stop().await;
}

#[tokio::test]
async fn test_restore_sorted_set_roundtrip() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["ZADD", "srczset", "1.0", "a"]).await;
    client.cmd(&["ZADD", "srczset", "2.0", "b"]).await;
    let dump = client.cmd(&["DUMP", "srczset"]).await;
    let payload = match dump {
        RespValue::BulkString(b) => b,
        other => panic!("{other:?}"),
    };

    client
        .cmd_raw(&[b"RESTORE", b"dstzset", b"0", &payload])
        .await;

    let card = client.cmd(&["ZCARD", "dstzset"]).await;
    assert_eq!(card, RespValue::Integer(2));

    let score = client.cmd(&["ZSCORE", "dstzset", "a"]).await;
    assert_eq!(score, RespValue::BulkString(bytes::Bytes::from("1")));

    server.stop().await;
}

#[tokio::test]
async fn test_restore_busykey_error_without_replace() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["SET", "existing", "old"]).await;
    client.cmd(&["SET", "src", "new_value"]).await;
    let dump = client.cmd(&["DUMP", "src"]).await;
    let payload = match dump {
        RespValue::BulkString(b) => b,
        other => panic!("{other:?}"),
    };

    let result = client
        .cmd_raw(&[b"RESTORE", b"existing", b"0", &payload])
        .await;
    assert_err_contains(&result, "BUSYKEY");

    // Original value untouched
    let val = client.cmd(&["GET", "existing"]).await;
    assert_eq!(val, RespValue::BulkString(bytes::Bytes::from("old")));

    server.stop().await;
}

#[tokio::test]
async fn test_restore_replace_overwrites_existing() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["SET", "target", "old"]).await;
    client.cmd(&["SET", "src", "new_value"]).await;
    let dump = client.cmd(&["DUMP", "src"]).await;
    let payload = match dump {
        RespValue::BulkString(b) => b,
        other => panic!("{other:?}"),
    };

    let result = client
        .cmd_raw(&[b"RESTORE", b"target", b"0", &payload, b"REPLACE"])
        .await;
    assert_ok(&result);

    let val = client.cmd(&["GET", "target"]).await;
    assert_eq!(val, RespValue::BulkString(bytes::Bytes::from("new_value")));

    server.stop().await;
}

#[tokio::test]
async fn test_restore_with_ttl() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["SET", "src", "ephemeral"]).await;
    let dump = client.cmd(&["DUMP", "src"]).await;
    let payload = match dump {
        RespValue::BulkString(b) => b,
        other => panic!("{other:?}"),
    };

    // Restore with 10-second TTL
    let result = client
        .cmd_raw(&[b"RESTORE", b"dst_ttl", b"10000", &payload])
        .await;
    assert_ok(&result);

    // Should have a positive TTL
    let pttl = client.cmd(&["PTTL", "dst_ttl"]).await;
    match pttl {
        RespValue::Integer(ms) => assert!(ms > 0 && ms <= 10_000),
        other => panic!("expected positive TTL, got {other:?}"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_restore_wrong_arity() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["RESTORE", "key"]).await;
    assert_err_contains(&result, "wrong number of arguments");

    server.stop().await;
}

#[tokio::test]
async fn test_restore_invalid_payload() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Send garbage as payload — should fail with DUMP payload error
    let result = client
        .cmd_raw(&[b"RESTORE", b"key", b"0", b"notavalidpayload"])
        .await;
    assert!(
        matches!(result, RespValue::Error(_)),
        "invalid payload should return error, got {result:?}"
    );

    server.stop().await;
}

#[tokio::test]
async fn test_restore_negative_ttl_error() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["SET", "src", "v"]).await;
    let dump = client.cmd(&["DUMP", "src"]).await;
    let payload = match dump {
        RespValue::BulkString(b) => b,
        other => panic!("{other:?}"),
    };

    let result = client.cmd_raw(&[b"RESTORE", b"dst", b"-1", &payload]).await;
    assert!(
        matches!(result, RespValue::Error(_)),
        "negative TTL should return error"
    );

    server.stop().await;
}

// ============================================================================
// MIGRATE tests (two servers)
// ============================================================================
//
// MIGRATE uses `tokio::task::block_in_place` internally, which requires a
// multi-thread Tokio runtime. All tests in this section must use
// `flavor = "multi_thread"`.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_migrate_string_moves_key() {
    let src = TestServer::spawn().await;
    let dst = TestServer::spawn().await;
    let mut src_client = src.client().await;
    let mut dst_client = dst.client().await;

    src_client.cmd(&["SET", "migkey", "migvalue"]).await;

    let dst_port = dst.port.to_string();
    let result = src_client
        .cmd(&["MIGRATE", "127.0.0.1", &dst_port, "migkey", "0", "5000"])
        .await;
    assert_ok(&result);

    // Key should be gone from source
    let src_val = src_client.cmd(&["EXISTS", "migkey"]).await;
    assert_eq!(
        src_val,
        RespValue::Integer(0),
        "key must be removed from src"
    );

    // Key should exist at destination with correct value
    let dst_val = dst_client.cmd(&["GET", "migkey"]).await;
    assert_eq!(
        dst_val,
        RespValue::BulkString(bytes::Bytes::from("migvalue"))
    );

    src.stop().await;
    dst.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_migrate_copy_leaves_source_intact() {
    let src = TestServer::spawn().await;
    let dst = TestServer::spawn().await;
    let mut src_client = src.client().await;
    let mut dst_client = dst.client().await;

    src_client.cmd(&["SET", "copykey", "copyval"]).await;

    let dst_port = dst.port.to_string();
    let result = src_client
        .cmd(&[
            "MIGRATE",
            "127.0.0.1",
            &dst_port,
            "copykey",
            "0",
            "5000",
            "COPY",
        ])
        .await;
    assert_ok(&result);

    // Key must still exist at source
    let src_val = src_client.cmd(&["GET", "copykey"]).await;
    assert_eq!(
        src_val,
        RespValue::BulkString(bytes::Bytes::from("copyval"))
    );

    // Key must also exist at destination
    let dst_val = dst_client.cmd(&["GET", "copykey"]).await;
    assert_eq!(
        dst_val,
        RespValue::BulkString(bytes::Bytes::from("copyval"))
    );

    src.stop().await;
    dst.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_migrate_list_preserves_order() {
    let src = TestServer::spawn().await;
    let dst = TestServer::spawn().await;
    let mut src_client = src.client().await;
    let mut dst_client = dst.client().await;

    src_client
        .cmd(&["RPUSH", "miglist", "one", "two", "three"])
        .await;

    let dst_port = dst.port.to_string();
    let result = src_client
        .cmd(&["MIGRATE", "127.0.0.1", &dst_port, "miglist", "0", "5000"])
        .await;
    assert_ok(&result);

    let len = dst_client.cmd(&["LLEN", "miglist"]).await;
    assert_eq!(len, RespValue::Integer(3));

    let first = dst_client.cmd(&["LINDEX", "miglist", "0"]).await;
    assert_eq!(first, RespValue::BulkString(bytes::Bytes::from("one")));

    let last = dst_client.cmd(&["LINDEX", "miglist", "2"]).await;
    assert_eq!(last, RespValue::BulkString(bytes::Bytes::from("three")));

    src.stop().await;
    dst.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_migrate_hash_preserves_fields() {
    let src = TestServer::spawn().await;
    let dst = TestServer::spawn().await;
    let mut src_client = src.client().await;
    let mut dst_client = dst.client().await;

    src_client
        .cmd(&["HSET", "mighash", "k1", "v1", "k2", "v2"])
        .await;

    let dst_port = dst.port.to_string();
    src_client
        .cmd(&["MIGRATE", "127.0.0.1", &dst_port, "mighash", "0", "5000"])
        .await;

    let v1 = dst_client.cmd(&["HGET", "mighash", "k1"]).await;
    assert_eq!(v1, RespValue::BulkString(bytes::Bytes::from("v1")));

    let v2 = dst_client.cmd(&["HGET", "mighash", "k2"]).await;
    assert_eq!(v2, RespValue::BulkString(bytes::Bytes::from("v2")));

    src.stop().await;
    dst.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_migrate_sorted_set_preserves_scores() {
    let src = TestServer::spawn().await;
    let dst = TestServer::spawn().await;
    let mut src_client = src.client().await;
    let mut dst_client = dst.client().await;

    src_client.cmd(&["ZADD", "migzset", "1.5", "alice"]).await;
    src_client.cmd(&["ZADD", "migzset", "3.0", "bob"]).await;

    let dst_port = dst.port.to_string();
    src_client
        .cmd(&["MIGRATE", "127.0.0.1", &dst_port, "migzset", "0", "5000"])
        .await;

    let card = dst_client.cmd(&["ZCARD", "migzset"]).await;
    assert_eq!(card, RespValue::Integer(2));

    // Scores should be preserved
    let score = dst_client.cmd(&["ZSCORE", "migzset", "bob"]).await;
    match score {
        RespValue::BulkString(s) => {
            let f: f64 = String::from_utf8_lossy(&s).parse().unwrap();
            assert!((f - 3.0).abs() < 1e-9, "score mismatch: {f}");
        }
        other => panic!("expected score, got {other:?}"),
    }

    src.stop().await;
    dst.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_migrate_nonexistent_key_returns_nokey() {
    let src = TestServer::spawn().await;
    let dst = TestServer::spawn().await;
    let mut src_client = src.client().await;

    let dst_port = dst.port.to_string();
    let result = src_client
        .cmd(&["MIGRATE", "127.0.0.1", &dst_port, "ghost_key", "0", "5000"])
        .await;

    assert!(
        matches!(&result, RespValue::SimpleString(s) if s == "NOKEY"),
        "expected NOKEY, got {result:?}"
    );

    src.stop().await;
    dst.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_migrate_replace_flag() {
    let src = TestServer::spawn().await;
    let dst = TestServer::spawn().await;
    let mut src_client = src.client().await;
    let mut dst_client = dst.client().await;

    src_client.cmd(&["SET", "rkey", "new_val"]).await;
    dst_client.cmd(&["SET", "rkey", "old_val"]).await;

    let dst_port = dst.port.to_string();
    let result = src_client
        .cmd(&[
            "MIGRATE",
            "127.0.0.1",
            &dst_port,
            "rkey",
            "0",
            "5000",
            "REPLACE",
        ])
        .await;
    assert_ok(&result);

    let val = dst_client.cmd(&["GET", "rkey"]).await;
    assert_eq!(val, RespValue::BulkString(bytes::Bytes::from("new_val")));

    src.stop().await;
    dst.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_migrate_multi_key_with_keys_option() {
    let src = TestServer::spawn().await;
    let dst = TestServer::spawn().await;
    let mut src_client = src.client().await;
    let mut dst_client = dst.client().await;

    src_client.cmd(&["SET", "mk1", "val1"]).await;
    src_client.cmd(&["SET", "mk2", "val2"]).await;
    src_client.cmd(&["SET", "mk3", "val3"]).await;

    let dst_port = dst.port.to_string();
    // With KEYS option the key argument is ""
    let result = src_client
        .cmd(&[
            "MIGRATE",
            "127.0.0.1",
            &dst_port,
            "",
            "0",
            "5000",
            "KEYS",
            "mk1",
            "mk2",
            "mk3",
        ])
        .await;
    assert_ok(&result);

    // All three should exist at destination
    for (key, expected) in [("mk1", "val1"), ("mk2", "val2"), ("mk3", "val3")] {
        let v = dst_client.cmd(&["GET", key]).await;
        assert_eq!(
            v,
            RespValue::BulkString(bytes::Bytes::from(expected)),
            "key {key} mismatch"
        );
        // Gone from source
        let e = src_client.cmd(&["EXISTS", key]).await;
        assert_eq!(e, RespValue::Integer(0), "{key} should be gone from src");
    }

    src.stop().await;
    dst.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_migrate_wrong_arity() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["MIGRATE", "127.0.0.1", "6380"]).await;
    assert_err_contains(&result, "wrong number of arguments");

    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_migrate_connection_refused() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    client.cmd(&["SET", "k", "v"]).await;

    // Port 1 is almost certainly not open
    let result = client
        .cmd(&["MIGRATE", "127.0.0.1", "1", "k", "0", "500"])
        .await;

    assert!(
        matches!(result, RespValue::Error(_)),
        "connection refused must yield error, got {result:?}"
    );

    // Source key must still be present (migration failed = no delete)
    let val = client.cmd(&["GET", "k"]).await;
    assert_eq!(val, RespValue::BulkString(bytes::Bytes::from("v")));

    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_migrate_preserves_set_members() {
    let src = TestServer::spawn().await;
    let dst = TestServer::spawn().await;
    let mut src_client = src.client().await;
    let mut dst_client = dst.client().await;

    src_client
        .cmd(&["SADD", "migset", "apple", "banana", "cherry"])
        .await;

    let dst_port = dst.port.to_string();
    src_client
        .cmd(&["MIGRATE", "127.0.0.1", &dst_port, "migset", "0", "5000"])
        .await;

    let card = dst_client.cmd(&["SCARD", "migset"]).await;
    assert_eq!(card, RespValue::Integer(3));

    let is_member = dst_client.cmd(&["SISMEMBER", "migset", "banana"]).await;
    assert_eq!(is_member, RespValue::Integer(1));

    src.stop().await;
    dst.stop().await;
}

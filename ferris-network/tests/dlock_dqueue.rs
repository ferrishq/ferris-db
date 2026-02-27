//! Integration tests for DLOCK and DQUEUE commands.
//!
//! Tests run against a real `TestServer` over the wire using the RESP protocol.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use ferris_protocol::RespValue;
use ferris_test_utils::TestServer;

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

#[allow(dead_code)]
fn assert_ok(v: &RespValue) {
    assert!(
        matches!(v, RespValue::SimpleString(s) if s == "OK"),
        "expected OK, got {v:?}"
    );
}

fn assert_integer(v: &RespValue, expected: i64) {
    assert_eq!(v, &RespValue::Integer(expected), "integer mismatch");
}

// ─────────────────────────────────────────────────────────────────────────────
// DLOCK integration tests
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_dlock_acquire_and_status() {
    let server = TestServer::spawn().await;
    let mut c = server.client().await;

    let token_resp = c
        .cmd(&["DLOCK", "ACQUIRE", "mylock", "worker-1", "5000"])
        .await;
    let token = match token_resp {
        RespValue::Integer(n) => {
            assert!(n > 0, "token must be positive");
            n
        }
        other => panic!("expected integer token, got {other:?}"),
    };

    // STATUS should show the lock
    let status = c.cmd(&["DLOCK", "STATUS", "mylock"]).await;
    match status {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 6);
            assert_eq!(arr[0], RespValue::bulk_string("holder"));
            assert_eq!(arr[1], RespValue::bulk_string("worker-1"));
            assert_eq!(arr[2], RespValue::bulk_string("token"));
            assert_eq!(arr[3], RespValue::bulk_string(token.to_string()));
            assert_eq!(arr[4], RespValue::bulk_string("ttl_ms"));
        }
        other => panic!("expected status array, got {other:?}"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_dlock_second_acquire_blocked() {
    let server = TestServer::spawn().await;
    let mut c = server.client().await;

    c.cmd(&["DLOCK", "ACQUIRE", "lock1", "w1", "5000"]).await;
    let r = c.cmd(&["DLOCK", "ACQUIRE", "lock1", "w2", "5000"]).await;
    assert_integer(&r, -1);

    server.stop().await;
}

#[tokio::test]
async fn test_dlock_release_and_reacquire() {
    let server = TestServer::spawn().await;
    let mut c = server.client().await;

    let tok = match c.cmd(&["DLOCK", "ACQUIRE", "lock1", "w1", "5000"]).await {
        RespValue::Integer(n) => n,
        other => panic!("{other:?}"),
    };

    let r = c
        .cmd(&["DLOCK", "RELEASE", "lock1", "w1", &tok.to_string()])
        .await;
    assert_integer(&r, 1);

    // Another caller can now acquire
    let r2 = c.cmd(&["DLOCK", "ACQUIRE", "lock1", "w2", "5000"]).await;
    assert!(matches!(r2, RespValue::Integer(n) if n > 0));

    server.stop().await;
}

#[tokio::test]
async fn test_dlock_release_wrong_token_fails() {
    let server = TestServer::spawn().await;
    let mut c = server.client().await;

    c.cmd(&["DLOCK", "ACQUIRE", "lock1", "w1", "5000"]).await;
    let r = c.cmd(&["DLOCK", "RELEASE", "lock1", "w1", "9999"]).await;
    assert_integer(&r, 0);

    server.stop().await;
}

#[tokio::test]
async fn test_dlock_extend_refreshes_ttl() {
    let server = TestServer::spawn().await;
    let mut c = server.client().await;

    let tok = match c.cmd(&["DLOCK", "ACQUIRE", "lock1", "w1", "5000"]).await {
        RespValue::Integer(n) => n,
        other => panic!("{other:?}"),
    };

    let r = c
        .cmd(&["DLOCK", "EXTEND", "lock1", "w1", &tok.to_string(), "60000"])
        .await;
    assert_integer(&r, 1);

    // Status must show remaining ttl near 60000 ms
    let status = c.cmd(&["DLOCK", "STATUS", "lock1"]).await;
    if let RespValue::Array(arr) = status {
        let ttl: u64 = match &arr[5] {
            RespValue::BulkString(b) => String::from_utf8_lossy(b).parse().unwrap(),
            other => panic!("bad ttl: {other:?}"),
        };
        assert!(
            ttl > 50_000,
            "extended TTL should be close to 60s, got {ttl}"
        );
    }

    server.stop().await;
}

#[tokio::test]
async fn test_dlock_forcerelease() {
    let server = TestServer::spawn().await;
    let mut c = server.client().await;

    c.cmd(&["DLOCK", "ACQUIRE", "lock1", "w1", "5000"]).await;
    let r = c.cmd(&["DLOCK", "FORCERELEASE", "lock1"]).await;
    assert_integer(&r, 1);

    // Status should now be Null
    let status = c.cmd(&["DLOCK", "STATUS", "lock1"]).await;
    assert_eq!(status, RespValue::Null);

    server.stop().await;
}

#[tokio::test]
async fn test_dlock_status_absent_lock() {
    let server = TestServer::spawn().await;
    let mut c = server.client().await;

    let r = c.cmd(&["DLOCK", "STATUS", "nobody"]).await;
    assert_eq!(r, RespValue::Null);

    server.stop().await;
}

#[tokio::test]
async fn test_dlock_tokens_monotonically_increase() {
    let server = TestServer::spawn().await;
    let mut c = server.client().await;

    let tok1 = match c.cmd(&["DLOCK", "ACQUIRE", "lock-a", "w1", "5000"]).await {
        RespValue::Integer(n) => n,
        other => panic!("{other:?}"),
    };
    c.cmd(&["DLOCK", "RELEASE", "lock-a", "w1", &tok1.to_string()])
        .await;

    let tok2 = match c.cmd(&["DLOCK", "ACQUIRE", "lock-a", "w2", "5000"]).await {
        RespValue::Integer(n) => n,
        other => panic!("{other:?}"),
    };
    assert!(tok2 > tok1, "tokens must be strictly increasing");

    server.stop().await;
}

#[tokio::test]
async fn test_dlock_wrong_arity() {
    let server = TestServer::spawn().await;
    let mut c = server.client().await;

    let r = c.cmd(&["DLOCK", "ACQUIRE", "mylock"]).await;
    assert!(matches!(r, RespValue::Error(_)));

    server.stop().await;
}

#[tokio::test]
async fn test_dlock_concurrent_acquire_only_one_wins() {
    let server = TestServer::spawn().await;
    let addr = server.addr;

    // Spawn 5 concurrent tasks all trying to acquire the same lock
    let mut handles = vec![];
    for i in 0..5usize {
        let handle = tokio::spawn(async move {
            let mut c = ferris_test_utils::TestClient::connect(addr).await;
            let holder = format!("worker-{i}");
            match c
                .cmd(&["DLOCK", "ACQUIRE", "contested", &holder, "5000"])
                .await
            {
                RespValue::Integer(n) => n, // -1 = lost, >0 = won
                other => panic!("unexpected: {other:?}"),
            }
        });
        handles.push(handle);
    }

    let mut results = Vec::new();
    for h in handles {
        results.push(h.await.unwrap());
    }

    let winner_count = results.iter().filter(|&&n| n > 0).count();
    assert_eq!(winner_count, 1, "exactly one worker must win the lock");

    server.stop().await;
}

// ─────────────────────────────────────────────────────────────────────────────
// DQUEUE integration tests
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_dqueue_push_pop_basic() {
    let server = TestServer::spawn().await;
    let mut c = server.client().await;

    let id_resp = c.cmd(&["DQUEUE", "PUSH", "q1", "hello"]).await;
    let _id = match id_resp {
        RespValue::BulkString(b) => String::from_utf8_lossy(&b).parse::<u64>().unwrap(),
        other => panic!("expected msg id, got {other:?}"),
    };

    let pop = c.cmd(&["DQUEUE", "POP", "q1"]).await;
    match pop {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[1], RespValue::bulk_string("hello"));
        }
        other => panic!("expected array, got {other:?}"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_dqueue_pop_empty_returns_null() {
    let server = TestServer::spawn().await;
    let mut c = server.client().await;

    let r = c.cmd(&["DQUEUE", "POP", "empty-q"]).await;
    assert_eq!(r, RespValue::Null);

    server.stop().await;
}

#[tokio::test]
async fn test_dqueue_fifo_ordering() {
    let server = TestServer::spawn().await;
    let mut c = server.client().await;

    c.cmd(&["DQUEUE", "PUSH", "q1", "first"]).await;
    c.cmd(&["DQUEUE", "PUSH", "q1", "second"]).await;
    c.cmd(&["DQUEUE", "PUSH", "q1", "third"]).await;

    for expected in &["first", "second", "third"] {
        let pop = c.cmd(&["DQUEUE", "POP", "q1"]).await;
        if let RespValue::Array(arr) = pop {
            assert_eq!(arr[1], RespValue::bulk_string(*expected), "order mismatch");
            // ACK each message
            if let RespValue::BulkString(id) = &arr[0] {
                c.cmd(&["DQUEUE", "ACK", "q1", &String::from_utf8_lossy(id)])
                    .await;
            }
        } else {
            panic!("POP returned {pop:?}");
        }
    }

    server.stop().await;
}

#[tokio::test]
async fn test_dqueue_ack_removes_from_inflight() {
    let server = TestServer::spawn().await;
    let mut c = server.client().await;

    c.cmd(&["DQUEUE", "PUSH", "q1", "msg"]).await;
    let pop = c.cmd(&["DQUEUE", "POP", "q1"]).await;
    let id = if let RespValue::Array(arr) = &pop {
        match &arr[0] {
            RespValue::BulkString(b) => String::from_utf8_lossy(b).to_string(),
            _ => panic!(),
        }
    } else {
        panic!("bad pop: {pop:?}");
    };

    // Inflight count = 1
    assert_integer(&c.cmd(&["DQUEUE", "INFLIGHT", "q1"]).await, 1);

    let ack = c.cmd(&["DQUEUE", "ACK", "q1", &id]).await;
    assert_integer(&ack, 1);

    // Inflight count = 0
    assert_integer(&c.cmd(&["DQUEUE", "INFLIGHT", "q1"]).await, 0);

    server.stop().await;
}

#[tokio::test]
async fn test_dqueue_nack_requeues() {
    let server = TestServer::spawn().await;
    let mut c = server.client().await;

    c.cmd(&["DQUEUE", "PUSH", "q1", "msg"]).await;
    let pop = c.cmd(&["DQUEUE", "POP", "q1"]).await;
    let id = if let RespValue::Array(arr) = &pop {
        match &arr[0] {
            RespValue::BulkString(b) => String::from_utf8_lossy(b).to_string(),
            _ => panic!(),
        }
    } else {
        panic!("bad pop: {pop:?}");
    };

    let nack = c.cmd(&["DQUEUE", "NACK", "q1", &id]).await;
    assert_integer(&nack, 1);

    // Message back in pending
    assert_integer(&c.cmd(&["DQUEUE", "LEN", "q1"]).await, 1);
    assert_integer(&c.cmd(&["DQUEUE", "INFLIGHT", "q1"]).await, 0);

    // Can pop again
    let repop = c.cmd(&["DQUEUE", "POP", "q1"]).await;
    assert!(matches!(repop, RespValue::Array(_)));

    server.stop().await;
}

#[tokio::test]
async fn test_dqueue_len_and_inflight() {
    let server = TestServer::spawn().await;
    let mut c = server.client().await;

    c.cmd(&["DQUEUE", "PUSH", "q1", "a"]).await;
    c.cmd(&["DQUEUE", "PUSH", "q1", "b"]).await;
    c.cmd(&["DQUEUE", "PUSH", "q1", "c"]).await;

    assert_integer(&c.cmd(&["DQUEUE", "LEN", "q1"]).await, 3);
    assert_integer(&c.cmd(&["DQUEUE", "INFLIGHT", "q1"]).await, 0);

    c.cmd(&["DQUEUE", "POP", "q1"]).await;

    assert_integer(&c.cmd(&["DQUEUE", "LEN", "q1"]).await, 2);
    assert_integer(&c.cmd(&["DQUEUE", "INFLIGHT", "q1"]).await, 1);

    server.stop().await;
}

#[tokio::test]
async fn test_dqueue_purge() {
    let server = TestServer::spawn().await;
    let mut c = server.client().await;

    c.cmd(&["DQUEUE", "PUSH", "q1", "x"]).await;
    c.cmd(&["DQUEUE", "PUSH", "q1", "y"]).await;
    c.cmd(&["DQUEUE", "POP", "q1"]).await; // 1 inflight

    let r = c.cmd(&["DQUEUE", "PURGE", "q1"]).await;
    // 1 pending + 1 inflight = 2
    assert_integer(&r, 2);

    assert_integer(&c.cmd(&["DQUEUE", "LEN", "q1"]).await, 0);
    assert_integer(&c.cmd(&["DQUEUE", "INFLIGHT", "q1"]).await, 0);

    server.stop().await;
}

#[tokio::test]
async fn test_dqueue_peek_does_not_consume() {
    let server = TestServer::spawn().await;
    let mut c = server.client().await;

    c.cmd(&["DQUEUE", "PUSH", "q1", "peekable"]).await;
    let peek = c.cmd(&["DQUEUE", "PEEK", "q1"]).await;
    match peek {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[1], RespValue::bulk_string("peekable"));
        }
        other => panic!("expected array, got {other:?}"),
    }

    // Still in pending — not consumed
    assert_integer(&c.cmd(&["DQUEUE", "LEN", "q1"]).await, 1);
    assert_integer(&c.cmd(&["DQUEUE", "INFLIGHT", "q1"]).await, 0);

    server.stop().await;
}

#[tokio::test]
async fn test_dqueue_pop_count_multiple() {
    let server = TestServer::spawn().await;
    let mut c = server.client().await;

    c.cmd(&["DQUEUE", "PUSH", "q1", "a"]).await;
    c.cmd(&["DQUEUE", "PUSH", "q1", "b"]).await;
    c.cmd(&["DQUEUE", "PUSH", "q1", "c"]).await;

    let pop = c.cmd(&["DQUEUE", "POP", "q1", "COUNT", "2"]).await;
    match pop {
        RespValue::Array(arr) => assert_eq!(arr.len(), 4), // 2 × (id + payload)
        other => panic!("expected array, got {other:?}"),
    }

    assert_integer(&c.cmd(&["DQUEUE", "LEN", "q1"]).await, 1);

    server.stop().await;
}

#[tokio::test]
async fn test_dqueue_wrong_arity() {
    let server = TestServer::spawn().await;
    let mut c = server.client().await;

    let r = c.cmd(&["DQUEUE", "PUSH", "q1"]).await;
    assert!(matches!(r, RespValue::Error(_)));

    let r = c.cmd(&["DQUEUE"]).await;
    assert!(matches!(r, RespValue::Error(_)));

    server.stop().await;
}

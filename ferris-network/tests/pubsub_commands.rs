#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_wrap)]

//! Integration tests for Pub/Sub commands: PUBLISH, SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, PUBSUB

use ferris_protocol::RespValue;
use ferris_test_utils::TestServer;
use tokio::time::Duration;

#[tokio::test]
async fn test_publish_to_no_subscribers() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Publish to a channel with no subscribers
    let result = client.cmd(&["PUBLISH", "channel1", "hello"]).await;
    assert_eq!(result, RespValue::Integer(0)); // 0 subscribers received it

    server.stop().await;
}

#[tokio::test]
async fn test_subscribe_basic() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Subscribe to a channel
    let result = client.cmd(&["SUBSCRIBE", "channel1"]).await;
    
    // Should return subscription confirmation
    match result {
        RespValue::Array(confirmations) => {
            assert_eq!(confirmations.len(), 1);
            match &confirmations[0] {
                RespValue::Array(parts) => {
                    assert_eq!(parts.len(), 3);
                    assert_eq!(parts[0], RespValue::bulk_string("subscribe"));
                    assert_eq!(parts[1], RespValue::bulk_string("channel1"));
                    assert_eq!(parts[2], RespValue::Integer(1)); // 1 channel subscribed
                }
                _ => panic!("Expected array"),
            }
        }
        _ => panic!("Expected array of confirmations"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_subscribe_multiple_channels() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Subscribe to multiple channels
    let result = client
        .cmd(&["SUBSCRIBE", "channel1", "channel2", "channel3"])
        .await;

    match result {
        RespValue::Array(confirmations) => {
            assert_eq!(confirmations.len(), 3);
            
            // Verify each subscription confirmation
            for (i, confirmation) in confirmations.iter().enumerate() {
                match confirmation {
                    RespValue::Array(parts) => {
                        assert_eq!(parts.len(), 3);
                        assert_eq!(parts[0], RespValue::bulk_string("subscribe"));
                        assert_eq!(parts[2], RespValue::Integer((i + 1) as i64)); // Count increases
                    }
                    _ => panic!("Expected array"),
                }
            }
        }
        _ => panic!("Expected array of confirmations"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_unsubscribe_basic() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Subscribe first
    client.cmd(&["SUBSCRIBE", "channel1"]).await;

    // Unsubscribe
    let result = client.cmd(&["UNSUBSCRIBE", "channel1"]).await;

    match result {
        RespValue::Array(confirmations) => {
            assert_eq!(confirmations.len(), 1);
            match &confirmations[0] {
                RespValue::Array(parts) => {
                    assert_eq!(parts.len(), 3);
                    assert_eq!(parts[0], RespValue::bulk_string("unsubscribe"));
                    assert_eq!(parts[1], RespValue::bulk_string("channel1"));
                    assert_eq!(parts[2], RespValue::Integer(0)); // 0 channels remaining
                }
                _ => panic!("Expected array"),
            }
        }
        _ => panic!("Expected array of confirmations"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_unsubscribe_all() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Subscribe to multiple channels
    client
        .cmd(&["SUBSCRIBE", "channel1", "channel2", "channel3"])
        .await;

    // Unsubscribe from all (no arguments)
    let result = client.cmd(&["UNSUBSCRIBE"]).await;

    match result {
        RespValue::Array(confirmations) => {
            assert_eq!(confirmations.len(), 1);
            match &confirmations[0] {
                RespValue::Array(parts) => {
                    assert_eq!(parts.len(), 3);
                    assert_eq!(parts[0], RespValue::bulk_string("unsubscribe"));
                    assert_eq!(parts[2], RespValue::Integer(0)); // 0 channels remaining
                }
                _ => panic!("Expected array"),
            }
        }
        _ => panic!("Expected array of confirmations"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_publish_subscribe_message_delivery() {
    let server = TestServer::spawn().await;
    let mut subscriber = server.client().await;
    let mut publisher = server.client().await;

    // Subscriber: Subscribe to channel
    subscriber.cmd(&["SUBSCRIBE", "news"]).await;

    // Give subscription time to register
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Publisher: Publish message
    let result = publisher.cmd(&["PUBLISH", "news", "Hello World"]).await;
    assert_eq!(result, RespValue::Integer(1)); // 1 subscriber received it

    // Subscriber: Should receive the message
    // Note: In a real implementation, this would need async message receiving
    // For now, we verify the publish count

    server.stop().await;
}

#[tokio::test]
async fn test_publish_multiple_subscribers() {
    let server = TestServer::spawn().await;
    let mut sub1 = server.client().await;
    let mut sub2 = server.client().await;
    let mut sub3 = server.client().await;
    let mut publisher = server.client().await;

    // All subscribe to the same channel
    sub1.cmd(&["SUBSCRIBE", "broadcast"]).await;
    sub2.cmd(&["SUBSCRIBE", "broadcast"]).await;
    sub3.cmd(&["SUBSCRIBE", "broadcast"]).await;

    tokio::time::sleep(Duration::from_millis(10)).await;

    // Publish should reach all 3 subscribers
    let result = publisher
        .cmd(&["PUBLISH", "broadcast", "Important announcement"])
        .await;
    assert_eq!(result, RespValue::Integer(3)); // 3 subscribers received it

    server.stop().await;
}

#[tokio::test]
async fn test_psubscribe_basic() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Subscribe to pattern
    let result = client.cmd(&["PSUBSCRIBE", "news.*"]).await;

    match result {
        RespValue::Array(confirmations) => {
            assert_eq!(confirmations.len(), 1);
            match &confirmations[0] {
                RespValue::Array(parts) => {
                    assert_eq!(parts.len(), 3);
                    assert_eq!(parts[0], RespValue::bulk_string("psubscribe"));
                    assert_eq!(parts[1], RespValue::bulk_string("news.*"));
                    assert_eq!(parts[2], RespValue::Integer(1)); // 1 pattern subscribed
                }
                _ => panic!("Expected array"),
            }
        }
        _ => panic!("Expected array of confirmations"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_psubscribe_pattern_matching() {
    let server = TestServer::spawn().await;
    let mut subscriber = server.client().await;
    let mut publisher = server.client().await;

    // Subscribe to pattern
    subscriber.cmd(&["PSUBSCRIBE", "news.*"]).await;

    tokio::time::sleep(Duration::from_millis(10)).await;

    // Publish to matching channels
    let result1 = publisher.cmd(&["PUBLISH", "news.sports", "Goal!"]).await;
    let result2 = publisher
        .cmd(&["PUBLISH", "news.weather", "Sunny"])
        .await;
    let result3 = publisher.cmd(&["PUBLISH", "other.topic", "Ignored"]).await;

    assert_eq!(result1, RespValue::Integer(1)); // Matched pattern
    assert_eq!(result2, RespValue::Integer(1)); // Matched pattern
    assert_eq!(result3, RespValue::Integer(0)); // Did not match pattern

    server.stop().await;
}

#[tokio::test]
async fn test_punsubscribe_basic() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Subscribe to pattern first
    client.cmd(&["PSUBSCRIBE", "news.*"]).await;

    // Unsubscribe from pattern
    let result = client.cmd(&["PUNSUBSCRIBE", "news.*"]).await;

    match result {
        RespValue::Array(confirmations) => {
            assert_eq!(confirmations.len(), 1);
            match &confirmations[0] {
                RespValue::Array(parts) => {
                    assert_eq!(parts.len(), 3);
                    assert_eq!(parts[0], RespValue::bulk_string("punsubscribe"));
                    assert_eq!(parts[1], RespValue::bulk_string("news.*"));
                    assert_eq!(parts[2], RespValue::Integer(0)); // 0 patterns remaining
                }
                _ => panic!("Expected array"),
            }
        }
        _ => panic!("Expected array of confirmations"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_pubsub_channels() {
    let server = TestServer::spawn().await;
    let mut client1 = server.client().await;
    let mut client2 = server.client().await;
    let mut client3 = server.client().await;

    // Subscribe to various channels
    client1.cmd(&["SUBSCRIBE", "channel1", "channel2"]).await;
    client2.cmd(&["SUBSCRIBE", "channel2", "channel3"]).await;

    tokio::time::sleep(Duration::from_millis(10)).await;

    // Query active channels
    let result = client3.cmd(&["PUBSUB", "CHANNELS"]).await;

    match result {
        RespValue::Array(channels) => {
            // Should have 3 active channels
            assert_eq!(channels.len(), 3);
        }
        _ => panic!("Expected array of channels"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_pubsub_channels_pattern() {
    let server = TestServer::spawn().await;
    let mut sub1 = server.client().await;
    let mut sub2 = server.client().await;
    let mut query = server.client().await;

    // Subscribe to various channels
    sub1.cmd(&["SUBSCRIBE", "news.sports", "news.weather"]).await;
    sub2.cmd(&["SUBSCRIBE", "other.topic"]).await;

    tokio::time::sleep(Duration::from_millis(10)).await;

    // Query channels matching pattern
    let result = query.cmd(&["PUBSUB", "CHANNELS", "news.*"]).await;

    match result {
        RespValue::Array(channels) => {
            // Should have 2 channels matching "news.*"
            assert_eq!(channels.len(), 2);
        }
        _ => panic!("Expected array of channels"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_pubsub_numsub() {
    let server = TestServer::spawn().await;
    let mut sub1 = server.client().await;
    let mut sub2 = server.client().await;
    let mut sub3 = server.client().await;
    let mut query = server.client().await;

    // Subscribe to channels
    sub1.cmd(&["SUBSCRIBE", "channel1"]).await;
    sub2.cmd(&["SUBSCRIBE", "channel1"]).await;
    sub3.cmd(&["SUBSCRIBE", "channel2"]).await;

    tokio::time::sleep(Duration::from_millis(10)).await;

    // Query subscriber counts
    let result = query.cmd(&["PUBSUB", "NUMSUB", "channel1", "channel2"]).await;

    match result {
        RespValue::Array(pairs) => {
            // Should return [channel1, count1, channel2, count2]
            assert_eq!(pairs.len(), 4);
            assert_eq!(pairs[0], RespValue::bulk_string("channel1"));
            assert_eq!(pairs[1], RespValue::Integer(2)); // 2 subscribers
            assert_eq!(pairs[2], RespValue::bulk_string("channel2"));
            assert_eq!(pairs[3], RespValue::Integer(1)); // 1 subscriber
        }
        _ => panic!("Expected array of channel/count pairs"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_pubsub_numpat() {
    let server = TestServer::spawn().await;
    let mut sub1 = server.client().await;
    let mut sub2 = server.client().await;
    let mut query = server.client().await;

    // Subscribe to patterns
    sub1.cmd(&["PSUBSCRIBE", "news.*", "sports.*"]).await;
    sub2.cmd(&["PSUBSCRIBE", "weather.*"]).await;

    tokio::time::sleep(Duration::from_millis(10)).await;

    // Query pattern subscription count
    let result = query.cmd(&["PUBSUB", "NUMPAT"]).await;

    assert_eq!(result, RespValue::Integer(3)); // 3 active patterns

    server.stop().await;
}

#[tokio::test]
async fn test_mixed_subscribe_and_psubscribe() {
    let server = TestServer::spawn().await;
    let mut subscriber = server.client().await;
    let mut publisher = server.client().await;

    // Subscribe to both exact channel and pattern
    subscriber.cmd(&["SUBSCRIBE", "news.sports"]).await;
    subscriber.cmd(&["PSUBSCRIBE", "news.*"]).await;

    tokio::time::sleep(Duration::from_millis(10)).await;

    // Publish to the channel
    // Should match both subscription and pattern
    let result = publisher.cmd(&["PUBLISH", "news.sports", "Game on!"]).await;
    
    // The subscriber gets the message twice (once for channel, once for pattern)
    // PUBLISH returns 2 because both subscriptions matched (even though same client)
    assert_eq!(result, RespValue::Integer(2));

    server.stop().await;
}

#[tokio::test]
async fn test_unsubscribe_from_nonexistent_channel() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Unsubscribe from channel we never subscribed to
    let result = client.cmd(&["UNSUBSCRIBE", "nonexistent"]).await;

    match result {
        RespValue::Array(confirmations) => {
            assert_eq!(confirmations.len(), 1);
            match &confirmations[0] {
                RespValue::Array(parts) => {
                    assert_eq!(parts.len(), 3);
                    assert_eq!(parts[0], RespValue::bulk_string("unsubscribe"));
                    assert_eq!(parts[1], RespValue::bulk_string("nonexistent"));
                    assert_eq!(parts[2], RespValue::Integer(0)); // 0 channels remaining
                }
                _ => panic!("Expected array"),
            }
        }
        _ => panic!("Expected array of confirmations"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_publish_empty_message() {
    let server = TestServer::spawn().await;
    let mut subscriber = server.client().await;
    let mut publisher = server.client().await;

    subscriber.cmd(&["SUBSCRIBE", "channel1"]).await;

    tokio::time::sleep(Duration::from_millis(10)).await;

    // Publish empty message
    let result = publisher.cmd(&["PUBLISH", "channel1", ""]).await;
    assert_eq!(result, RespValue::Integer(1)); // Still delivered

    server.stop().await;
}

#[tokio::test]
async fn test_publish_binary_message() {
    let server = TestServer::spawn().await;
    let mut subscriber = server.client().await;
    let mut publisher = server.client().await;

    subscriber.cmd(&["SUBSCRIBE", "binary"]).await;

    tokio::time::sleep(Duration::from_millis(10)).await;

    // Publish binary data (non-UTF8)
    let result = publisher
        .cmd(&["PUBLISH", "binary", "\x00\x01\x02\x7F"])
        .await;
    assert_eq!(result, RespValue::Integer(1));

    server.stop().await;
}

#[tokio::test]
async fn test_pubsub_wrong_arity() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // PUBLISH with wrong number of arguments
    let result = client.cmd(&["PUBLISH"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    let result = client.cmd(&["PUBLISH", "channel"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    // SUBSCRIBE with no arguments
    let result = client.cmd(&["SUBSCRIBE"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    // PSUBSCRIBE with no arguments
    let result = client.cmd(&["PSUBSCRIBE"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    server.stop().await;
}

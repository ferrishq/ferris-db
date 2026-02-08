//! Pub/Sub commands: PUBLISH, SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, PUBSUB
//!
//! Redis Pub/Sub implements the publish-subscribe messaging pattern.
//! Publishers send messages to channels without knowledge of subscribers.
//! Subscribers receive messages from channels they're interested in.

use crate::{CommandContext, CommandError, CommandResult};
use bytes::Bytes;
use ferris_protocol::RespValue;

/// PUBLISH channel message
///
/// Posts a message to the given channel.
/// Returns the number of clients that received the message.
///
/// Time complexity: O(N+M) where N is the number of clients subscribed to the channel
/// and M is the total number of subscribed patterns
pub fn publish(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongArity("PUBLISH".to_string()));
    }

    let channel = args[0]
        .as_bytes()
        .ok_or_else(|| CommandError::InvalidArgument("invalid channel".to_string()))?;
    let message = args[1]
        .as_bytes()
        .cloned()
        .or_else(|| args[1].as_str().map(|s| Bytes::from(s.to_owned())))
        .ok_or_else(|| CommandError::InvalidArgument("invalid message".to_string()))?;

    let count = ctx.pubsub_registry().publish(channel, message);

    Ok(RespValue::Integer(count as i64))
}

/// SUBSCRIBE channel [channel ...]
///
/// Subscribes the client to the specified channels.
/// Note: This puts the connection in pub/sub mode. Once subscribed, clients can only
/// use SUBSCRIBE, PSUBSCRIBE, UNSUBSCRIBE, PUNSUBSCRIBE, PING, and QUIT commands.
///
/// Time complexity: O(N) where N is the number of channels to subscribe to
pub fn subscribe(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("SUBSCRIBE".to_string()));
    }

    let subscriber_id = ctx.subscriber_id();
    let mut results = Vec::new();

    for arg in args {
        let channel = arg
            .as_bytes()
            .cloned()
            .or_else(|| arg.as_str().map(|s| Bytes::from(s.to_owned())))
            .ok_or_else(|| CommandError::InvalidArgument("invalid channel".to_string()))?;

        let count = ctx
            .pubsub_registry()
            .subscribe(subscriber_id, channel.clone());

        // Return subscription confirmation
        results.push(RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("subscribe")),
            RespValue::BulkString(channel),
            RespValue::Integer(count as i64),
        ]));
    }

    Ok(RespValue::Array(results))
}

/// UNSUBSCRIBE [channel [channel ...]]
///
/// Unsubscribes the client from the given channels, or from all channels if none is given.
///
/// Time complexity: O(N) where N is the number of channels to unsubscribe from
pub fn unsubscribe(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    let subscriber_id = ctx.subscriber_id();
    let mut results = Vec::new();

    if args.is_empty() {
        // Unsubscribe from all channels
        let count = ctx.pubsub_registry().unsubscribe_all(subscriber_id);
        results.push(RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("unsubscribe")),
            RespValue::Null,
            RespValue::Integer(count as i64),
        ]));
    } else {
        for arg in args {
            let channel = arg
                .as_bytes()
                .cloned()
                .or_else(|| arg.as_str().map(|s| Bytes::from(s.to_owned())))
                .ok_or_else(|| CommandError::InvalidArgument("invalid channel".to_string()))?;

            let count = ctx
                .pubsub_registry()
                .unsubscribe(subscriber_id, channel.clone());

            results.push(RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("unsubscribe")),
                RespValue::BulkString(channel),
                RespValue::Integer(count as i64),
            ]));
        }
    }

    Ok(RespValue::Array(results))
}

/// PSUBSCRIBE pattern [pattern ...]
///
/// Subscribes the client to the given patterns.
/// Supported patterns: h?llo subscribes to hello, hallo, hxllo; h*llo subscribes to hllo and heeeello
///
/// Time complexity: O(N) where N is the number of patterns to subscribe to
pub fn psubscribe(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("PSUBSCRIBE".to_string()));
    }

    let subscriber_id = ctx.subscriber_id();
    let mut results = Vec::new();

    for arg in args {
        let pattern = arg
            .as_bytes()
            .cloned()
            .or_else(|| arg.as_str().map(|s| Bytes::from(s.to_owned())))
            .ok_or_else(|| CommandError::InvalidArgument("invalid pattern".to_string()))?;

        let count = ctx
            .pubsub_registry()
            .psubscribe(subscriber_id, pattern.clone());

        results.push(RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("psubscribe")),
            RespValue::BulkString(pattern),
            RespValue::Integer(count as i64),
        ]));
    }

    Ok(RespValue::Array(results))
}

/// PUNSUBSCRIBE [pattern [pattern ...]]
///
/// Unsubscribes the client from the given patterns, or from all patterns if none is given.
///
/// Time complexity: O(N) where N is the number of patterns to unsubscribe from
pub fn punsubscribe(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    let subscriber_id = ctx.subscriber_id();
    let mut results = Vec::new();

    if args.is_empty() {
        // Unsubscribe from all patterns
        let count = ctx.pubsub_registry().punsubscribe_all(subscriber_id);
        results.push(RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("punsubscribe")),
            RespValue::Null,
            RespValue::Integer(count as i64),
        ]));
    } else {
        for arg in args {
            let pattern = arg
                .as_bytes()
                .cloned()
                .or_else(|| arg.as_str().map(|s| Bytes::from(s.to_owned())))
                .ok_or_else(|| CommandError::InvalidArgument("invalid pattern".to_string()))?;

            let count = ctx
                .pubsub_registry()
                .punsubscribe(subscriber_id, pattern.clone());

            results.push(RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("punsubscribe")),
                RespValue::BulkString(pattern),
                RespValue::Integer(count as i64),
            ]));
        }
    }

    Ok(RespValue::Array(results))
}

/// PUBSUB subcommand [argument [argument ...]]
///
/// Introspection command for the pub/sub subsystem.
/// Subcommands:
/// - CHANNELS [pattern]: Lists currently active channels
/// - NUMSUB [channel ...]: Returns the number of subscribers for the specified channels
/// - NUMPAT: Returns the number of subscriptions to patterns
///
/// Time complexity: O(N) for CHANNELS, O(N) for NUMSUB, O(1) for NUMPAT
pub fn pubsub(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("PUBSUB".to_string()));
    }

    let subcommand = args[0]
        .as_str()
        .ok_or_else(|| CommandError::SyntaxError)?
        .to_uppercase();

    match subcommand.as_str() {
        "CHANNELS" => {
            let pattern_opt = if args.len() > 1 {
                args[1].as_bytes().map(|b| b.as_ref())
            } else {
                None
            };

            let channels = ctx.pubsub_registry().channels(pattern_opt);
            let results: Vec<RespValue> = channels.into_iter().map(RespValue::BulkString).collect();

            Ok(RespValue::Array(results))
        }
        "NUMSUB" => {
            let channels: Vec<Bytes> = args[1..]
                .iter()
                .filter_map(|arg| {
                    arg.as_bytes()
                        .cloned()
                        .or_else(|| arg.as_str().map(|s| Bytes::from(s.to_owned())))
                })
                .collect();

            let counts = ctx.pubsub_registry().numsub(&channels);
            let mut results = Vec::new();

            for channel in channels {
                results.push(RespValue::BulkString(channel.clone()));
                results.push(RespValue::Integer(
                    counts.get(&channel).copied().unwrap_or(0) as i64,
                ));
            }

            Ok(RespValue::Array(results))
        }
        "NUMPAT" => {
            let count = ctx.pubsub_registry().numpat();
            Ok(RespValue::Integer(count as i64))
        }
        _ => Err(CommandError::InvalidArgument(format!(
            "Unknown PUBSUB subcommand '{}'",
            subcommand
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferris_core::{KeyStore, PubSubRegistry};
    use std::sync::Arc;

    fn make_ctx() -> CommandContext {
        let store = Arc::new(KeyStore::default());
        let pubsub = Arc::new(PubSubRegistry::new());
        CommandContext::with_pubsub(store, pubsub)
    }

    #[test]
    fn test_publish() {
        let mut ctx = make_ctx();
        let result = publish(
            &mut ctx,
            &[
                RespValue::BulkString(Bytes::from("news")),
                RespValue::BulkString(Bytes::from("Hello!")),
            ],
        )
        .unwrap();

        match result {
            RespValue::Integer(count) => assert_eq!(count, 0), // No subscribers
            _ => panic!("Expected integer"),
        }
    }

    #[test]
    fn test_publish_wrong_arity() {
        let mut ctx = make_ctx();
        let result = publish(&mut ctx, &[RespValue::BulkString(Bytes::from("news"))]);
        assert!(result.is_err());
    }

    #[test]
    fn test_subscribe() {
        let mut ctx = make_ctx();
        let result = subscribe(
            &mut ctx,
            &[
                RespValue::BulkString(Bytes::from("news")),
                RespValue::BulkString(Bytes::from("sports")),
            ],
        )
        .unwrap();

        match result {
            RespValue::Array(results) => {
                assert_eq!(results.len(), 2);
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_subscribe_wrong_arity() {
        let mut ctx = make_ctx();
        let result = subscribe(&mut ctx, &[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_unsubscribe_all() {
        let mut ctx = make_ctx();
        let result = unsubscribe(&mut ctx, &[]).unwrap();

        match result {
            RespValue::Array(results) => {
                assert_eq!(results.len(), 1);
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_unsubscribe_specific() {
        let mut ctx = make_ctx();
        let result = unsubscribe(&mut ctx, &[RespValue::BulkString(Bytes::from("news"))]).unwrap();

        match result {
            RespValue::Array(results) => {
                assert_eq!(results.len(), 1);
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_psubscribe() {
        let mut ctx = make_ctx();
        let result = psubscribe(&mut ctx, &[RespValue::BulkString(Bytes::from("news.*"))]).unwrap();

        match result {
            RespValue::Array(results) => {
                assert_eq!(results.len(), 1);
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_psubscribe_wrong_arity() {
        let mut ctx = make_ctx();
        let result = psubscribe(&mut ctx, &[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_punsubscribe_all() {
        let mut ctx = make_ctx();
        let result = punsubscribe(&mut ctx, &[]).unwrap();

        match result {
            RespValue::Array(results) => {
                assert_eq!(results.len(), 1);
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_punsubscribe_specific() {
        let mut ctx = make_ctx();
        let result =
            punsubscribe(&mut ctx, &[RespValue::BulkString(Bytes::from("news.*"))]).unwrap();

        match result {
            RespValue::Array(results) => {
                assert_eq!(results.len(), 1);
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_pubsub_channels() {
        let mut ctx = make_ctx();
        let result = pubsub(&mut ctx, &[RespValue::BulkString(Bytes::from("CHANNELS"))]).unwrap();

        match result {
            RespValue::Array(_) => {} // OK
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_pubsub_channels_with_pattern() {
        let mut ctx = make_ctx();
        let result = pubsub(
            &mut ctx,
            &[
                RespValue::BulkString(Bytes::from("CHANNELS")),
                RespValue::BulkString(Bytes::from("news*")),
            ],
        )
        .unwrap();

        match result {
            RespValue::Array(_) => {} // OK
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_pubsub_numsub() {
        let mut ctx = make_ctx();
        let result = pubsub(
            &mut ctx,
            &[
                RespValue::BulkString(Bytes::from("NUMSUB")),
                RespValue::BulkString(Bytes::from("news")),
            ],
        )
        .unwrap();

        match result {
            RespValue::Array(results) => {
                assert_eq!(results.len(), 2); // channel + count
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_pubsub_numpat() {
        let mut ctx = make_ctx();
        let result = pubsub(&mut ctx, &[RespValue::BulkString(Bytes::from("NUMPAT"))]).unwrap();

        match result {
            RespValue::Integer(_) => {} // OK
            _ => panic!("Expected integer"),
        }
    }

    #[test]
    fn test_pubsub_unknown_subcommand() {
        let mut ctx = make_ctx();
        let result = pubsub(&mut ctx, &[RespValue::BulkString(Bytes::from("INVALID"))]);
        assert!(result.is_err());
    }

    #[test]
    fn test_pubsub_wrong_arity() {
        let mut ctx = make_ctx();
        let result = pubsub(&mut ctx, &[]);
        assert!(result.is_err());
    }
}

//! Per-connection handling
//!
//! Each TCP connection gets its own async task that reads commands,
//! executes them via the CommandExecutor, and writes responses.
//!
//! # Pipeline Support
//!
//! Redis clients can send multiple commands without waiting for responses
//! (pipelining). This module supports pipelining by:
//!
//! 1. Reading all available commands from the buffer at once
//! 2. Executing them in order
//! 3. Sending all responses back in order
//!
//! Blocking commands (BLPOP, BRPOP, etc.) are handled specially and break
//! the pipeline - any commands after a blocking command wait until the
//! blocking command completes.

use crate::shutdown::Shutdown;
use bytes::Bytes;
use ferris_commands::{BlockingAction, CommandContext, CommandError, CommandExecutor};
use ferris_core::{BlockingRegistry, PubSubRegistry};
use ferris_persistence::AofWriter;
use ferris_replication::ReplicationManager;
use ferris_protocol::{Command, ProtocolVersion, RespCodec, RespValue};
use futures_util::{FutureExt, SinkExt, Stream, StreamExt};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tracing::{debug, trace, warn};

/// Handle a single client connection.
///
/// Reads RESP-framed commands from the socket, executes them,
/// and writes responses back. Exits when the client disconnects,
/// sends QUIT, or the server shuts down.
///
/// Supports blocking commands (BLPOP, BRPOP, etc.) by waiting on
/// per-key notifications from the shared `BlockingRegistry`.
///
/// # Pipelining
///
/// This handler supports Redis pipelining. When multiple commands are
/// available in the buffer, they are all parsed, executed in order,
/// and their responses are sent back in order. This improves throughput
/// by reducing round-trip latency.
#[allow(clippy::too_many_arguments)]
pub async fn handle_connection(
    stream: TcpStream,
    executor: Arc<CommandExecutor>,
    store: Arc<ferris_core::KeyStore>,
    blocking_registry: Arc<BlockingRegistry>,
    pubsub_registry: Arc<PubSubRegistry>,
    aof_writer: Option<Arc<AofWriter>>,
    replication_manager: Option<Arc<ReplicationManager>>,
    mut shutdown: Shutdown,
    timeout_secs: u64,
) {
    let peer = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".to_string());

    debug!(peer = %peer, "New connection");

    let mut framed = Framed::new(stream, RespCodec::new());
    let mut ctx = CommandContext::with_resources(
        store,
        blocking_registry,
        pubsub_registry,
        aof_writer,
        replication_manager,
    );

    // Create timeout sleep future if timeout is enabled
    let timeout_duration = if timeout_secs > 0 {
        Some(std::time::Duration::from_secs(timeout_secs))
    } else {
        None
    };

    loop {
        // Create a timeout future for this iteration
        let timeout_sleep = timeout_duration.map(tokio::time::sleep);

        tokio::select! {
            // Wait for shutdown signal
            () = shutdown.recv() => {
                debug!(peer = %peer, "Shutdown signal received, closing connection");
                break;
            }
            // Connection idle timeout
            () = async {
                if let Some(sleep) = timeout_sleep {
                    sleep.await;
                } else {
                    std::future::pending::<()>().await
                }
            } => {
                debug!(peer = %peer, timeout = timeout_secs, "Connection idle timeout");
                break;
            }
            // Wait for next command(s) from client
            frame = framed.next() => {
                let first_frame = match frame {
                    Some(Ok(f)) => f,
                    Some(Err(e)) => {
                        warn!(peer = %peer, error = %e, "Protocol error, closing connection");
                        // Send error response before closing
                        let err_resp = RespValue::Error(format!("ERR {e}"));
                        let _ = framed.send(err_resp).await;
                        break;
                    }
                    None => {
                        // Client disconnected
                        debug!(peer = %peer, "Client disconnected");
                        break;
                    }
                };

                // Collect all available commands from the buffer (pipelining support)
                let mut frames = vec![first_frame];

                // Try to read more frames that are already buffered
                // This is non-blocking - we only read what's already parsed
                loop {
                    // Use poll_next to check if more frames are immediately available
                    match futures_util::future::poll_fn(|cx| {
                        std::pin::Pin::new(&mut framed).poll_next(cx)
                    }).now_or_never() {
                        Some(Some(Ok(f))) => {
                            frames.push(f);
                        }
                        Some(Some(Err(e))) => {
                            warn!(peer = %peer, error = %e, "Protocol error in pipeline");
                            frames.push(RespValue::Error(format!("ERR {e}")));
                            break;
                        }
                        _ => break, // No more frames available
                    }
                }

                let pipeline_size = frames.len();
                if pipeline_size > 1 {
                    trace!(peer = %peer, count = pipeline_size, "Processing pipeline");
                }

                // Process all commands and collect responses
                let mut responses = Vec::with_capacity(frames.len());
                let mut should_quit = false;
                let mut should_break = false;

                for frame in frames {
                    // If it's already an error (from parsing), just add it as response
                    if let RespValue::Error(_) = &frame {
                        responses.push(frame);
                        continue;
                    }

                    // Parse the RespValue into a Command
                    let cmd = match Command::from_resp(frame) {
                        Ok(c) => c,
                        Err(e) => {
                            responses.push(RespValue::Error(format!("ERR {e}")));
                            continue;
                        }
                    };

                    let is_quit = cmd.name == "QUIT";
                    let is_hello = cmd.name == "HELLO";

                    // Check if we're in a transaction (MULTI mode)
                    // If so, queue the command instead of executing it
                    // Exception: EXEC, DISCARD, MULTI, WATCH, UNWATCH should be executed immediately
                    let response = if ctx.transaction_state().in_transaction()
                        && cmd.name != "EXEC"
                        && cmd.name != "DISCARD"
                        && cmd.name != "MULTI"
                        && cmd.name != "WATCH"
                        && cmd.name != "UNWATCH"
                    {
                        // Queue the command
                        let args: Vec<RespValue> = cmd
                            .args
                            .iter()
                            .map(|b| RespValue::BulkString(b.clone()))
                            .collect();
                        ctx.transaction_state_mut().queue_command(cmd.name.clone(), args);
                        RespValue::SimpleString("QUEUED".to_string())
                    } else {
                        // Execute the command normally
                        match executor.execute(&mut ctx, &cmd) {
                        Ok(resp) => resp,
                        Err(CommandError::Block(action)) => {
                            // Blocking commands break the pipeline
                            // First, send all responses collected so far
                            for resp in responses.drain(..) {
                                if framed.send(resp).await.is_err() {
                                    should_break = true;
                                    break;
                                }
                            }
                            if should_break {
                                break;
                            }

                            // Handle the blocking command
                            match handle_blocking(&executor, &mut ctx, &action, &mut shutdown).await {
                                BlockingOutcome::Result(resp) => resp,
                                BlockingOutcome::Shutdown => {
                                    debug!(peer = %peer, "Shutdown during blocking command");
                                    should_break = true;
                                    break;
                                }
                            }
                        }
                        Err(e) => RespValue::Error(e.to_resp_string()),
                        }
                    };

                    // If this was a HELLO command that changed the protocol version,
                    // update the codec.
                    if is_hello {
                        match ctx.protocol_version() {
                            2 => framed.codec_mut().set_version(ProtocolVersion::Resp2),
                            3 => framed.codec_mut().set_version(ProtocolVersion::Resp3),
                            _ => {}
                        }
                    }

                    responses.push(response);

                    if is_quit {
                        should_quit = true;
                        break; // Don't process more commands after QUIT
                    }
                }

                if should_break {
                    break;
                }

                // Send all responses
                for resp in responses {
                    if framed.send(resp).await.is_err() {
                        should_break = true;
                        break;
                    }
                }

                if should_break {
                    break;
                }

                // If QUIT was in the pipeline, close after sending all responses
                if should_quit {
                    debug!(peer = %peer, "Client sent QUIT");
                    break;
                }
            }
        }
    }

    debug!(peer = %peer, "Connection closed");
}

/// Outcome of a blocking wait
enum BlockingOutcome {
    /// Got a result (either data or timeout Null)
    Result(RespValue),
    /// Server is shutting down
    Shutdown,
}

/// Handle a blocking command by waiting for notifications on the specified keys.
///
/// The flow is:
/// 1. Register for notifications on all watched keys
/// 2. Wait for any notification (or timeout)
/// 3. Re-execute the original command to try to get data
/// 4. If data found, return it; otherwise go back to step 2
/// 5. On timeout, return Null array
async fn handle_blocking(
    executor: &CommandExecutor,
    ctx: &mut CommandContext,
    action: &BlockingAction,
    shutdown: &mut Shutdown,
) -> BlockingOutcome {
    let registry = ctx.blocking_registry().clone();

    // Get Notify handles for all watched keys
    let notifies: Vec<_> = action
        .keys
        .iter()
        .map(|key| registry.get_or_create_notify(action.db_index, key))
        .collect();

    // Calculate deadline
    let deadline = action.timeout.map(|d| tokio::time::Instant::now() + d);

    loop {
        // Wait for any notification, timeout, or shutdown
        let notified = async {
            // We need to wait for ANY of the notifiers to fire.
            // Create a future for each and use select.
            // Since we don't know the count at compile time, we poll in a loop
            // with a short sleep, or we can use tokio::select! with a notified future.
            //
            // Efficient approach: create notified futures and race them.
            // We use a helper that returns when any one fires.
            wait_any_notify(&notifies).await;
        };

        tokio::select! {
            biased;

            () = shutdown.recv() => {
                // Clean up notify registrations
                registry.cleanup_keys(action.db_index, &action.keys);
                return BlockingOutcome::Shutdown;
            }
            _ = async {
                if let Some(dl) = deadline {
                    tokio::time::sleep_until(dl).await;
                } else {
                    // No timeout — block forever (well, until notify or shutdown)
                    std::future::pending::<()>().await;
                }
            } => {
                // Timeout expired
                registry.cleanup_keys(action.db_index, &action.keys);
                return BlockingOutcome::Result(RespValue::Null);
            }
            () = notified => {
                // A key was modified — try to execute the command again
                let retry_cmd = Command {
                    name: action.command_name.clone(),
                    args: action
                        .args
                        .iter()
                        .filter_map(|v| v.as_bytes().cloned().or_else(|| v.as_str().map(|s| Bytes::from(s.to_owned()))))
                        .collect(),
                };

                match executor.execute(ctx, &retry_cmd) {
                    Ok(resp) => {
                        registry.cleanup_keys(action.db_index, &action.keys);
                        return BlockingOutcome::Result(resp);
                    }
                    Err(CommandError::Block(_)) => {
                        // Still no data, continue waiting
                        continue;
                    }
                    Err(e) => {
                        registry.cleanup_keys(action.db_index, &action.keys);
                        return BlockingOutcome::Result(RespValue::Error(e.to_resp_string()));
                    }
                }
            }
        }
    }
}

/// Wait for any one of the provided `Notify` handles to fire.
async fn wait_any_notify(notifies: &[Arc<tokio::sync::Notify>]) {
    if notifies.is_empty() {
        std::future::pending::<()>().await;
        return;
    }

    // Use tokio::select! dynamically by spawning futures
    // For a small number of keys (typically 1-10), this is fine.
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let tx = Arc::new(std::sync::Mutex::new(Some(tx)));

    let mut handles = Vec::with_capacity(notifies.len());
    for notify in notifies {
        let notify = notify.clone();
        let tx = tx.clone();
        handles.push(tokio::spawn(async move {
            notify.notified().await;
            if let Ok(mut guard) = tx.lock() {
                if let Some(sender) = guard.take() {
                    let _ = sender.send(());
                }
            }
        }));
    }

    // Wait for the first notification
    let _ = rx.await;

    // Cancel remaining tasks
    for handle in handles {
        handle.abort();
    }
}

/// Handle a single client connection for inline commands (used in tests).
/// This is a simpler version that processes raw bytes.
pub async fn process_raw_command(
    executor: &CommandExecutor,
    ctx: &mut CommandContext,
    parts: &[&str],
) -> RespValue {
    let cmd = Command {
        name: parts[0].to_uppercase(),
        args: parts[1..]
            .iter()
            .map(|s| Bytes::from(s.to_string()))
            .collect(),
    };

    match executor.execute(ctx, &cmd) {
        Ok(resp) => resp,
        Err(e) => RespValue::Error(e.to_resp_string()),
    }
}

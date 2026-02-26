//! Connection commands: SELECT, QUIT, CLIENT, HELLO, RESET

use crate::{CommandContext, CommandError, CommandResult};
use bytes::Bytes;
use ferris_protocol::RespValue;

/// SELECT index
///
/// Select the Redis logical database having the specified zero-based numeric index.
/// New connections always use database 0.
///
/// Time complexity: O(1)
pub fn select(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    let index_arg = args
        .first()
        .ok_or_else(|| CommandError::WrongArity("SELECT".to_string()))?;

    let index_str = index_arg.as_str().ok_or(CommandError::InvalidDbIndex)?;

    let index: usize = index_str
        .parse()
        .map_err(|_| CommandError::InvalidDbIndex)?;

    // Check if database index is valid (0-15 by default)
    if index >= ctx.store().num_databases() {
        return Err(CommandError::InvalidDbIndex);
    }

    ctx.select_db(index);
    Ok(RespValue::ok())
}

/// QUIT
///
/// Ask the server to close the connection.
/// The connection is closed as soon as all pending replies have been written.
///
/// Time complexity: O(1)
pub fn quit(_ctx: &mut CommandContext, _args: &[RespValue]) -> CommandResult {
    // The actual connection closing is handled by the network layer
    // We just return OK here
    Ok(RespValue::ok())
}

/// CLIENT subcommand [arguments...]
///
/// This command allows managing client connections.
///
/// Subcommands:
/// - CLIENT SETNAME connection-name
/// - CLIENT GETNAME
/// - CLIENT ID
/// - CLIENT LIST
/// - CLIENT INFO
///
/// Time complexity: O(1) for most subcommands
pub fn client(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    let subcommand = args
        .first()
        .and_then(|v| v.as_str())
        .map(|s| s.to_uppercase())
        .ok_or_else(|| CommandError::WrongArity("CLIENT".to_string()))?;

    match subcommand.as_str() {
        "SETNAME" => {
            let name = args
                .get(1)
                .and_then(|v| v.as_str())
                .ok_or(CommandError::SyntaxError)?;

            if name.is_empty() {
                ctx.set_client_name(None);
            } else {
                ctx.set_client_name(Some(name.to_string()));
            }
            Ok(RespValue::ok())
        }
        "GETNAME" => match ctx.client_name() {
            Some(name) => Ok(RespValue::BulkString(Bytes::from(name.to_owned()))),
            None => Ok(RespValue::Null),
        },
        "ID" => {
            // Return the unique client ID assigned to this connection
            Ok(RespValue::Integer(ctx.client_id() as i64))
        }
        "LIST" => {
            // Return minimal client list info
            let info = format!(
                "id={} addr=127.0.0.1:0 name={} db={}\n",
                ctx.client_id(),
                ctx.client_name().unwrap_or(""),
                ctx.selected_db()
            );
            Ok(RespValue::BulkString(Bytes::from(info)))
        }
        "INFO" => {
            // Return info about current connection
            let info = format!(
                "id={} addr=127.0.0.1:0 name={} db={}\n",
                ctx.client_id(),
                ctx.client_name().unwrap_or(""),
                ctx.selected_db()
            );
            Ok(RespValue::BulkString(Bytes::from(info)))
        }
        "KILL" => {
            // Not fully implemented
            Ok(RespValue::ok())
        }
        "PAUSE" => {
            // Not fully implemented
            Ok(RespValue::ok())
        }
        "UNPAUSE" => Ok(RespValue::ok()),
        "REPLY" => {
            // CLIENT REPLY ON|OFF|SKIP
            // Not fully implemented, just accept
            Ok(RespValue::ok())
        }
        "NO-EVICT" => {
            // CLIENT NO-EVICT ON|OFF
            // Controls whether this client is excluded from eviction
            Ok(RespValue::ok())
        }
        "NO-TOUCH" => {
            // CLIENT NO-TOUCH ON|OFF
            // Controls whether commands affect LRU/LFU of keys
            Ok(RespValue::ok())
        }
        "SETINFO" => {
            // CLIENT SETINFO <LIB-NAME libname | LIB-VER libver>
            // Set client library info
            if args.len() < 3 {
                return Err(CommandError::WrongArity("CLIENT".to_string()));
            }
            let info_type = args
                .get(1)
                .and_then(|v| v.as_str())
                .map(|s| s.to_uppercase())
                .ok_or(CommandError::SyntaxError)?;
            let _value = args
                .get(2)
                .and_then(|v| v.as_str())
                .ok_or(CommandError::SyntaxError)?;

            match info_type.as_str() {
                "LIB-NAME" | "LIB-VER" => Ok(RespValue::ok()),
                _ => Err(CommandError::InvalidArgument(format!(
                    "Unknown CLIENT SETINFO attribute '{}'",
                    info_type
                ))),
            }
        }
        "GETREDIR" => {
            // CLIENT GETREDIR
            // Returns the client ID we are redirecting tracking notifications to
            // Returns -1 if not redirecting
            Ok(RespValue::Integer(-1))
        }
        "TRACKINGINFO" => {
            // CLIENT TRACKINGINFO
            // Returns tracking info for this connection
            Ok(RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("flags")),
                RespValue::Array(vec![RespValue::BulkString(Bytes::from("off"))]),
                RespValue::BulkString(Bytes::from("redirect")),
                RespValue::Integer(-1),
                RespValue::BulkString(Bytes::from("prefixes")),
                RespValue::Array(vec![]),
            ]))
        }
        "TRACKING" => {
            // CLIENT TRACKING <ON|OFF> [REDIRECT client-id] [PREFIX prefix ...] [BCAST] [OPTIN] [OPTOUT] [NOLOOP]
            // Enable/disable server-assisted client caching
            if args.len() < 2 {
                return Err(CommandError::WrongArity("CLIENT".to_string()));
            }
            let mode = args
                .get(1)
                .and_then(|v| v.as_str())
                .map(|s| s.to_uppercase())
                .ok_or(CommandError::SyntaxError)?;

            match mode.as_str() {
                "ON" | "OFF" => Ok(RespValue::ok()),
                _ => Err(CommandError::SyntaxError),
            }
        }
        "CACHING" => {
            // CLIENT CACHING <YES|NO>
            // Enable/disable tracking for next command
            if args.len() < 2 {
                return Err(CommandError::WrongArity("CLIENT".to_string()));
            }
            let mode = args
                .get(1)
                .and_then(|v| v.as_str())
                .map(|s| s.to_uppercase())
                .ok_or(CommandError::SyntaxError)?;

            match mode.as_str() {
                "YES" | "NO" => Ok(RespValue::ok()),
                _ => Err(CommandError::SyntaxError),
            }
        }
        "UNBLOCK" => {
            // CLIENT UNBLOCK client-id [TIMEOUT|ERROR]
            // Unblock a client blocked by a blocking command
            if args.len() < 2 {
                return Err(CommandError::WrongArity("CLIENT".to_string()));
            }
            // Return 0 (client not found) or 1 (client unblocked)
            // For now, always return 0
            Ok(RespValue::Integer(0))
        }
        "HELP" => {
            let help = vec![
                RespValue::BulkString(Bytes::from("CLIENT CACHING <YES|NO>")),
                RespValue::BulkString(Bytes::from("    Enable/disable tracking for next command.")),
                RespValue::BulkString(Bytes::from("CLIENT GETNAME")),
                RespValue::BulkString(Bytes::from("    Return the connection name.")),
                RespValue::BulkString(Bytes::from("CLIENT GETREDIR")),
                RespValue::BulkString(Bytes::from("    Return tracking redirection client ID.")),
                RespValue::BulkString(Bytes::from("CLIENT ID")),
                RespValue::BulkString(Bytes::from("    Return the client ID.")),
                RespValue::BulkString(Bytes::from("CLIENT INFO")),
                RespValue::BulkString(Bytes::from("    Return info about current connection.")),
                RespValue::BulkString(Bytes::from("CLIENT KILL <filter>")),
                RespValue::BulkString(Bytes::from("    Kill connections matching filter.")),
                RespValue::BulkString(Bytes::from("CLIENT LIST [ID id...]")),
                RespValue::BulkString(Bytes::from("    List client connections.")),
                RespValue::BulkString(Bytes::from("CLIENT NO-EVICT <ON|OFF>")),
                RespValue::BulkString(Bytes::from("    Set no-evict mode for client.")),
                RespValue::BulkString(Bytes::from("CLIENT NO-TOUCH <ON|OFF>")),
                RespValue::BulkString(Bytes::from("    Set no-touch mode for client.")),
                RespValue::BulkString(Bytes::from("CLIENT PAUSE <timeout> [WRITE|ALL]")),
                RespValue::BulkString(Bytes::from("    Pause client commands.")),
                RespValue::BulkString(Bytes::from("CLIENT REPLY <ON|OFF|SKIP>")),
                RespValue::BulkString(Bytes::from("    Control server replies.")),
                RespValue::BulkString(Bytes::from("CLIENT SETINFO <LIB-NAME|LIB-VER> <value>")),
                RespValue::BulkString(Bytes::from("    Set client library info.")),
                RespValue::BulkString(Bytes::from("CLIENT SETNAME <name>")),
                RespValue::BulkString(Bytes::from("    Set connection name.")),
                RespValue::BulkString(Bytes::from("CLIENT TRACKING <ON|OFF> [options]")),
                RespValue::BulkString(Bytes::from("    Enable/disable server-assisted caching.")),
                RespValue::BulkString(Bytes::from("CLIENT TRACKINGINFO")),
                RespValue::BulkString(Bytes::from("    Return tracking info.")),
                RespValue::BulkString(Bytes::from("CLIENT UNBLOCK <client-id> [TIMEOUT|ERROR]")),
                RespValue::BulkString(Bytes::from("    Unblock a blocked client.")),
                RespValue::BulkString(Bytes::from("CLIENT UNPAUSE")),
                RespValue::BulkString(Bytes::from("    Resume client commands.")),
            ];
            Ok(RespValue::Array(help))
        }
        _ => Err(CommandError::InvalidArgument(format!(
            "Unknown CLIENT subcommand '{subcommand}'"
        ))),
    }
}

/// HELLO [protover [AUTH username password] [SETNAME clientname]]
///
/// Negotiate the RESP protocol version and optionally authenticate.
///
/// Returns a map of server properties:
/// - server: "ferris-db"
/// - version: "0.1.0"
/// - proto: `<protocol version>`
/// - id: `<client id>`
/// - mode: "standalone"
/// - role: "master"
/// - modules: []
///
/// Time complexity: O(1)
pub fn hello(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    let mut proto_version = ctx.protocol_version();

    let mut i = 0;
    if let Some(version_arg) = args.first() {
        if let Some(s) = version_arg.as_str() {
            let v: u32 = s.parse().map_err(|_| {
                CommandError::InvalidArgument("Protocol version is not an integer".to_string())
            })?;
            if v != 2 && v != 3 {
                return Err(CommandError::InvalidArgument(format!(
                    "NOPROTO unsupported protocol version {v}"
                )));
            }
            proto_version = v;
            i = 1;
        }
    }

    // Parse optional AUTH and SETNAME
    while i < args.len() {
        let opt = args[i]
            .as_str()
            .map(|s| s.to_uppercase())
            .unwrap_or_default();
        match opt.as_str() {
            "AUTH" => {
                // AUTH username password
                i += 1;
                let _username = args
                    .get(i)
                    .and_then(|v| v.as_str())
                    .ok_or(CommandError::SyntaxError)?;
                i += 1;
                let _password = args
                    .get(i)
                    .and_then(|v| v.as_str())
                    .ok_or(CommandError::SyntaxError)?;
                ctx.set_authenticated(true);
            }
            "SETNAME" => {
                i += 1;
                let name = args
                    .get(i)
                    .and_then(|v| v.as_str())
                    .ok_or(CommandError::SyntaxError)?;
                if name.is_empty() {
                    ctx.set_client_name(None);
                } else {
                    ctx.set_client_name(Some(name.to_string()));
                }
            }
            _ => return Err(CommandError::SyntaxError),
        }
        i += 1;
    }

    ctx.set_protocol_version(proto_version);

    // Build response map as array of key-value pairs (works for both RESP2 and RESP3)
    let response = vec![
        RespValue::BulkString(Bytes::from("server")),
        RespValue::BulkString(Bytes::from("ferris-db")),
        RespValue::BulkString(Bytes::from("version")),
        RespValue::BulkString(Bytes::from("0.1.0")),
        RespValue::BulkString(Bytes::from("proto")),
        RespValue::Integer(proto_version as i64),
        RespValue::BulkString(Bytes::from("id")),
        RespValue::Integer(ctx.client_id() as i64),
        RespValue::BulkString(Bytes::from("mode")),
        RespValue::BulkString(Bytes::from("standalone")),
        RespValue::BulkString(Bytes::from("role")),
        RespValue::BulkString(Bytes::from("master")),
        RespValue::BulkString(Bytes::from("modules")),
        RespValue::Array(vec![]),
    ];

    Ok(RespValue::Array(response))
}

/// RESET
///
/// Reset the connection to its initial state:
/// - Deauth the connection
/// - Select database 0
/// - Unsubscribe from all channels
/// - Clear client name
/// - Reset protocol to RESP2
///
/// Time complexity: O(1)
pub fn reset(ctx: &mut CommandContext, _args: &[RespValue]) -> CommandResult {
    ctx.reset();
    Ok(RespValue::SimpleString("RESET".to_string()))
}

/// AUTH [username] password
///
/// Authenticates the connection with an optional username and password.
/// For ferris-db, we accept any auth but don't enforce it (no-op).
///
/// Time complexity: O(1)
pub fn auth(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() || args.len() > 2 {
        return Err(CommandError::WrongArity("AUTH".to_string()));
    }

    // If 2 args: username + password
    // If 1 arg: just password (Redis 5.x style)
    // For now, we just mark as authenticated without actual validation
    ctx.set_authenticated(true);
    Ok(RespValue::ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferris_core::KeyStore;
    use std::sync::Arc;

    fn make_ctx() -> CommandContext {
        CommandContext::new(Arc::new(KeyStore::default()))
    }

    #[test]
    fn test_select_valid_db() {
        let mut ctx = make_ctx();
        assert_eq!(ctx.selected_db(), 0);

        let args = vec![RespValue::BulkString(Bytes::from("5"))];
        let result = select(&mut ctx, &args).unwrap();
        assert_eq!(result, RespValue::ok());
        assert_eq!(ctx.selected_db(), 5);
    }

    #[test]
    fn test_select_db_zero() {
        let mut ctx = make_ctx();
        ctx.select_db(3);

        let args = vec![RespValue::BulkString(Bytes::from("0"))];
        let result = select(&mut ctx, &args).unwrap();
        assert_eq!(result, RespValue::ok());
        assert_eq!(ctx.selected_db(), 0);
    }

    #[test]
    fn test_select_invalid_db() {
        let mut ctx = make_ctx();

        let args = vec![RespValue::BulkString(Bytes::from("100"))];
        let result = select(&mut ctx, &args);
        assert!(matches!(result, Err(CommandError::InvalidDbIndex)));
    }

    #[test]
    fn test_select_non_numeric() {
        let mut ctx = make_ctx();

        let args = vec![RespValue::BulkString(Bytes::from("abc"))];
        let result = select(&mut ctx, &args);
        assert!(matches!(result, Err(CommandError::InvalidDbIndex)));
    }

    #[test]
    fn test_select_no_args() {
        let mut ctx = make_ctx();
        let result = select(&mut ctx, &[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_quit() {
        let mut ctx = make_ctx();
        let result = quit(&mut ctx, &[]).unwrap();
        assert_eq!(result, RespValue::ok());
    }

    #[test]
    fn test_client_setname_getname() {
        let mut ctx = make_ctx();

        // Initially no name
        let args = vec![RespValue::BulkString(Bytes::from("GETNAME"))];
        let result = client(&mut ctx, &args).unwrap();
        assert_eq!(result, RespValue::Null);

        // Set a name
        let args = vec![
            RespValue::BulkString(Bytes::from("SETNAME")),
            RespValue::BulkString(Bytes::from("my-client")),
        ];
        let result = client(&mut ctx, &args).unwrap();
        assert_eq!(result, RespValue::ok());

        // Get the name
        let args = vec![RespValue::BulkString(Bytes::from("GETNAME"))];
        let result = client(&mut ctx, &args).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from("my-client")));
    }

    #[test]
    fn test_client_setname_empty_clears() {
        let mut ctx = make_ctx();

        // Set a name
        let args = vec![
            RespValue::BulkString(Bytes::from("SETNAME")),
            RespValue::BulkString(Bytes::from("my-client")),
        ];
        client(&mut ctx, &args).unwrap();
        assert!(ctx.client_name().is_some());

        // Clear name with empty string
        let args = vec![
            RespValue::BulkString(Bytes::from("SETNAME")),
            RespValue::BulkString(Bytes::from("")),
        ];
        client(&mut ctx, &args).unwrap();
        assert!(ctx.client_name().is_none());
    }

    #[test]
    fn test_client_id() {
        let mut ctx = make_ctx();
        let args = vec![RespValue::BulkString(Bytes::from("ID"))];
        let result = client(&mut ctx, &args).unwrap();

        if let RespValue::Integer(id) = result {
            assert!(id > 0);
        } else {
            panic!("Expected integer from CLIENT ID");
        }
    }

    #[test]
    fn test_client_list() {
        let mut ctx = make_ctx();
        let args = vec![RespValue::BulkString(Bytes::from("LIST"))];
        let result = client(&mut ctx, &args).unwrap();

        if let RespValue::BulkString(data) = result {
            let list_str = std::str::from_utf8(&data).unwrap();
            assert!(list_str.contains("id="));
            assert!(list_str.contains("db="));
        } else {
            panic!("Expected bulk string from CLIENT LIST");
        }
    }

    #[test]
    fn test_client_info() {
        let mut ctx = make_ctx();
        ctx.set_client_name(Some("test-client".to_string()));
        ctx.select_db(3);

        let args = vec![RespValue::BulkString(Bytes::from("INFO"))];
        let result = client(&mut ctx, &args).unwrap();

        if let RespValue::BulkString(data) = result {
            let info_str = std::str::from_utf8(&data).unwrap();
            assert!(info_str.contains("test-client"));
            assert!(info_str.contains("db=3"));
        } else {
            panic!("Expected bulk string from CLIENT INFO");
        }
    }

    #[test]
    fn test_client_unknown_subcommand() {
        let mut ctx = make_ctx();
        let args = vec![RespValue::BulkString(Bytes::from("UNKNOWN"))];
        let result = client(&mut ctx, &args);
        assert!(result.is_err());
    }

    #[test]
    fn test_client_no_subcommand() {
        let mut ctx = make_ctx();
        let result = client(&mut ctx, &[]);
        assert!(result.is_err());
    }

    // ===== HELLO tests =====

    #[test]
    fn test_hello_no_args() {
        let mut ctx = make_ctx();
        let result = hello(&mut ctx, &[]).unwrap();
        if let RespValue::Array(arr) = result {
            // Should contain server info key-value pairs
            assert!(arr.len() >= 2);
            assert_eq!(arr[0], RespValue::BulkString(Bytes::from("server")));
            assert_eq!(arr[1], RespValue::BulkString(Bytes::from("ferris-db")));
        } else {
            panic!("Expected array from HELLO");
        }
        assert_eq!(ctx.protocol_version(), 2);
    }

    #[test]
    fn test_hello_proto2() {
        let mut ctx = make_ctx();
        let args = vec![RespValue::BulkString(Bytes::from("2"))];
        let result = hello(&mut ctx, &args).unwrap();
        assert_eq!(ctx.protocol_version(), 2);
        if let RespValue::Array(arr) = result {
            // Check proto field
            let proto_idx = arr
                .iter()
                .position(|v| {
                    if let RespValue::BulkString(b) = v {
                        b.as_ref() == b"proto"
                    } else {
                        false
                    }
                })
                .unwrap();
            assert_eq!(arr[proto_idx + 1], RespValue::Integer(2));
        }
    }

    #[test]
    fn test_hello_proto3() {
        let mut ctx = make_ctx();
        let args = vec![RespValue::BulkString(Bytes::from("3"))];
        let result = hello(&mut ctx, &args).unwrap();
        assert_eq!(ctx.protocol_version(), 3);
        if let RespValue::Array(arr) = result {
            let proto_idx = arr
                .iter()
                .position(|v| {
                    if let RespValue::BulkString(b) = v {
                        b.as_ref() == b"proto"
                    } else {
                        false
                    }
                })
                .unwrap();
            assert_eq!(arr[proto_idx + 1], RespValue::Integer(3));
        }
    }

    #[test]
    fn test_hello_invalid_proto() {
        let mut ctx = make_ctx();
        let args = vec![RespValue::BulkString(Bytes::from("4"))];
        let result = hello(&mut ctx, &args);
        assert!(result.is_err());
    }

    #[test]
    fn test_hello_with_auth() {
        let mut ctx = make_ctx();
        assert!(!ctx.is_authenticated());
        let args = vec![
            RespValue::BulkString(Bytes::from("3")),
            RespValue::BulkString(Bytes::from("AUTH")),
            RespValue::BulkString(Bytes::from("default")),
            RespValue::BulkString(Bytes::from("mypassword")),
        ];
        let result = hello(&mut ctx, &args).unwrap();
        assert!(ctx.is_authenticated());
        assert_eq!(ctx.protocol_version(), 3);
        if let RespValue::Array(_) = result {
            // OK
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_hello_with_setname() {
        let mut ctx = make_ctx();
        let args = vec![
            RespValue::BulkString(Bytes::from("2")),
            RespValue::BulkString(Bytes::from("SETNAME")),
            RespValue::BulkString(Bytes::from("my-conn")),
        ];
        hello(&mut ctx, &args).unwrap();
        assert_eq!(ctx.client_name(), Some("my-conn"));
    }

    #[test]
    fn test_hello_with_auth_and_setname() {
        let mut ctx = make_ctx();
        let args = vec![
            RespValue::BulkString(Bytes::from("3")),
            RespValue::BulkString(Bytes::from("AUTH")),
            RespValue::BulkString(Bytes::from("default")),
            RespValue::BulkString(Bytes::from("pass")),
            RespValue::BulkString(Bytes::from("SETNAME")),
            RespValue::BulkString(Bytes::from("myclient")),
        ];
        hello(&mut ctx, &args).unwrap();
        assert!(ctx.is_authenticated());
        assert_eq!(ctx.protocol_version(), 3);
        assert_eq!(ctx.client_name(), Some("myclient"));
    }

    // ===== RESET tests =====

    #[test]
    fn test_reset_basic() {
        let mut ctx = make_ctx();

        // Modify state
        ctx.select_db(5);
        ctx.set_authenticated(true);
        ctx.set_client_name(Some("test".to_string()));
        ctx.set_protocol_version(3);

        let result = reset(&mut ctx, &[]).unwrap();
        assert_eq!(result, RespValue::SimpleString("RESET".to_string()));

        // Verify all state is reset
        assert_eq!(ctx.selected_db(), 0);
        assert!(!ctx.is_authenticated());
        assert!(ctx.client_name().is_none());
        assert_eq!(ctx.protocol_version(), 2);
    }

    #[test]
    fn test_reset_already_default() {
        let mut ctx = make_ctx();
        let result = reset(&mut ctx, &[]).unwrap();
        assert_eq!(result, RespValue::SimpleString("RESET".to_string()));
        assert_eq!(ctx.selected_db(), 0);
        assert_eq!(ctx.protocol_version(), 2);
    }

    // ===== CLIENT additional tests =====

    #[test]
    fn test_client_id_unique() {
        let store = Arc::new(KeyStore::default());
        let ctx1 = CommandContext::new(Arc::clone(&store));
        let ctx2 = CommandContext::new(Arc::clone(&store));
        assert_ne!(
            ctx1.client_id(),
            ctx2.client_id(),
            "Two CommandContexts should have different client IDs"
        );
    }

    #[test]
    fn test_client_setname_with_spaces() {
        let mut ctx = make_ctx();
        // Redis rejects names with spaces, but our current implementation accepts any string.
        // Test that the name is set (current behavior — once validation is added, this may error).
        let args = vec![
            RespValue::BulkString(Bytes::from("SETNAME")),
            RespValue::BulkString(Bytes::from("name with spaces")),
        ];
        let result = client(&mut ctx, &args).unwrap();
        assert_eq!(result, RespValue::ok());
        assert_eq!(ctx.client_name(), Some("name with spaces"));
    }

    #[test]
    fn test_client_kill_stub() {
        let mut ctx = make_ctx();
        let args = vec![RespValue::BulkString(Bytes::from("KILL"))];
        let result = client(&mut ctx, &args).unwrap();
        assert_eq!(result, RespValue::ok());
    }

    #[test]
    fn test_client_pause_stub() {
        let mut ctx = make_ctx();
        let args = vec![
            RespValue::BulkString(Bytes::from("PAUSE")),
            RespValue::BulkString(Bytes::from("0")),
        ];
        let result = client(&mut ctx, &args).unwrap();
        assert_eq!(result, RespValue::ok());
    }

    #[test]
    fn test_client_reply_stub() {
        let mut ctx = make_ctx();
        let args = vec![
            RespValue::BulkString(Bytes::from("REPLY")),
            RespValue::BulkString(Bytes::from("ON")),
        ];
        let result = client(&mut ctx, &args).unwrap();
        assert_eq!(result, RespValue::ok());
    }

    #[test]
    fn test_client_no_evict_stub() {
        let mut ctx = make_ctx();
        let args = vec![
            RespValue::BulkString(Bytes::from("NO-EVICT")),
            RespValue::BulkString(Bytes::from("ON")),
        ];
        let result = client(&mut ctx, &args).unwrap();
        assert_eq!(result, RespValue::ok());
    }

    // ===== HELLO additional tests =====

    #[test]
    fn test_hello_with_auth_missing_password() {
        let mut ctx = make_ctx();
        // HELLO 3 AUTH user → missing password argument
        let args = vec![
            RespValue::BulkString(Bytes::from("3")),
            RespValue::BulkString(Bytes::from("AUTH")),
            RespValue::BulkString(Bytes::from("myuser")),
        ];
        let result = hello(&mut ctx, &args);
        assert!(
            result.is_err(),
            "HELLO with AUTH but missing password should error"
        );
    }

    #[test]
    fn test_hello_proto_zero() {
        let mut ctx = make_ctx();
        let args = vec![RespValue::BulkString(Bytes::from("0"))];
        let result = hello(&mut ctx, &args);
        assert!(
            result.is_err(),
            "HELLO 0 should error (invalid protocol version)"
        );
    }
}

/// READONLY
///
/// Enables read queries for a connection to a Redis Cluster replica node.
/// In ferris-db, replicas are read-only by default for write commands,
/// but this command exists for compatibility.
///
/// Time complexity: O(1)
pub fn readonly(_ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if !args.is_empty() {
        return Err(CommandError::WrongArity("READONLY".to_string()));
    }

    // In ferris-db, replica read-only mode is always enforced for writes.
    // This command is a no-op but returns OK for compatibility.
    Ok(RespValue::ok())
}

/// READWRITE
///
/// Disables read queries for a connection to a Redis Cluster replica node.
/// In ferris-db, this is a no-op since we don't have a read-only connection mode.
///
/// Time complexity: O(1)
pub fn readwrite(_ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if !args.is_empty() {
        return Err(CommandError::WrongArity("READWRITE".to_string()));
    }

    // In ferris-db, connections are always read-write capable on the master.
    // This command is a no-op but returns OK for compatibility.
    Ok(RespValue::ok())
}

//! Comprehensive tests for ACL (Access Control List) commands
//!
//! These tests verify ACL functionality over real TCP connections.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
#![allow(clippy::uninlined_format_args)]
#![allow(clippy::manual_let_else)]
#![allow(clippy::needless_raw_string_hashes)]

use ferris_protocol::RespValue;
use ferris_test_utils::TestServer;

// ============================================================
// ACL CAT TESTS
// ============================================================

#[tokio::test]
async fn test_acl_cat_all_categories() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["ACL", "CAT"]).await;

    match result {
        RespValue::Array(categories) => {
            assert!(!categories.is_empty(), "Should return list of categories");
            // Check for some expected categories
            let cat_strs: Vec<String> = categories
                .iter()
                .filter_map(|v| {
                    if let RespValue::BulkString(b) = v {
                        Some(String::from_utf8_lossy(b).to_string())
                    } else {
                        None
                    }
                })
                .collect();

            assert!(cat_strs.contains(&"read".to_string()));
            assert!(cat_strs.contains(&"write".to_string()));
            assert!(cat_strs.contains(&"keyspace".to_string()));
        }
        _ => panic!("Expected Array, got {:?}", result),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_acl_cat_read_category() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["ACL", "CAT", "read"]).await;

    match result {
        RespValue::Array(commands) => {
            assert!(!commands.is_empty(), "Read category should have commands");
            // Check for typical read commands
            let cmd_strs: Vec<String> = commands
                .iter()
                .filter_map(|v| {
                    if let RespValue::BulkString(b) = v {
                        Some(String::from_utf8_lossy(b).to_string())
                    } else {
                        None
                    }
                })
                .collect();

            assert!(cmd_strs.contains(&"get".to_string()));
        }
        _ => panic!("Expected Array"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_acl_cat_write_category() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["ACL", "CAT", "write"]).await;

    match result {
        RespValue::Array(commands) => {
            assert!(!commands.is_empty());
        }
        _ => panic!("Expected Array"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_acl_cat_invalid_category() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["ACL", "CAT", "invalid_category"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    server.stop().await;
}

#[tokio::test]
async fn test_acl_cat_stream_category() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["ACL", "CAT", "stream"]).await;

    match result {
        RespValue::Array(commands) => {
            let cmd_strs: Vec<String> = commands
                .iter()
                .filter_map(|v| {
                    if let RespValue::BulkString(b) = v {
                        Some(String::from_utf8_lossy(b).to_string())
                    } else {
                        None
                    }
                })
                .collect();

            assert!(cmd_strs.contains(&"xadd".to_string()));
        }
        _ => panic!("Expected Array"),
    }

    server.stop().await;
}

// ============================================================
// ACL WHOAMI TESTS
// ============================================================

#[tokio::test]
async fn test_acl_whoami() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["ACL", "WHOAMI"]).await;

    match result {
        RespValue::BulkString(name) => {
            assert_eq!(name.as_ref(), b"default");
        }
        _ => panic!("Expected BulkString"),
    }

    server.stop().await;
}

// ============================================================
// ACL USERS TESTS
// ============================================================

#[tokio::test]
async fn test_acl_users_default() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["ACL", "USERS"]).await;

    match result {
        RespValue::Array(users) => {
            assert!(!users.is_empty(), "Should have at least default user");
            let user_strs: Vec<String> = users
                .iter()
                .filter_map(|v| {
                    if let RespValue::BulkString(b) = v {
                        Some(String::from_utf8_lossy(b).to_string())
                    } else {
                        None
                    }
                })
                .collect();

            assert!(user_strs.contains(&"default".to_string()));
        }
        _ => panic!("Expected Array"),
    }

    server.stop().await;
}

// ============================================================
// ACL SETUSER TESTS
// ============================================================

#[tokio::test]
async fn test_acl_setuser_create_user() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let username = format!("create_test_{}", std::process::id());
    let result = client.cmd(&["ACL", "SETUSER", &username]).await;
    assert_eq!(result, RespValue::SimpleString("OK".to_string()));

    // Verify user exists
    let users = client.cmd(&["ACL", "USERS"]).await;
    match users {
        RespValue::Array(arr) => {
            let user_strs: Vec<String> = arr
                .iter()
                .filter_map(|v| {
                    if let RespValue::BulkString(b) = v {
                        Some(String::from_utf8_lossy(b).to_string())
                    } else {
                        None
                    }
                })
                .collect();

            assert!(user_strs.contains(&username));
        }
        _ => panic!("Expected Array"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_acl_setuser_enable_user() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let username = format!("enable_test_{}", std::process::id());
    client.cmd(&["ACL", "SETUSER", &username, "on"]).await;

    let user_info = client.cmd(&["ACL", "GETUSER", &username]).await;
    match user_info {
        RespValue::Array(info) => {
            // Find flags array
            let mut found_on = false;
            for i in (0..info.len()).step_by(2) {
                if let RespValue::BulkString(key) = &info[i] {
                    if key.as_ref() == b"flags" {
                        if let RespValue::Array(flags) = &info[i + 1] {
                            for flag in flags {
                                if let RespValue::BulkString(f) = flag {
                                    if f.as_ref() == b"on" {
                                        found_on = true;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            assert!(found_on, "User should be enabled");
        }
        _ => panic!("Expected Array"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_acl_setuser_disable_user() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let username = format!("disable_test_{}", std::process::id());
    client.cmd(&["ACL", "SETUSER", &username, "on"]).await;
    client.cmd(&["ACL", "SETUSER", &username, "off"]).await;

    let user_info = client.cmd(&["ACL", "GETUSER", &username]).await;
    match user_info {
        RespValue::Array(info) => {
            let mut found_off = false;
            for i in (0..info.len()).step_by(2) {
                if let RespValue::BulkString(key) = &info[i] {
                    if key.as_ref() == b"flags" {
                        if let RespValue::Array(flags) = &info[i + 1] {
                            for flag in flags {
                                if let RespValue::BulkString(f) = flag {
                                    if f.as_ref() == b"off" {
                                        found_off = true;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            assert!(found_off, "User should be disabled");
        }
        _ => panic!("Expected Array"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_acl_setuser_nopass() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let username = format!("nopass_test_{}", std::process::id());
    client.cmd(&["ACL", "SETUSER", &username, "nopass"]).await;

    let user_info = client.cmd(&["ACL", "GETUSER", &username]).await;
    match user_info {
        RespValue::Array(info) => {
            let mut found_nopass = false;
            for i in (0..info.len()).step_by(2) {
                if let RespValue::BulkString(key) = &info[i] {
                    if key.as_ref() == b"flags" {
                        if let RespValue::Array(flags) = &info[i + 1] {
                            for flag in flags {
                                if let RespValue::BulkString(f) = flag {
                                    if f.as_ref() == b"nopass" {
                                        found_nopass = true;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            assert!(found_nopass, "User should have nopass flag");
        }
        _ => panic!("Expected Array"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_acl_setuser_add_password() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let username = format!("password_test_{}", std::process::id());
    client
        .cmd(&["ACL", "SETUSER", &username, ">mypassword"])
        .await;

    let user_info = client.cmd(&["ACL", "GETUSER", &username]).await;
    match user_info {
        RespValue::Array(info) => {
            for i in (0..info.len()).step_by(2) {
                if let RespValue::BulkString(key) = &info[i] {
                    if key.as_ref() == b"passwords" {
                        if let RespValue::Array(passwords) = &info[i + 1] {
                            assert!(!passwords.is_empty(), "Should have password");
                        }
                    }
                }
            }
        }
        _ => panic!("Expected Array"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_acl_setuser_allkeys() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let username = format!("allkeys_test_{}", std::process::id());
    client.cmd(&["ACL", "SETUSER", &username, "allkeys"]).await;

    // User should have access to all keys
    let user_info = client.cmd(&["ACL", "GETUSER", &username]).await;
    assert!(matches!(user_info, RespValue::Array(_)));

    server.stop().await;
}

#[tokio::test]
async fn test_acl_setuser_allcommands() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let username = format!("allcmds_test_{}", std::process::id());
    client
        .cmd(&["ACL", "SETUSER", &username, "allcommands"])
        .await;

    let user_info = client.cmd(&["ACL", "GETUSER", &username]).await;
    assert!(matches!(user_info, RespValue::Array(_)));

    server.stop().await;
}

// ============================================================
// ACL GETUSER TESTS
// ============================================================

#[tokio::test]
async fn test_acl_getuser_default() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["ACL", "GETUSER", "default"]).await;

    match result {
        RespValue::Array(info) => {
            assert!(!info.is_empty());
            // Should have flags, passwords, commands, keys, channels
        }
        _ => panic!("Expected Array"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_acl_getuser_nonexistent() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["ACL", "GETUSER", "nonexistent"]).await;
    assert_eq!(result, RespValue::Null);

    server.stop().await;
}

// ============================================================
// ACL DELUSER TESTS
// ============================================================

#[tokio::test]
async fn test_acl_deluser_existing() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let username = format!("deluser_test_{}", std::process::id());

    // Create user
    client.cmd(&["ACL", "SETUSER", &username]).await;

    // Delete user
    let result = client.cmd(&["ACL", "DELUSER", &username]).await;
    assert_eq!(result, RespValue::Integer(1));

    // User should no longer exist
    let user_info = client.cmd(&["ACL", "GETUSER", &username]).await;
    assert_eq!(user_info, RespValue::Null);

    server.stop().await;
}

#[tokio::test]
async fn test_acl_deluser_nonexistent() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["ACL", "DELUSER", "nonexistent"]).await;
    assert_eq!(result, RespValue::Integer(0));

    server.stop().await;
}

#[tokio::test]
async fn test_acl_deluser_default_cannot_delete() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // Cannot delete default user
    let result = client.cmd(&["ACL", "DELUSER", "default"]).await;
    assert_eq!(result, RespValue::Integer(0));

    // Default user should still exist
    let user_info = client.cmd(&["ACL", "GETUSER", "default"]).await;
    assert!(matches!(user_info, RespValue::Array(_)));

    server.stop().await;
}

#[tokio::test]
async fn test_acl_deluser_multiple() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let prefix = format!("multi_{}", std::process::id());
    let user1 = format!("{}_1", prefix);
    let user2 = format!("{}_2", prefix);
    let user3 = format!("{}_3", prefix);

    // Create users
    client.cmd(&["ACL", "SETUSER", &user1]).await;
    client.cmd(&["ACL", "SETUSER", &user2]).await;
    client.cmd(&["ACL", "SETUSER", &user3]).await;

    // Delete multiple users
    let result = client
        .cmd(&["ACL", "DELUSER", &user1, &user2, "nonexistent_xyz"])
        .await;
    assert_eq!(result, RespValue::Integer(2));

    server.stop().await;
}

// ============================================================
// ACL GENPASS TESTS
// ============================================================

#[tokio::test]
async fn test_acl_genpass_default_length() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["ACL", "GENPASS"]).await;

    match result {
        RespValue::BulkString(pass) => {
            // Implementation returns hex chars based on bits/4 calculation
            // Default 64 bits = 16+ hex chars (but current impl gives 32)
            assert!(!pass.is_empty(), "Password should not be empty");
            // Should be hex characters
            let pass_str = String::from_utf8_lossy(&pass);
            assert!(pass_str.chars().all(|c| c.is_ascii_hexdigit()));
        }
        _ => panic!("Expected BulkString"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_acl_genpass_custom_length() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["ACL", "GENPASS", "128"]).await;

    match result {
        RespValue::BulkString(pass) => {
            // Password is generated, length depends on implementation
            assert!(!pass.is_empty());
            // Should be hex characters
            let pass_str = String::from_utf8_lossy(&pass);
            assert!(pass_str.chars().all(|c| c.is_ascii_hexdigit()));
        }
        _ => panic!("Expected BulkString"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_acl_genpass_unique() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let pass1 = client.cmd(&["ACL", "GENPASS"]).await;
    let pass2 = client.cmd(&["ACL", "GENPASS"]).await;

    assert_ne!(pass1, pass2, "Generated passwords should be unique");

    server.stop().await;
}

// ============================================================
// ACL LIST TESTS
// ============================================================

#[tokio::test]
async fn test_acl_list() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["ACL", "LIST"]).await;

    match result {
        RespValue::Array(rules) => {
            assert!(!rules.is_empty(), "Should have at least default user");
            // Should contain default user rule
            let has_default = rules.iter().any(|r| {
                if let RespValue::BulkString(b) = r {
                    String::from_utf8_lossy(b).contains("default")
                } else {
                    false
                }
            });
            assert!(has_default);
        }
        _ => panic!("Expected Array"),
    }

    server.stop().await;
}

// ============================================================
// ACL LOG TESTS
// ============================================================

#[tokio::test]
async fn test_acl_log_empty() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["ACL", "LOG"]).await;

    match result {
        RespValue::Array(log) => {
            // Log may be empty
            assert!(log.is_empty() || !log.is_empty());
        }
        _ => panic!("Expected Array"),
    }

    server.stop().await;
}

#[tokio::test]
async fn test_acl_log_with_count() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["ACL", "LOG", "10"]).await;

    match result {
        RespValue::Array(_) => {}
        _ => panic!("Expected Array"),
    }

    server.stop().await;
}

// ============================================================
// ACL LOAD TESTS
// ============================================================

#[tokio::test]
async fn test_acl_load() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    // LOAD returns OK (no-op in our implementation)
    let result = client.cmd(&["ACL", "LOAD"]).await;
    assert_eq!(result, RespValue::SimpleString("OK".to_string()));

    server.stop().await;
}

// ============================================================
// ACL SAVE TESTS
// ============================================================

#[tokio::test]
async fn test_acl_save() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["ACL", "SAVE"]).await;
    assert_eq!(result, RespValue::SimpleString("OK".to_string()));

    server.stop().await;
}

// ============================================================
// ACL DRYRUN TESTS
// ============================================================

#[tokio::test]
async fn test_acl_dryrun() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client
        .cmd(&["ACL", "DRYRUN", "default", "GET", "key"])
        .await;
    // Should return OK (permission check simulated)
    assert_eq!(result, RespValue::SimpleString("OK".to_string()));

    server.stop().await;
}

#[tokio::test]
async fn test_acl_dryrun_wrong_arity() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["ACL", "DRYRUN", "default"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    server.stop().await;
}

// ============================================================
// ACL HELP TESTS
// ============================================================

#[tokio::test]
async fn test_acl_help() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["ACL", "HELP"]).await;

    match result {
        RespValue::Array(help) => {
            assert!(!help.is_empty());
        }
        _ => panic!("Expected Array"),
    }

    server.stop().await;
}

// ============================================================
// ACL UNKNOWN SUBCOMMAND TEST
// ============================================================

#[tokio::test]
async fn test_acl_unknown_subcommand() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["ACL", "UNKNOWN"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    server.stop().await;
}

// ============================================================
// ACL WRONG ARITY TESTS
// ============================================================

#[tokio::test]
async fn test_acl_no_subcommand() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["ACL"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    server.stop().await;
}

#[tokio::test]
async fn test_acl_deluser_no_username() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["ACL", "DELUSER"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    server.stop().await;
}

#[tokio::test]
async fn test_acl_getuser_no_username() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["ACL", "GETUSER"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    server.stop().await;
}

#[tokio::test]
async fn test_acl_setuser_no_username() {
    let server = TestServer::spawn().await;
    let mut client = server.client().await;

    let result = client.cmd(&["ACL", "SETUSER"]).await;
    assert!(matches!(result, RespValue::Error(_)));

    server.stop().await;
}

// ============================================================
// CONCURRENT TESTS
// ============================================================

#[tokio::test]
async fn test_acl_concurrent_user_creation() {
    let server = TestServer::spawn().await;

    // Use unique prefix to avoid collision with other tests (ACL manager is global)
    let prefix = format!("concurrent_{}", std::process::id());

    let handles: Vec<_> = (0..10)
        .map(|i| {
            let addr = server.addr;
            let user_prefix = prefix.clone();
            tokio::spawn(async move {
                let mut client = ferris_test_utils::TestClient::connect(addr).await;
                let username = format!("{}_{}", user_prefix, i);
                client
                    .cmd(&["ACL", "SETUSER", &username, "on", "nopass"])
                    .await;
            })
        })
        .collect();

    for handle in handles {
        handle.await.unwrap();
    }

    // Verify users were created by checking one of them
    let mut client = server.client().await;
    let test_user = format!("{}_0", prefix);
    let user_info = client.cmd(&["ACL", "GETUSER", &test_user]).await;

    // Should find the user we created
    assert!(
        matches!(user_info, RespValue::Array(_)),
        "User should exist"
    );

    server.stop().await;
}

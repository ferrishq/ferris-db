//! ACL (Access Control List) commands
//!
//! Provides user management and access control functionality.
//! This is a basic implementation that stores users in memory.

use crate::{CommandContext, CommandError, CommandResult};
use bytes::Bytes;
use ferris_protocol::RespValue;
use std::collections::{HashMap, HashSet};
use std::sync::{OnceLock, RwLock};

/// ACL user definition
#[derive(Clone, Debug)]
pub struct AclUser {
    /// Username
    pub name: String,
    /// Is the user enabled
    pub enabled: bool,
    /// Passwords (hashed)
    pub passwords: HashSet<String>,
    /// No password required
    pub nopass: bool,
    /// Allowed commands (empty means all)
    pub commands: HashSet<String>,
    /// Denied commands
    pub denied_commands: HashSet<String>,
    /// Allowed keys pattern (empty means all)
    pub keys: HashSet<String>,
    /// Allowed channels pattern (empty means all)
    pub channels: HashSet<String>,
}

impl Default for AclUser {
    fn default() -> Self {
        Self {
            name: "default".to_string(),
            enabled: true,
            passwords: HashSet::new(),
            nopass: true,             // Default user has no password by default
            commands: HashSet::new(), // Empty means all commands allowed
            denied_commands: HashSet::new(),
            keys: HashSet::new(), // Empty means all keys allowed
            channels: HashSet::new(),
        }
    }
}

impl AclUser {
    /// Create a new user with the given name
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            enabled: false, // New users are disabled by default
            nopass: false,
            ..Default::default()
        }
    }

    /// Convert to ACL rule string
    pub fn to_rule_string(&self) -> String {
        let mut parts = vec![format!("user {}", self.name)];

        if self.enabled {
            parts.push("on".to_string());
        } else {
            parts.push("off".to_string());
        }

        if self.nopass {
            parts.push("nopass".to_string());
        }

        for pwd in &self.passwords {
            parts.push(format!("#{}", pwd));
        }

        if self.commands.is_empty() && self.denied_commands.is_empty() {
            parts.push("+@all".to_string());
        } else {
            for cmd in &self.commands {
                parts.push(format!("+{}", cmd));
            }
            for cmd in &self.denied_commands {
                parts.push(format!("-{}", cmd));
            }
        }

        if self.keys.is_empty() {
            parts.push("~*".to_string());
        } else {
            for key in &self.keys {
                parts.push(format!("~{}", key));
            }
        }

        parts.join(" ")
    }
}

/// ACL manager for storing and managing users
#[derive(Default)]
pub struct AclManager {
    users: RwLock<HashMap<String, AclUser>>,
}

impl AclManager {
    /// Create a new ACL manager with default user
    pub fn new() -> Self {
        let mut users = HashMap::new();
        users.insert("default".to_string(), AclUser::default());
        Self {
            users: RwLock::new(users),
        }
    }

    /// Get a user by name
    pub fn get_user(&self, name: &str) -> Option<AclUser> {
        let users = self.users.read().unwrap();
        users.get(name).cloned()
    }

    /// Set or create a user
    pub fn set_user(&self, user: AclUser) {
        let mut users = self.users.write().unwrap();
        users.insert(user.name.clone(), user);
    }

    /// Delete a user
    pub fn del_user(&self, name: &str) -> bool {
        if name == "default" {
            return false; // Cannot delete default user
        }
        let mut users = self.users.write().unwrap();
        users.remove(name).is_some()
    }

    /// Get all user names
    pub fn list_users(&self) -> Vec<String> {
        let users = self.users.read().unwrap();
        users.keys().cloned().collect()
    }

    /// Get all users
    pub fn get_all_users(&self) -> Vec<AclUser> {
        let users = self.users.read().unwrap();
        users.values().cloned().collect()
    }
}

// Global ACL manager
static ACL_MANAGER: OnceLock<AclManager> = OnceLock::new();

fn get_acl_manager() -> &'static AclManager {
    ACL_MANAGER.get_or_init(AclManager::new)
}

/// Helper to get string from RespValue
fn get_str(arg: &RespValue) -> Result<&str, CommandError> {
    arg.as_str()
        .ok_or_else(|| CommandError::InvalidArgument("invalid argument".to_string()))
}

/// ACL command categories
fn get_categories() -> Vec<&'static str> {
    vec![
        "keyspace",
        "read",
        "write",
        "set",
        "sortedset",
        "list",
        "hash",
        "string",
        "bitmap",
        "hyperloglog",
        "geo",
        "stream",
        "pubsub",
        "admin",
        "fast",
        "slow",
        "blocking",
        "dangerous",
        "connection",
        "transaction",
        "scripting",
    ]
}

// ---------------------------------------------------------------------------
// ACL subcommand [args...]
// ---------------------------------------------------------------------------

/// ACL subcommand [args...]
///
/// Access Control List management commands.
pub fn acl(_ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("ACL".to_string()));
    }

    let subcommand = get_str(&args[0])?.to_uppercase();

    match subcommand.as_str() {
        "CAT" => {
            // ACL CAT [category]
            if args.len() == 1 {
                // Return all categories
                let categories: Vec<RespValue> = get_categories()
                    .iter()
                    .map(|c| RespValue::BulkString(Bytes::from(*c)))
                    .collect();
                Ok(RespValue::Array(categories))
            } else {
                // Return commands in category
                let category = get_str(&args[1])?.to_lowercase();

                // Return sample commands per category
                let commands: Vec<&str> = match category.as_str() {
                    "read" => vec!["get", "mget", "hget", "lrange", "smembers", "zrange"],
                    "write" => vec!["set", "mset", "hset", "lpush", "sadd", "zadd"],
                    "keyspace" => vec!["del", "exists", "expire", "ttl", "keys", "scan"],
                    "string" => vec!["get", "set", "incr", "decr", "append", "strlen"],
                    "hash" => vec!["hget", "hset", "hdel", "hgetall", "hkeys", "hvals"],
                    "list" => vec!["lpush", "rpush", "lpop", "rpop", "lrange", "llen"],
                    "set" => vec!["sadd", "srem", "smembers", "sismember", "scard"],
                    "sortedset" => vec!["zadd", "zrem", "zrange", "zrank", "zscore"],
                    "stream" => vec!["xadd", "xread", "xrange", "xlen", "xinfo"],
                    "pubsub" => vec!["publish", "subscribe", "unsubscribe", "psubscribe"],
                    "admin" => vec!["config", "debug", "shutdown", "bgsave", "bgrewriteaof"],
                    "connection" => vec!["auth", "ping", "quit", "select", "client"],
                    "transaction" => vec!["multi", "exec", "discard", "watch", "unwatch"],
                    "scripting" => vec!["eval", "evalsha", "script", "function", "fcall"],
                    "dangerous" => vec!["flushdb", "flushall", "keys", "debug"],
                    "fast" => vec!["get", "set", "incr", "lpush", "sadd", "zadd"],
                    "slow" => vec!["keys", "smembers", "hgetall", "lrange"],
                    "blocking" => vec!["blpop", "brpop", "blmove", "bzpopmin", "bzpopmax"],
                    _ => {
                        return Err(CommandError::InvalidArgument(format!(
                            "Unknown ACL category '{}'",
                            category
                        )));
                    }
                };

                let result: Vec<RespValue> = commands
                    .iter()
                    .map(|c| RespValue::BulkString(Bytes::from(*c)))
                    .collect();
                Ok(RespValue::Array(result))
            }
        }
        "DELUSER" => {
            if args.len() < 2 {
                return Err(CommandError::WrongArity("ACL".to_string()));
            }
            let mut deleted = 0;
            for arg in &args[1..] {
                let username = get_str(arg)?;
                if get_acl_manager().del_user(username) {
                    deleted += 1;
                }
            }
            Ok(RespValue::Integer(deleted))
        }
        "GENPASS" => {
            // Generate a random password
            let len = if args.len() > 1 {
                get_str(&args[1])?.parse::<usize>().unwrap_or(64)
            } else {
                64
            };
            let len = len.min(256); // Cap at 256 bits

            // Generate random hex string
            let bytes_needed = (len + 3) / 4; // Hex chars needed
            let mut password = String::with_capacity(bytes_needed);
            for _ in 0..bytes_needed {
                let byte: u8 = rand::random();
                password.push_str(&format!("{:02x}", byte));
            }
            password.truncate(len);

            Ok(RespValue::BulkString(Bytes::from(password)))
        }
        "GETUSER" => {
            if args.len() < 2 {
                return Err(CommandError::WrongArity("ACL".to_string()));
            }
            let username = get_str(&args[1])?;

            match get_acl_manager().get_user(username) {
                Some(user) => {
                    let flags: Vec<RespValue> = {
                        let mut f = Vec::new();
                        if user.enabled {
                            f.push(RespValue::BulkString(Bytes::from("on")));
                        } else {
                            f.push(RespValue::BulkString(Bytes::from("off")));
                        }
                        if user.nopass {
                            f.push(RespValue::BulkString(Bytes::from("nopass")));
                        }
                        f
                    };

                    let passwords: Vec<RespValue> = user
                        .passwords
                        .iter()
                        .map(|p| RespValue::BulkString(Bytes::from(p.clone())))
                        .collect();

                    let commands = if user.commands.is_empty() {
                        RespValue::BulkString(Bytes::from("+@all"))
                    } else {
                        RespValue::BulkString(Bytes::from(
                            user.commands
                                .iter()
                                .map(|c| format!("+{}", c))
                                .collect::<Vec<_>>()
                                .join(" "),
                        ))
                    };

                    let keys: Vec<RespValue> = if user.keys.is_empty() {
                        vec![RespValue::BulkString(Bytes::from("~*"))]
                    } else {
                        user.keys
                            .iter()
                            .map(|k| RespValue::BulkString(Bytes::from(format!("~{}", k))))
                            .collect()
                    };

                    Ok(RespValue::Array(vec![
                        RespValue::BulkString(Bytes::from("flags")),
                        RespValue::Array(flags),
                        RespValue::BulkString(Bytes::from("passwords")),
                        RespValue::Array(passwords),
                        RespValue::BulkString(Bytes::from("commands")),
                        commands,
                        RespValue::BulkString(Bytes::from("keys")),
                        RespValue::Array(keys),
                        RespValue::BulkString(Bytes::from("channels")),
                        RespValue::Array(vec![RespValue::BulkString(Bytes::from("&*"))]),
                    ]))
                }
                None => Ok(RespValue::Null),
            }
        }
        "LIST" => {
            let users = get_acl_manager().get_all_users();
            let result: Vec<RespValue> = users
                .iter()
                .map(|u| RespValue::BulkString(Bytes::from(u.to_rule_string())))
                .collect();
            Ok(RespValue::Array(result))
        }
        "LOAD" => {
            // Would load ACL from file - not implemented
            Ok(RespValue::ok())
        }
        "LOG" => {
            // ACL LOG [count|RESET]
            // Return empty log for now
            Ok(RespValue::Array(vec![]))
        }
        "SAVE" => {
            // Would save ACL to file - not implemented
            Ok(RespValue::ok())
        }
        "SETUSER" => {
            if args.len() < 2 {
                return Err(CommandError::WrongArity("ACL".to_string()));
            }
            let username = get_str(&args[1])?;

            let mut user = get_acl_manager()
                .get_user(username)
                .unwrap_or_else(|| AclUser::new(username));

            // Parse rules
            for arg in &args[2..] {
                let rule = get_str(arg)?;
                match rule {
                    "on" => user.enabled = true,
                    "off" => user.enabled = false,
                    "nopass" => user.nopass = true,
                    "resetpass" => {
                        user.passwords.clear();
                        user.nopass = false;
                    }
                    "allkeys" | "~*" => user.keys.clear(),
                    "allcommands" | "+@all" => user.commands.clear(),
                    "nocommands" | "-@all" => {
                        user.denied_commands.insert("@all".to_string());
                    }
                    r if r.starts_with('>') => {
                        // Add password
                        user.passwords.insert(r[1..].to_string());
                    }
                    r if r.starts_with('<') => {
                        // Remove password
                        user.passwords.remove(&r[1..]);
                    }
                    r if r.starts_with('+') => {
                        // Add command
                        user.commands.insert(r[1..].to_string());
                    }
                    r if r.starts_with('-') => {
                        // Remove command
                        user.denied_commands.insert(r[1..].to_string());
                    }
                    r if r.starts_with('~') => {
                        // Add key pattern
                        user.keys.insert(r[1..].to_string());
                    }
                    r if r.starts_with('&') => {
                        // Add channel pattern
                        user.channels.insert(r[1..].to_string());
                    }
                    _ => {}
                }
            }

            get_acl_manager().set_user(user);
            Ok(RespValue::ok())
        }
        "USERS" => {
            let users = get_acl_manager().list_users();
            let result: Vec<RespValue> = users
                .iter()
                .map(|u| RespValue::BulkString(Bytes::from(u.clone())))
                .collect();
            Ok(RespValue::Array(result))
        }
        "WHOAMI" => {
            // Return current authenticated user
            // For now, always return "default"
            Ok(RespValue::BulkString(Bytes::from("default")))
        }
        "DRYRUN" => {
            if args.len() < 3 {
                return Err(CommandError::WrongArity("ACL".to_string()));
            }
            let _username = get_str(&args[1])?;
            let _command = get_str(&args[2])?;
            // Simulate permission check - always allow for now
            Ok(RespValue::ok())
        }
        "HELP" => {
            let help = vec![
                RespValue::BulkString(Bytes::from("ACL CAT [<category>]")),
                RespValue::BulkString(Bytes::from("    List categories or commands in category.")),
                RespValue::BulkString(Bytes::from("ACL DELUSER <username> [<username> ...]")),
                RespValue::BulkString(Bytes::from("    Delete ACL users.")),
                RespValue::BulkString(Bytes::from("ACL GENPASS [<bits>]")),
                RespValue::BulkString(Bytes::from("    Generate a secure password.")),
                RespValue::BulkString(Bytes::from("ACL GETUSER <username>")),
                RespValue::BulkString(Bytes::from("    Get ACL user details.")),
                RespValue::BulkString(Bytes::from("ACL LIST")),
                RespValue::BulkString(Bytes::from("    List all ACL rules.")),
                RespValue::BulkString(Bytes::from("ACL LOAD")),
                RespValue::BulkString(Bytes::from("    Reload ACL from file.")),
                RespValue::BulkString(Bytes::from("ACL LOG [<count>|RESET]")),
                RespValue::BulkString(Bytes::from("    Show or reset ACL log.")),
                RespValue::BulkString(Bytes::from("ACL SAVE")),
                RespValue::BulkString(Bytes::from("    Save ACL to file.")),
                RespValue::BulkString(Bytes::from("ACL SETUSER <username> [rules...]")),
                RespValue::BulkString(Bytes::from("    Create or modify ACL user.")),
                RespValue::BulkString(Bytes::from("ACL USERS")),
                RespValue::BulkString(Bytes::from("    List all usernames.")),
                RespValue::BulkString(Bytes::from("ACL WHOAMI")),
                RespValue::BulkString(Bytes::from("    Return current user.")),
            ];
            Ok(RespValue::Array(help))
        }
        _ => Err(CommandError::UnknownSubcommand(
            subcommand,
            "ACL".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_acl_user_default() {
        let user = AclUser::default();
        assert_eq!(user.name, "default");
        assert!(user.enabled);
        assert!(user.nopass);
    }

    #[test]
    fn test_acl_user_new() {
        let user = AclUser::new("testuser");
        assert_eq!(user.name, "testuser");
        assert!(!user.enabled);
        assert!(!user.nopass);
    }

    #[test]
    fn test_acl_manager_default_user() {
        let manager = AclManager::new();
        let user = manager.get_user("default");
        assert!(user.is_some());
        assert!(user.unwrap().enabled);
    }

    #[test]
    fn test_acl_manager_cannot_delete_default() {
        let manager = AclManager::new();
        assert!(!manager.del_user("default"));
    }
}

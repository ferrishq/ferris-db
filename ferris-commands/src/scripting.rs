//! Scripting commands: EVAL, EVALSHA, EVALSHA_RO, EVAL_RO, SCRIPT, FCALL, FCALL_RO, FUNCTION
//!
//! Note: Full Lua scripting support requires embedding a Lua interpreter.
//! This module provides the command interface and basic script caching.
//! For now, scripts are stored but EVAL returns an error indicating Lua is not available.

use crate::{CommandContext, CommandError, CommandResult};
use bytes::Bytes;
use ferris_protocol::RespValue;
use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};

/// Script cache for storing loaded scripts
#[derive(Default)]
pub struct ScriptCache {
    /// SHA1 hash -> script source
    scripts: RwLock<HashMap<String, String>>,
}

impl ScriptCache {
    /// Create a new script cache
    #[must_use]
    pub fn new() -> Self {
        Self {
            scripts: RwLock::new(HashMap::new()),
        }
    }

    /// Load a script and return its SHA1 hash
    pub fn load(&self, script: &str) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        // Use a simple hash for now - in production, use actual SHA1
        let mut hasher = DefaultHasher::new();
        script.hash(&mut hasher);
        let hash = format!("{:016x}", hasher.finish());

        let mut scripts = self.scripts.write().unwrap();
        scripts.insert(hash.clone(), script.to_string());
        hash
    }

    /// Check if a script exists
    pub fn exists(&self, sha: &str) -> bool {
        let scripts = self.scripts.read().unwrap();
        scripts.contains_key(sha)
    }

    /// Get a script by SHA
    pub fn get(&self, sha: &str) -> Option<String> {
        let scripts = self.scripts.read().unwrap();
        scripts.get(sha).cloned()
    }

    /// Flush all scripts
    pub fn flush(&self) {
        let mut scripts = self.scripts.write().unwrap();
        scripts.clear();
    }

    /// Get number of cached scripts
    pub fn len(&self) -> usize {
        let scripts = self.scripts.read().unwrap();
        scripts.len()
    }

    /// Check if cache is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// Global script cache using OnceLock
static SCRIPT_CACHE: OnceLock<ScriptCache> = OnceLock::new();

fn get_script_cache() -> &'static ScriptCache {
    SCRIPT_CACHE.get_or_init(ScriptCache::new)
}

/// Helper to get bytes from RespValue
fn get_bytes(arg: &RespValue) -> Result<Bytes, CommandError> {
    arg.as_bytes()
        .cloned()
        .or_else(|| arg.as_str().map(|s| Bytes::from(s.to_owned())))
        .ok_or_else(|| CommandError::InvalidArgument("invalid argument".to_string()))
}

/// Helper to get string from RespValue
fn get_str(arg: &RespValue) -> Result<&str, CommandError> {
    arg.as_str()
        .ok_or_else(|| CommandError::InvalidArgument("invalid argument".to_string()))
}

/// Helper to parse an integer from RespValue
fn parse_int(arg: &RespValue) -> Result<i64, CommandError> {
    let s = arg.as_str().ok_or(CommandError::NotAnInteger)?;
    s.parse().map_err(|_| CommandError::NotAnInteger)
}

// ---------------------------------------------------------------------------
// EVAL script numkeys [key [key ...]] [arg [arg ...]]
// ---------------------------------------------------------------------------

/// EVAL script numkeys [key [key ...]] [arg [arg ...]]
///
/// Evaluates a Lua script server-side.
///
/// Note: Lua execution is not yet implemented. This command will cache the script
/// and return an error indicating Lua is not available.
pub fn eval(_ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("EVAL".to_string()));
    }

    let script = get_str(&args[0])?;
    let numkeys = parse_int(&args[1])? as usize;

    // Validate we have enough args for keys
    if args.len() < 2 + numkeys {
        return Err(CommandError::InvalidArgument(
            "Number of keys can't be greater than number of args".to_string(),
        ));
    }

    // Cache the script
    let _sha = get_script_cache().load(script);

    // Extract keys and args for future use
    let _keys: Vec<Bytes> = args[2..2 + numkeys]
        .iter()
        .map(get_bytes)
        .collect::<Result<_, _>>()?;

    let _script_args: Vec<Bytes> = args[2 + numkeys..]
        .iter()
        .map(get_bytes)
        .collect::<Result<_, _>>()?;

    // For now, return an error indicating Lua is not available
    // In a full implementation, we would execute the script here
    Err(CommandError::InvalidArgument(
        "NOSCRIPT Lua scripting is not available in this build".to_string(),
    ))
}

// ---------------------------------------------------------------------------
// EVAL_RO script numkeys [key [key ...]] [arg [arg ...]]
// ---------------------------------------------------------------------------

/// EVAL_RO script numkeys [key [key ...]] [arg [arg ...]]
///
/// Read-only variant of EVAL.
pub fn eval_ro(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    // Same as EVAL but marked as read-only
    eval(ctx, args)
}

// ---------------------------------------------------------------------------
// EVALSHA sha1 numkeys [key [key ...]] [arg [arg ...]]
// ---------------------------------------------------------------------------

/// EVALSHA sha1 numkeys [key [key ...]] [arg [arg ...]]
///
/// Evaluates a cached Lua script by its SHA1 hash.
pub fn evalsha(_ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("EVALSHA".to_string()));
    }

    let sha = get_str(&args[0])?;
    let numkeys = parse_int(&args[1])? as usize;

    // Check if script exists
    if !get_script_cache().exists(sha) {
        return Err(CommandError::InvalidArgument(format!(
            "NOSCRIPT No matching script. Please use EVAL. SHA: {}",
            sha
        )));
    }

    // Validate we have enough args for keys
    if args.len() < 2 + numkeys {
        return Err(CommandError::InvalidArgument(
            "Number of keys can't be greater than number of args".to_string(),
        ));
    }

    // For now, return error indicating Lua is not available
    Err(CommandError::InvalidArgument(
        "NOSCRIPT Lua scripting is not available in this build".to_string(),
    ))
}

// ---------------------------------------------------------------------------
// EVALSHA_RO sha1 numkeys [key [key ...]] [arg [arg ...]]
// ---------------------------------------------------------------------------

/// EVALSHA_RO sha1 numkeys [key [key ...]] [arg [arg ...]]
///
/// Read-only variant of EVALSHA.
pub fn evalsha_ro(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    evalsha(ctx, args)
}

// ---------------------------------------------------------------------------
// SCRIPT subcommand [args...]
// ---------------------------------------------------------------------------

/// SCRIPT subcommand [args...]
///
/// Script management commands.
pub fn script(_ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("SCRIPT".to_string()));
    }

    let subcommand = get_str(&args[0])?.to_uppercase();

    match subcommand.as_str() {
        "LOAD" => {
            if args.len() < 2 {
                return Err(CommandError::WrongArity("SCRIPT".to_string()));
            }
            let script = get_str(&args[1])?;
            let sha = get_script_cache().load(script);
            Ok(RespValue::BulkString(Bytes::from(sha)))
        }
        "EXISTS" => {
            let results: Vec<RespValue> = args[1..]
                .iter()
                .map(|arg| {
                    let sha = get_str(arg).unwrap_or("");
                    RespValue::Integer(if get_script_cache().exists(sha) { 1 } else { 0 })
                })
                .collect();
            Ok(RespValue::Array(results))
        }
        "FLUSH" => {
            // Parse optional ASYNC/SYNC flag
            let _async = if args.len() > 1 {
                let flag = get_str(&args[1])?.to_uppercase();
                flag == "ASYNC"
            } else {
                false
            };
            get_script_cache().flush();
            Ok(RespValue::ok())
        }
        "KILL" => {
            // No script is currently running, so this is a no-op
            Err(CommandError::InvalidArgument(
                "NOTBUSY No scripts in execution right now.".to_string(),
            ))
        }
        "DEBUG" => {
            if args.len() < 2 {
                return Err(CommandError::WrongArity("SCRIPT".to_string()));
            }
            let mode = get_str(&args[1])?.to_uppercase();
            match mode.as_str() {
                "YES" | "SYNC" | "NO" => Ok(RespValue::ok()),
                _ => Err(CommandError::SyntaxError),
            }
        }
        "HELP" => {
            let help = vec![
                RespValue::BulkString(Bytes::from("SCRIPT LOAD <script>")),
                RespValue::BulkString(Bytes::from("    Load a script into the scripts cache.")),
                RespValue::BulkString(Bytes::from("SCRIPT EXISTS <sha1> [<sha1> ...]")),
                RespValue::BulkString(Bytes::from("    Check if scripts exist in the cache.")),
                RespValue::BulkString(Bytes::from("SCRIPT FLUSH [ASYNC|SYNC]")),
                RespValue::BulkString(Bytes::from("    Flush the scripts cache.")),
                RespValue::BulkString(Bytes::from("SCRIPT KILL")),
                RespValue::BulkString(Bytes::from("    Kill the currently executing script.")),
                RespValue::BulkString(Bytes::from("SCRIPT DEBUG <YES|SYNC|NO>")),
                RespValue::BulkString(Bytes::from("    Set script debug mode.")),
            ];
            Ok(RespValue::Array(help))
        }
        _ => Err(CommandError::UnknownSubcommand(
            subcommand,
            "SCRIPT".to_string(),
        )),
    }
}

// ---------------------------------------------------------------------------
// FCALL function numkeys [key [key ...]] [arg [arg ...]]
// ---------------------------------------------------------------------------

/// FCALL function numkeys [key [key ...]] [arg [arg ...]]
///
/// Calls a Redis function.
///
/// Note: Redis Functions are not yet implemented.
pub fn fcall(_ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("FCALL".to_string()));
    }

    let function_name = get_str(&args[0])?;

    Err(CommandError::InvalidArgument(format!(
        "ERR Function not found: {}",
        function_name
    )))
}

// ---------------------------------------------------------------------------
// FCALL_RO function numkeys [key [key ...]] [arg [arg ...]]
// ---------------------------------------------------------------------------

/// FCALL_RO function numkeys [key [key ...]] [arg [arg ...]]
///
/// Read-only variant of FCALL.
pub fn fcall_ro(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    fcall(ctx, args)
}

// ---------------------------------------------------------------------------
// FUNCTION subcommand [args...]
// ---------------------------------------------------------------------------

/// FUNCTION subcommand [args...]
///
/// Function management commands.
pub fn function(_ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("FUNCTION".to_string()));
    }

    let subcommand = get_str(&args[0])?.to_uppercase();

    match subcommand.as_str() {
        "LOAD" => {
            // Would load a function library
            Err(CommandError::InvalidArgument(
                "ERR Functions are not supported in this build".to_string(),
            ))
        }
        "DELETE" => {
            if args.len() < 2 {
                return Err(CommandError::WrongArity("FUNCTION".to_string()));
            }
            let library_name = get_str(&args[1])?;
            Err(CommandError::InvalidArgument(format!(
                "ERR Library not found: {}",
                library_name
            )))
        }
        "FLUSH" => Ok(RespValue::ok()),
        "KILL" => Err(CommandError::InvalidArgument(
            "NOTBUSY No scripts in execution right now.".to_string(),
        )),
        "LIST" => {
            // Return empty list
            Ok(RespValue::Array(vec![]))
        }
        "STATS" => Ok(RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("running_script")),
            RespValue::Integer(0),
            RespValue::BulkString(Bytes::from("engines")),
            RespValue::Array(vec![]),
        ])),
        "DUMP" => {
            // Return serialized functions (empty for now)
            Ok(RespValue::Null)
        }
        "RESTORE" => Err(CommandError::InvalidArgument(
            "ERR Functions are not supported in this build".to_string(),
        )),
        "HELP" => {
            let help = vec![
                RespValue::BulkString(Bytes::from("FUNCTION LOAD <code>")),
                RespValue::BulkString(Bytes::from("    Load a library of functions.")),
                RespValue::BulkString(Bytes::from("FUNCTION DELETE <library-name>")),
                RespValue::BulkString(Bytes::from("    Delete a library.")),
                RespValue::BulkString(Bytes::from("FUNCTION FLUSH [ASYNC|SYNC]")),
                RespValue::BulkString(Bytes::from("    Delete all libraries.")),
                RespValue::BulkString(Bytes::from("FUNCTION KILL")),
                RespValue::BulkString(Bytes::from("    Kill the running function.")),
                RespValue::BulkString(Bytes::from(
                    "FUNCTION LIST [LIBRARYNAME pattern] [WITHCODE]",
                )),
                RespValue::BulkString(Bytes::from("    List all libraries.")),
                RespValue::BulkString(Bytes::from("FUNCTION STATS")),
                RespValue::BulkString(Bytes::from("    Return function statistics.")),
                RespValue::BulkString(Bytes::from("FUNCTION DUMP")),
                RespValue::BulkString(Bytes::from("    Dump all libraries.")),
                RespValue::BulkString(Bytes::from(
                    "FUNCTION RESTORE <dump> [FLUSH|APPEND|REPLACE]",
                )),
                RespValue::BulkString(Bytes::from("    Restore libraries from dump.")),
            ];
            Ok(RespValue::Array(help))
        }
        _ => Err(CommandError::UnknownSubcommand(
            subcommand,
            "FUNCTION".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_script_cache_load_and_exists() {
        let cache = ScriptCache::new();
        let sha = cache.load("return 1");
        assert!(cache.exists(&sha));
        assert!(!cache.exists("nonexistent"));
    }

    #[test]
    fn test_script_cache_get() {
        let cache = ScriptCache::new();
        let script = "return 'hello'";
        let sha = cache.load(script);
        assert_eq!(cache.get(&sha), Some(script.to_string()));
        assert_eq!(cache.get("nonexistent"), None);
    }

    #[test]
    fn test_script_cache_flush() {
        let cache = ScriptCache::new();
        cache.load("script1");
        cache.load("script2");
        assert_eq!(cache.len(), 2);
        cache.flush();
        assert!(cache.is_empty());
    }
}

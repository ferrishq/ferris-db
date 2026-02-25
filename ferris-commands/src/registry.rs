//! Command registry and dispatch

use crate::CommandHandler;
use std::collections::HashMap;

/// Command registry for looking up command handlers
#[derive(Default)]
pub struct CommandRegistry {
    commands: HashMap<String, CommandSpec>,
}

/// Specification for a command
#[derive(Clone)]
pub struct CommandSpec {
    /// Command name (uppercase)
    pub name: String,
    /// Minimum number of arguments (including command name)
    /// Negative means "at least abs(arity) arguments"
    pub arity: i32,
    /// Command flags
    pub flags: CommandFlags,
    /// The command handler function
    pub handler: CommandHandler,
}

impl std::fmt::Debug for CommandSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommandSpec")
            .field("name", &self.name)
            .field("arity", &self.arity)
            .field("flags", &self.flags)
            .finish()
    }
}

/// Flags describing command behavior
#[derive(Debug, Clone, Copy, Default)]
pub struct CommandFlags {
    /// Command performs writes
    pub write: bool,
    /// Command performs reads
    pub read: bool,
    /// Command may block the connection
    pub blocking: bool,
    /// Command can be run during loading
    pub loading: bool,
    /// Command can cause OOM
    pub denyoom: bool,
    /// Command is an admin command
    pub admin: bool,
    /// Command can be run on replicas
    pub readonly: bool,
    /// Command is a fast O(1) command
    pub fast: bool,
}

impl CommandRegistry {
    /// Create a new empty registry
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a command
    pub fn register(&mut self, name: &str, spec: CommandSpec) {
        self.commands.insert(name.to_uppercase(), spec);
    }

    /// Look up a command by name
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&CommandSpec> {
        self.commands.get(&name.to_uppercase())
    }

    /// Check if a command exists
    #[must_use]
    pub fn exists(&self, name: &str) -> bool {
        self.commands.contains_key(&name.to_uppercase())
    }

    /// Get all registered command names
    #[must_use]
    pub fn command_names(&self) -> Vec<&String> {
        self.commands.keys().collect()
    }

    /// Get number of registered commands
    #[must_use]
    pub fn len(&self) -> usize {
        self.commands.len()
    }

    /// Check if registry is empty
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.commands.is_empty()
    }
}

/// Helper to create a simple command spec
fn cmd(name: &str, arity: i32, flags: CommandFlags, handler: CommandHandler) -> CommandSpec {
    CommandSpec {
        name: name.to_uppercase(),
        arity,
        flags,
        handler,
    }
}

/// Read-only command flags
const fn read_flags() -> CommandFlags {
    CommandFlags {
        read: true,
        readonly: true,
        fast: true,
        write: false,
        blocking: false,
        loading: false,
        denyoom: false,
        admin: false,
    }
}

/// Write command flags
const fn write_flags() -> CommandFlags {
    CommandFlags {
        write: true,
        denyoom: true,
        read: false,
        readonly: false,
        fast: false,
        blocking: false,
        loading: false,
        admin: false,
    }
}

/// Admin command flags
const fn admin_flags() -> CommandFlags {
    CommandFlags {
        admin: true,
        loading: true,
        read: false,
        write: false,
        readonly: false,
        fast: false,
        blocking: false,
        denyoom: false,
    }
}

/// Fast read command flags (like GET, EXISTS)
const fn fast_read_flags() -> CommandFlags {
    CommandFlags {
        read: true,
        readonly: true,
        fast: true,
        write: false,
        blocking: false,
        loading: false,
        denyoom: false,
        admin: false,
    }
}

/// Fast write command flags (like SET, DEL)
const fn fast_write_flags() -> CommandFlags {
    CommandFlags {
        write: true,
        fast: true,
        denyoom: true,
        read: false,
        readonly: false,
        blocking: false,
        loading: false,
        admin: false,
    }
}

/// Blocking write command flags (like BLPOP, BRPOP, BZPOPMIN, etc.)
const fn blocking_write_flags() -> CommandFlags {
    CommandFlags {
        write: true,
        blocking: true,
        denyoom: true,
        read: false,
        readonly: false,
        fast: false,
        loading: false,
        admin: false,
    }
}

/// Register all commands into the registry
pub fn register_all_commands(registry: &mut CommandRegistry) {
    // Server commands
    registry.register("PING", cmd("PING", -1, read_flags(), crate::server::ping));
    registry.register("ECHO", cmd("ECHO", 2, read_flags(), crate::server::echo));
    registry.register(
        "DBSIZE",
        cmd("DBSIZE", 1, read_flags(), crate::server::dbsize),
    );
    registry.register("TIME", cmd("TIME", 1, read_flags(), crate::server::time));
    registry.register("INFO", cmd("INFO", -1, read_flags(), crate::server::info));
    registry.register(
        "COMMAND",
        cmd("COMMAND", -1, read_flags(), crate::server::command),
    );
    registry.register(
        "FLUSHDB",
        cmd("FLUSHDB", -1, admin_flags(), crate::server::flushdb),
    );
    registry.register(
        "FLUSHALL",
        cmd("FLUSHALL", -1, admin_flags(), crate::server::flushall),
    );
    registry.register(
        "DEBUG",
        cmd("DEBUG", -1, admin_flags(), crate::server::debug),
    );
    registry.register(
        "CONFIG",
        cmd("CONFIG", -2, admin_flags(), crate::server::config),
    );
    registry.register("AUTH", cmd("AUTH", -2, admin_flags(), crate::server::auth));
    registry.register(
        "SWAPDB",
        cmd("SWAPDB", 3, write_flags(), crate::server::swapdb),
    );
    registry.register(
        "SLOWLOG",
        cmd("SLOWLOG", -2, admin_flags(), crate::server::slowlog),
    );
    registry.register(
        "MEMORY",
        cmd("MEMORY", -2, read_flags(), crate::server::memory),
    );
    registry.register(
        "LASTSAVE",
        cmd("LASTSAVE", 1, read_flags(), crate::server::lastsave),
    );
    registry.register("SAVE", cmd("SAVE", 1, admin_flags(), crate::server::save));
    registry.register(
        "BGSAVE",
        cmd("BGSAVE", -1, admin_flags(), crate::server::bgsave),
    );
    registry.register(
        "BGREWRITEAOF",
        cmd(
            "BGREWRITEAOF",
            1,
            admin_flags(),
            crate::server::bgrewriteaof,
        ),
    );
    registry.register(
        "SHUTDOWN",
        cmd("SHUTDOWN", -1, admin_flags(), crate::server::shutdown),
    );

    // HyperLogLog commands
    registry.register(
        "PFADD",
        cmd("PFADD", -2, write_flags(), crate::hyperloglog::pfadd),
    );
    registry.register(
        "PFCOUNT",
        cmd("PFCOUNT", -2, read_flags(), crate::hyperloglog::pfcount),
    );
    registry.register(
        "PFMERGE",
        cmd("PFMERGE", -2, write_flags(), crate::hyperloglog::pfmerge),
    );

    // Geo commands
    registry.register(
        "GEOADD",
        cmd("GEOADD", -5, write_flags(), crate::geo::geoadd),
    );
    registry.register(
        "GEODIST",
        cmd("GEODIST", -4, read_flags(), crate::geo::geodist),
    );
    registry.register(
        "GEOHASH",
        cmd("GEOHASH", -2, read_flags(), crate::geo::geohash),
    );
    registry.register(
        "GEOPOS",
        cmd("GEOPOS", -2, read_flags(), crate::geo::geopos),
    );
    registry.register(
        "GEORADIUS",
        cmd("GEORADIUS", -6, read_flags(), crate::geo::georadius),
    );
    registry.register(
        "GEOSEARCH",
        cmd("GEOSEARCH", -4, read_flags(), crate::geo::geosearch),
    );

    // Pub/Sub commands
    registry.register(
        "PUBLISH",
        cmd("PUBLISH", 3, fast_read_flags(), crate::pubsub::publish),
    );
    registry.register(
        "SUBSCRIBE",
        cmd("SUBSCRIBE", -2, read_flags(), crate::pubsub::subscribe),
    );
    registry.register(
        "UNSUBSCRIBE",
        cmd("UNSUBSCRIBE", -1, read_flags(), crate::pubsub::unsubscribe),
    );
    registry.register(
        "PSUBSCRIBE",
        cmd("PSUBSCRIBE", -2, read_flags(), crate::pubsub::psubscribe),
    );
    registry.register(
        "PUNSUBSCRIBE",
        cmd(
            "PUNSUBSCRIBE",
            -1,
            read_flags(),
            crate::pubsub::punsubscribe,
        ),
    );
    registry.register(
        "PUBSUB",
        cmd("PUBSUB", -2, read_flags(), crate::pubsub::pubsub),
    );

    // Transaction commands
    registry.register(
        "MULTI",
        cmd("MULTI", 1, fast_read_flags(), crate::transaction::multi),
    );
    registry.register(
        "EXEC",
        cmd("EXEC", 1, read_flags(), crate::transaction::exec),
    );
    registry.register(
        "DISCARD",
        cmd("DISCARD", 1, fast_read_flags(), crate::transaction::discard),
    );
    registry.register(
        "WATCH",
        cmd("WATCH", -2, fast_read_flags(), crate::transaction::watch),
    );
    registry.register(
        "UNWATCH",
        cmd("UNWATCH", 1, fast_read_flags(), crate::transaction::unwatch),
    );

    // Connection commands
    registry.register(
        "SELECT",
        cmd("SELECT", 2, fast_read_flags(), crate::connection::select),
    );
    registry.register(
        "QUIT",
        cmd("QUIT", 1, read_flags(), crate::connection::quit),
    );
    registry.register(
        "CLIENT",
        cmd("CLIENT", -2, read_flags(), crate::connection::client),
    );
    registry.register(
        "HELLO",
        cmd("HELLO", -1, read_flags(), crate::connection::hello),
    );
    registry.register(
        "RESET",
        cmd("RESET", 1, read_flags(), crate::connection::reset),
    );
    registry.register(
        "AUTH",
        cmd("AUTH", -2, fast_read_flags(), crate::connection::auth),
    );

    // String commands
    registry.register("GET", cmd("GET", 2, fast_read_flags(), crate::string::get));
    registry.register(
        "SET",
        cmd("SET", -3, fast_write_flags(), crate::string::set),
    );
    registry.register(
        "SETNX",
        cmd("SETNX", 3, fast_write_flags(), crate::string::setnx),
    );
    registry.register(
        "SETEX",
        cmd("SETEX", 4, fast_write_flags(), crate::string::setex),
    );
    registry.register(
        "PSETEX",
        cmd("PSETEX", 4, fast_write_flags(), crate::string::psetex),
    );
    registry.register("MGET", cmd("MGET", -2, read_flags(), crate::string::mget));
    registry.register("MSET", cmd("MSET", -3, write_flags(), crate::string::mset));
    registry.register(
        "MSETNX",
        cmd("MSETNX", -3, write_flags(), crate::string::msetnx),
    );
    registry.register(
        "INCR",
        cmd("INCR", 2, fast_write_flags(), crate::string::incr),
    );
    registry.register(
        "INCRBY",
        cmd("INCRBY", 3, fast_write_flags(), crate::string::incrby),
    );
    registry.register(
        "INCRBYFLOAT",
        cmd(
            "INCRBYFLOAT",
            3,
            fast_write_flags(),
            crate::string::incrbyfloat,
        ),
    );
    registry.register(
        "DECR",
        cmd("DECR", 2, fast_write_flags(), crate::string::decr),
    );
    registry.register(
        "DECRBY",
        cmd("DECRBY", 3, fast_write_flags(), crate::string::decrby),
    );
    registry.register(
        "APPEND",
        cmd("APPEND", 3, fast_write_flags(), crate::string::append),
    );
    registry.register(
        "STRLEN",
        cmd("STRLEN", 2, fast_read_flags(), crate::string::strlen),
    );
    registry.register(
        "GETRANGE",
        cmd("GETRANGE", 4, read_flags(), crate::string::getrange),
    );
    registry.register(
        "SETRANGE",
        cmd("SETRANGE", 4, write_flags(), crate::string::setrange),
    );
    // SUBSTR is a deprecated alias for GETRANGE
    registry.register(
        "SUBSTR",
        cmd("SUBSTR", 4, read_flags(), crate::string::substr),
    );
    registry.register(
        "GETSET",
        cmd("GETSET", 3, fast_write_flags(), crate::string::getset),
    );
    registry.register(
        "GETDEL",
        cmd("GETDEL", 2, fast_write_flags(), crate::string::getdel),
    );
    registry.register(
        "GETEX",
        cmd("GETEX", -2, fast_write_flags(), crate::string::getex),
    );
    registry.register(
        "GETBIT",
        cmd("GETBIT", 3, fast_read_flags(), crate::string::getbit),
    );
    registry.register(
        "SETBIT",
        cmd("SETBIT", 4, fast_write_flags(), crate::string::setbit),
    );
    registry.register(
        "BITCOUNT",
        cmd("BITCOUNT", -2, read_flags(), crate::string::bitcount),
    );
    registry.register(
        "BITPOS",
        cmd("BITPOS", -3, read_flags(), crate::string::bitpos),
    );
    registry.register(
        "BITOP",
        cmd("BITOP", -4, write_flags(), crate::string::bitop),
    );
    registry.register(
        "BITFIELD",
        cmd("BITFIELD", -2, write_flags(), crate::string::bitfield),
    );
    registry.register("LCS", cmd("LCS", -3, read_flags(), crate::string::lcs));

    // Key commands
    registry.register("DEL", cmd("DEL", -2, fast_write_flags(), crate::key::del));
    registry.register(
        "UNLINK",
        cmd("UNLINK", -2, fast_write_flags(), crate::key::unlink),
    );
    registry.register(
        "EXISTS",
        cmd("EXISTS", -2, fast_read_flags(), crate::key::exists),
    );
    registry.register(
        "EXPIRE",
        cmd("EXPIRE", 3, fast_write_flags(), crate::key::expire),
    );
    registry.register(
        "PEXPIRE",
        cmd("PEXPIRE", 3, fast_write_flags(), crate::key::pexpire),
    );
    registry.register(
        "EXPIREAT",
        cmd("EXPIREAT", 3, fast_write_flags(), crate::key::expireat),
    );
    registry.register(
        "PEXPIREAT",
        cmd("PEXPIREAT", 3, fast_write_flags(), crate::key::pexpireat),
    );
    registry.register("TTL", cmd("TTL", 2, fast_read_flags(), crate::key::ttl));
    registry.register("PTTL", cmd("PTTL", 2, fast_read_flags(), crate::key::pttl));
    registry.register(
        "PERSIST",
        cmd("PERSIST", 2, fast_write_flags(), crate::key::persist),
    );
    registry.register(
        "TYPE",
        cmd("TYPE", 2, fast_read_flags(), crate::key::type_cmd),
    );
    registry.register("KEYS", cmd("KEYS", 2, read_flags(), crate::key::keys));
    registry.register("SCAN", cmd("SCAN", -2, read_flags(), crate::key::scan));
    registry.register(
        "RENAME",
        cmd("RENAME", 3, write_flags(), crate::key::rename),
    );
    registry.register(
        "RENAMENX",
        cmd("RENAMENX", 3, write_flags(), crate::key::renamenx),
    );
    registry.register("COPY", cmd("COPY", -3, write_flags(), crate::key::copy));
    registry.register(
        "TOUCH",
        cmd("TOUCH", -2, fast_read_flags(), crate::key::touch),
    );
    registry.register(
        "OBJECT",
        cmd("OBJECT", -2, read_flags(), crate::key::object),
    );
    registry.register("DUMP", cmd("DUMP", 2, read_flags(), crate::key::dump));
    registry.register(
        "RESTORE",
        cmd("RESTORE", -4, write_flags(), crate::key::restore),
    );
    registry.register(
        "RANDOMKEY",
        cmd("RANDOMKEY", 1, read_flags(), crate::key::randomkey),
    );
    registry.register(
        "EXPIRETIME",
        cmd("EXPIRETIME", 2, fast_read_flags(), crate::key::expiretime),
    );
    registry.register(
        "PEXPIRETIME",
        cmd("PEXPIRETIME", 2, fast_read_flags(), crate::key::pexpiretime),
    );
    registry.register("MOVE", cmd("MOVE", 3, write_flags(), crate::key::move_key));
    registry.register("SORT", cmd("SORT", -2, write_flags(), crate::key::sort));
    registry.register(
        "SORT_RO",
        cmd("SORT_RO", -2, read_flags(), crate::key::sort_ro),
    );

    // Hash commands
    registry.register("HGET", cmd("HGET", 3, fast_read_flags(), crate::hash::hget));
    registry.register(
        "HSET",
        cmd("HSET", -4, fast_write_flags(), crate::hash::hset),
    );
    registry.register(
        "HSETNX",
        cmd("HSETNX", 4, fast_write_flags(), crate::hash::hsetnx),
    );
    registry.register(
        "HDEL",
        cmd("HDEL", -3, fast_write_flags(), crate::hash::hdel),
    );
    registry.register(
        "HEXISTS",
        cmd("HEXISTS", 3, fast_read_flags(), crate::hash::hexists),
    );
    registry.register(
        "HGETALL",
        cmd("HGETALL", 2, read_flags(), crate::hash::hgetall),
    );
    registry.register("HMSET", cmd("HMSET", -4, write_flags(), crate::hash::hmset));
    registry.register("HMGET", cmd("HMGET", -3, read_flags(), crate::hash::hmget));
    registry.register(
        "HINCRBY",
        cmd("HINCRBY", 4, fast_write_flags(), crate::hash::hincrby),
    );
    registry.register(
        "HINCRBYFLOAT",
        cmd(
            "HINCRBYFLOAT",
            4,
            fast_write_flags(),
            crate::hash::hincrbyfloat,
        ),
    );
    registry.register("HKEYS", cmd("HKEYS", 2, read_flags(), crate::hash::hkeys));
    registry.register("HVALS", cmd("HVALS", 2, read_flags(), crate::hash::hvals));
    registry.register("HLEN", cmd("HLEN", 2, fast_read_flags(), crate::hash::hlen));
    registry.register("HSCAN", cmd("HSCAN", -3, read_flags(), crate::hash::hscan));
    registry.register(
        "HSTRLEN",
        cmd("HSTRLEN", 3, fast_read_flags(), crate::hash::hstrlen),
    );
    registry.register(
        "HRANDFIELD",
        cmd("HRANDFIELD", -2, read_flags(), crate::hash::hrandfield),
    );

    // List commands
    registry.register(
        "LPUSH",
        cmd("LPUSH", -3, fast_write_flags(), crate::list::lpush),
    );
    registry.register(
        "RPUSH",
        cmd("RPUSH", -3, fast_write_flags(), crate::list::rpush),
    );
    registry.register(
        "LPUSHX",
        cmd("LPUSHX", -3, fast_write_flags(), crate::list::lpushx),
    );
    registry.register(
        "RPUSHX",
        cmd("RPUSHX", -3, fast_write_flags(), crate::list::rpushx),
    );
    registry.register(
        "LPOP",
        cmd("LPOP", -2, fast_write_flags(), crate::list::lpop),
    );
    registry.register(
        "RPOP",
        cmd("RPOP", -2, fast_write_flags(), crate::list::rpop),
    );
    registry.register(
        "LRANGE",
        cmd("LRANGE", 4, read_flags(), crate::list::lrange),
    );
    registry.register(
        "LINDEX",
        cmd("LINDEX", 3, fast_read_flags(), crate::list::lindex),
    );
    registry.register(
        "LSET",
        cmd("LSET", 4, fast_write_flags(), crate::list::lset),
    );
    registry.register("LLEN", cmd("LLEN", 2, fast_read_flags(), crate::list::llen));
    registry.register("LREM", cmd("LREM", 4, write_flags(), crate::list::lrem));
    registry.register(
        "LINSERT",
        cmd("LINSERT", 5, write_flags(), crate::list::linsert),
    );
    registry.register("LTRIM", cmd("LTRIM", 4, write_flags(), crate::list::ltrim));
    registry.register("LPOS", cmd("LPOS", -3, read_flags(), crate::list::lpos));
    registry.register("LMOVE", cmd("LMOVE", 5, write_flags(), crate::list::lmove));
    registry.register("LMPOP", cmd("LMPOP", -4, write_flags(), crate::list::lmpop));
    // Legacy commands (deprecated, aliases)
    registry.register(
        "RPOPLPUSH",
        cmd("RPOPLPUSH", 3, write_flags(), crate::list::rpoplpush),
    );
    // Blocking list commands
    registry.register(
        "BLPOP",
        cmd("BLPOP", -3, blocking_write_flags(), crate::list::blpop),
    );
    registry.register(
        "BRPOP",
        cmd("BRPOP", -3, blocking_write_flags(), crate::list::brpop),
    );
    registry.register(
        "BLMOVE",
        cmd("BLMOVE", 6, blocking_write_flags(), crate::list::blmove),
    );
    registry.register(
        "BLMPOP",
        cmd("BLMPOP", -5, blocking_write_flags(), crate::list::blmpop),
    );
    // Legacy blocking command (deprecated, alias)
    registry.register(
        "BRPOPLPUSH",
        cmd(
            "BRPOPLPUSH",
            4,
            blocking_write_flags(),
            crate::list::brpoplpush,
        ),
    );

    // Set commands
    registry.register(
        "SADD",
        cmd("SADD", -3, fast_write_flags(), crate::set::sadd),
    );
    registry.register(
        "SREM",
        cmd("SREM", -3, fast_write_flags(), crate::set::srem),
    );
    registry.register(
        "SMEMBERS",
        cmd("SMEMBERS", 2, read_flags(), crate::set::smembers),
    );
    registry.register(
        "SISMEMBER",
        cmd("SISMEMBER", 3, fast_read_flags(), crate::set::sismember),
    );
    registry.register(
        "SMISMEMBER",
        cmd("SMISMEMBER", -3, read_flags(), crate::set::smismember),
    );
    registry.register(
        "SCARD",
        cmd("SCARD", 2, fast_read_flags(), crate::set::scard),
    );
    registry.register(
        "SINTER",
        cmd("SINTER", -2, read_flags(), crate::set::sinter),
    );
    registry.register(
        "SINTERSTORE",
        cmd("SINTERSTORE", -3, write_flags(), crate::set::sinterstore),
    );
    registry.register(
        "SINTERCARD",
        cmd("SINTERCARD", -3, read_flags(), crate::set::sintercard),
    );
    registry.register(
        "SUNION",
        cmd("SUNION", -2, read_flags(), crate::set::sunion),
    );
    registry.register(
        "SUNIONSTORE",
        cmd("SUNIONSTORE", -3, write_flags(), crate::set::sunionstore),
    );
    registry.register("SDIFF", cmd("SDIFF", -2, read_flags(), crate::set::sdiff));
    registry.register(
        "SDIFFSTORE",
        cmd("SDIFFSTORE", -3, write_flags(), crate::set::sdiffstore),
    );
    registry.register(
        "SRANDMEMBER",
        cmd("SRANDMEMBER", -2, read_flags(), crate::set::srandmember),
    );
    registry.register(
        "SPOP",
        cmd("SPOP", -2, fast_write_flags(), crate::set::spop),
    );
    registry.register(
        "SMOVE",
        cmd("SMOVE", 4, fast_write_flags(), crate::set::smove),
    );
    registry.register("SSCAN", cmd("SSCAN", -3, read_flags(), crate::set::sscan));

    // Sorted set commands
    registry.register(
        "ZADD",
        cmd("ZADD", -4, fast_write_flags(), crate::sorted_set::zadd),
    );
    registry.register(
        "ZREM",
        cmd("ZREM", -3, fast_write_flags(), crate::sorted_set::zrem),
    );
    registry.register(
        "ZSCORE",
        cmd("ZSCORE", 3, fast_read_flags(), crate::sorted_set::zscore),
    );
    registry.register(
        "ZRANK",
        cmd("ZRANK", -3, fast_read_flags(), crate::sorted_set::zrank),
    );
    registry.register(
        "ZREVRANK",
        cmd(
            "ZREVRANK",
            -3,
            fast_read_flags(),
            crate::sorted_set::zrevrank,
        ),
    );
    registry.register(
        "ZCARD",
        cmd("ZCARD", 2, fast_read_flags(), crate::sorted_set::zcard),
    );
    registry.register(
        "ZCOUNT",
        cmd("ZCOUNT", 4, read_flags(), crate::sorted_set::zcount),
    );
    registry.register(
        "ZRANGE",
        cmd("ZRANGE", -4, read_flags(), crate::sorted_set::zrange),
    );
    registry.register(
        "ZREVRANGE",
        cmd("ZREVRANGE", -4, read_flags(), crate::sorted_set::zrevrange),
    );
    registry.register(
        "ZRANGEBYSCORE",
        cmd(
            "ZRANGEBYSCORE",
            -4,
            read_flags(),
            crate::sorted_set::zrangebyscore,
        ),
    );
    registry.register(
        "ZREVRANGEBYSCORE",
        cmd(
            "ZREVRANGEBYSCORE",
            -4,
            read_flags(),
            crate::sorted_set::zrevrangebyscore,
        ),
    );
    registry.register(
        "ZINCRBY",
        cmd("ZINCRBY", 4, fast_write_flags(), crate::sorted_set::zincrby),
    );
    registry.register(
        "ZPOPMIN",
        cmd(
            "ZPOPMIN",
            -2,
            fast_write_flags(),
            crate::sorted_set::zpopmin,
        ),
    );
    registry.register(
        "ZPOPMAX",
        cmd(
            "ZPOPMAX",
            -2,
            fast_write_flags(),
            crate::sorted_set::zpopmax,
        ),
    );
    registry.register(
        "ZRANGESTORE",
        cmd(
            "ZRANGESTORE",
            -5,
            write_flags(),
            crate::sorted_set::zrangestore,
        ),
    );
    registry.register(
        "ZUNIONSTORE",
        cmd(
            "ZUNIONSTORE",
            -4,
            write_flags(),
            crate::sorted_set::zunionstore,
        ),
    );
    registry.register(
        "ZINTERSTORE",
        cmd(
            "ZINTERSTORE",
            -4,
            write_flags(),
            crate::sorted_set::zinterstore,
        ),
    );
    registry.register(
        "ZSCAN",
        cmd("ZSCAN", -3, read_flags(), crate::sorted_set::zscan),
    );
    registry.register(
        "ZMSCORE",
        cmd("ZMSCORE", -3, read_flags(), crate::sorted_set::zmscore),
    );
    registry.register(
        "ZRANDMEMBER",
        cmd(
            "ZRANDMEMBER",
            -2,
            read_flags(),
            crate::sorted_set::zrandmember,
        ),
    );
    registry.register(
        "ZLEXCOUNT",
        cmd("ZLEXCOUNT", 4, read_flags(), crate::sorted_set::zlexcount),
    );
    registry.register(
        "ZRANGEBYLEX",
        cmd(
            "ZRANGEBYLEX",
            -4,
            read_flags(),
            crate::sorted_set::zrangebylex,
        ),
    );
    registry.register(
        "ZREVRANGEBYLEX",
        cmd(
            "ZREVRANGEBYLEX",
            -4,
            read_flags(),
            crate::sorted_set::zrevrangebylex,
        ),
    );
    registry.register(
        "ZREMRANGEBYRANK",
        cmd(
            "ZREMRANGEBYRANK",
            4,
            write_flags(),
            crate::sorted_set::zremrangebyrank,
        ),
    );
    registry.register(
        "ZREMRANGEBYSCORE",
        cmd(
            "ZREMRANGEBYSCORE",
            4,
            write_flags(),
            crate::sorted_set::zremrangebyscore,
        ),
    );
    registry.register(
        "ZREMRANGEBYLEX",
        cmd(
            "ZREMRANGEBYLEX",
            4,
            write_flags(),
            crate::sorted_set::zremrangebylex,
        ),
    );
    registry.register(
        "ZMPOP",
        cmd("ZMPOP", -4, write_flags(), crate::sorted_set::zmpop),
    );
    registry.register(
        "ZUNION",
        cmd("ZUNION", -3, read_flags(), crate::sorted_set::zunion),
    );
    registry.register(
        "ZINTER",
        cmd("ZINTER", -3, read_flags(), crate::sorted_set::zinter),
    );
    registry.register(
        "ZINTERCARD",
        cmd(
            "ZINTERCARD",
            -3,
            read_flags(),
            crate::sorted_set::zintercard,
        ),
    );
    registry.register(
        "ZDIFF",
        cmd("ZDIFF", -3, read_flags(), crate::sorted_set::zdiff),
    );
    registry.register(
        "ZDIFFSTORE",
        cmd(
            "ZDIFFSTORE",
            -4,
            write_flags(),
            crate::sorted_set::zdiffstore,
        ),
    );
    // Blocking sorted set commands
    registry.register(
        "BZPOPMIN",
        cmd(
            "BZPOPMIN",
            -3,
            blocking_write_flags(),
            crate::sorted_set::bzpopmin,
        ),
    );
    registry.register(
        "BZPOPMAX",
        cmd(
            "BZPOPMAX",
            -3,
            blocking_write_flags(),
            crate::sorted_set::bzpopmax,
        ),
    );
    registry.register(
        "BZMPOP",
        cmd(
            "BZMPOP",
            -5,
            blocking_write_flags(),
            crate::sorted_set::bzmpop,
        ),
    );

    // Stream commands
    registry.register("XADD", cmd("XADD", -5, write_flags(), crate::stream::xadd));
    registry.register(
        "XLEN",
        cmd("XLEN", 2, fast_read_flags(), crate::stream::xlen),
    );
    registry.register(
        "XRANGE",
        cmd("XRANGE", -4, read_flags(), crate::stream::xrange),
    );
    registry.register(
        "XREVRANGE",
        cmd("XREVRANGE", -4, read_flags(), crate::stream::xrevrange),
    );
    registry.register(
        "XREAD",
        cmd("XREAD", -4, read_flags(), crate::stream::xread),
    );
    registry.register("XDEL", cmd("XDEL", -3, write_flags(), crate::stream::xdel));
    registry.register(
        "XTRIM",
        cmd("XTRIM", -4, write_flags(), crate::stream::xtrim),
    );
    registry.register(
        "XINFO",
        cmd("XINFO", -2, read_flags(), crate::stream::xinfo),
    );
    registry.register(
        "XGROUP",
        cmd("XGROUP", -2, write_flags(), crate::stream::xgroup),
    );
    registry.register(
        "XSETID",
        cmd("XSETID", 3, write_flags(), crate::stream::xsetid),
    );
    registry.register("XACK", cmd("XACK", -4, write_flags(), crate::stream::xack));
    registry.register(
        "XPENDING",
        cmd("XPENDING", -3, read_flags(), crate::stream::xpending),
    );
    registry.register(
        "XREADGROUP",
        cmd("XREADGROUP", -7, write_flags(), crate::stream::xreadgroup),
    );
    registry.register(
        "XCLAIM",
        cmd("XCLAIM", -6, write_flags(), crate::stream::xclaim),
    );
    registry.register(
        "XAUTOCLAIM",
        cmd("XAUTOCLAIM", -6, write_flags(), crate::stream::xautoclaim),
    );

    // Scripting commands
    registry.register(
        "EVAL",
        cmd("EVAL", -3, write_flags(), crate::scripting::eval),
    );
    registry.register(
        "EVAL_RO",
        cmd("EVAL_RO", -3, read_flags(), crate::scripting::eval_ro),
    );
    registry.register(
        "EVALSHA",
        cmd("EVALSHA", -3, write_flags(), crate::scripting::evalsha),
    );
    registry.register(
        "EVALSHA_RO",
        cmd("EVALSHA_RO", -3, read_flags(), crate::scripting::evalsha_ro),
    );
    registry.register(
        "SCRIPT",
        cmd("SCRIPT", -2, admin_flags(), crate::scripting::script),
    );
    registry.register(
        "FCALL",
        cmd("FCALL", -3, write_flags(), crate::scripting::fcall),
    );
    registry.register(
        "FCALL_RO",
        cmd("FCALL_RO", -3, read_flags(), crate::scripting::fcall_ro),
    );
    registry.register(
        "FUNCTION",
        cmd("FUNCTION", -2, admin_flags(), crate::scripting::function),
    );

    // ACL commands
    registry.register("ACL", cmd("ACL", -2, admin_flags(), crate::acl::acl));

    // Replication commands
    registry.register(
        "REPLICAOF",
        cmd("REPLICAOF", 3, admin_flags(), crate::replication::replicaof),
    );
    registry.register(
        "SLAVEOF",
        cmd("SLAVEOF", 3, admin_flags(), crate::replication::slaveof),
    );
    registry.register(
        "ROLE",
        cmd("ROLE", 1, fast_read_flags(), crate::replication::role),
    );
    registry.register(
        "WAIT",
        cmd("WAIT", 3, read_flags(), crate::replication::wait),
    );
    registry.register(
        "WAITAOF",
        cmd("WAITAOF", 4, read_flags(), crate::replication::waitaof),
    );
    registry.register(
        "PSYNC",
        cmd("PSYNC", 3, admin_flags(), crate::replication::psync),
    );
    registry.register(
        "REPLCONF",
        cmd(
            "REPLCONF",
            -3, // Variable args, minimum 1 option-value pair
            admin_flags(),
            crate::replication::replconf,
        ),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CommandContext, CommandResult};
    use ferris_protocol::RespValue;

    fn dummy_handler(_ctx: &mut CommandContext, _args: &[RespValue]) -> CommandResult {
        Ok(RespValue::ok())
    }

    #[test]
    fn test_registry_register_and_get() {
        let mut registry = CommandRegistry::new();
        registry.register(
            "PING",
            CommandSpec {
                name: "PING".to_string(),
                arity: 1,
                flags: CommandFlags {
                    readonly: true,
                    ..Default::default()
                },
                handler: dummy_handler,
            },
        );

        assert!(registry.exists("PING"));
        assert!(registry.exists("ping"));
        assert!(!registry.exists("UNKNOWN"));

        let spec = registry.get("ping").unwrap();
        assert_eq!(spec.name, "PING");
        assert_eq!(spec.arity, 1);
    }

    #[test]
    fn test_registry_len() {
        let mut registry = CommandRegistry::new();
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);

        registry.register(
            "PING",
            CommandSpec {
                name: "PING".to_string(),
                arity: 1,
                flags: Default::default(),
                handler: dummy_handler,
            },
        );

        assert!(!registry.is_empty());
        assert_eq!(registry.len(), 1);
    }
}

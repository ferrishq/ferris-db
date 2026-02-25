//! Command executor - dispatches commands to handlers

use crate::{CommandContext, CommandError, CommandRegistry, CommandResult};
use ferris_protocol::{Command, RespValue};

/// Executes commands by dispatching to registered handlers
pub struct CommandExecutor {
    registry: CommandRegistry,
}

impl CommandExecutor {
    /// Create a new executor with all commands registered
    #[must_use]
    pub fn new() -> Self {
        let mut registry = CommandRegistry::new();
        crate::registry::register_all_commands(&mut registry);
        Self { registry }
    }

    /// Execute a command
    pub fn execute(&self, ctx: &mut CommandContext, cmd: &Command) -> CommandResult {
        let spec = self
            .registry
            .get(&cmd.name)
            .ok_or_else(|| CommandError::UnknownCommand(cmd.name.clone()))?;

        // Set the current command name for propagation
        ctx.set_current_command(cmd.name.clone());

        // Check if this is a write command on a read-only replica
        // Allow writes if we're applying replicated commands from the leader
        if spec.flags.write && ctx.is_replica() && !ctx.is_applying_replication() {
            return Err(CommandError::ReadOnly);
        }

        // Check arity (negative means at least abs(arity) args)
        let expected_arity = spec.arity;
        let actual_arity = (cmd.args.len() + 1) as i32; // +1 for command name

        if expected_arity > 0 && actual_arity != expected_arity {
            return Err(CommandError::WrongArity(spec.name.clone()));
        }
        if expected_arity < 0 && actual_arity < expected_arity.abs() {
            return Err(CommandError::WrongArity(spec.name.clone()));
        }

        // Convert args to RespValue for handler
        let args: Vec<RespValue> = cmd
            .args
            .iter()
            .map(|b| RespValue::BulkString(b.clone()))
            .collect();

        // Execute the handler
        (spec.handler)(ctx, &args)
    }

    /// Get the command registry
    #[must_use]
    pub fn registry(&self) -> &CommandRegistry {
        &self.registry
    }
}

impl Default for CommandExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use ferris_core::KeyStore;
    use std::sync::Arc;

    fn make_command(parts: &[&str]) -> Command {
        Command {
            name: parts[0].to_uppercase(),
            args: parts[1..]
                .iter()
                .map(|s| Bytes::from(s.to_string()))
                .collect(),
        }
    }

    #[test]
    fn test_executor_ping() {
        let store = Arc::new(KeyStore::default());
        let mut ctx = CommandContext::new(store);
        let executor = CommandExecutor::new();

        let cmd = make_command(&["PING"]);
        let result = executor.execute(&mut ctx, &cmd).unwrap();
        assert_eq!(result, RespValue::SimpleString("PONG".to_string()));
    }

    #[test]
    fn test_executor_ping_with_message() {
        let store = Arc::new(KeyStore::default());
        let mut ctx = CommandContext::new(store);
        let executor = CommandExecutor::new();

        let cmd = make_command(&["PING", "hello"]);
        let result = executor.execute(&mut ctx, &cmd).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from("hello")));
    }

    #[test]
    fn test_executor_unknown_command() {
        let store = Arc::new(KeyStore::default());
        let mut ctx = CommandContext::new(store);
        let executor = CommandExecutor::new();

        let cmd = make_command(&["UNKNOWNCOMMAND"]);
        let result = executor.execute(&mut ctx, &cmd);
        assert!(matches!(result, Err(CommandError::UnknownCommand(_))));
    }

    #[test]
    fn test_executor_echo() {
        let store = Arc::new(KeyStore::default());
        let mut ctx = CommandContext::new(store);
        let executor = CommandExecutor::new();

        let cmd = make_command(&["ECHO", "Hello World"]);
        let result = executor.execute(&mut ctx, &cmd).unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from("Hello World")));
    }
}

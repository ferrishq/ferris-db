//! Command parsing and representation

use crate::error::ProtocolError;
use crate::types::RespValue;
use bytes::Bytes;

/// A parsed Redis command with its arguments
#[derive(Debug, Clone)]
pub struct Command {
    /// Command name (uppercase)
    pub name: String,
    /// Command arguments
    pub args: Vec<Bytes>,
}

impl Command {
    /// Parse a command from a RESP value (should be an array)
    pub fn from_resp(value: RespValue) -> Result<Self, ProtocolError> {
        let parts = match value {
            RespValue::Array(parts) => parts,
            _ => {
                return Err(ProtocolError::InvalidFormat(
                    "command must be an array".to_string(),
                ))
            }
        };

        if parts.is_empty() {
            return Err(ProtocolError::InvalidFormat("empty command".to_string()));
        }

        let name = match &parts[0] {
            RespValue::BulkString(b) => String::from_utf8(b.to_vec())
                .map_err(|_| ProtocolError::InvalidFormat("invalid command name".to_string()))?
                .to_uppercase(),
            RespValue::SimpleString(s) => s.to_uppercase(),
            _ => {
                return Err(ProtocolError::InvalidFormat(
                    "command name must be a string".to_string(),
                ))
            }
        };

        let args = parts[1..]
            .iter()
            .map(|v| match v {
                RespValue::BulkString(b) => Ok(b.clone()),
                RespValue::SimpleString(s) => Ok(Bytes::from(s.clone())),
                RespValue::Integer(n) => Ok(Bytes::from(n.to_string())),
                _ => Err(ProtocolError::InvalidFormat(
                    "invalid argument type".to_string(),
                )),
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self { name, args })
    }

    /// Get the number of arguments (excluding command name)
    #[must_use]
    pub fn arg_count(&self) -> usize {
        self.args.len()
    }

    /// Get an argument as bytes
    #[must_use]
    pub fn get_arg(&self, index: usize) -> Option<&Bytes> {
        self.args.get(index)
    }

    /// Get an argument as a string
    #[must_use]
    pub fn get_arg_str(&self, index: usize) -> Option<&str> {
        self.args
            .get(index)
            .and_then(|b| std::str::from_utf8(b).ok())
    }

    /// Get an argument as an integer
    pub fn get_arg_i64(&self, index: usize) -> Option<Result<i64, std::num::ParseIntError>> {
        self.get_arg_str(index).map(|s| s.parse())
    }

    /// Get an argument as a float
    pub fn get_arg_f64(&self, index: usize) -> Option<Result<f64, std::num::ParseFloatError>> {
        self.get_arg_str(index).map(|s| s.parse())
    }

    /// Check if this is a specific command
    #[must_use]
    pub fn is(&self, name: &str) -> bool {
        self.name.eq_ignore_ascii_case(name)
    }

    /// Check if a flag is present (case-insensitive)
    #[must_use]
    pub fn has_flag(&self, flag: &str) -> bool {
        self.args.iter().any(|a| {
            std::str::from_utf8(a)
                .map(|s| s.eq_ignore_ascii_case(flag))
                .unwrap_or(false)
        })
    }

    /// Get the position of a flag if present
    #[must_use]
    pub fn flag_position(&self, flag: &str) -> Option<usize> {
        self.args.iter().position(|a| {
            std::str::from_utf8(a)
                .map(|s| s.eq_ignore_ascii_case(flag))
                .unwrap_or(false)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_command(parts: &[&str]) -> Command {
        let resp = RespValue::Array(
            parts
                .iter()
                .map(|s| RespValue::BulkString(Bytes::from(s.to_string())))
                .collect(),
        );
        Command::from_resp(resp).unwrap()
    }

    #[test]
    fn test_parse_simple_command() {
        let cmd = make_command(&["PING"]);
        assert_eq!(cmd.name, "PING");
        assert_eq!(cmd.arg_count(), 0);
    }

    #[test]
    fn test_parse_command_with_args() {
        let cmd = make_command(&["SET", "mykey", "myvalue"]);
        assert_eq!(cmd.name, "SET");
        assert_eq!(cmd.arg_count(), 2);
        assert_eq!(cmd.get_arg_str(0), Some("mykey"));
        assert_eq!(cmd.get_arg_str(1), Some("myvalue"));
    }

    #[test]
    fn test_command_name_uppercase() {
        let cmd = make_command(&["get", "key"]);
        assert_eq!(cmd.name, "GET");
    }

    #[test]
    fn test_is_command() {
        let cmd = make_command(&["SET", "key", "value"]);
        assert!(cmd.is("SET"));
        assert!(cmd.is("set"));
        assert!(cmd.is("Set"));
        assert!(!cmd.is("GET"));
    }

    #[test]
    fn test_has_flag() {
        let cmd = make_command(&["SET", "key", "value", "NX", "EX", "100"]);
        assert!(cmd.has_flag("NX"));
        assert!(cmd.has_flag("nx"));
        assert!(cmd.has_flag("EX"));
        assert!(!cmd.has_flag("XX"));
    }

    #[test]
    fn test_flag_position() {
        let cmd = make_command(&["SET", "key", "value", "EX", "100"]);
        assert_eq!(cmd.flag_position("EX"), Some(2));
        assert_eq!(cmd.flag_position("XX"), None);
    }

    #[test]
    fn test_get_arg_i64() {
        let cmd = make_command(&["EXPIRE", "key", "3600"]);
        assert_eq!(cmd.get_arg_i64(1).unwrap().unwrap(), 3600);
    }

    #[test]
    fn test_get_arg_f64() {
        let cmd = make_command(&["INCRBYFLOAT", "key", "3.14"]);
        let result = cmd.get_arg_f64(1).unwrap().unwrap();
        assert!((result - 3.14).abs() < 0.001);
    }

    #[test]
    fn test_empty_command_error() {
        let resp = RespValue::Array(vec![]);
        let result = Command::from_resp(resp);
        assert!(result.is_err());
    }

    #[test]
    fn test_non_array_error() {
        let resp = RespValue::SimpleString("PING".to_string());
        let result = Command::from_resp(resp);
        assert!(result.is_err());
    }
}

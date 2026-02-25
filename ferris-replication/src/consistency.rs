//! Replication consistency modes
//!
//! Defines how write operations are synchronized with replicas:
//! - Async: Fire and forget (maximum performance)
//! - Semi-sync: Wait for N replicas to acknowledge
//! - Sync: Wait for all replicas to acknowledge

use std::time::Duration;

/// Replication consistency mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConsistencyMode {
    /// Async replication - fire and forget (Redis default)
    /// Maximum performance, no durability guarantees
    #[default]
    Async,

    /// Semi-synchronous replication - wait for N replicas
    /// Balanced performance and durability
    SemiSync {
        /// Number of replicas that must acknowledge writes
        num_replicas: usize,
        /// Timeout for waiting (milliseconds)
        timeout_ms: u64,
    },

    /// Synchronous replication - wait for all replicas
    /// Maximum durability, lower performance
    Sync {
        /// Timeout for waiting (milliseconds)
        timeout_ms: u64,
    },
}

impl ConsistencyMode {
    /// Create async mode
    #[must_use]
    pub const fn async_mode() -> Self {
        Self::Async
    }

    /// Create semi-sync mode with default timeout (1 second)
    #[must_use]
    pub const fn semi_sync(num_replicas: usize) -> Self {
        Self::SemiSync {
            num_replicas,
            timeout_ms: 1000,
        }
    }

    /// Create semi-sync mode with custom timeout
    #[must_use]
    pub const fn semi_sync_with_timeout(num_replicas: usize, timeout_ms: u64) -> Self {
        Self::SemiSync {
            num_replicas,
            timeout_ms,
        }
    }

    /// Create sync mode with default timeout (1 second)
    #[must_use]
    pub const fn sync_mode() -> Self {
        Self::Sync { timeout_ms: 1000 }
    }

    /// Create sync mode with custom timeout
    #[must_use]
    pub const fn sync_with_timeout(timeout_ms: u64) -> Self {
        Self::Sync { timeout_ms }
    }

    /// Check if this mode requires waiting for replicas
    #[must_use]
    pub const fn requires_wait(&self) -> bool {
        !matches!(self, Self::Async)
    }

    /// Get the number of replicas to wait for and timeout
    /// Returns (`num_replicas`, `timeout_duration`) or None for async mode
    #[must_use]
    pub fn wait_params(&self, total_replicas: usize) -> Option<(usize, Duration)> {
        match self {
            Self::Async => None,
            Self::SemiSync {
                num_replicas,
                timeout_ms,
            } => Some((
                (*num_replicas).min(total_replicas),
                Duration::from_millis(*timeout_ms),
            )),
            Self::Sync { timeout_ms } => Some((total_replicas, Duration::from_millis(*timeout_ms))),
        }
    }

    /// Parse consistency mode from string
    /// Format: "async", "semi-sync:N:timeout", "sync:timeout"
    ///
    /// # Errors
    ///
    /// Returns error if the format is invalid
    pub fn parse(s: &str) -> Result<Self, String> {
        let parts: Vec<&str> = s.split(':').collect();
        match parts[0] {
            "async" => Ok(Self::Async),
            "semi-sync" => {
                if parts.len() < 2 {
                    return Err("semi-sync requires num_replicas parameter".to_string());
                }
                let num_replicas = parts[1]
                    .parse::<usize>()
                    .map_err(|_| "invalid num_replicas")?;
                let timeout_ms = if parts.len() >= 3 {
                    parts[2].parse::<u64>().map_err(|_| "invalid timeout")?
                } else {
                    1000
                };
                Ok(Self::SemiSync {
                    num_replicas,
                    timeout_ms,
                })
            }
            "sync" => {
                let timeout_ms = if parts.len() >= 2 {
                    parts[1].parse::<u64>().map_err(|_| "invalid timeout")?
                } else {
                    1000
                };
                Ok(Self::Sync { timeout_ms })
            }
            _ => Err(format!("unknown consistency mode: {}", parts[0])),
        }
    }
}

impl std::fmt::Display for ConsistencyMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Async => write!(f, "async"),
            Self::SemiSync {
                num_replicas,
                timeout_ms,
            } => write!(f, "semi-sync:{num_replicas}:{timeout_ms}"),
            Self::Sync { timeout_ms } => write!(f, "sync:{timeout_ms}"),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_default_is_async() {
        assert_eq!(ConsistencyMode::default(), ConsistencyMode::Async);
    }

    #[test]
    fn test_async_no_wait() {
        assert!(!ConsistencyMode::Async.requires_wait());
        assert_eq!(ConsistencyMode::Async.wait_params(5), None);
    }

    #[test]
    fn test_semi_sync_wait_params() {
        let mode = ConsistencyMode::semi_sync(2);
        assert!(mode.requires_wait());

        let (num, timeout) = mode.wait_params(5).unwrap();
        assert_eq!(num, 2);
        assert_eq!(timeout, Duration::from_millis(1000));
    }

    #[test]
    fn test_semi_sync_clamps_to_available() {
        let mode = ConsistencyMode::semi_sync(10);
        let (num, _) = mode.wait_params(3).unwrap();
        assert_eq!(num, 3); // Clamped to available replicas
    }

    #[test]
    fn test_sync_waits_for_all() {
        let mode = ConsistencyMode::sync_mode();
        assert!(mode.requires_wait());

        let (num, timeout) = mode.wait_params(5).unwrap();
        assert_eq!(num, 5); // All replicas
        assert_eq!(timeout, Duration::from_millis(1000));
    }

    #[test]
    fn test_parse_async() {
        let mode = ConsistencyMode::parse("async").unwrap();
        assert_eq!(mode, ConsistencyMode::Async);
    }

    #[test]
    fn test_parse_semi_sync() {
        let mode = ConsistencyMode::parse("semi-sync:2:500").unwrap();
        assert!(matches!(
            mode,
            ConsistencyMode::SemiSync {
                num_replicas: 2,
                timeout_ms: 500
            }
        ));
    }

    #[test]
    fn test_parse_semi_sync_default_timeout() {
        let mode = ConsistencyMode::parse("semi-sync:3").unwrap();
        assert!(matches!(
            mode,
            ConsistencyMode::SemiSync {
                num_replicas: 3,
                timeout_ms: 1000
            }
        ));
    }

    #[test]
    fn test_parse_sync() {
        let mode = ConsistencyMode::parse("sync:2000").unwrap();
        assert!(matches!(mode, ConsistencyMode::Sync { timeout_ms: 2000 }));
    }

    #[test]
    fn test_display_roundtrip() {
        let modes = vec![
            ConsistencyMode::Async,
            ConsistencyMode::semi_sync_with_timeout(2, 500),
            ConsistencyMode::sync_with_timeout(2000),
        ];

        for mode in modes {
            let s = mode.to_string();
            let parsed = ConsistencyMode::parse(&s).unwrap();
            assert_eq!(mode, parsed);
        }
    }
}

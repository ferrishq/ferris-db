//! Memory tracking and eviction policies
//!
//! Tracks memory usage and implements eviction when maxmemory is reached.
//!
//! # Eviction Policies
//!
//! ferris-db supports the same eviction policies as Redis:
//!
//! - `noeviction`: Return OOM error when memory limit is reached
//! - `allkeys-lru`: Evict any key using approximate LRU
//! - `volatile-lru`: Evict keys with TTL using approximate LRU
//! - `allkeys-lfu`: Evict any key using approximate LFU
//! - `volatile-lfu`: Evict keys with TTL using approximate LFU
//! - `allkeys-random`: Evict random keys
//! - `volatile-random`: Evict random keys with TTL
//! - `volatile-ttl`: Evict keys with shortest TTL
//!
//! # Memory Tracking
//!
//! Memory is tracked approximately. Each write operation updates the global
//! counter. The tracker maintains both current and peak usage for monitoring.

use std::sync::atomic::{AtomicUsize, Ordering};

/// Memory eviction policy
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionPolicy {
    /// Return error on writes when maxmemory reached
    NoEviction,
    /// Evict any key using LRU
    AllKeysLru,
    /// Evict keys with TTL using LRU  
    VolatileLru,
    /// Evict any key using LFU
    AllKeysLfu,
    /// Evict keys with TTL using LFU
    VolatileLfu,
    /// Evict random keys
    AllKeysRandom,
    /// Evict random keys with TTL
    VolatileRandom,
    /// Evict keys with shortest TTL
    VolatileTtl,
}

impl Default for EvictionPolicy {
    fn default() -> Self {
        Self::NoEviction
    }
}

impl EvictionPolicy {
    /// Parse a policy from string (Redis config format)
    #[must_use]
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "noeviction" => Some(Self::NoEviction),
            "allkeys-lru" => Some(Self::AllKeysLru),
            "volatile-lru" => Some(Self::VolatileLru),
            "allkeys-lfu" => Some(Self::AllKeysLfu),
            "volatile-lfu" => Some(Self::VolatileLfu),
            "allkeys-random" => Some(Self::AllKeysRandom),
            "volatile-random" => Some(Self::VolatileRandom),
            "volatile-ttl" => Some(Self::VolatileTtl),
            _ => None,
        }
    }

    /// Convert policy to string (Redis config format)
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::NoEviction => "noeviction",
            Self::AllKeysLru => "allkeys-lru",
            Self::VolatileLru => "volatile-lru",
            Self::AllKeysLfu => "allkeys-lfu",
            Self::VolatileLfu => "volatile-lfu",
            Self::AllKeysRandom => "allkeys-random",
            Self::VolatileRandom => "volatile-random",
            Self::VolatileTtl => "volatile-ttl",
        }
    }

    /// Check if this policy only evicts keys with TTL (volatile)
    #[must_use]
    pub const fn is_volatile(&self) -> bool {
        matches!(
            self,
            Self::VolatileLru | Self::VolatileLfu | Self::VolatileRandom | Self::VolatileTtl
        )
    }

    /// Check if this policy uses LRU ordering
    #[must_use]
    pub const fn is_lru(&self) -> bool {
        matches!(self, Self::AllKeysLru | Self::VolatileLru)
    }

    /// Check if this policy uses LFU ordering
    #[must_use]
    pub const fn is_lfu(&self) -> bool {
        matches!(self, Self::AllKeysLfu | Self::VolatileLfu)
    }

    /// Check if this policy uses random selection
    #[must_use]
    pub const fn is_random(&self) -> bool {
        matches!(self, Self::AllKeysRandom | Self::VolatileRandom)
    }
}

/// Tracks memory usage across the entire store
#[derive(Debug)]
pub struct MemoryTracker {
    /// Current memory usage in bytes
    used: AtomicUsize,
    /// Peak memory usage in bytes (for MEMORY STATS)
    peak: AtomicUsize,
    /// Maximum allowed memory in bytes (0 = unlimited)
    max_memory: AtomicUsize,
    /// Eviction policy when max memory is reached
    policy: parking_lot::RwLock<EvictionPolicy>,
    /// Number of keys evicted (for stats)
    evicted_keys: AtomicUsize,
}

impl Default for MemoryTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryTracker {
    /// Create a new memory tracker with no limit
    #[must_use]
    pub fn new() -> Self {
        Self {
            used: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            max_memory: AtomicUsize::new(0),
            policy: parking_lot::RwLock::new(EvictionPolicy::NoEviction),
            evicted_keys: AtomicUsize::new(0),
        }
    }

    /// Create a new memory tracker with the specified limit and policy
    #[must_use]
    pub fn with_config(max_memory: usize, policy: EvictionPolicy) -> Self {
        Self {
            used: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            max_memory: AtomicUsize::new(max_memory),
            policy: parking_lot::RwLock::new(policy),
            evicted_keys: AtomicUsize::new(0),
        }
    }

    /// Get current memory usage in bytes
    #[must_use]
    pub fn used(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }

    /// Get peak memory usage in bytes
    #[must_use]
    pub fn peak(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }

    /// Get maximum memory in bytes (0 = unlimited)
    #[must_use]
    pub fn max_memory(&self) -> usize {
        self.max_memory.load(Ordering::Relaxed)
    }

    /// Set maximum memory in bytes (0 = unlimited)
    pub fn set_max_memory(&self, bytes: usize) {
        self.max_memory.store(bytes, Ordering::Relaxed);
    }

    /// Get the current eviction policy
    #[must_use]
    pub fn eviction_policy(&self) -> EvictionPolicy {
        *self.policy.read()
    }

    /// Set the eviction policy
    pub fn set_eviction_policy(&self, policy: EvictionPolicy) {
        *self.policy.write() = policy;
    }

    /// Get the number of keys evicted
    #[must_use]
    pub fn evicted_keys(&self) -> usize {
        self.evicted_keys.load(Ordering::Relaxed)
    }

    /// Record that a key was evicted
    pub fn record_eviction(&self) {
        self.evicted_keys.fetch_add(1, Ordering::Relaxed);
    }

    /// Add to memory usage and update peak if needed
    pub fn add(&self, bytes: usize) {
        let new_used = self.used.fetch_add(bytes, Ordering::Relaxed) + bytes;
        // Update peak if we've exceeded it
        let mut current_peak = self.peak.load(Ordering::Relaxed);
        while new_used > current_peak {
            match self.peak.compare_exchange_weak(
                current_peak,
                new_used,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current_peak = actual,
            }
        }
    }

    /// Subtract from memory usage
    pub fn subtract(&self, bytes: usize) {
        // Use saturating sub to prevent underflow
        let current = self.used.load(Ordering::Relaxed);
        let new_value = current.saturating_sub(bytes);
        self.used.store(new_value, Ordering::Relaxed);
    }

    /// Check if we're over the memory limit
    #[must_use]
    pub fn is_over_limit(&self) -> bool {
        let max = self.max_memory.load(Ordering::Relaxed);
        if max == 0 {
            return false; // No limit
        }
        self.used.load(Ordering::Relaxed) > max
    }

    /// Check if we need to evict before adding more data
    #[must_use]
    pub fn should_evict(&self, additional_bytes: usize) -> bool {
        let max = self.max_memory.load(Ordering::Relaxed);
        if max == 0 {
            return false; // No limit
        }
        self.used.load(Ordering::Relaxed) + additional_bytes > max
    }

    /// Reset stats (for CONFIG RESETSTAT)
    pub fn reset_stats(&self) {
        self.peak
            .store(self.used.load(Ordering::Relaxed), Ordering::Relaxed);
        self.evicted_keys.store(0, Ordering::Relaxed);
    }

    /// Calculate the amount of memory that needs to be freed
    #[must_use]
    pub fn bytes_to_free(&self, additional_bytes: usize) -> usize {
        let max = self.max_memory.load(Ordering::Relaxed);
        if max == 0 {
            return 0; // No limit
        }
        let needed = self.used.load(Ordering::Relaxed) + additional_bytes;
        needed.saturating_sub(max)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_tracker_creation() {
        let tracker = MemoryTracker::new();
        assert_eq!(tracker.used(), 0);
        assert_eq!(tracker.peak(), 0);
        assert_eq!(tracker.max_memory(), 0);
        assert_eq!(tracker.eviction_policy(), EvictionPolicy::NoEviction);
        assert_eq!(tracker.evicted_keys(), 0);
    }

    #[test]
    fn test_memory_tracker_with_config() {
        let tracker = MemoryTracker::with_config(1024 * 1024, EvictionPolicy::AllKeysLru);
        assert_eq!(tracker.max_memory(), 1024 * 1024);
        assert_eq!(tracker.eviction_policy(), EvictionPolicy::AllKeysLru);
    }

    #[test]
    fn test_memory_add_subtract() {
        let tracker = MemoryTracker::new();

        tracker.add(1000);
        assert_eq!(tracker.used(), 1000);

        tracker.add(500);
        assert_eq!(tracker.used(), 1500);

        tracker.subtract(300);
        assert_eq!(tracker.used(), 1200);
    }

    #[test]
    fn test_peak_memory_tracking() {
        let tracker = MemoryTracker::new();

        tracker.add(1000);
        assert_eq!(tracker.peak(), 1000);

        tracker.add(500);
        assert_eq!(tracker.peak(), 1500);

        // Peak should not decrease when we subtract
        tracker.subtract(1000);
        assert_eq!(tracker.used(), 500);
        assert_eq!(tracker.peak(), 1500);

        // But should increase if we exceed it again
        tracker.add(2000);
        assert_eq!(tracker.peak(), 2500);
    }

    #[test]
    fn test_subtract_underflow_protection() {
        let tracker = MemoryTracker::new();
        tracker.add(100);
        tracker.subtract(200); // Try to subtract more than we have
        assert_eq!(tracker.used(), 0); // Should saturate at 0, not underflow
    }

    #[test]
    fn test_memory_limit() {
        let tracker = MemoryTracker::new();
        tracker.set_max_memory(1000);

        assert!(!tracker.is_over_limit());

        tracker.add(1500);
        assert!(tracker.is_over_limit());
    }

    #[test]
    fn test_should_evict() {
        let tracker = MemoryTracker::new();
        tracker.set_max_memory(1000);
        tracker.add(800);

        assert!(!tracker.should_evict(100)); // 800 + 100 = 900 < 1000
        assert!(tracker.should_evict(300)); // 800 + 300 = 1100 > 1000
    }

    #[test]
    fn test_bytes_to_free() {
        let tracker = MemoryTracker::new();
        tracker.set_max_memory(1000);
        tracker.add(800);

        assert_eq!(tracker.bytes_to_free(100), 0); // 900 < 1000, nothing to free
        assert_eq!(tracker.bytes_to_free(300), 100); // 1100 - 1000 = 100 to free
        assert_eq!(tracker.bytes_to_free(500), 300); // 1300 - 1000 = 300 to free
    }

    #[test]
    fn test_no_limit() {
        let tracker = MemoryTracker::new();
        // max_memory = 0 means unlimited
        tracker.add(1_000_000_000);
        assert!(!tracker.is_over_limit());
        assert!(!tracker.should_evict(1_000_000_000));
        assert_eq!(tracker.bytes_to_free(1_000_000_000), 0);
    }

    #[test]
    fn test_eviction_policy() {
        let tracker = MemoryTracker::new();

        tracker.set_eviction_policy(EvictionPolicy::AllKeysLru);
        assert_eq!(tracker.eviction_policy(), EvictionPolicy::AllKeysLru);

        tracker.set_eviction_policy(EvictionPolicy::VolatileTtl);
        assert_eq!(tracker.eviction_policy(), EvictionPolicy::VolatileTtl);
    }

    #[test]
    fn test_evicted_keys_counter() {
        let tracker = MemoryTracker::new();
        assert_eq!(tracker.evicted_keys(), 0);

        tracker.record_eviction();
        assert_eq!(tracker.evicted_keys(), 1);

        tracker.record_eviction();
        tracker.record_eviction();
        assert_eq!(tracker.evicted_keys(), 3);
    }

    #[test]
    fn test_reset_stats() {
        let tracker = MemoryTracker::new();
        tracker.add(1000);
        tracker.add(500); // peak = 1500
        tracker.subtract(500); // used = 1000, peak still 1500
        tracker.record_eviction();
        tracker.record_eviction();

        assert_eq!(tracker.peak(), 1500);
        assert_eq!(tracker.evicted_keys(), 2);

        tracker.reset_stats();

        // Peak should be reset to current used
        assert_eq!(tracker.peak(), 1000);
        assert_eq!(tracker.evicted_keys(), 0);
    }

    #[test]
    fn test_policy_from_str() {
        assert_eq!(
            EvictionPolicy::from_str("noeviction"),
            Some(EvictionPolicy::NoEviction)
        );
        assert_eq!(
            EvictionPolicy::from_str("allkeys-lru"),
            Some(EvictionPolicy::AllKeysLru)
        );
        assert_eq!(
            EvictionPolicy::from_str("volatile-lru"),
            Some(EvictionPolicy::VolatileLru)
        );
        assert_eq!(
            EvictionPolicy::from_str("allkeys-lfu"),
            Some(EvictionPolicy::AllKeysLfu)
        );
        assert_eq!(
            EvictionPolicy::from_str("volatile-lfu"),
            Some(EvictionPolicy::VolatileLfu)
        );
        assert_eq!(
            EvictionPolicy::from_str("allkeys-random"),
            Some(EvictionPolicy::AllKeysRandom)
        );
        assert_eq!(
            EvictionPolicy::from_str("volatile-random"),
            Some(EvictionPolicy::VolatileRandom)
        );
        assert_eq!(
            EvictionPolicy::from_str("volatile-ttl"),
            Some(EvictionPolicy::VolatileTtl)
        );
        assert_eq!(
            EvictionPolicy::from_str("ALLKEYS-LRU"),
            Some(EvictionPolicy::AllKeysLru)
        ); // case insensitive
        assert_eq!(EvictionPolicy::from_str("invalid"), None);
    }

    #[test]
    fn test_policy_as_str() {
        assert_eq!(EvictionPolicy::NoEviction.as_str(), "noeviction");
        assert_eq!(EvictionPolicy::AllKeysLru.as_str(), "allkeys-lru");
        assert_eq!(EvictionPolicy::VolatileLru.as_str(), "volatile-lru");
        assert_eq!(EvictionPolicy::AllKeysLfu.as_str(), "allkeys-lfu");
        assert_eq!(EvictionPolicy::VolatileLfu.as_str(), "volatile-lfu");
        assert_eq!(EvictionPolicy::AllKeysRandom.as_str(), "allkeys-random");
        assert_eq!(EvictionPolicy::VolatileRandom.as_str(), "volatile-random");
        assert_eq!(EvictionPolicy::VolatileTtl.as_str(), "volatile-ttl");
    }

    #[test]
    fn test_policy_is_volatile() {
        assert!(!EvictionPolicy::NoEviction.is_volatile());
        assert!(!EvictionPolicy::AllKeysLru.is_volatile());
        assert!(EvictionPolicy::VolatileLru.is_volatile());
        assert!(!EvictionPolicy::AllKeysLfu.is_volatile());
        assert!(EvictionPolicy::VolatileLfu.is_volatile());
        assert!(!EvictionPolicy::AllKeysRandom.is_volatile());
        assert!(EvictionPolicy::VolatileRandom.is_volatile());
        assert!(EvictionPolicy::VolatileTtl.is_volatile());
    }

    #[test]
    fn test_policy_type_checks() {
        assert!(EvictionPolicy::AllKeysLru.is_lru());
        assert!(EvictionPolicy::VolatileLru.is_lru());
        assert!(!EvictionPolicy::AllKeysLfu.is_lru());

        assert!(EvictionPolicy::AllKeysLfu.is_lfu());
        assert!(EvictionPolicy::VolatileLfu.is_lfu());
        assert!(!EvictionPolicy::AllKeysLru.is_lfu());

        assert!(EvictionPolicy::AllKeysRandom.is_random());
        assert!(EvictionPolicy::VolatileRandom.is_random());
        assert!(!EvictionPolicy::VolatileTtl.is_random());
    }
}

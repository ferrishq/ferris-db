//! HyperLogLog commands: PFADD, PFCOUNT, PFMERGE
//!
//! HyperLogLog is a probabilistic data structure for cardinality estimation.
//! It can estimate the number of unique elements with ~0.81% standard error
//! while using only 12KB of memory.

use crate::{CommandContext, CommandError, CommandResult};
use bytes::Bytes;
use ferris_core::{Entry, RedisValue};
use ferris_protocol::RespValue;

/// Helper to get bytes from RespValue
fn get_bytes(arg: &RespValue) -> Result<Bytes, CommandError> {
    arg.as_bytes()
        .cloned()
        .or_else(|| arg.as_str().map(|s| Bytes::from(s.to_owned())))
        .ok_or_else(|| CommandError::InvalidArgument("invalid argument".to_string()))
}

/// Simple HyperLogLog implementation
/// Uses 16384 registers (2^14) for ~0.81% standard error
const HLL_REGISTERS: usize = 16384; // 2^14
const HLL_BITS: u32 = 14; // log2(16384)

/// HyperLogLog data structure
#[derive(Clone, Debug)]
pub struct HyperLogLog {
    registers: Vec<u8>,
}

impl HyperLogLog {
    pub fn new() -> Self {
        Self {
            registers: vec![0; HLL_REGISTERS],
        }
    }

    pub fn from_bytes(data: &[u8]) -> Self {
        if data.len() == HLL_REGISTERS {
            Self {
                registers: data.to_vec(),
            }
        } else {
            Self::new()
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        Bytes::from(self.registers.clone())
    }

    /// Add an element to the HyperLogLog
    /// Returns true if the register was modified
    pub fn add(&mut self, element: &[u8]) -> bool {
        let hash = Self::hash(element);
        let index = (hash & ((1 << HLL_BITS) - 1)) as usize;
        let remaining = hash >> HLL_BITS;
        let rho = (64 - HLL_BITS - remaining.leading_zeros()).min(255) as u8;

        if rho > self.registers[index] {
            self.registers[index] = rho;
            true
        } else {
            false
        }
    }

    /// Estimate the cardinality
    pub fn count(&self) -> u64 {
        let m = HLL_REGISTERS as f64;
        let alpha = 0.7213 / (1.0 + 1.079 / m);

        // Calculate raw estimate
        let raw_estimate: f64 = self
            .registers
            .iter()
            .map(|&reg| 2.0_f64.powi(-(reg as i32)))
            .sum();
        let raw_estimate = alpha * m * m / raw_estimate;

        // Apply bias correction
        let estimate = if raw_estimate <= 2.5 * m {
            // Small range correction
            let zeros = self.registers.iter().filter(|&&r| r == 0).count();
            if zeros > 0 {
                m * (m / zeros as f64).ln()
            } else {
                raw_estimate
            }
        } else if raw_estimate <= (1.0 / 30.0) * (1_u64 << 32) as f64 {
            // Intermediate range - no correction
            raw_estimate
        } else {
            // Large range correction
            -((1_u64 << 32) as f64) * (1.0 - raw_estimate / ((1_u64 << 32) as f64)).ln()
        };

        estimate.round() as u64
    }

    /// Merge another HyperLogLog into this one
    pub fn merge(&mut self, other: &HyperLogLog) {
        for (i, &other_reg) in other.registers.iter().enumerate() {
            if other_reg > self.registers[i] {
                self.registers[i] = other_reg;
            }
        }
    }

    /// Simple hash function (FNV-1a)
    fn hash(data: &[u8]) -> u64 {
        let mut hash = 0xcbf29ce484222325_u64;
        for &byte in data {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash
    }
}

/// PFADD key element [element ...]
///
/// Adds elements to a HyperLogLog.
/// Returns 1 if the HyperLogLog was modified, 0 otherwise.
///
/// Time complexity: O(N) where N is the number of elements
pub fn pfadd(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("PFADD".to_string()));
    }

    let key = get_bytes(&args[0])?;
    let db = ctx.store().database(ctx.selected_db());

    let mut hll = match db.get(&key) {
        Some(entry) => {
            if entry.is_expired() {
                db.delete(&key);
                HyperLogLog::new()
            } else {
                match &entry.value {
                    RedisValue::String(s) => HyperLogLog::from_bytes(s),
                    _ => return Err(CommandError::WrongType),
                }
            }
        }
        None => HyperLogLog::new(),
    };

    let mut modified = false;
    for arg in &args[1..] {
        let element = get_bytes(arg)?;
        if hll.add(&element) {
            modified = true;
        }
    }

    if modified {
        db.set(key.clone(), Entry::new(RedisValue::String(hll.to_bytes())));

        // Propagate to AOF and replication
        ctx.propagate_args(args);
    }

    Ok(RespValue::Integer(if modified { 1 } else { 0 }))
}

/// PFCOUNT key [key ...]
///
/// Returns the approximated cardinality of the set(s) observed by the HyperLogLog(s).
///
/// Time complexity: O(N) where N is the number of keys
pub fn pfcount(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongArity("PFCOUNT".to_string()));
    }

    let db = ctx.store().database(ctx.selected_db());

    if args.len() == 1 {
        // Single key - simple case
        let key = get_bytes(&args[0])?;
        match db.get(&key) {
            Some(entry) => {
                if entry.is_expired() {
                    db.delete(&key);
                    return Ok(RespValue::Integer(0));
                }
                match &entry.value {
                    RedisValue::String(s) => {
                        let hll = HyperLogLog::from_bytes(s);
                        Ok(RespValue::Integer(hll.count() as i64))
                    }
                    _ => Err(CommandError::WrongType),
                }
            }
            None => Ok(RespValue::Integer(0)),
        }
    } else {
        // Multiple keys - merge and count
        let mut merged = HyperLogLog::new();
        for arg in args {
            let key = get_bytes(arg)?;
            if let Some(entry) = db.get(&key) {
                if !entry.is_expired() {
                    match &entry.value {
                        RedisValue::String(s) => {
                            let hll = HyperLogLog::from_bytes(s);
                            merged.merge(&hll);
                        }
                        _ => return Err(CommandError::WrongType),
                    }
                }
            }
        }
        Ok(RespValue::Integer(merged.count() as i64))
    }
}

/// PFMERGE destkey sourcekey [sourcekey ...]
///
/// Merges multiple HyperLogLog values into a single one.
///
/// Time complexity: O(N) where N is the number of source keys
pub fn pfmerge(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("PFMERGE".to_string()));
    }

    let destkey = get_bytes(&args[0])?;
    let db = ctx.store().database(ctx.selected_db());

    let mut merged = HyperLogLog::new();

    for arg in &args[1..] {
        let key = get_bytes(arg)?;
        if let Some(entry) = db.get(&key) {
            if !entry.is_expired() {
                match &entry.value {
                    RedisValue::String(s) => {
                        let hll = HyperLogLog::from_bytes(s);
                        merged.merge(&hll);
                    }
                    _ => return Err(CommandError::WrongType),
                }
            }
        }
    }

    db.set(
        destkey.clone(),
        Entry::new(RedisValue::String(merged.to_bytes())),
    );

    // Propagate to AOF and replication
    ctx.propagate_args(args);

    Ok(RespValue::ok())
}

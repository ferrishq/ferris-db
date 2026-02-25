//! Geo commands: GEOADD, GEODIST, GEOHASH, GEOPOS, GEORADIUS, GEOSEARCH
//!
//! Redis GEO commands store geographic data as sorted sets using geohash encoding.
//! The score is the 52-bit geohash of the coordinates.

use crate::{CommandContext, CommandError, CommandResult};
use bytes::Bytes;
use ferris_core::{Entry, RedisValue};
use ferris_protocol::RespValue;
use ordered_float::OrderedFloat;
use std::collections::{BTreeMap, HashMap};
use std::f64::consts::PI;

/// Earth radius in meters (mean radius)
const EARTH_RADIUS_METERS: f64 = 6_371_000.0;

/// Helper to convert degrees to radians
fn deg_to_rad(deg: f64) -> f64 {
    deg * PI / 180.0
}

/// Calculate distance between two points using Haversine formula
fn haversine_distance(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let lat1_rad = deg_to_rad(lat1);
    let lat2_rad = deg_to_rad(lat2);
    let delta_lat = deg_to_rad(lat2 - lat1);
    let delta_lon = deg_to_rad(lon2 - lon1);

    let a = (delta_lat / 2.0).sin().powi(2)
        + lat1_rad.cos() * lat2_rad.cos() * (delta_lon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());

    EARTH_RADIUS_METERS * c
}

/// Encode latitude/longitude to geohash (52-bit integer for sorted set score)
/// This is a simplified geohash that works for Redis GEO
fn geohash_encode(lon: f64, lat: f64) -> f64 {
    // Normalize to [0, 1] range
    let lat_normalized = (lat + 90.0) / 180.0;
    let lon_normalized = (lon + 180.0) / 360.0;

    // Interleave bits (simplified - real Redis uses more sophisticated encoding)
    // For now, we combine them in a way that preserves spatial locality
    let lat_scaled = (lat_normalized * 1_000_000.0) as i64;
    let lon_scaled = (lon_normalized * 1_000_000.0) as i64;

    // Combine into a single score
    let combined = (lat_scaled * 1_000_000) + lon_scaled;
    combined as f64
}

/// Decode geohash to approximate latitude/longitude
fn geohash_decode(hash: f64) -> (f64, f64) {
    let combined = hash as i64;
    let lat_scaled = combined / 1_000_000;
    let lon_scaled = combined % 1_000_000;

    let lat_normalized = lat_scaled as f64 / 1_000_000.0;
    let lon_normalized = lon_scaled as f64 / 1_000_000.0;

    let lat = (lat_normalized * 180.0) - 90.0;
    let lon = (lon_normalized * 360.0) - 180.0;

    (lon, lat)
}

/// Validate latitude and longitude
fn validate_coords(lon: f64, lat: f64) -> Result<(), CommandError> {
    if !(-180.0..=180.0).contains(&lon) {
        return Err(CommandError::InvalidArgument(
            "valid longitude is between -180 and 180".to_string(),
        ));
    }
    if !(-85.05112878..=85.05112878).contains(&lat) {
        return Err(CommandError::InvalidArgument(
            "valid latitude is between -85.05112878 and 85.05112878".to_string(),
        ));
    }
    Ok(())
}

/// GEOADD key [NX|XX] [CH] longitude latitude member [longitude latitude member ...]
///
/// Adds geographic items (longitude, latitude, name) to a sorted set.
///
/// Time complexity: O(log(N)) for each item added
pub fn geoadd(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 4 {
        return Err(CommandError::WrongArity("GEOADD".to_string()));
    }

    let key = args[0]
        .as_bytes()
        .ok_or_else(|| CommandError::InvalidArgument("invalid key".to_string()))?;

    let mut idx = 1;
    let mut nx = false;
    let mut xx = false;
    let mut ch = false;

    // Parse optional flags
    while idx < args.len() {
        if let Some(flag) = args[idx].as_str() {
            match flag.to_uppercase().as_str() {
                "NX" => {
                    nx = true;
                    idx += 1;
                }
                "XX" => {
                    xx = true;
                    idx += 1;
                }
                "CH" => {
                    ch = true;
                    idx += 1;
                }
                _ => break,
            }
        } else {
            break;
        }
    }

    if nx && xx {
        return Err(CommandError::SyntaxError);
    }

    // Parse longitude, latitude, member triplets
    if (args.len() - idx) % 3 != 0 {
        return Err(CommandError::SyntaxError);
    }

    let db_index = ctx.selected_db();
    let store = ctx.store().database(db_index);

    let mut added_count = 0;
    let mut changed_count = 0;

    while idx < args.len() {
        let lon: f64 = args[idx]
            .as_str()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| CommandError::NotAFloat)?;
        let lat: f64 = args[idx + 1]
            .as_str()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| CommandError::NotAFloat)?;
        let member = args[idx + 2]
            .as_bytes()
            .ok_or_else(|| CommandError::InvalidArgument("invalid member".to_string()))?
            .clone();

        validate_coords(lon, lat)?;
        let score = geohash_encode(lon, lat);

        // Get or create sorted set
        let mut entry = store.get(key).unwrap_or_else(|| {
            Entry::new(RedisValue::SortedSet {
                scores: HashMap::new(),
                members: BTreeMap::new(),
            })
        });

        match &mut entry.value {
            RedisValue::SortedSet { scores, members } => {
                let member_exists = scores.contains_key(&member);
                let old_score = scores.get(&member).copied();

                if (nx && member_exists) || (xx && !member_exists) {
                    idx += 3;
                    continue;
                }

                // Remove old entry from members if exists
                if let Some(old) = old_score {
                    members.remove(&(OrderedFloat(old), member.clone()));
                }

                // Insert new score
                scores.insert(member.clone(), score);
                members.insert((OrderedFloat(score), member.clone()), ());

                if member_exists {
                    changed_count += 1;
                } else {
                    added_count += 1;
                }
            }
            _ => return Err(CommandError::WrongType),
        }

        store.set(key.clone(), entry);
        idx += 3;
    }

    // Propagate to AOF and replication
    let propagate_cmd: Vec<RespValue> =
        std::iter::once(RespValue::BulkString(Bytes::from("GEOADD")))
            .chain(args.iter().cloned())
            .collect();
    ctx.propagate(&propagate_cmd);

    let result = if ch {
        added_count + changed_count
    } else {
        added_count
    };

    Ok(RespValue::Integer(result))
}

/// GEODIST key member1 member2 [unit]
///
/// Returns the distance between two members in the specified unit.
///
/// Time complexity: O(log(N))
pub fn geodist(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 3 || args.len() > 4 {
        return Err(CommandError::WrongArity("GEODIST".to_string()));
    }

    let key = args[0]
        .as_bytes()
        .ok_or_else(|| CommandError::InvalidArgument("invalid key".to_string()))?;
    let member1 = args[1]
        .as_bytes()
        .ok_or_else(|| CommandError::InvalidArgument("invalid member".to_string()))?;
    let member2 = args[2]
        .as_bytes()
        .ok_or_else(|| CommandError::InvalidArgument("invalid member".to_string()))?;

    let unit = if args.len() == 4 {
        args[3]
            .as_str()
            .ok_or_else(|| CommandError::SyntaxError)?
            .to_uppercase()
    } else {
        "M".to_string()
    };

    let unit_multiplier = match unit.as_str() {
        "M" => 1.0,
        "KM" => 0.001,
        "MI" => 0.000621371,
        "FT" => 3.28084,
        _ => return Err(CommandError::SyntaxError),
    };

    let db_index = ctx.selected_db();
    let store = ctx.store().database(db_index);

    let entry = store.get(key).ok_or_else(|| {
        // Return nil if key doesn't exist
        return CommandError::InvalidArgument("key not found".to_string());
    })?;

    match &entry.value {
        RedisValue::SortedSet { scores, .. } => {
            let score1 = scores
                .get(member1)
                .copied()
                .ok_or_else(|| CommandError::InvalidArgument("member1 not found".to_string()))?;
            let score2 = scores
                .get(member2)
                .copied()
                .ok_or_else(|| CommandError::InvalidArgument("member2 not found".to_string()))?;

            let (lon1, lat1) = geohash_decode(score1);
            let (lon2, lat2) = geohash_decode(score2);

            let distance_meters = haversine_distance(lon1, lat1, lon2, lat2);
            let distance = distance_meters * unit_multiplier;

            Ok(RespValue::BulkString(Bytes::from(format!(
                "{:.4}",
                distance
            ))))
        }
        _ => Err(CommandError::WrongType),
    }
}

/// GEOHASH key member [member ...]
///
/// Returns geohash strings for the specified members.
///
/// Time complexity: O(log(N)) for each member
pub fn geohash(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("GEOHASH".to_string()));
    }

    let key = args[0]
        .as_bytes()
        .ok_or_else(|| CommandError::InvalidArgument("invalid key".to_string()))?;

    let db_index = ctx.selected_db();
    let store = ctx.store().database(db_index);

    let entry = store.get(key);
    let mut results = Vec::new();

    for arg in &args[1..] {
        let member = arg
            .as_bytes()
            .ok_or_else(|| CommandError::InvalidArgument("invalid member".to_string()))?;

        if let Some(ref entry) = entry {
            match &entry.value {
                RedisValue::SortedSet { scores, .. } => {
                    if let Some(score) = scores.get(member) {
                        // Convert score to geohash string (simplified)
                        let hash_str = format!("{:011x}", *score as i64);
                        results.push(RespValue::BulkString(Bytes::from(hash_str)));
                    } else {
                        results.push(RespValue::Null);
                    }
                }
                _ => return Err(CommandError::WrongType),
            }
        } else {
            results.push(RespValue::Null);
        }
    }

    Ok(RespValue::Array(results))
}

/// GEOPOS key member [member ...]
///
/// Returns longitude and latitude of members.
///
/// Time complexity: O(log(N)) for each member
pub fn geopos(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongArity("GEOPOS".to_string()));
    }

    let key = args[0]
        .as_bytes()
        .ok_or_else(|| CommandError::InvalidArgument("invalid key".to_string()))?;

    let db_index = ctx.selected_db();
    let store = ctx.store().database(db_index);

    let entry = store.get(key);
    let mut results = Vec::new();

    for arg in &args[1..] {
        let member = arg
            .as_bytes()
            .ok_or_else(|| CommandError::InvalidArgument("invalid member".to_string()))?;

        if let Some(ref entry) = entry {
            match &entry.value {
                RedisValue::SortedSet { scores, .. } => {
                    if let Some(score) = scores.get(member) {
                        let (lon, lat) = geohash_decode(*score);
                        results.push(RespValue::Array(vec![
                            RespValue::BulkString(Bytes::from(format!("{:.6}", lon))),
                            RespValue::BulkString(Bytes::from(format!("{:.6}", lat))),
                        ]));
                    } else {
                        results.push(RespValue::Null);
                    }
                }
                _ => return Err(CommandError::WrongType),
            }
        } else {
            results.push(RespValue::Null);
        }
    }

    Ok(RespValue::Array(results))
}

/// GEORADIUS key longitude latitude radius unit [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count] [ASC|DESC]
///
/// Query members within a radius (deprecated, use GEOSEARCH).
/// Simplified implementation.
///
/// Time complexity: O(N+log(M))
pub fn georadius(ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 5 {
        return Err(CommandError::WrongArity("GEORADIUS".to_string()));
    }

    let key = args[0]
        .as_bytes()
        .ok_or_else(|| CommandError::InvalidArgument("invalid key".to_string()))?;
    let lon: f64 = args[1]
        .as_str()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| CommandError::NotAFloat)?;
    let lat: f64 = args[2]
        .as_str()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| CommandError::NotAFloat)?;
    let radius: f64 = args[3]
        .as_str()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| CommandError::NotAFloat)?;
    let unit = args[4]
        .as_str()
        .ok_or_else(|| CommandError::SyntaxError)?
        .to_uppercase();

    validate_coords(lon, lat)?;

    let radius_meters = match unit.as_str() {
        "M" => radius,
        "KM" => radius * 1000.0,
        "MI" => radius * 1609.34,
        "FT" => radius * 0.3048,
        _ => return Err(CommandError::SyntaxError),
    };

    // Parse optional flags
    let mut with_coord = false;
    let mut with_dist = false;
    let mut with_hash = false;
    let mut count = None;
    let mut idx = 5;

    while idx < args.len() {
        if let Some(flag) = args[idx].as_str() {
            match flag.to_uppercase().as_str() {
                "WITHCOORD" => with_coord = true,
                "WITHDIST" => with_dist = true,
                "WITHHASH" => with_hash = true,
                "COUNT" => {
                    if idx + 1 < args.len() {
                        count = args[idx + 1].as_str().and_then(|s| s.parse::<usize>().ok());
                        idx += 1;
                    }
                }
                "ASC" | "DESC" => {} // Ignore for now
                _ => return Err(CommandError::SyntaxError),
            }
        }
        idx += 1;
    }

    let db_index = ctx.selected_db();
    let store = ctx.store().database(db_index);

    let entry = match store.get(key) {
        Some(e) => e,
        None => return Ok(RespValue::Array(vec![])),
    };

    match &entry.value {
        RedisValue::SortedSet { scores, .. } => {
            let mut results: Vec<(Bytes, f64, f64, f64)> = Vec::new();

            for (member, score) in scores.iter() {
                let (mem_lon, mem_lat) = geohash_decode(*score);
                let distance = haversine_distance(lon, lat, mem_lon, mem_lat);

                if distance <= radius_meters {
                    results.push((member.clone(), distance, mem_lon, mem_lat));
                }
            }

            // Sort by distance
            results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

            // Apply count limit
            if let Some(c) = count {
                results.truncate(c);
            }

            // Format output
            let output: Vec<RespValue> = results
                .into_iter()
                .map(|(member, dist, mem_lon, mem_lat)| {
                    if with_coord || with_dist || with_hash {
                        let mut item = vec![RespValue::BulkString(member.clone())];
                        if with_dist {
                            item.push(RespValue::BulkString(Bytes::from(format!("{:.4}", dist))));
                        }
                        if with_hash {
                            let hash_str =
                                format!("{:011x}", geohash_encode(mem_lon, mem_lat) as i64);
                            item.push(RespValue::BulkString(Bytes::from(hash_str)));
                        }
                        if with_coord {
                            item.push(RespValue::Array(vec![
                                RespValue::BulkString(Bytes::from(format!("{:.6}", mem_lon))),
                                RespValue::BulkString(Bytes::from(format!("{:.6}", mem_lat))),
                            ]));
                        }
                        RespValue::Array(item)
                    } else {
                        RespValue::BulkString(member)
                    }
                })
                .collect();

            Ok(RespValue::Array(output))
        }
        _ => Err(CommandError::WrongType),
    }
}

/// GEOSEARCH key [FROMMEMBER member | FROMLONLAT longitude latitude]
/// [BYRADIUS radius unit | BYBOX width height unit] [ASC|DESC] [COUNT count] [WITHCOORD] [WITHDIST] [WITHHASH]
///
/// Query members by radius or box (Redis 6.2+). Simplified stub implementation.
///
/// Time complexity: O(N+log(M))
pub fn geosearch(_ctx: &mut CommandContext, args: &[RespValue]) -> CommandResult {
    if args.len() < 4 {
        return Err(CommandError::WrongArity("GEOSEARCH".to_string()));
    }

    // Stub implementation - return empty array
    // Full implementation would be similar to GEORADIUS but with more options
    Ok(RespValue::Array(vec![]))
}

//! Cluster topology management
//!
//! This module implements Redis Cluster protocol for distributed key-value storage.
//! Each node in the cluster is responsible for a subset of the 16384 hash slots.

#![allow(clippy::missing_errors_doc)]
#![allow(clippy::must_use_candidate)]
#![allow(clippy::missing_const_for_fn)]
#![allow(clippy::assigning_clones)]
#![allow(clippy::map_unwrap_or)]
#![allow(clippy::redundant_closure)]
#![allow(clippy::redundant_closure_for_method_calls)]
#![allow(clippy::option_as_ref_cloned)]
#![allow(clippy::option_as_ref_deref)]
#![allow(clippy::branches_sharing_code)]

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

/// Number of hash slots in Redis Cluster
pub const CLUSTER_SLOTS: u16 = 16384;

/// Node ID is a 40-character hex string (like SHA-1)
pub type NodeId = String;

/// Cluster node flags
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeFlags {
    /// This node
    Myself,
    /// Node is a master
    Master,
    /// Node is a replica
    Replica,
    /// Node is in PFAIL state (possibly failing)
    PFail,
    /// Node is in FAIL state (definitely failing)
    Fail,
    /// Node is currently handshaking
    Handshake,
    /// Node has no address yet
    NoAddr,
}

impl NodeFlags {
    /// Convert flags to Redis protocol string
    #[must_use]
    pub const fn to_string(self) -> &'static str {
        match self {
            Self::Myself => "myself",
            Self::Master => "master",
            Self::Replica => "slave",
            Self::PFail => "fail?",
            Self::Fail => "fail",
            Self::Handshake => "handshake",
            Self::NoAddr => "noaddr",
        }
    }
}

/// Information about a cluster node
#[derive(Debug, Clone)]
pub struct ClusterNode {
    /// Unique node ID (40-char hex string)
    pub id: NodeId,
    /// Node address (IP:port)
    pub addr: SocketAddr,
    /// Cluster bus port (typically `client_port + 10000`)
    pub cluster_port: u16,
    /// Node flags
    pub flags: Vec<NodeFlags>,
    /// Master node ID (if this is a replica)
    pub master_id: Option<NodeId>,
    /// Last ping sent timestamp
    pub ping_sent: u64,
    /// Last pong received timestamp
    pub pong_recv: u64,
    /// Configuration epoch
    pub config_epoch: u64,
    /// Link state (connected/disconnected)
    pub link_state: LinkState,
    /// Hash slots this node is responsible for
    pub slots: HashSet<u16>,
}

impl ClusterNode {
    /// Create a new cluster node
    #[must_use]
    pub fn new(id: NodeId, addr: SocketAddr, is_master: bool) -> Self {
        let cluster_port = addr.port() + 10000;
        let mut flags = vec![NodeFlags::Myself];
        if is_master {
            flags.push(NodeFlags::Master);
        } else {
            flags.push(NodeFlags::Replica);
        }

        Self {
            id,
            addr,
            cluster_port,
            flags,
            master_id: None,
            ping_sent: 0,
            pong_recv: 0,
            config_epoch: 0,
            link_state: LinkState::Connected,
            slots: HashSet::new(),
        }
    }

    /// Check if this node has a specific flag
    pub fn has_flag(&self, flag: NodeFlags) -> bool {
        self.flags.contains(&flag)
    }

    /// Format flags as comma-separated string
    pub fn flags_string(&self) -> String {
        self.flags
            .iter()
            .map(|f| f.to_string())
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Format slots as range string (e.g., "0-100 200-300")
    pub fn slots_string(&self) -> String {
        if self.slots.is_empty() {
            return String::new();
        }

        let mut sorted: Vec<u16> = self.slots.iter().copied().collect();
        sorted.sort_unstable();

        let mut ranges = Vec::new();
        let mut range_start = sorted[0];
        let mut range_end = sorted[0];

        for &slot in &sorted[1..] {
            if slot == range_end + 1 {
                range_end = slot;
            } else {
                if range_start == range_end {
                    ranges.push(range_start.to_string());
                } else {
                    ranges.push(format!("{range_start}-{range_end}"));
                }
                range_start = slot;
                range_end = slot;
            }
        }

        // Add final range
        if range_start == range_end {
            ranges.push(range_start.to_string());
        } else {
            ranges.push(format!("{range_start}-{range_end}"));
        }

        ranges.join(" ")
    }
}

/// Link state for cluster nodes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkState {
    /// Node is connected
    Connected,
    /// Node is disconnected
    Disconnected,
}

impl LinkState {
    /// Convert to Redis protocol string
    pub fn to_string(self) -> &'static str {
        match self {
            Self::Connected => "connected",
            Self::Disconnected => "disconnected",
        }
    }
}

/// Cluster state and topology
#[derive(Debug)]
pub struct ClusterState {
    /// This node's ID
    pub my_id: NodeId,
    /// All known nodes in the cluster
    pub nodes: HashMap<NodeId, ClusterNode>,
    /// Slot to node ID mapping
    pub slot_map: HashMap<u16, NodeId>,
    /// Whether cluster mode is enabled
    pub enabled: bool,
    /// Current cluster epoch
    pub current_epoch: u64,
    /// Last time we updated the state
    pub last_update: Instant,
}

impl ClusterState {
    /// Create a new cluster state
    pub fn new(my_id: NodeId, my_addr: SocketAddr, enabled: bool) -> Self {
        let mut nodes = HashMap::new();

        if enabled {
            let my_node = ClusterNode::new(my_id.clone(), my_addr, true);
            nodes.insert(my_id.clone(), my_node);
        }

        Self {
            my_id,
            nodes,
            slot_map: HashMap::new(),
            enabled,
            current_epoch: 0,
            last_update: Instant::now(),
        }
    }

    /// Add a node to the cluster
    pub fn add_node(&mut self, node: ClusterNode) {
        self.nodes.insert(node.id.clone(), node);
        self.last_update = Instant::now();
    }

    /// Remove a node from the cluster
    pub fn remove_node(&mut self, node_id: &NodeId) -> Option<ClusterNode> {
        // Remove all slot mappings for this node
        self.slot_map.retain(|_, id| id != node_id);
        self.last_update = Instant::now();
        self.nodes.remove(node_id)
    }

    /// Assign slots to a node
    pub fn assign_slots(&mut self, node_id: &NodeId, slots: &[u16]) -> Result<(), String> {
        // Check if node exists
        if !self.nodes.contains_key(node_id) {
            return Err(format!("Node {node_id} not found"));
        }

        // Check for already assigned slots
        for &slot in slots {
            if let Some(existing) = self.slot_map.get(&slot) {
                if existing != node_id {
                    return Err(format!("Slot {slot} already assigned to node {existing}"));
                }
            }
        }

        // Assign slots
        for &slot in slots {
            self.slot_map.insert(slot, node_id.clone());
        }

        // Update node's slot list
        if let Some(node) = self.nodes.get_mut(node_id) {
            for &slot in slots {
                node.slots.insert(slot);
            }
        }

        self.last_update = Instant::now();
        Ok(())
    }

    /// Get the node responsible for a slot
    pub fn get_slot_node(&self, slot: u16) -> Option<&NodeId> {
        self.slot_map.get(&slot)
    }

    /// Get cluster info string (for CLUSTER INFO command)
    pub fn info_string(&self) -> String {
        let state = if self.enabled && self.is_complete() {
            "ok"
        } else {
            "fail"
        };

        let slots_assigned = self.slot_map.len();
        let slots_ok = if self.is_complete() {
            CLUSTER_SLOTS as usize
        } else {
            slots_assigned
        };

        let known_nodes = self.nodes.len();
        let cluster_size = self.count_masters();

        format!(
            "cluster_state:{state}\r\n\
             cluster_slots_assigned:{slots_assigned}\r\n\
             cluster_slots_ok:{slots_ok}\r\n\
             cluster_slots_pfail:0\r\n\
             cluster_slots_fail:0\r\n\
             cluster_known_nodes:{known_nodes}\r\n\
             cluster_size:{cluster_size}\r\n\
             cluster_current_epoch:{}\r\n\
             cluster_my_epoch:{}\r\n\
             cluster_stats_messages_sent:0\r\n\
             cluster_stats_messages_received:0\r\n",
            self.current_epoch,
            self.nodes.get(&self.my_id).map_or(0, |n| n.config_epoch)
        )
    }

    /// Check if all slots are assigned
    pub fn is_complete(&self) -> bool {
        self.slot_map.len() == CLUSTER_SLOTS as usize
    }

    /// Count number of master nodes
    pub fn count_masters(&self) -> usize {
        self.nodes
            .values()
            .filter(|n| n.has_flag(NodeFlags::Master))
            .count()
    }

    /// Get nodes string (for CLUSTER NODES command)
    pub fn nodes_string(&self) -> String {
        let mut lines = Vec::new();

        for node in self.nodes.values() {
            let master_id = node.master_id.as_ref().map(|s| s.as_str()).unwrap_or("-");

            let line = format!(
                "{} {}:{}@{} {} {} {} {} {} {} {}\n",
                node.id,
                node.addr.ip(),
                node.addr.port(),
                node.cluster_port,
                node.flags_string(),
                master_id,
                node.ping_sent,
                node.pong_recv,
                node.config_epoch,
                node.link_state.to_string(),
                node.slots_string()
            );
            lines.push(line);
        }

        lines.join("")
    }

    /// Get slots array (for CLUSTER SLOTS command)
    pub fn slots_array(&self) -> Vec<(u16, u16, Vec<NodeId>)> {
        if self.slot_map.is_empty() {
            return vec![];
        }

        let mut ranges = Vec::new();
        let mut slots: Vec<u16> = self.slot_map.keys().copied().collect();
        slots.sort_unstable();

        let mut range_start = slots[0];
        let mut range_end = slots[0];
        let mut current_node = self.slot_map[&slots[0]].clone();

        for &slot in &slots[1..] {
            let node = &self.slot_map[&slot];
            if slot == range_end + 1 && node == &current_node {
                range_end = slot;
            } else {
                // End current range
                ranges.push((range_start, range_end, vec![current_node.clone()]));
                range_start = slot;
                range_end = slot;
                current_node = node.clone();
            }
        }

        // Add final range
        ranges.push((range_start, range_end, vec![current_node]));

        ranges
    }
}

/// Thread-safe cluster state manager
pub struct ClusterManager {
    state: Arc<RwLock<ClusterState>>,
}

impl ClusterManager {
    /// Create a new cluster manager
    pub fn new(my_id: NodeId, my_addr: SocketAddr, enabled: bool) -> Self {
        Self {
            state: Arc::new(RwLock::new(ClusterState::new(my_id, my_addr, enabled))),
        }
    }

    /// Get a read lock on the cluster state
    pub async fn state(&self) -> tokio::sync::RwLockReadGuard<'_, ClusterState> {
        self.state.read().await
    }

    /// Get a write lock on the cluster state
    pub async fn state_mut(&self) -> tokio::sync::RwLockWriteGuard<'_, ClusterState> {
        self.state.write().await
    }

    /// Generate a random node ID
    #[allow(clippy::cast_possible_truncation)]
    pub fn generate_node_id() -> NodeId {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros();

        let random_bytes: [u8; 20] = std::array::from_fn(|i| {
            let shift = (i * 8) % 128; // Prevent shift overflow
            ((timestamp >> shift) ^ (i as u128 * 0x1234_5678)) as u8
        });

        hex::encode(random_bytes)
    }
}

impl Clone for ClusterManager {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::all)]
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn test_generate_node_id() {
        let id = ClusterManager::generate_node_id();
        assert_eq!(id.len(), 40);
        assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_cluster_state_creation() {
        let addr = "127.0.0.1:6379".parse().unwrap();
        let state = ClusterState::new("test123".to_string(), addr, true);
        assert!(state.enabled);
        assert_eq!(state.nodes.len(), 1);
        assert_eq!(state.slot_map.len(), 0);
    }

    #[test]
    fn test_assign_slots() {
        let addr = "127.0.0.1:6379".parse().unwrap();
        let mut state = ClusterState::new("node1".to_string(), addr, true);

        let slots = vec![0, 1, 2, 100, 101];
        state.assign_slots(&"node1".to_string(), &slots).unwrap();

        assert_eq!(state.slot_map.len(), 5);
        assert_eq!(state.get_slot_node(0), Some(&"node1".to_string()));
        assert_eq!(state.get_slot_node(100), Some(&"node1".to_string()));
        assert_eq!(state.get_slot_node(50), None);
    }

    #[test]
    fn test_slots_string_ranges() {
        let addr = "127.0.0.1:6379".parse().unwrap();
        let mut node = ClusterNode::new("node1".to_string(), addr, true);

        node.slots.insert(0);
        node.slots.insert(1);
        node.slots.insert(2);
        node.slots.insert(5);
        node.slots.insert(10);
        node.slots.insert(11);

        let slots_str = node.slots_string();
        assert_eq!(slots_str, "0-2 5 10-11");
    }

    #[test]
    fn test_cluster_not_complete() {
        let addr = "127.0.0.1:6379".parse().unwrap();
        let mut state = ClusterState::new("node1".to_string(), addr, true);

        state
            .assign_slots(&"node1".to_string(), &[0, 1, 2])
            .unwrap();
        assert!(!state.is_complete());
    }

    #[test]
    fn test_cluster_complete() {
        let addr = "127.0.0.1:6379".parse().unwrap();
        let mut state = ClusterState::new("node1".to_string(), addr, true);

        let all_slots: Vec<u16> = (0..CLUSTER_SLOTS).collect();
        state
            .assign_slots(&"node1".to_string(), &all_slots)
            .unwrap();
        assert!(state.is_complete());
    }

    #[test]
    fn test_info_string() {
        let addr = "127.0.0.1:6379".parse().unwrap();
        let state = ClusterState::new("node1".to_string(), addr, true);

        let info = state.info_string();
        assert!(info.contains("cluster_state:fail"));
        assert!(info.contains("cluster_slots_assigned:0"));
    }

    #[test]
    fn test_nodes_string() {
        let addr = "127.0.0.1:6379".parse().unwrap();
        let state = ClusterState::new("node1".to_string(), addr, true);

        let nodes = state.nodes_string();
        assert!(nodes.contains("node1"));
        assert!(nodes.contains("127.0.0.1:6379"));
        assert!(nodes.contains("myself,master"));
    }
}

//! ferris-replication: Replication and cluster support for ferris-db
//!
//! Handles leader/follower replication and Redis Cluster compatibility.

#![deny(unsafe_code)]
#![warn(clippy::all, clippy::pedantic, clippy::nursery)]
#![allow(clippy::module_name_repetitions)]

pub mod backlog;
pub mod manager;
pub mod state;

// Modules to be implemented later
// pub mod leader;
// pub mod follower;
// pub mod consistency;
// pub mod cluster;

pub use backlog::{BacklogConfig, BacklogEntry, ReplicationBacklog};
pub use manager::{ReplicationInfo, ReplicationManager};
pub use state::{MasterInfo, ReplicationRole, ReplicationState};

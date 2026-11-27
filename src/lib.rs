//! Quorum is a three-node Raft cluster.
//!
//! Client reads and writes go to the leader. Followers redirect.
//! Acknowledged writes are linearizable. Each log change is fsynced through
//! Forge before a node treats it as durable, and a client write is acknowledged
//! only after a majority has done that.

pub mod bench;
pub mod check;
pub mod logstore;
pub mod raft;
pub mod rpc;
pub mod server;
pub mod sim;

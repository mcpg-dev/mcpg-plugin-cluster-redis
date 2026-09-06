//! Redis-backed cluster-api primitive implementations.
//!
//! Internal sub-module of `mcpg-plugin-cluster-redis`; assembles
//! these primitives over the single shared Redis
//! `ConnectionManager` owned by the cluster plugin.
//!
//! Implements:
//! - [`RedisKv`] — `KeyValueStore` over GET/SET/DEL/SCAN, with
//!   `INCRBY`-backed atomic `incr`.
//! - [`RedisTopicBus`] — `PubSub` over PUBLISH/PSUBSCRIBE.

mod kv;
mod topic;

pub use kv::RedisKv;
pub use topic::RedisTopicBus;

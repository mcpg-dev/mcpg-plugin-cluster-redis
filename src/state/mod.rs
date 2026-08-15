//! Redis-backed cluster-api primitive implementations.
//!
//! Internal sub-module of `mcpg-plugin-cluster-redis`; assembles
//! these primitives over the single shared Redis
//! `ConnectionManager` owned by the cluster plugin.
//!
//! Implements:
//! - [`RedisKv`] — `KeyValueStore` over GET/SET/DEL/SCAN. When
//!   constructed via [`RedisKv::with_connection_manager_and_watch`],
//!   put/delete also XADD the operation to a watch stream.
//! - [`RedisLock`] — `Lease` via SETNX + Lua scripts with
//!   monotonic fence tokens (`INCR <key>:fence`).
//! - [`RedisTopicBus`] — `PubSub` over PUBLISH/PSUBSCRIBE.
//! - [`RedisWatch`] — `Watch` over a Redis Stream populated by the
//!   matching `RedisKv` instance.

mod kv;
mod lock;
mod topic;
mod watch;

pub use kv::RedisKv;
pub use lock::RedisLock;
pub use topic::RedisTopicBus;
pub use watch::{RedisWatch, default_watch_stream_key};

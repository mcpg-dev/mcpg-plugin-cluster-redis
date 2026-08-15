use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use mcpg_cluster_api::{ClusterError, Entry, KeyValueStore};
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use tokio::sync::Mutex;

use super::watch::{
    DELETE_WITH_WATCH_LUA, PUT_IF_ABSENT_WITH_WATCH_LUA, PUT_WITH_WATCH_LUA, ttl_arg,
};

/// Redis-backed KV state. Holds a `ConnectionManager` for automatic
/// reconnect; clones share the underlying connection.
pub struct RedisKv {
    inner: Arc<RedisKvInner>,
}

struct RedisKvInner {
    conn: Mutex<ConnectionManager>,
    key_prefix: String,
    /// When `Some`, every `put` / `delete` runs through a Lua script
    /// that also `XADD`s the operation to this stream. The matching
    /// [`crate::RedisWatch`] subscribes to the same stream. `None`
    /// disables watch publishing entirely (zero-cost path — the
    /// behaviour when no `Watch` primitive is wired).
    watch_stream: Option<String>,
}

impl std::fmt::Debug for RedisKv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisKv")
            .field("key_prefix", &self.inner.key_prefix)
            .finish()
    }
}

impl RedisKv {
    /// Construct a `RedisKv` over an already-built `ConnectionManager`
    /// that also publishes `put` / `delete` operations to a watch
    /// stream. Pair with a matching [`super::RedisWatch`] over the
    /// same stream key so subscribers see the events. The default
    /// stream key is `<key_prefix>:watch-stream` — use
    /// [`default_watch_stream_key`] to compute it from the same
    /// `key_prefix`.
    pub fn with_connection_manager_and_watch(
        conn: ConnectionManager,
        key_prefix: String,
        watch_stream: String,
    ) -> Self {
        Self {
            inner: Arc::new(RedisKvInner {
                conn: Mutex::new(conn),
                key_prefix,
                watch_stream: Some(watch_stream),
            }),
        }
    }

    fn full_key(&self, key: &str) -> String {
        if self.inner.key_prefix.is_empty() {
            key.to_owned()
        } else {
            format!("{}:{}", self.inner.key_prefix, key)
        }
    }
}

#[async_trait]
impl KeyValueStore for RedisKv {
    async fn get(&self, key: &str) -> Result<Option<Entry>, ClusterError> {
        let full = self.full_key(key);
        let mut conn = self.inner.conn.lock().await;
        let bytes: Option<Vec<u8>> = conn.get(&full).await.map_err(redis_err)?;
        let Some(bytes) = bytes else {
            return Ok(None);
        };
        // PTTL returns -2 if the key doesn't exist (race window),
        // -1 if no TTL is set, else ms-until-expiry.
        let pttl: i64 = conn.pttl(&full).await.map_err(redis_err)?;
        let expires_at = match pttl {
            -2 => return Ok(None),
            -1 => None,
            ms if ms >= 0 => Some(SystemTime::now() + Duration::from_millis(ms as u64)),
            _ => None,
        };
        Ok(Some(Entry {
            bytes: Bytes::from(bytes),
            expires_at,
        }))
    }

    async fn put(
        &self,
        key: &str,
        value: Bytes,
        ttl: Option<Duration>,
    ) -> Result<(), ClusterError> {
        let full = self.full_key(key);
        let mut conn = self.inner.conn.lock().await;
        let bytes = value.to_vec();
        // When watch is wired, every `put` runs through a Lua
        // script that does SET + XADD atomically. The 2x overhead
        // on writes is the documented trade-off for free reads
        // (watchers don't need to GET after the event arrives).
        if let Some(stream) = &self.inner.watch_stream {
            let _: i64 = redis::Script::new(PUT_WITH_WATCH_LUA)
                .key(&full)
                .key(stream)
                .arg(bytes)
                .arg(ttl_arg(ttl))
                .invoke_async(&mut *conn)
                .await
                .map_err(redis_err)?;
            return Ok(());
        }
        match ttl {
            Some(d) => {
                let ms: u64 = d.as_millis().max(1).min(u64::MAX as u128) as u64;
                let _: () = conn.pset_ex(&full, bytes, ms).await.map_err(redis_err)?;
            }
            None => {
                let _: () = conn.set(&full, bytes).await.map_err(redis_err)?;
                // SET clears existing TTL; ensure no leftover from a prior put.
                let _: i64 = conn.persist(&full).await.map_err(redis_err)?;
            }
        }
        Ok(())
    }

    async fn put_if_absent(
        &self,
        key: &str,
        value: Bytes,
        ttl: Option<Duration>,
    ) -> Result<bool, ClusterError> {
        let full = self.full_key(key);
        let mut conn = self.inner.conn.lock().await;
        let bytes = value.to_vec();
        // Atomic single-winner claim via `SET ... NX`. Redis auto-expires
        // keys, so a lapsed prior claim is already gone → NX naturally
        // succeeds (expired == absent).
        if let Some(stream) = &self.inner.watch_stream {
            let claimed: i64 = redis::Script::new(PUT_IF_ABSENT_WITH_WATCH_LUA)
                .key(&full)
                .key(stream)
                .arg(bytes)
                .arg(ttl_arg(ttl))
                .invoke_async(&mut *conn)
                .await
                .map_err(redis_err)?;
            return Ok(claimed == 1);
        }
        let mut cmd = redis::cmd("SET");
        cmd.arg(&full).arg(bytes).arg("NX");
        if let Some(d) = ttl {
            let ms: u64 = d.as_millis().max(1).min(u64::MAX as u128) as u64;
            cmd.arg("PX").arg(ms);
        }
        // `SET ... NX` returns "OK" when the key was set, nil otherwise.
        let set: Option<String> = cmd.query_async(&mut *conn).await.map_err(redis_err)?;
        Ok(set.is_some())
    }

    async fn delete(&self, key: &str) -> Result<bool, ClusterError> {
        let full = self.full_key(key);
        let mut conn = self.inner.conn.lock().await;
        if let Some(stream) = &self.inner.watch_stream {
            // Lua script: GET + DEL + XADD with prior value, all
            // atomic. Returns 1 when the key existed (matches the
            // trait contract).
            let removed: i64 = redis::Script::new(DELETE_WITH_WATCH_LUA)
                .key(&full)
                .key(stream)
                .invoke_async(&mut *conn)
                .await
                .map_err(redis_err)?;
            return Ok(removed > 0);
        }
        let n: i64 = conn.del(&full).await.map_err(redis_err)?;
        Ok(n > 0)
    }

    async fn list_prefix(
        &self,
        prefix: &str,
        limit: usize,
    ) -> Result<Vec<(String, Entry)>, ClusterError> {
        let full_prefix = self.full_key(prefix);
        let pattern = format!("{full_prefix}*");
        let mut conn = self.inner.conn.lock().await;

        // SCAN iterates all keys without blocking the server. We
        // bound the result set with `limit`.
        let mut iter: redis::AsyncIter<String> = conn
            .scan_match::<&str, String>(&pattern)
            .await
            .map_err(redis_err)?;
        let mut keys = Vec::new();
        while let Some(k) = iter.next_item().await {
            keys.push(k);
            if keys.len() >= limit {
                break;
            }
        }
        drop(iter);

        let mut out = Vec::with_capacity(keys.len());
        for full in keys {
            let bytes: Option<Vec<u8>> = conn.get(&full).await.map_err(redis_err)?;
            let Some(bytes) = bytes else {
                continue;
            };
            let pttl: i64 = conn.pttl(&full).await.map_err(redis_err)?;
            let expires_at = match pttl {
                -1 => None,
                ms if ms >= 0 => Some(SystemTime::now() + Duration::from_millis(ms as u64)),
                _ => continue,
            };
            // Strip the key_prefix the impl prepended; callers see
            // the logical key they put.
            let logical = if self.inner.key_prefix.is_empty() {
                full
            } else {
                full.strip_prefix(&format!("{}:", self.inner.key_prefix))
                    .map(|s| s.to_owned())
                    .unwrap_or(full)
            };
            out.push((
                logical,
                Entry {
                    bytes: Bytes::from(bytes),
                    expires_at,
                },
            ));
        }
        let _ = UNIX_EPOCH; // silence unused warning if compiler optimises everything
        Ok(out)
    }

    async fn expire(&self, key: &str, ttl: Option<Duration>) -> Result<bool, ClusterError> {
        let full = self.full_key(key);
        let mut conn = self.inner.conn.lock().await;
        match ttl {
            Some(d) => {
                let ms: u64 = d.as_millis().max(1).min(u64::MAX as u128) as u64;
                let updated: bool = conn.pexpire(&full, ms as i64).await.map_err(redis_err)?;
                Ok(updated)
            }
            None => {
                // Drop the TTL but keep the value.
                let exists: bool = conn.exists(&full).await.map_err(redis_err)?;
                if !exists {
                    return Ok(false);
                }
                let _: i64 = conn.persist(&full).await.map_err(redis_err)?;
                Ok(true)
            }
        }
    }
}

fn redis_err(e: redis::RedisError) -> ClusterError {
    ClusterError::BackendUnavailable {
        reason: format!("redis: {e}"),
    }
}

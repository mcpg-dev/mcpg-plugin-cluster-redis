use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use mcpg_cluster_api::{ClusterError, Entry, KeyValueStore};
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use tokio::sync::Mutex;

/// `INCRBY` + optional `PEXPIRE` in one server-side script, so the
/// add-and-get and its sliding TTL re-arm are atomic against every
/// other client. A non-integer value (or i64 overflow) makes `INCRBY`
/// raise; the sentinel prefix lets the host map that onto
/// `ClusterError::Precondition` instead of a generic backend error.
const INCR_LUA: &str = r"
local ok, ret = pcall(redis.call, 'INCRBY', KEYS[1], ARGV[1])
if not ok then
  local detail = ''
  if type(ret) == 'table' and ret.err then detail = ': ' .. ret.err end
  return redis.error_reply('MCPG_KV_INCR_PRECONDITION' .. detail)
end
local ttl = tonumber(ARGV[2])
if ttl and ttl > 0 then
  redis.call('PEXPIRE', KEYS[1], ttl)
end
return ret
";

/// Redis-backed KV state. Holds a `ConnectionManager` for automatic
/// reconnect; clones share the underlying connection.
pub struct RedisKv {
    inner: Arc<RedisKvInner>,
}

struct RedisKvInner {
    conn: Mutex<ConnectionManager>,
    key_prefix: String,
}

impl std::fmt::Debug for RedisKv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisKv")
            .field("key_prefix", &self.inner.key_prefix)
            .finish()
    }
}

impl RedisKv {
    /// Construct a `RedisKv` over an already-built `ConnectionManager`.
    /// Used by `mcpg-plugin-cluster-redis` to share its single
    /// connection across the coordinator + the primitive accessors.
    pub fn with_connection_manager(conn: ConnectionManager, key_prefix: String) -> Self {
        Self {
            inner: Arc::new(RedisKvInner {
                conn: Mutex::new(conn),
                key_prefix,
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

    async fn incr(
        &self,
        key: &str,
        delta: i64,
        ttl: Option<Duration>,
    ) -> Result<i64, ClusterError> {
        let full = self.full_key(key);
        let mut conn = self.inner.conn.lock().await;
        // 0 == "leave any existing TTL alone" inside the script.
        let ttl_ms: i64 = match ttl {
            Some(d) => d.as_millis().max(1).min(i64::MAX as u128) as i64,
            None => 0,
        };
        let r: Result<i64, redis::RedisError> = redis::Script::new(INCR_LUA)
            .key(&full)
            .arg(delta)
            .arg(ttl_ms)
            .invoke_async(&mut *conn)
            .await;
        r.map_err(|e| {
            let msg = e.to_string();
            if msg.contains("MCPG_KV_INCR_PRECONDITION") {
                ClusterError::Precondition {
                    reason: format!("redis incr `{key}`: {msg}"),
                }
            } else {
                redis_err(e)
            }
        })
    }
}

fn redis_err(e: redis::RedisError) -> ClusterError {
    ClusterError::BackendUnavailable {
        reason: format!("redis: {e}"),
    }
}

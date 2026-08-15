use async_trait::async_trait;
use mcpg_cluster_api::{ClusterError, FenceToken, Lease, LeaseHandle};
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::Mutex;

/// Redis-backed coordinated leases.
///
/// Acquire: `SET <name> <holder> NX PX <ttl_ms>` + `INCR <name>:fence`
/// for monotonic token. Renew: Lua script that compares the holder
/// before refreshing the TTL (CAS); release: Lua script that
/// compares-and-deletes.
///
/// Fence tokens are server-side-monotonic across crashes — we use
/// a separate `<name>:fence` counter that's `INCR`d on every
/// successful acquire.
pub struct RedisLock {
    inner: Arc<RedisLockInner>,
}

struct RedisLockInner {
    conn: Mutex<ConnectionManager>,
    key_prefix: String,
}

impl std::fmt::Debug for RedisLock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisLock")
            .field("key_prefix", &self.inner.key_prefix)
            .finish()
    }
}

impl RedisLock {
    /// Construct a `RedisLock` over an already-built `ConnectionManager`.
    /// Used by `mcpg-plugin-cluster-redis` to share its single
    /// connection across the coordinator + the primitive accessors.
    pub fn with_connection_manager(conn: ConnectionManager, key_prefix: String) -> Self {
        Self {
            inner: Arc::new(RedisLockInner {
                conn: Mutex::new(conn),
                key_prefix,
            }),
        }
    }

    fn lease_key(&self, name: &str) -> String {
        if self.inner.key_prefix.is_empty() {
            format!("lease:{name}")
        } else {
            format!("{}:lease:{name}", self.inner.key_prefix)
        }
    }
    fn fence_key(&self, name: &str) -> String {
        if self.inner.key_prefix.is_empty() {
            format!("lease:{name}:fence")
        } else {
            format!("{}:lease:{name}:fence", self.inner.key_prefix)
        }
    }
}

#[async_trait]
impl Lease for RedisLock {
    async fn try_acquire(
        &self,
        name: &str,
        holder: &str,
        ttl: Duration,
    ) -> Result<Option<LeaseHandle>, ClusterError> {
        let lease_key = self.lease_key(name);
        let fence_key = self.fence_key(name);
        let ttl_ms = ttl.as_millis().max(1).min(u64::MAX as u128) as u64;

        let mut conn = self.inner.conn.lock().await;

        // Lua: SET NX (or refresh if same holder) + INCR fence on success.
        // Returns: nil if another holder owns it, else the new fence token.
        let script = redis::Script::new(
            r#"
            local current = redis.call('GET', KEYS[1])
            if current == false or current == ARGV[1] then
                redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
                local fence = redis.call('INCR', KEYS[2])
                return fence
            else
                return nil
            end
            "#,
        );

        let fence: Option<i64> = script
            .key(&lease_key)
            .key(&fence_key)
            .arg(holder)
            .arg(ttl_ms)
            .invoke_async(&mut *conn)
            .await
            .map_err(|e| ClusterError::BackendUnavailable {
                reason: format!("redis lease try_acquire `{name}`: {e}"),
            })?;

        match fence {
            Some(f) => Ok(Some(LeaseHandle {
                name: name.to_owned(),
                holder: holder.to_owned(),
                fence: FenceToken(f as u64),
                expires_at: SystemTime::now() + ttl,
            })),
            None => Ok(None),
        }
    }

    async fn renew(&self, lease: &LeaseHandle, ttl: Duration) -> Result<LeaseHandle, ClusterError> {
        let lease_key = self.lease_key(&lease.name);
        let ttl_ms = ttl.as_millis().max(1).min(u64::MAX as u128) as u64;

        let mut conn = self.inner.conn.lock().await;

        // Lua: only PEXPIRE if holder matches. Returns 1 on success, 0 on mismatch.
        let script = redis::Script::new(
            r#"
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('PEXPIRE', KEYS[1], ARGV[2])
            else
                return 0
            end
            "#,
        );

        let updated: i64 = script
            .key(&lease_key)
            .arg(&lease.holder)
            .arg(ttl_ms)
            .invoke_async(&mut *conn)
            .await
            .map_err(|e| ClusterError::BackendUnavailable {
                reason: format!("redis lease renew `{}`: {e}", lease.name),
            })?;

        if updated == 0 {
            return Err(ClusterError::CasConflict {
                key: lease_key,
                reason: "holder mismatch or lease expired".to_owned(),
            });
        }
        Ok(LeaseHandle {
            name: lease.name.clone(),
            holder: lease.holder.clone(),
            fence: lease.fence,
            expires_at: SystemTime::now() + ttl,
        })
    }

    async fn release(&self, lease: &LeaseHandle) -> Result<(), ClusterError> {
        let lease_key = self.lease_key(&lease.name);

        let mut conn = self.inner.conn.lock().await;

        // Lua: only DEL if holder matches. Mismatches return 0 silently
        // (idempotent: lease may have already expired or been re-acquired).
        let script = redis::Script::new(
            r#"
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('DEL', KEYS[1])
            else
                return 0
            end
            "#,
        );

        let _: i64 = script
            .key(&lease_key)
            .arg(&lease.holder)
            .invoke_async(&mut *conn)
            .await
            .map_err(|e| ClusterError::BackendUnavailable {
                reason: format!("redis lease release `{}`: {e}", lease.name),
            })?;

        Ok(())
    }

    async fn current_holder(&self, name: &str) -> Result<Option<LeaseHandle>, ClusterError> {
        let lease_key = self.lease_key(name);
        let fence_key = self.fence_key(name);

        let mut conn = self.inner.conn.lock().await;
        let holder: Option<String> =
            conn.get(&lease_key)
                .await
                .map_err(|e| ClusterError::BackendUnavailable {
                    reason: format!("redis lease get `{name}`: {e}"),
                })?;
        let Some(holder) = holder else {
            return Ok(None);
        };
        let pttl: i64 =
            conn.pttl(&lease_key)
                .await
                .map_err(|e| ClusterError::BackendUnavailable {
                    reason: format!("redis lease pttl `{name}`: {e}"),
                })?;
        if pttl < 0 {
            return Ok(None);
        }
        let fence: i64 = conn.get(&fence_key).await.unwrap_or(0);
        Ok(Some(LeaseHandle {
            name: name.to_owned(),
            holder,
            fence: FenceToken(fence as u64),
            expires_at: SystemTime::now() + Duration::from_millis(pttl as u64),
        }))
    }
}

//! Lease lifecycle for `dev.mcpg.cluster.redis`.
//!
//! Redis doesn't have a native lease primitive — we get there via
//! `SET NX PX` + a per-lease fence counter (`INCR`) + Lua scripts
//! that compare-and-swap on the holder identity. The exact Lua
//! shape is shared with `mcpg-state-redis::RedisLock` so an
//! operator running both backends gets identical lease semantics.
//!
//! - `acquire_*(key, ttl)` runs a Lua script that does
//!   `SET NX PX ttl_ms` on the lease key + `INCR <key>:fence` on
//!   success. Returns the new fence token (the fencing token).
//!   `Ok(Some(state))` on acquire, `Ok(None)` if held by another
//!   holder. Spawns a background renewal task on success.
//! - **Renewal** is a Lua `if GET == holder then PEXPIRE`. A
//!   mismatched holder returns `LeaseExpired`.
//! - **Release** is a Lua `if GET == holder then DEL`. Idempotent
//!   via an `AtomicBool`; the renewal task aborts on drop.
//!
//! State lifecycle mirrors the etcd + consul plugins:
//! `Arc<LeaseState>` shared between async-trait holders and the
//! FFI leaked pointer; sync renew/release borrow via
//! `Arc::increment_strong_count`, the final `lease_drop` reclaims
//! via `Arc::from_raw`.

use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use chrono::SecondsFormat;
use mcpg_cluster_api::{ActiveLease, ClusterError};
use mcpg_plugin_protocol::async_trait;
use mcpg_plugin_sdk::ffi::WatchHandleBox;
use redis::aio::ConnectionManager;
use tokio::runtime::Handle as RuntimeHandle;
use tokio::sync::Mutex as TokioMutex;
use tokio::task::AbortHandle;
use tokio::time::sleep;

/// Shared connection wrapper. Redis async commands take
/// `&mut ConnectionManager`; behind a Tokio mutex we get cheap
/// `Arc` cloning + serialised access. ConnectionManager itself is
/// already cloneable + multiplexes pipelined ops, but the Lua
/// `invoke_async` API needs `&mut`, so the mutex is non-optional.
pub(crate) type SharedConn = Arc<TokioMutex<ConnectionManager>>;

pub(crate) struct LeaseState {
    pub(crate) conn: SharedConn,
    pub(crate) lease_key: String,
    pub(crate) holder: String,
    pub(crate) fence_token: u64,
    pub(crate) ttl_ms: u64,
    pub(crate) expires_at: StdMutex<String>,
    pub(crate) released: AtomicBool,
    pub(crate) renewal_abort: StdMutex<Option<AbortHandle>>,
}

impl Drop for LeaseState {
    fn drop(&mut self) {
        if let Some(h) = self.renewal_abort.lock().unwrap().take() {
            h.abort();
        }
    }
}

pub(crate) struct RedisLeaseHandle(pub(crate) Arc<LeaseState>);

#[async_trait]
impl ActiveLease for RedisLeaseHandle {
    fn fencing_token(&self) -> u64 {
        self.0.fence_token
    }

    fn expires_at(&self) -> String {
        self.0.expires_at.lock().unwrap().clone()
    }

    async fn renew(&self) -> Result<(), ClusterError> {
        renew_state(&self.0).await
    }

    async fn release(&self) -> Result<(), ClusterError> {
        release_state(&self.0).await
    }
}

// ---------------------------------------------------------------------------
// Acquire
// ---------------------------------------------------------------------------

/// Lua script for atomic SETNX + fence INCR.
///   KEYS[1] = lease key; KEYS[2] = fence key
///   ARGV[1] = holder    ; ARGV[2] = ttl_ms
/// Returns the new fence token on acquire, `nil` if held by anyone
/// (including the same holder — the contract verified by the
/// equivalence test `test_try_acquire_lock_returns_some_then_none`
/// is strict: a `try_acquire` while the key is occupied returns
/// `None` regardless of who holds it. Re-acquisition by the same
/// holder must go through release first, or the renewal path).
fn acquire_script() -> redis::Script {
    redis::Script::new(
        r#"
        if redis.call('GET', KEYS[1]) == false then
            redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
            local fence = redis.call('INCR', KEYS[2])
            return fence
        else
            return nil
        end
        "#,
    )
}

/// Lua script for renew: only PEXPIRE if the holder still matches.
///   KEYS[1] = lease key
///   ARGV[1] = holder ; ARGV[2] = ttl_ms
/// Returns 1 on success, 0 on mismatch.
fn renew_script() -> redis::Script {
    redis::Script::new(
        r#"
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            return redis.call('PEXPIRE', KEYS[1], ARGV[2])
        else
            return 0
        end
        "#,
    )
}

/// Lua script for release: only DEL if the holder still matches.
///   KEYS[1] = lease key
///   ARGV[1] = holder
/// Returns 1 on delete, 0 on mismatch (idempotent — caller treats
/// 0 as "lease already gone").
fn release_script() -> redis::Script {
    redis::Script::new(
        r#"
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            return redis.call('DEL', KEYS[1])
        else
            return 0
        end
        "#,
    )
}

/// Single attempt at acquiring the lease. Returns:
///   `Ok(Some(state))` — acquired, renewal task spawned.
///   `Ok(None)`        — backend reports the lease is held by
///                       another holder.
///   `Err(...)`        — backend unreachable or refused.
pub(crate) async fn try_acquire_async(
    conn: SharedConn,
    name: String,
    lease_key: String,
    fence_key: String,
    holder: String,
    ttl: Duration,
    renew_before_expiry_percent: u32,
) -> Result<Option<Arc<LeaseState>>, ClusterError> {
    if name.trim().is_empty() {
        return Err(ClusterError::InvalidReference {
            message: "lease name must not be empty".into(),
        });
    }
    let ttl_ms = ttl.as_millis().clamp(1, u64::MAX as u128) as u64;

    let fence: Option<i64> = {
        let mut c = conn.lock().await;
        acquire_script()
            .key(&lease_key)
            .key(&fence_key)
            .arg(&holder)
            .arg(ttl_ms)
            .invoke_async(&mut *c)
            .await
            .map_err(|e| ClusterError::BackendUnavailable {
                reason: format!("lease acquire `{name}`: {e}"),
            })?
    };

    let Some(fence_token) = fence else {
        return Ok(None);
    };

    // `fence_key` is consumed by the Lua acquire script (KEYS[2]
    // INCR'd above). Renew/release operate on `lease_key` only,
    // so we don't keep the fence key in the lease state.
    let _ = fence_key;
    let expires_at = StdMutex::new(rfc3339_after(Duration::from_millis(ttl_ms)));
    let state = Arc::new(LeaseState {
        conn: Arc::clone(&conn),
        lease_key,
        holder,
        fence_token: fence_token as u64,
        ttl_ms,
        expires_at,
        released: AtomicBool::new(false),
        renewal_abort: StdMutex::new(None),
    });

    // Spawn renewal task. Sleeps `ttl × (100 - pct) / 100` then
    // fires renew_state. Stops on AbortHandle drop or LeaseExpired.
    let pct = renew_before_expiry_percent.clamp(1, 99);
    let sleep_for = Duration::from_millis(ttl_ms).saturating_mul(100u32.saturating_sub(pct)) / 100;
    let sleep_for = if sleep_for.is_zero() {
        Duration::from_millis(100)
    } else {
        sleep_for
    };
    let renewal_state = Arc::clone(&state);
    let join = RuntimeHandle::current().spawn(async move {
        loop {
            sleep(sleep_for).await;
            if renewal_state.released.load(Ordering::SeqCst) {
                break;
            }
            if renew_state(&renewal_state).await.is_err() {
                // LeaseExpired or backend unreachable. Stop
                // renewing — caller will see LeaseExpired on
                // their next renew/release attempt too.
                break;
            }
        }
    });
    *state.renewal_abort.lock().unwrap() = Some(join.abort_handle());
    Ok(Some(state))
}

/// Blocking acquire — polls [`try_acquire_async`] with a small
/// backoff until the backend hands us the lease.
///
/// Backoff: 200 ms → 400 ms → 800 ms (clamped). Mirrors the
/// consul plugin's tail; small enough to feel responsive, large
/// enough to avoid hammering Redis when contention is high.
pub(crate) async fn acquire_async(
    conn: SharedConn,
    name: String,
    lease_key: String,
    fence_key: String,
    holder: String,
    ttl: Duration,
    renew_before_expiry_percent: u32,
) -> Result<Arc<LeaseState>, ClusterError> {
    let mut delay = Duration::from_millis(200);
    let cap = Duration::from_millis(800);
    loop {
        match try_acquire_async(
            Arc::clone(&conn),
            name.clone(),
            lease_key.clone(),
            fence_key.clone(),
            holder.clone(),
            ttl,
            renew_before_expiry_percent,
        )
        .await?
        {
            Some(state) => return Ok(state),
            None => {
                sleep(delay).await;
                delay = std::cmp::min(delay * 2, cap);
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn acquire_sync(
    runtime: &RuntimeHandle,
    conn: SharedConn,
    name: String,
    lease_key: String,
    fence_key: String,
    holder: String,
    ttl_ms: u64,
    renew_before_expiry_percent: u32,
) -> Result<(WatchHandleBox, u64, String), ClusterError> {
    let ttl = Duration::from_millis(ttl_ms.max(1));
    let state = runtime.block_on(async move {
        acquire_async(
            conn,
            name,
            lease_key,
            fence_key,
            holder,
            ttl,
            renew_before_expiry_percent,
        )
        .await
    })?;
    wrap_state(state)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn try_acquire_sync(
    runtime: &RuntimeHandle,
    conn: SharedConn,
    name: String,
    lease_key: String,
    fence_key: String,
    holder: String,
    ttl_ms: u64,
    renew_before_expiry_percent: u32,
) -> Result<Option<(WatchHandleBox, u64, String)>, ClusterError> {
    let ttl = Duration::from_millis(ttl_ms.max(1));
    let state_opt = runtime.block_on(async move {
        try_acquire_async(
            conn,
            name,
            lease_key,
            fence_key,
            holder,
            ttl,
            renew_before_expiry_percent,
        )
        .await
    })?;
    match state_opt {
        Some(s) => wrap_state(s).map(Some),
        None => Ok(None),
    }
}

fn wrap_state(state: Arc<LeaseState>) -> Result<(WatchHandleBox, u64, String), ClusterError> {
    let token = state.fence_token;
    let expires = state.expires_at.lock().unwrap().clone();
    let raw = Arc::into_raw(state);
    Ok((WatchHandleBox(raw as *mut ()), token, expires))
}

// ---------------------------------------------------------------------------
// Renew + release
// ---------------------------------------------------------------------------

pub(crate) async fn renew_state(state: &LeaseState) -> Result<(), ClusterError> {
    if state.released.load(Ordering::SeqCst) {
        return Err(ClusterError::LeaseExpired);
    }
    let updated: i64 = {
        let mut c = state.conn.lock().await;
        renew_script()
            .key(&state.lease_key)
            .arg(&state.holder)
            .arg(state.ttl_ms)
            .invoke_async(&mut *c)
            .await
            .map_err(|e| ClusterError::BackendUnavailable {
                reason: format!("lease renew `{}`: {e}", state.lease_key),
            })?
    };
    if updated == 0 {
        // Holder mismatch (someone else owns the key) or the key
        // expired between us and Redis. Either way the lease is
        // gone — caller MUST re-acquire.
        state.released.store(true, Ordering::SeqCst);
        return Err(ClusterError::LeaseExpired);
    }
    let new_expires = rfc3339_after(Duration::from_millis(state.ttl_ms));
    *state.expires_at.lock().unwrap() = new_expires;
    Ok(())
}

pub(crate) async fn release_state(state: &LeaseState) -> Result<(), ClusterError> {
    if state.released.swap(true, Ordering::SeqCst) {
        return Ok(());
    }
    if let Some(h) = state.renewal_abort.lock().unwrap().take() {
        h.abort();
    }
    let mut c = state.conn.lock().await;
    let _ = release_script()
        .key(&state.lease_key)
        .arg(&state.holder)
        .invoke_async::<i64>(&mut *c)
        .await;
    Ok(())
}

// ---------------------------------------------------------------------------
// Sync FFI helpers
// ---------------------------------------------------------------------------

/// SAFETY: caller MUST pass a `WatchHandleBox` produced by
/// `acquire_sync`. Pointer is valid for the duration of the
/// borrow per the host vtable contract.
pub(crate) unsafe fn borrow_state(handle: &WatchHandleBox) -> Option<Arc<LeaseState>> {
    let ptr = handle.0 as *const LeaseState;
    if ptr.is_null() {
        return None;
    }
    unsafe {
        Arc::increment_strong_count(ptr);
        Some(Arc::from_raw(ptr))
    }
}

/// SAFETY: exactly one `lease_drop` per `acquire_sync`.
pub(crate) unsafe fn drop_state(handle: WatchHandleBox) {
    let ptr = handle.0 as *const LeaseState;
    if ptr.is_null() {
        return;
    }
    unsafe {
        let _ = Arc::from_raw(ptr);
    }
}

pub(crate) fn renew_sync(
    runtime: &RuntimeHandle,
    handle: WatchHandleBox,
) -> Result<String, ClusterError> {
    let state = unsafe { borrow_state(&handle) }.ok_or(ClusterError::LeaseExpired)?;
    runtime.block_on(async move {
        renew_state(&state).await?;
        Ok(state.expires_at.lock().unwrap().clone())
    })
}

pub(crate) fn release_sync(
    runtime: &RuntimeHandle,
    handle: WatchHandleBox,
) -> Result<(), ClusterError> {
    let state = unsafe { borrow_state(&handle) };
    let state = match state {
        Some(s) => s,
        None => return Ok(()),
    };
    runtime.block_on(async move { release_state(&state).await })
}

// ---------------------------------------------------------------------------

fn rfc3339_after(ttl: Duration) -> String {
    let dt = chrono::Utc::now() + chrono::Duration::from_std(ttl).unwrap_or_default();
    dt.to_rfc3339_opts(SecondsFormat::Secs, true)
}

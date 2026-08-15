//! `dev.mcpg.cluster.redis` — Redis-backed
//! `cluster` plugin (v0.1).
//!
//! Sibling of:
//!   - `mcpg-plugin-cluster-etcd`
//!   - `mcpg-plugin-cluster-consul`
//!   - `mcpg-plugin-cluster-nats`
//!
//! Operators select this coordinator via:
//!
//! ```yaml
//! cluster:
//!   kind: redis
//!   url: ${env.REDIS_URL}
//!   key_prefix: "mcpg:cluster:"
//!   lease_ttl_ms: 30000
//!   peer_ttl_ms: 60000
//! ```
//!
//! # Backend mapping
//!
//! | Trait method                          | Redis primitive |
//! |---|---|
//! | `acquire_lock` / `acquire_leadership` | Lua `SET NX PX` + `INCR` for fence; renew via `if GET == holder then PEXPIRE` |
//! | `publish` / `subscribe`               | Native `PUBLISH` / `SUBSCRIBE` |
//! | `list_peers`                          | `SCAN MATCH <prefix>peers/*` + `MGET` |
//! | `watch_peers`                         | Polling diff against `list_peers` snapshots |
//!
//! Lease semantics match `mcpg-state-redis::RedisLock` so an operator
//! who already runs Redis for state can reuse the same instance for
//! cluster coordination.
//!
//! # Deferred to v0.2
//!
//! - Native PSUBSCRIBE wildcard topic patterns.
//! - Keyspace-event-driven `watch_peers` (vs polling).
//! - testcontainers-backed equivalence test against a real Redis.

mod config;
mod envelope;
mod lease;
mod peer;
mod pubsub;
mod state;

use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use mcpg_cluster_api::{
    BoxActiveLease, BoxPeerEventStream, BoxPublishedMessageStream, ClusterBackend, ClusterError,
    ClusterNodeInfo, ClusterPeer, KeyValueStore, Lease, PeerEvent, PubSub, Watch,
};
use mcpg_plugin_protocol::{PluginClass, PluginManifest};
use mcpg_plugin_sdk::declare_plugin;
use mcpg_plugin_sdk::ffi::{SyncClusterBackend, WatchHandleBox};
use redis::aio::ConnectionManager;

use crate::state::{RedisKv, RedisLock, RedisTopicBus, RedisWatch, default_watch_stream_key};
use tokio::runtime::Runtime;
use tokio::sync::{Mutex as TokioMutex, OnceCell};
use tokio::task::AbortHandle;

pub use config::{ConfigError, RedisBackendConfig};

const PLUGIN_ID: &str = "dev.mcpg.cluster.redis";

pub struct RedisBackend {
    inner: Arc<Inner>,
}

struct Inner {
    manifest: PluginManifest,
    config: RedisBackendConfig,
    node_id: String,
    started_at: String,
    /// Held purely so `subscribe` can request a fresh PubSub
    /// connection (Redis pub/sub is per-connection) and so the
    /// shared `ConnectionManager` can be built lazily on first use.
    client: redis::Client,
    /// Lazily-built shared multiplexed connection — used for SET /
    /// SCAN / Lua / PUBLISH ops. We don't construct this at boot
    /// because `ConnectionManager::new` aggressively retries +
    /// errors when Redis is unreachable; making the coordinator
    /// boot-time-tolerant matters for clusters where the gateway
    /// starts before Redis (and matches the consul + etcd
    /// coordinators' "config-validated, backend connected on
    /// first op" behaviour).
    conn_cell: OnceCell<lease::SharedConn>,
    /// Lazily-initialized primitive accessors. Filled on the first
    /// successful `get_or_init_conn` call (which both the
    /// coordinator's own methods and the plugin's
    /// `key_value_store()` / `lease()` / `pub_sub()` accessors go
    /// through). `from_validated_config` also kicks the init at
    /// construction time so a healthy Redis ends up populating the
    /// cell before the gateway boots its capabilities.
    primitives: OnceCell<RedisPrimitives>,
    runtime: Runtime,
    peer_refresh_abort: StdMutex<Option<AbortHandle>>,
    /// Set by `shutdown()` so the coordinator's background tasks are torn
    /// down proactively within the host's drain window instead of only on
    /// `Drop` (which fires when the last `Arc` ref — possibly an
    /// outstanding lease/stream guard — is released).
    draining: std::sync::atomic::AtomicBool,
}

impl Inner {
    /// Proactively abort background workers + flip the draining flag.
    /// Idempotent: a later `Drop` finds the abort handle already taken.
    fn shutdown(&self) {
        self.draining
            .store(true, std::sync::atomic::Ordering::SeqCst);
        if let Some(h) = self.peer_refresh_abort.lock().unwrap().take() {
            h.abort();
        }
    }
}

/// Bundle of primitive impls sharing the cluster plugin's single
/// `ConnectionManager`. Built once when the connection comes up;
/// returned `Arc`-cloned from each accessor call.
struct RedisPrimitives {
    kv: Arc<RedisKv>,
    lease: Arc<RedisLock>,
    pub_sub: Arc<RedisTopicBus>,
    /// `Watch` over a Redis Stream the matching `kv` populates as
    /// a side-effect of every `put` / `delete`. Subscribers
    /// `XREAD BLOCK` the stream + filter by prefix.
    watch: Arc<RedisWatch>,
}

impl RedisBackend {
    pub fn from_config_json(config_json: &str) -> Self {
        // Load-time manifest derivation builds + drops an instance only to read
        // its plugin-wide manifest. It has no real connection config, so the
        // host passes the manifest-probe sentinel. Substitute a placeholder url
        // (lazy `redis::Client`, no eager network I/O) so construction succeeds
        // for that probe; a REAL config still flows through parse + validate
        // below, so a genuinely misconfigured coordinator still refuses to load.
        if mcpg_plugin_protocol::is_manifest_probe_config(config_json) {
            let cfg = RedisBackendConfig::parse("{\"url\":\"redis://127.0.0.1:6379\"}")
                .expect("manifest-probe placeholder redis config is valid");
            return Self::from_validated_config(cfg);
        }
        let cfg = RedisBackendConfig::parse(config_json).unwrap_or_else(|err| {
            tracing::error!(
                plugin_id = PLUGIN_ID,
                error = %err,
                "redis cluster: config parse failed; refusing to register"
            );
            panic!(
                "redis cluster config parse failed: {err}. A misconfigured \
                 cluster_backend is a security hole; refusing to load."
            )
        });
        Self::from_validated_config(cfg)
    }

    fn from_validated_config(cfg: RedisBackendConfig) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("redis cluster: failed to build tokio runtime");

        // Build the connection from the URL, then inject the
        // out-of-URL credential so operators can supply the password
        // via a resolvable `password` field instead of embedding it in
        // the URL (which would leak into logs / the rendered ConfigMap).
        // URL parsing is not network I/O, so this still catches typos
        // without Redis being up.
        let client = {
            use redis::IntoConnectionInfo;
            let mut info = cfg
                .url
                .as_str()
                .into_connection_info()
                .unwrap_or_else(|err| {
                    panic!(
                        "redis cluster: invalid url `{}`: {err}",
                        crate::config::redact_redis_url(&cfg.url)
                    )
                });
            if cfg.username.is_some() {
                info.redis.username = cfg.username.clone();
            }
            if cfg.password.is_some() {
                info.redis.password = cfg.password.clone();
            }
            redis::Client::open(info).unwrap_or_else(|err| {
                panic!(
                    "redis cluster: failed to open client `{}`: {err}",
                    crate::config::redact_redis_url(&cfg.url)
                )
            })
        };

        let node_id = cfg.resolved_node_id();
        let started_at = now_rfc3339();
        let url_for_log = crate::config::redact_redis_url(&cfg.url);
        let key_prefix_for_log = cfg.key_prefix.clone();
        let node_id_for_log = node_id.clone();

        let inner = Arc::new(Inner {
            manifest: PluginManifest {
                id: PLUGIN_ID.into(),
                version: env!("CARGO_PKG_VERSION").into(),
                name: "Redis Cluster Coordinator".into(),
                plugin_class: PluginClass::Cluster,
                protocol_version: "1.0".into(),
                license: None,
                required_capabilities: Vec::new(),
                tags: Vec::new(),
                // Slot roles (cache/kv/bus), not primitive accessors.
                // Redis backs `kv` (string keys + TTL) and `cache` (same
                // primitive, eviction semantics for the cache slot). No
                // native `bus` today — wire bus to NATS/single-node.
                provides: vec!["cache".into(), "kv".into()],
                provides_schemes: Vec::new(),
                module_path_prefix: ::std::module_path!()
                    .split("::")
                    .next()
                    .unwrap_or("")
                    .to_owned(),
                backend_profile: None,
            },
            config: cfg,
            node_id,
            started_at,
            client,
            conn_cell: OnceCell::new(),
            primitives: OnceCell::new(),
            runtime,
            peer_refresh_abort: StdMutex::new(None),
            draining: std::sync::atomic::AtomicBool::new(false),
        });

        // Best-effort eager init of the connection + primitives.
        // If Redis is up at boot, the `primitives` cell populates
        // and the gateway's capabilities get real `key_value_store()`
        // / `lease()` / `pub_sub()` accessors. If Redis is down,
        // the init returns `BackendUnavailable`, the OnceCells stay
        // empty, and a later tick of the peer-refresh task retries
        // the connection — at which point primitives populate.
        // Accessors return `None` until population, matching the
        // contract on `ClusterBackend`.
        {
            let init_inner = Arc::clone(&inner);
            inner.runtime.block_on(async move {
                if let Err(err) = get_or_init_conn(&init_inner).await {
                    tracing::warn!(
                        plugin_id = PLUGIN_ID,
                        error = %err,
                        "redis cluster: connection unavailable at boot — primitive \
                         accessors will return None until first successful op"
                    );
                }
            });
        }

        // Spawn the peer-refresh task. It uses `get_or_init_conn`
        // internally so it tolerates a not-yet-up Redis at boot —
        // first refresh fails with BackendUnavailable, the next
        // tick retries and populates primitives along the way.
        let refresh_inner = Arc::clone(&inner);
        let refresh_interval =
            Duration::from_millis(inner.config.effective_peer_refresh_interval_ms().max(1));
        let address = crate::config::redact_redis_url(&inner.config.url);
        let peer_ttl_ms = inner.config.peer_ttl_ms;
        let prefix = inner.config.key_prefix.clone();
        let peer_node_id = inner.node_id.clone();
        let peer_refresh_abort = inner.runtime.block_on(async move {
            peer::spawn_peer_refresher_lazy(
                refresh_inner,
                prefix,
                peer_node_id,
                address,
                peer_ttl_ms,
                refresh_interval,
            )
        });
        *inner.peer_refresh_abort.lock().unwrap() = Some(peer_refresh_abort);

        tracing::info!(
            plugin_id = PLUGIN_ID,
            url = %url_for_log,
            key_prefix = %key_prefix_for_log,
            node_id = %node_id_for_log,
            "redis cluster: configured"
        );

        Self { inner }
    }

    /// Lazily build (or return the cached) shared `ConnectionManager`.
    /// Returns `BackendUnavailable` when Redis is unreachable so the
    /// caller can decide whether to retry.
    async fn get_or_init_conn(&self) -> Result<lease::SharedConn, ClusterError> {
        get_or_init_conn(&self.inner).await
    }

    /// Resolve the live `KeyValueStore`, lazily establishing the connection
    /// (which populates the primitive cell) on first use. Returns
    /// `BackendUnavailable` when Redis is unreachable.
    fn require_kv(&self) -> Result<Arc<dyn KeyValueStore>, ClusterError> {
        self.inner
            .runtime
            .block_on(async { get_or_init_conn(&self.inner).await })?;
        ClusterBackend::key_value_store(self).ok_or_else(|| ClusterError::BackendUnavailable {
            reason: "redis cluster: key_value_store unavailable".into(),
        })
    }
}

async fn get_or_init_conn(inner: &Arc<Inner>) -> Result<lease::SharedConn, ClusterError> {
    let conn = inner
        .conn_cell
        .get_or_try_init(|| async {
            // Use a tight retry profile so an unreachable Redis
            // surfaces as `BackendUnavailable` within a few hundred
            // milliseconds. ConnectionManager retries internally on
            // every op once initialised, so the small initial budget
            // doesn't compromise resilience — it just keeps unit
            // tests + fast-fail health checks responsive.
            let cfg = redis::aio::ConnectionManagerConfig::new()
                .set_number_of_retries(2)
                .set_factor(20)
                .set_max_delay(200)
                .set_connection_timeout(std::time::Duration::from_millis(500));
            let mgr = ConnectionManager::new_with_config(inner.client.clone(), cfg)
                .await
                .map_err(|e| ClusterError::BackendUnavailable {
                    reason: format!("connection manager init: {e}"),
                })?;
            Ok::<lease::SharedConn, ClusterError>(Arc::new(TokioMutex::new(mgr)))
        })
        .await?;
    // Once the connection is up, populate the primitive bundle on
    // first success — both this path and the boot-time eager init
    // funnel through here. Subsequent calls hit the OnceCell
    // fast-path with no extra work.
    let _ = inner
        .primitives
        .get_or_try_init(|| async {
            let mgr = {
                let guard = conn.lock().await;
                (*guard).clone()
            };
            let prefix = inner.config.key_prefix.clone();
            // Wire the watch stream. `kv` writes `put` / `delete`
            // ops to the stream via Lua scripts; `watch`
            // subscribers read from it via XREAD BLOCK. One stream
            // per cluster.redis instance; subscribers filter by
            // prefix client-side.
            let watch_stream = default_watch_stream_key(&prefix);
            let kv = Arc::new(RedisKv::with_connection_manager_and_watch(
                mgr.clone(),
                prefix.clone(),
                watch_stream.clone(),
            ));
            let lease = Arc::new(RedisLock::with_connection_manager(
                mgr.clone(),
                prefix.clone(),
            ));
            let pub_sub = Arc::new(RedisTopicBus::with_client_and_connection(
                inner.client.clone(),
                mgr.clone(),
                prefix.clone(),
            ));
            let watch = Arc::new(RedisWatch::with_client(
                inner.client.clone(),
                prefix,
                watch_stream,
            ));
            Ok::<RedisPrimitives, ClusterError>(RedisPrimitives {
                kv,
                lease,
                pub_sub,
                watch,
            })
        })
        .await?;
    Ok(Arc::clone(conn))
}

impl Drop for Inner {
    fn drop(&mut self) {
        // Idempotent with `shutdown()` — whichever runs first takes the
        // abort handle; the other finds `None`.
        self.shutdown();
    }
}

fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

/// Apply ±20% randomized jitter to a fixed sleep so N replicas that lost
/// the backend at the same instant don't retry in lock-step every
/// interval — synchronized spikes right as the backend recovers.
/// No `rand` dep: entropy is the current sub-second nanos (differs
/// per-call and per-replica; jitter needs only decorrelation, not
/// cryptographic randomness).
pub(crate) fn jittered(base: std::time::Duration) -> std::time::Duration {
    let base_ms = base.as_millis() as u64;
    let span = base_ms * 2 / 5; // 40% window → ±20%
    if span == 0 {
        return base;
    }
    let entropy = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    std::time::Duration::from_millis(base_ms - base_ms / 5 + entropy % (span + 1))
}

fn lease_keys(prefix: &str, kind: &str, name: &str) -> (String, String) {
    let lease_key = format!("{prefix}{kind}/{name}");
    let fence_key = format!("{prefix}{kind}/{name}:fence");
    (lease_key, fence_key)
}

// ---------------------------------------------------------------------------
// Async ClusterBackend impl
// ---------------------------------------------------------------------------

#[async_trait]
impl ClusterBackend for RedisBackend {
    // `cluster_provides()` uses the default impl: it derives the role
    // set from `manifest().provides` (= cache, kv).

    fn manifest(&self) -> &PluginManifest {
        &self.inner.manifest
    }

    fn key_value_store(&self) -> Option<Arc<dyn KeyValueStore>> {
        self.inner
            .primitives
            .get()
            .map(|p| Arc::clone(&p.kv) as Arc<dyn KeyValueStore>)
    }

    fn pub_sub(&self) -> Option<Arc<dyn PubSub>> {
        self.inner
            .primitives
            .get()
            .map(|p| Arc::clone(&p.pub_sub) as Arc<dyn PubSub>)
    }

    fn lease(&self) -> Option<Arc<dyn Lease>> {
        self.inner
            .primitives
            .get()
            .map(|p| Arc::clone(&p.lease) as Arc<dyn Lease>)
    }

    fn watch(&self) -> Option<Arc<dyn Watch>> {
        self.inner
            .primitives
            .get()
            .map(|p| Arc::clone(&p.watch) as Arc<dyn Watch>)
    }

    async fn node_info(&self) -> ClusterNodeInfo {
        ClusterNodeInfo {
            node_id: self.inner.node_id.clone(),
            address: crate::config::redact_redis_url(&self.inner.config.url),
            version: env!("CARGO_PKG_VERSION").into(),
            started_at: self.inner.started_at.clone(),
            roles: vec![],
        }
    }

    async fn list_peers(&self) -> Vec<ClusterPeer> {
        let conn = match self.get_or_init_conn().await {
            Ok(c) => c,
            Err(err) => {
                tracing::warn!(
                    plugin_id = PLUGIN_ID,
                    error = ?err,
                    "redis cluster: list_peers — backend unavailable; returning empty"
                );
                return vec![];
            }
        };
        match peer::list_peers(conn, &self.inner.config.key_prefix).await {
            Ok(peers) => peers,
            Err(err) => {
                tracing::warn!(
                    plugin_id = PLUGIN_ID,
                    error = ?err,
                    "redis cluster: list_peers failed; returning empty"
                );
                vec![]
            }
        }
    }

    async fn watch_peers(&self) -> BoxPeerEventStream {
        // v0.1 polls — we diff snapshots every refresh interval and
        // emit Joined / Left events. v0.2 will switch to keyspace
        // notifications (notify-keyspace-events Kx).
        let inner = Arc::clone(&self.inner);
        let prefix = self.inner.config.key_prefix.clone();
        let poll_ms = self
            .inner
            .config
            .effective_peer_refresh_interval_ms()
            .max(500);
        let buffer = self.inner.config.subscribe_pattern_buffer;
        let (tx, rx) = tokio::sync::mpsc::channel::<PeerEvent>(buffer);
        tokio::spawn(async move {
            let mut last: std::collections::BTreeMap<String, ClusterPeer> = Default::default();
            loop {
                if tx.is_closed() {
                    break;
                }
                let conn = match get_or_init_conn(&inner).await {
                    Ok(c) => c,
                    Err(err) => {
                        tracing::warn!(
                            plugin_id = PLUGIN_ID,
                            error = ?err,
                            "redis cluster: watch_peers — backend unavailable; backoff"
                        );
                        tokio::time::sleep(Duration::from_millis(poll_ms)).await;
                        continue;
                    }
                };
                match peer::list_peers(conn, &prefix).await {
                    Ok(peers) => {
                        let cur: std::collections::BTreeMap<String, ClusterPeer> = peers
                            .into_iter()
                            .filter(|p| !p.node_id.is_empty())
                            .map(|p| (p.node_id.clone(), p))
                            .collect();
                        // Joined: in cur, not in last.
                        for (node_id, peer) in &cur {
                            if !last.contains_key(node_id) {
                                let evt = PeerEvent::Joined { peer: peer.clone() };
                                if tx.send(evt).await.is_err() {
                                    return;
                                }
                            }
                        }
                        // Left: in last, not in cur.
                        for node_id in last.keys() {
                            if !cur.contains_key(node_id) {
                                let evt = PeerEvent::Left {
                                    node_id: node_id.clone(),
                                };
                                if tx.send(evt).await.is_err() {
                                    return;
                                }
                            }
                        }
                        last = cur;
                    }
                    Err(err) => {
                        tracing::warn!(
                            plugin_id = PLUGIN_ID,
                            error = ?err,
                            "redis cluster: watch_peers poll failed; backoff"
                        );
                    }
                }
                tokio::time::sleep(Duration::from_millis(poll_ms)).await;
            }
        });
        Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx))
    }

    async fn acquire_leadership(
        &self,
        role: &str,
        lease_ttl: Duration,
    ) -> Result<BoxActiveLease, ClusterError> {
        let conn = self.get_or_init_conn().await?;
        let (lease_key, fence_key) = lease_keys(&self.inner.config.key_prefix, "leadership", role);
        let state = lease::acquire_async(
            conn,
            format!("mcpg-leadership-{role}"),
            lease_key,
            fence_key,
            self.inner.node_id.clone(),
            lease_ttl,
            self.inner.config.lease_renew_before_expiry_percent,
        )
        .await?;
        Ok(Box::new(lease::RedisLeaseHandle(state)))
    }

    async fn acquire_lock(
        &self,
        key: &str,
        lease_ttl: Duration,
    ) -> Result<BoxActiveLease, ClusterError> {
        let conn = self.get_or_init_conn().await?;
        let (lease_key, fence_key) = lease_keys(&self.inner.config.key_prefix, "locks", key);
        let state = lease::acquire_async(
            conn,
            format!("mcpg-lock-{key}"),
            lease_key,
            fence_key,
            self.inner.node_id.clone(),
            lease_ttl,
            self.inner.config.lease_renew_before_expiry_percent,
        )
        .await?;
        Ok(Box::new(lease::RedisLeaseHandle(state)))
    }

    async fn try_acquire_leadership(
        &self,
        role: &str,
        lease_ttl: Duration,
    ) -> Result<Option<BoxActiveLease>, ClusterError> {
        let conn = self.get_or_init_conn().await?;
        let (lease_key, fence_key) = lease_keys(&self.inner.config.key_prefix, "leadership", role);
        let state_opt = lease::try_acquire_async(
            conn,
            format!("mcpg-leadership-{role}"),
            lease_key,
            fence_key,
            self.inner.node_id.clone(),
            lease_ttl,
            self.inner.config.lease_renew_before_expiry_percent,
        )
        .await?;
        Ok(state_opt.map(|state| Box::new(lease::RedisLeaseHandle(state)) as BoxActiveLease))
    }

    async fn try_acquire_lock(
        &self,
        key: &str,
        lease_ttl: Duration,
    ) -> Result<Option<BoxActiveLease>, ClusterError> {
        let conn = self.get_or_init_conn().await?;
        let (lease_key, fence_key) = lease_keys(&self.inner.config.key_prefix, "locks", key);
        let state_opt = lease::try_acquire_async(
            conn,
            format!("mcpg-lock-{key}"),
            lease_key,
            fence_key,
            self.inner.node_id.clone(),
            lease_ttl,
            self.inner.config.lease_renew_before_expiry_percent,
        )
        .await?;
        Ok(state_opt.map(|state| Box::new(lease::RedisLeaseHandle(state)) as BoxActiveLease))
    }

    async fn publish(
        &self,
        topic: &str,
        routing_key: Option<&str>,
        payload: Bytes,
    ) -> Result<(), ClusterError> {
        let conn = self.get_or_init_conn().await?;
        pubsub::publish_async(
            conn,
            &self.inner.config.key_prefix,
            topic,
            routing_key,
            payload,
        )
        .await
    }

    async fn subscribe(
        &self,
        topic: &str,
        _group: Option<&str>,
        routing_key: Option<&str>,
    ) -> Result<BoxPublishedMessageStream, ClusterError> {
        pubsub::subscribe_async(
            self.inner.client.clone(),
            self.inner.config.key_prefix.clone(),
            topic.to_owned(),
            routing_key.map(str::to_owned),
            self.inner.node_id.clone(),
            self.inner.config.subscribe_pattern_buffer,
        )
        .await
    }
}

// ---------------------------------------------------------------------------
// Sync FFI — required by declare_plugin!'s cluster_backend arm
// ---------------------------------------------------------------------------

impl SyncClusterBackend for RedisBackend {
    fn manifest(&self) -> &PluginManifest {
        &self.inner.manifest
    }

    /// Proactively abort the peer-refresh background task within the
    /// host's drain window rather than waiting for `Drop` (which fires
    /// only when the last `Arc<Inner>` ref — possibly an outstanding
    /// lease/stream guard — is released).
    fn shutdown(&self) {
        self.inner.shutdown();
    }

    fn node_info(&self) -> ClusterNodeInfo {
        self.inner
            .runtime
            .block_on(async { ClusterBackend::node_info(self).await })
    }

    fn list_peers(&self) -> Vec<ClusterPeer> {
        self.inner
            .runtime
            .block_on(async { ClusterBackend::list_peers(self).await })
    }

    fn publish(
        &self,
        topic: &str,
        routing_key: Option<&str>,
        payload: Vec<u8>,
    ) -> Result<(), ClusterError> {
        self.inner.runtime.block_on(async {
            ClusterBackend::publish(self, topic, routing_key, Bytes::from(payload)).await
        })
    }

    // Bridge the async pub/sub + peer-watch impls across the FFI via
    // the shared `cluster_forward` helper.
    fn subscribe(
        &self,
        topic: &str,
        group: Option<&str>,
        routing_key: Option<&str>,
        emit_event: Box<dyn Fn(&str) + Send + Sync + 'static>,
    ) -> Result<WatchHandleBox, ClusterError> {
        let stream = self
            .inner
            .runtime
            .block_on(async { ClusterBackend::subscribe(self, topic, group, routing_key).await })?;
        Ok(
            mcpg_plugin_sdk::ffi::cluster_forward::forward_cluster_stream(
                self.inner.runtime.handle(),
                stream,
                emit_event,
            ),
        )
    }

    fn watch_peers(
        &self,
        emit_event: Box<dyn Fn(&str) + Send + Sync + 'static>,
    ) -> Result<WatchHandleBox, ClusterError> {
        let stream = self
            .inner
            .runtime
            .block_on(async { ClusterBackend::watch_peers(self).await });
        Ok(
            mcpg_plugin_sdk::ffi::cluster_forward::forward_cluster_stream(
                self.inner.runtime.handle(),
                stream,
                emit_event,
            ),
        )
    }

    fn cancel_stream(&self, stream_handle: WatchHandleBox) {
        // SAFETY: handle came from our subscribe/watch_peers, not yet cancelled.
        unsafe { mcpg_plugin_sdk::ffi::cluster_forward::cancel_cluster_stream(stream_handle) }
    }

    fn acquire_leadership(
        &self,
        role: &str,
        ttl_ms: u64,
    ) -> Result<(WatchHandleBox, u64, String), ClusterError> {
        let conn = self
            .inner
            .runtime
            .block_on(async { get_or_init_conn(&self.inner).await })?;
        let (lease_key, fence_key) = lease_keys(&self.inner.config.key_prefix, "leadership", role);
        lease::acquire_sync(
            self.inner.runtime.handle(),
            conn,
            format!("mcpg-leadership-{role}"),
            lease_key,
            fence_key,
            self.inner.node_id.clone(),
            ttl_ms,
            self.inner.config.lease_renew_before_expiry_percent,
        )
    }

    fn acquire_lock(
        &self,
        key: &str,
        ttl_ms: u64,
    ) -> Result<(WatchHandleBox, u64, String), ClusterError> {
        let conn = self
            .inner
            .runtime
            .block_on(async { get_or_init_conn(&self.inner).await })?;
        let (lease_key, fence_key) = lease_keys(&self.inner.config.key_prefix, "locks", key);
        lease::acquire_sync(
            self.inner.runtime.handle(),
            conn,
            format!("mcpg-lock-{key}"),
            lease_key,
            fence_key,
            self.inner.node_id.clone(),
            ttl_ms,
            self.inner.config.lease_renew_before_expiry_percent,
        )
    }

    fn try_acquire_leadership(
        &self,
        role: &str,
        ttl_ms: u64,
    ) -> Result<Option<(WatchHandleBox, u64, String)>, ClusterError> {
        let conn = self
            .inner
            .runtime
            .block_on(async { get_or_init_conn(&self.inner).await })?;
        let (lease_key, fence_key) = lease_keys(&self.inner.config.key_prefix, "leadership", role);
        lease::try_acquire_sync(
            self.inner.runtime.handle(),
            conn,
            format!("mcpg-leadership-{role}"),
            lease_key,
            fence_key,
            self.inner.node_id.clone(),
            ttl_ms,
            self.inner.config.lease_renew_before_expiry_percent,
        )
    }

    fn try_acquire_lock(
        &self,
        key: &str,
        ttl_ms: u64,
    ) -> Result<Option<(WatchHandleBox, u64, String)>, ClusterError> {
        let conn = self
            .inner
            .runtime
            .block_on(async { get_or_init_conn(&self.inner).await })?;
        let (lease_key, fence_key) = lease_keys(&self.inner.config.key_prefix, "locks", key);
        lease::try_acquire_sync(
            self.inner.runtime.handle(),
            conn,
            format!("mcpg-lock-{key}"),
            lease_key,
            fence_key,
            self.inner.node_id.clone(),
            ttl_ms,
            self.inner.config.lease_renew_before_expiry_percent,
        )
    }

    fn lease_renew(&self, lease_handle: WatchHandleBox) -> Result<String, ClusterError> {
        lease::renew_sync(self.inner.runtime.handle(), lease_handle)
    }

    fn lease_release(&self, lease_handle: WatchHandleBox) -> Result<(), ClusterError> {
        lease::release_sync(self.inner.runtime.handle(), lease_handle)
    }

    fn lease_drop(&self, lease_handle: WatchHandleBox) {
        // SAFETY: host vtable contract — exactly one `lease_drop`
        // per acquire, and the pointer is still valid.
        unsafe { lease::drop_state(lease_handle) }
    }

    // KV primitive over FFI — block on the plugin's own runtime, routing
    // each method through the same `KeyValueStore` impl `key_value_store()`
    // exposes.
    fn kv_get(&self, key: &str) -> Result<Option<mcpg_cluster_api::Entry>, ClusterError> {
        let kv = self.require_kv()?;
        self.inner.runtime.block_on(async { kv.get(key).await })
    }

    fn kv_put(&self, key: &str, value: Vec<u8>, ttl_ms: Option<u64>) -> Result<(), ClusterError> {
        let kv = self.require_kv()?;
        self.inner
            .runtime
            .block_on(async { kv.put(key, Bytes::from(value), ttl_from_ms(ttl_ms)).await })
    }

    fn kv_put_if_absent(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl_ms: Option<u64>,
    ) -> Result<bool, ClusterError> {
        let kv = self.require_kv()?;
        self.inner.runtime.block_on(async {
            kv.put_if_absent(key, Bytes::from(value), ttl_from_ms(ttl_ms))
                .await
        })
    }

    fn kv_delete(&self, key: &str) -> Result<bool, ClusterError> {
        let kv = self.require_kv()?;
        self.inner.runtime.block_on(async { kv.delete(key).await })
    }

    fn kv_list_prefix(
        &self,
        prefix: &str,
        limit: usize,
    ) -> Result<Vec<(String, mcpg_cluster_api::Entry)>, ClusterError> {
        let kv = self.require_kv()?;
        self.inner
            .runtime
            .block_on(async { kv.list_prefix(prefix, limit).await })
    }

    fn kv_expire(&self, key: &str, ttl_ms: Option<u64>) -> Result<bool, ClusterError> {
        let kv = self.require_kv()?;
        self.inner
            .runtime
            .block_on(async { kv.expire(key, ttl_from_ms(ttl_ms)).await })
    }
}

/// Whole-millisecond TTL → `Duration` (None == no TTL).
fn ttl_from_ms(ttl_ms: Option<u64>) -> Option<Duration> {
    ttl_ms.map(Duration::from_millis)
}

declare_plugin! {
    plugin_id: "dev.mcpg.cluster.redis",
    plugin_version: env!("CARGO_PKG_VERSION"),
    descriptor_yaml: include_str!("../plugin.yaml"),
    capabilities: &[mcpg_plugin_protocol::capability::Capability::NetworkOutbound],
    entities: [
        cluster_backend as cluster {
            inner_name: "",
            plugin_type: RedisBackend,
            factory: |cfg: &str, _host: ::mcpg_plugin_sdk::HostHandle| -> RedisBackend {
                RedisBackend::from_config_json(cfg)
            },
        }
    ],
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn build_config() -> RedisBackendConfig {
        RedisBackendConfig::parse(
            &json!({
                "url": "redis://127.0.0.1:1",
                "node_id": "node-test"
            })
            .to_string(),
        )
        .unwrap()
    }

    #[test]
    fn config_parsing_works() {
        let cfg = build_config();
        assert_eq!(cfg.url, "redis://127.0.0.1:1");
        assert_eq!(cfg.resolved_node_id(), "node-test");
    }

    #[test]
    fn lease_key_format() {
        let (lease_key, fence_key) = lease_keys("mcpg:cluster:", "locks", "alpha");
        assert_eq!(lease_key, "mcpg:cluster:locks/alpha");
        assert_eq!(fence_key, "mcpg:cluster:locks/alpha:fence");

        let (lease_key, fence_key) = lease_keys("mcpg:cluster:", "leadership", "writer");
        assert_eq!(lease_key, "mcpg:cluster:leadership/writer");
        assert_eq!(fence_key, "mcpg:cluster:leadership/writer:fence");
    }

    #[test]
    fn shutdown_is_idempotent_and_marks_draining() {
        // shutdown() proactively aborts the peer-refresh worker + sets
        // the draining flag, and is safe to call repeatedly (and before
        // the subsequent Drop).
        let plugin = RedisBackend::from_validated_config(build_config());
        SyncClusterBackend::shutdown(&plugin);
        assert!(
            plugin
                .inner
                .draining
                .load(std::sync::atomic::Ordering::SeqCst),
            "shutdown must flip the draining flag"
        );
        // Idempotent: a second shutdown (and the eventual Drop) is a no-op.
        SyncClusterBackend::shutdown(&plugin);
    }

    #[test]
    fn manifest_reports_plugin_id_and_class() {
        // Build the coordinator against an unreachable Redis URL —
        // ConnectionManager retries lazily so initialisation
        // succeeds. The manifest fields don't depend on the
        // backend at all.
        let plugin = RedisBackend::from_validated_config(build_config());
        let manifest = ClusterBackend::manifest(&plugin);
        assert_eq!(manifest.id, PLUGIN_ID);
        assert!(matches!(manifest.plugin_class, PluginClass::Cluster));
        assert_eq!(manifest.protocol_version, "1.0");
        // Capabilities live on PluginRegistration.capabilities;
        // manifest is display-only.
    }

    #[test]
    fn envelope_wraps_routing_key() {
        // Sanity-check that the envelope module the coordinator
        // uses for `publish` round-trips routing keys correctly.
        let wire = envelope::encode(Some("rk"), b"payload").unwrap();
        let (rk, payload) = envelope::decode(&wire).unwrap();
        assert_eq!(rk.as_deref(), Some("rk"));
        assert_eq!(&payload[..], b"payload");
    }

    #[test]
    fn node_info_reports_configured_identity() {
        let plugin = RedisBackend::from_validated_config(build_config());
        let info = SyncClusterBackend::node_info(&plugin);
        assert_eq!(info.node_id, "node-test");
        assert_eq!(info.address, "redis://127.0.0.1:1");
        assert_eq!(info.version, env!("CARGO_PKG_VERSION"));
    }

    #[test]
    fn acquire_leadership_surfaces_unreachable_as_backend_unavailable() {
        let plugin = RedisBackend::from_validated_config(build_config());
        let err = plugin
            .inner
            .runtime
            .block_on(async {
                ClusterBackend::acquire_leadership(&plugin, "test-role", Duration::from_secs(60))
                    .await
            })
            .err()
            .expect("expected error");
        assert!(
            matches!(err, ClusterError::BackendUnavailable { .. }),
            "expected BackendUnavailable, got {err:?}"
        );
    }

    #[test]
    fn acquire_lock_surfaces_unreachable_as_backend_unavailable() {
        let plugin = RedisBackend::from_validated_config(build_config());
        let err = plugin
            .inner
            .runtime
            .block_on(async {
                ClusterBackend::acquire_lock(&plugin, "test-key", Duration::from_secs(60)).await
            })
            .err()
            .expect("expected error");
        assert!(
            matches!(err, ClusterError::BackendUnavailable { .. }),
            "expected BackendUnavailable, got {err:?}"
        );
    }

    #[test]
    fn list_peers_handles_unreachable_redis_gracefully() {
        let plugin = RedisBackend::from_validated_config(build_config());
        let peers = SyncClusterBackend::list_peers(&plugin);
        assert!(peers.is_empty());
    }
}

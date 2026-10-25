//! Peer registry helpers for `dev.mcpg.cluster.redis`.
//!
//! Each gateway instance writes a self-registration key under
//! `<prefix>peers/<node_id>` carrying a small JSON payload
//! (address + last_seen timestamp + roles). The key has a TTL —
//! operators who run a sidecar refresher get presence-via-TTL
//! semantics without us depending on Redis Sentinel or keyspace
//! events.
//!
//! The node id also sits in the sorted set `<prefix>#peers`, scored by
//! when its registration expires, so `list_peers` is a range read plus an
//! `MGET` rather than a scan of the keyspace. A build that predates the
//! set registers only the key; a scan finds those, at most once per
//! [`LEGACY_SCAN_INTERVAL`]. `register_peer` is `SET <key> <json> PX <ttl>`
//! plus `ZADD`. `spawn_peer_refresher` runs `register_peer` every
//! `peer_refresh_interval_ms` so a missed tick still leaves a slack
//! before the peer key expires.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chrono::SecondsFormat;
use mcpg_cluster_api::{ClusterError, ClusterPeer, PeerHealth};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use tokio::task::AbortHandle;
use tokio::time::{MissedTickBehavior, interval};

use crate::Inner;
use crate::lease::SharedConn;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct PeerRegistration {
    pub address: String,
    /// RFC3339 timestamp written at the most recent refresh.
    pub last_seen: String,
    #[serde(default)]
    pub roles: Vec<String>,
    /// The node writes the key index of the state store. A node that
    /// predates the field writes none, and while one is alive no listing
    /// trusts the index.
    #[serde(default)]
    pub kv_index: bool,
}

pub(crate) fn peer_key(prefix: &str, node_id: &str) -> String {
    format!("{prefix}peers/{node_id}")
}

pub(crate) fn peers_pattern(prefix: &str) -> String {
    format!("{prefix}peers/*")
}

/// The sorted set of registered node ids, scored by when the
/// registration expires (Unix ms).
pub(crate) fn peers_index_key(prefix: &str) -> String {
    format!("{prefix}#peers")
}

/// How often a peer list also scans for registrations outside the sorted
/// set (builds that predate it), at most; never less often than a
/// registration lives.
pub(crate) const LEGACY_SCAN_INTERVAL: Duration = Duration::from_secs(30);

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

/// A peer's node id and its registration.
pub(crate) type Registration = (String, PeerRegistration);

/// When a keyspace scan ran, and the registrations it found.
type Scan = (Instant, Vec<Registration>);

/// The registrations a scan of the keyspace found last, and when.
pub(crate) struct LegacyPeerScan {
    /// How old a scan may be before a peer list scans again.
    max_age: Duration,
    last: tokio::sync::Mutex<Option<Scan>>,
}

impl LegacyPeerScan {
    /// Scans at most [`LEGACY_SCAN_INTERVAL`] apart, and at most
    /// `peer_ttl` apart.
    pub(crate) fn new(peer_ttl: Duration) -> Self {
        Self {
            max_age: LEGACY_SCAN_INTERVAL.min(peer_ttl),
            last: tokio::sync::Mutex::new(None),
        }
    }

    /// Make the next peer list scan.
    pub(crate) async fn forget(&self) {
        *self.last.lock().await = None;
    }

    /// Every registration key's node id and registration, from a scan at
    /// most `max_age` old.
    async fn registrations(
        &self,
        conn: &SharedConn,
        prefix: &str,
    ) -> Result<Vec<(String, PeerRegistration)>, ClusterError> {
        let mut last = self.last.lock().await;
        if let Some((at, found)) = last.as_ref()
            && at.elapsed() < self.max_age
        {
            return Ok(found.clone());
        }
        let pattern = peers_pattern(prefix);
        let mut keys: Vec<String> = Vec::new();
        let mut scan = 0u64;
        loop {
            let (next, batch): (u64, Vec<String>) = {
                let mut c = conn.lock().await;
                redis::cmd("SCAN")
                    .arg(scan)
                    .arg("MATCH")
                    .arg(&pattern)
                    .arg("COUNT")
                    .arg(1000)
                    .query_async(&mut *c)
                    .await
                    .map_err(|e| ClusterError::BackendUnavailable {
                        reason: format!("peer SCAN: {e}"),
                    })?
            };
            keys.extend(batch);
            scan = next;
            if next == 0 {
                break;
            }
        }
        let found = read_registrations(conn, prefix, keys).await?;
        *last = Some((Instant::now(), found.clone()));
        Ok(found)
    }
}

/// `MGET` the registration `keys` and decode them as `(node id,
/// registration)`; a key gone or malformed is left out.
async fn read_registrations(
    conn: &SharedConn,
    prefix: &str,
    keys: Vec<String>,
) -> Result<Vec<(String, PeerRegistration)>, ClusterError> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let values: Vec<Option<String>> = {
        let mut c = conn.lock().await;
        c.mget(&keys)
            .await
            .map_err(|e| ClusterError::BackendUnavailable {
                reason: format!("peer MGET: {e}"),
            })?
    };
    let mut out = Vec::with_capacity(keys.len());
    for (key, value) in keys.into_iter().zip(values) {
        let Some(raw) = value else { continue };
        let Ok(reg) = serde_json::from_str::<PeerRegistration>(&raw) else {
            tracing::warn!(
                key = %key,
                "redis cluster: dropping malformed peer registration"
            );
            continue;
        };
        let node_id = key
            .strip_prefix(&format!("{prefix}peers/"))
            .map(str::to_owned)
            .unwrap_or(key);
        out.push((node_id, reg));
    }
    Ok(out)
}

pub(crate) fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
}

/// `SET <prefix>peers/<node_id> <json> PX <ttl>` — best-effort
/// self-registration. Errors propagate to the caller; the
/// background refresher logs + carries on.
pub(crate) async fn register_peer(
    conn: SharedConn,
    prefix: &str,
    node_id: &str,
    address: &str,
    peer_ttl_ms: u64,
    roles: &[String],
) -> Result<(), ClusterError> {
    let registration = PeerRegistration {
        address: address.to_owned(),
        last_seen: now_rfc3339(),
        roles: roles.to_vec(),
        kv_index: true,
    };
    let json = serde_json::to_string(&registration).map_err(|e| ClusterError::Internal {
        reason: format!("peer registration encode: {e}"),
    })?;
    let key = peer_key(prefix, node_id);
    let mut c = conn.lock().await;
    let _: () = redis::pipe()
        .cmd("SET")
        .arg(&key)
        .arg(json)
        .arg("PX")
        .arg(peer_ttl_ms)
        .ignore()
        .cmd("ZADD")
        .arg(peers_index_key(prefix))
        .arg(now_ms().saturating_add(peer_ttl_ms))
        .arg(node_id)
        .ignore()
        .query_async(&mut *c)
        .await
        .map_err(|e| ClusterError::BackendUnavailable {
            reason: format!("peer SET `{key}`: {e}"),
        })?;
    Ok(())
}

/// `DEL <prefix>peers/<node_id>` — leave the peer list at shutdown, so the
/// other replicas see this node gone now rather than after `peer_ttl_ms`.
pub(crate) async fn deregister_peer(
    conn: SharedConn,
    prefix: &str,
    node_id: &str,
) -> Result<(), ClusterError> {
    let key = peer_key(prefix, node_id);
    let mut c = conn.lock().await;
    let _: () = redis::pipe()
        .cmd("DEL")
        .arg(&key)
        .ignore()
        .cmd("ZREM")
        .arg(peers_index_key(prefix))
        .arg(node_id)
        .ignore()
        .query_async(&mut *c)
        .await
        .map_err(|e| ClusterError::BackendUnavailable {
            reason: format!("peer DEL `{key}`: {e}"),
        })?;
    Ok(())
}

/// Every live registration as `(node id, registration)`: the node ids the
/// sorted set holds as unexpired, and the registrations `legacy` found
/// outside it.
pub(crate) async fn list_registrations(
    conn: SharedConn,
    prefix: &str,
    legacy: &LegacyPeerScan,
) -> Result<Vec<(String, PeerRegistration)>, ClusterError> {
    let index_key = peers_index_key(prefix);
    let now = now_ms();
    let ids: Vec<String> = {
        let mut c = conn.lock().await;
        let (ids, _): (Vec<String>, i64) = redis::pipe()
            .cmd("ZRANGEBYSCORE")
            .arg(&index_key)
            .arg(format!("({now}"))
            .arg("+inf")
            .cmd("ZREMRANGEBYSCORE")
            .arg(&index_key)
            .arg("-inf")
            .arg(now)
            .query_async(&mut *c)
            .await
            .map_err(|e| ClusterError::BackendUnavailable {
                reason: format!("peer ZRANGEBYSCORE: {e}"),
            })?;
        ids
    };
    let keys = ids.iter().map(|id| peer_key(prefix, id)).collect();
    let mut found = read_registrations(&conn, prefix, keys).await?;
    // Only an older build is missing from the set; a current one that is
    // missing has left, whatever an older scan saw.
    for (node_id, registration) in legacy.registrations(&conn, prefix).await? {
        if !registration.kv_index && !ids.contains(&node_id) {
            found.push((node_id, registration));
        }
    }
    Ok(found)
}

/// The live peers, from [`list_registrations`].
pub(crate) async fn list_peers(
    conn: SharedConn,
    prefix: &str,
    legacy: &LegacyPeerScan,
) -> Result<Vec<ClusterPeer>, ClusterError> {
    Ok(list_registrations(conn, prefix, legacy)
        .await?
        .into_iter()
        .map(|(node_id, reg)| ClusterPeer {
            node_id,
            address: reg.address,
            last_seen: reg.last_seen,
            health: PeerHealth::Healthy,
            roles: reg.roles,
        })
        .collect())
}

/// Spawn a periodic refresher that re-runs `register_peer` every
/// `refresh_interval`. Lazily resolves the shared connection via
/// `Inner::get_or_init_conn` so the refresher tolerates Redis
/// being briefly down at boot — the first tick fails with
/// BackendUnavailable + warns, the next tick retries.
pub(crate) fn spawn_peer_refresher_lazy(
    inner: Arc<Inner>,
    prefix: String,
    node_id: String,
    address: String,
    peer_ttl_ms: u64,
    refresh_interval: Duration,
) -> AbortHandle {
    let join = tokio::spawn(async move {
        // First tick fires immediately so the peer is visible
        // before we wait a full interval.
        let mut tick = interval(refresh_interval);
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            let conn = match crate::get_or_init_conn(&inner).await {
                Ok(c) => c,
                Err(err) => {
                    tracing::warn!(
                        node_id = %node_id,
                        error = %err,
                        "redis cluster: peer refresh — backend unavailable; will retry"
                    );
                    continue;
                }
            };
            if let Err(err) =
                register_peer(conn, &prefix, &node_id, &address, peer_ttl_ms, &[]).await
            {
                // Best-effort — Redis may be transiently down.
                // Next tick retries; meanwhile the existing peer
                // key TTLs out, which is the correct signal.
                tracing::warn!(
                    node_id = %node_id,
                    error = %err,
                    "redis cluster: peer refresh failed; will retry"
                );
            }
        }
    });
    join.abort_handle()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_key_format() {
        assert_eq!(
            peer_key("mcpg:cluster:", "alpha"),
            "mcpg:cluster:peers/alpha"
        );
        assert_eq!(peers_pattern("mcpg:cluster:"), "mcpg:cluster:peers/*");
    }

    #[test]
    fn registration_json_roundtrip() {
        let r = PeerRegistration {
            address: "10.0.0.1:8080".into(),
            last_seen: "2026-05-02T12:00:00Z".into(),
            roles: vec!["leader".into()],
            kv_index: true,
        };
        let json = serde_json::to_string(&r).unwrap();
        let back: PeerRegistration = serde_json::from_str(&json).unwrap();
        assert_eq!(back, r);
    }

    #[test]
    fn registration_default_roles() {
        // Older registrations carry neither `roles` nor `kv_index` — make
        // sure we accept that, and read them as not writing the index.
        let raw = r#"{"address":"x:1","last_seen":"2026-05-02T00:00:00Z"}"#;
        let parsed: PeerRegistration = serde_json::from_str(raw).unwrap();
        assert!(parsed.roles.is_empty());
        assert!(!parsed.kv_index);
    }
}

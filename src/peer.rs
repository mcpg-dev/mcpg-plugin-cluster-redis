//! Peer registry helpers for `dev.mcpg.cluster.redis`.
//!
//! Each gateway instance writes a self-registration key under
//! `<prefix>peers/<node_id>` carrying a small JSON payload
//! (address + last_seen timestamp + roles). The key has a TTL —
//! operators who run a sidecar refresher get presence-via-TTL
//! semantics without us depending on Redis Sentinel or keyspace
//! events.
//!
//! `list_peers` does a `SCAN MATCH <prefix>peers/*` + `MGET` over
//! the matching keys; `register_peer` is `SET <key> <json> PX <ttl>`.
//! `spawn_peer_refresher` runs `register_peer` every
//! `peer_refresh_interval_ms` so a missed tick still leaves a slack
//! before the peer key expires.

use std::sync::Arc;
use std::time::Duration;

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
}

pub(crate) fn peer_key(prefix: &str, node_id: &str) -> String {
    format!("{prefix}peers/{node_id}")
}

pub(crate) fn peers_pattern(prefix: &str) -> String {
    format!("{prefix}peers/*")
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
    };
    let json = serde_json::to_string(&registration).map_err(|e| ClusterError::Internal {
        reason: format!("peer registration encode: {e}"),
    })?;
    let key = peer_key(prefix, node_id);
    let mut c = conn.lock().await;
    let _: () = c
        .set_options(
            &key,
            json,
            redis::SetOptions::default().with_expiration(redis::SetExpiry::PX(peer_ttl_ms)),
        )
        .await
        .map_err(|e| ClusterError::BackendUnavailable {
            reason: format!("peer SET `{key}`: {e}"),
        })?;
    Ok(())
}

/// `SCAN MATCH <prefix>peers/*` → `MGET` → JSON-decode → `ClusterPeer`.
pub(crate) async fn list_peers(
    conn: SharedConn,
    prefix: &str,
) -> Result<Vec<ClusterPeer>, ClusterError> {
    let pattern = peers_pattern(prefix);
    let mut keys: Vec<String> = Vec::new();
    {
        let mut c = conn.lock().await;
        let mut iter: redis::AsyncIter<String> = c
            .scan_match::<&str, String>(&pattern)
            .await
            .map_err(|e| ClusterError::BackendUnavailable {
                reason: format!("peer SCAN: {e}"),
            })?;
        while let Some(k) = iter.next_item().await {
            keys.push(k);
        }
    }
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
        out.push(ClusterPeer {
            node_id,
            address: reg.address,
            last_seen: reg.last_seen,
            health: PeerHealth::Healthy,
            roles: reg.roles,
        });
    }
    Ok(out)
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
        };
        let json = serde_json::to_string(&r).unwrap();
        let back: PeerRegistration = serde_json::from_str(&json).unwrap();
        assert_eq!(back, r);
    }

    #[test]
    fn registration_default_roles() {
        // Older registrations may not carry `roles` — make sure
        // we accept that without erroring.
        let raw = r#"{"address":"x:1","last_seen":"2026-05-02T00:00:00Z"}"#;
        let parsed: PeerRegistration = serde_json::from_str(raw).unwrap();
        assert!(parsed.roles.is_empty());
    }
}

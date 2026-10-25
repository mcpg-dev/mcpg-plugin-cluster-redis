//! The state store's key-index upkeep.
//!
//! Redis expires a data key itself, but its entries in the key index and
//! the expiry index stay; one replica at a time — the holder of the
//! [`SWEEP_LOCK`] lock — drops them every `kv_sweep_interval_ms`.
//!
//! The same task gates the key index. Every replica lets listings read it
//! only while no registered peer lacks `kv_index` (an older build, which
//! writes no index entries) and the index is marked complete; a replica
//! that sees such a peer marks the index incomplete. The lock holder
//! completes it again — a backfill — once no such peer has been seen for
//! `peer_ttl_ms`.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::task::AbortHandle;
use tokio::time::MissedTickBehavior;

use crate::state::RedisKv;
use crate::{Inner, lease, lease_keys, peer};

/// The lock whose holder sweeps.
const SWEEP_LOCK: &str = "mcpg-kv-sweep";

/// Milliseconds between ticks when sweeping is off.
const IDLE_INTERVAL_MS: u64 = 30_000;

/// Start the task on the plugin's runtime.
pub(crate) fn spawn(inner: Arc<Inner>) -> AbortHandle {
    tokio::spawn(run(inner)).abort_handle()
}

async fn run(inner: Arc<Inner>) {
    let configured = inner.config.kv_sweep_interval_ms;
    let sweep = configured > 0;
    let interval = Duration::from_millis(if sweep { configured } else { IDLE_INTERVAL_MS });
    let settle_after = Duration::from_millis(inner.config.peer_ttl_ms);
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut held: Option<Arc<lease::LeaseState>> = None;
    let mut unindexed_seen: Option<Instant> = None;
    loop {
        tick.tick().await;
        let Ok(conn) = crate::get_or_init_conn(&inner).await else {
            continue;
        };
        let Some(kv) = inner.primitives.get().map(|p| Arc::clone(&p.kv)) else {
            continue;
        };
        let prefix = inner.config.key_prefix.clone();
        let unindexed = match peer::list_registrations(Arc::clone(&conn), &prefix, &inner.peer_scan)
            .await
        {
            Ok(registrations) => registrations.iter().any(|(_, reg)| !reg.kv_index),
            Err(error) => {
                tracing::debug!(error = %error, "redis cluster: peer list unavailable; next tick");
                continue;
            }
        };
        if unindexed {
            unindexed_seen = Some(Instant::now());
        }
        gate_index(&kv, unindexed).await;

        if let Some(state) = &held
            && lease::renew_state(state).await.is_err()
        {
            tracing::info!("redis cluster: lost the KV sweep lock; another replica sweeps");
            held = None;
        }
        if held.is_none() {
            let (lease_key, fence_key) = lease_keys(&prefix, "locks", SWEEP_LOCK);
            match lease::try_acquire_async(
                Arc::clone(&conn),
                SWEEP_LOCK.to_owned(),
                lease_key,
                fence_key,
                inner.node_id.clone(),
                interval.saturating_mul(2),
                50,
            )
            .await
            {
                Ok(Some(state)) => held = Some(state),
                Ok(None) => continue,
                Err(error) => {
                    tracing::debug!(error = %error, "redis cluster: KV sweep lock unavailable; next tick");
                    continue;
                }
            }
        }

        if sweep {
            match kv.sweep().await {
                Ok(0) => {}
                Ok(gone) => tracing::info!(
                    gone,
                    "redis cluster: dropped the key-index entries of expired keys"
                ),
                Err(error) => tracing::warn!(
                    error = %error,
                    "redis cluster: KV sweep failed; next sweep"
                ),
            }
        }
        let settled = unindexed_seen.is_none_or(|seen| seen.elapsed() >= settle_after);
        if !unindexed && settled && !kv.index_ready() {
            complete_index(&inner, &kv, conn).await;
        }
    }
}

/// Let listings read the key index, or not, as this replica sees the fleet
/// now.
async fn gate_index(kv: &RedisKv, unindexed_peer: bool) {
    if unindexed_peer {
        if let Err(error) = kv.invalidate_index().await {
            tracing::debug!(error = %error, "redis cluster: index state unavailable; next tick");
        }
        return;
    }
    match kv.index_complete().await {
        Ok(ready) => {
            if ready != kv.index_ready() {
                tracing::info!(
                    ready,
                    "redis cluster: key listings {} the index",
                    if ready { "read" } else { "no longer read" }
                );
            }
            kv.set_index_ready(ready);
        }
        Err(error) => {
            tracing::debug!(error = %error, "redis cluster: index state unavailable; next tick");
        }
    }
}

/// Index every live key, and mark the index complete when no peer without
/// the index registered meanwhile.
pub(crate) async fn complete_index(inner: &Inner, kv: &RedisKv, conn: lease::SharedConn) {
    let started_at_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0);
    let started = Instant::now();
    match kv.backfill_index().await {
        Ok(indexed) => {
            // A fresh scan: an older peer that registered during the
            // backfill wrote keys the backfill may have missed.
            inner.peer_scan.forget().await;
            let unindexed =
                peer::list_registrations(conn, &inner.config.key_prefix, &inner.peer_scan)
                    .await
                    .map(|registrations| registrations.iter().any(|(_, reg)| !reg.kv_index))
                    .unwrap_or(true);
            if unindexed {
                return;
            }
            if let Err(error) = kv.mark_index_complete(started_at_ms).await {
                tracing::warn!(error = %error, "redis cluster: the key index could not be marked complete; next tick");
                return;
            }
            kv.set_index_ready(true);
            tracing::info!(
                indexed,
                took_ms = started.elapsed().as_millis() as u64,
                "redis cluster: the key index is complete; key listings read it"
            );
        }
        Err(error) => tracing::warn!(
            error = %error,
            "redis cluster: the key index backfill failed; key listings scan until the next tick"
        ),
    }
}

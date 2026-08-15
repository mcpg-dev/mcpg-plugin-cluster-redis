//! Publish / subscribe over Redis `PUBLISH` + `PSUBSCRIBE`.
//!
//! Redis `PUBLISH` only carries the channel name + an opaque
//! payload. Operators who want a routing-key channel encode it in
//! the wire envelope (see `envelope.rs`) — same shape as the
//! consul + etcd coordinators.
//!
//! - Topic → channel: `<prefix>topic:<topic>` (no wildcard for v0.1).
//! - `publish_async`: encode + `PUBLISH`.
//! - `subscribe_async`: open a fresh PubSub connection (Redis pub/sub
//!   is stateful per-connection), `PSUBSCRIBE` the literal channel,
//!   forward decoded messages over an mpsc channel. Reconnect on
//!   error with a 5s backoff.

use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use mcpg_cluster_api::{BoxPublishedMessageStream, ClusterError, PublishedMessage};
use redis::AsyncCommands;
use tokio::time::sleep;

use crate::envelope;
use crate::lease::SharedConn;

const PLUGIN_ID: &str = "dev.mcpg.cluster.redis";

/// Compose the Redis channel name for a topic.
pub(crate) fn channel_name(prefix: &str, topic: &str) -> String {
    format!("{prefix}topic:{topic}")
}

pub(crate) async fn publish_async(
    conn: SharedConn,
    prefix: &str,
    topic: &str,
    routing_key: Option<&str>,
    payload: Bytes,
) -> Result<(), ClusterError> {
    let channel = channel_name(prefix, topic);
    let wire = envelope::encode(routing_key, &payload).map_err(|e| ClusterError::Internal {
        reason: format!("publish envelope: {e}"),
    })?;
    let mut c = conn.lock().await;
    let _: i64 =
        c.publish(&channel, wire.to_vec())
            .await
            .map_err(|e| ClusterError::BackendUnavailable {
                reason: format!("PUBLISH `{channel}`: {e}"),
            })?;
    Ok(())
}

/// Subscribe to a topic. Opens a new PubSub connection (per Redis
/// semantics — pub/sub state lives on the connection), subscribes
/// to the literal channel, and forwards decoded `PublishedMessage`
/// values via an mpsc channel. The receiver is the returned
/// `Stream`; the caller drops the stream when they're done and the
/// background task tears the subscription down.
///
/// Reconnect policy: any pubsub error tears down the current
/// connection, sleeps 5s, then re-subscribes. Operators who want
/// at-most-once delivery handle dedupe at their own layer (Redis
/// pub/sub is fire-and-forget — we don't replay).
pub(crate) async fn subscribe_async(
    client: redis::Client,
    prefix: String,
    topic: String,
    routing_key_filter: Option<String>,
    node_id: String,
    buffer: usize,
) -> Result<BoxPublishedMessageStream, ClusterError> {
    let channel = channel_name(&prefix, &topic);
    // Open the first connection up-front so connection failures
    // surface to the caller (rather than burying them inside the
    // background task).
    let mut pubsub =
        client
            .get_async_pubsub()
            .await
            .map_err(|e| ClusterError::BackendUnavailable {
                reason: format!("redis pubsub connect: {e}"),
            })?;
    pubsub
        .subscribe(&channel)
        .await
        .map_err(|e| ClusterError::BackendUnavailable {
            reason: format!("redis SUBSCRIBE `{channel}`: {e}"),
        })?;

    let (tx, rx) = tokio::sync::mpsc::channel::<PublishedMessage>(buffer);
    tokio::spawn(async move {
        let mut current = Some(pubsub);
        loop {
            if tx.is_closed() {
                break;
            }
            // Take the current pubsub connection (re-establishing
            // it if we lost it on the previous iteration).
            let mut conn = match current.take() {
                Some(c) => c,
                None => match reconnect(&client, &channel).await {
                    Ok(c) => c,
                    Err(err) => {
                        tracing::warn!(
                            plugin_id = PLUGIN_ID,
                            channel = %channel,
                            error = %err,
                            "redis cluster: pubsub reconnect failed; backoff"
                        );
                        sleep(crate::jittered(Duration::from_secs(5))).await;
                        continue;
                    }
                },
            };
            let mut stream = conn.on_message();
            loop {
                if tx.is_closed() {
                    return;
                }
                let Some(msg) = stream.next().await else {
                    // Stream ended — connection went away. Drop it
                    // and reconnect.
                    break;
                };
                let raw: Vec<u8> = msg.get_payload_bytes().to_vec();
                let (msg_rk, payload) = match envelope::decode(&raw) {
                    Ok(pair) => pair,
                    Err(err) => {
                        tracing::warn!(
                            plugin_id = PLUGIN_ID,
                            channel = %channel,
                            error = %err,
                            "redis cluster: dropping message with bad envelope"
                        );
                        continue;
                    }
                };
                if let Some(want) = routing_key_filter.as_deref()
                    && msg_rk.as_deref() != Some(want)
                {
                    continue;
                }
                let out = PublishedMessage {
                    topic: topic.clone(),
                    routing_key: msg_rk,
                    payload,
                    from_node: node_id.clone(),
                };
                if tx.send(out).await.is_err() {
                    return;
                }
            }
            // Connection lost — drop `stream` (which borrows the
            // pubsub conn) and let the outer loop reconnect.
            drop(stream);
            // current is None at this point so the outer loop will
            // reconnect on its next iteration.
            sleep(crate::jittered(Duration::from_secs(5))).await;
        }
    });
    Ok(Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx)))
}

async fn reconnect(
    client: &redis::Client,
    channel: &str,
) -> Result<redis::aio::PubSub, ClusterError> {
    let mut pubsub =
        client
            .get_async_pubsub()
            .await
            .map_err(|e| ClusterError::BackendUnavailable {
                reason: format!("redis pubsub reconnect: {e}"),
            })?;
    pubsub
        .subscribe(channel)
        .await
        .map_err(|e| ClusterError::BackendUnavailable {
            reason: format!("redis re-SUBSCRIBE `{channel}`: {e}"),
        })?;
    Ok(pubsub)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_name_format() {
        assert_eq!(
            channel_name("mcpg:cluster:", "creds.events"),
            "mcpg:cluster:topic:creds.events"
        );
    }
}

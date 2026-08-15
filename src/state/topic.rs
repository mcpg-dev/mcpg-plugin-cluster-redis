use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, stream::BoxStream};
use mcpg_cluster_api::{ClusterError, Message, PubSub, Subscription};
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Redis-backed topic bus over `PUBLISH` / `PSUBSCRIBE`.
///
/// Each `subscribe` call opens a fresh PubSub connection (Redis
/// pub/sub is stateful per-connection). The publish side uses the
/// shared multiplexed `ConnectionManager`.
pub struct RedisTopicBus {
    publish_conn: Arc<Mutex<ConnectionManager>>,
    client: redis::Client,
    topic_prefix: String,
}

impl std::fmt::Debug for RedisTopicBus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisTopicBus")
            .field("topic_prefix", &self.topic_prefix)
            .finish()
    }
}

impl RedisTopicBus {
    /// Construct a `RedisTopicBus` reusing an already-built
    /// `redis::Client` + multiplexed `ConnectionManager`. Used by
    /// `mcpg-plugin-cluster-redis` so the coordinator and the
    /// primitive `pub_sub()` accessor share the same client.
    pub fn with_client_and_connection(
        client: redis::Client,
        publish_conn: ConnectionManager,
        topic_prefix: String,
    ) -> Self {
        Self {
            publish_conn: Arc::new(Mutex::new(publish_conn)),
            client,
            topic_prefix,
        }
    }

    fn full_topic(&self, topic: &str) -> String {
        if self.topic_prefix.is_empty() {
            topic.to_owned()
        } else {
            format!("{}:{}", self.topic_prefix, topic)
        }
    }
}

#[async_trait]
impl PubSub for RedisTopicBus {
    async fn publish(&self, topic: &str, payload: Bytes) -> Result<(), ClusterError> {
        let full = self.full_topic(topic);
        let mut conn = self.publish_conn.lock().await;
        let _: i64 = conn.publish(&full, payload.to_vec()).await.map_err(|e| {
            ClusterError::BackendUnavailable {
                reason: format!("redis publish `{full}`: {e}"),
            }
        })?;
        Ok(())
    }

    async fn subscribe(
        &self,
        pattern: &str,
        _queue_group: Option<&str>,
    ) -> Result<Subscription, ClusterError> {
        let full_pattern = self.full_topic(pattern);
        // Redis pub/sub requires a dedicated connection.
        let pubsub_conn =
            self.client
                .get_async_pubsub()
                .await
                .map_err(|e| ClusterError::BackendUnavailable {
                    reason: format!("redis pubsub connect: {e}"),
                })?;
        let mut pubsub = pubsub_conn;
        // PSUBSCRIBE for pattern matching (redis treats `*` as
        // single-token wildcard via glob — close enough to NATS
        // semantics for our purposes).
        pubsub
            .psubscribe(&full_pattern)
            .await
            .map_err(|e| ClusterError::BackendUnavailable {
                reason: format!("redis psubscribe `{full_pattern}`: {e}"),
            })?;
        let prefix_strip = if self.topic_prefix.is_empty() {
            String::new()
        } else {
            format!("{}:", self.topic_prefix)
        };
        let stream: BoxStream<'static, Result<Message, ClusterError>> =
            Box::pin(pubsub.into_on_message().map(move |msg| {
                let channel: String = msg.get_channel_name().to_owned();
                let logical = if !prefix_strip.is_empty() {
                    channel
                        .strip_prefix(&prefix_strip)
                        .map(|s| s.to_owned())
                        .unwrap_or(channel)
                } else {
                    channel
                };
                let payload: Vec<u8> = msg.get_payload_bytes().to_vec();
                Ok(Message {
                    topic: logical,
                    payload: Bytes::from(payload),
                })
            }));
        Ok(stream)
    }
}

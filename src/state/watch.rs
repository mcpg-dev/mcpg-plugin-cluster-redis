use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use mcpg_cluster_api::{ClusterError, Watch, WatchEvent, WatchEventKind, WatchStream};
use redis::AsyncCommands;
use redis::Client;
use tokio::sync::mpsc;

/// Redis-Stream-backed [`Watch`] primitive.
///
/// Listens on a single watch stream (`<key_prefix>:watch-stream`)
/// that [`crate::RedisKv`] populates as a side-effect of every
/// `put` / `delete` (when constructed via
/// [`crate::RedisKv::with_connection_manager_and_watch`]).
/// Subscribers `XREAD BLOCK` the stream from `$` (latest only —
/// no historical replay) and filter by prefix client-side.
///
/// Created vs Updated is per-subscriber state: the FIRST `put`
/// event a subscriber sees for any given key surfaces as
/// `Created`; subsequent puts are `Updated`. `delete` removes
/// the key from the seen-set so a re-create surfaces as
/// `Created` again.
///
/// Stream maintenance: `XADD` writes use `MAXLEN ~ 10000` to bound
/// the stream's memory footprint. Operators with very high
/// write rates and deep watcher backlogs may want to tune this
/// up via a dedicated config knob — out of scope for v0.2.
pub struct RedisWatch {
    inner: Arc<RedisWatchInner>,
}

impl std::fmt::Debug for RedisWatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisWatch")
            .field("stream_key", &self.inner.stream_key)
            .field("key_prefix", &self.inner.key_prefix)
            .finish()
    }
}

struct RedisWatchInner {
    /// Client used to open a fresh, dedicated connection per
    /// `watch_prefix` subscriber. A subscriber's XREAD BLOCK ties
    /// up its connection at the Redis-protocol level (BLOCK
    /// suspends client-side dispatch on the multiplexed pipeline),
    /// so a watcher cannot share its socket with the publisher
    /// (`RedisKv`) without the publisher's XADD stalling until the
    /// BLOCK returns. The equivalence test
    /// `test_watch_primitive_emits_create_update_delete` reproduces
    /// the stall in <1s. Each watcher therefore gets its own
    /// MultiplexedConnection minted on demand.
    client: Client,
    /// Stream key shared with the publishing `RedisKv`.
    stream_key: String,
    /// Prepended to every key in `WatchEvent.key` before the
    /// subscriber's prefix-filter runs. We strip it on the
    /// subscriber side so the user-visible key matches what
    /// they passed to `kv.put`.
    key_prefix: String,
}

impl RedisWatch {
    /// Construct a watcher that listens on `stream_key`. Use the
    /// same `stream_key` the matching [`crate::RedisKv`] writes
    /// into (default: `<key_prefix>:watch-stream`).
    ///
    /// `key_prefix` must match the KV's `key_prefix` so the
    /// publisher's full keys can be stripped back to user-visible
    /// shape.
    pub fn with_client(client: Client, key_prefix: String, stream_key: String) -> Self {
        Self {
            inner: Arc::new(RedisWatchInner {
                client,
                stream_key,
                key_prefix,
            }),
        }
    }
}

/// Default stream-key suffix appended to a `key_prefix`.
pub fn default_watch_stream_key(key_prefix: &str) -> String {
    if key_prefix.is_empty() {
        "mcpg:watch-stream".to_owned()
    } else {
        format!("{key_prefix}:watch-stream")
    }
}

#[async_trait]
impl Watch for RedisWatch {
    async fn watch_prefix(&self, prefix: &str) -> Result<WatchStream, ClusterError> {
        // Channel backing the returned stream. 256 matches MemoryBus
        // / WatchHub. A slow consumer that fills this channel sees
        // the pump task drop messages — the contract says watch is
        // best-effort, so this matches the trait expectation.
        let (tx, rx) = mpsc::channel::<Result<WatchEvent, ClusterError>>(256);
        let inner = Arc::clone(&self.inner);
        let prefix_owned = prefix.to_owned();
        // Strip the configured key_prefix off the publisher's full
        // key before the user-visible prefix-filter runs.
        let strip_prefix = if inner.key_prefix.is_empty() {
            String::new()
        } else {
            format!("{}:", inner.key_prefix)
        };
        tokio::spawn(async move {
            // Each watcher gets its OWN MultiplexedConnection so a
            // 5 s XREAD BLOCK can't stall the publisher (which uses
            // the shared connection). If the initial connect fails
            // we surface the error and exit; the subscriber can
            // resubscribe.
            let mut conn = match inner.client.get_multiplexed_async_connection().await {
                Ok(c) => c,
                Err(e) => {
                    let _ = tx
                        .send(Err(ClusterError::BackendUnavailable {
                            reason: format!("redis watch connect: {e}"),
                        }))
                        .await;
                    return;
                }
            };
            // Start at `$` so we only see events posted AFTER
            // subscription. Resume cursor advances per XREAD result.
            let mut last_id = "$".to_owned();
            let mut seen_keys: HashSet<String> = HashSet::new();
            loop {
                // Block up to 5 s per call. The shorter the block,
                // the faster this loop notices the consumer dropped
                // (rx.is_closed); the longer, the less Redis idle
                // chatter. 5 s is the same default cluster.redis
                // uses elsewhere.
                let xread_args = redis::streams::StreamReadOptions::default()
                    .block(5_000)
                    .count(100);
                let reply: Option<redis::streams::StreamReadReply> = match conn
                    .xread_options(&[&inner.stream_key], &[&last_id], &xread_args)
                    .await
                {
                    Ok(r) => r,
                    Err(e) => {
                        // Surface the error and exit. Subscriber may
                        // resubscribe.
                        let _ = tx
                            .send(Err(ClusterError::BackendUnavailable {
                                reason: format!("redis xread: {e}"),
                            }))
                            .await;
                        return;
                    }
                };
                let Some(reply) = reply else {
                    // BLOCK timeout with no new events; loop
                    // continues. Check whether the consumer is
                    // still listening before re-entering BLOCK.
                    if tx.is_closed() {
                        return;
                    }
                    continue;
                };
                if reply.keys.is_empty() {
                    if tx.is_closed() {
                        return;
                    }
                    continue;
                }
                for stream in reply.keys {
                    for entry in stream.ids {
                        last_id = entry.id.clone();
                        let op = entry.map.get("op").and_then(redis_value_as_str);
                        let key = entry.map.get("key").and_then(redis_value_as_str);
                        let value = entry.map.get("value").and_then(redis_value_as_bytes);
                        let (Some(op), Some(full_key)) = (op, key) else {
                            continue; // malformed entry — skip
                        };
                        // Strip the publisher's key_prefix.
                        let user_key =
                            if !strip_prefix.is_empty() && full_key.starts_with(&strip_prefix) {
                                full_key[strip_prefix.len()..].to_owned()
                            } else {
                                full_key
                            };
                        if !user_key.starts_with(&prefix_owned) {
                            continue;
                        }
                        let event = match op.as_str() {
                            "put" => {
                                let kind = if seen_keys.insert(user_key.clone()) {
                                    WatchEventKind::Created
                                } else {
                                    WatchEventKind::Updated
                                };
                                WatchEvent {
                                    key: user_key,
                                    kind,
                                    value,
                                }
                            }
                            "delete" => {
                                seen_keys.remove(&user_key);
                                WatchEvent {
                                    key: user_key,
                                    kind: WatchEventKind::Deleted,
                                    value,
                                }
                            }
                            _ => continue,
                        };
                        if tx.send(Ok(event)).await.is_err() {
                            return; // consumer dropped
                        }
                    }
                }
            }
        });
        let stream = futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        });
        Ok(Box::pin(stream))
    }
}

fn redis_value_as_str(v: &redis::Value) -> Option<String> {
    match v {
        redis::Value::BulkString(b) => std::str::from_utf8(b).ok().map(|s| s.to_owned()),
        redis::Value::SimpleString(s) => Some(s.clone()),
        _ => None,
    }
}

fn redis_value_as_bytes(v: &redis::Value) -> Option<Bytes> {
    match v {
        redis::Value::BulkString(b) => Some(Bytes::copy_from_slice(b)),
        redis::Value::SimpleString(s) => Some(Bytes::copy_from_slice(s.as_bytes())),
        _ => None,
    }
}

/// Lua script the publishing `RedisKv` runs on every `put` when a
/// watch stream is configured. SET (with optional PX) + XADD inside
/// a single round-trip — atomic, monotonic.
///
/// KEYS:
///   1 — full KV key
///   2 — watch stream key
/// ARGV:
///   1 — value bytes
///   2 — ttl_ms (decimal string; "0" means no TTL)
pub const PUT_WITH_WATCH_LUA: &str = r#"
local ttl = tonumber(ARGV[2]) or 0
if ttl > 0 then
  redis.call('SET', KEYS[1], ARGV[1], 'PX', ttl)
else
  redis.call('SET', KEYS[1], ARGV[1])
  redis.call('PERSIST', KEYS[1])
end
redis.call('XADD', KEYS[2], 'MAXLEN', '~', '10000', '*',
           'op', 'put', 'key', KEYS[1], 'value', ARGV[1])
return 1
"#;

/// Lua script the publishing `RedisKv` runs for `put_if_absent` when a
/// watch stream is configured. `SET NX` (with optional PX) + a watch
/// `XADD` **only when the set actually happened** — atomic single-winner
/// claim. Returns 1 when this caller created the key, 0 when it already
/// existed (no XADD on a lost claim).
///
/// KEYS:
///   1 — full KV key
///   2 — watch stream key
/// ARGV:
///   1 — value bytes
///   2 — ttl_ms (decimal string; "0" means no TTL)
pub const PUT_IF_ABSENT_WITH_WATCH_LUA: &str = r#"
local ttl = tonumber(ARGV[2]) or 0
local ok
if ttl > 0 then
  ok = redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ttl)
else
  ok = redis.call('SET', KEYS[1], ARGV[1], 'NX')
end
if ok then
  redis.call('XADD', KEYS[2], 'MAXLEN', '~', '10000', '*',
             'op', 'put', 'key', KEYS[1], 'value', ARGV[1])
  return 1
else
  return 0
end
"#;

/// Lua script the publishing `RedisKv` runs on every `delete` when
/// a watch stream is configured. GET (to capture the prior value
/// for the watch event) + DEL + XADD inside a single round-trip.
/// Returns 1 if the key was present, 0 otherwise (no XADD when
/// nothing existed — matches MemoryKv's "Delete event only when
/// the key existed" contract).
///
/// KEYS:
///   1 — full KV key
///   2 — watch stream key
pub const DELETE_WITH_WATCH_LUA: &str = r#"
local val = redis.call('GET', KEYS[1])
if val == false then
  return 0
end
redis.call('DEL', KEYS[1])
redis.call('XADD', KEYS[2], 'MAXLEN', '~', '10000', '*',
           'op', 'delete', 'key', KEYS[1], 'value', val)
return 1
"#;

/// Convenience: TTL `Duration` → milliseconds string for the put
/// script's `ARGV[2]`. Returns `"0"` for `None`.
pub(crate) fn ttl_arg(ttl: Option<Duration>) -> String {
    match ttl {
        Some(d) => d.as_millis().min(u64::MAX as u128).to_string(),
        None => "0".to_owned(),
    }
}

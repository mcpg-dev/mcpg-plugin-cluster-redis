use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use mcpg_cluster_api::{ClusterError, Entry, KeyValueStore, KvPage};
use redis::aio::ConnectionManager;

// Every write runs as one script that changes the data key, its entry in
// the key index (a sorted set in lex order) and its entry in the expiry
// index (a sorted set scored by expiry, Unix ms) together. Redis expires
// the data key itself; a sweep then drops its index entries, and a listing
// drops those of a key it finds gone. KEYS are always: data, key index,
// expiry index.

/// Store a value; with `ARGV[4] == '1'` only when the key is absent.
/// ARGV: value, ttl ms (0 = none), now ms, nx. Returns 1 when stored.
const SET_LUA: &str = r"
local ttl = tonumber(ARGV[2])
local ok
if ARGV[4] == '1' then
  if ttl > 0 then
    ok = redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ttl)
  else
    ok = redis.call('SET', KEYS[1], ARGV[1], 'NX')
  end
  if not ok then return 0 end
elseif ttl > 0 then
  redis.call('SET', KEYS[1], ARGV[1], 'PX', ttl)
else
  redis.call('SET', KEYS[1], ARGV[1])
end
if ttl > 0 then
  redis.call('ZADD', KEYS[3], tonumber(ARGV[3]) + ttl, KEYS[1])
else
  redis.call('ZREM', KEYS[3], KEYS[1])
end
redis.call('ZADD', KEYS[2], 0, KEYS[1])
return 1
";

/// Delete a key. Returns the number of keys deleted.
const DEL_LUA: &str = r"
local n = redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], KEYS[1])
redis.call('ZREM', KEYS[3], KEYS[1])
return n
";

/// Set or clear a live key's TTL. ARGV: ttl ms (0 = clear), now ms.
/// Returns 0 when the key is absent.
const EXPIRE_LUA: &str = r"
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
local ttl = tonumber(ARGV[1])
if ttl > 0 then
  redis.call('PEXPIRE', KEYS[1], ttl)
  redis.call('ZADD', KEYS[3], tonumber(ARGV[2]) + ttl, KEYS[1])
else
  redis.call('PERSIST', KEYS[1])
  redis.call('ZREM', KEYS[3], KEYS[1])
end
redis.call('ZADD', KEYS[2], 0, KEYS[1])
return 1
";

/// `INCRBY` + optional `PEXPIRE`, so the add-and-get and its sliding TTL
/// re-arm are atomic against every other client. A non-integer value (or
/// i64 overflow) makes `INCRBY` raise; the sentinel prefix lets the host
/// map that onto `ClusterError::Precondition` instead of a generic backend
/// error. ARGV: delta, ttl ms (0 = keep), now ms.
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
  redis.call('ZADD', KEYS[3], tonumber(ARGV[3]) + ttl, KEYS[1])
end
redis.call('ZADD', KEYS[2], 0, KEYS[1])
return ret
";

/// `incr_floor`: raise the counter to at least the floor, then add, as one
/// `INCRBY` of the difference, with the same TTL and index steps as
/// [`INCR_LUA`]. Lua computes with doubles, exact below 2^53 (a microsecond
/// clock is about 1.8e15), so an operand at or past it is refused rather
/// than rounded. ARGV: delta, ttl ms (0 = keep), now ms, floor.
const INCR_FLOOR_LUA: &str = r"
local exact = 9007199254740992
local raw = redis.call('GET', KEYS[1])
local current = 0
if raw then current = tonumber(raw) end
local delta = tonumber(ARGV[1])
local floor = tonumber(ARGV[4])
if current == nil then
  return redis.error_reply('MCPG_KV_INCR_PRECONDITION: incr target holds a non-integer value')
end
if math.abs(current) >= exact or math.abs(floor) >= exact or math.abs(delta) >= exact then
  return redis.error_reply('MCPG_KV_INCR_PRECONDITION: incr_floor operand beyond 2^53')
end
local base = current
if base < floor then base = floor end
local ok, ret = pcall(redis.call, 'INCRBY', KEYS[1], string.format('%.0f', base - current + delta))
if not ok then
  local detail = ''
  if type(ret) == 'table' and ret.err then detail = ': ' .. ret.err end
  return redis.error_reply('MCPG_KV_INCR_PRECONDITION' .. detail)
end
local ttl = tonumber(ARGV[2])
if ttl and ttl > 0 then
  redis.call('PEXPIRE', KEYS[1], ttl)
  redis.call('ZADD', KEYS[3], tonumber(ARGV[3]) + ttl, KEYS[1])
end
redis.call('ZADD', KEYS[2], 0, KEYS[1])
return ret
";

/// Drop the index entries of the keys in ARGV that are gone. KEYS: key
/// index, expiry index. Returns how many were dropped.
const PRUNE_LUA: &str = r"
local n = 0
for i = 1, #ARGV do
  if redis.call('EXISTS', ARGV[i]) == 0 then
    redis.call('ZREM', KEYS[1], ARGV[i])
    redis.call('ZREM', KEYS[2], ARGV[i])
    n = n + 1
  end
end
return n
";

/// Settle the keys in ARGV[2..] that the expiry index says are due at
/// ARGV[1] (now ms): drop a gone key's entries, forget a key that lost its
/// TTL, re-score one whose TTL moved. KEYS: key index, expiry index.
/// Returns how many keys were gone.
const SWEEP_LUA: &str = r"
local now = tonumber(ARGV[1])
local gone = 0
for i = 2, #ARGV do
  local k = ARGV[i]
  local pttl = redis.call('PTTL', k)
  if pttl == -2 then
    redis.call('ZREM', KEYS[1], k)
    redis.call('ZREM', KEYS[2], k)
    gone = gone + 1
  elseif pttl == -1 then
    redis.call('ZREM', KEYS[2], k)
  else
    redis.call('ZADD', KEYS[2], now + pttl, k)
  end
end
return gone
";

/// Index the live keys in ARGV[2..] (ARGV[1] = now ms). KEYS: key index,
/// expiry index. Returns how many were indexed.
const BACKFILL_LUA: &str = r"
local now = tonumber(ARGV[1])
local n = 0
for i = 2, #ARGV do
  local k = ARGV[i]
  local pttl = redis.call('PTTL', k)
  if pttl ~= -2 then
    redis.call('ZADD', KEYS[1], 0, k)
    if pttl > 0 then redis.call('ZADD', KEYS[2], now + pttl, k) end
    n = n + 1
  end
end
return n
";

static SET_SCRIPT: LazyLock<redis::Script> = LazyLock::new(|| redis::Script::new(SET_LUA));
static DEL_SCRIPT: LazyLock<redis::Script> = LazyLock::new(|| redis::Script::new(DEL_LUA));
static EXPIRE_SCRIPT: LazyLock<redis::Script> = LazyLock::new(|| redis::Script::new(EXPIRE_LUA));
static INCR_SCRIPT: LazyLock<redis::Script> = LazyLock::new(|| redis::Script::new(INCR_LUA));
static INCR_FLOOR_SCRIPT: LazyLock<redis::Script> =
    LazyLock::new(|| redis::Script::new(INCR_FLOOR_LUA));
static PRUNE_SCRIPT: LazyLock<redis::Script> = LazyLock::new(|| redis::Script::new(PRUNE_LUA));
static SWEEP_SCRIPT: LazyLock<redis::Script> = LazyLock::new(|| redis::Script::new(SWEEP_LUA));
static BACKFILL_SCRIPT: LazyLock<redis::Script> =
    LazyLock::new(|| redis::Script::new(BACKFILL_LUA));

/// Keys a `SCAN` step looks at.
const SCAN_COUNT: usize = 1000;

/// Keys a sweep settles per script call.
const SWEEP_BATCH: usize = 1000;

/// Redis-backed KV state. Holds a multiplexed `ConnectionManager` (with
/// automatic reconnect) that every operation clones, so operations run
/// concurrently.
pub struct RedisKv {
    inner: Arc<RedisKvInner>,
}

struct RedisKvInner {
    conn: ConnectionManager,
    key_prefix: String,
    /// The key index: every data key, score 0, so it sorts in byte order.
    index_key: String,
    /// The expiry index: every data key with a TTL, scored by its expiry.
    expiry_key: String,
    /// When the key index was last made complete (Unix ms).
    complete_key: String,
    /// Listings read the key index: every writer of the fleet writes it
    /// and a backfill completed it. Until then they scan.
    index_ready: AtomicBool,
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
        // Data keys are `{key_prefix}:{key}`; a `#` after the prefix keeps
        // the indexes out of every data key's way.
        let index_key = format!("{key_prefix}#kv-index");
        let expiry_key = format!("{key_prefix}#kv-expiry");
        let complete_key = format!("{key_prefix}#kv-index-complete");
        Self {
            inner: Arc::new(RedisKvInner {
                conn,
                key_prefix,
                index_key,
                expiry_key,
                complete_key,
                index_ready: AtomicBool::new(false),
            }),
        }
    }

    fn conn(&self) -> ConnectionManager {
        self.inner.conn.clone()
    }

    fn full_key(&self, key: &str) -> String {
        if self.inner.key_prefix.is_empty() {
            key.to_owned()
        } else {
            format!("{}:{}", self.inner.key_prefix, key)
        }
    }

    /// The caller's key for a full Redis key.
    fn logical_key(&self, full: String) -> String {
        if self.inner.key_prefix.is_empty() {
            return full;
        }
        full.strip_prefix(&format!("{}:", self.inner.key_prefix))
            .map(str::to_owned)
            .unwrap_or(full)
    }

    /// Let listings read the key index, or make them scan.
    pub fn set_index_ready(&self, ready: bool) {
        self.inner.index_ready.store(ready, Ordering::Release);
    }

    /// Whether listings read the key index.
    pub fn index_ready(&self) -> bool {
        self.inner.index_ready.load(Ordering::Acquire)
    }

    /// The live entries of `keys` (full Redis keys), in one round trip; a
    /// key gone since it was listed is left out, and its index entries are
    /// dropped when `prune`.
    async fn fetch_entries(
        &self,
        keys: Vec<String>,
        prune: bool,
    ) -> Result<Vec<(String, Entry)>, ClusterError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut pipe = redis::pipe();
        for key in &keys {
            pipe.cmd("GET").arg(key).cmd("PTTL").arg(key);
        }
        let replies: Vec<redis::Value> = pipe
            .query_async(&mut self.conn())
            .await
            .map_err(redis_err)?;
        let now = SystemTime::now();
        let mut out = Vec::with_capacity(keys.len());
        let mut gone = Vec::new();
        for (key, pair) in keys.into_iter().zip(replies.chunks(2)) {
            let [value, pttl] = pair else {
                continue;
            };
            let bytes = redis::from_redis_value::<Option<Vec<u8>>>(value)
                .ok()
                .flatten();
            let expires_at = match redis::from_redis_value::<i64>(pttl) {
                Ok(-1) => Some(None),
                Ok(ms) if ms >= 0 => Some(Some(now + Duration::from_millis(ms as u64))),
                _ => None,
            };
            match (bytes, expires_at) {
                (Some(bytes), Some(expires_at)) => out.push((
                    self.logical_key(key),
                    Entry {
                        bytes: Bytes::from(bytes),
                        expires_at,
                    },
                )),
                _ => gone.push(key),
            }
        }
        if prune && !gone.is_empty() {
            let mut invocation = PRUNE_SCRIPT.key(&self.inner.index_key);
            invocation.key(&self.inner.expiry_key);
            for key in &gone {
                invocation.arg(key);
            }
            let _: Result<i64, _> = invocation.invoke_async(&mut self.conn()).await;
        }
        Ok(out)
    }

    /// A page from the key index: the keys under `prefix` after `after` (a
    /// full key), in byte order.
    async fn index_page(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<KvPage, ClusterError> {
        let full_prefix = self.full_key(prefix);
        let min = match after {
            Some(full) => format!("({full}").into_bytes(),
            None => format!("[{full_prefix}").into_bytes(),
        };
        // Every key with the prefix sorts below the prefix followed by the
        // byte 0xFF, which UTF-8 never holds.
        let mut max = format!("({full_prefix}").into_bytes();
        max.push(0xFF);
        let mut keys: Vec<String> = redis::cmd("ZRANGEBYLEX")
            .arg(&self.inner.index_key)
            .arg(min)
            .arg(max)
            .arg("LIMIT")
            .arg(0)
            .arg(limit + 1)
            .query_async(&mut self.conn())
            .await
            .map_err(redis_err)?;
        let more = keys.len() > limit;
        keys.truncate(limit);
        let next = more.then(|| keys.last().map(|k| format!("i{k}"))).flatten();
        let entries = self.fetch_entries(keys, true).await?;
        Ok(KvPage { entries, next })
    }

    /// A page from a `SCAN` of the keyspace. `SCAN` steps cannot resume
    /// mid-step, so the matches of a step that do not fit on the page ride
    /// in the cursor to the next page.
    async fn scan_page(
        &self,
        prefix: &str,
        state: ScanCursor,
        limit: usize,
    ) -> Result<KvPage, ClusterError> {
        let pattern = format!("{}*", glob_escape(&self.full_key(prefix)));
        let mut keys = state.pending;
        let mut scan = state.scan;
        let mut done = state.done;
        while keys.len() < limit && !done {
            let (next, batch): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(scan)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(SCAN_COUNT)
                .query_async(&mut self.conn())
                .await
                .map_err(redis_err)?;
            keys.extend(batch);
            scan = next;
            done = next == 0;
        }
        let pending = if keys.len() > limit {
            keys.split_off(limit)
        } else {
            Vec::new()
        };
        let next = if done && pending.is_empty() {
            None
        } else {
            let state = serde_json::to_string(&ScanCursor {
                scan,
                pending,
                done,
            })
            .map_err(|e| ClusterError::Internal {
                reason: format!("redis kv list cursor: {e}"),
            })?;
            Some(format!("s{state}"))
        };
        let entries = self.fetch_entries(keys, false).await?;
        Ok(KvPage { entries, next })
    }

    /// Drop the index entries of every key whose TTL has run out. Returns
    /// how many keys were gone.
    pub async fn sweep(&self) -> Result<u64, ClusterError> {
        let mut gone = 0u64;
        loop {
            let now = now_ms();
            let due: Vec<String> = redis::cmd("ZRANGEBYSCORE")
                .arg(&self.inner.expiry_key)
                .arg("-inf")
                .arg(now)
                .arg("LIMIT")
                .arg(0)
                .arg(SWEEP_BATCH)
                .query_async(&mut self.conn())
                .await
                .map_err(redis_err)?;
            if due.is_empty() {
                return Ok(gone);
            }
            let mut invocation = SWEEP_SCRIPT.key(&self.inner.index_key);
            invocation.key(&self.inner.expiry_key).arg(now);
            for key in &due {
                invocation.arg(key);
            }
            let removed: i64 = invocation
                .invoke_async(&mut self.conn())
                .await
                .map_err(redis_err)?;
            gone += u64::try_from(removed).unwrap_or(0);
            if due.len() < SWEEP_BATCH {
                return Ok(gone);
            }
        }
    }

    /// Index every live data key. Returns how many were indexed. The
    /// caller marks the index complete when no writer without the index
    /// was seen meanwhile.
    pub async fn backfill_index(&self) -> Result<u64, ClusterError> {
        let pattern = if self.inner.key_prefix.is_empty() {
            "*".to_owned()
        } else {
            format!("{}:*", glob_escape(&self.inner.key_prefix))
        };
        let mut scan = 0u64;
        let mut indexed = 0u64;
        loop {
            let (next, batch): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(scan)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(SCAN_COUNT)
                .query_async(&mut self.conn())
                .await
                .map_err(redis_err)?;
            let batch: Vec<String> = batch
                .into_iter()
                .filter(|k| {
                    k != &self.inner.index_key
                        && k != &self.inner.expiry_key
                        && k != &self.inner.complete_key
                })
                .collect();
            if !batch.is_empty() {
                let mut invocation = BACKFILL_SCRIPT.key(&self.inner.index_key);
                invocation.key(&self.inner.expiry_key).arg(now_ms());
                for key in &batch {
                    invocation.arg(key);
                }
                let n: i64 = invocation
                    .invoke_async(&mut self.conn())
                    .await
                    .map_err(redis_err)?;
                indexed += u64::try_from(n).unwrap_or(0);
            }
            scan = next;
            if next == 0 {
                return Ok(indexed);
            }
        }
    }

    /// Record that the key index holds every live key, from a backfill
    /// that started at `started_at_ms` (Unix ms).
    pub async fn mark_index_complete(&self, started_at_ms: u64) -> Result<(), ClusterError> {
        let set: Result<(), _> = redis::cmd("SET")
            .arg(&self.inner.complete_key)
            .arg(started_at_ms)
            .query_async(&mut self.conn())
            .await;
        set.map_err(redis_err)
    }

    /// Whether the key index is marked complete.
    pub async fn index_complete(&self) -> Result<bool, ClusterError> {
        let exists: Result<i64, _> = redis::cmd("EXISTS")
            .arg(&self.inner.complete_key)
            .query_async(&mut self.conn())
            .await;
        exists.map(|n| n > 0).map_err(redis_err)
    }

    /// Mark the key index incomplete: a writer without it is alive.
    pub async fn invalidate_index(&self) -> Result<(), ClusterError> {
        self.set_index_ready(false);
        let deleted: Result<i64, _> = redis::cmd("DEL")
            .arg(&self.inner.complete_key)
            .query_async(&mut self.conn())
            .await;
        deleted.map(|_| ()).map_err(redis_err)
    }

    fn ttl_ms(ttl: Option<Duration>) -> u64 {
        ttl.map(|d| d.as_millis().clamp(1, u128::from(u64::MAX)) as u64)
            .unwrap_or(0)
    }
}

#[async_trait]
impl KeyValueStore for RedisKv {
    async fn get(&self, key: &str) -> Result<Option<Entry>, ClusterError> {
        let full = self.full_key(key);
        let (bytes, pttl): (Option<Vec<u8>>, i64) = redis::pipe()
            .cmd("GET")
            .arg(&full)
            .cmd("PTTL")
            .arg(&full)
            .query_async(&mut self.conn())
            .await
            .map_err(redis_err)?;
        let Some(bytes) = bytes else {
            return Ok(None);
        };
        // PTTL is -2 when the key expired between the two commands.
        let expires_at = match pttl {
            -2 => return Ok(None),
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
        let _: i64 = SET_SCRIPT
            .key(self.full_key(key))
            .key(&self.inner.index_key)
            .key(&self.inner.expiry_key)
            .arg(value.as_ref())
            .arg(Self::ttl_ms(ttl))
            .arg(now_ms())
            .arg("0")
            .invoke_async(&mut self.conn())
            .await
            .map_err(redis_err)?;
        Ok(())
    }

    async fn put_if_absent(
        &self,
        key: &str,
        value: Bytes,
        ttl: Option<Duration>,
    ) -> Result<bool, ClusterError> {
        // Atomic single-winner claim via `SET ... NX`. Redis auto-expires
        // keys, so a lapsed prior claim is already gone → NX naturally
        // succeeds (expired == absent).
        let stored: i64 = SET_SCRIPT
            .key(self.full_key(key))
            .key(&self.inner.index_key)
            .key(&self.inner.expiry_key)
            .arg(value.as_ref())
            .arg(Self::ttl_ms(ttl))
            .arg(now_ms())
            .arg("1")
            .invoke_async(&mut self.conn())
            .await
            .map_err(redis_err)?;
        Ok(stored == 1)
    }

    async fn delete(&self, key: &str) -> Result<bool, ClusterError> {
        let n: i64 = DEL_SCRIPT
            .key(self.full_key(key))
            .key(&self.inner.index_key)
            .key(&self.inner.expiry_key)
            .invoke_async(&mut self.conn())
            .await
            .map_err(redis_err)?;
        Ok(n > 0)
    }

    async fn list_page(
        &self,
        prefix: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<KvPage, ClusterError> {
        // A pass keeps the way its first page read (the cursor says
        // which), so it does not switch halfway.
        let limit = limit.max(1);
        let scan_state = |state: &str| {
            serde_json::from_str::<ScanCursor>(state).map_err(|_| ClusterError::Precondition {
                reason: "redis kv list: not a cursor of this store".to_owned(),
            })
        };
        match cursor {
            None if self.index_ready() => self.index_page(prefix, None, limit).await,
            None => self.scan_page(prefix, ScanCursor::default(), limit).await,
            Some(c) => match c.as_bytes().first() {
                Some(b'i') => self.index_page(prefix, Some(&c[1..]), limit).await,
                Some(b's') => self.scan_page(prefix, scan_state(&c[1..])?, limit).await,
                _ => self.scan_page(prefix, scan_state(c)?, limit).await,
            },
        }
    }

    async fn expire(&self, key: &str, ttl: Option<Duration>) -> Result<bool, ClusterError> {
        let updated: i64 = EXPIRE_SCRIPT
            .key(self.full_key(key))
            .key(&self.inner.index_key)
            .key(&self.inner.expiry_key)
            .arg(Self::ttl_ms(ttl))
            .arg(now_ms())
            .invoke_async(&mut self.conn())
            .await
            .map_err(redis_err)?;
        Ok(updated == 1)
    }

    async fn incr(
        &self,
        key: &str,
        delta: i64,
        ttl: Option<Duration>,
    ) -> Result<i64, ClusterError> {
        // 0 == "leave any existing TTL alone" inside the script.
        let r: Result<i64, redis::RedisError> = INCR_SCRIPT
            .key(self.full_key(key))
            .key(&self.inner.index_key)
            .key(&self.inner.expiry_key)
            .arg(delta)
            .arg(Self::ttl_ms(ttl))
            .arg(now_ms())
            .invoke_async(&mut self.conn())
            .await;
        r.map_err(|e| incr_err("incr", key, e))
    }

    async fn incr_floor(
        &self,
        key: &str,
        delta: i64,
        floor: i64,
        ttl: Option<Duration>,
    ) -> Result<i64, ClusterError> {
        let r: Result<i64, redis::RedisError> = INCR_FLOOR_SCRIPT
            .key(self.full_key(key))
            .key(&self.inner.index_key)
            .key(&self.inner.expiry_key)
            .arg(delta)
            .arg(Self::ttl_ms(ttl))
            .arg(now_ms())
            .arg(floor)
            .invoke_async(&mut self.conn())
            .await;
        r.map_err(|e| incr_err("incr_floor", key, e))
    }
}

/// A counter script's error: its precondition sentinel is a
/// [`ClusterError::Precondition`], anything else the backend's.
fn incr_err(op: &str, key: &str, e: redis::RedisError) -> ClusterError {
    let msg = e.to_string();
    if msg.contains("MCPG_KV_INCR_PRECONDITION") {
        ClusterError::Precondition {
            reason: format!("redis {op} `{key}`: {msg}"),
        }
    } else {
        redis_err(e)
    }
}

/// Where a `SCAN` pass stands: the server cursor of the next step, the
/// matches of the last step that did not fit on the page, and whether the
/// server pass is over.
#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
struct ScanCursor {
    #[serde(rename = "s")]
    scan: u64,
    #[serde(rename = "p", default, skip_serializing_if = "Vec::is_empty")]
    pending: Vec<String>,
    #[serde(rename = "d", default, skip_serializing_if = "std::ops::Not::not")]
    done: bool,
}

/// `key` as a `SCAN MATCH` pattern that matches only itself.
fn glob_escape(key: &str) -> String {
    let mut out = String::with_capacity(key.len());
    for c in key.chars() {
        if matches!(c, '*' | '?' | '[' | ']' | '\\') {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn redis_err(e: redis::RedisError) -> ClusterError {
    ClusterError::BackendUnavailable {
        reason: format!("redis: {e}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_prefix_with_glob_characters_matches_only_itself() {
        assert_eq!(glob_escape("a*b?c[d]e\\f"), "a\\*b\\?c\\[d\\]e\\\\f");
        assert_eq!(glob_escape("mcpg:cluster::sub:u:"), "mcpg:cluster::sub:u:");
    }

    #[test]
    fn a_scan_cursor_round_trips_compactly() {
        let state = ScanCursor {
            scan: 42,
            pending: vec!["k1".into()],
            done: true,
        };
        let json = serde_json::to_string(&state).unwrap();
        assert_eq!(json, r#"{"s":42,"p":["k1"],"d":true}"#);
        let back: ScanCursor = serde_json::from_str(&json).unwrap();
        assert_eq!(
            (back.scan, back.pending, back.done),
            (42, vec!["k1".to_owned()], true)
        );
        let fresh: ScanCursor = serde_json::from_str(r#"{"s":7}"#).unwrap();
        assert!(fresh.pending.is_empty() && !fresh.done);
    }
}

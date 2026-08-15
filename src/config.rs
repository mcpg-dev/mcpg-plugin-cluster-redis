//! Operator-supplied configuration schema for `dev.mcpg.cluster.redis`.

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedisBackendConfig {
    /// Redis connection URL — `redis://…` or `rediss://…`. Prefer
    /// `rediss://` (the gateway boot guard rejects a plaintext
    /// coordinator unless `cluster.allow_insecure_transport`).
    pub url: String,

    /// Optional Redis ACL username (Redis 6+), applied to the
    /// connection alongside / instead of any userinfo in `url`.
    #[serde(default)]
    pub username: Option<String>,

    /// Optional Redis password. Preferred over embedding the credential
    /// in `url` (which would land in logs / the rendered ConfigMap).
    /// Resolve from a secret via `${env.VAR}` / `env://VAR`. Redacted in
    /// `Debug`.
    #[serde(default)]
    pub password: Option<String>,

    /// Prefix prepended to every key the coordinator owns. Lets
    /// operators run multiple gateways against one Redis instance
    /// by giving each a distinct namespace.
    #[serde(default = "default_key_prefix")]
    pub key_prefix: String,

    /// Default TTL applied to leases (`acquire_lock`,
    /// `acquire_leadership`) when the caller doesn't supply one.
    #[serde(default = "default_lease_ttl_ms")]
    pub lease_ttl_ms: u64,

    /// TTL applied to the per-instance peer-presence key. The
    /// background refresher writes the key every
    /// `peer_refresh_interval_ms` (defaulting to `peer_ttl_ms / 2`),
    /// so a missing refresh expires the peer within `peer_ttl_ms`.
    #[serde(default = "default_peer_ttl_ms")]
    pub peer_ttl_ms: u64,

    /// Optional operator-stable node id. When omitted the coordinator
    /// generates a synthesised id at boot (service_name + hostname,
    /// falling back to a random suffix when the hostname is missing).
    #[serde(default)]
    pub node_id: Option<String>,

    /// Background renewal task fires every
    /// `ttl × (100 - pct) / 100`. Default 80 — renewal at 20% of
    /// the TTL has elapsed (keeps margin for Lua + RTT). Clamped
    /// to `[1, 99]` at runtime.
    #[serde(default = "default_renew_pct")]
    pub lease_renew_before_expiry_percent: u32,

    /// How often the background peer-refresher re-runs `register_peer`.
    /// Defaults to `peer_ttl_ms / 2` when omitted (so a single missed
    /// tick still leaves slack before the peer key expires).
    #[serde(default)]
    pub peer_refresh_interval_ms: Option<u64>,

    /// Buffer size of the mpsc channel feeding subscriber streams +
    /// peer-event watch streams. Default 256.
    #[serde(default = "default_subscribe_pattern_buffer")]
    pub subscribe_pattern_buffer: usize,

    /// Logical service name this gateway instance announces under.
    /// Used as a prefix for the synthesised node_id when one isn't
    /// configured.
    #[serde(default = "default_service_name")]
    pub service_name: String,
}

impl std::fmt::Debug for RedisBackendConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Manual impl so the `password` (and any credential in `url`) is
        // never rendered into a log line / panic message.
        f.debug_struct("RedisBackendConfig")
            .field("url", &redact_redis_url(&self.url))
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "***"))
            .field("key_prefix", &self.key_prefix)
            .field("lease_ttl_ms", &self.lease_ttl_ms)
            .field("peer_ttl_ms", &self.peer_ttl_ms)
            .field("node_id", &self.node_id)
            .field(
                "lease_renew_before_expiry_percent",
                &self.lease_renew_before_expiry_percent,
            )
            .field("peer_refresh_interval_ms", &self.peer_refresh_interval_ms)
            .field("subscribe_pattern_buffer", &self.subscribe_pattern_buffer)
            .field("service_name", &self.service_name)
            .finish()
    }
}

/// Strip any `user:pass@` userinfo from a redis URL so it is safe to log.
///
/// The `@` search is bounded to the **authority** component (between
/// `://` and the first `/`, `?` or `#`). A bare `rfind('@')` over the
/// whole URL would mis-fire on an `@` in the path/query (e.g.
/// `?sentinel=a@b`), truncating the host — so we locate the authority
/// first and only redact userinfo inside it. Scheme, host:port, and any
/// path/query are preserved verbatim.
pub(crate) fn redact_redis_url(url: &str) -> String {
    let Some(scheme_end) = url.find("://") else {
        return url.to_owned();
    };
    let authority_start = scheme_end + 3;
    let authority_end = url[authority_start..]
        .find(['/', '?', '#'])
        .map_or(url.len(), |i| authority_start + i);
    let authority = &url[authority_start..authority_end];
    match authority.rfind('@') {
        Some(rel_at) => format!(
            "{}{}",
            &url[..authority_start],
            &url[authority_start + rel_at + 1..]
        ),
        None => url.to_owned(),
    }
}

fn default_key_prefix() -> String {
    "mcpg:cluster:".into()
}

fn default_lease_ttl_ms() -> u64 {
    30_000
}

fn default_peer_ttl_ms() -> u64 {
    60_000
}

fn default_renew_pct() -> u32 {
    80
}

fn default_subscribe_pattern_buffer() -> usize {
    256
}

fn default_service_name() -> String {
    "mcpg".into()
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("invalid cluster.redis config JSON: {0}")]
    InvalidJson(#[from] serde_json::Error),
    #[error("cluster.redis: url is empty")]
    EmptyUrl,
    #[error("cluster.redis: url must start with redis:// or rediss://")]
    InvalidUrlScheme,
    #[error("cluster.redis: key_prefix is empty (use a distinct namespace per deployment)")]
    EmptyKeyPrefix,
    #[error("cluster.redis: lease_ttl_ms must be > 0")]
    InvalidLeaseTtl,
    #[error("cluster.redis: peer_ttl_ms must be > 0")]
    InvalidPeerTtl,
    #[error("cluster.redis: service_name is empty")]
    EmptyServiceName,
    #[error("cluster.redis: subscribe_pattern_buffer must be > 0")]
    InvalidBuffer,
}

impl RedisBackendConfig {
    pub fn parse(s: &str) -> Result<Self, ConfigError> {
        let cfg: Self = serde_json::from_str(s)?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if self.url.trim().is_empty() {
            return Err(ConfigError::EmptyUrl);
        }
        if !self.url.starts_with("redis://") && !self.url.starts_with("rediss://") {
            return Err(ConfigError::InvalidUrlScheme);
        }
        if self.key_prefix.is_empty() {
            return Err(ConfigError::EmptyKeyPrefix);
        }
        if self.lease_ttl_ms == 0 {
            return Err(ConfigError::InvalidLeaseTtl);
        }
        if self.peer_ttl_ms == 0 {
            return Err(ConfigError::InvalidPeerTtl);
        }
        if self.service_name.trim().is_empty() {
            return Err(ConfigError::EmptyServiceName);
        }
        if self.subscribe_pattern_buffer == 0 {
            return Err(ConfigError::InvalidBuffer);
        }
        Ok(())
    }

    /// Stable node id for this gateway instance. Resolution order:
    /// 1. Explicit `node_id` from operator config.
    /// 2. `<service_name>-<hostname>` when `HOSTNAME` is set.
    /// 3. `<service_name>-<random-128-bit hex>` as a last resort.
    pub fn resolved_node_id(&self) -> String {
        if let Some(id) = &self.node_id {
            return id.clone();
        }
        let host = std::env::var("HOSTNAME").ok();
        match host {
            Some(h) if !h.trim().is_empty() => format!("{}-{h}", self.service_name),
            _ => format!("{}-{}", self.service_name, random_suffix()),
        }
    }

    /// Effective peer-refresh tick, defaulting to `peer_ttl_ms / 2`
    /// when not set by the operator.
    pub fn effective_peer_refresh_interval_ms(&self) -> u64 {
        self.peer_refresh_interval_ms
            .unwrap_or_else(|| (self.peer_ttl_ms / 2).max(1))
    }
}

/// Synthesises a 128-bit random hex string by hashing the current
/// time + a thread-local counter. Avoids the `uuid` dep — operators
/// who need deterministic ids set `node_id` explicitly.
fn random_suffix() -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let now_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let mut hasher = DefaultHasher::new();
    now_ns.hash(&mut hasher);
    seq.hash(&mut hasher);
    let h1 = hasher.finish();
    seq.wrapping_add(0x9E37_79B9_7F4A_7C15).hash(&mut hasher);
    now_ns.wrapping_mul(0xBF58_476D_1CE4_E5B9).hash(&mut hasher);
    let h2 = hasher.finish();
    format!("{h1:016x}{h2:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_minimal_config() {
        let cfg = RedisBackendConfig::parse(&json!({"url": "redis://127.0.0.1:6379"}).to_string())
            .unwrap();
        assert_eq!(cfg.url, "redis://127.0.0.1:6379");
        assert_eq!(cfg.key_prefix, "mcpg:cluster:");
        assert_eq!(cfg.lease_ttl_ms, 30_000);
        assert_eq!(cfg.peer_ttl_ms, 60_000);
        assert_eq!(cfg.lease_renew_before_expiry_percent, 80);
        assert_eq!(cfg.subscribe_pattern_buffer, 256);
        assert_eq!(cfg.service_name, "mcpg");
        assert!(cfg.node_id.is_none());
        assert!(cfg.username.is_none());
        assert!(cfg.password.is_none());
        assert_eq!(cfg.effective_peer_refresh_interval_ms(), 30_000);
    }

    #[test]
    fn parses_username_and_password() {
        // Out-of-URL credentials.
        let cfg = RedisBackendConfig::parse(
            &json!({"url": "rediss://r:6380", "username": "u", "password": "p"}).to_string(),
        )
        .unwrap();
        assert_eq!(cfg.username.as_deref(), Some("u"));
        assert_eq!(cfg.password.as_deref(), Some("p"));
    }

    #[test]
    fn debug_redacts_password_and_url_userinfo() {
        let cfg = RedisBackendConfig::parse(
            &json!({"url": "rediss://user:secret@r:6380", "password": "topsecret"}).to_string(),
        )
        .unwrap();
        let dbg = format!("{cfg:?}");
        assert!(
            !dbg.contains("topsecret"),
            "password leaked in Debug: {dbg}"
        );
        assert!(
            !dbg.contains("secret@"),
            "url userinfo leaked in Debug: {dbg}"
        );
    }

    #[test]
    fn redact_redis_url_strips_userinfo() {
        assert_eq!(
            super::redact_redis_url("rediss://user:pass@host:6380/0"),
            "rediss://host:6380/0"
        );
        // No userinfo → unchanged.
        assert_eq!(
            super::redact_redis_url("redis://host:6379"),
            "redis://host:6379"
        );
        // An '@' in the path/query must NOT be mistaken for userinfo: the
        // authority (host:port) and the query are preserved intact.
        assert_eq!(
            super::redact_redis_url("redis://host:6379/0?token=a@b"),
            "redis://host:6379/0?token=a@b"
        );
        assert_eq!(
            super::redact_redis_url("rediss://user:pass@host:6380/0?sentinel=a@b.example"),
            "rediss://host:6380/0?sentinel=a@b.example"
        );
        // An '@' inside the password is still stripped (last '@' in authority).
        assert_eq!(
            super::redact_redis_url("rediss://user:p@ss@host:6380"),
            "rediss://host:6380"
        );
    }

    #[test]
    fn parses_overrides() {
        let cfg = RedisBackendConfig::parse(
            &json!({
                "url": "rediss://r.svc:6380",
                "key_prefix": "stage:",
                "lease_ttl_ms": 1000,
                "peer_ttl_ms": 2000,
                "node_id": "alpha",
                "lease_renew_before_expiry_percent": 50,
                "peer_refresh_interval_ms": 333,
                "subscribe_pattern_buffer": 16,
                "service_name": "gw"
            })
            .to_string(),
        )
        .unwrap();
        assert_eq!(cfg.url, "rediss://r.svc:6380");
        assert_eq!(cfg.key_prefix, "stage:");
        assert_eq!(cfg.lease_ttl_ms, 1000);
        assert_eq!(cfg.peer_ttl_ms, 2000);
        assert_eq!(cfg.node_id.as_deref(), Some("alpha"));
        assert_eq!(cfg.lease_renew_before_expiry_percent, 50);
        assert_eq!(cfg.subscribe_pattern_buffer, 16);
        assert_eq!(cfg.effective_peer_refresh_interval_ms(), 333);
        assert_eq!(cfg.resolved_node_id(), "alpha");
    }

    #[test]
    fn rejects_empty_url() {
        let err = RedisBackendConfig::parse(&json!({"url": ""}).to_string()).unwrap_err();
        assert!(matches!(err, ConfigError::EmptyUrl));
    }

    #[test]
    fn rejects_invalid_scheme() {
        let err =
            RedisBackendConfig::parse(&json!({"url": "http://r:6379"}).to_string()).unwrap_err();
        assert!(matches!(err, ConfigError::InvalidUrlScheme));
    }

    #[test]
    fn rejects_zero_lease_ttl() {
        let err = RedisBackendConfig::parse(
            &json!({"url": "redis://r:6379", "lease_ttl_ms": 0}).to_string(),
        )
        .unwrap_err();
        assert!(matches!(err, ConfigError::InvalidLeaseTtl));
    }

    #[test]
    fn rejects_zero_peer_ttl() {
        let err = RedisBackendConfig::parse(
            &json!({"url": "redis://r:6379", "peer_ttl_ms": 0}).to_string(),
        )
        .unwrap_err();
        assert!(matches!(err, ConfigError::InvalidPeerTtl));
    }

    #[test]
    fn rejects_empty_service_name() {
        let err = RedisBackendConfig::parse(
            &json!({"url": "redis://r:6379", "service_name": ""}).to_string(),
        )
        .unwrap_err();
        assert!(matches!(err, ConfigError::EmptyServiceName));
    }

    #[test]
    fn resolved_node_id_falls_back_to_synthetic() {
        // SAFETY: tests run single-threaded under cargo test's
        // default; HOSTNAME isn't observed elsewhere here.
        let saved = std::env::var("HOSTNAME").ok();
        // SAFETY: env mutation is unsafe in edition 2024, but this
        // test never observes HOSTNAME concurrently.
        unsafe {
            std::env::remove_var("HOSTNAME");
        }
        let cfg = RedisBackendConfig::parse(&json!({"url": "redis://r:6379"}).to_string()).unwrap();
        let id = cfg.resolved_node_id();
        assert!(id.starts_with("mcpg-"), "got {id}");
        if let Some(v) = saved {
            unsafe {
                std::env::set_var("HOSTNAME", v);
            }
        }
    }
}

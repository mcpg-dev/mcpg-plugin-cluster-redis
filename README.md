# Redis Cluster Coordinator — `dev.mcpg.cluster.redis`

> class `cluster` · `native` · package `mcpg-plugin-cluster-redis` · artifact `libmcpg_plugin_cluster_redis.so` · BUSL-1.1

Redis-backed cluster coordinator for a multi-replica MCPG gateway. It supplies
the full coordination surface — key/value store, pub/sub, leases with fence
tokens, key-change watch, and peer discovery — over a single Redis instance, so
replicas can share sessions, leases, bundle-reload events, and approval
notifications. Gateway capabilities inherit its primitives directly, so a single
Redis instance backs both shared gateway state and cluster coordination. Reach
for it when you already run Redis and do not want to stand up etcd, Consul, or
NATS purely to cluster the gateway.

## What it does
- Implements the four coordination primitives — `KeyValueStore`, `PubSub`,
  `Lease`, and `Watch` — over one shared connection manager.
- Issues leases and leadership with monotonically increasing fence tokens, and
  renews them from a background task before they expire.
- Registers this replica under a TTL'd presence key and refreshes it in the
  background, so a dead replica disappears from peer listings on its own.
- Declares `provides: [cache, kv]`, so gateway capabilities that need a shared
  cache or key/value store inherit those primitives without further wiring;
  capability buses inherit its `pub_sub()` accessor the same way.
- Declares the `network_outbound` capability, consumed by every Redis connection;
  the entry's `granted_capabilities` must list it or the plugin is refused at load.
- Redacts the password and any URL userinfo from `Debug` output, so a panic or
  log line cannot leak the credential.
- Aborts its background workers on `shutdown()` rather than waiting for the last
  outstanding lease or stream guard to drop.
- Validates its configuration at construction but opens the Redis connection on
  first use, so a coordinator that is momentarily unreachable does not fail
  plugin registration itself.

## Configuration
Selected via the dedicated top-level `cluster:` block, keyed by `cluster.kind`.
Kind-specific fields are written **flat** under `cluster:` and flow to the
plugin's factory as JSON; the cdylib itself is still declared in the top-level
`plugins:` list, and the inline `cluster.*` fields override any `config:` block
on the matching entry.

```yaml
cluster:
  kind: redis
  url: ${env.REDIS_URL}             # required; redis:// or rediss://
  username: ${env.REDIS_USERNAME}   # optional Redis 6+ ACL user
  password: ${env.REDIS_PASSWORD}   # preferred over userinfo in the URL
  key_prefix: "mcpg:cluster:"       # one namespace per deployment
  lease_ttl_ms: 30000
  peer_ttl_ms: 60000
  service_name: mcpg
  # node_id: gateway-pod-7          # default <service_name>-<hostname>
  # lease_renew_before_expiry_percent: 80
  # peer_refresh_interval_ms: 30000 # default peer_ttl_ms / 2
  # subscribe_pattern_buffer: 256

plugins:
  - id: dev.mcpg.cluster.redis
    class: cluster
    source: { path: ./plugins/libmcpg_plugin_cluster_redis.so }
    granted_capabilities: ["network_outbound"]
```

| Field | Type | Default | Description |
|---|---|---|---|
| `url` | string | — (required) | `redis://` or `rediss://` connection URL; any other scheme is rejected. |
| `username` | string? | `null` | Redis 6+ ACL username, applied alongside or instead of URL userinfo. |
| `password` | string? | `null` | Redis password; keeps the credential out of the URL and out of logs. |
| `key_prefix` | string | `mcpg:cluster:` | Prefix on every key the coordinator owns; must be non-empty. |
| `lease_ttl_ms` | u64 | `30000` | Default lease TTL when the caller supplies none; must be > 0. |
| `peer_ttl_ms` | u64 | `60000` | TTL on this replica's peer-presence key; must be > 0. |
| `node_id` | string? | synthesised | Stable node identity; see the resolution order below. |
| `service_name` | string | `mcpg` | Logical service name; prefixes the synthesised node id. |
| `lease_renew_before_expiry_percent` | u32 | `80` | Renewal fires after `ttl × (100 − pct) / 100`; clamped to `[1, 99]`. |
| `peer_refresh_interval_ms` | u64? | `peer_ttl_ms / 2` | Cadence of background peer re-registration. |
| `subscribe_pattern_buffer` | usize | `256` | Channel buffer feeding subscriber and peer-event streams; must be > 0. |

Unknown fields are rejected.

`node_id` resolves in three steps: the explicit config value, then
`<service_name>-<HOSTNAME>` when the `HOSTNAME` environment variable is set, then
`<service_name>-<random hex>` as a last resort. Set it explicitly when you need
deterministic identities across restarts.

## Operations

| Coordination call | Redis primitive |
|---|---|
| `acquire_lock` / `acquire_leadership` | Lua `SET NX PX` plus `INCR` on a sibling `:fence` key; renewal re-checks the holder before `PEXPIRE` |
| `publish` / `subscribe` | native `PUBLISH` / `SUBSCRIBE` on the channel `<key_prefix>topic:<topic>`; the routing key travels in the message envelope |
| key/value `get` / `put` / `delete` / prefix scan | `GET` / `SET` / `DEL` / `SCAN` |
| key-change `watch` | a Redis Stream the matching key/value store appends to on every write, consumed with `XREAD BLOCK` and filtered by prefix |
| `list_peers` | `SCAN MATCH <key_prefix>peers/*` followed by `MGET` |
| `watch_peers` | polled diff of successive `list_peers` snapshots |

At boot the gateway probes every primitive this coordinator advertises with a
live round-trip and refuses to start if one fails, so a clustered deployment
never comes up silently de-clustered onto per-replica state. Set
`cluster.allow_degraded_boot: true` to downgrade that to a loud error and start
anyway.

## Security
The coordinator carries shared state — sessions, delivery records, credentials
cached for the fleet — so the gateway refuses to boot a plaintext `redis://`
coordinator unless `cluster.allow_insecure_transport: true` is set explicitly.
Prefer `rediss://` and treat that escape hatch as local-development only. That
check reads the literal configured value, so a URL supplied as `${env.REDIS_URL}`
is opaque to it — point the variable at a TLS endpoint yourself.

Give each deployment its own `key_prefix`, and pair it with Redis ACLs (a
key-pattern rule such as `~mcpg:cluster:*`) so one Redis instance can host
several gateways without letting them read one another's state.

Values written through the coordinator are plaintext unless the gateway is given
a key: `cluster.state_encryption_key_env` names an environment variable holding a
URL-safe-base64 32-byte key, and the gateway then seals coordinator-backed
capability state per key and per topic before it reaches Redis. Key and topic
names stay in the clear so routing still works.

## Build
`cdylib-export` is enabled by default, so the plain build already produces the
loadable artifact. Disable the default features when linking this crate as an
rlib path dependency alongside other plugins, so the build does not emit two
`mcpg_plugin_register` exports.

```bash
cargo build -p mcpg-plugin-cluster-redis --features cdylib-export --release   # → target/release/libmcpg_plugin_cluster_redis.so
```

## Testing
Unit tests run offline with `cargo test -p mcpg-plugin-cluster-redis`. The shared
coordinator equivalence suite — the same contract every MCPG coordinator must
satisfy — boots a `redis:7-alpine` container through testcontainers and needs a
working Docker daemon:

```bash
cargo test -p mcpg-plugin-cluster-redis --features integration-tests --test equivalence
```

## Sign & load (production)
Sign the artifact, pin/verify via the entry's `signature:` block, and honour
revocations. See <https://mcpg.dev/docs/security/plugin-security>.

## See also
- Clustering a gateway fleet: <https://mcpg.dev/docs/self-hosting/clustering>
- Plugin classes and the ABI: <https://mcpg.dev/docs/plugins/plugins-and-protocol>
- Sibling coordinators: `libs/plugins/cluster/etcd`, `libs/plugins/cluster/consul`,
  `libs/plugins/cluster/nats`

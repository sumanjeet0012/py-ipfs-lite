Production memory audit: modules enabled by py-ipfs-lite

The daemon constructs its network in `py_ipfs_lite/peer/_hostfactory.py`, starts
services in `_lifecycle.py`, configures pubsub in `_pubsub.py`, and sends
keepalive pings from `_maintenance.py`. The production command is `daemon --api
--api-host 0.0.0.0`; its environment enables pubsub, disables topic auto-join,
and sets connection watermarks to 300/500. The advertised transports are TCP,
WebSocket and QUIC. A full direct-import inventory is retained separately in
the local investigation artifacts; imports alone do not prove a feature is
enabled.

| Active path | libp2p modules reached | Memory ownership reviewed | Result |
| --- | --- | --- | --- |
| Host and connection management | `host/basic_host`, `network/swarm`, `network/auto_connector`, `network/tag_store`, `rcmgr` | Connection registration, background tasks, dial/disconnect history, tags, resource scopes | Bound recent dial diagnostics; expire disconnect streaks; prune even above target; remove empty unowned tags |
| TCP and WebSocket | `transport/tcp`, `transport/websocket`, `security/noise`, `stream_muxer/yamux`, `network/stream` | Stream dictionaries, buffered data, half-close lifecycle, connection cleanup | Prior stream fixes retained; intentional write half-close preserved |
| QUIC | `transport/quic`, `aioquic` | Wrapper and native stream registries, connection-ID routing, pending handshakes | Prior bidirectional reset cleanup retained; pending-ID cleanup and closed-connection accounting inspected |
| Identity and address observations | `identity/identify`, `identity/identify_push`, `host/observed_addr_manager` | Per-peer identify tasks, connection observations, address cache | Disconnect cleanup and weak connection observations inspected; no new unbounded owner reproduced in this pass |
| Persistent peerstore | `peer/persistent/sync/peerstore`, `datastore/sqlite_sync` | In-memory peer/record caches versus on-disk discovered peers | In-memory caches capped at 1,000; total persisted peer count is not a count of full in-memory records |
| Server-mode DHT and random walk | `kad_dht/kad_dht`, `routing_table`, `peer_routing`, `provider_store`, `value_store`, `discovery/random_walk` | Bucket eviction, one-shot pings, tags, provider/value records and lookup tasks | Prior PingService disposal retained; DHT tag removal now releases empty records. Remote provider/value stores have explicit size/expiry mechanisms; locally provided keys remain intentional node state |
| Bitswap and UnixFS/DAG | `bitswap/client`, `message_queue`, `session`, `sim`, `presence`, `provider_query`, `block_store`, `dag` | Queues per connected peer, session interests, provider cache, presence TTL, file buffers | Existing queue disconnect cleanup and session-interest removal inspected. Provider cache capped at 1,000 CIDs; file buffers are request-owned. No new Bitswap leak reproduced in this pass |
| GossipSub | `pubsub/pubsub`, `gossipsub`, `score`, `mcache` | Peer queues, scores, message cache, rate state and backoff maps | Expire departed peers' backoff during heartbeat; preserve bans until their deadline |
| Metrics and service lifecycle | `events/bus`, `metrics`, `tools/anyio_service` | Listener lifetime, error tracebacks, service task ownership | DHT temporary listeners fixed previously; ownership counts exposed for live verification |
| Naming and content codecs | IPNS records, protobuf/DAG codecs, crypto/signatures | Operation-local decoded blocks and records | Used on demand; no new background retention owner reproduced |

The current deployment does not enable mDNS, delegated HTTP routing/IPNI,
WebRTC, WebTransport or AutoTLS. Optional resource-manager per-service limits
and fixed/sliding-window rate-limiter variants are not configured by the
normal startup path. Previously reproduced bugs specific to those optional
configurations are excluded from this deployment's remediation scope.

The connection diagnostic `unique_dialed` now reports distinct peers in the
bounded, recent 30-minute dial history (maximum 4,096 tracked peers), rather
than retaining exact lifetime distinct identities. The log labels it
`recent_unique_dialed`. Cumulative outcome counters remain cumulative.
Disconnect streaks expire after the existing 15-minute offense-decay period;
live quarantine deadlines are preserved. Tracking tables are pruned at most
every 30 seconds, including when no dialing is needed.

Native memory is a separate layer. The production image now uses the
jemalloc-supported `MALLOC_CONF` variable, with four arenas, background
purging and one-second dirty/muzzy decay. The prior `JEMALLOC_CONF` was ignored.
The debug memory endpoint exposes native allocated/active/resident/retained
bytes and effective options, plus live ownership counts. It no longer treats
glibc malloc_trim as a jemalloc purge. The endpoint still performs Python GC
and an object scan, so regular RSS measurements are taken before scans.

Validation and comparison use the same API, node identity, persistent volume,
ports and connection watermarks. Compare warmed-up intervals and report peer
counts, CPU and file descriptors alongside RSS; a drop caused merely by
restarting a process is not evidence of a reduced growth rate. Since both
library retention fixes and allocator settings change in this deployment,
the observed effect belongs to the combined change, not an isolated causal
estimate for any one fix.

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
| TCP and WebSocket | `transport/tcp`, `transport/websocket`, `security/noise`, `stream_muxer/yamux`, `stream_muxer/mplex`, `network/stream` | Stream dictionaries, buffered data, half-close lifecycle, connection cleanup | Yamux is preferred and Mplex is the negotiated compatibility fallback. Remove Mplex channel entries on graceful completion in either close order and on local reset; intentional write half-close preserved |
| QUIC | `transport/quic`, `aioquic` | Wrapper and native stream registries, connection-ID routing, pending handshakes | Bridge passive transport termination to the outer NetStream/SwarmConn owner and release stream credits. FIN completion preserves buffered replies; late callback registration handles reset during opened notifications |
| Identity and address observations | `identity/identify`, `identity/identify_push`, `host/observed_addr_manager` | Per-peer identify tasks, connection observations, address cache | Disconnect cleanup and weak connection observations inspected; no new unbounded owner reproduced in this pass |
| Persistent peerstore | `peer/persistent/sync/peerstore`, `datastore/sqlite_sync` | In-memory peer/record caches versus on-disk discovered peers | In-memory caches capped at 1,000; preserve address TTL across cache eviction/reopen and persist deletion of cleared fields so stale records cannot reload. Record-size counters supplement cache counts; total persisted peers are not full in-memory records |
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

Residual stream ownership found during overnight monitoring

After the first rollout, matched 30-minute windows showed RSS growth of 0.243
versus 0.169 MiB/min (about 30% lower), with similar mean connection counts
(427 versus 422). The next two hours still grew at 0.132 MiB/min. This is
evidence of improvement, not evidence that retention stopped.

The two-hour and overnight heap snapshots showed QUICStream objects growing
from 2,829 to 9,583, NetStream from 3,227 to 10,025, and Mplex StreamID objects
from 2,268 to 7,027. Mplex retained send-channel entries after removing fully
closed streams from its other registry. QUIC remote reset removed transport
ownership but left outbound NetStream wrappers in SwarmConn.streams; there is
no inbound-handler finally block for outbound streams. Repeated streams on a
long-lived connection therefore accumulated even when ping caches held only
one current stream per peer. Both paths reproduce in offline regression tests. Live ownership counters then caught the same Mplex channel omission on local reset; its cleanup now also runs if reset transmission is cancelled.

The lightweight endpoint now reports registry counts by transport, including
closed network wrappers and Mplex orphan channel entries. Track these with
RSS and allocator allocated bytes; process restart alone cannot validate a fix.

Persistent peerstore follow-up

The sync store previously saved only nonempty fields and did not persist
`ttl`/`last_identified`. After an LRU eviction, expired addresses reloaded as
permanent (the PeerData default), and explicitly cleared addresses, protocols
or metadata reappeared from their old disk values. Regression tests reproduce
both memory and SQLite backends, cache eviction, and SQLite reopen. Optional
expiry metadata is now persisted, and empty fields delete their prior stored
values. Unchanged fields are not rewritten. Legacy records without timestamps
retain their prior behavior, and explicit TTL zero remains permanent. This
preserves the existing per-peer TTL model; per-address expiry is not introduced.

A read-only production scan found 35,946 protocol records with no duplicate
protocols (maximum 49 protocols per record), ruling out protocol append as an
active source: Identify clears the prior protocol list before adding it. An
address scan found 38,551 records, a maximum count of 4,160 addresses, and a
largest serialized record of 213,889 bytes (2,614 addresses in that record).
About 25 MB of address data is serialized across all peers on disk. Those
are persisted sizes, not measured resident cache sizes. Cached address counts
and largest cached records are exposed to test their relationship with RSS.

The lightweight readings also include Python allocated blocks and the total /
largest cached signed-record payload sizes. These are observational counters;
they do not enable allocation tracing or change garbage collection policy.

WebSocket transport ownership follow-up

Outbound WebSocket connections were inserted into `_connections`, but its
removal method had no callers. Closing a wrapper, observing remote EOF, or
cancelling close left both a strong reference and a consumed connection slot.
Also, cancelling the dialer while its private background handshake was running
did not cancel that background task/socket. Close callbacks now release tracking
and capacity, private reader termination closes/untracks its wrapper, and an
abandoned dial cancels its private scope. Registration failure/cancellation
closes the connected wrapper; already-closed wrappers are rejected. Regression
tests reproduce all these ownership transitions, including 100 close cycles
with a one-connection limit. Live transport registry counts are exposed to
verify the fix under the actual WS/WSS workload. The uninstrumented previous
process cannot establish this path's quantitative share of RSS growth.

Broader retention follow-up

The next library revision also cleans timed-out PubSub readiness waiters,
deduplicates repeated protocol-book additions, bounds all rate-limiter entity
strategies, removes zero scoped-resource counters on release and rollback,
bounds QUIC CID diagnostic histories/histograms, and cancels each Bitswap
fetch's private provider-discovery worker when that fetch exits. Regression
coverage includes shared waiters and concurrent live resource owners, so
cleanup does not discard another operation's state. See the library's
`docs/memory_retention_audit.md` for reproduced cases and diagnostic changes.
These fixes include optional APIs not enabled by the production node and are
not all claimed as causes of its measured RSS growth. They do not constitute
a proof that every native dependency and optional transport is leak-free.

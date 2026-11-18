"""Read-only ownership counts and native allocator diagnostics."""

import ctypes
import sys
from collections import Counter
from typing import Any


def retention_counts(peer: Any, network: Any) -> dict[str, Any]:
    """Expose counts for the long-lived stores used by this node."""
    notifees = getattr(network, "notifees", [])
    counts: dict[str, Any] = {
        "notifees": len(notifees),
        "notifees_by_type": dict(Counter(type(n).__name__ for n in notifees)),
    }
    if hasattr(sys, "getallocatedblocks"):
        counts["python_allocated_blocks"] = sys.getallocatedblocks()
    manager = getattr(network, "transport_manager", None)
    get_transports = getattr(manager, "get_transports", None)
    if callable(get_transports):
        transport_owners = {}
        for transport in get_transports():
            connections = getattr(transport, "_connections", None)
            if isinstance(connections, dict):
                closed = 0
                for connection in connections.values():
                    is_closed = getattr(connection, "is_closed", False)
                    closed += bool(is_closed() if callable(is_closed) else is_closed)
                transport_owners[type(transport).__name__] = {
                    "tracked_connections": len(connections),
                    "closed_tracked_connections": closed,
                }
        counts["transport_owners"] = transport_owners
    stream_owners: dict[str, Any] = {}
    for connections in getattr(network, "connections", {}).values():
        for connection in connections:
            muxed = connection.muxed_conn
            name = type(muxed).__name__
            entry = stream_owners.setdefault(
                name,
                {
                    "connections": 0,
                    "network_streams": 0,
                    "closed_network_streams": 0,
                    "transport_streams": 0,
                    "message_channels": 0,
                    "orphan_message_channels": 0,
                },
            )
            streams = getattr(connection, "streams", ())
            entry["connections"] += 1
            entry["network_streams"] += len(streams)
            entry["closed_network_streams"] += sum(s.is_closed for s in streams)
            registry = getattr(muxed, "_streams", getattr(muxed, "streams", {}))
            channels = getattr(muxed, "streams_msg_channels", {})
            entry["transport_streams"] += len(registry)
            entry["message_channels"] += len(channels)
            entry["orphan_message_channels"] += sum(
                sid not in registry for sid in channels
            )
    counts["stream_owners"] = stream_owners
    host = getattr(peer, "host", None)
    raw_host = getattr(host, "_host", host)
    get_peerstore = getattr(raw_host, "get_peerstore", None)
    if callable(get_peerstore):
        store = get_peerstore()
        cache = getattr(store, "peer_data_map", None)
        if isinstance(cache, dict):
            records = [record for record in cache.values() if record is not None]
            address_counts = [len(record.addrs) for record in records]
            payload_sizes = [
                len(getattr(getattr(record, "envelope", None), "raw_payload", b""))
                for record in getattr(store, "peer_record_map", {}).values()
            ]
            counts["peerstore_cache"] = {
                "peers": len(records),
                "capacity": getattr(store, "max_cached_peers", None),
                "address_entries": sum(address_counts),
                "max_addresses_per_peer": max(address_counts, default=0),
                "protocol_entries": sum(len(record.protocols) for record in records),
                "permanent_or_legacy_records": sum(
                    record.ttl == 0 for record in records
                ),
                "signed_records": len(getattr(store, "peer_record_map", {})),
                "signed_payload_bytes": sum(payload_sizes),
                "max_signed_payload_bytes": max(payload_sizes, default=0),
            }
    connector = getattr(network, "auto_connector", None)
    if connector is not None:
        counts["auto_connector"] = {
            key: len(getattr(connector, key, {}))
            for key in (
                "_dial_history",
                "_last_connect_attempt",
                "_failure_counts",
                "_recent_disconnects",
                "_quick_deaths",
                "_quick_death_at",
                "_quarantine_until",
            )
        }
        counts["auto_connector"]["tracking_capacity"] = getattr(
            connector, "_max_tracking_peers", None
        )
    tags = getattr(network, "tag_store", None)
    if tags is not None:
        records = [tags.get_tag_info(pid) for pid in tags.get_all_peers()]
        counts["tags"] = len(records)
        counts["empty_disconnected_tags"] = sum(
            1
            for info in records
            if info is not None and not info.tags and not info.conns
        )
    gossip = getattr(peer, "_gossipsub", None)
    if gossip is not None:
        counts["gossipsub_backoff_topics"] = len(gossip.back_off)
        counts["gossipsub_backoff_peers"] = sum(
            len(v) for v in gossip.back_off.values()
        )
        counts["gossipsub_histories"] = {
            "connections": len(getattr(gossip, "recent_peer_connections", ())),
            "disconnections": len(getattr(gossip, "recent_peer_disconnections", ())),
            "delivery_topics": len(getattr(gossip, "recent_message_deliveries", {})),
            "delivery_timestamps": sum(
                len(v)
                for v in getattr(gossip, "recent_message_deliveries", {}).values()
            ),
            "publish_topics": len(getattr(gossip, "time_since_last_publish", {})),
            "fanout_topics": len(getattr(gossip, "fanout", {})),
        }
        scorer = getattr(gossip, "scorer", None)
        if scorer is not None:
            subnets = scorer.subnet_tracking
            addresses = scorer.ip_by_peer
            counts["gossipsub_scoring"] = {
                "ip_peers": len(addresses),
                "subnets": len(subnets),
                "subnet_memberships": sum(len(peers) for peers in subnets.values()),
                "stale_subnet_memberships": sum(
                    scorer._get_subnet(addresses.get(pid, "")) != subnet
                    for subnet, peers in subnets.items()
                    for pid in peers
                ),
            }
    adapter = getattr(peer, "_exchange", None)
    exchange = getattr(adapter, "_exchange", adapter)
    if exchange is not None:
        counts["bitswap_queues"] = len(exchange._message_queues)
        counts["bitswap_session_interest_cids"] = len(exchange.sim._interests)
        counts["bitswap_waiting_sessions"] = len(
            {
                session
                for sessions in exchange.sim._interests.values()
                for session in sessions
            }
        )
        peer_stats = getattr(getattr(exchange, "peer_manager", None), "peers", {})
        counts["bitswap_request_state"] = {
            "wants": len(getattr(exchange, "_wantlist", {})),
            "broadcast_wants": len(getattr(exchange, "_broadcast_wants", ())),
            "peer_statistics": len(peer_stats),
            "pending_latency_samples": sum(
                len(stats._pending_requests) for stats in peer_stats.values()
            ),
        }
        query = getattr(exchange, "provider_query_manager", None)
        if query is not None:
            counts["bitswap_provider_cache"] = query.cache.stats()
    return counts


def _offset_bucket(value: Any) -> str:
    """Bucket a stream offset so the census stays a fixed-size summary."""
    if not value:
        return "0"
    if value <= 1024:
        return "<=1k"
    if value <= 65536:
        return "<=64k"
    return ">64k"


def _connection_closed(muxed: Any) -> bool:
    """Read a closed flag that may be a bool or a method."""
    attribute = getattr(muxed, "is_closed", None)
    reported = attribute() if callable(attribute) else bool(attribute)
    return bool(getattr(muxed, "_closed", False) or reported)


def quic_stream_census(network: Any) -> dict[str, Any]:
    """
    Classify the aioquic streams live QUIC connections still track.

    aioquic drops a stream from ``QuicConnection._streams`` only once both
    halves report finished, and the send half finishes only when its FIN or
    RESET is acknowledged. A stream that py-libp2p abandons without wrapping
    (ignored late events, refused inbound streams) therefore stays in that
    dict forever. Splitting the population by finished state and by whether a
    py-libp2p wrapper exists separates live traffic from abandoned streams.

    aioquic also keeps every stream that exceeded the peer's stream-credit
    allowance in ``_streams_blocked_bidi``/``_streams_blocked_uni`` and only
    edits those lists when more credit arrives, so finished streams can be
    retained there after leaving ``_streams``. Count them separately: they are
    invisible to a census that only walks the main registry.
    """
    totals = {
        "connections": 0,
        "streams": 0,
        "wrappers": 0,
        "orphan_streams": 0,
        "orphan_unfinished": 0,
        "finished_not_discarded": 0,
        "blocked_bidi": 0,
        "blocked_uni": 0,
        "blocked_finished": 0,
        "blocked_absent": 0,
    }
    states: Counter[str] = Counter()
    orphan_detail: Counter[str] = Counter()
    worst: list[tuple[int, str, int]] = []
    connection_detail: list[dict[str, Any]] = []
    for connections in getattr(network, "connections", {}).values():
        for connection in connections:
            muxed = getattr(connection, "muxed_conn", None)
            quic = getattr(muxed, "_quic", None)
            streams = getattr(quic, "_streams", None)
            if not isinstance(streams, dict):
                continue
            is_client = bool(
                getattr(getattr(quic, "configuration", None), "is_client", False)
            )
            wrappers = getattr(muxed, "_streams", {})
            wrapper_ids = set(wrappers) if isinstance(wrappers, dict) else set()
            totals["connections"] += 1
            totals["streams"] += len(streams)
            totals["wrappers"] += len(wrapper_ids)
            for label, blocked in (
                ("blocked_bidi", getattr(quic, "_streams_blocked_bidi", None)),
                ("blocked_uni", getattr(quic, "_streams_blocked_uni", None)),
            ):
                if not isinstance(blocked, list):
                    continue
                totals[label] += len(blocked)
                for blocked_stream in blocked:
                    if getattr(blocked_stream, "is_finished", False):
                        totals["blocked_finished"] += 1
                    elif getattr(blocked_stream, "stream_id", None) not in streams:
                        totals["blocked_absent"] += 1
            orphans = 0
            for stream_id, stream in list(streams.items()):
                sender = getattr(stream, "sender", None)
                receiver = getattr(stream, "receiver", None)
                send_done = bool(getattr(sender, "is_finished", False))
                recv_done = bool(getattr(receiver, "is_finished", False))
                peer_initiated = bool(stream_id & 1) == is_client
                orphan = stream_id not in wrapper_ids
                states[
                    "sf=%d rf=%d peer_initiated=%d orphan=%d"
                    % (send_done, recv_done, peer_initiated, orphan)
                ] += 1
                if orphan:
                    orphans += 1
                    totals["orphan_streams"] += 1
                    if not (send_done and recv_done):
                        totals["orphan_unfinished"] += 1
                    orphan_detail[
                        "sent=%s recv=%s reset=%d stop=%d acked_fin=%d "
                        "empty=%d final=%d id_mod4=%d"
                        % (
                            _offset_bucket(getattr(sender, "highest_offset", 0)),
                            _offset_bucket(getattr(receiver, "highest_offset", 0)),
                            bool(getattr(sender, "reset_pending", False)),
                            bool(getattr(receiver, "stop_pending", False)),
                            bool(getattr(sender, "_acked_fin", False)),
                            bool(getattr(sender, "buffer_is_empty", False)),
                            getattr(receiver, "_final_size", None) is not None,
                            stream_id % 4,
                        )
                    ] += 1
                elif send_done and recv_done:
                    # Finished on both halves but still not discarded: aioquic
                    # only pops them while writing a packet, so this is expected
                    # to be transient, never a large standing population.
                    totals["finished_not_discarded"] += 1
            if orphans:
                peer_id = getattr(connection, "peer_id", None)
                worst.append((orphans, str(peer_id), len(streams)))
                stats = getattr(muxed, "_stats", {}) or {}
                connection_detail.append(
                    {
                        "orphans": orphans,
                        "streams": len(streams),
                        "wrappers": len(wrapper_ids),
                        "is_client": is_client,
                        "closed": _connection_closed(muxed),
                        "handshake_completed": bool(
                            getattr(muxed, "_handshake_completed", False)
                        ),
                        "next_stream_id": getattr(muxed, "_next_stream_id", None),
                        "recently_closed": len(
                            getattr(muxed, "_recently_closed_streams", ())
                        ),
                        "bytes_sent": stats.get("bytes_sent"),
                        "bytes_received": stats.get("bytes_received"),
                        "streams_opened": stats.get("streams_opened"),
                        "streams_reset": stats.get("streams_reset"),
                    }
                )
    worst.sort(reverse=True)
    connection_detail.sort(key=lambda item: -item["orphans"])
    return {
        "totals": totals,
        "states": dict(states.most_common()),
        "orphan_detail": dict(orphan_detail.most_common(10)),
        "connections_with_orphans": connection_detail[:8],
        "worst_connections": [
            {"orphans": orphans, "peer_id": peer_id, "streams": streams}
            for orphans, peer_id, streams in worst[:5]
        ],
    }


def allocator_stats() -> dict[str, Any]:
    """Read jemalloc statistics if it is already loaded into this process."""
    library = ctypes.CDLL(None)
    control = getattr(library, "mallctl", None)
    if control is None:
        return {"backend": "system", "jemalloc_available": False}
    control.argtypes = [
        ctypes.c_char_p,
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_size_t),
        ctypes.c_void_p,
        ctypes.c_size_t,
    ]
    control.restype = ctypes.c_int
    epoch = ctypes.c_size_t(1)
    control(b"epoch", None, None, ctypes.byref(epoch), ctypes.sizeof(epoch))
    result: dict[str, Any] = {"backend": "jemalloc", "jemalloc_available": True}
    for name, kind in (
        ("stats.allocated", ctypes.c_size_t),
        ("stats.active", ctypes.c_size_t),
        ("stats.resident", ctypes.c_size_t),
        ("stats.retained", ctypes.c_size_t),
        ("background_thread", ctypes.c_bool),
        ("opt.narenas", ctypes.c_uint),
        ("opt.dirty_decay_ms", ctypes.c_ssize_t),
        ("opt.muzzy_decay_ms", ctypes.c_ssize_t),
    ):
        value = kind()
        size = ctypes.c_size_t(ctypes.sizeof(value))
        if (
            control(name.encode(), ctypes.byref(value), ctypes.byref(size), None, 0)
            == 0
        ):
            result[name] = value.value
    return result

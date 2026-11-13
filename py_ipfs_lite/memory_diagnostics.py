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
        query = getattr(exchange, "provider_query_manager", None)
        if query is not None:
            counts["bitswap_provider_cache"] = query.cache.stats()
    return counts


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

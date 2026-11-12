"""Read-only ownership counts and native allocator diagnostics."""

import ctypes
from collections import Counter
from typing import Any


def retention_counts(peer: Any, network: Any) -> dict[str, Any]:
    """Expose counts for the long-lived stores used by this node."""
    notifees = getattr(network, "notifees", [])
    counts: dict[str, Any] = {
        "notifees": len(notifees),
        "notifees_by_type": dict(Counter(type(n).__name__ for n in notifees)),
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
    exchange = getattr(getattr(peer, "exchange", None), "_exchange", None)
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

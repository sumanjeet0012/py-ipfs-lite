"""
Systematic memory attribution for a running py-ipfs-lite node.

The goal is to answer "which subsystem is holding the RAM?" with evidence
instead of RSS deltas.  Four independent views are produced:

1. ``rss`` / allocator: how much memory the process mapped, how much jemalloc
   says is live/active/retained, and how much Python itself accounts for.
2. ``tracemalloc``: allocations grouped by the module that performed them
   (peerstore, transport, bitswap, ...).  This is the only view that sees
   ``str``/``bytes``/``int`` payloads, which are not tracked by ``gc``.
3. ``objects``: a census of gc-tracked live objects grouped by type/module,
   with object counts and shallow sizes.
4. ``subsystems``: deep size of explicit ownership roots (peerstore cache,
   swarm connection table, bitswap, pubsub, DHT), traversed with one shared
   visited set so a container shared by two roots is only counted once.

Tracing must be enabled at process start:

    IPFS_LITE_MEMORY_TRACING=1 py-ipfs-lite daemon ...

It is off by default because ``tracemalloc`` adds overhead to every
allocation.
"""

import gc
import os
import sys
import time
import tracemalloc
import types
from collections import Counter
from collections.abc import Iterable
from typing import Any

TRACING_ENV = "IPFS_LITE_MEMORY_TRACING"

# Files that allocate on behalf of a subsystem.  The first match wins, so
# more specific paths are listed first.
_MODULE_GROUPS: tuple[tuple[str, str], ...] = (
    ("libp2p/peer/persistent/datastore", "peerstore.sqlite"),
    ("libp2p/peer/persistent", "peerstore"),
    ("libp2p/peer/peerdata", "peerstore"),
    ("libp2p/peer/peerstore", "peerstore"),
    ("peer/persistent", "peerstore"),
    ("libp2p/bitswap", "bitswap"),
    ("libp2p/pubsub", "pubsub"),
    ("libp2p/kad_dht", "dht"),
    ("libp2p/discovery", "dht.discovery"),
    ("libp2p/relay", "relay"),
    ("libp2p/transport/quic", "transport.quic"),
    ("libp2p/transport/websocket", "transport.websocket"),
    ("libp2p/transport/tcp", "transport.tcp"),
    ("libp2p/transport", "transport"),
    ("libp2p/stream_muxer", "stream_muxer"),
    ("libp2p/security", "security"),
    ("libp2p/network", "network.swarm"),
    ("libp2p/host", "host"),
    ("libp2p/rcmgr", "rcmgr"),
    ("libp2p", "libp2p.other"),
    ("py_ipfs_lite/api", "api"),
    ("py_ipfs_lite/peer", "py_ipfs_lite.peer"),
    ("py_ipfs_lite", "py_ipfs_lite.other"),
    ("/trio/", "trio"),
    ("multidict", "python.stdlib"),
    ("multiformats", "multiformats"),
    ("multiaddr", "multiaddr"),
    ("aioquic", "aioquic"),
    ("cryptography", "cryptography"),
    ("_sqlite3", "sqlite3"),
    ("/sqlite3/", "sqlite3"),
    ("/json/", "python.stdlib"),
    ("/logging/", "python.stdlib"),
)

# GC object types are grouped the same way using their defining module.
_TYPE_GROUP_PREFIXES: tuple[tuple[str, str], ...] = (
    ("libp2p.peer.persistent", "peerstore"),
    ("libp2p.peer.peerdata", "peerstore"),
    ("libp2p.bitswap", "bitswap"),
    ("libp2p.pubsub", "pubsub"),
    ("libp2p.kad_dht", "dht"),
    ("libp2p.discovery", "dht.discovery"),
    ("libp2p.transport.quic", "transport.quic"),
    ("libp2p.transport.websocket", "transport.websocket"),
    ("libp2p.transport.tcp", "transport.tcp"),
    ("libp2p.transport", "transport"),
    ("libp2p.stream_muxer", "stream_muxer"),
    ("libp2p.network", "network.swarm"),
    ("libp2p.host", "host"),
    ("libp2p.security", "security"),
    ("trio", "trio"),
    ("py_ipfs_lite.peer", "py_ipfs_lite.peer"),
    ("py_ipfs_lite.api", "api"),
)

_SKIP_REFERENT_TYPES = (
    type,
    types.ModuleType,
    types.FunctionType,
    types.BuiltinFunctionType,
    types.MethodDescriptorType,
    types.WrapperDescriptorType,
    types.MethodType,
)


# ---------------------------------------------------------------------------
# tracing control
# ---------------------------------------------------------------------------


def tracing_enabled() -> bool:
    """Whether the current process is collecting allocation tracebacks."""
    return tracemalloc.is_tracing()


def maybe_start_tracing() -> bool:
    """
    Start tracemalloc when IPFS_LITE_MEMORY_TRACING is set.

    ``25`` frames is enough to find the allocating call site without paying
    for a full stack on every allocation.
    """
    if tracing_enabled():
        return True
    value = os.environ.get(TRACING_ENV, "").strip().lower()
    if value in ("", "0", "false", "no", "off"):
        return False
    frames = int(os.environ.get("IPFS_LITE_MEMORY_TRACING_FRAMES", "25"))
    tracemalloc.start(frames)
    return True


# ---------------------------------------------------------------------------
# RSS / allocator decomposition
# ---------------------------------------------------------------------------


def _proc_status() -> dict[str, int]:
    """Selected /proc/self/status fields in bytes."""
    wanted = {
        "VmRSS": "rss_bytes",
        "VmHWM": "peak_rss_bytes",
        "VmSize": "virtual_bytes",
        "RssAnon": "rss_anon_bytes",
        "RssFile": "rss_file_bytes",
        "VmData": "data_bytes",
        "VmSwap": "swap_bytes",
    }
    out: dict[str, int] = {}
    try:
        with open("/proc/self/status") as handle:
            for line in handle:
                key, _, rest = line.partition(":")
                if key in wanted:
                    value = rest.strip().split()
                    if value:
                        out[wanted[key]] = int(value[0]) * 1024
    except OSError:
        pass
    if not out:
        # Non-Linux development hosts have no /proc; psutil reports the same
        # headline number on every platform.
        try:
            import psutil

            info = psutil.Process(os.getpid()).memory_info()
            out["rss_bytes"] = info.rss
        except Exception:
            pass
    return out


def _allocator() -> dict[str, Any]:
    from py_ipfs_lite.memory_diagnostics import allocator_stats

    return allocator_stats()


def rss_breakdown() -> dict[str, Any]:
    """Process totals plus the Python/allocator share of them."""
    rss = _proc_status()
    breakdown: dict[str, Any] = {
        **rss,
        "pid": os.getpid(),
    }
    if "rss_anon_bytes" in rss and "rss_file_bytes" in rss:
        breakdown["rss_accounted_bytes"] = rss["rss_anon_bytes"] + rss["rss_file_bytes"]
    if hasattr(sys, "getallocatedblocks"):
        breakdown["python_allocated_blocks"] = sys.getallocatedblocks()
    if tracing_enabled():
        current, peak = tracemalloc.get_traced_memory()
        breakdown["tracemalloc_current_bytes"] = current
        breakdown["tracemalloc_peak_bytes"] = peak
        breakdown["tracemalloc_traceback_frames"] = tracemalloc.get_traceback_limit()
    else:
        breakdown["tracemalloc_current_bytes"] = None
        breakdown["tracemalloc_peak_bytes"] = None
    allocator = _allocator()
    breakdown["allocator"] = allocator
    if allocator.get("stats.resident"):
        # jemalloc's resident measure is the page footprint it owns, which is
        # the closest native analogue of RSS.
        breakdown["jemalloc_resident_bytes"] = allocator["stats.resident"]
    if allocator.get("stats.allocated"):
        breakdown["jemalloc_allocated_bytes"] = allocator["stats.allocated"]
    if allocator.get("stats.retained"):
        breakdown["jemalloc_retained_bytes"] = allocator["stats.retained"]
    return breakdown


# ---------------------------------------------------------------------------
# tracemalloc attribution
# ---------------------------------------------------------------------------


def _group_for_path(path: str) -> str:
    normalized = path.replace("\\", "/")
    for needle, group in _MODULE_GROUPS:
        if needle in normalized:
            return group
    if "site-packages/" in normalized:
        package = normalized.split("site-packages/", 1)[1].split("/", 1)[0]
    elif "dist-packages/" in normalized:
        package = normalized.split("dist-packages/", 1)[1].split("/", 1)[0]
    else:
        package = ""
    if package:
        if package.startswith(("libp2p", "py_ipfs_lite")):
            return "other:" + package
        return "python.package:" + package
    if "/lib/python" in normalized or normalized.startswith("<frozen"):
        return "python.stdlib"
    return "other"


def _short_path(path: str) -> str:
    marker = "site-packages/"
    if marker in path:
        return path.split(marker, 1)[1]
    marker = "dist-packages/"
    if marker in path:
        return path.split(marker, 1)[1]
    return path


def tracemalloc_report(top: int = 25, detailed: bool = False) -> dict[str, Any]:
    """
    Allocation attribution by module, file and call site.

    ``detailed=False`` groups by file only, which is much cheaper than
    walking a full traceback for every live block and is enough to attribute
    memory to a subsystem.  ``detailed=True`` also returns call sites.
    """
    if not tracing_enabled():
        return {
            "enabled": False,
            "hint": f"restart with {TRACING_ENV}=1 to collect allocation sites",
        }

    current, peak = tracemalloc.get_traced_memory()
    snapshot = tracemalloc.take_snapshot()
    stats = snapshot.statistics("traceback" if detailed else "filename")

    by_group: Counter[str] = Counter()
    by_group_count: Counter[str] = Counter()
    by_file: Counter[str] = Counter()
    sites: list[dict[str, Any]] = []

    for stat in stats:
        group = "unknown"
        file_name = "unknown"
        line = 0
        # traceback[-1] is the frame closest to the allocation.
        if stat.traceback:
            last = stat.traceback[-1]
            file_name = _short_path(last.filename)
            line = last.lineno
            group = _group_for_path(last.filename)
        by_group[group] += stat.size
        by_group_count[group] += stat.count
        by_file[f"{file_name}:{line}"] += stat.size
        if detailed and len(sites) < top:
            sites.append(
                {
                    "group": group,
                    "file": file_name,
                    "line": line,
                    "size_bytes": stat.size,
                    "count": stat.count,
                }
            )

    total = sum(by_group.values())
    groups = [
        {
            "group": group,
            "size_bytes": size,
            "count": by_group_count[group],
            "share": round(size / total, 4) if total else 0.0,
        }
        for group, size in by_group.most_common(top)
    ]
    files = [
        {"file": name, "size_bytes": size} for name, size in by_file.most_common(top)
    ]
    return {
        "enabled": True,
        "detailed": detailed,
        "current_bytes": current,
        "peak_bytes": peak,
        "total_traced_bytes": total,
        "groups": groups,
        "files": files,
        "sites": sites,
    }


# ---------------------------------------------------------------------------
# GC object census
# ---------------------------------------------------------------------------


def _type_group(type_name: str) -> str:
    for prefix, group in _TYPE_GROUP_PREFIXES:
        if type_name.startswith(prefix):
            return group
    module = type_name.rsplit(".", 1)[0]
    if module == "builtins":
        return "builtins"
    if module.startswith("libp2p"):
        return "libp2p.other"
    if module.startswith("py_ipfs_lite"):
        return "py_ipfs_lite.other"
    top = module.split(".", 1)[0]
    if top in ("gc", "sys", "tracemalloc", "threading", "typing", "abc", "enum"):
        return "python.stdlib"
    return f"python.package:{top}"


def object_census(top: int = 30) -> dict[str, Any]:
    """Counts and shallow sizes of gc-tracked live objects."""
    gc.collect()
    counts: Counter[str] = Counter()
    shallow: Counter[str] = Counter()
    group_counts: Counter[str] = Counter()
    group_shallow: Counter[str] = Counter()
    for obj in gc.get_objects():
        type_name = f"{type(obj).__module__}.{type(obj).__name__}"
        counts[type_name] += 1
        try:
            size = sys.getsizeof(obj)
        except Exception:
            size = 0
        shallow[type_name] += size
        group = _type_group(type_name)
        group_counts[group] += 1
        group_shallow[group] += size

    top_types = [
        {
            "type": name,
            "count": count,
            "shallow_bytes": shallow[name],
        }
        for name, count in counts.most_common(top)
    ]
    largest_types = [
        {"type": name, "count": counts[name], "shallow_bytes": size}
        for name, size in shallow.most_common(top)
    ]
    groups = [
        {
            "group": group,
            "count": count,
            "shallow_bytes": group_shallow[group],
        }
        for group, count in group_counts.most_common()
    ]
    return {
        "total_objects": len(gc.get_objects()),
        "groups": groups,
        "by_count": top_types,
        "by_shallow_size": largest_types,
    }


# ---------------------------------------------------------------------------
# Deep size of explicit ownership roots
# ---------------------------------------------------------------------------


def deep_size(
    root: Any,
    visited: set[int] | None = None,
    max_objects: int = 150_000,
    deadline: float = 2.0,
) -> int:
    """
    Transitive size of ``root`` in bytes, without double counting.

    Shared ``visited`` sets let several roots be measured in a defined order:
    the first root that reaches an object is charged for it.  Traversal stops
    after ``max_objects`` or ``deadline`` seconds, so the endpoint stays
    bounded even on a very large heap.
    """
    seen = visited if visited is not None else set()
    stack = [root]
    total = 0
    examined = 0
    started = time.monotonic()
    while stack and examined < max_objects:
        if (examined & 0x3FFF) == 0 and time.monotonic() - started > deadline:
            break
        obj = stack.pop()
        obj_id = id(obj)
        if obj_id in seen:
            continue
        seen.add(obj_id)
        examined += 1
        try:
            total += sys.getsizeof(obj)
        except Exception:
            continue
        try:
            referents = gc.get_referents(obj)
        except Exception:
            continue
        for ref in referents:
            if isinstance(ref, _SKIP_REFERENT_TYPES):
                continue
            stack.append(ref)
    return total


def subsystem_sizes(peer: Any) -> dict[str, Any]:
    """Deep size of each subsystem root, charged in a fixed order."""
    roots: list[tuple[str, Any]] = []
    raw_host = getattr(peer, "host", None)
    raw_host = getattr(raw_host, "_host", raw_host)

    try:
        roots.append(("peerstore", raw_host.get_peerstore()))
    except Exception:
        pass
    try:
        network = raw_host.get_network()
        roots.append(("swarm", network))
    except Exception:
        pass
    try:
        roots.append(("bitswap", peer._exchange))
    except Exception:
        pass
    try:
        roots.append(("pubsub", peer._gossipsub or peer._pubsub))
    except Exception:
        pass
    try:
        roots.append(("dht", peer.routing))
    except Exception:
        pass
    try:
        roots.append(("blockstore", peer.blockstore))
    except Exception:
        pass
    try:
        roots.append(("dag_service", peer.dag_service))
    except Exception:
        pass

    seen: set[int] = set()
    sizes: dict[str, Any] = {}
    total = 0
    for name, root in roots:
        if root is None:
            sizes[name] = {"deep_bytes": 0, "present": False}
            continue
        before = len(seen)
        size = deep_size(root, visited=seen)
        sizes[name] = {
            "deep_bytes": size,
            "present": True,
            "objects_charged": len(seen) - before,
        }
        total += size
    return {"roots": sizes, "charged_bytes": total}


# ---------------------------------------------------------------------------
# baseline diff
# ---------------------------------------------------------------------------

_BASELINE: Any = None
_BASELINE_AT: str | None = None


def set_baseline() -> bool:
    """Store the current allocation snapshot as the comparison baseline."""
    global _BASELINE, _BASELINE_AT
    if not tracing_enabled():
        return False
    _BASELINE = tracemalloc.take_snapshot()
    _BASELINE_AT = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    return True


def diff_report(top: int = 25, detailed: bool = False) -> dict[str, Any]:
    """Growth per module and per call site since the stored baseline."""
    if not tracing_enabled() or _BASELINE is None:
        return {"available": False, "hint": "call with ?baseline=set first"}

    current = tracemalloc.take_snapshot()
    diffs = current.compare_to(_BASELINE, "traceback" if detailed else "filename")
    by_group: Counter[str] = Counter()
    by_group_count: Counter[str] = Counter()
    sites: list[dict[str, Any]] = []
    for stat in diffs:
        group = "unknown"
        file_name = "unknown"
        line = 0
        if stat.traceback:
            last = stat.traceback[-1]
            file_name = _short_path(last.filename)
            line = last.lineno
            group = _group_for_path(last.filename)
        by_group[group] += stat.size_diff
        by_group_count[group] += stat.count_diff
        if stat.size_diff > 0 and len(sites) < top:
            sites.append(
                {
                    "group": group,
                    "file": file_name,
                    "line": line,
                    "size_diff_bytes": stat.size_diff,
                    "size_bytes": stat.size,
                    "count_diff": stat.count_diff,
                }
            )
    groups = [
        {
            "group": group,
            "size_diff_bytes": size,
            "count_diff": by_group_count[group],
        }
        for group, size in by_group.most_common()
    ]
    return {
        "available": True,
        "baseline_at": _BASELINE_AT,
        "groups": groups,
        "sites": sites,
    }


# ---------------------------------------------------------------------------
# top-level snapshot
# ---------------------------------------------------------------------------


def profile(
    peer: Any | None = None,
    *,
    objects: bool = False,
    subsystems: bool = False,
    tracemalloc_top: int = 25,
    objects_top: int = 30,
    diff: bool = False,
    detailed: bool = False,
) -> dict[str, Any]:
    """One consistent memory snapshot for the HTTP endpoint."""
    result: dict[str, Any] = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "rss": rss_breakdown(),
        "tracemalloc": tracemalloc_report(tracemalloc_top, detailed=detailed),
    }
    if diff:
        result["growth"] = diff_report(tracemalloc_top, detailed=detailed)
    if objects:
        result["objects"] = object_census(objects_top)
    if subsystems and peer is not None:
        result["subsystems"] = subsystem_sizes(peer)
    return result


def _human(value: int | None) -> str:
    if not value:
        return "0 B"
    units: Iterable[str] = ("B", "KiB", "MiB", "GiB")
    size = float(value)
    for unit in units:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TiB"


def format_report(report: dict[str, Any], top: int = 15) -> str:
    """Human-readable summary used by the CLI helper."""
    lines: list[str] = []
    rss = report.get("rss", {})
    lines.append(
        f"RSS {_human(rss.get('rss_bytes'))} "
        f"(anon {_human(rss.get('rss_anon_bytes'))}, "
        f"file {_human(rss.get('rss_file_bytes'))})"
    )
    if rss.get("tracemalloc_current_bytes") is not None:
        lines.append(
            f"Python traced {_human(rss['tracemalloc_current_bytes'])} "
            f"(peak {_human(rss.get('tracemalloc_peak_bytes'))})"
        )
    allocator = rss.get("allocator", {})
    if allocator.get("backend") == "jemalloc":
        lines.append(
            f"jemalloc allocated {_human(allocator.get('stats.allocated'))}, "
            f"resident {_human(allocator.get('stats.resident'))}, "
            f"retained {_human(allocator.get('stats.retained'))}"
        )
    trace = report.get("tracemalloc", {})
    if trace.get("enabled"):
        lines.append("")
        lines.append("Top allocating subsystems:")
        for group in trace.get("groups", [])[:top]:
            lines.append(
                f"  {group['group']:<22} {_human(group['size_bytes']):>10} "
                f"{group['share'] * 100:5.1f}%  ({group['count']} blocks)"
            )
        lines.append("")
        lines.append("Top allocation sites:")
        for site in trace.get("sites", [])[:top]:
            lines.append(
                f"  {_human(site['size_bytes']):>10}  {site['file']}:{site['line']}"
            )
    objects = report.get("objects")
    if objects:
        lines.append("")
        lines.append("GC-tracked objects by subsystem:")
        for group in objects.get("groups", [])[:top]:
            lines.append(
                f"  {group['group']:<22} {group['count']:>8} objects "
                f"{_human(group['shallow_bytes']):>10} shallow"
            )
    subs = report.get("subsystems")
    if subs:
        lines.append("")
        lines.append("Deep size by ownership root (charged in order):")
        for name, entry in subs.get("roots", {}).items():
            if not entry.get("present"):
                continue
            lines.append(
                f"  {name:<22} {_human(entry['deep_bytes']):>10} "
                f"({entry['objects_charged']} objects)"
            )
    return "\n".join(lines)

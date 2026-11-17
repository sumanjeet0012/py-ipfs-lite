"""
Identify leaks by object accounting instead of by watching RSS.

RSS is a structurally poor leak detector.  The allocator caches freed pages,
``_memory_hygiene_loop`` and ``_stream_leak_monitor_loop`` call
``gc.collect()`` plus ``malloc_trim(0)`` every 60s, CPython arenas are mmap'd
outside jemalloc, and a leak of a few hundred objects per hour hides under
several MiB of sampling noise.  Waiting for it to clear that noise is why leak
investigations cost hours.

A leak is not "RSS went up".  A leak is one of three exactly measurable things:

1. **Live objects grow per unit of work.**  Every connection already reports a
   monotonic ``streams_opened`` counter and every registry is O(1) to size, so
   ``live / opened`` is free to compute.  In a bounded system that ratio decays
   towards zero; in a leaking one it converges to a positive constant, because a
   fixed fraction of every cohort is born and never reaped.  This needs minutes,
   not hours, because ``opened`` accumulates at the traffic rate.

2. **Old births survive.**  This is the definitive test and it needs no
   statistics.  Every live stream is stamped the first time it is observed.  A
   stream with no protocol attached that is still alive later than any
   legitimate retention window cannot be anything but leaked.  Stamp ages are
   lower bounds (a stream that predates the sampler is stamped at start), so the
   test can under-report age but can never produce a false positive.

3. **The bytes have a provenance.**  CPython's live object count
   (``sys.getallocatedblocks``), jemalloc's live/active/resident bytes and the
   process's anonymous RSS are sampled separately and never mixed, so growth is
   attributed to a heap instead of to "the process".  The anonymous gap that
   neither heap explains is reported explicitly; it is where mmap'd buffers and
   pymalloc arenas hide.

Everything here is O(number of live streams) and allocation-free in steady
state: no tracemalloc, no ``gc.get_objects()``, no heap walk.  Sampling this
does not perturb what it measures.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from collections import Counter, deque
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger("py_ipfs_lite.leak_watch")

#: Sampling cadence.  Cheap enough to run continuously in production.
DEFAULT_INTERVAL_S = float(os.environ.get("IPFS_LITE_LEAK_SAMPLE_INTERVAL_S", "10"))
#: 2160 samples at 10s covers six hours of history in a few MiB.
DEFAULT_CAPACITY = int(os.environ.get("IPFS_LITE_LEAK_CAPACITY", "2160"))
#: An unwrapped stream older than this is a leak, not a slow protocol.
DEFAULT_ORPHAN_AGE_S = float(os.environ.get("IPFS_LITE_LEAK_ORPHAN_AGE_S", "300"))
#: Minimum window before a trend verdict is issued.
MIN_VERDICT_SPAN_S = 90.0
#: Minimum cumulative streams opened before a ratio verdict is meaningful.
MIN_VERDICT_WORK = 500
#: Samples needed inside a stable peer population before judging.
MIN_STABLE_SAMPLES = 6


@dataclass(frozen=True)
class Sample:
    """One observation of live objects, work done and heap provenance."""

    t: float
    wall: float
    connections: int
    live: dict[str, int]
    work: dict[str, float]
    mem: dict[str, float]


def _family(muxed: Any) -> str | None:
    name = type(muxed).__name__
    if name == "QUICConnection":
        return "quic"
    if name == "Yamux":
        return "yamux"
    if name == "Mplex":
        return "mplex"
    return None


def _status_kb(field_name: str) -> int | None:
    """Read one ``/proc/self/status`` field, e.g. ``VmRSS``, in KiB."""
    try:
        with open("/proc/self/status", encoding="ascii") as handle:
            for line in handle:
                if line.startswith(field_name + ":"):
                    return int(line.split()[1])
    except Exception:
        return None
    return None


def memory_provenance() -> dict[str, float]:
    """
    Split current memory into heaps that can be trended independently.

    ``python_blocks`` counts live PyObjects, which is exact and noise-free.
    ``jemalloc_allocated`` counts live bytes handed out by malloc.  ``rss_anon``
    is everything anonymous, including pymalloc arenas that jemalloc never sees.
    ``anon_unexplained`` is the remainder: if *that* grows, the leak is in an
    mmap'd native buffer that neither heap reports.
    """
    rss_kb = 0
    rss_anon_kb = 0
    rss_file_kb = 0
    try:
        with open("/proc/self/smaps_rollup", encoding="ascii") as handle:
            for line in handle:
                if line.startswith("Rss:"):
                    rss_kb = int(line.split()[1])
                elif line.startswith("Anonymous:"):
                    rss_anon_kb = int(line.split()[1])
        rss_file_kb = max(0, rss_kb - rss_anon_kb)
    except Exception:
        rss_kb = _status_kb("VmRSS") or 0
        rss_anon_kb = _status_kb("RssAnon") or rss_kb
        rss_file_kb = _status_kb("RssFile") or 0
    provenance: dict[str, float] = {
        "rss": float(rss_kb) * 1024.0,
        "rss_anon": float(rss_anon_kb) * 1024.0,
        "rss_file": float(rss_file_kb) * 1024.0,
    }
    try:
        provenance["python_blocks"] = float(sys.getallocatedblocks())
    except Exception:
        pass
    try:
        from py_ipfs_lite.memory_diagnostics import allocator_stats

        stats = allocator_stats()
        for key in (
            "stats.allocated",
            "stats.active",
            "stats.resident",
            "stats.retained",
        ):
            value = stats.get(key)
            if isinstance(value, int):
                provenance[key.replace(".", "_")] = float(value)
        resident = stats.get("stats.resident")
        if isinstance(resident, int) and rss_anon_kb:
            provenance["anon_unexplained"] = max(
                0.0, provenance["rss_anon"] - float(resident)
            )
    except Exception:
        pass
    return provenance


def _ols(points: list[tuple[float, float]]) -> tuple[float, float, float]:
    """Least squares slope, its standard error and r² for (x, y) points."""
    n = len(points)
    if n < 3:
        return 0.0, float("inf"), 0.0
    mean_x = sum(x for x, _ in points) / n
    mean_y = sum(y for _, y in points) / n
    sxx = sum((x - mean_x) ** 2 for x, _ in points)
    if sxx == 0:
        return 0.0, float("inf"), 0.0
    sxy = sum((x - mean_x) * (y - mean_y) for x, y in points)
    slope = sxy / sxx
    intercept = mean_y - slope * mean_x
    residuals = [y - (intercept + slope * x) for x, y in points]
    sse = sum(r * r for r in residuals)
    syy = sum((y - mean_y) ** 2 for _, y in points)
    r2 = 0.0 if syy == 0 else 1.0 - sse / syy
    if n > 2:
        variance = sse / (n - 2)
        stderr = (variance / sxx) ** 0.5
    else:
        stderr = float("inf")
    return slope, stderr, r2


class LeakWatch:
    """Sample live objects against cumulative work and issue a verdict."""

    def __init__(
        self,
        peer: Any,
        interval: float = DEFAULT_INTERVAL_S,
        capacity: int = DEFAULT_CAPACITY,
    ) -> None:
        self._peer = peer
        self.interval = max(1.0, interval)
        self._samples: deque[Sample] = deque(maxlen=capacity)
        self._last_stats: dict[int, dict[str, float]] = {}
        self._work: Counter[str] = Counter()
        self._first_seen: dict[int, dict[int, float]] = {}
        self._started = time.monotonic()
        self._last_logged_signature: tuple[Any, ...] | None = None
        self._last_logged_at = 0.0

    # ---------------------------------------------------------------- collect

    def _network(self) -> Any:
        host = getattr(self._peer, "host", None)
        raw_host = getattr(host, "_host", host)
        get_network = getattr(raw_host, "get_network", None)
        return get_network() if callable(get_network) else None

    def _collect(
        self, now: float
    ) -> tuple[Counter[str], set[int], dict[int, dict[str, float]], int]:
        network = self._network()
        live: Counter[str] = Counter()
        quic_connections: set[int] = set()
        stats: dict[int, dict[str, float]] = {}
        connections = 0
        if network is None:
            return live, quic_connections, stats, connections
        for peer_connections in getattr(network, "connections", {}).values():
            for connection in peer_connections:
                connections += 1
                muxed = getattr(connection, "muxed_conn", None)
                family = _family(muxed)
                if family is None:
                    continue
                live[f"{family}_connections"] += 1
                # QUIC keeps wrappers in ``_streams``; yamux/mplex expose
                # ``streams``.  Accept either so the count cannot silently be 0.
                wrappers = getattr(muxed, "_streams", None)
                if not isinstance(wrappers, dict):
                    wrappers = getattr(muxed, "streams", None)
                wrapper_ids = (
                    frozenset(wrappers) if isinstance(wrappers, dict) else frozenset()
                )
                channels = getattr(muxed, "streams_msg_channels", None)
                if isinstance(channels, dict) and channels:
                    live[f"{family}_orphan_channels"] += sum(
                        1 for stream_id in channels if stream_id not in wrapper_ids
                    )
                if family == "quic":
                    if isinstance(wrappers, dict):
                        live["quic_wrappers"] += len(wrappers)
                    quic = getattr(muxed, "_quic", None)
                    transport = getattr(quic, "_streams", None)
                    if isinstance(transport, dict):
                        live["quic_streams"] += len(transport)
                        orphans = [sid for sid in transport if sid not in wrapper_ids]
                        live["quic_orphans"] += len(orphans)
                        seen = self._first_seen.setdefault(id(muxed), {})
                        for stream_id in orphans:
                            seen.setdefault(stream_id, now)
                        for stream_id in list(seen):
                            if stream_id not in transport or (stream_id in wrapper_ids):
                                del seen[stream_id]
                        quic_connections.add(id(muxed))
                elif isinstance(wrappers, dict):
                    # yamux/mplex expose exactly one stream registry; reporting
                    # it twice as "streams" and "wrappers" only adds noise.
                    live[f"{family}_streams"] += len(wrappers)
                connection_stats = getattr(muxed, "_stats", None)
                if isinstance(connection_stats, dict):
                    stats[id(muxed)] = {
                        key: float(connection_stats.get(key) or 0)
                        for key in (
                            "streams_opened",
                            "streams_closed",
                            "streams_accepted",
                            "bytes_sent",
                            "bytes_received",
                        )
                    }
        for stale in [key for key in self._first_seen if key not in quic_connections]:
            del self._first_seen[stale]
        return live, quic_connections, stats, connections

    def _accrue_work(self, stats: dict[int, dict[str, float]]) -> None:
        """Sum positive counter deltas into process-lifetime work totals."""
        for key, values in stats.items():
            previous = self._last_stats.get(key)
            if previous is not None:
                for name, value in values.items():
                    delta = value - previous.get(name, value)
                    if delta > 0:
                        self._work[name] += delta
            self._last_stats[key] = dict(values)
        for stale in [key for key in self._last_stats if key not in stats]:
            del self._last_stats[stale]

    def sample(self) -> Sample:
        now = time.monotonic()
        live, quic_connections, stats, connections = self._collect(now)
        self._accrue_work(stats)
        live["connections"] = connections
        sample = Sample(
            t=now,
            wall=time.time(),
            connections=connections,
            live=dict(live),
            work=dict(self._work),
            mem=memory_provenance(),
        )
        self._samples.append(sample)
        return sample

    # ---------------------------------------------------------------- analyse

    def _orphan_age(self) -> tuple[int, float, float]:
        """(count, oldest lower-bound age, p99 lower-bound age) for unwrapped."""
        now = time.monotonic()
        ages: list[float] = []
        for per_connection in self._first_seen.values():
            ages.extend(now - first for first in per_connection.values())
        if not ages:
            return 0, 0.0, 0.0
        ages.sort()
        index = min(len(ages) - 1, int(0.99 * len(ages)))
        return len(ages), ages[-1], ages[index]

    def report(self, window: int | None = None) -> dict[str, Any]:
        samples = list(self._samples)
        if window:
            samples = samples[-window:]
        if not samples:
            return {
                "state": "NO_DATA",
                "hint": "sampler has not run yet",
                "interval_s": self.interval,
            }
        # Absolute counts legitimately grow while peers are still being dialled:
        # that is population growth, not a leak, and treating it as one is how
        # "wait an hour and see" becomes the default.  Anchor the analysis on
        # the trailing period during which the peer population was stable, and
        # refuse to judge anything if there is no such period yet.
        final_connections = samples[-1].connections
        anchored_from = 0
        if final_connections > 0:
            low_water = 0.85 * final_connections
            below = [i for i, s in enumerate(samples) if s.connections < low_water]
            if below:
                anchored_from = below[-1] + 1
        stable = len(samples) - anchored_from >= MIN_STABLE_SAMPLES
        if stable:
            samples = samples[anchored_from:]
        connection_counts = [s.connections for s in samples]
        connection_range = {
            "min": min(connection_counts),
            "max": max(connection_counts),
        }
        first, last = samples[0], samples[-1]
        span = max(0.0, last.t - first.t)
        work_delta = last.work.get("streams_opened", 0.0) - first.work.get(
            "streams_opened", 0.0
        )
        classes: dict[str, dict[str, Any]] = {}
        for name in sorted(set(first.live) | set(last.live)):
            start = first.live.get(name, 0)
            end = last.live.get(name, 0)
            growth = end - start
            points = [
                (sample.work.get("streams_opened", 0.0), sample.live.get(name, 0))
                for sample in samples
            ]
            slope, stderr, r2 = _ols(points)
            first_per_connection = start / max(1, first.connections)
            last_per_connection = end / max(1, last.connections)
            per_connection_growth = last_per_connection - first_per_connection
            entry: dict[str, Any] = {
                "live": end,
                "previous_live": start,
                "growth": growth,
                "per_connection": round(last_per_connection, 3),
                "per_connection_growth": round(per_connection_growth, 3),
                "growth_per_1000_streams": (
                    round(slope * 1000.0, 3) if span >= MIN_VERDICT_SPAN_S else None
                ),
                "r2": round(r2, 3),
            }
            significant = (
                slope > 0
                and r2 >= 0.5
                and growth > max(8, 0.05 * start)
                and per_connection_growth > 0
                and (
                    slope > 3.0 * stderr if stderr not in (0.0, float("inf")) else True
                )
            )
            if not stable or span < MIN_VERDICT_SPAN_S or work_delta < MIN_VERDICT_WORK:
                entry["verdict"] = "INSUFFICIENT_DATA"
            elif significant:
                entry["verdict"] = "LEAKING"
                if slope > 0 and work_delta > 0:
                    growth_per_minute = slope * work_delta / (span / 60.0)
                    if growth_per_minute > 0:
                        entry["eta_minutes_to_double"] = round(
                            end / growth_per_minute, 1
                        )
            elif per_connection_growth <= 0:
                entry["verdict"] = "BOUNDED"
            else:
                entry["verdict"] = "SUSPECT"
            classes[name] = entry

        memory: dict[str, Any] = {}
        for key in last.mem:
            points = [(sample.t, sample.mem.get(key, 0.0)) for sample in samples]
            slope, _stderr, r2 = _ols(points)
            memory[key] = {
                "value": int(last.mem.get(key, 0.0)),
                "delta": int(last.mem.get(key, 0.0) - first.mem.get(key, 0.0)),
                "per_minute": round(slope * 60.0, 1) if span > 0 else None,
                "r2": round(r2, 3),
            }

        orphan_count, orphan_oldest, orphan_p99 = self._orphan_age()
        leaks = [
            name for name, entry in classes.items() if entry["verdict"] == "LEAKING"
        ]
        hard_leak = orphan_count > 0 and orphan_oldest > DEFAULT_ORPHAN_AGE_S
        if hard_leak or leaks:
            state = "LEAK"
        elif not stable or span < MIN_VERDICT_SPAN_S or work_delta < MIN_VERDICT_WORK:
            state = "WARMING_UP"
        else:
            state = "BOUNDED"
        return {
            "state": state,
            "span_seconds": round(span, 1),
            "samples": len(samples),
            "discarded_rampup_samples": anchored_from,
            "connections_stable": stable,
            "connection_range": connection_range,
            "streams_opened_since_start": int(last.work.get("streams_opened", 0.0)),
            "streams_opened_in_window": int(work_delta),
            "leaking_classes": leaks,
            "orphans": {
                "count": orphan_count,
                "oldest_age_seconds": round(orphan_oldest, 1),
                "p99_age_seconds": round(orphan_p99, 1),
                "threshold_seconds": DEFAULT_ORPHAN_AGE_S,
                "hard_leak": hard_leak,
            },
            "classes": classes,
            "memory": memory,
            "note": (
                "verdicts are per-connection and only issued once the peer "
                "population is stable; orphan ages are lower bounds (streams "
                "predating the sampler are stamped at start), so a hard_leak "
                "verdict cannot be a false positive"
            ),
        }

    def verdict_signature(self) -> tuple[Any, ...]:
        report = self.report()
        return (
            report["state"],
            tuple(report["leaking_classes"]),
            report["orphans"]["hard_leak"],
        )

    def should_log(self, min_interval: float = 300.0) -> bool:
        signature = self.verdict_signature()
        now = time.monotonic()
        if signature != self._last_logged_signature or (
            now - self._last_logged_at > min_interval
        ):
            self._last_logged_signature = signature
            self._last_logged_at = now
            return True
        return False


_WATCH: LeakWatch | None = None


def get_watch(peer: Any) -> LeakWatch | None:
    """Return the process-wide sampler, creating it on first use."""
    global _WATCH
    if peer is None:
        return None
    if _WATCH is None:
        _WATCH = LeakWatch(peer)
    return _WATCH


def reset_watch() -> None:
    """Drop the sampler.  Used by tests and on peer shutdown."""
    global _WATCH
    _WATCH = None

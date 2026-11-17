"""Leak verdicts must resolve in minutes and never fire on bounded retention."""

import time

from py_ipfs_lite.leak_watch import LeakWatch, Sample, _ols


def _watch_with(
    samples: list[tuple[float, float, int]],
    connections: int | list[int] | None = None,
) -> LeakWatch:
    """
    Build a watch over synthetic (t, streams_opened, live_streams) samples.

    Collection is bypassed so the analysis is a pure function of the series;
    this is what lets the detection latency be proven in milliseconds.
    """
    watch = LeakWatch(peer=None)
    for index, (t, opened, live) in enumerate(samples):
        if connections is None:
            peer_count = 400
        elif isinstance(connections, int):
            peer_count = connections
        else:
            peer_count = connections[index]
        watch._samples.append(
            Sample(
                t=t,
                wall=1_700_000_000.0 + t,
                connections=peer_count,
                live={"quic_streams": live, "quic_orphans": 0},
                work={"streams_opened": opened},
                mem={"python_blocks": 1_300_000.0 + index, "rss": 214_000_000.0},
            )
        )
    return watch


def test_bounded_retention_is_not_flagged() -> None:
    """A fixed retention window keeps live streams flat while work explodes."""
    samples = [(float(t), t * 50.0, 300) for t in range(0, 121, 4)]
    report = _watch_with(samples).report()
    assert report["state"] == "BOUNDED"
    assert report["classes"]["quic_streams"]["verdict"] == "BOUNDED"
    assert report["streams_opened_in_window"] == 6000


def test_leak_is_detected_in_a_two_minute_window() -> None:
    """0.5% of every cohort survives forever: visible after ~2 minutes."""
    samples = [
        (float(t), t * 50.0, 300 + int(t * 50.0 / 200)) for t in range(0, 121, 4)
    ]
    report = _watch_with(samples).report()
    assert report["span_seconds"] == 120.0
    assert report["state"] == "LEAK"
    entry = report["classes"]["quic_streams"]
    assert entry["verdict"] == "LEAKING"
    # 5 retained objects per 1000 streams opened.
    assert entry["growth_per_1000_streams"] == 5.0
    assert entry["growth"] > 9


def test_warmup_is_reported_as_warmup_not_as_bounded() -> None:
    """Refusing to answer is the correct answer before there is evidence."""
    samples = [(float(t), t * 50.0, 300) for t in range(0, 41, 4)]
    report = _watch_with(samples).report()
    assert report["state"] == "WARMING_UP"
    assert report["classes"]["quic_streams"]["verdict"] == "INSUFFICIENT_DATA"


def test_peer_rampup_is_not_mistaken_for_a_leak() -> None:
    """
    Growing resources during connection ramp-up are population growth.

    This is the exact false verdict that made "wait an hour and see" look
    necessary: absolute counts rise for minutes while peers are dialled.
    """
    samples = []
    connections = []
    for index, t in enumerate(range(0, 192, 6)):
        peers = 20 + index * 10  # still climbing when the series ends
        connections.append(peers)
        # One stream registry entry per peer, i.e. perfectly proportional.
        samples.append((float(t), t * 50.0, peers))
    report = _watch_with(samples, connections).report()
    assert report["state"] == "WARMING_UP"
    assert report["leaking_classes"] == []
    assert report["connections_stable"] is False


def test_verdict_anchors_after_rampup_and_then_bounds() -> None:
    """Once peers stabilise, proportional resources must read as BOUNDED."""
    samples = []
    connections = []
    for index, t in enumerate(range(0, 361, 6)):
        peers = min(400, 20 + index * 40)
        connections.append(peers)
        samples.append((float(t), t * 50.0, peers))
    report = _watch_with(samples, connections).report()
    assert report["connections_stable"] is True
    assert report["discarded_rampup_samples"] > 0
    assert report["state"] == "BOUNDED"
    assert report["classes"]["quic_streams"]["verdict"] == "BOUNDED"
    assert report["classes"]["quic_streams"]["per_connection_growth"] == 0.0


def test_orphan_age_is_a_zero_false_positive_test() -> None:
    """A wrappless stream older than any retention window is proof."""
    watch = _watch_with([(float(t), t * 50.0, 300) for t in range(0, 121, 4)])
    now = time.monotonic()
    watch._first_seen = {1: {8: now - 400.0, 12: now - 30.0}}
    report = watch.report()
    assert report["orphans"]["hard_leak"] is True
    assert report["orphans"]["count"] == 2
    assert report["orphans"]["oldest_age_seconds"] >= 400.0
    assert report["state"] == "LEAK"


def test_young_orphans_are_not_a_leak() -> None:
    """Freshly unwrapped streams are normal: the sweep has not run yet."""
    watch = _watch_with([(float(t), t * 50.0, 300) for t in range(0, 121, 4)])
    now = time.monotonic()
    watch._first_seen = {1: {8: now - 20.0}}
    report = watch.report()
    assert report["orphans"]["hard_leak"] is False
    assert report["state"] == "BOUNDED"


def test_ols_recovers_a_known_slope() -> None:
    slope, _stderr, r2 = _ols([(float(x), 3.0 * x + 7.0) for x in range(20)])
    assert abs(slope - 3.0) < 1e-9
    assert abs(r2 - 1.0) < 1e-9


def test_memory_block_trends_are_separated_by_heap() -> None:
    """Python, jemalloc and RSS are reported independently, never summed."""
    report = _watch_with([(float(t), t * 50.0, 300) for t in range(0, 121, 4)]).report()
    assert "python_blocks" in report["memory"]
    assert report["memory"]["python_blocks"]["per_minute"] is not None
    assert "rss" in report["memory"]


# --------------------------------------------------------------- collection


class _FakeAioquic:
    def __init__(self, streams: dict[int, object]) -> None:
        self._streams = streams


class QUICConnection:  # name is matched by leak_watch._family
    def __init__(
        self,
        wrappers: dict[int, object],
        transport: dict[int, object],
        stats: dict[str, int],
    ) -> None:
        self._streams = wrappers
        self._quic = _FakeAioquic(transport)
        self._stats = stats


class Yamux:
    def __init__(self, streams: dict[int, object]) -> None:
        self.streams = streams
        self.streams_msg_channels: dict[int, object] = {}


class _FakeConnection:
    def __init__(self, muxed_conn: object) -> None:
        self.muxed_conn = muxed_conn


class _FakeNetwork:
    def __init__(self, connections: dict[str, list[object]]) -> None:
        self.connections = connections


class _FakeHost:
    def __init__(self, network: _FakeNetwork) -> None:
        self._host = self
        self._network = network

    def get_network(self) -> _FakeNetwork:
        return self._network


class _FakePeer:
    def __init__(self, network: _FakeNetwork) -> None:
        self.host = _FakeHost(network)


def _connection_collector(connections: list[object]) -> LeakWatch:
    wrapped = [_FakeConnection(muxed) for muxed in connections]
    watch = LeakWatch(peer=_FakePeer(_FakeNetwork({"peer": wrapped})))
    watch._collect(time.monotonic())
    return watch


def test_collect_separates_orphans_from_wrappers() -> None:
    """aioquic streams without a py-libp2p wrapper are the leak-shaped ones."""
    transport = {0: object(), 4: object(), 8: object(), 12: object()}
    conn = QUICConnection(
        wrappers={0: object(), 4: object()},
        transport=transport,
        stats={"streams_opened": 120, "streams_closed": 116},
    )
    watch = LeakWatch(peer=_FakePeer(_FakeNetwork({"peer": [_FakeConnection(conn)]})))
    live, _keys, _stats, count = watch._collect(time.monotonic())
    assert count == 1
    assert live["quic_connections"] == 1
    assert live["quic_streams"] == 4
    assert live["quic_wrappers"] == 2
    assert live["quic_orphans"] == 2
    # Only the unwrapped ids get an age stamp, so ages cannot be inflated by
    # long-lived protocol streams that legitimately stay open.
    assert set(watch._first_seen[id(conn)]) == {8, 12}


def test_collect_reads_yamux_public_streams_registry() -> None:
    """yamux exposes ``streams``, not ``_streams``: a distinct attribute bug."""
    yamux = Yamux(streams={1: object(), 3: object(), 5: object()})
    watch = _connection_collector([yamux])
    live, _keys, _stats, _count = watch._collect(time.monotonic())
    assert live["yamux_connections"] == 1
    assert live["yamux_streams"] == 3


def test_work_accrual_counts_deltas_not_absolute_totals() -> None:
    """Cumulative counters must be summed as deltas across connections."""
    stats = {"streams_opened": 100, "streams_closed": 90}
    conn = QUICConnection(wrappers={}, transport={}, stats=stats)
    watch = LeakWatch(peer=_FakePeer(_FakeNetwork({"peer": [_FakeConnection(conn)]})))
    watch.sample()
    assert watch._work["streams_opened"] == 0  # first sight is a baseline only
    stats["streams_opened"] = 140
    watch.sample()
    assert watch._work["streams_opened"] == 40
    stats["streams_opened"] = 175
    watch.sample()
    assert watch._work["streams_opened"] == 75

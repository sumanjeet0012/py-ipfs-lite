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


def test_churn_is_not_mistaken_for_a_leak() -> None:
    """
    Stream counts sawtooth as cohorts open and are swept.

    A real node held yamux streams between 451 and 596 while its peer count
    held near 400; comparing endpoints of that series produced a false LEAK.
    A verdict must require a sustained per-connection trend, not a difference.
    """
    samples = []
    for index, t in enumerate(range(0, 181, 6)):
        live = 500 + (70 if index % 2 else -70)
        samples.append((float(t), t * 50.0, live))
    report = _watch_with(samples).report()
    entry = report["classes"]["quic_streams"]
    assert entry["verdict"] != "LEAKING"
    assert report["state"] != "LEAK"


def test_sustained_per_connection_trend_is_still_caught() -> None:
    """Requiring a trend must not blind the detector to a real one."""
    samples = []
    for index, t in enumerate(range(0, 181, 6)):
        # Sawtooth churn plus a steady 12%-per-window leak underneath it.
        live = 500 + (40 if index % 2 else -40) + index * 6
        samples.append((float(t), t * 50.0, live))
    report = _watch_with(samples).report()
    entry = report["classes"]["quic_streams"]
    assert entry["verdict"] == "LEAKING"
    assert entry["per_connection_growth_fraction"] >= 0.10
    assert report["state"] == "LEAK"
    assert report["evidence"] == "trend"


def test_real_node_transport_shift_is_not_a_leak() -> None:
    """
    Replay of six samples taken from the production node.

    QUIC connections fell 130 -> 102 while yamux rose 264 -> 284 and the total
    held near 400.  The old endpoint comparison called yamux_connections a leak;
    it is just the transport mix changing, and connection counts are bounded by
    the connection manager's watermarks in any case.
    """
    observed = [
        (402, 264, 556, 130, 112),
        (411, 274, 596, 129, 111),
        (402, 264, 475, 127, 110),
        (387, 267, 478, 109, 80),
        (398, 277, 451, 112, 84),
        (395, 284, 494, 102, 71),
    ]
    watch = LeakWatch(peer=None)
    for index, (
        conns,
        yamux_conns,
        yamux_streams,
        quic_conns,
        quic_streams,
    ) in enumerate(observed):
        watch._samples.append(
            Sample(
                t=float(index * 30),
                wall=0.0,
                connections=conns,
                live={
                    "connections": conns,
                    "yamux_connections": yamux_conns,
                    "yamux_streams": yamux_streams,
                    "quic_connections": quic_conns,
                    "quic_streams": quic_streams,
                    "quic_orphans": 3,
                },
                work={"streams_opened": index * 170.0},
                mem={},
            )
        )
    report = watch.report()
    assert report["state"] == "BOUNDED"
    assert report["classes"]["yamux_connections"]["verdict"] == "INFORMATIONAL"
    assert report["classes"]["yamux_streams"]["verdict"] == "BOUNDED"
    assert report["classes"]["connections"]["verdict"] == "INFORMATIONAL"


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


def test_connection_spike_on_latest_sample_does_not_reanchor() -> None:
    """
    The ramp-up anchor must not depend on the newest connection count.

    Production saw a 500-level connection sawtooth with a brief spike to 596.
    The old anchor used 85% of the *last* sample, so the spike declared the
    previous 4.4h to be ramp-up and a transient per-connection rise became
    LEAK; one sample later the same history read BOUNDED over the full window.
    """
    connections = [470 if i % 2 else 505 for i in range(1573)]
    connections += [520 if i % 2 else 550 for i in range(587)]
    samples = [
        (float(index * 10), index * 50.0, int(count * 0.6))
        for index, count in enumerate(connections)
    ]

    calm_counts = list(connections)
    calm_counts[-1] = 542
    spike_counts = list(connections)
    spike_counts[-1] = 596

    calm = _watch_with(samples, calm_counts).report()
    spike = _watch_with(samples, spike_counts).report()

    assert calm["discarded_rampup_samples"] == 0
    assert spike["discarded_rampup_samples"] == 0
    assert calm["span_seconds"] == spike["span_seconds"]
    assert calm["state"] == spike["state"] == "BOUNDED"


def test_tiny_absolute_swing_is_not_suspect() -> None:
    """
    A 15% move in a handful of channels is noise, not a suspicion.

    Production flagged SUSPECT on six mplex channels that went 13→15 (median
    thirds) while the endpoint moved by one. Percentages need an absolute floor.
    """
    watch = LeakWatch(peer=None)
    for index in range(30):
        live = 13 if index < 10 else (15 if index >= 20 else 14)
        watch._samples.append(
            Sample(
                t=float(index * 4),
                wall=1_700_000_000.0 + index * 4,
                connections=400,
                live={"mplex_streams": live},
                work={"streams_opened": index * 200.0},
                mem={"python_blocks": 1_300_000.0, "rss": 214_000_000.0},
            )
        )

    report = watch.report()
    entry = report["classes"]["mplex_streams"]
    assert entry["per_connection_growth_fraction"] >= 0.05
    assert entry["verdict"] == "BOUNDED"
    assert report["suspect_classes"] == []
    assert report["state"] == "BOUNDED"


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

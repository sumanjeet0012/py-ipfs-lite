import sys

from py_ipfs_lite.memory_profile import (
    _group_for_path,
    deep_size,
    format_report,
    object_census,
    profile,
    rss_breakdown,
    tracemalloc_report,
)


def test_group_for_path_maps_subsystems():
    assert _group_for_path("/x/site-packages/libp2p/bitswap/client.py") == "bitswap"
    assert (
        _group_for_path("/x/site-packages/libp2p/peer/persistent/datastore/x.py")
        == "peerstore.sqlite"
    )
    assert _group_for_path("/x/site-packages/libp2p/transport/quic/x.py") == (
        "transport.quic"
    )
    assert _group_for_path("/x/site-packages/trio/_core/_run.py") == "trio"
    assert _group_for_path("/usr/lib/python3.12/json/__init__.py") == "python.stdlib"
    assert _group_for_path("/x/site-packages/pydantic/main.py") == (
        "python.package:pydantic"
    )


def test_deep_size_counts_nested_payloads():
    payload = {"key": [b"x" * 4096, b"y" * 8192], "meta": {"n": list(range(50))}}
    size = deep_size(payload, max_objects=10_000, deadline=10)
    shallow = sys.getsizeof(payload)
    assert size > shallow + 4096 + 8192


def test_deep_size_does_not_double_count_shared_objects():
    shared = [b"z" * 10_000]
    first = {"a": shared}
    second = {"b": shared}
    seen: set[int] = set()
    one = deep_size(first, visited=seen, max_objects=10_000, deadline=10)
    two = deep_size(second, visited=seen, max_objects=10_000, deadline=10)
    # The second root shares the payload, so it must be charged far less.
    assert one > 10_000
    assert two < 1_000


def test_object_census_reports_groups():
    census = object_census(top=10)
    assert census["total_objects"] > 0
    assert census["groups"]
    assert census["by_count"]


def test_profile_without_tracing_is_explicit():
    report = profile(None, objects=True)
    assert "rss" in report
    assert report["tracemalloc"]["enabled"] in (True, False)
    if not report["tracemalloc"]["enabled"]:
        assert "hint" in report["tracemalloc"]
    assert "objects" in report
    # format_report must accept the structure with or without tracing.
    assert "RSS" in format_report(report)


def test_rss_breakdown_has_total():
    breakdown = rss_breakdown()
    assert "pid" in breakdown
    assert "allocator" in breakdown


def test_tracemalloc_report_after_start():
    import tracemalloc

    already = tracemalloc.is_tracing()
    if not already:
        tracemalloc.start(5)
    try:
        payload = [bytearray(1024) for _ in range(20)]
        assert payload
        report = tracemalloc_report(top=5)
        assert report["enabled"] is True
        assert report["total_traced_bytes"] > 0
        assert report["groups"]
    finally:
        if not already:
            tracemalloc.stop()


def test_baseline_diff_reports_growth():
    import tracemalloc

    from py_ipfs_lite.memory_profile import diff_report, set_baseline

    already = tracemalloc.is_tracing()
    if not already:
        tracemalloc.start(5)
    try:
        assert set_baseline() is True
        payload = [bytearray(2048) for _ in range(50)]
        assert payload
        report = diff_report(top=5)
        assert report["available"] is True
        assert report["groups"]
        total_growth = sum(g["size_diff_bytes"] for g in report["groups"])
        assert total_growth > 0
    finally:
        if not already:
            tracemalloc.stop()


def test_tracemalloc_report_can_skip_grouping():
    import tracemalloc

    from py_ipfs_lite.memory_profile import tracemalloc_report

    already = tracemalloc.is_tracing()
    if not already:
        tracemalloc.start(1)
    try:
        payload = [bytearray(512) for _ in range(10)]
        assert payload
        report = tracemalloc_report(top=5, groups=False)
        assert report["enabled"] is True
        assert report["groups_skipped"] is True
        assert report["groups"] == []
        assert report["total_traced_bytes"] > 0
    finally:
        if not already:
            tracemalloc.stop()


def test_network_snapshot_reports_limits():
    from py_ipfs_lite.memory_profile import network_snapshot

    class FakeLimiter:
        total_tokens = 250
        borrowed_tokens = 180

    class FakeNetwork:
        connection_config = type(
            "Cfg",
            (),
            {
                "max_connections": 300,
                "min_connections": 50,
                "low_watermark": 100,
                "high_watermark": 300,
                "max_connections_per_peer": 3,
            },
        )()
        _inbound_limiter = FakeLimiter()
        connections = {"a": [1, 2], "b": [3]}

    class FakeHost:
        def get_network(self):
            return FakeNetwork()

    class FakePeer:
        host = FakeHost()

    snap = network_snapshot(FakePeer())
    assert snap["max_connections"] == 300
    assert snap["min_connections"] == 50
    assert snap["inbound_capacity"] == 250
    assert snap["inbound_borrowed"] == 180
    assert snap["connections"] == 3
    assert "Connections:" in format_report({"rss": {}, "network": snap})

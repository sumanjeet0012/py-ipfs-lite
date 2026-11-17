from types import SimpleNamespace

from py_ipfs_lite.config import Config
from py_ipfs_lite.memory_diagnostics import (
    _connection_closed,
    quic_stream_census,
    retention_counts,
)
from py_ipfs_lite.peer import Peer


def test_bitswap_counts_follow_the_peers_exchange_adapter():
    """Peer.exchange is a method; inspect the actual stored adapter/client."""
    session = object()
    client = SimpleNamespace(
        _message_queues={"peer": object()},
        sim=SimpleNamespace(_interests={"cid-a": {session}, "cid-b": {session}}),
        provider_query_manager=None,
    )
    peer = Peer(Config(offline=True), exchange=SimpleNamespace(_exchange=client))
    result = retention_counts(peer, SimpleNamespace(notifees=[]))
    assert result["bitswap_queues"] == 1
    assert result["bitswap_session_interest_cids"] == 2
    assert result["bitswap_waiting_sessions"] == 1


def test_stream_ownership_exposes_closed_wrappers_and_orphan_channels():
    class Mplex:
        streams = {1: object()}
        streams_msg_channels = {1: object(), 2: object()}

    conn = SimpleNamespace(
        muxed_conn=Mplex(),
        streams=[SimpleNamespace(is_closed=False), SimpleNamespace(is_closed=True)],
    )
    result = retention_counts(
        SimpleNamespace(), SimpleNamespace(notifees=[], connections={"peer": [conn]})
    )
    assert result["stream_owners"]["Mplex"] == {
        "connections": 1,
        "network_streams": 2,
        "closed_network_streams": 1,
        "transport_streams": 1,
        "message_channels": 2,
        "orphan_message_channels": 1,
    }


def test_quic_stream_census_separates_wrapped_streams_from_orphans():
    class Quic:
        configuration = SimpleNamespace(is_client=True)
        _streams = {
            0: SimpleNamespace(
                sender=SimpleNamespace(is_finished=False),
                receiver=SimpleNamespace(is_finished=False),
            ),
            1: SimpleNamespace(
                sender=SimpleNamespace(is_finished=False),
                receiver=SimpleNamespace(is_finished=True),
            ),
            3: SimpleNamespace(
                sender=SimpleNamespace(is_finished=True),
                receiver=SimpleNamespace(is_finished=True),
            ),
        }

    class Muxed:
        _quic = Quic()
        _streams = {0: object(), 3: object()}

    conn = SimpleNamespace(muxed_conn=Muxed(), peer_id="peer-a")
    result = quic_stream_census(SimpleNamespace(connections={"peer-a": [conn]}))
    assert result["totals"] == {
        "connections": 1,
        "streams": 3,
        "wrappers": 2,
        "orphan_streams": 1,
        "orphan_unfinished": 1,
        "finished_not_discarded": 1,
    }
    assert result["states"] == {
        "sf=0 rf=0 peer_initiated=0 orphan=0": 1,
        "sf=0 rf=1 peer_initiated=1 orphan=1": 1,
        "sf=1 rf=1 peer_initiated=1 orphan=0": 1,
    }
    assert result["worst_connections"] == [
        {"orphans": 1, "peer_id": "peer-a", "streams": 3}
    ]


def test_connection_closed_reads_both_bool_and_method_forms():
    assert _connection_closed(SimpleNamespace(is_closed=True)) is True
    assert _connection_closed(SimpleNamespace(is_closed=False, _closed=False)) is False
    assert _connection_closed(SimpleNamespace(is_closed=lambda: True)) is True
    assert _connection_closed(SimpleNamespace(_closed=True)) is True


def test_quic_stream_census_ignores_non_quic_transports():
    class Yamux:
        streams = {1: object()}

    conn = SimpleNamespace(muxed_conn=Yamux(), peer_id="peer-b")
    result = quic_stream_census(SimpleNamespace(connections={"peer-b": [conn]}))
    assert result["totals"]["connections"] == 0
    assert result["states"] == {}


def test_peerstore_reports_record_sizes_as_well_as_cache_capacity():
    store = SimpleNamespace(
        peer_data_map={
            "one": SimpleNamespace(addrs=[1, 2], protocols=[1], ttl=0),
            "two": SimpleNamespace(addrs=[1, 2, 3], protocols=[1, 2], ttl=3600),
        },
        max_cached_peers=1000,
        peer_record_map={
            "one": SimpleNamespace(envelope=SimpleNamespace(raw_payload=b"abc"))
        },
    )
    peer = SimpleNamespace(host=SimpleNamespace(get_peerstore=lambda: store))
    result = retention_counts(peer, SimpleNamespace(notifees=[]))
    assert result["peerstore_cache"] == {
        "peers": 2,
        "capacity": 1000,
        "address_entries": 5,
        "max_addresses_per_peer": 3,
        "protocol_entries": 3,
        "permanent_or_legacy_records": 1,
        "signed_records": 1,
        "signed_payload_bytes": 3,
        "max_signed_payload_bytes": 3,
    }


def test_transport_owners_count_closed_connections_with_both_interfaces():
    class WebsocketTransport:
        _connections = {
            "live": SimpleNamespace(is_closed=lambda: False),
            "closed": SimpleNamespace(is_closed=lambda: True),
        }

    class QUICTransport:
        _connections = {"live": SimpleNamespace(is_closed=False)}

    network = SimpleNamespace(
        notifees=[],
        transport_manager=SimpleNamespace(
            get_transports=lambda: [WebsocketTransport(), QUICTransport()]
        ),
    )
    result = retention_counts(SimpleNamespace(), network)
    assert result["transport_owners"] == {
        "WebsocketTransport": {
            "tracked_connections": 2,
            "closed_tracked_connections": 1,
        },
        "QUICTransport": {"tracked_connections": 1, "closed_tracked_connections": 0},
    }


def test_bitswap_request_counters_expose_retained_latency_samples():
    client = SimpleNamespace(
        _message_queues={},
        sim=SimpleNamespace(_interests={}),
        _wantlist={"cid": {}},
        _broadcast_wants={"cid"},
        peer_manager=SimpleNamespace(
            peers={"one": SimpleNamespace(_pending_requests={"a": 1, "b": 2})}
        ),
    )
    result = retention_counts(
        SimpleNamespace(_exchange=client), SimpleNamespace(notifees=[])
    )
    assert result["bitswap_request_state"] == {
        "wants": 1,
        "broadcast_wants": 1,
        "peer_statistics": 1,
        "pending_latency_samples": 2,
    }


def test_scorer_diagnostics_detect_stale_subnet_membership_without_mutation():
    from libp2p.peer.id import ID
    from libp2p.pubsub.score import PeerScorer, ScoreParams

    scorer = PeerScorer(ScoreParams())
    peer = ID(b"remote")
    scorer.add_peer_ip(peer, "10.0.0.1")
    scorer.subnet_tracking["10.1.0"].add(peer)
    gossip = SimpleNamespace(back_off={}, scorer=scorer)
    result = retention_counts(
        SimpleNamespace(_gossipsub=gossip), SimpleNamespace(notifees=[])
    )
    assert result["gossipsub_scoring"] == {
        "ip_peers": 1,
        "subnets": 2,
        "subnet_memberships": 2,
        "stale_subnet_memberships": 1,
    }
    assert scorer.subnet_tracking["10.1.0"] == {peer}

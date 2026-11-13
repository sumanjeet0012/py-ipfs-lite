from types import SimpleNamespace

from py_ipfs_lite.config import Config
from py_ipfs_lite.memory_diagnostics import retention_counts
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

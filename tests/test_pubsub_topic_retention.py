"""Regression coverage for PubSub topic-discovery retention."""

import collections
from types import SimpleNamespace

import pytest

from py_ipfs_lite.peer._pubsub import PubsubMixin
from py_ipfs_lite.peer.state import PeerState


class _Config:
    pubsub_topics = []
    pubsub_auto_join_min_peers = 5
    pubsub_join_confirmations = 1
    pubsub_leave_misses = 2
    pubsub_auto_join_max_topics = 0
    pubsub_persist_topics = False
    blockstore_type = "memory"
    blockstore_path = None


class _Subscription:
    async def unsubscribe(self) -> None:
        pass


class _Peer(PubsubMixin):
    def __init__(self) -> None:
        self.config = _Config()
        self.pubsub = SimpleNamespace(peer_topics={})
        self._topic_stats = {}
        self._pubsub_subscriptions = {}
        self._pubsub_messages = {}
        self._auto_joined_topics = set()
        self._own_ipns_topic = ""
        self._state = PeerState.RUNNING

    async def subscribe_pubsub_topic(self, topic: str) -> None:
        pass

    def _save_auto_pubsub_topics(self) -> None:
        pass


@pytest.mark.trio
async def test_below_threshold_topic_stats_are_pruned():
    """Topics that never reach the join threshold must not accumulate."""
    peer = _Peer()
    for scan in range(10):
        peer.pubsub.peer_topics = {
            f"unique-below-threshold-{scan}-{i}": {"peer"} for i in range(100)
        }
        await peer._pubsub_discovery_pass()

    # leave_misses=2 keeps at most the current scan's topics, not all history.
    assert len(peer._topic_stats) <= 100
    assert not peer._pubsub_subscriptions


@pytest.mark.trio
async def test_auto_leave_clears_topic_stats_and_message_buffer():
    """Auto-leaving a stale topic must release its subscription and buffer."""
    peer = _Peer()
    peer._pubsub_subscriptions["stale-topic"] = _Subscription()
    peer._topic_stats["stale-topic"] = {"hits": 0, "misses": 2, "max_peers": 0}
    peer._pubsub_messages["stale-topic"] = collections.deque(
        [{"topic": "stale-topic", "data": "x" * 1024}] * 100, maxlen=100
    )

    await peer._pubsub_discovery_pass()

    assert "stale-topic" not in peer._pubsub_subscriptions
    assert "stale-topic" not in peer._topic_stats
    assert "stale-topic" not in peer._pubsub_messages

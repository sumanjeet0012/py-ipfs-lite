import os

# Start allocation tracing before any submodule (and therefore before the
# heavy libp2p/fastapi imports) allocates long-lived data, so the profiler can
# attribute those bytes too. Opt-in via IPFS_LITE_MEMORY_TRACING=1.
if os.environ.get("IPFS_LITE_MEMORY_TRACING", "").strip().lower() not in (
    "",
    "0",
    "false",
    "no",
    "off",
):
    from py_ipfs_lite.memory_profile import maybe_start_tracing

    maybe_start_tracing()

from py_ipfs_lite.config import AddParams, Config
from py_ipfs_lite.interfaces import BlockStore, DagService, Exchange, Host, Routing
from py_ipfs_lite.peer import (
    IPLDNode,
    Peer,
    PeerSession,
    PeerState,
    SeekableReader,
    default_bootstrap_peers,
    new_in_memory_datastore,
    setup_libp2p,
)

__all__ = [
    "Peer",
    "PeerState",
    "Config",
    "AddParams",
    "Host",
    "Routing",
    "BlockStore",
    "Exchange",
    "DagService",
    "IPLDNode",
    "PeerSession",
    "SeekableReader",
    "setup_libp2p",
    "default_bootstrap_peers",
    "new_in_memory_datastore",
]
__version__ = "0.1.2"

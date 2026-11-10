import logging
import os
import sys

import trio

from py_ipfs_lite.config import Config
from py_ipfs_lite.peer import Peer

logging.getLogger("py_ipfs_lite.reprovider").setLevel(logging.WARNING)


async def main(cid: str):
    print("Demo 5b: Reading data from disk (persistence test)")

    demo_dir = os.environ.get(
        "IPFS_DEMO_BLOCKSTORE",
        os.path.join(os.path.dirname(__file__), "demo_blocks"),
    )

    print("\nStarting an IPFS Peer (Reader)...")
    config = Config(
        blockstore_type="filesystem",
        blockstore_path=demo_dir,
        reprovide_interval_seconds=-1,
    )
    peer = Peer(config, listen_addrs=["/ip4/127.0.0.1/tcp/0"])
    await peer.start()

    print(f"Peer initialized with Peer ID: {peer.host.id()}")
    print(f"Listening on multiaddr: {peer.host.addrs()[0]}")

    print(f"\nAttempting to read node from local blockstore...\nTarget CID: {cid}")
    try:
        data = await peer.get_node(cid)
        print("\nSuccessfully read node from disk:")
        print(data)
    except Exception as e:
        print(f"\nFailed to read node: {e}")
    finally:
        print("\nClosing the peer cleanly...")
        await peer.close()


if __name__ == "__main__":
    demo_dir = os.environ.get(
        "IPFS_DEMO_BLOCKSTORE",
        os.path.join(os.path.dirname(__file__), "demo_blocks"),
    )
    cid_file = os.path.join(demo_dir, "last_cid.txt")

    if len(sys.argv) > 1:
        target_cid = sys.argv[1]
    elif os.path.exists(cid_file):
        with open(cid_file) as f:
            target_cid = f.read().strip()
        print(
            f"No CID argument provided; reading last CID written by 05a: {target_cid}"
        )
    else:
        print("Usage: python examples/05b_localstore_read.py <cid>")
        print("Hint: Run examples/05a_localstore_write.py first.")
        sys.exit(1)

    trio.run(main, target_cid)

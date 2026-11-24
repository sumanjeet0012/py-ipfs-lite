import json
import os
import tempfile
from unittest.mock import AsyncMock

import pytest
from httpx import ASGITransport, AsyncClient

from py_ipfs_lite.api import app
from py_ipfs_lite.config import Config
from py_ipfs_lite.peer import Peer


@pytest.fixture
def memory_config():
    return Config(
        blockstore_type="memory",
        reprovide_interval_seconds=-1,
        default_timeout=0.2,
        enable_pubsub=True,
    )


@pytest.fixture
async def api_client(memory_config):
    peer = Peer(memory_config, listen_addrs=["/ip4/127.0.0.1/tcp/0"])
    await peer.start()
    try:
        app.state.peer = peer
        transport = ASGITransport(app=app, raise_app_exceptions=False)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            yield client
    finally:
        await peer.close()


# ── 1. Node Identity & Version ───────────────────────────────────────────────────


@pytest.mark.trio
async def test_node_version_and_id_apis(api_client):
    # GET and POST version
    r = await api_client.get("/api/v0/version")
    assert r.status_code == 200
    assert r.json()["System"] == "py-ipfs-lite"
    assert "Version" in r.json()

    r = await api_client.post("/api/v0/version")
    assert r.status_code == 200

    # GET and POST id
    r = await api_client.get("/api/v0/id")
    assert r.status_code == 200
    data = r.json()
    assert "ID" in data
    assert isinstance(data["Addresses"], list)

    r = await api_client.post("/api/v0/id")
    assert r.status_code == 200


# ── 2. Content (Add, Cat, Ls) ────────────────────────────────────────────────────


@pytest.mark.trio
async def test_content_apis(api_client):
    content = b"Content API test file data"
    # POST /api/v0/add
    r = await api_client.post(
        "/api/v0/add",
        files={"file": ("test.txt", content, "text/plain")},
    )
    assert r.status_code == 200
    cid = r.json()["Hash"]
    assert cid is not None
    assert int(r.json()["Size"]) == len(content)

    # GET /api/v0/cat
    r = await api_client.get(f"/api/v0/cat?arg={cid}")
    assert r.status_code == 200
    assert r.content == content

    # POST /api/v0/cat
    r = await api_client.post(f"/api/v0/cat?arg={cid}")
    assert r.status_code == 200
    assert r.content == content

    # Cat 404 for missing block
    r = await api_client.get(
        "/api/v0/cat?arg=bafkreicwbc3r3ivsekh26gvj7t67zjucmbyelvsqwouyjnghkkmr7f5ynu"
    )
    assert r.status_code == 404

    # Cat 400 for invalid CID
    r = await api_client.get("/api/v0/cat?arg=not-a-cid")
    assert r.status_code == 400

    # Add empty file
    r = await api_client.post(
        "/api/v0/add",
        files={"file": ("empty.txt", b"", "application/octet-stream")},
    )
    assert r.status_code == 200
    empty_cid = r.json()["Hash"]
    r = await api_client.get(f"/api/v0/cat?arg={empty_cid}")
    assert r.status_code == 200
    assert r.content == b""

    # LS on non-directory (the text file) -> 400
    r = await api_client.get(f"/api/v0/ls?arg={cid}")
    assert r.status_code == 400


# ── 3. Blocks (Stat, Get, Put, Rm) ───────────────────────────────────────────────


@pytest.mark.trio
async def test_block_apis(api_client):
    block_data = b"Raw block payload for test_block_apis"

    # POST /api/v0/block/put with multipart form
    r = await api_client.post(
        "/api/v0/block/put",
        files={"file": ("raw.bin", block_data, "application/octet-stream")},
    )
    assert r.status_code == 200
    block_cid = r.json()["Key"]
    assert r.json()["Size"] == len(block_data)

    # POST /api/v0/block/put with raw body
    r = await api_client.post("/api/v0/block/put", content=b"another raw block")
    assert r.status_code == 200
    raw_cid = r.json()["Key"]
    assert r.json()["Size"] == len(b"another raw block")

    # GET /api/v0/block/stat
    r = await api_client.get(f"/api/v0/block/stat?arg={block_cid}")
    assert r.status_code == 200
    assert r.json()["Key"] == block_cid
    assert r.json()["Size"] == len(block_data)

    # POST /api/v0/block/stat
    r = await api_client.post(f"/api/v0/block/stat?arg={block_cid}")
    assert r.status_code == 200

    # GET /api/v0/block/get
    r = await api_client.get(f"/api/v0/block/get?arg={block_cid}")
    assert r.status_code == 200
    assert r.content == block_data

    # POST /api/v0/block/get
    r = await api_client.post(f"/api/v0/block/get?arg={block_cid}")
    assert r.status_code == 200
    assert r.content == block_data

    # Missing block stat/get -> 404
    missing_cid = "bafkreicwbc3r3ivsekh26gvj7t67zjucmbyelvsqwouyjnghkkmr7f5ynu"
    r = await api_client.get(f"/api/v0/block/stat?arg={missing_cid}")
    assert r.status_code == 404
    r = await api_client.get(f"/api/v0/block/get?arg={missing_cid}")
    assert r.status_code == 404

    # Invalid CID -> 400
    r = await api_client.get("/api/v0/block/stat?arg=invalid_cid")
    assert r.status_code == 400

    # POST /api/v0/block/rm
    r = await api_client.post(f"/api/v0/block/rm?arg={raw_cid}")
    assert r.status_code == 200
    assert r.json()["Hash"] == raw_cid

    # Verify it is removed
    r = await api_client.get(f"/api/v0/block/stat?arg={raw_cid}")
    assert r.status_code == 404


# ── 4. DAG & CAR & Refs ──────────────────────────────────────────────────────────


@pytest.mark.trio
async def test_dag_car_and_refs_apis(api_client):
    # DAG put (dag-json)
    payload = {"hello": "dag", "number": 123}
    r = await api_client.post(
        "/api/v0/dag/put?store-codec=dag-json",
        content=json.dumps(payload),
        headers={"content-type": "application/json"},
    )
    assert r.status_code == 200
    dag_json_cid = r.json()["Cid"]["/"]

    # DAG get (dag-json)
    r = await api_client.get(f"/api/v0/dag/get?arg={dag_json_cid}")
    assert r.status_code == 200
    assert r.json() == payload

    # DAG put (dag-cbor via format=dag-cbor)
    cbor_payload = {"msg": "cbor_test", "link": {"/": dag_json_cid}}
    r = await api_client.post(
        "/api/v0/dag/put?format=dag-cbor&input-enc=json",
        content=json.dumps(cbor_payload),
        headers={"content-type": "application/json"},
    )
    assert r.status_code == 200
    dag_cbor_cid = r.json()["Cid"]["/"]

    # DAG get (dag-cbor) as json
    r = await api_client.post(f"/api/v0/dag/get?arg={dag_cbor_cid}")
    assert r.status_code == 200
    assert r.json()["msg"] == "cbor_test"

    # DAG get (dag-cbor) with accept: application/cbor
    r = await api_client.get(
        f"/api/v0/dag/get?arg={dag_cbor_cid}",
        headers={"accept": "application/cbor"},
    )
    assert r.status_code == 200
    import cbor2

    decoded = cbor2.loads(r.content)
    assert decoded["msg"] == "cbor_test"

    # Refs on dag_cbor_cid (extracts child links!)
    r = await api_client.get(f"/api/v0/refs?arg={dag_cbor_cid}")
    assert r.status_code == 200
    refs = r.json()["Refs"]
    assert any(ref.get("Ref") == dag_json_cid for ref in refs)

    # Recursive refs
    r = await api_client.post(f"/api/v0/refs?arg={dag_cbor_cid}&recursive=true")
    assert r.status_code == 200
    assert len(r.json()["Refs"]) >= 1

    # DAG export as CAR
    r = await api_client.get(f"/api/v0/dag/export?arg={dag_cbor_cid}")
    assert r.status_code == 200
    car_bytes = r.content
    assert len(car_bytes) > 0

    # DAG import (multipart)
    r = await api_client.post(
        "/api/v0/dag/import",
        files={"file": ("export.car", car_bytes, "application/vnd.ipld.car")},
    )
    assert r.status_code == 200
    roots = [item["Hash"] for item in r.json()["Roots"]]
    assert dag_cbor_cid in roots

    # DAG import (raw body)
    r = await api_client.post("/api/v0/dag/import", content=car_bytes)
    assert r.status_code == 200
    assert dag_cbor_cid in [item["Hash"] for item in r.json()["Roots"]]

    # Empty CAR import -> 400
    r = await api_client.post("/api/v0/dag/import", content=b"")
    assert r.status_code == 400


# ── 5. Pinning & Garbage Collection ─────────────────────────────────────────────


@pytest.mark.trio
async def test_pin_and_gc_apis(api_client):
    r = await api_client.post(
        "/api/v0/add",
        files={"file": ("pintest.txt", b"pin me", "text/plain")},
    )
    cid = r.json()["Hash"]

    # POST /api/v0/pin/add (recursive)
    r = await api_client.post(f"/api/v0/pin/add?arg={cid}&recursive=true")
    assert r.status_code == 200
    assert cid in r.json()["Pins"]

    # GET /api/v0/pin/ls
    r = await api_client.get("/api/v0/pin/ls")
    assert r.status_code == 200
    assert cid in r.json()["Keys"]

    # GET /api/v0/pin/ls with specific arg
    r = await api_client.get(f"/api/v0/pin/ls?arg={cid}")
    assert r.status_code == 200
    assert cid in r.json()["Keys"]

    # GET /api/v0/pin/ls with missing arg -> 404
    missing_cid = "bafkreicwbc3r3ivsekh26gvj7t67zjucmbyelvsqwouyjnghkkmr7f5ynu"
    r = await api_client.get(f"/api/v0/pin/ls?arg={missing_cid}")
    assert r.status_code == 404

    # POST /api/v0/pin/rm
    r = await api_client.post(f"/api/v0/pin/rm?arg={cid}")
    assert r.status_code == 200
    assert cid in r.json()["Pins"]

    # Verify unpinned
    r = await api_client.get(f"/api/v0/pin/ls?arg={cid}")
    assert r.status_code == 404

    # Repo GC
    r = await api_client.post("/api/v0/repo/gc")
    assert r.status_code == 200
    assert "Key" in r.json()


# ── 6. Repository Statistics & Local Refs ────────────────────────────────────────


@pytest.mark.trio
async def test_repo_apis(api_client):
    # GET & POST /api/v0/repo/stat
    r = await api_client.get("/api/v0/repo/stat")
    assert r.status_code == 200
    data = r.json()
    assert "NumObjects" in data
    assert "RepoSize" in data

    r = await api_client.post("/api/v0/repo/stat")
    assert r.status_code == 200

    # GET & POST /api/v0/repo/version
    r = await api_client.get("/api/v0/repo/version")
    assert r.status_code == 200
    assert "Version" in r.json()

    r = await api_client.post("/api/v0/repo/version")
    assert r.status_code == 200

    # GET & POST /api/v0/refs/local
    r = await api_client.get("/api/v0/refs/local")
    assert r.status_code == 200
    assert "Refs" in r.json()

    r = await api_client.post("/api/v0/refs/local")
    assert r.status_code == 200


# ── 7. Swarm & Connection Manager ────────────────────────────────────────────────


@pytest.mark.trio
async def test_swarm_and_tag_apis(api_client):
    # Swarm peers
    r = await api_client.get("/api/v0/swarm/peers")
    assert r.status_code == 200
    assert "peers" in r.json()

    r = await api_client.post("/api/v0/swarm/peers")
    assert r.status_code == 200

    # Connection stats & stream stats
    r = await api_client.get("/api/v0/swarm/connection_stats")
    assert r.status_code == 200
    assert "Stats" in r.json()

    r = await api_client.get("/api/v0/swarm/stream_stats")
    assert r.status_code == 200

    r = await api_client.get("/api/v0/swarm/connection_metrics")
    assert r.status_code == 200
    r = await api_client.post("/api/v0/swarm/connection_metrics")
    assert r.status_code == 200

    # Swarm connect / disconnect validation
    r = await api_client.post("/api/v0/swarm/connect?arg=invalid_multiaddr")
    assert r.status_code == 400

    # Unreachable address gives 502 with dial failure
    unreachable = "/ip4/127.0.0.1/tcp/59999/p2p/12D3KooWD3eckifWpRn9wQpMG9R9hX3ChoCbzA366gm52knwqZLi"
    r = await api_client.post(f"/api/v0/swarm/connect?arg={unreachable}")
    assert r.status_code == 502

    r = await api_client.post("/api/v0/swarm/disconnect?arg=invalid_peer_id")
    assert r.status_code == 400

    # Peer tag and protection
    valid_peer_id = "12D3KooWD3eckifWpRn9wQpMG9R9hX3ChoCbzA366gm52knwqZLi"

    # Swarm tag set
    r = await api_client.post(
        f"/api/v0/swarm/tags?arg={valid_peer_id}&tag=priority&value=50"
    )
    assert r.status_code == 200
    assert r.json()["Tag"] == "priority"

    # Swarm tags list
    r = await api_client.get("/api/v0/swarm/tags")
    assert r.status_code == 200
    assert any(p["peer"] == valid_peer_id for p in r.json()["peers"])

    # Swarm protect
    r = await api_client.post(
        f"/api/v0/swarm/protect?arg={valid_peer_id}&tag=keep-alive"
    )
    assert r.status_code == 200

    # Swarm protection list
    r = await api_client.get("/api/v0/swarm/protection")
    assert r.status_code == 200
    assert any(p["peer"] == valid_peer_id for p in r.json()["peers"])

    r = await api_client.post("/api/v0/swarm/protection")
    assert r.status_code == 200

    # Swarm unprotect
    r = await api_client.post(
        f"/api/v0/swarm/unprotect?arg={valid_peer_id}&tag=keep-alive"
    )
    assert r.status_code == 200
    assert r.json()["Removed"] is True

    # Swarm tag remove
    r = await api_client.post(
        f"/api/v0/swarm/tags/remove?arg={valid_peer_id}&tag=priority"
    )
    assert r.status_code == 200
    assert r.json()["RemovedValue"] == 50


# ── 8. PubSub ────────────────────────────────────────────────────────────────────


@pytest.mark.trio
async def test_pubsub_apis(api_client):
    topic = "test-topic"

    # GET & POST pubsub/ls
    r = await api_client.get("/api/v0/pubsub/ls")
    assert r.status_code == 200
    assert "my_topics" in r.json()

    # PubSub publish (raw body)
    r = await api_client.post(f"/api/v0/pubsub/pub?arg={topic}", content=b"msg1")
    assert r.status_code == 200
    assert r.json()["Topic"] == topic

    # PubSub publish (empty body -> 400)
    r = await api_client.post(f"/api/v0/pubsub/pub?arg={topic}", content=b"")
    assert r.status_code == 400

    # PubSub sub (reads messages)
    r = await api_client.get(f"/api/v0/pubsub/sub?arg={topic}&timeout=0.1")
    assert r.status_code == 200
    assert r.json()["Topic"] == topic

    # PubSub unsub (DELETE)
    r = await api_client.delete(f"/api/v0/pubsub/sub?arg={topic}")
    assert r.status_code == 200
    assert r.json()["Topic"] == topic

    # PubSub unsub (POST alias)
    r = await api_client.post(f"/api/v0/pubsub/unsub?arg={topic}")
    assert r.status_code == 200


# ── 9. DHT & IPNS Naming ─────────────────────────────────────────────────────────


@pytest.mark.trio
async def test_dht_and_naming_apis(api_client):
    peer = app.state.peer
    orig_routing = peer.routing

    # Test 503 when routing is unavailable
    peer.routing = None
    r = await api_client.post("/api/v0/dht/provide?arg=bafytest")
    assert r.status_code == 503

    # IPNS publish in offline mode returns 404 (RoutingError) or 503
    r = await api_client.post("/api/v0/name/publish?arg=bafytest&lifetime=24h")
    assert r.status_code in (404, 503)

    # Restore routing
    peer.routing = orig_routing

    # IPNS publish invalid lifetime -> 400
    r = await api_client.post("/api/v0/name/publish?arg=bafytest&lifetime=invalid")
    assert r.status_code == 400

    # IPNS resolve invalid name -> 400
    r = await api_client.get("/api/v0/name/resolve?arg=not-a-valid-peer-id")
    assert r.status_code == 400

    # Test with simulated routing
    peer = app.state.peer
    mock_routing = AsyncMock()
    mock_routing.provide = AsyncMock(return_value=None)
    mock_routing.get_value = AsyncMock(return_value=b"mock-value")
    peer.routing = mock_routing

    # Now dht provide succeeds
    r = await api_client.post("/api/v0/dht/provide?arg=bafytest")
    assert r.status_code == 200
    assert r.json()["OK"] is True

    # Restore routing
    peer.routing = None


# ── 10. Operations & Diagnostics Endpoints ──────────────────────────────────────


@pytest.mark.trio
async def test_ops_and_diagnostics_apis(api_client):
    # debug/conns
    r = await api_client.get("/debug/conns")
    assert r.status_code == 200
    assert "total_connections" in r.json()

    # debug/connection-stats
    r = await api_client.get("/api/v0/debug/connection-stats")
    assert r.status_code == 200

    # Prometheus metrics
    r = await api_client.get("/metrics")
    assert r.status_code == 200
    assert "ipfs_" in r.text or "python_" in r.text or len(r.text) > 0

    r = await api_client.get("/debug/metrics/prometheus")
    assert r.status_code == 200

    # Debug peerstore and routing table
    r = await api_client.get("/api/v0/debug/peerstore")
    assert r.status_code == 200
    assert "count" in r.json()

    r = await api_client.get("/api/v0/debug/routing_table")
    assert r.status_code == 200
    assert "count" in r.json()

    # Memory leak watch
    r = await api_client.get("/api/v0/debug/memory/leak")
    assert r.status_code == 200

    # Memory retention
    r = await api_client.get("/api/v0/debug/memory/retention")
    assert r.status_code == 200
    assert "retention" in r.json()

    # Memory profile
    r = await api_client.get("/api/v0/debug/memory/profile?top=5")
    assert r.status_code == 200

    # Debug memory
    r = await api_client.get("/api/v0/debug/memory")
    assert r.status_code == 200
    assert "total_objects" in r.json()


# ── 11. Python Peer API Full Coverage ────────────────────────────────────────────


@pytest.mark.trio
async def test_peer_python_api(memory_config):
    peer = Peer(memory_config, listen_addrs=["/ip4/127.0.0.1/tcp/0"])
    await peer.start()
    try:
        assert peer._started is True

        # Test add_file variants
        test_bytes = b"Testing Python Peer API!"
        node1 = await peer.add_file(test_bytes)
        cid1 = str(node1)
        assert await peer.has_block(cid1)

        # get_file variants
        reader = await peer.get_file(cid1)
        assert (await reader.read()) == test_bytes

        # get_file output_path
        with tempfile.NamedTemporaryFile(delete=False) as tf:
            out_file = tf.name
        try:
            res = await peer.get_file(cid1, output_path=out_file)
            assert res is None
            with open(out_file, "rb") as f:
                assert f.read() == test_bytes
        finally:
            if os.path.exists(out_file):
                os.unlink(out_file)

        # DAGService standard methods: add, get, remove, get_many
        data_dict = {"title": "ipfs-lite-test", "count": 42}
        dag_node = await peer.add(data_dict, codec="dag-json")
        dag_cid = str(dag_node)

        fetched = await peer.get(dag_cid)
        assert fetched == data_dict

        many = await peer.get_many([dag_cid])
        assert len(many) == 1
        assert many[0] == data_dict

        # CAR import & export
        with tempfile.NamedTemporaryFile(suffix=".car", delete=False) as cf:
            car_path = cf.name
        try:
            await peer.export_car(dag_cid, car_path)
            roots = await peer.import_car(car_path)
            assert dag_cid in roots
        finally:
            if os.path.exists(car_path):
                os.unlink(car_path)

        # Pinning
        await peer.add_pin(dag_cid, recursive=True)
        pins = await peer.list_pins(type_filter="recursive")
        assert dag_cid in pins

        await peer.remove_pin(dag_cid)
        pins_after = await peer.list_pins(type_filter="all")
        assert dag_cid not in pins_after

        # GC
        gc_res = await peer.gc()
        assert hasattr(gc_res, "reclaimed_blocks")
        assert hasattr(gc_res, "retained_blocks")

        # Pubsub methods
        test_sub_topic = "python-api-topic"
        await peer.subscribe_pubsub_topic(test_sub_topic)
        await peer.publish_pubsub(test_sub_topic, b"hello peer pubsub")
        unsub_ok = await peer.unsubscribe_pubsub_topic(test_sub_topic)
        assert unsub_ok is True

        # DAG remove
        await peer.remove(dag_cid)
        assert not await peer.has_block(dag_cid)

    finally:
        await peer.close()

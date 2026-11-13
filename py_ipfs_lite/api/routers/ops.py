"""Operations & debugging routes for the py-ipfs-lite HTTP API."""

from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from py_ipfs_lite.peer import Peer

router = APIRouter()


@router.post("/api/v0/debug/connection-stats")
@router.get("/api/v0/debug/connection-stats")
@router.get("/debug/conns")
async def debug_conns(request: Request) -> Any:
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import swarm_service

    total = await swarm_service.count_connections(peer)
    return JSONResponse(content={"total_connections": total})


@router.get("/metrics")
@router.get("/debug/metrics/prometheus")
async def metrics(request: Request) -> Any:
    """Expose Prometheus metrics."""
    peer: Peer = getattr(request.app.state, "peer", None)
    from py_ipfs_lite.metrics import update_live_metrics

    update_live_metrics(peer)
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@router.post("/api/v0/debug/peerstore")
@router.get("/api/v0/debug/peerstore")
async def debug_peerstore(request: Request) -> Any:
    """Return peer store information for debugging."""
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import swarm_service

    res = await swarm_service.list_peerstore_peers(peer)
    return JSONResponse(content={"count": res.count, "peers": res.peers})


@router.post("/api/v0/debug/routing_table")
@router.get("/api/v0/debug/routing_table")
async def debug_routing_table(request: Request) -> Any:
    """Return routing table information for debugging."""
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import swarm_service

    res = await swarm_service.list_routing_table_peers(peer)
    return JSONResponse(content={"count": res.count, "peers": res.peers})


@router.get("/api/v0/debug/memory/retention")
async def debug_memory_retention(request: Request) -> Any:
    """Read ownership and allocator counters without GC or a heap scan."""
    from py_ipfs_lite.memory_diagnostics import allocator_stats, retention_counts

    peer: Peer = request.app.state.peer
    raw_host = getattr(peer.host, "_host", peer.host) if peer.host else None
    retention = retention_counts(peer, raw_host.get_network()) if raw_host else {}
    return JSONResponse(
        content={"retention": retention, "allocator": allocator_stats()}
    )


@router.get("/api/v0/debug/memory")
async def debug_memory(request: Request) -> Any:
    """Detailed live memory and object introspection endpoint."""
    import gc
    import sys
    from collections import Counter

    gc.collect()

    peer: Peer = request.app.state.peer
    counts: Counter[str] = Counter()
    sizes: Counter[str] = Counter()
    all_objs = gc.get_objects()
    for obj in all_objs:
        t_name = f"{type(obj).__module__}.{type(obj).__name__}"
        counts[t_name] += 1
        try:
            sizes[t_name] += sys.getsizeof(obj)
        except Exception:
            pass

    top_by_count = [
        {"type": t, "count": c, "size_mb": round(sizes[t] / (1024 * 1024), 2)}
        for t, c in counts.most_common(50)
    ]
    top_by_size = [
        {"type": t, "count": counts[t], "size_mb": round(s / (1024 * 1024), 2)}
        for t, s in sorted(sizes.items(), key=lambda x: x[1], reverse=True)[:50]
    ]

    # Subsystem inspect
    subsystems: dict[str, Any] = {}
    try:
        raw_host = getattr(peer.host, "_host", peer.host) if peer.host else None
        if raw_host is not None:
            raw_swarm = raw_host.get_network()  # type: ignore
            subsystems["swarm_connections_len"] = (
                len(raw_swarm.connections)
                if hasattr(raw_swarm, "connections")
                else None
            )
            subsystems["swarm_listeners_len"] = (
                len(raw_swarm.listeners) if hasattr(raw_swarm, "listeners") else None
            )
            # Peer keys can stay flat while per-peer connection lists keep
            # retired SwarmConn objects alive; report the real object count.
            if hasattr(raw_swarm, "connections"):
                subsystems["swarm_conn_objects"] = sum(
                    len(conns) if isinstance(conns, list) else 1
                    for conns in raw_swarm.connections.values()
                )
            if hasattr(raw_swarm, "transport_manager"):
                tm = raw_swarm.transport_manager
                subsystems["transports"] = [
                    type(t).__name__ for t in tm.get_transports()
                ]
    except Exception as e:
        subsystems["swarm_err"] = str(e)

    try:
        raw_host = getattr(peer.host, "_host", peer.host) if peer.host else None
        if raw_host is not None and hasattr(raw_host, "get_peerstore"):
            ps = raw_host.get_peerstore()
            if hasattr(ps, "count_peers"):
                subsystems["peerstore_peers_count"] = ps.count_peers()
            elif hasattr(ps, "peer_data_map"):
                subsystems["peerstore_peers_count"] = len(ps.peer_data_map)
            else:
                subsystems["peerstore_peers_count"] = len(ps.peer_ids())
            if hasattr(ps, "peer_data_map"):
                subsystems["peer_data_map_len"] = len(ps.peer_data_map)
            if hasattr(ps, "peer_record_map"):
                subsystems["peer_record_map_len"] = len(ps.peer_record_map)
    except Exception as e:
        subsystems["peerstore_err"] = str(e)

    try:
        ping_svc = getattr(peer, "_ping_service", None)
        if ping_svc is not None:
            subsystems["ping_outbound_streams_len"] = len(
                getattr(ping_svc, "_outbound_streams", {})
            )
            subsystems["ping_inbound_streams_len"] = len(
                getattr(ping_svc, "_inbound_streams", {})
            )
    except Exception as e:
        subsystems["ping_err"] = str(e)

    try:
        from py_ipfs_lite.memory_diagnostics import retention_counts

        raw_host = getattr(peer.host, "_host", peer.host) if peer.host else None
        if raw_host is not None:
            subsystems["retention"] = retention_counts(peer, raw_host.get_network())
    except Exception as e:
        subsystems["retention_error"] = str(e)

    # Introspect Trio tasks via GC
    task_counts: Counter[str] = Counter()
    start_task_stacks: Counter[str] = Counter()
    try:
        task_objs = [o for o in all_objs if type(o).__name__ == "Task"]
        for t in task_objs:
            t_name = getattr(t, "name", None)
            if t_name and not t_name.startswith("<"):
                task_counts[t_name] += 1
            elif hasattr(t, "coro") and hasattr(t.coro, "cr_code"):
                co = t.coro.cr_code
                task_counts[
                    f"{co.co_filename.split('/')[-1]}:{co.co_name}:{co.co_firstlineno}"
                ] += 1
            else:
                task_counts[str(t_name or "unknown")] += 1

            if t_name == "start[daemon=False]":
                coro = getattr(t, "coro", None)
                curr = coro
                frames = []
                while curr:
                    if hasattr(curr, "cr_frame") and curr.cr_frame:
                        f = curr.cr_frame
                        frames.append(
                            f"{f.f_code.co_filename.split('/')[-1]}:{f.f_lineno} ({f.f_code.co_name})"
                        )
                        curr = getattr(curr, "cr_await", None)
                    else:
                        break
                stack_str = " -> ".join(frames[-4:]) if frames else "no_frames"
                start_task_stacks[stack_str] += 1
    except Exception as e:
        start_task_stacks[f"error: {e}"] += 1

    # Introspect referrers of ServerQuicConnection
    server_conns = [o for o in all_objs if type(o).__name__ == "ServerQuicConnection"]
    server_conn_referrers = []
    try:
        if server_conns:
            for r in gc.get_referrers(server_conns[0])[:5]:
                try:
                    if isinstance(r, dict):
                        server_conn_referrers.append(f"dict(keys={list(r.keys())[:5]})")
                    else:
                        server_conn_referrers.append(
                            f"{type(r).__name__}: {str(r)[:80]}"
                        )
                except Exception:
                    pass
    except Exception:
        pass

    # Connection/teardown accounting. These counts must stay flat in steady
    # state; a rising SwarmConn/Yamux/QUIC count while swarm_connections_len is
    # flat means retired connections are being retained.
    tracked_classes = (
        "libp2p.network.connection.swarm_connection.SwarmConn",
        "libp2p.stream_muxer.yamux.yamux.Yamux",
        "libp2p.transport.quic.connection.QUICConnection",
        "libp2p.transport.quic.listener.ServerQuicConnection",
        "aioquic.quic.connection.QuicConnection",
        "libp2p.bitswap.message_queue.BitswapMessageQueue",
        "trio.lowlevel.Task",
    )
    class_counts = {name: counts.get(name, 0) for name in tracked_classes}

    # Test malloc_trim
    rss_before_trim = 0.0
    rss_after_trim = 0.0
    allocator: dict[str, Any] = {}
    trim_attempted = False
    try:
        import ctypes
        import os

        import psutil

        proc = psutil.Process(os.getpid())
        rss_before_trim = proc.memory_info().rss / (1024 * 1024)
        from py_ipfs_lite.memory_diagnostics import allocator_stats

        allocator = allocator_stats()
        if allocator["backend"] != "jemalloc":
            libc = ctypes.CDLL("libc.so.6")
            libc.malloc_trim(0)
            trim_attempted = True
        rss_after_trim = proc.memory_info().rss / (1024 * 1024)
    except Exception:
        pass

    return JSONResponse(
        content={
            "total_objects": len(all_objs),
            "rss_before_trim_mb": round(rss_before_trim, 2),
            "rss_after_trim_mb": round(rss_after_trim, 2),
            "allocator": allocator,
            "allocator_trim_attempted": trim_attempted,
            "server_conns_count": len(server_conns),
            "server_conn_referrers": server_conn_referrers,
            "class_counts": class_counts,
            "start_task_stacks": dict(start_task_stacks.most_common(20)),
            "top_tasks": [
                {"task": k, "count": int(v)} for k, v in task_counts.most_common(20)
            ],
            "top_by_count": top_by_count,
            "top_by_size": top_by_size,
            "subsystems": subsystems,
        }
    )

"""IPNS naming routes for the py-ipfs-lite HTTP API."""

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from py_ipfs_lite.peer import Peer

router = APIRouter()


@router.post("/api/v0/name/publish")
async def name_publish(
    request: Request,
    arg: str = Query(..., description="IPFS path of the object to be published"),
    lifetime: str = Query("24h"),
) -> Any:
    """Publish an IPNS record."""
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.services import naming_service

    # Parse lifetime: "24h", "7d", "30m", "3600s", or plain number (hours)
    try:
        if lifetime.endswith("d"):
            lifetime_hours = max(1, int(lifetime[:-1]) * 24)
        elif lifetime.endswith("h"):
            lifetime_hours = max(1, int(lifetime[:-1]))
        elif lifetime.endswith("m"):
            lifetime_hours = max(1, int(lifetime[:-1]) // 60)
        elif lifetime.endswith("s"):
            lifetime_hours = max(1, int(lifetime[:-1]) // 3600)
        else:
            lifetime_hours = max(1, int(lifetime))
    except (ValueError, TypeError):
        raise HTTPException(
            status_code=400, detail=f"Invalid lifetime format: {lifetime}"
        )

    result_name = await naming_service.publish_name(
        peer, arg, lifetime_hours=lifetime_hours
    )
    return JSONResponse(content={"Name": result_name, "Value": arg})


@router.post("/api/v0/name/resolve")
@router.get("/api/v0/name/resolve")
async def name_resolve(
    request: Request,
    arg: str = Query(..., description="IPFS path of the name to resolve"),
) -> Any:
    """Resolve an IPNS record."""
    peer: Peer = request.app.state.peer
    from py_ipfs_lite.exceptions import InvalidCidError
    from py_ipfs_lite.services import naming_service

    try:
        path = await naming_service.resolve_name(peer, arg)
    except (InvalidCidError, ValueError) as e:
        raise HTTPException(status_code=400, detail=f"Invalid IPNS name: {arg}") from e
    return JSONResponse(content={"Path": path})

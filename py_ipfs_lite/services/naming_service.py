from py_ipfs_lite.exceptions import InvalidCidError
from py_ipfs_lite.peer import Peer


async def publish_name(peer: Peer, arg: str, lifetime_hours: int = 24) -> str:
    # IPNS record values must be full paths (/ipfs/, /ipns/ or /dnslink/).
    # Accept bare CIDs for Kubo-API compatibility and normalise them.
    value = arg if arg.startswith(("/ipfs/", "/ipns/", "/dnslink/")) else f"/ipfs/{arg}"
    return await peer.publish_name(value, lifetime_hours=lifetime_hours)


async def resolve_name(peer: Peer, name: str) -> str:
    clean_name = name.removeprefix("/ipns/") if name.startswith("/ipns/") else name
    try:
        return await peer.resolve_name(clean_name)
    except ValueError as e:
        raise InvalidCidError(f"Invalid IPNS name: {name}") from e

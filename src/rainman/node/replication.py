"""Leader fanout and per-follower replication helpers.

DDIA §5 — Single-leader replication: the leader writes first to its own
WAL, then fans out the same entry to every follower concurrently via
POST /replicate.  A write is durable from the client's perspective only
after a quorum (majority) of nodes have persisted it.
"""

import asyncio
import logging

import httpx

logger = logging.getLogger(__name__)


async def _send_replicate(
    client: httpx.AsyncClient,
    peer: dict,
    entry: dict,
    timeout_s: float,
) -> bool:
    """POST one WAL entry to a single peer's /replicate endpoint.

    Returns True if the peer responded 200.  Any non-200 status or
    network exception is treated as a failed ack and logged at WARNING.
    Never raises — the caller aggregates results with asyncio.gather.
    """
    url = f"http://{peer['host']}:{peer['port']}/replicate"
    try:
        resp = await client.post(url, json=entry, timeout=timeout_s)
        if resp.status_code == 200:
            return True
        logger.warning(
            "Peer %s rejected replicate LSN %d: HTTP %d — %s",
            peer["node_id"],
            entry["lsn"],
            resp.status_code,
            resp.text[:200],
        )
        return False
    except Exception as exc:
        logger.warning(
            "Replicate to %s LSN %d failed: %s",
            peer["node_id"],
            entry["lsn"],
            exc,
        )
        return False


async def fanout_and_wait_majority(
    peers: list[dict],
    entry: dict,
    replication_timeout_ms: int,
    majority: int,
) -> bool:
    """Fan out a WAL entry to all peers; return True if majority persisted it.

    The leader counts as one ack (it already wrote to its own WAL before
    this call), so the threshold for follower acks is (majority - 1).
    All peer calls are issued concurrently via asyncio.gather with the
    same timeout, so total latency ≈ slowest responding peer.
    Returns False if fewer than (majority - 1) followers acked in time.
    DDIA §5: majority acknowledgement as the durability boundary.
    """
    needed = majority - 1  # leader already persisted its copy
    timeout_s = replication_timeout_ms / 1000.0

    if needed <= 0:
        # Majority satisfied by the leader alone (e.g., single-node test).
        return True

    async with httpx.AsyncClient() as client:
        results = await asyncio.gather(
            *[
                _send_replicate(client, peer, entry, timeout_s)
                for peer in peers
            ],
            return_exceptions=True,
        )

    acks = sum(1 for r in results if r is True)
    logger.debug(
        "Replicated LSN %d — %d/%d follower acks (needed %d)",
        entry["lsn"],
        acks,
        len(peers),
        needed,
    )
    return acks >= needed


async def send_heartbeat(
    client: httpx.AsyncClient,
    peer: dict,
    payload: dict,
    timeout_s: float,
) -> None:
    """Send one heartbeat to a single peer.  Best-effort; never raises.

    DDIA §5: periodic leader heartbeats suppress follower election
    timeouts.  A failed heartbeat is logged at DEBUG only — transient
    network blips should not generate noise at WARNING.
    """
    url = f"http://{peer['host']}:{peer['port']}/heartbeat"
    try:
        await client.post(url, json=payload, timeout=timeout_s)
    except Exception as exc:
        logger.debug("Heartbeat to %s failed: %s", peer["node_id"], exc)

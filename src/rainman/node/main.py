"""FastAPI node application for the Rainman key-value store.

Phase 2: Three-node replication.  The node with priority 1 in
cluster.json is the static leader; all others are followers.  The
leader fans out every write to followers and waits for a majority ack
before confirming to the client.  Followers reject writes with 409 and
accept replication entries via POST /replicate.

Phase 3 will replace the static role assignment with election.py logic.
"""

import asyncio
import datetime
import logging
import os
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from rainman.node import replication as repl
from rainman.node.config import (
    get_peers,
    get_static_leader,
    load_cluster_config,
)
from rainman.node.storage import StorageEngine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

NODE_ID: str = os.getenv("NODE_ID", "node1")

# ---------------------------------------------------------------------------
# Mutable cluster state
# Updated by lifespan on startup; Phase 3 election.py will mutate these
# on leader change.
# ---------------------------------------------------------------------------
_role: str = "follower"
_term: int = 1
_leader_id: str | None = None
_peers: list[dict] = []
_replication_cfg: dict = {}

storage: StorageEngine | None = None

# Serialises LSN generation + WAL write so concurrent requests cannot
# claim the same LSN (DDIA §5: leader assigns sequence numbers).
# asyncio.Lock is safe at module level in Python ≥ 3.10 — it does not
# bind to an event loop until first acquired.
_write_lock = asyncio.Lock()


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialise the node on startup and clean up on shutdown."""
    global storage, _role, _term, _leader_id, _peers, _replication_cfg

    cfg = load_cluster_config()
    _peers = get_peers(cfg, NODE_ID)
    _replication_cfg = cfg["replication"]

    # Phase 2: static leader is the node with priority 1.
    static_leader = get_static_leader(cfg)
    _leader_id = static_leader["node_id"]
    _role = "leader" if NODE_ID == _leader_id else "follower"

    wal_path = os.getenv("WAL_PATH", "/data/wal.jsonl")
    wal_dir = os.path.dirname(os.path.abspath(wal_path))
    os.makedirs(wal_dir, exist_ok=True)
    storage = StorageEngine(wal_path)
    recovered_lsn = storage.replay()
    logger.info(
        "Node %s started as %s (term=%d). WAL replayed — LSN=%d",
        NODE_ID,
        _role,
        _term,
        recovered_lsn,
    )

    hb_task: asyncio.Task | None = None
    if _role == "leader":
        hb_task = asyncio.create_task(_heartbeat_loop())

    yield

    if hb_task is not None:
        hb_task.cancel()
        try:
            await hb_task
        except asyncio.CancelledError:
            pass


app = FastAPI(lifespan=lifespan)


# ---------------------------------------------------------------------------
# Background heartbeat (leader only)
# ---------------------------------------------------------------------------


async def _heartbeat_loop() -> None:
    """Send periodic heartbeats to all followers.

    Runs every heartbeat_interval_ms milliseconds for the lifetime of
    the leader process.  A failed heartbeat to any follower is silently
    dropped — transient loss does not stop the loop.
    DDIA §5: heartbeats suppress follower election timeouts.
    """
    interval_s = _replication_cfg.get("heartbeat_interval_ms", 200) / 1000.0
    # Short timeout: heartbeats are latency-sensitive
    timeout_s = 0.1
    while True:
        await asyncio.sleep(interval_s)
        payload = {
            "leader_id": NODE_ID,
            "term": _term,
            "lsn": storage.current_lsn(),
        }
        async with httpx.AsyncClient() as client:
            await asyncio.gather(
                *[
                    repl.send_heartbeat(client, p, payload, timeout_s)
                    for p in _peers
                ],
                return_exceptions=True,
            )


# ---------------------------------------------------------------------------
# Pydantic request models
# ---------------------------------------------------------------------------


class PutRequest(BaseModel):
    """Request body for PUT /kv/{key}."""

    value: dict


class ReplicateRequest(BaseModel):
    """Request body for POST /replicate."""

    lsn: int
    term: int
    op: str
    key: str
    value: dict


class HeartbeatRequest(BaseModel):
    """Request body for POST /heartbeat."""

    leader_id: str
    term: int
    lsn: int


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now_iso() -> str:
    """Return current UTC time formatted to millisecond precision."""
    return (
        datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%S.%f"
        )[:-3]
        + "Z"
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@app.get("/health")
def health():
    """Return node identity and current state.

    Polled by the recovery agent and scripts/verify_cluster.py.
    Never raises — always returns 200.
    """
    return {
        "node_id": NODE_ID,
        "role": _role,
        "term": _term,
        "lsn": storage.current_lsn(),
        "leader_id": _leader_id,
        "timestamp": _now_iso(),
    }


@app.put("/kv/{key}")
async def put_kv(key: str, body: PutRequest):
    """Write a value to the store.

    Accepted only by the leader.  Followers return 409 so the client can
    redirect to the correct node.  The leader writes to its WAL first,
    then fans out to followers.  Returns 200 only after majority ack;
    returns 503 if the majority threshold is not met within the timeout
    (the entry remains in the leader WAL — no rollback).
    DDIA §5: single-leader replication with synchronous majority ack.
    """
    if _role != "leader":
        return JSONResponse(
            status_code=409,
            content={
                "status": "not_leader",
                "leader_id": _leader_id,
            },
        )

    async with _write_lock:
        lsn = storage.current_lsn() + 1
        storage.put(lsn, _term, key, body.value)
        entry = {
            "lsn": lsn,
            "term": _term,
            "op": "PUT",
            "key": key,
            "value": body.value,
        }
        acked = await repl.fanout_and_wait_majority(
            _peers,
            entry,
            _replication_cfg.get("replication_timeout_ms", 500),
            _replication_cfg.get("majority", 2),
        )

    if not acked:
        logger.warning(
            "LSN %d written to leader WAL but majority ack not reached",
            lsn,
        )
        return JSONResponse(
            status_code=503,
            content={"status": "majority_ack_failed", "lsn": lsn},
        )

    return {"status": "ok", "lsn": lsn}


@app.get("/kv/{key}")
def get_kv(key: str):
    """Read a value from the local in-memory index.

    Any node accepts reads — no forwarding to the leader.
    Returns 404 if the key has never been written to this node.
    Never raises beyond HTTP 500 on a catastrophic error.
    """
    value = storage.get(key)
    if value is None:
        return JSONResponse(status_code=404, content={"status": "not_found"})
    return {
        "key": key,
        "value": value,
        "lsn": storage.current_lsn(),
        "node_id": NODE_ID,
    }


@app.post("/replicate")
async def replicate(body: ReplicateRequest):
    """Apply a WAL entry forwarded by the leader.

    Rejects with 409 if the incoming term is behind this node's current
    term (stale leader), or if the incoming LSN is not exactly
    current_lsn + 1 (LSN gap means this follower missed prior entries).
    On success, persists the entry and returns ack.
    DDIA §5: follower replication path.
    """
    global _term

    if body.term < _term:
        return JSONResponse(
            status_code=409,
            content={"status": "term_mismatch", "current_term": _term},
        )

    async with _write_lock:
        # Accept a higher term from a newly elected leader.
        if body.term > _term:
            _term = body.term

        expected = storage.current_lsn() + 1
        if body.lsn != expected:
            return JSONResponse(
                status_code=409,
                content={
                    "status": "lsn_gap",
                    "expected": expected,
                    "got": body.lsn,
                },
            )

        storage.put(body.lsn, body.term, body.key, body.value)

    return {"status": "ok", "lsn": body.lsn}


@app.post("/heartbeat")
def heartbeat(body: HeartbeatRequest):
    """Accept a heartbeat from the current leader.

    Updates the known leader_id and current term.  Phase 3 will use this
    callback to reset the follower's election timeout.
    Always returns 200.
    DDIA §5: follower liveness signaling from leader.
    """
    global _term, _leader_id
    if body.term >= _term:
        _term = body.term
        _leader_id = body.leader_id
    return {"status": "ok"}

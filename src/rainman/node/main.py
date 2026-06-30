"""FastAPI node application for the Rainman key-value store.

Phase 3: Leader election.  All nodes start as followers; an elected
leader is determined by election.py's timeout + vote protocol.  On each
restart the node reads its highest term from the WAL to preserve term
monotonicity across crashes (DDIA §5: currentTerm is persistent state).

Phase 2 note: the static-leader assignment has been replaced by
ElectionManager.  Heartbeats now reset the election timeout; /vote
drives the candidate protocol.
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
from rainman.node.config import get_peers, load_cluster_config
from rainman.node.election import ElectionManager
from rainman.node.storage import StorageEngine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

NODE_ID: str = os.getenv("NODE_ID", "node1")

# ---------------------------------------------------------------------------
# Mutable cluster state (mutated by election callbacks and endpoints)
# ---------------------------------------------------------------------------

_role: str = "follower"
_term: int = 0
_leader_id: str | None = None
_peers: list[dict] = []
_replication_cfg: dict = {}

storage: StorageEngine | None = None

# Serialises LSN generation + WAL write so concurrent requests cannot
# claim the same LSN (DDIA §5: leader assigns sequence numbers).
_write_lock = asyncio.Lock()

# Heartbeat sender task; set/cleared by _on_became_leader / _step_down.
_hb_task: asyncio.Task | None = None

# ElectionManager instance; created in lifespan.
_election_mgr: ElectionManager | None = None

# Artificial response delay injected by the adversary agent via
# POST /admin/inject_delay (DDIA §8: fault injection for testing).
_inject_delay_ms: int = 0


# ---------------------------------------------------------------------------
# Term helper (needed as a named function for the ElectionManager callback)
# ---------------------------------------------------------------------------


def _set_term_global(new_term: int) -> None:
    """Update the module-level _term.  Used as ElectionManager.set_term."""
    global _term
    _term = new_term


# ---------------------------------------------------------------------------
# Role transition helpers
# ---------------------------------------------------------------------------


async def _on_became_leader(new_term: int) -> None:
    """Transition this node to leader after winning an election.

    Updates role, term, and leader_id, then starts the heartbeat sender.
    If a heartbeat task is already running (shouldn't happen in normal
    operation), it is cancelled before the new one starts.
    Called from ElectionManager._run_election inside the asyncio loop.
    """
    global _role, _term, _leader_id, _hb_task
    _role = "leader"
    _term = new_term
    _leader_id = NODE_ID
    if _hb_task and not _hb_task.done():
        _hb_task.cancel()
        try:
            await _hb_task
        except asyncio.CancelledError:
            pass
    _hb_task = asyncio.create_task(_heartbeat_loop())
    logger.info("Node %s became leader for term %d", NODE_ID, new_term)


async def _step_down(new_term: int, new_leader_id: str | None) -> None:
    """Transition this node from leader to follower.

    Stops the heartbeat sender and updates role/term/leader_id.
    Safe to call when already a follower (just updates state, no task
    to cancel).  new_leader_id may be None when the new leader is not
    yet known from the message that triggered the step-down.
    DDIA §5: a node reverts to follower whenever it sees a higher term.
    """
    global _role, _term, _leader_id, _hb_task
    _role = "follower"
    _term = new_term
    _leader_id = new_leader_id
    if _hb_task and not _hb_task.done():
        _hb_task.cancel()
        try:
            await _hb_task
        except asyncio.CancelledError:
            pass
        _hb_task = None
    logger.info(
        "Node %s stepped down to follower for term %d (leader=%s)",
        NODE_ID,
        new_term,
        new_leader_id,
    )


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialise the node on startup and clean up on shutdown."""
    global storage, _role, _term, _leader_id, _peers, _replication_cfg
    global _election_mgr, _hb_task

    cfg = load_cluster_config()
    _peers = get_peers(cfg, NODE_ID)
    _replication_cfg = cfg["replication"]

    # Phase 3: all nodes start as followers; election determines leader.
    _role = "follower"
    _leader_id = None

    wal_path = os.getenv("WAL_PATH", "/data/wal.jsonl")
    wal_dir = os.path.dirname(os.path.abspath(wal_path))
    os.makedirs(wal_dir, exist_ok=True)
    storage = StorageEngine(wal_path)
    recovered_lsn = storage.replay()

    # Restore term from WAL so we never reuse a term we participated in.
    # DDIA §5: currentTerm must survive crashes (persisted via WAL here).
    _term = storage.highest_term()

    logger.info(
        "Node %s started as follower (term=%d). WAL replayed — LSN=%d",
        NODE_ID,
        _term,
        recovered_lsn,
    )

    _election_mgr = ElectionManager(
        node_id=NODE_ID,
        peers=_peers,
        replication_cfg=_replication_cfg,
        get_term=lambda: _term,
        set_term=_set_term_global,
        get_lsn=lambda: storage.current_lsn(),
        get_role=lambda: _role,
        on_became_leader=_on_became_leader,
    )
    _election_mgr.start()

    yield

    await _election_mgr.stop()
    if _hb_task and not _hb_task.done():
        _hb_task.cancel()
        try:
            await _hb_task
        except asyncio.CancelledError:
            pass


app = FastAPI(lifespan=lifespan)


# ---------------------------------------------------------------------------
# Background heartbeat sender (runs only while this node is leader)
# ---------------------------------------------------------------------------


async def _heartbeat_loop() -> None:
    """Send periodic heartbeats to all followers.

    Runs until cancelled (on step-down or shutdown).  A failed heartbeat
    to any individual follower is silently dropped — transient loss does
    not stop the loop.
    DDIA §5: heartbeats suppress follower election timeouts.
    """
    interval_s = _replication_cfg.get("heartbeat_interval_ms", 200) / 1000.0
    timeout_s = 0.1  # short timeout: heartbeats are latency-sensitive
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


class VoteRequest(BaseModel):
    """Request body for POST /vote."""

    candidate_id: str
    term: int
    candidate_lsn: int


class InjectDelayRequest(BaseModel):
    """Request body for POST /admin/inject_delay."""

    delay_ms: int


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

    # Simulate artificial replication lag for fault injection testing.
    # DDIA §8: injected delays expose timing-dependent failure modes.
    if _inject_delay_ms > 0:
        await asyncio.sleep(_inject_delay_ms / 1000.0)

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
    If the incoming term is higher and we are the leader, steps down.
    On success, persists the entry and returns ack.
    DDIA §5: follower replication path.
    """
    global _term

    if body.term < _term:
        return JSONResponse(
            status_code=409,
            content={"status": "term_mismatch", "current_term": _term},
        )

    # Simulate artificial replication lag for fault injection testing.
    if _inject_delay_ms > 0:
        await asyncio.sleep(_inject_delay_ms / 1000.0)

    async with _write_lock:
        if body.term > _term:
            if _role == "leader":
                # A higher-term leader has emerged; relinquish leadership
                # before applying the entry (DDIA §5: step down on higher term).
                await _step_down(body.term, None)
            else:
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
async def heartbeat(body: HeartbeatRequest):
    """Accept a heartbeat from the current leader.

    Updates the known leader_id and current term.  Resets the election
    timeout so this follower does not call a spurious election while a
    valid leader is alive.  If this node is the leader and a higher-term
    heartbeat arrives (split-brain resolution), it steps down.
    DDIA §5: follower liveness signaling from leader.
    """
    global _term, _leader_id

    prior_term = _term
    if body.term >= _term:
        _term = body.term
        _leader_id = body.leader_id

    # Reset election countdown whenever we hear from a valid leader.
    if _election_mgr is not None:
        _election_mgr.reset_timeout()

    # Step down if we discover a higher-term leader while we are leader.
    # DDIA §5: at most one leader per term; higher term wins.
    if _role == "leader" and body.term > prior_term:
        await _step_down(body.term, body.leader_id)

    return {"status": "ok"}


@app.post("/vote")
async def vote(body: VoteRequest):
    """Evaluate a vote request from a candidate.

    Delegates grant/deny logic to the ElectionManager, which enforces
    the three vote-grant conditions (term, voted_for, log completeness).
    If the candidate's term is higher than ours and we are the leader,
    steps down before responding.
    DDIA §5.3: vote grant conditions.
    """
    prior_term = _term
    vote_granted, current_term, reason = _election_mgr.handle_vote_request(
        body.candidate_id, body.term, body.candidate_lsn
    )

    # If the vote advanced our term and we are currently the leader,
    # we must step down — a higher term means our leadership is stale.
    if _role == "leader" and current_term > prior_term:
        await _step_down(current_term, None)

    if not vote_granted:
        return {
            "vote_granted": False,
            "term": current_term,
            "reason": reason,
        }
    return {"vote_granted": True, "term": current_term}


@app.post("/admin/inject_delay")
async def inject_delay(body: InjectDelayRequest):
    """Toggle artificial response delay on this node.

    Used by the adversary agent to simulate a slow or overloaded node.
    Set delay_ms to 0 to clear the delay.  The delay is applied to
    write and replication endpoints (PUT /kv, POST /replicate).
    DDIA §8: injecting delays is a class of fault distinct from crashes.
    """
    global _inject_delay_ms
    _inject_delay_ms = body.delay_ms
    logger.info(
        "Node %s inject_delay set to %d ms", NODE_ID, _inject_delay_ms
    )
    return {"status": "ok", "delay_ms": _inject_delay_ms}

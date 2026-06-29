"""Single-node FastAPI application for the Rainman key-value store.

Phase 1: storage only, no replication or election.  The node always
acts as leader and accepts all PUT requests directly.  Replication
stubs (POST /replicate, POST /heartbeat, POST /vote) are added in
Phase 2/3.
"""

import datetime
import logging
import os
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from rainman.node.storage import StorageEngine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# Node identity — Phase 1: always leader, term fixed at 1.
NODE_ID: str = os.getenv("NODE_ID", "node1")
_ROLE: str = "leader"
_TERM: int = 1

storage: StorageEngine | None = None

# Serialises LSN generation + WAL write so concurrent requests cannot
# claim the same LSN (DDIA §5: leader assigns sequence numbers).
_write_lock = threading.Lock()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialise storage on startup and replay WAL to restore state."""
    global storage
    wal_path = os.getenv("WAL_PATH", "/data/wal.jsonl")
    wal_dir = os.path.dirname(os.path.abspath(wal_path))
    os.makedirs(wal_dir, exist_ok=True)
    storage = StorageEngine(wal_path)
    recovered_lsn = storage.replay()
    logger.info(
        "Node %s started. WAL replayed — LSN = %d",
        NODE_ID,
        recovered_lsn,
    )
    yield


app = FastAPI(lifespan=lifespan)


class PutRequest(BaseModel):
    """Request body for PUT /kv/{key}."""

    value: dict


@app.get("/health")
def health():
    """Return node identity and current state.

    Polled by the recovery agent and scripts/verify_cluster.py.
    Never raises — always returns 200.
    """
    return {
        "node_id": NODE_ID,
        "role": _ROLE,
        "term": _TERM,
        "lsn": storage.current_lsn(),
        "leader_id": NODE_ID,
        "timestamp": (
            datetime.datetime.now(datetime.timezone.utc)
            .strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
            + "Z"
        ),
    }


@app.put("/kv/{key}")
def put_kv(key: str, body: PutRequest):
    """Write a value to the store.

    In Phase 1, always accepted (single-node, no follower rejection).
    Phase 2 adds follower redirect (409) and majority-ack before
    confirming to the client (DDIA §5 single-leader replication).
    Raises nothing visible beyond HTTP 500 on a storage error.
    """
    with _write_lock:
        lsn = storage.current_lsn() + 1
        storage.put(lsn, _TERM, key, body.value)
    return {"status": "ok", "lsn": lsn}


@app.get("/kv/{key}")
def get_kv(key: str):
    """Read a value from the local in-memory index.

    Any node accepts reads — no forwarding to leader.
    Returns 404 if the key has never been written.
    Never raises beyond HTTP 500 on a catastrophic error.
    """
    value = storage.get(key)
    if value is None:
        return JSONResponse(
            status_code=404, content={"status": "not_found"}
        )
    return {
        "key": key,
        "value": value,
        "lsn": storage.current_lsn(),
        "node_id": NODE_ID,
    }

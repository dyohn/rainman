"""Tests for Phase 2 replication: endpoints, fanout, follower rejection.

All tests run in-process against real StorageEngine instances — no
Docker or real HTTP servers required.  The fanout coroutine is
monkeypatched where needed so tests do not make outbound network calls.
"""

import asyncio
import json

import pytest
from fastapi.testclient import TestClient

import rainman.node.main as node_main
from rainman.node import replication as repl
from rainman.node.storage import StorageEngine


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def storage_engine(tmp_path) -> StorageEngine:
    """Fresh StorageEngine backed by a temp WAL file."""
    engine = StorageEngine(str(tmp_path / "wal.jsonl"))
    engine.replay()
    return engine


@pytest.fixture()
def _patch_node(storage_engine, monkeypatch):
    """Patch all module-level node globals that endpoints read.

    Returns (storage_engine, monkeypatch) so calling fixtures can
    further override _role or _peers.
    """
    monkeypatch.setattr(node_main, "storage", storage_engine)
    monkeypatch.setattr(node_main, "_term", 1)
    monkeypatch.setattr(node_main, "_peers", [])
    monkeypatch.setattr(
        node_main,
        "_replication_cfg",
        {"replication_timeout_ms": 500, "majority": 2},
    )
    monkeypatch.setattr(node_main, "_write_lock", asyncio.Lock())
    return storage_engine, monkeypatch


@pytest.fixture()
def leader_client(_patch_node, monkeypatch):
    """TestClient configured as the static leader (node1)."""
    monkeypatch.setattr(node_main, "_role", "leader")
    monkeypatch.setattr(node_main, "_leader_id", "node1")
    # Return client without entering lifespan context
    return TestClient(node_main.app, raise_server_exceptions=True)


@pytest.fixture()
def follower_client(_patch_node, monkeypatch):
    """TestClient configured as a follower (node2)."""
    monkeypatch.setattr(node_main, "_role", "follower")
    monkeypatch.setattr(node_main, "_leader_id", "node1")
    return TestClient(node_main.app, raise_server_exceptions=True)


# ---------------------------------------------------------------------------
# Health endpoint
# ---------------------------------------------------------------------------


def test_health_leader_returns_correct_role(leader_client):
    """GET /health on the leader returns role='leader'."""
    resp = leader_client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["role"] == "leader"
    assert data["leader_id"] == "node1"
    assert "lsn" in data
    assert "term" in data
    assert "timestamp" in data


def test_health_follower_returns_correct_role(follower_client):
    """GET /health on a follower returns role='follower'."""
    resp = follower_client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["role"] == "follower"


# ---------------------------------------------------------------------------
# Follower write rejection
# ---------------------------------------------------------------------------


def test_follower_rejects_put_with_409(follower_client):
    """A follower returns 409 not_leader for any PUT /kv request."""
    resp = follower_client.put(
        "/kv/some-key", json={"value": {"name": "Garaje"}}
    )
    assert resp.status_code == 409
    body = resp.json()
    assert body["status"] == "not_leader"
    assert body["leader_id"] == "node1"


def test_follower_reject_includes_leader_id(follower_client, monkeypatch):
    """The 409 body always contains the current leader_id."""
    monkeypatch.setattr(node_main, "_leader_id", "node2")
    resp = follower_client.put("/kv/x", json={"value": {}})
    assert resp.json()["leader_id"] == "node2"


# ---------------------------------------------------------------------------
# Leader write path — success
# ---------------------------------------------------------------------------


def test_leader_accepts_put_when_majority_acks(leader_client, monkeypatch):
    """Leader returns 200 and LSN=1 when fanout returns True."""

    async def mock_fanout(*_args, **_kwargs):
        return True

    monkeypatch.setattr(repl, "fanout_and_wait_majority", mock_fanout)

    resp = leader_client.put("/kv/biz1", json={"value": {"name": "Nopa"}})
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok", "lsn": 1}


def test_leader_put_increments_lsn(leader_client, monkeypatch):
    """Each successful PUT increments the LSN by 1."""

    async def mock_fanout(*_args, **_kwargs):
        return True

    monkeypatch.setattr(repl, "fanout_and_wait_majority", mock_fanout)

    r1 = leader_client.put("/kv/a", json={"value": {"v": 1}})
    r2 = leader_client.put("/kv/b", json={"value": {"v": 2}})
    assert r1.json()["lsn"] == 1
    assert r2.json()["lsn"] == 2


def test_leader_put_writes_to_storage(
    leader_client, storage_engine, monkeypatch
):
    """A successful leader PUT persists the value in the storage engine."""

    async def mock_fanout(*_args, **_kwargs):
        return True

    monkeypatch.setattr(repl, "fanout_and_wait_majority", mock_fanout)

    leader_client.put("/kv/biz2", json={"value": {"name": "Garaje"}})
    assert storage_engine.get("biz2") == {"name": "Garaje"}


# ---------------------------------------------------------------------------
# Leader write path — majority failure
# ---------------------------------------------------------------------------


def test_leader_returns_503_when_fanout_fails(leader_client, monkeypatch):
    """Leader returns 503 when fanout does not achieve majority."""

    async def mock_fanout(*_args, **_kwargs):
        return False

    monkeypatch.setattr(repl, "fanout_and_wait_majority", mock_fanout)

    resp = leader_client.put("/kv/biz3", json={"value": {"name": "Limon"}})
    assert resp.status_code == 503
    assert resp.json()["status"] == "majority_ack_failed"


def test_leader_wal_retains_entry_after_503(
    leader_client, storage_engine, monkeypatch
):
    """Even after a 503, the entry is present in the leader WAL."""

    async def mock_fanout(*_args, **_kwargs):
        return False

    monkeypatch.setattr(repl, "fanout_and_wait_majority", mock_fanout)

    leader_client.put("/kv/biz4", json={"value": {"name": "Nopa"}})
    # Entry was written to leader WAL before fanout was attempted
    assert storage_engine.get("biz4") == {"name": "Nopa"}


# ---------------------------------------------------------------------------
# POST /replicate — follower side
# ---------------------------------------------------------------------------


def test_replicate_valid_entry_returns_200(follower_client):
    """POST /replicate with correct LSN and term returns 200 and acks."""
    resp = follower_client.post(
        "/replicate",
        json={
            "lsn": 1,
            "term": 1,
            "op": "PUT",
            "key": "k1",
            "value": {"stars": 4.5},
        },
    )
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok", "lsn": 1}


def test_replicate_stores_entry_in_storage(follower_client, storage_engine):
    """A replicated entry is accessible via GET /kv after replication."""
    follower_client.post(
        "/replicate",
        json={
            "lsn": 1,
            "term": 1,
            "op": "PUT",
            "key": "biz5",
            "value": {"name": "Delfina"},
        },
    )
    assert storage_engine.get("biz5") == {"name": "Delfina"}


def test_replicate_sequential_entries(follower_client, storage_engine):
    """Follower correctly applies two consecutive LSNs."""
    for lsn, key in [(1, "a"), (2, "b")]:
        resp = follower_client.post(
            "/replicate",
            json={
                "lsn": lsn,
                "term": 1,
                "op": "PUT",
                "key": key,
                "value": {"n": lsn},
            },
        )
        assert resp.status_code == 200

    assert storage_engine.get("a") == {"n": 1}
    assert storage_engine.get("b") == {"n": 2}


def test_replicate_term_mismatch_returns_409(follower_client, monkeypatch):
    """POST /replicate with a stale term (term < current) returns 409."""
    monkeypatch.setattr(node_main, "_term", 5)
    resp = follower_client.post(
        "/replicate",
        json={
            "lsn": 1,
            "term": 2,
            "op": "PUT",
            "key": "k",
            "value": {},
        },
    )
    assert resp.status_code == 409
    body = resp.json()
    assert body["status"] == "term_mismatch"
    assert body["current_term"] == 5


def test_replicate_lsn_gap_returns_409(follower_client):
    """POST /replicate with a non-consecutive LSN returns 409 lsn_gap."""
    # Follower is at LSN 0; send LSN 5 → gap
    resp = follower_client.post(
        "/replicate",
        json={
            "lsn": 5,
            "term": 1,
            "op": "PUT",
            "key": "k",
            "value": {},
        },
    )
    assert resp.status_code == 409
    body = resp.json()
    assert body["status"] == "lsn_gap"
    assert body["expected"] == 1
    assert body["got"] == 5


def test_replicate_higher_term_accepted_and_updates_term(
    follower_client, monkeypatch
):
    """POST /replicate with a higher term is accepted and updates _term."""
    monkeypatch.setattr(node_main, "_term", 1)
    resp = follower_client.post(
        "/replicate",
        json={
            "lsn": 1,
            "term": 3,
            "op": "PUT",
            "key": "k",
            "value": {},
        },
    )
    assert resp.status_code == 200
    assert node_main._term == 3


# ---------------------------------------------------------------------------
# POST /heartbeat
# ---------------------------------------------------------------------------


def test_heartbeat_returns_200(follower_client):
    """POST /heartbeat always returns 200 with status ok."""
    resp = follower_client.post(
        "/heartbeat",
        json={"leader_id": "node1", "term": 1, "lsn": 10},
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_heartbeat_updates_leader_id(follower_client, monkeypatch):
    """A heartbeat from a new leader updates _leader_id."""
    monkeypatch.setattr(node_main, "_leader_id", "node1")
    monkeypatch.setattr(node_main, "_term", 1)
    follower_client.post(
        "/heartbeat",
        json={"leader_id": "node2", "term": 3, "lsn": 5},
    )
    assert node_main._leader_id == "node2"
    assert node_main._term == 3


def test_heartbeat_does_not_downgrade_term(follower_client, monkeypatch):
    """A heartbeat with a lower term does not overwrite current term."""
    monkeypatch.setattr(node_main, "_term", 10)
    monkeypatch.setattr(node_main, "_leader_id", "node1")
    follower_client.post(
        "/heartbeat",
        json={"leader_id": "node2", "term": 2, "lsn": 0},
    )
    assert node_main._term == 10  # unchanged
    assert node_main._leader_id == "node1"  # unchanged


# ---------------------------------------------------------------------------
# GET /kv — reads from any node
# ---------------------------------------------------------------------------


def test_get_kv_returns_stored_value(follower_client, storage_engine):
    """GET /kv/{key} reads from local storage regardless of role."""
    storage_engine.put(1, 1, "mykey", {"data": "value"})
    resp = follower_client.get("/kv/mykey")
    assert resp.status_code == 200
    data = resp.json()
    assert data["key"] == "mykey"
    assert data["value"] == {"data": "value"}


def test_get_kv_returns_404_for_absent_key(follower_client):
    """GET /kv/{key} returns 404 if the key does not exist."""
    resp = follower_client.get("/kv/no-such-key")
    assert resp.status_code == 404
    assert resp.json()["status"] == "not_found"


# ---------------------------------------------------------------------------
# fanout_and_wait_majority unit tests (no HTTP, asyncio.run)
# ---------------------------------------------------------------------------


def test_fanout_returns_true_when_no_followers_needed():
    """majority=1 means the leader alone satisfies quorum; no peers needed."""
    result = asyncio.run(
        repl.fanout_and_wait_majority(
            peers=[],
            entry={"lsn": 1, "term": 1, "op": "PUT", "key": "k", "value": {}},
            replication_timeout_ms=500,
            majority=1,
        )
    )
    assert result is True


def test_fanout_returns_false_when_no_peers_and_majority_2():
    """majority=2 requires 1 follower ack; empty peers list → False."""
    result = asyncio.run(
        repl.fanout_and_wait_majority(
            peers=[],
            entry={"lsn": 1, "term": 1, "op": "PUT", "key": "k", "value": {}},
            replication_timeout_ms=500,
            majority=2,
        )
    )
    assert result is False


# ---------------------------------------------------------------------------
# WAL persistence of replicated entries
# ---------------------------------------------------------------------------


def test_replicated_entries_survive_wal_replay(
    follower_client, storage_engine, tmp_path
):
    """Entries applied via /replicate are durably written and survive replay."""
    follower_client.post(
        "/replicate",
        json={
            "lsn": 1,
            "term": 1,
            "op": "PUT",
            "key": "durable",
            "value": {"city": "SF"},
        },
    )

    # Re-open the same WAL as a fresh engine and replay
    wal_path = storage_engine._wal_path
    fresh = StorageEngine(wal_path)
    fresh.replay()

    assert fresh.get("durable") == {"city": "SF"}
    assert fresh.current_lsn() == 1


def test_wal_file_contains_replicated_entry(follower_client, storage_engine):
    """Each /replicate call appends a readable JSON line to the WAL file."""
    follower_client.post(
        "/replicate",
        json={
            "lsn": 1,
            "term": 1,
            "op": "PUT",
            "key": "k",
            "value": {"n": 1},
        },
    )
    with open(storage_engine._wal_path) as f:
        lines = [json.loads(ln) for ln in f if ln.strip()]

    assert len(lines) == 1
    assert lines[0]["lsn"] == 1
    assert lines[0]["key"] == "k"

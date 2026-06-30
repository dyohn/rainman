"""Tests for Phase 3 leader election.

Covers:
  - ElectionManager.handle_vote_request() grant/deny conditions
  - POST /vote endpoint behaviour
  - POST /admin/inject_delay endpoint
  - Heartbeat integration with the election timeout reset
  - Role-transition helpers (_on_became_leader, _step_down)

All tests run in-process — no Docker or real inter-node HTTP calls.
ElectionManager unit tests use injected callbacks; HTTP tests use
FastAPI TestClient with monkeypatched module globals.
"""

import asyncio

import pytest
from fastapi.testclient import TestClient

import rainman.node.main as node_main
from rainman.node.election import ElectionManager
from rainman.node.storage import StorageEngine


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _make_mgr(
    node_id: str = "node2",
    term: int = 0,
    lsn: int = 0,
    role: str = "follower",
    peers: list[dict] | None = None,
    majority: int = 2,
):
    """Return (ElectionManager, state_dict) with mutable callback state.

    state_dict holds the shared mutable values that the callbacks read
    and write, so tests can inspect and mutate them directly.
    """
    state: dict = {
        "term": term,
        "lsn": lsn,
        "role": role,
        "became_leader_term": None,
    }

    async def on_became_leader(new_term: int) -> None:
        state["role"] = "leader"
        state["became_leader_term"] = new_term

    mgr = ElectionManager(
        node_id=node_id,
        peers=peers or [],
        replication_cfg={
            "heartbeat_interval_ms": 200,
            "election_timeout_min_ms": 600,
            "election_timeout_max_ms": 1000,
            "vote_timeout_ms": 300,
            "majority": majority,
        },
        get_term=lambda: state["term"],
        set_term=lambda t: state.__setitem__("term", t),
        get_lsn=lambda: state["lsn"],
        get_role=lambda: state["role"],
        on_became_leader=on_became_leader,
    )
    return mgr, state


# ---------------------------------------------------------------------------
# ElectionManager.handle_vote_request() — grant conditions
# ---------------------------------------------------------------------------


def test_vote_granted_when_all_conditions_met():
    """Vote is granted when term, voted_for, and log conditions all hold."""
    mgr, state = _make_mgr(term=0, lsn=0)
    granted, term, reason = mgr.handle_vote_request("node2", 1, 0)
    assert granted is True
    assert term == 1
    assert reason is None


def test_vote_grant_advances_our_term():
    """Granting a vote for a higher term advances our local term."""
    mgr, state = _make_mgr(term=1, lsn=0)
    granted, term, _ = mgr.handle_vote_request("node2", 5, 0)
    assert granted is True
    assert term == 5
    assert state["term"] == 5


def test_vote_denied_stale_term():
    """Candidate term below current term is rejected as stale."""
    mgr, state = _make_mgr(term=5)
    granted, term, reason = mgr.handle_vote_request("node2", 3, 100)
    assert granted is False
    assert reason == "stale_term"
    assert term == 5  # our term is unchanged and returned


def test_vote_denied_already_voted_different_candidate():
    """Second vote request in the same term from a different candidate is denied."""
    mgr, _ = _make_mgr(term=1)
    mgr.handle_vote_request("node2", 2, 0)  # vote for node2 in term 2
    granted, _, reason = mgr.handle_vote_request("node3", 2, 0)
    assert granted is False
    assert reason == "already_voted"


def test_vote_idempotent_same_candidate():
    """Repeated vote request from the same candidate in the same term is granted."""
    mgr, _ = _make_mgr(term=0)
    granted1, _, _ = mgr.handle_vote_request("node2", 1, 0)
    granted2, _, _ = mgr.handle_vote_request("node2", 1, 0)
    assert granted1 is True
    assert granted2 is True


def test_vote_denied_log_not_current():
    """Candidate with fewer entries than us is denied to protect completeness."""
    mgr, _ = _make_mgr(term=0, lsn=10)
    granted, _, reason = mgr.handle_vote_request("node2", 1, 5)
    assert granted is False
    assert reason == "log_not_current"


def test_vote_granted_candidate_lsn_equal():
    """Candidate with exactly our LSN meets the log-completeness condition."""
    mgr, _ = _make_mgr(term=0, lsn=10)
    granted, _, _ = mgr.handle_vote_request("node2", 1, 10)
    assert granted is True


def test_vote_granted_candidate_lsn_ahead():
    """Candidate with a higher LSN than ours is granted."""
    mgr, _ = _make_mgr(term=0, lsn=5)
    granted, _, _ = mgr.handle_vote_request("node2", 1, 20)
    assert granted is True


def test_vote_records_voted_for():
    """After granting a vote, voted_for[term] is recorded."""
    mgr, _ = _make_mgr(term=0)
    mgr.handle_vote_request("node2", 1, 0)
    assert mgr._voted_for.get(1) == "node2"


def test_vote_different_terms_independent():
    """Voted-for state is tracked independently per term."""
    mgr, _ = _make_mgr(term=0)
    # Vote for node2 in term 1
    granted1, _, _ = mgr.handle_vote_request("node2", 1, 0)
    # Vote for node3 in term 2 (new term — different candidate allowed)
    granted2, _, _ = mgr.handle_vote_request("node3", 2, 0)
    assert granted1 is True
    assert granted2 is True
    assert mgr._voted_for[1] == "node2"
    assert mgr._voted_for[2] == "node3"


def test_vote_grant_sets_heartbeat_event():
    """Granting a vote sets the heartbeat event to reset the election timeout."""
    mgr, _ = _make_mgr(term=0)
    mgr.handle_vote_request("node2", 1, 0)
    assert mgr._heartbeat_event.is_set()


def test_vote_deny_does_not_set_heartbeat_event():
    """A denied vote (stale term) does not reset the election timeout."""
    mgr, _ = _make_mgr(term=5)
    mgr.handle_vote_request("node2", 3, 0)
    assert not mgr._heartbeat_event.is_set()


# ---------------------------------------------------------------------------
# ElectionManager.reset_timeout()
# ---------------------------------------------------------------------------


def test_reset_timeout_sets_event():
    """reset_timeout() sets the heartbeat event."""
    mgr, _ = _make_mgr()
    assert not mgr._heartbeat_event.is_set()
    mgr.reset_timeout()
    assert mgr._heartbeat_event.is_set()


# ---------------------------------------------------------------------------
# Fixtures for HTTP endpoint tests
# ---------------------------------------------------------------------------


@pytest.fixture()
def storage_engine(tmp_path) -> StorageEngine:
    """Fresh StorageEngine backed by a temp WAL file."""
    engine = StorageEngine(str(tmp_path / "wal.jsonl"))
    engine.replay()
    return engine


@pytest.fixture()
def _patch_node(storage_engine, monkeypatch):
    """Patch all module-level node globals that endpoints read."""
    monkeypatch.setattr(node_main, "storage", storage_engine)
    monkeypatch.setattr(node_main, "_term", 1)
    monkeypatch.setattr(node_main, "_role", "follower")
    monkeypatch.setattr(node_main, "_leader_id", "node1")
    monkeypatch.setattr(node_main, "_peers", [])
    monkeypatch.setattr(
        node_main,
        "_replication_cfg",
        {"replication_timeout_ms": 500, "majority": 2},
    )
    monkeypatch.setattr(node_main, "_write_lock", asyncio.Lock())
    monkeypatch.setattr(node_main, "_inject_delay_ms", 0)
    monkeypatch.setattr(node_main, "_hb_task", None)

    # Create a real ElectionManager wired to the module globals.
    mgr = ElectionManager(
        node_id="node1",
        peers=[],
        replication_cfg={
            "election_timeout_min_ms": 600,
            "election_timeout_max_ms": 1000,
            "vote_timeout_ms": 300,
            "majority": 2,
        },
        get_term=lambda: node_main._term,
        set_term=node_main._set_term_global,
        get_lsn=lambda: storage_engine.current_lsn(),
        get_role=lambda: node_main._role,
        on_became_leader=node_main._on_became_leader,
    )
    monkeypatch.setattr(node_main, "_election_mgr", mgr)
    return storage_engine, mgr, monkeypatch


@pytest.fixture()
def follower_client(_patch_node):
    """TestClient configured as a follower."""
    return TestClient(node_main.app, raise_server_exceptions=True)


@pytest.fixture()
def leader_client(_patch_node, monkeypatch):
    """TestClient configured as a leader."""
    monkeypatch.setattr(node_main, "_role", "leader")
    monkeypatch.setattr(node_main, "_leader_id", "node1")
    return TestClient(node_main.app, raise_server_exceptions=True)


# ---------------------------------------------------------------------------
# POST /vote endpoint
# ---------------------------------------------------------------------------


def test_vote_endpoint_grants_vote(follower_client):
    """POST /vote returns vote_granted=true when conditions are met."""
    resp = follower_client.post(
        "/vote",
        json={"candidate_id": "node2", "term": 2, "candidate_lsn": 0},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["vote_granted"] is True
    assert data["term"] == 2


def test_vote_endpoint_denies_stale_term(follower_client, monkeypatch):
    """POST /vote returns vote_granted=false with reason for a stale term."""
    monkeypatch.setattr(node_main, "_term", 10)
    resp = follower_client.post(
        "/vote",
        json={"candidate_id": "node2", "term": 3, "candidate_lsn": 0},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["vote_granted"] is False
    assert data["reason"] == "stale_term"
    assert data["term"] == 10


def test_vote_endpoint_denies_already_voted(follower_client):
    """Second vote request for different candidate in same term is denied."""
    # First vote for node2 in term 2
    follower_client.post(
        "/vote",
        json={"candidate_id": "node2", "term": 2, "candidate_lsn": 0},
    )
    # Second attempt from node3 in same term
    resp = follower_client.post(
        "/vote",
        json={"candidate_id": "node3", "term": 2, "candidate_lsn": 0},
    )
    data = resp.json()
    assert data["vote_granted"] is False
    assert data["reason"] == "already_voted"


def test_vote_endpoint_denies_log_not_current(
    follower_client, storage_engine
):
    """Candidate with fewer WAL entries than us is denied."""
    storage_engine.put(1, 1, "k", {"v": 1})  # our LSN is now 1
    resp = follower_client.post(
        "/vote",
        json={"candidate_id": "node2", "term": 2, "candidate_lsn": 0},
    )
    data = resp.json()
    assert data["vote_granted"] is False
    assert data["reason"] == "log_not_current"


def test_vote_endpoint_updates_term(follower_client, monkeypatch):
    """POST /vote with a higher term updates the module-level _term."""
    monkeypatch.setattr(node_main, "_term", 1)
    follower_client.post(
        "/vote",
        json={"candidate_id": "node2", "term": 5, "candidate_lsn": 0},
    )
    assert node_main._term == 5


def test_vote_endpoint_leader_steps_down_on_higher_term(
    leader_client, monkeypatch
):
    """A leader that receives a higher-term vote request steps down."""
    monkeypatch.setattr(node_main, "_term", 2)
    monkeypatch.setattr(node_main, "_role", "leader")

    resp = leader_client.post(
        "/vote",
        json={"candidate_id": "node2", "term": 5, "candidate_lsn": 0},
    )
    assert resp.status_code == 200
    assert node_main._role == "follower"


def test_vote_response_includes_reason_on_deny(follower_client):
    """Denied vote response always includes a 'reason' field."""
    resp = follower_client.post(
        "/vote",
        json={"candidate_id": "node2", "term": 2, "candidate_lsn": 0},
    )
    assert resp.json()["vote_granted"] is True  # sanity: first vote granted
    resp2 = follower_client.post(
        "/vote",
        json={"candidate_id": "node3", "term": 2, "candidate_lsn": 0},
    )
    body = resp2.json()
    assert body["vote_granted"] is False
    assert "reason" in body


# ---------------------------------------------------------------------------
# POST /admin/inject_delay endpoint
# ---------------------------------------------------------------------------


def test_inject_delay_sets_delay(follower_client):
    """POST /admin/inject_delay sets _inject_delay_ms and echoes it."""
    resp = follower_client.post(
        "/admin/inject_delay", json={"delay_ms": 200}
    )
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok", "delay_ms": 200}
    assert node_main._inject_delay_ms == 200


def test_inject_delay_clears_with_zero(follower_client):
    """Setting delay_ms=0 clears the injected delay."""
    follower_client.post("/admin/inject_delay", json={"delay_ms": 500})
    resp = follower_client.post(
        "/admin/inject_delay", json={"delay_ms": 0}
    )
    assert resp.json()["delay_ms"] == 0
    assert node_main._inject_delay_ms == 0


# ---------------------------------------------------------------------------
# POST /heartbeat — Phase 3 integration
# ---------------------------------------------------------------------------


def test_heartbeat_resets_election_timeout(follower_client, _patch_node):
    """POST /heartbeat calls election_mgr.reset_timeout()."""
    _, mgr, _ = _patch_node
    assert not mgr._heartbeat_event.is_set()
    follower_client.post(
        "/heartbeat",
        json={"leader_id": "node1", "term": 1, "lsn": 0},
    )
    assert mgr._heartbeat_event.is_set()


def test_heartbeat_leader_steps_down_on_higher_term(
    leader_client, monkeypatch
):
    """A leader that receives a heartbeat with a higher term steps down."""
    monkeypatch.setattr(node_main, "_term", 2)
    monkeypatch.setattr(node_main, "_role", "leader")

    leader_client.post(
        "/heartbeat",
        json={"leader_id": "node2", "term": 5, "lsn": 0},
    )
    assert node_main._role == "follower"
    assert node_main._term == 5
    assert node_main._leader_id == "node2"


def test_heartbeat_does_not_step_down_same_term(leader_client, monkeypatch):
    """A heartbeat at the same term as the current leader does not step down."""
    monkeypatch.setattr(node_main, "_term", 3)
    monkeypatch.setattr(node_main, "_role", "leader")

    leader_client.post(
        "/heartbeat",
        json={"leader_id": "node1", "term": 3, "lsn": 0},
    )
    # Still leader — same term heartbeat is just an ack
    assert node_main._role == "leader"


# ---------------------------------------------------------------------------
# _on_became_leader and _step_down (async helpers)
# ---------------------------------------------------------------------------


def test_on_became_leader_sets_state(monkeypatch):
    """_on_became_leader sets role='leader', updates term and leader_id."""

    class _FakeStorage:
        def current_lsn(self):
            return 0

    monkeypatch.setattr(node_main, "_role", "follower")
    monkeypatch.setattr(node_main, "_term", 1)
    monkeypatch.setattr(node_main, "_leader_id", None)
    monkeypatch.setattr(node_main, "_hb_task", None)
    monkeypatch.setattr(node_main, "NODE_ID", "node1")
    monkeypatch.setattr(
        node_main, "_replication_cfg", {"heartbeat_interval_ms": 200}
    )
    monkeypatch.setattr(node_main, "storage", _FakeStorage())
    monkeypatch.setattr(node_main, "_peers", [])

    async def _run():
        await node_main._on_became_leader(3)
        # Clean up the background heartbeat task before exiting the loop
        task = node_main._hb_task
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    asyncio.run(_run())

    assert node_main._role == "leader"
    assert node_main._term == 3
    assert node_main._leader_id == "node1"


def test_step_down_sets_state(monkeypatch):
    """_step_down sets role='follower', updates term and leader_id."""
    monkeypatch.setattr(node_main, "_role", "leader")
    monkeypatch.setattr(node_main, "_term", 5)
    monkeypatch.setattr(node_main, "_leader_id", "node1")
    monkeypatch.setattr(node_main, "_hb_task", None)

    asyncio.run(node_main._step_down(7, "node2"))

    assert node_main._role == "follower"
    assert node_main._term == 7
    assert node_main._leader_id == "node2"


def test_step_down_cancels_heartbeat_task(monkeypatch):
    """_step_down cancels the heartbeat task if one is running."""
    monkeypatch.setattr(node_main, "_role", "leader")
    monkeypatch.setattr(node_main, "_term", 3)
    monkeypatch.setattr(node_main, "_leader_id", "node1")

    async def _run():
        async def _dummy():
            await asyncio.sleep(9999)

        task = asyncio.create_task(_dummy())
        monkeypatch.setattr(node_main, "_hb_task", task)
        await node_main._step_down(5, None)
        return task

    task = asyncio.run(_run())

    assert task.cancelled()
    assert node_main._hb_task is None


# ---------------------------------------------------------------------------
# StorageEngine.highest_term() — regression for WAL replay
# ---------------------------------------------------------------------------


def test_storage_highest_term_empty_wal(tmp_path):
    """highest_term() returns 0 for an empty WAL."""
    engine = StorageEngine(str(tmp_path / "wal.jsonl"))
    engine.replay()
    assert engine.highest_term() == 0


def test_storage_highest_term_after_writes(tmp_path):
    """highest_term() returns the largest term written across all entries."""
    engine = StorageEngine(str(tmp_path / "wal.jsonl"))
    engine.replay()
    engine.put(1, 1, "a", {})
    engine.put(2, 3, "b", {})
    engine.put(3, 2, "c", {})

    fresh = StorageEngine(str(tmp_path / "wal.jsonl"))
    fresh.replay()
    assert fresh.highest_term() == 3


def test_storage_highest_term_single_entry(tmp_path):
    """highest_term() works with a single WAL entry."""
    engine = StorageEngine(str(tmp_path / "wal.jsonl"))
    engine.replay()
    engine.put(1, 7, "k", {"v": 1})

    fresh = StorageEngine(str(tmp_path / "wal.jsonl"))
    fresh.replay()
    assert fresh.highest_term() == 7

"""Unit tests for Rainman agent modules.

Covers the pure, deterministic functions in both agents:
- detect_anomalies() from recovery_agent
- _is_safe_to_inject() and _parse_decision() from adversary_agent

No HTTP, no Docker, no asyncio, no Ollama — plain dicts only.
"""

from rainman.agents.adversary_agent import (
    _is_safe_to_inject,
    _parse_decision,
)
from rainman.agents.recovery_agent import detect_anomalies

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

VALID_ACTIONS = {
    "KILL_NODE",
    "RESTART_NODE",
    "PAUSE_NODE",
    "RESUME_NODE",
    "PARTITION_NODE",
    "HEAL_PARTITION",
    "INJECT_DELAY",
    "CLEAR_DELAY",
    "CORRUPT_WAL",
    "no_action",
}


def _snap(
    node_id: str,
    *,
    reachable: bool = True,
    role: str = "follower",
    term: int = 1,
    lsn: int = 10,
    leader_id: str | None = None,
) -> dict:
    """Build a NodeSnapshot dict for use in tests."""
    return {
        "node_id": node_id,
        "reachable": reachable,
        "role": role if reachable else None,
        "term": term if reachable else None,
        "lsn": lsn if reachable else None,
        "leader_id": leader_id,
        "timestamp": "2026-07-01T00:00:00.000Z",
    }


def _healthy(leader_lsn: int = 100) -> list[dict]:
    """Return a healthy 3-node cluster snapshot."""
    return [
        _snap(
            "node1",
            role="leader",
            term=2,
            lsn=leader_lsn,
            leader_id="node1",
        ),
        _snap(
            "node2",
            role="follower",
            term=2,
            lsn=leader_lsn,
            leader_id="node1",
        ),
        _snap(
            "node3",
            role="follower",
            term=2,
            lsn=leader_lsn,
            leader_id="node1",
        ),
    ]


# ---------------------------------------------------------------------------
# Observer: detect_anomalies()
# ---------------------------------------------------------------------------


def test_detect_no_anomaly():
    assert detect_anomalies(_healthy()) == []


def test_detect_node_unreachable():
    snaps = [
        _snap("node1", role="leader", term=2, lsn=100),
        _snap("node2", role="follower", term=2, lsn=100),
        _snap("node3", reachable=False),
    ]
    result = detect_anomalies(snaps)
    types = [a["type"] for a in result]
    assert "node_unreachable" in types
    detail = next(
        a["detail"] for a in result if a["type"] == "node_unreachable"
    )
    assert detail["node_id"] == "node3"


def test_detect_no_leader():
    snaps = [
        _snap("node1", role="follower", term=2, lsn=100),
        _snap("node2", role="follower", term=2, lsn=100),
        _snap("node3", role="follower", term=2, lsn=100),
    ]
    result = detect_anomalies(snaps)
    assert any(a["type"] == "no_leader" for a in result)


def test_detect_split_brain():
    snaps = [
        _snap("node1", role="leader", term=2, lsn=100),
        _snap("node2", role="leader", term=2, lsn=100),
        _snap("node3", role="follower", term=2, lsn=100),
    ]
    result = detect_anomalies(snaps)
    assert any(a["type"] == "split_brain" for a in result)
    detail = next(
        a["detail"] for a in result if a["type"] == "split_brain"
    )
    assert set(detail["leaders"]) == {"node1", "node2"}
    assert detail["term"] == 2


def test_detect_replication_lag():
    snaps = [
        _snap("node1", role="leader", term=2, lsn=100),
        _snap("node2", role="follower", term=2, lsn=85),  # lag 15
        _snap("node3", role="follower", term=2, lsn=100),
    ]
    result = detect_anomalies(snaps, lag_threshold=10)
    assert any(a["type"] == "replication_lag" for a in result)
    detail = next(
        a["detail"] for a in result if a["type"] == "replication_lag"
    )
    assert detail["node_id"] == "node2"
    assert detail["lag"] == 15


def test_detect_lag_below_threshold():
    snaps = [
        _snap("node1", role="leader", term=2, lsn=100),
        _snap("node2", role="follower", term=2, lsn=95),  # lag 5
        _snap("node3", role="follower", term=2, lsn=100),
    ]
    assert detect_anomalies(snaps, lag_threshold=10) == []


def test_detect_multiple_anomalies():
    snaps = [
        _snap("node1", reachable=False),
        _snap("node2", role="follower", term=2, lsn=100),
        _snap("node3", role="follower", term=2, lsn=100),
    ]
    result = detect_anomalies(snaps)
    types = [a["type"] for a in result]
    assert "node_unreachable" in types
    assert "no_leader" in types


# ---------------------------------------------------------------------------
# Adversary: _is_safe_to_inject()
# ---------------------------------------------------------------------------


def test_is_safe_no_leader():
    snaps = [
        _snap("node1", role="follower", term=2, lsn=100),
        _snap("node2", role="follower", term=2, lsn=100),
        _snap("node3", role="follower", term=2, lsn=100),
    ]
    assert _is_safe_to_inject(snaps) is False


def test_is_safe_minority():
    snaps = [
        _snap("node1", role="leader", term=2, lsn=100),
        _snap("node2", reachable=False),
        _snap("node3", reachable=False),
    ]
    assert _is_safe_to_inject(snaps) is False


def test_is_safe_healthy():
    assert _is_safe_to_inject(_healthy()) is True


# ---------------------------------------------------------------------------
# Adversary: _parse_decision()
# ---------------------------------------------------------------------------


def test_parse_decision_valid():
    raw = (
        '{"action": "KILL_NODE", "target": "node2",'
        ' "rationale": "node2 is a follower"}'
    )
    result = _parse_decision(raw, VALID_ACTIONS)
    assert result["action"] == "KILL_NODE"
    assert result["target"] == "node2"
    assert "node2" in result["rationale"]


def test_parse_decision_unknown_action():
    raw = (
        '{"action": "NUKE_EVERYTHING", "target": "node1",'
        ' "rationale": "chaos"}'
    )
    result = _parse_decision(raw, VALID_ACTIONS)
    assert result["action"] == "no_action"


def test_parse_decision_invalid_json():
    result = _parse_decision("not json }{", VALID_ACTIONS)
    assert result["action"] == "no_action"


def test_parse_decision_wrong_target():
    raw = (
        '{"action": "KILL_NODE", "target": "node9",'
        ' "rationale": "wrong"}'
    )
    result = _parse_decision(raw, VALID_ACTIONS)
    assert result["action"] == "no_action"


def test_parse_decision_no_action():
    raw = (
        '{"action": "no_action", "target": null,'
        ' "rationale": "cluster looks good"}'
    )
    result = _parse_decision(raw, VALID_ACTIONS)
    assert result["action"] == "no_action"
    assert result["target"] is None

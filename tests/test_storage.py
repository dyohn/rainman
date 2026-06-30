"""Tests for StorageEngine: WAL write, replay, fsync, and corruption.

These tests run against a real filesystem using pytest's tmp_path
fixture — no mocking of storage primitives.
"""

import json
import os

from rainman.node.storage import StorageEngine


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_engine(tmp_path, filename="wal.jsonl") -> StorageEngine:
    """Return a fresh StorageEngine backed by a temp WAL file."""
    return StorageEngine(str(tmp_path / filename))


def write_raw_wal(path, lines: list[str]) -> None:
    """Write raw text lines to a WAL file, bypassing StorageEngine."""
    with open(path, "w") as f:
        for line in lines:
            f.write(line + "\n")


def good_entry(lsn: int, key: str = "k", value: dict | None = None):
    """Return a valid WAL JSON line string."""
    return json.dumps(
        {
            "lsn": lsn,
            "term": 1,
            "op": "PUT",
            "key": key,
            "value": value or {"v": lsn},
        }
    )


# ---------------------------------------------------------------------------
# Basic put / get
# ---------------------------------------------------------------------------


def test_put_and_get(tmp_path):
    """A value written with put() is immediately readable via get()."""
    engine = make_engine(tmp_path)
    engine.put(1, 1, "key1", {"name": "Alice"})
    assert engine.get("key1") == {"name": "Alice"}


def test_get_absent_key_returns_none(tmp_path):
    """get() returns None for a key that has never been written."""
    engine = make_engine(tmp_path)
    assert engine.get("no_such_key") is None


def test_put_overwrites_existing_key(tmp_path):
    """A second put() to the same key replaces the previous value."""
    engine = make_engine(tmp_path)
    engine.put(1, 1, "k", {"v": "first"})
    engine.put(2, 1, "k", {"v": "second"})
    assert engine.get("k") == {"v": "second"}


# ---------------------------------------------------------------------------
# LSN tracking
# ---------------------------------------------------------------------------


def test_lsn_starts_at_zero(tmp_path):
    """current_lsn() is 0 before any writes."""
    engine = make_engine(tmp_path)
    assert engine.current_lsn() == 0


def test_lsn_tracks_latest_put(tmp_path):
    """current_lsn() reflects the LSN passed to the most recent put()."""
    engine = make_engine(tmp_path)
    engine.put(1, 1, "a", {})
    assert engine.current_lsn() == 1
    engine.put(7, 1, "b", {})
    assert engine.current_lsn() == 7


# ---------------------------------------------------------------------------
# WAL replay
# ---------------------------------------------------------------------------


def test_replay_restores_state(tmp_path):
    """A new StorageEngine opened on an existing WAL recovers all data."""
    wal = str(tmp_path / "wal.jsonl")
    e1 = StorageEngine(wal)
    e1.put(1, 1, "k1", {"city": "SF"})
    e1.put(2, 1, "k2", {"city": "NY"})

    e2 = StorageEngine(wal)
    e2.replay()

    assert e2.get("k1") == {"city": "SF"}
    assert e2.get("k2") == {"city": "NY"}
    assert e2.current_lsn() == 2


def test_replay_returns_highest_lsn(tmp_path):
    """replay() returns the highest LSN seen in the WAL."""
    wal = str(tmp_path / "wal.jsonl")
    write_raw_wal(wal, [good_entry(5), good_entry(3), good_entry(9)])

    engine = StorageEngine(wal)
    result = engine.replay()

    assert result == 9
    assert engine.current_lsn() == 9


def test_replay_empty_wal_returns_zero(tmp_path):
    """replay() on an empty WAL returns 0 and leaves index empty."""
    engine = make_engine(tmp_path)
    assert engine.replay() == 0
    assert engine.snapshot() == {}


# ---------------------------------------------------------------------------
# Malformed line handling
# ---------------------------------------------------------------------------


def test_replay_skips_malformed_json(tmp_path):
    """Malformed JSON lines are skipped without raising an exception."""
    wal = str(tmp_path / "wal.jsonl")
    write_raw_wal(
        wal,
        [
            good_entry(1, "k1"),
            "NOT VALID JSON",
            good_entry(3, "k3"),
        ],
    )
    engine = StorageEngine(wal)
    engine.replay()

    assert engine.get("k1") == {"v": 1}
    assert engine.get("k3") == {"v": 3}
    assert engine.current_lsn() == 3


def test_replay_skips_entry_missing_fields(tmp_path):
    """WAL entries missing required fields are skipped gracefully."""
    wal = str(tmp_path / "wal.jsonl")
    write_raw_wal(
        wal,
        [
            good_entry(1, "k1"),
            json.dumps({"lsn": 2}),  # missing key/value
            good_entry(3, "k3"),
        ],
    )
    engine = StorageEngine(wal)
    engine.replay()

    assert engine.get("k1") == {"v": 1}
    assert engine.get("k3") == {"v": 3}


def test_replay_skips_blank_lines(tmp_path):
    """Blank lines in the WAL are silently ignored."""
    wal = str(tmp_path / "wal.jsonl")
    write_raw_wal(wal, [good_entry(1, "k1"), "", "   ", good_entry(2, "k2")])

    engine = StorageEngine(wal)
    engine.replay()

    assert engine.get("k1") == {"v": 1}
    assert engine.get("k2") == {"v": 2}


# ---------------------------------------------------------------------------
# fsync
# ---------------------------------------------------------------------------


def test_fsync_called_on_every_put(tmp_path, monkeypatch):
    """os.fsync() is called exactly once per put() call."""
    calls: list[int] = []
    real_fsync = os.fsync

    def tracking_fsync(fd: int) -> None:
        calls.append(fd)
        real_fsync(fd)

    monkeypatch.setattr(os, "fsync", tracking_fsync)

    engine = make_engine(tmp_path)
    engine.put(1, 1, "a", {})
    engine.put(2, 1, "b", {})

    assert len(calls) == 2


# ---------------------------------------------------------------------------
# Snapshot
# ---------------------------------------------------------------------------


def test_snapshot_returns_full_copy(tmp_path):
    """snapshot() returns all keys present in the index."""
    engine = make_engine(tmp_path)
    engine.put(1, 1, "x", {"n": 1})
    engine.put(2, 1, "y", {"n": 2})

    snap = engine.snapshot()
    assert snap == {"x": {"n": 1}, "y": {"n": 2}}


def test_snapshot_is_independent_of_engine(tmp_path):
    """Mutating the snapshot dict does not affect the StorageEngine."""
    engine = make_engine(tmp_path)
    engine.put(1, 1, "k", {"v": "original"})

    snap = engine.snapshot()
    snap["k"] = {"v": "mutated"}
    snap["new_key"] = {}

    assert engine.get("k") == {"v": "original"}
    assert engine.get("new_key") is None


# ---------------------------------------------------------------------------
# WAL persistence (data survives to disk)
# ---------------------------------------------------------------------------


def test_wal_file_contains_written_entries(tmp_path):
    """Every put() appends a readable JSON line to the WAL file."""
    wal = str(tmp_path / "wal.jsonl")
    engine = StorageEngine(wal)
    engine.put(1, 1, "biz1", {"name": "Garaje"})
    engine.put(2, 1, "biz2", {"name": "Nopa"})

    with open(wal) as f:
        lines = [json.loads(ln) for ln in f if ln.strip()]

    assert len(lines) == 2
    assert lines[0]["key"] == "biz1"
    assert lines[1]["lsn"] == 2

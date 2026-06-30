"""WAL-backed key-value storage engine.

DDIA Chapter 3 — Storage and Retrieval: append-only log with an
in-memory hash index.  The WAL is the source of truth; the dict is
always reconstructed from it on startup.
"""

import json
import logging
import os
import threading

logger = logging.getLogger(__name__)


class StorageEngine:
    """Append-only WAL + in-memory dict key-value store.

    Raises OSError if the WAL file cannot be opened, read, or written.
    Thread-safe for concurrent reads and writes via an internal lock.
    """

    def __init__(self, wal_path: str) -> None:
        """Open (or create) the WAL file at wal_path.

        The parent directory of wal_path must already exist.
        Raises OSError if the file cannot be opened for appending.
        """
        self._wal_path = wal_path
        self._index: dict[str, dict] = {}
        self._lsn: int = 0
        self._highest_term: int = 0
        self._lock = threading.Lock()
        self._wal = open(wal_path, "a")  # creates file if absent

    def replay(self) -> int:
        """Rebuild the in-memory index by replaying all WAL entries.

        Skips and logs malformed lines without crashing — tolerates
        partial writes left by a prior crash (DDIA §3 crash recovery).
        Returns the highest LSN seen; 0 if the WAL is empty.
        Raises OSError if the WAL file cannot be read.
        """
        highest_lsn = 0
        with open(self._wal_path, "r") as f:
            for lineno, raw in enumerate(f, start=1):
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    entry = json.loads(raw)
                    lsn = int(entry["lsn"])
                    term = int(entry.get("term", 0))
                    self._index[entry["key"]] = entry["value"]
                    if lsn > highest_lsn:
                        highest_lsn = lsn
                    if term > self._highest_term:
                        self._highest_term = term
                except (KeyError, ValueError, json.JSONDecodeError) as exc:
                    logger.warning(
                        "WAL line %d skipped (malformed): %s — %r",
                        lineno,
                        exc,
                        raw[:120],
                    )
        self._lsn = highest_lsn
        return highest_lsn

    def put(self, lsn: int, term: int, key: str, value: dict) -> None:
        """Append a PUT entry to the WAL, fsync, then update the index.

        The WAL record is persisted before the dict is updated so the
        entry survives a crash between the two steps (DDIA §3
        write-ahead property).
        Raises OSError if the WAL cannot be written or fsynced.
        """
        entry = {
            "lsn": lsn,
            "term": term,
            "op": "PUT",
            "key": key,
            "value": value,
        }
        with self._lock:
            self._wal.write(json.dumps(entry) + "\n")
            self._wal.flush()
            os.fsync(self._wal.fileno())
            self._index[key] = value
            self._lsn = lsn

    def get(self, key: str) -> dict | None:
        """Return the value for key, or None if the key is absent.

        Never raises.
        """
        return self._index.get(key)

    def current_lsn(self) -> int:
        """Return the highest LSN written so far; 0 if no entries exist.

        Never raises.
        """
        return self._lsn

    def highest_term(self) -> int:
        """Return the highest term seen across all replayed WAL entries.

        Returns 0 if the WAL is empty or contains no term fields.
        Used on startup to restore term continuity after a crash so the
        node never reuses a term it has already participated in.
        DDIA §5: currentTerm is persistent state in leader election.
        Never raises.
        """
        return self._highest_term

    def snapshot(self) -> dict:
        """Return a shallow copy of the entire in-memory index.

        Modifications to the returned dict do not affect the engine.
        Never raises.
        """
        return dict(self._index)

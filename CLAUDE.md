# CLAUDE.md — Rainman Project Context

This file is the authoritative reference for all development in this repository.
Read it fully at the start of every session before writing any code.

The full project specification, architecture, and phased build plan is in:
**`project-description.md`**

---

## What This Project Is

**Rainman** is a three-node, leader-based, replicated key-value store built from
scratch in Python, deployed via Docker. It is stress-tested using a sample of the
Yelp Open Dataset and managed by two hybrid AI agents: an **Adversarial Fault
Injector** (LLM-driven fault selection, Docker-level execution) and a
**Consistency Observer** (LLM-generated narration over structured log data).

**Purpose:** Demonstrate mastery of concepts from *Designing Data-Intensive
Applications* (Kleppmann) — specifically replication, write-ahead logging, crash
recovery, and leader election — and to explore agentic management of distributed
systems as an academic research contribution.

**Deadline:** End of day, July 8, 2026. Part-time. Scope is fixed — do not add
features not in the spec without explicit discussion.

---

## Development Setup

```bash
# Initial setup — creates venv and installs all dev dependencies
./devSetup.sh

# Activate venv for a dev session
source venv/bin/activate

# Start the three-node cluster
docker compose up --build

# Stop the cluster
docker compose down
```

## Common Commands

```bash
# Run tests
./runTests.sh
# or directly:
pytest tests/

# Run a single test file
pytest tests/path/to/test_file.py

# Run a single test by name
pytest tests/ -k "test_name"

# Lint
ruff check src/

# Format
ruff format src/

# Build wheel
./build.sh

# Clean build artifacts
./clean.sh

# Build oracle (run after sample_data.py)
python scripts/build_oracle.py

# Verify cluster state against oracle
python scripts/verify_cluster.py

# End-to-end demo
python run_demo.py
```

---

## Technology Constraints — Do Not Deviate Without Discussion

| Concern | Decision | Rationale |
|---|---|---|
| Language | Python 3.13 | Only language in use |
| Transport | FastAPI + httpx | Familiar; lets us focus on DB logic not networking |
| Infrastructure | Docker + Docker Compose | Real network-level partition simulation |
| Storage | Append-only JSON-lines WAL + Python `dict` | Demonstrates storage engine from scratch |
| External DB | **None** | Must not use SQLite, Redis, or any DB library |
| Data structures | Python builtins only | No external tree/index libraries |
| Election algorithm | Simplified fixed-priority fallback | Full Raft is a named future extension |
| Agent models | Ollama (local) only for now | Claude API migration is a future extension |
| DELETE operation | **Not implemented** | Out of scope; only PUT and GET |
| Linter/formatter | `ruff` (line length 79, target py313) | Matches CC's existing config |
| Test runner | `pytest` | Output to `unitTestsReport.xml` |
| Build backend | `hatchling` | Matches CC's existing `pyproject.toml` |

---

## Repository Layout

```
/
├── CLAUDE.md                        ← you are here
├── project-description.md           ← full architecture and build plan
├── docker-compose.yml               ← three-node cluster definition
├── Dockerfile                       ← single image used by all nodes
├── pyproject.toml                   ← hatchling build config, ruff config
├── devSetup.sh                      ← venv creation + editable install
├── runTests.sh                      ← pytest wrapper
├── build.sh                         ← wheel build
├── clean.sh                         ← remove build artifacts
├── run_demo.py                      ← end-to-end demo runner
├── src/
│   └── rainman/
│       ├── node/
│       │   ├── main.py              ← FastAPI node application
│       │   ├── storage.py           ← WAL + dict storage engine
│       │   ├── replication.py       ← leader fanout, follower apply
│       │   └── election.py          ← heartbeat, timeout, vote logic
│       └── agents/
│           ├── recovery_agent.py    ← monitors cluster, triggers recovery
│           └── adversary_agent.py   ← LLM-driven fault injection
├── scripts/
│   ├── sample_data.py               ← produces data/sample/ from data/raw/
│   ├── build_oracle.py              ← computes expected_state.json
│   └── verify_cluster.py            ← diffs live cluster state vs oracle
├── config/
│   ├── cluster.json                 ← node hostnames, ports, priority order
│   └── adversary_config.json        ← fault catalog + Ollama model config
├── data/
│   ├── raw/                         ← Yelp JSON files (gitignored)
│   ├── sample/                      ← sampled working datasets (gitignored)
│   └── expected_state.json          ← correctness oracle output
├── logs/                            ← runtime logs (gitignored)
└── tests/
    ├── test_storage.py
    ├── test_replication.py
    ├── test_election.py
    └── test_scripts.py
```

---

## Non-Negotiable Code Standards

- Every module and public function has a docstring. Docstrings on functions must
  state what the function does and what can go wrong.
- WAL entries are always flushed (`file.flush()` then `os.fsync()`) before
  acknowledging a write. Never skip fsync.
- No write is confirmed to the client until a majority (2 of 3 nodes) have acked.
- Follower startup always replays WAL before accepting any requests.
- All inter-node calls use `httpx` with explicit timeouts. Never block indefinitely.
- All agent decisions and fault events are logged with ISO 8601 timestamps.
- Annotate non-obvious logic with the relevant DDIA chapter and section.
- Do not add features not in the spec without flagging them first.

---

## Key Correctness Invariants

These must hold at all times and are verified by `scripts/verify_cluster.py`:

1. **Durability:** Any write confirmed to the client must survive a single-node
   crash and be present after recovery.
2. **Convergence:** After a fault/recovery cycle with no new writes, all live nodes
   must hold identical dict state.
3. **WAL ordering:** LSN is strictly monotonically increasing within a term; terms
   are strictly monotonically increasing.
4. **Leader uniqueness:** At most one node believes it is leader at any given time.

---

## Connection to DDIA

| Component | DDIA Reference |
|---|---|
| WAL + in-memory index | Chapter 3 — Storage and Retrieval |
| Single-leader replication, majority ack | Chapter 5 — Replication |
| Leader election and failover | Chapter 5 — Replication |
| Network partitions and split-brain | Chapter 8 — The Trouble with Distributed Systems |
| Recovery and convergence after failure | Chapter 9 — Consistency and Consensus |
| Replication log as ordered event stream | Chapter 11 — Stream Processing |
| Observability and anomaly detection | Chapter 1 — Reliable, Scalable, Maintainable |

# Rainman — Distributed Key-Value Storage Engine
## with Agentic Fault Injection and Consistency Monitoring

**Course:** Independent Study
**Timeline:** Part-time, due July 8, 2026
**Reference:** *Designing Data-Intensive Applications* — Martin Kleppmann (Release 17, 2021)

---

## 1. System Overview

Rainman is a three-node, leader-based, replicated key-value store built from scratch
in Python and deployed as a Docker cluster. It is exercised by streaming a sample of
the Yelp Open Dataset through the cluster while two hybrid AI agents inject faults and
monitor cluster health. Correctness is evaluated against a pre-computed oracle derived
offline from the same dataset.

The research contribution is the study of LLM-driven agentic processes as a fault
management mechanism in a distributed system with known, verifiable correctness
properties. The system being tested must be fundamentally correct — without that
foundation, no observation of the agents is scientifically meaningful.

---

## 2. Storage Engine (Per Node)

### 2.1 In-Memory Index

A plain Python `dict` mapping `str` keys to `dict` values. This is the live,
queryable state of the node. It is never persisted directly — it is always
reconstructed from the WAL on startup.

### 2.2 Write-Ahead Log (WAL)

An append-only file of newline-delimited JSON records, one per line.

**WAL entry schema:**
```json
{
  "lsn": 42,
  "term": 1,
  "op": "PUT",
  "key": "tnhfDv5Il8EaGSXZGiuQGg",
  "value": {"name": "Garaje", "stars": 4.31, "review_count": 312}
}
```

- `lsn` — Log Sequence Number. Strictly monotonically increasing. Never reused.
- `term` — Leadership term. Increments on each election.
- `op` — Only `PUT` is implemented. DELETE is out of scope.
- `key` — String. In the Yelp workload, always a `business_id`.
- `value` — Any JSON-serializable dict.

**Write procedure (enforced in `storage.py`):**
1. Serialize entry to JSON and append to WAL file
2. Call `file.flush()` then `os.fsync(file.fileno())`
3. Update in-memory dict
4. Return success

Step 2 is mandatory. It is what makes the log "write-ahead" — the record survives
a crash before the in-memory state is updated.

**Crash recovery / startup procedure:**
1. Open WAL file (create if absent)
2. Read line by line; skip and log any malformed lines (do not crash on corruption)
3. Apply each valid entry to the dict in order
4. Set current LSN to the highest LSN seen
5. Node is now ready to accept requests

### 2.3 `StorageEngine` Interface (`src/rainman/node/storage.py`)

```python
class StorageEngine:
    def __init__(self, wal_path: str) -> None: ...
    def replay(self) -> int: ...           # returns highest LSN seen; called on startup
    def put(self, lsn: int, term: int, key: str, value: dict) -> None: ...
    def get(self, key: str) -> dict | None: ...
    def current_lsn(self) -> int: ...
    def snapshot(self) -> dict: ...        # returns a full copy of the dict
```

---

## 3. Node API (`src/rainman/node/main.py`)

Each node is a FastAPI application running inside a Docker container. All three
containers share a private Docker bridge network (`rainman_net`). Container
hostnames are `node1`, `node2`, `node3`, all listening on port `8000` internally,
mapped to distinct host ports for external access.

### 3.1 Endpoints

#### `GET /health`
Returns node identity and current state. Polled by the recovery agent.

```json
{
  "node_id": "node1",
  "role": "leader",
  "term": 2,
  "lsn": 304,
  "leader_id": "node1",
  "timestamp": "2026-06-28T14:32:01.123Z"
}
```

#### `PUT /kv/{key}`
Write a value. Accepted only by the leader; rejected by followers.

**Request body:** `{"value": {...}}`

**Success (200):** `{"status": "ok", "lsn": 305}`

**Rejected by follower (409):** `{"status": "not_leader", "leader_id": "node1"}`

#### `GET /kv/{key}`
Read from local dict. Any node accepts reads (no forwarding).

**Found (200):** `{"key": "...", "value": {...}, "lsn": 305, "node_id": "node1"}`

**Not found (404):** `{"status": "not_found"}`

#### `POST /replicate`
Called by the leader to push a WAL entry to a follower.

**Request body:** `{"lsn": 305, "term": 2, "op": "PUT", "key": "...", "value": {...}}`

**Success (200):** `{"status": "ok", "lsn": 305}`

**Term mismatch (409):** `{"status": "term_mismatch", "current_term": 3}`

**LSN gap detected (409):** `{"status": "lsn_gap", "expected": 300, "got": 305}`

#### `POST /heartbeat`
Called by leader every 200ms. Follower resets its election timeout on receipt.

**Request body:** `{"leader_id": "node1", "term": 2, "lsn": 304}`

**Response (200):** `{"status": "ok"}`

#### `POST /vote`
Vote request during leader election.

**Request body:** `{"candidate_id": "node2", "term": 3, "candidate_lsn": 304}`

**Grant (200):** `{"vote_granted": true, "term": 3}`

**Deny (200):** `{"vote_granted": false, "term": 3, "reason": "already_voted"}`

#### `POST /admin/inject_delay`
Used by the adversary agent to toggle artificial response delay on this node.
Set `delay_ms` to 0 to clear.

**Request body:** `{"delay_ms": 500}`

---

## 4. Replication (`src/rainman/node/replication.py`)

### 4.1 Leader Write Path

On receiving a valid `PUT /kv/{key}`:
1. Generate next LSN (`current_lsn + 1`)
2. Write entry to own WAL via `StorageEngine.put()`
3. Fan out `POST /replicate` to all followers concurrently (`asyncio.gather`)
4. Wait for at least one follower ack — majority = 2 of 3 total
5. Majority ack within 500ms → return 200 to client
6. Timeout without majority → return 503 (entry remains in leader WAL; no rollback)

The leader tracks per-follower LSN to detect and report replication lag.

### 4.2 Follower Replication Path

On receiving `POST /replicate`:
1. Reject if `term < current_term`
2. Reject if `lsn != current_lsn + 1` (LSN gap — follower has missed entries)
3. Apply via `StorageEngine.put()`
4. Return ack

LSN gaps indicate the follower was down during writes. The recovery agent handles
detection and remediation (see Section 6).

### 4.3 Heartbeat Timing

- Leader sends `POST /heartbeat` to all followers every **200ms**
- Follower election timeout is randomized between **600ms–1000ms**
- Randomization reduces split-vote probability during simultaneous timeouts

---

## 5. Leader Election (`src/rainman/node/election.py`)

### 5.1 Node Priority

Defined in `config/cluster.json`. Lower number = preferred leader.

```json
{
  "nodes": [
    {"node_id": "node1", "host": "node1", "port": 8000, "priority": 1},
    {"node_id": "node2", "host": "node2", "port": 8000, "priority": 2},
    {"node_id": "node3", "host": "node3", "port": 8000, "priority": 3}
  ],
  "replication": {
    "heartbeat_interval_ms": 200,
    "election_timeout_min_ms": 600,
    "election_timeout_max_ms": 1000,
    "replication_timeout_ms": 500,
    "majority": 2
  }
}
```

### 5.2 Election Procedure

When a follower's election timeout fires:
1. Increment `current_term`
2. Vote for self
3. Send `POST /vote` to all other nodes concurrently
4. Majority votes received within 300ms → become leader, begin heartbeats
5. No majority → reset randomized timeout and retry

### 5.3 Vote Grant Conditions

Grant a vote if ALL of:
- `candidate_term >= current_term`
- Node has not already voted this term
- `candidate_lsn >= current_lsn` (candidate is at least as up-to-date)

---

## 6. AI Agent Components

Both agents use a locally-hosted Ollama model. The model endpoint and name are
defined in `config/adversary_config.json` so that a future migration to the Claude
API requires only a config change.

### 6.1 Adversary Agent (`src/rainman/agents/adversary_agent.py`)

**Role:** Uses an LLM to select and time fault injections based on observed cluster
state. Executes faults via Docker commands.

**Architecture — hybrid design:**
- The agent polls `/health` on all nodes at regular intervals to build a structured
  state snapshot (current leader, per-node LSN, role, term)
- That snapshot is passed to the LLM with the fault catalog and a prompt asking it
  to decide: which fault (if any) to inject now, against which target, and why
- The LLM response is parsed for a structured decision; if it chooses to act, the
  agent executes the corresponding Docker command
- Every decision (including "do nothing") is logged with the LLM's rationale

**Fault catalog:**

| Fault ID | Mechanism | Observable Effect |
|---|---|---|
| `KILL_NODE` | `docker compose kill <service>` | Node disappears entirely |
| `RESTART_NODE` | `docker compose restart <service>` | Node restarts; replays WAL |
| `PAUSE_NODE` | `docker compose pause <service>` | Node stops responding; not crashed |
| `RESUME_NODE` | `docker compose unpause <service>` | Paused node resumes |
| `PARTITION_NODE` | `docker network disconnect rainman_net <container>` | Real network isolation |
| `HEAL_PARTITION` | `docker network connect rainman_net <container>` | Reconnects isolated node |
| `INJECT_DELAY` | `POST /admin/inject_delay` to target node | Artificial replication lag |
| `CLEAR_DELAY` | `POST /admin/inject_delay` with `delay_ms: 0` | Clears injected delay |
| `CORRUPT_WAL` | Append malformed JSON to node's WAL file on host mount | Tests WAL hardening |

**Safety constraint:** The agent will not inject a new fault while the cluster has
no live leader (undefined state). All other timing is LLM-driven.

**Logging:** All decisions logged to `logs/adversary_agent.log` with ISO timestamp,
cluster state snapshot, LLM rationale, and action taken.

### 6.2 Consistency Observer (`src/rainman/agents/recovery_agent.py`)

**Role:** Monitors cluster health via `/health` polling and structured log reading.
Produces LLM-generated natural language observations about cluster state. Read-only —
it never affects cluster state.

**Behavior:**
- Polls `/health` on all nodes every 1 second
- Detects structural anomalies via rule-based logic (fast, deterministic):
  - Node unreachable
  - Replication lag (leader LSN − follower LSN > threshold, default 10)
  - No leader (all nodes report `role: follower`)
  - Split brain (two nodes report `role: leader` in same term)
- For each detected anomaly, passes the structured state to the LLM and asks it
  to produce a human-readable observation explaining what it sees and what it implies
- Observations are printed to stdout and written to `logs/observer.log`

**Example observation (LLM-generated):**
> *"Node 3's LSN (288) is 16 entries behind the leader (304). This gap appeared
> approximately 8 seconds ago, coinciding with the last network partition event.
> Node 3 is reachable but not receiving replication — possible one-directional
> partition or replication timeout. The cluster is still in majority and accepting
> writes, but node 3 will need to resync before it can safely serve consistent reads."*

**Scope constraint:** The observer reasons over structured data and produces
language. It does not implement anomaly detection algorithms — that is the rule-based
layer's job. It does not take any action.

### 6.3 Agent Interaction

The agents are complementary and loosely coupled:
- The Fault Injector creates failure conditions
- The Observer watches whether the system surfaces and recovers from those failures
- Together they demonstrate the full failure lifecycle: **inject → observe → recover**

They do not communicate directly. Both read cluster state independently.

---

## 7. Data Pipeline

### 7.1 Dataset Files Required

Place the following Yelp JSON files in `data/raw/` (gitignored):
- `yelp_academic_dataset_business.json`
- `yelp_academic_dataset_review.json`

Yelp Academic Dataset is available free for academic use at
https://www.yelp.com/dataset. Requires agreement to Yelp's dataset terms.

### 7.2 Sampling Script (`scripts/sample_data.py`)

**Purpose:** Produces a minimal, self-consistent working dataset.

**Algorithm:**
1. Read `business.json` with reservoir sampling (single pass, no full file in memory)
   to select N businesses (default N=1,000)
2. Collect sampled `business_id` values into a set
3. Read `review.json` line by line; keep reviews where `business_id` is in the set
4. Limit to M reviews total (default M=5,000), sorted by date ascending
5. Write:
   - `data/sample/businesses.jsonl` — one business JSON per line
   - `data/sample/reviews.jsonl` — one review JSON per line, chronological

```bash
python scripts/sample_data.py --businesses 1000 --reviews 5000
```

Prints a summary: businesses sampled, reviews matched, date range covered.

### 7.3 Oracle Builder (`scripts/build_oracle.py`)

**Purpose:** Pre-computes the expected final cluster state without touching the
cluster. This is the correctness ground truth.

**Algorithm:**
1. Read `businesses.jsonl` — each record is an initial PUT
2. Read `reviews.jsonl` in order — each review updates the target business:
   - Recalculate `stars` as a running weighted average
   - Increment `review_count`
3. Serialize final state to `data/expected_state.json`

**Output schema:**
```json
{
  "generated_at": "2026-06-28T14:00:00Z",
  "total_keys": 1000,
  "total_writes": 6000,
  "state": {
    "tnhfDv5Il8EaGSXZGiuQGg": {
      "name": "Garaje",
      "stars": 4.31,
      "review_count": 312
    }
  }
}
```

```bash
python scripts/build_oracle.py
```

### 7.4 Cluster Verification Script (`scripts/verify_cluster.py`)

**Purpose:** Diffs live cluster state against the oracle. Run after any
fault/recovery cycle to confirm correctness.

**Algorithm:**
1. Read `data/expected_state.json`
2. For each key, call `GET /kv/{key}` on the leader and both followers
3. Compare each response to the oracle value and to each other
4. Report: missing keys, value mismatches, follower/leader divergence

**Output:** Human-readable summary to stdout + machine-readable
`logs/verification_{timestamp}.json`

```bash
python scripts/verify_cluster.py --tolerance 0.01
```

`--tolerance` applies only to float fields (star rating rounding).

### 7.5 Fallback: Synthetic Data Generator

If the Yelp dataset is unavailable, `scripts/generate_synthetic.py` produces a
structurally equivalent dataset: 1,000 business-like records with nested JSON values
averaging ~2KB, and 5,000 synthetic update events. The oracle builder works
identically against synthetic data.

---

## 8. Infrastructure

### 8.1 Docker Compose (`docker-compose.yml`)

Three services — `node1`, `node2`, `node3` — built from a single `Dockerfile`.
Each service:
- Runs the FastAPI node on port 8000 internally
- Mounts a host directory for its WAL file (so WAL persists across container restarts)
- Is on a shared bridge network `rainman_net`
- Receives its `NODE_ID` and priority via environment variable

Host port mapping (for scripts running outside Docker):
- `node1` → `localhost:8001`
- `node2` → `localhost:8002`
- `node3` → `localhost:8003`

### 8.2 Demo Runner (`run_demo.py`)

End-to-end demonstration script:
1. Verify cluster is up (`docker compose ps`)
2. Build oracle if `expected_state.json` is absent
3. Bulk-load `businesses.jsonl` into cluster via `PUT /kv/{business_id}`
4. Start Consistency Observer subprocess
5. Start Adversary Agent subprocess
6. Stream `reviews.jsonl` into cluster as sequential writes
7. Wait for all writes to complete and cluster to converge (poll `/health` until
   all node LSNs match)
8. Run `verify_cluster.py` and print result
9. Shut down both agent subprocesses

---

## 9. Phased Build Plan

### Phase 1 — Single-Node Storage Engine (Days 1–2) ✓ COMPLETE

**Goal:** Prove the storage engine is correct in isolation.

- [x] Implement `StorageEngine` in `src/rainman/node/storage.py`
- [x] Implement single-node FastAPI app with `PUT /kv/{key}`, `GET /kv/{key}`,
      `GET /health` (no replication yet)
- [x] Write `tests/test_storage.py` — WAL replay, fsync, malformed line handling
- [x] Implement `scripts/sample_data.py` → produce working sample files
- [x] Implement `scripts/build_oracle.py` → produce `expected_state.json`
- [x] Manual test: load all businesses, verify all readable, SIGKILL container,
      verify WAL replay restores full state

**Exit criterion:** Node correctly replays WAL after a container kill and all data
is intact.

> ✓ Exit criterion met 2026-06-29. WAL replay confirmed after SIGKILL.
> Sampled data and oracle retained in `data/sample/` and
> `data/expected_state.json` for use in Phase 2 verification.

### Phase 2 — Three-Node Replication (Days 3–5)

**Goal:** Static leader fans out writes; followers stay in sync.

- [ ] Define `config/cluster.json` and implement config loader
- [ ] Implement `POST /replicate` and `POST /heartbeat` endpoints
- [ ] Implement leader fanout and majority ack in `replication.py`
- [ ] Set up `docker-compose.yml` with three nodes on `rainman_net`
- [ ] Implement `scripts/verify_cluster.py`
- [ ] Write `tests/test_replication.py`
- [ ] Manual test: kill a follower container, restart it, verify WAL replay
      catches it up to leader; run `verify_cluster.py`

**Exit criterion:** `verify_cluster.py` reports zero mismatches after bulk load
across three nodes.

### Phase 3 — Leader Election (Days 5–6)

**Goal:** Cluster survives leader failure and resumes writes automatically.

- [ ] Implement `election.py` (heartbeat timeout, candidate logic, vote grant)
- [ ] Implement `POST /vote` endpoint
- [ ] Implement `POST /admin/inject_delay` endpoint
- [ ] Write `tests/test_election.py`
- [ ] Manual test: kill `node1` (leader), verify `node2` wins election and writes
      continue; restart `node1` as follower; run `verify_cluster.py`

**Exit criterion:** Cluster survives leader kill and `verify_cluster.py` still
passes after recovery.

### Phase 4 — Agentic Processes (Days 6–8)

**Goal:** Agents autonomously manage the fault/observe cycle.

- [ ] Implement `recovery_agent.py` (rule-based detection + LLM narration via Ollama)
- [ ] Implement `adversary_agent.py` (cluster state polling + LLM fault decision +
      Docker execution)
- [ ] Define `config/adversary_config.json` (Ollama endpoint, model name, fault
      catalog, safety constraints)
- [ ] Implement `run_demo.py`
- [ ] End-to-end demo run: bulk load → agent activation → fault injection →
      automatic observation → `verify_cluster.py`

**Exit criterion:** Full demo run completes with `verify_cluster.py` passing after
all fault/recovery cycles. Observer produces meaningful natural-language output.
Adversary makes at least one autonomous, state-driven decision logged with rationale.

### Phase 5 — Hardening and Documentation (Days 9–10)

**Goal:** Clean, demonstrable, well-documented.

- [ ] Handle WAL corruption gracefully on startup
- [ ] Handle split-brain edge case (two leaders detected → observer flags it;
      adversary agent can kill lower-priority one)
- [ ] Write `README.md` with setup and demo instructions
- [ ] Annotate key code sections with DDIA chapter references
- [ ] Record or document a clean demo run showing full fault/recovery lifecycle

**Exit criterion:** An unfamiliar reader can clone the repo, follow `README.md`,
and observe a successful demo run.

---

## 10. Evaluation Criteria

The project is considered complete when all of the following can be demonstrated:

- [ ] Three nodes running with one elected leader
- [ ] Writes replicated to followers and majority-acknowledged before client confirm
- [ ] Leader can be killed and a new leader elected without data loss
- [ ] A killed or partitioned node can rejoin and resynchronize via WAL replay
- [ ] The Consistency Observer correctly identifies and narrates a divergence or lag
      condition in natural language
- [ ] The Adversary Agent makes at least one autonomous, state-driven fault decision
      with a logged LLM rationale
- [ ] `verify_cluster.py` passes (zero mismatches) after a complete fault/recovery
      cycle against the correctness oracle

---

## 11. Future Extensions (Out of Scope Now)

- Full Raft log matching and log compaction
- Migration from Ollama to Claude API for agent models
- Leaderless replication mode (direct DDIA Chapter 5 comparison)
- Isolation anomaly reproduction (dirty reads, write skew, phantom reads)
- Quantitative benchmarking (replication lag under load, time-to-election, MTTR)
- DELETE operation

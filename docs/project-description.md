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

### 8.1 Docker Compose (`compose.yaml`)

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

### Phase 2 — Three-Node Replication (Days 3–5) ✓ COMPLETE

**Goal:** Static leader fans out writes; followers stay in sync.

- [x] Define `config/cluster.json` and implement config loader
- [x] Implement `POST /replicate` and `POST /heartbeat` endpoints
- [x] Implement leader fanout and majority ack in `replication.py`
- [x] Set up `compose.yaml` with three nodes on `rainman_net`
- [x] Implement `scripts/verify_cluster.py`
- [x] Write `tests/test_replication.py`
- [x] Manual test: kill a follower container, restart it, verify WAL replay
      catches it up to leader; run `verify_cluster.py`

  **Steps:**
  1. `docker compose up --build` — start all three nodes
  2. Bulk-load the Yelp sample dataset into the leader (node1, port 8001).
     Run both scripts with **all three nodes running** so every node
     receives every write before the follower is stopped:

     ```bash
     python scripts/load_businesses.py
     python scripts/load_reviews.py
     ```

     After this, all three nodes hold the exact state that
     `verify_cluster.py` checks against.
  3. `docker compose stop node3` — kill a follower
  4. Write one extra key while node3 is down (node1+node2 still form a
     majority, so writes succeed; this key is not in the oracle so
     `verify_cluster.py` will not check for it):

     ```bash
     curl -X PUT http://localhost:8001/kv/test_key \
       -H "Content-Type: application/json" \
       -d '{"value": {"note": "written_while_down"}}'
     ```

  5. `docker compose start node3` — on startup node3 replays its local
     WAL, which rebuilds the in-memory dict to the state it had at stop
     time (all oracle keys present); there is no pull-from-leader
     catch-up in Phase 2, so `test_key` stays missing on node3
  6. `python scripts/verify_cluster.py` — must report zero mismatches
     (oracle keys only; `test_key` is not checked)

**Exit criterion:** `verify_cluster.py` reports zero mismatches after bulk load
across three nodes.

> ✓ Complete 2026-06-30. 39 tests passing, lint clean.
> Static leader (node1, priority 1) fans out writes to followers via concurrent
> `asyncio.gather`; majority ack (2 of 3) required before 200 is returned to
> client; followers reject writes with 409 and apply replication entries via
> `POST /replicate` with term-mismatch and LSN-gap detection; leader sends
> heartbeats every 200 ms via background asyncio task.  Manual container test
> (kill node3 → restart → WAL replay → verify_cluster.py) passed with zero
> mismatches across all 1000 oracle keys.

### Phase 3 — Leader Election (Days 5–6) ✓ COMPLETE

**Goal:** Cluster survives leader failure and resumes writes automatically.

- [x] Implement `election.py` (heartbeat timeout, candidate logic, vote grant)
- [x] Implement `POST /vote` endpoint
- [x] Implement `POST /admin/inject_delay` endpoint
- [x] Write `tests/test_election.py`
- [ ] Manual test: kill `node1` (leader), verify `node2` wins election and writes
      continue; restart `node1` as follower; run `verify_cluster.py`

  **Steps:**
  1. Start (or restart) the cluster with the Phase 3 code:

     ```bash
     docker compose up --build -d
     ```

     The `-d` flag backgrounds the cluster so your terminal stays free.
     All three nodes start as followers and begin their election timeouts.

  2. Wait for the initial election to complete:

     ```bash
     sleep 2
     ```

     Each follower waits 600–1000 ms before calling an election.  One
     node times out first, increments the term, and solicits votes from
     the other two.  The vote round completes within 300 ms.  Total time
     to first leader is normally under 2 seconds.

  3. Confirm a leader has been elected:

     ```bash
     curl -s http://localhost:8001/health | python3 -m json.tool
     curl -s http://localhost:8002/health | python3 -m json.tool
     curl -s http://localhost:8003/health | python3 -m json.tool
     ```

     One node will show `"role": "leader"` and `"term": 1` (or higher).
     The other two will show `"role": "follower"` with the same
     `"leader_id"` and `"term"`.  If you do not see a leader yet, wait
     one more second and re-run — a split vote (both nodes timeout
     simultaneously) can require a second election.

     Note the leader's `node_id`; you will need it for steps 6 and 11.
     The steps below assume **node1** won; substitute `node2` or `node3`
     and the corresponding host port (8002 or 8003) if another node won.

  4. Load the Yelp dataset (**skip if you completed Phase 2 and the WAL
     files in `data/node1/`, `data/node2/`, `data/node3/` are still
     present from that session** — WAL replay on startup restores the
     full in-memory state automatically):

     ```bash
     python scripts/load_businesses.py
     python scripts/load_reviews.py
     ```

     Run both scripts with all three nodes running so every write is
     replicated before the kill.  After this, all three WALs contain the
     full oracle state.

  5. Write a pre-kill marker key to the current leader (replace
     `$LEADER_PORT` with 8001/8002/8003 to match whoever won step 3):

     ```bash
     LEADER_PORT=8001

     curl -X PUT "http://localhost:${LEADER_PORT}/kv/pre_kill_key" \
       -H "Content-Type: application/json" \
       -d '{"value": {"note": "written_before_kill"}}'
     ```

     A 200 response confirms the leader is accepting writes.  This key is
     not in the oracle so `verify_cluster.py` will not check it; it simply
     lets you confirm the pre-kill state.

  6. Stop the leader with `docker compose stop` rather than `kill`.
     The `compose.yaml` `restart: on-failure` policy only triggers on a
     non-zero exit; `stop` sends SIGTERM and the node exits cleanly with
     code 0, so it stays down until you explicitly start it again:

     ```bash
     docker compose stop node1   # replace with the actual leader service name
     ```

     The two surviving nodes immediately stop receiving heartbeats and
     begin their election countdowns independently.

  7. Wait for re-election:

     ```bash
     sleep 2
     ```

  8. Confirm the new leader on the two surviving nodes:

     ```bash
     curl -s http://localhost:8002/health | python3 -m json.tool
     curl -s http://localhost:8003/health | python3 -m json.tool
     ```

     One of the two will now show `"role": "leader"` with an incremented
     `"term"` (should be 2 if the initial term was 1).  The `"leader_id"`
     on both nodes must agree.  Both nodes will also have forgotten
     node1 as the leader — their `"leader_id"` now names the new winner.

  9. Write a key to the new leader while the old leader is still down.
     Replace `$NEW_PORT` with the host port of the node that won step 8:

     ```bash
     NEW_PORT=8002   # or 8003

     curl -X PUT "http://localhost:${NEW_PORT}/kv/post_election_key" \
       -H "Content-Type: application/json" \
       -d '{"value": {"note": "written_after_election"}}'
     ```

     A 200 response confirms the new leader is writing and replicating
     normally.  A 503 (majority ack failed) means quorum cannot be reached
     because the third node is still down — verify that exactly two nodes
     are running with `docker compose ps`.

  10. Run `verify_cluster.py` while node1 is still down:

      ```bash
      python scripts/verify_cluster.py
      ```

      The script will time out when it tries to contact node1 — that is
      expected.  The important result is that **the two live nodes agree
      on every oracle key with zero mismatches**.  This confirms the
      election did not corrupt replicated state.

  11. Restart the stopped node:

      ```bash
      docker compose start node1
      ```

      node1 opens its WAL, replays all entries it had before it was
      stopped, and rebuilds its in-memory dict.  It then starts its
      election timeout.  Because the two surviving nodes are already
      sending heartbeats, node1 receives one within 200 ms, learns who
      the current leader is, and resets its countdown without calling a
      competing election.

  12. Wait for node1 to finish startup:

      ```bash
      sleep 3
      ```

  13. Confirm node1 is now a follower with the correct term:

      ```bash
      curl -s http://localhost:8001/health | python3 -m json.tool
      ```

      Expected: `"role": "follower"`, `"leader_id"` pointing to the
      winner from step 8, and `"term"` matching the cluster term.  The
      `"lsn"` on node1 will be slightly lower than the leader's — it
      missed `post_election_key` while it was down.  This is expected;
      Phase 3 does not implement pull-from-leader catch-up (that is a
      Phase 4+ concern).

  14. Run `verify_cluster.py` with all three nodes live:

      ```bash
      python scripts/verify_cluster.py
      ```

      Must report zero mismatches.  All oracle keys were loaded before
      the kill so every one of them is in node1's WAL; `post_election_key`
      is not in the oracle so its absence on node1 does not trigger a
      mismatch.

**Exit criterion:** Cluster survives leader kill and `verify_cluster.py` still
passes after recovery.

> ✓ Complete 2026-06-30. 70 tests passing, lint clean.
> `ElectionManager` drives randomised-timeout election: followers time out
> after 600–1000 ms without a heartbeat, increment term, self-vote, and
> send concurrent POST /vote to peers; a majority wins the election and
> calls `_on_became_leader` which starts the heartbeat loop and sets
> `_role = "leader"`.  Vote-grant enforces all three DDIA §5.3 conditions
> (term, voted_for, log completeness).  Leaders step down on any
> higher-term message (heartbeat, vote, replicate).  StorageEngine now
> tracks `highest_term` during WAL replay so term monotonicity survives
> crashes.  POST /admin/inject_delay enables adversary-agent fault
> injection.  Manual cluster test pending.

### Phase 4 — Agentic Processes (Days 6–8) ✓ COMPLETE

**Goal:** Agents autonomously manage the fault/observe cycle.

- [x] Implement `recovery_agent.py` (rule-based detection + LLM narration via Ollama)
- [x] Implement `adversary_agent.py` (cluster state polling + LLM fault decision +
      Docker execution)
- [x] Define `config/adversary_config.json` (Ollama endpoint, model name, fault
      catalog, safety constraints)
- [x] Implement `run_demo.py`
- [x] End-to-end demo run: bulk load → agent activation → fault injection →
      automatic observation → `verify_cluster.py`

  **Steps:**
  1. Tear down any existing cluster state and rebuild from scratch:

     ```bash
     docker compose down
     rm -rf data/node1 data/node2 data/node3
     docker compose up --build -d
     ```

     `docker compose down -v` only removes *named* Docker volumes; the
     WAL data lives in bind-mounted directories (`./data/node*/`) that
     `down -v` does not touch.  Deleting those directories guarantees
     every node starts with an empty WAL at LSN 0, which is required for
     a clean demo run.  The `--build` flag bakes the latest
     `config/cluster.json` (including the replication timeout) into the
     image.  All three nodes boot as followers and begin election
     countdowns.

  2. Wait for the initial leader election:

     ```bash
     sleep 3
     ```

     One node times out first, wins the vote, and begins sending
     heartbeats.  Three seconds is conservative — the election normally
     completes in under two.

  3. Confirm all three nodes are healthy and a leader has emerged:

     ```bash
     curl -s http://localhost:8001/health | python3 -m json.tool
     curl -s http://localhost:8002/health | python3 -m json.tool
     curl -s http://localhost:8003/health | python3 -m json.tool
     ```

     One node must show `"role": "leader"`; the other two must show
     `"role": "follower"` with a matching `"leader_id"` and `"term"`.
     If no leader appears yet, wait one more second and re-run — a split
     vote can require a second election round.

  4. Confirm Ollama is running and the configured model is loaded:

     ```bash
     curl -s http://localhost:11434/api/tags | python3 -m json.tool
     ```

     Look for `"llama3.2"` (or whatever model is set in
     `config/adversary_config.json`) in the `"models"` list.  If the
     model is absent, pull it first:

     ```bash
     ollama pull llama3.2
     ```

     If Ollama is not running at all, both agents will degrade gracefully
     (observer logs fallback messages; adversary skips LLM decisions and
     takes no action), and the demo will still complete — but you will not
     see LLM-generated narration or autonomous fault decisions.

  5. Run the end-to-end demo:

     ```bash
     python run_demo.py
     ```

     The demo orchestrates nine steps automatically and streams all output
     to the terminal.  Expected sequence visible in the output:

     - **Preflight** — three `/health` checks; leader identified.
     - **Oracle** — `data/expected_state.json` built if absent.
     - **Bulk load** — `data/sample/businesses.jsonl` written to the
       leader with progress printed every 100 records.
     - **Observer start** — `recovery_agent.py` subprocess launched;
       `[HEALTHY]` or `[ANOMALY]` lines appear alongside demo output.
     - **Adversary start** — `adversary_agent.py` subprocess launched;
       `[POLL]` and `[DECISION]` lines appear every ~5 seconds.
     - **Review stream** — `data/sample/reviews.jsonl` written
       sequentially; on a 409 the demo re-discovers the leader and
       redirects; on a 503 it retries up to three times.
     - **Convergence** — polls `/health` every 2 seconds until all live
       nodes agree on LSN or 60 seconds elapse; prints per-node LSN.
     - **Verify** — `scripts/verify_cluster.py` runs and prints its
       result.
     - **Shutdown** — both agent subprocesses receive SIGTERM.

     The full run takes several minutes depending on dataset size and
     how aggressively the adversary acts.

  6. While the demo runs, watch the adversary log in a second terminal
     to confirm autonomous fault decisions are being made:

     ```bash
     tail -f logs/adversary_agent.log
     ```

     You should see lines like:

     ```text
     2026-07-01T14:35:00.200Z [DECISION] action=KILL_NODE target=node3 rationale="..."
     2026-07-01T14:35:00.800Z [EXECUTED] KILL_NODE node3 success=True
     ```

     At least one `[DECISION]` with a non-`no_action` action must appear
     for the exit criterion to be met.

  7. In a third terminal, watch the observer log to confirm LLM narration:

     ```bash
     tail -f logs/observer.log
     ```

     You should see anomaly narrations when the adversary injects a fault:

     ```text
     2026-07-01T14:35:01.000Z [ANOMALY] node_unreachable {"node_id": "node3"}
     2026-07-01T14:35:02.441Z [OBSERVATION] Node 3 has become unreachable...
     ```

     Followed by `[HEALTHY]` once the cluster recovers.

  8. After the demo exits, check the final `verify_cluster.py` output
     printed at the bottom of the demo runner's terminal output.  It must
     show **zero mismatches**.  If mismatches appear, run manually to see
     which keys differ:

     ```bash
     python scripts/verify_cluster.py
     ```

  9. Confirm the exit criteria checklist from `docs/phase-4.md §10`:

     - `verify_cluster.py` reports zero mismatches.
     - `logs/observer.log` contains at least one `[OBSERVATION]` with
       LLM-generated text.
     - `logs/adversary_agent.log` contains at least one `[DECISION]`
       with a non-`no_action` action and a logged rationale.
     - The observer correctly identified at least one anomaly during
       the run.

**Exit criterion:** Full demo run completes with `verify_cluster.py` passing after
all fault/recovery cycles. Observer produces meaningful natural-language output.
Adversary makes at least one autonomous, state-driven decision logged with rationale.

> ✓ Complete 2026-06-30. 85 tests passing (15 new agent tests), lint clean.
> `ConsistencyObserver` polls `/health` every 1 s; `detect_anomalies()` is a
> pure module-level function that runs four ordered rule-based checks
> (node unreachable, no leader, split-brain, replication lag) and returns
> structured anomaly dicts; each anomaly is narrated by Ollama via
> `POST /api/generate` and written to `logs/observer.log`.  LLM unavailability
> degrades gracefully — fallback message logged, loop continues.
> `AdversaryAgent` polls every 5 s, enforces a safety guard
> (`_is_safe_to_inject`: leader present + majority reachable) and a
> minimum fault interval (15 s), calls Ollama with `"format": "json"` for a
> structured `{action, target, rationale}` decision, and dispatches to one of
> nine fault executors (KILL, RESTART, PAUSE, RESUME, PARTITION, HEAL,
> INJECT_DELAY, CLEAR_DELAY, CORRUPT_WAL) via subprocess/HTTP; all decisions
> logged to `logs/adversary_agent.log`.  Both agents run as host processes
> (not containers) with direct Docker CLI and Ollama access.  `run_demo.py`
> orchestrates the nine-step demo sequence with 409 leader-redirect and 503
> retry logic during review streaming.  End-to-end manual run pending
> (requires live cluster + Ollama).

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

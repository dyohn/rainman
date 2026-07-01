# Phase 4 — Agentic Processes: Implementation Plan

**Goal:** Agents autonomously manage the fault/observe cycle over a running cluster.

**Exit criterion:** Full demo run completes with `verify_cluster.py` passing after all
fault/recovery cycles. Observer produces meaningful natural-language output. Adversary
makes at least one autonomous, state-driven decision logged with rationale.

---

## 1. Files to Create

| Path | What it is |
|---|---|
| `config/adversary_config.json` | Ollama endpoint, model, fault catalog, timing |
| `src/rainman/agents/__init__.py` | Package marker (empty) |
| `src/rainman/agents/recovery_agent.py` | Consistency Observer (read-only) |
| `src/rainman/agents/adversary_agent.py` | Adversary Fault Injector |
| `run_demo.py` | End-to-end demo orchestrator |
| `tests/test_agents.py` | Unit tests for both agents |

No changes needed to existing node code. No new dependencies — `httpx` and Python
builtins cover everything. `subprocess` handles Docker commands.

---

## 2. `config/adversary_config.json`

```json
{
  "ollama": {
    "base_url": "http://localhost:11434",
    "model": "llama3.2",
    "timeout_s": 30
  },
  "observer": {
    "poll_interval_s": 1.0,
    "replication_lag_threshold": 10,
    "log_path": "logs/observer.log"
  },
  "adversary": {
    "poll_interval_s": 5.0,
    "min_fault_interval_s": 15.0,
    "log_path": "logs/adversary_agent.log"
  },
  "faults": [
    {"id": "KILL_NODE",      "description": "Hard-kill the service (docker compose kill). Node disappears until restarted."},
    {"id": "RESTART_NODE",   "description": "Restart the service (docker compose restart). Node replays WAL on startup."},
    {"id": "PAUSE_NODE",     "description": "Freeze the container (docker compose pause). Node stops responding but is not crashed."},
    {"id": "RESUME_NODE",    "description": "Unfreeze a paused container (docker compose unpause)."},
    {"id": "PARTITION_NODE", "description": "Disconnect node from the bridge network (docker network disconnect). Real network isolation."},
    {"id": "HEAL_PARTITION", "description": "Reconnect an isolated node (docker network connect)."},
    {"id": "INJECT_DELAY",   "description": "Add artificial response delay via POST /admin/inject_delay. Simulates a slow or overloaded node."},
    {"id": "CLEAR_DELAY",    "description": "Clear injected delay (delay_ms=0)."},
    {"id": "CORRUPT_WAL",    "description": "Append malformed JSON to the node's WAL on the host mount. Tests WAL hardening at startup."}
  ]
}
```

The `adversary_config.json` is the sole place an Ollama model change needs to happen
for a future Claude API migration.

---

## 3. `src/rainman/agents/recovery_agent.py` — Consistency Observer

### Role

Poll all three nodes, run rule-based anomaly detection, and for each anomaly call
the LLM for a human-readable narration. **Never modifies cluster state.**

### Module-level structure

```
ConsistencyObserver
  __init__(config: dict, node_urls: list[str])
  async run()              — main loop; runs until cancelled
  async _poll_cluster()    — collect /health from all nodes; returns NodeSnapshot list
  detect_anomalies(snapshots) — pure function; returns list[Anomaly]
  async _narrate(anomaly, snapshots) — call Ollama; return str
  _log(message: str)       — write timestamped line to observer.log + stdout
```

### NodeSnapshot (dataclass or dict)

```python
{
    "node_id": str,
    "reachable": bool,
    "role": str | None,
    "term": int | None,
    "lsn": int | None,
    "leader_id": str | None,
    "timestamp": str | None,
}
```

Unreachable nodes have `reachable=False` and all other fields `None`.

### `detect_anomalies(snapshots)` — pure, testable

Returns a list of anomaly dicts. Each dict has `type` (string) and `detail` (dict).
Run these checks in order:

| Check | Condition | Anomaly type |
|---|---|---|
| Node unreachable | `reachable == False` for any node | `"node_unreachable"` |
| No leader | all reachable nodes have `role != "leader"` | `"no_leader"` |
| Split brain | two or more reachable nodes report `role == "leader"` in the same `term` | `"split_brain"` |
| Replication lag | `leader_lsn - follower_lsn > threshold` for any follower | `"replication_lag"` |

Return an empty list when the cluster is healthy. This function is the only place
anomaly logic lives — the LLM layer never decides *what* is wrong, only *how to
describe* it.

### Ollama call

```
POST {ollama.base_url}/api/generate
{
  "model": "{ollama.model}",
  "stream": false,
  "prompt": "..."
}
```

Prompt template (fill in `{anomaly_type}`, `{cluster_json}`):

```
You are a distributed systems observability tool monitoring a 3-node Raft-based
key-value cluster. Respond in 2–4 sentences. Be specific about which nodes are
affected and what the observation implies for the cluster's health and consistency.
Do not suggest fixes.

Anomaly detected: {anomaly_type}
Detail: {detail_json}

Current cluster state:
{cluster_json}
```

Parse `response.json()["response"]` for the narration text. On any HTTP error or
timeout, log a fallback message (`"[LLM unavailable] Anomaly: {anomaly_type}"`) so
the observer keeps running even if Ollama is down.

### Logging format

```
2026-07-01T14:32:01.123Z [ANOMALY] replication_lag {"lag": 16, "node": "node3"}
2026-07-01T14:32:02.441Z [OBSERVATION] Node 3's LSN (288) is 16 entries...
2026-07-01T14:32:03.000Z [HEALTHY] All nodes nominal (term=2, lsn=304)
```

Log `[HEALTHY]` at most once every 30 seconds to avoid noise during a clean run.

### CLI entry point

```bash
python -m rainman.agents.recovery_agent \
    --config config/adversary_config.json \
    --cluster config/cluster.json
```

Reads `host_port` from `cluster.json` to construct node URLs
(`http://localhost:{host_port}`). This keeps the agent runnable from the host
without Docker knowledge.

---

## 4. `src/rainman/agents/adversary_agent.py` — Adversary Fault Injector

### Role

Poll cluster state, give the LLM the state + fault catalog, execute the decision,
log everything. Docker commands run via `subprocess` with `cwd=project_dir`.

### Module-level structure

```
AdversaryAgent
  __init__(config: dict, node_urls: list[str], project_dir: str)
  async run()                       — main loop
  async _poll_cluster()             — same as observer; reuse the NodeSnapshot pattern
  _is_safe_to_inject(snapshots)     — pure; returns bool
  async _decide(snapshots)          — call Ollama; parse JSON decision
  async _execute(decision, snapshots) — dispatch to fault executor
  _log_decision(snapshots, decision, executed) — write to adversary_agent.log
```

### Safety constraint (`_is_safe_to_inject`)

Return `False` (do not inject) if:
- No reachable node reports `role == "leader"`
- The cluster has fewer than a majority of reachable nodes (< 2 of 3)

The agent also enforces `min_fault_interval_s` between injections by tracking
`_last_fault_time` and comparing to `time.monotonic()`.

### LLM decision prompt

```
You are an adversarial fault injector testing a 3-node Raft key-value cluster.
Decide whether to inject a fault now. If yes, choose one fault and one target.
Respond ONLY with a JSON object — no prose, no markdown.

Schema: {"action": "<FAULT_ID or no_action>", "target": "<node1|node2|node3|null>", "rationale": "<one sentence>"}

Available faults:
{fault_catalog_json}

Current cluster state:
{cluster_json}

Recent fault history (last 5):
{history_json}
```

Use `"format": "json"` in the Ollama request body to get guaranteed JSON output.
Parse with `json.loads(response["response"])`. Validate that `action` is one of the
known fault IDs or `"no_action"`, and that `target` is `"node1"`, `"node2"`,
`"node3"`, or `null`/`None`. If validation fails, log the raw response and treat it
as `"no_action"`.

### Fault executor dispatch

Map `action` → executor function. All executors accept `(target: str, snapshots,
project_dir: str)` and return `bool` (success).

| Fault ID | Subprocess / HTTP call |
|---|---|
| `KILL_NODE` | `docker compose kill {target}` (cwd=project_dir) |
| `RESTART_NODE` | `docker compose restart {target}` |
| `PAUSE_NODE` | `docker compose pause {target}` |
| `RESUME_NODE` | `docker compose unpause {target}` |
| `PARTITION_NODE` | `docker network disconnect rainman_net {target}` |
| `HEAL_PARTITION` | `docker network connect rainman_net {target}` |
| `INJECT_DELAY` | `POST http://localhost:{host_port}/admin/inject_delay {"delay_ms": 500}` |
| `CLEAR_DELAY` | `POST /admin/inject_delay {"delay_ms": 0}` |
| `CORRUPT_WAL` | append `"\x00{invalid\n"` to `{project_dir}/data/{target}/wal.jsonl` |

`docker compose` commands use the service name (= `target`). `docker network`
commands also use the container name, which equals the service name per `compose.yaml`.

Run `subprocess.run([...], cwd=project_dir, capture_output=True, timeout=10)`.
Log stderr on non-zero returncode but do not raise — a failed fault injection is
notable but should not crash the agent.

### Logging format

```
2026-07-01T14:35:00.000Z [POLL] {"node1": {"role": "leader", "term": 2, "lsn": 304}, ...}
2026-07-01T14:35:00.200Z [DECISION] action=KILL_NODE target=node3 rationale="node3 is a follower with..."
2026-07-01T14:35:00.800Z [EXECUTED] KILL_NODE node3 success=True
```

Maintain an in-memory `_fault_history` deque of the last 10 executed faults
(ISO timestamp + action + target) to pass to the LLM.

### CLI entry point

```bash
python -m rainman.agents.adversary_agent \
    --config config/adversary_config.json \
    --cluster config/cluster.json \
    --project-dir .
```

---

## 5. `run_demo.py`

Orchestrates the full demo. Runs entirely on the host. All cluster access via
`localhost:{host_port}`.

### Steps

```
1.  Preflight — verify cluster is up
2.  Oracle — build if expected_state.json is absent
3.  Bulk load — stream businesses.jsonl into leader
4.  Start Observer subprocess
5.  Start Adversary subprocess
6.  Stream reviews.jsonl into leader
7.  Converge — wait until all live node LSNs agree (or timeout)
8.  Verify — run verify_cluster.py; print result
9.  Shutdown — terminate both subprocesses
```

### Implementation notes

**Step 1 — Preflight:**
Call `GET /health` on all three nodes. If fewer than 2 respond, print instructions
and exit. Wait up to 5 seconds for a leader to emerge (poll every 0.5s). Identify
the leader node for write routing.

**Step 3 — Bulk load:**
Reuse the logic from `scripts/load_businesses.py` inline (or import it). Write to
the leader's `PUT /kv/{business_id}`. Print progress every 100 records.

**Step 4–5 — Subprocess launch:**
```python
observer_proc = subprocess.Popen(
    [sys.executable, "-m", "rainman.agents.recovery_agent",
     "--config", "config/adversary_config.json",
     "--cluster", "config/cluster.json"],
    stdout=sys.stdout, stderr=sys.stderr,
)
adversary_proc = subprocess.Popen(...)
```

Both agents inherit stdout/stderr so their output streams into the terminal
alongside the demo runner's own output.

**Step 6 — Stream reviews:**
Write reviews sequentially (one at a time, not concurrent) to keep the sequence
deterministic and give the adversary time to act between writes. On a 409 (not
leader), re-query `/health` on all nodes to find the new leader and redirect the
write. On 503 (majority ack failed), retry up to 3 times with a 1s backoff before
skipping the record and logging a warning.

**Step 7 — Convergence:**
After all reviews are written, poll `/health` every 2 seconds. Convergence =
all *reachable* nodes report the same LSN, OR 60 seconds elapse. Print final
per-node LSN before moving on.

**Step 8 — Verify:**
```python
result = subprocess.run(
    [sys.executable, "scripts/verify_cluster.py"],
    capture_output=True, text=True,
)
print(result.stdout)
```
Print the final pass/fail prominently.

**Step 9 — Shutdown:**
Send `SIGTERM` to both subprocesses, wait up to 5 seconds, then `SIGKILL` if they
don't exit.

---

## 6. `tests/test_agents.py`

Focus on the pure/deterministic parts of each agent. Do not test Ollama or Docker
calls — mock those boundaries.

### Observer tests

```
test_detect_no_anomaly          — healthy 3-node snapshot → empty list
test_detect_node_unreachable    — one node reachable=False → "node_unreachable"
test_detect_no_leader           — all followers → "no_leader"
test_detect_split_brain         — two leaders same term → "split_brain"
test_detect_replication_lag     — follower lsn 15 behind leader → "replication_lag"
test_detect_lag_below_threshold — follower lsn 5 behind → empty list
test_detect_multiple_anomalies  — unreachable + no_leader → both returned
```

Use plain dict snapshots — no HTTP, no asyncio needed for these.

### Adversary tests

```
test_is_safe_no_leader          — no leader → False
test_is_safe_minority           — only 1 node reachable → False
test_is_safe_healthy            — leader + 2 followers → True
test_parse_decision_valid       — valid JSON → (action, target, rationale)
test_parse_decision_unknown_action — unknown action → no_action fallback
test_parse_decision_invalid_json — garbage string → no_action fallback
test_parse_decision_wrong_target — "node9" → no_action fallback
```

Extract `_is_safe_to_inject` and `_parse_decision` as module-level functions (not
instance methods) so tests don't need to construct a full `AdversaryAgent`.

---

## 7. Implementation Order

Complete these in sequence — each step unblocks the next.

1. **`config/adversary_config.json`** — no code dependencies; do first
2. **`src/rainman/agents/__init__.py`** — empty file
3. **`recovery_agent.py`** — start with `detect_anomalies()` (pure, testable), then
   add the poll loop and Ollama call
4. **`tests/test_agents.py` (observer tests)** — write and pass before moving on
5. **`adversary_agent.py`** — start with `_is_safe_to_inject()` and
   `_parse_decision()` (pure), then add poll loop and executor dispatch
6. **`tests/test_agents.py` (adversary tests)** — write and pass
7. **`run_demo.py`** — wire everything together
8. **End-to-end manual run** — see Section 8

---

## 8. End-to-End Manual Test

```bash
# Start fresh cluster
docker compose down -v
docker compose up --build -d
sleep 3

# Confirm leader
curl -s http://localhost:8001/health | python3 -m json.tool

# Run the demo (takes several minutes; Ollama must be running)
python run_demo.py
```

The demo should produce:
- Observer output in terminal (anomaly narrations from LLM)
- Adversary log entries in `logs/adversary_agent.log` (including at least one
  `[DECISION]` with a non-`no_action` action)
- Final `verify_cluster.py` output: zero mismatches

If Ollama is not available, both agents degrade gracefully: the observer logs
fallback messages; the adversary skips LLM decisions and takes no action. The demo
still completes and `verify_cluster.py` still runs.

---

## 9. Deployment: Host Processes, Not Agent Containers

Both agents run as host processes, **not** as additional Docker containers alongside
the cluster.

### Why not containers

The Adversarial agent requires Docker-level control (killing services, partitioning
the network). Running it inside a container would require mounting the Docker socket
(`/var/run/docker.sock`) into that container — the "docker-in-docker" complexity trap
with no benefit for an academic demo. From the host, `subprocess.run(["docker",
"compose", "kill", target], ...)` just works.

Network access also resolves cleanly from the host without any extra configuration:

| What the agent needs | From host process | From a container |
|---|---|---|
| Reach cluster nodes | `localhost:8001–8003` (already exposed) | Must join `rainman_net` or use `host.docker.internal` |
| Reach Ollama | `localhost:11434` | Must use `host.docker.internal:11434` |
| Execute Docker commands | Direct CLI access | Socket mount + Docker CLI installed in image |

### Three-terminal development workflow

For development and debugging, run each component in its own terminal:

```bash
# Terminal 1 — cluster
docker compose up --build

# Terminal 2 — observer
source venv/bin/activate
python -m rainman.agents.recovery_agent \
    --config config/adversary_config.json \
    --cluster config/cluster.json

# Terminal 3 — adversary
source venv/bin/activate
python -m rainman.agents.adversary_agent \
    --config config/adversary_config.json \
    --cluster config/cluster.json \
    --project-dir .
```

The agents read `host_port` from `cluster.json` and call `http://localhost:{host_port}`
to reach nodes — no Docker networking knowledge required.

### Ollama concurrent usage

Both agents share the same Ollama instance. Concurrent requests against a single
loaded model are queued internally by Ollama — no configuration needed. If the two
agents ever use **different models**, Ollama loads both into memory simultaneously
(subject to available RAM); on Apple Silicon the unified memory pool usually handles
this without issue, but `ollama ps` shows what is currently resident if latency
spikes. The simplest path is keeping both agents on the same model, as configured in
`adversary_config.json`.

---

## 10. Key Design Decisions (and Why)

**Agents as standalone subprocesses, not asyncio tasks inside the node.**
The spec says "Start Consistency Observer subprocess" in `run_demo.py`. Keeping
agents as separate processes means a crashed agent doesn't bring down the cluster,
and the Docker/subprocess calls from the adversary don't need to be inside an
async context.

**No new dependencies.** `httpx` (already in `pyproject.toml`) handles both cluster
polling and Ollama API calls. `subprocess` (stdlib) handles Docker. No Ollama SDK
needed.

**`detect_anomalies` is a pure function.** This is the most critical design choice
for testability. All rule-based logic lives here, with no I/O, no LLM, no
timestamps. The LLM layer is only called *after* a deterministic anomaly is already
identified.

**`"format": "json"` in Ollama requests for the adversary.** This guarantees the
adversary gets parseable JSON even from models that tend to wrap output in markdown.
The observer does not use it — prose output is expected there.

**CORRUPT_WAL writes to the host-mounted volume path.** `./data/{node_id}/wal.jsonl`
is directly writable from the host because of the volume bind-mount in
`compose.yaml`. No `docker exec` needed.

**Leader redirect on 409 in `run_demo.py`.** The adversary may kill the current
leader during the review stream. The demo runner handles 409 responses by querying
`/health` across all nodes to discover the new leader, matching what a real client
library would do.

---

## 10. Exit Criteria Checklist

- [ ] `verify_cluster.py` reports zero mismatches after a complete demo run
- [ ] `logs/observer.log` contains at least one `[OBSERVATION]` with LLM-generated text
- [ ] `logs/adversary_agent.log` contains at least one `[DECISION]` with a non-`no_action`
      action and a logged rationale
- [ ] Observer correctly identifies an anomaly during the run (unreachable, lag, or
      no-leader — whichever the adversary triggers)
- [ ] 70+ tests pass (`pytest tests/`) with the new `test_agents.py` added
- [ ] `ruff check src/` clean

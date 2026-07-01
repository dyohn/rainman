#!/usr/bin/env python3
"""End-to-end demo orchestrator for the Rainman cluster.

Runs the full fault/observe cycle:
  1. Preflight — verify cluster is up and a leader has emerged
  2. Oracle   — build expected_state.json if absent
  3. Load     — stream businesses.jsonl into the leader
  4. Observer — start Consistency Observer subprocess
  5. Adversary— start Adversary Fault Injector subprocess
  6. Reviews  — stream reviews.jsonl into the leader (with redirect)
  7. Converge — wait for all live nodes to agree on LSN
  8. Verify   — run verify_cluster.py and print result
  9. Shutdown — SIGTERM then SIGKILL both agent subprocesses

Usage:
    python run_demo.py
"""

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).parent
CLUSTER_PATH = ROOT / "config" / "cluster.json"
CONFIG_PATH = ROOT / "config" / "adversary_config.json"
ORACLE_PATH = ROOT / "data" / "expected_state.json"
BUSINESS_FILE = ROOT / "data" / "sample" / "businesses.jsonl"
REVIEW_FILE = ROOT / "data" / "sample" / "reviews.jsonl"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_nodes() -> list[dict]:
    """Return the node list from cluster.json."""
    with open(CLUSTER_PATH) as f:
        return json.load(f)["nodes"]


def _url(node: dict) -> str:
    """Return the base URL for a node using its host_port."""
    return f"http://localhost:{node['host_port']}"


def _health(node: dict) -> dict | None:
    """GET /health for a node; return parsed JSON or None on any error."""
    try:
        resp = httpx.get(f"{_url(node)}/health", timeout=3.0)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return None


def _find_leader(nodes: list[dict]) -> dict | None:
    """Poll all nodes; return config dict of the current leader or None."""
    for node in nodes:
        h = _health(node)
        if h and h.get("role") == "leader":
            return node
    return None


# ---------------------------------------------------------------------------
# Step 1 — Preflight
# ---------------------------------------------------------------------------


def preflight(nodes: list[dict]) -> dict:
    """Verify cluster is up and a leader has emerged.

    Exits if fewer than 2 nodes respond.  Polls for a leader up to 5 s.
    Returns the leader node config dict.
    Raises SystemExit if no leader emerges within the deadline.
    """
    print("==> Preflight: checking cluster health …")
    live = [n for n in nodes if _health(n) is not None]
    if len(live) < 2:
        raise SystemExit(
            f"Only {len(live)}/3 nodes reachable. "
            "Run: docker compose up --build -d"
        )
    print(f"    {len(live)}/3 nodes reachable")

    deadline = time.monotonic() + 5.0
    leader_node = None
    while time.monotonic() < deadline:
        leader_node = _find_leader(nodes)
        if leader_node:
            break
        time.sleep(0.5)

    if leader_node is None:
        raise SystemExit(
            "No leader emerged within 5 s. Check cluster logs."
        )

    h = _health(leader_node)
    print(
        f"    Leader: {h['node_id']}"
        f" (term={h['term']}, lsn={h['lsn']})"
    )
    return leader_node


# ---------------------------------------------------------------------------
# Step 2 — Oracle
# ---------------------------------------------------------------------------


def build_oracle_if_needed() -> None:
    """Run build_oracle.py if expected_state.json is absent.

    Raises SystemExit if the script fails.
    """
    if ORACLE_PATH.exists():
        print("==> Oracle: expected_state.json already present")
        return
    print("==> Oracle: building expected_state.json …")
    result = subprocess.run(
        [sys.executable, "scripts/build_oracle.py"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise SystemExit(
            f"build_oracle.py failed:\n{result.stdout}\n{result.stderr}"
        )
    print("    Oracle built.")


# ---------------------------------------------------------------------------
# Step 3 — Bulk load businesses
# ---------------------------------------------------------------------------


def load_businesses(leader_node: dict) -> None:
    """Stream businesses.jsonl into the leader; print progress every 100.

    Raises SystemExit if businesses.jsonl is missing.
    """
    if not BUSINESS_FILE.exists():
        raise SystemExit(
            f"Missing: {BUSINESS_FILE}\n"
            "Run scripts/sample_data.py first."
        )
    print("==> Loading businesses …")
    ok = 0
    failed = 0
    with httpx.Client(base_url=_url(leader_node), timeout=10.0) as client:
        with open(BUSINESS_FILE) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                biz = json.loads(line)
                bid = biz["business_id"]
                try:
                    resp = client.put(f"/kv/{bid}", json={"value": biz})
                    resp.raise_for_status()
                    ok += 1
                except httpx.HTTPError as exc:
                    print(
                        f"    FAILED {bid}: {exc}", file=sys.stderr
                    )
                    failed += 1
                if ok % 100 == 0 and ok > 0:
                    print(f"    {ok} businesses loaded …")
    print(f"    Done. {ok} succeeded, {failed} failed.")


# ---------------------------------------------------------------------------
# Step 6 — Stream reviews
# ---------------------------------------------------------------------------


def stream_reviews(
    nodes: list[dict], leader_node: dict
) -> None:
    """Write reviews.jsonl into the leader with 409 redirect and 503 retry.

    On 409 (not leader): re-queries all nodes to find the new leader.
    On 503 (majority ack failed): retries up to 3 times with 1 s backoff.
    Records that still fail after retries are skipped with a warning.
    Raises SystemExit if reviews.jsonl or businesses.jsonl is missing.
    """
    for path in (BUSINESS_FILE, REVIEW_FILE):
        if not path.exists():
            raise SystemExit(
                f"Missing: {path}\n"
                "Run scripts/sample_data.py first."
            )
    print("==> Streaming reviews …")

    # Rebuild in-memory business state to compute running averages
    # locally — mirrors build_oracle.py exactly (DDIA §3: deterministic).
    state: dict[str, dict] = {}
    with open(BUSINESS_FILE) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            biz = json.loads(line)
            state[biz["business_id"]] = dict(biz)

    current_leader = leader_node
    ok = 0
    skipped = 0

    with open(REVIEW_FILE) as f:
        for lineno, raw in enumerate(f, start=1):
            raw = raw.strip()
            if not raw:
                continue
            review = json.loads(raw)
            bid = review.get("business_id")
            if bid not in state:
                continue

            # Running weighted average — mirrors build_oracle.py exactly.
            old_stars = state[bid]["stars"]
            old_count = state[bid]["review_count"]
            new_count = old_count + 1
            state[bid]["stars"] = (
                old_stars * old_count + review["stars"]
            ) / new_count
            state[bid]["review_count"] = new_count

            retries = 0
            success = False
            while retries <= 3 and not success:
                url = _url(current_leader)
                try:
                    resp = httpx.put(
                        f"{url}/kv/{bid}",
                        json={"value": state[bid]},
                        timeout=10.0,
                    )
                    if resp.status_code == 200:
                        ok += 1
                        success = True
                    elif resp.status_code == 409:
                        new_leader = _find_leader(nodes)
                        if new_leader:
                            current_leader = new_leader
                        else:
                            time.sleep(1.0)
                        retries += 1
                    elif resp.status_code == 503:
                        retries += 1
                        if retries <= 3:
                            time.sleep(1.0)
                    else:
                        retries += 1
                except httpx.HTTPError:
                    new_leader = _find_leader(nodes)
                    if new_leader:
                        current_leader = new_leader
                    else:
                        time.sleep(1.0)
                    retries += 1

            if not success:
                print(
                    f"    SKIPPED line {lineno} ({bid}) after retries",
                    file=sys.stderr,
                )
                skipped += 1

            if ok % 500 == 0 and ok > 0:
                print(f"    {ok} reviews applied …")

    print(f"    Done. {ok} reviews applied, {skipped} skipped.")


# ---------------------------------------------------------------------------
# Step 7 — Convergence
# ---------------------------------------------------------------------------


def wait_convergence(
    nodes: list[dict], timeout_s: float = 60.0
) -> None:
    """Poll /health until all reachable nodes agree on LSN or timeout.

    Convergence = all reachable nodes report the same LSN.
    DDIA §5: after a fault/recovery cycle all replicas must converge.
    """
    print("==> Waiting for convergence …")
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        healths = [_health(n) for n in nodes]
        live = [(n, h) for n, h in zip(nodes, healths) if h is not None]
        lsns = {h["lsn"] for _, h in live}
        if len(lsns) == 1:
            lsn = next(iter(lsns))
            print(
                f"    Converged: {len(live)} live nodes at LSN {lsn}"
            )
            for n, h in live:
                print(
                    f"    {h['node_id']}: role={h['role']}"
                    f" term={h['term']} lsn={h['lsn']}"
                )
            return
        time.sleep(2.0)

    print("    Convergence timeout — final per-node state:")
    for node in nodes:
        h = _health(node)
        if h:
            print(
                f"    {h['node_id']}: role={h['role']}"
                f" term={h['term']} lsn={h['lsn']}"
            )
        else:
            print(f"    {node['node_id']}: unreachable")


# ---------------------------------------------------------------------------
# Step 8 — Verify
# ---------------------------------------------------------------------------


def verify() -> bool:
    """Run verify_cluster.py and return True if it passes."""
    print("==> Running verify_cluster.py …")
    result = subprocess.run(
        [sys.executable, "scripts/verify_cluster.py"],
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.stderr:
        print(result.stderr, file=sys.stderr)
    return result.returncode == 0


# ---------------------------------------------------------------------------
# Step 9 — Shutdown
# ---------------------------------------------------------------------------


def shutdown_agents(procs: list[subprocess.Popen]) -> None:
    """SIGTERM each process; SIGKILL if not exited within 5 seconds."""
    for proc in procs:
        if proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
    deadline = time.monotonic() + 5.0
    for proc in procs:
        remaining = max(0.0, deadline - time.monotonic())
        try:
            proc.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            proc.kill()


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def main() -> None:
    """Run the full Rainman demo end to end."""
    os.makedirs("logs", exist_ok=True)
    nodes = _load_nodes()

    # 1. Preflight
    leader_node = preflight(nodes)

    # 2. Oracle
    build_oracle_if_needed()

    # 3. Bulk load
    load_businesses(leader_node)

    # 4. Start observer
    print("==> Starting Consistency Observer …")
    observer_proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "rainman.agents.recovery_agent",
            "--config",
            str(CONFIG_PATH),
            "--cluster",
            str(CLUSTER_PATH),
        ],
        stdout=sys.stdout,
        stderr=sys.stderr,
    )

    # 5. Start adversary
    print("==> Starting Adversary Fault Injector …")
    adversary_proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "rainman.agents.adversary_agent",
            "--config",
            str(CONFIG_PATH),
            "--cluster",
            str(CLUSTER_PATH),
            "--project-dir",
            str(ROOT),
        ],
        stdout=sys.stdout,
        stderr=sys.stderr,
    )
    agent_procs = [observer_proc, adversary_proc]

    passed = False
    try:
        # 6. Stream reviews
        stream_reviews(nodes, leader_node)

        # 7. Convergence
        wait_convergence(nodes)

        # 8. Verify
        passed = verify()

    finally:
        # 9. Shutdown agents
        print("==> Shutting down agents …")
        shutdown_agents(agent_procs)
        print("    Agents stopped.")

    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()

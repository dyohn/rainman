"""Adversary Fault Injector agent for the Rainman cluster.

Polls cluster state, asks a local Ollama LLM to choose a fault,
executes it via Docker or HTTP, and logs every decision.
DDIA §8: deliberate fault injection exposes failure-mode behaviour.
"""

import argparse
import asyncio
import collections
import datetime
import json
import subprocess
import sys
import time
from pathlib import Path

import httpx


_VALID_TARGETS = {"node1", "node2", "node3"}


def _now_iso() -> str:
    """Return current UTC time in ISO 8601 to millisecond precision."""
    return (
        datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%S.%f"
        )[:-3]
        + "Z"
    )


def _is_safe_to_inject(snapshots: list[dict]) -> bool:
    """Return True only when the cluster can absorb a fault safely.

    Safe requires: at least one reachable leader AND at least 2 of 3
    nodes reachable (majority quorum).  Injecting without a leader or
    majority could cause unrecoverable split-brain.
    DDIA §5: majority quorum must be maintained for writes to succeed.
    """
    reachable = [s for s in snapshots if s["reachable"]]
    has_leader = any(s.get("role") == "leader" for s in reachable)
    return has_leader and len(reachable) >= 2


def _parse_decision(raw: str, valid_actions: set[str]) -> dict:
    """Parse and validate a JSON decision string from the LLM.

    Returns a dict with keys action, target, rationale.  Falls back to
    no_action if the JSON is unparseable, the action is unknown, or the
    target is not node1/node2/node3/null.

    raw: raw string from Ollama response field
    valid_actions: set of known fault IDs plus "no_action"
    """
    fallback: dict = {
        "action": "no_action",
        "target": None,
        "rationale": "",
    }
    try:
        decision = json.loads(raw)
    except (json.JSONDecodeError, ValueError):
        return fallback

    action = decision.get("action", "no_action")
    target = decision.get("target")
    rationale = str(decision.get("rationale", ""))

    if action not in valid_actions:
        return {**fallback, "rationale": f"unknown action: {action}"}

    if target is not None and target not in _VALID_TARGETS:
        return {**fallback, "rationale": f"invalid target: {target}"}

    return {"action": action, "target": target, "rationale": rationale}


# ---------------------------------------------------------------------------
# Fault executors
# ---------------------------------------------------------------------------


def _compose(args_list: list[str], project_dir: str) -> bool:
    """Run a docker compose subcommand; return True on success."""
    result = subprocess.run(
        ["docker", "compose"] + args_list,
        cwd=project_dir,
        capture_output=True,
        timeout=10,
    )
    if result.returncode != 0:
        print(
            f"[WARN] docker compose {args_list} failed: "
            f"{result.stderr.decode()[:200]}",
            file=sys.stderr,
        )
    return result.returncode == 0


def _docker(args_list: list[str], project_dir: str) -> bool:
    """Run a bare docker command; return True on success."""
    result = subprocess.run(
        ["docker"] + args_list,
        cwd=project_dir,
        capture_output=True,
        timeout=10,
    )
    if result.returncode != 0:
        print(
            f"[WARN] docker {args_list} failed: "
            f"{result.stderr.decode()[:200]}",
            file=sys.stderr,
        )
    return result.returncode == 0


def _exec_kill(
    target: str,
    _snapshots: list[dict],
    project_dir: str,
    _node_urls: list[str],
) -> bool:
    """Hard-kill a container via docker compose kill."""
    return _compose(["kill", target], project_dir)


def _exec_restart(
    target: str,
    _snapshots: list[dict],
    project_dir: str,
    _node_urls: list[str],
) -> bool:
    """Restart a container via docker compose restart."""
    return _compose(["restart", target], project_dir)


def _exec_pause(
    target: str,
    _snapshots: list[dict],
    project_dir: str,
    _node_urls: list[str],
) -> bool:
    """Freeze a container via docker compose pause."""
    return _compose(["pause", target], project_dir)


def _exec_resume(
    target: str,
    _snapshots: list[dict],
    project_dir: str,
    _node_urls: list[str],
) -> bool:
    """Unfreeze a container via docker compose unpause."""
    return _compose(["unpause", target], project_dir)


def _exec_partition(
    target: str,
    _snapshots: list[dict],
    project_dir: str,
    _node_urls: list[str],
) -> bool:
    """Disconnect a container from rainman_net (real network isolation)."""
    return _docker(
        ["network", "disconnect", "rainman_net", target], project_dir
    )


def _exec_heal(
    target: str,
    _snapshots: list[dict],
    project_dir: str,
    _node_urls: list[str],
) -> bool:
    """Reconnect a container to rainman_net."""
    return _docker(
        ["network", "connect", "rainman_net", target], project_dir
    )


def _target_url(target: str, node_urls: list[str]) -> str | None:
    """Return the base URL for target node, or None if target is invalid."""
    try:
        idx = int(target[-1]) - 1  # "node1" → 0, "node2" → 1, etc.
    except (ValueError, IndexError):
        return None
    if idx < 0 or idx >= len(node_urls):
        return None
    return node_urls[idx]


def _exec_inject_delay(
    target: str,
    _snapshots: list[dict],
    _project_dir: str,
    node_urls: list[str],
) -> bool:
    """POST /admin/inject_delay to add 500 ms of artificial delay."""
    url = _target_url(target, node_urls)
    if url is None:
        return False
    try:
        resp = httpx.post(
            f"{url}/admin/inject_delay",
            json={"delay_ms": 500},
            timeout=5.0,
        )
        return resp.status_code == 200
    except Exception:
        return False


def _exec_clear_delay(
    target: str,
    _snapshots: list[dict],
    _project_dir: str,
    node_urls: list[str],
) -> bool:
    """POST /admin/inject_delay with delay_ms=0 to clear injected delay."""
    url = _target_url(target, node_urls)
    if url is None:
        return False
    try:
        resp = httpx.post(
            f"{url}/admin/inject_delay",
            json={"delay_ms": 0},
            timeout=5.0,
        )
        return resp.status_code == 200
    except Exception:
        return False


def _exec_corrupt_wal(
    target: str,
    _snapshots: list[dict],
    project_dir: str,
    _node_urls: list[str],
) -> bool:
    """Append malformed JSON to the target node's WAL file.

    The WAL lives at {project_dir}/data/{target}/wal.jsonl via the
    volume bind-mount in compose.yaml — no docker exec needed.
    DDIA §3: WAL corruption tests startup hardening.
    """
    wal_path = Path(project_dir) / "data" / target / "wal.jsonl"
    try:
        with open(wal_path, "ab") as f:
            f.write(b"\x00{invalid\n")
        return True
    except Exception as exc:
        print(f"[WARN] CORRUPT_WAL failed: {exc}", file=sys.stderr)
        return False


_EXECUTORS: dict = {
    "KILL_NODE": _exec_kill,
    "RESTART_NODE": _exec_restart,
    "PAUSE_NODE": _exec_pause,
    "RESUME_NODE": _exec_resume,
    "PARTITION_NODE": _exec_partition,
    "HEAL_PARTITION": _exec_heal,
    "INJECT_DELAY": _exec_inject_delay,
    "CLEAR_DELAY": _exec_clear_delay,
    "CORRUPT_WAL": _exec_corrupt_wal,
}


class AdversaryAgent:
    """LLM-driven fault injector for the Rainman cluster.

    Polls cluster state, calls Ollama to choose a fault, executes it,
    and logs every decision with ISO 8601 timestamps.
    DDIA §8: deliberate faults test failure-mode correctness.
    """

    def __init__(
        self,
        config: dict,
        node_urls: list[str],
        project_dir: str,
    ) -> None:
        """Initialise with parsed config, node URLs, and project root.

        config: full parsed adversary_config.json dict
        node_urls: base URLs ordered node1, node2, node3
        project_dir: repo root path (used as cwd for subprocess calls)
        """
        self._ollama = config["ollama"]
        self._cfg = config["adversary"]
        self._faults: list[dict] = config["faults"]
        self._node_urls = node_urls
        self._project_dir = project_dir
        self._poll_interval: float = self._cfg.get("poll_interval_s", 5.0)
        self._min_fault_interval: float = self._cfg.get(
            "min_fault_interval_s", 15.0
        )
        log_path = Path(
            self._cfg.get("log_path", "logs/adversary_agent.log")
        )
        log_path.parent.mkdir(parents=True, exist_ok=True)
        self._log_file = open(log_path, "a", buffering=1)  # noqa: SIM115
        self._last_fault_time: float = 0.0
        self._fault_history: collections.deque = collections.deque(
            maxlen=10
        )
        self._valid_actions: set[str] = (
            {f["id"] for f in self._faults} | {"no_action"}
        )

    def _log(self, message: str) -> None:
        """Write a timestamped line to adversary_agent.log and stdout."""
        line = f"{_now_iso()} {message}"
        print(line, flush=True)
        self._log_file.write(line + "\n")

    async def _poll_cluster(self) -> list[dict]:
        """Collect /health from all nodes; return NodeSnapshot list."""
        snapshots: list[dict] = []
        async with httpx.AsyncClient() as client:
            for i, url in enumerate(self._node_urls):
                node_id = f"node{i + 1}"
                try:
                    resp = await client.get(
                        f"{url}/health", timeout=2.0
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        snapshots.append(
                            {
                                "node_id": data.get("node_id", node_id),
                                "reachable": True,
                                "role": data.get("role"),
                                "term": data.get("term"),
                                "lsn": data.get("lsn"),
                                "leader_id": data.get("leader_id"),
                                "timestamp": data.get("timestamp"),
                            }
                        )
                        continue
                except Exception:
                    pass
                snapshots.append(
                    {
                        "node_id": node_id,
                        "reachable": False,
                        "role": None,
                        "term": None,
                        "lsn": None,
                        "leader_id": None,
                        "timestamp": None,
                    }
                )
        return snapshots

    async def _decide(self, snapshots: list[dict]) -> dict:
        """Ask Ollama to choose a fault; validate and return the decision.

        Falls back to no_action if Ollama is unavailable or returns
        invalid JSON, so the agent keeps running without LLM access.
        """
        cluster_json = json.dumps(
            [
                {k: v for k, v in s.items() if k != "timestamp"}
                for s in snapshots
            ],
            indent=2,
        )
        fault_catalog = json.dumps(self._faults, indent=2)
        history = json.dumps(list(self._fault_history)[-5:], indent=2)
        prompt = (
            "You are an adversarial fault injector testing a 3-node"
            " Raft key-value cluster. Decide whether to inject a fault"
            " now. If yes, choose one fault and one target."
            " Respond ONLY with a JSON object — no prose, no markdown."
            '\n\nSchema: {"action": "<FAULT_ID or no_action>",'
            ' "target": "<node1|node2|node3|null>",'
            ' "rationale": "<one sentence>"}'
            f"\n\nAvailable faults:\n{fault_catalog}"
            f"\n\nCurrent cluster state:\n{cluster_json}"
            f"\n\nRecent fault history (last 5):\n{history}"
        )
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    f"{self._ollama['base_url']}/api/generate",
                    json={
                        "model": self._ollama["model"],
                        "stream": False,
                        "format": "json",
                        "prompt": prompt,
                    },
                    timeout=float(self._ollama.get("timeout_s", 30)),
                )
                resp.raise_for_status()
                raw = resp.json().get("response", "")
                return _parse_decision(raw, self._valid_actions)
        except Exception as exc:
            self._log(f"[WARN] Ollama unavailable: {exc}")
            return {
                "action": "no_action",
                "target": None,
                "rationale": "",
            }

    async def _execute(
        self, decision: dict, snapshots: list[dict]
    ) -> bool:
        """Dispatch the chosen fault to its executor; return success."""
        executor = _EXECUTORS.get(decision["action"])
        if executor is None:
            return False
        return executor(
            decision["target"],
            snapshots,
            self._project_dir,
            self._node_urls,
        )

    def _log_decision(
        self,
        snapshots: list[dict],
        decision: dict,
        executed: bool,
    ) -> None:
        """Write poll state and decision to the adversary log."""
        state = {
            s["node_id"]: {
                "role": s.get("role"),
                "term": s.get("term"),
                "lsn": s.get("lsn"),
            }
            for s in snapshots
        }
        self._log(f"[POLL] {json.dumps(state)}")
        if decision["action"] != "no_action":
            self._log(
                f"[DECISION] action={decision['action']}"
                f" target={decision['target']}"
                f" rationale=\"{decision['rationale']}\""
            )
            self._log(
                f"[EXECUTED] {decision['action']}"
                f" {decision['target']} success={executed}"
            )

    async def run(self) -> None:
        """Main poll loop; runs until cancelled."""
        self._log("[START] Adversary Agent started")
        try:
            while True:
                snapshots = await self._poll_cluster()

                if not _is_safe_to_inject(snapshots):
                    await asyncio.sleep(self._poll_interval)
                    continue

                now = time.monotonic()
                if now - self._last_fault_time < self._min_fault_interval:
                    await asyncio.sleep(self._poll_interval)
                    continue

                decision = await self._decide(snapshots)
                executed = False

                if decision["action"] != "no_action":
                    executed = await self._execute(decision, snapshots)
                    if executed:
                        self._last_fault_time = time.monotonic()
                        self._fault_history.append(
                            {
                                "timestamp": _now_iso(),
                                "action": decision["action"],
                                "target": decision["target"],
                            }
                        )

                self._log_decision(snapshots, decision, executed)
                await asyncio.sleep(self._poll_interval)
        except asyncio.CancelledError:
            self._log("[STOP] Adversary Agent stopped")
        finally:
            self._log_file.close()


def _build_node_urls(cluster_cfg: dict) -> list[str]:
    """Build localhost base URLs from host_port fields in cluster.json."""
    return [
        f"http://localhost:{n['host_port']}"
        for n in cluster_cfg["nodes"]
    ]


def main() -> None:
    """Parse CLI args and run the Adversary Fault Injector.

    Raises SystemExit on missing config or cluster files.
    """
    parser = argparse.ArgumentParser(
        description="Rainman Adversary Fault Injector"
    )
    parser.add_argument(
        "--config",
        default="config/adversary_config.json",
        help="Path to adversary_config.json",
    )
    parser.add_argument(
        "--cluster",
        default="config/cluster.json",
        help="Path to cluster.json",
    )
    parser.add_argument(
        "--project-dir",
        default=".",
        help="Project root (cwd for docker compose commands)",
    )
    args = parser.parse_args()

    with open(args.config) as f:
        config = json.load(f)
    with open(args.cluster) as f:
        cluster_cfg = json.load(f)

    node_urls = _build_node_urls(cluster_cfg)
    agent = AdversaryAgent(config, node_urls, args.project_dir)
    asyncio.run(agent.run())


if __name__ == "__main__":
    main()

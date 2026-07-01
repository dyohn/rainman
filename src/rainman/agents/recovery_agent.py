"""Consistency Observer agent for the Rainman cluster.

Polls all three nodes, detects anomalies with pure rule-based logic,
and narrates each anomaly via a local Ollama LLM call.  Never modifies
cluster state.
DDIA §1: observability is a first-class reliability concern.
"""

import argparse
import asyncio
import datetime
import json
import time
from pathlib import Path

import httpx


def _now_iso() -> str:
    """Return current UTC time in ISO 8601 to millisecond precision."""
    return (
        datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%S.%f"
        )[:-3]
        + "Z"
    )


def detect_anomalies(
    snapshots: list[dict],
    lag_threshold: int = 10,
) -> list[dict]:
    """Detect anomalies in a list of NodeSnapshot dicts.

    Runs four ordered checks: node unreachable, no leader, split-brain,
    and replication lag.  Returns an empty list when the cluster is
    healthy.  Pure function — no I/O, no LLM, no timestamps.
    DDIA §5: split-brain and replication lag are canonical failure modes.

    snapshots: list of NodeSnapshot dicts
    lag_threshold: maximum tolerated LSN gap between leader and follower
    """
    anomalies: list[dict] = []
    reachable = [s for s in snapshots if s["reachable"]]

    # 1. Node unreachable
    for s in snapshots:
        if not s["reachable"]:
            anomalies.append(
                {
                    "type": "node_unreachable",
                    "detail": {"node_id": s["node_id"]},
                }
            )

    # 2. No leader among reachable nodes
    leaders = [s for s in reachable if s.get("role") == "leader"]
    if reachable and not leaders:
        anomalies.append(
            {
                "type": "no_leader",
                "detail": {
                    "reachable_nodes": [s["node_id"] for s in reachable]
                },
            }
        )

    # 3. Split-brain — two or more leaders in the same term.
    # DDIA §5: at most one leader per term is a core Raft invariant.
    if len(leaders) >= 2:
        term_groups: dict[int, list[str]] = {}
        for s in leaders:
            t = s.get("term")
            if t is not None:
                term_groups.setdefault(t, []).append(s["node_id"])
        for term, node_ids in term_groups.items():
            if len(node_ids) >= 2:
                anomalies.append(
                    {
                        "type": "split_brain",
                        "detail": {
                            "term": term,
                            "leaders": node_ids,
                        },
                    }
                )

    # 4. Replication lag
    if leaders:
        leader_lsn = leaders[0].get("lsn") or 0
        for s in reachable:
            if s.get("role") == "leader":
                continue
            follower_lsn = s.get("lsn") or 0
            lag = leader_lsn - follower_lsn
            if lag > lag_threshold:
                anomalies.append(
                    {
                        "type": "replication_lag",
                        "detail": {
                            "node_id": s["node_id"],
                            "lag": lag,
                            "leader_lsn": leader_lsn,
                            "follower_lsn": follower_lsn,
                        },
                    }
                )

    return anomalies


class ConsistencyObserver:
    """Poll the cluster, detect anomalies, and narrate them via LLM.

    Reads node health via GET /health.  For each anomaly found by
    detect_anomalies(), calls Ollama for a human-readable narration.
    Logs everything with ISO 8601 timestamps.
    DDIA §1: reliability requires observability.
    """

    def __init__(self, config: dict, node_urls: list[str]) -> None:
        """Initialise with parsed adversary_config.json and node URLs.

        config: full parsed adversary_config.json dict
        node_urls: list of base URLs, e.g. ["http://localhost:8001", …]
        """
        self._ollama = config["ollama"]
        self._cfg = config["observer"]
        self._node_urls = node_urls
        self._lag_threshold: int = self._cfg.get(
            "replication_lag_threshold", 10
        )
        self._poll_interval: float = self._cfg.get("poll_interval_s", 1.0)
        log_path = Path(self._cfg.get("log_path", "logs/observer.log"))
        log_path.parent.mkdir(parents=True, exist_ok=True)
        self._log_file = open(log_path, "a", buffering=1)  # noqa: SIM115
        self._last_healthy_log: float = 0.0

    def _log(self, message: str) -> None:
        """Write a timestamped line to observer.log and stdout."""
        line = f"{_now_iso()} {message}"
        print(line, flush=True)
        self._log_file.write(line + "\n")

    async def _poll_cluster(self) -> list[dict]:
        """Collect /health from all nodes; return NodeSnapshot list.

        Unreachable nodes get reachable=False with all other fields None.
        """
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

    async def _narrate(
        self, anomaly: dict, snapshots: list[dict]
    ) -> str:
        """Call Ollama and return a natural-language narration.

        Returns a fallback string if Ollama is unavailable or times out,
        so the observer keeps running even without LLM access.
        """
        cluster_json = json.dumps(
            [
                {k: v for k, v in s.items() if k != "timestamp"}
                for s in snapshots
            ],
            indent=2,
        )
        prompt = (
            "You are a distributed systems observability tool monitoring"
            " a 3-node Raft-based key-value cluster. Respond in 2–4"
            " sentences. Be specific about which nodes are affected and"
            " what the observation implies for the cluster's health and"
            " consistency. Do not suggest fixes."
            f"\n\nAnomaly detected: {anomaly['type']}"
            f"\nDetail: {json.dumps(anomaly['detail'])}"
            f"\n\nCurrent cluster state:\n{cluster_json}"
        )
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    f"{self._ollama['base_url']}/api/generate",
                    json={
                        "model": self._ollama["model"],
                        "stream": False,
                        "prompt": prompt,
                    },
                    timeout=float(self._ollama.get("timeout_s", 30)),
                )
                resp.raise_for_status()
                return resp.json().get("response", "").strip()
        except Exception as exc:
            return (
                f"[LLM unavailable] Anomaly: {anomaly['type']} ({exc})"
            )

    async def run(self) -> None:
        """Main poll loop; runs until cancelled.

        Polls at poll_interval_s, logs anomalies and narrations, and
        throttles HEALTHY lines to at most once every 30 seconds.
        """
        self._log("[START] Consistency Observer started")
        try:
            while True:
                snapshots = await self._poll_cluster()
                anomalies = detect_anomalies(
                    snapshots, self._lag_threshold
                )

                if anomalies:
                    for anomaly in anomalies:
                        self._log(
                            f"[ANOMALY] {anomaly['type']} "
                            f"{json.dumps(anomaly['detail'])}"
                        )
                        narration = await self._narrate(
                            anomaly, snapshots
                        )
                        self._log(f"[OBSERVATION] {narration}")
                else:
                    now = time.monotonic()
                    if now - self._last_healthy_log >= 30.0:
                        reachable = [
                            s for s in snapshots if s["reachable"]
                        ]
                        terms = {s.get("term") for s in reachable}
                        lsns = {s.get("lsn") for s in reachable}
                        t = (
                            next(iter(terms))
                            if len(terms) == 1
                            else terms
                        )
                        lsn = (
                            next(iter(lsns))
                            if len(lsns) == 1
                            else lsns
                        )
                        self._log(
                            f"[HEALTHY] All nodes nominal"
                            f" (term={t}, lsn={lsn})"
                        )
                        self._last_healthy_log = now

                await asyncio.sleep(self._poll_interval)
        except asyncio.CancelledError:
            self._log("[STOP] Consistency Observer stopped")
        finally:
            self._log_file.close()


def _build_node_urls(cluster_cfg: dict) -> list[str]:
    """Build localhost base URLs from host_port fields in cluster.json."""
    return [
        f"http://localhost:{n['host_port']}"
        for n in cluster_cfg["nodes"]
    ]


def main() -> None:
    """Parse CLI args and run the Consistency Observer.

    Raises SystemExit on missing config or cluster files.
    """
    parser = argparse.ArgumentParser(
        description="Rainman Consistency Observer"
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
    args = parser.parse_args()

    with open(args.config) as f:
        config = json.load(f)
    with open(args.cluster) as f:
        cluster_cfg = json.load(f)

    node_urls = _build_node_urls(cluster_cfg)
    observer = ConsistencyObserver(config, node_urls)
    asyncio.run(observer.run())


if __name__ == "__main__":
    main()

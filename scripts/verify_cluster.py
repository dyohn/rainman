"""Verify live cluster state against the correctness oracle.

Reads data/expected_state.json (produced by build_oracle.py), then
queries GET /kv/{key} on every cluster node for each key in the oracle
and diffs the results.

Usage:
    python scripts/verify_cluster.py [--tolerance 0.01]

--tolerance applies to all float-valued fields (e.g. star ratings).
Zero mismatches means the cluster passed.

Output:
  - Human-readable summary to stdout
  - Machine-readable logs/verification_{timestamp}.json
"""

import argparse
import asyncio
import datetime
import json
import math
import os
import sys

import httpx

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_ORACLE_PATH = os.path.join(_REPO_ROOT, "data", "expected_state.json")
_CONFIG_PATH = os.path.join(_REPO_ROOT, "config", "cluster.json")
_LOGS_DIR = os.path.join(_REPO_ROOT, "logs")


# ---------------------------------------------------------------------------
# Comparison helpers
# ---------------------------------------------------------------------------


def _floats_close(a: float, b: float, tol: float) -> bool:
    """Return True if abs(a - b) <= tol, treating NaN as unequal."""
    if math.isnan(a) or math.isnan(b):
        return False
    return abs(a - b) <= tol


def _values_match(
    oracle_val: dict,
    node_val: dict,
    tolerance: float,
) -> list[str]:
    """Return a list of field-level mismatch descriptions.

    Compares every key present in the oracle value against the node
    value.  Float fields use tolerance; all other types use equality.
    """
    mismatches: list[str] = []
    for field, expected in oracle_val.items():
        if field not in node_val:
            mismatches.append(f"missing field '{field}'")
            continue
        actual = node_val[field]
        if isinstance(expected, float) or isinstance(actual, float):
            try:
                if not _floats_close(
                    float(expected), float(actual), tolerance
                ):
                    mismatches.append(
                        f"'{field}': expected {expected}, got {actual}"
                    )
            except (TypeError, ValueError):
                mismatches.append(
                    f"'{field}': cannot compare {expected!r} and"
                    f" {actual!r} as floats"
                )
        elif expected != actual:
            mismatches.append(
                f"'{field}': expected {expected!r}, got {actual!r}"
            )
    return mismatches


# ---------------------------------------------------------------------------
# Network helpers
# ---------------------------------------------------------------------------


async def _check_health(client: httpx.AsyncClient, node: dict) -> dict | None:
    """Return the /health response dict, or None if the node is down."""
    url = f"http://localhost:{node['host_port']}/health"
    try:
        resp = await client.get(url, timeout=3.0)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return None


async def _fetch_key(
    client: httpx.AsyncClient,
    node: dict,
    key: str,
) -> dict | None:
    """Return the /kv/{key} response dict, or None if not found / down."""
    url = f"http://localhost:{node['host_port']}/kv/{key}"
    try:
        resp = await client.get(url, timeout=5.0)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Core verification logic
# ---------------------------------------------------------------------------


async def verify(tolerance: float) -> dict:
    """Run the full verification pass against the live cluster.

    Returns a result dict with keys: passed, total_keys, checked_keys,
    missing_keys, value_mismatches, node_divergences, node_health, and
    per_key_errors (list of dicts with key and description).
    Raises FileNotFoundError if oracle or config are absent.
    """
    with open(_ORACLE_PATH) as f:
        oracle = json.load(f)
    with open(_CONFIG_PATH) as f:
        cluster_cfg = json.load(f)

    nodes = cluster_cfg["nodes"]
    oracle_state: dict = oracle["state"]
    total_keys = len(oracle_state)

    per_key_errors: list[dict] = []
    checked_keys = 0

    async with httpx.AsyncClient() as client:
        # -- Health check all nodes ----------------------------------------
        health_results = await asyncio.gather(
            *[_check_health(client, n) for n in nodes]
        )
        node_health = {
            n["node_id"]: (h is not None)
            for n, h in zip(nodes, health_results)
        }
        live_nodes = [
            n for n, h in zip(nodes, health_results) if h is not None
        ]

        if not live_nodes:
            return {
                "passed": False,
                "error": "No nodes reachable",
                "node_health": node_health,
                "total_keys": total_keys,
                "checked_keys": 0,
                "per_key_errors": [],
            }

        print(
            f"Live nodes: "
            f"{[n['node_id'] for n in live_nodes]} / "
            f"{[n['node_id'] for n in nodes]}"
        )

        # -- Key verification (concurrent batches of 50) -------------------
        keys = list(oracle_state.keys())
        batch_size = 50

        for batch_start in range(0, len(keys), batch_size):
            batch = keys[batch_start : batch_start + batch_size]

            # Fetch each key from every live node concurrently
            tasks = [
                _fetch_key(client, n, k) for k in batch for n in live_nodes
            ]
            raw = await asyncio.gather(*tasks)

            # Reshape: raw[i * len(live_nodes) + j] = node j's response
            #          for batch[i]
            n_live = len(live_nodes)
            for i, key in enumerate(batch):
                checked_keys += 1
                responses = {
                    live_nodes[j]["node_id"]: raw[i * n_live + j]
                    for j in range(n_live)
                }
                expected_val = oracle_state[key]

                key_errors: list[str] = []

                # Oracle comparison
                for node_id, resp in responses.items():
                    if resp is None:
                        key_errors.append(
                            f"{node_id}: key missing / node unreachable"
                        )
                        continue
                    mismatches = _values_match(
                        expected_val, resp["value"], tolerance
                    )
                    for m in mismatches:
                        key_errors.append(f"{node_id}: {m}")

                # Cross-node divergence (compare all live node values
                # against each other regardless of oracle)
                node_values = {
                    nid: r["value"]
                    for nid, r in responses.items()
                    if r is not None
                }
                node_ids = list(node_values.keys())
                for a in range(len(node_ids)):
                    for b in range(a + 1, len(node_ids)):
                        na, nb = node_ids[a], node_ids[b]
                        div = _values_match(
                            node_values[na], node_values[nb], tolerance
                        )
                        for d in div:
                            key_errors.append(f"DIVERGENCE {na} vs {nb}: {d}")

                if key_errors:
                    per_key_errors.append({"key": key, "errors": key_errors})

    missing_keys = sum(
        1
        for e in per_key_errors
        if any("missing" in err for err in e["errors"])
    )
    value_mismatches = sum(
        1
        for e in per_key_errors
        if any(
            "missing" not in err and "DIVERGENCE" not in err
            for err in e["errors"]
        )
    )
    node_divergences = sum(
        1
        for e in per_key_errors
        if any("DIVERGENCE" in err for err in e["errors"])
    )

    return {
        "passed": len(per_key_errors) == 0,
        "total_keys": total_keys,
        "checked_keys": checked_keys,
        "missing_keys": missing_keys,
        "value_mismatches": value_mismatches,
        "node_divergences": node_divergences,
        "node_health": node_health,
        "per_key_errors": per_key_errors[:100],  # cap log size
    }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def _print_summary(result: dict) -> None:
    """Print a human-readable summary of the verification result."""
    status = "PASSED" if result["passed"] else "FAILED"
    print(f"\n{'=' * 55}")
    print(f"  Cluster Verification: {status}")
    print(f"{'=' * 55}")
    if "error" in result:
        print(f"  Error: {result['error']}")
        return
    print(
        f"  Keys checked : {result['checked_keys']} / {result['total_keys']}"
    )
    print(f"  Missing keys : {result['missing_keys']}")
    print(f"  Value mismatches : {result['value_mismatches']}")
    print(f"  Node divergences : {result['node_divergences']}")
    print(f"  Node health      : {result['node_health']}")
    if result["per_key_errors"]:
        print(f"\n  First {len(result['per_key_errors'])} key error(s):")
        for e in result["per_key_errors"][:5]:
            print(f"    {e['key']}: {e['errors']}")
    print(f"{'=' * 55}\n")


def main() -> None:
    """Parse arguments, run verification, write report, exit with status."""
    parser = argparse.ArgumentParser(
        description="Verify cluster state against the correctness oracle."
    )
    parser.add_argument(
        "--tolerance",
        type=float,
        default=0.0,
        help=(
            "Absolute tolerance for float field comparison "
            "(default 0.0, use 0.01 for star-rating rounding)."
        ),
    )
    args = parser.parse_args()

    result = asyncio.run(verify(args.tolerance))
    _print_summary(result)

    os.makedirs(_LOGS_DIR, exist_ok=True)
    ts = datetime.datetime.now(datetime.timezone.utc).strftime(
        "%Y%m%dT%H%M%SZ"
    )
    report_path = os.path.join(_LOGS_DIR, f"verification_{ts}.json")
    with open(report_path, "w") as f:
        json.dump(result, f, indent=2)
    print(f"Report written to {report_path}")

    sys.exit(0 if result["passed"] else 1)


if __name__ == "__main__":
    main()

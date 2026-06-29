#!/usr/bin/env python3
"""Pre-compute expected cluster state from sampled data files.

Reads data/sample/{businesses,reviews}.jsonl and writes
data/expected_state.json.  This file is the correctness ground truth
used by scripts/verify_cluster.py.

The oracle models the same PUT semantics as the storage engine: every
business write is a full record overwrite; every review write updates
only stars (running weighted average) and review_count.

DDIA Chapter 3 — Storage and Retrieval: the oracle applies the same
log of operations as the cluster, offline, to produce a known-good
final state.

Usage:
    python scripts/build_oracle.py
"""

import datetime
import json
from pathlib import Path

ROOT = Path(__file__).parent.parent
SAMPLE_DIR = ROOT / "data" / "sample"
DATA_DIR = ROOT / "data"

BUSINESS_FILE = SAMPLE_DIR / "businesses.jsonl"
REVIEW_FILE = SAMPLE_DIR / "reviews.jsonl"
ORACLE_FILE = DATA_DIR / "expected_state.json"


def main() -> None:
    """Build oracle from businesses and reviews; write expected_state.json.

    Raises SystemExit if businesses.jsonl is missing.
    Raises json.JSONDecodeError if any data file contains invalid JSON.
    """
    if not BUSINESS_FILE.exists():
        raise SystemExit(
            f"Missing: {BUSINESS_FILE}\n"
            "Run scripts/sample_data.py first."
        )

    state: dict[str, dict] = {}
    total_writes = 0

    # Initial PUT for each business — full record stored as-is.
    print("Loading businesses…")
    with open(BUSINESS_FILE, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            biz = json.loads(line)
            state[biz["business_id"]] = dict(biz)
            total_writes += 1
    print(f"  → {len(state)} businesses loaded")

    # Apply reviews as sequential PUT updates.
    # Each review recalculates stars as a running weighted average and
    # increments review_count (DDIA §11: ordered event stream).
    review_count = 0
    if REVIEW_FILE.exists():
        print("Applying reviews…")
        with open(REVIEW_FILE, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                review = json.loads(line)
                bid = review.get("business_id")
                if bid not in state:
                    continue
                old_stars = state[bid]["stars"]
                old_count = state[bid]["review_count"]
                new_count = old_count + 1
                new_stars = (
                    old_stars * old_count + review["stars"]
                ) / new_count
                state[bid]["stars"] = new_stars
                state[bid]["review_count"] = new_count
                total_writes += 1
                review_count += 1
        print(f"  → {review_count} reviews applied")

    oracle = {
        "generated_at": (
            datetime.datetime.now(datetime.timezone.utc)
            .strftime("%Y-%m-%dT%H:%M:%S")
            + "Z"
        ),
        "total_keys": len(state),
        "total_writes": total_writes,
        "state": state,
    }

    with open(ORACLE_FILE, "w") as f:
        json.dump(oracle, f, indent=2)

    print(f"\nOracle written to {ORACLE_FILE}")
    print(f"  total_keys   = {oracle['total_keys']}")
    print(f"  total_writes = {oracle['total_writes']}")


if __name__ == "__main__":
    main()

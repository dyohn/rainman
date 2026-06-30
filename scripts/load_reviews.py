#!/usr/bin/env python3
"""Apply sampled review updates to the running cluster.

Reads data/sample/{businesses,reviews}.jsonl and replays the same
running-average star/review_count updates that build_oracle.py applies
offline.  Each review becomes a full PUT /kv/{business_id} of the
updated business record.

Must be run AFTER load_businesses.py so that all base records exist.

Usage:
    python scripts/load_reviews.py [--url http://localhost:8001]
"""

import argparse
import json
import sys
from pathlib import Path

import httpx

ROOT = Path(__file__).parent.parent
SAMPLE_DIR = ROOT / "data" / "sample"
BUSINESS_FILE = SAMPLE_DIR / "businesses.jsonl"
REVIEW_FILE = SAMPLE_DIR / "reviews.jsonl"


def main() -> None:
    """Apply all review updates; exit non-zero if any PUT fails.

    Raises SystemExit if reviews.jsonl or businesses.jsonl is missing,
    or if the cluster leader is unreachable.
    """
    parser = argparse.ArgumentParser(
        description="Apply review updates to the Rainman cluster"
    )
    parser.add_argument(
        "--url",
        default="http://localhost:8001",
        help="Leader base URL (default: http://localhost:8001)",
    )
    args = parser.parse_args()

    for path in (BUSINESS_FILE, REVIEW_FILE):
        if not path.exists():
            raise SystemExit(
                f"Missing: {path}\nRun scripts/sample_data.py first."
            )

    # Rebuild in-memory state from businesses.jsonl (same starting point
    # as build_oracle.py) so we can compute running averages locally
    # without GETting each record from the cluster on every review.
    state: dict[str, dict] = {}
    with open(BUSINESS_FILE) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            biz = json.loads(line)
            state[biz["business_id"]] = dict(biz)

    ok = 0
    failed = 0

    with httpx.Client(base_url=args.url, timeout=10.0) as client:
        with open(REVIEW_FILE) as f:
            for lineno, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                review = json.loads(line)
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

                try:
                    resp = client.put(
                        f"/kv/{bid}", json={"value": state[bid]}
                    )
                    resp.raise_for_status()
                    ok += 1
                except httpx.HTTPError as exc:
                    print(
                        f"  FAILED line {lineno} ({bid}): {exc}",
                        file=sys.stderr,
                    )
                    failed += 1

                if ok % 500 == 0 and ok > 0:
                    print(f"  {ok} reviews applied…")

    print(f"\nDone. {ok} reviews applied, {failed} failed.")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()

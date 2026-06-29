#!/usr/bin/env python3
"""Bulk-load all businesses from data/sample/businesses.jsonl into the node.

Used for the Phase 1 manual exit-criterion test.  Reads every business
record and sends a PUT /kv/{business_id} to the running node.

Usage:
    python scripts/load_businesses.py [--url http://localhost:8000]

Prints a progress count and a final summary of successes / failures.
"""

import argparse
import json
import sys
from pathlib import Path

import httpx

ROOT = Path(__file__).parent.parent
BUSINESS_FILE = ROOT / "data" / "sample" / "businesses.jsonl"


def main() -> None:
    """Load businesses.jsonl into the node; exit non-zero on any failure.

    Raises SystemExit if businesses.jsonl is missing or the node is
    unreachable.
    """
    parser = argparse.ArgumentParser(
        description="Bulk-load businesses into the Rainman node"
    )
    parser.add_argument(
        "--url",
        default="http://localhost:8000",
        help="Node base URL (default: http://localhost:8000)",
    )
    args = parser.parse_args()

    if not BUSINESS_FILE.exists():
        raise SystemExit(
            f"Missing: {BUSINESS_FILE}\n"
            "Run scripts/sample_data.py first."
        )

    ok = 0
    failed = 0

    with httpx.Client(base_url=args.url, timeout=10.0) as client:
        with open(BUSINESS_FILE, "r") as f:
            for lineno, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                biz = json.loads(line)
                bid = biz["business_id"]
                try:
                    resp = client.put(
                        f"/kv/{bid}", json={"value": biz}
                    )
                    resp.raise_for_status()
                    ok += 1
                except httpx.HTTPError as exc:
                    print(
                        f"  FAILED line {lineno} ({bid}): {exc}",
                        file=sys.stderr,
                    )
                    failed += 1

                if ok % 100 == 0 and ok > 0:
                    print(f"  {ok} loaded…")

    print(f"\nDone. {ok} succeeded, {failed} failed.")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()

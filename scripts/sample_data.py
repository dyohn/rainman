#!/usr/bin/env python3
"""Sample a self-consistent subset of the Yelp dataset.

Reads from data/raw/ and writes to data/sample/:
  businesses.jsonl  — N sampled business records (one JSON per line)
  reviews.jsonl     — up to M reviews for those businesses, chronological

Usage:
    python scripts/sample_data.py --businesses 1000 --reviews 5000
"""

import argparse
import json
import random
from pathlib import Path

ROOT = Path(__file__).parent.parent
RAW_DIR = ROOT / "data" / "raw"
SAMPLE_DIR = ROOT / "data" / "sample"

BUSINESS_FILE = RAW_DIR / "yelp_academic_dataset_business.json"
REVIEW_FILE = RAW_DIR / "yelp_academic_dataset_review.json"


def reservoir_sample(filepath: Path, n: int) -> list[dict]:
    """Return n records drawn uniformly at random using reservoir sampling.

    Single-pass; holds at most n records in memory at once (Algorithm R).
    Raises FileNotFoundError if filepath does not exist.
    Raises json.JSONDecodeError if any non-blank line is invalid JSON.
    """
    reservoir: list[dict] = []
    count = 0
    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            if count < n:
                reservoir.append(record)
            else:
                # Replace a random earlier item with probability n/(count+1)
                j = random.randint(0, count)
                if j < n:
                    reservoir[j] = record
            count += 1
    return reservoir


def main() -> None:
    """Parse CLI args, sample businesses, filter reviews, write output."""
    parser = argparse.ArgumentParser(
        description="Sample Yelp dataset to data/sample/"
    )
    parser.add_argument(
        "--businesses",
        type=int,
        default=1000,
        help="Number of businesses to sample (default: 1000)",
    )
    parser.add_argument(
        "--reviews",
        type=int,
        default=5000,
        help="Maximum reviews to include (default: 5000)",
    )
    args = parser.parse_args()

    if not BUSINESS_FILE.exists():
        raise SystemExit(
            f"Missing: {BUSINESS_FILE}\n"
            "Place Yelp dataset files in data/raw/ before running."
        )
    if not REVIEW_FILE.exists():
        raise SystemExit(
            f"Missing: {REVIEW_FILE}\n"
            "Place Yelp dataset files in data/raw/ before running."
        )

    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Sampling {args.businesses} businesses (reservoir pass)…")
    businesses = reservoir_sample(BUSINESS_FILE, args.businesses)
    sampled_ids = {b["business_id"] for b in businesses}

    out_biz = SAMPLE_DIR / "businesses.jsonl"
    with open(out_biz, "w") as f:
        for biz in businesses:
            f.write(json.dumps(biz) + "\n")
    print(f"  → {len(businesses)} businesses written to {out_biz}")

    print("Scanning reviews for sampled businesses…")
    matched: list[dict] = []
    with open(REVIEW_FILE, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            review = json.loads(line)
            if review.get("business_id") in sampled_ids:
                matched.append(review)

    # Sort chronologically then truncate so the oracle sees a consistent
    # time-ordered stream of updates regardless of file ordering.
    matched.sort(key=lambda r: r.get("date", ""))
    matched = matched[: args.reviews]

    out_rev = SAMPLE_DIR / "reviews.jsonl"
    with open(out_rev, "w") as f:
        for review in matched:
            f.write(json.dumps(review) + "\n")

    if matched:
        first = matched[0].get("date", "?")
        last = matched[-1].get("date", "?")
        date_range = f"{first} – {last}"
    else:
        date_range = "n/a"

    print(f"  → {len(matched)} reviews written to {out_rev}")
    print(f"  → Date range: {date_range}")


if __name__ == "__main__":
    main()

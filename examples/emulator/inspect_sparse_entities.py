"""Inspect raw emulator entities to prove sparse DataFrame cells were omitted."""

from __future__ import annotations

import argparse
from collections import Counter

from common import client


OPTIONAL_PROPERTIES = (
    "pool_length_m",
    "stroke",
    "bike_power_w",
    "bike_trainer",
    "run_cadence_spm",
    "shoe_model",
    "notes",
    "last_reviewed_at",
)


def run(*, limit: int, namespace: str) -> None:
    ds = client()
    query = ds.query(kind="Workout", namespace=namespace)
    counts = Counter()
    total = 0
    for entity in query.fetch(limit=limit):
        total += 1
        for name in OPTIONAL_PROPERTIES:
            if name in entity:
                counts[name] += 1

    print(f"inspected raw entities={total:,} namespace={namespace}")
    for name in OPTIONAL_PROPERTIES:
        print(f"{name:18s} present_on={counts[name]:,}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=2_000)
    parser.add_argument("--namespace", default="tenant-a")
    args = parser.parse_args()
    run(limit=args.limit, namespace=args.namespace)


if __name__ == "__main__":
    main()

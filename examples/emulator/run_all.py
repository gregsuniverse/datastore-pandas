"""Run the end-to-end Datastore emulator example suite."""

from __future__ import annotations

import argparse
from pathlib import Path

from common import DEFAULT_DATA_FILE
from generate_mock_data import generate_workouts
from inspect_sparse_entities import run as inspect_sparse
from load_mock_data import load
from patch_sparse_rows import run as run_patch
from query_examples import run as run_queries
from transaction_example import run as run_transaction


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=20_000)
    parser.add_argument("--users", type=int, default=1_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out", type=Path, default=DEFAULT_DATA_FILE)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--batch-size", type=int, default=400)
    parser.add_argument("--user-id", default="user-00042")
    parser.add_argument("--tenant", default="tenant-a")
    args = parser.parse_args()

    args.out.parent.mkdir(parents=True, exist_ok=True)
    df = generate_workouts(args.rows, users=args.users, seed=args.seed)
    df.to_csv(args.out, index=False)
    print(f"generated {len(df):,} rows at {args.out}")

    report = load(args.out, workers=args.workers, batch_size=args.batch_size)
    report.raise_for_errors()

    run_queries(args.user_id, tenant=args.tenant, limit=8)
    run_patch(args.user_id, tenant=args.tenant)
    run_transaction(tenant=args.tenant, counter_name="run-all-counter", increments=5)
    inspect_sparse(limit=min(args.rows, 2_000), namespace=args.tenant)


if __name__ == "__main__":
    main()

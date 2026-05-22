"""Read examples: full entities, projections, keys-only scans, and distinct projections."""

from __future__ import annotations

import argparse

from common import BACKENDS, Backend, WORKOUT_SCHEMA, adapter, ancestor_key, client, print_frame


def run(user_id: str, *, tenant: str, limit: int, backend: Backend = "pandas") -> None:
    ds = client()
    dsp = adapter(backend)
    ancestor = ancestor_key(user_id, tenant=tenant)

    full = dsp.read_datastore(
        kind="Workout",
        schema=WORKOUT_SCHEMA,
        client=ds,
        ancestor=ancestor,
        order=["-started_at"],
        limit=limit,
        include_key=True,
    )
    print_frame("Full ancestor query", full)

    projected = dsp.read_datastore(
        kind="Workout",
        schema=WORKOUT_SCHEMA,
        client=ds,
        ancestor=ancestor,
        projection=["started_at", "duration_sec", "distance_m", "activity_type"],
        limit=limit,
        include_key=True,
    )
    print_frame("Projection query", projected)

    keys = dsp.read_datastore(
        kind="Workout",
        client=ds,
        ancestor=ancestor,
        keys_only=True,
        limit=limit,
        include_key=True,
    )
    print_frame("Keys-only query", keys)

    activity_types = dsp.read_datastore(
        kind="Workout",
        schema=WORKOUT_SCHEMA,
        client=ds,
        namespace=tenant,
        projection=["activity_type"],
        distinct_on=["activity_type"],
        order=["activity_type"],
    )
    print_frame("Distinct activity_type projection", activity_types, rows=10)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--user-id", default="user-00042")
    parser.add_argument("--tenant", default="tenant-a")
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--backend", choices=BACKENDS, default="pandas")
    args = parser.parse_args()
    run(args.user_id, tenant=args.tenant, limit=args.limit, backend=args.backend)


if __name__ == "__main__":
    main()

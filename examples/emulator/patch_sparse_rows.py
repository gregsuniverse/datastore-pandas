"""Patch sparse rows without writing unused DataFrame columns as null properties."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone

from common import (
    BACKENDS,
    Backend,
    WORKOUT_SCHEMA,
    adapter,
    ancestor_key,
    client,
    frame_from_records,
    frame_is_empty,
    iter_records,
    print_frame,
)


def run(user_id: str, *, tenant: str, backend: Backend = "pandas") -> None:
    ds = client()
    dsp = adapter(backend)
    before = dsp.read_datastore(
        kind="Workout",
        schema=WORKOUT_SCHEMA,
        client=ds,
        ancestor=ancestor_key(user_id, tenant=tenant),
        order=["-started_at"],
        limit=3,
        include_key=True,
    )
    print_frame("Before patch", before)
    if frame_is_empty(before):
        print("No rows found to patch. Load mock data first.")
        return

    patch = frame_from_records(
        [
            {
                "__key__": row["__key__"],
                "notes": f"Reviewed locally at {datetime.now(timezone.utc).isoformat()}",
                "last_reviewed_at": datetime.now(timezone.utc),
            }
            for row in iter_records(before)
        ],
        backend,
    )
    report = dsp.patch_datastore(
        patch,
        schema=WORKOUT_SCHEMA,
        client=ds,
        properties=["notes", "last_reviewed_at"],
    )
    report.raise_for_errors()
    print(f"patched sparse rows: {report.succeeded}")

    after = dsp.read_datastore(
        kind="Workout",
        schema=WORKOUT_SCHEMA,
        client=ds,
        ancestor=ancestor_key(user_id, tenant=tenant),
        order=["-started_at"],
        limit=3,
        include_key=True,
    )
    print_frame("After patch", after)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--user-id", default="user-00042")
    parser.add_argument("--tenant", default="tenant-a")
    parser.add_argument("--backend", choices=BACKENDS, default="pandas")
    args = parser.parse_args()
    run(args.user_id, tenant=args.tenant, backend=args.backend)


if __name__ == "__main__":
    main()

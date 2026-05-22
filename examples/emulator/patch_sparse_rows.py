"""Patch sparse rows without writing unused DataFrame columns as null properties."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone

import pandas as pd

from common import WORKOUT_SCHEMA, ancestor_key, client, print_frame
import datastore_pandas as dsp


def run(user_id: str, *, tenant: str) -> None:
    ds = client()
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
    if before.empty:
        print("No rows found to patch. Load mock data first.")
        return

    patch = pd.DataFrame(
        [
            {
                "__key__": row["__key__"],
                "notes": f"Reviewed locally at {datetime.now(timezone.utc).isoformat()}",
                "last_reviewed_at": datetime.now(timezone.utc),
            }
            for _, row in before.iterrows()
        ]
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
    args = parser.parse_args()
    run(args.user_id, tenant=args.tenant)


if __name__ == "__main__":
    main()


"""Example usage for datastore-pandas.

Run after installing the project and authenticating Google Application Default
Credentials:

    pip install -e ".[test]"
    gcloud auth application-default login
    python examples/basic_usage.py
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone

import datastore_pandas as dsp


schema = dsp.Schema(
    kind="Workout",
    key=dsp.KeySpec(
        [
            ("User", dsp.KeyPart("user_id", kind="name")),
            ("Workout", dsp.KeyPart("workout_id", kind="name")),
        ],
        namespace_source="tenant",
    ),
    properties={
        "started_at": dsp.Field(dsp.TimestampType(), nullable=False),
        "duration_sec": dsp.Field(dsp.Int64Type(), nullable=False),
        "distance_m": dsp.Field(dsp.Float64Type()),
        "notes": dsp.Field(dsp.StringType(), indexed=False),
    },
    strict=True,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--backend", choices=["pandas", "polars"], default="pandas")
    args = parser.parse_args()
    adapter = _adapter(args.backend)

    workouts = _frame_from_records(
        [
            {
                "tenant": "tenant-a",
                "user_id": "sample-user",
                "workout_id": "w-001",
                "started_at": datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc),
                "duration_sec": 3600,
                "distance_m": 1200.0,
                "notes": "Technique session; do not index long free text.",
            }
        ],
        args.backend,
    )

    report = adapter.to_datastore(workouts, schema=schema, mode="upsert")
    print(f"wrote={report.succeeded} failed={report.failed}")

    projection = adapter.read_datastore(
        kind="Workout",
        schema=schema,
        filters=[("user_id", "=", "sample-user")],
        projection=["started_at", "duration_sec", "distance_m"],
        order=["-started_at"],
        include_key=True,
    )
    print(projection)


def _adapter(backend: str):
    if backend == "polars":
        import datastore_pandas.polars as dsp_pl

        return dsp_pl
    return dsp


def _frame_from_records(records: list[dict], backend: str):
    if backend == "polars":
        import polars as pl

        return pl.DataFrame(records, strict=False)
    import pandas as pd

    return pd.DataFrame.from_records(records)


if __name__ == "__main__":
    main()

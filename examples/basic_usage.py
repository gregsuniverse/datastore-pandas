"""Example usage for datastore-pandas.

Run after installing the project and authenticating Google Application Default
Credentials:

    pip install -e ".[test]"
    gcloud auth application-default login
    python examples/basic_usage.py
"""

from __future__ import annotations

import pandas as pd

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
    workouts = pd.DataFrame(
        [
            {
                "tenant": "tenant-a",
                "user_id": "sample-user",
                "workout_id": "w-001",
                "started_at": pd.Timestamp("2026-05-01T12:00:00Z"),
                "duration_sec": 3600,
                "distance_m": 1200.0,
                "notes": "Technique session; do not index long free text.",
            }
        ]
    )

    report = dsp.to_datastore(workouts, schema=schema, mode="upsert")
    print(f"wrote={report.succeeded} failed={report.failed}")

    projection = dsp.read_datastore(
        kind="Workout",
        schema=schema,
        filters=[("user_id", "=", "sample-user")],
        projection=["started_at", "duration_sec", "distance_m"],
        order=["-started_at"],
        include_key=True,
    )
    print(projection)


if __name__ == "__main__":
    main()

"""Instantiated accessor example.

This example uses dry-run and planning paths so it does not require a running
Datastore emulator. Use the emulator examples for live integration runs.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

import datastore_pandas as dsp


class PlanningOnlyClient:
    project = "planning-only"

    def key(self, *flat_path, namespace=None):
        from google.cloud import datastore

        return datastore.Key(*flat_path, project=self.project, namespace=namespace)

    def get_multi(self, keys):
        return [None for _ in keys]


WORKOUT_SCHEMA = dsp.Schema(
    kind="Workout",
    key=dsp.key_policy(
        "Workout",
        id_field="workout_id",
        namespace_field="tenant",
        ancestors=[("User", "user_id")],
    ),
    properties={
        "external_id": dsp.Field(dsp.StringType()),
        "started_at": dsp.Field(dsp.TimestampType(), nullable=False),
        "duration_sec": dsp.Field(dsp.Int64Type(), nullable=False),
        "created_at": dsp.Field(dsp.TimestampType()),
        "updated_at": dsp.Field(dsp.TimestampType()),
    },
)


def main() -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    workouts = dsp.kind(
        schema=WORKOUT_SCHEMA,
        client=PlanningOnlyClient(),
        namespace="tenant-a",
        audit=dsp.AuditPolicy(
            created_at="created_at",
            updated_at="updated_at",
            now=lambda: now,
        ),
    )

    frame = pd.DataFrame.from_records(
        [
            {
                "tenant": "tenant-a",
                "user_id": "u1",
                "workout_id": "w1",
                "external_id": "source-1",
                "started_at": now,
                "duration_sec": 1800,
            }
        ]
    )

    dry_report = workouts.write(frame, dry_run=True)
    print("planned writes:", dry_report.planned_writes)
    print("planned payload:", dict(dry_report.planned[0].properties))

    duplicate_frame = pd.DataFrame.from_records(
        [
            {
                "__key__": dsp.DatastoreKey(
                    namespace="tenant-a",
                    path=(("User", "u1"), ("Workout", "old")),
                ),
                "external_id": "source-1",
                "updated_at": pd.Timestamp("2026-01-01T00:00:00Z"),
            },
            {
                "__key__": dsp.DatastoreKey(
                    namespace="tenant-a",
                    path=(("User", "u1"), ("Workout", "new")),
                ),
                "external_id": "source-1",
                "updated_at": pd.Timestamp("2026-01-02T00:00:00Z"),
            },
        ]
    )
    cleanup = workouts.cleanup_duplicates(
        frame=duplicate_frame,
        by=["external_id"],
        order=["-updated_at"],
    )
    print("duplicate deletes planned:", cleanup.planned_writes)


if __name__ == "__main__":
    main()

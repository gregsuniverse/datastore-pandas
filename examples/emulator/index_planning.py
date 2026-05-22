"""Index planning examples for Datastore-mode queries."""

from __future__ import annotations

from common import ancestor_key
import datastore_pandas as dsp


def main() -> None:
    examples = [
        dsp.QuerySpec(
            kind="Workout",
            ancestor=ancestor_key("user-00042"),
            namespace="tenant-a",
            order=["-started_at"],
            projection=["started_at", "duration_sec", "distance_m", "activity_type"],
        ),
        dsp.QuerySpec(
            kind="Workout",
            filters=[("activity_type", "=", "run"), ("started_at", ">=", "2025-01-01")],
            order=["started_at"],
        ),
        dsp.QuerySpec(
            kind="Workout",
            projection=["activity_type"],
            distinct_on=["activity_type"],
            order=["activity_type"],
        ),
    ]

    for index, query in enumerate(examples, start=1):
        plan = dsp.plan_indexes(query)
        print(f"\n== Query {index} ==")
        print(f"needs_composite_index={plan.needs_composite_index}")
        for warning in plan.warnings:
            print(f"warning: {warning}")
        for suggestion in plan.suggestions:
            print(suggestion.to_index_yaml())


if __name__ == "__main__":
    main()

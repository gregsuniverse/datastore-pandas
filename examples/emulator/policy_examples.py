"""Policy-oriented examples for instantiated Datastore kind accessors."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone

import datastore_pandas as dsp
from common import BACKENDS, Backend, client, frame_from_records, frame_len, print_frame

FIXED_NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)

POLICY_SCHEMA = dsp.Schema(
    kind="PolicyEvent",
    key=dsp.key_policy(
        "PolicyEvent",
        id_field="event_id",
        namespace_field="tenant",
        ancestors=[("Tenant", "tenant")],
    ),
    properties={
        "external_id": dsp.Field(dsp.StringType(), nullable=False),
        "event_type": dsp.Field(dsp.StringType(), nullable=False),
        "value": dsp.Field(dsp.Int64Type(), nullable=False),
        "created": dsp.Field(dsp.TimestampType()),
        "created_at": dsp.Field(dsp.TimestampType()),
        "modified": dsp.Field(dsp.TimestampType()),
        "modified_at": dsp.Field(dsp.TimestampType()),
        "imported_ts": dsp.Field(dsp.TimestampType()),
    },
)


def run(*, tenant: str = "policy-tenant", backend: Backend = "pandas") -> None:
    ds = client()
    ancestor = dsp.DatastoreKey(namespace=tenant, path=(("Tenant", tenant),))
    store = dsp.kind(
        schema=POLICY_SCHEMA,
        client=ds,
        backend=backend,
        namespace=tenant,
        ancestor=ancestor,
        audit=dsp.AuditPolicy(
            created="created",
            created_at="created_at",
            modified="modified",
            modified_at="modified_at",
            imported_at="imported_ts",
            now=lambda: FIXED_NOW,
        ),
        batch_size=100,
    )

    base = _base_frame(tenant=tenant, backend=backend)

    dry_report = store.write(base, dry_run=True)
    assert dry_report.dry_run
    assert dry_report.planned_writes == 4
    assert dry_report.planned[0].properties["created"] == FIXED_NOW
    assert dry_report.planned[0].properties["created_at"] == FIXED_NOW
    assert dry_report.planned[0].properties["modified"] == FIXED_NOW
    assert dry_report.planned[0].properties["modified_at"] == FIXED_NOW
    print(f"dry-run planned writes={dry_report.planned_writes}")

    read_only_report = store.with_scope(read_only=True).write(base)
    assert read_only_report.failed == 4
    assert _count(store) == 0
    print(f"read-only blocked writes={read_only_report.failed}")

    first_report = store.write(base)
    first_report.raise_for_errors()
    assert first_report.succeeded == 4
    assert _count(store) == 4
    print(f"initial accessor write succeeded={first_report.succeeded}")

    skip_report = store.write(base, skip_unchanged=True)
    skip_report.raise_for_errors()
    assert skip_report.skipped == 4
    print(f"skip-unchanged full write skipped={skip_report.skipped}")

    patch = frame_from_records(
        [
            {
                "tenant": tenant,
                "event_id": "event-001",
                "external_id": "external-a",
                "event_type": "view",
                "value": 10,
            },
            {
                "tenant": tenant,
                "event_id": "event-002",
                "external_id": "external-a",
                "event_type": "click",
                "value": 99,
            },
        ],
        backend,
    )
    patch_report = store.patch(patch, properties=["value"], skip_unchanged=True)
    patch_report.raise_for_errors()
    assert patch_report.skipped == 1
    assert patch_report.succeeded == 2
    print(f"skip-unchanged patch skipped={patch_report.skipped}")

    try:
        store.write(
            frame_from_records(
                [
                    {
                        "tenant": "other-tenant",
                        "event_id": "event-outside",
                        "external_id": "external-outside",
                        "event_type": "view",
                        "value": 1,
                    }
                ],
                backend,
            ),
            dry_run=True,
        )
        raise AssertionError("ancestor scope validation did not reject out-of-scope key")
    except dsp.SchemaError:
        print("bound ancestor rejected out-of-scope write")

    cleanup_dry_run = store.cleanup_duplicates(by=["external_id"], order=["-modified_at"])
    assert cleanup_dry_run.dry_run
    assert cleanup_dry_run.planned_writes == 1
    assert _count(store) == 4
    print(f"duplicate cleanup dry-run deletes={cleanup_dry_run.planned_writes}")

    cleanup_report = store.cleanup_duplicates(
        by=["external_id"],
        order=["-modified_at"],
        dry_run=False,
    )
    cleanup_report.raise_for_errors()
    assert cleanup_report.succeeded == 1
    assert _count(store) == 3
    print(f"duplicate cleanup executed deletes={cleanup_report.succeeded}")

    after = store.read(order=["external_id"], include_key=True)
    print_frame("PolicyEvent rows after cleanup", after)


def _base_frame(*, tenant: str, backend: Backend):
    return frame_from_records(
        [
            {
                "tenant": tenant,
                "event_id": "event-001",
                "external_id": "external-a",
                "event_type": "view",
                "value": 10,
            },
            {
                "tenant": tenant,
                "event_id": "event-002",
                "external_id": "external-a",
                "event_type": "click",
                "value": 20,
            },
            {
                "tenant": tenant,
                "event_id": "event-003",
                "external_id": "external-b",
                "event_type": "view",
                "value": 30,
            },
            {
                "tenant": tenant,
                "event_id": "event-004",
                "external_id": "external-c",
                "event_type": "view",
                "value": 40,
            },
        ],
        backend,
    )


def _count(store: dsp.DatastoreFrame) -> int:
    return frame_len(store.read(keys_only=True, include_key=True))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant", default="policy-tenant")
    parser.add_argument("--backend", choices=BACKENDS, default="pandas")
    args = parser.parse_args()
    run(tenant=args.tenant, backend=args.backend)


if __name__ == "__main__":
    main()

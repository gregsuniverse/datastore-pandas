"""Focused edge-case checks for write policies, keys, models, and inference."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import sys
from typing import Any

import datastore_pandas as dsp
from common import BACKENDS, Backend, client, frame_from_records, frame_len, print_frame

FIXED_NOW = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)

EDGE_SCHEMA = dsp.Schema(
    kind="EdgeCaseEvent",
    key=dsp.key_policy(
        "EdgeCaseEvent",
        id_field="event_id",
        namespace_field="tenant",
        ancestors=[("Tenant", "tenant")],
    ),
    properties={
        "tenant": dsp.Field(dsp.StringType(), nullable=False),
        "event_id": dsp.Field(dsp.StringType(), nullable=False),
        "external_id": dsp.Field(dsp.StringType(), nullable=False),
        "status": dsp.Field(dsp.StringType(), nullable=False),
        "value": dsp.Field(dsp.Int64Type(), nullable=False),
        "created": dsp.Field(dsp.TimestampType()),
        "created_at": dsp.Field(dsp.TimestampType()),
        "modified": dsp.Field(dsp.TimestampType()),
        "modified_at": dsp.Field(dsp.TimestampType()),
        "updated_at": dsp.Field(dsp.TimestampType()),
    },
)

EDGE_SUMMARY_SCHEMA = dsp.Schema(
    kind="EdgeCaseSummary",
    key=dsp.key_policy(
        "EdgeCaseSummary",
        id_field="status",
        namespace_field="tenant",
        ancestors=[("Tenant", "tenant")],
    ),
    properties={
        "tenant": dsp.Field(dsp.StringType(), nullable=False),
        "status": dsp.Field(dsp.StringType(), nullable=False),
        "value_sum": dsp.Field(dsp.Int64Type(), nullable=False),
    },
)

INFERRED_KIND = "EdgeCaseInferred"


def run(*, tenant: str = "edge-tenant", backend: Backend = "pandas") -> None:
    _offline_no_client_checks(backend=backend)
    _fake_client_checks(backend=backend)
    _emulator_checks(tenant=tenant, backend=backend)


def _offline_no_client_checks(*, backend: Backend) -> None:
    before_secret_modules = _loaded_secret_manager_modules()
    frame = _edge_frame(tenant="offline-edge", backend=backend)
    store = dsp.kind(
        schema=EDGE_SCHEMA,
        backend=backend,
        audit=_audit_policy(),
        batch_size=2,
    )

    with _forbid_default_datastore_client():
        dry_report = store.write(frame, dry_run=True)
        read_only_report = store.with_scope(read_only=True).write(frame)

    assert _loaded_secret_manager_modules() == before_secret_modules
    assert dry_report.dry_run
    assert dry_report.would_write == 3
    assert dry_report.wrote == 0
    assert read_only_report.read_only
    assert read_only_report.failed == 3
    assert read_only_report.would_write == 3
    assert read_only_report.wrote == 0

    first = dry_report.planned[0]
    assert first.key == dsp.DatastoreKey(
        namespace="offline-edge",
        path=(("Tenant", "offline-edge"), ("EdgeCaseEvent", "event-a")),
    )
    for field in ["created", "created_at", "modified", "modified_at", "updated_at"]:
        assert first.properties[field] == FIXED_NOW
    print(
        "offline planning: "
        f"would_write={dry_report.would_write} "
        f"read_only_blocked={read_only_report.failed}"
    )


def _fake_client_checks(*, backend: Backend) -> None:
    fake = FakeDatastoreClient()
    frame = _edge_frame(tenant="fake-edge", backend=backend)
    store = dsp.kind(
        schema=EDGE_SCHEMA,
        client=fake,
        backend=backend,
        audit=_audit_policy(),
        batch_size=2,
        retry=_google_retry_policy(),
    )

    write_report = store.write(frame)
    write_report.raise_for_errors()
    assert write_report.wrote == 3
    assert fake.batch_commits == 2
    assert fake.max_batch_size == 2

    unchanged_report = store.write(frame, skip_unchanged=True)
    unchanged_report.raise_for_errors()
    assert unchanged_report.skipped == 3
    assert unchanged_report.wrote == 0
    print(
        "fake client: "
        f"batches={fake.batch_commits} "
        f"max_batch_size={fake.max_batch_size} "
        f"skip_unchanged={unchanged_report.skipped}"
    )


def _emulator_checks(*, tenant: str, backend: Backend) -> None:
    ds = client()
    ancestor = dsp.DatastoreKey(namespace=tenant, path=(("Tenant", tenant),))
    store = dsp.kind(
        schema=EDGE_SCHEMA,
        client=ds,
        backend=backend,
        namespace=tenant,
        ancestor=ancestor,
        audit=_audit_policy(),
        batch_size=2,
        retry=_google_retry_policy(),
    )
    base = _edge_frame(tenant=tenant, backend=backend)

    plan = store.plan_write(base)
    assert plan.mutations[0].key == dsp.DatastoreKey(
        namespace=tenant,
        path=(("Tenant", tenant), ("EdgeCaseEvent", "event-a")),
    )

    first_report = store.write(base)
    first_report.raise_for_errors()
    assert first_report.wrote == 3

    unchanged_report = store.write(base, skip_unchanged=True)
    unchanged_report.raise_for_errors()
    assert unchanged_report.skipped == 3

    changed_report = store.write(_changed_frame(base, backend=backend), skip_unchanged=True)
    changed_report.raise_for_errors()
    assert changed_report.wrote == 1
    assert changed_report.skipped == 2

    patch_report = store.patch(
        _single_patch_frame(tenant=tenant, backend=backend),
        properties=["value"],
        skip_unchanged=True,
    )
    patch_report.raise_for_errors()
    assert patch_report.skipped == 1

    _assert_bound_ancestor_blocks_out_of_scope_write(store, backend=backend)

    cleanup_dry_run = store.cleanup_duplicates(by=["external_id"], order=["event_id"])
    assert cleanup_dry_run.dry_run
    assert cleanup_dry_run.would_write == 1
    cleanup_report = store.cleanup_duplicates(
        by=["external_id"],
        order=["event_id"],
        dry_run=False,
    )
    cleanup_report.raise_for_errors()
    assert cleanup_report.wrote == 1

    loaded = dsp.dspdf(
        kind="EdgeCaseEvent",
        schema=EDGE_SCHEMA,
        client=ds,
        backend=backend,
        namespace=tenant,
        ancestor=ancestor,
        keep_original=True,
    ).load(order=["event_id"])
    assert loaded.original_df is not None
    assert frame_len(loaded.df) == 2

    summary = loaded.aggregate(by=["tenant", "status"], metrics={"value": "sum"})
    try:
        summary.write(dry_run=True)
        raise AssertionError("derived model write unexpectedly targeted the source kind")
    except dsp.DerivedFrameWriteError:
        pass
    summary_report = summary.write_to(schema=EDGE_SUMMARY_SCHEMA)
    summary_report.raise_for_errors()
    assert summary_report.wrote == 2

    _seed_mixed_type_entities(ds, tenant=tenant)
    inferred = dsp.dspdf(
        kind=INFERRED_KIND,
        client=ds,
        backend=backend,
        namespace=tenant,
        infer_schema=True,
    ).load()
    assert inferred.schema_report is not None
    assert inferred.schema_report.mixed_fields == ("payload",)

    print(
        "emulator edge cases: "
        f"wrote={first_report.wrote} "
        f"skipped={unchanged_report.skipped} "
        f"changed={changed_report.wrote} "
        f"cleanup_deletes={cleanup_report.wrote} "
        f"summary_writes={summary_report.wrote} "
        f"mixed_fields={inferred.schema_report.mixed_fields}"
    )
    print_frame("EdgeCaseEvent after duplicate cleanup", loaded.df)


def _audit_policy() -> dsp.AuditPolicy:
    return dsp.AuditPolicy(
        created="created",
        created_at="created_at",
        modified="modified",
        modified_at="modified_at",
        updated_at="updated_at",
        now=lambda: FIXED_NOW,
    )


def _edge_frame(*, tenant: str, backend: Backend):
    return frame_from_records(
        [
            {
                "tenant": tenant,
                "event_id": "event-a",
                "external_id": "external-1",
                "status": "new",
                "value": 10,
            },
            {
                "tenant": tenant,
                "event_id": "event-b",
                "external_id": "external-1",
                "status": "active",
                "value": 20,
            },
            {
                "tenant": tenant,
                "event_id": "event-c",
                "external_id": "external-2",
                "status": "active",
                "value": 30,
            },
        ],
        backend,
    )


def _changed_frame(df: Any, *, backend: Backend):
    if backend == "polars":
        import polars as pl

        return df.with_columns(
            pl.when(pl.col("event_id") == "event-b")
            .then(25)
            .otherwise(pl.col("value"))
            .alias("value")
        )
    changed = df.copy()
    changed.loc[changed["event_id"] == "event-b", "value"] = 25
    return changed


def _single_patch_frame(*, tenant: str, backend: Backend):
    return frame_from_records(
        [
            {
                "tenant": tenant,
                "event_id": "event-a",
                "external_id": "external-1",
                "status": "new",
                "value": 10,
            }
        ],
        backend,
    )


def _assert_bound_ancestor_blocks_out_of_scope_write(
    store: dsp.DatastoreFrame,
    *,
    backend: Backend,
) -> None:
    try:
        store.write(
            frame_from_records(
                [
                    {
                        "tenant": "outside-edge",
                        "event_id": "event-outside",
                        "external_id": "external-outside",
                        "status": "new",
                        "value": 1,
                    }
                ],
                backend,
            ),
            dry_run=True,
        )
        raise AssertionError("bound ancestor accepted an out-of-scope key")
    except dsp.SchemaError:
        return


def _seed_mixed_type_entities(ds: Any, *, tenant: str) -> None:
    from google.cloud import datastore

    entities = []
    for name, payload in [("inferred-a", 1), ("inferred-b", "one")]:
        entity = datastore.Entity(key=ds.key(INFERRED_KIND, name, namespace=tenant))
        entity.update({"payload": payload, "tenant": tenant})
        entities.append(entity)
    with ds.batch() as batch:
        for entity in entities:
            batch.put(entity)


def _google_retry_policy():
    from google.api_core.retry import Retry

    return Retry(initial=2.0, multiplier=2.0, deadline=40.0)


def _loaded_secret_manager_modules() -> set[str]:
    return {name for name in sys.modules if name.startswith("google.cloud.secretmanager")}


@contextmanager
def _forbid_default_datastore_client():
    from google.cloud import datastore

    original = datastore.Client

    def forbidden_client(*args, **kwargs):
        raise AssertionError("default Datastore client construction should not be needed")

    datastore.Client = forbidden_client
    try:
        yield
    finally:
        datastore.Client = original


class FakeBatch:
    def __init__(self, client: FakeDatastoreClient) -> None:
        self.client = client
        self.pending = []

    def __enter__(self) -> FakeBatch:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is not None:
            return None
        self.client.batch_commits += 1
        self.client.max_batch_size = max(self.client.max_batch_size, len(self.pending))
        for entity in self.pending:
            self.client.entities[_key_identity(entity.key)] = entity

    def put(self, entity) -> None:
        self.pending.append(entity)


class FakeDatastoreClient:
    project = "fake-project"

    def __init__(self) -> None:
        self.entities: dict[str, Any] = {}
        self.batch_commits = 0
        self.max_batch_size = 0

    def key(self, *flat_path, namespace=None):
        from google.cloud import datastore

        return datastore.Key(*flat_path, project=self.project, namespace=namespace)

    def batch(self) -> FakeBatch:
        return FakeBatch(self)

    def get_multi(self, keys):
        return [self.entities.get(_key_identity(key)) for key in keys]


def _key_identity(key: Any) -> str:
    return dsp.DatastoreKey.from_client_key(key).without_partition().to_json()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant", default="edge-tenant")
    parser.add_argument("--backend", choices=BACKENDS, default="pandas")
    args = parser.parse_args()
    run(tenant=args.tenant, backend=args.backend)


if __name__ == "__main__":
    main()

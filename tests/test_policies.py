from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import pytest

import datastore_pandas as dsp


class FakeBatch:
    def __init__(self, client: FakeClient) -> None:
        self.client = client

    def __enter__(self) -> FakeBatch:
        self.client.batch_entries += 1
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def put(self, entity) -> None:
        self.client.puts.append(entity)


class FakeClient:
    project = "fake-project"

    def __init__(self) -> None:
        self.batch_entries = 0
        self.puts = []

    def key(self, *flat_path, namespace=None):
        from google.cloud import datastore

        return datastore.Key(
            *flat_path,
            project=self.project,
            namespace=namespace,
        )

    def batch(self):
        return FakeBatch(self)

    def get_multi(self, keys):
        return [None for _ in keys]


def test_key_policy_builds_deterministic_ancestor_keys():
    policy = dsp.key_policy(
        "Workout",
        id_field="workout_id",
        namespace_field="tenant",
        ancestors=[("User", "user_id")],
    )

    key = policy.build({"tenant": "tenant-a", "user_id": "u1", "workout_id": "w1"})

    assert key.namespace == "tenant-a"
    assert key.path == (("User", "u1"), ("Workout", "w1"))


def test_audit_policy_applies_custom_timestamp_fields_to_write_plan():
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    schema = dsp.Schema(
        kind="Doc",
        key=dsp.key_policy("Doc", id_field="doc_id"),
        properties={
            "value": dsp.Field(dsp.Int64Type()),
            "created": dsp.Field(dsp.TimestampType()),
            "modified": dsp.Field(dsp.TimestampType()),
        },
    )
    store = dsp.kind(
        schema=schema,
        client=FakeClient(),
        audit=dsp.AuditPolicy(
            created_at="created",
            updated_at="modified",
            now=lambda: now,
        ),
    )

    report = store.write(pd.DataFrame({"doc_id": ["a"], "value": [1]}), dry_run=True)

    assert report.planned[0].properties["created"] == now
    assert report.planned[0].properties["modified"] == now


def test_audit_policy_supports_created_and_modified_alias_fields():
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    schema = dsp.Schema(
        kind="Doc",
        key=dsp.key_policy("Doc", id_field="doc_id"),
        properties={
            "created": dsp.Field(dsp.TimestampType()),
            "created_at": dsp.Field(dsp.TimestampType()),
            "modified": dsp.Field(dsp.TimestampType()),
            "modified_at": dsp.Field(dsp.TimestampType()),
        },
    )
    store = dsp.kind(
        schema=schema,
        audit=dsp.AuditPolicy(
            created="created",
            created_at="created_at",
            modified="modified",
            modified_at="modified_at",
            now=lambda: now,
        ),
    )

    report = store.write(pd.DataFrame({"doc_id": ["a"]}), dry_run=True)

    assert report.planned[0].properties == {
        "created": now,
        "created_at": now,
        "modified": now,
        "modified_at": now,
    }


def test_bound_ancestor_rejects_out_of_scope_writes():
    schema = dsp.Schema(
        kind="Workout",
        key=dsp.key_policy("Workout", id_field="workout_id", ancestors=[("User", "user_id")]),
        properties={"value": dsp.Field(dsp.Int64Type())},
    )
    store = dsp.kind(
        schema=schema,
        client=FakeClient(),
        ancestor=dsp.DatastoreKey(path=(("User", "u1"),)),
    )

    with pytest.raises(dsp.SchemaError, match="outside the bound ancestor"):
        store.write(pd.DataFrame({"user_id": ["u2"], "workout_id": ["w1"], "value": [1]}))

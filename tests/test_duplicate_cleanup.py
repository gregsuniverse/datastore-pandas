from __future__ import annotations

import pandas as pd

import datastore_pandas as dsp


class FakeBatch:
    def __init__(self, client: FakeClient) -> None:
        self.client = client

    def __enter__(self) -> FakeBatch:
        self.client.batch_entries += 1
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def delete(self, key) -> None:
        self.client.deletes.append(key)


class FakeClient:
    project = "fake-project"

    def __init__(self) -> None:
        self.batch_entries = 0
        self.deletes = []

    def key(self, *flat_path, namespace=None):
        from google.cloud import datastore

        return datastore.Key(
            *flat_path,
            project=self.project,
            namespace=namespace,
        )

    def batch(self):
        return FakeBatch(self)


def _schema() -> dsp.Schema:
    return dsp.Schema(
        kind="Doc",
        key=dsp.key_policy("Doc", id_field="doc_id"),
        properties={
            "external_id": dsp.Field(dsp.StringType()),
            "updated_at": dsp.Field(dsp.TimestampType()),
        },
    )


def _frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "__key__": [
                dsp.DatastoreKey(path=(("Doc", "old"),)),
                dsp.DatastoreKey(path=(("Doc", "new"),)),
                dsp.DatastoreKey(path=(("Doc", "unique"),)),
            ],
            "external_id": ["same", "same", "unique"],
            "updated_at": [
                pd.Timestamp("2026-01-01T00:00:00Z"),
                pd.Timestamp("2026-01-02T00:00:00Z"),
                pd.Timestamp("2026-01-03T00:00:00Z"),
            ],
        }
    )


def test_duplicate_cleanup_plan_marks_non_kept_rows_for_delete():
    store = dsp.kind(schema=_schema(), client=FakeClient())

    plan = store.plan_duplicate_cleanup(
        frame=_frame(),
        by=["external_id"],
        order=["-updated_at"],
    )

    assert len(plan.mutations) == 1
    assert plan.mutations[0].action == "delete"
    assert plan.mutations[0].key == dsp.DatastoreKey(path=(("Doc", "old"),))


def test_duplicate_cleanup_executes_only_when_dry_run_is_false():
    client = FakeClient()
    store = dsp.kind(schema=_schema(), client=client)

    dry_report = store.cleanup_duplicates(
        frame=_frame(),
        by=["external_id"],
        order=["-updated_at"],
    )
    write_report = store.cleanup_duplicates(
        frame=_frame(),
        by=["external_id"],
        order=["-updated_at"],
        dry_run=False,
    )

    assert dry_report.dry_run is True
    assert len(client.deletes) == 1
    assert tuple(client.deletes[0].flat_path) == ("Doc", "old")
    assert write_report.results[0].action == "delete"

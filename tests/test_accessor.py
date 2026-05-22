from __future__ import annotations

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


class FakeQuery:
    def __init__(self) -> None:
        self.filters = []

    def add_filter(self, name, op, value):
        self.filters.append((name, op, value))

    def keys_only(self) -> None:
        return None

    def fetch(self, limit=None):
        return []


class FakeClient:
    project = "fake-project"

    def __init__(self) -> None:
        self.puts = []
        self.batch_entries = 0
        self.query_kwargs = None
        self.query_instance = None

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

    def query(self, **kwargs):
        self.query_kwargs = kwargs
        self.query_instance = FakeQuery()
        return self.query_instance


def _schema() -> dsp.Schema:
    return dsp.Schema(
        kind="Doc",
        key=dsp.KeySpec.from_columns("Doc", "doc_id"),
        properties={"value": dsp.Field(dsp.Int64Type())},
    )


def test_kind_accessor_binds_read_scope_and_filters():
    client = FakeClient()
    store = dsp.kind(
        schema=_schema(),
        client=client,
        namespace="tenant-a",
        filters=[("value", ">", 1)],
        projection=["value"],
    )

    store.read(filters=[("value", "<", 10)], limit=5)

    assert client.query_kwargs["kind"] == "Doc"
    assert client.query_kwargs["namespace"] == "tenant-a"
    assert client.query_kwargs["projection"] == ("value",)
    assert client.query_instance.filters == [("value", ">", 1), ("value", "<", 10)]


def test_kind_accessor_read_only_blocks_write_execution():
    client = FakeClient()
    store = dsp.kind(schema=_schema(), client=client, read_only=True)
    df = pd.DataFrame({"doc_id": ["a"], "value": [1]})

    report = store.write(df)

    assert report.read_only is True
    assert report.failed == 1
    assert client.batch_entries == 0


def test_kind_accessor_polars_dry_run_uses_polars_backend():
    pl = pytest.importorskip("polars")
    client = FakeClient()
    store = dsp.kind(schema=_schema(), client=client, backend="polars")
    df = pl.DataFrame({"doc_id": ["a"], "value": [1]})

    report = store.write(df, dry_run=True)

    assert report.dry_run is True
    assert report.planned[0].properties == {"value": 1}
    assert client.batch_entries == 0

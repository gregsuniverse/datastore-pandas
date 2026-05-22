from __future__ import annotations

import pandas as pd

import datastore_pandas as dsp
from datastore_pandas import io


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
        self.existing = {}
        self.puts = []
        self.batch_entries = 0

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
        return [self.existing.get(_client_key_identity(key)) for key in keys]

    def seed(self, key: dsp.DatastoreKey, properties: dict) -> None:
        from google.cloud import datastore

        entity = datastore.Entity(key=key.to_client_key(self))
        entity.update(properties)
        self.existing[_client_key_identity(entity.key)] = entity


def _client_key_identity(key) -> tuple:
    return (key.namespace, tuple(key.flat_path))


def _schema() -> dsp.Schema:
    return dsp.Schema(
        kind="Doc",
        key=dsp.KeySpec.from_columns("Doc", "doc_id"),
        properties={"value": dsp.Field(dsp.Int64Type())},
    )


def test_dry_run_captures_write_plan_without_committing():
    client = FakeClient()
    df = pd.DataFrame({"doc_id": ["a"], "value": [1]})

    report = dsp.to_datastore(df, schema=_schema(), client=client, dry_run=True)

    assert client.batch_entries == 0
    assert report.dry_run is True
    assert report.planned[0].action == "upsert"
    assert report.planned[0].properties == {"value": 1}
    assert report.results[0].dry_run is True
    assert report.would_write == 1
    assert report.wrote == 0


def test_dry_run_does_not_construct_default_client(monkeypatch):
    def fail_get_client(client):
        raise AssertionError("client construction should not be needed")

    monkeypatch.setattr(io, "_get_client", fail_get_client)
    df = pd.DataFrame({"doc_id": ["a"], "value": [1]})

    report = dsp.to_datastore(df, schema=_schema(), dry_run=True)
    plan = dsp.plan_datastore_write(df, schema=_schema())

    assert report.would_write == 1
    assert plan.mutations[0].properties == {"value": 1}


def test_read_only_does_not_construct_default_client(monkeypatch):
    def fail_get_client(client):
        raise AssertionError("client construction should not be needed")

    monkeypatch.setattr(io, "_get_client", fail_get_client)
    df = pd.DataFrame({"doc_id": ["a"], "value": [1]})

    report = dsp.to_datastore(df, schema=_schema(), read_only=True)

    assert report.read_only is True
    assert report.failed == 1


def test_skip_unchanged_write_commits_only_changed_entities():
    client = FakeClient()
    client.seed(dsp.DatastoreKey(path=(("Doc", "a"),)), {"value": 1})
    client.seed(dsp.DatastoreKey(path=(("Doc", "b"),)), {"value": 1})
    df = pd.DataFrame({"doc_id": ["a", "b", "c"], "value": [1, 2, 3]})

    report = dsp.to_datastore(df, schema=_schema(), client=client, skip_unchanged=True)

    assert report.skipped == 1
    assert len(client.puts) == 2
    assert report.wrote == 2
    assert [tuple(entity.key.flat_path) for entity in client.puts] == [
        ("Doc", "b"),
        ("Doc", "c"),
    ]
    assert [mutation.action for mutation in report.planned] == ["skip", "update", "create"]


def test_patch_skip_unchanged_compares_only_patch_properties():
    client = FakeClient()
    client.seed(dsp.DatastoreKey(path=(("Doc", "a"),)), {"value": 1, "kept": "yes"})
    client.seed(dsp.DatastoreKey(path=(("Doc", "b"),)), {"value": 1, "kept": "yes"})
    df = pd.DataFrame({"doc_id": ["a", "b"], "value": [1, 2]})

    report = dsp.patch_datastore(
        df,
        schema=_schema(),
        properties=["value"],
        client=client,
        skip_unchanged=True,
    )

    assert report.skipped == 1
    assert len(client.puts) == 1
    assert dict(client.puts[0]) == {"value": 2, "kept": "yes"}
    assert [mutation.action for mutation in report.planned] == ["skip", "patch"]


def test_read_only_reports_blocked_writes_without_committing():
    client = FakeClient()
    df = pd.DataFrame({"doc_id": ["a"], "value": [1]})

    report = dsp.to_datastore(df, schema=_schema(), client=client, read_only=True)

    assert client.batch_entries == 0
    assert report.read_only is True
    assert report.failed == 1
    assert report.results[0].error == "read_only prevents write execution"


def test_commit_retry_policy_accepts_google_retry_style_attributes():
    class RetryLike:
        _initial = 2.0
        _multiplier = 2.0
        _deadline = 40.0

    policy = io._coerce_commit_retry_policy(RetryLike())

    assert policy == dsp.CommitRetryPolicy(initial=2.0, multiplier=2.0, deadline=40.0)

from __future__ import annotations

import pandas as pd
import pytest

import datastore_pandas as dsp


class FakeQuery:
    def __init__(self, entities) -> None:
        self.entities = entities

    def add_filter(self, name, op, value):
        return None

    def fetch(self, limit=None):
        return self.entities[:limit]


class FakeClient:
    project = "fake-project"

    def __init__(self, entities=None) -> None:
        self.entities = entities or []

    def key(self, *flat_path, namespace=None):
        from google.cloud import datastore

        return datastore.Key(*flat_path, project=self.project, namespace=namespace)

    def query(self, **kwargs):
        return FakeQuery(self.entities)


def _entity(client: FakeClient, name: str, properties: dict):
    from google.cloud import datastore

    entity = datastore.Entity(key=client.key("Doc", name))
    entity.update(properties)
    return entity


def test_infer_schema_from_frame_reports_mixed_type_columns():
    df = pd.DataFrame({"doc_id": ["a", "b"], "value": [1, "one"]})

    report = dsp.infer_schema_from_frame(df, kind="Doc")

    assert report.mixed_fields == ("value",)
    assert report.fields["value"].observed_types == frozenset({"int64", "string"})
    assert isinstance(report.schema.properties["value"].dtype, dsp.DatastoreType)


def test_infer_schema_mixed_type_error_policy_raises():
    df = pd.DataFrame({"value": [1, "one"]})

    with pytest.raises(dsp.SchemaError, match="mixed Datastore types"):
        dsp.infer_schema_from_frame(df, kind="Doc", mixed_type_policy="error")


def test_infer_schema_samples_datastore_entities():
    client = FakeClient()
    client.entities = [
        _entity(client, "a", {"value": 1}),
        _entity(client, "b", {"value": 2, "note": "present"}),
    ]

    report = dsp.infer_schema(kind="Doc", client=client)

    assert report.sampled_entities == 2
    assert isinstance(report.schema.properties["value"].dtype, dsp.Int64Type)
    assert report.fields["note"].nullable is True


def test_read_datastore_can_infer_schema_when_schema_is_not_provided():
    client = FakeClient()
    client.entities = [_entity(client, "a", {"value": 1})]

    df = dsp.read_datastore(kind="Doc", client=client, infer_schema=True)

    assert df["value"].tolist() == [1]


def test_dspdf_can_load_with_inferred_schema():
    client = FakeClient()
    client.entities = [
        _entity(client, "a", {"value": 1}),
        _entity(client, "b", {"value": "one"}),
    ]

    model = dsp.dspdf(kind="Doc", client=client, infer_schema=True).load()

    assert model.schema_report is not None
    assert model.schema_report.mixed_fields == ("value",)
    assert "value" in model.schema.properties
    assert model.df["value"].tolist() == [1, "one"]

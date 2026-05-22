from __future__ import annotations

import pandas as pd
import pytest

import datastore_pandas as dsp


class FakeQuery:
    def __init__(self, entities):
        self.entities = entities

    def add_filter(self, name, op, value):
        return None

    def keys_only(self):
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


def _schema(kind: str = "Doc") -> dsp.Schema:
    return dsp.Schema(
        kind=kind,
        key=dsp.key_policy(kind, id_field="doc_id"),
        properties={
            "group": dsp.Field(dsp.StringType()),
            "value": dsp.Field(dsp.Int64Type()),
        },
    )


def _summary_schema(kind: str = "DocSummary") -> dsp.Schema:
    return dsp.Schema(
        kind=kind,
        key=dsp.key_policy(kind, id_field="group"),
        properties={"value_sum": dsp.Field(dsp.Int64Type())},
    )


def test_dspdf_load_holds_dataframe_and_original_snapshot():
    from google.cloud import datastore

    client = FakeClient()
    entity = datastore.Entity(key=client.key("Doc", "a"))
    entity.update({"group": "g1", "value": 1})
    client.entities = [entity]
    model = dsp.dspdf(kind="Doc", schema=_schema(), client=client, keep_original=True)

    loaded = model.load()

    assert loaded.is_loaded
    assert len(loaded.df) == 1
    assert len(loaded.original_df) == 1
    assert loaded.loaded_at is not None


def test_dspdf_source_write_uses_owned_dataframe_without_client_for_dry_run():
    model = dsp.dspdf(
        kind="Doc",
        schema=_schema(),
        df=pd.DataFrame({"doc_id": ["a"], "group": ["g1"], "value": [1]}),
    )

    report = model.write(dry_run=True)

    assert report.would_write == 1
    assert report.wrote == 0


def test_dspdf_derived_frame_cannot_write_to_source_kind():
    model = dsp.dspdf(
        kind="Doc",
        schema=_schema(),
        df=pd.DataFrame({"doc_id": ["a"], "group": ["g1"], "value": [1]}),
    )
    derived = model.derive(pd.DataFrame({"group": ["g1"], "value_sum": [1]}))

    with pytest.raises(dsp.DerivedFrameWriteError):
        derived.write(dry_run=True)


def test_dspdf_derived_frame_writes_to_explicit_target():
    model = dsp.dspdf(
        kind="Doc",
        schema=_schema(),
        df=pd.DataFrame({"doc_id": ["a"], "group": ["g1"], "value": [1]}),
    )
    derived = model.derive(
        pd.DataFrame({"group": ["g1"], "value_sum": [1]}),
        target_schema=_summary_schema(),
    )

    report = derived.write(dry_run=True)

    assert report.would_write == 1
    assert derived.target_kind == "DocSummary"
    assert derived.target_store.schema.key.path[-1][0] == "DocSummary"


def test_dspdf_write_to_sets_one_off_target():
    model = dsp.dspdf(
        kind="Doc",
        schema=_schema(),
        df=pd.DataFrame({"doc_id": ["a"], "group": ["g1"], "value": [1]}),
    )
    derived = model.derive(pd.DataFrame({"group": ["g1"], "value_sum": [1]}))

    report = derived.write_to(schema=_summary_schema(), dry_run=True)

    assert report.would_write == 1


def test_dspdf_aggregate_marks_model_derived():
    model = dsp.dspdf(
        kind="Doc",
        schema=_schema(),
        df=pd.DataFrame(
            {
                "doc_id": ["a", "b", "c"],
                "group": ["g1", "g1", "g2"],
                "value": [1, 2, 3],
            }
        ),
    )

    summary = model.aggregate(
        by=["group"],
        metrics={"value": "sum"},
        target_schema=_summary_schema(),
    )

    assert summary.is_derived
    assert list(summary.df.columns) == ["group", "value_sum"]
    assert summary.df.sort_values("group")["value_sum"].tolist() == [3, 3]


def test_dspdf_polars_aggregate_marks_model_derived():
    pl = pytest.importorskip("polars")
    model = dsp.dspdf(
        kind="Doc",
        schema=_schema(),
        backend="polars",
        df=pl.DataFrame(
            {
                "doc_id": ["a", "b", "c"],
                "group": ["g1", "g1", "g2"],
                "value": [1, 2, 3],
            }
        ),
    )

    summary = model.aggregate(
        by=["group"],
        metrics={"value": "sum"},
        target_schema=_summary_schema(),
    )

    assert summary.is_derived
    assert summary.df.sort("group")["value_sum"].to_list() == [3, 3]


def test_dspdf_original_row_count_guard_blocks_source_shape_change():
    model = dsp.dspdf(
        kind="Doc",
        schema=_schema(),
        df=pd.DataFrame(
            {
                "doc_id": ["a", "b"],
                "group": ["g1", "g1"],
                "value": [1, 2],
            }
        ),
        keep_original=True,
    )
    changed = model.replace_df(pd.DataFrame({"doc_id": ["a"], "group": ["g1"], "value": [1]}))

    with pytest.raises(dsp.DerivedFrameWriteError):
        changed.write(dry_run=True)

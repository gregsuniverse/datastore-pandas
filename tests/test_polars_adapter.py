import pytest

import datastore_pandas as dsp
from datastore_pandas import io


pl = pytest.importorskip("polars")
dsp_pl = pytest.importorskip("datastore_pandas.polars")


def test_polars_iter_rows_yields_dict_rows():
    df = pl.DataFrame(
        {
            "doc_id": ["a", "b"],
            "value": [1, 2],
            "optional": [None, "present"],
        }
    )

    rows = list(dsp_pl._iter_rows(df))

    assert rows == [
        (0, {"doc_id": "a", "value": 1, "optional": None}),
        (1, {"doc_id": "b", "value": 2, "optional": "present"}),
    ]


def test_polars_missing_values_are_omitted_by_schema():
    schema = dsp.Schema(
        kind="Doc",
        properties={
            "doc_id": dsp.Field(dsp.StringType(), nullable=False),
            "optional": dsp.Field(dsp.StringType()),
        },
    )
    row = list(dsp_pl._iter_rows(pl.DataFrame({"doc_id": ["a"], "optional": [None]})))[0][1]

    encoded, _ = schema.encode_properties(row)

    assert encoded == {"doc_id": "a"}


def test_polars_dry_run_does_not_construct_default_client(monkeypatch):
    def fail_get_client(client):
        raise AssertionError("client construction should not be needed")

    monkeypatch.setattr(io, "_get_client", fail_get_client)
    monkeypatch.setattr(dsp_pl, "_get_client", fail_get_client)
    schema = dsp.Schema(
        kind="Doc",
        key=dsp.key_policy("Doc", id_field="doc_id"),
        properties={"value": dsp.Field(dsp.Int64Type())},
    )

    report = dsp_pl.to_datastore(
        pl.DataFrame({"doc_id": ["a"], "value": [1]}),
        schema=schema,
        dry_run=True,
    )

    assert report.would_write == 1
    assert report.wrote == 0

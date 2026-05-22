"""DataFrame-owning model examples for the Datastore emulator."""

from __future__ import annotations

import argparse

import datastore_pandas as dsp
from common import BACKENDS, Backend, client, frame_len, print_frame
from policy_examples import FIXED_NOW

MODEL_EVENT_SCHEMA = dsp.Schema(
    kind="ModelEvent",
    key=dsp.key_policy("ModelEvent", id_field="event_id", namespace_field="tenant"),
    properties={
        "tenant": dsp.Field(dsp.StringType(), nullable=False),
        "event_id": dsp.Field(dsp.StringType(), nullable=False),
        "group": dsp.Field(dsp.StringType(), nullable=False),
        "value": dsp.Field(dsp.Int64Type(), nullable=False),
        "modified_at": dsp.Field(dsp.TimestampType()),
    },
)

MODEL_SUMMARY_SCHEMA = dsp.Schema(
    kind="ModelSummary",
    key=dsp.key_policy("ModelSummary", id_field="group", namespace_field="tenant"),
    properties={
        "tenant": dsp.Field(dsp.StringType(), nullable=False),
        "group": dsp.Field(dsp.StringType(), nullable=False),
        "value_sum": dsp.Field(dsp.Int64Type(), nullable=False),
        "modified_at": dsp.Field(dsp.TimestampType()),
    },
)


def run(*, tenant: str = "model-tenant", backend: Backend = "pandas") -> None:
    ds = client()
    _seed_source(tenant=tenant, backend=backend)

    model = dsp.dspdf(
        kind="ModelEvent",
        schema=MODEL_EVENT_SCHEMA,
        client=ds,
        backend=backend,
        namespace=tenant,
        keep_original=True,
        audit=dsp.AuditPolicy(modified_at="modified_at", now=lambda: FIXED_NOW),
    ).load(order=["event_id"])

    assert model.is_loaded
    assert model.original_df is not None
    assert frame_len(model.df) == 3
    print_frame("Loaded ModelEvent source frame", model.df)

    edited = model.replace_df(_set_first_value(model.df, backend=backend, value=15))
    write_report = edited.write(skip_unchanged=True)
    write_report.raise_for_errors()
    assert write_report.skipped == 2
    assert write_report.wrote == 1
    print(f"model source write skipped={write_report.skipped} wrote={write_report.wrote}")

    summary = edited.aggregate(
        by=["tenant", "group"],
        metrics={"value": "sum"},
    )
    try:
        summary.write(dry_run=True)
        raise AssertionError("derived model write unexpectedly targeted source kind")
    except dsp.DerivedFrameWriteError:
        print("derived model source write blocked")

    summary = summary.with_target(schema=MODEL_SUMMARY_SCHEMA)
    summary_report = summary.write()
    summary_report.raise_for_errors()
    assert summary_report.wrote == 2
    loaded_summary = dsp.dspdf(
        kind="ModelSummary",
        schema=MODEL_SUMMARY_SCHEMA,
        client=ds,
        backend=backend,
        namespace=tenant,
    ).load(order=["group"])
    assert frame_len(loaded_summary.df) == 2
    print_frame("Loaded ModelSummary target frame", loaded_summary.df)

    _seed_mixed_inference(tenant=tenant)
    inferred = dsp.dspdf(
        kind="InferredEvent",
        client=ds,
        backend=backend,
        namespace=tenant,
        infer_schema=True,
    ).load()
    assert inferred.schema_report is not None
    assert inferred.schema_report.mixed_fields == ("payload",)
    assert frame_len(inferred.df) == 2
    print(f"inferred mixed fields={inferred.schema_report.mixed_fields}")


def _seed_source(*, tenant: str, backend: Backend) -> None:
    rows = [
        {
            "tenant": tenant,
            "event_id": "event-a",
            "group": "g1",
            "value": 10,
            "modified_at": FIXED_NOW,
        },
        {
            "tenant": tenant,
            "event_id": "event-b",
            "group": "g1",
            "value": 20,
            "modified_at": FIXED_NOW,
        },
        {
            "tenant": tenant,
            "event_id": "event-c",
            "group": "g2",
            "value": 30,
            "modified_at": FIXED_NOW,
        },
    ]
    frame = _frame(rows, backend=backend)
    store = dsp.kind(schema=MODEL_EVENT_SCHEMA, client=client(), backend=backend, namespace=tenant)
    report = store.write(frame)
    report.raise_for_errors()


def _seed_mixed_inference(*, tenant: str) -> None:
    from google.cloud import datastore

    ds = client()
    entities = []
    for name, payload in [("mixed-a", 1), ("mixed-b", "one")]:
        entity = datastore.Entity(key=ds.key("InferredEvent", name, namespace=tenant))
        entity.update({"payload": payload})
        entities.append(entity)
    with ds.batch() as batch:
        for entity in entities:
            batch.put(entity)


def _frame(rows: list[dict], *, backend: Backend):
    if backend == "polars":
        import polars as pl

        return pl.DataFrame(rows, strict=False)
    import pandas as pd

    return pd.DataFrame.from_records(rows)


def _set_first_value(df, *, backend: Backend, value: int):
    if backend == "polars":
        import polars as pl

        return df.with_columns(
            pl.when(pl.col("event_id") == "event-a")
            .then(value)
            .otherwise(pl.col("value"))
            .alias("value")
        )
    edited = df.copy()
    edited.loc[edited.index[0], "value"] = value
    return edited


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant", default="model-tenant")
    parser.add_argument("--backend", choices=BACKENDS, default="pandas")
    args = parser.parse_args()
    run(tenant=args.tenant, backend=args.backend)


if __name__ == "__main__":
    main()

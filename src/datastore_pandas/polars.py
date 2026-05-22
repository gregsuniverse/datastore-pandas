"""Polars read/write API for Firestore in Datastore mode.

Install with:

    pip install datastore-pandas[polars]
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Iterator, Sequence

from datastore_pandas.batches import chunk_items, validate_unique_complete_keys
from datastore_pandas.convert import entity_to_record
from datastore_pandas.errors import SchemaError
from datastore_pandas.io import (
    WriteMode,
    _commit_chunk,
    _filter_rows_for_plan,
    _get_client,
    _patch_with_planned_results,
    _patch_chunk,
    _require_complete_keys,
    _write_with_planned_results,
)
from datastore_pandas.keys import DatastoreKey
from datastore_pandas.planning import WritePlan, plan_write_rows
from datastore_pandas.query import QuerySpec
from datastore_pandas.reports import WriteReport
from datastore_pandas.schema import Schema


def read_datastore(
    *,
    kind: str,
    client: Any | None = None,
    schema: Schema | None = None,
    filters: Sequence[tuple[str, str, Any]] = (),
    namespace: str | None = None,
    projection: Sequence[str] | None = None,
    order: Sequence[str] = (),
    distinct_on: Sequence[str] | None = None,
    ancestor: DatastoreKey | None = None,
    keys_only: bool = False,
    limit: int | None = None,
    include_key: bool = False,
    chunksize: int | None = None,
):
    """Read a Datastore query into a Polars DataFrame."""

    pl = _polars()
    frames = list(
        iter_datastore(
            kind=kind,
            client=client,
            schema=schema,
            filters=filters,
            namespace=namespace,
            projection=projection,
            order=order,
            distinct_on=distinct_on,
            ancestor=ancestor,
            keys_only=keys_only,
            limit=limit,
            include_key=include_key,
            chunksize=chunksize or 1000,
        )
    )
    if not frames:
        return pl.DataFrame()
    return pl.concat(frames, how="diagonal_relaxed")


def iter_datastore(
    *,
    kind: str,
    client: Any | None = None,
    schema: Schema | None = None,
    filters: Sequence[tuple[str, str, Any]] = (),
    namespace: str | None = None,
    projection: Sequence[str] | None = None,
    order: Sequence[str] = (),
    distinct_on: Sequence[str] | None = None,
    ancestor: DatastoreKey | None = None,
    keys_only: bool = False,
    limit: int | None = None,
    include_key: bool = False,
    chunksize: int = 1000,
) -> Iterator[Any]:
    """Yield Datastore query results as Polars DataFrame chunks."""

    pl = _polars()
    client = _get_client(client)
    spec = QuerySpec(
        kind=kind,
        namespace=namespace,
        filters=filters,  # type: ignore[arg-type]
        order=order,
        projection=projection,
        distinct_on=distinct_on,
        ancestor=ancestor,
        keys_only=keys_only,
        limit=limit,
    )
    query = spec.build(client)
    iterator = query.fetch(limit=limit)
    records: list[dict[str, Any]] = []
    for entity in iterator:
        records.append(entity_to_record(entity, schema=schema, include_key=include_key))
        if len(records) >= chunksize:
            yield pl.DataFrame(records, strict=False)
            records = []
    if records:
        yield pl.DataFrame(records, strict=False)


def to_datastore(
    df: Any,
    *,
    schema: Schema,
    client: Any | None = None,
    mode: WriteMode = "upsert",
    properties: list[str] | None = None,
    batch_size: int = 400,
    max_workers: int = 1,
    dry_run: bool = False,
    read_only: bool = False,
    skip_unchanged: bool = False,
) -> WriteReport:
    """Write a Polars DataFrame to Datastore."""

    if schema.key is None and "__key__" not in df.columns:
        raise SchemaError("to_datastore requires schema.key or a __key__ column.")
    client = _get_client(client)
    rows = list(_iter_rows(df))
    if dry_run or read_only or skip_unchanged:
        plan = plan_write_rows(
            rows,
            schema=schema,
            client=client,
            operation="write",
            mode=mode,
            properties=properties,
            skip_unchanged=skip_unchanged,
            batch_size=batch_size,
        )
        if dry_run or read_only:
            return plan.to_report(dry_run=dry_run, read_only=read_only)
        rows = _filter_rows_for_plan(rows, plan)

    keys = [schema.key_for_row(row) for _, row in rows]
    validate_unique_complete_keys(keys)
    _require_complete_keys(keys, operation="to_datastore")
    items = list(zip(rows, keys))
    if skip_unchanged:
        return _write_with_planned_results(
            plan=plan,
            items=items,
            schema=schema,
            client=client,
            mode=mode,
            properties=properties,
            batch_size=batch_size,
            max_workers=max_workers,
        )

    if max_workers <= 1:
        report = WriteReport()
        for chunk in chunk_items(items, max_items=batch_size):
            report.extend(
                _commit_chunk(
                    chunk,
                    schema=schema,
                    client=client,
                    mode=mode,
                    properties=properties,
                )
            )
        return report

    report = WriteReport()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                _commit_chunk,
                chunk,
                schema=schema,
                client=client,
                mode=mode,
                properties=properties,
            )
            for chunk in chunk_items(items, max_items=batch_size)
        ]
        for future in as_completed(futures):
            report.extend(future.result())
    return report


def patch_datastore(
    df: Any,
    *,
    schema: Schema,
    properties: list[str],
    client: Any | None = None,
    max_workers: int = 1,
    batch_size: int = 400,
    dry_run: bool = False,
    read_only: bool = False,
    skip_unchanged: bool = False,
) -> WriteReport:
    """Partially update Datastore entities from a Polars DataFrame."""

    client = _get_client(client)
    rows = list(_iter_rows(df))
    if dry_run or read_only or skip_unchanged:
        plan = plan_write_rows(
            rows,
            schema=schema,
            client=client,
            operation="patch",
            properties=properties,
            skip_unchanged=skip_unchanged,
            batch_size=batch_size,
        )
        if dry_run or read_only:
            return plan.to_report(dry_run=dry_run, read_only=read_only)
        rows = _filter_rows_for_plan(rows, plan)

    keys = [schema.key_for_row(row) for _, row in rows]
    validate_unique_complete_keys(keys)
    _require_complete_keys(keys, operation="patch_datastore")
    items = list(zip(rows, keys))
    if skip_unchanged:
        return _patch_with_planned_results(
            plan=plan,
            items=items,
            schema=schema,
            client=client,
            properties=properties,
            batch_size=batch_size,
            max_workers=max_workers,
        )

    if max_workers <= 1:
        report = WriteReport()
        for chunk in chunk_items(items, max_items=batch_size):
            report.extend(_patch_chunk(chunk, schema=schema, client=client, properties=properties))
        return report

    report = WriteReport()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                _patch_chunk,
                chunk,
                schema=schema,
                client=client,
                properties=properties,
            )
            for chunk in chunk_items(items, max_items=batch_size)
        ]
        for future in as_completed(futures):
            report.extend(future.result())
    return report


def plan_datastore_write(
    df: Any,
    *,
    schema: Schema,
    client: Any | None = None,
    mode: WriteMode = "upsert",
    properties: list[str] | None = None,
    patch: bool = False,
    skip_unchanged: bool = False,
    batch_size: int = 400,
) -> WritePlan:
    """Plan Polars DataFrame writes without committing them."""

    if schema.key is None and "__key__" not in df.columns:
        raise SchemaError("plan_datastore_write requires schema.key or a __key__ column.")
    if patch and properties is None:
        raise SchemaError("patch planning requires an explicit properties list.")
    client = _get_client(client)
    return plan_write_rows(
        list(_iter_rows(df)),
        schema=schema,
        client=client,
        operation="patch" if patch else "write",
        mode=mode,
        properties=properties,
        skip_unchanged=skip_unchanged,
        batch_size=batch_size,
    )


def _iter_rows(df: Any) -> Iterator[tuple[int, dict[str, Any]]]:
    for row_index, row in enumerate(df.iter_rows(named=True)):
        yield row_index, dict(row)


def _polars():
    try:
        import polars as pl
    except ImportError as exc:
        raise ImportError(
            "Polars support requires the optional extra: pip install datastore-pandas[polars]"
        ) from exc
    return pl

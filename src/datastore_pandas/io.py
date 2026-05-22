"""Public pandas read/write API for Firestore in Datastore mode."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Iterable, Iterator, Literal, Sequence

from datastore_pandas.batches import chunk_items, validate_unique_complete_keys
from datastore_pandas.convert import entity_to_record, row_to_entity
from datastore_pandas.errors import SchemaError
from datastore_pandas.keys import DatastoreKey
from datastore_pandas.query import QuerySpec
from datastore_pandas.reports import WriteReport, WriteResult
from datastore_pandas.schema import Schema

WriteMode = Literal["insert", "update", "upsert"]


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
    """Read a Datastore query into a pandas DataFrame.

    Use `iter_datastore` for streaming large result sets.
    """

    import pandas as pd

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
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


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
    import pandas as pd

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
            yield pd.DataFrame.from_records(records)
            records = []
    if records:
        yield pd.DataFrame.from_records(records)


def to_datastore(
    df: Any,
    *,
    schema: Schema,
    client: Any | None = None,
    mode: WriteMode = "upsert",
    properties: list[str] | None = None,
    batch_size: int = 400,
    max_workers: int = 1,
) -> WriteReport:
    """Write a DataFrame to Datastore using schema-derived keys and typed values."""

    if schema.key is None and "__key__" not in df.columns:
        raise SchemaError("to_datastore requires schema.key or a __key__ column.")
    client = _get_client(client)
    rows = list(_iter_rows(df))
    keys = [schema.key_for_row(row) for _, row in rows]
    validate_unique_complete_keys(keys)
    _require_complete_keys(keys, operation="patch_datastore")
    items = list(zip(rows, keys))

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
) -> WriteReport:
    """Partially update Datastore entities by read-merge-write.

    This avoids the data-loss risk of writing partial entities through the
    high-level client's `put`. For compare-and-swap or server-side property
    masks, replace this backend with a lower-level Datastore `Commit` call.
    """

    client = _get_client(client)
    rows = list(_iter_rows(df))
    keys = [schema.key_for_row(row) for _, row in rows]
    validate_unique_complete_keys(keys)
    items = list(zip(rows, keys))

    if max_workers <= 1:
        report = WriteReport()
        for chunk in chunk_items(items, max_items=batch_size):
            report.extend(_patch_chunk(chunk, schema=schema, client=client, properties=properties))
        return report

    report = WriteReport()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(_patch_chunk, chunk, schema=schema, client=client, properties=properties)
            for chunk in chunk_items(items, max_items=batch_size)
        ]
        for future in as_completed(futures):
            report.extend(future.result())
    return report


def _commit_chunk(
    chunk: Iterable[tuple[tuple[Any, dict[str, Any]], DatastoreKey]],
    *,
    schema: Schema,
    client: Any,
    mode: WriteMode,
    properties: list[str] | None,
) -> WriteReport:
    report = WriteReport()
    rows = list(chunk)
    entities: list[Any] = []
    valid_rows: list[tuple[tuple[Any, dict[str, Any]], DatastoreKey]] = []
    for row_ref, key in rows:
        row_index, row = row_ref
        try:
            schema.validate_row(row)
            entities.append(row_to_entity(row, schema=schema, client=client, properties=properties))
            valid_rows.append((row_ref, key))
        except Exception as exc:
            report.results.append(WriteResult(row_index=row_index, error=str(exc)))

    if not entities:
        return report

    try:
        with client.batch() as batch:
            for entity in entities:
                if mode == "insert":
                    _batch_insert(batch, entity)
                elif mode == "update":
                    _batch_update(batch, entity)
                elif mode == "upsert":
                    batch.put(entity)
                else:
                    raise SchemaError(f"Unsupported write mode: {mode!r}.")
    except Exception as exc:
        for (row_index, _), _ in valid_rows:
            report.results.append(WriteResult(row_index=row_index, error=str(exc)))
        return report

    for ((row_index, _), key), entity in zip(valid_rows, entities):
        written_key = DatastoreKey.from_client_key(entity.key) if hasattr(entity, "key") else key
        report.results.append(WriteResult(row_index=row_index, key=written_key))
    return report


def _patch_chunk(
    chunk: Iterable[tuple[tuple[Any, dict[str, Any]], DatastoreKey]],
    *,
    schema: Schema,
    client: Any,
    properties: list[str],
) -> WriteReport:
    from google.cloud import datastore

    report = WriteReport()
    rows = list(chunk)
    client_keys = [key.to_client_key(client) for _, key in rows]
    existing = client.get_multi(client_keys)
    existing_by_key = {
        DatastoreKey.from_client_key(entity.key).to_json(): entity for entity in existing if entity is not None
    }

    entities: list[Any] = []
    valid_rows: list[tuple[tuple[Any, dict[str, Any]], DatastoreKey]] = []
    for row_ref, key in rows:
        row_index, row = row_ref
        try:
            encoded, exclude_from_indexes = schema.encode_properties(row, properties=properties)
            entity = existing_by_key.get(key.to_json())
            if entity is None:
                entity = datastore.Entity(
                    key=key.to_client_key(client),
                    exclude_from_indexes=exclude_from_indexes,
                )
            else:
                _merge_excluded_indexes(entity, exclude_from_indexes)
            entity.update(encoded)
            entities.append(entity)
            valid_rows.append((row_ref, key))
        except Exception as exc:
            report.results.append(WriteResult(row_index=row_index, error=str(exc)))

    if not entities:
        return report

    try:
        with client.batch() as batch:
            for entity in entities:
                batch.put(entity)
    except Exception as exc:
        for (row_index, _), _ in valid_rows:
            report.results.append(WriteResult(row_index=row_index, error=str(exc)))
        return report

    for ((row_index, _), key), entity in zip(valid_rows, entities):
        written_key = DatastoreKey.from_client_key(entity.key) if hasattr(entity, "key") else key
        report.results.append(WriteResult(row_index=row_index, key=written_key))
    return report


def _merge_excluded_indexes(entity: Any, exclude_from_indexes: list[str]) -> None:
    if not exclude_from_indexes:
        return
    current = set(getattr(entity, "exclude_from_indexes", ()) or ())
    current.update(exclude_from_indexes)
    try:
        entity.exclude_from_indexes = tuple(sorted(current))
    except Exception:
        return


def _require_complete_keys(keys: Iterable[DatastoreKey], *, operation: str) -> None:
    incomplete = [key for key in keys if not key.is_complete]
    if incomplete:
        raise SchemaError(f"{operation} requires complete keys; found {len(incomplete)} incomplete key(s).")


def _batch_insert(batch: Any, entity: Any) -> None:
    if hasattr(batch, "insert"):
        batch.insert(entity)
        return
    raise NotImplementedError(
        "insert mode requires a datastore batch implementation with insert(), or "
        "the lower-level Datastore API backend."
    )


def _batch_update(batch: Any, entity: Any) -> None:
    if hasattr(batch, "update"):
        batch.update(entity)
        return
    raise NotImplementedError(
        "update mode requires a datastore batch implementation with update(), or "
        "the lower-level Datastore API backend."
    )


def _iter_rows(df: Any) -> Iterator[tuple[Any, dict[str, Any]]]:
    for row in df.itertuples(index=True, name=None):
        row_index = row[0]
        values = row[1:]
        yield row_index, dict(zip(df.columns, values))


def _get_client(client: Any | None) -> Any:
    if client is not None:
        return client
    from google.cloud import datastore

    return datastore.Client()

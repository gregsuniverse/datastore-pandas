"""Public pandas read/write API for Firestore in Datastore mode."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep
from typing import Any, Iterable, Iterator, Literal, Sequence

from datastore_pandas.batches import chunk_items, validate_unique_complete_keys
from datastore_pandas.convert import entity_to_record, row_to_entity, to_client_datastore_value
from datastore_pandas.errors import SchemaError
from datastore_pandas.keys import DatastoreKey
from datastore_pandas.planning import WritePlan, plan_write_rows
from datastore_pandas.query import QuerySpec
from datastore_pandas.reports import WriteReport, WriteResult
from datastore_pandas.schema import Schema

WriteMode = Literal["insert", "update", "upsert"]
COMMIT_MAX_ATTEMPTS = 3
COMMIT_RETRY_INITIAL_DELAY_SEC = 0.5


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
    dry_run: bool = False,
    read_only: bool = False,
    skip_unchanged: bool = False,
) -> WriteReport:
    """Write a DataFrame to Datastore using schema-derived keys and typed values."""

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
    """Partially update Datastore entities by read-merge-write.

    This avoids the data-loss risk of writing partial entities through the
    high-level client's `put`. For compare-and-swap or server-side property
    masks, replace this backend with a lower-level Datastore `Commit` call.
    """

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
                _patch_chunk, chunk, schema=schema, client=client, properties=properties
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
    """Plan DataFrame writes without committing them."""

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


def _filter_rows_for_plan(
    rows: list[tuple[Any, dict[str, Any]]],
    plan: WritePlan,
) -> list[tuple[Any, dict[str, Any]]]:
    write_positions = plan.write_positions
    return [row for row_position, row in enumerate(rows) if row_position in write_positions]


def _write_with_planned_results(
    *,
    plan: WritePlan,
    items: list[tuple[tuple[Any, dict[str, Any]], DatastoreKey]],
    schema: Schema,
    client: Any,
    mode: WriteMode,
    properties: list[str] | None,
    batch_size: int,
    max_workers: int,
) -> WriteReport:
    report = _report_for_non_writes(plan)
    report.extend(
        _write_items(
            items,
            schema=schema,
            client=client,
            mode=mode,
            properties=properties,
            batch_size=batch_size,
            max_workers=max_workers,
        )
    )
    return report


def _patch_with_planned_results(
    *,
    plan: WritePlan,
    items: list[tuple[tuple[Any, dict[str, Any]], DatastoreKey]],
    schema: Schema,
    client: Any,
    properties: list[str],
    batch_size: int,
    max_workers: int,
) -> WriteReport:
    report = _report_for_non_writes(plan)
    report.extend(
        _patch_items(
            items,
            schema=schema,
            client=client,
            properties=properties,
            batch_size=batch_size,
            max_workers=max_workers,
        )
    )
    return report


def _report_for_non_writes(plan: WritePlan) -> WriteReport:
    report = WriteReport(planned=list(plan.mutations))
    for mutation in plan.mutations:
        if mutation.should_write:
            continue
        report.results.append(
            WriteResult(
                row_index=mutation.row_index,
                key=mutation.key,
                action=mutation.action,
                skipped=mutation.action == "skip",
                reason=mutation.reason,
                error=mutation.error,
            )
        )
    return report


def _write_items(
    items: list[tuple[tuple[Any, dict[str, Any]], DatastoreKey]],
    *,
    schema: Schema,
    client: Any,
    mode: WriteMode,
    properties: list[str] | None,
    batch_size: int,
    max_workers: int,
) -> WriteReport:
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


def _patch_items(
    items: list[tuple[tuple[Any, dict[str, Any]], DatastoreKey]],
    *,
    schema: Schema,
    client: Any,
    properties: list[str],
    batch_size: int,
    max_workers: int,
) -> WriteReport:
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
        _commit_entities_with_retry(client, entities, mode=mode)
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
        _key_lookup_identity(DatastoreKey.from_client_key(entity.key)): entity
        for entity in existing
        if entity is not None
    }

    entities: list[Any] = []
    valid_rows: list[tuple[tuple[Any, dict[str, Any]], DatastoreKey]] = []
    for row_ref, key in rows:
        row_index, row = row_ref
        try:
            encoded, exclude_from_indexes = schema.encode_properties(row, properties=properties)
            encoded = {
                name: to_client_datastore_value(value, client) for name, value in encoded.items()
            }
            entity = existing_by_key.get(_key_lookup_identity(key))
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
        _commit_entities_with_retry(client, entities, mode="upsert")
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


def _key_lookup_identity(key: DatastoreKey) -> str:
    return DatastoreKey(path=key.path, namespace=key.namespace).to_json()


def _require_complete_keys(keys: Iterable[DatastoreKey], *, operation: str) -> None:
    incomplete = [key for key in keys if not key.is_complete]
    if incomplete:
        raise SchemaError(
            f"{operation} requires complete keys; found {len(incomplete)} incomplete key(s)."
        )


def _commit_entities_with_retry(
    client: Any,
    entities: list[Any],
    *,
    mode: WriteMode,
    max_attempts: int = COMMIT_MAX_ATTEMPTS,
    initial_delay_sec: float = COMMIT_RETRY_INITIAL_DELAY_SEC,
) -> None:
    attempt = 1
    while True:
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
            return
        except Exception as exc:
            if mode != "upsert" or attempt >= max_attempts or not _is_retryable_commit_error(exc):
                raise
            sleep(initial_delay_sec * (2 ** (attempt - 1)))
            attempt += 1


def _is_retryable_commit_error(exc: Exception) -> bool:
    message = str(exc).lower()
    retryable_fragments = (
        "deadline exceeded",
        "goaway",
        "unavailable",
        "application error processing rpc",
        "503",
        "504",
    )
    return any(fragment in message for fragment in retryable_fragments)


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

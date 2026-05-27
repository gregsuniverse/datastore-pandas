"""Public pandas read/write API for Firestore in Datastore mode."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from random import uniform
from time import perf_counter, sleep
from typing import Any, Callable, Iterable, Iterator, Literal, Sequence

from datastore_pandas.batches import DEFAULT_MAX_BYTES, chunk_items, validate_unique_complete_keys
from datastore_pandas.convert import entity_to_record, row_to_entity, to_client_datastore_value
from datastore_pandas.errors import SchemaError
from datastore_pandas.inference import infer_schema as infer_schema_for_query
from datastore_pandas.keys import DatastoreKey
from datastore_pandas.planning import WritePlan, plan_write_rows
from datastore_pandas.query import QuerySpec, ReadConsistency
from datastore_pandas.reports import WriteReport, WriteResult
from datastore_pandas.schema import Schema
from datastore_pandas.throttle import (
    AdaptiveWriteLimiter,
    DynamicBatchPolicy,
    DynamicBatchSizer,
    WriteThrottlePolicy,
)

WriteMode = Literal["insert", "update", "upsert"]


@dataclass(frozen=True)
class CommitRetryPolicy:
    initial: float = 2.0
    multiplier: float = 2.0
    deadline: float = 40.0
    max_attempts: int | None = None
    max_delay: float = 30.0
    jitter: float = 0.2
    retryable_exceptions: tuple[type[BaseException], ...] = ()
    on_retry: Callable[["CommitRetryContext"], None] | None = field(
        default=None,
        compare=False,
        repr=False,
    )


DEFAULT_COMMIT_RETRY = CommitRetryPolicy()
DEFAULT_WRITE_THROTTLE = WriteThrottlePolicy()
DEFAULT_DYNAMIC_BATCH = DynamicBatchPolicy()


@dataclass(frozen=True)
class CommitRetryContext:
    attempt: int
    delay: float
    exception: Exception
    retryable: bool


@dataclass
class _CommitStats:
    attempts: int = 0
    successes: int = 0
    failures: int = 0
    retry_attempts: int = 0
    retryable_failures: int = 0
    throttled_seconds: float = 0.0
    latency_ms: int | None = None
    index_updates: int = 0
    batch_size: int = 0


class _CommitFailure(Exception):
    def __init__(self, original: Exception, stats: _CommitStats) -> None:
        super().__init__(str(original))
        self.original = original
        self.stats = stats


class _NativePatchUnavailable(Exception):
    pass


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
    cursor: bytes | str | None = None,
    consistency: ReadConsistency | None = None,
    include_key: bool = False,
    chunksize: int | None = None,
    infer_schema: bool = False,
    schema_sample_size: int = 1000,
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
            cursor=cursor,
            consistency=consistency,
            include_key=include_key,
            chunksize=chunksize or 1000,
            infer_schema=infer_schema,
            schema_sample_size=schema_sample_size,
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
    cursor: bytes | str | None = None,
    consistency: ReadConsistency | None = None,
    include_key: bool = False,
    chunksize: int = 1000,
    infer_schema: bool = False,
    schema_sample_size: int = 1000,
) -> Iterator[Any]:
    import pandas as pd

    client = _get_client(client)
    if infer_schema and schema is None:
        schema = infer_schema_for_query(
            kind=kind,
            client=client,
            namespace=namespace,
            filters=filters,
            ancestor=ancestor,
            sample_size=schema_sample_size,
        ).schema
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
        cursor=cursor,
        consistency=consistency,
    )
    query = spec.build(client)
    iterator = spec.fetch(query, limit=limit)
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
    retry: Any = DEFAULT_COMMIT_RETRY,
    throttle: Any = DEFAULT_WRITE_THROTTLE,
    adaptive_batching: Any = DEFAULT_DYNAMIC_BATCH,
) -> WriteReport:
    """Write a DataFrame to Datastore using schema-derived keys and typed values."""

    if schema.key is None and "__key__" not in df.columns:
        raise SchemaError("to_datastore requires schema.key or a __key__ column.")
    client = (
        _get_client(client)
        if _write_requires_client(dry_run, read_only, skip_unchanged)
        else client
    )
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
            retry=retry,
            throttle=throttle,
            adaptive_batching=adaptive_batching,
        )

    return _write_items(
        items,
        schema=schema,
        client=client,
        mode=mode,
        properties=properties,
        batch_size=batch_size,
        max_workers=max_workers,
        retry=retry,
        throttle=throttle,
        adaptive_batching=adaptive_batching,
    )


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
    retry: Any = DEFAULT_COMMIT_RETRY,
    throttle: Any = DEFAULT_WRITE_THROTTLE,
    adaptive_batching: Any = DEFAULT_DYNAMIC_BATCH,
    patch_backend: Literal["auto", "native", "merge"] = "auto",
) -> WriteReport:
    """Partially update Datastore entities with a masked commit when available."""

    client = (
        _get_client(client)
        if _write_requires_client(dry_run, read_only, skip_unchanged)
        else client
    )
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
            retry=retry,
            throttle=throttle,
            adaptive_batching=adaptive_batching,
            patch_backend=patch_backend,
        )

    return _patch_items(
        items,
        schema=schema,
        client=client,
        properties=properties,
        batch_size=batch_size,
        max_workers=max_workers,
        retry=retry,
        throttle=throttle,
        adaptive_batching=adaptive_batching,
        patch_backend=patch_backend,
    )


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
    client = _get_client(client) if skip_unchanged else client
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


def _write_requires_client(dry_run: bool, read_only: bool, skip_unchanged: bool) -> bool:
    if skip_unchanged:
        return True
    return not (dry_run or read_only)


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
    retry: Any,
    throttle: Any,
    adaptive_batching: Any,
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
            retry=retry,
            throttle=throttle,
            adaptive_batching=adaptive_batching,
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
    retry: Any,
    throttle: Any,
    adaptive_batching: Any,
    patch_backend: Literal["auto", "native", "merge"],
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
            retry=retry,
            throttle=throttle,
            adaptive_batching=adaptive_batching,
            patch_backend=patch_backend,
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


def _coerce_write_limiter(throttle: Any) -> AdaptiveWriteLimiter:
    if isinstance(throttle, AdaptiveWriteLimiter):
        return throttle
    if throttle is None or throttle is False:
        return AdaptiveWriteLimiter(WriteThrottlePolicy(enabled=False))
    if throttle is True:
        return AdaptiveWriteLimiter(DEFAULT_WRITE_THROTTLE)
    if isinstance(throttle, WriteThrottlePolicy):
        return AdaptiveWriteLimiter(throttle)
    return AdaptiveWriteLimiter(throttle)


def _coerce_batch_policy(adaptive_batching: Any, *, enabled: bool) -> DynamicBatchPolicy:
    if not enabled or adaptive_batching is None or adaptive_batching is False:
        return DynamicBatchPolicy(enabled=False)
    if adaptive_batching is True:
        return DEFAULT_DYNAMIC_BATCH
    if isinstance(adaptive_batching, DynamicBatchPolicy):
        return adaptive_batching
    return adaptive_batching


def _dynamic_chunks(
    items: list[tuple[tuple[Any, dict[str, Any]], DatastoreKey]],
    *,
    batch_sizer: DynamicBatchSizer,
) -> Iterator[list[tuple[tuple[Any, dict[str, Any]], DatastoreKey]]]:
    offset = 0
    while offset < len(items):
        size = batch_sizer.next_batch_size()
        chunk = items[offset : offset + size]
        offset += len(chunk)
        yield chunk


def _add_commit_stats(report: WriteReport, stats: _CommitStats) -> None:
    report.commit_attempts += stats.attempts
    report.commit_successes += stats.successes
    report.commit_failures += stats.failures
    report.retry_attempts += stats.retry_attempts
    report.retryable_failures += stats.retryable_failures
    report.throttled_seconds += stats.throttled_seconds
    report.index_updates += stats.index_updates
    if stats.batch_size:
        report.batch_sizes.append(stats.batch_size)
    if stats.latency_ms is not None:
        report.batch_latency_ms.append(stats.latency_ms)


def _report_batch_to_sizer(batch_sizer: DynamicBatchSizer, report: WriteReport) -> None:
    failed = report.commit_failures > 0
    latency = report.batch_latency_ms[-1] if report.batch_latency_ms else None
    batch_sizer.report(latency_ms=latency, failed=failed)


def _write_items(
    items: list[tuple[tuple[Any, dict[str, Any]], DatastoreKey]],
    *,
    schema: Schema,
    client: Any,
    mode: WriteMode,
    properties: list[str] | None,
    batch_size: int,
    max_workers: int,
    retry: Any,
    throttle: Any,
    adaptive_batching: Any,
) -> WriteReport:
    write_limiter = _coerce_write_limiter(throttle)
    if max_workers <= 1:
        report = WriteReport()
        batch_sizer = DynamicBatchSizer(
            batch_size,
            _coerce_batch_policy(adaptive_batching, enabled=True),
        )
        for chunk in _dynamic_chunks(items, batch_sizer=batch_sizer):
            chunk_report = _commit_chunk(
                chunk,
                schema=schema,
                client=client,
                mode=mode,
                properties=properties,
                retry=retry,
                write_limiter=write_limiter,
            )
            report.extend(chunk_report)
            _report_batch_to_sizer(batch_sizer, chunk_report)
        return report

    report = WriteReport()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_index = {
            executor.submit(
                _commit_chunk,
                chunk,
                schema=schema,
                client=client,
                mode=mode,
                properties=properties,
                retry=retry,
                write_limiter=write_limiter,
            ): index
            for index, chunk in enumerate(chunk_items(items, max_items=batch_size))
        }
        ordered_reports: list[WriteReport | None] = [None] * len(future_to_index)
        for future in as_completed(future_to_index):
            ordered_reports[future_to_index[future]] = future.result()
        for chunk_report in ordered_reports:
            if chunk_report is not None:
                report.extend(chunk_report)
    return report


def _patch_items(
    items: list[tuple[tuple[Any, dict[str, Any]], DatastoreKey]],
    *,
    schema: Schema,
    client: Any,
    properties: list[str],
    batch_size: int,
    max_workers: int,
    retry: Any,
    throttle: Any,
    adaptive_batching: Any,
    patch_backend: Literal["auto", "native", "merge"],
) -> WriteReport:
    write_limiter = _coerce_write_limiter(throttle)
    if max_workers <= 1:
        report = WriteReport()
        batch_sizer = DynamicBatchSizer(
            batch_size,
            _coerce_batch_policy(adaptive_batching, enabled=True),
        )
        for chunk in _dynamic_chunks(items, batch_sizer=batch_sizer):
            chunk_report = _patch_chunk(
                chunk,
                schema=schema,
                client=client,
                properties=properties,
                retry=retry,
                write_limiter=write_limiter,
                patch_backend=patch_backend,
            )
            report.extend(chunk_report)
            _report_batch_to_sizer(batch_sizer, chunk_report)
        return report

    report = WriteReport()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_index = {
            executor.submit(
                _patch_chunk,
                chunk,
                schema=schema,
                client=client,
                properties=properties,
                retry=retry,
                write_limiter=write_limiter,
                patch_backend=patch_backend,
            ): index
            for index, chunk in enumerate(chunk_items(items, max_items=batch_size))
        }
        ordered_reports: list[WriteReport | None] = [None] * len(future_to_index)
        for future in as_completed(future_to_index):
            ordered_reports[future_to_index[future]] = future.result()
        for chunk_report in ordered_reports:
            if chunk_report is not None:
                report.extend(chunk_report)
    return report


def _commit_chunk(
    chunk: Iterable[tuple[tuple[Any, dict[str, Any]], DatastoreKey]],
    *,
    schema: Schema,
    client: Any,
    mode: WriteMode,
    properties: list[str] | None,
    retry: Any,
    write_limiter: AdaptiveWriteLimiter,
) -> WriteReport:
    report = WriteReport()
    rows = list(chunk)
    prepared: list[tuple[tuple[tuple[Any, dict[str, Any]], DatastoreKey], Any]] = []
    for row_ref, key in rows:
        row_index, row = row_ref
        try:
            schema.validate_row(row)
            entity = row_to_entity(row, schema=schema, client=client, properties=properties)
            prepared.append(((row_ref, key), entity))
        except Exception as exc:
            report.results.append(WriteResult(row_index=row_index, error=str(exc)))

    if not prepared:
        return report

    for prepared_chunk in _chunk_prepared_entities(prepared):
        chunk_rows = [item[0] for item in prepared_chunk]
        entities = [item[1] for item in prepared_chunk]
        try:
            stats = _commit_entities_with_retry(
                client,
                entities,
                mode=mode,
                retry=retry,
                write_limiter=write_limiter,
            )
            _add_commit_stats(report, stats)
        except _CommitFailure as exc:
            _add_commit_stats(report, exc.stats)
            for (row_index, _), _ in chunk_rows:
                report.results.append(WriteResult(row_index=row_index, error=str(exc.original)))
            continue

        for ((row_index, _), key), entity in zip(chunk_rows, entities):
            written_key = (
                DatastoreKey.from_client_key(entity.key)
                if hasattr(entity, "key")
                else key
            )
            report.results.append(WriteResult(row_index=row_index, key=written_key, action=mode))
    return report


def _patch_chunk(
    chunk: Iterable[tuple[tuple[Any, dict[str, Any]], DatastoreKey]],
    *,
    schema: Schema,
    client: Any,
    properties: list[str],
    retry: Any,
    write_limiter: AdaptiveWriteLimiter,
    patch_backend: Literal["auto", "native", "merge"],
) -> WriteReport:
    rows = list(chunk)
    if patch_backend in {"auto", "native"}:
        try:
            return _patch_chunk_native(
                rows,
                schema=schema,
                client=client,
                properties=properties,
                retry=retry,
                write_limiter=write_limiter,
            )
        except _NativePatchUnavailable:
            if patch_backend == "native":
                report = WriteReport()
                for row_ref, _ in rows:
                    report.results.append(
                        WriteResult(
                            row_index=row_ref[0],
                            action="patch",
                            error="native patch backend is unavailable",
                        )
                    )
                return report

    return _patch_chunk_merge(
        rows,
        schema=schema,
        client=client,
        properties=properties,
        retry=retry,
        write_limiter=write_limiter,
    )


def _patch_chunk_native(
    rows: list[tuple[tuple[Any, dict[str, Any]], DatastoreKey]],
    *,
    schema: Schema,
    client: Any,
    properties: list[str],
    retry: Any,
    write_limiter: AdaptiveWriteLimiter,
) -> WriteReport:
    _require_native_patch_backend(client)
    from google.cloud import datastore

    report = WriteReport()
    prepared: list[
        tuple[tuple[tuple[Any, dict[str, Any]], DatastoreKey], Any, tuple[str, ...]]
    ] = []
    for row_ref, key in rows:
        row_index, row = row_ref
        try:
            encoded, exclude_from_indexes = schema.encode_properties(row, properties=properties)
            encoded = {
                name: to_client_datastore_value(value, client) for name, value in encoded.items()
            }
            if not encoded:
                report.results.append(
                    WriteResult(
                        row_index=row_index,
                        key=key,
                        action="skip",
                        skipped=True,
                        reason="no properties to patch",
                    )
                )
                continue
            entity = datastore.Entity(
                key=key.to_client_key(client),
                exclude_from_indexes=exclude_from_indexes,
            )
            entity.update(encoded)
            prepared.append(((row_ref, key), entity, tuple(encoded)))
        except Exception as exc:
            report.results.append(WriteResult(row_index=row_index, error=str(exc)))

    if not prepared:
        return report

    for prepared_chunk in _chunk_prepared_patch_entities(prepared):
        chunk_rows = [item[0] for item in prepared_chunk]
        entities = [item[1] for item in prepared_chunk]
        masks = [item[2] for item in prepared_chunk]
        try:
            stats = _commit_masked_patch_with_retry(
                client,
                entities,
                masks,
                retry=retry,
                write_limiter=write_limiter,
            )
            _add_commit_stats(report, stats)
            report.native_patches += len(entities)
        except _CommitFailure as exc:
            _add_commit_stats(report, exc.stats)
            for (row_index, _), _ in chunk_rows:
                report.results.append(
                    WriteResult(
                        row_index=row_index,
                        action="patch",
                        error=str(exc.original),
                    )
                )
            continue
        for ((row_index, _), key), entity in zip(chunk_rows, entities):
            written_key = (
                DatastoreKey.from_client_key(entity.key)
                if hasattr(entity, "key")
                else key
            )
            report.results.append(WriteResult(row_index=row_index, key=written_key, action="patch"))
    return report


def _patch_chunk_merge(
    rows: list[tuple[tuple[Any, dict[str, Any]], DatastoreKey]],
    *,
    schema: Schema,
    client: Any,
    properties: list[str],
    retry: Any,
    write_limiter: AdaptiveWriteLimiter,
) -> WriteReport:
    from google.cloud import datastore

    report = WriteReport()
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

    for prepared_chunk in _chunk_prepared_entities(list(zip(valid_rows, entities))):
        chunk_rows = [item[0] for item in prepared_chunk]
        entity_chunk = [item[1] for item in prepared_chunk]
        try:
            stats = _commit_entities_with_retry(
                client,
                entity_chunk,
                mode="upsert",
                retry=retry,
                write_limiter=write_limiter,
            )
            _add_commit_stats(report, stats)
            report.fallback_patches += len(entity_chunk)
        except _CommitFailure as exc:
            _add_commit_stats(report, exc.stats)
            for (row_index, _), _ in chunk_rows:
                report.results.append(WriteResult(row_index=row_index, error=str(exc.original)))
            continue

        for ((row_index, _), key), entity in zip(chunk_rows, entity_chunk):
            written_key = (
                DatastoreKey.from_client_key(entity.key)
                if hasattr(entity, "key")
                else key
            )
            report.results.append(WriteResult(row_index=row_index, key=written_key, action="patch"))
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


def _chunk_prepared_entities(
    prepared: list[tuple[tuple[tuple[Any, dict[str, Any]], DatastoreKey], Any]],
    *,
    max_bytes: int = DEFAULT_MAX_BYTES,
) -> Iterator[list[tuple[tuple[tuple[Any, dict[str, Any]], DatastoreKey], Any]]]:
    chunk: list[tuple[tuple[tuple[Any, dict[str, Any]], DatastoreKey], Any]] = []
    size = 0
    for item in prepared:
        entity_size = _entity_size(item[1])
        if chunk and size + entity_size > max_bytes:
            yield chunk
            chunk = []
            size = 0
        chunk.append(item)
        size += entity_size
    if chunk:
        yield chunk


def _chunk_prepared_patch_entities(
    prepared: list[tuple[tuple[tuple[Any, dict[str, Any]], DatastoreKey], Any, tuple[str, ...]]],
    *,
    max_bytes: int = DEFAULT_MAX_BYTES,
) -> Iterator[
    list[tuple[tuple[tuple[Any, dict[str, Any]], DatastoreKey], Any, tuple[str, ...]]]
]:
    chunk: list[tuple[tuple[tuple[Any, dict[str, Any]], DatastoreKey], Any, tuple[str, ...]]] = []
    size = 0
    for item in prepared:
        entity_size = _entity_size(item[1])
        if chunk and size + entity_size > max_bytes:
            yield chunk
            chunk = []
            size = 0
        chunk.append(item)
        size += entity_size
    if chunk:
        yield chunk


def _entity_size(entity: Any) -> int:
    try:
        from google.cloud.datastore.helpers import entity_to_protobuf

        return entity_to_protobuf(entity).ByteSize()
    except Exception:
        try:
            return len(repr(dict(entity)).encode("utf-8"))
        except Exception:
            return len(repr(entity).encode("utf-8"))


def _require_native_patch_backend(client: Any) -> None:
    if not hasattr(client, "_datastore_api"):
        raise _NativePatchUnavailable()
    try:
        import google.cloud.datastore.helpers as helpers
        import google.cloud.datastore_v1.types as types
    except Exception as exc:
        raise _NativePatchUnavailable() from exc
    if not all(
        hasattr(module, name)
        for module, name in (
            (helpers, "entity_to_protobuf"),
            (types, "CommitRequest"),
            (types, "Mutation"),
            (types, "PropertyMask"),
        )
    ):
        raise _NativePatchUnavailable()


def _commit_masked_patch_with_retry(
    client: Any,
    entities: list[Any],
    masks: list[tuple[str, ...]],
    *,
    retry: Any = DEFAULT_COMMIT_RETRY,
    write_limiter: AdaptiveWriteLimiter,
) -> _CommitStats:
    from google.cloud.datastore.helpers import entity_to_protobuf
    from google.cloud.datastore_v1.types import CommitRequest, Mutation, PropertyMask

    def commit_once() -> Any:
        mutations = [
            Mutation(
                upsert=entity_to_protobuf(entity),
                property_mask=PropertyMask(paths=list(mask)),
            )
            for entity, mask in zip(entities, masks)
        ]
        request = CommitRequest(
            project_id=getattr(client, "project", None),
            database_id=_request_database_id(client),
            mode=CommitRequest.Mode.NON_TRANSACTIONAL,
            mutations=mutations,
        )
        return client._datastore_api.commit(request=request)

    return _execute_commit_with_retry(
        commit_once,
        operation_count=len(entities),
        retry=retry,
        retry_safe=True,
        write_limiter=write_limiter,
    )


def _request_database_id(client: Any) -> str:
    database = getattr(client, "database", None) or ""
    return "" if database == "(default)" else database


def _commit_entities_with_retry(
    client: Any,
    entities: list[Any],
    *,
    mode: WriteMode,
    retry: Any = DEFAULT_COMMIT_RETRY,
    write_limiter: AdaptiveWriteLimiter,
) -> _CommitStats:
    def commit_once() -> None:
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

    return _execute_commit_with_retry(
        commit_once,
        operation_count=len(entities),
        retry=retry,
        retry_safe=mode == "upsert",
        write_limiter=write_limiter,
    )


def _execute_commit_with_retry(
    commit_once: Callable[[], Any],
    *,
    operation_count: int,
    retry: Any,
    retry_safe: bool,
    write_limiter: AdaptiveWriteLimiter,
) -> _CommitStats:
    retry_policy = _coerce_commit_retry_policy(retry)
    stats = _CommitStats(batch_size=operation_count)
    attempt = 1
    delay = retry_policy.initial
    deadline_at = perf_counter() + retry_policy.deadline if retry_policy.deadline else None
    while True:
        try:
            throttle_result = write_limiter.before_commit(operation_count)
            stats.throttled_seconds += throttle_result.slept_seconds
            stats.attempts += 1
            started = perf_counter()
            response = commit_once()
            stats.latency_ms = int((perf_counter() - started) * 1000)
            stats.successes += 1
            stats.index_updates += int(getattr(response, "index_updates", 0) or 0)
            write_limiter.record_success()
            return stats
        except Exception as exc:
            stats.failures += 1
            write_limiter.record_failure()
            retryable = retry_safe and _is_retryable_commit_error(exc, retry_policy)
            if (
                not retryable
                or _retry_attempts_exhausted(attempt, retry_policy)
                or _retry_deadline_exhausted(deadline_at)
            ):
                raise _CommitFailure(exc, stats) from exc
            stats.retryable_failures += 1
            stats.retry_attempts += 1
            sleep_for = _bounded_retry_delay(delay, retry_policy, deadline_at)
            if retry_policy.on_retry is not None:
                retry_policy.on_retry(
                    CommitRetryContext(
                        attempt=attempt,
                        delay=sleep_for,
                        exception=exc,
                        retryable=retryable,
                    )
                )
            sleep(sleep_for)
            attempt += 1
            delay = min(retry_policy.max_delay, delay * retry_policy.multiplier)


def _coerce_commit_retry_policy(retry: Any) -> CommitRetryPolicy:
    if retry is None:
        return CommitRetryPolicy(max_attempts=1, deadline=0, jitter=0)
    if isinstance(retry, CommitRetryPolicy):
        return retry
    return CommitRetryPolicy(
        initial=float(
            _retry_attr(retry, "initial", "_initial", default=DEFAULT_COMMIT_RETRY.initial)
        ),
        multiplier=float(
            _retry_attr(retry, "multiplier", "_multiplier", default=DEFAULT_COMMIT_RETRY.multiplier)
        ),
        deadline=float(
            _retry_attr(
                retry,
                "deadline",
                "_deadline",
                "timeout",
                "_timeout",
                default=DEFAULT_COMMIT_RETRY.deadline,
            )
        ),
        max_attempts=_retry_attr(retry, "max_attempts", "_max_attempts", default=None),
        max_delay=float(
            _retry_attr(
                retry,
                "maximum",
                "_maximum",
                "max_delay",
                "_max_delay",
                default=DEFAULT_COMMIT_RETRY.max_delay,
            )
        ),
    )


def _retry_attr(retry: Any, *names: str, default: Any) -> Any:
    for name in names:
        if hasattr(retry, name):
            value = getattr(retry, name)
            if not callable(value):
                return value
    return default


def _retry_attempts_exhausted(attempt: int, retry_policy: CommitRetryPolicy) -> bool:
    return retry_policy.max_attempts is not None and attempt >= retry_policy.max_attempts


def _retry_deadline_exhausted(deadline_at: float | None) -> bool:
    return deadline_at is not None and deadline_at - perf_counter() <= 0


def _bounded_retry_delay(
    delay: float,
    retry_policy: CommitRetryPolicy,
    deadline_at: float | None,
) -> float:
    bounded = min(delay, retry_policy.max_delay)
    if deadline_at is not None:
        remaining = deadline_at - perf_counter()
        if remaining <= 0:
            return 0.0
        bounded = min(bounded, remaining)
    if retry_policy.jitter <= 0 or bounded <= 0:
        return bounded
    low = max(0.0, bounded * (1.0 - retry_policy.jitter))
    return uniform(low, bounded)


def _is_retryable_commit_error(
    exc: Exception,
    retry_policy: CommitRetryPolicy | None = None,
) -> bool:
    if retry_policy is not None and retry_policy.retryable_exceptions:
        if isinstance(exc, retry_policy.retryable_exceptions):
            return True
    if _is_google_retryable_error(exc):
        return True
    message = str(exc).lower()
    retryable_fragments = (
        "aborted",
        "contention",
        "deadline exceeded",
        "goaway",
        "internal",
        "unavailable",
        "resource exhausted",
        "too many requests",
        "application error processing rpc",
        "503",
        "504",
    )
    return any(fragment in message for fragment in retryable_fragments)


def _is_google_retryable_error(exc: Exception) -> bool:
    try:
        from google.api_core import exceptions as google_exceptions
    except Exception:
        return False
    retryable_types = tuple(
        getattr(google_exceptions, name)
        for name in (
            "Aborted",
            "DeadlineExceeded",
            "InternalServerError",
            "ResourceExhausted",
            "ServiceUnavailable",
            "TooManyRequests",
        )
        if hasattr(google_exceptions, name)
    )
    return bool(retryable_types) and isinstance(exc, retryable_types)


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

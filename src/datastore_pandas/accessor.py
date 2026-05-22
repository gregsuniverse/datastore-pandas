"""Instantiated Datastore kind accessors."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any, Literal, Sequence

from datastore_pandas.audit import AuditPolicy
from datastore_pandas.batches import chunk_items
from datastore_pandas.errors import SchemaError
from datastore_pandas.keys import DatastoreKey
from datastore_pandas.planning import WritePlan, _stable_value
from datastore_pandas.reports import PlannedMutation, WriteReport, WriteResult
from datastore_pandas.schema import Schema

Backend = Literal["pandas", "polars"]


@dataclass(frozen=True)
class DatastoreFrame:
    """Bound accessor for one Datastore kind and DataFrame backend."""

    schema: Schema
    client: Any | None = None
    backend: Backend = "pandas"
    namespace: str | None = None
    ancestor: DatastoreKey | None = None
    filters: Sequence[tuple[str, str, Any]] = ()
    order: Sequence[str] = ()
    projection: Sequence[str] | None = None
    distinct_on: Sequence[str] | None = None
    read_only: bool = False
    audit: AuditPolicy | None = None
    enforce_ancestor: bool = True
    batch_size: int = 400
    max_workers: int = 1

    @property
    def kind(self) -> str:
        return self.schema.kind

    def read(
        self,
        *,
        filters: Sequence[tuple[str, str, Any]] | None = None,
        namespace: str | None = None,
        projection: Sequence[str] | None = None,
        order: Sequence[str] | None = None,
        distinct_on: Sequence[str] | None = None,
        ancestor: DatastoreKey | None = None,
        keys_only: bool = False,
        limit: int | None = None,
        include_key: bool = False,
        chunksize: int | None = None,
    ):
        adapter = self._adapter()
        return adapter.read_datastore(
            kind=self.kind,
            client=self.client,
            schema=self.schema,
            filters=self._filters(filters),
            namespace=self._namespace(namespace, ancestor),
            projection=self.projection if projection is None else projection,
            order=self.order if order is None else order,
            distinct_on=self.distinct_on if distinct_on is None else distinct_on,
            ancestor=self.ancestor if ancestor is None else ancestor,
            keys_only=keys_only,
            limit=limit,
            include_key=include_key,
            chunksize=chunksize,
        )

    def iter(
        self,
        *,
        filters: Sequence[tuple[str, str, Any]] | None = None,
        namespace: str | None = None,
        projection: Sequence[str] | None = None,
        order: Sequence[str] | None = None,
        distinct_on: Sequence[str] | None = None,
        ancestor: DatastoreKey | None = None,
        keys_only: bool = False,
        limit: int | None = None,
        include_key: bool = False,
        chunksize: int = 1000,
    ):
        adapter = self._adapter()
        return adapter.iter_datastore(
            kind=self.kind,
            client=self.client,
            schema=self.schema,
            filters=self._filters(filters),
            namespace=self._namespace(namespace, ancestor),
            projection=self.projection if projection is None else projection,
            order=self.order if order is None else order,
            distinct_on=self.distinct_on if distinct_on is None else distinct_on,
            ancestor=self.ancestor if ancestor is None else ancestor,
            keys_only=keys_only,
            limit=limit,
            include_key=include_key,
            chunksize=chunksize,
        )

    def plan_write(
        self,
        df: Any,
        *,
        mode: Literal["insert", "update", "upsert"] = "upsert",
        properties: list[str] | None = None,
        patch: bool = False,
        skip_unchanged: bool = False,
        batch_size: int | None = None,
    ) -> WritePlan:
        df = self._prepare_frame(df)
        adapter = self._adapter()
        plan = adapter.plan_datastore_write(
            df,
            schema=self.schema,
            client=self.client,
            mode=mode,
            properties=properties,
            patch=patch,
            skip_unchanged=skip_unchanged,
            batch_size=batch_size or self.batch_size,
        )
        self._validate_plan_scope(plan)
        return plan

    def write(
        self,
        df: Any,
        *,
        mode: Literal["insert", "update", "upsert"] = "upsert",
        properties: list[str] | None = None,
        batch_size: int | None = None,
        max_workers: int | None = None,
        dry_run: bool = False,
        read_only: bool | None = None,
        skip_unchanged: bool = False,
    ) -> WriteReport:
        df = self._prepare_frame(df)
        self._validate_frame_scope(
            df,
            mode=mode,
            properties=properties,
            batch_size=batch_size or self.batch_size,
        )
        adapter = self._adapter()
        return adapter.to_datastore(
            df,
            schema=self.schema,
            client=self.client,
            mode=mode,
            properties=properties,
            batch_size=batch_size or self.batch_size,
            max_workers=max_workers or self.max_workers,
            dry_run=dry_run,
            read_only=self.read_only if read_only is None else read_only,
            skip_unchanged=skip_unchanged,
        )

    def patch(
        self,
        df: Any,
        *,
        properties: list[str],
        batch_size: int | None = None,
        max_workers: int | None = None,
        dry_run: bool = False,
        read_only: bool | None = None,
        skip_unchanged: bool = False,
    ) -> WriteReport:
        df = self._prepare_frame(df)
        self._validate_frame_scope(
            df,
            properties=properties,
            patch=True,
            batch_size=batch_size or self.batch_size,
        )
        adapter = self._adapter()
        return adapter.patch_datastore(
            df,
            schema=self.schema,
            properties=properties,
            client=self.client,
            batch_size=batch_size or self.batch_size,
            max_workers=max_workers or self.max_workers,
            dry_run=dry_run,
            read_only=self.read_only if read_only is None else read_only,
            skip_unchanged=skip_unchanged,
        )

    def plan_duplicate_cleanup(
        self,
        *,
        by: Sequence[str],
        frame: Any | None = None,
        keep: Literal["first", "last"] = "first",
        order: Sequence[str] = (),
        filters: Sequence[tuple[str, str, Any]] | None = None,
        limit: int | None = None,
    ) -> WritePlan:
        if not by:
            raise SchemaError("Duplicate cleanup requires at least one grouping field.")
        frame = (
            frame
            if frame is not None
            else self.read(filters=filters, limit=limit, include_key=True)
        )
        rows = self._rows_from_frame(frame)
        groups: dict[tuple[Any, ...], list[tuple[int, Any, dict[str, Any]]]] = {}
        for row_position, (row_index, row) in enumerate(rows):
            raw_key = row.get("__key__")
            if not isinstance(raw_key, DatastoreKey):
                raise SchemaError("Duplicate cleanup requires a __key__ DatastoreKey column.")
            group_key = tuple(_stable_value(row.get(name)) for name in by)
            groups.setdefault(group_key, []).append((row_position, row_index, row))

        mutations: list[PlannedMutation] = []
        for group_key, group_rows in groups.items():
            if len(group_rows) < 2:
                continue
            ordered_rows = _order_duplicate_group(group_rows, order=order)
            if keep == "last":
                ordered_rows = list(reversed(ordered_rows))
            elif keep != "first":
                raise SchemaError("Duplicate cleanup keep must be 'first' or 'last'.")
            for row_position, row_index, row in ordered_rows[1:]:
                mutations.append(
                    PlannedMutation(
                        row_position=row_position,
                        row_index=row_index,
                        action="delete",
                        key=row["__key__"],
                        properties={name: row.get(name) for name in by},
                        reason=f"duplicate group {group_key!r}",
                    )
                )
        return WritePlan(tuple(mutations), operation="write", mode="upsert")

    def cleanup_duplicates(
        self,
        *,
        by: Sequence[str],
        frame: Any | None = None,
        keep: Literal["first", "last"] = "first",
        order: Sequence[str] = (),
        filters: Sequence[tuple[str, str, Any]] | None = None,
        limit: int | None = None,
        dry_run: bool = True,
        read_only: bool | None = None,
        batch_size: int | None = None,
    ) -> WriteReport:
        plan = self.plan_duplicate_cleanup(
            by=by,
            frame=frame,
            keep=keep,
            order=order,
            filters=filters,
            limit=limit,
        )
        active_read_only = self.read_only if read_only is None else read_only
        if dry_run or active_read_only:
            return plan.to_report(dry_run=dry_run, read_only=active_read_only)
        return _delete_from_plan(
            plan,
            client=self.client,
            batch_size=batch_size or self.batch_size,
        )

    def with_scope(
        self,
        *,
        namespace: str | None = None,
        ancestor: DatastoreKey | None = None,
        filters: Sequence[tuple[str, str, Any]] | None = None,
        read_only: bool | None = None,
        audit: AuditPolicy | None = None,
    ) -> "DatastoreFrame":
        return replace(
            self,
            namespace=self.namespace if namespace is None else namespace,
            ancestor=self.ancestor if ancestor is None else ancestor,
            filters=self.filters if filters is None else filters,
            read_only=self.read_only if read_only is None else read_only,
            audit=self.audit if audit is None else audit,
        )

    def with_ancestor(self, ancestor: DatastoreKey) -> "DatastoreFrame":
        return self.with_scope(ancestor=ancestor)

    def _adapter(self):
        if self.backend == "pandas":
            from datastore_pandas import io

            return io
        if self.backend == "polars":
            from datastore_pandas import polars

            return polars
        raise ValueError(f"Unsupported DataFrame backend: {self.backend!r}.")

    def _rows_from_frame(self, frame: Any) -> list[tuple[Any, dict[str, Any]]]:
        if self.backend == "pandas":
            from datastore_pandas.io import _iter_rows

            return list(_iter_rows(frame))
        if self.backend == "polars":
            from datastore_pandas.polars import _iter_rows

            return list(_iter_rows(frame))
        raise ValueError(f"Unsupported DataFrame backend: {self.backend!r}.")

    def _prepare_frame(self, df: Any) -> Any:
        if self.audit is None:
            return df
        return self.audit.apply_to_frame(df, backend=self.backend)

    def _validate_frame_scope(
        self,
        df: Any,
        *,
        mode: Literal["insert", "update", "upsert"] = "upsert",
        properties: list[str] | None = None,
        patch: bool = False,
        batch_size: int | None = None,
    ) -> None:
        if self.ancestor is None or not self.enforce_ancestor:
            return
        adapter = self._adapter()
        plan = adapter.plan_datastore_write(
            df,
            schema=self.schema,
            client=self.client,
            mode=mode,
            properties=properties,
            patch=patch,
            batch_size=batch_size or self.batch_size,
        )
        self._validate_plan_scope(plan)

    def _validate_plan_scope(self, plan: WritePlan) -> None:
        if self.ancestor is None or not self.enforce_ancestor:
            return
        ancestor_length = len(self.ancestor.path)
        for mutation in plan.mutations:
            if mutation.key is None:
                continue
            if mutation.key.namespace != self.ancestor.namespace:
                raise SchemaError(
                    "Write key namespace does not match the bound ancestor namespace."
                )
            if mutation.key.path[:ancestor_length] != self.ancestor.path:
                raise SchemaError("Write key path is outside the bound ancestor path.")

    def _filters(
        self,
        filters: Sequence[tuple[str, str, Any]] | None,
    ) -> tuple[tuple[str, str, Any], ...]:
        return tuple(self.filters) + tuple(filters or ())

    def _namespace(
        self,
        namespace: str | None,
        ancestor: DatastoreKey | None,
    ) -> str | None:
        active_ancestor = self.ancestor if ancestor is None else ancestor
        if active_ancestor is not None:
            return active_ancestor.namespace
        return self.namespace if namespace is None else namespace


def kind(
    *,
    schema: Schema,
    client: Any | None = None,
    backend: Backend = "pandas",
    namespace: str | None = None,
    ancestor: DatastoreKey | None = None,
    filters: Sequence[tuple[str, str, Any]] = (),
    order: Sequence[str] = (),
    projection: Sequence[str] | None = None,
    distinct_on: Sequence[str] | None = None,
    read_only: bool = False,
    audit: AuditPolicy | None = None,
    enforce_ancestor: bool = True,
    batch_size: int = 400,
    max_workers: int = 1,
) -> DatastoreFrame:
    """Create a bound accessor for one schema/kind."""

    return DatastoreFrame(
        schema=schema,
        client=client,
        backend=backend,
        namespace=namespace,
        ancestor=ancestor,
        filters=filters,
        order=order,
        projection=projection,
        distinct_on=distinct_on,
        read_only=read_only,
        audit=audit,
        enforce_ancestor=enforce_ancestor,
        batch_size=batch_size,
        max_workers=max_workers,
    )


def _order_duplicate_group(
    group_rows: list[tuple[int, Any, dict[str, Any]]],
    *,
    order: Sequence[str],
) -> list[tuple[int, Any, dict[str, Any]]]:
    ordered_rows = list(group_rows)
    for field in reversed(order):
        descending = field.startswith("-")
        name = field[1:] if descending else field
        ordered_rows.sort(
            key=lambda item: _sortable_value(item[2].get(name)),
            reverse=descending,
        )
    return ordered_rows


def _sortable_value(value: Any) -> tuple[bool, Any]:
    stable = _stable_value(value)
    return (stable is None, stable)


def _delete_from_plan(
    plan: WritePlan,
    *,
    client: Any | None,
    batch_size: int,
) -> WriteReport:
    from datastore_pandas.io import _get_client

    active_client = _get_client(client)
    report = WriteReport(planned=list(plan.mutations))
    for chunk in chunk_items(plan.mutations, max_items=batch_size):
        try:
            with active_client.batch() as batch:
                for mutation in chunk:
                    if mutation.key is None:
                        continue
                    batch.delete(mutation.key.to_client_key(active_client))
        except Exception as exc:
            for mutation in chunk:
                report.results.append(
                    WriteResult(
                        row_index=mutation.row_index,
                        key=mutation.key,
                        action="delete",
                        error=str(exc),
                    )
                )
            continue
        for mutation in chunk:
            report.results.append(
                WriteResult(
                    row_index=mutation.row_index,
                    key=mutation.key,
                    action="delete",
                )
            )
    return report

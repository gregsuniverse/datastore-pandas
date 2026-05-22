"""Instantiated Datastore kind accessors."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any, Literal, Sequence

from datastore_pandas.keys import DatastoreKey
from datastore_pandas.planning import WritePlan
from datastore_pandas.reports import WriteReport
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
        adapter = self._adapter()
        return adapter.plan_datastore_write(
            df,
            schema=self.schema,
            client=self.client,
            mode=mode,
            properties=properties,
            patch=patch,
            skip_unchanged=skip_unchanged,
            batch_size=batch_size or self.batch_size,
        )

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

    def with_scope(
        self,
        *,
        namespace: str | None = None,
        ancestor: DatastoreKey | None = None,
        filters: Sequence[tuple[str, str, Any]] | None = None,
        read_only: bool | None = None,
    ) -> "DatastoreFrame":
        return replace(
            self,
            namespace=self.namespace if namespace is None else namespace,
            ancestor=self.ancestor if ancestor is None else ancestor,
            filters=self.filters if filters is None else filters,
            read_only=self.read_only if read_only is None else read_only,
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
        batch_size=batch_size,
        max_workers=max_workers,
    )

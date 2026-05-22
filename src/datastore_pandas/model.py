"""DataFrame-owning model layer over Datastore kind accessors."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any, Literal, Mapping, Sequence

from datastore_pandas.accessor import Backend, DatastoreFrame, kind as make_kind
from datastore_pandas.errors import DerivedFrameWriteError, SchemaError
from datastore_pandas.planning import WritePlan
from datastore_pandas.reports import WriteReport
from datastore_pandas.schema import Schema


@dataclass(frozen=True)
class DatastoreDataFrame:
    """A DataFrame plus Datastore source/target context."""

    store: DatastoreFrame
    df: Any | None = None
    original_df: Any | None = None
    keep_original: bool = False
    loaded_at: datetime | None = None
    is_derived: bool = False
    lineage: tuple[str, ...] = ()
    target_store: DatastoreFrame | None = None

    @property
    def schema(self) -> Schema:
        return self.store.schema

    @property
    def backend(self) -> Backend:
        return self.store.backend

    @property
    def source_kind(self) -> str:
        return self.store.kind

    @property
    def target_kind(self) -> str:
        return (self.target_store or self.store).kind

    @property
    def is_loaded(self) -> bool:
        return self.df is not None

    def load(
        self,
        *,
        include_key: bool = True,
        keep_original: bool | None = None,
        **read_kwargs: Any,
    ) -> "DatastoreDataFrame":
        frame = self.store.read(include_key=include_key, **read_kwargs)
        snapshot_enabled = self.keep_original if keep_original is None else keep_original
        return replace(
            self,
            df=frame,
            original_df=_clone_frame(frame) if snapshot_enabled else None,
            keep_original=snapshot_enabled,
            loaded_at=datetime.now(timezone.utc),
            is_derived=False,
        )

    def read(self, **kwargs: Any) -> Any:
        return self.load(**kwargs).df

    def refresh(self, **kwargs: Any) -> "DatastoreDataFrame":
        return self.load(keep_original=self.keep_original, **kwargs)

    def snapshot(self) -> "DatastoreDataFrame":
        self._require_frame()
        return replace(self, original_df=_clone_frame(self.df), keep_original=True)

    def replace_df(
        self,
        df: Any,
        *,
        derived: bool = False,
        lineage: str | Sequence[str] | None = None,
    ) -> "DatastoreDataFrame":
        return replace(
            self,
            df=df,
            is_derived=derived,
            lineage=_extend_lineage(self.lineage, lineage),
        )

    def derive(
        self,
        df: Any,
        *,
        target_kind: str | None = None,
        target_schema: Schema | None = None,
        lineage: str | Sequence[str] | None = None,
    ) -> "DatastoreDataFrame":
        model = replace(
            self,
            df=df,
            original_df=None,
            is_derived=True,
            lineage=_extend_lineage(self.lineage, lineage or "derived"),
        )
        if target_kind is not None or target_schema is not None:
            model = model.with_target(kind=target_kind, schema=target_schema)
        return model

    def aggregate(
        self,
        *,
        by: Sequence[str],
        metrics: Mapping[str, str],
        target_kind: str | None = None,
        target_schema: Schema | None = None,
    ) -> "DatastoreDataFrame":
        self._require_frame()
        if not by:
            raise SchemaError("aggregate requires at least one group-by column.")
        if not metrics:
            raise SchemaError("aggregate requires at least one metric.")
        frame = _aggregate_frame(self.df, backend=self.backend, by=by, metrics=metrics)
        return self.derive(
            frame,
            target_kind=target_kind,
            target_schema=target_schema,
            lineage=f"aggregate by {', '.join(by)}",
        )

    def with_target(
        self,
        *,
        kind: str | None = None,
        schema: Schema | None = None,
        namespace: str | None = None,
        ancestor: Any | None = None,
    ) -> "DatastoreDataFrame":
        target_schema = schema or self.schema
        if kind is not None and target_schema.kind != kind:
            target_schema = _schema_with_kind(target_schema, kind)
        target_store = replace(
            self.store,
            schema=target_schema,
            namespace=self.store.namespace if namespace is None else namespace,
            ancestor=self.store.ancestor if ancestor is None else ancestor,
        )
        return replace(self, target_store=target_store)

    def plan_write(
        self,
        *,
        mode: Literal["insert", "update", "upsert"] = "upsert",
        properties: list[str] | None = None,
        patch: bool = False,
        skip_unchanged: bool = False,
        allow_shape_change: bool = False,
        **kwargs: Any,
    ) -> WritePlan:
        frame = self._require_frame()
        target = self._resolve_write_store(allow_shape_change=allow_shape_change)
        return target.plan_write(
            frame,
            mode=mode,
            properties=properties,
            patch=patch,
            skip_unchanged=skip_unchanged,
            **kwargs,
        )

    def write(
        self,
        *,
        mode: Literal["insert", "update", "upsert"] = "upsert",
        properties: list[str] | None = None,
        skip_unchanged: bool = False,
        allow_shape_change: bool = False,
        **kwargs: Any,
    ) -> WriteReport:
        frame = self._require_frame()
        target = self._resolve_write_store(allow_shape_change=allow_shape_change)
        return target.write(
            frame,
            mode=mode,
            properties=properties,
            skip_unchanged=skip_unchanged,
            **kwargs,
        )

    def patch(
        self,
        *,
        properties: list[str],
        skip_unchanged: bool = False,
        allow_shape_change: bool = False,
        **kwargs: Any,
    ) -> WriteReport:
        frame = self._require_frame()
        target = self._resolve_write_store(allow_shape_change=allow_shape_change)
        return target.patch(
            frame,
            properties=properties,
            skip_unchanged=skip_unchanged,
            **kwargs,
        )

    def _require_frame(self) -> Any:
        if self.df is None:
            raise SchemaError("DataFrame model is not loaded and has no df.")
        return self.df

    def _resolve_write_store(self, *, allow_shape_change: bool) -> DatastoreFrame:
        if self.target_store is not None:
            return self.target_store
        if self.is_derived:
            raise DerivedFrameWriteError(
                "Derived DataFrame cannot write back to source kind. Use with_target(...)."
            )
        if not allow_shape_change and self.original_df is not None:
            if _frame_len(self.df) != _frame_len(self.original_df):
                raise DerivedFrameWriteError(
                    "DataFrame row count differs from the original source frame. "
                    "Use with_target(...) or allow_shape_change=True."
                )
        if not _has_write_identity(self.df, self.schema):
            raise DerivedFrameWriteError(
                "Source writes require __key__ or all schema key source columns."
            )
        return self.store


def dspdf(
    *,
    kind: str | None = None,
    schema: Schema,
    client: Any | None = None,
    backend: Backend = "pandas",
    df: Any | None = None,
    keep_original: bool = False,
    **store_kwargs: Any,
) -> DatastoreDataFrame:
    """Create a DataFrame-owning model for one Datastore kind."""

    if kind is not None and schema.kind != kind:
        schema = _schema_with_kind(schema, kind)
    store = make_kind(
        schema=schema,
        client=client,
        backend=backend,
        **store_kwargs,
    )
    return DatastoreDataFrame(
        store=store,
        df=df,
        original_df=_clone_frame(df) if keep_original and df is not None else None,
        keep_original=keep_original,
    )


def _clone_frame(df: Any) -> Any:
    if df is None:
        return None
    if hasattr(df, "clone"):
        return df.clone()
    if hasattr(df, "copy"):
        return df.copy()
    return df


def _frame_len(df: Any) -> int:
    if df is None:
        return 0
    if hasattr(df, "height"):
        return int(df.height)
    return len(df)


def _has_write_identity(df: Any, schema: Schema) -> bool:
    columns = set(getattr(df, "columns", ()))
    if "__key__" in columns:
        return True
    if schema.key is None:
        return False
    sources = set()
    for _, key_part in schema.key.path:
        if key_part.source:
            sources.add(key_part.source)
    if schema.key.namespace_source:
        sources.add(schema.key.namespace_source)
    return sources.issubset(columns)


def _extend_lineage(
    current: tuple[str, ...],
    next_lineage: str | Sequence[str] | None,
) -> tuple[str, ...]:
    if next_lineage is None:
        return current
    if isinstance(next_lineage, str):
        return current + (next_lineage,)
    return current + tuple(next_lineage)


def _aggregate_frame(
    df: Any,
    *,
    backend: Backend,
    by: Sequence[str],
    metrics: Mapping[str, str],
) -> Any:
    if backend == "polars":
        import polars as pl

        expressions = []
        for column, metric in metrics.items():
            output = f"{column}_{metric}"
            if metric == "count":
                expressions.append(pl.col(column).count().alias(output))
            elif metric == "sum":
                expressions.append(pl.col(column).sum().alias(output))
            elif metric == "mean":
                expressions.append(pl.col(column).mean().alias(output))
            elif metric == "min":
                expressions.append(pl.col(column).min().alias(output))
            elif metric == "max":
                expressions.append(pl.col(column).max().alias(output))
            else:
                raise SchemaError(f"Unsupported aggregate metric: {metric!r}.")
        return df.group_by(list(by)).agg(expressions)

    import pandas as pd

    unsupported = {metric for metric in metrics.values()} - {"count", "sum", "mean", "min", "max"}
    if unsupported:
        raise SchemaError(f"Unsupported aggregate metric: {', '.join(sorted(unsupported))}.")
    grouped = df.groupby(list(by), dropna=False).agg(metrics).reset_index()
    return pd.DataFrame(
        grouped.rename(columns={name: f"{name}_{metric}" for name, metric in metrics.items()})
    )


def _schema_with_kind(schema: Schema, kind: str) -> Schema:
    key = schema.key
    if key is not None and key.path:
        path = list(key.path)
        _, key_part = path[-1]
        path[-1] = (kind, key_part)
        key = replace(key, path=tuple(path))
    return replace(schema, kind=kind, key=key)

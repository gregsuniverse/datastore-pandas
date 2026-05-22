"""Audit field helpers for bound accessors."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Literal

MissingMode = Literal["missing", "always", "never"]


@dataclass(frozen=True)
class AuditPolicy:
    created: str | None = None
    created_at: str | None = None
    modified: str | None = None
    modified_at: str | None = None
    updated_at: str | None = None
    imported_at: str | None = None
    now: Callable[[], Any] = lambda: datetime.now(timezone.utc)
    set_created: MissingMode = "missing"
    set_created_at: MissingMode = "missing"
    set_modified: MissingMode = "always"
    set_modified_at: MissingMode = "always"
    set_updated_at: MissingMode = "always"
    set_imported_at: MissingMode = "missing"

    def apply_to_frame(self, df: Any, *, backend: Literal["pandas", "polars"]) -> Any:
        values = self.values()
        if not values:
            return df
        if backend == "pandas":
            return self._apply_to_pandas(df, values)
        if backend == "polars":
            return self._apply_to_polars(df, values)
        raise ValueError(f"Unsupported DataFrame backend: {backend!r}.")

    def values(self) -> dict[str, tuple[Any, MissingMode]]:
        now = self.now()
        values: dict[str, tuple[Any, MissingMode]] = {}
        if self.created:
            values[self.created] = (now, self.set_created)
        if self.created_at:
            values[self.created_at] = (now, self.set_created_at)
        if self.modified:
            values[self.modified] = (now, self.set_modified)
        if self.modified_at:
            values[self.modified_at] = (now, self.set_modified_at)
        if self.updated_at:
            values[self.updated_at] = (now, self.set_updated_at)
        if self.imported_at:
            values[self.imported_at] = (now, self.set_imported_at)
        return values

    def _apply_to_pandas(self, df: Any, values: dict[str, tuple[Any, MissingMode]]) -> Any:
        import pandas as pd

        updated = df.copy()
        for name, (value, mode) in values.items():
            if mode == "never":
                continue
            if name not in updated.columns:
                if mode in {"always", "missing"}:
                    updated[name] = value
                continue
            if mode == "always":
                updated[name] = value
            elif mode == "missing":
                mask = pd.isna(updated[name])
                updated.loc[mask, name] = value
        return updated

    def _apply_to_polars(self, df: Any, values: dict[str, tuple[Any, MissingMode]]) -> Any:
        import polars as pl

        expressions = []
        for name, (value, mode) in values.items():
            if mode == "never":
                continue
            if name not in df.columns or mode == "always":
                expressions.append(pl.lit(value).alias(name))
            elif mode == "missing":
                expressions.append(
                    pl.when(pl.col(name).is_null())
                    .then(pl.lit(value))
                    .otherwise(pl.col(name))
                    .alias(name)
                )
        if not expressions:
            return df
        return df.with_columns(expressions)

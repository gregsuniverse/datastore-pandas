"""Write result reporting."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from datastore_pandas.keys import DatastoreKey


@dataclass(frozen=True)
class WriteResult:
    row_index: Any
    key: DatastoreKey | None = None
    version: int | None = None
    create_time: datetime | None = None
    update_time: datetime | None = None
    conflict_detected: bool = False
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None and not self.conflict_detected


@dataclass
class WriteReport:
    results: list[WriteResult] = field(default_factory=list)
    index_updates: int = 0

    @property
    def succeeded(self) -> int:
        return sum(result.ok for result in self.results)

    @property
    def failed(self) -> int:
        return len(self.results) - self.succeeded

    def extend(self, other: "WriteReport") -> None:
        self.results.extend(other.results)
        self.index_updates += other.index_updates

    def raise_for_errors(self) -> None:
        errors = [result for result in self.results if not result.ok]
        if errors:
            sample = "; ".join(result.error or "conflict detected" for result in errors[:3])
            raise RuntimeError(f"{len(errors)} Datastore write(s) failed: {sample}")

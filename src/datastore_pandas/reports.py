"""Write result reporting."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal, Mapping

from datastore_pandas.keys import DatastoreKey

WriteAction = Literal["create", "update", "upsert", "patch", "skip", "delete", "error"]


@dataclass(frozen=True)
class PlannedMutation:
    row_position: int
    row_index: Any
    action: WriteAction
    key: DatastoreKey | None = None
    properties: Mapping[str, Any] = field(default_factory=dict)
    exclude_from_indexes: tuple[str, ...] = ()
    reason: str | None = None
    error: str | None = None

    @property
    def should_write(self) -> bool:
        return self.error is None and self.action in {
            "create",
            "update",
            "upsert",
            "patch",
            "delete",
        }


@dataclass(frozen=True)
class WriteResult:
    row_index: Any
    key: DatastoreKey | None = None
    action: WriteAction | None = None
    skipped: bool = False
    dry_run: bool = False
    reason: str | None = None
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
    planned: list[PlannedMutation] = field(default_factory=list)
    index_updates: int = 0
    dry_run: bool = False
    read_only: bool = False
    commit_attempts: int = 0
    commit_successes: int = 0
    commit_failures: int = 0
    retry_attempts: int = 0
    retryable_failures: int = 0
    throttled_seconds: float = 0.0
    batch_sizes: list[int] = field(default_factory=list)
    batch_latency_ms: list[int] = field(default_factory=list)
    native_patches: int = 0
    fallback_patches: int = 0

    @property
    def succeeded(self) -> int:
        return sum(result.ok for result in self.results)

    @property
    def failed(self) -> int:
        return len(self.results) - self.succeeded

    @property
    def skipped(self) -> int:
        return sum(result.skipped for result in self.results)

    @property
    def planned_writes(self) -> int:
        return sum(mutation.should_write for mutation in self.planned)

    @property
    def would_write(self) -> int:
        return self.planned_writes

    @property
    def wrote(self) -> int:
        return sum(
            result.ok
            and not result.dry_run
            and not result.skipped
            and result.action in {"create", "update", "upsert", "patch", "delete"}
            for result in self.results
        )

    def extend(self, other: "WriteReport") -> None:
        self.results.extend(other.results)
        self.planned.extend(other.planned)
        self.index_updates += other.index_updates
        self.commit_attempts += other.commit_attempts
        self.commit_successes += other.commit_successes
        self.commit_failures += other.commit_failures
        self.retry_attempts += other.retry_attempts
        self.retryable_failures += other.retryable_failures
        self.throttled_seconds += other.throttled_seconds
        self.batch_sizes.extend(other.batch_sizes)
        self.batch_latency_ms.extend(other.batch_latency_ms)
        self.native_patches += other.native_patches
        self.fallback_patches += other.fallback_patches

    def raise_for_errors(self) -> None:
        errors = [result for result in self.results if not result.ok]
        if errors:
            sample = "; ".join(result.error or "conflict detected" for result in errors[:3])
            raise RuntimeError(f"{len(errors)} Datastore write(s) failed: {sample}")

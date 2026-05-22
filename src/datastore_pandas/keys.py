"""Datastore key representation and row-to-key mapping."""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any, Iterable, Literal, Mapping, Sequence

from datastore_pandas.errors import KeyValidationError

IdentifierKind = Literal["id", "name", "auto"]


@dataclass(frozen=True)
class DatastoreKey:
    """Stable, serializable representation of a Datastore key.

    A key's path preserves the distinction between string names and integer IDs.
    `None` is allowed only for the final path element and represents an incomplete
    key for auto-ID allocation.
    """

    path: tuple[tuple[str, int | str | None], ...]
    project: str | None = None
    database: str | None = None
    namespace: str | None = None

    def __post_init__(self) -> None:
        if not self.path:
            raise KeyValidationError("A key must contain at least one path element.")
        for index, (kind, identifier) in enumerate(self.path):
            if not isinstance(kind, str) or not kind:
                raise KeyValidationError("Key kind values must be non-empty strings.")
            if kind.startswith("__") and kind.endswith("__"):
                raise KeyValidationError(f"Reserved Datastore kind is not writable: {kind!r}.")
            if identifier is None and index != len(self.path) - 1:
                raise KeyValidationError("Only the final path element may be incomplete.")
            if isinstance(identifier, bool):
                raise KeyValidationError("Boolean identifiers are ambiguous; use int or str.")
            if isinstance(identifier, int) and identifier == 0:
                raise KeyValidationError("Numeric Datastore key IDs must not be zero.")
            if isinstance(identifier, str):
                if not identifier:
                    raise KeyValidationError("String Datastore key names must not be empty.")
                if identifier.startswith("__") and identifier.endswith("__"):
                    raise KeyValidationError(
                        f"Reserved Datastore key name is not writable: {identifier!r}."
                    )

    @property
    def is_complete(self) -> bool:
        return self.path[-1][1] is not None

    @property
    def flat_path(self) -> tuple[Any, ...]:
        parts: list[Any] = []
        for kind, identifier in self.path:
            parts.extend([kind, identifier])
        return tuple(parts)

    def without_partition(self) -> "DatastoreKey":
        return DatastoreKey(path=self.path)

    def to_json(self) -> str:
        payload = {
            "project": self.project,
            "database": self.database,
            "namespace": self.namespace,
            "path": [
                {
                    "kind": kind,
                    "id": identifier
                    if isinstance(identifier, int) and not isinstance(identifier, bool)
                    else None,
                    "name": identifier if isinstance(identifier, str) else None,
                }
                for kind, identifier in self.path
            ],
        }
        return json.dumps(payload, separators=(",", ":"), sort_keys=True)

    @classmethod
    def from_json(cls, payload: str) -> "DatastoreKey":
        raw = json.loads(payload)
        path = []
        for element in raw["path"]:
            if element.get("id") is not None and element.get("name") is not None:
                raise KeyValidationError("A key path element cannot contain both id and name.")
            identifier = element.get("id") if element.get("id") is not None else element.get("name")
            path.append((element["kind"], identifier))
        return cls(
            path=tuple(path),
            project=raw.get("project"),
            database=raw.get("database"),
            namespace=raw.get("namespace"),
        )

    def to_client_key(self, client: Any):
        """Build a `google.cloud.datastore.Key` using the supplied client."""

        if self.database is not None or self.project is not None:
            from google.cloud import datastore

            return datastore.Key(
                *self.flat_path,
                project=self.project or getattr(client, "project", None),
                namespace=self.namespace,
                database=self.database,
            )
        return client.key(
            *self.flat_path,
            namespace=self.namespace,
        )

    @classmethod
    def from_client_key(cls, key: Any) -> "DatastoreKey":
        """Convert a `google.cloud.datastore.Key` or protobuf-like key to DatastoreKey."""

        if hasattr(key, "flat_path"):
            return cls(
                path=_path_from_flat_path(key.flat_path),
                project=getattr(key, "project", None),
                database=getattr(key, "database", None),
                namespace=getattr(key, "namespace", None),
            )

        partition = getattr(key, "partition_id", None)
        path = []
        for element in getattr(key, "path", []):
            identifier = getattr(element, "id", None) or getattr(element, "name", None)
            path.append((element.kind, identifier))
        return cls(
            path=tuple(path),
            project=getattr(partition, "project_id", None),
            database=getattr(partition, "database_id", None),
            namespace=getattr(partition, "namespace_id", None),
        )


@dataclass(frozen=True)
class KeyPart:
    """Describes how one key path identifier is populated from a DataFrame row."""

    source: str | None = None
    kind: IdentifierKind = "name"
    constant: int | str | None = None

    def resolve(self, row: Mapping[str, Any]) -> int | str | None:
        value = self.constant if self.constant is not None else row.get(self.source or "")
        if _is_missing(value):
            if self.kind == "auto":
                return None
            raise KeyValidationError(f"Missing key value for source {self.source!r}.")

        if self.kind == "auto":
            raise KeyValidationError("Auto key parts cannot provide a concrete identifier.")
        if self.kind == "id":
            if isinstance(value, bool):
                raise KeyValidationError("Boolean values cannot be Datastore numeric IDs.")
            try:
                value = int(value)
            except (TypeError, ValueError) as exc:
                raise KeyValidationError(
                    f"Key ID source {self.source!r} must be int-like."
                ) from exc
            if value == 0:
                raise KeyValidationError("Datastore numeric IDs must not be zero.")
            return value
        if not isinstance(value, str):
            value = str(value)
        if not value:
            raise KeyValidationError("Datastore key names must not be empty.")
        return value


@dataclass(frozen=True)
class KeySpec:
    """Maps rows to Datastore keys.

    `path` is an ordered sequence of `(kind, KeyPart)` pairs. Ancestor paths are
    represented by putting their elements before the leaf element.
    """

    path: Sequence[tuple[str, KeyPart]]
    project: str | None = None
    database: str | None = None
    namespace: str | None = None
    namespace_source: str | None = None

    def build(self, row: Mapping[str, Any]) -> DatastoreKey:
        namespace = self.namespace
        if self.namespace_source:
            raw_namespace = row.get(self.namespace_source)
            namespace = None if _is_missing(raw_namespace) else str(raw_namespace)
        return DatastoreKey(
            path=tuple((kind, part.resolve(row)) for kind, part in self.path),
            project=self.project,
            database=self.database,
            namespace=namespace,
        )

    @classmethod
    def from_columns(
        cls,
        kind: str,
        columns: str | Sequence[str],
        *,
        id_kind: IdentifierKind = "name",
        namespace: str | None = None,
        namespace_source: str | None = None,
    ) -> "KeySpec":
        if isinstance(columns, str):
            columns = [columns]
        if len(columns) != 1:
            raise KeyValidationError(
                "Use KeySpec(path=...) for ancestor or composite-derived key paths."
            )
        return cls(
            path=[(kind, KeyPart(columns[0], kind=id_kind))],
            namespace=namespace,
            namespace_source=namespace_source,
        )


def key_policy(
    kind: str,
    *,
    id_field: str | None = None,
    key_field: str | None = None,
    id_kind: IdentifierKind = "name",
    namespace: str | None = None,
    namespace_field: str | None = None,
    ancestors: Sequence[tuple[str, KeyPart | str | int]] = (),
) -> KeySpec:
    """Build a deterministic row-derived key policy for one kind.

    `id_field` and `key_field` are aliases; `key_field` is provided for callers
    that name their logical identifier separately from Datastore terminology.
    """

    source = id_field or key_field
    if source is None:
        raise KeyValidationError("key_policy requires id_field or key_field.")
    path = [(ancestor_kind, _coerce_key_part(part)) for ancestor_kind, part in ancestors]
    path.append((kind, KeyPart(source, kind=id_kind)))
    return KeySpec(
        path=path,
        namespace=namespace,
        namespace_source=namespace_field,
    )


def _path_from_flat_path(flat_path: Iterable[Any]) -> tuple[tuple[str, int | str | None], ...]:
    parts = tuple(flat_path)
    if len(parts) % 2:
        raise KeyValidationError("Datastore flat_path must contain kind/id pairs.")
    return tuple((str(parts[i]), parts[i + 1]) for i in range(0, len(parts), 2))


def _coerce_key_part(value: KeyPart | str | int) -> KeyPart:
    if isinstance(value, KeyPart):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return KeyPart(constant=value, kind="id")
    return KeyPart(str(value), kind="name")


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        import pandas as pd

        return bool(pd.isna(value))
    except Exception:
        return False

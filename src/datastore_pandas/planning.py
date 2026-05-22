"""Write planning helpers for dry runs and skip-unchanged writes."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Literal, Mapping, Sequence

from datastore_pandas.batches import chunk_items, validate_unique_complete_keys
from datastore_pandas.errors import SchemaError
from datastore_pandas.keys import DatastoreKey
from datastore_pandas.reports import PlannedMutation, WriteAction, WriteReport, WriteResult
from datastore_pandas.schema import Schema

WriteOperation = Literal["write", "patch"]
WriteMode = Literal["insert", "update", "upsert"]


@dataclass(frozen=True)
class WritePlan:
    mutations: tuple[PlannedMutation, ...]
    operation: WriteOperation
    mode: WriteMode = "upsert"
    skip_unchanged: bool = False

    @property
    def has_errors(self) -> bool:
        return any(mutation.error is not None for mutation in self.mutations)

    @property
    def write_positions(self) -> set[int]:
        return {mutation.row_position for mutation in self.mutations if mutation.should_write}

    def to_report(self, *, dry_run: bool = False, read_only: bool = False) -> WriteReport:
        report = WriteReport(
            planned=list(self.mutations),
            dry_run=dry_run,
            read_only=read_only,
        )
        for mutation in self.mutations:
            if read_only and mutation.should_write:
                report.results.append(
                    WriteResult(
                        row_index=mutation.row_index,
                        key=mutation.key,
                        action=mutation.action,
                        dry_run=dry_run,
                        error="read_only prevents write execution",
                        reason=mutation.reason,
                    )
                )
                continue
            report.results.append(
                WriteResult(
                    row_index=mutation.row_index,
                    key=mutation.key,
                    action=mutation.action,
                    skipped=mutation.action == "skip",
                    dry_run=dry_run,
                    reason=mutation.reason,
                    error=mutation.error,
                )
            )
        return report


def plan_write_rows(
    rows: Sequence[tuple[Any, Mapping[str, Any]]],
    *,
    schema: Schema,
    client: Any,
    operation: WriteOperation,
    mode: WriteMode = "upsert",
    properties: list[str] | None = None,
    skip_unchanged: bool = False,
    batch_size: int = 400,
) -> WritePlan:
    """Build a write plan from row dictionaries without committing mutations."""

    candidates: list[PlannedMutation] = []
    keys: list[DatastoreKey] = []
    for row_position, (row_index, row) in enumerate(rows):
        try:
            if operation == "write":
                schema.validate_row(row)
            key = schema.key_for_row(row)
            if not key.is_complete:
                raise SchemaError(f"{operation} requires complete keys.")
            encoded, exclude_from_indexes = schema.encode_properties(row, properties=properties)
            candidates.append(
                PlannedMutation(
                    row_position=row_position,
                    row_index=row_index,
                    key=key,
                    action=_default_action(operation, mode),
                    properties=encoded,
                    exclude_from_indexes=tuple(exclude_from_indexes),
                )
            )
            keys.append(key)
        except Exception as exc:
            candidates.append(
                PlannedMutation(
                    row_position=row_position,
                    row_index=row_index,
                    key=None,
                    action="error",
                    error=str(exc),
                )
            )

    validate_unique_complete_keys(keys)
    if not skip_unchanged:
        return WritePlan(tuple(candidates), operation=operation, mode=mode)

    existing_by_key = _fetch_existing(client, keys, batch_size=batch_size)
    planned = [
        _classify_against_existing(mutation, existing_by_key, operation=operation, mode=mode)
        for mutation in candidates
    ]
    return WritePlan(
        tuple(planned),
        operation=operation,
        mode=mode,
        skip_unchanged=skip_unchanged,
    )


def _default_action(operation: WriteOperation, mode: WriteMode) -> WriteAction:
    if operation == "patch":
        return "patch"
    return mode


def _fetch_existing(
    client: Any,
    keys: Iterable[DatastoreKey],
    *,
    batch_size: int,
) -> dict[str, Mapping[str, Any]]:
    existing_by_key: dict[str, Mapping[str, Any]] = {}
    for chunk in chunk_items(keys, max_items=batch_size):
        client_keys = [key.to_client_key(client) for key in chunk]
        for entity in client.get_multi(client_keys):
            if entity is None:
                continue
            key = DatastoreKey.from_client_key(entity.key)
            existing_by_key[_key_identity(key)] = dict(entity)
    return existing_by_key


def _classify_against_existing(
    mutation: PlannedMutation,
    existing_by_key: Mapping[str, Mapping[str, Any]],
    *,
    operation: WriteOperation,
    mode: WriteMode,
) -> PlannedMutation:
    if mutation.error is not None or mutation.key is None:
        return mutation
    existing = existing_by_key.get(_key_identity(mutation.key))
    if existing is None:
        if mode == "update" and operation == "write":
            return _replace_mutation(mutation, action="error", error="update target does not exist")
        return _replace_mutation(mutation, action="create", reason="entity does not exist")

    if mode == "insert" and operation == "write":
        return _replace_mutation(mutation, action="error", error="insert target already exists")

    if operation == "patch":
        if not mutation.properties:
            return _replace_mutation(mutation, action="skip", reason="no properties to patch")
        if _patch_is_unchanged(existing, mutation.properties):
            return _replace_mutation(mutation, action="skip", reason="patch properties unchanged")
        return _replace_mutation(mutation, action="patch", reason="patch properties changed")

    if _full_entity_is_unchanged(existing, mutation.properties):
        return _replace_mutation(mutation, action="skip", reason="entity unchanged")
    return _replace_mutation(mutation, action="update", reason="entity changed")


def _replace_mutation(
    mutation: PlannedMutation,
    *,
    action: Any,
    reason: str | None = None,
    error: str | None = None,
) -> PlannedMutation:
    return PlannedMutation(
        row_position=mutation.row_position,
        row_index=mutation.row_index,
        action=action,
        key=mutation.key,
        properties=mutation.properties,
        exclude_from_indexes=mutation.exclude_from_indexes,
        reason=reason,
        error=error,
    )


def _patch_is_unchanged(
    existing: Mapping[str, Any],
    patch_properties: Mapping[str, Any],
) -> bool:
    for name, value in patch_properties.items():
        if _stable_value(existing.get(name)) != _stable_value(value):
            return False
    return True


def _full_entity_is_unchanged(
    existing: Mapping[str, Any],
    properties: Mapping[str, Any],
) -> bool:
    return _stable_mapping(existing) == _stable_mapping(properties)


def _stable_mapping(value: Mapping[str, Any]) -> dict[str, Any]:
    return {name: _stable_value(value[name]) for name in sorted(value)}


def _stable_value(value: Any) -> Any:
    if isinstance(value, DatastoreKey):
        return {"__key__": _key_identity(value)}
    if hasattr(value, "flat_path") or hasattr(value, "path"):
        try:
            return {"__key__": _key_identity(DatastoreKey.from_client_key(value))}
        except Exception:
            pass
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        value = value.astimezone(timezone.utc)
        return value.isoformat(timespec="microseconds")
    if isinstance(value, Mapping):
        return _stable_mapping(value)
    if isinstance(value, (list, tuple)):
        return [_stable_value(item) for item in value]
    return value


def _key_identity(key: DatastoreKey) -> str:
    return DatastoreKey(path=key.path, namespace=key.namespace).to_json()

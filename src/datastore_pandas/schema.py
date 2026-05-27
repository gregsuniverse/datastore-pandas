"""Schema objects used for safe pandas <-> Datastore conversion."""

from __future__ import annotations

from dataclasses import dataclass, field as dataclass_field
from typing import Any, Literal, Mapping

from datastore_pandas.errors import SchemaError
from datastore_pandas.keys import DatastoreKey, KeySpec
from datastore_pandas.types import DatastoreType, is_missing

MissingPolicy = Literal["omit", "null", "error"]
DEFAULT_UNSET = object()
OMIT_PROPERTY = object()


@dataclass(frozen=True)
class Field:
    dtype: DatastoreType
    nullable: bool = True
    indexed: bool = True
    default: Any = DEFAULT_UNSET
    missing_policy: MissingPolicy = "omit"

    def encode(self, value: Any) -> Any:
        if is_missing(value):
            if self.default is not DEFAULT_UNSET:
                value = self.default() if callable(self.default) else self.default
            elif self.missing_policy == "omit":
                if not self.nullable:
                    raise SchemaError(f"Non-nullable field of type {self.dtype.name} is missing.")
                return OMIT_PROPERTY
            elif self.missing_policy == "null":
                if not self.nullable:
                    raise SchemaError(f"Non-nullable field of type {self.dtype.name} is missing.")
                return None
            elif self.missing_policy == "error":
                raise SchemaError(f"Non-nullable field of type {self.dtype.name} is missing.")
            else:
                raise SchemaError(f"Unsupported missing policy: {self.missing_policy!r}.")
        encoded = self.dtype.to_datastore(value)
        if self.indexed and encoded is not None:
            self.dtype.validate_indexed(encoded)
        return encoded

    def decode(self, value: Any) -> Any:
        if value is None:
            return None
        return self.dtype.from_datastore(value)


@dataclass(frozen=True)
class Schema:
    kind: str
    properties: Mapping[str, Field] = dataclass_field(default_factory=dict)
    key: KeySpec | None = None
    strict: bool = True

    def validate_row(self, row: Mapping[str, Any]) -> None:
        missing = [
            name
            for name, field in self.properties.items()
            if not field.nullable and (name not in row or is_missing(row.get(name)))
        ]
        if missing:
            raise SchemaError(f"Missing required fields: {', '.join(missing)}.")
        if self.strict:
            unknown = {
                name
                for name, value in row.items()
                if name not in self.properties and name != "__key__" and not is_missing(value)
            }
            key_sources = set()
            if self.key is not None:
                for _, key_part in self.key.path:
                    if key_part.source:
                        key_sources.add(key_part.source)
                if self.key.namespace_source:
                    key_sources.add(self.key.namespace_source)
            unknown -= key_sources
            if unknown:
                raise SchemaError(
                    f"Unknown fields for strict schema: {', '.join(sorted(unknown))}."
                )

    def encode_properties(
        self,
        row: Mapping[str, Any],
        *,
        properties: list[str] | None = None,
    ) -> tuple[dict[str, Any], list[str]]:
        names = list(self.properties) if properties is None else properties
        encoded: dict[str, Any] = {}
        exclude_from_indexes: list[str] = []
        for name in names:
            if name not in self.properties:
                if self.strict:
                    raise SchemaError(f"Field {name!r} is not declared in schema.")
                continue
            field = self.properties[name]
            encoded_value = field.encode(row.get(name))
            if encoded_value is OMIT_PROPERTY:
                continue
            encoded[name] = encoded_value
            if not field.indexed and name in encoded:
                exclude_from_indexes.append(name)
        return encoded, exclude_from_indexes

    def decode_entity(
        self,
        entity: Mapping[str, Any],
        *,
        key: DatastoreKey | None = None,
        include_key: bool = False,
    ) -> dict[str, Any]:
        record: dict[str, Any] = {}
        for name, value in entity.items():
            if name in self.properties:
                record[name] = self.properties[name].decode(value)
            elif not self.strict:
                record[name] = value
        if include_key:
            record["__key__"] = key
        return record

    def key_for_row(self, row: Mapping[str, Any]) -> DatastoreKey:
        if "__key__" in row and not is_missing(row["__key__"]):
            raw_key = row["__key__"]
            if isinstance(raw_key, DatastoreKey):
                return raw_key
            if isinstance(raw_key, str):
                return DatastoreKey.from_json(raw_key)
        if self.key is None:
            raise SchemaError("Schema has no KeySpec and row does not contain __key__.")
        return self.key.build(row)

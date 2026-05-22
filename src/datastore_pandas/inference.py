"""Schema inference for Datastore entities and DataFrames."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, Mapping, Sequence

from datastore_pandas.errors import SchemaError
from datastore_pandas.keys import DatastoreKey, KeySpec
from datastore_pandas.query import QuerySpec
from datastore_pandas.schema import Field, Schema
from datastore_pandas.types import (
    ArrayType,
    BlobType,
    BoolType,
    DatastoreType,
    EmbeddedEntityType,
    Float64Type,
    GeoPoint,
    GeoPointType,
    Int64Type,
    KeyType,
    StringType,
    TimestampType,
    is_missing,
)

MixedTypePolicy = Literal["object", "string", "error"]


@dataclass(frozen=True)
class InferredField:
    name: str
    dtype: DatastoreType
    observed_types: frozenset[str]
    nullable: bool
    mixed: bool = False


@dataclass(frozen=True)
class SchemaInferenceReport:
    schema: Schema
    fields: Mapping[str, InferredField]
    sampled_entities: int
    mixed_fields: tuple[str, ...]


def infer_schema(
    *,
    kind: str,
    client: Any,
    namespace: str | None = None,
    filters: Sequence[tuple[str, str, Any]] = (),
    ancestor: DatastoreKey | None = None,
    sample_size: int = 1000,
    key: KeySpec | None = None,
    mixed_type_policy: MixedTypePolicy = "object",
) -> SchemaInferenceReport:
    """Infer a schema from sampled Datastore entities."""

    spec = QuerySpec(
        kind=kind, namespace=namespace, filters=filters, ancestor=ancestor, limit=sample_size
    )
    query = spec.build(client)
    entities = list(query.fetch(limit=sample_size))
    return infer_schema_from_records(
        [dict(entity) for entity in entities],
        kind=kind,
        key=key,
        mixed_type_policy=mixed_type_policy,
    )


def infer_schema_from_frame(
    df: Any,
    *,
    kind: str,
    key: KeySpec | None = None,
    mixed_type_policy: MixedTypePolicy = "object",
) -> SchemaInferenceReport:
    """Infer a schema from a pandas or Polars DataFrame."""

    return infer_schema_from_records(
        _records_from_frame(df),
        kind=kind,
        key=key,
        mixed_type_policy=mixed_type_policy,
    )


def infer_schema_from_records(
    records: Sequence[Mapping[str, Any]],
    *,
    kind: str,
    key: KeySpec | None = None,
    mixed_type_policy: MixedTypePolicy = "object",
) -> SchemaInferenceReport:
    sampled = len(records)
    names = sorted({name for record in records for name in record if name != "__key__"})
    fields: dict[str, InferredField] = {}
    properties: dict[str, Field] = {}
    for name in names:
        observed = [_type_name(record.get(name)) for record in records if name in record]
        non_null_types = frozenset(type_name for type_name in observed if type_name != "null")
        nullable = len(observed) < sampled or "null" in observed
        dtype = _dtype_for_observed(name, non_null_types, mixed_type_policy=mixed_type_policy)
        mixed = len(non_null_types) > 1
        fields[name] = InferredField(
            name=name,
            dtype=dtype,
            observed_types=frozenset(observed),
            nullable=nullable,
            mixed=mixed,
        )
        properties[name] = Field(dtype, nullable=nullable)
    schema = Schema(kind=kind, key=key, properties=properties, strict=False)
    mixed_fields = tuple(name for name, field in fields.items() if field.mixed)
    return SchemaInferenceReport(
        schema=schema,
        fields=fields,
        sampled_entities=sampled,
        mixed_fields=mixed_fields,
    )


def _records_from_frame(df: Any) -> list[dict[str, Any]]:
    if hasattr(df, "iter_rows"):
        return [dict(row) for row in df.iter_rows(named=True)]
    return [dict(row) for _, row in df.iterrows()]


def _dtype_for_observed(
    name: str,
    observed: frozenset[str],
    *,
    mixed_type_policy: MixedTypePolicy,
) -> DatastoreType:
    if not observed:
        return DatastoreType()
    if len(observed) == 1:
        return _dtype_for_type(next(iter(observed)))
    if mixed_type_policy == "object":
        return DatastoreType()
    if mixed_type_policy == "string":
        return StringType()
    if mixed_type_policy == "error":
        raise SchemaError(
            f"Field {name!r} has mixed Datastore types: {', '.join(sorted(observed))}."
        )
    raise SchemaError(f"Unsupported mixed type policy: {mixed_type_policy!r}.")


def _dtype_for_type(type_name: str) -> DatastoreType:
    return {
        "array": ArrayType(DatastoreType()),
        "blob": BlobType(),
        "bool": BoolType(),
        "embedded_entity": EmbeddedEntityType(),
        "float64": Float64Type(),
        "geo_point": GeoPointType(),
        "int64": Int64Type(),
        "key": KeyType(),
        "string": StringType(),
        "timestamp": TimestampType(),
    }.get(type_name, DatastoreType())


def _type_name(value: Any) -> str:
    if is_missing(value):
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int64"
    if isinstance(value, float):
        return "float64"
    if isinstance(value, str):
        return "string"
    if isinstance(value, bytes):
        return "blob"
    if isinstance(value, datetime):
        return "timestamp"
    if isinstance(value, DatastoreKey) or hasattr(value, "flat_path"):
        return "key"
    if isinstance(value, GeoPoint) or (hasattr(value, "latitude") and hasattr(value, "longitude")):
        return "geo_point"
    if isinstance(value, list):
        return "array"
    if isinstance(value, Mapping):
        return "embedded_entity"
    return value.__class__.__name__

"""Conversion between schema rows and google-cloud-datastore entities."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from datastore_pandas.keys import DatastoreKey
from datastore_pandas.schema import Schema


def row_to_entity(
    row: Mapping[str, Any],
    *,
    schema: Schema,
    client: Any,
    properties: list[str] | None = None,
):
    from google.cloud import datastore

    key = schema.key_for_row(row)
    encoded, exclude_from_indexes = schema.encode_properties(row, properties=properties)
    encoded = {name: to_client_datastore_value(value, client) for name, value in encoded.items()}
    entity = datastore.Entity(
        key=key.to_client_key(client),
        exclude_from_indexes=exclude_from_indexes,
    )
    entity.update(encoded)
    return entity


def to_client_datastore_value(value: Any, client: Any) -> Any:
    """Convert package-native key values into google-cloud-datastore values."""

    if isinstance(value, DatastoreKey):
        return value.to_client_key(client)
    if isinstance(value, (list, tuple)):
        return [to_client_datastore_value(item, client) for item in value]
    if isinstance(value, Mapping):
        return {name: to_client_datastore_value(item, client) for name, item in value.items()}
    return value


def entity_to_record(
    entity: Mapping[str, Any],
    *,
    schema: Schema | None,
    include_key: bool = False,
) -> dict[str, Any]:
    key = None
    if include_key and hasattr(entity, "key"):
        key = DatastoreKey.from_client_key(entity.key)
    if schema is None:
        record = dict(entity)
        if include_key:
            record["__key__"] = key
        return record
    return schema.decode_entity(entity, key=key, include_key=include_key)

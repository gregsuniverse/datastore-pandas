"""Batch planning for Datastore writes and lookups."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
import json
from typing import Any

from datastore_pandas.errors import KeyValidationError
from datastore_pandas.keys import DatastoreKey

DEFAULT_MAX_MUTATIONS = 400
DEFAULT_MAX_BYTES = 8 * 1024 * 1024


def chunk_items(
    items: Iterable[Any],
    *,
    max_items: int = DEFAULT_MAX_MUTATIONS,
    max_bytes: int = DEFAULT_MAX_BYTES,
) -> Iterator[list[Any]]:
    chunk: list[Any] = []
    size = 0
    for item in items:
        item_size = approximate_size(item)
        if chunk and (len(chunk) >= max_items or size + item_size > max_bytes):
            yield chunk
            chunk = []
            size = 0
        chunk.append(item)
        size += item_size
    if chunk:
        yield chunk


def validate_unique_complete_keys(keys: Iterable[DatastoreKey]) -> None:
    seen: set[str] = set()
    for key in keys:
        if not key.is_complete:
            continue
        serialized = key.to_json()
        if serialized in seen:
            raise KeyValidationError(f"Duplicate Datastore key in one commit: {serialized}.")
        seen.add(serialized)


def approximate_size(value: Any) -> int:
    try:
        return len(json.dumps(value, default=str).encode("utf-8"))
    except TypeError:
        return len(repr(value).encode("utf-8"))

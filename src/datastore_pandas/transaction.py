"""Small transaction helper for schema-aware Datastore workflows."""

from __future__ import annotations

from typing import Any, Mapping

from datastore_pandas.convert import entity_to_record, row_to_entity
from datastore_pandas.keys import DatastoreKey
from datastore_pandas.schema import Schema


class Transaction:
    """Context manager wrapping `google-cloud-datastore` transactions.

    Keep transactions small. Use this for read-modify-write workflows that need
    Datastore ACID semantics, not for bulk DataFrame ingestion.
    """

    def __init__(self, client: Any | None = None):
        self.client = client or self._default_client()
        self._transaction = None

    def __enter__(self) -> "Transaction":
        self._transaction = self.client.transaction()
        self._transaction.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        if self._transaction is None:
            return None
        return self._transaction.__exit__(exc_type, exc, tb)

    def get(
        self,
        key: DatastoreKey,
        *,
        schema: Schema | None = None,
        include_key: bool = True,
    ) -> dict[str, Any] | None:
        entity = self.client.get(key.to_client_key(self.client))
        if entity is None:
            return None
        return entity_to_record(entity, schema=schema, include_key=include_key)

    def put(
        self,
        row: Mapping[str, Any],
        *,
        schema: Schema,
        properties: list[str] | None = None,
    ) -> None:
        entity = row_to_entity(row, schema=schema, client=self.client, properties=properties)
        self.client.put(entity)

    @staticmethod
    def _default_client() -> Any:
        from google.cloud import datastore

        return datastore.Client()


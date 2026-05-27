"""Small transaction helper for schema-aware Datastore workflows."""

from __future__ import annotations

from time import perf_counter, sleep
from typing import Any, Callable, Mapping, TypeVar

from datastore_pandas.convert import entity_to_record, row_to_entity
from datastore_pandas.io import (
    DEFAULT_COMMIT_RETRY,
    CommitRetryContext,
    _bounded_retry_delay,
    _coerce_commit_retry_policy,
    _is_retryable_commit_error,
    _retry_attempts_exhausted,
    _retry_deadline_exhausted,
)
from datastore_pandas.keys import DatastoreKey
from datastore_pandas.schema import Schema

T = TypeVar("T")


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
        client_key = key.to_client_key(self.client)
        active = self._active_context()
        if hasattr(active, "get"):
            entity = active.get(client_key)
        else:
            entity = self.client.get(client_key, transaction=self._transaction)
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
        self._active_context().put(entity)

    def delete(self, key: DatastoreKey) -> None:
        self._active_context().delete(key.to_client_key(self.client))

    def _active_context(self) -> Any:
        return self._transaction if self._transaction is not None else self.client

    @staticmethod
    def _default_client() -> Any:
        from google.cloud import datastore

        return datastore.Client()


def run_transaction(
    callback: Callable[[Transaction], T],
    *,
    client: Any | None = None,
    retry: Any = DEFAULT_COMMIT_RETRY,
) -> T:
    """Run a small Datastore transaction callback with retry handling."""

    active_client = client or Transaction._default_client()
    retry_policy = _coerce_commit_retry_policy(retry)
    attempt = 1
    delay = retry_policy.initial
    deadline_at = perf_counter() + retry_policy.deadline if retry_policy.deadline else None
    while True:
        try:
            with Transaction(active_client) as transaction:
                return callback(transaction)
        except Exception as exc:
            retryable = _is_retryable_commit_error(exc, retry_policy)
            if (
                not retryable
                or _retry_attempts_exhausted(attempt, retry_policy)
                or _retry_deadline_exhausted(deadline_at)
            ):
                raise
            sleep_for = _bounded_retry_delay(delay, retry_policy, deadline_at)
            if retry_policy.on_retry is not None:
                retry_policy.on_retry(
                    CommitRetryContext(
                        attempt=attempt,
                        delay=sleep_for,
                        exception=exc,
                        retryable=retryable,
                    )
                )
            sleep(sleep_for)
            attempt += 1
            delay = min(retry_policy.max_delay, delay * retry_policy.multiplier)

from __future__ import annotations

from time import sleep

import pytest

from datastore_pandas import io
from datastore_pandas.keys import DatastoreKey
from datastore_pandas.throttle import (
    AdaptiveWriteLimiter,
    DynamicBatchPolicy,
    DynamicBatchSizer,
    WriteThrottlePolicy,
)
from datastore_pandas.transaction import Transaction, run_transaction
from datastore_pandas.reports import WriteReport, WriteResult


def _disabled_limiter() -> AdaptiveWriteLimiter:
    return AdaptiveWriteLimiter(WriteThrottlePolicy(enabled=False))


def test_commit_retry_retries_retryable_upsert_commit(monkeypatch):
    sleeps: list[float] = []
    attempts = 0

    def commit_once():
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("503 unavailable")
        return type("Response", (), {"index_updates": 3})()

    monkeypatch.setattr(io, "sleep", sleeps.append)

    stats = io._execute_commit_with_retry(
        commit_once,
        operation_count=10,
        retry=io.CommitRetryPolicy(initial=1, max_attempts=3, jitter=0),
        retry_safe=True,
        write_limiter=_disabled_limiter(),
    )

    assert attempts == 2
    assert sleeps == [1]
    assert stats.attempts == 2
    assert stats.successes == 1
    assert stats.failures == 1
    assert stats.retry_attempts == 1
    assert stats.retryable_failures == 1
    assert stats.index_updates == 3
    assert stats.batch_size == 10


def test_commit_retry_does_not_retry_non_retry_safe_operation(monkeypatch):
    attempts = 0

    def commit_once():
        nonlocal attempts
        attempts += 1
        raise RuntimeError("aborted by contention")

    monkeypatch.setattr(io, "sleep", lambda _seconds: None)

    with pytest.raises(io._CommitFailure) as raised:
        io._execute_commit_with_retry(
            commit_once,
            operation_count=1,
            retry=io.CommitRetryPolicy(initial=1, max_attempts=3, jitter=0),
            retry_safe=False,
            write_limiter=_disabled_limiter(),
        )

    assert attempts == 1
    assert raised.value.stats.failures == 1
    assert raised.value.stats.retry_attempts == 0


def test_commit_retry_accepts_typed_retryable_exceptions(monkeypatch):
    class CustomTransient(Exception):
        pass

    contexts: list[io.CommitRetryContext] = []
    attempts = 0

    def commit_once():
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise CustomTransient("temporary write pressure")

    monkeypatch.setattr(io, "sleep", lambda _seconds: None)

    stats = io._execute_commit_with_retry(
        commit_once,
        operation_count=2,
        retry=io.CommitRetryPolicy(
            initial=2,
            max_attempts=2,
            jitter=0,
            retryable_exceptions=(CustomTransient,),
            on_retry=contexts.append,
        ),
        retry_safe=True,
        write_limiter=_disabled_limiter(),
    )

    assert attempts == 2
    assert stats.retry_attempts == 1
    assert len(contexts) == 1
    assert contexts[0].attempt == 1
    assert contexts[0].delay == 2
    assert isinstance(contexts[0].exception, CustomTransient)


def test_adaptive_write_limiter_enforces_process_local_rate():
    now = 0.0
    sleeps: list[float] = []

    def clock() -> float:
        return now

    def sleeper(seconds: float) -> None:
        nonlocal now
        sleeps.append(seconds)
        now += seconds

    limiter = AdaptiveWriteLimiter(
        WriteThrottlePolicy(
            enabled=True,
            initial_ops_per_second=10,
            ramp_up_interval=0,
        ),
        clock=clock,
        sleeper=sleeper,
    )

    first = limiter.before_commit(10)
    second = limiter.before_commit(5)

    assert first.slept_seconds == 0
    assert second.slept_seconds == 1
    assert sleeps == [1]


def test_dynamic_batch_sizer_grows_and_shrinks_from_commit_feedback():
    sizer = DynamicBatchSizer(
        100,
        DynamicBatchPolicy(
            enabled=True,
            min_batch_size=10,
            target_latency_ms=100,
            grow_multiplier=2,
            shrink_multiplier=0.5,
        ),
    )

    assert sizer.next_batch_size() == 10
    sizer.report(latency_ms=50, failed=False)
    assert sizer.next_batch_size() == 20
    sizer.report(latency_ms=50, failed=True)
    assert sizer.next_batch_size() == 10


def test_native_patch_backend_reports_unavailable_when_forced():
    rows = [((7, {"value": 1}), DatastoreKey(path=(("Doc", "a"),)))]

    report = io._patch_chunk(
        rows,
        schema=object(),
        client=object(),
        properties=["value"],
        retry=None,
        write_limiter=_disabled_limiter(),
        patch_backend="native",
    )

    assert report.failed == 1
    assert report.results[0].row_index == 7
    assert report.results[0].action == "patch"
    assert "unavailable" in report.results[0].error


def test_concurrent_write_reports_keep_input_order(monkeypatch):
    def fake_commit_chunk(chunk, **_kwargs):
        rows = list(chunk)
        row_index = rows[0][0][0]
        if row_index == 0:
            sleep(0.05)
        return WriteReport(results=[WriteResult(row_index=row_index, action="upsert")])

    monkeypatch.setattr(io, "_commit_chunk", fake_commit_chunk)
    items = [
        ((row_index, {}), DatastoreKey(path=(("Doc", str(row_index)),)))
        for row_index in range(4)
    ]

    report = io._write_items(
        items,
        schema=object(),
        client=object(),
        mode="upsert",
        properties=None,
        batch_size=1,
        max_workers=4,
        retry=None,
        throttle=False,
        adaptive_batching=False,
    )

    assert [result.row_index for result in report.results] == [0, 1, 2, 3]


def test_run_transaction_retries_callback(monkeypatch):
    class FakeDatastoreTransaction:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

    class FakeClient:
        def transaction(self):
            return FakeDatastoreTransaction()

    attempts = 0

    def callback(tx: Transaction) -> str:
        nonlocal attempts
        attempts += 1
        assert isinstance(tx, Transaction)
        if attempts == 1:
            raise RuntimeError("aborted by contention")
        return "ok"

    monkeypatch.setattr("datastore_pandas.transaction.sleep", lambda _seconds: None)

    result = run_transaction(
        callback,
        client=FakeClient(),
        retry=io.CommitRetryPolicy(initial=1, max_attempts=2, jitter=0),
    )

    assert result == "ok"
    assert attempts == 2

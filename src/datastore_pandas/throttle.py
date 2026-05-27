"""Write throttling and adaptive batch sizing helpers."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from threading import Lock
from time import monotonic, sleep
from typing import Callable


@dataclass(frozen=True)
class WriteThrottlePolicy:
    enabled: bool = True
    initial_ops_per_second: float = 500.0
    ramp_up_multiplier: float = 1.5
    ramp_up_interval: float = 300.0
    max_ops_per_second: float | None = None
    failure_backoff_multiplier: float = 2.0
    max_failure_backoff: float = 16.0


@dataclass(frozen=True)
class DynamicBatchPolicy:
    enabled: bool = True
    min_batch_size: int = 25
    target_latency_ms: int = 6000
    grow_multiplier: float = 1.25
    shrink_multiplier: float = 0.5


@dataclass(frozen=True)
class ThrottleResult:
    slept_seconds: float = 0.0


class AdaptiveWriteLimiter:
    """Process-local limiter for Datastore write pressure."""

    def __init__(
        self,
        policy: WriteThrottlePolicy | None = None,
        *,
        clock: Callable[[], float] = monotonic,
        sleeper: Callable[[float], None] = sleep,
    ) -> None:
        self.policy = policy or WriteThrottlePolicy(enabled=False)
        self._clock = clock
        self._sleeper = sleeper
        self._lock = Lock()
        self._started_at = self._clock()
        self._next_allowed_at = self._started_at
        self._failure_backoff = 1.0

    def before_commit(self, operation_count: int) -> ThrottleResult:
        if not self.policy.enabled or operation_count <= 0:
            return ThrottleResult()
        with self._lock:
            now = self._clock()
            rate = self._effective_rate(now)
            interval = operation_count / rate if rate > 0 else 0.0
            sleep_for = max(0.0, self._next_allowed_at - now)
            if sleep_for:
                self._sleeper(sleep_for)
                now = self._clock()
            self._next_allowed_at = max(now, self._next_allowed_at) + interval
            return ThrottleResult(slept_seconds=sleep_for)

    def record_success(self) -> None:
        if not self.policy.enabled:
            return
        with self._lock:
            self._failure_backoff = max(
                1.0,
                self._failure_backoff / self.policy.failure_backoff_multiplier,
            )

    def record_failure(self) -> None:
        if not self.policy.enabled:
            return
        with self._lock:
            self._failure_backoff = min(
                self.policy.max_failure_backoff,
                max(1.0, self._failure_backoff) * self.policy.failure_backoff_multiplier,
            )

    def _effective_rate(self, now: float) -> float:
        policy = self.policy
        elapsed = max(0.0, now - self._started_at)
        if policy.ramp_up_interval > 0 and policy.ramp_up_multiplier > 1:
            intervals = elapsed / policy.ramp_up_interval
            rate = policy.initial_ops_per_second * (policy.ramp_up_multiplier**intervals)
        else:
            rate = policy.initial_ops_per_second
        if policy.max_ops_per_second is not None:
            rate = min(rate, policy.max_ops_per_second)
        return max(1.0, rate / self._failure_backoff)


class DynamicBatchSizer:
    """Adjusts batch size from recent commit latency and failures."""

    def __init__(self, max_batch_size: int, policy: DynamicBatchPolicy | None = None) -> None:
        self.max_batch_size = max(1, max_batch_size)
        self.policy = policy or DynamicBatchPolicy(enabled=False)
        self._current = max(1, min(self.max_batch_size, self.policy.min_batch_size))
        self._latencies: deque[int] = deque(maxlen=20)

    def next_batch_size(self) -> int:
        if not self.policy.enabled:
            return self.max_batch_size
        return max(1, min(self.max_batch_size, self._current))

    def report(self, *, latency_ms: int | None, failed: bool) -> None:
        if not self.policy.enabled:
            return
        if failed:
            self._current = max(1, int(self._current * self.policy.shrink_multiplier))
            return
        if latency_ms is None:
            return
        self._latencies.append(latency_ms)
        recent = sum(self._latencies) / len(self._latencies)
        if recent <= self.policy.target_latency_ms:
            self._current = min(
                self.max_batch_size,
                max(self._current + 1, int(self._current * self.policy.grow_multiplier)),
            )
        else:
            self._current = max(1, int(self._current * self.policy.shrink_multiplier))

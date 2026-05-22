"""Large linked-kind emulator load using Datastore keys as references.

This script is intentionally synthetic so it can scale into hundreds of
thousands or millions of rows without relying on a network dataset. It writes
several related kinds:

    Tenant(<tenant>)
      LinkedUser(<user_id>)
        LinkedSession(<session_id>)
          LinkedEvent(<event_id>)

    Tenant(<tenant>)
      LinkedDevice(<device_id>)

`LinkedDevice`, `LinkedSession`, and `LinkedEvent` also store `KeyType`
properties that point across kinds. These are key references, not joins:
Datastore can filter by indexed key properties, but relationship traversal is
still application-managed.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from time import perf_counter
from typing import Any

from common import (
    BACKENDS,
    adapter,
    client,
    frame_from_records,
    frame_is_empty,
    frame_len,
    iter_records,
    print_frame,
)
import datastore_pandas as dsp


NAMESPACE = "linked-large"
START = datetime(2026, 1, 1, tzinfo=timezone.utc)

COUNTRIES = ("US", "CA", "GB", "DE", "JP", "BR", "AU")
PLANS = ("free", "team", "enterprise")
PLATFORMS = ("ios", "android", "web", "edge")
EVENT_TYPES = ("view", "click", "heartbeat", "purchase", "error")


USER_SCHEMA = dsp.Schema(
    kind="LinkedUser",
    key=dsp.KeySpec(
        [
            ("Tenant", dsp.KeyPart("tenant")),
            ("LinkedUser", dsp.KeyPart("user_id")),
        ],
        namespace=NAMESPACE,
    ),
    properties={
        "user_id": dsp.Field(dsp.StringType(), nullable=False),
        "country": dsp.Field(dsp.StringType(), nullable=False),
        "plan": dsp.Field(dsp.StringType(), nullable=False),
        "signup_at": dsp.Field(dsp.TimestampType(), nullable=False),
    },
    strict=True,
)

DEVICE_SCHEMA = dsp.Schema(
    kind="LinkedDevice",
    key=dsp.KeySpec(
        [
            ("Tenant", dsp.KeyPart("tenant")),
            ("LinkedDevice", dsp.KeyPart("device_id")),
        ],
        namespace=NAMESPACE,
    ),
    properties={
        "device_id": dsp.Field(dsp.StringType(), nullable=False),
        "assigned_user_key": dsp.Field(dsp.KeyType(), nullable=False),
        "platform": dsp.Field(dsp.StringType(), nullable=False),
        "activated_at": dsp.Field(dsp.TimestampType(), nullable=False, indexed=False),
    },
    strict=True,
)

SESSION_SCHEMA = dsp.Schema(
    kind="LinkedSession",
    key=dsp.KeySpec(
        [
            ("Tenant", dsp.KeyPart("tenant")),
            ("LinkedUser", dsp.KeyPart("user_id")),
            ("LinkedSession", dsp.KeyPart("session_id")),
        ],
        namespace=NAMESPACE,
    ),
    properties={
        "session_id": dsp.Field(dsp.StringType(), nullable=False),
        "user_id": dsp.Field(dsp.StringType(), nullable=False),
        "device_id": dsp.Field(dsp.StringType(), nullable=False),
        "user_key": dsp.Field(dsp.KeyType(), nullable=False),
        "device_key": dsp.Field(dsp.KeyType(), nullable=False),
        "started_at": dsp.Field(dsp.TimestampType(), nullable=False, indexed=False),
        "event_count": dsp.Field(dsp.Int64Type(), nullable=False, indexed=False),
    },
    strict=True,
)

EVENT_SCHEMA = dsp.Schema(
    kind="LinkedEvent",
    key=dsp.KeySpec(
        [
            ("Tenant", dsp.KeyPart("tenant")),
            ("LinkedUser", dsp.KeyPart("user_id")),
            ("LinkedSession", dsp.KeyPart("session_id")),
            ("LinkedEvent", dsp.KeyPart("event_id")),
        ],
        namespace=NAMESPACE,
    ),
    properties={
        "event_id": dsp.Field(dsp.StringType(), nullable=False),
        "session_id": dsp.Field(dsp.StringType(), nullable=False),
        "user_id": dsp.Field(dsp.StringType(), nullable=False),
        "device_id": dsp.Field(dsp.StringType(), nullable=False),
        "user_key": dsp.Field(dsp.KeyType(), nullable=False, indexed=False),
        "session_key": dsp.Field(dsp.KeyType(), nullable=False),
        "device_key": dsp.Field(dsp.KeyType(), nullable=False),
        "occurred_at": dsp.Field(dsp.TimestampType(), nullable=False, indexed=False),
        "event_type": dsp.Field(dsp.StringType(), nullable=False),
        "event_value": dsp.Field(dsp.Float64Type(), nullable=False, indexed=False),
        "error_code": dsp.Field(dsp.StringType(), indexed=False),
        "campaign": dsp.Field(dsp.StringType(), indexed=False),
        "tags": dsp.Field(dsp.ArrayType(dsp.StringType()), indexed=False),
        "attributes": dsp.Field(dsp.EmbeddedEntityType(), indexed=False),
    },
    strict=True,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--backend", choices=BACKENDS, default="pandas")
    parser.add_argument("--tenant", default="tenant-linked")
    parser.add_argument("--users", type=int, default=1_000)
    parser.add_argument("--devices", type=int, default=1_000)
    parser.add_argument("--sessions", type=int, default=10_000)
    parser.add_argument("--events", type=int, default=200_000)
    parser.add_argument("--chunk-size", type=int, default=25_000)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--validate-limit", type=int, default=25)
    args = parser.parse_args()

    _validate_args(args)
    run(args)


def run(args: argparse.Namespace) -> None:
    if args.events <= 1_000_000:
        print("note: use --events 1000001 or more for a strict >1M event load")

    ds = client()
    dsp_io = adapter(args.backend)
    total_entities = args.users + args.devices + args.sessions + args.events
    print(
        "loading linked dataset "
        f"backend={args.backend} total_entities={total_entities:,} "
        f"events={args.events:,}"
    )

    started = perf_counter()
    _load_generated(
        "LinkedUser",
        args.users,
        lambda index: _user_record(index, args),
        USER_SCHEMA,
        dsp_io,
        ds,
        args,
    )
    _load_generated(
        "LinkedDevice",
        args.devices,
        lambda index: _device_record(index, args),
        DEVICE_SCHEMA,
        dsp_io,
        ds,
        args,
    )
    _load_generated(
        "LinkedSession",
        args.sessions,
        lambda index: _session_record(index, args),
        SESSION_SCHEMA,
        dsp_io,
        ds,
        args,
    )
    _load_generated(
        "LinkedEvent",
        args.events,
        lambda index: _event_record(index, args),
        EVENT_SCHEMA,
        dsp_io,
        ds,
        args,
    )

    elapsed = perf_counter() - started
    print(f"loaded linked dataset total_entities={total_entities:,} elapsed={elapsed:.2f}s")
    _validate_links(ds, dsp_io, args)


def _load_generated(
    label: str,
    total: int,
    factory: Callable[[int], dict[str, Any]],
    schema: dsp.Schema,
    dsp_io: Any,
    ds: Any,
    args: argparse.Namespace,
) -> None:
    started = perf_counter()
    loaded = 0
    for start in range(0, total, args.chunk_size):
        end = min(start + args.chunk_size, total)
        records = [factory(index) for index in range(start, end)]
        df = frame_from_records(records, args.backend)
        report = dsp_io.to_datastore(
            df,
            schema=schema,
            client=ds,
            mode="upsert",
            batch_size=args.batch_size,
            max_workers=args.workers,
        )
        report.raise_for_errors()
        loaded += report.succeeded
        print(f"{label}: loaded={loaded:,}/{total:,} chunk_rows={frame_len(df):,}")
    elapsed = perf_counter() - started
    print(f"{label}: complete rows={loaded:,} elapsed={elapsed:.2f}s")


def _validate_links(ds: Any, dsp_io: Any, args: argparse.Namespace) -> None:
    session_index = _probe_session_index(args)
    user_index = _session_user_index(session_index, args.users)
    device_index = _session_device_index(session_index, args.devices)
    user_id = _user_id(user_index)
    device_id = _device_id(device_index)
    session_id = _session_id(session_index)

    user_key = _user_key(args.tenant, user_id)
    assigned_user_key = _user_key(args.tenant, _user_id(device_index % args.users))
    device_key = _device_key(args.tenant, device_id)
    session_key = _session_key(args.tenant, user_id, session_id)
    expected_events = _event_count_for_session(session_index, args.events, args.sessions)

    session_events = dsp_io.read_datastore(
        kind="LinkedEvent",
        schema=EVENT_SCHEMA,
        client=ds,
        ancestor=session_key,
        limit=args.validate_limit,
        include_key=True,
    )
    print_frame("LinkedEvent descendants under one LinkedSession", session_events)

    events_by_key_property = dsp_io.read_datastore(
        kind="LinkedEvent",
        schema=EVENT_SCHEMA,
        client=ds,
        namespace=NAMESPACE,
        filters=[("session_key", "=", session_key)],
        limit=args.validate_limit,
        include_key=True,
    )
    print_frame("LinkedEvent rows filtered by session_key property", events_by_key_property)

    sessions_for_device = dsp_io.read_datastore(
        kind="LinkedSession",
        schema=SESSION_SCHEMA,
        client=ds,
        namespace=NAMESPACE,
        filters=[("device_key", "=", device_key)],
        limit=args.validate_limit,
        include_key=True,
    )
    print_frame("LinkedSession rows filtered by device_key property", sessions_for_device)

    users_for_device = dsp_io.read_datastore(
        kind="LinkedDevice",
        schema=DEVICE_SCHEMA,
        client=ds,
        namespace=NAMESPACE,
        filters=[("assigned_user_key", "=", assigned_user_key)],
        limit=args.validate_limit,
        include_key=True,
    )
    print_frame("LinkedDevice rows filtered by assigned_user_key property", users_for_device)

    assert expected_events > 0
    assert not frame_is_empty(session_events)
    assert not frame_is_empty(events_by_key_property)
    assert not frame_is_empty(sessions_for_device)
    assert not frame_is_empty(users_for_device)
    assert frame_len(session_events) == min(expected_events, args.validate_limit)
    assert frame_len(events_by_key_property) == min(expected_events, args.validate_limit)

    for row in iter_records(session_events):
        assert row["__key__"].path[:3] == session_key.path
        assert _same_logical_key(row["user_key"], user_key)
        assert _same_logical_key(row["session_key"], session_key)
        assert _same_logical_key(row["device_key"], device_key)

    print(
        "linked key checks passed: "
        f"session={session_id} expected_events={expected_events:,} "
        f"returned={frame_len(session_events):,}"
    )


def _user_record(index: int, args: argparse.Namespace) -> dict[str, Any]:
    user_id = _user_id(index)
    return {
        "tenant": args.tenant,
        "user_id": user_id,
        "country": COUNTRIES[index % len(COUNTRIES)],
        "plan": PLANS[index % len(PLANS)],
        "signup_at": START - timedelta(days=index % 365),
    }


def _device_record(index: int, args: argparse.Namespace) -> dict[str, Any]:
    assigned_user_id = _user_id(index % args.users)
    return {
        "tenant": args.tenant,
        "device_id": _device_id(index),
        "assigned_user_key": _user_key(args.tenant, assigned_user_id),
        "platform": PLATFORMS[index % len(PLATFORMS)],
        "activated_at": START - timedelta(hours=index % 10_000),
    }


def _session_record(index: int, args: argparse.Namespace) -> dict[str, Any]:
    user_index = _session_user_index(index, args.users)
    device_index = _session_device_index(index, args.devices)
    user_id = _user_id(user_index)
    device_id = _device_id(device_index)
    return {
        "tenant": args.tenant,
        "session_id": _session_id(index),
        "user_id": user_id,
        "device_id": device_id,
        "user_key": _user_key(args.tenant, user_id),
        "device_key": _device_key(args.tenant, device_id),
        "started_at": START + timedelta(minutes=index),
        "event_count": _event_count_for_session(index, args.events, args.sessions),
    }


def _event_record(index: int, args: argparse.Namespace) -> dict[str, Any]:
    session_index = index % args.sessions
    user_index = _session_user_index(session_index, args.users)
    device_index = _session_device_index(session_index, args.devices)
    user_id = _user_id(user_index)
    session_id = _session_id(session_index)
    device_id = _device_id(device_index)
    event_type = EVENT_TYPES[index % len(EVENT_TYPES)]
    return {
        "tenant": args.tenant,
        "event_id": _event_id(index),
        "session_id": session_id,
        "user_id": user_id,
        "device_id": device_id,
        "user_key": _user_key(args.tenant, user_id),
        "session_key": _session_key(args.tenant, user_id, session_id),
        "device_key": _device_key(args.tenant, device_id),
        "occurred_at": START + timedelta(seconds=index),
        "event_type": event_type,
        "event_value": float((index * 17) % 10_000) / 100.0,
        "error_code": f"E{index % 97:03d}" if event_type == "error" else None,
        "campaign": f"campaign-{index % 11}" if index % 13 == 0 else None,
        "tags": ["sampled", event_type] if index % 100 == 0 else None,
        "attributes": {"bucket": index % 1024, "synthetic": True} if index % 250 == 0 else None,
    }


def _user_key(tenant: str, user_id: str) -> dsp.DatastoreKey:
    return dsp.DatastoreKey(
        namespace=NAMESPACE,
        path=(("Tenant", tenant), ("LinkedUser", user_id)),
    )


def _device_key(tenant: str, device_id: str) -> dsp.DatastoreKey:
    return dsp.DatastoreKey(
        namespace=NAMESPACE,
        path=(("Tenant", tenant), ("LinkedDevice", device_id)),
    )


def _session_key(tenant: str, user_id: str, session_id: str) -> dsp.DatastoreKey:
    return dsp.DatastoreKey(
        namespace=NAMESPACE,
        path=(
            ("Tenant", tenant),
            ("LinkedUser", user_id),
            ("LinkedSession", session_id),
        ),
    )


def _event_count_for_session(session_index: int, events: int, sessions: int) -> int:
    base = events // sessions
    return base + (1 if session_index < events % sessions else 0)


def _probe_session_index(args: argparse.Namespace) -> int:
    index = min(args.sessions - 1, max(0, args.sessions // 2))
    if _event_count_for_session(index, args.events, args.sessions) > 0:
        return index
    return 0


def _session_user_index(session_index: int, users: int) -> int:
    return session_index % users


def _session_device_index(session_index: int, devices: int) -> int:
    return session_index % devices


def _user_id(index: int) -> str:
    return f"user-{index:08d}"


def _device_id(index: int) -> str:
    return f"device-{index:08d}"


def _session_id(index: int) -> str:
    return f"session-{index:08d}"


def _event_id(index: int) -> str:
    return f"event-{index:010d}"


def _same_logical_key(actual: Any, expected: dsp.DatastoreKey) -> bool:
    if not isinstance(actual, dsp.DatastoreKey):
        actual = dsp.DatastoreKey.from_client_key(actual)
    return actual.path == expected.path and actual.namespace == expected.namespace


def _validate_args(args: argparse.Namespace) -> None:
    for name in ["users", "devices", "sessions", "events", "chunk_size", "workers", "batch_size"]:
        if getattr(args, name) <= 0:
            raise ValueError(f"--{name.replace('_', '-')} must be positive")


if __name__ == "__main__":
    main()

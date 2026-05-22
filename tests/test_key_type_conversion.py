from __future__ import annotations

from dataclasses import dataclass

from datastore_pandas import DatastoreKey, QuerySpec
from datastore_pandas.convert import to_client_datastore_value


@dataclass(frozen=True)
class FakeClientKey:
    flat_path: tuple
    namespace: str | None


class FakeClient:
    def __init__(self) -> None:
        self.query_kwargs = None

    def key(self, *flat_path, namespace=None):
        return FakeClientKey(flat_path=flat_path, namespace=namespace)

    def query(self, **kwargs):
        self.query_kwargs = kwargs
        return FakeQuery()


class FakeQuery:
    def __init__(self) -> None:
        self.filters = []
        self.keys_only_called = False

    def add_filter(self, name, op, value):
        self.filters.append((name, op, value))

    def keys_only(self):
        self.keys_only_called = True


def test_datastore_key_properties_convert_to_client_keys_recursively():
    client = FakeClient()
    key = DatastoreKey(namespace="tenant-a", path=(("User", "u1"),))

    converted = to_client_datastore_value({"owner": key, "refs": [key]}, client)

    expected = FakeClientKey(flat_path=("User", "u1"), namespace="tenant-a")
    assert converted == {"owner": expected, "refs": [expected]}


def test_query_filters_convert_datastore_key_values_to_client_keys():
    client = FakeClient()
    key = DatastoreKey(namespace="tenant-a", path=(("User", "u1"),))

    query = QuerySpec(
        kind="Session",
        namespace="tenant-a",
        filters=[("owner_key", "=", key), ("any_key", "IN", [key])],
    ).build(client)

    expected = FakeClientKey(flat_path=("User", "u1"), namespace="tenant-a")
    assert query.filters == [
        ("owner_key", "=", expected),
        ("any_key", "IN", [expected]),
    ]

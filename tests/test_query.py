import pytest

from datastore_pandas import QuerySpec
from datastore_pandas.errors import QueryValidationError


def test_projection_rejects_equality_filtered_property():
    spec = QuerySpec(
        kind="Workout", filters=[("user_id", "=", "sample-user")], projection=["user_id"]
    )

    with pytest.raises(QueryValidationError):
        spec.validate()


def test_distinct_on_must_prefix_order():
    spec = QuerySpec(kind="Workout", distinct_on=["user_id"], order=["-started_at", "user_id"])

    with pytest.raises(QueryValidationError):
        spec.validate()


def test_query_namespace_must_match_ancestor_namespace():
    from datastore_pandas import DatastoreKey

    spec = QuerySpec(
        kind="Workout",
        namespace="tenant-b",
        ancestor=DatastoreKey(namespace="tenant-a", path=(("User", "sample-user"),)),
    )

    with pytest.raises(QueryValidationError):
        spec.validate()


def test_query_fetch_honors_cursor_and_eventual_consistency():
    class FakeQuery:
        def __init__(self) -> None:
            self.fetch_kwargs = None

        def fetch(self, **kwargs):
            self.fetch_kwargs = kwargs
            return []

    query = FakeQuery()
    spec = QuerySpec(
        kind="Workout",
        limit=10,
        cursor=b"cursor-token",
        consistency="eventual",
    )

    rows = list(spec.fetch(query))

    assert rows == []
    assert query.fetch_kwargs == {
        "limit": 10,
        "start_cursor": b"cursor-token",
        "eventual": True,
    }

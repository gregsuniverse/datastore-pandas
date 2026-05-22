import pytest

from datastore_pandas import QuerySpec
from datastore_pandas.errors import QueryValidationError


def test_projection_rejects_equality_filtered_property():
    spec = QuerySpec(kind="Workout", filters=[("user_id", "=", "sample-user")], projection=["user_id"])

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

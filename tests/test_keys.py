import pytest

from datastore_pandas import DatastoreKey, KeyPart, KeySpec
from datastore_pandas.errors import KeyValidationError


def test_key_serialization_preserves_id_and_name_types():
    key = DatastoreKey(
        project="project-a",
        namespace="tenant-a",
        path=(("User", "123"), ("Workout", 123)),
    )

    restored = DatastoreKey.from_json(key.to_json())

    assert restored == key
    assert isinstance(restored.path[0][1], str)
    assert isinstance(restored.path[1][1], int)


def test_keyspec_builds_ancestor_path_from_row():
    spec = KeySpec(
        [
            ("User", KeyPart("user_id", kind="name")),
            ("Workout", KeyPart("workout_id", kind="id")),
        ],
        namespace_source="tenant",
    )

    key = spec.build({"tenant": "t1", "user_id": "sample-user", "workout_id": "42"})

    assert key.namespace == "t1"
    assert key.path == (("User", "sample-user"), ("Workout", 42))


def test_incomplete_key_only_allowed_at_leaf():
    with pytest.raises(KeyValidationError):
        DatastoreKey(path=(("User", None), ("Workout", "w1")))

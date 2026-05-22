import pytest

from datastore_pandas.batches import validate_unique_complete_keys
from datastore_pandas.errors import KeyValidationError
from datastore_pandas.keys import DatastoreKey


def test_duplicate_complete_keys_are_rejected():
    key = DatastoreKey(path=(("Doc", "a"),))

    with pytest.raises(KeyValidationError):
        validate_unique_complete_keys([key, key])


def test_incomplete_keys_can_repeat_before_allocation():
    key = DatastoreKey(path=(("Doc", None),))

    validate_unique_complete_keys([key, key])

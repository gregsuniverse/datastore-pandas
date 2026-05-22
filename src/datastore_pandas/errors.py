"""Package-specific exceptions."""


class DatastorePandasError(Exception):
    """Base exception for datastore-pandas."""


class SchemaError(DatastorePandasError):
    """Raised when a schema cannot safely represent the requested data."""


class DerivedFrameWriteError(DatastorePandasError):
    """Raised when a derived DataFrame cannot safely write to its source kind."""


class KeyValidationError(DatastorePandasError):
    """Raised when a Datastore key is malformed or ambiguous."""


class QueryValidationError(DatastorePandasError):
    """Raised when a query cannot be expressed safely in Datastore mode."""


class WriteConflictError(DatastorePandasError):
    """Raised when a compare-and-swap or transactional write detects a conflict."""

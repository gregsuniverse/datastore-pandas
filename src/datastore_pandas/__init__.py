"""Pandas helpers for Firestore in Datastore mode."""

from datastore_pandas.accessor import DatastoreFrame, kind
from datastore_pandas.audit import AuditPolicy
from datastore_pandas.errors import DatastorePandasError, KeyValidationError, SchemaError
from datastore_pandas.io import (
    iter_datastore,
    patch_datastore,
    plan_datastore_write,
    read_datastore,
    to_datastore,
)
from datastore_pandas.keys import DatastoreKey, KeyPart, KeySpec, key_policy
from datastore_pandas.planning import WritePlan
from datastore_pandas.query import IndexSuggestion, QueryPlan, QuerySpec, plan_indexes
from datastore_pandas.reports import PlannedMutation, WriteAction, WriteReport, WriteResult
from datastore_pandas.schema import Field, MissingPolicy, Schema
from datastore_pandas.transaction import Transaction
from datastore_pandas.types import (
    ArrayType,
    BlobType,
    BoolType,
    DatastoreType,
    EmbeddedEntityType,
    Float64Type,
    GeoPoint,
    GeoPointType,
    Int64Type,
    KeyType,
    StringType,
    TimestampType,
)

__version__ = "0.2.0"

__all__ = [
    "ArrayType",
    "AuditPolicy",
    "BlobType",
    "BoolType",
    "DatastoreKey",
    "DatastoreFrame",
    "DatastorePandasError",
    "DatastoreType",
    "EmbeddedEntityType",
    "Field",
    "Float64Type",
    "GeoPoint",
    "GeoPointType",
    "IndexSuggestion",
    "Int64Type",
    "KeyPart",
    "KeySpec",
    "KeyType",
    "KeyValidationError",
    "MissingPolicy",
    "QuerySpec",
    "QueryPlan",
    "Schema",
    "SchemaError",
    "StringType",
    "TimestampType",
    "__version__",
    "Transaction",
    "PlannedMutation",
    "WriteAction",
    "WritePlan",
    "WriteReport",
    "WriteResult",
    "iter_datastore",
    "kind",
    "key_policy",
    "patch_datastore",
    "plan_datastore_write",
    "plan_indexes",
    "read_datastore",
    "to_datastore",
]

"""Schema-aware conversion between pandas values and Datastore property values."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import math
from typing import Any, Mapping, Sequence

from datastore_pandas.errors import SchemaError
from datastore_pandas.keys import DatastoreKey

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1


@dataclass(frozen=True)
class GeoPoint:
    latitude: float
    longitude: float

    def __post_init__(self) -> None:
        if not -90 <= self.latitude <= 90:
            raise SchemaError("GeoPoint latitude must be between -90 and 90.")
        if not -180 <= self.longitude <= 180:
            raise SchemaError("GeoPoint longitude must be between -180 and 180.")


class DatastoreType:
    """Base converter for one logical Datastore property type."""

    name = "object"

    def to_datastore(self, value: Any) -> Any:
        return value

    def from_datastore(self, value: Any) -> Any:
        return value

    def validate_indexed(self, value: Any) -> None:
        return None


class Int64Type(DatastoreType):
    name = "int64"

    def to_datastore(self, value: Any) -> int:
        if isinstance(value, bool):
            raise SchemaError("Boolean values cannot be written as int64.")
        value = int(value)
        if value < INT64_MIN or value > INT64_MAX:
            raise SchemaError(f"Integer {value!r} is outside Datastore int64 bounds.")
        return value


class Float64Type(DatastoreType):
    name = "float64"

    def to_datastore(self, value: Any) -> float:
        return float(value)


class BoolType(DatastoreType):
    name = "bool"

    def to_datastore(self, value: Any) -> bool:
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"true", "1", "yes"}:
                return True
            if lowered in {"false", "0", "no"}:
                return False
        return bool(value)


class StringType(DatastoreType):
    name = "string"

    def to_datastore(self, value: Any) -> str:
        return str(value)

    def validate_indexed(self, value: Any) -> None:
        if len(str(value).encode("utf-8")) > 1500:
            raise SchemaError("Indexed Datastore strings must be at most 1500 bytes.")


class BlobType(DatastoreType):
    name = "blob"

    def to_datastore(self, value: Any) -> bytes:
        if isinstance(value, memoryview):
            return value.tobytes()
        if isinstance(value, bytearray):
            return bytes(value)
        if not isinstance(value, bytes):
            raise SchemaError("Blob values must be bytes-like.")
        return value

    def validate_indexed(self, value: Any) -> None:
        if len(self.to_datastore(value)) > 1500:
            raise SchemaError("Indexed Datastore blobs must be at most 1500 bytes.")


@dataclass(frozen=True)
class TimestampType(DatastoreType):
    """UTC timestamp converter.

    Datastore stores timestamps at microsecond precision. pandas nanoseconds are
    intentionally truncated to avoid implying precision Datastore cannot preserve.
    """

    assume_timezone: timezone = timezone.utc
    name = "timestamp"

    def to_datastore(self, value: Any) -> datetime:
        import pandas as pd

        timestamp = pd.Timestamp(value)
        if timestamp.tzinfo is None:
            timestamp = timestamp.tz_localize(self.assume_timezone)
        timestamp = timestamp.tz_convert(timezone.utc)
        microsecond = timestamp.microsecond
        return datetime(
            timestamp.year,
            timestamp.month,
            timestamp.day,
            timestamp.hour,
            timestamp.minute,
            timestamp.second,
            microsecond,
            tzinfo=timezone.utc,
        )

    def from_datastore(self, value: Any) -> Any:
        import pandas as pd

        if value is None:
            return pd.NaT
        if isinstance(value, int) and not isinstance(value, bool):
            return pd.Timestamp(value, unit="us", tz="UTC")
        timestamp = pd.Timestamp(value)
        if timestamp.tzinfo is None:
            return timestamp.tz_localize("UTC")
        return timestamp.tz_convert("UTC")


class KeyType(DatastoreType):
    name = "key"

    def to_datastore(self, value: Any) -> Any:
        if isinstance(value, DatastoreKey):
            return value
        if isinstance(value, str):
            return DatastoreKey.from_json(value)
        if hasattr(value, "flat_path") or hasattr(value, "path"):
            return DatastoreKey.from_client_key(value)
        raise SchemaError("Key values must be DatastoreKey, client Key, or key JSON.")

    def from_datastore(self, value: Any) -> DatastoreKey | None:
        if value is None:
            return None
        if isinstance(value, DatastoreKey):
            return value
        return DatastoreKey.from_client_key(value)


class GeoPointType(DatastoreType):
    name = "geo_point"

    def to_datastore(self, value: Any) -> GeoPoint:
        if isinstance(value, GeoPoint):
            return value
        if isinstance(value, Mapping):
            return GeoPoint(float(value["latitude"]), float(value["longitude"]))
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
            return GeoPoint(float(value[0]), float(value[1]))
        raise SchemaError("GeoPoint values must be GeoPoint, mapping, or two-item sequence.")

    def from_datastore(self, value: Any) -> GeoPoint | None:
        if value is None:
            return None
        if isinstance(value, GeoPoint):
            return value
        if hasattr(value, "latitude") and hasattr(value, "longitude"):
            return GeoPoint(float(value.latitude), float(value.longitude))
        if isinstance(value, Mapping):
            return GeoPoint(float(value["latitude"]), float(value["longitude"]))
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
            return GeoPoint(float(value[0]), float(value[1]))
        raise SchemaError("GeoPoint values must expose latitude and longitude.")


@dataclass(frozen=True)
class ArrayType(DatastoreType):
    item_type: DatastoreType
    name = "array"

    def to_datastore(self, value: Any) -> list[Any]:
        if isinstance(value, (str, bytes)):
            raise SchemaError("Array values must be list-like, not scalar strings/blobs.")
        values = list(value)
        for item in values:
            if isinstance(item, (list, tuple)):
                raise SchemaError("Datastore arrays cannot contain nested arrays.")
        return [self.item_type.to_datastore(item) for item in values]

    def from_datastore(self, value: Any) -> list[Any]:
        if value is None:
            return []
        return [self.item_type.from_datastore(item) for item in value]

    def validate_indexed(self, value: Any) -> None:
        for item in value:
            self.item_type.validate_indexed(item)


class EmbeddedEntityType(DatastoreType):
    name = "entity"

    def to_datastore(self, value: Any) -> dict[str, Any]:
        if not isinstance(value, Mapping):
            raise SchemaError("Embedded entity values must be mapping-like.")
        return dict(value)


def is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    try:
        import pandas as pd

        result = pd.isna(value)
        if isinstance(result, bool):
            return result
        return False
    except Exception:
        return False

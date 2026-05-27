import pandas as pd
import pytest

from datastore_pandas import Field, GeoPoint, GeoPointType, Int64Type, Schema, StringType, TimestampType
from datastore_pandas.convert import to_client_datastore_value
from datastore_pandas.errors import SchemaError


def test_timestamp_type_truncates_to_microseconds_and_sets_utc():
    value = TimestampType().to_datastore(pd.Timestamp("2026-01-01T00:00:00.123456789Z"))

    assert value.microsecond == 123456
    assert value.tzinfo is not None


def test_timestamp_decode_handles_projection_microsecond_integer():
    value = TimestampType().from_datastore(1_767_225_600_123456)

    assert value == pd.Timestamp("2026-01-01T00:00:00.123456Z")


def test_timestamp_decode_localizes_naive_values_to_utc():
    value = TimestampType().from_datastore("2026-01-01T00:00:00")

    assert value == pd.Timestamp("2026-01-01T00:00:00Z")


def test_indexed_string_limit_is_enforced():
    schema = Schema(
        kind="Doc",
        properties={"body": Field(StringType(), indexed=True)},
    )

    with pytest.raises(SchemaError):
        schema.encode_properties({"body": "x" * 1501})


def test_strict_schema_allows_key_source_columns():
    from datastore_pandas import KeyPart, KeySpec

    schema = Schema(
        kind="Doc",
        key=KeySpec([("Doc", KeyPart("doc_id"))]),
        properties={"value": Field(Int64Type())},
        strict=True,
    )

    schema.validate_row({"doc_id": "abc", "value": 1})


def test_nullable_missing_values_are_omitted_by_default():
    schema = Schema(
        kind="Doc",
        properties={
            "title": Field(StringType()),
            "optional_note": Field(StringType()),
        },
    )

    encoded, excluded = schema.encode_properties({"title": "present", "optional_note": pd.NA})

    assert encoded == {"title": "present"}
    assert excluded == []


def test_explicit_empty_properties_encodes_no_properties():
    schema = Schema(
        kind="Doc",
        properties={
            "title": Field(StringType()),
            "optional_note": Field(StringType()),
        },
    )

    encoded, excluded = schema.encode_properties(
        {"title": "present", "optional_note": "present"},
        properties=[],
    )

    assert encoded == {}
    assert excluded == []


def test_nullable_fields_can_write_explicit_nulls():
    schema = Schema(
        kind="Doc",
        properties={"optional_note": Field(StringType(), missing_policy="null")},
    )

    encoded, _ = schema.encode_properties({"optional_note": pd.NA})

    assert encoded == {"optional_note": None}


def test_strict_schema_ignores_unknown_columns_when_value_is_missing():
    schema = Schema(
        kind="Doc",
        properties={"title": Field(StringType())},
        strict=True,
    )

    schema.validate_row({"title": "present", "sparse_other_kind_column": pd.NA})


def test_geopoint_type_converts_to_client_serializable_value():
    from google.cloud import datastore
    from google.cloud.datastore.helpers import GeoPoint as ClientGeoPoint
    from google.cloud.datastore.helpers import entity_to_protobuf

    value = GeoPointType().to_datastore({"latitude": 41.88, "longitude": -87.63})
    client_value = to_client_datastore_value(value, client=object())

    assert isinstance(client_value, ClientGeoPoint)

    entity = datastore.Entity(key=datastore.Key("Doc", "a", project="fake-project"))
    entity["point"] = client_value
    entity_to_protobuf(entity)


def test_geopoint_type_decodes_client_value_to_package_value():
    from google.cloud.datastore.helpers import GeoPoint as ClientGeoPoint

    value = GeoPointType().from_datastore(ClientGeoPoint(41.88, -87.63))

    assert value == GeoPoint(41.88, -87.63)

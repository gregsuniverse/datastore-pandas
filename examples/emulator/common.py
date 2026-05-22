"""Shared setup for the Datastore emulator examples."""

from __future__ import annotations

import os
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if SRC.exists() and str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import pandas as pd

import datastore_pandas as dsp


PROJECT_ID = os.getenv("DATASTORE_PROJECT_ID", "datastore-pandas-emulator")
EMULATOR_HOST = os.getenv("DATASTORE_EMULATOR_HOST", "localhost:8081")
DEFAULT_DATA_FILE = Path(__file__).resolve().parent / "data" / "workouts.csv"


def configure_emulator_env() -> None:
    os.environ.setdefault("DATASTORE_PROJECT_ID", PROJECT_ID)
    os.environ.setdefault("DATASTORE_EMULATOR_HOST", EMULATOR_HOST)


def client():
    configure_emulator_env()
    from google.cloud import datastore

    return datastore.Client(project=PROJECT_ID)


WORKOUT_SCHEMA = dsp.Schema(
    kind="Workout",
    key=dsp.KeySpec(
        [
            ("User", dsp.KeyPart("user_id", kind="name")),
            ("Workout", dsp.KeyPart("workout_id", kind="name")),
        ],
        namespace_source="tenant",
    ),
    properties={
        "started_at": dsp.Field(dsp.TimestampType(), nullable=False),
        "duration_sec": dsp.Field(dsp.Int64Type(), nullable=False),
        "distance_m": dsp.Field(dsp.Float64Type(), nullable=False),
        "activity_type": dsp.Field(dsp.StringType(), nullable=False),
        "pool_length_m": dsp.Field(dsp.Int64Type()),
        "stroke": dsp.Field(dsp.StringType()),
        "bike_power_w": dsp.Field(dsp.Int64Type()),
        "bike_trainer": dsp.Field(dsp.BoolType()),
        "run_cadence_spm": dsp.Field(dsp.Int64Type()),
        "shoe_model": dsp.Field(dsp.StringType()),
        "notes": dsp.Field(dsp.StringType(), indexed=False),
        "last_reviewed_at": dsp.Field(dsp.TimestampType()),
    },
    strict=True,
)

COUNTER_SCHEMA = dsp.Schema(
    kind="Counter",
    key=dsp.KeySpec(
        [
            ("Tenant", dsp.KeyPart("tenant", kind="name")),
            ("Counter", dsp.KeyPart("counter_name", kind="name")),
        ]
    ),
    properties={
        "value": dsp.Field(dsp.Int64Type(), nullable=False),
        "updated_at": dsp.Field(dsp.TimestampType()),
    },
    strict=True,
)


def read_workout_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, parse_dates=["started_at"])
    for column in ["bike_trainer"]:
        if column in df:
            df[column] = df[column].map(_parse_bool).astype("boolean")
    return df


def ancestor_key(user_id: str, *, tenant: str = "tenant-a") -> dsp.DatastoreKey:
    return dsp.DatastoreKey(namespace=tenant, path=(("User", user_id),))


def counter_key(tenant: str, counter_name: str) -> dsp.DatastoreKey:
    return dsp.DatastoreKey(path=(("Tenant", tenant), ("Counter", counter_name)))


def print_frame(title: str, df: pd.DataFrame, *, rows: int = 8) -> None:
    print(f"\n== {title} ==")
    if df.empty:
        print("<empty>")
        return
    print(df.head(rows).to_string(index=False))


def _parse_bool(value):
    if pd.isna(value) or value == "":
        return pd.NA
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes"}

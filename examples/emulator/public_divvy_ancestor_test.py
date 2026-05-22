"""Public dataset integration test using Divvy trip data and Datastore ancestors.

This example downloads a public CSV-in-ZIP dataset into pandas, maps it into a
Datastore hierarchy, loads it through datastore-pandas, and validates ancestor
queries against the emulator.

Hierarchy:

    Dataset("divvy-2024-01")
      Station(<start_station_id_or_slug>)
        Ride(<ride_id>)
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from time import perf_counter
from urllib.request import urlretrieve
import zipfile

import pandas as pd

from common import client, print_frame
import datastore_pandas as dsp


DATASET_ID = "divvy-2024-01"
NAMESPACE = "divvy-public"
DEFAULT_URL = "https://divvy-tripdata.s3.amazonaws.com/202401-divvy-tripdata.zip"
DEFAULT_ZIP = Path(__file__).resolve().parent / "data" / "202401-divvy-tripdata.zip"


DATASET_KEY = dsp.DatastoreKey(
    namespace=NAMESPACE,
    path=(("Dataset", DATASET_ID),),
)

STATION_SCHEMA = dsp.Schema(
    kind="Station",
    key=dsp.KeySpec(
        [
            ("Dataset", dsp.KeyPart(constant=DATASET_ID, kind="name")),
            ("Station", dsp.KeyPart("station_key", kind="name")),
        ],
        namespace=NAMESPACE,
    ),
    properties={
        "station_key": dsp.Field(dsp.StringType(), nullable=False),
        "start_station_name": dsp.Field(dsp.StringType(), nullable=False),
        "ride_count": dsp.Field(dsp.Int64Type(), nullable=False),
        "avg_start_lat": dsp.Field(dsp.Float64Type()),
        "avg_start_lng": dsp.Field(dsp.Float64Type()),
    },
    strict=True,
)

RIDE_SCHEMA = dsp.Schema(
    kind="Ride",
    key=dsp.KeySpec(
        [
            ("Dataset", dsp.KeyPart(constant=DATASET_ID, kind="name")),
            ("Station", dsp.KeyPart("station_key", kind="name")),
            ("Ride", dsp.KeyPart("ride_id", kind="name")),
        ],
        namespace=NAMESPACE,
    ),
    properties={
        "ride_id": dsp.Field(dsp.StringType(), nullable=False),
        "station_key": dsp.Field(dsp.StringType(), nullable=False),
        "started_at": dsp.Field(dsp.TimestampType(), nullable=False),
        "ended_at": dsp.Field(dsp.TimestampType(), nullable=False),
        "duration_sec": dsp.Field(dsp.Int64Type(), nullable=False),
        "rideable_type": dsp.Field(dsp.StringType(), nullable=False),
        "member_casual": dsp.Field(dsp.StringType(), nullable=False),
        "start_station_name": dsp.Field(dsp.StringType(), nullable=False),
        "end_station_name": dsp.Field(dsp.StringType()),
        "start_lat": dsp.Field(dsp.Float64Type()),
        "start_lng": dsp.Field(dsp.Float64Type()),
        "end_lat": dsp.Field(dsp.Float64Type()),
        "end_lng": dsp.Field(dsp.Float64Type()),
    },
    strict=True,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--zip-path", type=Path, default=DEFAULT_ZIP)
    parser.add_argument("--rows", type=int, default=50_000)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--batch-size", type=int, default=400)
    parser.add_argument("--ancestor-limit", type=int, default=1000)
    args = parser.parse_args()

    zip_path = download_dataset(args.url, args.zip_path)
    rides = load_rides(zip_path, rows=args.rows)
    stations = summarize_stations(rides)

    busiest = stations.sort_values("ride_count", ascending=False).iloc[0]
    station_key = busiest["station_key"]
    station_ancestor = dsp.DatastoreKey(
        namespace=NAMESPACE,
        path=(("Dataset", DATASET_ID), ("Station", station_key)),
    )

    ds = client()
    started = perf_counter()
    station_report = dsp.to_datastore(
        stations,
        schema=STATION_SCHEMA,
        client=ds,
        mode="upsert",
        batch_size=args.batch_size,
        max_workers=args.workers,
    )
    station_report.raise_for_errors()

    ride_report = dsp.to_datastore(
        rides,
        schema=RIDE_SCHEMA,
        client=ds,
        mode="upsert",
        batch_size=args.batch_size,
        max_workers=args.workers,
    )
    ride_report.raise_for_errors()
    elapsed = perf_counter() - started

    print(
        f"loaded public Divvy data stations={station_report.succeeded:,} "
        f"rides={ride_report.succeeded:,} elapsed={elapsed:.2f}s"
    )
    print(f"busiest station: {busiest['start_station_name']} ({station_key})")

    station_children = dsp.read_datastore(
        kind="Ride",
        schema=RIDE_SCHEMA,
        client=ds,
        ancestor=station_ancestor,
        limit=args.ancestor_limit,
        include_key=True,
    )
    print_frame("Ride descendants under one Station ancestor", station_children)

    projected_children = dsp.read_datastore(
        kind="Ride",
        schema=RIDE_SCHEMA,
        client=ds,
        ancestor=station_ancestor,
        projection=["started_at", "duration_sec", "rideable_type", "member_casual"],
        limit=10,
        include_key=True,
    )
    print_frame("Projected Ride descendants under Station ancestor", projected_children)

    keys_only = dsp.read_datastore(
        kind="Ride",
        client=ds,
        ancestor=station_ancestor,
        keys_only=True,
        limit=10,
        include_key=True,
    )
    print_frame("Keys-only Ride descendants under Station ancestor", keys_only)

    stations_under_dataset = dsp.read_datastore(
        kind="Station",
        schema=STATION_SCHEMA,
        client=ds,
        ancestor=DATASET_KEY,
        projection=["station_key", "start_station_name", "ride_count"],
        limit=10,
        include_key=True,
    )
    print_frame("Station descendants under Dataset ancestor", stations_under_dataset)

    expected_station_count = int(busiest["ride_count"])
    assert not station_children.empty, "station ancestor query returned no rides"
    assert len(station_children) == min(expected_station_count, args.ancestor_limit)
    assert not projected_children.empty, "projection ancestor query returned no rides"
    assert len(keys_only) == min(10, expected_station_count)
    assert not stations_under_dataset.empty, "dataset ancestor query returned no stations"
    for key in station_children["__key__"]:
        assert key.path[:2] == (("Dataset", DATASET_ID), ("Station", station_key))

    print(
        "ancestor checks passed: "
        f"station_expected={expected_station_count:,} "
        f"station_returned={len(station_children):,}"
    )


def download_dataset(url: str, zip_path: Path) -> Path:
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    if zip_path.exists() and zip_path.stat().st_size > 0:
        print(f"using cached public dataset {zip_path}")
        return zip_path
    print(f"downloading public dataset from {url}")
    urlretrieve(url, zip_path)
    print(f"downloaded {zip_path} ({zip_path.stat().st_size:,} bytes)")
    return zip_path


def load_rides(zip_path: Path, *, rows: int) -> pd.DataFrame:
    with zipfile.ZipFile(zip_path) as archive:
        csv_name = next(name for name in archive.namelist() if name.endswith(".csv"))
        with archive.open(csv_name) as source:
            raw = pd.read_csv(
                source,
                nrows=rows,
                dtype={
                    "ride_id": "string",
                    "rideable_type": "string",
                    "start_station_name": "string",
                    "start_station_id": "string",
                    "end_station_name": "string",
                    "end_station_id": "string",
                    "member_casual": "string",
                },
            )

    raw = raw.dropna(subset=["ride_id", "started_at", "ended_at", "start_station_name"])
    raw["started_at"] = pd.to_datetime(raw["started_at"], utc=True, errors="coerce")
    raw["ended_at"] = pd.to_datetime(raw["ended_at"], utc=True, errors="coerce")
    raw = raw.dropna(subset=["started_at", "ended_at"])
    raw["duration_sec"] = (raw["ended_at"] - raw["started_at"]).dt.total_seconds().astype("int64")
    raw = raw[raw["duration_sec"] > 0].copy()
    raw["station_key"] = raw.apply(_station_key, axis=1)

    columns = [
        "ride_id",
        "station_key",
        "started_at",
        "ended_at",
        "duration_sec",
        "rideable_type",
        "member_casual",
        "start_station_name",
        "end_station_name",
        "start_lat",
        "start_lng",
        "end_lat",
        "end_lng",
    ]
    rides = raw[columns].copy()
    print(f"loaded public dataset rows={len(rides):,} stations={rides['station_key'].nunique():,}")
    return rides


def summarize_stations(rides: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        rides.groupby("station_key", dropna=False)
        .agg(
            start_station_name=("start_station_name", "first"),
            ride_count=("ride_id", "count"),
            avg_start_lat=("start_lat", "mean"),
            avg_start_lng=("start_lng", "mean"),
        )
        .reset_index()
    )
    grouped["ride_count"] = grouped["ride_count"].astype("int64")
    return grouped


def _station_key(row) -> str:
    station_id = row.get("start_station_id")
    if pd.notna(station_id) and str(station_id).strip():
        return str(station_id).strip()
    return "station-" + re.sub(r"[^a-z0-9]+", "-", str(row["start_station_name"]).lower()).strip("-")


if __name__ == "__main__":
    main()

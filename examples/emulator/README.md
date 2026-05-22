# Datastore Emulator Examples

These examples run against the Firestore emulator in Datastore mode. They populate
the emulator with synthetic sparse entities, then exercise reads, projection queries,
keys-only scans, patch updates, transactions, index planning, and batched concurrent
writes.

## Start the Emulator

From the repository root:

```powershell
cd <repo>
docker compose -f examples\emulator\docker-compose.yml up --build
```

In a second terminal:

```powershell
$env:DATASTORE_EMULATOR_HOST = "localhost:8081"
$env:DATASTORE_PROJECT_ID = "datastore-pandas-emulator"
python -m pip install -e ".[test,polars]"
```

The examples default to `localhost:8081` and `datastore-pandas-emulator`, so those
environment variables are mostly there to make the emulator target explicit.

## Run the Full Flow

```powershell
python examples\emulator\run_all.py --rows 20000 --workers 8
python examples\emulator\run_all.py --backend polars --rows 20000 --workers 8
```

The examples default to pandas. The main data-generation, load, query, patch,
transaction, run-all, and public-dataset scripts also accept `--backend polars`.

## Run Individual Steps

```powershell
python examples\emulator\generate_mock_data.py --backend polars --rows 50000 --out examples\emulator\data\workouts.csv
python examples\emulator\load_mock_data.py --backend polars --input examples\emulator\data\workouts.csv --workers 8
python examples\emulator\query_examples.py --backend polars --user-id user-00042
python examples\emulator\patch_sparse_rows.py --backend polars --user-id user-00042
python examples\emulator\transaction_example.py --backend polars
python examples\emulator\inspect_sparse_entities.py --limit 2000 --namespace tenant-a
python examples\emulator\index_planning.py
python examples\emulator\public_divvy_ancestor_test.py --backend polars --rows 50000 --workers 8
python examples\emulator\reset_emulator_data.py
```

## What the Data Covers

The generated `Workout` kind is intentionally heterogeneous:

- every entity has `started_at`, `duration_sec`, `distance_m`, and `activity_type`
- swim entities may have `pool_length_m` and `stroke`
- bike entities may have `bike_power_w` and `bike_trainer`
- run entities may have `run_cadence_spm` and `shoe_model`
- some rows have `notes`
- many DataFrame cells are blank/NA and should be omitted from Datastore writes

This is the important sparse-entity case: the DataFrame is rectangular, but the
Datastore entities should not be forced to carry unused null-valued properties.

## Public Dataset Ancestor Test

`public_divvy_ancestor_test.py` downloads Divvy's public bike-share trip data from
the official trip-history bucket, normalizes the source data, and maps it into this
Datastore hierarchy:

```text
Dataset("divvy-2024-01")
  Station(<start_station_id_or_slug>)
    Ride(<ride_id>)
```

It writes both `Station` and `Ride` entities through the selected
`datastore-pandas` DataFrame adapter, then
checks:

- `Dataset` ancestor queries return station descendants
- `Station` ancestor queries return ride descendants
- projection queries work under an ancestor
- keys-only queries work under an ancestor
- returned ride keys have the expected ancestor path prefix

The default run loads 50,000 public rows:

```powershell
python examples\emulator\public_divvy_ancestor_test.py --rows 50000 --workers 8
python examples\emulator\public_divvy_ancestor_test.py --backend polars --rows 50000 --workers 8
```

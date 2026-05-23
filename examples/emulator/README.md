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
environment variables are mostly there to make the emulator target explicit. The
Docker Compose file sets the emulator Java heap to 4 GB because the large linked
dataset is intentionally a local stress test.

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
python examples\emulator\policy_examples.py --backend polars
python examples\emulator\dataframe_model_examples.py --backend polars
python examples\emulator\edge_case_examples.py --backend polars
python examples\emulator\inspect_sparse_entities.py --limit 2000 --namespace tenant-a
python examples\emulator\index_planning.py
python examples\emulator\public_divvy_ancestor_test.py --backend polars --rows 50000 --workers 8
python examples\emulator\large_linked_dataset.py --backend polars
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

## Instantiated Accessor And Write Policy Test

`policy_examples.py` uses `dsp.kind(...)` against the emulator with a small
`PolicyEvent` kind. It validates:

- deterministic `key_policy(...)` keys with a `Tenant -> PolicyEvent` ancestor
  path
- custom audit timestamp fields through `AuditPolicy`
- dry-run write planning without committing entities
- read-only write blocking
- skip-unchanged full writes
- skip-unchanged patch writes
- bound ancestor write-scope rejection
- logical duplicate cleanup dry-run and explicit delete execution

Run it with either DataFrame backend:

```powershell
python examples\emulator\policy_examples.py
python examples\emulator\policy_examples.py --backend polars
```

## DataFrame Model And Schema Inference Test

`dataframe_model_examples.py` uses `dspdf(...)`, the DataFrame-owning model layer.
It validates:

- loading source entities into a model that retains source context
- optional original DataFrame snapshots
- row-preserving source writes with `skip_unchanged=True`
- blocking derived aggregate writes back to the source kind
- writing derived aggregate results to an alternate summary kind
- schema inference for a kind with mixed property types

Run it with either DataFrame backend:

```powershell
python examples\emulator\dataframe_model_examples.py
python examples\emulator\dataframe_model_examples.py --backend polars
```

## Focused Edge-Case Test

`edge_case_examples.py` is an executable checklist for behavior that is easy to
miss in broad examples. It validates:

- dry-run and read-only report generation without default Datastore client
  construction
- no Secret Manager module loading during offline planning
- clear `would_write`, `wrote`, `failed`, and `skipped` report counts
- deterministic `key_policy(...)` keys with custom id, namespace, and ancestor
  fields
- bound ancestor rejection for out-of-scope writes
- all custom audit timestamp aliases: `created`, `created_at`, `modified`,
  `modified_at`, and `updated_at`
- batch sizing with an injectable fake client
- compatibility with `google.api_core.retry.Retry(initial=2.0, multiplier=2.0,
  deadline=40.0)`
- skip-unchanged full writes and patch writes against the emulator
- logical duplicate cleanup dry-run and delete execution
- `dspdf(...)` source snapshots, derived write blocking, and alternate target
  writes
- schema inference for a kind containing mixed property types

Run it with either DataFrame backend:

```powershell
python examples\emulator\edge_case_examples.py
python examples\emulator\edge_case_examples.py --backend polars
```

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

## Large Linked-Kind Test

`large_linked_dataset.py` generates a synthetic linked-kind dataset without
downloading external data. It writes this shape:

```text
Tenant(<tenant>)
  LinkedUser(<user_id>)
    LinkedSession(<session_id>)
      LinkedEvent(<event_id>)

Tenant(<tenant>)
  LinkedDevice(<device_id>)
```

The rows also contain `KeyType` properties that point across kinds:

- `LinkedDevice.assigned_user_key -> LinkedUser`
- `LinkedSession.user_key -> LinkedUser`
- `LinkedSession.device_key -> LinkedDevice`
- `LinkedEvent.user_key -> LinkedUser`
- `LinkedEvent.session_key -> LinkedSession`
- `LinkedEvent.device_key -> LinkedDevice`

The default run writes 1,000 users, 1,000 devices, 10,000 sessions, and 200,000
events in chunks. It uses conservative local-emulator defaults (`--workers 4`,
`--batch-size 100`) because very large concurrent emulator commits can be less
stable than production Datastore. Docker Desktop must have enough memory
available for the emulator heap:

```powershell
python examples\emulator\large_linked_dataset.py
python examples\emulator\large_linked_dataset.py --backend polars
```

To scale the non-event entity counts higher, pass larger `--users`, `--devices`,
and `--sessions` values explicitly.

To attempt a million-event local stress run:

```powershell
python examples\emulator\large_linked_dataset.py --backend polars --events 1000001
```

On the local verification machine, the 200,000-event run completed successfully.
The 1,000,001-event run exceeded the Firestore emulator Java heap even with the
Compose file's 4 GB heap setting. Treat million-row emulator runs as host-memory
stress tests, not as a guarantee that the local emulator can retain the full
dataset.

For a quick smoke test before a full load:

```powershell
python examples\emulator\large_linked_dataset.py --events 10000 --sessions 2500 --users 1000 --devices 1000
```

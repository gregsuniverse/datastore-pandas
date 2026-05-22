# datastore-pandas

`datastore-pandas` is a schema-aware DataFrame interface for Firestore in
Datastore mode. It provides a pandas API by default and an optional Polars adapter
through `datastore-pandas[polars]`. It is designed for Datastore's real execution
model: indexed entity queries, key lookups, projections, cursor scans,
transactions, and batched entity writes.

It does not try to turn Datastore into BigQuery. BigQuery can expose a broad
pandas-like API because it has SQL, columnar execution, joins, aggregations, and
server-side query planning. Datastore is an operational NoSQL entity store. This
package keeps that boundary explicit so DataFrame workflows remain convenient
without hiding Datastore's limits.

## Current Status

This is an initial implementation scaffold. The code already covers the core
shape of the package:

- explicit schema objects for safe DataFrame-to-entity conversion
- first-class Datastore key representation and row-to-key mapping
- sparse entity writes that omit missing DataFrame values by default
- typed conversion for strings, integers, floats, booleans, timestamps, blobs,
  keys, arrays, geo points, and embedded entities
- query construction for projections, keys-only queries, distinct projections,
  ancestor queries, filters, orderings, and limits
- batched writes with bounded concurrency
- read-merge-write patch updates for partial DataFrames
- a transaction helper for small read-modify-write workflows
- index-planning helpers that produce `index.yaml`-style suggestions
- emulator examples for local integration testing
- an optional Polars adapter with matching read, chunked-read, write, and patch
  operations

The package is not yet a complete production client. The current high-level write
path relies on `google-cloud-datastore`; lower-level mutation support is still the
right next step for property masks, compare-and-swap writes, generated-key result
metadata, and conflict details.

## License And Disclaimer

This repository uses [The Unlicense](LICENSE), a public-domain-style dedication.
The repository does not identify an owner and does not make ownership claims over
the software.

Before using, copying, modifying, or relying on this repository, read the
[Disclaimer](DISCLAIMER.md). In short: this project is experimental, provided
without warranty, not recommended for any particular use, not guaranteed to be
maintained, and includes AI-generated or AI-assisted software and documentation.

## Installation

From this repository:

```powershell
cd <repo>
python -m pip install -e ".[test]"
```

For the optional Polars adapter:

```powershell
python -m pip install -e ".[test,polars]"
```

When installed from a package index, use:

```powershell
python -m pip install datastore-pandas
python -m pip install "datastore-pandas[polars]"
```

For local development against the emulator, no Google Cloud credentials are needed
when `DATASTORE_EMULATOR_HOST` is set. For real Datastore mode projects, configure
Application Default Credentials:

```powershell
gcloud auth application-default login
```

## Quick Start

```python
import datastore_pandas as dsp

schema = dsp.Schema(
    kind="Workout",
    key=dsp.KeySpec([
        ("User", dsp.KeyPart("user_id", kind="name")),
        ("Workout", dsp.KeyPart("workout_id", kind="name")),
    ]),
    properties={
        "started_at": dsp.Field(dsp.TimestampType(), nullable=False),
        "duration_sec": dsp.Field(dsp.Int64Type(), nullable=False),
        "distance_m": dsp.Field(dsp.Float64Type()),
        "activity_type": dsp.Field(dsp.StringType(), nullable=False),
        "notes": dsp.Field(dsp.StringType(), indexed=False),
    },
    strict=True,
)

df = dsp.read_datastore(
    kind="Workout",
    schema=schema,
    filters=[("activity_type", "=", "run")],
    projection=["started_at", "duration_sec", "distance_m"],
    order=["-started_at"],
    include_key=True,
)

report = dsp.to_datastore(
    df,
    schema=schema,
    mode="upsert",
    batch_size=400,
    max_workers=8,
)
report.raise_for_errors()
```

For Polars, install the optional extra and use the adapter module. The schema,
query, key, batching, projection, and sparse-write behavior is shared:

```python
import datastore_pandas as dsp
import datastore_pandas.polars as dsp_pl

df = dsp_pl.read_datastore(kind="Workout", schema=schema, limit=1000)
report = dsp_pl.to_datastore(df, schema=schema, batch_size=400)
```

## Why Schema Is Required For Writes

Datastore mode does not enforce one fixed schema per kind. Two entities of the
same kind can have different property sets and different property types. pandas
and Polars, however, rectangularize data into columns. Without an explicit
schema, a write adapter cannot safely tell whether a missing DataFrame cell
means:

- the property should be omitted
- the property should be written as Datastore `null`
- the property is required and the row is invalid
- the property is an accidental extra column from another entity shape

`datastore-pandas` makes that decision explicit with `Schema` and `Field`.

```python
schema = dsp.Schema(
    kind="Document",
    properties={
        "title": dsp.Field(dsp.StringType(), nullable=False),
        "summary": dsp.Field(dsp.StringType()),
        "raw_text": dsp.Field(dsp.StringType(), indexed=False),
    },
)
```

By default, nullable missing values are omitted:

```python
encoded, excluded = schema.encode_properties({
    "title": "present",
    "summary": None,
})

assert encoded == {"title": "present"}
```

To intentionally write a Datastore `null` property, opt in:

```python
dsp.Field(dsp.StringType(), missing_policy="null")
```

For required fields, use `nullable=False`. Missing or `NA` values will raise a
schema error before the row reaches Datastore.

## Sparse Entities And DataFrames

Sparse entities are a first-order design case. For example, a `Workout` kind might
store swim, bike, and run entities together:

| Property | Swim | Bike | Run |
|---|---:|---:|---:|
| `started_at` | yes | yes | yes |
| `duration_sec` | yes | yes | yes |
| `pool_length_m` | yes | no | no |
| `bike_power_w` | no | yes | no |
| `run_cadence_spm` | no | no | yes |

When these entities are read into pandas or Polars, the DataFrame must contain all
columns, so absent Datastore properties appear as missing values. On write, those
missing values should not become stored null properties on every entity. The
default `missing_policy` is therefore `omit`.

This matters for correctness, index size, write cost, and query behavior.

## Key Management

Keys are identity, not ordinary properties. `datastore-pandas` preserves the full
Datastore key shape:

- project
- database
- namespace
- ancestor path
- kind names
- string name IDs
- numeric IDs
- incomplete leaf keys for auto-ID allocation

Example:

```python
key = dsp.DatastoreKey(
    project="my-project",
    namespace="tenant-a",
    path=(
        ("User", "sample-user"),
        ("Workout", 123456789),
    ),
)
```

For DataFrame writes, `KeySpec` maps row columns to key path elements:

```python
key = dsp.KeySpec(
    [
        ("User", dsp.KeyPart("user_id", kind="name")),
        ("Workout", dsp.KeyPart("workout_id", kind="name")),
    ],
    namespace_source="tenant",
)
```

The package keeps numeric IDs and string names distinct. `123` and `"123"` are
different Datastore keys.

## Reading

Use `read_datastore` for normal DataFrame reads:

```python
df = dsp.read_datastore(
    kind="Workout",
    schema=schema,
    filters=[("activity_type", "=", "bike")],
    order=["-started_at"],
    limit=1000,
    include_key=True,
)
```

Use `datastore_pandas.polars.read_datastore` with the same arguments when you
want a Polars `DataFrame`.

Use projections to read only indexed properties:

```python
df = dsp.read_datastore(
    kind="Workout",
    schema=schema,
    projection=["started_at", "duration_sec", "distance_m"],
    order=["-started_at"],
)
```

Use keys-only queries when planning deletes, existence checks, or staged fan-out
lookups:

```python
keys = dsp.read_datastore(
    kind="Workout",
    keys_only=True,
    include_key=True,
)
```

Use `iter_datastore` for chunked processing:

```python
for chunk in dsp.iter_datastore(kind="Workout", schema=schema, chunksize=1000):
    process(chunk)
```

## Writing

Use `to_datastore` for full entity writes:

```python
report = dsp.to_datastore(
    df,
    schema=schema,
    mode="upsert",
    batch_size=400,
    max_workers=8,
)
report.raise_for_errors()
```

The write path:

- validates rows against the schema
- builds Datastore keys from `KeySpec` or `__key__`
- converts DataFrame values to Datastore-safe values
- omits nullable missing values by default
- excludes unindexed fields from indexes
- rejects duplicate complete keys in one commit
- chunks writes into bounded batches

`insert` and `update` modes are represented in the API, but full correctness for
those modes depends on the active Datastore batch backend exposing insert/update
methods. `upsert` is the safest path in the initial scaffold.

## Patching Partial DataFrames

Projection queries and sparse application workflows often produce partial
DataFrames. Do not send those through replacement-style writes unless you intend
to replace the full entity.

Use `patch_datastore`:

```python
patch = df[["__key__", "notes", "last_reviewed_at"]]

dsp.patch_datastore(
    patch,
    schema=schema,
    properties=["notes", "last_reviewed_at"],
)
```

The current implementation uses read-merge-write so omitted properties are
preserved. A lower-level Datastore `Commit` backend should eventually replace this
for native `property_mask` support.

## Transactions

Transactions are for small atomic workflows, not bulk ingestion:

```python
with dsp.Transaction(client) as tx:
    row = tx.get(counter_key, schema=counter_schema)
    row["value"] += 1
    tx.put(row, schema=counter_schema)
```

Keep transactions small, retryable, and focused on read-modify-write logic.
Bulk writes should use `to_datastore`.

## Index Planning

`plan_indexes` provides conservative local guidance for composite index needs:

```python
query = dsp.QuerySpec(
    kind="Workout",
    filters=[("activity_type", "=", "run"), ("started_at", ">=", start)],
    order=["started_at"],
)

plan = dsp.plan_indexes(query)
for suggestion in plan.suggestions:
    print(suggestion.to_index_yaml())
```

This is not a replacement for Datastore Query Explain. It is intended to catch
common index shapes before runtime and generate a starting point for `index.yaml`.

## Emulator Examples

The repository includes a Docker Compose setup that runs the Firestore emulator in
Datastore mode and a complete sparse-data example suite:

```powershell
docker compose -f examples\emulator\docker-compose.yml up --build
```

In a second terminal:

```powershell
$env:DATASTORE_EMULATOR_HOST = "localhost:8081"
$env:DATASTORE_PROJECT_ID = "datastore-pandas-emulator"
python -m pip install -e ".[test,polars]"
python examples\emulator\run_all.py --rows 20000 --workers 8
python examples\emulator\run_all.py --backend polars --rows 20000 --workers 8
```

The emulator examples include:

- `generate_mock_data.py`: creates heterogeneous swim/bike/run workout rows
- `load_mock_data.py`: loads large DataFrames with batched concurrent writes
- `query_examples.py`: demonstrates full reads, projections, keys-only queries,
  and distinct projections
- `patch_sparse_rows.py`: patches a subset of properties without filling sparse
  entities with nulls
- `transaction_example.py`: increments a counter transactionally
- `inspect_sparse_entities.py`: inspects raw entities to confirm sparse properties
  are omitted
- `index_planning.py`: prints index suggestions
- `reset_emulator_data.py`: clears sample entities from the emulator
- `public_divvy_ancestor_test.py`: downloads public Divvy bike-share trip data,
  loads `Dataset -> Station -> Ride` ancestor paths, and validates ancestor,
  projection, and keys-only queries

The main examples accept `--backend pandas` or `--backend polars`.

Full instructions are in [examples/emulator/README.md](examples/emulator/README.md).

## Type Mapping

| Datastore concept | Package type |
|---|---|
| integer | `Int64Type` |
| double | `Float64Type` |
| boolean | `BoolType` |
| timestamp | `TimestampType` |
| string | `StringType` |
| blob | `BlobType` |
| key | `KeyType` / `DatastoreKey` |
| geo point | `GeoPointType` / `GeoPoint` |
| array | `ArrayType(...)` |
| embedded entity | `EmbeddedEntityType` |

Important conversion rules:

- timestamps are normalized to UTC
- Datastore timestamp precision is microseconds
- integers are validated against signed 64-bit bounds
- indexed strings and blobs must fit Datastore indexed-value limits
- arrays cannot contain nested arrays
- unindexed fields should be declared with `indexed=False`

## Design Principles

- Prefer Datastore-native operations over pretending arbitrary pandas operations
  can be pushed down.
- Make schemas explicit for writes.
- Treat keys as first-class identity values.
- Omit nullable missing values by default to preserve sparse entities.
- Use projection, keys-only, ancestor, and cursor-aware queries where appropriate.
- Keep transactions explicit and small.
- Use BigQuery or BigQuery DataFrames for analytics, joins, and broad scans.

## Repository Layout

```text
src/datastore_pandas/
  batches.py       batch planning and duplicate-key checks
  convert.py       row/entity conversion
  errors.py        package exceptions
  io.py            read_datastore, iter_datastore, to_datastore, patch_datastore
  keys.py          DatastoreKey, KeySpec, KeyPart
  polars.py        optional Polars adapter
  query.py         QuerySpec and index planning
  reports.py       write result reporting
  schema.py        Schema and Field
  transaction.py   transaction context manager
  types.py         Datastore type converters

examples/
  basic_usage.py
  emulator/
    docker-compose.yml
    Dockerfile
    README.md
    run_all.py
```

## Limitations And Next Steps

Current limitations:

- `pytest` and `google-cloud-datastore` must be installed locally to run the full
  test and emulator flow.
- `patch_datastore` uses read-merge-write instead of native mutation property
  masks.
- write reports do not yet include generated keys, entity versions, update times,
  or conflict details from lower-level mutation results.
- compare-and-swap writes using `base_version` or `update_time` are not implemented
  yet.
- aggregation queries and Query Explain are design targets but not implemented in
  the package API yet.
- the index planner is conservative and should be validated against emulator and
  production Query Explain output.
- the Polars adapter shares the same Datastore backend; transaction helpers still
  work with dictionaries rather than DataFrame-native transaction objects.

Useful next work:

- add a lower-level Datastore `Commit` backend
- support native property masks and conflict detection
- add generated-key allocation and result mapping
- add aggregation helpers such as `count`
- add Query Explain integration
- add live emulator integration tests in CI

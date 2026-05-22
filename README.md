# datastore-pandas

`datastore-pandas` is a schema-aware DataFrame interface for Firestore in
Datastore mode. It provides a pandas API by default and an optional Polars adapter
through `datastore-pandas[polars]`. It is designed for Datastore's real execution
model: indexed entity queries, key lookups, projections, cursor scans,
transactions, and batched entity writes.

It does not try to turn Datastore into SQL or a general analytics engine.
Datastore is an operational NoSQL entity store, so joins, aggregations, and broad
server-side query planning are outside the package boundary. This package keeps
that boundary explicit so DataFrame workflows remain convenient without hiding
Datastore's limits.

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
- dry-run write planning, read-only write blocking, and skip-unchanged writes
- instantiated kind accessors for bound schema/client/query defaults
- deterministic key-policy and audit timestamp helpers
- logical duplicate cleanup planning and opt-in delete execution
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

This repository uses
[The Unlicense](https://github.com/gregsuniverse/datastore-pandas/blob/main/LICENSE),
a public-domain-style dedication. The repository does not identify an owner and
does not make ownership claims over the software.

Before using, copying, modifying, or relying on this repository, read the
[Disclaimer](https://github.com/gregsuniverse/datastore-pandas/blob/main/DISCLAIMER.md).
In short: this project is experimental, provided without warranty, not
recommended for any particular use, not guaranteed to be maintained, and includes
AI-generated or AI-assisted software and documentation.

Release history is documented in the
[Changelog](https://github.com/gregsuniverse/datastore-pandas/blob/main/CHANGELOG.md).

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

The base package depends on `pandas` and `google-cloud-datastore`. The Polars
adapter is optional and is installed with the `polars` extra.

Minimum supported third-party versions:

| Package | Minimum | Notes |
|---|---:|---|
| Python | `3.10` | Earlier Python versions are not supported. |
| `google-cloud-datastore` | `2.20.0` | Required for all Datastore reads and writes. |
| `pandas` | `2.0.0` | Required by the base package and timestamp conversion layer. |
| `polars` | `1.0.0` | Optional; required only for `datastore_pandas.polars`. |
| `pytest` | `8.0.0` | Test/dev extra only. |
| `ruff` | `0.8.0` | Test/dev extra only. |

The current local verification pass used Python 3.11 with
`google-cloud-datastore 2.24.0`, `pandas 3.0.3`, `polars 1.40.1`, `pytest 9.0.3`,
and `ruff 0.15.14`. For repeatable production or CI runs, pin exact dependency
versions in your application environment.

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

For policy-aware writes, opt into planning:

```python
plan = dsp.plan_datastore_write(
    df,
    schema=schema,
    skip_unchanged=True,
)

dry_report = dsp.to_datastore(df, schema=schema, dry_run=True)
report = dsp.to_datastore(df, schema=schema, skip_unchanged=True)
```

`skip_unchanged=True` reads existing entities before writing. Full writes compare
the effective replacement payload, so extra existing properties still count as a
change. Patch writes compare only the patched properties.

`insert` and `update` modes are represented in the API, but full correctness for
those modes depends on the active Datastore batch backend exposing insert/update
methods. `upsert` is the safest path in the initial scaffold.

## Instantiated Kind Accessors

`dsp.kind(...)` creates a bound accessor for one schema/kind. It is not a
DataFrame subclass; it is a scoped Datastore accessor that returns and accepts
pandas or Polars DataFrames.

```python
workouts = dsp.kind(
    schema=schema,
    client=client,
    namespace="tenant-a",
    filters=[("activity_type", "=", "run")],
    backend="pandas",
    read_only=False,
)

df = workouts.read(limit=1000)
plan = workouts.plan_write(df, skip_unchanged=True)
report = workouts.write(df, skip_unchanged=True)
```

Bound accessors can carry an ancestor scope:

```python
user_workouts = workouts.with_ancestor(
    dsp.DatastoreKey(namespace="tenant-a", path=(("User", "sample-user"),))
)
```

When an ancestor is bound, writes are validated so generated keys must stay under
that ancestor path and namespace.

For deterministic keys, `key_policy` is a convenience wrapper around `KeySpec`:

```python
schema = dsp.Schema(
    kind="Workout",
    key=dsp.key_policy(
        "Workout",
        id_field="workout_id",
        namespace_field="tenant",
        ancestors=[("User", "user_id")],
    ),
    properties={...},
)
```

Accessors can also apply custom audit timestamp fields before planning/writing:

```python
workouts = dsp.kind(
    schema=schema,
    client=client,
    audit=dsp.AuditPolicy(
        created_at="created_at",
        updated_at="updated_at",
        imported_at="imported_at",
    ),
)
```

Logical duplicate cleanup is explicit and dry-run by default:

```python
plan = workouts.plan_duplicate_cleanup(
    by=["external_id"],
    order=["-updated_at"],
)

dry_report = workouts.cleanup_duplicates(
    by=["external_id"],
    order=["-updated_at"],
)

delete_report = workouts.cleanup_duplicates(
    by=["external_id"],
    order=["-updated_at"],
    dry_run=False,
)
```

This cleanup is for logical duplicates by property values. Datastore cannot store
two entities with the same exact key.

## DataFrame Models

`dsp.dspdf(...)` is a second layer over `dsp.kind(...)`. It owns a current
DataFrame and retains source context:

```python
events = dsp.dspdf(
    kind="Workout",
    schema=schema,
    client=client,
    backend="pandas",
    namespace="tenant-a",
    keep_original=True,
)

events = events.load()
events.df["duration_sec"] = events.df["duration_sec"] + 60
report = events.write(skip_unchanged=True)
```

Derived or aggregated frames cannot write back to the source kind by accident:

```python
summary = events.aggregate(
    by=["user_id"],
    metrics={"duration_sec": "sum"},
    target_schema=summary_schema,
)

summary.write_to(schema=summary_schema)
```

If a transformed DataFrame drops source keys, changes row shape, or is explicitly
marked as derived, use `with_target(...)` or `write_to(...)` with a target schema.

## Schema Inference

Datastore entities are typed, but a kind can still contain sparse and mixed-shape
entities. The package can infer a conservative schema from sampled entities or a
DataFrame:

```python
report = dsp.infer_schema(kind="Event", client=client, sample_size=1000)
schema = report.schema
print(report.mixed_fields)

events = dsp.dspdf(kind="Event", client=client, infer_schema=True).load()
```

If one property has different value types across entities, the default
`mixed_type_policy="object"` uses a pass-through field type and records the
observed variants in the inference report. Use `mixed_type_policy="error"` to
fail on mixed types, or `"string"` to coerce mixed values on writes.

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
- `policy_examples.py`: validates the instantiated accessor, dry-run/read-only
  write policies, skip-unchanged writes, audit fields, bound ancestor validation,
  and logical duplicate cleanup against the emulator
- `dataframe_model_examples.py`: validates `dspdf(...)` source context,
  row-preserving write-back, derived aggregate target writes, and schema
  inference for mixed property types
- `inspect_sparse_entities.py`: inspects raw entities to confirm sparse properties
  are omitted
- `index_planning.py`: prints index suggestions
- `reset_emulator_data.py`: clears sample entities from the emulator
- `public_divvy_ancestor_test.py`: downloads public Divvy bike-share trip data,
  loads `Dataset -> Station -> Ride` ancestor paths, and validates ancestor,
  projection, and keys-only queries
- `large_linked_dataset.py`: loads a synthetic linked-kind dataset with
  `Tenant -> LinkedUser -> LinkedSession -> LinkedEvent` ancestor paths and
  `KeyType` references across `LinkedUser`, `LinkedDevice`, `LinkedSession`, and
  `LinkedEvent`; the default local run uses 200,000 events, while
  `--events 1000001` is available as a host-memory stress test

The main examples accept `--backend pandas` or `--backend polars`.

Full instructions are in the
[emulator examples README](https://github.com/gregsuniverse/datastore-pandas/blob/main/examples/emulator/README.md).

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
- Use a SQL or analytical database for analytics, joins, and broad scans.

## Repository Layout

```text
src/datastore_pandas/
  accessor.py      instantiated kind accessor API
  audit.py         custom audit timestamp policies
  batches.py       batch planning and duplicate-key checks
  convert.py       row/entity conversion
  errors.py        package exceptions
  inference.py     schema inference and mixed-type reports
  io.py            read_datastore, iter_datastore, to_datastore, patch_datastore
  keys.py          DatastoreKey, KeySpec, KeyPart
  model.py         dspdf DataFrame-owning model layer
  planning.py      dry-run, read-only, and skip-unchanged write planning
  polars.py        optional Polars adapter
  query.py         QuerySpec and index planning
  reports.py       write result reporting
  schema.py        Schema and Field
  transaction.py   transaction context manager
  types.py         Datastore type converters

examples/
  basic_usage.py
  instantiated_accessor.py
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

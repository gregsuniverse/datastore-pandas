# Changelog

All notable changes to this project are documented here.

This project does not make a promise of compatibility, maintenance, support, or
future releases. Version entries describe repository history only.

## Unreleased

### Added

- Added write planning primitives with dry-run reports, read-only write blocking,
  skip-unchanged full writes, and skip-unchanged patch writes.
- Added `dsp.kind(...)` / `DatastoreFrame`, an instantiated kind accessor that
  binds schema, client, backend, namespace, ancestor, query defaults, read-only
  state, batch size, and worker defaults.
- Added pandas and Polars parity for write planning, dry-run writes, read-only
  writes, and skip-unchanged writes.
- Added `key_policy(...)` as a deterministic per-kind key helper over `KeySpec`.
- Added `AuditPolicy` for custom `created_at`, `updated_at`, and `imported_at`
  field population before planning/writing.
- Added bound-ancestor write validation for instantiated accessors.
- Added logical duplicate cleanup planning and explicit delete execution through
  the instantiated accessor API.
- Added examples and unit tests for the instantiated accessor and write policies.
- Added an emulator-backed policy example covering instantiated accessors,
  dry-run/read-only writes, skip-unchanged writes, audit fields, bound ancestor
  validation, and duplicate cleanup with pandas and Polars.
- Added `dspdf(...)`, a DataFrame-owning model layer that retains source context,
  optional original snapshots, derived-frame lineage, and alternate write targets.
- Added derived aggregate helpers and `write_to(...)` / `plan_write_to(...)` for
  writing transformed DataFrames to explicit target kinds.
- Added schema inference from Datastore entities, DataFrames, or records,
  including mixed-type field reporting and configurable mixed-type policies.
- Added an emulator-backed model example covering source write-back,
  derived-write blocking, aggregate summary writes, and mixed-type schema
  inference with pandas and Polars.
- Added a focused emulator-backed edge-case example covering no-client dry-run
  and read-only planning, clear write reports, deterministic keys, bound
  ancestors, audit timestamp aliases, retry/batch behavior, injectable fake
  clients, skip-unchanged writes, duplicate cleanup, derived model targets, and
  mixed-type schema inference.

### Changed

- Patch write key matching now compares namespace plus key path so package keys
  and client keys still match when the client key includes a project partition.

## 0.2.0

### Added

- Added optional Polars support through `datastore-pandas[polars]` and the
  `datastore_pandas.polars` adapter module.
- Added Polars implementations for `read_datastore`, `iter_datastore`,
  `to_datastore`, and `patch_datastore`.
- Added `--backend pandas|polars` options to the main examples and emulator
  examples.
- Added Polars coverage for the public Divvy ancestor test.
- Added unit tests for Polars row iteration and sparse missing-value omission.
- Added `examples/emulator/large_linked_dataset.py`, a synthetic linked-kind
  emulator scenario that writes 200,000 `LinkedEvent` rows by default and can be
  configured past 1,000,000 `LinkedEvent` rows for host-memory stress testing.
- Added cross-kind `KeyType` relationship coverage in the large linked dataset:
  `LinkedDevice.assigned_user_key`, `LinkedSession.user_key`,
  `LinkedSession.device_key`, `LinkedEvent.user_key`,
  `LinkedEvent.session_key`, and `LinkedEvent.device_key`.
- Added unit coverage for converting package `DatastoreKey` values into
  `google-cloud-datastore` client key values for writes and query filters.
- Added `ruff` to the test/development dependency extra.

### Changed

- Updated package metadata from a pandas-only description to a DataFrame
  interface with pandas and optional Polars support.
- Updated emulator output helpers to render Polars tables with ASCII-safe output
  on Windows consoles.
- Aligned pandas `patch_datastore` key validation with the Polars adapter by
  requiring complete keys.
- Large linked-kind emulator load defaults use conservative batch and worker
  settings for local emulator stability.
- Large linked-kind event rows keep only relationship and event-type fields
  indexed by default to reduce local emulator index pressure.
- Upsert and patch commits now retry a small number of transient commit failures
  such as deadline, unavailable, and GOAWAY errors.
- The emulator Docker Compose configuration now allocates a larger Java heap for
  high-volume local stress tests.
- The large linked-kind example documents that 200,000 events were verified
  locally, while 1,000,001 events exceeded the local Firestore emulator heap.
- Query filter construction now uses the `PropertyFilter` API when available,
  with a fallback for simpler test doubles.
- `KeyType` values are converted recursively before writes and query filters,
  so package-native `DatastoreKey` objects can be used in key-reference
  properties.
- The top-level README now uses fully qualified GitHub links for files that are
  also displayed on PyPI.
- The README describes the third-party minimum and locally verified dependency
  versions.
- The README no longer references a specific analytics database product.
- Emulator reset now includes the linked-kind example entity kinds and
  namespaces.

## 0.1.0

### Added

- Initial package scaffold for a schema-aware pandas interface to Firestore in
  Datastore mode.
- Added schema objects, typed field conversion, sparse missing-value handling,
  key management, query construction, batched writes, patch updates, transactions,
  and index-planning helpers.
- Added Datastore emulator Docker configuration and examples covering synthetic
  sparse workout data, public Divvy ancestor data, projections, keys-only
  queries, patching, transactions, sparse-entity inspection, and index planning.
- Added README, disclaimer, license, and PyPI publishing workflow.

# Changelog

All notable changes to this project are documented here.

This project does not make a promise of compatibility, maintenance, support, or
future releases. Version entries describe repository history only.

## Unreleased

### Added

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

### Changed

- Updated package metadata from a pandas-only description to a DataFrame
  interface with pandas and optional Polars support.
- Updated emulator output helpers to render Polars tables with ASCII-safe output
  on Windows consoles.
- Aligned pandas `patch_datastore` key validation with the Polars adapter by
  requiring complete keys.

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

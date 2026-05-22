"""Load generated workout data into the Datastore emulator."""

from __future__ import annotations

import argparse
from pathlib import Path
from time import perf_counter

from common import (
    BACKENDS,
    Backend,
    DEFAULT_DATA_FILE,
    WORKOUT_SCHEMA,
    adapter,
    client,
    frame_head,
    frame_len,
    read_workout_csv,
)


def load(
    path: Path,
    *,
    workers: int,
    batch_size: int,
    limit: int | None = None,
    backend: Backend = "pandas",
):
    df = read_workout_csv(path, backend=backend)
    if limit is not None:
        df = frame_head(df, limit)

    started = perf_counter()
    report = adapter(backend).to_datastore(
        df,
        schema=WORKOUT_SCHEMA,
        client=client(),
        mode="upsert",
        batch_size=batch_size,
        max_workers=workers,
    )
    elapsed = perf_counter() - started
    print(
        f"loaded backend={backend} rows={frame_len(df):,} "
        f"succeeded={report.succeeded:,} failed={report.failed:,} elapsed={elapsed:.2f}s"
    )
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_DATA_FILE)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--batch-size", type=int, default=400)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--backend", choices=BACKENDS, default="pandas")
    args = parser.parse_args()

    report = load(
        args.input,
        workers=args.workers,
        batch_size=args.batch_size,
        limit=args.limit,
        backend=args.backend,
    )
    report.raise_for_errors()


if __name__ == "__main__":
    main()

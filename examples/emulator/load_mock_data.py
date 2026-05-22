"""Load generated workout data into the Datastore emulator."""

from __future__ import annotations

import argparse
from pathlib import Path
from time import perf_counter

from common import DEFAULT_DATA_FILE, WORKOUT_SCHEMA, client, read_workout_csv
import datastore_pandas as dsp


def load(path: Path, *, workers: int, batch_size: int, limit: int | None = None) -> dsp.WriteReport:
    df = read_workout_csv(path)
    if limit is not None:
        df = df.head(limit)

    started = perf_counter()
    report = dsp.to_datastore(
        df,
        schema=WORKOUT_SCHEMA,
        client=client(),
        mode="upsert",
        batch_size=batch_size,
        max_workers=workers,
    )
    elapsed = perf_counter() - started
    print(
        f"loaded rows={len(df):,} succeeded={report.succeeded:,} "
        f"failed={report.failed:,} elapsed={elapsed:.2f}s"
    )
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_DATA_FILE)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--batch-size", type=int, default=400)
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()

    report = load(args.input, workers=args.workers, batch_size=args.batch_size, limit=args.limit)
    report.raise_for_errors()


if __name__ == "__main__":
    main()

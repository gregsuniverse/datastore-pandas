"""Generate heterogeneous workout data for Datastore emulator examples."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import random
from pathlib import Path

from common import BACKENDS, Backend, DEFAULT_DATA_FILE, frame_from_records, frame_len, write_csv


ACTIVITY_TYPES = ("swim", "bike", "run")
STROKES = ("free", "back", "breast", "fly", "mixed")
SHOES = ("Tempo 4", "Daily Max", "Carbon Racer", "Trail Grip")


def generate_workouts(rows: int, *, users: int, seed: int, backend: Backend = "pandas"):
    rng = random.Random(seed)
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    records = []

    for index in range(rows):
        user_index = index % users
        user_id = f"user-{user_index:05d}"
        activity_type = rng.choices(ACTIVITY_TYPES, weights=[0.25, 0.35, 0.40])[0]
        started_at = start + timedelta(minutes=index * 37 + rng.randint(0, 30))
        duration_sec = _duration(activity_type, rng)
        distance_m = _distance(activity_type, duration_sec, rng)
        record = {
            "tenant": "tenant-a" if user_index % 2 == 0 else "tenant-b",
            "user_id": user_id,
            "workout_id": f"{started_at:%Y%m%d%H%M}-{activity_type}-{index:08d}",
            "started_at": started_at.isoformat(),
            "duration_sec": duration_sec,
            "distance_m": round(distance_m, 2),
            "activity_type": activity_type,
            "pool_length_m": None,
            "stroke": None,
            "bike_power_w": None,
            "bike_trainer": None,
            "run_cadence_spm": None,
            "shoe_model": None,
            "notes": None,
            "last_reviewed_at": None,
        }

        if activity_type == "swim":
            record["pool_length_m"] = rng.choice([25, 50])
            record["stroke"] = rng.choice(STROKES)
        elif activity_type == "bike":
            record["bike_power_w"] = rng.randint(120, 340)
            record["bike_trainer"] = rng.random() < 0.3
        else:
            record["run_cadence_spm"] = rng.randint(156, 190)
            record["shoe_model"] = rng.choice(SHOES)

        if rng.random() < 0.08:
            record["notes"] = (
                f"{activity_type} note for {user_id}; this free text is intentionally unindexed."
            )

        records.append(record)

    return frame_from_records(records, backend)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=50_000)
    parser.add_argument("--users", type=int, default=1_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out", type=Path, default=DEFAULT_DATA_FILE)
    parser.add_argument("--backend", choices=BACKENDS, default="pandas")
    args = parser.parse_args()

    args.out.parent.mkdir(parents=True, exist_ok=True)
    df = generate_workouts(args.rows, users=args.users, seed=args.seed, backend=args.backend)
    write_csv(df, args.out)
    print(f"wrote {frame_len(df):,} sparse workout rows to {args.out} using {args.backend}")
    print(df.head(5))


def _duration(activity_type: str, rng: random.Random) -> int:
    if activity_type == "swim":
        return rng.randint(1_200, 5_400)
    if activity_type == "bike":
        return rng.randint(1_800, 14_400)
    return rng.randint(900, 8_000)


def _distance(activity_type: str, duration_sec: int, rng: random.Random) -> float:
    if activity_type == "swim":
        return rng.choice([800, 1_000, 1_500, 2_000, 3_000, 4_000])
    if activity_type == "bike":
        return duration_sec * rng.uniform(5.5, 11.0)
    return duration_sec * rng.uniform(2.2, 4.8)


if __name__ == "__main__":
    main()

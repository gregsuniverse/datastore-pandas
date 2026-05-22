"""Transaction example: increment a counter with Datastore ACID semantics."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone

import pandas as pd

from common import COUNTER_SCHEMA, client, counter_key
import datastore_pandas as dsp


def run(*, tenant: str, counter_name: str, increments: int) -> None:
    ds = client()
    key = counter_key(tenant, counter_name)

    seed = pd.DataFrame(
        [{"__key__": key, "value": 0, "updated_at": datetime.now(timezone.utc)}]
    )
    dsp.to_datastore(seed, schema=COUNTER_SCHEMA, client=ds, mode="upsert").raise_for_errors()

    for _ in range(increments):
        with dsp.Transaction(ds) as tx:
            row = tx.get(key, schema=COUNTER_SCHEMA, include_key=True)
            if row is None:
                row = {"__key__": key, "value": 0}
            row["value"] += 1
            row["updated_at"] = datetime.now(timezone.utc)
            tx.put(row, schema=COUNTER_SCHEMA)

    final = ds.get(key.to_client_key(ds))
    print(f"counter {tenant}/{counter_name} value={final['value']}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant", default="tenant-a")
    parser.add_argument("--counter-name", default="example-counter")
    parser.add_argument("--increments", type=int, default=5)
    args = parser.parse_args()
    run(tenant=args.tenant, counter_name=args.counter_name, increments=args.increments)


if __name__ == "__main__":
    main()


"""Delete sample entities from the Datastore emulator."""

from __future__ import annotations

import argparse

from common import client


def reset(*, batch_size: int, namespaces: list[str]) -> None:
    ds = client()
    total = 0
    for namespace in namespaces:
        for kind in [
            "Workout",
            "Counter",
            "Station",
            "Ride",
            "LinkedUser",
            "LinkedDevice",
            "LinkedSession",
            "LinkedEvent",
            "PolicyEvent",
            "ModelEvent",
            "ModelSummary",
            "InferredEvent",
            "EdgeCaseEvent",
            "EdgeCaseSummary",
            "EdgeCaseInferred",
        ]:
            query = ds.query(kind=kind, namespace=None if namespace == "<default>" else namespace)
            query.keys_only()
            chunk = []
            deleted = 0
            for entity in query.fetch():
                chunk.append(entity.key)
                if len(chunk) >= batch_size:
                    ds.delete_multi(chunk)
                    deleted += len(chunk)
                    chunk = []
            if chunk:
                ds.delete_multi(chunk)
                deleted += len(chunk)
            total += deleted
            print(f"deleted namespace={namespace} kind={kind} count={deleted:,}")
    print(f"deleted total={total:,}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=400)
    parser.add_argument("--namespace", action="append", dest="namespaces")
    args = parser.parse_args()
    reset(
        batch_size=args.batch_size,
        namespaces=args.namespaces
        or [
            "<default>",
            "tenant-a",
            "tenant-b",
            "tenant-a-policy",
            "tenant-a-model",
            "tenant-a-edge",
            "divvy-public",
            "linked-large",
            "policy-tenant",
            "model-tenant",
            "edge-tenant",
        ],
    )


if __name__ == "__main__":
    main()

"""Datastore query descriptions and validation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Sequence

from datastore_pandas.convert import to_client_datastore_value
from datastore_pandas.errors import QueryValidationError
from datastore_pandas.keys import DatastoreKey

FilterOp = Literal["=", "<", "<=", ">", ">=", "!=", "IN", "NOT_IN"]
ReadConsistency = Literal["strong", "eventual"]


@dataclass(frozen=True)
class QuerySpec:
    kind: str
    namespace: str | None = None
    filters: Sequence[tuple[str, FilterOp, Any]] = field(default_factory=list)
    order: Sequence[str] = field(default_factory=list)
    projection: Sequence[str] | None = None
    distinct_on: Sequence[str] | None = None
    ancestor: DatastoreKey | None = None
    keys_only: bool = False
    limit: int | None = None
    cursor: bytes | str | None = None
    consistency: ReadConsistency | None = None

    def validate(self) -> None:
        if (
            self.namespace is not None
            and self.ancestor is not None
            and self.ancestor.namespace != self.namespace
        ):
            raise QueryValidationError("Query namespace must match the ancestor key namespace.")
        if self.keys_only and self.projection:
            raise QueryValidationError("keys_only and projection cannot be used together.")
        inequality_props = {
            name for name, op, _ in self.filters if op in {"<", "<=", ">", ">=", "!=", "NOT_IN"}
        }
        if self.order and inequality_props:
            first_order = self.order[0].removeprefix("-")
            if first_order not in inequality_props:
                raise QueryValidationError(
                    "Datastore inequality filters require the first sort order to match "
                    "an inequality property."
                )
        if self.distinct_on and self.order:
            ordered_props = [item.removeprefix("-") for item in self.order]
            distinct = list(self.distinct_on)
            if ordered_props[: len(distinct)] != distinct:
                raise QueryValidationError(
                    "distinct_on properties must appear first in the query order."
                )
        equality_props = {name for name, op, _ in self.filters if op == "="}
        projected_equalities = equality_props & set(self.projection or [])
        if projected_equalities:
            raise QueryValidationError(
                "Datastore projection queries cannot project equality-filtered properties: "
                + ", ".join(sorted(projected_equalities))
            )

    def build(self, client: Any):
        self.validate()
        query = client.query(
            kind=self.kind,
            namespace=self.namespace if self.namespace is not None else self._inferred_namespace(),
            ancestor=self.ancestor.to_client_key(client) if self.ancestor else None,
            projection=tuple(self.projection or ()),
            order=tuple(self.order),
            distinct_on=tuple(self.distinct_on or ()),
        )
        for name, op, value in self.filters:
            _add_filter(query, name, op, to_client_datastore_value(value, client))
        if self.keys_only:
            query.keys_only()
        return query

    def _inferred_namespace(self) -> str | None:
        if self.ancestor is not None:
            return self.ancestor.namespace
        return None


@dataclass(frozen=True)
class IndexSuggestion:
    kind: str
    properties: tuple[tuple[str, Literal["asc", "desc"]], ...]
    ancestor: bool = False

    def to_index_yaml(self) -> str:
        lines = [
            "- kind: " + self.kind,
            "  ancestor: " + ("yes" if self.ancestor else "no"),
            "  properties:",
        ]
        for name, direction in self.properties:
            lines.append(f"  - name: {name}")
            lines.append(f"    direction: {direction}")
        return "\n".join(lines)


@dataclass(frozen=True)
class QueryPlan:
    query: QuerySpec
    needs_composite_index: bool
    suggestions: tuple[IndexSuggestion, ...] = ()
    warnings: tuple[str, ...] = ()


def plan_indexes(query: QuerySpec) -> QueryPlan:
    """Produce conservative index guidance for a Datastore query.

    This is not a replacement for Datastore Query Explain. It catches common
    composite-index shapes early and can generate an `index.yaml` starting point.
    """

    query.validate()
    warnings: list[str] = []
    props: list[tuple[str, Literal["asc", "desc"]]] = []

    equality_props = [name for name, op, _ in query.filters if op == "="]
    range_props = [name for name, op, _ in query.filters if op != "="]
    ordered_props = [
        (item.removeprefix("-"), "desc" if item.startswith("-") else "asc") for item in query.order
    ]

    if query.projection:
        for name in query.projection:
            if name not in equality_props and name not in {prop for prop, _ in ordered_props}:
                props.append((name, "asc"))

    for name in equality_props:
        if name not in {prop for prop, _ in props}:
            props.append((name, "asc"))
    for name in range_props:
        if name not in {prop for prop, _ in props}:
            props.append((name, "asc"))
    for name, direction in ordered_props:
        props = [(prop, dir_) for prop, dir_ in props if prop != name]
        props.append((name, direction))  # type: ignore[arg-type]

    needs_composite = (
        len(set(equality_props + range_props + [prop for prop, _ in ordered_props])) > 1
        or bool(query.projection and (query.filters or query.order or query.distinct_on))
        or bool(query.distinct_on)
    )

    if query.keys_only:
        warnings.append("keys_only queries usually do not need a projection/composite index plan.")
    if any(
        name.endswith("_at") or name in {"created", "updated", "timestamp"}
        for name, _, _ in query.filters
    ):
        warnings.append(
            "Monotonic timestamp indexes can hotspot under high write rates; shard or exempt when possible."
        )

    suggestions = ()
    if needs_composite and props:
        suggestions = (
            IndexSuggestion(query.kind, tuple(props), ancestor=query.ancestor is not None),
        )
    return QueryPlan(query, needs_composite, suggestions, tuple(warnings))


def _add_filter(query: Any, name: str, op: FilterOp, value: Any) -> None:
    try:
        from google.cloud.datastore.query import PropertyFilter

        query.add_filter(filter=PropertyFilter(name, op, value))
    except TypeError:
        query.add_filter(name, op, value)

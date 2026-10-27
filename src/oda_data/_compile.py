"""Compile a ``Sum`` indicator and a resolved call into DuckDB relations.

The three stages follow ``FilterStage``, ``ConvertStage`` and
``AggregateStage`` in ``_model``:

- ``filtered`` reads one table view and applies the binding's, the
  indicator's, the basis's and the call's filters, and leaves out rows with
  no value on the basis. Each output row keeps the table's row grain.
- ``converted`` joins every filtered row to its provider-year factor, one row
  per unit, before any sum, because each provider has its own deflator.
- ``aggregated`` sums the converted rows by the call's ``by`` dimensions plus
  ``basis``, ``line``, ``flow`` and the unit columns, which always split a
  result.

Groups, totals, ``allow_partial``, ``Ratio``, ``Derived`` and ``Method`` belong
to ``_combine``, which builds on these stages. ``filtered`` keeps the
``provider`` column and the dimension of every group in the call, so
``_combine`` can sum group totals from stage 2.

Broad sectors come from a relation named ``broad_sectors`` (see
``BROAD_SECTORS``) that the caller registers on the cursor, with columns
``sector`` (BIGINT) and ``broad_sector`` (VARCHAR). It is read only when a
call groups or filters by ``broad_sector``.

A dimension whose column has ``labels`` in ``VOCABULARY``, such as a
policy-marker score, shows the labels in place of the codes. A code with no
label fails the query with a message holding ``UNLABELLED_CODE``.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

import pyarrow as pa

from oda_data._errors import (
    BasisRangeError,
    DimensionError,
    MissingFactorError,
    UnsupportedQueryError,
)
from oda_data._factors import FACTOR_SCHEMA
from oda_data._model import (
    VOCABULARY,
    AnyOf,
    Between,
    Flow,
    HasTag,
    Headline,
    In,
    IsNull,
    Line,
    NotIn,
    NotNull,
    Sum,
    Value,
)
from oda_data._names import PROVIDER_AGGREGATE_FROM

if TYPE_CHECKING:
    from collections.abc import Iterable

    from duckdb import DuckDBPyConnection, DuckDBPyRelation

    from oda_data._model import (
        BasisRule,
        Binding,
        Factors,
        Filter,
        Indicator,
        ResolvedCall,
    )
    from oda_data._types import Basis, Code, Dimension, TableName
    from oda_data._units import Unit

#: The relation mapping sector codes to broad sectors: ``sector``,
#: ``broad_sector``. Sector codes it lacks get a null broad sector.
BROAD_SECTORS = "broad_sectors"

UNIT_COLUMNS: tuple[str, ...] = ("currency", "prices", "base_year")

#: The words of the query error a labelled column raises on a code with no
#: label. ``_context`` turns that error into ``UnknownEntityError``.
UNLABELLED_CODE = "holds a code with no label"

#: Columns that split every result they appear in, so no row adds one basis,
#: line, flow or unit to another.
SPLIT_COLUMNS: tuple[str, ...] = ("basis", "line", "flow", *UNIT_COLUMNS)


# Stage 1 ---------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Segment:
    """One concrete basis over a span of years, with the line it sums."""

    basis: Basis
    rule: Value | Flow | Line
    line: int | None
    start: int | None  # first year, inclusive
    stop: int | None  # first year after the span


def filtered(
    cursor: DuckDBPyConnection,
    indicator: Indicator,
    call: ResolvedCall,
    /,
    *,
    columns: tuple[str, ...] = (),
) -> DuckDBPyRelation:
    """Return an indicator's filtered rows at the table's row grain.

    Columns: ``provider``, every other dimension in ``call.by`` and in
    ``call.groups``, ``year``, ``basis`` (the concrete basis of the row's
    year, so a headline call reads ``net_disbursement`` rows to 2017),
    ``line`` when ``line`` is an indicator dimension, ``flow`` when the call
    filters flows, ``value`` in current USD millions, then ``columns``.

    Args:
        cursor: A cursor holding the table's view, and ``broad_sectors`` when
            the call uses that dimension.
        indicator: A ``Sum`` indicator.
        call: The resolved call.
        columns: Extra view columns for ``records()``, by logical or raw name.

    Returns:
        A lazy relation on ``cursor``.

    Raises:
        UnsupportedQueryError: The indicator is not a ``Sum``, lacks the
            basis, has no line for a basis, or the call filters a DAC1 flow
            the basis does not split into.
        DimensionError: The call groups or filters by a dimension the
            indicator lacks.
        BasisRangeError: The call asks for years before the basis starts.
    """
    if not isinstance(indicator.kind, Sum):
        raise UnsupportedQueryError(
            f"{indicator.id} is a {type(indicator.kind).__name__} indicator; "
            "the compiler sums Sum indicators only"
        )
    check_dimensions(indicator, call)
    segments = _segments(indicator, call)

    selects = [_select(indicator, call, segment, columns) for segment in segments]
    return cursor.sql("\nUNION ALL\n".join(selects))


def check_basis(indicator: Indicator, call: ResolvedCall, /) -> None:
    """Check that the indicator has the call's basis over the call's years.

    ``_combine`` runs this for ``Method`` indicators, whose rows come from the
    indicator's own function.

    Raises:
        UnsupportedQueryError: The binding lacks the basis.
        BasisRangeError: The call asks for years before the basis starts.
    """
    _segments(indicator, call)


def basis_sql(indicator: Indicator, call: ResolvedCall, /, *, year: str) -> str:
    """SQL for the concrete basis of a row in the year held by ``year``.

    A headline call gives ``net_disbursement`` before the switch year and
    ``grant_equivalent`` from it; any other call gives its own basis.
    """
    segments = _segments(indicator, call)
    *earlier, last = segments
    if not earlier:
        return literal(last.basis)
    cases = " ".join(
        f"WHEN {year} < {segment.stop} THEN {literal(segment.basis)}"
        for segment in earlier
    )
    return f"CASE {cases} ELSE {literal(last.basis)} END"


def check_dimensions(indicator: Indicator, call: ResolvedCall, /) -> None:
    """Raise ``DimensionError`` when the call groups, filters or totals by a
    dimension the indicator lacks."""
    asked = [*call.by, *call.members, *(group.dimension for group in call.groups)]
    for dimension in dict.fromkeys(asked):
        if dimension not in indicator.dimensions:
            raise DimensionError(
                f"{indicator.id} has no {dimension!r} dimension; its dimensions "
                f"are {', '.join(indicator.dimensions)}"
            )


def _segments(indicator: Indicator, call: ResolvedCall) -> list[_Segment]:
    """Split the call's basis into concrete rules over spans of years."""
    bases = indicator.binding.bases
    rule = bases.get(call.basis)
    if rule is None:
        raise UnsupportedQueryError(
            f"{indicator.id} has no {call.basis!r} basis; its bases are "
            f"{', '.join(bases)}"
        )

    lines = indicator.kind.lines if isinstance(indicator.kind, Sum) else {}
    segments = []
    for name, start, stop in basis_spans(call.basis, rule):
        concrete = bases[name]
        assert not isinstance(concrete, Headline)  # Binding forbids nesting
        _check_range(indicator, call, concrete.from_year, start, stop)
        _check_flows(indicator, call, name, concrete)
        _check_basis_rows(indicator, call, name, concrete)
        if lines and name not in lines:
            raise UnsupportedQueryError(
                f"{indicator.id} has no line for the {name!r} basis"
            )
        first = max(
            (year for year in (start, concrete.from_year) if year is not None),
            default=None,
        )
        segments.append(_Segment(name, concrete, lines.get(name), first, stop))
    return segments


def basis_spans(
    basis: Basis, rule: BasisRule, /
) -> list[tuple[Basis, int | None, int | None]]:
    """Split a basis into the concrete bases it reads, each with its first
    year and the first year after it (``None`` when open).

    A headline gives its ``before`` basis to the switch year and its
    ``after`` basis from it; any other basis is itself in every year.
    """
    if isinstance(rule, Headline):
        return [(rule.before, None, rule.year), (rule.after, rule.year, None)]
    return [(basis, None, None)]


def _check_flows(
    indicator: Indicator, call: ResolvedCall, name: Basis, rule: Value | Flow | Line
) -> None:
    """Raise when the call filters flows a DAC1 flow basis does not split into."""
    flows = call.members.get("flow")
    if flows is None or not isinstance(rule, Flow):
        return
    stray = sorted(flows - set(rule.parts), key=str)
    if not stray:
        return
    split = [
        f"{basis!r} ({', '.join(map(str, other.parts))})"
        for basis, other in indicator.binding.bases.items()
        if isinstance(other, Flow) and other.parts
    ]
    hint = f"; flows= picks among the parts of {', '.join(split)}" if split else ""
    raise UnsupportedQueryError(
        f"{indicator.id} on the {name!r} basis reads flow {rule.code}, which "
        f"flows {stray} do not split{hint}"
    )


def _check_basis_rows(
    indicator: Indicator, call: ResolvedCall, name: Basis, rule: Value | Flow | Line
) -> None:
    """Raise when the call names a code that the basis's own row filters leave
    out, such as gross line 240 on the net basis of ``oecd.dac2a.lines``.

    The message names the bases that keep the code.
    """
    if not isinstance(rule, Value):
        return
    dimensions = {column: d for d, column in indicator.binding.dimensions.items()}
    for item in rule.rows:
        if not isinstance(item, In | NotIn) or item.column not in dimensions:
            continue
        dimension = dimensions[item.column]
        dropped = sorted(
            (c for c in call.members.get(dimension, ()) if not _keeps(item, c)),
            key=str,
        )
        if not dropped:
            continue
        holders = [
            repr(other)
            for other, held in indicator.binding.bases.items()
            if isinstance(held, Value)
            and all(
                _keeps(f, c)
                for f in held.rows
                if isinstance(f, In | NotIn) and f.column == item.column
                for c in dropped
            )
        ]
        hint = f"; the {' and '.join(holders)} basis reads it" if holders else ""
        listed = ", ".join(map(str, dropped))
        raise UnsupportedQueryError(
            f"{indicator.id} on the {name!r} basis reads no {dimension} {listed}{hint}"
        )


def _keeps(item: In | NotIn, code: Code) -> bool:
    """Whether an ``In`` or an unscoped ``NotIn`` keeps rows holding ``code``."""
    return (code in item.values) == isinstance(item, In)


def _check_range(
    indicator: Indicator,
    call: ResolvedCall,
    from_year: int | None,
    start: int | None,
    stop: int | None,
) -> None:
    """Raise when the call asks for years in a span the rule does not cover.

    A call with no years reads every year the basis covers.
    """
    if from_year is None or call.years is None:
        return
    early = [
        year
        for year in call.years
        if year < from_year
        and (start is None or year >= start)
        and (stop is None or year < stop)
    ]
    if not early:
        return
    covering = [
        name
        for name in indicator.binding.bases
        if name != call.basis
        and (_first_year(indicator.binding, name) or early[0]) <= early[0]
    ]
    hint = f"; basis {' or '.join(map(repr, covering))} covers them" if covering else ""
    raise BasisRangeError(
        f"{indicator.id} on the {call.basis!r} basis starts in {from_year}, "
        f"so it has no {_years(early)}{hint}"
    )


def _first_year(binding: Binding, name: Basis) -> int | None:
    rule = binding.bases[name]
    if isinstance(rule, Headline):
        return _first_year(binding, rule.before)
    return rule.from_year


def _years(years: list[int]) -> str:
    if len(years) == 1:
        return str(years[0])
    return f"{years[0]}-{years[-1]}"


def _select(
    indicator: Indicator,
    call: ResolvedCall,
    segment: _Segment,
    extra: tuple[str, ...],
) -> str:
    binding = indicator.binding
    dims = binding.dimensions
    line_column = dims.get("line", "line")

    outputs: dict[str, str] = {}
    grouped: Iterable[Dimension] = (
        "provider",
        *call.by,
        *(group.dimension for group in call.groups),
        *(["flow"] if "flow" in call.members else []),
    )
    for dimension in grouped:
        if dimension in {"year", "line"}:
            continue
        outputs[dimension] = (
            f"{BROAD_SECTORS}.broad_sector"
            if dimension == "broad_sector"
            else _labelled(binding.table, dims[dimension])
        )
    outputs["year"] = _col(dims["year"])
    outputs["basis"] = literal(segment.basis)
    if "line" in indicator.dimensions:
        outputs["line"] = _col(line_column)
    outputs["value"] = f"CAST({_value(segment.rule)} AS DOUBLE)"
    for name in extra:
        outputs.setdefault(name, _col(name))

    conditions = [
        *map(_condition, binding.rows),
        *map(_condition, indicator.rows),
        *_basis_conditions(call, segment, line_column),
        *_year_conditions(call, segment, dims["year"]),
        *_member_conditions(indicator, call, segment),
    ]

    sql = "SELECT " + ", ".join(
        f"{expr} AS {ident(name)}" for name, expr in outputs.items()
    )
    sql += f"\nFROM {ident(binding.table)} AS t"
    if "broad_sector" in call.by or "broad_sector" in call.members:
        sql += (
            f"\nLEFT JOIN {BROAD_SECTORS} "
            f"ON {BROAD_SECTORS}.sector = {_col(dims['broad_sector'])}"
        )
    if conditions:
        sql += "\nWHERE " + "\n  AND ".join(conditions)
    return sql


def _labelled(table: TableName, column: str) -> str:
    """SQL for a column's labels, or its codes when it has none.

    A code the column's labels lack raises when the query runs, so a new
    code in a release cannot pass as another or as null.
    """
    labels = next(
        (c.labels for c in VOCABULARY[table].columns if c.name == column), None
    )
    if labels is None:
        return _col(column)
    cases = " ".join(
        f"WHEN {_col(column)} IS NULL THEN {literal(label)}"
        if code is None
        else f"WHEN {_col(column)} = {literal(code)} THEN {literal(label)}"
        for code, label in labels.items()
    )
    unknown = literal(f"{table} column {column} {UNLABELLED_CODE}: ")
    return f"CASE {cases} ELSE error({unknown} || CAST({_col(column)} AS VARCHAR)) END"


def _value(rule: Value | Flow | Line) -> str:
    match rule:
        case Value(column=column, minus=None):
            return _col(column)
        case Value(column=column, minus=minus):
            return f"coalesce({_col(column)}, 0) - coalesce({_col(minus)}, 0)"
        case Flow() | Line():
            return _col("value")


def _has_value(rule: Value | Flow | Line) -> str:
    """The condition that a row holds a value on the basis.

    A row with none, such as a CRS commitment row on a disbursement basis,
    is left out, so a cell with no rows is absent from the result rather than
    null, which means a member that did not report.
    """
    match rule:
        case Value(column=column, minus=None):
            return f"{_col(column)} IS NOT NULL"
        case Value(column=column, minus=minus):
            return f"coalesce({_col(column)}, {_col(minus)}) IS NOT NULL"
        case Flow() | Line():
            return f"{_col('value')} IS NOT NULL"


def _basis_conditions(
    call: ResolvedCall, segment: _Segment, line_column: str
) -> list[str]:
    conditions = [_has_value(segment.rule)]
    match segment.rule:
        case Value(rows=rows):
            conditions += map(_condition, rows)
        case Flow() if "flow" in call.members:
            # ``_check_flows`` has kept the filter to the basis's parts.
            parts = tuple(sorted(call.members["flow"], key=str))
            conditions.append(_condition(In("flow", parts)))
        case Flow(code=code):
            conditions.append(f"{_col('flow')} = {code}")
        case Line(code=code):
            conditions.append(f"{_col('line')} = {code}")
    if segment.line is not None:
        conditions.append(f"{_col(line_column)} = {segment.line}")
    return conditions


def _year_conditions(call: ResolvedCall, segment: _Segment, column: str) -> list[str]:
    conditions = []
    if call.years is not None:
        conditions.append(_condition(In(column, call.years)))
    if segment.start is not None:
        conditions.append(f"{_col(column)} >= {segment.start}")
    if segment.stop is not None:
        conditions.append(f"{_col(column)} < {segment.stop}")
    return conditions


def _member_conditions(
    indicator: Indicator, call: ResolvedCall, segment: _Segment
) -> list[str]:
    dims = indicator.binding.dimensions
    conditions = []
    for dimension, codes in call.members.items():
        values = tuple(sorted(codes, key=str))
        if dimension == "flow" and isinstance(segment.rule, Flow):
            continue  # the basis reads the named flows
        if dimension == "broad_sector":
            conditions.append(
                f"{BROAD_SECTORS}.broad_sector IN ({literals(values)})"
                if values
                else "FALSE"
            )
        else:
            conditions.append(_condition(In(dims[dimension], values)))

    provider = dims["provider"]
    if "provider" not in call.members:
        conditions.append(f"{_col(provider)} < {PROVIDER_AGGREGATE_FROM}")
    return conditions


def _condition(item: Filter) -> str:
    if isinstance(item, NotIn) and item.when is not None:
        # The rule holds outside its scope, or the unscoped rule holds.
        outside = NotIn(item.when.column, item.when.values)
        item = AnyOf((outside, replace(item, when=None)))
    match item:
        case In(column=column, values=values):
            sql = f"{_col(column)} IN ({literals(values)})" if values else "FALSE"
        case NotIn(column=column, values=values) if values:
            sql = f"{_col(column)} NOT IN ({literals(values)})"
        case NotIn(column=column) | NotNull(column=column):
            sql = f"{_col(column)} IS NOT NULL"
        case IsNull(column=column):
            sql = f"{_col(column)} IS NULL"
        case Between(column=column, low=low, high=high):
            sql = f"{_col(column)} BETWEEN {int(low)} AND {int(high)}"
        case HasTag(column=column, tags=tags):
            listed = f"[{literals(tags)}]"
            sql = f"list_has_any(string_split({_col(column)}, ';'), {listed})"
        case AnyOf(filters=filters):
            either = " OR ".join(f"({_condition(f)})" for f in filters)
            sql = f"({either})" if filters else "FALSE"
        case _:
            raise TypeError(f"unknown row filter {item!r}")
    return sql


def ident(name: str) -> str:
    """Quote a SQL identifier."""
    return '"' + name.replace('"', '""') + '"'


def _col(name: str) -> str:
    return f"t.{ident(name)}"


def literal(value: Code) -> str:
    """Write a code as a SQL literal."""
    if isinstance(value, bool):
        raise TypeError(f"a code cannot be a bool, got {value!r}")
    if isinstance(value, int):
        return str(int(value))
    return "'" + value.replace("'", "''") + "'"


def literals(values: Iterable[Code]) -> str:
    """Write codes as a comma-separated list of SQL literals."""
    return ", ".join(map(literal, values))


# Stage 2 ---------------------------------------------------------------------


def converted(
    cursor: DuckDBPyConnection,
    rows: DuckDBPyRelation,
    call: ResolvedCall,
    factors: Factors,
    /,
) -> tuple[DuckDBPyRelation, frozenset[int]]:
    """Convert filtered rows to each of the call's units at row grain.

    Every row joins to its own provider-year factor before any sum, so a sum
    across providers uses each provider's deflator.

    Args:
        cursor: The cursor ``rows`` lives on.
        rows: Stage 1 rows, with ``provider``, ``year`` and ``value``.
        call: The resolved call, whose ``units`` set the output units.
        factors: The Context's factor source.

    Returns:
        The rows with ``currency``, ``prices`` and ``base_year`` added before
        ``value``, one row per input row and unit, and the providers that took
        the DAC total's factors.

    Raises:
        MissingFactorError: A provider-year in ``rows`` lacks a factor for
            one of the units.
    """
    keys = rows.select(
        "CAST(provider AS BIGINT) AS provider, CAST(year AS BIGINT) AS year"
    )
    keys = keys.distinct().to_arrow_table()
    if keys.num_rows == 0:
        table, fallback = FACTOR_SCHEMA.empty_table(), frozenset[int]()
    else:
        result = factors.table(keys, call.units)
        table = _factors_for(keys, result.factors, call.units)
        fallback = result.fallback_providers

    names = [name for name in rows.columns if name != "value"]
    joined = rows.set_alias("r").join(
        cursor.from_arrow(table).set_alias("f"),
        "r.provider = f.provider AND r.year = f.year",
    )
    select = [f"r.{ident(name)}" for name in names]
    select += [f"f.{name}" for name in UNIT_COLUMNS]
    select.append("r.value * f.factor AS value")
    return joined.select(", ".join(select)), fallback


def _factors_for(
    keys: pa.Table, factors: pa.Table, units: tuple[Unit, ...]
) -> pa.Table:
    """Keep the factor rows for ``units``, and raise if a key lacks one."""
    by_key: dict[tuple[int, int, str, str, int | None], float] = {
        (r["provider"], r["year"], r["currency"], r["prices"], r["base_year"]): r[
            "factor"
        ]
        for r in factors.select(list(FACTOR_SCHEMA.names)).to_pylist()
    }
    kept: list[dict[str, object]] = []
    for key in keys.to_pylist():
        for unit in units:
            factor = by_key.get(
                (
                    key["provider"],
                    key["year"],
                    unit.currency,
                    unit.prices,
                    unit.base_year,
                )
            )
            if factor is None:
                raise MissingFactorError(
                    f"no {_unit_label(unit)} factor for provider {key['provider']} "
                    f"in {key['year']}"
                )
            kept.append(
                key
                | {
                    "currency": unit.currency,
                    "prices": unit.prices,
                    "base_year": unit.base_year,
                    "factor": factor,
                }
            )
    return pa.Table.from_pylist(kept, schema=FACTOR_SCHEMA)


def _unit_label(unit: Unit) -> str:
    if unit.base_year is None:
        return f"{unit.prices} {unit.currency}"
    return f"constant {unit.base_year} {unit.currency}"


# Stage 3 ---------------------------------------------------------------------


def aggregated(rows: DuckDBPyRelation, call: ResolvedCall, /) -> DuckDBPyRelation:
    """Sum converted rows by ``call.by`` plus ``SPLIT_COLUMNS``.

    Args:
        rows: Stage 2 rows.
        call: The resolved call.

    Returns:
        One row per combination of the grouping columns, ordered by them,
        with ``value`` summed.
    """
    always = [name for name in SPLIT_COLUMNS if name in rows.columns]
    keys = ", ".join(ident(name) for name in dict.fromkeys([*call.by, *always]))
    return rows.aggregate(f"{keys}, sum(value) AS value", keys).order(keys)

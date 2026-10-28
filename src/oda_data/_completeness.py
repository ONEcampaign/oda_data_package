"""Gaps in what providers reported, and the rows that hold them.

``reporting_gaps`` finds the ``Incomplete`` cells of a call: the provider-years,
or the parts of one, that a leaf's value covers only in part. Two rules decide
them:

- A provider that reported to one of a leaf's tables in an earlier year and
  has no row in a year or any later one has stopped (``_stopped``). So has a
  provider with no row for a ratio's ``Sum`` side in that year or later, as
  GNI without ODA (``_side_stopped``).
- In the table's last year, a provider with rows for a ``Sum`` leaf in the
  year before at a key of the binding's ``reporting`` dimensions, and none at
  it in the last year, has not yet reported that key (``_dropped``), as when a
  preliminary DAC1 year holds a provider's total ODA and not yet its refugee
  costs. Presence reads the leaf's basis and the rules and call codes on the
  provider and reporting dimensions alone (``_reported_rows``), so a recipient,
  sector or channel slice with no rows in the last year is a zero, and a
  provider whose last-year rows for a line go only to a recipient total or an
  excluded recipient has reported it. A ``Method`` leaf on such a binding tests
  presence of its own rows at provider grain. A binding without ``reporting``
  has no such cells.

Both rules consider only the providers a leaf can count, after the provider
exclusions the call meets (``_compile.counted_providers``), so a multilateral
that line 106 leaves out has no cell on a call for that line.

``reporting_gaps`` also marks the cells of leaves the indicator subtracts.

``normalised`` drops a keyed cell that a cell for the whole provider-year
covers. ``outcomes`` is the policy: a pure function that gives each cell its
``Outcome`` (null its provider's row, and warn when the call shows a provider
that has not yet reported it; null every sum that holds it; warn in a sum; or
change no row), from the call's shape and the table's last year.
``holding`` and ``held`` say which rows hold which cells, one rule for member
rows, totals and sums across providers: a row holds a cell when its provider
and year match, and each key whose dimension the row carries matches. The
rows a provider total sums, and a sum across providers, match by year and
keys alone, since a member with a cell may have no rows. ``_combine`` applies
the outcomes through ``holding`` and ``held`` and writes the text.

``last_year`` gives the last year of the tables an indicator reads, read once
per held release through the Context's ``_store.Releases``. Both modules read
the leaf calls through ``_rows``.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from enum import StrEnum
from itertools import product
from typing import TYPE_CHECKING

import pyarrow as pa

from oda_data._compile import (
    basis_spans,
    counted_providers,
    filtered,
    ident,
    literals,
)
from oda_data._errors import BasisRangeError
from oda_data._model import (
    Derived,
    Flow,
    Incomplete,
    Method,
    Ratio,
    Sum,
    basis_lines,
    filter_columns,
)
from oda_data._rows import (
    bindings,
    own_years,
    ratio_parts,
    read_call,
    shown_codes,
    stage1,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Mapping

    from duckdb import DuckDBPyConnection, DuckDBPyRelation

    from oda_data._model import Binding, Filter, Indicator, ResolvedCall
    from oda_data._store import Releases
    from oda_data._types import Code, Dimension, TableName


#: A key a provider reported to a leaf's table in a year: the provider, the
#: year, and the codes of the binding's ``reporting`` dimensions, as
#: ``_dropped`` reads them.
type Reported = tuple[Code, int, tuple[tuple[Dimension, Code], ...]]


@dataclass(frozen=True, slots=True)
class Gaps:
    """The cells ``reporting_gaps`` finds.

    Args:
        cells: Every cell of the call's leaves.
        subtracted: The cells of leaves the indicator subtracts, such as core
            ODA's in-donor refugee costs. A value that lacks one overstates.
        fixed: The code of each reporting dimension that the basis of every
            ``Sum`` leaf fixes in the table's last year, as the line and flow
            of in-donor refugee costs, where they agree. A cell of
            ``_dropped`` carries these keys.
        dropped: The cells of ``_dropped`` alone, before ``normalised`` drops
            those a cell for the whole provider-year covers. For a provider
            with no row in the table's last year they are the keys it
            reported the year before, which ``unheld`` reads.
        reported: The keys each provider reported in the table's last year,
            as ``_dropped`` reads them, or with no codes on a binding without
            ``reporting`` (``_whole_years``), read for a call with a provider
            total that allows partial values, which ``reporters`` reads.
    """

    cells: frozenset[Incomplete] = frozenset()
    subtracted: frozenset[Incomplete] = frozenset()
    fixed: Mapping[Dimension, Code] = field(default_factory=dict)
    dropped: frozenset[Incomplete] = frozenset()
    reported: frozenset[Reported] = frozenset()


def reporting_gaps(
    cursor: DuckDBPyConnection,
    indicator: Indicator,
    call: ResolvedCall,
    years: list[int],
    last: int | None,
    *,
    releases: Releases,
) -> Gaps:
    """The cells of every leaf by the engine's two rules (``_stopped`` and
    ``_dropped``), for the providers the call names that each leaf can count
    (``_counted``), in ``years``, and, when
    ``years`` holds the table's last year ``last``, in the year before, so a
    sum can tell a provider that reported then from one that stopped earlier.

    A call that names no providers has none. A ``Sum`` or ``Method`` alone
    has rows only where its provider reported, and a ``Method`` reports its
    own cells, so for a result that shows each provider on its own row only
    ``_dropped`` runs: its cells hold no row and name, for ``explain()``, the
    parts a shown provider has not yet reported.
    """
    sums = not isinstance(indicator.kind, Sum | Method) or _sums_providers(call, years)
    reads = [
        (leaf, leaf_call, subtracted, codes)
        for leaf, leaf_call, subtracted in _leaf_calls(indicator, call)
        if (codes := _counted(cursor, leaf, leaf_call))
    ]
    tables: dict[TableName, tuple[Binding, set[Code]]] = {}
    for leaf, _, _, codes in reads:
        tables.setdefault(leaf.binding.table, (leaf.binding, set()))[1].update(codes)

    checked = years
    if last is not None and last in years:
        checked = sorted({*years, last - 1})
    cells: set[Incomplete] = set()
    subtracted_cells: set[Incomplete] = set()
    dropped_cells: set[Incomplete] = set()
    reported: set[Reported] = set()
    for binding, codes in tables.values():
        if sums:
            cells |= _stopped(cursor, binding, codes, checked)
    # Only a provider total that allows partial values names who reported.
    named = call.allow_partial and any(key[0] == "provider" for key in call.totals)
    for leaf, leaf_call, subtracted, codes in reads:
        if not leaf.binding.reporting:
            if named:
                reported |= _whole_years(cursor, leaf.binding, codes, years, releases)
            continue
        dropped, now = _dropped(cursor, leaf, leaf_call, codes, years, releases)
        dropped_cells |= dropped
        reported |= now
        if subtracted:
            subtracted_cells |= dropped
    cells |= dropped_cells
    if sums and isinstance(indicator.kind, Ratio):
        for side, side_call in ratio_parts(indicator, call):
            codes = _counted(cursor, side, side_call)
            if isinstance(side.kind, Sum) and codes:
                cells |= _side_stopped(
                    cursor, side, side_call, codes, checked, releases
                )
    return Gaps(
        frozenset(cells),
        frozenset(subtracted_cells),
        _agreed_fixed(reads, last) if last is not None else {},
        frozenset(dropped_cells),
        frozenset(reported),
    )


def _agreed_fixed(
    reads: list[tuple[Indicator, ResolvedCall, bool, frozenset[Code]]], last: int
) -> dict[Dimension, Code]:
    """The codes of the reporting dimensions that every ``Sum`` leaf's basis
    fixes in ``last``, as ``_dropped`` keys its cells, where they agree."""
    found: list[dict[Dimension, Code]] = []
    for leaf, leaf_call, _, _ in reads:
        if not isinstance(leaf.kind, Sum):
            return {}
        members = read_call(leaf, leaf_call).members
        found.append(
            {
                d: code
                for d, code in _fixed_codes(leaf, leaf_call, last).items()
                if d in leaf.binding.reporting and d not in members
            }
        )
    if not found:
        return {}
    first, *rest = found
    return {d: code for d, code in first.items() if all(f.get(d) == code for f in rest)}


def _counted(
    cursor: DuckDBPyConnection, leaf: Indicator, call: ResolvedCall
) -> frozenset[Code]:
    """The providers the call names that the leaf can count: those of its
    read call that every provider exclusion the call meets keeps, as
    ``counted_providers`` decides. Empty when the call names none."""
    read = read_call(leaf, call)
    codes = read.members.get("provider")
    if not codes:
        return frozenset()
    return counted_providers(cursor, leaf, read, codes)


def normalised(cells: Iterable[Incomplete]) -> frozenset[Incomplete]:
    """``cells`` without the keyed cells that a cell for the whole
    provider-year with the same missing inputs covers, as a provider that
    stopped reporting covers each line it has not yet reported."""
    cells = frozenset(cells)
    whole = {(c.provider, c.year, c.missing) for c in cells if not c.keys}
    return frozenset(
        cell
        for cell in cells
        if not cell.keys or (cell.provider, cell.year, cell.missing) not in whole
    )


def _sums_providers(call: ResolvedCall, years: list[int]) -> bool:
    """Whether the result sums providers: in a provider total, or in member
    rows of a year outside ``own_years``."""
    totals = any(key[0] == "provider" for key in call.totals)
    return totals or own_years(call, years) != set(years)


def _leaf_calls(
    indicator: Indicator, call: ResolvedCall, *, subtracted: bool = False
) -> Iterator[tuple[Indicator, ResolvedCall, bool]]:
    """Each ``Sum`` and ``Method`` the indicator reads, with the call it reads
    its rows with (a ratio's denominator its own basis), and whether the
    indicator subtracts its value: a leaf under an odd number of ``Derived``
    ``minus`` parts. A ratio's sides keep the sign of the ratio, since a side
    with no row leaves its pair out of a sum."""
    match indicator.kind:
        case Sum() | Method():
            yield indicator, call, subtracted
        case Ratio():
            for part, part_call in ratio_parts(indicator, call):
                yield from _leaf_calls(part, part_call, subtracted=subtracted)
        case Derived(plus=plus, minus=minus):
            for part in plus:
                yield from _leaf_calls(part, call, subtracted=subtracted)
            for part in minus:
                yield from _leaf_calls(part, call, subtracted=not subtracted)


def _stopped(
    cursor: DuckDBPyConnection,
    binding: Binding,
    codes: Iterable[Code],
    years: list[int],
) -> set[Incomplete]:
    """The cells of providers that have stopped reporting to the binding's
    table: each of ``years`` after the last year in which the provider has
    any row. On CRS, that is any row for the provider and year."""
    provider = ident(binding.dimensions["provider"])
    year = ident(binding.dimensions["year"])
    last = cursor.sql(
        f"SELECT {provider}, max(CAST({year} AS BIGINT)) "
        f"FROM {ident(binding.table)} "
        f"WHERE {provider} IN ({literals(sorted(codes, key=str))}) "
        f"GROUP BY {provider}"
    ).fetchall()
    return {
        Incomplete(code, year, frozenset({code}))
        for code, reported in last
        for year in years
        if reported < year
    }


def _side_stopped(
    cursor: DuckDBPyConnection,
    side: Indicator,
    call: ResolvedCall,
    codes: Iterable[Code],
    years: list[int],
    releases: Releases,
) -> set[Incomplete]:
    """The cells of providers whose rows for a ratio's ``Sum`` side have
    stopped while the other side may go on, as GNI without ODA: each of
    ``years`` after the provider's last row for the side under the call's
    filters, for the providers of ``codes``.

    One query reads each provider's last year, the side's filters and the
    call's providers pushed into the scan. A ``Method`` side reports its
    own cells, and a provider that has not yet reported a side leaves its
    pair out of a sum.
    """
    every_year = replace(call, years=None, by=("provider", "year"))
    last = (
        stage1(cursor, side, every_year, releases=releases)
        .rows.filter(f"provider IN ({literals(sorted(codes, key=str))})")
        .aggregate("provider, max(CAST(year AS BIGINT)) AS last", "provider")
        .fetchall()
    )
    return {
        Incomplete(code, year, frozenset({code}))
        for code, reported in last
        for year in years
        if reported < year
    }


def _dropped(
    cursor: DuckDBPyConnection,
    leaf: Indicator,
    call: ResolvedCall,
    codes: Iterable[Code],
    years: list[int],
    releases: Releases,
) -> tuple[set[Incomplete], set[Reported]]:
    """The cells of providers with rows for the leaf in the year before its
    table's last year at a key of the binding's ``reporting`` dimensions, and
    none at that key in the last year, when ``years`` holds the last year,
    and the keys each provider has rows at in the last year.

    The last year of DAC1 is preliminary. Every member reports total ODA, but
    only a few report detail lines such as in-donor refugee costs, so a total
    over the reporters reads as the full total. A provider with no rows in
    the year before, such as one that reports debt relief in some years only,
    counts as zero.

    A ``Sum`` reads presence by provider, year and each reporting dimension
    the basis does not fix, through ``_reported_rows``, with the call's codes
    on those dimensions alone, so ``flows=1121`` reads the grants part. The
    basis fixes the line of a named DAC1 indicator, and the flow of a DAC1
    basis when the call names no flows; the cell's keys name those codes
    too. A ``Method`` reads its own rows under the call at provider grain
    and reports anything finer itself. Either reads the two years alone,
    which a ``Method``'s rows allow.
    """
    binding = leaf.binding
    last = last_year(cursor, leaf, releases=releases)
    if last is None or last not in years:
        return set(), set()
    grain: tuple[Dimension, ...] = ()
    fixed: dict[Dimension, Code] = {}
    presence = replace(call, years=(last - 1, last))
    if isinstance(leaf.kind, Sum):
        members = read_call(leaf, call).members
        fixed = {
            d: code
            for d, code in _fixed_codes(leaf, call, last).items()
            if d in binding.reporting and d not in members
        }
        grain = tuple(
            d for d in binding.reporting if d in leaf.dimensions and d not in fixed
        )
        presence = replace(
            presence,
            by=("provider", "year", *grain),
            members={d: members[d] for d in ("provider", *grain) if d in members},
            groups=(),
            totals=(),
            rows={},
        )
    try:
        rows = (
            _reported_rows(cursor, leaf, presence)
            if isinstance(leaf.kind, Sum)
            else stage1(cursor, leaf, presence, releases=releases).rows
        )
    except BasisRangeError:
        # The basis starts in the last year, so no provider has its rows the
        # year before.
        return set(), set()
    codes = sorted(codes, key=str)
    select = ["provider", "CAST(year AS BIGINT) AS year", *map(ident, grain)]
    seen = (
        rows.filter(f"provider IN ({literals(codes)})")
        .select(", ".join(select))
        .distinct()
        .fetchall()
    )
    before = {(key[0], *key[2:]) for key in seen if key[1] == last - 1}
    now = {(key[0], *key[2:]) for key in seen if key[1] == last}

    def keyed(key: tuple) -> tuple[tuple[Dimension, Code], ...]:
        codes_at = fixed | dict(zip(grain, key[1:], strict=True))
        return tuple((d, codes_at[d]) for d in binding.reporting if d in codes_at)

    cells = {
        Incomplete(key[0], last, frozenset({key[0]}), keys=keyed(key))
        for key in before - now
    }
    return cells, {(key[0], last, keyed(key)) for key in now}


def _whole_years(
    cursor: DuckDBPyConnection,
    binding: Binding,
    codes: Iterable[Code],
    years: list[int],
    releases: Releases,
) -> set[Reported]:
    """The providers of ``codes`` with any row in the binding's table in its
    last year, when ``years`` holds it, as keys without codes: a binding
    without ``reporting``, as the CRS, has a provider report a year whole."""
    last = table_last_year(cursor, binding, releases=releases)
    if last is None or last not in years:
        return set()
    provider = ident(binding.dimensions["provider"])
    year = ident(binding.dimensions["year"])
    found = cursor.sql(
        f"SELECT DISTINCT {provider} FROM {ident(binding.table)} "
        f"WHERE CAST({year} AS BIGINT) = {last} "
        f"AND {provider} IN ({literals(sorted(codes, key=str))})"
    ).fetchall()
    return {(code, last, ()) for (code,) in found}


def _reported_rows(
    cursor: DuckDBPyConnection, leaf: Indicator, call: ResolvedCall
) -> DuckDBPyRelation:
    """The rows that show which keys a provider has reported to a ``Sum``
    leaf's table.

    A provider reports by provider, year and the binding's ``reporting``
    dimensions, so presence reads the leaf's basis and each row filter of
    the binding and the leaf that names no column of another dimension:
    fixed rows such as current USD, and rules on provider, line or flow.
    Rules on an activity dimension (recipient, sector, channel) do not
    apply, nor does the screen of recipient totals, so a provider whose
    last-year rows for a line go only to 10100 (developing countries, total)
    or to a recipient the leaf leaves out has reported the line.
    """
    binding = leaf.binding
    kept = {"provider", "year", *binding.reporting}
    activity = {column for d, column in binding.dimensions.items() if d not in kept}

    def applies(rules: tuple[Filter, ...]) -> tuple[Filter, ...]:
        return tuple(
            rule for rule in rules if activity.isdisjoint(filter_columns((rule,)))
        )

    presence = replace(
        leaf,
        binding=replace(binding, rows=applies(binding.rows)),
        rows=applies(leaf.rows),
    )
    return filtered(cursor, presence, call)


def _fixed_codes(
    leaf: Indicator, call: ResolvedCall, year: int
) -> dict[Dimension, Code]:
    """The codes the call's basis fixes in ``year``: the line of a one-line
    ``Sum`` or a ``Line`` basis, and the flow of a ``Flow`` basis."""
    rule = leaf.binding.bases.get(call.basis)
    if rule is None:
        return {}
    for basis, start, stop in basis_spans(call.basis, rule):
        if (start is None or start <= year) and (stop is None or year < stop):
            fixed: dict[Dimension, Code] = {}
            if (line := basis_lines(leaf).get(basis)) is not None:
                fixed["line"] = line
            if isinstance(concrete := leaf.binding.bases[basis], Flow):
                fixed["flow"] = concrete.code
            return fixed
    return {}


# Rows that hold cells -----------------------------------------------------------


def holding(
    cursor: DuckDBPyConnection,
    rows: DuckDBPyRelation,
    cells: Iterable[Incomplete],
    *,
    providers: bool = True,
) -> DuckDBPyRelation:
    """``rows`` with ``incomplete``, whether the row holds one of ``cells``.

    With ``providers`` false a row holds a cell by year and keys alone, as
    the rows a provider total sums hold the cells of its members, some of
    which have no rows. Each row appears once, whichever number of cells it
    holds.
    """
    found = _cells_table(cursor, rows, cells, providers=providers)
    if found is None:
        return rows.select("*, FALSE AS incomplete")
    table, on = found
    left = rows.set_alias("r")
    marked = left.join(table, on, how="semi").select("r.*, TRUE AS incomplete")
    clear = left.join(table, on, how="anti").select("r.*, FALSE AS incomplete")
    return marked.union(clear)


def held(
    cursor: DuckDBPyConnection,
    rows: DuckDBPyRelation,
    cells: Iterable[Incomplete],
    *,
    providers: bool = True,
) -> frozenset[Incomplete]:
    """The cells of ``cells`` that one of ``rows`` holds, as ``holding``
    decides, or by year and keys alone when ``providers`` is false."""
    listed = sorted(set(cells), key=cell_order)
    found = _cells_table(cursor, rows, listed, providers=providers)
    if found is None:
        return frozenset()
    table, on = found
    matched = table.join(rows.set_alias("r"), on, how="semi").select("g.cell")
    return frozenset(listed[index] for (index,) in matched.fetchall())


def unheld(
    cursor: DuckDBPyConnection,
    rows: DuckDBPyRelation,
    cells: Iterable[Incomplete],
    *,
    split: Iterable[Dimension],
    fixed: Mapping[Dimension, Code],
    named: Mapping[Dimension, frozenset[Code]],
    dropped: Iterable[Incomplete],
    providers: bool = True,
) -> frozenset[Incomplete]:
    """The cells of ``cells`` that no row of ``rows`` holds, as ``held``
    decides, at the grain of the ``split`` dimensions (``reporting_parts``),
    with their keys in the order of ``split``."""
    parts = reporting_parts(
        rows, cells, split=split, fixed=fixed, named=named, dropped=dropped
    )
    return parts - held(cursor, rows, parts, providers=providers)


def reporting_parts(
    rows: DuckDBPyRelation,
    cells: Iterable[Incomplete],
    *,
    split: Iterable[Dimension],
    fixed: Mapping[Dimension, Code],
    named: Mapping[Dimension, frozenset[Code]],
    dropped: Iterable[Incomplete],
) -> frozenset[Incomplete]:
    """``cells``, cells a provider has not yet reported in the table's last
    year, at the grain of the ``split`` dimensions, a binding's
    ``reporting``, each with its keys in the order of ``split``.

    A cell without a key on a split dimension that ``rows`` carry stands for
    one cell at each of the dimension's codes, those the call names
    (``named``) or else those ``rows`` hold, so a provider that reported
    nothing in a year can be held by a row of one line and not by any row of
    another. On a split dimension ``rows`` do not carry, the cell takes the
    code the call's basis fixes (``Gaps.fixed``), as ``_dropped`` keys its
    cells. A cell keeps its keys where ``rows`` hold no code of a dimension
    it lacks.

    A cell for the whole provider-year stands only for the codes at which
    its provider reported the year before: the cells ``_dropped`` found for
    its provider and year (``dropped``, as ``Gaps.dropped``), read at the
    grain the gap is reported at, those of them at the codes above where
    ``rows`` hold any, and none where it has none. A provider with rows the
    year before only under other lines is not missing a line it never
    reported.
    """
    order = {d: i for i, d in enumerate(split)}
    codes = {
        d: sorted(named[d] if d in named else _codes_in(rows, d), key=str)
        for d in order
        if d in rows.columns
    }
    before: dict[tuple[Code, int], list[Incomplete]] = {}
    for cell in dropped:
        before.setdefault((cell.provider, cell.year), []).append(cell)
    every: set[Incomplete] = set()
    for cell in cells:
        if order and not cell.keys:
            for part in before.get((cell.provider, cell.year), []):
                if all(code in codes[d] for d, code in part.keys if codes.get(d)):
                    every |= _split(part, codes, fixed, order)
        else:
            every |= _split(cell, codes, fixed, order)
    return frozenset(every)


def _split(
    cell: Incomplete,
    codes: Mapping[Dimension, list[Code]],
    fixed: Mapping[Dimension, Code],
    order: Mapping[Dimension, int],
) -> set[Incomplete]:
    """``cell`` at each code of the ``codes`` dimensions it has no key on,
    and at the ``fixed`` code of each ``order`` dimension outside ``codes``,
    as ``reporting_parts`` reads it, or ``cell`` alone where a dimension has
    no code."""
    keyed = dict(cell.keys)
    free = [d for d in codes if d not in keyed]
    if not all(codes[d] for d in free):
        return {cell}
    known = [
        (d, code)
        for d, code in fixed.items()
        if d in order and d not in keyed and d not in codes
    ]
    return {
        replace(
            cell,
            keys=tuple(
                sorted(
                    (*cell.keys, *known, *zip(free, combination, strict=True)),
                    key=lambda key: order.get(key[0], -1),
                )
            ),
        )
        for combination in product(*(codes[d] for d in free))
    }


def reporters(reported: Iterable[Reported], cell: Incomplete) -> frozenset[Code]:
    """The providers of ``reported`` (``Gaps.reported``) with rows in the
    cell's year at each of its keys: the members a provider total sums at
    the part a cell names, whichever columns the total's rows carry. A key
    read at provider grain, as a ``Method``'s, holds a cell without keys
    alone."""
    keys = set(cell.keys)
    return frozenset(
        provider
        for provider, year, codes in reported
        if year == cell.year and keys <= set(codes)
    )


def _codes_in(rows: DuckDBPyRelation, dimension: Dimension) -> list[Code]:
    """The codes of ``dimension`` that ``rows`` hold."""
    found = rows.select(ident(dimension)).distinct().fetchall()
    return [code for (code,) in found if code is not None]


def _cells_table(
    cursor: DuckDBPyConnection,
    rows: DuckDBPyRelation,
    cells: Iterable[Incomplete],
    *,
    providers: bool = True,
) -> tuple[DuckDBPyRelation, str] | None:
    """``cells`` as a relation aliased ``g``, one row per cell numbered by
    ``cell`` in the order given, and the SQL true where row ``r`` holds cell
    ``g``: the year matches, the provider where ``rows`` have one and
    ``providers`` is true, and each key whose dimension ``rows`` carry. A
    cell without a key on such a dimension holds a null there, which every
    row matches."""
    cells = list(cells)
    if not cells:
        return None
    columns = set(rows.columns)
    keyed = sorted({d for c in cells for d, _ in c.keys if d in columns})
    data: dict[str, pa.Array] = {
        "cell": pa.array(range(len(cells)), pa.int64()),
        "provider": pa.array([c.provider for c in cells]),
        "year": pa.array([c.year for c in cells], pa.int64()),
    }
    conditions = ["r.year = g.year"]
    if providers and "provider" in columns:
        conditions.append("r.provider = g.provider")
    for dimension in keyed:
        key = f"key_{dimension}"
        data[key] = pa.array([dict(c.keys).get(dimension) for c in cells])
        conditions.append(
            f"(g.{ident(key)} IS NULL OR r.{ident(dimension)} = g.{ident(key)})"
        )
    table = cursor.from_arrow(pa.table(data)).set_alias("g")
    return table, " AND ".join(conditions)


# What a cell does to a result ---------------------------------------------------


class Outcome(StrEnum):
    """What an incomplete cell does to the rows of a result.

    A provider total over members is null where any member's cell matches its
    year and keys, whatever the cell's outcome, unless the call allows partial
    values.
    """

    #: The cell's provider has its own rows in the year, which are null.
    NULL_ROW = "null_row"
    #: As ``NULL_ROW``, for a provider the call shows that reported the cell
    #: the year before the table's last year and has not reported it in the
    #: last year: its rows that hold the cell are null, and the result warns
    #: of it whether or not the provider has a row for it.
    WARN_ROW = "warn_row"
    #: Every row that holds the cell is null, a sum across providers
    #: included, since the cell can overstate it: a ``Method``'s cell, a
    #: ratio side that stopped, or a subtracted part not yet reported.
    NULL_SUM = "null_sum"
    #: A sum across providers keeps its value, which covers the providers
    #: that have reported, and warns of the cell.
    WARN_SUM = "warn_sum"
    #: No row but a provider total reads the cell: a provider that stopped
    #: before the year counts as zero in a sum across providers.
    NOTE = "note"


def outcomes(
    cells: frozenset[Incomplete],
    call: ResolvedCall,
    *,
    own: set[int],
    years: list[int],
    last: int | None,
    ratio: bool,
    subtracted: frozenset[Incomplete] = frozenset(),
) -> dict[Incomplete, Outcome]:
    """What each of a call's normalised ``cells`` does to its rows.

    ``own`` are the years in which member rows show each provider on its own,
    ``years`` the years the call reads (those the rows hold, and the table's
    last year when the call asks for it), ``last`` the table's last year,
    ``ratio`` whether the indicator is a ``Ratio``, and ``subtracted`` the
    cells of parts the indicator subtracts.

    A provider's own gap in the last year after it reported the year before
    nulls its own row and, when the call shows the provider, warns, whether
    the provider has a null row for it or none. In a sum across providers
    the same gap leaves the sum short by the provider's value, so the sum
    warns. A gap that can overstate the sum nulls it: a ``Method``'s cell
    on the members or funders it sums, a subtracted part not yet reported,
    and on a ratio a side that stopped, which pairs a numerator with too
    small a denominator.
    """
    shown = last_shown(call, years, last)
    found: dict[Incomplete, Outcome] = {}
    for cell in cells:
        waiting = not_yet_reported(cell, cells, last)
        if cell.year in own:
            shows = waiting and (shown is None or cell.provider in shown)
            found[cell] = Outcome.WARN_ROW if shows else Outcome.NULL_ROW
        elif (
            not is_own_gap(cell)
            or (ratio and not waiting)
            or (waiting and cell in subtracted)
        ):
            found[cell] = Outcome.NULL_SUM
        elif waiting and (shown is None or cell.provider in shown):
            found[cell] = Outcome.WARN_SUM
        else:
            found[cell] = Outcome.NOTE
    return found


def last_shown(
    call: ResolvedCall, years: list[int], last: int | None
) -> frozenset[Code] | None:
    """The providers the call shows in the table's last year, or ``None``
    for any, when ``years`` hold that year, and none otherwise."""
    if last is None or last not in years:
        return frozenset()
    shown = shown_codes(call, "provider", [last])
    return None if shown is None else shown[last]


def not_yet_reported(
    cell: Incomplete, cells: frozenset[Incomplete], last: int | None
) -> bool:
    """Whether ``cell`` is a provider's own gap in the table's last year after
    it reported the year before. A provider that stopped earlier has a cell
    for the whole year before. A keyed cell comes from ``_dropped``, so the
    provider had rows the year before."""
    return (
        cell.year == last
        and is_own_gap(cell)
        and Incomplete(cell.provider, cell.year - 1, cell.missing) not in cells
    )


def is_own_gap(cell: Incomplete) -> bool:
    """Whether the cell's provider itself did not report. Other cells name the
    members or funders its value sums."""
    return cell.missing == {cell.provider}


def cell_order(cell: Incomplete) -> tuple[str, int, tuple[str, ...], tuple[str, ...]]:
    """Cells by provider, year, keys and missing inputs, as text."""
    return (
        str(cell.provider),
        cell.year,
        tuple(f"{d} {c}" for d, c in cell.keys),
        tuple(sorted(map(str, cell.missing))),
    )


def last_year(
    cursor: DuckDBPyConnection,
    indicator: Indicator,
    /,
    *,
    releases: Releases,
) -> int | None:
    """The last year of any table the indicator reads."""
    lasts = (
        table_last_year(cursor, binding, releases=releases)
        for binding in {b.table: b for b in bindings(indicator)}.values()
    )
    return max((last for last in lasts if last is not None), default=None)


def table_last_year(
    cursor: DuckDBPyConnection,
    binding: Binding,
    /,
    *,
    releases: Releases,
) -> int | None:
    """The last year of the binding's table, read once per held release."""
    year = ident(binding.dimensions["year"])

    def scan() -> int | None:
        [(last,)] = cursor.sql(
            f"SELECT max(CAST({year} AS BIGINT)) FROM {ident(binding.table)}"
        ).fetchall()
        return last

    return releases.get(binding.table, ("last_year", year), scan)

"""Combine compiled rows into a call's result.

``combine`` runs any indicator kind over a resolved call and returns one
Arrow table in the result shape. It builds on the compiler's stages in
``_compile`` and the stage 1 rows in ``_rows``:

- ``Sum`` rows come from ``filtered``. ``Method`` rows come from the
  indicator's function. ``Derived`` rows come from each part, the ``minus``
  parts negated. Each is converted to the call's units at row grain by ``converted``.
- ``Ratio`` reads its numerator and denominator in current USD, pairs them
  per member, and divides sums. It refuses units.
- Member rows sum by ``by`` plus ``SPLIT_COLUMNS``. A group passed without
  ``oda.total()`` shows each member in the years it is a member, and a code
  named directly shows in every year. Group totals sum the same rows over
  each group's members in each year.
- Incomplete inputs follow one rule for every kind. Each leaf, a ``Sum`` or a
  ``Method``, yields ``Incomplete`` cells, the provider-years, or the lines of
  one, that its value covers only in part. ``_completeness`` finds them (a
  provider that stopped reporting, or that has not yet reported a line of
  the table's last year) and decides which rows hold them. A ``Method`` adds
  the cells it reports itself, such as EU Institutions' (918) GNI in a year
  after an EU member's last GNI. A ``Derived`` or ``Ratio`` holds the cells
  of its parts. A provider that has not yet reported, or that skips a year
  and reports again, counts as zero.
- Unless the call allows partial values, a row is null where it holds an
  incomplete cell, and a provider total row is null where a member's cell
  matches its year and keys. A row sums providers when ``provider`` is
  outside ``by`` and the call shows more than one provider in the row's year.
  Such a row keeps its value where a provider that reported the year before
  is missing from the table's last year, and warns of it, since the sum then
  covers only the providers that have reported. It is null, and does not
  warn, where it holds a cell that can overstate it: a ``Method``'s cell, or
  a part the indicator subtracts that a provider has not yet reported. A
  provider the call shows on its own that has not yet reported a part of the
  table's last year warns too, when the call asks for that year, whether its
  row for the part is null or it has none.
  ``_completeness.outcomes`` decides each cell's effect. A ratio pairs each
  member's numerator with its denominator, and a member with no row on one
  side leaves its pair out of a total.

``records`` returns stage 2 for ``Sum`` and ``Method`` indicators.

``_combine`` works from ``ResolvedCall`` alone and does not import ``_resolve``
or ``_groups``.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass, replace
from enum import StrEnum
from typing import TYPE_CHECKING

import pyarrow as pa

from oda_data._compile import (
    SPLIT_COLUMNS,
    UNIT_COLUMNS,
    basis_spans,
    basis_sql,
    check_dimensions,
    converted,
    ident,
    literal,
    literals,
)
from oda_data._completeness import (
    Outcome,
    cell_order,
    held,
    holding,
    is_own_gap,
    last_year,
    normalised,
    not_yet_reported,
    outcomes,
    reporting_gaps,
    table_last_year,
    unheld,
)
from oda_data._errors import OdaDataWarning, UnsupportedQueryError
from oda_data._model import (
    Derived,
    In,
    Incomplete,
    Method,
    NotIn,
    Ratio,
    Sum,
)
from oda_data._names import (
    EU_INSTITUTIONS,
    eu_members,
    is_aggregate,
    names,
)
from oda_data._rows import (
    Rows,
    aggregate_sql,
    bindings,
    check_years,
    leaves,
    own_years,
    ratio_parts,
    shown_codes,
    stage1,
    walk,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Mapping

    from duckdb import DuckDBPyConnection, DuckDBPyRelation

    from oda_data._model import (
        Binding,
        Caveat,
        Factors,
        GroupKey,
        Indicator,
        ResolvedCall,
        ResolvedGroup,
    )
    from oda_data._store import Releases
    from oda_data._types import Code, Dimension, TableName

_AGGREGATE_DIMENSIONS: tuple[Dimension, ...] = ("provider", "recipient")


#: The most codes a sentence names one by one. More are counted.
LISTED = 5

#: The DAC1 line of what a provider pays EU Institutions (918).
_EU_PAYMENT_LINE = 2102


@dataclass(frozen=True, slots=True)
class Combined:
    """A call's rows.

    Args:
        table: For ``combine``, the result: each ``by`` dimension (codes as
            strings, a group total's code is the group's name, ``year`` as
            int64), ``line`` and ``flow`` when the rows hold them and they are
            not in ``by``, then ``indicator``, ``indicator_version``, ``basis``,
            ``currency``, ``prices``, ``base_year``, ``scale``,
            ``is_aggregate`` and ``value``. Member rows come first, ordered by
            ``by``, then each total in the order of ``call.totals``. For
            ``records``, stage 2 rows with table types.
        fallback_providers: Providers whose factors came from the DAC total.
        incomplete: The incomplete cells the result holds: those its member
            and total rows hold, which are null unless the call allows
            partial values, including a subtracted part a sum across
            providers holds, and those a sum across providers warns of.
        notes: Sentences for ``explain()`` on the incomplete cells, then the
            notes of each ``Method`` part, such as the EU members counted as
            zero in 918's GNI.
    """

    table: pa.Table
    fallback_providers: frozenset[int]
    incomplete: tuple[Incomplete, ...] = ()
    notes: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class _Held:
    """The incomplete cells behind each part of a result.

    Args:
        own: Cells that make a member row null unless the call allows
            partial values.
        totals: Cells of the providers a provider total sums in the year,
            and the cells another total's rows hold.
        summed: Cells a sum across providers outside ``by`` warns of: those of
            providers missing from the table's last year that reported the
            year before, held by a row that is not null.
        overstated: Cells of a subtracted part that a sum across providers
            outside ``by`` holds, which make it null unless the call allows
            partial values.
        waiting: The cells of ``own`` that a provider the call shows has not
            yet reported (``Outcome.WARN_ROW``), of which the result warns.
        absent: Cells a provider the call shows has not yet reported, which no
            row holds since the provider has no row for them.
        unsummed: Cells of the table's last year that a sum across providers
            outside ``by`` holds, which a provider has not yet reported, and
            that no row holds since none of the providers it sums has a row
            for them.
        untotalled: For each provider total, its name and the cells of its
            members in the table's last year that they have not yet
            reported and that no row of the total holds, since none of its
            members has a row for them.
    """

    own: frozenset[Incomplete] = frozenset()
    totals: frozenset[Incomplete] = frozenset()
    summed: frozenset[Incomplete] = frozenset()
    overstated: frozenset[Incomplete] = frozenset()
    waiting: frozenset[Incomplete] = frozenset()
    absent: frozenset[Incomplete] = frozenset()
    unsummed: frozenset[Incomplete] = frozenset()
    untotalled: tuple[tuple[str, frozenset[Incomplete]], ...] = ()


def tables_for(indicator: Indicator, /) -> tuple[TableName, ...]:
    """Return the tables a call on ``indicator`` reads, for ``Tables.cursor``.

    The cursor ``combine`` and ``records`` take must hold a view for each.
    """
    tables = dict.fromkeys(binding.table for binding in bindings(indicator))
    for part in leaves(indicator):
        if isinstance(part.kind, Method):
            tables |= dict.fromkeys(part.kind.tables)
    return tuple(tables)


def combine(
    cursor: DuckDBPyConnection,
    indicator: Indicator,
    call: ResolvedCall,
    factors: Factors,
    /,
    *,
    releases: Releases,
) -> Combined:
    """Compute an indicator for a resolved call.

    Emits each scoped caveat of the indicator, and of its parts, that
    concerns a basis and year the call reads, as an ``OdaDataWarning``.
    When the call asks for the table's last year and providers have not yet
    reported a part of it that they reported the year before, it emits one
    warning for the year, listing each part (``last_year_warning``).

    Args:
        cursor: A cursor holding a view for each of ``tables_for(indicator)``,
            and ``broad_sectors`` when the call uses that dimension.
        indicator: Any indicator.
        call: The resolved call. Codes in ``members`` but outside ``rows``
            feed group totals and are dropped from the member rows. ``units``
            is empty for a ``Ratio`` and holds at least one unit otherwise.
        factors: The Context's factor source.
        releases: The Context's store as the call's held releases see it,
            which keeps each table's last year and the Methods' memo across
            calls.

    Returns:
        The result rows, the providers that took fallback factors, and the
        incomplete cells the rows hold.

    Raises:
        DimensionError: The call groups, filters or totals by a dimension the
            indicator lacks.
        UnsupportedQueryError: ``oda.total()`` without its dimension in
            ``by``; a total over a group holding an aggregate code; an
            aggregate code summed with other codes; EU Institutions (918)
            summed with a provider that pays it in one year, or a total over
            a group holding both in one year, on a binding whose members'
            figures hold their EU share; a code the indicator's row filters
            leave out; ``unit=`` on a ratio; a basis the indicator lacks.
        BasisRangeError: The call asks for years before the basis starts.
        MissingFactorError: A provider-year lacks a factor.
        ValueError: The call breaks the ``ResolvedCall`` contract: a total's
            group lacks a year the rows hold, or ``rows`` narrows a dimension
            that is neither in ``by`` nor a group's.
    """
    _check_call(cursor, indicator, call, releases)
    _warn_caveats(indicator, call)

    if isinstance(indicator.kind, Ratio):
        read = _ratio_rows(cursor, indicator, call, releases)
        rows, fallback = read.rows, frozenset[int]()
    else:
        read = stage1(cursor, indicator, call, releases=releases)
        stage2, fallback = _in_units(cursor, indicator, read.rows, call, factors)
        rows = _materialize(cursor, stage2)

    years = _years(rows)
    last = last_year(cursor, indicator, releases=releases)
    reads = _read_years(call, years, last)
    gaps = reporting_gaps(cursor, indicator, call, reads, last, releases=releases)
    cells = normalised(read.incomplete | gaps.cells)
    table, held = _assemble(
        cursor,
        indicator,
        call,
        rows,
        cells,
        years=years,
        reads=reads,
        last=last,
        subtracted=gaps.subtracted & cells,
        fixed=gaps.fixed,
    )
    parts = _last_year_parts(held, call, warned=True)
    for year in sorted({part.year for part in parts}):
        text = last_year_warning(year, [part for part in parts if part.year == year])
        warnings.warn(f"{indicator.id}: {text}", OdaDataWarning, stacklevel=2)
    incomplete = held.own | held.totals | held.summed | held.overstated
    return Combined(
        table,
        fallback,
        tuple(sorted(incomplete, key=cell_order)),
        (*_incomplete_text(held, call), *read.notes),
    )


def _read_years(call: ResolvedCall, years: list[int], last: int | None) -> list[int]:
    """The years the rows hold, and the table's last year when the call asks
    for it, so a provider the call shows that has not yet reported a part of
    that year has a cell even where no row survives."""
    if last is None or last in years:
        return years
    if call.years is not None and last not in call.years:
        return years
    return sorted({*years, last})


def partial_text(year: int, count: int, /, *, part: str | None = None) -> str:
    """The note on a sum across providers some of which reported the year
    before the table's last year, or ``part`` of it such as ``"line 1820"``,
    and have not reported it."""
    providers = "provider" if count == 1 else "providers"
    have = "has" if count == 1 else "have"
    if part is None:
        what = f"{year - 1} {have} not reported {year}"
    else:
        what = f"{part} in {year - 1} {have} not reported it in {year}"
    return (
        f"{year} is partial: {count} {providers} that reported {what}, so its "
        "sum covers only those that have; add 'provider' to by to see who "
        "reported"
    )


def nulled_text(
    year: int,
    codes: Iterable[Code],
    /,
    *,
    part: str | None = None,
    listed: int = LISTED,
) -> str:
    """The note on providers whose own row is null in a partial last year,
    for ``part`` of the indicator such as ``"line 1820"`` when one is named.

    It names the first ``listed`` providers, in order of name, and counts the
    rest.
    """
    have = "has" if len(set(codes)) == 1 else "have"
    rows = f"{year} rows" if part is None else f"{year} rows of {part}"
    return (
        f"{rows} are null for {_providers_text(codes, listed=listed)}, which "
        f"reported {part or 'a part of the indicator'} in {year - 1} and {have} "
        f"not reported it in {year}; allow_partial=True returns the partial values"
    )


def unreported_text(
    year: int,
    codes: Iterable[Code],
    /,
    *,
    part: str | None = None,
    nulled: bool,
    listed: int = LISTED,
) -> str:
    """The warning on providers the call shows that reported ``part`` of the
    indicator, such as ``"line 1820"``, the year before the table's last year
    and have not reported it in the last year: their rows for it are null
    when ``nulled``, and the result has none otherwise."""
    one = len(set(codes)) == 1
    have = "has" if one else "have"
    providers = _providers_text(codes, listed=listed)
    if part is None:
        what = f"{providers} reported {year - 1} and {have} not reported {year}"
    else:
        what = (
            f"{providers} reported {part} in {year - 1} and {have} not reported "
            f"it in {year}"
        )
    if nulled:
        rows = f"{year} rows" if part is None else f"{year} rows that hold it"
        return (
            f"{year} is partial: {what}, so {'its' if one else 'their'} {rows} are "
            "null; allow_partial=True returns the partial values"
        )
    rows = f"{year} rows" if part is None else f"{year} rows of {part}"
    return (
        f"{year} is partial: {what}, so the result has no {rows} for "
        f"{'it' if one else 'them'}"
    )


def _part_text(cell: Incomplete) -> str | None:
    """The keys of a cell as text, such as ``"line 1820"``, or ``None``."""
    if not cell.keys:
        return None
    return ", ".join(f"{dimension} {code}" for dimension, code in cell.keys)


def reported_text(
    provider: Code,
    years: Iterable[int],
    missing: Iterable[Code],
    /,
    *,
    inputs: str = "its inputs",
    nulled: bool,
    listed: int = LISTED,
) -> str:
    """The note on a provider whose value in ``years`` is incomplete while
    ``missing`` have not reported ``inputs``, such as 918's GNI without a
    member's GNI, or 918's attributed ODA before its funders report line
    2102."""
    [label] = _labels("provider", [provider])
    have = "has" if len(set(missing)) == 1 else "have"
    text = (
        f"{label} in {_joined([str(year) for year in sorted(years)])} is "
        f"incomplete: {_providers_text(missing, listed=listed)} {have} not "
        f"reported {inputs}"
    )
    if nulled:
        text += (
            ", so its rows and the totals that hold it are null; "
            "allow_partial=True returns the partial values"
        )
    return text


def overstated_text(
    year: int,
    codes: Iterable[Code],
    /,
    *,
    part: str | None = None,
    nulled: bool,
    listed: int = LISTED,
) -> str:
    """The note on a sum across providers that holds providers which have
    not yet reported ``part``, a part the indicator subtracts, so the sum
    would overstate."""
    have = "has" if len(set(codes)) == 1 else "have"
    text = (
        f"{_providers_text(codes, listed=listed)} reported "
        f"{part or 'a subtracted part'} in {year - 1} and {have} not reported it "
        f"in {year}, and the indicator subtracts it, so "
    )
    if nulled:
        return (
            f"{year} sums across providers are null: {text}a sum would "
            "overstate; allow_partial=True returns the partial values"
        )
    return f"{year} sums across providers overstate: {text}a sum holds them without it"


def _by_part(cells: Iterable[Incomplete]) -> list[tuple[int, str | None, set[Code]]]:
    """Each year and part of ``cells``, with its providers, in order."""
    found: dict[tuple[int, str], tuple[str | None, set[Code]]] = {}
    for cell in cells:
        part = _part_text(cell)
        found.setdefault((cell.year, part or ""), (part, set()))[1].add(cell.provider)
    return [(year, part, codes) for (year, _), (part, codes) in sorted(found.items())]


def _incomplete_text(held: _Held, call: ResolvedCall) -> list[str]:
    """The sentences ``explain()`` writes on a result's incomplete cells."""
    lines = [_part_sentence(part) for part in _last_year_parts(held, call)]
    lines += [
        overstated_text(year, codes, part=part, nulled=not call.allow_partial)
        for year, part, codes in _by_part(held.overstated)
    ]
    if not call.allow_partial:
        own = [cell for cell in held.own - held.waiting if is_own_gap(cell)]
        lines += [
            nulled_text(year, codes, part=part) for year, part, codes in _by_part(own)
        ]
    nulled = frozenset() if call.allow_partial else held.own | held.totals
    lines += _reported_text(held.own | held.totals, nulled=nulled)
    return lines


class Effect(StrEnum):
    """What a part of the table's last year that providers have not yet
    reported does to a result."""

    #: Providers the call shows have no row for it.
    MISSING = "missing"
    #: Providers the call shows have null rows for it.
    NULL = "null"
    #: A sum across providers keeps its value without the providers.
    SHORT = "short"
    #: A sum across providers has no row for it.
    UNSUMMED = "unsummed"
    #: A provider total has no row for it.
    UNTOTALLED = "untotalled"


@dataclass(frozen=True, slots=True)
class LastYearPart:
    """A part of the table's last year that providers have not yet reported,
    such as ``"line 1820"`` or ``None`` for the whole year, its providers,
    what it does to the result, and the provider total it does it to."""

    year: int
    part: str | None
    effect: Effect
    codes: frozenset[Code]
    total: str | None = None


def _last_year_parts(
    held: _Held, call: ResolvedCall, *, warned: bool = False
) -> list[LastYearPart]:
    """The parts of the table's last year that providers have not yet
    reported, in order of year and part, as ``explain()`` names them, or
    as the warning lists them when ``warned``: unless the call allows
    partial values, a part that leaves a row null or a sum short; whatever
    it allows, a part that leaves the result without a row."""
    sources: list[tuple[Iterable[Incomplete], Effect, str | None]] = [
        (held.absent, Effect.MISSING, None)
    ]
    if not call.allow_partial:
        sources.append((held.waiting, Effect.NULL, None))
    if not (warned and call.allow_partial):
        sources.append((held.summed, Effect.SHORT, None))
    sources.append((held.unsummed, Effect.UNSUMMED, None))
    sources += [(cells, Effect.UNTOTALLED, name) for name, cells in held.untotalled]
    found = [
        LastYearPart(year, part, effect, frozenset(codes), total)
        for cells, effect, total in sources
        for year, part, codes in _by_part(cells)
    ]
    order = list(Effect)
    return sorted(
        found,
        key=lambda p: (p.year, p.part or "", order.index(p.effect), p.total or ""),
    )


def _part_sentence(found: LastYearPart) -> str:
    """The ``explain()`` sentence on one part of the table's last year that
    providers have not yet reported."""
    year, part, codes = found.year, found.part, found.codes
    match found.effect:
        case Effect.MISSING | Effect.NULL:
            return unreported_text(
                year, codes, part=part, nulled=found.effect is Effect.NULL
            )
        case Effect.SHORT:
            return partial_text(year, len(codes), part=part)
        case Effect.UNSUMMED:
            return unsummed_text(year, len(codes), part=part, where="the sum")
        case Effect.UNTOTALLED:
            where = f"the {found.total} total"
            return unsummed_text(year, len(codes), part=part, where=where)


def unsummed_text(
    year: int, count: int, /, *, part: str | None = None, where: str
) -> str:
    """The note on a sum or total, ``where``, that has no row of ``part`` of
    the table's last year, since none of the ``count`` providers it sums
    that reported it the year before has reported it yet."""
    providers = "provider" if count == 1 else "providers"
    have = "has" if count == 1 else "have"
    if part is None:
        what = f"reported {year - 1} {have} not reported {year}"
        rows = f"no {year} row"
    else:
        what = f"reported {part} in {year - 1} {have} not reported it in {year}"
        rows = f"no {year} row of it"
    return f"{year} is partial: {count} {providers} that {what}, so {where} has {rows}"


def last_year_warning(
    year: int, parts: list[LastYearPart], /, *, listed: int = LISTED
) -> str:
    """The one warning on the parts of the table's last year ``year`` that
    providers reported the year before and have not reported yet.

    It names each part once, with what it does to the result: missing or
    null rows of providers the call shows, named, and sums and provider
    totals that are short or have no row, with their providers counted. It
    names the first ``listed`` parts and the first ``listed`` providers of
    each, and counts the rest.
    """
    grouped: dict[str, list[LastYearPart]] = {}
    for found in parts:
        grouped.setdefault(found.part or f"all of {year}", []).append(found)
    sentences = []
    for label, found in list(grouped.items())[:listed]:
        effects = [_effect_text(item, listed=listed) for item in found]
        listing = (
            effects[0]
            if len(effects) == 1
            else f"{', '.join(effects[:-1])}, and {effects[-1]}"
        )
        sentences.append(f"{label[0].upper()}{label[1:]} is {listing}.")
    if len(grouped) > listed:
        sentences.append(f"{len(grouped) - listed} more parts are partial too.")
    effects = {found.effect for found in parts}
    if Effect.NULL in effects:
        sentences.append(
            "allow_partial=True returns the partial values of the null rows."
        )
    if effects & {Effect.SHORT, Effect.UNSUMMED, Effect.UNTOTALLED}:
        sentences.append("Show the providers on their own rows to see who reported.")
    return (
        f"{year} is partial: providers that reported a part in {year - 1} have not "
        f"reported it in {year}. {' '.join(sentences)}"
    )


def _effect_text(found: LastYearPart, *, listed: int) -> str:
    """What one part does to the result, with its providers named or
    counted."""
    count = len(found.codes)
    counted = f"{count} {'provider' if count == 1 else 'providers'}"
    match found.effect:
        case Effect.MISSING:
            return f"missing for {_providers_text(found.codes, listed=listed)}"
        case Effect.NULL:
            return f"null for {_providers_text(found.codes, listed=listed)}"
        case Effect.SHORT:
            return f"left out of the sum for {counted}"
        case Effect.UNSUMMED:
            return f"missing from the sum for {counted}"
        case Effect.UNTOTALLED:
            return f"missing from the {found.total} total for {counted}"


def _reported_text(
    cells: Iterable[Incomplete], *, nulled: frozenset[Incomplete]
) -> list[str]:
    """One sentence per provider and missing inputs on the cells a ``Method``
    reported, with the years they hold."""
    runs: dict[tuple[Code, frozenset[Code], str, bool], list[int]] = {}
    for cell in sorted(cells, key=cell_order):
        if not is_own_gap(cell):
            key = (cell.provider, cell.missing, cell.inputs, cell in nulled)
            runs.setdefault(key, []).append(cell.year)
    return [
        reported_text(provider, years, missing, inputs=inputs, nulled=is_null)
        for (provider, missing, inputs, is_null), years in runs.items()
    ]


def _labels(dimension: Dimension, codes: Iterable[Code]) -> list[str]:
    known = names(dimension)
    return sorted(f"{known.get(code, code)} ({code})" for code in codes)


def _providers_text(codes: Iterable[Code], *, listed: int) -> str:
    """The providers in order of name, the first ``listed`` named and the
    rest counted."""
    labels = _labels("provider", set(codes))
    if len(labels) > listed:
        return f"{', '.join(labels[:listed])} and {len(labels) - listed} more"
    return _joined(labels)


def _joined(items: list[str]) -> str:
    return items[0] if len(items) == 1 else f"{', '.join(items[:-1])} and {items[-1]}"


def records(
    cursor: DuckDBPyConnection,
    indicator: Indicator,
    call: ResolvedCall,
    factors: Factors,
    /,
    *,
    columns: tuple[str, ...] = (),
    releases: Releases,
) -> Combined:
    """Return the converted rows a ``Sum`` or ``Method`` indicator adds up.

    The rows cover every code in ``call.members`` and every member of the
    call's groups. A ``Sum`` adds ``columns`` from the table view. A
    ``Method`` returns every column its function returned.

    Raises:
        UnsupportedQueryError: The indicator is a ``Ratio`` or ``Derived``, or
            its rows lack one of ``columns``. Also as ``combine``.
    """
    if isinstance(indicator.kind, Ratio | Derived):
        raise UnsupportedQueryError(
            f"records() needs a summed indicator; {indicator.id} is a "
            f"{type(indicator.kind).__name__}"
        )
    _check_call(cursor, indicator, call, releases)
    _warn_caveats(indicator, call)

    if isinstance(indicator.kind, Sum):
        view = cursor.table(indicator.binding.table)
        _check_columns(indicator, columns, view.columns)
    found = stage1(
        cursor, indicator, call, columns=columns, keep_all=True, releases=releases
    ).rows
    if isinstance(indicator.kind, Method):
        _check_columns(indicator, columns, found.columns)
    stage2, fallback = _in_units(cursor, indicator, found, call, factors)
    return Combined(stage2.to_arrow_table(), fallback)


def _in_units(
    cursor: DuckDBPyConnection,
    indicator: Indicator,
    rows: DuckDBPyRelation,
    call: ResolvedCall,
    factors: Factors,
) -> tuple[DuckDBPyRelation, frozenset[int]]:
    """Stage 2: money converted to the call's units, and a count, such as
    population, left as it is with null unit columns."""
    if indicator.is_money:
        return converted(cursor, rows, call, factors)
    select = [ident(name) for name in rows.columns if name != "value"]
    select += [
        "CAST(NULL AS VARCHAR) AS currency",
        "CAST(NULL AS VARCHAR) AS prices",
        "CAST(NULL AS BIGINT) AS base_year",
        "value",
    ]
    return rows.select(", ".join(select)), frozenset()


# Checks ----------------------------------------------------------------------


def _check_columns(
    indicator: Indicator, columns: tuple[str, ...], available: list[str]
) -> None:
    absent = sorted(set(columns) - set(available))
    if absent:
        raise UnsupportedQueryError(
            f"{indicator.id} has no columns {absent}; its rows hold "
            f"{', '.join(available)}"
        )


def check_left_out(indicator: Indicator, call: ResolvedCall, /) -> None:
    """Raise when the call filters a dimension by a code that a ``NotIn`` in
    the row filters of the indicator, or of an indicator it combines, leaves
    out.

    Such a code would return no rows and look like missing data. The message
    gives the rule's reason. A rule with ``when`` raises only when the call
    names both a code it leaves out and a code of its scope, such as provider
    918 and line 106.

    Raises:
        UnsupportedQueryError: The call names a left-out code.
    """
    for part in walk(indicator):
        columns = {column: d for d, column in part.binding.dimensions.items()}
        for rule in (*part.binding.rows, *part.rows):
            if not isinstance(rule, NotIn) or rule.column not in columns:
                continue
            named = _named_codes(call, columns, rule)
            if not named:
                continue
            where = ""
            if rule.when is not None:
                # Default providers keep the codes a scoped rule leaves out,
                # since it leaves out only some of their rows.
                scope = _named_codes(call, columns, rule.when)
                if not scope or columns[rule.column] not in call.rows:
                    continue
                where = f" on {columns[rule.when.column]} {_listed(scope)}"
            reason = f": {rule.reason}" if rule.reason else ""
            raise UnsupportedQueryError(
                f"{indicator.id} leaves out {columns[rule.column]} {_listed(named)}"
                f"{where}{reason}"
            )


def _named_codes(
    call: ResolvedCall, columns: Mapping[str, Dimension], rule: In | NotIn
) -> set[Code]:
    """The codes of ``rule`` the call names in the dimension of its column."""
    dimension = columns.get(rule.column)
    if dimension is None:
        return set()
    return set(call.members.get(dimension, frozenset())) & set(rule.values)


def _listed(codes: Iterable[Code]) -> str:
    return ", ".join(map(str, sorted(codes, key=str)))


def _check_call(
    cursor: DuckDBPyConnection,
    indicator: Indicator,
    call: ResolvedCall,
    releases: Releases,
) -> None:
    check_dimensions(indicator, call)
    check_left_out(indicator, call)

    shown = {*call.by, *(group.dimension for group in call.groups)}
    for dimension, codes in call.rows.items():
        if dimension not in shown and codes != call.members.get(dimension):
            raise ValueError(
                f"rows narrows {dimension!r}, which is neither in by nor a group's"
            )

    groups = {group.key: group for group in call.groups}
    for key in call.totals:
        group = groups[key]
        name = group.name
        if group.dimension not in call.by:
            raise UnsupportedQueryError(
                f"oda.total({name!r}) needs {group.dimension!r} in by, or its "
                "total would be added to its members"
            )
        aggregates = sorted(
            {
                code
                for codes in group.members.values()
                for code in codes
                if is_aggregate(group.dimension, code)
            },
            key=str,
        )
        if aggregates:
            raise UnsupportedQueryError(
                f"group {name!r} holds aggregate codes {aggregates}, whose rows "
                "already hold their members'; its total cannot be summed"
            )

    for dimension in _AGGREGATE_DIMENSIONS:
        codes = call.members.get(dimension, frozenset())
        aggregates = sorted((c for c in codes if is_aggregate(dimension, c)), key=str)
        if dimension not in call.by and aggregates and len(codes) > 1:
            raise UnsupportedQueryError(
                f"{dimension} aggregate codes {aggregates} hold other codes' rows, "
                f"so they cannot be summed with them; add {dimension!r} to by"
            )

    _check_eu_share(cursor, indicator, call, releases)

    if indicator.is_money:
        if not call.units:
            raise ValueError(f"{indicator.id} needs at least one unit")
    elif call.units:
        why = (
            "a ratio, computed in current USD so that each provider's deflator "
            "leaves it unchanged"
            if isinstance(indicator.kind, Ratio)
            else f"a count of {indicator.measure}, not money, so it has no "
            "currency or prices"
        )
        raise UnsupportedQueryError(f"{indicator.id} is {why}; it takes no unit=")


def _check_eu_share(
    cursor: DuckDBPyConnection,
    indicator: Indicator,
    call: ResolvedCall,
    releases: Releases,
) -> None:
    """Refuse a sum of EU Institutions (918) with a provider that pays it, on
    an indicator whose providers' figures hold what they pay 918.

    Such a sum counts that money twice. Other tables, and indicators that
    attribute 918's flows to its funders, sum them freely. The check covers
    every sum, of named or default providers; a default set leaves 918 out
    of a sum on such an indicator (``_context._default_providers``), so it
    raises only on providers the caller named.
    """
    holding = [b for b in bindings(indicator) if b.members_hold_eu_share]
    if not holding:
        return
    sums = [entry for entry in _provider_sums(call) if EU_INSTITUTIONS in entry[2]]
    if not sums:
        return

    paid = _eu_payers(cursor, holding, releases)
    clashes: dict[GroupKey | None, set[Code]] = {}
    for key, years, codes in sums:
        if found := codes & _payers(paid, years):
            clashes.setdefault(key, set()).update(found)
    reason = (
        "which paid EU Institutions in a year of the call, as EU members or "
        f"through DAC1 line {_EU_PAYMENT_LINE}; their figures hold those "
        "payments as their own multilateral ODA, so the sum counts that money "
        "twice"
    )
    for key, codes in clashes.items():
        listed = sorted(codes, key=str)
        if key is None:
            raise UnsupportedQueryError(
                f"the providers summed hold EU Institutions (918) and {listed}, "
                f"{reason}; add 'provider' to by"
            )
        raise UnsupportedQueryError(
            f"group {key[1]!r} holds EU Institutions (918) and providers "
            f"{listed}, {reason}"
        )


def _provider_sums(
    call: ResolvedCall,
) -> Iterator[tuple[GroupKey | None, Iterable[int] | None, frozenset[Code]]]:
    """The sums of providers a call holds, each with the years it covers
    (``None`` for every year): the member rows outside ``by`` (key ``None``),
    then each provider total. The member rows cover the providers named
    directly, or the default set when the call names none.
    """
    direct = call.rows.get("provider", call.members.get("provider"))
    if direct is not None and "provider" not in call.by:
        years = sorted(
            {
                year
                for group in call.groups
                if group.shown and group.dimension == "provider"
                for year in group.members
            }
        )
        if not years:
            yield None, call.years, direct
        for year, codes in (shown_codes(call, "provider", years) or {}).items():
            yield None, (year,), codes
    groups = {group.key: group for group in call.groups}
    for key in call.totals:
        if key[0] == "provider":
            for year, codes in groups[key].members.items():
                yield key, (year,), codes


def _eu_payers(
    cursor: DuckDBPyConnection, bindings: Iterable[Binding], releases: Releases
) -> dict[int, set[Code]]:
    """The providers other than 918 that report DAC1 line 2102 in each year,
    when a binding reads DAC1.

    In DAC1's last year, a provider with line 2102 the year before counts
    too, since most have not reported it yet.
    """
    binding = next((b for b in bindings if b.table == "dac1"), None)
    if binding is None:
        return {}
    provider, year, line = (
        ident(binding.dimensions[name]) for name in ("provider", "year", "line")
    )
    rows = cursor.sql(
        f"SELECT DISTINCT {provider}, CAST({year} AS BIGINT) FROM dac1 "
        f"WHERE {line} = {_EU_PAYMENT_LINE} AND {provider} != {EU_INSTITUTIONS}"
    ).fetchall()
    last = table_last_year(cursor, binding, releases=releases)
    paid: dict[int, set[Code]] = {}
    for code, reported in rows:
        paid.setdefault(reported, set()).add(code)
    if last is not None:
        paid.setdefault(last, set()).update(paid.get(last - 1, ()))
    return paid


def _payers(paid: Mapping[int, set[Code]], years: Iterable[int] | None) -> set[Code]:
    """The providers that pay 918 in any of ``years``: the EU members then,
    and those in ``paid``."""
    found: set[Code] = set(eu_members(years))
    for year in paid if years is None else years:
        found |= paid.get(year, set())
    return found


def _warn_caveats(indicator: Indicator, call: ResolvedCall) -> None:
    for caveat in caveats_for(indicator, call):
        if is_scoped(caveat):
            warnings.warn(
                f"{indicator.id}: {caveat.text}", OdaDataWarning, stacklevel=3
            )


def caveats_for(indicator: Indicator, call: ResolvedCall, /) -> tuple[Caveat, ...]:
    """The caveats of the indicator and its parts that concern the call, once
    each.

    An unscoped caveat concerns every call. A scoped one concerns a call that
    reads its basis in one of its years; ``combine`` and ``records`` warn of
    those.
    """
    return tuple(dict.fromkeys(_caveats(indicator, call)))


def is_scoped(caveat: Caveat, /) -> bool:
    """Whether a caveat names a basis or years, so it warns."""
    return caveat.basis is not None or caveat.years is not None


def _caveats(indicator: Indicator, call: ResolvedCall) -> Iterator[Caveat]:
    """The caveats of the indicator and its parts that the call reads, each
    part with the call it reads (a ratio's denominator its own basis)."""
    for caveat in indicator.caveats:
        if _concerns(caveat, indicator, call):
            yield caveat
    match indicator.kind:
        case Ratio():
            for part, part_call in ratio_parts(indicator, call):
                yield from _caveats(part, part_call)
        case Derived(plus=plus, minus=minus):
            for part in (*plus, *minus):
                yield from _caveats(part, call)


def _concerns(caveat: Caveat, indicator: Indicator, call: ResolvedCall) -> bool:
    """Whether the call reads the caveat's basis in one of its years."""
    if not is_scoped(caveat):
        return True
    rule = indicator.binding.bases.get(call.basis)
    if rule is None:
        return False
    first, last = caveat.years or (None, None)
    for basis, start, stop in basis_spans(call.basis, rule):
        if caveat.basis not in (None, basis):
            continue
        end = None if stop is None else stop - 1
        low = max((y for y in (start, first) if y is not None), default=None)
        high = min((y for y in (end, last) if y is not None), default=None)
        if call.years is None:
            if low is None or high is None or low <= high:
                return True
        elif any(
            (low is None or low <= year) and (high is None or year <= high)
            for year in call.years
        ):
            return True
    return False


def _materialize(
    cursor: DuckDBPyConnection, rows: DuckDBPyRelation
) -> DuckDBPyRelation:
    """Sum rows to the grain of their key columns and read the result once,
    so totals and member rows do not each scan the table again."""
    keys = ", ".join(ident(name) for name in rows.columns if name != "value")
    summed = rows.aggregate(f"{keys}, sum(value) AS value", keys)
    return cursor.from_arrow(summed.to_arrow_table())


# Ratio -----------------------------------------------------------------------


def _ratio_rows(
    cursor: DuckDBPyConnection,
    indicator: Indicator,
    call: ResolvedCall,
    releases: Releases,
) -> Rows:
    """Numerators paired with denominators at member grain, with the cells
    the parts' ``Method``s report.

    Columns: the stage 1 keys, ``basis`` (the numerator's concrete basis in
    each year), ``num`` and ``den``. A member-year with a denominator and no
    numerator rows has a null numerator, which a member row reads as 0 and a
    total leaves out. A member-year with no denominator has no row.
    """
    (numerator, num_call), (denominator, den_call) = ratio_parts(indicator, call)
    num_read = stage1(cursor, numerator, num_call, releases=releases)
    den_read = stage1(cursor, denominator, den_call, releases=releases)
    num_rows, den_rows = num_read.rows, den_read.rows

    keys = [name for name in num_rows.columns if name not in {"basis", "line", "value"}]
    listed = ", ".join(map(ident, keys))
    num = num_rows.aggregate(f"{listed}, sum(value) AS num", listed)
    den = den_rows.aggregate(f"{listed}, sum(value) AS den", listed)
    on = " AND ".join(f"d.{ident(k)} IS NOT DISTINCT FROM n.{ident(k)}" for k in keys)
    paired = den.set_alias("d").join(num.set_alias("n"), on, how="left")
    select = [f"d.{ident(k)} AS {ident(k)}" for k in keys]
    select += [
        f"{basis_sql(numerator, num_call, year='d.year')} AS basis",
        "n.num AS num",
        "d.den AS den",
    ]
    return Rows(
        cursor.from_arrow(paired.select(", ".join(select)).to_arrow_table()),
        num_read.incomplete | den_read.incomplete,
        num_read.notes + den_read.notes,
    )


# Result ----------------------------------------------------------------------


def _assemble(
    cursor: DuckDBPyConnection,
    indicator: Indicator,
    call: ResolvedCall,
    rows: DuckDBPyRelation,
    cells: frozenset[Incomplete],
    *,
    years: list[int],
    reads: list[int],
    last: int | None,
    subtracted: frozenset[Incomplete],
    fixed: Mapping[Dimension, Code],
) -> tuple[pa.Table, _Held]:
    """Member rows, then each total, and the incomplete cells behind each.

    ``years`` are the years ``rows`` hold, ``reads`` those and the table's
    last year ``last`` when the call asks for it, ``last`` the year of which
    a sum across providers warns, and ``subtracted`` the cells of parts the
    indicator subtracts. ``_completeness.outcomes`` decides what each cell
    does; this applies it.
    """
    always = [name for name in SPLIT_COLUMNS if name in rows.columns]
    keys = list(dict.fromkeys([*call.by, *always]))
    own = own_years(call, years)
    value = "sum(value)"
    ratio = isinstance(indicator.kind, Ratio)
    if ratio:
        # A sum leaves out a member with no numerator, as a total does.
        paired = "num IS NOT NULL"
        if own:
            paired += f" OR year IN ({literals(sorted(own))})"
        value = (
            f"100 * coalesce(sum(num), 0) / nullif(sum(den) FILTER (WHERE {paired}), 0)"
        )
    decided = outcomes(
        cells,
        call,
        own=own_years(call, reads),
        years=reads,
        last=last,
        ratio=ratio,
        subtracted=subtracted,
    )
    nulling = frozenset(
        cell
        for cell, outcome in decided.items()
        if outcome in {Outcome.NULL_ROW, Outcome.WARN_ROW, Outcome.NULL_SUM}
    )
    overstating = frozenset(
        cell
        for cell in subtracted & nulling
        if decided[cell] is Outcome.NULL_SUM and is_own_gap(cell)
    )
    warned = [cell for cell, outcome in decided.items() if outcome is Outcome.WARN_SUM]
    rows = holding(cursor, rows, nulling)

    members = _member_rows(cursor, call, rows, years)
    summed = _aggregate(members, keys, value, "bool_or(incomplete) AS partial")
    tables = [
        _shape(
            summed,
            indicator,
            call,
            flag=_member_flag(call),
            value=_unless_partial(call),
        )
    ]
    own_held = held(cursor, members, nulling - overstating)
    unreported = {cell for cell in nulling if decided[cell] is Outcome.WARN_ROW}
    found = _Held(
        own=own_held,
        summed=held(cursor, _kept(members, keys, call), warned, providers=False),
        overstated=held(cursor, members, overstating),
        waiting=own_held & unreported,
        absent=frozenset(unreported - own_held),
    )

    # A not-yet-reported cell that a sum across providers, or a provider
    # total, holds and that no row of it holds: none of the providers it
    # sums has a row for the part.
    waiting = {cell for cell in cells if not_yet_reported(cell, cells, last)}
    summing = {
        cell
        for cell in waiting
        if decided[cell] in {Outcome.WARN_SUM, Outcome.NULL_SUM}
    }
    split = tuple(dict.fromkeys(d for b in bindings(indicator) for d in b.reporting))
    groups = {group.key: group for group in call.groups}
    in_totals: set[Incomplete] = set()
    untotalled = []
    for key in call.totals:
        group = groups[key]
        table, inside, missing = _total(
            cursor,
            indicator,
            call,
            rows,
            group,
            keys,
            value,
            cells,
            nulling,
            waiting=waiting,
            split=split,
            fixed=fixed,
        )
        tables.append(table)
        in_totals |= inside
        if missing:
            untotalled.append((group.name, missing))
    return pa.concat_tables(tables), replace(
        found,
        totals=frozenset(in_totals),
        unsummed=unheld(
            cursor,
            members,
            summing,
            split=split,
            fixed=fixed,
            named=call.members,
            providers=False,
        ),
        untotalled=tuple(untotalled),
    )


def _kept(
    members: DuckDBPyRelation, keys: list[str], call: ResolvedCall
) -> DuckDBPyRelation:
    """The member rows whose summed row keeps its value: all of them when the
    call allows partial values, else those of a summed row that holds no
    incomplete row. A sum the policy nulls says why, and does not also warn
    that it covers only the providers that have reported."""
    if call.allow_partial:
        return members
    window = f"PARTITION BY {', '.join(map(ident, keys))}" if keys else ""
    return members.select(f"*, bool_or(incomplete) OVER ({window}) AS nulled").filter(
        "NOT nulled"
    )


def _unless_partial(call: ResolvedCall) -> str:
    """SQL for a summed row's value: null where it holds an incomplete cell
    (``partial``), unless the call allows partial values."""
    return (
        "value" if call.allow_partial else "CASE WHEN partial THEN NULL ELSE value END"
    )


def _member_rows(
    cursor: DuckDBPyConnection,
    call: ResolvedCall,
    rows: DuckDBPyRelation,
    years: list[int],
) -> DuckDBPyRelation:
    """The rows the result shows as their own: in each dimension of
    ``call.rows``, the codes ``shown_codes`` gives for the row's year."""
    members = rows
    for dimension in call.rows:
        if dimension in rows.columns:
            shown = shown_codes(call, dimension, years)
            assert shown is not None  # the dimension is in call.rows
            members = _within(cursor, members, dimension, shown)
    return members


def _total(
    cursor: DuckDBPyConnection,
    indicator: Indicator,
    call: ResolvedCall,
    rows: DuckDBPyRelation,
    group: ResolvedGroup,
    keys: list[str],
    value: str,
    cells: frozenset[Incomplete],
    nulling: frozenset[Incomplete],
    *,
    waiting: set[Incomplete],
    split: tuple[Dimension, ...],
    fixed: Mapping[Dimension, Code],
) -> tuple[pa.Table, frozenset[Incomplete], frozenset[Incomplete]]:
    """One group's total rows, summed over its members in each year, the
    incomplete cells they hold, and for a provider total the cells of
    ``waiting``, those not yet reported, of its members that no total row
    holds at the grain of the ``split`` dimensions (``unheld``).

    A provider total row holds the cells of the members it sums in its year
    whose keys match the row's, whether or not the member has rows, and is
    null where it holds one. Any other total row is null where it holds a row
    marked incomplete, as a member row is. Neither is null when the call
    allows partial values.
    """
    dimension = group.dimension
    years = _years(rows)
    check_years(group, years)
    inside = _within(cursor, rows, dimension, group.members)
    if isinstance(indicator.kind, Ratio):
        inside = inside.filter("num IS NOT NULL")

    if dimension == "provider":
        members = [
            cell
            for cell in cells
            if cell.year in years and cell.provider in group.members[cell.year]
        ]
        # The member rows' own marks give way to the members' cells.
        unmarked = [ident(name) for name in inside.columns if name != "incomplete"]
        inside = holding(
            cursor, inside.select(", ".join(unmarked)), members, providers=False
        )
        found = held(cursor, inside, members, providers=False)
        missing = unheld(
            cursor,
            inside,
            [c for c in waiting if c.provider in group.members.get(c.year, ())],
            split=split,
            fixed=fixed,
            named=call.members,
            providers=False,
        )
    else:
        found = held(cursor, inside, nulling)
        missing = frozenset()
    summed = _aggregate(
        inside,
        [k for k in keys if k != dimension],
        value,
        "bool_or(incomplete) AS partial",
    )
    table = _shape(
        summed,
        indicator,
        call,
        flag="TRUE",
        labels={dimension: group.name},
        value=_unless_partial(call),
    )
    return table, found, missing


def _years(rows: DuckDBPyRelation) -> list[int]:
    """The years ``rows`` hold, sorted."""
    found = rows.select("year").distinct().fetchall()
    return sorted(year for (year,) in found if year is not None)


def _within(
    cursor: DuckDBPyConnection,
    rows: DuckDBPyRelation,
    dimension: Dimension,
    members: Mapping[int, frozenset[Code]],
) -> DuckDBPyRelation:
    """The rows whose ``dimension`` code is in ``members`` for the row's year."""
    pairs = [(year, code) for year, codes in members.items() for code in codes]
    codes = [code for _, code in pairs]
    code_type = pa.int64() if all(isinstance(c, int) for c in codes) else pa.string()
    membership = pa.table(
        {
            "year": pa.array([year for year, _ in pairs], pa.int64()),
            "code": pa.array(codes, code_type),
        }
    )
    return rows.set_alias("r").join(
        cursor.from_arrow(membership).set_alias("m"),
        f"r.{ident(dimension)} = m.code AND r.year = m.year",
        how="semi",
    )


def _member_flag(call: ResolvedCall) -> str:
    """SQL for ``is_aggregate`` on member rows."""
    checks = []
    for dimension in _AGGREGATE_DIMENSIONS:
        codes = call.members.get(dimension)
        if dimension in call.by:
            checks.append(f"coalesce({aggregate_sql(dimension)}, FALSE)")
        elif codes and all(is_aggregate(dimension, code) for code in codes):
            return "TRUE"
    return " OR ".join(checks) or "FALSE"


def _aggregate(
    rows: DuckDBPyRelation, keys: list[str], value: str, extra: str = ""
) -> DuckDBPyRelation:
    """Sum ``rows`` by ``keys``. ``value`` names the summed value and
    ``extra`` adds aliased aggregates."""
    listed = ", ".join(map(ident, keys))
    aggregates = f"{value} AS value" + (f", {extra}" if extra else "")
    return rows.aggregate(f"{listed}, {aggregates}" if listed else aggregates, listed)


def _shape(
    rows: DuckDBPyRelation,
    indicator: Indicator,
    call: ResolvedCall,
    *,
    flag: str,
    labels: Mapping[str, str] | None = None,
    value: str = "value",
) -> pa.Table:
    """Project summed rows onto the result columns and read them."""
    labels = labels or {}
    ratio = isinstance(indicator.kind, Ratio)
    select = []
    for dimension in call.by:
        if dimension in labels:
            select.append(f"{literal(labels[dimension])} AS {ident(dimension)}")
        elif dimension == "year":
            select.append("CAST(year AS BIGINT) AS year")
        else:
            select.append(f"CAST({ident(dimension)} AS VARCHAR) AS {ident(dimension)}")
    select += [
        f"CAST({split} AS VARCHAR) AS {split}"
        for split in ("line", "flow")
        if split in rows.columns and split not in call.by
    ]
    select += [
        f"{literal(indicator.id)} AS indicator",
        f"CAST({int(indicator.version)} AS BIGINT) AS indicator_version",
        "basis",
    ]
    if ratio:
        select += [
            "CAST(NULL AS VARCHAR) AS currency",
            "CAST(NULL AS VARCHAR) AS prices",
            "CAST(NULL AS BIGINT) AS base_year",
        ]
    else:
        select += ["currency", "prices", "CAST(base_year AS BIGINT) AS base_year"]
    select += [
        f"{literal('percent' if ratio else 'millions')} AS scale",
        f"{flag} AS is_aggregate",
        f"CAST({value} AS DOUBLE) AS value",
    ]
    # Order by the source columns, so codes sort as numbers (4 before 12).
    order = [
        f"rows.{ident(name)}"
        for name in dict.fromkeys((*call.by, "line", "flow", "basis", *UNIT_COLUMNS))
        if name in rows.columns and name not in labels
    ]
    sql = f"SELECT {', '.join(select)} FROM rows"
    if order:
        sql += " ORDER BY " + ", ".join(order)
    shaped = rows.query("rows", sql)
    assert shaped is not None  # a SELECT always returns a relation
    return shaped.to_arrow_table()

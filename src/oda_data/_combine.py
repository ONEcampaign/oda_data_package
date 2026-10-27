"""Combine compiled rows into a call's result.

``combine`` runs any indicator kind over a resolved call and returns one
Arrow table in the result shape. It builds on the compiler's stages in
``_compile``:

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
  ``Method``, yields ``Incomplete`` cells, the provider-years its value covers
  only in part. A provider that reported to one of the leaf's tables in an
  earlier year and has no row in that year or any later one has stopped
  (``_stopped``). So has a provider with no row for a ratio's ``Sum`` side in
  that year or later, as GNI without ODA (``_side_stopped``). In the table's
  last year, a provider that has rows for the leaf in the year before and
  none in it has not yet reported, as when a preliminary year holds only some
  members' detail lines (``_dropped``). The leaf reads those two years alone.
  A ``Method`` adds the cells it reports
  itself, such as EU Institutions' (918) GNI in a year after an EU member's
  last GNI. A ``Derived`` or ``Ratio`` holds the cells of its parts. A
  provider that has not yet reported, or that skips a year and reports again,
  counts as zero.
- Unless the call allows partial values, a row is null where it holds an
  incomplete cell, and a provider total is null in a year in which a
  member's cell is. A row sums providers when ``provider`` is outside ``by``
  and the call shows more than one provider in the row's year. Such a row
  keeps its value where a provider that reported the year before is missing
  from the table's last year, and warns of it, since the sum then covers only
  the providers that have reported. It is null where it holds a ``Method``'s
  cell, which can overstate it. A ratio pairs each member's numerator with
  its denominator, and a member with no row on one side leaves its pair out
  of a total.

``records`` returns stage 2 for ``Sum`` and ``Method`` indicators.

``_combine`` works from ``ResolvedCall`` alone and does not import ``_resolve``
or ``_groups``.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

import pyarrow as pa

from oda_data._compile import (
    SPLIT_COLUMNS,
    UNIT_COLUMNS,
    basis_spans,
    basis_sql,
    check_basis,
    check_dimensions,
    converted,
    filtered,
    ident,
    literal,
    literals,
)
from oda_data._errors import BasisRangeError, OdaDataWarning, UnsupportedQueryError
from oda_data._model import (
    Derived,
    Headline,
    In,
    Incomplete,
    Method,
    NotIn,
    Ratio,
    Sum,
)
from oda_data._names import (
    EU_INSTITUTIONS,
    PROVIDER_AGGREGATE_FROM,
    aggregate_codes,
    eu_members,
    is_aggregate,
    names,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Mapping, Sequence

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
    from oda_data._types import Basis, Code, Dimension, TableName

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
            partial values, and those a sum across providers warns of.
        notes: Sentences for ``explain()`` on the incomplete cells, then the
            notes of each ``Method`` part, such as the EU members counted as
            zero in 918's GNI.
    """

    table: pa.Table
    fallback_providers: frozenset[int]
    incomplete: tuple[Incomplete, ...] = ()
    notes: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class _Rows:
    """Rows read for a call, with the cells its ``Method`` parts report.

    Args:
        rows: Stage 1 rows, or for a ratio its numerator and denominator
            pairs.
        incomplete: The ``Incomplete`` cells each ``Method`` part returned.
        notes: Each ``Method`` part's notes.
    """

    rows: DuckDBPyRelation
    incomplete: frozenset[Incomplete] = frozenset()
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
            year before.
    """

    own: frozenset[Incomplete] = frozenset()
    totals: frozenset[Incomplete] = frozenset()
    summed: frozenset[Incomplete] = frozenset()


def tables_for(indicator: Indicator, /) -> tuple[TableName, ...]:
    """Return the tables a call on ``indicator`` reads, for ``Tables.cursor``.

    The cursor ``combine`` and ``records`` take must hold a view for each.
    """
    tables = dict.fromkeys(binding.table for binding in _bindings(indicator))
    for part in _leaves(indicator):
        if isinstance(part.kind, Method):
            tables |= dict.fromkeys(part.kind.tables)
    return tuple(tables)


def combine(
    cursor: DuckDBPyConnection,
    indicator: Indicator,
    call: ResolvedCall,
    factors: Factors,
    /,
) -> Combined:
    """Compute an indicator for a resolved call.

    Emits each scoped caveat of the indicator, and of its parts, that
    concerns a basis and year the call reads, as an ``OdaDataWarning``, and,
    unless the call allows partial values, one for a sum across providers
    some of which have not reported the table's last year.

    Args:
        cursor: A cursor holding a view for each of ``tables_for(indicator)``,
            and ``broad_sectors`` when the call uses that dimension.
        indicator: Any indicator.
        call: The resolved call. Codes in ``members`` but outside ``rows``
            feed group totals and are dropped from the member rows. ``units``
            is empty for a ``Ratio`` and holds at least one unit otherwise.
        factors: The Context's factor source.

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
    _check_call(cursor, indicator, call)
    _warn_caveats(indicator, call)

    if isinstance(indicator.kind, Ratio):
        read = _ratio_rows(cursor, indicator, call)
        rows, fallback = read.rows, frozenset[int]()
    else:
        read = _stage1(cursor, indicator, call)
        stage2, fallback = _in_units(cursor, indicator, read.rows, call, factors)
        rows = _materialize(cursor, stage2)

    years = _years(rows)
    last = _last_year(cursor, indicator)
    cells = read.incomplete | _reporting_gaps(cursor, indicator, call, years, last)
    table, held = _assemble(
        cursor, indicator, call, rows, cells, years=years, last=last
    )
    if held.summed and not call.allow_partial:
        for text in _summed_text(held.summed, last):
            warnings.warn(f"{indicator.id}: {text}", OdaDataWarning, stacklevel=2)
    incomplete = held.own | held.totals | held.summed
    return Combined(
        table,
        fallback,
        tuple(sorted(incomplete, key=_cell_order)),
        (*_incomplete_text(held, call, last), *read.notes),
    )


def partial_text(year: int, count: int, /) -> str:
    """The note on a sum across providers some of which reported the year
    before the table's last year and have not reported it."""
    providers = "provider" if count == 1 else "providers"
    return (
        f"{year} is partial: {count} {providers} that reported {year - 1} "
        f"{'has' if count == 1 else 'have'} not reported {year}, so its sum "
        "covers only those that have; add 'provider' to by to see who reported"
    )


def nulled_text(year: int, codes: Iterable[Code], /, *, listed: int = LISTED) -> str:
    """The note on providers whose own row is null in a partial last year.

    It names the first ``listed`` providers, in order of name, and counts the
    rest.
    """
    have = "has" if len(set(codes)) == 1 else "have"
    return (
        f"{year} rows are null for {_providers_text(codes, listed=listed)}, which "
        f"reported a part of the indicator in {year - 1} and {have} not reported "
        f"it in {year}; allow_partial=True returns the partial values"
    )


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


def _incomplete_text(held: _Held, call: ResolvedCall, last: int | None) -> list[str]:
    """The sentences ``explain()`` writes on a result's incomplete cells."""
    lines = _summed_text(held.summed, last)
    if not call.allow_partial:
        own: dict[int, set[Code]] = {}
        for cell in held.own:
            if _is_own_gap(cell):
                own.setdefault(cell.year, set()).add(cell.provider)
        lines += [nulled_text(year, codes) for year, codes in sorted(own.items())]
    nulled = frozenset() if call.allow_partial else held.own | held.totals
    lines += _reported_text(held.own | held.totals, nulled=nulled)
    return lines


def _summed_text(summed: frozenset[Incomplete], last: int | None) -> list[str]:
    """The sentence on a sum across providers some of which have not reported
    the table's last year."""
    if not summed or last is None:
        return []
    return [partial_text(last, len({cell.provider for cell in summed}))]


def _reported_text(
    cells: Iterable[Incomplete], *, nulled: frozenset[Incomplete]
) -> list[str]:
    """One sentence per provider and missing inputs on the cells a ``Method``
    reported, with the years they hold."""
    runs: dict[tuple[Code, frozenset[Code], str, bool], list[int]] = {}
    for cell in sorted(cells, key=_cell_order):
        if not _is_own_gap(cell):
            key = (cell.provider, cell.missing, cell.inputs, cell in nulled)
            runs.setdefault(key, []).append(cell.year)
    return [
        reported_text(provider, years, missing, inputs=inputs, nulled=is_null)
        for (provider, missing, inputs, is_null), years in runs.items()
    ]


def _is_own_gap(cell: Incomplete) -> bool:
    """Whether the cell's provider itself did not report. Other cells name the
    members or funders its value sums."""
    return cell.missing == {cell.provider}


def _cell_order(cell: Incomplete) -> tuple[str, int, tuple[str, ...]]:
    return (str(cell.provider), cell.year, tuple(sorted(map(str, cell.missing))))


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
    _check_call(cursor, indicator, call)
    _warn_caveats(indicator, call)

    if isinstance(indicator.kind, Sum):
        view = cursor.table(indicator.binding.table)
        _check_columns(indicator, columns, view.columns)
    stage1 = _stage1(cursor, indicator, call, columns=columns, keep_all=True).rows
    if isinstance(indicator.kind, Method):
        _check_columns(indicator, columns, stage1.columns)
    stage2, fallback = _in_units(cursor, indicator, stage1, call, factors)
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
    cursor: DuckDBPyConnection, indicator: Indicator, call: ResolvedCall
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

    _check_eu_share(cursor, indicator, call)

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
    cursor: DuckDBPyConnection, indicator: Indicator, call: ResolvedCall
) -> None:
    """Refuse a sum of EU Institutions (918) with a provider that pays it, on
    an indicator whose providers' figures hold what they pay 918.

    Such a sum counts that money twice. Other tables, and indicators that
    attribute 918's flows to its funders, sum them freely.
    """
    bindings = [b for b in _bindings(indicator) if b.members_hold_eu_share]
    if not bindings:
        return
    sums = [entry for entry in _provider_sums(call) if EU_INSTITUTIONS in entry[2]]
    if not sums:
        return

    paid = _eu_payers(cursor, bindings)
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
    """The sums of providers the caller chose, each with the years it covers
    (``None`` for every year): the member rows outside ``by`` (key ``None``),
    then each provider total.

    Named providers only. A default is the engine's choice.
    """
    direct = call.rows.get("provider")
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
        for year, codes in (_shown_codes(call, "provider", years) or {}).items():
            yield None, (year,), codes
    groups = {group.key: group for group in call.groups}
    for key in call.totals:
        if key[0] == "provider":
            for year, codes in groups[key].members.items():
                yield key, (year,), codes


def _eu_payers(
    cursor: DuckDBPyConnection, bindings: Iterable[Binding]
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
    [(last,)] = cursor.sql(f"SELECT max(CAST({year} AS BIGINT)) FROM dac1").fetchall()
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
            for part, part_call in _ratio_parts(indicator, call):
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


def walk(indicator: Indicator, /) -> Iterator[Indicator]:
    """Yield the indicator and every indicator it combines, depth first."""
    yield indicator
    match indicator.kind:
        case Ratio(numerator=numerator, denominator=denominator):
            yield from walk(numerator)
            yield from walk(denominator)
        case Derived(plus=plus, minus=minus):
            for part in (*plus, *minus):
                yield from walk(part)


def _leaves(indicator: Indicator) -> Iterator[Indicator]:
    """Yield the indicators that read rows: ``Sum`` and ``Method``."""
    for part in walk(indicator):
        if isinstance(part.kind, Sum | Method):
            yield part


def _bindings(indicator: Indicator) -> list[Binding]:
    return [part.binding for part in _leaves(indicator)]


# Stage 1 ---------------------------------------------------------------------


def _read_call(indicator: Indicator, call: ResolvedCall) -> ResolvedCall:
    """The call a leaf reads with: group members join ``members``, and
    ``recipient`` joins ``by`` when aggregate recipients need screening."""
    members = dict(call.members)
    for group in call.groups:
        if group.dimension in members:
            members[group.dimension] = members[group.dimension].union(
                *group.members.values()
            )
    by = call.by
    if _screens_recipients(indicator, call) and "recipient" not in by:
        by = (*by, "recipient")
    return replace(call, by=by, members=members)


def _screens_recipients(indicator: Indicator, call: ResolvedCall) -> bool:
    """Whether a call that names no recipients must drop recipient totals."""
    return "recipient" in indicator.dimensions and "recipient" not in call.members


def _stage1(
    cursor: DuckDBPyConnection,
    indicator: Indicator,
    call: ResolvedCall,
    *,
    columns: tuple[str, ...] = (),
    keep_all: bool = False,
) -> _Rows:
    """Rows at row grain in current USD, for any kind but ``Ratio``, with
    the cells its ``Method`` parts report.

    Columns as ``_compile.filtered``, without ``line`` for a ``Derived``.
    """
    match indicator.kind:
        case Sum():
            read = _read_call(indicator, call)
            found = _Rows(filtered(cursor, indicator, read, columns=columns))
        case Method():
            read = _read_call(indicator, call)
            found = _method_rows(cursor, indicator, read, keep_all=keep_all)
        case Derived(plus=plus, minus=minus):
            return _derived_rows(cursor, call, plus, minus)
        case Ratio():
            raise UnsupportedQueryError(
                f"{indicator.id} is a ratio and cannot be part of a sum"
            )

    if not _screens_recipients(indicator, call):
        return found
    rows = found.rows.filter(f"NOT coalesce({_aggregate_sql('recipient')}, FALSE)")
    wanted = {*call.by, *(group.dimension for group in call.groups), *columns}
    if not (keep_all or "recipient" in wanted):
        rows = rows.select(
            ", ".join(ident(name) for name in rows.columns if name != "recipient")
        )
    return replace(found, rows=rows)


def _method_rows(
    cursor: DuckDBPyConnection,
    indicator: Indicator,
    read: ResolvedCall,
    *,
    keep_all: bool,
) -> _Rows:
    """Run a ``Method`` and filter its rows, and the cells it reports, as the
    compiler filters a table."""
    assert isinstance(indicator.kind, Method)
    check_basis(indicator, read)
    result = indicator.kind.function(cursor, read)
    table = result.table

    keys = [
        "provider",
        *(d for d in read.by if d != "year"),
        *(group.dimension for group in read.groups),
        *read.members,
    ]
    needed = dict.fromkeys([*keys, "year", "basis", "value"])
    absent = sorted(set(needed) - set(table.column_names))
    if absent:
        raise ValueError(f"{indicator.id} returned no columns {absent}")

    rows = cursor.from_arrow(table)
    conditions = [_in(dimension, codes) for dimension, codes in read.members.items()]
    if read.years is not None:
        conditions.append(_in("year", read.years))
    if "provider" not in read.members:
        conditions.append(f"provider < {PROVIDER_AGGREGATE_FROM}")
    for rule in indicator.provider_exclusions:
        excluded = f"provider NOT IN ({literals(rule.values)})"
        if rule.when is not None:
            scope = f"{ident(rule.when.column)} NOT IN ({literals(rule.when.values)})"
            excluded = f"({scope} OR {excluded})"
        conditions.append(excluded)
    if conditions:
        rows = rows.filter(" AND ".join(conditions))
    if not keep_all:
        rows = rows.select(", ".join(map(ident, needed)))

    providers = read.members.get("provider")
    incomplete = frozenset(
        cell
        for cell in result.incomplete
        if (providers is None or cell.provider in providers)
        and (read.years is None or cell.year in read.years)
    )
    return _Rows(rows, incomplete, result.notes)


def _derived_rows(
    cursor: DuckDBPyConnection,
    call: ResolvedCall,
    plus: tuple[Indicator, ...],
    minus: tuple[Indicator, ...],
) -> _Rows:
    """Each part on the call's basis, which a headline splits by year, with
    the ``minus`` parts negated. Each part's own lines are dropped, since
    the parts' lines differ by construction."""
    parts = [(part, 1) for part in plus] + [(part, -1) for part in minus]
    names: list[str] | None = None
    union: DuckDBPyRelation | None = None
    incomplete: set[Incomplete] = set()
    notes: list[str] = []
    for part, sign in parts:
        read = _stage1(cursor, part, call)
        incomplete |= read.incomplete
        notes += read.notes
        if names is None:
            names = [
                name
                for name in read.rows.columns
                if name != "value" and (name != "line" or "line" in call.by)
            ]
        select = ", ".join(map(ident, names)) + f", value * {sign} AS value"
        rows = read.rows.select(select)
        union = rows if union is None else union.union(rows)
    assert union is not None  # a Derived has at least one part
    return _Rows(union, frozenset(incomplete), tuple(notes))


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
    cursor: DuckDBPyConnection, indicator: Indicator, call: ResolvedCall
) -> _Rows:
    """Numerators paired with denominators at member grain, with the cells
    the parts' ``Method``s report.

    Columns: the stage 1 keys, ``basis`` (the numerator's concrete basis in
    each year), ``num`` and ``den``. A member-year with a denominator and no
    numerator rows has a null numerator, which a member row reads as 0 and a
    total leaves out. A member-year with no denominator has no row.
    """
    (numerator, num_call), (denominator, den_call) = _ratio_parts(indicator, call)
    num_read = _stage1(cursor, numerator, num_call)
    den_read = _stage1(cursor, denominator, den_call)
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
    return _Rows(
        cursor.from_arrow(paired.select(", ".join(select)).to_arrow_table()),
        num_read.incomplete | den_read.incomplete,
        num_read.notes + den_read.notes,
    )


def _ratio_parts(
    indicator: Indicator, call: ResolvedCall
) -> tuple[tuple[Indicator, ResolvedCall], tuple[Indicator, ResolvedCall]]:
    """The numerator and denominator, each with the call it reads."""
    kind = indicator.kind
    assert isinstance(kind, Ratio)
    den_basis = _denominator_basis(kind.denominator, call.basis)
    return (kind.numerator, call), (kind.denominator, replace(call, basis=den_basis))


def _denominator_basis(denominator: Indicator, basis: Basis) -> Basis:
    """The call's basis, or the denominator's default when it has no line
    for one of the bases the call's basis uses (GNI is net flows for every
    basis)."""
    kind = denominator.kind
    rule = denominator.binding.bases.get(basis)
    if rule is None:
        return denominator.default_basis
    used = (rule.before, rule.after) if isinstance(rule, Headline) else (basis,)
    if isinstance(kind, Sum) and kind.lines and not set(used) <= set(kind.lines):
        return denominator.default_basis
    return basis


# Result ----------------------------------------------------------------------


def _assemble(
    cursor: DuckDBPyConnection,
    indicator: Indicator,
    call: ResolvedCall,
    rows: DuckDBPyRelation,
    cells: frozenset[Incomplete],
    *,
    years: list[int],
    last: int | None,
) -> tuple[pa.Table, _Held]:
    """Member rows, then each total, and the incomplete cells behind each.

    ``years`` are the years ``rows`` hold, and ``last`` the table's last
    year, of which a sum across providers warns.
    """
    always = [name for name in SPLIT_COLUMNS if name in rows.columns]
    keys = list(dict.fromkeys([*call.by, *always]))
    own = _own_years(call, years)
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
    # A summed row is null where it holds a Method's cell and, for a ratio,
    # where a member's side has stopped, as a provider total is.
    nulling = frozenset(
        cell
        for cell in cells
        if cell.year in own
        or not _is_own_gap(cell)
        or (ratio and not _not_yet_reported(cell, cells, last))
    )
    rows = _marked(cursor, rows, nulling)

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
    held = _Held(
        own=_held(members, nulling),
        summed=_summed_cells(call, cells, own, years, last),
    )

    groups = {group.key: group for group in call.groups}
    in_totals: set[Incomplete] = set()
    for key in call.totals:
        table, inside = _total(
            cursor, indicator, call, rows, groups[key], keys, value, cells, nulling
        )
        tables.append(table)
        in_totals |= inside
    return pa.concat_tables(tables), replace(held, totals=frozenset(in_totals))


def _marked(
    cursor: DuckDBPyConnection, rows: DuckDBPyRelation, cells: frozenset[Incomplete]
) -> DuckDBPyRelation:
    """``rows`` with ``incomplete``, whether the row's provider-year is a
    cell."""
    if not cells:
        return rows.select("*, FALSE AS incomplete")
    keys = sorted({(cell.provider, cell.year) for cell in cells}, key=str)
    gaps = pa.table(
        {
            "provider": pa.array([provider for provider, _ in keys], pa.int64()),
            "year": pa.array([year for _, year in keys], pa.int64()),
        }
    )
    select = [f"r.{ident(name)}" for name in rows.columns]
    select.append("g.provider IS NOT NULL AS incomplete")
    return (
        rows.set_alias("r")
        .join(
            cursor.from_arrow(gaps).set_alias("g"),
            "r.provider = g.provider AND r.year = g.year",
            how="left",
        )
        .select(", ".join(select))
    )


def _held(
    rows: DuckDBPyRelation, cells: frozenset[Incomplete]
) -> frozenset[Incomplete]:
    """The cells of ``cells`` that mark one of ``rows``."""
    if not cells:
        return frozenset()
    found = set(rows.filter("incomplete").select("provider, year").fetchall())
    return frozenset(cell for cell in cells if (cell.provider, cell.year) in found)


def _unless_partial(call: ResolvedCall) -> str:
    """SQL for a summed row's value: null where it holds an incomplete cell
    (``partial``), unless the call allows partial values."""
    return (
        "value" if call.allow_partial else "CASE WHEN partial THEN NULL ELSE value END"
    )


def _own_years(call: ResolvedCall, years: Sequence[int]) -> set[int]:
    """The years in which each member row is one provider's: every year with
    ``provider`` in ``by``, else those in which the rows show one provider."""
    if "provider" in call.by:
        return set(years)
    shown = _shown_codes(call, "provider", years)
    if shown is None:
        return set()
    return {year for year, codes in shown.items() if len(codes) == 1}


def _summed_cells(
    call: ResolvedCall,
    cells: frozenset[Incomplete],
    own: set[int],
    years: list[int],
    last: int | None,
) -> frozenset[Incomplete]:
    """The cells a sum across providers outside ``by`` warns of: those of the
    providers it holds in the table's last year that have not reported it
    and reported the year before."""
    if last is None or last not in years or last in own:
        return frozenset()
    shown = _shown_codes(call, "provider", [last])
    return frozenset(
        cell
        for cell in cells
        if _not_yet_reported(cell, cells, last)
        and (shown is None or cell.provider in shown[last])
    )


def _not_yet_reported(
    cell: Incomplete, cells: frozenset[Incomplete], last: int | None
) -> bool:
    """Whether ``cell`` is a provider's own gap in the table's last year after
    it reported the year before. A provider that stopped earlier has a cell
    the year before."""
    return (
        cell.year == last
        and _is_own_gap(cell)
        and Incomplete(cell.provider, cell.year - 1, cell.missing) not in cells
    )


def _shown_codes(
    call: ResolvedCall, dimension: Dimension, years: Sequence[int]
) -> dict[int, frozenset[Code]] | None:
    """The codes of ``dimension`` the member rows hold in each of ``years``:
    those in ``call.rows`` in every year and each shown group's members in
    the years they are members, the membership the group's total uses.

    Without ``call.rows`` for the dimension, the rows hold every code in
    ``call.members``, and ``None`` means every code.
    """
    codes = call.rows.get(dimension)
    if codes is None:
        members = call.members.get(dimension)
        return None if members is None else dict.fromkeys(years, members)
    shown = [g for g in call.groups if g.shown and g.dimension == dimension]
    for group in shown:
        _check_years(group, years)
    return {
        year: codes.union(*(group.members[year] for group in shown)) for year in years
    }


def _member_rows(
    cursor: DuckDBPyConnection,
    call: ResolvedCall,
    rows: DuckDBPyRelation,
    years: list[int],
) -> DuckDBPyRelation:
    """The rows the result shows as their own: in each dimension of
    ``call.rows``, the codes ``_shown_codes`` gives for the row's year."""
    members = rows
    for dimension in call.rows:
        if dimension in rows.columns:
            shown = _shown_codes(call, dimension, years)
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
) -> tuple[pa.Table, frozenset[Incomplete]]:
    """One group's total rows, summed over its members in each year, and the
    incomplete cells they hold.

    A provider total holds the cells of the members it sums and is null in a
    year with one. Any other total row is null where it holds a row marked
    incomplete, as a member row is. Neither is null when the call allows
    partial values.
    """
    dimension = group.dimension
    years = _years(rows)
    _check_years(group, years)
    inside = _within(cursor, rows, dimension, group.members)
    if isinstance(indicator.kind, Ratio):
        inside = inside.filter("num IS NOT NULL")

    if dimension == "provider":
        held = frozenset(
            cell
            for cell in cells
            if cell.year in years and cell.provider in group.members[cell.year]
        )
        missing = sorted({cell.year for cell in held})
        partial = f"bool_or(year IN ({literals(missing)}))" if missing else "FALSE"
    else:
        held = _held(inside, nulling)
        partial = "bool_or(incomplete)"
    summed = _aggregate(
        inside, [k for k in keys if k != dimension], value, f"{partial} AS partial"
    )
    table = _shape(
        summed,
        indicator,
        call,
        flag="TRUE",
        labels={dimension: group.name},
        value=_unless_partial(call),
    )
    return table, held


def _years(rows: DuckDBPyRelation) -> list[int]:
    """The years ``rows`` hold, sorted."""
    found = rows.select("year").distinct().fetchall()
    return sorted(year for (year,) in found if year is not None)


def _check_years(group: ResolvedGroup, years: Iterable[int]) -> None:
    absent = [year for year in years if year not in group.members]
    if absent:
        raise ValueError(
            f"group {group.name!r} has no members for years {absent}, which the "
            "rows hold"
        )


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


# Incomplete cells ------------------------------------------------------------


def _reporting_gaps(
    cursor: DuckDBPyConnection,
    indicator: Indicator,
    call: ResolvedCall,
    years: list[int],
    last: int | None,
) -> frozenset[Incomplete]:
    """The cells of every leaf by the engine's two rules (``_stopped`` and
    ``_dropped``), for the providers the call names, in ``years``, and, when
    ``years`` holds the table's last year ``last``, in the year before, so a
    sum can tell a provider that reported then from one that stopped earlier.

    A call that names no providers has none. A ``Sum`` or ``Method`` alone
    has rows only where its provider reported, and a ``Method`` reports its
    own cells, so the two rules matter to a result that sums providers alone.
    """
    if isinstance(indicator.kind, Sum | Method) and not _sums_providers(call, years):
        return frozenset()
    reads = [
        (leaf, leaf_call, codes)
        for leaf, leaf_call in _leaf_calls(indicator, call)
        if (codes := _read_call(leaf, leaf_call).members.get("provider"))
    ]
    tables: dict[TableName, tuple[Binding, set[Code]]] = {}
    for leaf, _, codes in reads:
        tables.setdefault(leaf.binding.table, (leaf.binding, set()))[1].update(codes)

    checked = years
    if last is not None and last in years:
        checked = sorted({*years, last - 1})
    cells: set[Incomplete] = set()
    for binding, codes in tables.values():
        cells |= _stopped(cursor, binding, codes, checked)
    for leaf, leaf_call, codes in reads:
        cells |= _dropped(cursor, leaf, leaf_call, codes, years)
    if isinstance(indicator.kind, Ratio):
        for side, side_call in _ratio_parts(indicator, call):
            codes = _read_call(side, side_call).members.get("provider")
            if isinstance(side.kind, Sum) and codes:
                cells |= _side_stopped(cursor, side, side_call, checked)
    return frozenset(cells)


def _sums_providers(call: ResolvedCall, years: list[int]) -> bool:
    """Whether the result sums providers: in a provider total, or in member
    rows of a year outside ``_own_years``."""
    totals = any(key[0] == "provider" for key in call.totals)
    return totals or _own_years(call, years) != set(years)


def _leaf_calls(
    indicator: Indicator, call: ResolvedCall
) -> Iterator[tuple[Indicator, ResolvedCall]]:
    """Each ``Sum`` and ``Method`` the indicator reads, with the call it reads
    its rows with (a ratio's denominator its own basis)."""
    match indicator.kind:
        case Sum() | Method():
            yield indicator, call
        case Ratio():
            for part, part_call in _ratio_parts(indicator, call):
                yield from _leaf_calls(part, part_call)
        case Derived(plus=plus, minus=minus):
            for part in (*plus, *minus):
                yield from _leaf_calls(part, call)


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
    years: list[int],
) -> set[Incomplete]:
    """The cells of providers whose rows for a ratio's ``Sum`` side have
    stopped while the other side may go on, as GNI without ODA: each of
    ``years`` after the provider's last row for the side under the call's
    filters.

    One query reads each named provider's last year, the side's filters and
    the call's providers pushed into the scan. A ``Method`` side reports its
    own cells, and a provider that has not yet reported a side leaves its
    pair out of a sum.
    """
    every_year = replace(call, years=None, by=("provider", "year"))
    last = (
        _stage1(cursor, side, every_year)
        .rows.aggregate("provider, max(CAST(year AS BIGINT)) AS last", "provider")
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
) -> set[Incomplete]:
    """The cells of providers with rows for the leaf in the year before its
    table's last year and none in it, when ``years`` holds the last year.

    The last year of DAC1 is preliminary. Every member reports total ODA, but
    only a few report detail lines such as in-donor refugee costs, so a total
    over the reporters reads as the full total. A provider with no rows in
    the year before, such as one that reports debt relief in some years only,
    counts as zero. Rows are the leaf's under the call's filters, read for
    those two years alone, which a ``Method``'s rows allow.
    """
    last = _last_year(cursor, leaf)
    if last is None or last not in years:
        return set()
    try:
        rows = _stage1(cursor, leaf, replace(call, years=(last - 1, last))).rows
    except BasisRangeError:
        # The basis starts in the last year, so no provider has its rows the
        # year before.
        return set()
    codes = sorted(codes, key=str)
    seen = set(
        rows.filter(f"provider IN ({literals(codes)})")
        .select("provider, CAST(year AS BIGINT) AS year")
        .distinct()
        .fetchall()
    )
    return {
        Incomplete(code, last, frozenset({code}))
        for code in codes
        if (code, last - 1) in seen and (code, last) not in seen
    }


def _last_year(cursor: DuckDBPyConnection, indicator: Indicator) -> int | None:
    """The last year of any table the indicator reads."""
    lasts: list[int] = []
    for binding in {b.table: b for b in _bindings(indicator)}.values():
        year = ident(binding.dimensions["year"])
        [(last,)] = cursor.sql(
            f"SELECT max(CAST({year} AS BIGINT)) FROM {ident(binding.table)}"
        ).fetchall()
        if last is not None:
            lasts.append(last)
    return max(lasts, default=None)


def _member_flag(call: ResolvedCall) -> str:
    """SQL for ``is_aggregate`` on member rows."""
    checks = []
    for dimension in _AGGREGATE_DIMENSIONS:
        codes = call.members.get(dimension)
        if dimension in call.by:
            checks.append(f"coalesce({_aggregate_sql(dimension)}, FALSE)")
        elif codes and all(is_aggregate(dimension, code) for code in codes):
            return "TRUE"
    return " OR ".join(checks) or "FALSE"


def _aggregate_sql(dimension: Dimension) -> str:
    """``_names.is_aggregate`` as SQL over the ``dimension`` column."""
    column = ident(dimension)
    if dimension == "provider":
        return f"{column} >= {PROVIDER_AGGREGATE_FROM}"
    return f"{column} IN ({literals(sorted(aggregate_codes(dimension)))})"


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


# SQL text --------------------------------------------------------------------


def _in(column: str, codes: Iterable[Code]) -> str:
    values = sorted(codes, key=str)
    return f"{ident(column)} IN ({literals(values)})" if values else "FALSE"

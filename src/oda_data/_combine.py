"""Combine compiled rows into a call's result.

``combine`` runs any indicator kind over a resolved call and returns one
Arrow table in the result shape. It builds on the compiler's stages in
``_compile``:

- ``Sum`` rows come from ``filtered``. ``Method`` rows come from the
  indicator's function. ``Derived`` rows come from each part, the ``minus``
  parts negated. Each is converted to the call's units at row grain by ``converted``.
- ``Ratio`` reads its numerator and denominator in current USD, pairs them
  per member, and divides sums. It refuses units.
- Member rows sum by ``by`` plus ``SPLIT_COLUMNS``. Group totals sum the
  same rows over each group's members in each year. Unless the call allows
  partial totals, a total is null in a year in which a member is missing by
  either of two rules. A member that reported to the table in an earlier year
  has stopped, with no row in that year or any later one (``stopped_years``). Or, in
  the table's last year, a member that has rows for the indicator in the year
  before has none, as when a preliminary year holds only some members' detail
  lines (``_dropped``). A member that has not yet reported, or that skips a
  year and reports again later, counts as zero. A ratio applies the first
  rule to its numerator's and its denominator's own rows, which covers the
  second.
- A ``Derived`` member's own row is null by the second rule, applied to each
  part, unless the call allows partial totals: a member that reports total
  ODA in the last year but not yet the refugee costs it reported the year
  before has a core ODA row that reads as complete (``_nulled_members``).

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
from oda_data._errors import OdaDataWarning, UnsupportedQueryError
from oda_data._model import Derived, Headline, In, Method, NotIn, Ratio, Sum
from oda_data._names import (
    EU_INSTITUTIONS,
    PROVIDER_AGGREGATE_FROM,
    aggregate_codes,
    eu_members,
    is_aggregate,
    names,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Mapping

    from duckdb import DuckDBPyConnection, DuckDBPyRelation

    from oda_data._model import (
        Binding,
        Caveat,
        Factors,
        Indicator,
        ResolvedCall,
        ResolvedGroup,
    )
    from oda_data._types import Basis, Code, Dimension, TableName

_AGGREGATE_DIMENSIONS: tuple[Dimension, ...] = ("provider", "recipient")


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
        partial: For a sum across providers that reads the table's last year,
            that year and the number of the summed providers that reported
            the year before and have not reported it; ``None`` otherwise.
        nulled: For a ``Derived`` indicator, the table's last year and the
            providers whose own row is null in it, since a part they reported
            the year before is missing; ``None`` when no row is nulled.
        notes: The notes of each ``Method`` part on its rows, such as the EU
            members missing from 918's GNI.
    """

    table: pa.Table
    fallback_providers: frozenset[int]
    partial: tuple[int, int] | None = None
    nulled: tuple[int, frozenset[Code]] | None = None
    notes: tuple[str, ...] = ()


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
    concerns a basis and year the call reads, as an ``OdaDataWarning``.

    Args:
        cursor: A cursor holding a view for each of ``tables_for(indicator)``,
            and ``broad_sectors`` when the call uses that dimension.
        indicator: Any indicator.
        call: The resolved call. Codes in ``members`` but outside ``rows``
            feed group totals and are dropped from the member rows. ``units``
            is empty for a ``Ratio`` and holds at least one unit otherwise.
        factors: The Context's factor source.

    Returns:
        The result rows and the providers that took fallback factors.

    Raises:
        DimensionError: The call groups, filters or totals by a dimension the
            indicator lacks.
        UnsupportedQueryError: ``oda.total()`` without its dimension in
            ``by``; a total over a group holding an aggregate code; an
            aggregate code summed with other codes; EU Institutions (918)
            summed with EU members it names, or a total over a group holding
            both in one year, on a binding whose members' figures hold their EU
            share; a code the indicator's row filters leave out;
            ``unit=`` on a ratio; a basis the indicator lacks.
        BasisRangeError: The call asks for years before the basis starts.
        MissingFactorError: A provider-year lacks a factor.
        ValueError: The call breaks the ``ResolvedCall`` contract: a total's
            group lacks a year the rows hold, or ``rows`` narrows a dimension
            that is neither in ``by`` nor a group's.
    """
    _check_call(indicator, call)
    _warn_caveats(indicator, call)

    if isinstance(indicator.kind, Ratio):
        rows = _ratio_rows(cursor, indicator, call)
        result = _assemble(cursor, indicator, call, rows, ratio=True)
        return Combined(result, frozenset(), notes=_notes(cursor, indicator, call))

    stage1 = _stage1(cursor, indicator, call)
    stage2, fallback = _in_units(cursor, indicator, stage1, call, factors)
    rows = _materialize(cursor, stage2)
    nulled = _nulled_members(cursor, indicator, call, rows)
    table = _assemble(cursor, indicator, call, rows, ratio=False, nulled=nulled)
    partial = _partial_sum(cursor, indicator, call, rows)
    if partial is not None and not call.allow_partial:
        warnings.warn(
            f"{indicator.id}: {partial_text(*partial)}", OdaDataWarning, stacklevel=2
        )
    return Combined(table, fallback, partial, nulled, _notes(cursor, indicator, call))


def partial_text(year: int, count: int, /) -> str:
    """The note on a sum across providers whose last year is partial."""
    providers = "provider" if count == 1 else "providers"
    return (
        f"{year} is partial: {count} {providers} that reported {year - 1} "
        f"{'has' if count == 1 else 'have'} not reported {year}, so its sum "
        "covers only those that have; add 'provider' to by to see who reported"
    )


def nulled_text(year: int, codes: Iterable[Code], /, *, listed: int) -> str:
    """The note on providers whose own row is null in a partial last year.

    It names the first ``listed`` providers, in order of name, and counts the
    rest.
    """
    known = names("provider")
    labels = sorted(f"{known.get(code, code)} ({code})" for code in codes)
    have = "has" if len(labels) == 1 else "have"
    shown = ", ".join(labels[:listed])
    if len(labels) > listed:
        shown += f" and {len(labels) - listed} more"
    return (
        f"{year} rows are null for {shown}, which reported a part "
        f"of the indicator in {year - 1} and {have} not reported it in {year}; "
        "allow_partial=True returns the partial values"
    )


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
    _check_call(indicator, call)
    _warn_caveats(indicator, call)

    if isinstance(indicator.kind, Sum):
        view = cursor.table(indicator.binding.table)
        _check_columns(indicator, columns, view.columns)
    stage1 = _stage1(cursor, indicator, call, columns=columns, keep_all=True)
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


def _check_call(indicator: Indicator, call: ResolvedCall) -> None:
    check_dimensions(indicator, call)
    check_left_out(indicator, call)

    shown = {*call.by, *(group.dimension for group in call.groups)}
    for dimension, codes in call.rows.items():
        if dimension not in shown and codes != call.members.get(dimension):
            raise ValueError(
                f"rows narrows {dimension!r}, which is neither in by nor a group's"
            )

    groups = {group.name: group for group in call.groups}
    for name in call.totals:
        group = groups[name]
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

    _check_eu_share(indicator, call)

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


def _check_eu_share(indicator: Indicator, call: ResolvedCall) -> None:
    """Refuse a sum of EU Institutions (918) with an EU member, on an
    indicator whose members' figures hold their EU contribution.

    Such a sum counts that money twice. Other tables, and indicators that
    attribute 918's flows to its funders, sum them freely.
    """
    if not any(binding.members_hold_eu_share for binding in _bindings(indicator)):
        return
    reason = (
        "which were EU members in a year of the call; members report their EU "
        "contributions as their own multilateral ODA, so their sum counts that "
        "money twice"
    )

    # Named providers only, since a default is no sum the caller chose.
    providers = call.members.get("provider", frozenset())
    if "provider" in call.rows and "provider" not in call.by:
        clashes = sorted(providers & eu_members(call.years), key=str)
        if EU_INSTITUTIONS in providers and clashes:
            raise UnsupportedQueryError(
                f"the providers summed hold EU Institutions (918) and {clashes}, "
                f"{reason}; add 'provider' to by"
            )

    # A dated group may hold 918 and a member in different years only.
    groups = {group.name: group for group in call.groups}
    for name in call.totals:
        group = groups[name]
        if group.dimension != "provider":
            continue
        clashes = sorted(
            {
                code
                for year, held in group.members.items()
                if EU_INSTITUTIONS in held
                for code in held & eu_members([year])
            },
            key=str,
        )
        if clashes:
            raise UnsupportedQueryError(
                f"group {name!r} holds EU Institutions (918) and providers "
                f"{clashes}, {reason}"
            )


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
) -> DuckDBPyRelation:
    """Rows at row grain in current USD, for any kind but ``Ratio``.

    Columns as ``_compile.filtered``, without ``line`` for a ``Derived``.
    """
    match indicator.kind:
        case Sum():
            read = _read_call(indicator, call)
            rows = filtered(cursor, indicator, read, columns=columns)
        case Method():
            read = _read_call(indicator, call)
            rows = _method_rows(cursor, indicator, read, keep_all=keep_all)
        case Derived(plus=plus, minus=minus):
            return _derived_rows(cursor, call, plus, minus)
        case Ratio():
            raise UnsupportedQueryError(
                f"{indicator.id} is a ratio and cannot be part of a sum"
            )

    if not _screens_recipients(indicator, call):
        return rows
    rows = rows.filter(f"NOT coalesce({_aggregate_sql('recipient')}, FALSE)")
    wanted = {*call.by, *(group.dimension for group in call.groups), *columns}
    if keep_all or "recipient" in wanted:
        return rows
    return rows.select(
        ", ".join(ident(name) for name in rows.columns if name != "recipient")
    )


def _notes(
    cursor: DuckDBPyConnection, indicator: Indicator, call: ResolvedCall
) -> tuple[str, ...]:
    """The notes of each ``Method`` part, read with the call the part reads
    its rows with."""
    match indicator.kind:
        case Method(notes=notes) if notes is not None:
            return notes(cursor, _read_call(indicator, call))
        case Ratio():
            return tuple(
                note
                for part, part_call in _ratio_parts(indicator, call)
                for note in _notes(cursor, part, part_call)
            )
        case Derived(plus=plus, minus=minus):
            return tuple(
                note for part in (*plus, *minus) for note in _notes(cursor, part, call)
            )
    return ()


def _method_rows(
    cursor: DuckDBPyConnection,
    indicator: Indicator,
    read: ResolvedCall,
    *,
    keep_all: bool,
) -> DuckDBPyRelation:
    """Run a ``Method`` and filter its rows as the compiler filters a table."""
    assert isinstance(indicator.kind, Method)
    check_basis(indicator, read)
    table = indicator.kind.function(cursor, read)

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
    if keep_all:
        return rows
    return rows.select(", ".join(map(ident, needed)))


def _derived_rows(
    cursor: DuckDBPyConnection,
    call: ResolvedCall,
    plus: tuple[Indicator, ...],
    minus: tuple[Indicator, ...],
) -> DuckDBPyRelation:
    """Each part on the call's basis, which a headline splits by year, with
    the ``minus`` parts negated. Each part's own lines are dropped, since
    the parts' lines differ by construction."""
    parts = [(part, 1) for part in plus] + [(part, -1) for part in minus]
    names: list[str] | None = None
    union: DuckDBPyRelation | None = None
    for part, sign in parts:
        rows = _stage1(cursor, part, call)
        if names is None:
            names = [
                name
                for name in rows.columns
                if name != "value" and (name != "line" or "line" in call.by)
            ]
        select = ", ".join(map(ident, names)) + f", value * {sign} AS value"
        rows = rows.select(select)
        union = rows if union is None else union.union(rows)
    assert union is not None  # a Derived has at least one part
    return union


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
) -> DuckDBPyRelation:
    """Numerators paired with denominators at member grain.

    Columns: the stage 1 keys, ``basis`` (the numerator's concrete basis in
    each year), ``num`` and ``den``. A member-year with a denominator and no
    numerator rows has a null numerator, which a member row reads as 0 and a
    total leaves out. A member-year with no denominator has no row.
    """
    (numerator, num_call), (denominator, den_call) = _ratio_parts(indicator, call)
    num_rows = _stage1(cursor, numerator, num_call)
    den_rows = _stage1(cursor, denominator, den_call)

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
    return cursor.from_arrow(paired.select(", ".join(select)).to_arrow_table())


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
    *,
    ratio: bool,
    nulled: tuple[int, frozenset[Code]] | None = None,
) -> pa.Table:
    """Member rows, then each total. ``nulled`` names a year and the providers
    whose member rows are null in it."""
    value = (
        "100 * coalesce(sum(num), 0) / nullif(sum(den), 0)" if ratio else "sum(value)"
    )
    always = [name for name in SPLIT_COLUMNS if name in rows.columns]
    keys = list(dict.fromkeys([*call.by, *always]))

    members = rows
    for dimension, codes in call.rows.items():
        if dimension in rows.columns:
            members = members.filter(_in(dimension, codes))
    if nulled is None:
        summed = _aggregate(members, keys, value)
        tables = [_shape(summed, indicator, call, flag=_member_flag(call))]
    else:
        year, codes = nulled
        partial = (
            f"bool_or(year = {year} AND provider IN ({literals(sorted(codes))})) "
            "AS partial"
        )
        summed = _aggregate(members, keys, value, partial)
        tables = [
            _shape(
                summed,
                indicator,
                call,
                flag=_member_flag(call),
                value="CASE WHEN partial THEN NULL ELSE value END",
            )
        ]

    groups = {group.name: group for group in call.groups}
    for name in call.totals:
        group = groups[name]
        tables.append(_total(cursor, indicator, call, rows, group, keys, value, ratio))
    return pa.concat_tables(tables)


def _total(
    cursor: DuckDBPyConnection,
    indicator: Indicator,
    call: ResolvedCall,
    rows: DuckDBPyRelation,
    group: ResolvedGroup,
    keys: list[str],
    value: str,
    ratio: bool,
) -> pa.Table:
    """One group's total rows, summed over its members in each year."""
    dimension = group.dimension
    years = sorted(
        {
            year
            for (year,) in rows.select("year").distinct().fetchall()
            if year is not None
        }
    )
    absent = [year for year in years if year not in group.members]
    if absent:
        raise ValueError(
            f"group {group.name!r} has no members for years {absent}, which the "
            "rows hold"
        )

    membership = _membership(group, years)
    inside = rows.set_alias("r").join(
        cursor.from_arrow(membership).set_alias("m"),
        f"r.{ident(dimension)} = m.code AND r.year = m.year",
        how="semi",
    )
    if ratio:
        inside = inside.filter("num IS NOT NULL")

    missing: set[int] = set()
    if dimension == "provider" and not call.allow_partial:
        if ratio:
            missing = _ratio_gaps(cursor, indicator, call, group, years)
        else:
            missing = _unreported(cursor, indicator, group, years)
            missing |= _dropped(cursor, indicator, call, group, years)

    partial = (
        f"bool_or(year IN ({literals(sorted(missing))})) AS partial" if missing else ""
    )
    summed = _aggregate(inside, [k for k in keys if k != dimension], value, partial)
    return _shape(
        summed,
        indicator,
        call,
        flag="TRUE",
        labels={dimension: group.name},
        value="CASE WHEN partial THEN NULL ELSE value END" if missing else "value",
    )


def _membership(group: ResolvedGroup, years: Iterable[int]) -> pa.Table:
    pairs = [(year, code) for year in years for code in group.members[year]]
    codes = [code for _, code in pairs]
    code_type = pa.int64() if all(isinstance(c, int) for c in codes) else pa.string()
    return pa.table(
        {
            "year": pa.array([year for year, _ in pairs], pa.int64()),
            "code": pa.array(codes, code_type),
        }
    )


def _unreported(
    cursor: DuckDBPyConnection,
    indicator: Indicator,
    group: ResolvedGroup,
    years: list[int],
) -> set[int]:
    """Years in which a member has stopped reporting to one of the
    indicator's tables (see ``stopped_years``).

    Any row counts as reporting. On CRS, that is any row for the provider and
    year.
    """
    codes = _codes(group, years)
    missing: set[int] = set()
    for last in _last_reports(cursor, indicator, codes):
        missing |= stopped_years(group, years, last)
    return missing


def _last_reports(
    cursor: DuckDBPyConnection, indicator: Indicator, codes: list[Code]
) -> list[dict[Code, int]]:
    """For each table the indicator reads, each provider in ``codes`` and the
    last year it has any row."""
    if not codes:
        return []
    by_table = {binding.table: binding for binding in _bindings(indicator)}
    reports = []
    for table, binding in by_table.items():
        provider = ident(binding.dimensions["provider"])
        year = ident(binding.dimensions["year"])
        last = cursor.sql(
            f"SELECT {provider}, max(CAST({year} AS BIGINT)) "
            f"FROM {ident(table)} WHERE {provider} IN ({literals(codes)}) "
            f"GROUP BY {provider}"
        ).fetchall()
        reports.append(dict(last))
    return reports


def _ratio_gaps(
    cursor: DuckDBPyConnection,
    indicator: Indicator,
    call: ResolvedCall,
    group: ResolvedGroup,
    years: list[int],
) -> set[int]:
    """Years in which a member has stopped reporting a numerator, or a
    denominator (see ``stopped_years``).

    Each part's own rows set the last year, so this holds ``_dropped``'s
    rule as well. A member with rows for a part in the year before the
    table's last year and none in it has stopped reporting that part.
    """
    codes = _codes(group, years)
    if not codes:
        return set()
    missing: set[int] = set()
    for part, part_call in _ratio_parts(indicator, call):
        history = _stage1(cursor, part, replace(part_call, years=None))
        last = (
            history.filter(f"provider IN ({literals(codes)})")
            .aggregate("provider, max(CAST(year AS BIGINT))", "provider")
            .fetchall()
        )
        missing |= stopped_years(group, years, dict(last))
    return missing


def _dropped(
    cursor: DuckDBPyConnection,
    indicator: Indicator,
    call: ResolvedCall,
    group: ResolvedGroup,
    years: list[int],
) -> set[int]:
    """The table's last year, when a member has rows for the indicator in the
    year before and none in it (see ``_dropped_codes``)."""
    last = _last_year(cursor, indicator)
    if last is None or last not in years:
        return set()
    codes = sorted(group.members[last], key=str)
    return {last} if _dropped_codes(cursor, indicator, call, codes, last) else set()


def _dropped_codes(
    cursor: DuckDBPyConnection,
    indicator: Indicator,
    call: ResolvedCall,
    codes: list[Code],
    last: int,
) -> set[Code]:
    """The providers in ``codes`` with rows for the indicator in the year
    before ``last``, the table's last year, and none in it.

    The last year of DAC1 is preliminary. Every member reports total ODA, but
    only a few report detail lines such as in-donor refugee costs, so a total
    over the reporters reads as the full total. A provider with no rows in the
    year before, such as one that reports debt relief in some years only,
    counts as zero. Rows are the indicator's rows under the call's filters.

    A ``Derived`` indicator checks each part, since a member that reports
    total ODA but not yet in-donor refugee costs has rows for their
    difference that read as complete.
    """
    if not codes:
        return set()
    if isinstance(indicator.kind, Derived):
        parts = (*indicator.kind.plus, *indicator.kind.minus)
        dropped: set[Code] = set()
        for part in parts:
            dropped |= _dropped_codes(cursor, part, call, codes, last)
        return dropped
    history = _stage1(cursor, indicator, replace(call, years=None))
    seen = set(
        history.filter(
            f"provider IN ({literals(codes)}) AND year IN ({last - 1}, {last})"
        )
        .select("provider, CAST(year AS BIGINT) AS year")
        .distinct()
        .fetchall()
    )
    return {
        code for code in codes if (code, last - 1) in seen and (code, last) not in seen
    }


def _nulled_members(
    cursor: DuckDBPyConnection,
    indicator: Indicator,
    call: ResolvedCall,
    rows: DuckDBPyRelation,
) -> tuple[int, frozenset[Code]] | None:
    """The table's last year and the providers whose own ``Derived`` row is
    null in it: those with a part they reported the year before and not in
    it (``_dropped_codes``).

    A member row is one provider's when ``provider`` is in ``by`` or the call
    reads one provider. A sum over several is ``_partial_sum``'s. A part the
    provider never reports, or skips in an earlier year, counts as zero. A
    provider with no row in the last year is left out, since it has no row to
    null.
    """
    if not isinstance(indicator.kind, Derived) or call.allow_partial:
        return None
    providers: frozenset[Code] = call.rows.get(
        "provider", call.members.get("provider", frozenset())
    )
    if "provider" not in call.by and len(providers) != 1:
        return None
    last = _last_year(cursor, indicator)
    if last is None:
        return None
    # A provider with no row in the last year has no row to null.
    shown = rows.filter(f"year = {last}").select("provider").distinct().fetchall()
    codes = sorted({code for (code,) in shown} & providers, key=str)
    dropped = _dropped_codes(cursor, indicator, call, codes, last)
    return (last, frozenset(dropped)) if dropped else None


def _partial_sum(
    cursor: DuckDBPyConnection,
    indicator: Indicator,
    call: ResolvedCall,
    rows: DuckDBPyRelation,
) -> tuple[int, int] | None:
    """The table's last year and how many providers summed in it reported
    the year before and have not reported it, for a result that sums across
    providers; ``None`` when no such provider exists.

    A provider has not reported when it has no row in the table that year,
    the rule ``stopped_years`` applies to group totals, or no row for the indicator
    under the call's filters (``_dropped_codes``). Group totals are null in
    such a year instead, and a result split by provider shows who reported.
    """
    if "provider" in call.by:
        return None
    codes = sorted(_read_call(indicator, call).members.get("provider", ()), key=str)
    last = _last_year(cursor, indicator)
    if len(codes) < 2 or last is None:
        return None
    if not rows.filter(f"year = {last}").limit(1).fetchall():
        return None
    stopped = {
        code
        for reports in _last_reports(cursor, indicator, codes)
        for code, year in reports.items()
        if year == last - 1
    }
    stopped |= _dropped_codes(cursor, indicator, call, codes, last)
    return (last, len(stopped)) if stopped else None


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


def _codes(group: ResolvedGroup, years: list[int]) -> list[Code]:
    return sorted(set().union(*(group.members[year] for year in years)), key=str)


def stopped_years(
    group: ResolvedGroup, years: list[int], last: dict[Code, int]
) -> set[int]:
    """Years in which a member has stopped reporting: it reported in an
    earlier year and has no row in that year or any later year of the table.

    ``last`` holds each member's last reporting year in the whole table. A
    member not yet reporting, or between two reporting years, counts as zero.
    """
    return {
        year
        for year in years
        if any(last.get(code, year) < year for code in group.members[year])
    }


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

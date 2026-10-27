"""Combine compiled rows into a call's result.

``combine`` runs any indicator kind over a resolved call and returns one
Arrow table in the result shape. It builds on the compiler's stages in
``_compile``:

- ``Sum`` rows come from ``filtered``; ``Method`` rows from the indicator's
  function; ``Derived`` rows from each part, the ``minus`` parts negated.
  Each is converted to the call's units at row grain by ``converted``.
- ``Ratio`` reads its numerator and denominator in current USD, pairs them
  per member, and divides sums. It refuses units.
- Member rows sum by ``by`` plus ``SPLIT_COLUMNS``. Group totals sum the
  same rows over each group's members in each year. Unless the call allows
  partial totals, a total is null in a year in which a member is missing by
  either of two rules. A member that reported to the table in an earlier year
  has stopped, with no row in that year or any later one (``_gaps``). Or, in
  the table's last year, a member that has rows for the indicator in the year
  before has none, as when a preliminary year holds only some members' detail
  lines (``_dropped``). A member that has not yet reported, or that skips a
  year and reports again later, counts as zero. A ratio applies the first
  rule to its numerator's and its denominator's own rows, which covers the
  second.

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
    check_excluded,
    converted,
    filtered,
    ident,
    literal,
    literals,
)
from oda_data._errors import OdaDataWarning, UnsupportedQueryError
from oda_data._model import Derived, Headline, Method, Ratio, Sum
from oda_data._names import (
    EU_INSTITUTIONS,
    PROVIDER_AGGREGATE_FROM,
    aggregate_codes,
    eu_members,
    is_aggregate,
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
    """

    table: pa.Table
    fallback_providers: frozenset[int]


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

    Emits each caveat of the indicator, and of its parts, that concerns a
    basis and year the call reads, as an ``OdaDataWarning``.

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
            summed with EU members; a provider the indicator excludes;
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
        return Combined(result, frozenset())

    stage1 = _stage1(cursor, indicator, call)
    stage2, fallback = converted(cursor, stage1, call, factors)
    rows = _materialize(cursor, stage2)
    return Combined(_assemble(cursor, indicator, call, rows, ratio=False), fallback)


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
            its rows lack one of ``columns``; and as ``combine``.
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
    stage2, fallback = converted(cursor, stage1, call, factors)
    return Combined(stage2.to_arrow_table(), fallback)


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


def _check_call(indicator: Indicator, call: ResolvedCall) -> None:
    check_dimensions(indicator, call)
    check_excluded(indicator, call)

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

    # Named providers only: a default such as every CRS provider is no sum
    # the caller chose.
    providers = call.members.get("provider", frozenset())
    if "provider" in call.rows and "provider" not in call.by:
        clashes = sorted(providers & eu_members(call.years), key=str)
        if EU_INSTITUTIONS in providers and clashes:
            raise UnsupportedQueryError(
                f"the providers summed hold EU Institutions (918) and {clashes}, "
                "which were EU members in a year of the call; members report "
                "their EU contributions as their own multilateral ODA, so their "
                "sum counts that money twice; add 'provider' to by"
            )

    if isinstance(indicator.kind, Ratio):
        if call.units:
            raise UnsupportedQueryError(
                f"{indicator.id} is a ratio, computed in current USD so that each "
                "provider's deflator leaves it unchanged; it takes no unit="
            )
    elif not call.units:
        raise ValueError(f"{indicator.id} needs at least one unit")


def _warn_caveats(indicator: Indicator, call: ResolvedCall) -> None:
    for text in dict.fromkeys(_caveats(indicator, call)):
        warnings.warn(f"{indicator.id}: {text}", OdaDataWarning, stacklevel=3)


def _caveats(indicator: Indicator, call: ResolvedCall) -> Iterator[str]:
    """The caveats of the indicator and its parts that the call reads, each
    part with the call it reads (a ratio's denominator its own basis)."""
    for caveat in indicator.caveats:
        if _concerns(caveat, indicator, call):
            yield caveat.text
    match indicator.kind:
        case Ratio():
            for part, part_call in _ratio_parts(indicator, call):
                yield from _caveats(part, part_call)
        case Derived(plus=plus, minus=minus):
            for part in (*plus, *minus):
                yield from _caveats(part, call)


def _concerns(caveat: Caveat, indicator: Indicator, call: ResolvedCall) -> bool:
    """Whether the call reads the caveat's basis in one of its years."""
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
    if indicator.excluded_providers:
        excluded = literals(sorted(indicator.excluded_providers))
        conditions.append(f"provider NOT IN ({excluded})")
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
) -> pa.Table:
    value = (
        "100 * coalesce(sum(num), 0) / nullif(sum(den), 0)" if ratio else "sum(value)"
    )
    always = [name for name in SPLIT_COLUMNS if name in rows.columns]
    keys = list(dict.fromkeys([*call.by, *always]))

    members = rows
    for dimension, codes in call.rows.items():
        if dimension in rows.columns:
            members = members.filter(_in(dimension, codes))
    summed = _aggregate(members, keys, value)
    tables = [_shape(summed, indicator, call, flag=_member_flag(call))]

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
    indicator's tables (see ``_gaps``).

    Any row counts as reporting: on CRS, any row for the provider and year.
    """
    codes = _codes(group, years)
    if not codes:
        return set()
    by_table = {binding.table: binding for binding in _bindings(indicator)}
    missing: set[int] = set()
    for table, binding in by_table.items():
        provider = ident(binding.dimensions["provider"])
        year = ident(binding.dimensions["year"])
        last = cursor.sql(
            f"SELECT {provider}, max(CAST({year} AS BIGINT)) "
            f"FROM {ident(table)} WHERE {provider} IN ({literals(codes)}) "
            f"GROUP BY {provider}"
        ).fetchall()
        missing |= _gaps(group, years, dict(last))
    return missing


def _ratio_gaps(
    cursor: DuckDBPyConnection,
    indicator: Indicator,
    call: ResolvedCall,
    group: ResolvedGroup,
    years: list[int],
) -> set[int]:
    """Years in which a member has stopped reporting a numerator, or a
    denominator (see ``_gaps``).

    Each part's own rows set the last year, so this holds ``_dropped``'s
    rule as well: a member with rows for a part in the year before the
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
        missing |= _gaps(group, years, dict(last))
    return missing


def _dropped(
    cursor: DuckDBPyConnection,
    indicator: Indicator,
    call: ResolvedCall,
    group: ResolvedGroup,
    years: list[int],
) -> set[int]:
    """The table's last year, when a member has rows for the indicator in the
    year before and none in it.

    The last year of DAC1 is preliminary: every member reports total ODA, but
    only a few report detail lines such as in-donor refugee costs, so a total
    over the reporters reads as the full total. A member with no rows in the
    year before, such as one that reports debt relief in some years only,
    counts as zero. Rows are the indicator's rows under the call's filters.
    """
    last = _last_year(cursor, indicator)
    if last is None or last not in years:
        return set()
    codes = sorted(group.members[last], key=str)
    if not codes:
        return set()
    history = _stage1(cursor, indicator, replace(call, years=None))
    seen = set(
        history.filter(
            f"provider IN ({literals(codes)}) AND year IN ({last - 1}, {last})"
        )
        .select("provider, CAST(year AS BIGINT) AS year")
        .distinct()
        .fetchall()
    )
    if any((code, last - 1) in seen and (code, last) not in seen for code in codes):
        return {last}
    return set()


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


def _gaps(group: ResolvedGroup, years: list[int], last: dict[Code, int]) -> set[int]:
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
    """Sum ``rows`` by ``keys``; ``value`` names the summed value and
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

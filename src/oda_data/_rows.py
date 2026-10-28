"""Stage 1 rows: what a call reads from an indicator's tables.

``stage1`` returns the rows of any indicator but a ``Ratio`` at row grain in
current USD, with the ``Incomplete`` cells its ``Method`` parts report, as
``Rows``:

- A ``Sum`` reads its table through ``_compile.filtered``.
- A ``Method`` runs its function, passing it the call's ``Releases`` as its
  memo, and filters the rows it returns as the compiler filters a table.
- A ``Derived`` reads each part and negates the ``minus`` parts.

``read_call`` gives the call a leaf reads with, and ``ratio_parts`` the call
each side of a ``Ratio`` reads with. ``walk``, ``leaves`` and ``bindings`` list
the indicators an indicator combines, the ones that read rows, and their
bindings. ``own_years`` and ``shown_codes`` say which codes a call's member rows
show in each year, and ``aggregate_sql`` is ``_names.is_aggregate`` as SQL.

``_rows`` works from ``ResolvedCall`` alone and imports neither ``_combine`` nor
``_completeness``, which build on it.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

from oda_data._compile import check_basis, filtered, ident, literals
from oda_data._errors import UnsupportedQueryError
from oda_data._model import Derived, Headline, Incomplete, Method, Ratio, Sum
from oda_data._names import PROVIDER_AGGREGATE_FROM, aggregate_codes

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Sequence

    from duckdb import DuckDBPyConnection, DuckDBPyRelation

    from oda_data._model import Binding, Indicator, ResolvedCall, ResolvedGroup
    from oda_data._store import Releases
    from oda_data._types import Basis, Code, Dimension


@dataclass(frozen=True, slots=True)
class Rows:
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


def leaves(indicator: Indicator) -> Iterator[Indicator]:
    """Yield the indicators that read rows: ``Sum`` and ``Method``."""
    for part in walk(indicator):
        if isinstance(part.kind, Sum | Method):
            yield part


def bindings(indicator: Indicator) -> list[Binding]:
    """The binding of each indicator that reads rows."""
    return [part.binding for part in leaves(indicator)]


def read_call(indicator: Indicator, call: ResolvedCall) -> ResolvedCall:
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


def stage1(
    cursor: DuckDBPyConnection,
    indicator: Indicator,
    call: ResolvedCall,
    *,
    columns: tuple[str, ...] = (),
    keep_all: bool = False,
    releases: Releases,
) -> Rows:
    """Rows at row grain in current USD, for any kind but ``Ratio``, with
    the cells its ``Method`` parts report.

    Columns as ``_compile.filtered``, without ``line`` for a ``Derived``.
    ``releases`` is the memo each ``Method`` part receives.
    """
    match indicator.kind:
        case Sum():
            read = read_call(indicator, call)
            found = Rows(filtered(cursor, indicator, read, columns=columns))
        case Method():
            read = read_call(indicator, call)
            found = _method_rows(
                cursor, indicator, read, keep_all=keep_all, releases=releases
            )
        case Derived(plus=plus, minus=minus):
            return _derived_rows(cursor, call, plus, minus, releases)
        case Ratio():
            raise UnsupportedQueryError(
                f"{indicator.id} is a ratio and cannot be part of a sum"
            )

    if not _screens_recipients(indicator, call):
        return found
    rows = found.rows.filter(f"NOT coalesce({aggregate_sql('recipient')}, FALSE)")
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
    releases: Releases,
) -> Rows:
    """Run a ``Method`` with ``releases`` as its memo and filter its rows,
    and the cells it reports, as the compiler filters a table."""
    assert isinstance(indicator.kind, Method)
    check_basis(indicator, read)
    result = indicator.kind.function(cursor, read, memo=releases)
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
    return Rows(rows, incomplete, result.notes)


def _derived_rows(
    cursor: DuckDBPyConnection,
    call: ResolvedCall,
    plus: tuple[Indicator, ...],
    minus: tuple[Indicator, ...],
    releases: Releases,
) -> Rows:
    """Each part on the call's basis, which a headline splits by year, with
    the ``minus`` parts negated. Each part's own lines are dropped, since
    the parts' lines differ by construction."""
    parts = [(part, 1) for part in plus] + [(part, -1) for part in minus]
    names: list[str] | None = None
    union: DuckDBPyRelation | None = None
    incomplete: set[Incomplete] = set()
    notes: list[str] = []
    for part, sign in parts:
        read = stage1(cursor, part, call, releases=releases)
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
    return Rows(union, frozenset(incomplete), tuple(notes))


def ratio_parts(
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


def own_years(call: ResolvedCall, years: Sequence[int]) -> set[int]:
    """The years in which each member row is one provider's: every year with
    ``provider`` in ``by``, else those in which the rows show one provider."""
    if "provider" in call.by:
        return set(years)
    shown = shown_codes(call, "provider", years)
    if shown is None:
        return set()
    return {year for year, codes in shown.items() if len(codes) == 1}


def shown_codes(
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
        check_years(group, years)
    return {
        year: codes.union(*(group.members[year] for group in shown)) for year in years
    }


def check_years(group: ResolvedGroup, years: Iterable[int]) -> None:
    """Raise ``ValueError`` if ``group`` has no members in one of ``years``."""
    absent = [year for year in years if year not in group.members]
    if absent:
        raise ValueError(
            f"group {group.name!r} has no members for years {absent}, which the "
            "rows hold"
        )


def aggregate_sql(dimension: Dimension) -> str:
    """``_names.is_aggregate`` as SQL over the ``dimension`` column."""
    column = ident(dimension)
    if dimension == "provider":
        return f"{column} >= {PROVIDER_AGGREGATE_FROM}"
    return f"{column} IN ({literals(sorted(aggregate_codes(dimension)))})"


def _in(column: str, codes: Iterable[Code]) -> str:
    values = sorted(codes, key=str)
    return f"{ident(column)} IN ({literals(values)})" if values else "FALSE"

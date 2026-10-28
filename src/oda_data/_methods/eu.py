"""EU Institutions (918) beside the providers that fund them.

EU members, and a few other providers, report their contributions to EU
Institutions as their own multilateral ODA (DAC1 line 2102). 918's ODA spends
that money again, so a sum of 918 and its contributors counts it twice.

``EuAttribution`` keeps in 918's row only what no other provider paid: 918's
own value minus the line 2102 of every provider other than 918 and the
aggregates, on the same basis and year. The row is the same whichever
providers the call names beside 918, so 918 sums once with any of them. It
is negative in a year in which the recorded contributions exceed 918's ODA.

``EuGni`` reads GNI for every provider in the call and gives 918 the summed
GNI of the EU's members, either those of each year or today's EU27 in every
year, so 918 has an ODA/GNI ratio of its own.

Both read DAC1 through ``_compile.filtered`` and take their indicators as
arguments, so they keep each indicator's rows, bases and current-USD rule.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

import pyarrow as pa

from oda_data._compile import filtered, ident
from oda_data._errors import BasisRangeError
from oda_data._model import Incomplete, MethodRows
from oda_data._names import (
    EU_INSTITUTIONS,
    PROVIDER_AGGREGATE_FROM,
    eu27,
    eu_members,
    names,
)

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection, DuckDBPyRelation

    from oda_data._model import Indicator, ResolvedCall
    from oda_data._types import Code

_KEYS = "provider, year, basis"


@dataclass(frozen=True, slots=True)
class EuAttribution:
    """918's value net of what every other provider paid it.

    Every other provider's row is ``base`` unchanged. A call without 918
    reads ``base`` alone.

    A provider that paid 918 in the year before the table's last year and has
    no line 2102 in the last year has not yet reported it, so 918's row in
    the last year is ``Incomplete``, missing those providers. A provider with
    no line 2102 in an earlier year paid nothing that year.

    Args:
        base: The indicator each provider's row reads, such as total ODA.
        contributions: The indicator of what a provider pays EU Institutions,
            line 2102 on each basis of ``base``.
    """

    base: Indicator
    contributions: Indicator

    def __call__(self, cursor: DuckDBPyConnection, call: ResolvedCall, /) -> MethodRows:
        """Return ``provider``, ``year``, ``basis`` and ``value`` rows, and
        918's row in the table's last year as incomplete while providers that
        paid it the year before have not reported."""
        providers = call.members.get("provider")
        rows = _summed(filtered(cursor, self.base, _reading(call, providers)))
        if providers is not None and EU_INSTITUTIONS not in providers:
            return MethodRows(rows.to_arrow_table())

        paid = self._paid(cursor, call).aggregate(
            "year, basis, sum(value) AS paid", "year, basis"
        )
        netted = rows.set_alias("r").join(
            paid.set_alias("p"), "r.year = p.year AND r.basis = p.basis", how="left"
        )
        return MethodRows(
            netted.select(
                "r.provider AS provider, r.year AS year, r.basis AS basis, "
                f"CASE WHEN r.provider = {EU_INSTITUTIONS} "
                "THEN r.value - coalesce(p.paid, 0) ELSE r.value END AS value"
            ).to_arrow_table(),
            incomplete=self._unreported(cursor, call),
        )

    def _paid(self, cursor: DuckDBPyConnection, call: ResolvedCall) -> DuckDBPyRelation:
        """Each provider's line 2102 in the call's years, 918 and the
        aggregates left out."""
        return _summed(
            filtered(cursor, self.contributions, _reading(call, None))
        ).filter(
            f"provider != {EU_INSTITUTIONS} AND provider < {PROVIDER_AGGREGATE_FROM}"
        )

    def _unreported(
        self, cursor: DuckDBPyConnection, call: ResolvedCall
    ) -> tuple[Incomplete, ...]:
        """918's cell in the table's last year, missing the providers with
        line 2102 in the year before and none in it, when the call asks for
        the last year."""
        binding = self.contributions.binding
        [(last,)] = cursor.sql(
            f"SELECT max(CAST({ident(binding.dimensions['year'])} AS BIGINT)) "
            f"FROM {ident(binding.table)}"
        ).fetchall()
        if last is None or (call.years is not None and last not in call.years):
            return ()
        try:
            paid = self._paid(cursor, replace(call, years=(last - 1, last)))
        except BasisRangeError:
            # The basis starts in the last year, so nobody paid the year before.
            return ()
        reported: dict[int, set[Code]] = {last - 1: set(), last: set()}
        for code, year in paid.select("provider, CAST(year AS BIGINT)").fetchall():
            reported[year].add(code)
        missing = frozenset(reported[last - 1] - reported[last])
        if not missing:
            return ()
        return (Incomplete(EU_INSTITUTIONS, last, missing, "line 2102"),)


@dataclass(frozen=True, slots=True)
class EuGni:
    """GNI, with 918's GNI the summed GNI of the EU's members.

    With ``fixed_eu27`` false, the members of each year come from
    ``_names.eu_members``: each EU27 country from its accession, and the
    United Kingdom from 1973 to 2020. With it true, they are today's 27
    members in every year.

    A member that has not yet reported GNI, or that skips years and reports
    again, counts as zero, and a note names it. In a year after a member's
    last GNI, 918's GNI sums the members that reported and is ``Incomplete``,
    missing the members that stopped.

    Args:
        gni: The indicator each provider's GNI reads.
        fixed_eu27: Whether 918's members are today's EU27 in every year.
    """

    gni: Indicator
    fixed_eu27: bool = False

    def __call__(self, cursor: DuckDBPyConnection, call: ResolvedCall, /) -> MethodRows:
        """Return ``provider``, ``year``, ``basis`` and ``value`` rows, 918's
        years after a member's last GNI as incomplete, and a note on each run
        of years with the same members counted as zero."""
        providers = call.members.get("provider")
        own = None if providers is None else providers - {EU_INSTITUTIONS}
        rows = (
            _summed(filtered(cursor, self.gni, _reading(call, own)))
            .filter(f"provider != {EU_INSTITUTIONS}")
            .to_arrow_table()
        )
        if providers is not None and EU_INSTITUTIONS not in providers:
            return MethodRows(rows)

        reports = self._reports(cursor, call)
        years = sorted(reports.values)
        eu = pa.table(
            {
                "provider": [EU_INSTITUTIONS] * len(years),
                "year": years,
                "basis": [reports.basis] * len(years),
                "value": [reports.values[year] for year in years],
            }
        )
        runs: dict[frozenset[int], list[int]] = {}
        for year, codes in sorted(reports.missing.items()):
            if year not in reports.stopped:
                runs.setdefault(codes, []).append(year)
        return MethodRows(
            pa.concat_tables([rows, eu.select(rows.column_names).cast(rows.schema)]),
            incomplete=tuple(
                Incomplete(EU_INSTITUTIONS, year, reports.missing[year], "GNI")
                for year in sorted(reports.stopped)
            ),
            notes=tuple(_gap_text(codes, years) for codes, years in runs.items()),
        )

    def _members(self, year: int | None) -> frozenset[int]:
        """918's members in ``year``; ``None`` means any year."""
        if self.fixed_eu27:
            return eu27()
        return eu_members(None if year is None else [year])

    def _reports(self, cursor: DuckDBPyConnection, call: ResolvedCall) -> _Reports:
        """Read every member's GNI in every year, for the gap rule, and sum
        the members of each year the call asks for."""
        everyone = self._members(None)
        history = _summed(
            filtered(cursor, self.gni, _reading(replace(call, years=None), everyone))
        ).fetchall()
        reported: dict[int, dict[int, float]] = {}
        basis = None
        for code, year, row_basis, value in history:
            reported.setdefault(year, {})[code] = value
            basis = row_basis
        first: dict[Code, int] = {}
        last: dict[Code, int] = {}
        for year, values in sorted(reported.items()):
            first = dict.fromkeys(values, year) | first
            last |= dict.fromkeys(values, year)

        members = {
            year: self._members(year)
            for year in sorted(reported)
            if call.years is None or year in call.years
        }
        members = {
            year: codes
            for year, codes in members.items()
            if not codes.isdisjoint(reported[year])
        }
        return _Reports(
            values={
                year: sum(reported[year].get(code, 0.0) for code in codes)
                for year, codes in members.items()
            },
            basis=basis,
            missing={
                year: gap
                for year, codes in members.items()
                if (
                    gap := frozenset(
                        code
                        for code in codes.difference(reported[year])
                        if first.get(code, year) < year
                    )
                )
            },
            stopped=frozenset(
                year
                for year, codes in members.items()
                if any(last.get(code, year) < year for code in codes)
            ),
        )


@dataclass(frozen=True, slots=True)
class _Reports:
    """918's GNI in the years a call asks for, before the gap rule.

    Args:
        values: Each year and the summed GNI of the members that reported.
        basis: The basis of the GNI rows.
        missing: Each year in which members that reported GNI in an earlier
            year have none, and those members.
        stopped: The years after a member's last GNI.
    """

    values: dict[int, float]
    basis: str | None
    missing: dict[int, frozenset[int]]
    stopped: frozenset[int]


def _gap_text(codes: frozenset[int], years: list[int]) -> str:
    """The note on members counted as zero in 918's GNI in ``years``."""
    known = names("provider")
    labels = _joined(sorted(f"{known.get(code, code)} ({code})" for code in codes))
    when = _joined([str(year) for year in years])
    those = "that year" if len(years) == 1 else "those years"
    return (
        f"EU Institutions (918) GNI in {when} sums the members that reported; "
        f"{labels} reported no GNI in {those}"
    )


def _joined(items: list[str]) -> str:
    return items[0] if len(items) == 1 else f"{', '.join(items[:-1])} and {items[-1]}"


def _reading(call: ResolvedCall, providers: frozenset[Code] | None) -> ResolvedCall:
    """The call a part reads with: ``providers`` (every provider for
    ``None``), by provider and year, with no groups."""
    members = {} if providers is None else {"provider": providers}
    return replace(
        call, by=("provider", "year"), members=members, groups=(), totals=(), rows={}
    )


def _summed(rows: DuckDBPyRelation) -> DuckDBPyRelation:
    return rows.aggregate(f"{_KEYS}, sum(value) AS value", _KEYS)

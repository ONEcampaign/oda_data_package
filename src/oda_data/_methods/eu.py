"""EU Institutions (918) beside the providers that fund them.

EU members, and a few other providers, report their contributions to EU
Institutions as their own multilateral ODA (DAC1 line 2102). 918's ODA spends
that money again, so a sum of 918 and its contributors counts it twice.

``EuAttribution`` reads an indicator for every provider in the call and keeps
in 918's row only the part the call's other providers did not pay for. Its
weight in each year is ``1 - paid / spent``, where ``spent`` is 918's own value
and ``paid`` the other providers' line 2102 on the same basis, so 918's row is
``spent - paid``.

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

from oda_data._combine import stopped_years
from oda_data._compile import filtered
from oda_data._model import ResolvedGroup
from oda_data._names import EU_INSTITUTIONS, eu27, eu_members, names

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection, DuckDBPyRelation

    from oda_data._model import Indicator, ResolvedCall
    from oda_data._types import Code

_KEYS = "provider, year, basis"


@dataclass(frozen=True, slots=True)
class EuAttribution:
    """918's value net of what the call's other providers paid it.

    Every other provider's row is ``base`` unchanged. A call without 918
    reads ``base`` alone.

    Args:
        base: The indicator each provider's row reads, such as total ODA.
        contributions: The indicator of what a provider pays EU Institutions,
            line 2102 on each basis of ``base``.
    """

    base: Indicator
    contributions: Indicator

    def __call__(self, cursor: DuckDBPyConnection, call: ResolvedCall, /) -> pa.Table:
        """Return ``provider``, ``year``, ``basis`` and ``value`` rows."""
        providers = call.members.get("provider")
        rows = _summed(filtered(cursor, self.base, _reading(call, providers)))
        if providers is not None and EU_INSTITUTIONS not in providers:
            return rows.to_arrow_table()

        others = None if providers is None else providers - {EU_INSTITUTIONS}
        paid = (
            filtered(cursor, self.contributions, _reading(call, others))
            .filter(f"provider != {EU_INSTITUTIONS}")
            .aggregate("year, basis, sum(value) AS paid", "year, basis")
        )
        netted = rows.set_alias("r").join(
            paid.set_alias("p"), "r.year = p.year AND r.basis = p.basis", how="left"
        )
        return netted.select(
            "r.provider AS provider, r.year AS year, r.basis AS basis, "
            f"CASE WHEN r.provider = {EU_INSTITUTIONS} "
            "THEN r.value - coalesce(p.paid, 0) ELSE r.value END AS value"
        ).to_arrow_table()


@dataclass(frozen=True, slots=True)
class EuGni:
    """GNI, with 918's GNI the summed GNI of the EU's members.

    With ``fixed_eu27`` false, the members of each year come from
    ``_names.eu_members``: each EU27 country from its accession, and the
    United Kingdom from 1973 to 2020. With it true, they are today's 27
    members in every year.

    A member's gaps follow ``_combine.stopped_years``, the rule for group
    totals. A member that has not yet reported GNI, or that skips years and
    reports again, counts as zero. In a year after a member's last GNI, 918's
    GNI is null unless the call allows partial sums. ``notes`` names the
    members behind each gap.

    Args:
        gni: The indicator each provider's GNI reads.
        fixed_eu27: Whether 918's members are today's EU27 in every year.
    """

    gni: Indicator
    fixed_eu27: bool = False

    def __call__(self, cursor: DuckDBPyConnection, call: ResolvedCall, /) -> pa.Table:
        """Return ``provider``, ``year``, ``basis`` and ``value`` rows."""
        providers = call.members.get("provider")
        own = None if providers is None else providers - {EU_INSTITUTIONS}
        rows = (
            _summed(filtered(cursor, self.gni, _reading(call, own)))
            .filter(f"provider != {EU_INSTITUTIONS}")
            .to_arrow_table()
        )
        if providers is not None and EU_INSTITUTIONS not in providers:
            return rows

        reports = self._reports(cursor, call)
        nulled = set() if call.allow_partial else reports.stopped
        years = sorted(reports.values)
        eu = pa.table(
            {
                "provider": [EU_INSTITUTIONS] * len(years),
                "year": years,
                "basis": [reports.basis] * len(years),
                "value": [
                    None if year in nulled else reports.values[year] for year in years
                ],
            }
        )
        return pa.concat_tables([rows, eu.select(rows.column_names).cast(rows.schema)])

    def notes(
        self, cursor: DuckDBPyConnection, call: ResolvedCall, /
    ) -> tuple[str, ...]:
        """Name the members behind each gap in 918's GNI, one note per run
        of years with the same members missing."""
        providers = call.members.get("provider")
        if providers is not None and EU_INSTITUTIONS not in providers:
            return ()
        reports = self._reports(cursor, call)
        runs: dict[tuple[frozenset[int], bool], list[int]] = {}
        for year, codes in sorted(reports.missing.items()):
            runs.setdefault((codes, year in reports.stopped), []).append(year)
        return tuple(
            _gap_text(codes, years, nulled=stopped and not call.allow_partial)
            for (codes, stopped), years in runs.items()
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
        group = ResolvedGroup(str(EU_INSTITUTIONS), "provider", members)
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
            stopped=frozenset(stopped_years(group, list(members), last)),
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


def _gap_text(codes: frozenset[int], years: list[int], *, nulled: bool) -> str:
    """The note on members missing from 918's GNI in ``years``."""
    known = names("provider")
    labels = _joined(sorted(f"{known.get(code, code)} ({code})" for code in codes))
    when = _joined([str(year) for year in years])
    those = "that year" if len(years) == 1 else "those years"
    if nulled:
        return (
            f"EU Institutions (918) GNI is null in {when}, since {labels} "
            "reported no GNI after an earlier year; allow_partial=True sums the "
            "members that reported"
        )
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

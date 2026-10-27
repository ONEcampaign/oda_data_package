"""The engine's contracts: table vocabulary, indicators, bindings, and calls.

Everything here is data. ``_compile`` and ``_combine`` read these types,
``_tables`` and ``_factors`` implement the protocols, and the registry builds
indicators from them. The module imports nothing heavy at run time. DuckDB and
Arrow appear only in annotations.

A call runs in three stages, and each stage is a DuckDB relation:

1. Filtered rows: the binding's, the indicator's, and the call's filters
   applied to one table. Columns: ``provider``, the dimension of each group
   in the call, and every other dimension the call groups by, each named
   after the dimension (``recipient``, ``sector``, ``broad_sector``, ...);
   ``year``; ``basis``, the concrete basis of the row's year (a headline
   call reads ``net_disbursement`` rows to 2017); ``line`` where ``line`` is
   an indicator dimension; ``flow`` where the call filters flows; and
   ``value`` in current USD millions. ``records()`` adds the columns it is
   asked for.
2. Converted rows: each filtered row joined to its provider-year factor, one
   row per requested unit, adding ``currency``, ``prices`` and ``base_year``.
   Conversion happens at row grain, before any sum.
3. Aggregated rows: converted rows summed by the call's ``by`` dimensions
   plus ``basis``, the unit columns, ``line`` and ``flow``, which always
   split a result.

``records()`` returns stage 2 for ``Sum`` and ``Method`` indicators. A
``Method`` replaces stage 1 with its own function's output.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Literal, Protocol

from oda_data._types import Basis, Code, Dimension, TableName

if TYPE_CHECKING:
    import pyarrow as pa
    from duckdb import DuckDBPyConnection, DuckDBPyRelation

    from oda_data._provenance import Release
    from oda_data._units import Unit


def _frozen[K, V](mapping: Mapping[K, V]) -> Mapping[K, V]:
    """Return a read-only copy, so a frozen dataclass cannot change later."""
    return MappingProxyType(dict(mapping))


# Table vocabulary ------------------------------------------------------------

type ColumnType = Literal["BIGINT", "VARCHAR", "DOUBLE"]


@dataclass(frozen=True, slots=True)
class Column:
    """One logical column of a table view.

    Args:
        name: The logical name bindings and filters use, such as ``provider``.
        source: The raw column in the Parquet file that oda-reader's ``scan``
            returns. The view exposes it as ``CAST(source AS type) AS name``.
        type: The DuckDB type of the logical column.
        labels: The label of each code, ``None`` keyed for a null. A call
            that groups or filters by the column shows the labels in place of
            the codes, and a code with no label raises when the query runs.
            ``None`` when the column shows its codes.
    """

    name: str
    source: str
    type: ColumnType
    labels: Mapping[Code | None, str] | None = None

    def __post_init__(self) -> None:
        if self.labels is not None:
            object.__setattr__(self, "labels", _frozen(self.labels))


@dataclass(frozen=True, slots=True)
class TableSpec:
    """The logical columns of one OECD table.

    A raw column mapped to a logical column appears in the view only under
    its logical name. When ``passthrough`` is true, every other raw column
    also appears, under its raw name, so ``records(columns=[...])`` can ask
    for it. Bindings and filters may name only logical columns.

    Args:
        table: The table, which is also the name of its DuckDB view.
        columns: The logical columns, in view order.
        passthrough: Whether unmapped raw columns appear in the view.
    """

    table: TableName
    columns: tuple[Column, ...]
    passthrough: bool = False

    @property
    def names(self) -> frozenset[str]:
        """The logical column names."""
        return frozenset(column.name for column in self.columns)


def _c(name: str, source: str, type_: ColumnType = "BIGINT") -> Column:
    return Column(name, source, type_)


#: CRS policy-marker columns. Each holds 0, 1, 2 or null (not screened);
#: desertification also holds 3.
CRS_MARKERS: tuple[str, ...] = (
    "gender",
    "environment",
    "dig",
    "trade",
    "rmnch",
    "drr",
    "nutrition",
    "disability",
    "biodiversity",
    "climate_mitigation",
    "climate_adaptation",
    "desertification",
)

#: What each policy-marker score means. A null score means the activity was
#: not screened against the marker.
_SCORE_LABELS: Mapping[Code | None, str] = {
    2: "principal",
    1: "significant",
    0: "not_targeted",
    None: "not_screened",
}


def _marker_column(marker: str) -> Column:
    """A policy-marker column with its score labels. Desertification also
    scores 3, principal and in support of an action programme against
    desertification, which the OECD counts as principal."""
    labels = _SCORE_LABELS
    if marker == "desertification":
        labels = {**labels, 3: "principal"}
    return Column(marker, marker, "BIGINT", labels)


#: Logical columns per table. ``_tables`` builds each view from this map.
#:
#: Value columns hold millions of current USD (DAC1, DAC2A and Multisystem rows
#: with ``amount_type`` ``"A"`` or ``"Current prices"``; every CRS ``usd_*``
#: column).
VOCABULARY: Mapping[TableName, TableSpec] = _frozen(
    {
        "dac1": TableSpec(
            "dac1",
            (
                _c("provider", "donor_code"),
                _c("year", "year"),
                _c("line", "aidtype_code"),
                _c("flow", "flows_code"),
                _c("amount_type", "amounttype_code", "VARCHAR"),
                _c("value", "value", "DOUBLE"),
            ),
        ),
        "dac2a": TableSpec(
            "dac2a",
            (
                _c("provider", "donor_code"),
                _c("recipient", "recipient_code"),
                _c("year", "year"),
                _c("line", "aidtype_code"),
                _c("part", "part_code"),
                _c("amount_type", "data_type_code", "VARCHAR"),
                _c("value", "value", "DOUBLE"),
            ),
        ),
        "crs": TableSpec(
            "crs",
            (
                _c("provider", "donor_code"),
                _c("recipient", "recipient_code"),
                _c("year", "year"),
                _c("flow", "flow_code"),
                _c("bi_multi", "bi_multi"),
                _c("category", "category"),
                _c("finance_type", "finance_t"),
                _c("modality", "aid_t", "VARCHAR"),
                _c("sector", "purpose_code"),
                _c("channel", "channel_code"),
                _c("commitment", "usd_commitment", "DOUBLE"),
                _c("disbursement", "usd_disbursement", "DOUBLE"),
                _c("received", "usd_received", "DOUBLE"),
                _c("grant_equivalent", "usd_grant_equiv", "DOUBLE"),
                *map(_marker_column, CRS_MARKERS),
            ),
            passthrough=True,
        ),
        "multisystem": TableSpec(
            "multisystem",
            (
                _c("provider", "donor_code"),
                _c("recipient", "recipient_code"),
                _c("year", "year"),
                _c("flow", "flow_code"),
                _c("modality", "aid_t", "VARCHAR"),
                _c("sector", "purpose_code"),
                _c("channel", "channel_code"),
                _c("flow_type", "flow_type", "VARCHAR"),
                _c("amount_type", "amount_type", "VARCHAR"),
                _c("aid_to_or_thru", "aid_to_or_thru", "VARCHAR"),
                _c("value", "amount", "DOUBLE"),
            ),
        ),
    }
)


# Filters ---------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class In:
    """Rows whose ``column`` holds one of ``values``."""

    column: str
    values: tuple[Code, ...]


@dataclass(frozen=True, slots=True)
class NotIn:
    """Rows whose ``column`` holds none of ``values``. Null rows are dropped.

    With ``when``, the rule applies only to the rows ``when`` matches, such as
    the providers left out of line 106 alone, and keeps every other row.

    On a binding's or an indicator's ``rows``, a call that filters the
    dimension held in ``column`` by one of ``values`` raises
    ``UnsupportedQueryError``, with ``reason`` in the message. With ``when``,
    it raises only if the call also names one of ``when.values``.
    """

    column: str
    values: tuple[Code, ...]
    reason: str = ""
    when: In | None = None


@dataclass(frozen=True, slots=True)
class IsNull:
    """Rows whose ``column`` is null, such as an unscreened policy marker."""

    column: str


@dataclass(frozen=True, slots=True)
class NotNull:
    """Rows whose ``column`` is not null."""

    column: str


@dataclass(frozen=True, slots=True)
class Between:
    """Rows whose ``column`` lies between ``low`` and ``high``, both included."""

    column: str
    low: int
    high: int


@dataclass(frozen=True, slots=True)
class AnyOf:
    """Rows that match at least one of ``filters``, such as mitigation or
    adaptation."""

    filters: tuple[Filter, ...]


#: A closed set of row filters over logical columns. Filters in a tuple all
#: apply (AND); ``AnyOf`` expresses OR.
type Filter = In | NotIn | IsNull | NotNull | Between | AnyOf


def filter_columns(filters: tuple[Filter, ...]) -> Iterator[str]:
    """Yield every column the filters name, descending into ``AnyOf``."""
    for item in filters:
        if isinstance(item, AnyOf):
            yield from filter_columns(item.filters)
            continue
        yield item.column
        if isinstance(item, NotIn) and item.when is not None:
            yield item.when.column


# Bases -----------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Value:
    """A basis read from a value column, as on CRS.

    Args:
        column: The value column, such as ``disbursement``.
        minus: A value column subtracted from ``column``. Net disbursements
            are ``Value("disbursement", minus="received")``. Either column's
            null reads as 0, so a repayment with no disbursement counts; a row
            with both null has no value on the basis and is left out.
        rows: Filters that apply under this basis only.
        from_year: The first year the basis covers. Earlier years raise
            ``BasisRangeError``.
    """

    column: str
    minus: str | None = None
    rows: tuple[Filter, ...] = ()
    from_year: int | None = None


@dataclass(frozen=True, slots=True)
class Flow:
    """A basis that selects one DAC1 flow code and sums ``value``.

    Args:
        code: The flow, such as 1140 (net disbursements) or 1160 (grant
            equivalents).
        from_year: The first year the basis covers.
        parts: Flows that split ``code``, such as 1121 (grants) and 1122
            (non-grants) of gross disbursements 1120. A call that filters
            ``flow`` reads the parts it names in place of ``code``, each part
            its own row; a flow outside ``parts`` raises.
        only_lines: Lines the table publishes under this flow alone, such as
            the grant-equivalent lines 11010 and 12000. Every other flow of
            the binding reads no row of them.
    """

    code: int
    from_year: int | None = None
    parts: tuple[int, ...] = ()
    only_lines: tuple[int, ...] = ()


@dataclass(frozen=True, slots=True)
class Line:
    """A basis that selects one DAC2A line and sums ``value``.

    Args:
        code: The line, such as 206 (net disbursements) or 240 (gross).
        from_year: The first year the basis covers.
    """

    code: int
    from_year: int | None = None


@dataclass(frozen=True, slots=True)
class Headline:
    """A basis that uses one basis before ``year`` and another from it on.

    Args:
        before: The basis for years before ``year``.
        after: The basis for ``year`` and later.
        year: The first year of ``after``.
    """

    before: Basis
    after: Basis
    year: int = 2018


type BasisRule = Value | Flow | Line | Headline


# Bindings --------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Binding:
    """How public dimensions and bases map onto one table's logical columns.

    Args:
        table: The table the binding reads.
        dimensions: Each public dimension and the logical column that holds
            it. ``broad_sector`` maps to ``sector``, and the compiler groups
            sector codes into broad sectors. ``score`` maps to one policy
            marker column.
        bases: Each basis the table offers and its rule.
        rows: Filters that apply to every call, such as current USD only.
        members_hold_eu_share: Whether an EU member's figures include its
            contribution to EU Institutions (918), as on DAC1, whose
            multilateral ODA counts it. A call on such a table that sums 918
            with a member it names counts that money twice, so it raises.

    Raises:
        ValueError: If a column is not in the table's vocabulary, or a
            ``Headline`` names a basis the binding lacks or another headline.
    """

    table: TableName
    dimensions: Mapping[Dimension, str]
    bases: Mapping[Basis, BasisRule]
    rows: tuple[Filter, ...] = ()
    members_hold_eu_share: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "dimensions", _frozen(self.dimensions))
        object.__setattr__(self, "bases", _frozen(self.bases))

        used = [*self.dimensions.values(), *filter_columns(self.rows)]
        for name, rule in self.bases.items():
            match rule:
                case Value(column=column, minus=minus, rows=rows):
                    used += [column, *filter_columns(rows)]
                    if minus is not None:
                        used.append(minus)
                case Flow():
                    used += ["flow", "value"]
                case Line():
                    used += ["line", "value"]
                case Headline(before=before, after=after):
                    for part in (before, after):
                        target = self.bases.get(part)
                        if target is None or isinstance(target, Headline):
                            raise ValueError(
                                f"headline basis {name!r} needs {part!r}, which "
                                f"the {self.table} binding lacks or is a headline"
                            )

        unknown = sorted(set(used) - VOCABULARY[self.table].names)
        if unknown:
            raise ValueError(f"columns not in the {self.table} vocabulary: {unknown}")


# Kinds -----------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Sum:
    """Sums the basis value over filtered rows.

    Args:
        lines: One line per basis, for indicators whose line depends on the
            basis (total ODA is 1010 on net flows, 11010 on grant
            equivalents). Empty when the indicator sums rows, or when the
            caller picks lines with ``lines=``.
    """

    lines: Mapping[Basis, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "lines", _frozen(self.lines))


@dataclass(frozen=True, slots=True)
class Ratio:
    """Sum of numerators over sum of denominators, in current USD, per cent.

    Each member's numerator pairs with its own denominator. A denominator
    with no basis, such as GNI, ignores the numerator's. A ratio refuses
    ``unit=`` and ``records()``.
    """

    numerator: Indicator
    denominator: Indicator


@dataclass(frozen=True, slots=True)
class Derived:
    """The sum of ``plus`` minus the sum of ``minus``, each indicator on the
    basis that applies in each year."""

    plus: tuple[Indicator, ...]
    minus: tuple[Indicator, ...] = ()


class MethodFunction(Protocol):
    """The function behind a ``Method`` indicator."""

    def __call__(self, cursor: DuckDBPyConnection, call: ResolvedCall, /) -> pa.Table:
        """Compute the indicator's rows.

        Args:
            cursor: A cursor holding a view for each table in
                ``Method.tables``, under the table's name. The engine pushes
                no provider filter into these views, because a method may
                need every provider's rows (imputation needs every channel's
                CRS spending when the call asks for one provider).
            call: The resolved call.

        Returns:
            An Arrow table at provider-year grain or finer, with columns
            ``provider`` (int64), ``year`` (int64), ``basis`` (string),
            ``value`` (float64, current USD millions), and one column per
            indicator dimension. The engine then applies units, ``by``,
            groups, and provenance. Extra columns reach ``records()`` only.
        """
        ...


@dataclass(frozen=True, slots=True)
class Method:
    """An indicator whose stage 1 rows come from a function.

    Args:
        function: The function, which follows ``MethodFunction``.
        tables: The tables the function reads.
    """

    function: MethodFunction
    tables: tuple[TableName, ...]


type Kind = Sum | Ratio | Derived | Method


# Indicators ------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Caveat:
    """A note on what an indicator counts.

    A caveat with a basis or years is a warning the engine emits when a call
    reads its basis in one of its years. A caveat with neither concerns every
    call, so it does not warn. ``describe()`` and ``explain()`` show it.

    Args:
        text: The note.
        basis: The concrete basis it concerns, such as ``grant_equivalent``,
            which a headline call reads from its switch year. ``None`` for
            every basis.
        years: The first and last year it concerns, both included. ``None``
            for every year.
    """

    text: str
    basis: Basis | None = None
    years: tuple[int, int] | None = None


@dataclass(frozen=True, slots=True)
class Indicator:
    """A named rule for what to count from one table.

    Args:
        id: The public id, such as ``"oecd.dac1.total_oda"``.
        name: A short title.
        description: What the indicator counts, in one or two sentences.
        binding: The table binding.
        kind: How the value is computed.
        dimensions: The dimensions a call may filter on and group by. A
            ``Sum`` indicator's dimensions are all in ``binding.dimensions``;
            a ``Method`` may declare its own, such as ``allocation_status``.
        default_basis: The basis a call gets when it passes none.
        default_providers: The name of a group in ``oda.groups`` a call gets
            when it names no providers, or ``None`` for every provider in the
            table except aggregates.
        rows: Filters on top of the binding's. A ``NotIn`` on the provider
            column excludes those providers. A call that names one raises,
            and explain() and provenance give its ``reason``.
        caveats: Notes on what the indicator counts; see ``Caveat``.
        version: Bumped when a published number changes.

    Raises:
        ValueError: If ``rows`` names a column outside the table vocabulary,
            a provider exclusion on the binding or the indicator has no
            reason, the default basis is not in the binding, a ``Sum`` names a
            dimension the binding lacks, or ``Sum.lines`` names a basis the
            binding lacks.
    """

    id: str
    name: str
    description: str
    binding: Binding
    kind: Kind
    dimensions: tuple[Dimension, ...]
    default_basis: Basis
    default_providers: str | None
    rows: tuple[Filter, ...] = ()
    caveats: tuple[Caveat, ...] = ()
    version: int = 1

    def __post_init__(self) -> None:
        binding = self.binding
        unknown = sorted(
            set(filter_columns(self.rows)) - VOCABULARY[binding.table].names
        )
        if unknown:
            raise ValueError(
                f"{self.id}: columns not in the {binding.table} vocabulary: {unknown}"
            )
        unexplained = [rule for rule in self.provider_exclusions if not rule.reason]
        if unexplained:
            raise ValueError(f"{self.id}: a provider exclusion needs a reason")
        if self.default_basis not in binding.bases:
            raise ValueError(
                f"{self.id}: default basis {self.default_basis!r} is not in the binding"
            )
        if isinstance(self.kind, Sum):
            missing = sorted(set(self.dimensions) - set(binding.dimensions))
            if missing:
                raise ValueError(f"{self.id}: the binding lacks dimensions {missing}")
            stray = sorted(set(self.kind.lines) - set(binding.bases))
            if stray:
                raise ValueError(f"{self.id}: lines name unknown bases {stray}")

    @property
    def provider_exclusions(self) -> tuple[NotIn, ...]:
        """The ``NotIn`` rules on the provider column, the binding's first.

        A rule with ``when`` leaves the providers out of some rows only, so
        they stay among the providers a call reads by default.
        """
        column = self.binding.dimensions.get("provider")
        return tuple(
            rule
            for rule in (*self.binding.rows, *self.rows)
            if isinstance(rule, NotIn) and rule.column == column and rule.values
        )


def basis_lines(indicator: Indicator, /) -> Mapping[Basis, int]:
    """Each concrete basis of a one-line indicator and the line it reads.

    The line comes from ``Sum.lines`` (DAC1) or a ``Line`` basis (DAC2A).
    Empty for an indicator that reads rows rather than one line.
    """
    found: dict[Basis, int] = {}
    if isinstance(indicator.kind, Sum):
        found |= indicator.kind.lines
    for basis, rule in indicator.binding.bases.items():
        if isinstance(rule, Line):
            found[basis] = rule.code
    return found


def line_basis(indicator: Indicator, code: Code, /) -> Basis:
    """The basis of a lines indicator that reads line ``code``.

    The default basis when it reads the line, otherwise the first basis of the
    binding that does, such as ``gross_disbursement`` for DAC2A line 240.
    Falls back to the default basis when none does.
    """
    binding = indicator.binding
    order = sorted(binding.bases, key=lambda name: name != indicator.default_basis)
    return next(
        (name for name in order if _reads_line(binding, binding.bases[name], code)),
        indicator.default_basis,
    )


def _reads_line(binding: Binding, rule: BasisRule, code: Code) -> bool:
    """Whether a basis rule reads rows of line ``code``, from the registry alone.

    A ``Value`` basis reads what its line filters keep, a ``Flow`` basis every
    line that no other flow holds alone, and a headline what both of its
    bases read.
    """
    match rule:
        case Value(rows=rows):
            column = binding.dimensions["line"]
            for item in rows:
                if item.column != column:
                    continue
                if isinstance(item, In) and code not in item.values:
                    return False
                if (
                    isinstance(item, NotIn)
                    and item.when is None
                    and code in item.values
                ):
                    return False
            return True
        case Flow():
            return not any(
                code in other.only_lines
                for other in binding.bases.values()
                if isinstance(other, Flow) and other != rule
            )
        case Headline(before=before, after=after):
            return all(
                _reads_line(binding, binding.bases[part], code)
                for part in (before, after)
            )
        case _:
            return True


def line_exclusions(named: tuple[Indicator, ...], /) -> tuple[NotIn, ...]:
    """The row exclusions of each named indicator, scoped to its lines.

    A lines indicator carries these, so a line it reads counts the rows the
    named indicator on that line counts, and no others.

    Raises:
        ValueError: A named indicator holds a row filter other than an
            unscoped ``NotIn``, which cannot be scoped to its lines.
    """
    rules: list[NotIn] = []
    for indicator in named:
        lines = tuple(sorted(set(basis_lines(indicator).values())))
        for rule in indicator.rows:
            if not isinstance(rule, NotIn) or rule.when is not None or not lines:
                raise ValueError(
                    f"{indicator.id}: only an unscoped NotIn on a one-line "
                    "indicator can carry over to its lines"
                )
            rules.append(replace(rule, when=In("line", lines)))
    return tuple(rules)


# Resolved calls --------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ResolvedGroup:
    """A group with its members as codes, per year.

    Args:
        name: The group's name, which labels its total row.
        dimension: The dimension the members belong to.
        members: Each year of the call and the member codes in that year. A
            group with one composition holds the same set in every year.
    """

    name: str
    dimension: Dimension
    members: Mapping[int, frozenset[Code]]

    def __post_init__(self) -> None:
        object.__setattr__(self, "members", _frozen(self.members))


@dataclass(frozen=True, slots=True)
class ResolvedCall:
    """A call with every name, group, and default turned into codes.

    ``_compile`` and ``_combine`` take this and do not import ``_resolve`` or
    ``_groups``.

    Args:
        basis: The basis.
        units: The units, current USD when the caller passed none. Empty for
            a ratio.
        by: The dimensions the result splits by.
        years: The years asked for, sorted, or ``None`` for every year.
        members: Each filtered dimension and its codes. Group members are
            already expanded here. A dimension absent from the map is not
            filtered, except that providers then exclude aggregate codes.
        groups: The groups the call named, with per-year members.
        totals: Names of groups in ``groups`` whose total rows the call asked
            for with ``oda.total()``.
        allow_partial: Whether the caller accepts partial sums: a group
            total sums the members that reported when some did not, and a
            sum across providers over a partial last year does not warn.
        rows: Each dimension and the codes the result shows as their own
            rows. Other codes in ``members`` feed group totals only, so
            ``providers=oda.total(g7)`` holds the G7 in ``members`` and an
            empty set here. The map holds every dimension the caller
            filtered and no other, so a default, such as the providers an
            indicator reads when the caller names none, is absent and shows
            every member code as a row. An entry for a dimension that is
            neither in ``by`` nor a group's must equal its ``members``.

    Raises:
        ValueError: If ``totals`` names a group not in ``groups``.
    """

    basis: Basis
    units: tuple[Unit, ...]
    by: tuple[Dimension, ...]
    years: tuple[int, ...] | None = None
    members: Mapping[Dimension, frozenset[Code]] = field(default_factory=dict)
    groups: tuple[ResolvedGroup, ...] = ()
    totals: tuple[str, ...] = ()
    allow_partial: bool = False
    rows: Mapping[Dimension, frozenset[Code]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "members", _frozen(self.members))
        object.__setattr__(self, "rows", _frozen(self.rows))
        stray = sorted(set(self.totals) - {group.name for group in self.groups})
        if stray:
            raise ValueError(f"totals name groups the call lacks: {stray}")


# Engine interfaces -----------------------------------------------------------


class Tables(Protocol):
    """Table access for one ``Context``. ``_tables`` implements it."""

    def cursor(self, tables: tuple[TableName, ...]) -> DuckDBPyConnection:
        """Return a new cursor with a view for each of ``tables``.

        Each view is named after its table and exposes the columns in
        ``VOCABULARY``. A table is scanned on first use and its release then
        holds for the life of the ``Context``.

        Raises:
            OfflineCacheMissError: Offline, and a table is not cached.
            ReleaseMismatchError: A pinned release differs from the cache.
            DataUnavailableError: oda-reader could not fetch a table.
        """
        ...

    def path(self, table: TableName) -> Path:
        """Return the Parquet file behind ``table``'s view.

        ``Factors`` reads DAC1 from this file, so the deflators and the ODA
        figures come from one release.
        """
        ...

    def release(self, table: TableName) -> Release:
        """Return the release of ``table`` the views read."""
        ...


@dataclass(frozen=True, slots=True)
class FactorTable:
    """Conversion factors for one call.

    Args:
        factors: An Arrow table with one row per provider, year, and unit:
            ``provider`` (int64), ``year`` (int64), ``currency`` (string),
            ``prices`` (string), ``base_year`` (int64, null for current
            prices), and ``factor`` (float64). Current USD has factor 1.
        fallback_providers: Providers that took the DAC total's factors
            (20001) because pydeflate has none of their own.
    """

    factors: pa.Table
    fallback_providers: frozenset[int]


class Factors(Protocol):
    """Conversion factors from the Context's DAC1 release. ``_factors``
    implements it."""

    def table(self, keys: pa.Table, units: tuple[Unit, ...], /) -> FactorTable:
        """Return factors for every provider-year key and unit.

        Args:
            keys: Distinct ``provider`` and ``year`` pairs (int64).
            units: The units asked for.

        Raises:
            MissingFactorError: A provider with factors lacks one year.
        """
        ...

    def release(self) -> Release:
        """Return the DAC1 release the factors come from."""
        ...


class FilterStage(Protocol):
    """Stage 1: an indicator's filtered rows. ``_compile.filtered`` follows
    it."""

    def __call__(
        self,
        cursor: DuckDBPyConnection,
        indicator: Indicator,
        call: ResolvedCall,
        /,
        *,
        columns: tuple[str, ...] = (),
    ) -> DuckDBPyRelation:
        """Return the filtered rows, plus ``columns`` for ``records()``."""
        ...


class ConvertStage(Protocol):
    """Stage 2: rows converted to each unit at row grain.
    ``_compile.converted`` follows it."""

    def __call__(
        self,
        cursor: DuckDBPyConnection,
        rows: DuckDBPyRelation,
        call: ResolvedCall,
        factors: Factors,
        /,
    ) -> tuple[DuckDBPyRelation, frozenset[int]]:
        """Return the converted rows and the providers that took fallback
        factors.

        Raises:
            MissingFactorError: A provider-year in ``rows`` has no factor.
        """
        ...


class AggregateStage(Protocol):
    """Stage 3: converted rows summed by ``by``, basis, unit, and line.
    ``_compile.aggregated`` follows it."""

    def __call__(
        self, rows: DuckDBPyRelation, call: ResolvedCall, /
    ) -> DuckDBPyRelation:
        """Return the summed rows."""
        ...

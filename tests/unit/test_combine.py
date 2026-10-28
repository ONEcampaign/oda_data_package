"""Groups, totals, ratios, derived and method indicators on in-memory tables.

Each table stands in for a view and carries the logical column names from
``_model.VOCABULARY``. Factors are fakes: 1 for current USD, and a fixed rate
per provider for other units.
"""

from collections.abc import Iterator
from dataclasses import replace

import duckdb
import pyarrow as pa
import pytest

from oda_data._combine import combine, records, tables_for
from oda_data._errors import (
    BasisRangeError,
    DimensionError,
    OdaDataWarning,
    UnsupportedQueryError,
)
from oda_data._factors import FACTOR_SCHEMA
from oda_data._model import (
    Binding,
    Caveat,
    Derived,
    FactorTable,
    Flow,
    Headline,
    In,
    Indicator,
    Line,
    Method,
    NotIn,
    Ratio,
    ResolvedCall,
    ResolvedGroup,
    Sum,
    Value,
)
from oda_data._units import Unit, constant, current

USD = current("USD")
USD_2023 = constant("USD", 2023)


class FakeFactors:
    """Factor 1 for current USD, ``rates[provider]`` for any other unit."""

    def __init__(self, rates: dict[int, float] | None = None) -> None:
        self.rates = rates or {}

    def table(self, keys: pa.Table, units: tuple[Unit, ...], /) -> FactorTable:
        rows = [
            key
            | {
                "currency": unit.currency,
                "prices": unit.prices,
                "base_year": unit.base_year,
                "factor": 1.0 if unit == USD else self.rates[key["provider"]],
            }
            for key in keys.to_pylist()
            for unit in units
        ]
        table = pa.Table.from_pylist(rows, schema=FACTOR_SCHEMA)
        return FactorTable(table, frozenset())

    def release(self) -> object:
        raise NotImplementedError


# Tables -------------------------------------------------------------------------

DAC1_ROWS = [
    # provider, year, line, flow, value. Every row is amount type A.
    # Net flows in 2017, grant equivalents in 2018.
    (1, 2017, 1010, 1140, 10.0),
    (2, 2017, 1010, 1140, 20.0),
    (3, 2017, 1010, 1140, 30.0),
    (4, 2017, 1010, 1140, 40.0),  # provider 4 reports no GNI
    (1, 2018, 11010, 1160, 11.0),
    (2, 2018, 11010, 1160, 22.0),
    # GNI is line 1 under net flows. Provider 3 reports nothing in 2018.
    (1, 2017, 1, 1140, 1000.0),
    (2, 2017, 1, 1140, 2000.0),
    (3, 2017, 1, 1140, 3000.0),
    (1, 2018, 1, 1140, 1100.0),
    (2, 2018, 1, 1140, 2200.0),
    # In-donor refugee costs.
    (1, 2017, 1820, 1140, 1.0),
    (1, 2018, 1820, 1160, 2.0),
    (2, 2018, 1820, 1160, 3.0),
    # The DAC total holds its members' rows.
    (20001, 2017, 1010, 1140, 60.0),
    (20001, 2018, 11010, 1160, 33.0),
    # Provider 5 first reports in 2018.
    (5, 2018, 11010, 1160, 5.0),
    # Provider 12 reports ODA in 2017 and GNI in both years, but no ODA in 2018.
    (12, 2017, 1010, 1140, 8.0),
    (12, 2017, 1, 1140, 800.0),
    (12, 2018, 1, 1140, 900.0),
    # Provider 13 reports GNI and has never reported ODA.
    (13, 2017, 1, 1140, 700.0),
    # Gross disbursements split into grants (1121) and non-grants (1122).
    (1, 2017, 1010, 1121, 6.0),
    (1, 2017, 1010, 1122, 4.0),
]

CRS_ROWS = [
    # provider, recipient, year, category, bi_multi, disbursement
    (1, 300, 2019, 10, 1, 4.0),  # provider 1 gave Kenya (248) nothing
    (2, 248, 2019, 10, 1, 7.0),
    (2, 300, 2019, 10, 1, 9.0),
    (3, 248, 2018, 10, 1, 5.0),  # provider 3 reported only in 2018
]

DAC2A_ROWS = [
    # provider, recipient, year, line, value
    (1, 248, 2019, 206, 5.0),
    (1, 10280, 2019, 206, 2.0),  # Middle Africa unspecified, a real row
    (1, 10100, 2019, 206, 7.0),  # Developing countries, a total
    (1, 15006, 2019, 206, 9.0),  # Regional and unspecified, a total
]


@pytest.fixture
def con() -> Iterator[duckdb.DuckDBPyConnection]:
    connection = duckdb.connect()
    connection.execute(
        "CREATE TABLE dac1 (provider BIGINT, year BIGINT, line BIGINT, flow BIGINT,"
        " amount_type VARCHAR, value DOUBLE)"
    )
    connection.executemany("INSERT INTO dac1 VALUES (?, ?, ?, ?, 'A', ?)", DAC1_ROWS)
    connection.execute(
        "CREATE TABLE crs (provider BIGINT, recipient BIGINT, year BIGINT,"
        " category BIGINT, bi_multi BIGINT, disbursement DOUBLE)"
    )
    connection.executemany("INSERT INTO crs VALUES (?, ?, ?, ?, ?, ?)", CRS_ROWS)
    connection.execute(
        "CREATE TABLE dac2a (provider BIGINT, recipient BIGINT, year BIGINT,"
        " line BIGINT, value DOUBLE)"
    )
    connection.executemany("INSERT INTO dac2a VALUES (?, ?, ?, ?, ?)", DAC2A_ROWS)
    yield connection
    connection.close()


# Indicators ---------------------------------------------------------------------

DAC1 = Binding(
    table="dac1",
    dimensions={"provider": "provider", "line": "line", "year": "year"},
    bases={
        "net_disbursement": Flow(1140),
        "grant_equivalent": Flow(1160, from_year=2018),
        "headline": Headline("net_disbursement", "grant_equivalent"),
    },
    rows=(In("amount_type", ("A",)),),
    members_hold_eu_share=True,
)

CRS = Binding(
    table="crs",
    dimensions={"provider": "provider", "recipient": "recipient", "year": "year"},
    bases={"gross_disbursement": Value("disbursement", rows=(In("category", (10,)),))},
    rows=(In("bi_multi", (1, 3, 7, 8)),),
)

DAC2A = Binding(
    table="dac2a",
    dimensions={"provider": "provider", "recipient": "recipient", "year": "year"},
    bases={"headline": Line(206)},
)


def dac1(id_: str, lines: dict, **overrides: object) -> Indicator:
    fields: dict = {
        "id": id_,
        "name": id_,
        "description": id_,
        "binding": DAC1,
        "kind": Sum(lines=lines),
        "dimensions": ("provider", "year"),
        "default_basis": "headline",
        "default_providers": "dac_countries",
    }
    return Indicator(**(fields | overrides))


TOTAL_ODA = dac1(
    "oecd.dac1.total_oda", {"net_disbursement": 1010, "grant_equivalent": 11010}
)
IDRC = dac1("oecd.dac1.idrc", {"net_disbursement": 1820, "grant_equivalent": 1820})
GNI = dac1("oecd.dac1.gni", {"net_disbursement": 1}, default_basis="net_disbursement")
ODA_GNI = dac1("oecd.dac1.oda_gni_ratio", {}, kind=Ratio(TOTAL_ODA, GNI))
ODA_EXCL_IDRC = dac1(
    "one.dac1.oda_excluding_idrc", {}, kind=Derived(plus=(TOTAL_ODA,), minus=(IDRC,))
)

BILATERAL = Indicator(
    id="oecd.crs.bilateral_oda",
    name="Bilateral ODA",
    description="Bilateral ODA.",
    binding=CRS,
    kind=Sum(),
    dimensions=("provider", "recipient", "year"),
    default_basis="gross_disbursement",
    default_providers=None,
)

RECIPIENT_ODA = Indicator(
    id="oecd.dac2a.recipient_oda",
    name="Recipient ODA",
    description="ODA by recipient.",
    binding=DAC2A,
    kind=Sum(),
    dimensions=("provider", "recipient", "year"),
    default_basis="headline",
    default_providers=None,
)


def call(**overrides: object) -> ResolvedCall:
    fields: dict = {
        "basis": "headline",
        "units": (USD,),
        "by": ("provider", "year"),
        "years": (2017, 2018),
    }
    return ResolvedCall(**(fields | overrides))


def group(
    name: str, codes: set, years: tuple = (2017, 2018), dimension: str = "provider"
) -> ResolvedGroup:
    return ResolvedGroup(name, dimension, dict.fromkeys(years, frozenset(codes)))


def with_total(
    g: ResolvedGroup, shown: set | None = None, **overrides: object
) -> ResolvedCall:
    """A call for ``[g, oda.total(g)]``, or for ``oda.total(g)`` alone when
    ``shown`` is empty. ``shown`` holds the codes shown as their own rows."""
    codes = frozenset().union(*g.members.values())
    rows = {} if shown is None else {g.dimension: frozenset(shown)}
    return call(
        members={g.dimension: codes},
        rows=rows,
        groups=(g,),
        totals=(g.name,),
        **overrides,
    )


def rows(result: pa.Table) -> list[dict]:
    return result.to_pylist()


def values(result: pa.Table, *keys: str) -> dict:
    return {tuple(row[key] for key in keys): row["value"] for row in rows(result)}


def totals(result: pa.Table, *keys: str) -> dict:
    return {
        tuple(row[key] for key in keys): row["value"]
        for row in rows(result)
        if row["is_aggregate"]
    }


def _preliminary_2019(con: duckdb.DuckDBPyConnection) -> None:
    """2019 is the table's last year: providers 1, 2 and 6 report total ODA,
    and only provider 1 refugee costs. Provider 2 reported them in 2018;
    provider 6 last reported them in 2017."""
    con.executemany(
        "INSERT INTO dac1 VALUES (?, 2019, ?, 1160, 'A', ?)",
        [(1, 11010, 12.0), (2, 11010, 24.0), (6, 11010, 1.0), (1, 1820, 5.0)],
    )
    con.execute("INSERT INTO dac1 VALUES (6, 2017, 1820, 1140, 'A', 4.0)")


# Group totals and allow_partial ---------------------------------------------------


@pytest.mark.parametrize(
    ("allow_partial", "expected"),
    [(False, {2017: 60.0, 2018: None}), (True, {2017: 60.0, 2018: 33.0})],
    ids=["null", "allow-partial"],
)
def test_group_total_is_null_once_a_member_stops_reporting(
    con: duckdb.DuckDBPyConnection, allow_partial: bool, expected: dict
) -> None:
    g = group("trio", {1, 2, 3})

    result = combine(
        con, TOTAL_ODA, with_total(g, allow_partial=allow_partial), FakeFactors()
    ).table

    assert totals(result, "provider", "year") == {
        ("trio", year): value for year, value in expected.items()
    }
    assert values(result, "provider", "year")[("3", 2017)] == 30.0


@pytest.mark.parametrize(
    "allow_partial", [False, True], ids=["strict", "allow-partial"]
)
def test_a_member_that_has_not_yet_reported_counts_as_zero(
    con: duckdb.DuckDBPyConnection, allow_partial: bool
) -> None:
    # Provider 5 reports nothing to the table before 2018.
    g = group("late", {1, 5})
    resolved = with_total(g, shown=set(), allow_partial=allow_partial)

    result = combine(con, TOTAL_ODA, resolved, FakeFactors()).table

    assert totals(result, "year") == {(2017,): 10.0, (2018,): 11.0 + 5.0}


@pytest.mark.parametrize(
    ("returns", "expected"),
    [(True, 11.0), (False, None)],
    ids=["interior-gap-counts-as-zero", "trailing-gap-is-null"],
)
def test_only_a_member_that_stopped_reporting_nulls_a_total(
    con: duckdb.DuckDBPyConnection, returns: bool, expected: float | None
) -> None:
    # Provider 6 reports in 2017 and nothing in 2018. With a 2019 row it has
    # skipped 2018 and counts as zero; without one it has stopped.
    con.execute("INSERT INTO dac1 VALUES (6, 2017, 1010, 1140, 'A', 3.0)")
    if returns:
        con.execute("INSERT INTO dac1 VALUES (6, 2019, 11010, 1160, 'A', 4.0)")
    g = group("pair", {1, 6}, years=(2018,))
    resolved = with_total(g, shown=set(), years=(2018,))

    result = combine(con, TOTAL_ODA, resolved, FakeFactors()).table

    assert totals(result, "year") == {(2018,): expected}


@pytest.mark.parametrize(
    ("members", "allow_partial", "expected"),
    [({1, 2}, False, None), ({1, 2}, True, 5.0), ({1, 6}, False, 5.0)],
    ids=["dropped-is-null", "dropped-allow-partial", "absent-the-year-before"],
)
def test_a_member_that_drops_a_line_in_the_last_year_nulls_the_total(
    con: duckdb.DuckDBPyConnection,
    members: set,
    allow_partial: bool,
    expected: float | None,
) -> None:
    _preliminary_2019(con)
    g = group("pair", members, years=(2019,))
    resolved = with_total(g, shown=set(), years=(2019,), allow_partial=allow_partial)

    result = combine(con, IDRC, resolved, FakeFactors()).table

    assert totals(result, "year") == {(2019,): expected}


@pytest.mark.parametrize(
    ("members", "expected"),
    [({1, 2}, 7.0), ({2, 3}, None)],
    ids=["any-row-counts", "no-row-is-null"],
)
def test_crs_reporting_is_any_row_for_the_provider_year(
    con: duckdb.DuckDBPyConnection, members: set, expected: float | None
) -> None:
    # Provider 1 has a 2019 CRS row, though none to Kenya; provider 3 has none.
    g = group("pair", members, years=(2019,))
    resolved = call(
        basis="gross_disbursement",
        years=(2019,),
        by=("provider",),
        members={"provider": frozenset(members), "recipient": frozenset({248})},
        groups=(g,),
        totals=("pair",),
    )

    result = combine(con, BILATERAL, resolved, FakeFactors()).table

    assert totals(result, "provider") == {("pair",): expected}


@pytest.mark.parametrize(
    ("shown", "expected"),
    [
        (set(), {("duo", 2017, True): 30.0, ("duo", 2018, True): 33.0}),
        (
            None,
            {
                ("1", 2017, False): 10.0,
                ("2", 2017, False): 20.0,
                ("1", 2018, False): 11.0,
                ("2", 2018, False): 22.0,
                ("duo", 2017, True): 30.0,
                ("duo", 2018, True): 33.0,
            },
        ),
        (
            {1},
            {
                ("1", 2017, False): 10.0,
                ("1", 2018, False): 11.0,
                ("duo", 2017, True): 30.0,
                ("duo", 2018, True): 33.0,
            },
        ),
    ],
    ids=["total-alone", "members-and-total", "one-member-and-total"],
)
def test_rows_decide_which_members_show_beside_a_total(
    con: duckdb.DuckDBPyConnection, shown: set | None, expected: dict
) -> None:
    g = group("duo", {1, 2})

    result = combine(con, TOTAL_ODA, with_total(g, shown=shown), FakeFactors()).table

    assert values(result, "provider", "year", "is_aggregate") == expected


def test_rows_on_a_dimension_the_result_does_not_hold_narrow_nothing(
    con: duckdb.DuckDBPyConnection,
) -> None:
    kenya = frozenset({248})
    resolved = call(
        basis="gross_disbursement",
        years=(2019,),
        by=("provider",),
        members={"recipient": kenya},
        rows={"recipient": kenya},
    )

    result = combine(con, BILATERAL, resolved, FakeFactors()).table

    assert values(result, "provider") == {("2",): 7.0}


def test_member_rows_order_codes_as_numbers(con: duckdb.DuckDBPyConnection) -> None:
    resolved = call(years=(2017,), by=("provider",), members={"provider": {12, 2}})

    result = combine(con, TOTAL_ODA, resolved, FakeFactors()).table

    assert result.column("provider").to_pylist() == ["2", "12"]


def test_rows_outside_by_and_groups_are_refused(
    con: duckdb.DuckDBPyConnection,
) -> None:
    resolved = call(
        by=("year",),
        members={"provider": frozenset({1, 2})},
        rows={"provider": frozenset({1})},
    )

    with pytest.raises(ValueError, match="rows narrows"):
        combine(con, TOTAL_ODA, resolved, FakeFactors())


def test_dated_group_sums_each_years_members(con: duckdb.DuckDBPyConnection) -> None:
    g = ResolvedGroup(
        "dated", "provider", {2017: frozenset({1}), 2018: frozenset({1, 2})}
    )

    result = combine(con, TOTAL_ODA, with_total(g, shown=set()), FakeFactors()).table

    assert totals(result, "year") == {(2017,): 10.0, (2018,): 33.0}


def test_total_needs_its_dimension_in_by(con: duckdb.DuckDBPyConnection) -> None:
    with pytest.raises(UnsupportedQueryError) as raised:
        combine(
            con,
            TOTAL_ODA,
            with_total(group("duo", {1, 2}), by=("year",)),
            FakeFactors(),
        )

    assert raised.value.code == "unsupported_query"


def test_total_needs_every_row_year_in_the_group(
    con: duckdb.DuckDBPyConnection,
) -> None:
    g = group("duo", {1, 2}, years=(2017,))

    with pytest.raises(ValueError, match="2018"):
        combine(con, TOTAL_ODA, with_total(g), FakeFactors())


# Filters are sets ---------------------------------------------------------------


def test_a_provider_in_two_groups_counts_once(con: duckdb.DuckDBPyConnection) -> None:
    first, second = group("first", {1, 2}), group("second", {2, 3})
    resolved = call(
        years=(2017,),
        by=("year",),
        members={"provider": frozenset({1, 2, 3})},
        groups=(first, second),
    )
    with_totals = call(
        years=(2017,),
        members={"provider": frozenset({1, 2, 3})},
        rows={"provider": frozenset()},
        groups=(first, second),
        totals=("first", "second"),
    )

    summed = combine(con, TOTAL_ODA, resolved, FakeFactors()).table
    split = combine(con, TOTAL_ODA, with_totals, FakeFactors()).table

    assert values(summed, "year") == {(2017,): 60.0}
    assert values(split, "provider") == {("first",): 30.0, ("second",): 50.0}


# Aggregate rows -------------------------------------------------------------------


def test_named_aggregate_returns_its_own_row_flagged(
    con: duckdb.DuckDBPyConnection,
) -> None:
    resolved = call(members={"provider": frozenset({1, 20001})}, years=(2017,))

    result = combine(con, TOTAL_ODA, resolved, FakeFactors()).table

    assert values(result, "provider", "is_aggregate") == {
        ("1", False): 10.0,
        ("20001", True): 60.0,
    }


@pytest.mark.parametrize(
    "resolved",
    [
        call(by=("year",), members={"provider": frozenset({1, 20001})}),
        with_total(group("mixed", {1, 20001})),
    ],
    ids=["summed-with-members", "in-a-total"],
)
def test_aggregates_are_never_summed_with_members(
    con: duckdb.DuckDBPyConnection, resolved: ResolvedCall
) -> None:
    with pytest.raises(UnsupportedQueryError):
        combine(con, TOTAL_ODA, resolved, FakeFactors())


@pytest.mark.parametrize(
    ("indicator", "by", "member", "years", "named", "raises"),
    [
        (TOTAL_ODA, ("year",), 4, (2017, 2018), True, True),
        (TOTAL_ODA, ("provider", "year"), 4, (2017, 2018), True, False),
        (TOTAL_ODA, ("year",), 4, (2017, 2018), False, False),
        (TOTAL_ODA, ("year",), 12, (2020,), True, True),
        (TOTAL_ODA, ("year",), 12, (2021,), True, False),
        (TOTAL_ODA, ("year",), 12, None, True, True),
        (ODA_GNI, ("year",), 4, (2017, 2018), True, True),
        (BILATERAL, ("year",), 4, (2019,), True, False),
    ],
    ids=[
        "named-and-summed",
        "split-by-provider",
        "a-default",
        "uk-while-a-member",
        "uk-after-it-left",
        "uk-every-year",
        "a-ratio-over-dac1",
        "a-binding-whose-members-leave-out-their-eu-share",
    ],
)
def test_eu_institutions_are_never_summed_with_named_eu_members(
    con: duckdb.DuckDBPyConnection,
    indicator: Indicator,
    by: tuple,
    member: int,
    years: tuple | None,
    named: bool,
    raises: bool,
) -> None:
    # France (4) is an EU member; the United Kingdom (12) was one to 2020. A
    # default holds no entry in ``rows``. Only DAC1 members' figures hold their
    # contribution to EU Institutions.
    providers = frozenset({918, member})
    resolved = call(
        basis=indicator.default_basis,
        units=() if isinstance(indicator.kind, Ratio) else (USD,),
        by=by,
        years=years,
        members={"provider": providers},
        rows={"provider": providers} if named else {},
    )

    if raises:
        with pytest.raises(UnsupportedQueryError, match="918"):
            combine(con, indicator, resolved, FakeFactors())
    else:
        combine(con, indicator, resolved, FakeFactors())


@pytest.mark.parametrize(
    ("indicator", "codes", "years", "raises"),
    [
        (TOTAL_ODA, {918, 4}, (2017, 2018), True),
        (TOTAL_ODA, {918, 12}, (2020,), True),
        (TOTAL_ODA, {918, 12}, (2021,), False),
        (BILATERAL, {918, 4}, (2019,), False),
    ],
    ids=["dac1", "uk-while-a-member", "uk-after-it-left", "crs"],
)
def test_a_group_total_of_eu_institutions_and_a_member_follows_the_binding(
    con: duckdb.DuckDBPyConnection,
    indicator: Indicator,
    codes: set,
    years: tuple,
    raises: bool,
) -> None:
    # A group holding 918 and France resolves on any table; only a binding
    # whose members' figures hold their EU share refuses its total.
    g = group("eu", codes, years=years)
    resolved = with_total(g, shown=set(), years=years, basis=indicator.default_basis)

    if raises:
        with pytest.raises(UnsupportedQueryError, match="group 'eu' holds EU"):
            combine(con, indicator, resolved, FakeFactors())
    else:
        combine(con, indicator, resolved, FakeFactors())


@pytest.mark.parametrize(
    ("recipients", "expected"),
    [
        (None, {("1", False): 5.0 + 2.0}),
        (frozenset({10100}), {("1", True): 7.0}),
    ],
    ids=["unnamed-drops-totals", "named-total"],
)
def test_recipient_totals_are_dropped_unless_named(
    con: duckdb.DuckDBPyConnection,
    recipients: frozenset | None,
    expected: dict,
) -> None:
    members = {} if recipients is None else {"recipient": recipients}
    resolved = call(years=(2019,), by=("provider",), members=members)

    result = combine(con, RECIPIENT_ODA, resolved, FakeFactors()).table

    assert values(result, "provider", "is_aggregate") == expected
    assert "recipient" not in result.column_names


# Basis, unit and line split; conversion before sums -----------------------------


def test_totals_convert_each_member_before_the_sum_and_split_by_unit(
    con: duckdb.DuckDBPyConnection,
) -> None:
    g = group("duo", {1, 2})
    resolved = with_total(g, shown=set(), years=(2017,), units=(USD, USD_2023))

    result = combine(con, TOTAL_ODA, resolved, FakeFactors({1: 2.0, 2: 10.0})).table

    assert values(result, "provider", "prices") == {
        ("duo", "current"): 30.0,
        ("duo", "constant"): 10.0 * 2.0 + 20.0 * 10.0,
    }


def test_result_columns(con: duckdb.DuckDBPyConnection) -> None:
    result = combine(con, TOTAL_ODA, with_total(group("duo", {1, 2})), FakeFactors())

    assert result.table.column_names == [
        "provider",
        "year",
        "indicator",
        "indicator_version",
        "basis",
        "currency",
        "prices",
        "base_year",
        "scale",
        "is_aggregate",
        "value",
    ]
    first = rows(result.table)[0]
    assert first["provider"] == "1"
    assert first["scale"] == "millions"
    assert [row["is_aggregate"] for row in rows(result.table)] == [
        False,
        False,
        False,
        False,
        True,
        True,
    ]


# Ratio -----------------------------------------------------------------------------


def test_ratio_pairs_each_members_numerator_with_its_denominator(
    con: duckdb.DuckDBPyConnection,
) -> None:
    # Headline: net flows in 2017, grant equivalents in 2018; GNI is line 1
    # under net flows in both years.
    g = group("trio", {1, 2, 3})

    result = combine(con, ODA_GNI, with_total(g, units=()), FakeFactors()).table

    assert values(result, "provider", "year") == pytest.approx(
        {
            ("1", 2017): 1.0,
            ("2", 2017): 1.0,
            ("3", 2017): 1.0,
            ("1", 2018): 1.0,
            ("2", 2018): 1.0,
            ("trio", 2017): 100 * 60.0 / 6000.0,
            ("trio", 2018): None,
        }
    )
    first = rows(result)[0]
    assert (first["scale"], first["currency"], first["prices"]) == (
        "percent",
        None,
        None,
    )


def test_partial_ratio_sums_only_paired_members(
    con: duckdb.DuckDBPyConnection,
) -> None:
    g = group("trio", {1, 2, 3})
    resolved = with_total(g, shown=set(), units=(), allow_partial=True, years=(2018,))

    result = combine(con, ODA_GNI, resolved, FakeFactors()).table

    assert totals(result, "year") == {(2018,): pytest.approx(100 * 33.0 / 3300.0)}


@pytest.mark.parametrize(
    ("member", "year", "allow_partial", "expected"),
    [
        # Provider 4 has ODA and has never reported GNI: left out of both.
        (4, 2017, False, 100 * 10.0 / 1000.0),
        # Provider 13 has GNI and has never reported ODA: left out of both.
        (13, 2017, False, 100 * 10.0 / 1000.0),
        # Provider 12 reported ODA in 2017 and has none in 2018.
        (12, 2018, False, None),
        (12, 2018, True, 100 * 11.0 / 1100.0),
    ],
    ids=["no-gni-yet", "no-oda-yet", "oda-gap", "oda-gap-allow-partial"],
)
def test_ratio_total_pairs_members_that_have_reported(
    con: duckdb.DuckDBPyConnection,
    member: int,
    year: int,
    allow_partial: bool,
    expected: float | None,
) -> None:
    g = group("pair", {1, member}, years=(year,))
    resolved = with_total(
        g, shown=set(), units=(), years=(year,), allow_partial=allow_partial
    )

    result = combine(con, ODA_GNI, resolved, FakeFactors()).table

    assert totals(result, "year") == {(year,): pytest.approx(expected)}


@pytest.mark.parametrize(
    ("returns", "expected"),
    [(True, 100 * 11.0 / 1100.0), (False, None)],
    ids=["interior-oda-gap-left-out", "trailing-oda-gap-is-null"],
)
def test_ratio_total_nulls_only_on_a_member_that_stopped_reporting(
    con: duckdb.DuckDBPyConnection, returns: bool, expected: float | None
) -> None:
    # Provider 12 reports ODA in 2017 and none in 2018, and GNI in both. A
    # 2019 ODA row makes 2018 an interior gap in its numerator.
    if returns:
        con.execute("INSERT INTO dac1 VALUES (12, 2019, 11010, 1160, 'A', 9.0)")
    g = group("pair", {1, 12}, years=(2018,))
    resolved = with_total(g, shown=set(), units=(), years=(2018,))

    result = combine(con, ODA_GNI, resolved, FakeFactors()).table

    assert totals(result, "year") == {(2018,): pytest.approx(expected)}


def test_ratio_rows_carry_the_numerators_basis_in_each_year(
    con: duckdb.DuckDBPyConnection,
) -> None:
    resolved = call(units=(), members={"provider": frozenset({1})})

    result = combine(con, ODA_GNI, resolved, FakeFactors()).table

    assert values(result, "year", "basis") == {
        (2017, "net_disbursement"): 1.0,
        (2018, "grant_equivalent"): 1.0,
    }


def test_ratio_refuses_units(con: duckdb.DuckDBPyConnection) -> None:
    with pytest.raises(UnsupportedQueryError):
        combine(con, ODA_GNI, call(units=(USD,)), FakeFactors())


def test_ratio_refuses_records(con: duckdb.DuckDBPyConnection) -> None:
    with pytest.raises(UnsupportedQueryError):
        records(con, ODA_GNI, call(units=()), FakeFactors())


# Derived ----------------------------------------------------------------------------


def test_derived_uses_the_basis_of_each_year(con: duckdb.DuckDBPyConnection) -> None:
    resolved = call(members={"provider": frozenset({1})}, units=(USD, USD_2023))

    result = combine(con, ODA_EXCL_IDRC, resolved, FakeFactors({1: 3.0})).table

    assert values(result, "year", "prices") == {
        (2017, "current"): 10.0 - 1.0,
        (2018, "current"): 11.0 - 2.0,
        (2017, "constant"): (10.0 - 1.0) * 3.0,
        (2018, "constant"): (11.0 - 2.0) * 3.0,
    }
    assert "line" not in result.column_names


@pytest.mark.parametrize(
    ("by", "providers", "allow_partial", "expected", "nulled"),
    [
        (
            ("provider", "year"),
            {1, 2, 6},
            False,
            {("1", 2019): 7.0, ("2", 2019): None, ("6", 2019): 1.0},
            (2019, frozenset({2})),
        ),
        (
            ("provider", "year"),
            {1, 2, 6},
            True,
            {("1", 2019): 7.0, ("2", 2019): 24.0, ("6", 2019): 1.0},
            None,
        ),
        (("year",), {2}, False, {(2019,): None}, (2019, frozenset({2}))),
        (("year",), {6}, False, {(2019,): 1.0}, None),
    ],
    ids=["by-provider", "allow-partial", "one-provider", "interior-gap-is-zero"],
)
def test_derived_member_row_is_null_when_a_part_drops_in_the_last_year(
    con: duckdb.DuckDBPyConnection,
    by: tuple,
    providers: set,
    allow_partial: bool,
    expected: dict,
    nulled: tuple | None,
) -> None:
    _preliminary_2019(con)
    resolved = call(
        by=by,
        years=(2019,),
        members={"provider": frozenset(providers)},
        rows={"provider": frozenset(providers)},
        allow_partial=allow_partial,
    )

    combined = combine(con, ODA_EXCL_IDRC, resolved, FakeFactors())

    keys = ("provider", "year") if "provider" in by else ("year",)
    assert values(combined.table, *keys) == expected
    assert combined.nulled == nulled


def test_derived_member_rows_before_the_last_year_are_never_null(
    con: duckdb.DuckDBPyConnection,
) -> None:
    _preliminary_2019(con)
    resolved = call(
        years=(2018, 2019), members={"provider": frozenset({2})}, by=("year",)
    )

    result = combine(con, ODA_EXCL_IDRC, resolved, FakeFactors()).table

    assert values(result, "year") == {(2018,): 22.0 - 3.0, (2019,): None}


# Method ------------------------------------------------------------------------------


class Imputation:
    """A fake method that records what it saw and returns fixed rows."""

    def __init__(self) -> None:
        self.seen: list[ResolvedCall] = []

    def __call__(
        self, cursor: duckdb.DuckDBPyConnection, call: ResolvedCall, /
    ) -> pa.Table:
        self.seen.append(call)
        providers = cursor.sql("SELECT count(DISTINCT provider) FROM crs").fetchall()
        assert providers == [(3,)]  # the engine pushes no provider filter
        return pa.table(
            {
                "provider": pa.array([1, 1, 2, 20001], pa.int64()),
                "year": pa.array([2019, 2019, 2019, 2019], pa.int64()),
                "basis": ["gross_disbursement"] * 4,
                "allocation_status": [
                    "allocated",
                    "unallocated",
                    "allocated",
                    "allocated",
                ],
                "channel_name": ["a", "b", "c", "d"],
                "value": [1.0, 2.0, 4.0, 100.0],
            }
        )


def imputed(function: Imputation, **overrides: object) -> Indicator:
    fields: dict = {
        "id": "one.crs.imputed_multilateral_oda",
        "name": "Imputed multilateral ODA",
        "description": "Imputed multilateral ODA.",
        "binding": Binding(
            table="crs",
            dimensions=CRS.dimensions,
            bases={
                "gross_disbursement": Value("disbursement"),
                "grant_equivalent": Value("disbursement", from_year=2020),
            },
        ),
        "kind": Method(function, tables=("crs",)),
        "dimensions": ("provider", "year", "allocation_status"),
        "default_basis": "gross_disbursement",
        "default_providers": None,
    }
    return Indicator(**(fields | overrides))


def test_method_rows_take_units_by_and_groups(con: duckdb.DuckDBPyConnection) -> None:
    g = group("duo", {1, 2}, years=(2019,))
    resolved = with_total(
        g,
        basis="gross_disbursement",
        years=(2019,),
        by=("provider", "allocation_status"),
        units=(USD_2023,),
    )

    result = combine(
        con, imputed(Imputation()), resolved, FakeFactors({1: 2.0, 2: 10.0})
    )

    assert values(result.table, "provider", "allocation_status") == {
        ("1", "allocated"): 2.0,
        ("1", "unallocated"): 4.0,
        ("2", "allocated"): 40.0,
        ("duo", "allocated"): 42.0,
        ("duo", "unallocated"): 4.0,
    }


def test_method_records_keep_its_extra_columns(con: duckdb.DuckDBPyConnection) -> None:
    function = Imputation()
    resolved = call(basis="gross_disbursement", years=(2019,))

    result = records(con, imputed(function), resolved, FakeFactors()).table

    assert sorted(result.column("channel_name").to_pylist()) == ["a", "b", "c"]
    assert function.seen == [resolved]


def test_method_drops_the_providers_its_rows_exclude(
    con: duckdb.DuckDBPyConnection,
) -> None:
    indicator = imputed(Imputation(), rows=(NotIn("provider", (2,), reason="twice"),))

    result = combine(
        con, indicator, call(basis="gross_disbursement", years=(2019,)), FakeFactors()
    )

    assert values(result.table, "provider") == {("1",): 3.0}


def test_method_basis_before_its_start_raises(con: duckdb.DuckDBPyConnection) -> None:
    with pytest.raises(BasisRangeError):
        combine(
            con,
            imputed(Imputation()),
            call(basis="grant_equivalent", years=(2019,)),
            FakeFactors(),
        )


# Errors, warnings, tables -------------------------------------------------------------


_EXCLUDE_2 = NotIn("provider", (2,), reason="counted by provider 1")
_BILATERAL_WITHOUT_2 = replace(BILATERAL, rows=(_EXCLUDE_2,))


@pytest.mark.parametrize(
    "indicator",
    [
        pytest.param(_BILATERAL_WITHOUT_2, id="sum"),
        pytest.param(imputed(Imputation(), rows=(_EXCLUDE_2,)), id="method"),
        pytest.param(
            replace(BILATERAL, id="derived", kind=Derived((_BILATERAL_WITHOUT_2,))),
            id="derived-part",
        ),
    ],
)
def test_naming_an_excluded_provider_raises_with_the_reason(
    con: duckdb.DuckDBPyConnection, indicator: Indicator
) -> None:
    resolved = call(
        basis="gross_disbursement", years=(2019,), members={"provider": {1, 2}}
    )

    with pytest.raises(UnsupportedQueryError, match="provider 2: counted by"):
        combine(con, indicator, resolved, FakeFactors())


def test_dimension_outside_the_indicator_raises(con: duckdb.DuckDBPyConnection) -> None:
    with pytest.raises(DimensionError):
        combine(con, ODA_GNI, call(units=(), by=("recipient",)), FakeFactors())


def test_scoped_caveats_warn_and_unscoped_ones_do_not(
    con: duckdb.DuckDBPyConnection,
) -> None:
    # pytest turns the unscoped caveat's warning, were there one, into an error.
    caveated = dac1(
        "oecd.dac1.idrc",
        IDRC.kind.lines,  # type: ignore[union-attr]
        caveats=(
            Caveat("Few members report this before 2018.", years=(2010, 2017)),
            Caveat("Some members count students too."),
        ),
    )
    derived = dac1("x", {}, kind=Derived(plus=(TOTAL_ODA,), minus=(caveated,)))

    with pytest.warns(OdaDataWarning, match="Few members") as caught:
        combine(con, derived, call(members={"provider": frozenset({1})}), FakeFactors())

    assert len(caught) == 1


def test_records_refuse_a_column_the_view_lacks(
    con: duckdb.DuckDBPyConnection,
) -> None:
    resolved = call(basis="gross_disbursement", years=(2019,))

    with pytest.raises(UnsupportedQueryError, match="project_title"):
        records(con, BILATERAL, resolved, FakeFactors(), columns=("project_title",))


def test_a_flow_filter_splits_the_result_even_outside_by(
    con: duckdb.DuckDBPyConnection,
) -> None:
    binding = Binding(
        table="dac1",
        dimensions={"provider": "provider", "flow": "flow", "year": "year"},
        bases={"gross_disbursement": Flow(1120, parts=(1121, 1122))},
        rows=DAC1.rows,
    )
    gross = dac1(
        "oecd.dac1.total_oda",
        {"gross_disbursement": 1010},
        binding=binding,
        dimensions=("provider", "year", "flow"),
        default_basis="gross_disbursement",
    )
    resolved = call(
        basis="gross_disbursement",
        years=(2017,),
        by=("year",),
        members={"provider": frozenset({1}), "flow": frozenset({1121, 1122})},
    )

    result = combine(con, gross, resolved, FakeFactors()).table

    assert values(result, "flow") == {("1121",): 6.0, ("1122",): 4.0}


def test_records_cover_group_members(con: duckdb.DuckDBPyConnection) -> None:
    g = group("duo", {1, 2})

    result = records(con, TOTAL_ODA, with_total(g, shown=set()), FakeFactors()).table

    assert sorted(result.column("provider").to_pylist()) == [1, 1, 2, 2]


def test_tables_for_walks_every_part() -> None:
    assert tables_for(ODA_EXCL_IDRC) == ("dac1",)
    assert tables_for(imputed(Imputation())) == ("crs",)

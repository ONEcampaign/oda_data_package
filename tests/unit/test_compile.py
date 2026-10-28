"""The staged compiler on in-memory DuckDB tables with fake factors.

Each table here stands in for a view: it carries the logical column names
from ``_model.VOCABULARY``.
"""

import subprocess
import sys
from collections.abc import Iterator

import duckdb
import pyarrow as pa
import pytest

from oda_data._compile import (
    BROAD_SECTORS,
    _condition,
    aggregated,
    converted,
    filtered,
)
from oda_data._errors import (
    BasisRangeError,
    DimensionError,
    MissingFactorError,
    UnsupportedQueryError,
)
from oda_data._model import (
    AnyOf,
    Between,
    Binding,
    FactorTable,
    Flow,
    Headline,
    In,
    Indicator,
    IsNull,
    Line,
    NotIn,
    Ratio,
    ResolvedCall,
    ResolvedGroup,
    Sum,
    Value,
)
from oda_data._units import Unit, constant, current

USD = current("USD")
EUR = current("EUR")
USD_2023 = constant("USD", 2023)


# Fixtures ----------------------------------------------------------------------


class FakeFactors:
    """Factor 1 for current USD; other units from ``rates``, keyed by provider
    and unit, the same in every year unless ``(provider, year, unit)`` is
    given."""

    def __init__(
        self,
        rates: dict[tuple, float] | None = None,
        fallback: frozenset[int] = frozenset(),
    ) -> None:
        self.rates = rates or {}
        self.fallback = fallback
        self.calls: list[pa.Table] = []

    def _rate(self, provider: int, year: int, unit: Unit) -> float | None:
        if unit == USD:
            return 1.0
        return self.rates.get((provider, year, unit), self.rates.get((provider, unit)))

    def table(self, keys: pa.Table, units: tuple[Unit, ...], /) -> FactorTable:
        self.calls.append(keys)
        rows = [
            {
                "provider": key["provider"],
                "year": key["year"],
                "currency": unit.currency,
                "prices": unit.prices,
                "base_year": unit.base_year,
                "factor": rate,
            }
            for key in keys.to_pylist()
            for unit in units
            if (rate := self._rate(key["provider"], key["year"], unit)) is not None
        ]
        schema = pa.schema(
            [
                ("provider", pa.int64()),
                ("year", pa.int64()),
                ("currency", pa.string()),
                ("prices", pa.string()),
                ("base_year", pa.int64()),
                ("factor", pa.float64()),
            ]
        )
        return FactorTable(pa.Table.from_pylist(rows, schema=schema), self.fallback)

    def release(self) -> object:
        raise NotImplementedError


DAC1_ROWS = [
    # provider, year, line, flow, amount_type, value
    (1, 2016, 1010, 1140, "A", 10.0),
    (1, 2017, 1010, 1140, "A", 11.0),
    (1, 2018, 1010, 1140, "A", 12.0),
    (1, 2019, 1010, 1140, "A", 13.0),
    (1, 2016, 11010, 1160, "A", 100.0),
    (1, 2017, 11010, 1160, "A", 101.0),
    (1, 2018, 11010, 1160, "A", 102.0),
    (1, 2019, 11010, 1160, "A", 103.0),
    # Decoys: wrong flow for the line, another amount type, a nested line.
    (1, 2018, 1010, 1160, "A", 9000.0),
    (1, 2016, 11010, 1140, "A", 9000.0),
    (1, 2016, 1010, 1140, "D", 9000.0),
    (1, 2018, 11010, 1160, "D", 9000.0),
    (1, 2016, 1015, 1140, "A", 4.0),
    (1, 2019, 1015, 1160, "A", 40.0),
    # A second member and the DAC total.
    (2, 2016, 1010, 1140, "A", 20.0),
    (2, 2019, 11010, 1160, "A", 200.0),
    (20001, 2016, 1010, 1140, "A", 30.0),
    (20001, 2019, 11010, 1160, "A", 303.0),
    # Gross disbursements 1120 and their parts: grants 1121, non-grants 1122.
    (1, 2016, 1010, 1120, "A", 15.0),
    (1, 2016, 1010, 1121, "A", 9.0),
    (1, 2016, 1010, 1122, "A", 6.0),
]

CRS_COLUMNS = (
    "provider",
    "recipient",
    "year",
    "category",
    "bi_multi",
    "sector",
    "disbursement",
    "received",
    "grant_equivalent",
    "gender",
    "climate_mitigation",
    "climate_adaptation",
    "project_title",
)

CRS_ROWS = [
    (1, 248, 2019, 10, 1, 11110, 5.0, 1.0, 4.0, 2, 0, 0, "schools"),
    (1, 248, 2019, 10, 3, 12220, 7.0, None, 6.0, 1, 1, None, "clinic"),
    (1, 248, 2019, 60, 1, 11110, 50.0, None, 3.0, 0, None, None, "psi"),
    (1, 248, 2019, 10, 2, 99810, 70.0, None, 70.0, None, None, None, "core"),
    (1, 248, 2019, 10, 4, 11110, 90.0, None, 90.0, None, None, 2, "outflow"),
    (2, 248, 2019, 10, 7, 12220, 11.0, 2.0, 10.0, None, 0, 1, "hospital"),
    (2, 300, 2019, 10, 8, 31110, 13.0, None, 12.0, 0, 2, 0, "farms"),
    (2, 248, 2017, 10, 1, 31110, 17.0, None, 16.0, 0, 0, 0, "old"),
]

BROAD = {11110: "Education", 12220: "Health", 31110: "Agriculture"}


@pytest.fixture
def con() -> Iterator[duckdb.DuckDBPyConnection]:
    connection = duckdb.connect()
    connection.execute(
        "CREATE TABLE dac1 (provider BIGINT, year BIGINT, line BIGINT, flow BIGINT,"
        " amount_type VARCHAR, value DOUBLE)"
    )
    connection.executemany("INSERT INTO dac1 VALUES (?, ?, ?, ?, ?, ?)", DAC1_ROWS)
    types = dict.fromkeys(CRS_COLUMNS, "BIGINT") | {
        "disbursement": "DOUBLE",
        "received": "DOUBLE",
        "grant_equivalent": "DOUBLE",
        "project_title": "VARCHAR",
    }
    columns = ", ".join(f"{name} {types[name]}" for name in CRS_COLUMNS)
    connection.execute(f"CREATE TABLE crs ({columns})")
    marks = ", ".join("?" * len(CRS_COLUMNS))
    connection.executemany(f"INSERT INTO crs VALUES ({marks})", CRS_ROWS)
    connection.register(
        BROAD_SECTORS,
        pa.table({"sector": list(BROAD), "broad_sector": list(BROAD.values())}),
    )
    yield connection
    connection.close()


DAC1 = Binding(
    table="dac1",
    dimensions={"provider": "provider", "line": "line", "flow": "flow", "year": "year"},
    bases={
        "net_disbursement": Flow(1140),
        "gross_disbursement": Flow(1120, parts=(1121, 1122)),
        "grant_equivalent": Flow(1160, from_year=2018),
        "headline": Headline("net_disbursement", "grant_equivalent"),
    },
    rows=(In("amount_type", ("A",)),),
)

ODA_ROWS = In("category", (10,))

CRS = Binding(
    table="crs",
    dimensions={
        "provider": "provider",
        "recipient": "recipient",
        "sector": "sector",
        "broad_sector": "sector",
        "year": "year",
        "score": "gender",
    },
    bases={
        "gross_disbursement": Value("disbursement", rows=(ODA_ROWS,)),
        "net_disbursement": Value("disbursement", minus="received", rows=(ODA_ROWS,)),
        "grant_equivalent": Value(
            "grant_equivalent", rows=(In("category", (10, 60)),), from_year=2018
        ),
    },
    rows=(In("bi_multi", (1, 3, 7, 8)),),
)


def total_oda(**overrides: object) -> Indicator:
    fields: dict = {
        "id": "oecd.dac1.total_oda",
        "name": "Total ODA",
        "description": "ODA from each provider.",
        "binding": DAC1,
        "kind": Sum(
            lines={
                "net_disbursement": 1010,
                "gross_disbursement": 1010,
                "grant_equivalent": 11010,
            }
        ),
        "dimensions": ("provider", "year"),
        "default_basis": "headline",
        "default_providers": "dac_countries",
    }
    return Indicator(**(fields | overrides))


def dac1_lines() -> Indicator:
    return total_oda(
        id="oecd.dac1.lines", kind=Sum(), dimensions=("provider", "year", "line")
    )


def bilateral(**overrides: object) -> Indicator:
    fields: dict = {
        "id": "oecd.crs.bilateral_oda",
        "name": "Bilateral ODA",
        "description": "Bilateral ODA from each provider.",
        "binding": CRS,
        "kind": Sum(),
        "dimensions": ("provider", "recipient", "sector", "broad_sector", "year"),
        "default_basis": "gross_disbursement",
        "default_providers": None,
    }
    return Indicator(**(fields | overrides))


def call(**overrides: object) -> ResolvedCall:
    fields: dict = {"basis": "headline", "units": (USD,), "by": ("provider", "year")}
    return ResolvedCall(**(fields | overrides))


def run(
    con: duckdb.DuckDBPyConnection,
    indicator: Indicator,
    resolved: ResolvedCall,
    factors: FakeFactors | None = None,
) -> list[dict]:
    rows = filtered(con, indicator, resolved)
    done, _ = converted(con, rows, resolved, factors or FakeFactors())
    return aggregated(done, resolved).to_arrow_table().to_pylist()


def values(result: list[dict], *keys: str) -> dict:
    return {tuple(row[key] for key in keys): row["value"] for row in result}


# Bases, binding rows and unit conversion ---------------------------------------


def test_dac1_headline_switches_from_net_flows_to_grant_equivalents_in_2018(
    con: duckdb.DuckDBPyConnection,
) -> None:
    result = run(con, total_oda(), call(members={"provider": frozenset({1})}))

    # Each row carries the basis of its year, and the indicator's own lines
    # stay out of the result.
    assert values(result, "year", "basis") == {
        (2016, "net_disbursement"): 10.0,
        (2017, "net_disbursement"): 11.0,
        (2018, "grant_equivalent"): 102.0,
        (2019, "grant_equivalent"): 103.0,
    }
    assert "line" not in result[0]


def test_dac1_sums_current_usd_rows_only(con: duckdb.DuckDBPyConnection) -> None:
    result = run(
        con,
        total_oda(),
        call(basis="net_disbursement", years=(2016,), members={"provider": {1}}),
    )

    assert values(result, "year") == {(2016,): 10.0}


@pytest.mark.parametrize(
    ("years", "covered"),
    [((2017, 2018), "2017"), ((2015, 2016), "2015-2016")],
)
def test_grant_equivalent_before_2018_raises_basis_range_error(
    con: duckdb.DuckDBPyConnection, years: tuple[int, ...], covered: str
) -> None:
    with pytest.raises(BasisRangeError) as raised:
        filtered(con, total_oda(), call(basis="grant_equivalent", years=years))

    assert raised.value.code == "basis_range"
    assert covered in raised.value.message


def test_grant_equivalent_with_no_years_reads_its_years_only(
    con: duckdb.DuckDBPyConnection,
) -> None:
    result = run(
        con, total_oda(), call(basis="grant_equivalent", members={"provider": {1}})
    )

    assert values(result, "year") == {(2018,): 102.0, (2019,): 103.0}


def test_headline_years_before_2018_need_no_grant_equivalents(
    con: duckdb.DuckDBPyConnection,
) -> None:
    resolved = call(
        basis="headline",
        years=(2016,),
        members={"provider": {1}},
        by=("year",),
    )
    indicator = total_oda(
        binding=Binding(
            table="dac1",
            dimensions=DAC1.dimensions,
            bases={
                "net_disbursement": Flow(1140),
                "gross_disbursement": Flow(1120),
                "grant_equivalent": Flow(1160, from_year=2019),
                "headline": Headline("net_disbursement", "grant_equivalent"),
            },
            rows=DAC1.rows,
        )
    )

    assert values(run(con, indicator, resolved), "year") == {(2016,): 10.0}


@pytest.mark.parametrize(
    ("basis", "expected"),
    [
        # Category 10, bi_multi 1, 3, 7 and 8 only.
        ("gross_disbursement", 5.0 + 7.0 + 11.0 + 13.0),
        # The same rows, received subtracted with null read as 0.
        ("net_disbursement", (5.0 - 1.0) + 7.0 + (11.0 - 2.0) + 13.0),
        # Category 60 joins category 10 on grant equivalents.
        ("grant_equivalent", 4.0 + 6.0 + 3.0 + 10.0 + 12.0),
    ],
)
def test_crs_category_and_bi_multi_come_from_binding_rows(
    con: duckdb.DuckDBPyConnection, basis: str, expected: float
) -> None:
    result = run(con, bilateral(), call(basis=basis, years=(2019,), by=("year",)))

    assert values(result, "year") == {(2019,): expected}


def test_factor_joins_each_row_before_any_sum(con: duckdb.DuckDBPyConnection) -> None:
    resolved = call(
        basis="gross_disbursement",
        years=(2019,),
        units=(USD_2023,),
        by=("year",),
    )
    factors = FakeFactors({(1, USD_2023): 2.0, (2, USD_2023): 10.0})

    rows = filtered(con, bilateral(), resolved)
    done, _ = converted(con, rows, resolved, factors)

    assert done.shape[0] == rows.shape[0]
    result = aggregated(done, resolved).to_arrow_table().to_pylist()
    assert values(result, "year") == {(2019,): (5.0 + 7.0) * 2.0 + (11.0 + 13.0) * 10.0}


def test_basis_unit_and_line_always_split_the_result(
    con: duckdb.DuckDBPyConnection,
) -> None:
    resolved = call(
        basis="net_disbursement",
        years=(2016,),
        units=(USD, USD_2023),
        by=("year",),
        members={"provider": {1}, "line": {1010, 1015}},
    )
    factors = FakeFactors({(1, USD_2023): 3.0})

    result = run(con, dac1_lines(), resolved, factors)

    assert values(result, "line", "basis", "prices", "base_year") == {
        (1010, "net_disbursement", "current", None): 10.0,
        (1015, "net_disbursement", "current", None): 4.0,
        (1010, "net_disbursement", "constant", 2023): 30.0,
        (1015, "net_disbursement", "constant", 2023): 12.0,
    }


# Providers ---------------------------------------------------------------------


def test_unnamed_providers_exclude_aggregates(con: duckdb.DuckDBPyConnection) -> None:
    result = run(con, total_oda(), call(years=(2016,)))

    assert values(result, "provider") == {(1,): 10.0, (2,): 20.0}


def test_named_aggregate_returns_its_own_row(con: duckdb.DuckDBPyConnection) -> None:
    result = run(con, total_oda(), call(years=(2016,), members={"provider": {20001}}))

    assert values(result, "provider") == {(20001,): 30.0}


def test_excluded_providers_are_dropped(con: duckdb.DuckDBPyConnection) -> None:
    indicator = total_oda(rows=(NotIn("provider", (2,), reason="counted twice"),))

    assert values(run(con, indicator, call(years=(2016,))), "provider") == {(1,): 10.0}


# Dimensions and filters ----------------------------------------------------------


def test_broad_sector_groups_and_filters_through_the_mapping(
    con: duckdb.DuckDBPyConnection,
) -> None:
    grouped = run(
        con,
        bilateral(),
        call(basis="gross_disbursement", years=(2019,), by=("broad_sector",)),
    )
    picked = run(
        con,
        bilateral(),
        call(
            basis="gross_disbursement",
            years=(2019,),
            by=("year",),
            members={"broad_sector": {"Health"}},
        ),
    )

    assert values(grouped, "broad_sector") == {
        ("Education",): 5.0,
        ("Health",): 7.0 + 11.0,
        ("Agriculture",): 13.0,
    }
    assert values(picked, "year") == {(2019,): 18.0}


def test_call_members_filter_rows(con: duckdb.DuckDBPyConnection) -> None:
    result = run(
        con,
        bilateral(),
        call(
            basis="gross_disbursement",
            years=(2019,),
            by=("recipient",),
            members={"recipient": {300}, "sector": {31110, 12220}},
        ),
    )

    assert values(result, "recipient") == {(300,): 13.0}


@pytest.mark.parametrize(
    ("rows", "expected"),
    [
        ((IsNull("gender"),), 11.0),
        ((NotIn("gender", (0,)),), 5.0 + 7.0),
        ((Between("gender", 1, 2),), 5.0 + 7.0),
        (
            (AnyOf((In("climate_mitigation", (2,)), In("climate_adaptation", (1,)))),),
            24.0,
        ),
        ((AnyOf(()),), None),
    ],
    ids=["is-null", "not-in", "between", "any-of", "empty-any-of"],
)
def test_indicator_rows_filter(
    con: duckdb.DuckDBPyConnection, rows: tuple, expected: float | None
) -> None:
    result = run(
        con,
        bilateral(rows=rows),
        call(basis="gross_disbursement", years=(2019,), by=("year",)),
    )

    assert values(result, "year").get((2019,)) == expected


def flows_call(flows: set[int], basis: str = "gross_disbursement") -> ResolvedCall:
    return call(
        basis=basis,
        years=(2016,),
        by=("year",),
        members={"provider": {1}, "flow": frozenset(flows)},
    )


def test_a_flow_filter_reads_the_basis_parts_each_as_its_own_row(
    con: duckdb.DuckDBPyConnection,
) -> None:
    indicator = total_oda(dimensions=("provider", "year", "flow"))

    result = run(con, indicator, flows_call({1121, 1122}))

    assert values(result, "flow") == {(1121,): 9.0, (1122,): 6.0}


@pytest.mark.parametrize(
    ("flows", "basis"),
    [({1121, 1140}, "gross_disbursement"), ({1121}, "net_disbursement")],
    ids=["not-a-part", "basis-without-parts"],
)
def test_a_flow_outside_the_basis_parts_raises(
    con: duckdb.DuckDBPyConnection, flows: set[int], basis: str
) -> None:
    indicator = total_oda(dimensions=("provider", "year", "flow"))

    with pytest.raises(UnsupportedQueryError, match="1121, 1122"):
        filtered(con, indicator, flows_call(flows, basis))


def test_dimension_outside_the_indicator_raises(
    con: duckdb.DuckDBPyConnection,
) -> None:
    with pytest.raises(DimensionError):
        filtered(con, total_oda(), call(by=("recipient",)))


def test_basis_outside_the_binding_raises(con: duckdb.DuckDBPyConnection) -> None:
    with pytest.raises(UnsupportedQueryError):
        filtered(con, total_oda(), call(basis="commitment"))


def test_compiler_refuses_kinds_other_than_sum(
    con: duckdb.DuckDBPyConnection,
) -> None:
    ratio = total_oda(kind=Ratio(total_oda(), total_oda()))

    with pytest.raises(UnsupportedQueryError):
        filtered(con, ratio, call())


# Stage outputs -------------------------------------------------------------------


def test_filtered_keeps_provider_and_group_dimensions(
    con: duckdb.DuckDBPyConnection,
) -> None:
    group = ResolvedGroup("East Africa", "recipient", {2019: frozenset({248})})
    rows = filtered(
        con,
        bilateral(),
        call(basis="gross_disbursement", by=("year",), groups=(group,)),
    )

    assert rows.columns == ["provider", "recipient", "year", "basis", "value"]


def test_records_columns_pass_through_conversion(
    con: duckdb.DuckDBPyConnection,
) -> None:
    resolved = call(basis="gross_disbursement", years=(2019,), by=("provider",))

    rows = filtered(con, bilateral(), resolved, columns=("project_title", "sector"))
    done, _ = converted(con, rows, resolved, FakeFactors())

    assert done.columns == [
        "provider",
        "year",
        "basis",
        "project_title",
        "sector",
        "currency",
        "prices",
        "base_year",
        "value",
    ]
    titles = sorted(row[3] for row in done.fetchall())
    assert titles == ["clinic", "farms", "hospital", "schools"]


def test_missing_factor_raises(con: duckdb.DuckDBPyConnection) -> None:
    resolved = call(years=(2016,), units=(EUR,))
    factors = FakeFactors({(1, EUR): 0.9})

    with pytest.raises(MissingFactorError) as raised:
        run(con, total_oda(), resolved, factors)

    assert raised.value.code == "missing_factor"


def test_fallback_providers_are_reported(con: duckdb.DuckDBPyConnection) -> None:
    resolved = call(years=(2016,))
    factors = FakeFactors(fallback=frozenset({2}))

    _, fallback = converted(
        con, filtered(con, total_oda(), resolved), resolved, factors
    )

    assert fallback == {2}


def test_empty_rows_skip_the_factor_source(con: duckdb.DuckDBPyConnection) -> None:
    resolved = call(years=(1990,), units=(USD_2023,))
    factors = FakeFactors()

    done, fallback = converted(
        con, filtered(con, total_oda(), resolved), resolved, factors
    )

    assert done.fetchall() == []
    assert fallback == frozenset()
    assert factors.calls == []


def test_dac2a_line_basis(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        "CREATE TABLE dac2a (provider BIGINT, recipient BIGINT, year BIGINT,"
        " line BIGINT, part BIGINT, amount_type VARCHAR, value DOUBLE)"
    )
    con.executemany(
        "INSERT INTO dac2a VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            (1, 248, 2019, 206, 1, "A", 8.0),
            (1, 248, 2019, 206, 1, "D", 800.0),
            (1, 248, 2019, 240, 1, "A", 9.0),
        ],
    )
    binding = Binding(
        table="dac2a",
        dimensions={"provider": "provider", "recipient": "recipient", "year": "year"},
        bases={"net_disbursement": Line(206), "headline": Line(206)},
        rows=(In("amount_type", ("A",)),),
    )
    indicator = bilateral(
        id="oecd.dac2a.recipient_oda",
        binding=binding,
        dimensions=("provider", "recipient", "year"),
        default_basis="headline",
    )

    result = run(con, indicator, call(by=("recipient", "year")))

    assert values(result, "recipient", "year", "basis") == {
        (248, 2019, "headline"): 8.0
    }


def test_compile_imports_no_registry_module() -> None:
    # Labels live on the vocabulary, so the engine never reads the registry.
    code = (
        "import sys, oda_data._compile; "
        "loaded = [m for m in sys.modules if m.startswith('oda_data._registry')]; "
        "assert not loaded, loaded"
    )
    subprocess.run([sys.executable, "-c", code], check=True)


def test_an_unknown_row_filter_raises_type_error() -> None:
    with pytest.raises(TypeError, match="unknown row filter"):
        _condition(object())  # type: ignore[arg-type]  # outside the Filter union

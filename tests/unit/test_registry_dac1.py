"""The DAC1 indicators in the registry, run on a small in-memory DAC1 table.

Each case pins one DAC1 counting rule with a
table built to break it: rows in other amount types, decoy lines under other
flows, providers outside ``dac_countries``, and an aggregate. ``FakeFactors``
gives factor 1 for current USD and a different factor per provider for any
other unit, so a sum that deflates after adding shows.
"""

import importlib.util
import warnings
from collections.abc import Iterator
from dataclasses import replace
from datetime import datetime
from pathlib import Path

import duckdb
import pyarrow as pa
import pytest

from oda_data import _registry
from oda_data._context import Context
from oda_data._discover import describe
from oda_data._errors import (
    BasisRangeError,
    DimensionError,
    OdaDataWarning,
    UnsupportedQueryError,
)
from oda_data._factors import FACTOR_SCHEMA
from oda_data._groups import groups, total
from oda_data._model import Derived, FactorTable, Indicator, Ratio, Sum
from oda_data._provenance import Release
from oda_data._registry import dac1
from oda_data._units import Unit, constant, current

USD = current("USD")
RELEASE = Release("dac1-token", datetime(2026, 10, 12))
FRANCE, GERMANY, TURKIYE, EU_INSTITUTIONS, DAC_TOTAL = 4, 5, 55, 918, 20001

ROWS = [
    # provider, year, line, flow, amount type, value
    (FRANCE, 2017, 1010, 1140, "A", 10.0),
    (FRANCE, 2017, 1010, 1140, "N", 900.0),  # national currency
    (FRANCE, 2017, 1010, 1140, "D", 990.0),  # constant prices
    (FRANCE, 2017, 1010, 1120, "A", 12.0),
    (FRANCE, 2017, 1010, 1121, "A", 7.0),  # grants, part of 1120
    (FRANCE, 2017, 1010, 1150, "A", 15.0),
    (FRANCE, 2017, 1015, 1140, "A", 6.0),
    (FRANCE, 2017, 11010, 1160, "A", 9.0),  # reported before 2018
    (GERMANY, 2017, 1010, 1140, "A", 20.0),
    (TURKIYE, 2017, 1010, 1140, "A", 3.0),  # reports, outside the DAC
    (EU_INSTITUTIONS, 2017, 1010, 1140, "A", 5.0),
    (DAC_TOTAL, 2017, 1010, 1140, "A", 30.0),
    (FRANCE, 2018, 1010, 1140, "A", 13.0),
    (FRANCE, 2018, 1010, 1160, "A", 77.0),  # line 1010 under grant equivalents
    (FRANCE, 2018, 11010, 1160, "A", 11.0),
    (GERMANY, 2018, 11010, 1160, "A", 22.0),
    (DAC_TOTAL, 2018, 11010, 1160, "A", 33.0),
    (FRANCE, 2017, 1, 1140, "A", 1000.0),
    (FRANCE, 2017, 1, 1120, "A", 999_999.0),  # GNI repeated under another flow
    (GERMANY, 2017, 1, 1140, "A", 4000.0),
    (FRANCE, 2018, 1, 1140, "A", 1100.0),
    (GERMANY, 2018, 1, 1140, "A", 4400.0),
    # Lines that are not money: ODA as a percentage of GNI, and population.
    (FRANCE, 2017, 2, 1140, "A", 0.5),
    (GERMANY, 2017, 2, 1140, "A", 0.6),
    (FRANCE, 2018, 11002, 1160, "A", 0.55),
    (FRANCE, 2017, 4, 1140, "A", 67.0),
    (GERMANY, 2017, 4, 1140, "A", 83.0),
]

#: The factor each provider takes for any unit other than current USD.
PROVIDER_FACTORS = {FRANCE: 2.0, GERMANY: 3.0}


class FakeTables:
    """A ``dac1`` table with the view's logical columns."""

    def __init__(self) -> None:
        self.connection = duckdb.connect()
        self.connection.execute(
            "CREATE TABLE dac1 (provider BIGINT, year BIGINT, line BIGINT,"
            " flow BIGINT, amount_type VARCHAR, value DOUBLE)"
        )
        self.connection.executemany("INSERT INTO dac1 VALUES (?, ?, ?, ?, ?, ?)", ROWS)

    def cursor(self, tables: tuple[str, ...]) -> duckdb.DuckDBPyConnection:
        return self.connection.cursor()

    def path(self, table: str) -> Path:
        raise NotImplementedError

    def release(self, table: str) -> Release:
        return RELEASE

    def close(self) -> None:
        self.connection.close()


class FakeFactors:
    def table(self, keys: pa.Table, units: tuple[Unit, ...], /) -> FactorTable:
        rows = [
            key
            | {
                "currency": unit.currency,
                "prices": unit.prices,
                "base_year": unit.base_year,
                "factor": 1.0 if unit == USD else PROVIDER_FACTORS[key["provider"]],
            }
            for key in keys.to_pylist()
            for unit in units
        ]
        return FactorTable(
            pa.Table.from_pylist(rows, schema=FACTOR_SCHEMA), frozenset()
        )

    def release(self) -> Release:
        return RELEASE


@pytest.fixture
def ctx() -> Iterator[Context]:
    with Context._from_parts(FakeTables(), FakeFactors()) as context:
        yield context


def _rows(frame, *columns: str) -> list[tuple]:
    return [tuple(row) for row in frame[list(columns)].itertuples(index=False)]


# Registry ------------------------------------------------------------------------


def test_registry_maps_every_dac1_indicator_by_id() -> None:
    assert [i.id for i in dac1.INDICATORS] == [
        i for i in _registry.INDICATORS if i.startswith("oecd.dac1.")
    ]
    assert all(_registry.INDICATORS[i.id] is i for i in dac1.INDICATORS)


def test_two_indicators_with_one_id_refuse_to_load() -> None:
    with pytest.raises(ValueError):
        _registry._by_id((dac1.TOTAL_ODA,), (dac1.TOTAL_ODA,))


def test_ids_module_matches_the_registry() -> None:
    script = Path(__file__).parents[2] / "scripts" / "generate_ids.py"
    spec = importlib.util.spec_from_file_location("generate_ids", script)
    assert spec is not None and spec.loader is not None
    generator = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(generator)

    expected = generator.render(_registry.INDICATORS)

    assert generator.TARGET.read_text() == expected, (
        "src/oda_data/_ids.py is stale; run scripts/generate_ids.py"
    )


@pytest.mark.parametrize(
    "indicator",
    [i for i in dac1.INDICATORS if isinstance(i.kind, Sum) and i.kind.lines],
    ids=lambda i: i.id,
)
def test_named_line_indicators_have_a_line_on_every_basis_they_offer(
    indicator: Indicator,
) -> None:
    """GNI and population offer net flows only; every other named line
    indicator offers all four concrete bases, so the headline basis always
    resolves."""
    assert isinstance(indicator.kind, Sum)
    concrete = (
        {"net_disbursement"}
        if indicator in (dac1.GNI, dac1.POPULATION)
        else {
            "net_disbursement",
            "gross_disbursement",
            "commitment",
            "grant_equivalent",
        }
    )
    assert set(indicator.kind.lines) == concrete


# DAC1 sums current USD rows only --------------------------------------------------


def test_sums_current_usd_rows_only(ctx: Context) -> None:
    df = ctx.get(
        "oecd.dac1.total_oda", years=2017, providers=FRANCE, basis="net_disbursement"
    )

    assert _rows(df, "provider", "value") == [("4", 10.0)]


# Headline is net flows (1010) to 2017 and grant equivalents (11010) from 2018 -----


def test_headline_reads_net_flows_to_2017_and_grant_equivalents_from_2018(
    ctx: Context,
) -> None:
    df = ctx.get("oecd.dac1.total_oda", providers=FRANCE)

    assert _rows(df, "year", "basis", "value") == [
        (2017, "net_disbursement", 10.0),
        (2018, "grant_equivalent", 11.0),  # not 13 (net) nor 77 (line 1010)
    ]
    assert "line" not in df.columns
    assert "flow" not in df.columns


@pytest.mark.parametrize(
    ("basis", "year", "value"),
    [
        ("net_disbursement", 2017, 10.0),
        ("gross_disbursement", 2017, 12.0),
        ("commitment", 2017, 15.0),
        ("grant_equivalent", 2018, 11.0),
    ],
)
def test_each_basis_reads_its_own_flow(
    ctx: Context, basis: str, year: int, value: float
) -> None:
    df = ctx.get("oecd.dac1.total_oda", years=year, providers=FRANCE, basis=basis)

    assert _rows(df, "basis", "value") == [(basis, value)]


# DAC1 offers its flows' parts through flows= --------------------------------------


@pytest.mark.parametrize(
    ("flows", "by", "expected"),
    [
        (1121, ["provider"], [("1121", 7.0)]),  # a filter splits even outside by
        (None, ["provider", "flow"], [("1120", 12.0)]),
    ],
)
def test_flows_read_the_parts_of_gross_disbursements(
    ctx: Context, flows: int | None, by: list[str], expected: list[tuple]
) -> None:
    df = ctx.get(
        "oecd.dac1.total_oda",
        years=2017,
        providers=FRANCE,
        flows=flows,
        by=by,
        basis="gross_disbursement",
    )

    assert _rows(df, "flow", "value") == expected


def test_a_flow_the_basis_does_not_split_into_raises(ctx: Context) -> None:
    with pytest.raises(UnsupportedQueryError, match="gross_disbursement"):
        ctx.get(
            "oecd.dac1.total_oda",
            years=2017,
            providers=FRANCE,
            flows=1121,
            basis="net_disbursement",
        )


# Grant equivalents start in 2018; 2015-2017 only through lines, with a caveat -----


def test_grant_equivalents_before_2018_raise_and_name_a_covering_basis(
    ctx: Context,
) -> None:
    with pytest.raises(BasisRangeError, match="net_disbursement"):
        ctx.get(
            "oecd.dac1.total_oda",
            years=[2017, 2018],
            providers=FRANCE,
            basis="grant_equivalent",
        )


def test_lines_reach_grant_equivalents_from_2015_with_a_caveat(ctx: Context) -> None:
    with pytest.warns(OdaDataWarning, match="cover only the 25 to 27"):
        df = ctx.get(
            "oecd.dac1.lines",
            years=2017,
            providers=FRANCE,
            lines=11010,
            basis="grant_equivalent",
        )
    assert _rows(df, "line", "value") == [("11010", 9.0)]

    with pytest.raises(BasisRangeError), warnings.catch_warnings():
        warnings.simplefilter("ignore", OdaDataWarning)
        ctx.get("oecd.dac1.lines", years=2014, lines=11010, basis="grant_equivalent")


def test_the_grant_equivalent_caveat_states_its_years_once() -> None:
    # describe() prints the caveat's scope after its text.
    [caveat] = describe("oecd.dac1.lines").caveats

    assert caveat.count("2015") == 1


@pytest.mark.parametrize(
    ("years", "basis", "warns"),
    [
        (2017, "grant_equivalent", True),
        (None, "grant_equivalent", True),
        (2018, "grant_equivalent", False),
        ([2017, 2018], "headline", False),
        (2017, "net_disbursement", False),
    ],
)
def test_the_grant_equivalent_caveat_warns_only_on_2015_to_2017_grant_equivalents(
    ctx: Context, years, basis: str, warns: bool
) -> None:
    # pytest turns any other warning into an error.
    if warns:
        with pytest.warns(OdaDataWarning, match="cover only the 25 to 27"):
            ctx.get("oecd.dac1.lines", years=years, providers=FRANCE, basis=basis)
    else:
        ctx.get("oecd.dac1.lines", years=years, providers=FRANCE, basis=basis)


# GNI is line 1 under net flows for every basis -------------------------------------


@pytest.mark.parametrize(
    ("basis", "year", "resolved", "expected"),
    [
        ("headline", 2017, "net_disbursement", 100 * 10 / 1000),
        ("headline", 2018, "grant_equivalent", 100 * 11 / 1100),
        # GNI is line 1 under 1140, not 1120.
        ("gross_disbursement", 2017, "gross_disbursement", 100 * 12 / 1000),
        ("grant_equivalent", 2018, "grant_equivalent", 100 * 11 / 1100),
    ],
)
def test_oda_gni_divides_by_gni_under_net_flows(
    ctx: Context, basis: str, year: int, resolved: str, expected: float
) -> None:
    df = ctx.get("oecd.dac1.oda_gni_ratio", years=year, providers=FRANCE, basis=basis)

    assert _rows(df, "basis", "scale", "value") == [(resolved, "percent", expected)]


def test_gni_has_no_grant_equivalent_basis(ctx: Context) -> None:
    with pytest.raises(UnsupportedQueryError, match="grant_equivalent"):
        ctx.get("oecd.dac1.gni", years=2018, basis="grant_equivalent")


def test_oda_gni_of_a_group_is_total_oda_over_total_gni(ctx: Context) -> None:
    dac = groups.dac_countries
    df = ctx.get(
        "oecd.dac1.oda_gni_ratio",
        years=2018,
        providers=total(dac),
    )

    assert _rows(df, "provider", "value") == [
        ("dac_countries", 100 * (11 + 22) / (1100 + 4400))
    ]


@pytest.mark.parametrize("indicator", ["oecd.dac1.oda_gni_ratio", "oecd.dac1.gni"])
@pytest.mark.parametrize(
    "split",
    [{"by": ["provider", "year", "flow"]}, {"flows": 1121}],
    ids=["by", "flows"],
)
def test_gni_and_the_ratio_have_no_flow_to_split_by(
    ctx: Context, indicator: str, split: dict
) -> None:
    # GNI is flow 1140 on every basis, a flow ODA on grant equivalents or
    # gross disbursements never shares.
    with pytest.raises(DimensionError, match="'flow'"):
        ctx.get(indicator, years=2017, providers=FRANCE, **split)


# Percentages and population are not money, so lines leaves them out ---------------


def test_lines_leave_out_lines_that_are_not_money(ctx: Context) -> None:
    df = ctx.get("oecd.dac1.lines", years=2017, by=["line"])

    assert set(df["line"]) == {"1", "1010", "1015"}


@pytest.mark.parametrize(
    "line", [2, 3, 4, 11002, "ODA as percent of GNI, grant equivalent"]
)
def test_naming_a_line_that_is_not_money_raises(ctx: Context, line) -> None:
    with pytest.raises(UnsupportedQueryError) as raised:
        ctx.get("oecd.dac1.lines", years=2017, providers=FRANCE, lines=line)

    assert "oecd.dac1.oda_gni_ratio" in raised.value.message


# Lines read net flows by default; a line its flow lacks raises --------------------


def test_lines_default_to_net_flows_so_gni_covers_every_year(ctx: Context) -> None:
    q = ctx.query("oecd.dac1.lines", providers=FRANCE, lines=1)
    df = q.to_pandas()

    assert _rows(df, "year", "basis", "value") == [
        (2017, "net_disbursement", 1000.0),
        (2018, "net_disbursement", 1100.0),
    ]
    assert q.provenance.defaults["basis"] == "net_disbursement"


@pytest.mark.parametrize(
    ("line", "basis", "years", "holders"),
    [
        (11010, "net_disbursement", None, ["grant_equivalent"]),
        # Line 1 is published under net flows (and a decoy gross flow) only.
        (1, "headline", [2017, 2018], ["net_disbursement", "gross_disbursement"]),
        (1, "grant_equivalent", 2018, ["net_disbursement", "gross_disbursement"]),
    ],
)
def test_a_line_the_bases_flow_never_publishes_raises_naming_the_bases_that_do(
    ctx: Context, line: int, basis: str, years, holders: list[str]
) -> None:
    with pytest.raises(UnsupportedQueryError) as raised, warnings.catch_warnings():
        warnings.simplefilter("ignore", OdaDataWarning)
        ctx.get("oecd.dac1.lines", years=years, lines=line, basis=basis)

    named = raised.value.message.rsplit("; the ", 1)[1]
    bases = ("net_disbursement", "gross_disbursement", "commitment", "grant_equivalent")
    assert [basis for basis in bases if repr(basis) in named] == holders


def test_a_headline_span_the_call_does_not_cover_is_not_checked(
    ctx: Context,
) -> None:
    df = ctx.get(
        "oecd.dac1.lines", years=2017, providers=FRANCE, lines=1, basis="headline"
    )

    assert df["value"].tolist() == [1000.0]


# Lines are never summed with each other -------------------------------------------


def test_lines_split_the_result_even_outside_by(ctx: Context) -> None:
    df = ctx.get(
        "oecd.dac1.lines",
        years=2017,
        providers=FRANCE,
        lines=[1010, 1015],
        by=["provider"],
        basis="net_disbursement",
    )

    assert _rows(df, "line", "value") == [("1010", 10.0), ("1015", 6.0)]


# Providers default to DAC countries; 918, non-DAC and aggregates stay out ---------


def test_providers_default_to_dac_countries(ctx: Context) -> None:
    q = ctx.query("oecd.dac1.total_oda", years=2017, by=["provider"])

    assert _rows(q.to_pandas(), "provider", "value") == [("4", 10.0), ("5", 20.0)]
    assert q.provenance.defaults["providers"] == "dac_countries"


def test_eu_institutions_and_non_dac_reporters_answer_when_named(
    ctx: Context,
) -> None:
    df = ctx.get(
        "oecd.dac1.total_oda",
        years=2017,
        providers=[EU_INSTITUTIONS, TURKIYE],
        by=["provider"],
    )

    assert _rows(df, "provider", "is_aggregate", "value") == [
        ("55", False, 3.0),
        ("918", False, 5.0),
    ]


@pytest.mark.parametrize("indicator", ["oecd.dac1.total_oda", "oecd.dac1.lines"])
def test_eu_institutions_summed_with_a_named_eu_member_raise(
    ctx: Context, indicator: str
) -> None:
    # France's multilateral ODA on DAC1 holds its contribution to the EU.
    with pytest.raises(UnsupportedQueryError, match="918"):
        ctx.get(indicator, years=2017, providers=[EU_INSTITUTIONS, FRANCE], by=["year"])


# An aggregate code returns its own row and is never summed with members -----------


def test_dac_total_code_returns_its_own_row(ctx: Context) -> None:
    df = ctx.get("oecd.dac1.total_oda", providers=DAC_TOTAL, by=["provider", "year"])

    assert _rows(df, "provider", "year", "is_aggregate", "value") == [
        ("20001", 2017, True, 30.0),
        ("20001", 2018, True, 33.0),
    ]


# Members that have not yet reported count as zero in a group total ---------------


def test_dac_total_counts_members_not_yet_reporting_as_zero(ctx: Context) -> None:
    # Only France and Germany report to this table; the other members have
    # never reported, so they add nothing and leave the total whole.
    dac = groups.dac_countries
    df = ctx.get("oecd.dac1.total_oda", providers=total(dac), by=["provider", "year"])

    assert _rows(df, "year", "value") == [(2017, 30.0), (2018, 33.0)]


# Constant prices use each provider's deflator before any sum ---------------------


def test_constant_prices_deflate_each_provider_before_the_sum(ctx: Context) -> None:
    df = ctx.get(
        "oecd.dac1.total_oda",
        years=2017,
        providers=total(groups.dac_countries),
        unit=constant("USD", 2023),
    )

    assert df["value"].tolist() == [10.0 * 2.0 + 20.0 * 3.0]


def test_the_ratio_refuses_a_unit(ctx: Context) -> None:
    with pytest.raises(UnsupportedQueryError):
        ctx.get("oecd.dac1.oda_gni_ratio", years=2017, unit=constant("USD", 2023))


# Population is a count of people, never converted or combined with money ---------


class RefusingFactors(FakeFactors):
    def table(self, keys: pa.Table, units: tuple[Unit, ...], /) -> FactorTable:
        raise AssertionError("a count asked for conversion factors")


def test_population_sums_a_group_without_asking_for_factors() -> None:
    with Context._from_parts(FakeTables(), RefusingFactors()) as context:
        df = context.get(
            "oecd.dac1.population",
            years=2017,
            providers=[FRANCE, GERMANY, total(groups.dac_countries)],
        )

    assert _rows(df, "provider", "basis", "scale", "value") == [
        (str(FRANCE), "net_disbursement", "millions", 67.0),
        (str(GERMANY), "net_disbursement", "millions", 83.0),
        ("dac_countries", "net_disbursement", "millions", 150.0),
    ]
    assert df[["currency", "prices", "base_year"]].isna().all().all()


@pytest.mark.parametrize("unit", [USD, constant("USD", 2023)], ids=str)
def test_population_refuses_a_unit(ctx: Context, unit: Unit) -> None:
    with pytest.raises(UnsupportedQueryError, match="count of people"):
        ctx.get("oecd.dac1.population", years=2017, unit=unit)


def test_population_records_carry_no_unit() -> None:
    with Context._from_parts(FakeTables(), RefusingFactors()) as context:
        df = context.records("oecd.dac1.population", years=2017, providers=FRANCE)

    assert df["value"].tolist() == [67.0]
    assert df["currency"].isna().all()


def test_describe_says_population_is_a_count() -> None:
    assert "millions of people, a count that takes no unit=" in repr(
        describe("oecd.dac1.population")
    )


@pytest.mark.parametrize(
    "kind",
    [
        Derived(plus=(dac1.TOTAL_ODA, dac1.POPULATION)),
        Ratio(numerator=dac1.TOTAL_ODA, denominator=dac1.POPULATION),
    ],
    ids=["derived", "ratio"],
)
def test_population_cannot_be_combined_with_money(kind: Derived | Ratio) -> None:
    with pytest.raises(ValueError, match=r"oecd\.dac1\.population"):
        replace(dac1.TOTAL_ODA, id="test.mixed", kind=kind)


def test_every_dac1_indicator_on_a_line_that_is_not_money_is_not_money() -> None:
    for indicator in dac1.INDICATORS:
        if isinstance(indicator.kind, Sum) and indicator.kind.lines:
            reads = set(indicator.kind.lines.values()) & set(dac1.NON_MONEY_LINES)
            assert bool(reads) == (not indicator.is_money), indicator.id

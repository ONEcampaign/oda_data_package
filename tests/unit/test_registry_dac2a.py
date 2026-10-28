"""The DAC2A indicators in the registry, run on a small in-memory DAC2A table.

Each case pins one DAC2A counting rule with a table built to break it:
constant-price rows beside current ones, decoy lines, a private donor, a
multilateral provider, a provider aggregate, a recipient total, a
multilateral organisation as recipient, a Part II row, and a last year that
one DAC member has not reported. ``FakeFactors`` gives factor 1 for current USD and a different factor per
provider for any other unit, so a sum that deflates after adding shows.
"""

import re
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path

import duckdb
import pyarrow as pa
import pytest

from oda_data import _registry
from oda_data._context import Context
from oda_data._errors import DimensionError, OdaDataWarning, UnsupportedQueryError
from oda_data._factors import FACTOR_SCHEMA
from oda_data._groups import groups, total
from oda_data._model import FactorTable, basis_lines
from oda_data._provenance import Release
from oda_data._registry import dac2a
from oda_data._units import Unit, constant, current

USD = current("USD")
RELEASE = Release("dac2a-token", datetime(2026, 10, 12))
FRANCE, GERMANY, TURKIYE, EU_INSTITUTIONS, IDA = 4, 5, 55, 918, 905
GATES, DAC_TOTAL = 1601, 20001
KENYA, RUSSIA, SOUTH_OF_SAHARA, UNSPECIFIED, DEVELOPING_TOTAL = (
    248,
    87,
    289,
    9998,
    10100,
)

ROWS = [
    # provider, recipient, year, line, part, amount type, value
    (FRANCE, KENYA, 2023, 206, 1, "A", 10.0),
    (FRANCE, KENYA, 2023, 206, 1, "D", 990.0),  # constant prices
    (FRANCE, KENYA, 2023, 240, 1, "A", 12.0),
    (FRANCE, KENYA, 2023, 207, 1, "A", 3.0),
    (FRANCE, KENYA, 2023, 106, 1, "A", 4.0),
    (FRANCE, KENYA, 2023, 286, 1, "A", 0.5),  # ODA as % of recipient GNI
    (FRANCE, KENYA, 2023, 296, 1, "A", 1.2),  # ODA per capita
    (FRANCE, SOUTH_OF_SAHARA, 2023, 206, 1, "A", 2.0),  # regional
    (FRANCE, UNSPECIFIED, 2023, 206, 1, "A", 1.0),  # unspecified
    (FRANCE, DEVELOPING_TOTAL, 2023, 206, 1, "A", 13.0),  # a recipient total
    (FRANCE, IDA, 2023, 206, 1, "A", 6.0),  # a core contribution to IDA
    (GERMANY, KENYA, 2023, 206, 1, "A", 20.0),
    (GERMANY, KENYA, 2023, 106, 1, "A", 6.0),
    (TURKIYE, KENYA, 2023, 206, 1, "A", 5.0),  # non-DAC, official
    (IDA, KENYA, 2023, 206, 1, "A", 40.0),  # multilateral, official
    (EU_INSTITUTIONS, KENYA, 2023, 106, 1, "A", 7.0),  # 918's own imputed shares
    (GATES, KENYA, 2023, 206, 1, "A", 9.0),  # private donor
    (DAC_TOTAL, KENYA, 2023, 206, 1, "A", 30.0),  # provider aggregate
    (DAC_TOTAL, KENYA, 2023, 106, 1, "A", 10.0),
    (FRANCE, RUSSIA, 2000, 206, 2, "A", 8.0),  # Part II: official aid
    (FRANCE, KENYA, 2000, 206, 1, "A", 11.0),
    (FRANCE, UNSPECIFIED, 2024, 206, 1, "A", 1.5),  # the last year; no Germany
]

#: The factor each provider takes for any unit other than current USD.
PROVIDER_FACTORS = {FRANCE: 2.0, GERMANY: 3.0}


class FakeTables:
    """A ``dac2a`` table with the view's logical columns."""

    def __init__(self) -> None:
        self.connection = duckdb.connect()
        self.connection.execute(
            "CREATE TABLE dac2a (provider BIGINT, recipient BIGINT, year BIGINT,"
            " line BIGINT, part BIGINT, amount_type VARCHAR, value DOUBLE)"
        )
        self.connection.executemany(
            "INSERT INTO dac2a VALUES (?, ?, ?, ?, ?, ?, ?)", ROWS
        )

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


def test_registry_maps_every_dac2a_indicator_by_id() -> None:
    assert [i.id for i in dac2a.INDICATORS] == [
        i for i in _registry.INDICATORS if i.startswith("oecd.dac2a.")
    ]
    assert all(_registry.INDICATORS[i.id] is i for i in dac2a.INDICATORS)


# DAC2A sums current USD rows only --------------------------------------------------


def test_sums_current_usd_rows_only(ctx: Context) -> None:
    df = ctx.get("oecd.dac2a.recipient_oda", providers=FRANCE, recipients=KENYA)

    assert _rows(df, "year", "value") == [(2000, 11.0), (2023, 10.0)]


# Headline is net disbursements (line 206) on DAC2A --------------------------------


@pytest.mark.parametrize(
    ("indicator", "basis", "resolved", "value"),
    [
        ("oecd.dac2a.recipient_oda", "headline", "net_disbursement", 10.0),
        ("oecd.dac2a.recipient_oda", "net_disbursement", "net_disbursement", 10.0),
        ("oecd.dac2a.recipient_oda", "gross_disbursement", "gross_disbursement", 12.0),
        ("oecd.dac2a.technical_cooperation", "headline", "net_disbursement", 3.0),
        ("oecd.dac2a.imputed_multilateral_oda", "headline", "net_disbursement", 4.0),
    ],
)
def test_each_basis_reads_its_own_line(
    ctx: Context, indicator: str, basis: str, resolved: str, value: float
) -> None:
    df = ctx.get(indicator, years=2023, providers=FRANCE, recipients=KENYA, basis=basis)

    assert _rows(df, "basis", "value") == [(resolved, value)]
    assert "line" not in df.columns


@pytest.mark.parametrize(
    "indicator",
    ["oecd.dac2a.technical_cooperation", "oecd.dac2a.imputed_multilateral_oda"],
)
def test_single_line_indicators_have_no_gross_basis(
    ctx: Context, indicator: str
) -> None:
    with pytest.raises(UnsupportedQueryError, match="gross_disbursement"):
        ctx.get(indicator, years=2023, basis="gross_disbursement")


# Providers default to every official provider; aggregates and private donors out --


def test_providers_default_to_every_official_provider(ctx: Context) -> None:
    q = ctx.query(
        "oecd.dac2a.recipient_oda", years=2023, recipients=KENYA, by=["provider"]
    )

    # DAC members, Türkiye (non-DAC) and IDA (multilateral) count; the
    # Gates Foundation (private) and 20001 (an aggregate) do not.
    assert _rows(q.to_pandas(), "provider", "value") == [
        ("4", 10.0),
        ("5", 20.0),
        ("55", 5.0),
        ("905", 40.0),
    ]
    assert q.provenance.defaults["providers"] == "official_providers"


def test_a_private_donor_answers_when_named(ctx: Context) -> None:
    df = ctx.get("oecd.dac2a.recipient_oda", years=2023, providers=GATES)

    assert df["value"].tolist() == [9.0]


def test_provider_aggregate_returns_its_own_row(ctx: Context) -> None:
    df = ctx.get(
        "oecd.dac2a.recipient_oda",
        years=2023,
        providers=DAC_TOTAL,
        recipients=KENYA,
        by=["provider"],
    )

    assert _rows(df, "provider", "is_aggregate", "value") == [("20001", True, 30.0)]


# Recipient totals drop when no recipient is named; regional rows stay -------------


def test_recipients_default_to_every_recipient_but_the_totals(ctx: Context) -> None:
    df = ctx.get(
        "oecd.dac2a.recipient_oda", years=2023, providers=FRANCE, by=["recipient"]
    )

    # Kenya, South of Sahara regional and unspecified; not 10100, the total,
    # and not IDA, a multilateral organisation as recipient.
    assert _rows(df, "recipient", "value") == [
        ("248", 10.0),
        ("289", 2.0),
        ("9998", 1.0),
    ]


def test_a_recipient_total_answers_when_named(ctx: Context) -> None:
    df = ctx.get(
        "oecd.dac2a.recipient_oda",
        years=2023,
        providers=FRANCE,
        recipients=DEVELOPING_TOTAL,
        by=["recipient"],
    )

    assert _rows(df, "recipient", "is_aggregate", "value") == [("10100", True, 13.0)]


@pytest.mark.parametrize(
    "indicator",
    [
        "oecd.dac2a.recipient_oda",
        "oecd.dac2a.imputed_multilateral_oda",
        "oecd.dac2a.technical_cooperation",
        "oecd.dac2a.lines",
    ],
)
@pytest.mark.parametrize("recipients", [IDA, [KENYA, 98]], ids=["ida", "list"])
def test_naming_a_multilateral_recipient_raises(
    ctx: Context, indicator: str, recipients
) -> None:
    with pytest.raises(UnsupportedQueryError, match="counts that money twice"):
        ctx.get(indicator, years=2023, recipients=recipients)


# Part II rows count: a country sits in one part in a year ------------------------


def test_part_ii_rows_count(ctx: Context) -> None:
    df = ctx.get(
        "oecd.dac2a.recipient_oda", years=2000, providers=FRANCE, by=["recipient"]
    )

    assert _rows(df, "recipient", "value") == [("87", 8.0), ("248", 11.0)]


# Imputed multilateral shares exclude multilateral providers ----------------------


def test_imputed_shares_leave_out_multilateral_providers(ctx: Context) -> None:
    df = ctx.get(
        "oecd.dac2a.imputed_multilateral_oda",
        years=2023,
        recipients=KENYA,
        by=["provider"],
    )

    # 918's own line 106 row stays out.
    assert _rows(df, "provider", "value") == [("4", 4.0), ("5", 6.0)]


_IMPUTED_EXCLUSION = dac2a.IMPUTED_MULTILATERAL_ODA.provider_exclusions[0]


@pytest.mark.parametrize(
    "providers",
    [
        EU_INSTITUTIONS,
        [FRANCE, IDA],
        groups.multilateral,
        20002,
        20004,
        [DAC_TOTAL, 20005],
    ],
    ids=["918", "list", "group", "20002", "20004", "20005"],
)
def test_naming_a_multilateral_provider_for_imputed_shares_raises(
    ctx: Context, providers
) -> None:
    with pytest.raises(
        UnsupportedQueryError, match=re.escape(_IMPUTED_EXCLUSION.reason)
    ):
        ctx.get("oecd.dac2a.imputed_multilateral_oda", years=2023, providers=providers)


def test_imputed_shares_state_their_exclusion_in_explain_and_provenance(
    ctx: Context,
) -> None:
    q = ctx.query("oecd.dac2a.imputed_multilateral_oda", years=2023)
    codes = _IMPUTED_EXCLUSION.values

    assert f"Leaves out {len(codes)} providers: {_IMPUTED_EXCLUSION.reason}." in (
        q.explain().splitlines()
    )
    assert q.provenance.defaults["excluded_providers"] == (
        f"{', '.join(map(str, codes))}: {_IMPUTED_EXCLUSION.reason}"
    )


def test_finance_type_on_dac2a_names_lines_and_an_indicator_that_has_it(
    ctx: Context,
) -> None:
    with pytest.raises(
        DimensionError,
        match=r"lines=, such as 201 .*\. Use oecd\.crs\.bilateral_oda for "
        "figures by finance type",
    ):
        ctx.query("oecd.dac2a.recipient_oda", finance_types=110)


@pytest.mark.parametrize(
    ("indicator", "arguments", "holder"),
    [
        ("oecd.dac1.total_oda", {"by": "sector"}, "oecd.crs.bilateral_oda"),
        ("oecd.dac1.total_oda", {"recipients": KENYA}, "oecd.dac2a.recipient_oda"),
        ("oecd.dac2a.recipient_oda", {"by": "line"}, "oecd.dac2a.lines"),
        ("oecd.crs.bilateral_oda", {"by": "score"}, "oecd.crs.gender_marker"),
        ("oecd.dac2a.recipient_oda", {"flows": 1121}, "oecd.dac1.total_oda"),
    ],
    ids=["sector", "recipient", "same-table-first", "score", "flow"],
)
def test_a_missing_dimension_names_an_indicator_that_has_it(
    ctx: Context, indicator: str, arguments: dict, holder: str
) -> None:
    # The check runs before any table is read, so the DAC2A table serves all.
    with pytest.raises(
        DimensionError, match=rf"\. Use {re.escape(holder)} for figures by "
    ):
        ctx.query(indicator, **arguments)


def test_imputed_shares_answer_for_aggregates_without_multilaterals(
    ctx: Context,
) -> None:
    df = ctx.get("oecd.dac2a.imputed_multilateral_oda", years=2023, providers=DAC_TOTAL)

    assert df["value"].tolist() == [10.0]


def test_recipient_oda_counts_multilateral_providers(ctx: Context) -> None:
    df = ctx.get(
        "oecd.dac2a.recipient_oda", years=2023, providers=IDA, recipients=KENYA
    )

    assert df["value"].tolist() == [40.0]


# Lines: each line its own row; lines that are not money left out -----------------


@pytest.mark.parametrize(
    ("basis", "lines", "expected"),
    [
        (
            None,
            [206, 207],
            [("206", "net_disbursement", 10.0), ("207", "net_disbursement", 3.0)],
        ),
        ("gross_disbursement", [240], [("240", "gross_disbursement", 12.0)]),
    ],
    ids=["net", "gross"],
)
def test_lines_split_the_result_and_label_each_line_with_its_basis(
    ctx: Context, basis: str | None, lines: list[int], expected: list[tuple]
) -> None:
    q = ctx.query(
        "oecd.dac2a.lines",
        years=2023,
        providers=FRANCE,
        recipients=KENYA,
        lines=lines,
        basis=basis,
        by=["provider"],
    )

    assert _rows(q.to_pandas(), "line", "basis", "value") == expected
    label = expected[0][1].replace("_disbursement", " disbursements")
    assert q.explain().splitlines()[0] == f"DAC2A lines (DAC2A), {label} 2023."


def test_lines_leave_out_lines_that_are_not_money(ctx: Context) -> None:
    found = {
        basis: set(
            ctx.get(
                "oecd.dac2a.lines",
                years=2023,
                providers=FRANCE,
                recipients=KENYA,
                basis=basis,
                by=["line"],
            )["line"]
        )
        for basis in ("net_disbursement", "gross_disbursement")
    }

    assert found == {
        "net_disbursement": {"106", "206", "207"},
        "gross_disbursement": {"240"},
    }


@pytest.mark.parametrize("line", [286, 296])
def test_naming_a_line_that_is_not_money_raises(ctx: Context, line: int) -> None:
    with pytest.raises(UnsupportedQueryError, match="not money"):
        ctx.get("oecd.dac2a.lines", years=2023, lines=line)


def test_a_line_on_the_other_basis_raises_and_names_its_basis(ctx: Context) -> None:
    with pytest.raises(
        UnsupportedQueryError,
        match="'net_disbursement' basis reads no line 240; the "
        "'gross_disbursement' basis reads it",
    ):
        ctx.get("oecd.dac2a.lines", years=2023, lines=[206, 240])


def test_lines_have_no_headline_basis(ctx: Context) -> None:
    with pytest.raises(UnsupportedQueryError, match="no 'headline' basis"):
        ctx.get("oecd.dac2a.lines", years=2023, basis="headline")


# A line counts what its named indicator counts --------------------------------------


def _named_lines() -> list[tuple[str, str, int]]:
    """Each named DAC2A indicator, each basis it reads one line on, and that
    line."""
    return [
        (indicator.id, basis, line)
        for indicator in dac2a.INDICATORS
        for basis, line in basis_lines(indicator).items()
    ]


@pytest.mark.parametrize(("indicator", "basis", "line"), _named_lines())
def test_a_named_indicator_equals_its_line(
    ctx: Context, indicator: str, basis: str, line: int
) -> None:
    kwargs = {"years": 2023, "basis": basis, "by": ["year"]}
    named = ctx.get(indicator, **kwargs)
    lines = ctx.get("oecd.dac2a.lines", lines=line, **kwargs)

    # 918's own line 106 row (7.0) stays out of both.
    assert _rows(lines, "year", "basis", "value") == _rows(
        named, "year", "basis", "value"
    )


def test_line_106_names_the_providers_it_leaves_out(ctx: Context) -> None:
    q = ctx.query("oecd.dac2a.lines", years=2023, providers=EU_INSTITUTIONS)
    reason = _IMPUTED_EXCLUSION.reason

    assert q.to_pandas()["value"].tolist() == []  # 918 has line 106 rows only
    assert (
        f"Leaves out {len(_IMPUTED_EXCLUSION.values)} providers on line 106: "
        f"{reason}." in q.explain().splitlines()
    )
    with pytest.raises(
        UnsupportedQueryError, match="leaves out provider 918 on line 106"
    ):
        ctx.get("oecd.dac2a.lines", years=2023, providers=EU_INSTITUTIONS, lines=106)


def test_line_106_exclusions_stay_out_of_explain_for_other_lines(
    ctx: Context,
) -> None:
    q = ctx.query("oecd.dac2a.lines", years=2023, lines=206)

    assert "on line 106" not in q.explain()


# Group totals sum members; constant prices deflate each provider first ------------


def test_a_group_total_sums_its_members(ctx: Context) -> None:
    df = ctx.get(
        "oecd.dac2a.recipient_oda",
        years=2023,
        providers=total(groups.dac_countries),
        recipients=KENYA,
    )

    assert df["value"].tolist() == [10.0 + 20.0]


def test_a_member_missing_from_the_last_year_nulls_the_group_total(
    ctx: Context,
) -> None:
    # France reports 2024, DAC2A's last year here; Germany does not.
    kwargs = {"years": 2024, "providers": total(groups.dac_countries)}
    df = ctx.get("oecd.dac2a.recipient_oda", **kwargs)
    partial = ctx.get("oecd.dac2a.recipient_oda", allow_partial=True, **kwargs)

    assert df["value"].isna().tolist() == [True]
    assert partial["value"].tolist() == [1.5]


def test_a_sum_over_providers_in_a_partial_last_year_warns_and_explains(
    ctx: Context,
) -> None:
    # Germany, Türkiye, IDA and 918 reported 2023 and not 2024; France did.
    q = ctx.query("oecd.dac2a.recipient_oda", years=[2023, 2024], by=["year"])
    with pytest.warns(OdaDataWarning) as caught:
        df = q.to_pandas()

    assert [str(w.message) for w in caught] == [
        "oecd.dac2a.recipient_oda: 2024 is partial: providers that reported a part "
        "in 2023 have not reported it in 2024. All of 2024 is left out of the sum "
        "for 4 providers. Show the providers on their own rows to see who reported."
    ]
    # 2023: France 13 (Kenya, regional, unspecified), Germany 20, Türkiye 5, IDA 40.
    assert _rows(df, "year", "value") == [(2023, 78.0), (2024, 1.5)]
    assert (
        "2024 is partial: 4 providers that reported 2023 have not reported 2024, "
        "so its sum covers only those that have; add 'provider' to by to see who "
        "reported." in q.explain().splitlines()
    )


def test_allow_partial_silences_the_partial_last_year_warning(ctx: Context) -> None:
    q = ctx.query(
        "oecd.dac2a.recipient_oda",
        years=[2023, 2024],
        by=["year"],
        allow_partial=True,
    )
    df = q.to_pandas()  # a warning would fail the test (filterwarnings = error)

    assert _rows(df, "year", "value") == [(2023, 78.0), (2024, 1.5)]
    assert any(
        "2024 is partial: 4 providers" in line for line in q.explain().splitlines()
    )


def test_a_result_split_by_provider_warns_of_those_that_have_not_reported(
    ctx: Context,
) -> None:
    q = ctx.query(
        "oecd.dac2a.recipient_oda", years=[2023, 2024], by=["provider", "year"]
    )

    with pytest.warns(OdaDataWarning) as caught:
        q.to_pandas()

    providers = (
        "Germany (5), International Development Association (905) and Türkiye (55)"
    )
    warning = (
        "2024 is partial: providers that reported a part in 2023 have not reported "
        f"it in 2024. Line 206 is missing for {providers}."
    )
    assert [str(w.message) for w in caught] == [f"oecd.dac2a.recipient_oda: {warning}"]
    text = (
        f"2024 is partial: {providers} reported line 206 in 2023 and have not "
        "reported it in 2024, so the result has no 2024 rows of line 206 for them."
    )
    assert text in q.explain().splitlines()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"years": 2023, "by": ["year"]},
        {"years": 2024, "providers": FRANCE, "by": ["year"]},
    ],
    ids=["not-the-last-year", "one-provider"],
)
def test_a_result_that_sums_no_partial_last_year_does_not_warn(
    ctx: Context, kwargs: dict
) -> None:
    q = ctx.query("oecd.dac2a.recipient_oda", **kwargs)
    q.to_pandas()  # a warning would fail the test (filterwarnings = error)

    assert "partial" not in q.explain()


@pytest.mark.parametrize(
    "indicator",
    [
        "oecd.dac2a.recipient_oda",
        "oecd.dac2a.imputed_multilateral_oda",
        "oecd.dac2a.technical_cooperation",
    ],
)
def test_the_official_donors_gap_shows_in_explain_without_a_warning(
    ctx: Context, indicator: str
) -> None:
    q = ctx.query(indicator, years=2023, providers=FRANCE)
    q.to_pandas()  # a warning would fail the test (filterwarnings = error)

    assert "20005" in q.explain()
    assert "Saudi Arabia" in q.explain()


@pytest.mark.parametrize("indicator", dac2a.INDICATORS[:3], ids=lambda i: i.id)
def test_the_official_donors_gap_gives_figures_for_its_own_line(indicator) -> None:
    [caveat] = indicator.caveats

    assert f"On line {basis_lines(indicator)['net_disbursement']} the gap" in (
        caveat.text
    )


def test_constant_prices_deflate_each_provider_before_the_sum(ctx: Context) -> None:
    df = ctx.get(
        "oecd.dac2a.recipient_oda",
        years=2023,
        providers=total(groups.dac_countries),
        recipients=KENYA,
        unit=constant("USD", 2023),
    )

    assert df["value"].tolist() == [10.0 * 2.0 + 20.0 * 3.0]

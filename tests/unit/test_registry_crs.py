"""The CRS indicators in the registry, run on a small in-memory CRS table.

Each case pins one CRS counting rule with rows built to break it: private
sector instruments (category 60) with both cash flows and a grant equivalent,
other official flows (category 21), core contributions (``bi_multi`` 2) and
multilateral outflows (``bi_multi`` 4), unscreened modalities, a private
donor, and an aggregate provider code. ``FakeFactors`` gives factor 1 for
current USD and a different factor per provider for any other unit.
"""

from collections.abc import Iterator
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pytest

from oda_data import _registry
from oda_data._context import Context
from oda_data._errors import (
    BasisRangeError,
    DimensionError,
    UnknownEntityError,
    UnsupportedQueryError,
)
from oda_data._factors import FACTOR_SCHEMA
from oda_data._groups import Group, SectorGroup, sectors, total
from oda_data._model import CRS_MARKERS, VOCABULARY, FactorTable, Indicator
from oda_data._provenance import Release
from oda_data._registry import crs
from oda_data._units import Unit, constant, current

USD = current("USD")
RELEASE = Release("crs-token", datetime(2026, 10, 6))
FRANCE, GERMANY, TURKIYE, GATES, IDA, DAC_TOTAL = 4, 5, 55, 1601, 905, 20001
KENYA, AFRICA_REGIONAL, UNSPECIFIED = 248, 298, 998
HEALTH, EDUCATION = 12220, 11110

COLUMNS = (*(column.name for column in VOCABULARY["crs"].columns), "project_title")

#: The defaults of a bilateral ODA row France reports for Kenya in 2023.
BASE = {
    "provider": FRANCE,
    "recipient": KENYA,
    "year": 2023,
    "flow": 11,
    "bi_multi": 1,
    "category": 10,
    "finance_type": 110,
    "modality": "C01",
    "sector": HEALTH,
    "channel": None,
    "commitment": 0.0,
    "disbursement": 0.0,
    "received": None,
    "grant_equivalent": 0.0,
    "project_title": "row",
}


def _row(**values: object) -> dict[str, object]:
    return dict.fromkeys(COLUMNS) | BASE | values


ROWS = [
    # France 2023, bilateral ODA
    _row(
        disbursement=10.0,
        commitment=12.0,
        grant_equivalent=9.0,
        gender=2,
        project_title="clinics",
    ),
    _row(
        bi_multi=3,
        disbursement=4.0,
        received=1.0,
        grant_equivalent=4.0,
        gender=1,
        channel=41122,
        project_title="through UNICEF",
    ),
    _row(
        bi_multi=7,
        sector=EDUCATION,
        disbursement=2.0,
        grant_equivalent=2.0,
        gender=0,
        channel=22000,
        project_title="through NGOs",
    ),
    _row(bi_multi=8, disbursement=1.0, grant_equivalent=1.0, project_title="other"),
    # Not screened: budget support has a marker score but is not allocable.
    _row(modality="A01", disbursement=100.0, grant_equivalent=100.0, gender=2),
    # Private sector instrument: cash flows out, grant equivalent in.
    _row(category=60, flow=60, disbursement=50.0, grant_equivalent=3.0, gender=2),
    # Other official flows, bilateral and through a multilateral.
    _row(category=21, flow=14, disbursement=30.0, commitment=31.0, received=5.0),
    _row(category=21, flow=14, bi_multi=4, disbursement=300.0),
    # Core contribution and a multilateral's own outflow.
    _row(bi_multi=2, modality="B02", disbursement=1000.0, gender=2),
    _row(provider=IDA, bi_multi=4, disbursement=70.0, grant_equivalent=20.0),
    # Earlier years, before grant equivalents became the ODA measure.
    _row(year=2017, disbursement=8.0, grant_equivalent=6.0),
    # Germany, Türkiye, a private donor, and the DAC total.
    _row(provider=GERMANY, disbursement=20.0, grant_equivalent=18.0, gender=1),
    _row(provider=GERMANY, year=2022, disbursement=19.0),
    _row(provider=TURKIYE, disbursement=5.0),
    _row(provider=GATES, disbursement=40.0, gender=2),
    _row(provider=DAC_TOTAL, disbursement=999.0),
]

#: The factor each provider takes for any unit other than current USD.
PROVIDER_FACTORS = {FRANCE: 2.0, GERMANY: 3.0}


class FakeTables:
    """A ``crs`` table with the view's logical columns and one passthrough
    column, ``project_title``."""

    def __init__(self, rows: list[dict[str, object]] = ROWS) -> None:
        self.connection = duckdb.connect()
        self.connection.register("rows", pd.DataFrame(rows, columns=list(COLUMNS)))
        types = {c.name: c.type for c in VOCABULARY["crs"].columns}
        select = ", ".join(
            f"CAST({name} AS {types.get(name, 'VARCHAR')}) AS {name}"
            for name in COLUMNS
        )
        self.connection.execute(f"CREATE TABLE crs AS SELECT {select} FROM rows")
        self.connection.unregister("rows")

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


def _rows(frame: pd.DataFrame, *columns: str) -> list[tuple]:
    """Rows as tuples, with missing codes as ``None``."""
    return [
        tuple(None if pd.isna(value) else value for value in row)
        for row in frame[list(columns)].itertuples(index=False)
    ]


def _value(ctx: Context, indicator: str, **call: object) -> float:
    """The single value of a call summed over everything but year."""
    df = ctx.get(indicator, **{"years": 2023, "by": ["year"]} | call)
    assert len(df) == 1, df
    return float(df["value"].iloc[0])


# Registry ------------------------------------------------------------------------


def test_registry_maps_every_crs_indicator_by_id() -> None:
    assert [i.id for i in crs.INDICATORS] == [
        "oecd.crs.bilateral_oda",
        "oecd.crs.multilateral_oda",
        "oecd.crs.bilateral_oof",
        "oecd.crs.multilateral_oof",
        "oecd.crs.private_development_finance",
        *(f"oecd.crs.{marker}_marker" for marker in CRS_MARKERS),
    ]
    assert all(_registry.INDICATORS[i.id] is i for i in crs.INDICATORS)


@pytest.mark.parametrize("indicator", crs.MARKERS, ids=lambda i: i.id)
def test_each_marker_scores_by_its_own_column(indicator: Indicator) -> None:
    column = indicator.id.removeprefix("oecd.crs.").removesuffix("_marker")
    assert indicator.binding.dimensions["score"] == column
    assert "score" in indicator.dimensions


#: Row values that put a row inside each kind of CRS indicator, and the
#: official provider whose row it is.
_IN_INDICATOR: dict[str, tuple[dict[str, object], int]] = {
    "oecd.crs.bilateral_oda": ({}, FRANCE),
    "oecd.crs.multilateral_oda": ({"bi_multi": 4}, IDA),
    "oecd.crs.bilateral_oof": ({"category": 21, "flow": 14}, FRANCE),
    "oecd.crs.multilateral_oof": ({"category": 21, "flow": 14, "bi_multi": 4}, IDA),
}


@pytest.mark.parametrize(
    "indicator",
    [i for i in crs.INDICATORS if i is not crs.PRIVATE_DEVELOPMENT_FINANCE],
    ids=lambda i: i.id,
)
def test_crs_indicators_default_to_every_official_provider(
    indicator: Indicator,
) -> None:
    values, official = _IN_INDICATOR.get(indicator.id, ({}, FRANCE))
    rows = [
        _row(**values | {"provider": provider, "disbursement": 1.0})
        for provider in (official, TURKIYE, GATES, DAC_TOTAL)
    ]
    with Context._from_parts(FakeTables(rows), FakeFactors()) as context:
        query = context.query(indicator.id, years=2023, by=["provider"])
        df = query.to_pandas()

    # A non-DAC official provider counts; a private donor and an aggregate
    # code do not.
    assert _rows(df, "provider", "basis", "value") == sorted(
        [
            (str(official), "gross_disbursement", 1.0),
            (str(TURKIYE), "gross_disbursement", 1.0),
        ]
    )
    assert query.provenance.defaults["providers"] == "official_providers"


# CRS ODA is category 10 on cash bases, 10 and 60 on grant equivalents -------------


@pytest.mark.parametrize(
    ("basis", "expected"),
    [
        # 10 + 4 + 2 + 1 + 100 (budget support is bilateral ODA); not the 50
        # of category 60 cash flows nor the 30 of category 21.
        ("gross_disbursement", 117.0),
        ("commitment", 12.0),
        ("net_disbursement", 116.0),  # 117 minus 1 received; null counts as 0
        ("grant_equivalent", 119.0),  # 9 + 4 + 2 + 1 + 100 + 3 of category 60
    ],
)
def test_oda_rows_are_category_10_and_60_only_on_grant_equivalents(
    ctx: Context, basis: str, expected: float
) -> None:
    value = _value(ctx, "oecd.crs.bilateral_oda", providers=FRANCE, basis=basis)

    assert value == expected


def test_grant_equivalents_before_2018_raise_and_name_a_covering_basis(
    ctx: Context,
) -> None:
    with pytest.raises(BasisRangeError, match="gross_disbursement"):
        ctx.get("oecd.crs.bilateral_oda", years=2017, basis="grant_equivalent")


# CRS bilateral flows are bi_multi 1, 3, 7 and 8 ----------------------------------


def test_bilateral_flows_leave_out_core_contributions_and_multilateral_outflows(
    ctx: Context,
) -> None:
    # The DAC1 caveat names no basis or years, so explain() shows it and no
    # call warns (pytest turns a warning into an error).
    query = ctx.query(
        "oecd.crs.bilateral_oda", years=2023, providers=FRANCE, by=["channel"]
    )
    df = query.to_pandas()

    assert "Caveat: CRS totals differ" in query.explain()

    # bi_multi 2 (1000) and 4 (IDA's 70) are not here; 3 and 7 are.
    assert _rows(df, "channel", "value") == [
        ("22000", 2.0),
        ("41122", 4.0),
        (None, 111.0),
    ]


def test_multilateral_oda_counts_the_organisations_own_outflows(
    ctx: Context,
) -> None:
    df = ctx.get("oecd.crs.multilateral_oda", years=2023, by=["provider"])

    assert _rows(df, "provider", "value") == [(str(IDA), 70.0)]


def test_net_disbursements_count_repayments_and_leave_out_rows_with_no_value() -> None:
    rows = [
        _row(disbursement=10.0),
        _row(disbursement=None, received=2.0),  # a repayment, nothing disbursed
        _row(sector=EDUCATION, disbursement=None, commitment=5.0),  # no cash
    ]
    with Context._from_parts(FakeTables(rows), FakeFactors()) as context:
        found = {
            basis: _rows(
                context.get(
                    "oecd.crs.bilateral_oda", years=2023, basis=basis, by=["sector"]
                ),
                "sector",
                "value",
            )
            for basis in ("net_disbursement", "gross_disbursement")
        }

    assert found == {
        "net_disbursement": [(str(HEALTH), 8.0)],
        "gross_disbursement": [(str(HEALTH), 10.0)],
    }


def test_recipients_default_to_every_recipient_with_regional_and_unspecified_rows(
    ctx: Context,
) -> None:
    rows = [
        _row(disbursement=1.0),
        _row(recipient=AFRICA_REGIONAL, disbursement=2.0),
        _row(recipient=UNSPECIFIED, disbursement=4.0),
    ]
    with Context._from_parts(FakeTables(rows), FakeFactors()) as context:
        df = context.get("oecd.crs.bilateral_oda", years=2023, by=["recipient"])

    assert _rows(df, "recipient", "value") == [
        (str(KENYA), 1.0),
        (str(AFRICA_REGIONAL), 2.0),
        (str(UNSPECIFIED), 4.0),
    ]


# Finance type is a CRS dimension ----------------------------------------------------


def test_finance_types_filter_and_split_crs_rows() -> None:
    rows = [
        _row(finance_type=110, disbursement=1.0),
        _row(finance_type=210, disbursement=2.0),
        _row(finance_type=421, disbursement=4.0),  # a standard loan
        _row(provider=GERMANY, finance_type=110, disbursement=8.0),
    ]
    with Context._from_parts(FakeTables(rows), FakeFactors()) as context:
        df = context.get(
            "oecd.crs.bilateral_oda",
            years=2023,
            finance_types=[110, 210],
            by=["provider", "finance_type"],
        )

    assert _rows(df, "provider", "finance_type", "value") == [
        (str(FRANCE), "110", 1.0),
        (str(FRANCE), "210", 2.0),
        (str(GERMANY), "110", 8.0),
    ]


# Bilateral other official flows are category 21 ------------------------------------


@pytest.mark.parametrize(
    ("basis", "expected"),
    [("gross_disbursement", 30.0), ("commitment", 31.0), ("net_disbursement", 25.0)],
)
def test_bilateral_oof_reads_category_21_bilateral_rows(
    ctx: Context, basis: str, expected: float
) -> None:
    assert _value(ctx, "oecd.crs.bilateral_oof", basis=basis) == expected


@pytest.mark.parametrize(
    ("basis", "expected"),
    [("gross_disbursement", 300.0), ("commitment", 0.0), ("net_disbursement", 300.0)],
)
def test_multilateral_oof_reads_category_21_multilateral_outflows(
    ctx: Context, basis: str, expected: float
) -> None:
    assert _value(ctx, "oecd.crs.multilateral_oof", basis=basis) == expected


#: Private development finance from Gates (a private donor) and France, beside
#: rows of other categories and a core contribution to a multilateral
#: (bi_multi 2), which it leaves out. A row with no bi_multi stays in.
_PRIVATE_ROWS = [
    _row(
        provider=GATES,
        category=30,
        flow=30,
        bi_multi=6,
        disbursement=40.0,
        commitment=50.0,
        received=1.0,
    ),
    _row(provider=GATES, category=30, flow=30, bi_multi=2, disbursement=5.0),
    _row(provider=GATES, category=30, flow=30, bi_multi=None, disbursement=3.0),
    _row(provider=GATES, disbursement=100.0),
    _row(provider=FRANCE, category=30, flow=30, bi_multi=6, disbursement=7.0),
    _row(provider=FRANCE, disbursement=9.0),
]


@pytest.mark.parametrize(
    ("basis", "expected"),
    [("gross_disbursement", 43.0), ("commitment", 50.0), ("net_disbursement", 42.0)],
)
def test_private_development_finance_reads_category_30_from_private_donors(
    basis: str, expected: float
) -> None:
    with Context._from_parts(FakeTables(_PRIVATE_ROWS), FakeFactors()) as context:
        query = context.query(
            "oecd.crs.private_development_finance",
            years=2023,
            by=["provider"],
            basis=basis,
        )
        df = query.to_pandas()
        named = context.get(
            "oecd.crs.private_development_finance", years=2023, providers=FRANCE
        )

    # France's category 30 row counts only when a call names France.
    assert _rows(df, "provider", "value") == [(str(GATES), expected)]
    assert query.provenance.defaults["providers"] == "private_donors"
    assert named["value"].tolist() == [7.0]


@pytest.mark.parametrize(
    "indicator",
    [
        "oecd.crs.bilateral_oof",
        "oecd.crs.multilateral_oda",
        "oecd.crs.multilateral_oof",
        "oecd.crs.private_development_finance",
    ],
)
def test_flows_without_a_grant_equivalent_offer_cash_bases_only(
    ctx: Context, indicator: str
) -> None:
    # IDA's bi_multi 4 row carries a grant equivalent here; the CRS gives none.
    with pytest.raises(UnsupportedQueryError, match="its bases are gross_disb"):
        ctx.get(indicator, years=2023, basis="grant_equivalent")


# Providers default to every official provider; aggregates and private donors out --


def test_providers_default_to_official_providers_without_aggregates(
    ctx: Context,
) -> None:
    q = ctx.query("oecd.crs.bilateral_oda", years=2023, by=["provider"])
    df = q.to_pandas()

    # Türkiye reports outside the DAC; Gates (private) and 20001 stay out.
    assert _rows(df, "provider", "value") == [
        (str(FRANCE), 117.0),
        (str(GERMANY), 20.0),
        (str(TURKIYE), 5.0),
    ]
    assert q.provenance.defaults["providers"] == "official_providers"


# Policy markers: one indicator per marker, scores as a dimension -------------------


def test_marker_scores_split_bilateral_allocable_oda(ctx: Context) -> None:
    df = ctx.get("oecd.crs.gender_marker", years=2023, providers=FRANCE, by=["score"])

    # Budget support (A01) and the core contribution (B02) are not screened
    # modalities, so their score 2 counts nowhere. The null-score row is not
    # screened.
    assert _rows(df, "score", "value") == [
        ("not_screened", 1.0),
        ("not_targeted", 2.0),
        ("principal", 10.0),
        ("significant", 4.0),
    ]


def test_a_marker_on_grant_equivalents_adds_category_60(ctx: Context) -> None:
    df = ctx.get(
        "oecd.crs.gender_marker",
        years=2023,
        providers=FRANCE,
        by=["score"],
        basis="grant_equivalent",
    )

    assert dict(_rows(df, "score", "value"))["principal"] == 9.0 + 3.0


@pytest.mark.parametrize("marker", CRS_MARKERS)
def test_each_marker_indicator_reads_its_own_column(marker: str) -> None:
    rows = [
        _row(disbursement=6.0, **{marker: 2}),
        _row(
            disbursement=1.0, **{other: 2 for other in CRS_MARKERS if other != marker}
        ),
    ]
    with Context._from_parts(FakeTables(rows), FakeFactors()) as context:
        df = context.get(f"oecd.crs.{marker}_marker", years=2023, by=["score"])

    assert _rows(df, "score", "value") == [("not_screened", 1.0), ("principal", 6.0)]


def test_desertification_counts_score_3_as_principal() -> None:
    rows = [
        _row(disbursement=1.0, desertification=3),
        _row(disbursement=2.0, desertification=2),
    ]
    with Context._from_parts(FakeTables(rows), FakeFactors()) as context:
        df = context.get("oecd.crs.desertification_marker", years=2023, by=["score"])

    assert _rows(df, "score", "value") == [("principal", 3.0)]


def test_a_score_with_no_label_raises() -> None:
    # Only desertification scores 3, so a 3 elsewhere is a code nobody named.
    rows = [_row(disbursement=1.0, gender=3)]
    with (
        Context._from_parts(FakeTables(rows), FakeFactors()) as context,
        pytest.raises(UnknownEntityError, match=r"crs column gender .* label: 3"),
    ):
        context.get("oecd.crs.gender_marker", years=2023, by=["score"])


def test_markers_start_in_2010_when_the_aid_type_is_filled() -> None:
    # Before 2010 the CRS often leaves the aid type blank, so the screened-
    # modality filter would drop most activities.
    rows = [
        _row(year=2009, modality=None, disbursement=50.0, gender=2),
        _row(year=2009, disbursement=5.0, gender=2),
        _row(year=2010, disbursement=1.0, gender=2),
    ]
    with Context._from_parts(FakeTables(rows), FakeFactors()) as context:
        with pytest.raises(BasisRangeError, match="starts in 2010"):
            context.get("oecd.crs.gender_marker", years=[2009, 2010])
        every_year = context.get("oecd.crs.gender_marker", by=["year"])
        bilateral = context.get("oecd.crs.bilateral_oda", years=2009)

    assert _rows(every_year, "year", "value") == [(2010, 1.0)]
    assert bilateral["value"].sum() == 55.0


def test_score_belongs_to_markers_only(ctx: Context) -> None:
    with pytest.raises(DimensionError, match="'score'"):
        ctx.get("oecd.crs.bilateral_oda", years=2023, by=["score"])


# Dimensions: broad sectors, flow when asked for -----------------------------------


def test_broad_sectors_group_purpose_codes(ctx: Context) -> None:
    df = ctx.get(
        "oecd.crs.bilateral_oda",
        years=2023,
        providers=FRANCE,
        by=["broad_sector"],
    )

    assert _rows(df, "broad_sector", "value") == [
        ("education", 2.0),
        ("health", 115.0),
    ]


#: Rows at heading level (122, 120) and under three headings of Health.
HEADING_ROWS = [
    _row(sector=sector, disbursement=value)
    for sector, value in (
        (122, 1.0),
        (12220, 2.0),
        (12230, 4.0),
        (12110, 8.0),
        (120, 16.0),
    )
]


@pytest.mark.parametrize(
    ("sectors", "expected"),
    [
        (122, 7.0),
        ("122", 7.0),
        (SectorGroup("basic", [122]), 7.0),
        ([122, 12110], 15.0),
        (120, 31.0),  # a parent heading holds 121, 122, and 123
        (12220, 2.0),
        (sectors.reported_at(122), 1.0),
        (SectorGroup("own", [sectors.reported_at(122), 12110]), 9.0),
    ],
    ids=[
        "int",
        "str",
        "group",
        "with-a-code",
        "parent",
        "purpose-code",
        "reported-at",
        "reported-at-in-a-group",
    ],
)
def test_a_sector_heading_reads_every_code_under_it(sectors, expected) -> None:
    with Context._from_parts(FakeTables(HEADING_ROWS), FakeFactors()) as context:
        assert _value(context, "oecd.crs.bilateral_oda", sectors=sectors) == expected


@pytest.mark.parametrize(
    ("call", "has_flow"),
    [
        ({}, False),
        ({"by": ["provider", "flow"]}, True),
        ({"flows": 11}, True),
    ],
    ids=["default", "by", "filter"],
)
def test_flow_appears_when_a_call_filters_or_groups_by_it(
    ctx: Context, call: dict, has_flow: bool
) -> None:
    df = ctx.get(
        "oecd.crs.bilateral_oda",
        **{"years": 2023, "providers": FRANCE, "basis": "grant_equivalent"} | call,
    )

    assert ("flow" in df.columns) is has_flow
    if has_flow:
        # Grant equivalents add category 60, whose flow is 60.
        expected = [("11", 116.0)] if "flows" in call else [("11", 116.0), ("60", 3.0)]
        assert _rows(df, "flow", "value") == expected


# records() returns the rows a Sum adds up -----------------------------------------


def test_records_rows_sum_to_the_value_with_passthrough_columns(ctx: Context) -> None:
    rows = ctx.records(
        "oecd.crs.bilateral_oda",
        years=2023,
        providers=FRANCE,
        sectors=HEALTH,
        columns=["project_title"],
    )
    value = _value(ctx, "oecd.crs.bilateral_oda", providers=FRANCE, sectors=HEALTH)

    assert sorted(rows["project_title"]) == [
        "clinics",
        "other",
        "row",
        "through UNICEF",
    ]
    assert rows["value"].sum() == value == 115.0


# Constant prices use each provider's deflator before any sum -----------------------


def test_constant_prices_deflate_each_provider_before_the_sum(ctx: Context) -> None:
    group = Group("pair", [FRANCE, GERMANY])
    value = _value(
        ctx,
        "oecd.crs.bilateral_oda",
        providers=total(group),
        by=["provider"],
        unit=constant("USD", 2023),
    )

    assert value == 117.0 * 2.0 + 20.0 * 3.0


# On CRS, reporting means any CRS row for the provider and year -----------------


@pytest.mark.parametrize(
    ("germany_2023", "expected"),
    [
        # Germany's only 2023 row is OOF, so it still reports: it adds nothing.
        ([_row(provider=GERMANY, category=21, disbursement=30.0)], 10.0),
        # Germany has no 2023 row at all, so the total is null.
        ([], None),
    ],
    ids=["oof-row", "no-row"],
)
def test_a_member_with_any_crs_row_counts_as_reporting(
    germany_2023: list[dict[str, object]], expected: float | None
) -> None:
    rows = [
        _row(disbursement=10.0),
        _row(provider=GERMANY, year=2022, disbursement=19.0),
        *germany_2023,
        # A later year, so 2023 is not the table's preliminary last year.
        _row(provider=TURKIYE, year=2024, disbursement=1.0),
    ]
    group = Group("pair", [FRANCE, GERMANY])
    with Context._from_parts(FakeTables(rows), FakeFactors()) as context:
        df = context.get(
            "oecd.crs.bilateral_oda",
            years=2023,
            providers=total(group),
            by=["provider", "year"],
        )

    assert _rows(df, "provider", "value") == [("pair", expected)]

"""DAC1 indicators through ``oda.get`` against the cached DAC1 table.

Every expected value comes from the same DAC1 release the indicators read,
queried here straight from the table view: the OECD's own DAC total (provider
20001), its GNI and ODA lines, its published ODA/GNI line, and its
constant-price rows. A revision by the OECD moves both sides at once.
"""

import math
from collections.abc import Iterator

import pytest

import oda_data as oda
from oda_data._errors import MissingFactorError, OfflineCacheMissError
from oda_data._resolve import resolve_group
from oda_data._tables import OecdTables

pytestmark = pytest.mark.data

DAC_TOTAL = 20001
FRANCE, GERMANY, UK, NORWAY = 4, 5, 12, 8


@pytest.fixture(scope="module")
def dac1() -> Iterator[OecdTables]:
    tables = OecdTables(offline=True)
    try:
        tables.cursor(("dac1",))
    except OfflineCacheMissError:
        pytest.skip("DAC1 is not cached; run scripts/warm_cache.py")
    yield tables
    tables.close()


def _lines(
    tables: OecdTables, *, line: int, flow: int, amount_type: str = "A"
) -> dict[tuple[int, int], float]:
    """Each (provider, year) value of one DAC1 line under one flow."""
    rows = (
        tables.cursor(("dac1",))
        .sql(
            "SELECT provider, year, sum(value) FROM dac1 "
            f"WHERE line = {line} AND flow = {flow} "
            f"AND amount_type = '{amount_type}' GROUP BY ALL"
        )
        .fetchall()
    )
    return {(provider, year): value for provider, year, value in rows}


def _headline_total(tables: OecdTables) -> dict[tuple[int, int], float]:
    """Total ODA on the headline basis: line 1010 under net flows (1140) to
    2017, line 11010 under grant equivalents (1160) from 2018."""
    net = _lines(tables, line=1010, flow=1140)
    grant_equivalent = _lines(tables, line=11010, flow=1160)
    return {key: value for key, value in net.items() if key[1] < 2018} | {
        key: value for key, value in grant_equivalent.items() if key[1] >= 2018
    }


def test_dac_countries_sum_to_the_dac_total_in_every_year(dac1: OecdTables) -> None:
    """Expected: provider 20001's own total ODA rows in DAC1. Members that had
    not yet reported, and members that skipped years (Spain and Portugal
    1971-79, Hungary 1993-2002), count as zero, so no year is null."""
    dac = oda.groups.dac_countries
    df = oda.get(
        "oecd.dac1.total_oda",
        providers=oda.total(dac),
        by=["provider", "year"],
    )
    expected = {
        year: value
        for (provider, year), value in _headline_total(dac1).items()
        if provider == DAC_TOTAL
    }

    found = dict(zip(df["year"], df["value"], strict=True))

    assert sorted(found) == sorted(expected)
    assert min(found) == 1960
    for year, value in expected.items():
        assert found[year] == pytest.approx(value, rel=1e-6, abs=0.01), year


def test_dac_countries_beside_their_total(dac1: OecdTables) -> None:
    """Expected: the 33 members' own rows, and provider 20001's 2023 total."""
    dac = oda.groups.dac_countries
    df = oda.get(
        "oecd.dac1.total_oda",
        years=2023,
        providers=[dac, oda.total(dac)],
        by=["provider"],
    )
    members = df[~df["is_aggregate"]]
    totals = df[df["is_aggregate"]]
    headline = _headline_total(dac1)

    assert len(members) == 33
    for provider, value in zip(members["provider"], members["value"], strict=True):
        assert value == pytest.approx(headline[(int(provider), 2023)])
    assert totals["provider"].tolist() == ["dac_countries"]
    assert totals["value"].iloc[0] == pytest.approx(members["value"].sum())
    assert totals["value"].iloc[0] == pytest.approx(
        headline[(DAC_TOTAL, 2023)], rel=1e-6
    )


@pytest.mark.parametrize("year", [2023, 2024])
def test_oda_gni_ratio_for_members_and_the_dac_total(
    dac1: OecdTables, year: int
) -> None:
    """Expected for a member: 100 x line 11010 (flow 1160) / line 1 (GNI, flow
    1140), and the OECD's published ratio, line 11002, to its rounding. For
    ``oda.total(dac_countries)``: 100 x the members' summed line 11010 over
    their summed line 1, and 20001's line 11002."""
    dac = oda.groups.dac_countries
    df = oda.get(
        "oecd.dac1.oda_gni_ratio",
        years=year,
        providers=[FRANCE, GERMANY, UK, oda.total(dac)],
        by=["provider"],
    )
    found = dict(zip(df["provider"], df["value"], strict=True))
    oda_ge = _lines(dac1, line=11010, flow=1160)
    gni = _lines(dac1, line=1, flow=1140)
    published = _lines(dac1, line=11002, flow=1160)

    for provider in (FRANCE, GERMANY, UK):
        ratio = found[str(provider)]
        assert ratio == pytest.approx(
            100 * oda_ge[(provider, year)] / gni[(provider, year)]
        )
        assert ratio == pytest.approx(published[(provider, year)], abs=0.005)

    members = resolve_group(dac, "provider", years=(year,)).members[year]
    assert len(members) == 33
    group_ratio = (
        100
        * sum(oda_ge[(code, year)] for code in members)
        / sum(gni[(code, year)] for code in members)
    )
    assert found["dac_countries"] == pytest.approx(group_ratio)
    assert found["dac_countries"] == pytest.approx(
        published[(DAC_TOTAL, year)], abs=0.005
    )


def test_constant_prices_match_dac1_constant_rows(dac1: OecdTables) -> None:
    """Expected: DAC1's own constant-price rows (amount type D) of line 1010
    under net flows, whose base year is the one year in which 20001's current
    and constant rows agree."""
    current = _lines(dac1, line=1010, flow=1140)
    constant = _lines(dac1, line=1010, flow=1140, amount_type="D")
    (base_year,) = {
        year
        for (provider, year), value in constant.items()
        if provider == DAC_TOTAL and value == pytest.approx(current[(provider, year)])
    }
    years = range(2015, base_year + 1)

    df = oda.get(
        "oecd.dac1.total_oda",
        years=years,
        providers=FRANCE,
        basis="net_disbursement",
        unit=oda.constant("USD", base_year),
    )

    assert set(df["base_year"]) == {base_year}
    found = dict(zip(df["year"], df["value"], strict=True))
    for year in years:
        assert found[year] == pytest.approx(constant[(FRANCE, year)], rel=1e-6), year


def test_dac_countries_oda_gni_matches_the_dac_total_in_every_year(
    dac1: OecdTables,
) -> None:
    """Expected: 100 x provider 20001's headline total ODA over its GNI (line
    1, net flows), to 0.005 percentage points. A member with ODA and no GNI in
    a year (Hungary in 1992) is left out of both sums, which moves the ratio
    by up to 0.002 points in 1975-1994. The published ratio (line 2) is not
    the reference: for 1990-1992 it differs from 20001's own lines by 0.013."""
    df = oda.get(
        "oecd.dac1.oda_gni_ratio",
        providers=oda.total(oda.groups.dac_countries),
        by=["provider", "year"],
    )
    gni = _lines(dac1, line=1, flow=1140)
    expected = {
        year: 100 * value / gni[(DAC_TOTAL, year)]
        for (provider, year), value in _headline_total(dac1).items()
        if provider == DAC_TOTAL
    }

    found = dict(zip(df["year"], df["value"], strict=True))

    assert sorted(found) == sorted(expected)
    for year, value in expected.items():
        assert found[year] == pytest.approx(value, abs=0.005), year


def test_a_total_over_a_preliminary_year_is_null_where_members_drop_a_line(
    dac1: OecdTables,
) -> None:
    """DAC1's last year is preliminary: every DAC country reports total ODA,
    but few report in-donor refugee costs. Expected: refugee costs over
    ``dac_countries`` are null in the last year and equal provider 20001's
    own row the year before; total ODA is not null in the last year."""
    [(last,)] = dac1.cursor(("dac1",)).sql("SELECT max(year) FROM dac1").fetchall()
    total = oda.total(oda.groups.dac_countries)

    refugees = oda.get(
        "oecd.dac1.in_donor_refugee_costs",
        years=[last - 1, last],
        providers=total,
        by=["provider", "year"],
    )
    oda_total = oda.get(
        "oecd.dac1.total_oda", years=last, providers=total, by=["provider", "year"]
    )

    found = dict(zip(refugees["year"], refugees["value"], strict=True))
    published = _lines(dac1, line=1820, flow=1160)[(DAC_TOTAL, last - 1)]
    assert found[last - 1] == pytest.approx(published, rel=1e-6)
    assert math.isnan(found[last])
    assert oda_total["value"].notna().all() and len(oda_total) == 1


@pytest.mark.parametrize(
    ("provider", "year", "raises"),
    [(UK, 2015, True), (UK, 2022, True), (NORWAY, 1992, True), (NORWAY, 2022, False)],
    ids=["uk-a-member", "uk-pays-after-it-left", "norway-pays", "norway-pays-nothing"],
)
def test_eu_institutions_are_summed_only_with_providers_that_do_not_pay_them(
    dac1: OecdTables, provider: int, year: int, raises: bool
) -> None:
    """A provider that pays EU Institutions (918), as an EU member or through
    line 2102, holds that payment in its own multilateral ODA. The United
    Kingdom left the EU in 2020 and reports line 2102 after it; Norway
    reports it in 1992 only."""
    call = {"years": year, "providers": [918, provider], "by": ["year"]}
    if raises:
        with pytest.raises(oda.UnsupportedQueryError, match="918"):
            oda.get("oecd.dac1.total_oda", **call)
    else:
        oda.get("oecd.dac1.total_oda", **call)


def test_current_eur_before_the_ecu_series_raises(dac1: OecdTables) -> None:
    """DAC1 holds no euro (ECU) rates for 1970-1972, so current EUR raises
    rather than returning US dollars labelled euros."""
    with pytest.raises(MissingFactorError):
        oda.get(
            "oecd.dac1.total_oda",
            years=1971,
            providers=[302, FRANCE],
            unit=oda.current("EUR"),
        )


def test_population_is_the_dac1_row_and_the_dac_total_sums_it(
    dac1: OecdTables,
) -> None:
    """Expected: DAC1 line 4 under net flows, millions of people: France's
    own row in each year, and provider 20001's row for the DAC countries'
    total in every year from 1960, the last year included."""
    published = _lines(dac1, line=4, flow=1140)
    france = oda.get("oecd.dac1.population", providers=FRANCE)
    dac = oda.get("oecd.dac1.population", providers=oda.total(oda.groups.dac_countries))

    assert france["currency"].isna().all()
    assert dict(zip(france["year"], france["value"], strict=True)) == pytest.approx(
        {year: value for (code, year), value in published.items() if code == FRANCE}
    )
    assert dict(zip(dac["year"], dac["value"], strict=True)) == pytest.approx(
        {year: value for (code, year), value in published.items() if code == DAC_TOTAL}
    )

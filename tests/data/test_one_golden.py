"""ONE's indicators through ``oda.get`` against the cached DAC1, DAC2A and CRS.

Every expected value comes from the same releases the indicators read,
queried here straight from the table views: the DAC1 lines core ODA is built
from, EU Institutions' (918) total ODA and its funders' line 2102, the EU
members' GNI, and the CRS rows tagged #COVID-19. The DAC2A and CRS-plus-
Multisystem sums are checked against their named parts. A revision by the
OECD moves both sides at once. ``test_goldens.py`` pins 2.8.2's EU
Institutions ratios and the Cyprus GNI gaps to a DAC1 release.
"""

import math
from collections.abc import Iterator

import pytest

import oda_data as oda
from oda_data._errors import OfflineCacheMissError
from oda_data._names import eu_members
from oda_data._resolve import resolve_group
from oda_data._tables import OecdTables

pytestmark = pytest.mark.data

EU_INSTITUTIONS = 918

type Cells = dict[tuple[int, int], float]

#: Total ODA under grant equivalents, then the lines core ODA subtracts.
CORE_LINES = (11010, 1820, 1500, 1600)


@pytest.fixture(scope="module")
def tables() -> Iterator[OecdTables]:
    tables = OecdTables(offline=True)
    try:
        tables.cursor(("dac1", "dac2a", "crs"))
    except OfflineCacheMissError:
        pytest.skip("DAC1, DAC2A or CRS is not cached; run scripts/warm_cache.py")
    yield tables
    tables.close()


def _line(tables: OecdTables, *, line: int, flow: int) -> Cells:
    """Each (provider, year) current USD value of one DAC1 line under one flow."""
    rows = (
        tables.cursor(("dac1",))
        .sql(
            "SELECT provider, year, sum(value) FROM dac1 WHERE line = ? AND flow = ? "
            "AND amount_type = 'A' GROUP BY ALL",
            params=[line, flow],
        )
        .fetchall()
    )
    return {(provider, year): value for provider, year, value in rows}


def _by_provider_year(frame) -> Cells:
    return {
        (int(provider), int(year)): value
        for provider, year, value in zip(
            frame["provider"], frame["year"], frame["value"], strict=True
        )
    }


def test_core_oda_is_total_oda_less_the_three_lines(tables: OecdTables) -> None:
    """Expected for each DAC country: line 1010 less lines 1820, 1500 and 1600
    under net flows (1140) to 2017, and line 11010 less the same lines under
    grant equivalents (1160) from 2018."""
    years = range(2014, 2025)
    df = oda.get("one.dac1.core_oda", years=years)
    found = _by_provider_year(df)

    def core(total: int, flow: int) -> Cells:
        value = _line(tables, line=total, flow=flow)
        for line in (1820, 1500, 1600):
            for key, subtracted in _line(tables, line=line, flow=flow).items():
                if key in value:
                    value[key] -= subtracted
        return value

    expected = {
        key: value
        for key, value in core(1010, 1140).items()
        if 2014 <= key[1] <= 2017 and key in found
    } | {
        key: value
        for key, value in core(11010, 1160).items()
        if 2018 <= key[1] <= 2024 and key in found
    }

    assert len(found) == len(expected) > 30 * len(years)
    for key, value in expected.items():
        assert found[key] == pytest.approx(value, abs=1e-6), key


def test_core_oda_is_null_for_members_with_a_line_missing_in_the_last_year(
    tables: OecdTables,
) -> None:
    """In DAC1's last year, a preliminary one, a DAC country's core ODA is null
    when it reported line 11010, 1820, 1500 or 1600 the year before and not
    yet that year. allow_partial=True returns each country's partial value."""
    [(last,)] = tables.cursor(("dac1",)).sql("SELECT max(year) FROM dac1").fetchall()
    assert last >= 2025
    lines = {line: _line(tables, line=line, flow=1160) for line in CORE_LINES}
    df = oda.get("one.dac1.core_oda", years=[last - 1, last])
    partial = oda.get("one.dac1.core_oda", years=[last - 1, last], allow_partial=True)
    found = _by_provider_year(df)
    partial_found = _by_provider_year(partial)

    nulled = {
        provider
        for provider, year in found
        if year == last
        and any(
            (provider, last - 1) in cells and (provider, last) not in cells
            for cells in lines.values()
        )
    }
    assert nulled, "expected some members to lack a line in the preliminary year"
    for (provider, year), value in found.items():
        expected = lines[11010].get((provider, year), 0.0) - sum(
            lines[line].get((provider, year), 0.0) for line in CORE_LINES[1:]
        )
        assert partial_found[(provider, year)] == pytest.approx(expected, abs=1e-6)
        if year == last and provider in nulled:
            assert math.isnan(value), (provider, year)
        else:
            assert value == pytest.approx(expected, abs=1e-6), (provider, year)


def test_recipient_oda_with_imputed_multilateral_is_its_two_parts(
    tables: OecdTables,
) -> None:
    """Expected: oecd.dac2a.recipient_oda plus oecd.dac2a.imputed_multilateral_oda
    for each DAC country."""
    call = {"years": [2015, 2023], "by": ["provider", "year"]}
    providers = oda.groups.dac_countries
    found = _by_provider_year(
        oda.get(
            "one.dac2a.recipient_oda_with_imputed_multilateral",
            providers=providers,
            **call,
        )
    )
    bilateral = _by_provider_year(
        oda.get("oecd.dac2a.recipient_oda", providers=providers, **call)
    )
    imputed = _by_provider_year(
        oda.get("oecd.dac2a.imputed_multilateral_oda", providers=providers, **call)
    )

    assert set(found) == set(bilateral) | set(imputed)
    for key, value in found.items():
        expected = bilateral.get(key, 0.0) + imputed.get(key, 0.0)
        assert value == pytest.approx(expected, rel=1e-9), key


@pytest.mark.parametrize(
    ("basis", "line", "flow"),
    [("net_disbursement", 1010, 1140), ("grant_equivalent", 11010, 1160)],
)
@pytest.mark.parametrize("year", [2019, 2023])
def test_eu_institutions_keep_what_eu27_did_not_pay(
    tables: OecdTables, basis: str, line: int, flow: int, year: int
) -> None:
    """Expected: 918's total ODA times 1 - (EU27's line 2102 / 918's total ODA),
    the weight 2.8.1's ``research.eu.get_eui_oda_weights`` gives for the EU27
    on the same flow."""
    eu27 = resolve_group(oda.groups.eu27, "provider", years=[year]).members[year]
    df = oda.get(
        "one.dac1.total_oda_eu_attributed",
        years=year,
        providers=[oda.groups.eu27, EU_INSTITUTIONS],
        basis=basis,
    )
    found = _by_provider_year(df)
    spent = _line(tables, line=line, flow=flow)[(EU_INSTITUTIONS, year)]
    contributions = _line(tables, line=2102, flow=flow)
    paid = sum(contributions.get((code, year), 0.0) for code in eu27)
    weight = 1 - paid / spent

    assert 0 < weight < 1
    assert found[(EU_INSTITUTIONS, year)] == pytest.approx(spent * weight, rel=1e-9)
    total_oda = _line(tables, line=line, flow=flow)
    for code in eu27:
        if (code, year) in total_oda:
            assert found[(code, year)] == pytest.approx(total_oda[(code, year)])


@pytest.mark.parametrize("year", [2019, 2023])
def test_eu_institutions_oda_gni_uses_the_gni_of_that_years_members(
    tables: OecdTables, year: int
) -> None:
    """Expected: 100 x 918's line 11010 (flow 1160) over the summed line 1 (GNI,
    flow 1140) of the countries that were EU members that year, the United
    Kingdom in 2019 included."""
    df = oda.get("one.dac1.oda_gni_ratio", years=year, providers=EU_INSTITUTIONS)
    gni = _line(tables, line=1, flow=1140)
    members = eu_members([year])
    assert all((code, year) in gni for code in members)
    eu_gni = sum(gni[(code, year)] for code in members)
    oda_ge = _line(tables, line=11010, flow=1160)[(EU_INSTITUTIONS, year)]

    assert df["value"].tolist() == [pytest.approx(100 * oda_ge / eu_gni)]
    assert (12 in members) == (year <= 2020)


def test_members_oda_gni_matches_the_oecd_ratio(tables: OecdTables) -> None:
    call = {"years": 2023, "providers": oda.groups.dac_countries}
    one = _by_provider_year(oda.get("one.dac1.oda_gni_ratio", **call))
    oecd = _by_provider_year(oda.get("oecd.dac1.oda_gni_ratio", **call))

    assert one == pytest.approx(oecd)


def test_covid_oda_is_the_bilateral_rows_tagged_covid_19(tables: OecdTables) -> None:
    """Expected: CRS category 10 bilateral gross disbursements (bi_multi 1, 3, 7,
    8) whose keywords hold #COVID-19, the only COVID tag in the CRS, summed
    over every official provider."""
    [(expected,)] = (
        tables.cursor(("crs",))
        .sql(
            "SELECT sum(disbursement) FROM crs WHERE year = 2021 AND category = 10 "
            "AND bi_multi IN (1, 3, 7, 8) AND keywords LIKE '%#COVID-19%' "
            "AND provider < 20000"
        )
        .fetchall()
    )
    private = resolve_group(oda.groups.private_donors, "provider", years=[2021])
    [(private_value,)] = (
        tables.cursor(("crs",))
        .sql(
            "SELECT coalesce(sum(disbursement), 0) FROM crs WHERE year = 2021 "
            "AND category = 10 AND bi_multi IN (1, 3, 7, 8) "
            "AND keywords LIKE '%#COVID-19%' AND provider IN "
            f"({', '.join(map(str, private.members[2021]))})"
        )
        .fetchall()
    )
    df = oda.get("one.crs.covid_oda", years=2021, by=["year"])

    assert df["value"].iloc[0] == pytest.approx(expected - private_value, rel=1e-9)
    assert df["value"].iloc[0] > 10_000


def test_bilateral_plus_multilateral_is_its_two_parts(tables: OecdTables) -> None:
    """Expected: oecd.crs.bilateral_oda plus oecd.multisystem.core_contributions
    for each DAC country, gross disbursements."""
    call = {"years": [2019, 2023], "providers": oda.groups.dac_countries}
    found = _by_provider_year(
        oda.get("one.crs.bilateral_plus_multilateral_oda", **call)
    )
    parts = [
        _by_provider_year(oda.get(part, **call))
        for part in ("oecd.crs.bilateral_oda", "oecd.multisystem.core_contributions")
    ]

    assert len(found) > 60
    for key, value in found.items():
        expected = sum(part.get(key, 0.0) for part in parts)
        assert value == pytest.approx(expected, rel=1e-9), key

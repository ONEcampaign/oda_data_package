"""DAC2A indicators through ``oda.get`` against the cached DAC2A table.

Every expected value comes from the same DAC2A release the indicators read,
queried here straight from the table view: the OECD's provider totals (20001
DAC countries, 20002 multilaterals, 20005 official donors, 20006 non-DAC
countries), its developing-countries total (10100), and its constant-price
rows. A revision by the OECD moves both sides at once.
"""

from collections.abc import Iterator

import pytest

import oda_data as oda
from oda_data._errors import OfflineCacheMissError
from oda_data._groups import groups
from oda_data._names import aggregate_codes, names
from oda_data._registry.dac2a import MULTILATERAL_AGGREGATES, MULTILATERAL_RECIPIENTS
from oda_data._tables import OecdTables

pytestmark = pytest.mark.data

DAC_TOTAL, MULTILATERAL_TOTAL, OFFICIAL_TOTAL, NON_DAC_TOTAL = (
    20001,
    20002,
    20005,
    20006,
)
DEVELOPING_COUNTRIES = 10100
FRANCE, SAUDI_ARABIA, EU_INSTITUTIONS = 4, 566, 918
#: Kenya, Ethiopia, Ukraine, Afghanistan, Bangladesh, and Israel, which sits in
#: Part II from 1997 to 2014.
RECIPIENTS = (248, 238, 85, 625, 666, 546)

type Cells = dict[tuple[int, int], float]


@pytest.fixture(scope="module")
def dac2a() -> Iterator[OecdTables]:
    tables = OecdTables(offline=True)
    try:
        tables.cursor(("dac2a",))
    except OfflineCacheMissError:
        pytest.skip("DAC2A is not cached; run scripts/warm_cache.py")
    yield tables
    tables.close()


def _cells(
    tables: OecdTables,
    *,
    line: int,
    providers: tuple[int, ...],
    recipients: tuple[int, ...] = RECIPIENTS,
    amount_type: str = "A",
) -> Cells:
    """Each (recipient, year) sum of one DAC2A line over ``providers``."""
    rows = (
        tables.cursor(("dac2a",))
        .sql(
            "SELECT recipient, year, sum(value) FROM dac2a "
            f"WHERE line = {line} AND amount_type = '{amount_type}' "
            f"AND provider IN ({', '.join(map(str, providers))}) "
            f"AND recipient IN ({', '.join(map(str, recipients))}) GROUP BY ALL"
        )
        .fetchall()
    )
    return {(recipient, year): value for recipient, year, value in rows}


def _codes(name: str) -> tuple[int, ...]:
    return tuple(code for code in groups[name].members if isinstance(code, int))


def _found(df) -> Cells:
    return {
        (int(recipient), year): value
        for recipient, year, value in zip(
            df["recipient"], df["year"], df["value"], strict=True
        )
    }


def _distinct(tables: OecdTables, column: str, where: str = "TRUE") -> set[int]:
    rows = tables.cursor(("dac2a",)).sql(
        f"SELECT DISTINCT {column} FROM dac2a WHERE {where}"
    )
    return {value for (value,) in rows.fetchall()}


def _official_expected(tables: OecdTables, *, line: int, years: range) -> Cells:
    """The provider composition of 20005 (official donors): 20005 less the
    non-DAC money no provider row holds.

    20005 is 20001 + 20002 + 20006: DAC countries, multilaterals, and non-DAC
    countries, the providers ``default_providers=None`` reads. 20006 also
    carries non-DAC amounts that the OECD attributes to no reporting provider
    (USD 5.0bn for Egypt in 2021, under USD 8m a year for Kenya), so the
    comparison takes 20006's excess over its members' own rows out.
    """
    official = _cells(tables, line=line, providers=(OFFICIAL_TOTAL,))
    non_dac = _cells(tables, line=line, providers=(NON_DAC_TOTAL,))
    members = _cells(tables, line=line, providers=_codes("non_dac_countries"))
    return {
        key: value - (non_dac.get(key, 0.0) - members.get(key, 0.0))
        for key, value in official.items()
        if key[1] in years
    }


def test_dac_countries_sum_to_the_dac_total_for_each_recipient(
    dac2a: OecdTables,
) -> None:
    """Expected: provider 20001's own line 206 rows for each recipient, in
    every year it has (1960-2024; DAC2A's 2025 is a partial year without
    provider totals). Israel's 1997-2014 rows are Part II."""
    expected = _cells(dac2a, line=206, providers=(DAC_TOTAL,))
    df = oda.get(
        "oecd.dac2a.recipient_oda",
        providers=oda.total(groups.dac_countries),
        recipients=list(RECIPIENTS),
        by=["provider", "recipient", "year"],
    )
    found = _found(df)

    assert min(year for _, year in expected) == 1960
    assert any(recipient == 546 and year == 2000 for recipient, year in expected)
    for key, value in expected.items():
        assert found[key] == pytest.approx(value, rel=1e-6, abs=0.01), key


def test_every_official_provider_sums_to_the_official_donors_total(
    dac2a: OecdTables,
) -> None:
    """Expected: 20005, the OECD's official donors total, less non-DAC money
    no provider row holds, for 1995-2024. Before 1995 20002 also holds
    multilateral money no provider row carries."""
    years = range(1995, 2025)
    expected = _official_expected(dac2a, line=206, years=years)
    df = oda.get(
        "oecd.dac2a.recipient_oda",
        years=years,
        recipients=list(RECIPIENTS),
        by=["recipient", "year"],
    )
    found = _found(df)
    private = _cells(dac2a, line=206, providers=_codes("private_donors"))

    assert sorted(found) == sorted(expected)
    assert any(private.get(key) for key in expected)  # private donors stay out
    for key, value in expected.items():
        assert found[key] == pytest.approx(value, rel=1e-6, abs=0.01), key


def test_official_donors_gap_is_saudi_arabias_unallocated_oda(
    dac2a: OecdTables,
) -> None:
    """Expected: on line 206 over every recipient, 20006's excess over the
    non-DAC countries' own rows equals Saudi Arabia's 10100 row less its
    recipient rows, USD 0.6bn to 6.4bn a year in 2015-2024 (the caveat on the
    DAC2A indicators). A release that closes the gap fails here."""
    years = range(2015, 2025)
    df = oda.get(
        "oecd.dac2a.recipient_oda", years=years, providers=SAUDI_ARABIA, by=["year"]
    )
    found = dict(zip(df["year"], df["value"], strict=True))
    recipients = tuple(_distinct(dac2a, "recipient", f"year IN {tuple(years)}"))
    non_dac = _cells(dac2a, line=206, providers=(NON_DAC_TOTAL,), recipients=recipients)
    members = _cells(
        dac2a, line=206, providers=_codes("non_dac_countries"), recipients=recipients
    )
    saudi = _cells(
        dac2a, line=206, providers=(SAUDI_ARABIA,), recipients=(DEVELOPING_COUNTRIES,)
    )
    totals = aggregate_codes("recipient") | set(MULTILATERAL_RECIPIENTS)

    for year in years:
        excess = sum(
            value - members.get(key, 0.0)
            for key, value in non_dac.items()
            if key[1] == year and key[0] not in totals
        )
        gap = saudi[(DEVELOPING_COUNTRIES, year)] - found[year]
        assert 500 < gap < 7_000, year
        assert gap == pytest.approx(excess, abs=1.0), year


def test_each_provider_over_every_recipient_equals_its_developing_countries_total(
    dac2a: OecdTables,
) -> None:
    """Expected: each default provider's own 10100 row (Developing Countries,
    Total) in every year without Part II rows: 1960-1989 and 2018-2024.

    With no recipient named, the indicator drops the OECD's recipient totals
    and the multilateral organisations listed as recipients (10150), so what
    remains is the recipient breakdown of 10100. Part II countries (1990-2017)
    count in the indicator and sit outside 10100, so those years are left out.
    Saudi Arabia's 10100 row also holds ODA it reports by no recipient (see
    the official donors gap test), so it is left out too.
    """
    years = [*range(1960, 1990), *range(2018, 2025)]
    df = oda.get("oecd.dac2a.recipient_oda", years=years, by=["provider", "year"])
    found = {
        (int(provider), year): value
        for provider, year, value in zip(
            df["provider"], df["year"], df["value"], strict=True
        )
        if int(provider) != SAUDI_ARABIA
    }
    providers = tuple(provider for provider, _ in found)
    expected = {
        (provider, year): value
        for (provider, year), value in _provider_cells(
            dac2a, line=206, recipient=DEVELOPING_COUNTRIES, providers=providers
        ).items()
        if year in years
    }

    assert len({provider for provider, _ in found}) > 80
    for key, value in found.items():
        assert value == pytest.approx(expected.get(key, 0.0), rel=1e-6, abs=0.05), key


def _provider_cells(
    tables: OecdTables, *, line: int, recipient: int, providers: tuple[int, ...]
) -> dict[tuple[int, int], float]:
    """Each (provider, year) value of one DAC2A line to one recipient."""
    rows = tables.cursor(("dac2a",)).sql(
        "SELECT provider, year, sum(value) FROM dac2a "
        f"WHERE line = {line} AND amount_type = 'A' AND recipient = {recipient} "
        f"AND provider IN ({', '.join(map(str, set(providers)))}) GROUP BY ALL"
    )
    return {(provider, year): value for provider, year, value in rows.fetchall()}


def test_every_multilateral_provider_in_dac2a_is_in_the_multilateral_group(
    dac2a: OecdTables,
) -> None:
    """Imputed shares exclude ``oda.groups.multilateral``. Every other DAC2A
    provider below 20000 is a DAC member, a non-DAC country, or a private
    donor, so no multilateral escapes the exclusion."""
    countries_and_private = {
        *_codes("dac_countries"),
        *_codes("non_dac_countries"),
        *_codes("private_donors"),
    }
    outside = _distinct(dac2a, "provider", "provider < 20000") - set(
        _codes("multilateral")
    )

    assert outside
    assert outside <= countries_and_private


def test_aggregates_left_out_of_imputed_shares_hold_eu_institutions(
    dac2a: OecdTables,
) -> None:
    """On line 106 to 10100, 20002 (multilaterals) is EU Institutions' row,
    20004 (DAC EU countries and EU Institutions) is 20011 (DAC EU countries)
    plus it, and 20005 (official donors) is 20001 + 20002 + 20006. The other
    provider totals hold countries only: 20001 (DAC countries), 20003 (G7),
    20006 (non-DAC countries), 20007 (other countries, 1970-1993), and 20011
    (DAC EU countries)."""
    codes = (EU_INSTITUTIONS, 20001, 20002, 20004, 20005, 20006, 20011)
    rows = _provider_cells(
        dac2a, line=106, recipient=DEVELOPING_COUNTRIES, providers=codes
    )
    years = sorted({year for (provider, year) in rows if provider == EU_INSTITUTIONS})
    aggregates = _distinct(dac2a, "provider", "provider >= 20000 AND line = 106")

    assert aggregates == {20001, 20002, 20003, 20004, 20005, 20006, 20007, 20011}
    assert set(MULTILATERAL_AGGREGATES) == {20002, 20004, 20005}
    assert years
    for year in years:
        cell = {code: rows.get((code, year), 0.0) for code in codes}
        assert cell[20002] == pytest.approx(cell[EU_INSTITUTIONS], abs=0.01), year
        assert cell[20004] == pytest.approx(
            cell[20011] + cell[EU_INSTITUTIONS], abs=0.01
        ), year
        assert cell[20005] == pytest.approx(
            cell[20001] + cell[20002] + cell[20006], abs=0.05
        ), year


def test_every_dac2a_line_and_recipient_has_a_name(dac2a: OecdTables) -> None:
    assert _distinct(dac2a, "line") <= set(names("line", table="dac2a"))
    assert _distinct(dac2a, "recipient") <= set(names("recipient"))


def test_imputed_shares_leave_out_multilateral_providers(dac2a: OecdTables) -> None:
    """Expected: for DAC countries, 20001's line 106 rows; for every provider,
    20005 less 20002 (multilaterals, whose line 106 rows are EU Institutions'
    own imputed shares) less non-DAC money no provider row holds."""
    years = range(2015, 2025)
    dac = oda.get(
        "oecd.dac2a.imputed_multilateral_oda",
        years=years,
        providers=oda.total(groups.dac_countries),
        recipients=list(RECIPIENTS),
        by=["provider", "recipient", "year"],
    )
    every = oda.get(
        "oecd.dac2a.imputed_multilateral_oda",
        years=years,
        recipients=list(RECIPIENTS),
        by=["recipient", "year"],
    )
    dac_total = _cells(dac2a, line=106, providers=(DAC_TOTAL,))
    multilateral = _cells(dac2a, line=106, providers=(MULTILATERAL_TOTAL,))
    official = _official_expected(dac2a, line=106, years=years)

    assert any(multilateral.get(key) for key in official)  # 918 has rows to leave out
    for key, value in _found(dac).items():
        assert value == pytest.approx(dac_total[key], rel=1e-6, abs=0.01), key
    found = _found(every)
    assert sorted(found) == sorted(official)
    for key, value in official.items():
        expected = value - multilateral.get(key, 0.0)
        assert found[key] == pytest.approx(expected, rel=1e-6, abs=0.01), key

    with pytest.raises(oda.UnsupportedQueryError):
        oda.get(
            "oecd.dac2a.imputed_multilateral_oda", years=2024, providers=EU_INSTITUTIONS
        )


def test_constant_prices_match_dac2a_constant_rows(dac2a: OecdTables) -> None:
    """Expected: DAC2A's own constant-price rows (amount type D) of France's
    line 206 to each recipient, whose base year is the one year in which
    France's current and constant rows agree."""
    current = _cells(dac2a, line=206, providers=(FRANCE,))
    constant = _cells(dac2a, line=206, providers=(FRANCE,), amount_type="D")
    (base_year,) = {
        year
        for key, value in constant.items()
        if (year := key[1]) >= 2000 and value == pytest.approx(current[key])
    }
    years = range(2015, base_year + 1)

    df = oda.get(
        "oecd.dac2a.recipient_oda",
        years=years,
        providers=FRANCE,
        recipients=list(RECIPIENTS),
        by=["recipient", "year"],
        unit=oda.constant("USD", base_year),
    )

    found = _found(df)
    assert set(df["base_year"]) == {base_year}
    assert sorted(found) == sorted(key for key in constant if key[1] in years)
    for key, value in found.items():
        assert value == pytest.approx(constant[key], rel=1e-6, abs=0.01), key


def test_a_sum_over_providers_warns_of_the_partial_last_year(
    dac2a: OecdTables,
) -> None:
    """Expected: DAC2A's 2025 holds a few providers' rows only, so a sum over
    the default providers warns, and explain() names the year and the count."""
    q = oda.query("oecd.dac2a.recipient_oda", years=[2024, 2025], by=["year"])
    with pytest.warns(
        oda.OdaDataWarning,
        match=r"All of 2025 is left out of the sum for \d+ providers",
    ):
        df = q.to_pandas()
    reported = _distinct(dac2a, "provider", "year = 2025 AND provider < 20000")

    value = dict(zip(df["year"], df["value"], strict=True))
    assert value[2025] < value[2024] / 50
    [line] = [line for line in q.explain().splitlines() if "is partial" in line]
    count = int(line.split()[3])
    assert count > 5 * len(reported)

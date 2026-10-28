"""CRS indicators through ``oda.get`` and ``oda.records`` against the cached CRS.

DAC1 is the reference for bilateral ODA: the OECD's DAC total (provider
20001) publishes bilateral ODA as line 1015 (gross disbursements under flow
1120, net disbursements under flow 1140) and line 11015 (grant equivalents
under flow 1160). The CRS rows of DAC countries add up to it on the A3 rules.
The policy-marker, partial-year and ``records()`` checks compare the
indicators with the CRS view itself.
"""

from collections.abc import Iterator

import pytest

import oda_data as oda
from oda_data._errors import OfflineCacheMissError
from oda_data._names import names
from oda_data._resolve import resolve_group
from oda_data._tables import OecdTables

pytestmark = pytest.mark.data

DAC_TOTAL = 20001


@pytest.fixture(scope="module")
def tables() -> Iterator[OecdTables]:
    tables = OecdTables(offline=True)
    try:
        tables.cursor(("crs", "dac1"))
    except OfflineCacheMissError:
        pytest.skip("CRS or DAC1 is not cached; run scripts/warm_cache.py")
    yield tables
    tables.close()


def _dac_total(tables: OecdTables, *, line: int, flow: int, year: int) -> float:
    """Provider 20001's current USD value of one DAC1 line under one flow."""
    [(value,)] = (
        tables.cursor(("dac1",))
        .sql(
            "SELECT sum(value) FROM dac1 WHERE provider = ? AND line = ? "
            "AND flow = ? AND amount_type = 'A' AND year = ?",
            params=[DAC_TOTAL, line, flow, year],
        )
        .fetchall()
    )
    return value


def _crs_dac_total(year: int, basis: str) -> float:
    df = oda.get(
        "oecd.crs.bilateral_oda",
        years=year,
        providers=oda.total(oda.groups.dac_countries),
        by=["provider"],
        basis=basis,
    )
    assert len(df) == 1
    return float(df["value"].iloc[0])


@pytest.mark.parametrize("year", [2016, 2023])
def test_category_10_gross_disbursements_match_dac1_bilateral(
    tables: OecdTables, year: int
) -> None:
    """Expected: 20001's line 1015 under gross disbursements (1120). The gap
    is under $1m (rounding in the published rows)."""
    expected = _dac_total(tables, line=1015, flow=1120, year=year)

    assert _crs_dac_total(year, "gross_disbursement") == pytest.approx(
        expected, abs=1.0
    )


@pytest.mark.parametrize("year", [2016, 2023])
def test_net_disbursements_match_dac1_bilateral_net_flows(
    tables: OecdTables, year: int
) -> None:
    """Expected: 20001's line 1015 under net disbursements (1140). The CRS is
    $122m (0.12%) above DAC1 in 2016 and $490m (0.30%) above in 2023, so the
    test allows 0.5%. Leaving out repayments on rows with no disbursement put
    the CRS $6bn to $7bn (4% to 7%) above DAC1 in these years."""
    expected = _dac_total(tables, line=1015, flow=1140, year=year)

    assert _crs_dac_total(year, "net_disbursement") == pytest.approx(
        expected, rel=0.005
    )


def test_grant_equivalents_with_category_60_match_dac1_line_11015(
    tables: OecdTables,
) -> None:
    """Expected: 20001's line 11015 under grant equivalents (1160) in 2023,
    within $60m. Without category 60 the gap is about $2bn."""
    expected = _dac_total(tables, line=11015, flow=1160, year=2023)

    assert _crs_dac_total(2023, "grant_equivalent") == pytest.approx(expected, abs=60.0)


#: Modalities the OECD leaves out of bilateral allocable aid, the activities
#: it screens against policy markers: budget support, core contributions,
#: imputed student costs, debt relief, administrative costs, awareness, and
#: refugee and asylum-seeker costs in provider countries.
NOT_ALLOCABLE = (
    "A01",
    "B02",
    "E02",
    "F01",
    "G01",
    "H01",
    "H02",
    "H03",
    "H04",
    "H05",
    "H06",
)
#: The OECD's meaning of each raw policy-marker score.
SCORE_LABELS = {
    2: "principal",
    1: "significant",
    0: "not_targeted",
    None: "not_screened",
}


def test_gender_marker_scores_match_bilateral_allocable_crs_rows(
    tables: OecdTables,
) -> None:
    """Expected: 2023 gross disbursements of bilateral ODA from DAC countries
    in every modality the table holds except the OECD's non-allocable ones,
    grouped by the raw gender column. The modality list comes from the data,
    so a modality the registry does not name is counted here."""
    dac = oda.groups.dac_countries
    df = oda.get("oecd.crs.gender_marker", years=2023, providers=dac, by=["score"])
    found = dict(zip(df["score"], df["value"], strict=True))
    members = sorted(resolve_group(dac, "provider", years=(2023,)).members[2023])
    rows = (
        tables.cursor(("crs",))
        .sql(
            "SELECT gender, sum(disbursement) FROM crs "
            "WHERE year = 2023 AND category = 10 AND bi_multi IN (1, 3, 7, 8) "
            f"AND modality NOT IN ({', '.join(map(repr, NOT_ALLOCABLE))}) "
            f"AND provider IN ({', '.join(map(str, members))}) GROUP BY ALL"
        )
        .fetchall()
    )
    expected = {SCORE_LABELS[score]: value for score, value in rows}

    assert set(found) == set(expected) == set(SCORE_LABELS.values())
    for score, value in expected.items():
        assert found[score] == pytest.approx(value, rel=1e-9), score


def test_the_partial_last_year_shows_providers_but_no_group_total(
    tables: OecdTables,
) -> None:
    """Expected: the CRS's last year holds a few providers. The DAC total is
    null in it, because members that reported the year before have not yet
    reported, and each provider that did report shows its own rows."""
    cursor = tables.cursor(("crs",))
    [(last,)] = cursor.sql("SELECT max(year) FROM crs").fetchall()
    reported = dict(
        cursor.sql(
            "SELECT provider, sum(disbursement) FROM crs "
            "WHERE year = ? AND category = 10 AND bi_multi IN (1, 3, 7, 8) "
            "AND disbursement IS NOT NULL GROUP BY ALL",
            params=[last],
        ).fetchall()
    )
    dac = oda.groups.dac_countries
    members = resolve_group(dac, "provider", years=(last,)).members[last]
    reporters = sorted(set(reported) & members)

    group_total = oda.get(
        "oecd.crs.bilateral_oda",
        years=last,
        providers=oda.total(dac),
        by=["provider", "year"],
    )
    own_rows = oda.get(
        "oecd.crs.bilateral_oda", years=last, providers=reporters, by=["provider"]
    )

    assert 0 < len(reporters) < len(members) / 2
    assert len(group_total) == 1
    assert group_total["value"].isna().all()
    assert dict(zip(own_rows["provider"], own_rows["value"], strict=True)) == (
        pytest.approx({str(code): reported[code] for code in reporters}, rel=1e-9)
    )


def test_records_sum_to_the_matching_get(tables: OecdTables) -> None:
    """Expected: the rows behind France's 2023 bilateral ODA to basic health care (12220) add up
    to the ``get`` value, and carry the CRS's own project titles beside the
    package's channel names."""
    call = {"years": 2023, "providers": "FRA", "sectors": 12220}
    rows = oda.records("oecd.crs.bilateral_oda", columns=["project_title"], **call)
    value = oda.get("oecd.crs.bilateral_oda", **call)["value"]

    assert len(value) == 1
    assert len(rows) > 10
    assert {"project_title", "channel_name"} <= set(rows.columns)
    assert rows["project_title"].notna().any()
    assert rows["value"].sum() == pytest.approx(value.iloc[0], rel=1e-9)


@pytest.mark.parametrize("dimension", ["finance_type", "channel", "modality"])
def test_every_code_in_the_crs_has_a_name(tables: OecdTables, dimension: str) -> None:
    """Expected: every non-null code of the dimension in the cached CRS has a
    name, so ``add_names`` and name filters cover the table."""
    codes = {
        code
        for (code,) in tables.cursor(("crs",))
        .sql(f"SELECT DISTINCT {dimension} FROM crs WHERE {dimension} IS NOT NULL")
        .fetchall()
    }

    assert sorted(codes - set(names(dimension)), key=str) == []


def test_multilateral_outflows_have_no_grant_equivalent(tables: OecdTables) -> None:
    """Expected: no ``bi_multi`` 4 row carries a grant equivalent, which is why
    ``oecd.crs.multilateral_oda`` offers cash bases only, and EU Institutions
    (918) report their USD 40.0bn of 2023 outflows as bilateral (``bi_multi``
    1), so they sit in ``oecd.crs.bilateral_oda``."""
    cursor = tables.cursor(("crs",))
    [(rows, with_grant_equivalent)] = cursor.sql(
        "SELECT count(*), count(grant_equivalent) FROM crs WHERE bi_multi = 4"
    ).fetchall()
    eu = dict(
        cursor.sql(
            "SELECT bi_multi, sum(disbursement) FROM crs WHERE provider = 918 "
            "AND year = 2023 AND category = 10 GROUP BY bi_multi"
        ).fetchall()
    )

    assert rows > 0
    assert with_grant_equivalent == 0
    assert 4 not in eu
    assert round(eu[1] / 1000, 1) == 40.0
    with pytest.raises(oda.UnsupportedQueryError, match="grant_equivalent"):
        oda.get("oecd.crs.multilateral_oda", years=2023, basis="grant_equivalent")

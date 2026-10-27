"""Sector groups and sector names against the cached CRS.

The CRS carries its own classification beside each purpose code: the DAC5
sector code (``sector_code``, such as 122 for basic health). A broad sector
group must hold exactly the purpose codes the CRS files under its DAC5 codes.
"""

from collections.abc import Iterator

import pytest

import oda_data as oda
from oda_data._errors import OfflineCacheMissError
from oda_data._names import names
from oda_data._resolve import resolve_group
from oda_data._tables import OecdTables

pytestmark = pytest.mark.data

FRANCE = 4


@pytest.fixture(scope="module")
def crs() -> Iterator[OecdTables]:
    tables = OecdTables(offline=True)
    try:
        tables.cursor(("crs",))
    except OfflineCacheMissError:
        pytest.skip("CRS is not cached; run scripts/warm_cache.py")
    yield tables
    tables.close()


def _dac5(tables: OecdTables) -> dict[int, set[int]]:
    """Each purpose code in the CRS and the DAC5 sector codes it is filed under."""
    pairs = (
        tables.cursor(("crs",))
        .sql(
            "SELECT DISTINCT sector, sector_code FROM crs "
            "WHERE sector IS NOT NULL AND sector_code IS NOT NULL"
        )
        .fetchall()
    )
    found: dict[int, set[int]] = {}
    for purpose, sector in pairs:
        found.setdefault(purpose, set()).add(sector)
    return found


def test_every_crs_purpose_code_has_a_name(crs: OecdTables) -> None:
    assert set(_dac5(crs)) <= set(names("sector"))


@pytest.mark.parametrize(
    ("group", "dac5"),
    [("health", {120, 121, 122, 123, 130}), ("education", {110, 111, 112, 113, 114})],
)
def test_broad_sector_holds_the_purpose_codes_the_crs_files_under_it(
    crs: OecdTables, group: str, dac5: set[int]
) -> None:
    filed = {purpose for purpose, sectors in _dac5(crs).items() if sectors & dac5}
    members = resolve_group(oda.sectors[group], "sector", years=[2023]).members[2023]

    assert filed <= members
    assert all(_dac5(crs).get(code, dac5) & dac5 for code in members)


def test_a_sector_group_filter_returns_its_purpose_codes(crs: OecdTables) -> None:
    """Expected: France's 2023 bilateral ODA by purpose code, summed over the
    codes the CRS files under DAC5 health (120-130)."""
    health = {p for p, s in _dac5(crs).items() if s & {120, 121, 122, 123, 130}}
    by_sector = oda.get(
        "oecd.crs.bilateral_oda", years=2023, providers=FRANCE, by=["sector"]
    )
    expected = by_sector.loc[by_sector["sector"].astype(int).isin(health), "value"]

    found = oda.get(
        "oecd.crs.bilateral_oda",
        years=2023,
        providers=FRANCE,
        sectors=oda.sectors.health,
    )

    assert expected.sum() > 0
    assert found["value"].tolist() == [pytest.approx(expected.sum(), rel=1e-9)]

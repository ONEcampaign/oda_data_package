"""``one.crs.imputed_multilateral_oda`` against the cached CRS and Multisystem.

Conservation: the imputation spreads each provider's core contributions, so
its rows add up to ``oecd.multisystem.core_contributions`` for every provider,
channel and year. A contribution of exactly zero has no rows.

2.8.1: oda-data 2.8.1's ``imputed_multilateral_by_purpose(years=[year],
providers=[4, 12], crs=..., multisystem=...)``, run on frames read from the
same cached Parquet files (CRS release ``CRS_RELEASE``, Multisystem release
``MULTISYSTEM_RELEASE``), gave the totals and row counts in ``V2_8_1``. 3.0
matched it row for row: the same 131,952 rows, with values within 3e-14.
"""

from collections.abc import Iterator

import pandas as pd
import pytest

import oda_data as oda
from oda_data._errors import OfflineCacheMissError, ReleaseMismatchError

pytestmark = pytest.mark.data

INDICATOR = "one.crs.imputed_multilateral_oda"
CORE = "oecd.multisystem.core_contributions"
FRANCE, UK = 4, 12

CRS_RELEASE = "0x8DF2533665E7CB7"  # label v20261006
MULTISYSTEM_RELEASE = "0x8DF2522071A444B"

#: 2.8.1's money per year, provider and allocation status, in current USD
#: millions, and its row count per year and provider.
V2_8_1: dict[tuple[int, int, str], float] = {
    (2022, FRANCE, "imputed"): 6496.450020000014,
    (2022, FRANCE, "proxy"): 166.52656299999958,
    (2022, FRANCE, "unallocated"): 464.19000900000003,
    (2022, UK, "imputed"): 3387.4793140000083,
    (2022, UK, "proxy"): 296.18701400000066,
    (2022, UK, "unallocated"): 194.548223,
    (2023, FRANCE, "imputed"): 6057.604298000012,
    (2023, FRANCE, "proxy"): 146.24743200000006,
    (2023, FRANCE, "unallocated"): 388.65029899999996,
    (2023, UK, "imputed"): 6060.316684999968,
    (2023, UK, "proxy"): 451.31893099999945,
    (2023, UK, "unallocated"): 219.58801100000002,
}
V2_8_1_ROWS: dict[tuple[int, int], int] = {
    (2022, FRANCE): 33925,
    (2022, UK): 31097,
    (2023, FRANCE): 34346,
    (2023, UK): 32584,
}


@pytest.fixture(scope="module")
def ctx() -> Iterator[oda.Context]:
    context = oda.Context(offline=True)
    try:
        context.get(CORE, years=2023, providers=FRANCE)
        context.get(INDICATOR, years=2023, providers=FRANCE)
    except OfflineCacheMissError:
        pytest.skip("CRS or Multisystem is not cached; run scripts/warm_cache.py")
    yield context
    context.close()


@pytest.fixture(scope="module")
def pinned() -> Iterator[oda.Context]:
    context = oda.Context(
        offline=True, releases={"crs": CRS_RELEASE, "multisystem": MULTISYSTEM_RELEASE}
    )
    try:
        context.get(INDICATOR, years=2023, providers=FRANCE)
    except (OfflineCacheMissError, ReleaseMismatchError):
        pytest.skip(
            "the 2.8.1 golden needs CRS and Multisystem at the pinned releases; "
            "rerun 2.8.1 on the new release and update V2_8_1"
        )
    yield context
    context.close()


def _nonzero(values: pd.Series) -> pd.Series:
    return values[values != 0]


def _by(frame: pd.DataFrame, *columns: str) -> pd.Series:
    return frame.groupby(list(columns), observed=True)["value"].sum()


@pytest.mark.parametrize("year", [2022, 2023])
def test_imputation_conserves_each_providers_core_contributions_by_channel(
    ctx: oda.Context, year: int
) -> None:
    providers = [FRANCE, UK]
    imputed = ctx.records(INDICATOR, years=year, providers=providers)
    core = ctx.get(CORE, years=year, providers=providers, by=["provider", "channel"])

    spread = _by(imputed, "provider", "channel")
    contributed = _nonzero(core.set_index(["provider", "channel"])["value"])
    assert len(contributed) > 50
    pd.testing.assert_series_equal(
        spread.sort_index(), contributed.sort_index(), check_names=False, rtol=1e-9
    )


def test_imputation_conserves_every_provider_year(ctx: oda.Context) -> None:
    imputed = ctx.get(INDICATOR, by=["provider", "year"])
    core = ctx.get(
        CORE,
        providers=[int(code) for code in imputed["provider"].unique()],
        by=["provider", "year"],
    )

    spread = imputed.set_index(["provider", "year"])["value"].sort_index()
    contributed = _nonzero(core.set_index(["provider", "year"])["value"]).sort_index()
    pd.testing.assert_series_equal(spread, contributed, check_names=False, rtol=1e-9)


@pytest.mark.parametrize("year", [2022, 2023])
def test_imputation_matches_2_8_1_on_the_same_tables(
    pinned: oda.Context, year: int
) -> None:
    imputed = pinned.records(INDICATOR, years=year, providers=[FRANCE, UK])

    found = _by(imputed, "provider", "allocation_status")
    for (y, provider, status), expected in V2_8_1.items():
        if y == year:
            assert found[(str(provider), status)] == pytest.approx(expected, rel=1e-12)
    counts = imputed.groupby("provider", observed=True).size()
    assert counts.to_dict() == {
        str(provider): rows for (y, provider), rows in V2_8_1_ROWS.items() if y == year
    }

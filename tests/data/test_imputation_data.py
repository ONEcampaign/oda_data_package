"""``one.crs.imputed_multilateral_oda`` against the cached CRS and Multisystem.

Conservation: the imputation spreads each provider's core contributions, so
its rows add up to ``oecd.multisystem.core_contributions`` for every provider,
channel and year. A contribution of exactly zero has no rows.

``test_goldens.py`` compares the imputation with oda-data 2.8.2's
``imputed_multilateral_by_purpose`` on pinned CRS and Multisystem releases.
"""

from collections.abc import Iterator

import pandas as pd
import pytest

import oda_data as oda
from oda_data._errors import OfflineCacheMissError

pytestmark = pytest.mark.data

INDICATOR = "one.crs.imputed_multilateral_oda"
CORE = "oecd.multisystem.core_contributions"
FRANCE, UK = 4, 12


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

"""Conversion factors from the real DAC1 table in the oda-reader cache."""

import pytest

from oda_data._compile import converted
from oda_data._errors import OfflineCacheMissError
from oda_data._factors import DacFactors
from oda_data._model import ResolvedCall
from oda_data._tables import OecdTables
from oda_data._units import constant, current

pytestmark = pytest.mark.data

# France, the United States, EU Institutions, and IDA, which reports no DAC1
# total ODA of its own and so takes the DAC total's factors.
_PROVIDERS = (4, 302, 918, 905)

# Total ODA as grant equivalents, in current (A) and constant (D) USD.
_TOTAL_ODA = "line = 11010 AND flow = 1160"


@pytest.fixture
def tables():
    tables = OecdTables(offline=True)
    try:
        tables.cursor(("dac1",))
    except OfflineCacheMissError:
        pytest.skip("DAC1 is not cached; run scripts/warm_cache.py")
    yield tables
    tables.close()


def test_converts_dac1_total_oda_for_a_few_providers(tables):
    cursor = tables.cursor(("dac1",))
    rows = cursor.sql(
        "SELECT provider, year, sum(value) AS value FROM dac1 "
        f"WHERE {_TOTAL_ODA} AND amount_type = 'A' AND year IN (2022, 2023) "
        f"AND provider IN {(*_PROVIDERS[:3], 20001)} GROUP BY ALL"
    )
    # IDA's rows come from CRS, so give it DAC1's France values to convert.
    rows = rows.union(
        cursor.sql(
            "SELECT 905 AS provider, year, value FROM dac1 "
            f"WHERE {_TOTAL_ODA} AND amount_type = 'A' AND year IN (2022, 2023) "
            "AND provider = 4"
        )
    )
    call = ResolvedCall(
        basis="grant_equivalent",
        units=(current("USD"), current("EUR"), constant("USD", 2023)),
        by=("provider", "year"),
    )
    factors = DacFactors(tables)

    result, fallback = converted(cursor, rows, call, factors)
    values = {
        (provider, year, currency, base_year): value
        for provider, year, currency, base_year, value in result.select(
            "provider, year, currency, base_year, value"
        ).fetchall()
    }
    published = dict(
        cursor.execute(
            "SELECT year, sum(value) FILTER (WHERE amount_type = 'D') "
            "/ sum(value) FILTER (WHERE amount_type = 'A') FROM dac1 "
            f"WHERE {_TOTAL_ODA} AND provider = ? AND year IN (2022, 2023) "
            "GROUP BY year",
            [4],
        ).fetchall()
    )

    assert fallback == frozenset({905})
    assert factors.release() == tables.release("dac1")
    for provider in (4, 302, 918, 905):
        for year in (2022, 2023):
            usd = values[(provider, year, "USD", None)]
            # Current EUR uses one rate for every provider in a year.
            assert values[(provider, year, "EUR", None)] / usd == pytest.approx(
                values[(4, year, "EUR", None)] / values[(4, year, "USD", None)]
            )
    # France's constant-price factor follows its own published D / A ratio,
    # rebased to 2023, and IDA's follows the DAC total's.
    france = {
        y: values[(4, y, "USD", 2023)] / values[(4, y, "USD", None)]
        for y in (2022, 2023)
    }
    assert france[2023] == pytest.approx(1.0)
    assert france[2022] == pytest.approx(published[2022] / published[2023], rel=1e-4)
    for year in (2022, 2023):
        ida = values[(905, year, "USD", 2023)] / values[(905, year, "USD", None)]
        dac = values[(20001, year, "USD", 2023)] / values[(20001, year, "USD", None)]
        assert ida == pytest.approx(dac)

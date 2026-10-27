"""The Multisystem indicator in the registry, run on a small in-memory table.

Each case pins one Multisystem counting rule with rows built to break it:
the same contribution in constant prices, earmarked money sent through a
channel, a commitment beside each disbursement, a non-DAC provider, and EU
Institutions (918) beside an EU member. ``FakeFactors`` gives factor 1 for
current USD and a different factor per provider for any other unit.
"""

import re
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pytest

from oda_data import _registry
from oda_data._context import Context
from oda_data._errors import DimensionError, UnsupportedQueryError
from oda_data._factors import FACTOR_SCHEMA
from oda_data._model import VOCABULARY, FactorTable
from oda_data._provenance import Release
from oda_data._registry import multisystem
from oda_data._units import Unit, constant, current

USD = current("USD")
RELEASE = Release("multisystem-token", datetime(2026, 10, 27))
FRANCE, GERMANY, SAUDI_ARABIA, EU_INSTITUTIONS = 4, 5, 566, 918
IDA, UNICEF, EU_BUDGET = 44002, 41122, 42001

COLUMNS = tuple(column.name for column in VOCABULARY["multisystem"].columns)

#: France's core disbursement to IDA in 2023.
BASE = {
    "provider": FRANCE,
    "recipient": 9998,
    "year": 2023,
    "flow": 11,
    "modality": "B02",
    "sector": 99810,
    "channel": IDA,
    "flow_type": "Disbursements",
    "amount_type": "Current prices",
    "aid_to_or_thru": "Core contributions to",
    "value": 0.0,
}


def _row(**values: object) -> dict[str, object]:
    return BASE | values


ROWS = [
    # France 2023: core to IDA and UNICEF, and its commitment to IDA.
    _row(value=100.0),
    _row(channel=UNICEF, value=10.0),
    _row(flow_type="Commitments", value=150.0),
    # The same disbursement in constant prices.
    _row(amount_type="Constant prices", value=90.0),
    # Earmarked money France sends through UNICEF for a recipient.
    _row(channel=UNICEF, aid_to_or_thru="Contributions through", value=500.0),
    # Germany, a non-DAC provider, and EU Institutions.
    _row(provider=GERMANY, value=40.0),
    _row(provider=GERMANY, year=2022, value=30.0),
    _row(provider=SAUDI_ARABIA, value=7.0),
    _row(provider=EU_INSTITUTIONS, channel=UNICEF, value=3.0),
    _row(channel=EU_BUDGET, value=1000.0),
]

PROVIDER_FACTORS = {FRANCE: 2.0, GERMANY: 3.0, SAUDI_ARABIA: 1.0}


class FakeTables:
    def __init__(self, rows: list[dict[str, object]] = ROWS) -> None:
        self.connection = duckdb.connect()
        self.connection.register("rows", pd.DataFrame(rows, columns=list(COLUMNS)))
        select = ", ".join(
            f"CAST({c.name} AS {c.type}) AS {c.name}"
            for c in VOCABULARY["multisystem"].columns
        )
        self.connection.execute(
            f"CREATE TABLE multisystem AS SELECT {select} FROM rows"
        )
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


def _values(df: pd.DataFrame, *columns: str) -> dict[tuple, float]:
    return {
        tuple(row[:-1]): row[-1]
        for row in df[[*columns, "value"]].itertuples(index=False)
    }


def test_registry_maps_the_multisystem_indicators_by_id() -> None:
    assert [i.id for i in multisystem.INDICATORS] == [
        "oecd.multisystem.core_contributions",
        "one.crs.imputed_multilateral_oda",
    ]
    assert all(_registry.INDICATORS[i.id] is i for i in multisystem.INDICATORS)


# Core contributions in current prices, by channel -------------------------------


def test_core_contributions_count_core_rows_in_current_prices_only(
    ctx: Context,
) -> None:
    df = ctx.get(
        "oecd.multisystem.core_contributions",
        years=2023,
        providers=FRANCE,
        by=["channel"],
    )

    # Not the 90 in constant prices, nor the 500 sent through UNICEF.
    assert _values(df, "channel") == {
        (str(IDA),): 100.0,
        (str(UNICEF),): 10.0,
        (str(EU_BUDGET),): 1000.0,
    }


@pytest.mark.parametrize(
    ("basis", "expected"),
    [("gross_disbursement", 100.0), ("commitment", 150.0)],
)
def test_the_basis_picks_the_flow_type(
    ctx: Context, basis: str, expected: float
) -> None:
    df = ctx.get(
        "oecd.multisystem.core_contributions",
        years=2023,
        providers=FRANCE,
        channels=IDA,
        by=["year"],
        basis=basis,
    )

    assert df["value"].tolist() == [expected]


def test_the_table_has_no_net_or_grant_equivalent_basis(ctx: Context) -> None:
    with pytest.raises(UnsupportedQueryError, match="gross_disbursement"):
        ctx.get("oecd.multisystem.core_contributions", basis="net_disbursement")


def test_providers_default_to_dac_countries(ctx: Context) -> None:
    query = ctx.query("oecd.multisystem.core_contributions", years=2023)
    df = query.to_pandas()

    # Saudi Arabia and EU Institutions report, but are not DAC countries.
    assert _values(df, "provider") == {(str(FRANCE),): 1110.0, (str(GERMANY),): 40.0}
    assert query.provenance.defaults["providers"] == "dac_countries"


def test_a_named_non_dac_provider_counts(ctx: Context) -> None:
    df = ctx.get(
        "oecd.multisystem.core_contributions", years=2023, providers=SAUDI_ARABIA
    )

    assert df["value"].tolist() == [7.0]


def test_eu_institutions_summed_with_an_eu_member_raise(ctx: Context) -> None:
    with pytest.raises(UnsupportedQueryError, match="918"):
        ctx.get(
            "oecd.multisystem.core_contributions",
            years=2023,
            providers=[FRANCE, EU_INSTITUTIONS],
            by=["year"],
        )


def test_constant_prices_use_each_providers_factor(ctx: Context) -> None:
    df = ctx.get(
        "oecd.multisystem.core_contributions",
        years=2023,
        providers=[FRANCE, GERMANY],
        channels=IDA,
        by=["year"],
        unit=constant("USD", 2023),
    )

    assert df["value"].tolist() == [100.0 * 2.0 + 40.0 * 3.0]


def test_core_contributions_have_no_recipient_or_sector(ctx: Context) -> None:
    with pytest.raises(
        DimensionError, match=re.escape("one.crs.imputed_multilateral_oda")
    ):
        ctx.get("oecd.multisystem.core_contributions", by=["sector"])

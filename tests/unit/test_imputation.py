"""``one.crs.imputed_multilateral_oda`` on small in-memory CRS and Multisystem
tables.

The CRS rows are multilateral organisations' own spending, filed under real
reporters the bundled crosswalk maps: Nordic Development Fund (provider 104,
agency 1) to channel 47128 and IDA (905, agency 1) to 44002. The bundled
proxy table sends IDA-MDRI (44007) to IDA's shares and UN peacekeeping
(41310) to purpose code 15230. Channel 909090 has neither spending nor a
proxy. ``FakeFactors`` gives factor 1 for current USD and a different factor
per provider for any other unit.
"""

from collections.abc import Iterator
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pytest

from oda_data._context import Context
from oda_data._errors import ImputationConservationError, UnmappedChannelError
from oda_data._factors import FACTOR_SCHEMA
from oda_data._groups import Group, total
from oda_data._methods import channels, imputation
from oda_data._model import VOCABULARY, FactorTable, ResolvedCall
from oda_data._provenance import Release
from oda_data._registry.multisystem import CORE_CONTRIBUTIONS
from oda_data._units import Unit, constant, current

INDICATOR = "one.crs.imputed_multilateral_oda"
USD = current("USD")
RELEASE = Release("token", datetime(2026, 10, 27))
FRANCE, GERMANY, UK = 4, 5, 12
NDF, IDA = 104, 905  # CRS reporters
NDF_CHANNEL, IDA_CHANNEL, MDRI, PEACEKEEPING, NOWHERE = (
    47128,
    44002,
    44007,
    41310,
    909090,
)
KENYA, GHANA = 248, 238
HEALTH, EDUCATION, BANKING, OTHER = 12220, 11110, 24010, 99810

CRS_COLUMNS = {
    "provider": "BIGINT",
    "agency_code": "BIGINT",
    "donor_name": "VARCHAR",
    "agency_name": "VARCHAR",
    "recipient": "BIGINT",
    "year": "BIGINT",
    "category": "BIGINT",
    "bi_multi": "BIGINT",
    "sector": "BIGINT",
    "disbursement": "DOUBLE",
}


def spending(
    provider: int,
    sector: int,
    years: range | list[int],
    value: float,
    *,
    agency: int | None = 1,
    recipient: int = KENYA,
    category: int = 10,
    bi_multi: int = 4,
) -> list[dict[str, object]]:
    """One CRS row a multilateral files in each of ``years``."""
    return [
        {
            "provider": provider,
            "agency_code": agency,
            "donor_name": f"Reporter {provider}",
            "agency_name": f"Agency {agency}",
            "recipient": recipient,
            "year": year,
            "category": category,
            "bi_multi": bi_multi,
            "sector": sector,
            "disbursement": value,
        }
        for year in years
    ]


def core(
    provider: int,
    channel: int,
    year: int,
    value: float,
    **values: object,
) -> dict[str, object]:
    """A provider's core disbursement to a channel, in current prices."""
    return {
        "provider": provider,
        "recipient": 9998,
        "year": year,
        "flow": 11,
        "modality": "B02",
        "sector": OTHER,
        "channel": channel,
        "flow_type": "Disbursements",
        "amount_type": "Current prices",
        "aid_to_or_thru": "Core contributions to",
        "value": value,
    } | values


class FakeTables:
    def __init__(
        self, crs: list[dict[str, object]], multisystem: list[dict[str, object]]
    ) -> None:
        self.connection = duckdb.connect()
        self._create("crs", crs, CRS_COLUMNS)
        types = {c.name: c.type for c in VOCABULARY["multisystem"].columns}
        self._create("multisystem", multisystem, types)

    def _create(
        self, name: str, rows: list[dict[str, object]], types: dict[str, str]
    ) -> None:
        self.connection.register("rows", pd.DataFrame(rows, columns=list(types)))
        select = ", ".join(f"CAST({c} AS {t}) AS {c}" for c, t in types.items())
        self.connection.execute(f"CREATE TABLE {name} AS SELECT {select} FROM rows")
        self.connection.unregister("rows")

    def cursor(self, tables: tuple[str, ...]) -> duckdb.DuckDBPyConnection:
        return self.connection.cursor()

    def path(self, table: str) -> Path:
        raise NotImplementedError

    def release(self, table: str) -> Release:
        return RELEASE

    def close(self) -> None:
        self.connection.close()


PROVIDER_FACTORS = {FRANCE: 2.0, GERMANY: 3.0, UK: 5.0}


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


def context(
    crs: list[dict[str, object]], multisystem: list[dict[str, object]]
) -> Context:
    return Context._from_parts(FakeTables(crs, multisystem), FakeFactors())


def rows(
    crs: list[dict[str, object]],
    multisystem: list[dict[str, object]],
    **call: object,
) -> pd.DataFrame:
    """The imputation's rows for a call, through ``records``."""
    with context(crs, multisystem) as ctx:
        return ctx.records(INDICATOR, **call)


def listed(frame: pd.DataFrame, *columns: str) -> list[tuple]:
    """Rows as sorted tuples, with missing values as ``None``."""
    return sorted(
        (
            tuple(None if pd.isna(value) else value for value in row)
            for row in frame[list(columns)].itertuples(index=False)
        ),
        key=str,
    )


def totals(frame: pd.DataFrame) -> dict[tuple[str, str], float]:
    """Money per provider and channel."""
    summed = frame.groupby(["provider", "channel"], observed=True)["value"].sum()
    return {key: pytest.approx(value) for key, value in summed.items()}


# Allocation paths ------------------------------------------------------------


def test_own_shares_spread_a_contribution_by_sector_and_recipient() -> None:
    crs = [
        *spending(NDF, HEALTH, range(2018, 2023), 300.0),
        *spending(NDF, EDUCATION, range(2018, 2023), 100.0, recipient=GHANA),
    ]

    found = rows(crs, [core(FRANCE, NDF_CHANNEL, 2022, 1000.0)], years=2022)

    assert listed(
        found,
        "channel",
        "recipient",
        "sector",
        "allocation_status",
        "share_channel",
        "share_years",
        "value",
    ) == [
        (
            str(NDF_CHANNEL),
            str(GHANA),
            str(EDUCATION),
            "imputed",
            47128,
            "2020-2022",
            250.0,
        ),
        (
            str(NDF_CHANNEL),
            str(KENYA),
            str(HEALTH),
            "imputed",
            47128,
            "2020-2022",
            750.0,
        ),
    ]


def test_a_lapsed_reporter_takes_its_latest_window_as_a_stale_share() -> None:
    crs = spending(NDF, HEALTH, range(2010, 2016), 100.0)

    found = rows(crs, [core(FRANCE, NDF_CHANNEL, 2019, 1000.0)], years=2019)

    assert listed(found, "allocation_status", "share_years", "value") == [
        ("stale_share", "2013-2015", 1000.0)
    ]


def test_a_window_older_than_the_maximum_age_gives_no_share() -> None:
    crs = spending(NDF, HEALTH, range(2008, 2014), 100.0)

    found = rows(crs, [core(FRANCE, NDF_CHANNEL, 2019, 1000.0)], years=2019)

    # 2013 ends six years before 2019, one more than MAX_SHARE_AGE allows.
    assert listed(found, "allocation_status", "sector", "value") == [
        ("unallocated", None, 1000.0)
    ]


def test_a_parent_fund_proxy_lends_its_shares_and_keeps_the_channel() -> None:
    crs = spending(IDA, BANKING, range(2018, 2023), 100.0)

    found = rows(crs, [core(FRANCE, MDRI, 2022, 7000.0)], years=2022)

    assert listed(
        found, "channel", "share_channel", "sector", "allocation_status", "value"
    ) == [(str(MDRI), IDA_CHANNEL, str(BANKING), "proxy", 7000.0)]


def test_own_stale_shares_win_over_a_proxy(monkeypatch: pytest.MonkeyPatch) -> None:
    # MDRI has a proxy row, but here it also files CRS spending of its own.
    text = (
        "provider_code,agency_code,channel_code,status,reviewed\n"
        f"{NDF},1,{MDRI},mapped,true\n"
    )
    monkeypatch.setattr(channels, "crosswalk", lambda: channels.crosswalk_table(text))
    crs = spending(NDF, EDUCATION, range(2010, 2016), 100.0)

    found = rows(crs, [core(FRANCE, MDRI, 2019, 1000.0)], years=2019)

    assert listed(found, "allocation_status", "share_channel", "sector") == [
        ("stale_share", MDRI, str(EDUCATION))
    ]


def test_a_fixed_purpose_proxy_puts_the_whole_amount_in_one_sector() -> None:
    found = rows([], [core(GERMANY, PEACEKEEPING, 2022, 500.0)], years=2022)

    assert listed(
        found, "recipient", "sector", "allocation_status", "share_channel", "value"
    ) == [(None, "15230", "proxy", None, 500.0)]


def test_no_share_and_no_proxy_leaves_the_whole_amount_unallocated() -> None:
    found = rows([], [core(UK, NOWHERE, 2022, 250.0)], years=2022)

    assert listed(
        found,
        "recipient",
        "sector",
        "allocation_status",
        "share_channel",
        "share_years",
        "value",
    ) == [(None, None, "unallocated", None, None, 250.0)]


# Conservation ----------------------------------------------------------------


def test_every_contribution_is_conserved_across_statuses() -> None:
    crs = [
        *spending(NDF, HEALTH, range(2018, 2023), 100.0),
        *spending(NDF, EDUCATION, range(2018, 2023), 30.0, recipient=GHANA),
        *spending(IDA, BANKING, range(2018, 2023), 100.0),
    ]
    multisystem = [
        core(FRANCE, NDF_CHANNEL, 2022, 1000.0),  # imputed
        core(FRANCE, MDRI, 2022, 7000.0),  # parent fund proxy
        core(GERMANY, PEACEKEEPING, 2022, 500.0),  # fixed purpose proxy
        core(UK, NOWHERE, 2022, 250.0),  # unallocated
    ]

    found = rows(crs, multisystem, years=2022)

    assert totals(found) == {
        (str(FRANCE), str(NDF_CHANNEL)): 1000.0,
        (str(FRANCE), str(MDRI)): 7000.0,
        (str(GERMANY), str(PEACEKEEPING)): 500.0,
        (str(UK), str(NOWHERE)): 250.0,
    }
    assert set(found["allocation_status"]) == {"imputed", "proxy", "unallocated"}


def test_a_channel_that_spends_only_other_official_flows_is_imputed() -> None:
    crs = spending(NDF, BANKING, range(2018, 2023), 500.0, category=21)

    found = rows(crs, [core(FRANCE, NDF_CHANNEL, 2022, 1000.0)], years=2022)

    assert listed(found, "allocation_status", "value") == [("imputed", 1000.0)]


@pytest.mark.parametrize(
    "decoy",
    [
        pytest.param({"bi_multi": 2}, id="core-contribution-to-another-multilateral"),
        pytest.param({"category": 60}, id="private-sector-instrument"),
    ],
)
def test_shares_leave_out_rows_outside_oda_and_oof_spending(
    decoy: dict[str, object],
) -> None:
    crs = [
        *spending(NDF, HEALTH, range(2018, 2023), 100.0),
        *[row | decoy for row in spending(NDF, EDUCATION, range(2018, 2023), 900.0)],
    ]

    found = rows(crs, [core(FRANCE, NDF_CHANNEL, 2022, 1000.0)], years=2022)

    assert listed(found, "sector", "value") == [(str(HEALTH), 1000.0)]


def test_check_conservation_passes_within_tolerance() -> None:
    contributions = _keys([(2022, FRANCE, NDF_CHANNEL, 1000.0)])
    allocated = _keys([(2022, FRANCE, NDF_CHANNEL, 1000.0 + 1e-7)])

    with duckdb.connect() as cursor:
        imputation.check_conservation(cursor, allocated, contributions)


@pytest.mark.parametrize(
    "allocated",
    [
        pytest.param([(2022, FRANCE, NDF_CHANNEL, 900.0)], id="money-dropped"),
        pytest.param([], id="key-missing"),
        pytest.param(
            [(2022, FRANCE, NDF_CHANNEL, 1000.0), (2022, UK, NOWHERE, 5.0)],
            id="money-from-nowhere",
        ),
    ],
)
def test_check_conservation_raises_when_money_is_lost_or_added(
    allocated: list[tuple[int, int, int, float]],
) -> None:
    contributions = _keys([(2022, FRANCE, NDF_CHANNEL, 1000.0)])

    with duckdb.connect() as cursor, pytest.raises(ImputationConservationError):
        imputation.check_conservation(cursor, _keys(allocated), contributions)


def test_check_conservation_with_no_contributions_passes() -> None:
    with duckdb.connect() as cursor:
        imputation.check_conservation(cursor, _keys([]), _keys([]))


def _keys(values: list[tuple[int, int, int, float]]) -> pa.Table:
    names = ("year", "provider", "channel", "value")
    types = (pa.int64(), pa.int64(), pa.int64(), pa.float64())
    return pa.table(
        {
            name: pa.array([row[i] for row in values], kind)
            for i, (name, kind) in enumerate(zip(names, types, strict=True))
        }
    )


# Zero and negative rows ------------------------------------------------------

#: NDF spends on health every year, and on banking only in 2011, which a read
#: for 2018-2022 reaches (padded back seven years to 2011) and a read for 2020
#: alone does not (padded to 2013).
_HISTORY = [
    *spending(NDF, HEALTH, range(2005, 2023), 300.0),
    *spending(NDF, BANKING, [2011], 900.0),
]


def test_one_year_equals_that_year_of_a_longer_call() -> None:
    columns = ("provider", "channel", "recipient", "sector", "value")
    single = rows(_HISTORY, [core(FRANCE, NDF_CHANNEL, 2020, 1000.0)], years=2020)
    longer = rows(
        _HISTORY,
        [core(FRANCE, NDF_CHANNEL, year, 1000.0) for year in range(2018, 2023)],
        years=range(2018, 2023),
    )

    assert listed(single, *columns) == listed(longer[longer.year == 2020], *columns)
    assert listed(single, "sector", "value") == [(str(HEALTH), 1000.0)]


def test_no_row_is_exactly_zero() -> None:
    found = rows(
        _HISTORY,
        [core(FRANCE, NDF_CHANNEL, year, 1000.0) for year in range(2018, 2023)]
        + [core(GERMANY, NOWHERE, 2022, 0.0)],
        years=range(2018, 2023),
    )

    assert (found["value"] != 0).all()
    assert str(GERMANY) not in set(found["provider"])


def test_a_reversal_in_a_channels_spending_is_a_negative_row() -> None:
    crs = [
        *spending(NDF, HEALTH, range(2018, 2021), 400.0),
        *spending(NDF, EDUCATION, range(2018, 2021), -100.0),
    ]

    found = rows(crs, [core(FRANCE, NDF_CHANNEL, 2020, 1000.0)], years=2020)

    assert listed(found, "sector", "value") == [
        (str(EDUCATION), pytest.approx(-1000.0 / 3)),
        (str(HEALTH), pytest.approx(4000.0 / 3)),
    ]


# Crosswalk -------------------------------------------------------------------


def test_an_unmapped_reporter_with_spending_raises_naming_it() -> None:
    crs = [
        *spending(NDF, HEALTH, range(2018, 2023), 100.0),
        *spending(IDA, HEALTH, [2021], 42.0, agency=77),
    ]

    with pytest.raises(UnmappedChannelError, match=r"provider 905, agency 77 .*42\.00"):
        rows(crs, [core(FRANCE, NDF_CHANNEL, 2022, 1000.0)], years=2022)


def test_an_unmapped_reporter_with_no_spending_is_ignored() -> None:
    crs = [
        *spending(NDF, HEALTH, range(2018, 2023), 100.0),
        *spending(IDA, HEALTH, [2021], 0.0, agency=77),
    ]

    found = rows(crs, [core(FRANCE, NDF_CHANNEL, 2022, 1000.0)], years=2022)

    assert listed(found, "sector", "value") == [(str(HEALTH), 1000.0)]


def test_an_excluded_reporter_leaves_the_shares() -> None:
    # EU Macro-Financial Assistance (918, agency 5) is excluded in the crosswalk.
    crs = [
        *spending(NDF, HEALTH, range(2018, 2023), 100.0),
        *spending(918, EDUCATION, range(2018, 2023), 5000.0, agency=5),
    ]

    found = rows(crs, [core(FRANCE, NDF_CHANNEL, 2022, 1000.0)], years=2022)

    assert listed(found, "sector", "value") == [(str(HEALTH), 1000.0)]


def test_a_reporter_with_no_agency_joins_the_crosswalk_row_with_none() -> None:
    # EU Institutions with no agency map to channel 42001.
    crs = spending(918, HEALTH, range(2018, 2023), 100.0, agency=None)

    found = rows(crs, [core(FRANCE, 42001, 2022, 1000.0)], years=2022)

    assert listed(found, "allocation_status", "share_channel") == [("imputed", 42001)]


# The engine around the method ------------------------------------------------

_MIXED_CRS = [
    *spending(NDF, HEALTH, range(2018, 2023), 300.0),
    *spending(NDF, EDUCATION, range(2018, 2023), 100.0, recipient=GHANA),
]
_MIXED_CORE = [
    core(FRANCE, NDF_CHANNEL, 2022, 1000.0),
    core(GERMANY, NDF_CHANNEL, 2022, 100.0),
    core(GERMANY, NOWHERE, 2022, 50.0),
    # Not core contributions in current prices, so never imputed.
    core(FRANCE, NDF_CHANNEL, 2022, 900.0, amount_type="Constant prices"),
    core(FRANCE, NDF_CHANNEL, 2022, 800.0, aid_to_or_thru="Contributions through"),
    core(FRANCE, NDF_CHANNEL, 2022, 2000.0, flow_type="Commitments"),
]


@pytest.fixture
def mixed() -> Iterator[Context]:
    with context(_MIXED_CRS, _MIXED_CORE) as ctx:
        yield ctx


def _values(df: pd.DataFrame, *columns: str) -> dict[tuple, float]:
    return {row[:-1]: pytest.approx(row[-1]) for row in listed(df, *columns, "value")}


def test_results_split_by_sector_and_allocation_status(mixed: Context) -> None:
    df = mixed.get(INDICATOR, years=2022, by=["sector", "allocation_status"])

    assert _values(df, "sector", "allocation_status") == {
        (str(EDUCATION), "imputed"): 275.0,
        (str(HEALTH), "imputed"): 825.0,
        (None, "unallocated"): 50.0,
    }


def test_commitments_read_the_commitment_rows(mixed: Context) -> None:
    df = mixed.get(INDICATOR, years=2022, by=["provider"], basis="commitment")

    assert _values(df, "provider") == {(str(FRANCE),): 2000.0}


def test_a_recipient_filter_leaves_out_unallocated_money(mixed: Context) -> None:
    df = mixed.get(INDICATOR, years=2022, recipients=KENYA, by=["provider"])

    assert _values(df, "provider") == {(str(FRANCE),): 750.0, (str(GERMANY),): 75.0}


def test_rows_convert_with_each_providers_factor_before_the_sum(
    mixed: Context,
) -> None:
    df = mixed.get(
        INDICATOR, years=2022, by=["broad_sector"], unit=constant("USD", 2022)
    )

    assert _values(df, "broad_sector") == {
        ("education",): 250.0 * 2.0 + 25.0 * 3.0,
        ("health",): 750.0 * 2.0 + 75.0 * 3.0,
        (None,): 50.0 * 3.0,
    }


def test_a_group_total_sums_its_members(mixed: Context) -> None:
    pair = Group("pair", [FRANCE, GERMANY])
    df = mixed.get(INDICATOR, years=2022, providers=[pair, total(pair)])

    assert _values(df, "provider") == {
        (str(FRANCE),): 1000.0,
        (str(GERMANY),): 150.0,
        ("pair",): 1150.0,
    }


def test_records_carry_the_share_window(mixed: Context) -> None:
    found = mixed.records(INDICATOR, years=2022, providers=FRANCE)

    assert set(found.columns) >= {"share_channel", "share_years", "allocation_status"}
    assert found["value"].sum() == pytest.approx(1000.0)
    assert set(found["share_years"]) == {"2020-2022"}


def test_method_output_follows_its_schema(mixed: Context) -> None:
    call = ResolvedCall(
        basis="gross_disbursement",
        units=(USD,),
        by=("provider", "year"),
        years=(2022,),
        members={"provider": frozenset({FRANCE, GERMANY})},
    )
    cursor = mixed._tables.cursor(("multisystem", "crs"))

    table = imputation.imputed_rows(cursor, call, core=CORE_CONTRIBUTIONS)

    assert table.schema == imputation.OUTPUT_SCHEMA
    assert set(table["allocation_status"].to_pylist()) == {"imputed", "unallocated"}

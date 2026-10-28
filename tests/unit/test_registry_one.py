"""ONE's indicators in the registry, run on small in-memory DAC1, DAC2A and CRS tables.

Each case pins one rule with rows built to break it: decoy lines under other
flows and amount types, a member that drops a detail line in the last year,
multilateral providers beside DAC2A line 106, contributions to EU
Institutions (918) from members and from a non-member, an EU member with no
GNI, and CRS keywords that hold the COVID-19 tag inside another tag.
``FakeFactors`` gives factor 1 for current USD and a different factor per
provider for any other unit.
"""

import warnings
from collections.abc import Iterator, Sequence
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pytest

from oda_data import _registry
from oda_data._context import Context
from oda_data._errors import OdaDataWarning, UnsupportedQueryError
from oda_data._factors import FACTOR_SCHEMA
from oda_data._groups import Group, total
from oda_data._model import VOCABULARY, FactorTable
from oda_data._names import eu_members
from oda_data._provenance import Release
from oda_data._registry import one
from oda_data._units import Unit, constant, current

USD = current("USD")
RELEASE = Release("token", datetime(2026, 10, 12))
FRANCE, GERMANY, UK, NORWAY, EU_INSTITUTIONS, IDA = 4, 5, 12, 8, 918, 905
CYPRUS = 30
KENYA = 248

#: The factor each provider takes for any unit other than current USD.
PROVIDER_FACTORS = {FRANCE: 2.0, GERMANY: 3.0}

DAC1_COLUMNS = "provider, year, line, flow, amount_type, value"
DAC2A_COLUMNS = "provider, recipient, year, line, part, amount_type, value"
MULTISYSTEM_COLUMNS = (
    "provider, channel, year, flow_type, amount_type, aid_to_or_thru, value"
)

# Core ODA: France and Germany, 2017-2019; 2019 is the table's last year.
CORE_ROWS = [
    # provider, year, line, flow, amount type, value
    (FRANCE, 2017, 1010, 1140, "A", 100.0),
    (FRANCE, 2017, 1820, 1140, "A", 10.0),
    (FRANCE, 2017, 1820, 1140, "N", 777.0),  # national currency
    (FRANCE, 2017, 1500, 1140, "A", 5.0),
    (FRANCE, 2017, 1600, 1140, "A", 2.0),
    (FRANCE, 2018, 11010, 1160, "A", 120.0),
    (FRANCE, 2018, 1010, 1160, "A", 999.0),  # line 1010 under grant equivalents
    (FRANCE, 2018, 1820, 1160, "A", 12.0),
    (FRANCE, 2018, 1820, 1140, "A", 50.0),  # net flows, off the 2018 headline
    (FRANCE, 2018, 1500, 1160, "A", 6.0),
    (FRANCE, 2019, 11010, 1160, "A", 130.0),
    (FRANCE, 2019, 1820, 1160, "A", 13.0),
    (FRANCE, 2019, 1500, 1160, "A", 7.0),
    (GERMANY, 2017, 1010, 1140, "A", 200.0),
    (GERMANY, 2018, 11010, 1160, "A", 220.0),
    (GERMANY, 2018, 1820, 1160, "A", 20.0),
    (GERMANY, 2019, 11010, 1160, "A", 230.0),  # no refugee costs reported yet
]


def _eu_rows() -> list[tuple]:
    """GNI for every EU member, ever, in 2019, 2021 and 2022, except Germany in
    2022, the table's last year, and Cyprus in 2021; 918's total ODA; and line
    2102 from France, Germany and Norway."""
    gni = [
        (code, year, 1, 1140, "A", 1000.0 + code)
        for code in sorted(eu_members(None))
        for year in (2019, 2021, 2022)
        if (code, year) not in {(GERMANY, 2022), (CYPRUS, 2021)}
    ]
    return [
        *gni,
        (EU_INSTITUTIONS, 2019, 1010, 1140, "A", 50.0),
        (EU_INSTITUTIONS, 2019, 11010, 1160, "A", 40.0),
        (EU_INSTITUTIONS, 2021, 1010, 1140, "A", 60.0),
        (FRANCE, 2019, 1010, 1140, "A", 300.0),
        (FRANCE, 2019, 11010, 1160, "A", 310.0),
        (GERMANY, 2019, 1010, 1140, "A", 400.0),
        (FRANCE, 2019, 2102, 1140, "A", 10.0),
        (FRANCE, 2019, 2102, 1160, "A", 9.0),
        (FRANCE, 2019, 2102, 1140, "D", 900.0),  # constant prices
        (GERMANY, 2019, 2102, 1140, "A", 20.0),
        (NORWAY, 2019, 2102, 1140, "A", 1.0),  # outside the EU, pays 918
        (FRANCE, 2021, 2102, 1140, "A", 11.0),
    ]


DAC2A_ROWS = [
    # provider, recipient, year, line, part, amount type, value
    (FRANCE, KENYA, 2023, 206, 1, "A", 10.0),
    (FRANCE, KENYA, 2023, 206, 1, "D", 990.0),  # constant prices
    (FRANCE, KENYA, 2023, 106, 1, "A", 3.0),
    (FRANCE, KENYA, 2023, 240, 1, "A", 12.0),  # gross, another basis
    (FRANCE, KENYA, 2023, 207, 1, "A", 4.0),  # technical co-operation, inside 206
    (FRANCE, IDA, 2023, 206, 1, "A", 40.0),  # a core contribution to IDA
    (GERMANY, KENYA, 2023, 206, 1, "A", 20.0),
    (GERMANY, KENYA, 2023, 106, 1, "A", 6.0),
    (IDA, KENYA, 2023, 206, 1, "A", 50.0),  # members' 106 already holds it
    (EU_INSTITUTIONS, KENYA, 2023, 206, 1, "A", 7.0),
    (EU_INSTITUTIONS, KENYA, 2023, 106, 1, "A", 1.0),
]

CRS_COLUMNS = tuple(column.name for column in VOCABULARY["crs"].columns)
CRS_BASE = {
    "provider": FRANCE,
    "recipient": KENYA,
    "year": 2021,
    "flow": 11,
    "bi_multi": 1,
    "category": 10,
    "finance_type": 110,
    "modality": "C01",
    "sector": 12264,
    "keywords": "#COVID-19",
    "disbursement": 1.0,
    "grant_equivalent": 1.0,
}


def _crs(**values: object) -> dict[str, object]:
    return dict.fromkeys(CRS_COLUMNS) | CRS_BASE | values


CRS_ROWS = [
    _crs(),
    _crs(keywords="#COVID-19;TEI000", disbursement=2.0),
    _crs(keywords="TEI000;#COVID-19", disbursement=4.0),
    _crs(keywords="#COVID-19-like", disbursement=100.0),  # another tag
    _crs(keywords="COVID-19", disbursement=200.0),  # without the hash
    _crs(keywords=None, disbursement=300.0),
    _crs(bi_multi=2, disbursement=400.0),  # a core contribution
    _crs(category=21, disbursement=500.0),  # other official flows
    _crs(provider=GERMANY, disbursement=8.0),
]


MULTISYSTEM_ROWS = [
    # provider, channel, year, flow type, amount type, aid to or thru, value
    (
        FRANCE,
        44002,
        2021,
        "Disbursements",
        "Current prices",
        "Core contributions to",
        30.0,
    ),
    (
        FRANCE,
        44002,
        2021,
        "Disbursements",
        "Constant prices",
        "Core contributions to",
        999.0,
    ),
    (
        FRANCE,
        44002,
        2021,
        "Disbursements",
        "Current prices",
        "Contributions through",
        77.0,
    ),
    (
        FRANCE,
        44002,
        2021,
        "Commitments",
        "Current prices",
        "Core contributions to",
        40.0,
    ),
]


class FakeTables:
    """DAC1, DAC2A, Multisystem and CRS tables with the views' logical columns."""

    def __init__(
        self,
        *,
        dac1: Sequence[tuple] = (),
        dac2a: Sequence[tuple] = (),
        crs: Sequence[dict[str, object]] = (),
        multisystem: Sequence[tuple] = (),
    ) -> None:
        self.connection = duckdb.connect()
        for table, columns, rows in (
            ("dac1", DAC1_COLUMNS, dac1),
            ("dac2a", DAC2A_COLUMNS, dac2a),
            ("multisystem", MULTISYSTEM_COLUMNS, multisystem),
        ):
            names = columns.split(", ")
            types = {c.name: c.type for c in VOCABULARY[table].columns}
            listed = ", ".join(f"{name} {types[name]}" for name in names)
            self.connection.execute(f"CREATE TABLE {table} ({listed})")
            if rows:
                marks = ", ".join("?" * len(names))
                self.connection.executemany(
                    f"INSERT INTO {table} VALUES ({marks})", list(rows)
                )
        frame = pd.DataFrame(list(crs), columns=list(CRS_COLUMNS))
        self.connection.register("crs_rows", frame)
        types = {c.name: c.type for c in VOCABULARY["crs"].columns}
        select = ", ".join(f"CAST({n} AS {types[n]}) AS {n}" for n in CRS_COLUMNS)
        self.connection.execute(f"CREATE TABLE crs AS SELECT {select} FROM crs_rows")
        self.connection.unregister("crs_rows")

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
                "factor": 1.0
                if unit == USD
                else PROVIDER_FACTORS.get(key["provider"], 1.0),
            }
            for key in keys.to_pylist()
            for unit in units
        ]
        return FactorTable(
            pa.Table.from_pylist(rows, schema=FACTOR_SCHEMA), frozenset()
        )

    def release(self) -> Release:
        return RELEASE


def _context(**tables: Sequence) -> Iterator[Context]:
    with Context._from_parts(FakeTables(**tables), FakeFactors()) as context:
        yield context


@pytest.fixture
def core() -> Iterator[Context]:
    yield from _context(dac1=CORE_ROWS)


@pytest.fixture
def eu() -> Iterator[Context]:
    yield from _context(dac1=_eu_rows())


@pytest.fixture
def recipients() -> Iterator[Context]:
    yield from _context(dac2a=DAC2A_ROWS)


@pytest.fixture
def covid() -> Iterator[Context]:
    yield from _context(crs=CRS_ROWS, multisystem=MULTISYSTEM_ROWS)


def _values(frame: pd.DataFrame, *keys: str) -> dict[tuple, float]:
    return {
        tuple(row[:-1]): row[-1]
        for row in frame[[*keys, "value"]].itertuples(index=False)
    }


# Registry ------------------------------------------------------------------------


def test_registry_maps_every_one_indicator_by_id() -> None:
    assert all(i.id.startswith("one.") for i in one.INDICATORS)
    assert all(_registry.INDICATORS[i.id] is i for i in one.INDICATORS)
    assert one.EU_CONTRIBUTIONS.id not in _registry.INDICATORS


# Core and non-core ODA --------------------------------------------------------------


def test_core_oda_subtracts_each_line_on_the_basis_of_each_year(
    core: Context,
) -> None:
    """Net flows to 2017 and grant equivalents from 2018, current USD only."""
    df = core.get("one.dac1.core_oda", years=[2017, 2018], providers=[FRANCE, GERMANY])

    assert _values(df, "provider", "year", "basis") == {
        ("4", 2017, "net_disbursement"): 100.0 - 10.0 - 5.0 - 2.0,
        ("4", 2018, "grant_equivalent"): 120.0 - 12.0 - 6.0,
        ("5", 2017, "net_disbursement"): 200.0,
        ("5", 2018, "grant_equivalent"): 220.0 - 20.0,
    }


@pytest.mark.parametrize("basis", ["headline", "net_disbursement"])
def test_core_plus_non_core_is_total_oda(core: Context, basis: str) -> None:
    call = {"years": [2017, 2018], "providers": [FRANCE, GERMANY], "basis": basis}
    core_oda = _values(core.get("one.dac1.core_oda", **call), "provider", "year")
    non_core = _values(core.get("one.dac1.non_core_oda", **call), "provider", "year")
    total_oda = _values(core.get("oecd.dac1.total_oda", **call), "provider", "year")

    keys = core_oda.keys() | total_oda.keys()
    for key in keys:
        assert core_oda[key] + non_core.get(key, 0.0) == total_oda.get(key, 0.0), key


def test_explain_names_only_providers_with_a_nulled_row(core: Context) -> None:
    """Germany has no non-core line in 2019, so it has no 2019 row, and the
    result warns that it has none rather than calling one null."""
    query = core.query(
        "one.dac1.non_core_oda", years=[2018, 2019], providers=[FRANCE, GERMANY]
    )

    with pytest.warns(
        OdaDataWarning, match="Line 1820, flow 1160 is missing for Germany"
    ):
        found = _values(query.to_pandas(), "provider", "year")

    assert ("5", 2019) not in found
    assert "are null" not in query.explain()


def test_core_oda_converts_each_provider_before_the_sum(core: Context) -> None:
    df = core.get(
        "one.dac1.core_oda",
        years=2017,
        providers=[FRANCE, GERMANY],
        by=["year"],
        unit=constant("USD", 2023),
    )

    assert df["value"].tolist() == [(100.0 - 17.0) * 2.0 + 200.0 * 3.0]


def test_a_member_that_drops_a_subtracted_line_nulls_the_last_year_total(
    core: Context,
) -> None:
    """Germany reports total ODA in 2019, the last year, but not yet the
    refugee costs it reported in 2018, so its core ODA reads too high."""
    duo = Group("duo", [FRANCE, GERMANY])
    totals = core.get("one.dac1.core_oda", years=[2018, 2019], providers=total(duo))
    partial = core.get(
        "one.dac1.core_oda",
        years=[2018, 2019],
        providers=total(duo),
        allow_partial=True,
    )

    assert _values(totals, "year")[(2018,)] == 102.0 + 200.0
    assert pd.isna(_values(totals, "year")[(2019,)])
    assert _values(partial, "year")[(2019,)] == 130.0 - 13.0 - 7.0 + 230.0


@pytest.mark.filterwarnings("ignore:.*is partial:oda_data.OdaDataWarning")
@pytest.mark.parametrize(
    ("allow_partial", "germany_2019"),
    [(False, None), (True, 230.0)],
    ids=["null", "allow-partial"],
)
def test_a_member_that_drops_a_subtracted_line_has_a_null_last_year_row(
    core: Context, allow_partial: bool, germany_2019: float | None
) -> None:
    """Germany's 2019 core ODA is null until it reports refugee costs. A line
    France never reports in 2019 (1600) counts as zero."""
    df = core.get(
        "one.dac1.core_oda",
        years=[2018, 2019],
        providers=[FRANCE, GERMANY],
        allow_partial=allow_partial,
    )

    found = _values(df, "provider", "year")
    assert found[("4", 2019)] == 130.0 - 13.0 - 7.0
    assert found[("5", 2018)] == 220.0 - 20.0
    if germany_2019 is None:
        assert pd.isna(found[("5", 2019)])
    else:
        assert found[("5", 2019)] == germany_2019


def test_a_sum_over_providers_is_null_when_a_subtracted_line_is_missing(
    core: Context,
) -> None:
    """Germany's 2019 total ODA is in the sum and its refugee costs are not
    taken out, so the sum would overstate: it is null, says why, and does not
    also warn that it covers only the providers that have reported."""
    with warnings.catch_warnings():
        warnings.simplefilter("error", OdaDataWarning)
        q = core.query("one.dac1.core_oda", years=2019, by=["year"])
    partial = core.get("one.dac1.core_oda", years=2019, by=["year"], allow_partial=True)

    assert pd.isna(q.to_pandas()["value"].item())
    assert any(
        line.startswith("2019 sums across providers are null: Germany (5) reported")
        for line in q.explain().splitlines()
    )
    assert partial["value"].tolist() == [130.0 - 13.0 - 7.0 + 230.0]


# Recipient ODA with imputed multilateral ------------------------------------------


def test_recipient_oda_with_imputed_multilateral_adds_lines_206_and_106(
    recipients: Context,
) -> None:
    df = recipients.get(
        "one.dac2a.recipient_oda_with_imputed_multilateral",
        years=2023,
        recipients=KENYA,
    )

    assert _values(df, "provider") == {("4",): 10.0 + 3.0, ("5",): 20.0 + 6.0}


def test_recipient_oda_with_imputed_multilateral_equals_its_two_parts(
    recipients: Context,
) -> None:
    providers = [FRANCE, GERMANY]
    found = _values(
        recipients.get(
            "one.dac2a.recipient_oda_with_imputed_multilateral",
            years=2023,
            providers=providers,
            by=["provider", "recipient"],
        ),
        "provider",
        "recipient",
    )
    parts = [
        _values(
            recipients.get(
                part, years=2023, providers=providers, by=["provider", "recipient"]
            ),
            "provider",
            "recipient",
        )
        for part in ("oecd.dac2a.recipient_oda", "oecd.dac2a.imputed_multilateral_oda")
    ]

    assert found == {key: sum(p.get(key, 0.0) for p in parts) for key in found}


@pytest.mark.parametrize("provider", [EU_INSTITUTIONS, IDA])
def test_recipient_oda_with_imputed_multilateral_refuses_multilateral_providers(
    recipients: Context, provider: int
) -> None:
    with pytest.raises(UnsupportedQueryError, match=f"leaves out provider {provider}"):
        recipients.get(
            "one.dac2a.recipient_oda_with_imputed_multilateral",
            years=2023,
            providers=[FRANCE, provider],
        )


def test_recipient_oda_with_imputed_multilateral_has_no_gross_basis(
    recipients: Context,
) -> None:
    with pytest.raises(UnsupportedQueryError, match="gross_disbursement"):
        recipients.get(
            "one.dac2a.recipient_oda_with_imputed_multilateral",
            years=2023,
            basis="gross_disbursement",
        )


# EU Institutions beside their funders ------------------------------------------------


#: 918's net and grant-equivalent ODA in 2019 less every other provider's line
#: 2102: France, Germany and Norway on net flows, France alone on grant
#: equivalents.
EU_NET_2019 = {
    "net_disbursement": 50.0 - 10.0 - 20.0 - 1.0,
    "grant_equivalent": 40.0 - 9.0,
}


@pytest.mark.parametrize(
    ("providers", "basis"),
    [
        pytest.param(
            [FRANCE, GERMANY, EU_INSTITUTIONS], "net_disbursement", id="members"
        ),
        pytest.param([FRANCE, EU_INSTITUTIONS], "net_disbursement", id="a-member"),
        pytest.param(
            [FRANCE, GERMANY, NORWAY, EU_INSTITUTIONS],
            "net_disbursement",
            id="a-non-member-that-pays",
        ),
        pytest.param([EU_INSTITUTIONS], "net_disbursement", id="alone"),
        pytest.param([FRANCE, EU_INSTITUTIONS], "grant_equivalent", id="ge"),
    ],
)
def test_eu_institutions_keep_what_no_other_provider_paid(
    eu: Context, providers: list[int], basis: str
) -> None:
    """918's row nets every provider's line 2102, Norway's included, whichever
    providers the call names."""
    df = eu.get(
        "one.dac1.total_oda_eu_attributed", years=2019, providers=providers, basis=basis
    )
    found = _values(df, "provider")

    assert found[(str(EU_INSTITUTIONS),)] == pytest.approx(EU_NET_2019[basis])
    if FRANCE in providers:
        assert found[(str(FRANCE),)] == (
            300.0 if basis == "net_disbursement" else 310.0
        )


def test_eu_institutions_net_of_every_provider_by_default(eu: Context) -> None:
    df = eu.get(
        "one.dac1.total_oda_eu_attributed", years=2019, basis="net_disbursement"
    )

    assert _values(df, "provider")[("918",)] == pytest.approx(50.0 - 31.0)


def test_eu_institutions_sum_with_their_funders(eu: Context) -> None:
    """``oecd.dac1.total_oda`` refuses this sum; the attributed indicator
    counts each dollar once."""
    providers = [FRANCE, GERMANY, EU_INSTITUTIONS]
    df = eu.get(
        "one.dac1.total_oda_eu_attributed",
        years=2019,
        providers=providers,
        by=["year"],
        basis="net_disbursement",
    )

    assert df["value"].tolist() == [pytest.approx(300.0 + 400.0 + 19.0)]
    with pytest.raises(UnsupportedQueryError, match="EU Institutions"):
        eu.get("oecd.dac1.total_oda", years=2019, providers=providers, by=["year"])


def test_eu_institutions_total_with_their_funders(eu: Context) -> None:
    """A group total of 918 and EU members works where the indicator
    attributes 918's flows, and raises on total ODA."""
    eu_group = Group("eu", [FRANCE, GERMANY, EU_INSTITUTIONS])
    df = eu.get(
        "one.dac1.total_oda_eu_attributed",
        years=2019,
        providers=total(eu_group),
        basis="net_disbursement",
    )

    assert df["value"].tolist() == [pytest.approx(300.0 + 400.0 + 19.0)]
    with pytest.raises(UnsupportedQueryError, match="group 'eu' holds EU"):
        eu.get("oecd.dac1.total_oda", years=2019, providers=total(eu_group))


def test_a_subtotal_with_eu_institutions_keeps_its_value_beside_another_funder(
    eu: Context,
) -> None:
    """France and 918 are worth the same with Germany in the call or not, and
    with Germany add up to the total of all three."""
    pair = total(Group("fr_eu", [FRANCE, EU_INSTITUTIONS]))
    kwargs = {"years": 2019, "basis": "net_disbursement"}
    alone = eu.get("one.dac1.total_oda_eu_attributed", providers=pair, **kwargs)
    beside = _values(
        eu.get("one.dac1.total_oda_eu_attributed", providers=[pair, GERMANY], **kwargs),
        "provider",
    )
    everything = eu.get(
        "one.dac1.total_oda_eu_attributed",
        providers=total(Group("all", [FRANCE, GERMANY, EU_INSTITUTIONS])),
        **kwargs,
    )

    assert alone["value"].tolist() == [pytest.approx(300.0 + 19.0)]
    assert beside[("fr_eu",)] == pytest.approx(300.0 + 19.0)
    assert beside[("fr_eu",)] + beside[(str(GERMANY),)] == pytest.approx(
        everything["value"].iloc[0]
    )


#: 2024 is the table's last year. France paid 918 in 2023 and has not reported
#: line 2102 in 2024; Germany pays in both years, more than 918 spent in 2023.
_UNREPORTED_ROWS = [
    (FRANCE, 2023, 11010, 1160, "A", 100.0),
    (FRANCE, 2024, 11010, 1160, "A", 100.0),
    (EU_INSTITUTIONS, 2023, 11010, 1160, "A", 30.0),
    (EU_INSTITUTIONS, 2024, 11010, 1160, "A", 30.0),
    (FRANCE, 2023, 2102, 1160, "A", 10.0),
    (GERMANY, 2023, 2102, 1160, "A", 25.0),
    (GERMANY, 2024, 2102, 1160, "A", 25.0),
]


@pytest.mark.parametrize(
    ("allow_partial", "expected_2024"),
    [(False, None), (True, 30.0 - 25.0)],
    ids=["null", "allow-partial"],
)
@pytest.mark.parametrize("shape", ["rows", "total", "by-year"])
def test_eu_institutions_are_incomplete_while_a_funder_has_not_reported(
    allow_partial: bool, expected_2024: float | None, shape: str
) -> None:
    """France's 2023 payment has no 2024 counterpart yet, so 918's 2024 row,
    and a sum that holds it, covers only Germany's payment. 918's 2023 row
    is negative, as computed."""
    pair = [FRANCE, EU_INSTITUTIONS]
    call = {
        "rows": {"providers": pair},
        "total": {"providers": total(Group("pair", pair))},
        "by-year": {"providers": pair, "by": ["year"]},
    }[shape]
    with Context._from_parts(FakeTables(dac1=_UNREPORTED_ROWS), FakeFactors()) as ctx:
        query = ctx.query(
            "one.dac1.total_oda_eu_attributed",
            years=[2023, 2024],
            allow_partial=allow_partial,
            **call,
        )
        frame = query.to_pandas()
        explained = query.explain()

    eu_2023 = 30.0 - 10.0 - 25.0
    offset = 0.0 if shape == "rows" else 100.0
    key = {"rows": (str(EU_INSTITUTIONS),), "total": ("pair",), "by-year": ()}[shape]
    found = _values(frame, *(("provider",) if key else ()), "year")
    assert found[(*key, 2023)] == pytest.approx(offset + eu_2023)
    if expected_2024 is None:
        assert pd.isna(found[(*key, 2024)])
    else:
        assert found[(*key, 2024)] == pytest.approx(offset + expected_2024)
    assert (
        "EU Institutions (918) in 2024 is incomplete: France (4) has not "
        "reported line 2102" in explained
    )
    assert "Germany (5)" not in explained


def test_a_group_total_of_eu_institutions_and_a_member_sums_on_dac2a(
    recipients: Context,
) -> None:
    """DAC2A members' recipient figures leave out their EU share."""
    df = recipients.get(
        "oecd.dac2a.recipient_oda",
        years=2023,
        providers=total(Group("eu", [FRANCE, EU_INSTITUTIONS])),
        recipients=KENYA,
    )

    assert df["value"].tolist() == [pytest.approx(10.0 + 7.0)]


def test_a_group_total_of_eu_institutions_and_a_member_sums_on_crs(
    covid: Context,
) -> None:
    group = Group("eu", [FRANCE, GERMANY, EU_INSTITUTIONS])
    df = covid.get("oecd.crs.bilateral_oda", years=2021, providers=total(group))
    bilateral = covid.get(
        "oecd.crs.bilateral_oda", years=2021, providers=[FRANCE, GERMANY]
    )

    assert df["value"].tolist() == [pytest.approx(bilateral["value"].sum())]


# GNI and ODA/GNI with EU Institutions -------------------------------------------------


def _eu_gni(year: int, members: frozenset[int] | None = None) -> float:
    members = eu_members([year]) if members is None else members
    return sum(
        value
        for code, row_year, line, *_, value in _eu_rows()
        if line == 1 and row_year == year and code in members
    )


def test_eu_institutions_gni_is_the_gni_of_each_years_members(eu: Context) -> None:
    """The United Kingdom counts in 2019 and not in 2021. Cyprus skips 2021,
    so it counts as zero there. Germany stops after 2021, so 918's 2022 GNI is
    incomplete: null unless the call allows partial values."""
    query = eu.query("one.dac1.gni", years=[2019, 2021, 2022], providers=918)
    strict = query.to_pandas()
    partial = eu.get(
        "one.dac1.gni", years=[2019, 2021, 2022], providers=918, allow_partial=True
    )

    assert strict["year"].tolist() == [2019, 2021, 2022]
    assert strict["value"].tolist()[:2] == [
        pytest.approx(_eu_gni(2019)),
        pytest.approx(_eu_gni(2021)),
    ]
    assert pd.isna(strict["value"].iloc[2])
    assert _eu_gni(2019) - _eu_gni(2021) == (1000.0 + UK) + (1000.0 + CYPRUS)
    assert _values(partial, "year")[(2022,)] == pytest.approx(_eu_gni(2022))
    explained = query.explain()
    assert "EU Institutions (918) GNI in 2021 sums the members that reported" in (
        explained
    )
    assert "Cyprus (30)" in explained
    assert (
        "EU Institutions (918) in 2022 is incomplete: Germany (5) has not "
        "reported GNI" in explained
    )
    assert "Germany (5)" in explained


@pytest.mark.filterwarnings("ignore:.*is partial:oda_data.OdaDataWarning")
@pytest.mark.parametrize("allow_partial", [False, True], ids=["null", "allow-partial"])
@pytest.mark.parametrize(
    "indicator",
    ["one.dac1.gni", "one.dac1.gni_fixed_eu27", "one.dac1.total_oda_eu_attributed"],
)
def test_a_method_gives_each_year_what_a_call_for_that_year_gives(
    eu: Context, indicator: str, allow_partial: bool
) -> None:
    """A Method's rows for a year do not depend on the other years asked."""
    kwargs = {
        "providers": [FRANCE, EU_INSTITUTIONS],
        "basis": "net_disbursement",
        "allow_partial": allow_partial,
    }
    years = [2019, 2021, 2022]
    together = _values(eu.get(indicator, years=years, **kwargs), "provider", "year")
    alone = {
        key: value
        for year in years
        for key, value in _values(
            eu.get(indicator, years=year, **kwargs), "provider", "year"
        ).items()
    }

    assert together == pytest.approx(alone, nan_ok=True)


#: France stops reporting GNI after 2023, so 918's 2024 GNI holds Germany's
#: alone. Norway reports ODA and GNI in both years.
_RATIO_ROWS = [
    (FRANCE, 2023, 1, 1140, "A", 1000.0),
    (FRANCE, 2024, 11010, 1160, "A", 1.0),
    (GERMANY, 2023, 1, 1140, "A", 2000.0),
    (GERMANY, 2024, 1, 1140, "A", 2000.0),
    (EU_INSTITUTIONS, 2023, 11010, 1160, "A", 30.0),
    (EU_INSTITUTIONS, 2024, 11010, 1160, "A", 30.0),
    (NORWAY, 2023, 1, 1140, "A", 100.0),
    (NORWAY, 2024, 1, 1140, "A", 100.0),
    (NORWAY, 2023, 11010, 1160, "A", 10.0),
    (NORWAY, 2024, 11010, 1160, "A", 10.0),
]


@pytest.mark.parametrize("shape", ["total", "by-year"])
@pytest.mark.parametrize(
    ("allow_partial", "expected_2024"),
    [(False, None), (True, 100 * (30.0 + 10.0) / (2000.0 + 100.0))],
    ids=["null", "allow-partial"],
)
def test_a_group_ratio_is_null_when_a_members_denominator_is_incomplete(
    allow_partial: bool, expected_2024: float | None, shape: str
) -> None:
    pair = [EU_INSTITUTIONS, NORWAY]
    call = {
        "total": {"providers": total(Group("pair", pair))},
        "by-year": {"providers": pair, "by": ["year"]},
    }[shape]
    with Context._from_parts(FakeTables(dac1=_RATIO_ROWS), FakeFactors()) as ctx:
        query = ctx.query(
            "one.dac1.oda_gni_ratio",
            years=[2023, 2024],
            allow_partial=allow_partial,
            **call,
        )
        found = _values(query.to_pandas(), "year")
        explained = query.explain()

    assert found[(2023,)] == pytest.approx(100 * (30.0 + 10.0) / (3000.0 + 100.0))
    if expected_2024 is None:
        assert pd.isna(found[(2024,)])
    else:
        assert found[(2024,)] == pytest.approx(expected_2024)
    assert (
        "EU Institutions (918) in 2024 is incomplete: France (4) has not "
        "reported GNI" in explained
    )


def test_eu_institutions_fixed_eu27_gni_leaves_the_united_kingdom_out(
    eu: Context,
) -> None:
    eu27 = eu_members([2025])
    df = eu.get("one.dac1.gni_fixed_eu27", years=[2019, 2021], providers=918)
    dated = eu.get("one.dac1.gni", years=[2019, 2021], providers=918)

    assert _values(df, "year") == {
        (2019,): pytest.approx(_eu_gni(2019, eu27)),
        (2021,): pytest.approx(_eu_gni(2021, eu27)),
    }
    assert _values(dated, "year")[(2019,)] - _values(df, "year")[(2019,)] == (
        pytest.approx(1000.0 + UK)
    )


def test_gni_of_a_member_is_its_own(eu: Context) -> None:
    df = eu.get("one.dac1.gni", years=2019, providers=[FRANCE, EU_INSTITUTIONS])

    assert _values(df, "provider")[("4",)] == 1000.0 + FRANCE


def test_oda_gni_ratio_gives_eu_institutions_a_ratio(eu: Context) -> None:
    """Headline 2019 reads grant equivalents over GNI on net flows."""
    df = eu.get(
        "one.dac1.oda_gni_ratio", years=2019, providers=[FRANCE, EU_INSTITUTIONS]
    )
    oecd = eu.get("oecd.dac1.oda_gni_ratio", years=2019, providers=FRANCE)

    assert _values(df, "provider") == {
        ("4",): pytest.approx(100 * 310.0 / (1000.0 + FRANCE)),
        ("918",): pytest.approx(100 * 40.0 / _eu_gni(2019)),
    }
    assert oecd["value"].tolist() == [pytest.approx(100 * 310.0 / (1000.0 + FRANCE))]


def test_oda_gni_ratio_refuses_eu_institutions_summed_with_members(
    eu: Context,
) -> None:
    with pytest.raises(UnsupportedQueryError, match="EU Institutions"):
        eu.get(
            "one.dac1.oda_gni_ratio",
            years=2019,
            providers=[FRANCE, EU_INSTITUTIONS],
            by=["year"],
        )


# COVID-19 ODA ---------------------------------------------------------------------------


def test_covid_oda_counts_bilateral_rows_tagged_covid_19(covid: Context) -> None:
    """The tag counts wherever it sits in the list, and only as a whole tag."""
    df = covid.get("one.crs.covid_oda", years=2021)

    assert _values(df, "provider") == {("4",): 1.0 + 2.0 + 4.0, ("5",): 8.0}


# Bilateral plus multilateral, activity data -------------------------------------------


@pytest.mark.parametrize(
    ("basis", "multilateral"),
    [("gross_disbursement", 30.0), ("commitment", 40.0)],  # no CRS commitments
)
def test_bilateral_plus_multilateral_adds_core_contributions(
    covid: Context, basis: str, multilateral: float
) -> None:
    """Bilateral CRS rows (category 10, bi_multi 1, 3, 7, 8) plus Multisystem
    core contributions in current prices on the same basis."""
    call = {"years": 2021, "providers": FRANCE, "basis": basis}
    df = covid.get("one.crs.bilateral_plus_multilateral_oda", **call)
    bilateral = covid.get("oecd.crs.bilateral_oda", **call)["value"].sum()

    assert df["value"].tolist() == [pytest.approx(bilateral + multilateral)]


def test_bilateral_plus_multilateral_has_no_grant_equivalents(covid: Context) -> None:
    with pytest.raises(UnsupportedQueryError, match="grant_equivalent"):
        covid.get(
            "one.crs.bilateral_plus_multilateral_oda",
            years=2021,
            basis="grant_equivalent",
        )

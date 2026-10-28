"""Context, Query, and the module functions on in-memory tables.

``FakeTables`` serves DuckDB tables that carry the logical column names from
``_model.VOCABULARY``. ``FakeFactors`` gives factor 1 for current USD and 2
for any other unit. The registry is a stand-in mapping of the indicators
defined here.
"""

import dataclasses
import inspect
import io
import threading
from collections import Counter
from collections.abc import Iterator
from datetime import datetime

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import oda_data as oda
from oda_data import _context
from oda_data._context import Context
from oda_data._discover import _info
from oda_data._errors import (
    DimensionError,
    OfflineCacheMissError,
    UnknownIndicatorError,
    UnsupportedQueryError,
)
from oda_data._factors import FACTOR_SCHEMA, DacFactors
from oda_data._groups import Group, total
from oda_data._model import (
    Binding,
    Caveat,
    FactorTable,
    Flow,
    Headline,
    In,
    Indicator,
    NotIn,
    Ratio,
    Sum,
    Value,
)
from oda_data._provenance import Release, to_metadata
from oda_data._registry import INDICATORS as REGISTRY
from oda_data._store import UNCACHED
from oda_data._tables import TABLES
from oda_data._units import Unit, constant, current

USD = current("USD")
DAC1_RELEASE = Release("dac1-token", datetime(2026, 10, 12))
CRS_RELEASE = Release("v20260803", datetime(2026, 9, 1))

DAC1_ROWS = [
    # provider, year, line, flow, value
    (4, 2017, 1010, 1140, 10.0),
    (5, 2017, 1010, 1140, 20.0),
    (12, 2017, 1010, 1140, 30.0),
    (918, 2017, 1010, 1140, 5.0),  # EU Institutions report only in 2017
    (55, 2017, 1010, 1140, 7.0),  # so does Türkiye, which is no DAC member
    (20001, 2017, 1010, 1140, 60.0),
    (4, 2018, 11010, 1160, 11.0),
    (5, 2018, 11010, 1160, 22.0),
    (12, 2018, 11010, 1160, 33.0),
    (20001, 2018, 11010, 1160, 66.0),
    (4, 2017, 1, 1140, 1000.0),
    (5, 2017, 1, 1140, 2000.0),
    (4, 2018, 1, 1140, 1100.0),
    (5, 2018, 1, 1140, 2200.0),
]

CRS_ROWS = [
    # provider, recipient, year, sector, finance_type, disbursement, project_title
    (4, 248, 2019, 12220, 110, 3.0, "Clinic"),
    (918, 248, 2019, 11110, 421, 2.0, "School"),
    (1601, 248, 2019, 12220, 110, 9.0, "Vaccines"),  # a private donor
    (20001, 248, 2019, 12220, 110, 50.0, "DAC total"),  # an aggregate
]


class FakeTables:
    """Views as DuckDB tables, with a cursor count and a close flag."""

    def __init__(self) -> None:
        self.connection = duckdb.connect()
        self.connection.execute(
            "CREATE TABLE dac1 (provider BIGINT, year BIGINT, line BIGINT,"
            " flow BIGINT, amount_type VARCHAR, value DOUBLE)"
        )
        self.connection.executemany(
            "INSERT INTO dac1 VALUES (?, ?, ?, ?, 'A', ?)", DAC1_ROWS
        )
        self.connection.execute(
            "CREATE TABLE crs (provider BIGINT, recipient BIGINT, year BIGINT,"
            " category BIGINT, bi_multi BIGINT, sector BIGINT,"
            " finance_type BIGINT, disbursement DOUBLE, project_title VARCHAR)"
        )
        self.connection.executemany(
            "INSERT INTO crs VALUES (?, ?, ?, 10, 1, ?, ?, ?, ?)", CRS_ROWS
        )
        self.cursors = 0
        self.closed = False

    def cursor(self, tables: tuple[str, ...]) -> duckdb.DuckDBPyConnection:
        self.cursors += 1
        return self.connection.cursor()

    def path(self, table: str) -> object:
        raise NotImplementedError

    def release(self, table: str) -> Release:
        return {"dac1": DAC1_RELEASE, "crs": CRS_RELEASE}[table]

    def close(self) -> None:
        self.closed = True


class FakeFactors:
    """Factor 1 for current USD and 2 for any other unit."""

    def __init__(self, fallback: frozenset[int] = frozenset()) -> None:
        self.fallback = fallback

    def table(self, keys: pa.Table, units: tuple[Unit, ...], /) -> FactorTable:
        rows = [
            key
            | {
                "currency": unit.currency,
                "prices": unit.prices,
                "base_year": unit.base_year,
                "factor": 1.0 if unit == USD else 2.0,
            }
            for key in keys.to_pylist()
            for unit in units
        ]
        return FactorTable(
            pa.Table.from_pylist(rows, schema=FACTOR_SCHEMA), self.fallback
        )

    def release(self) -> Release:
        return DAC1_RELEASE


# Indicators ---------------------------------------------------------------------

DAC1 = Binding(
    table="dac1",
    dimensions={"provider": "provider", "line": "line", "year": "year"},
    bases={
        "net_disbursement": Flow(1140),
        "grant_equivalent": Flow(1160, from_year=2018),
        "headline": Headline("net_disbursement", "grant_equivalent"),
    },
    rows=(In("amount_type", ("A",)),),
)

CRS = Binding(
    table="crs",
    dimensions={
        "provider": "provider",
        "recipient": "recipient",
        "sector": "sector",
        "broad_sector": "sector",
        "finance_type": "finance_type",
        "year": "year",
    },
    bases={
        "gross_disbursement": Value("disbursement", rows=(In("category", (10,)),)),
        # A stand-in: the fake table holds no repayments to subtract.
        "net_disbursement": Value("disbursement", rows=(In("category", (10,)),)),
    },
    rows=(In("bi_multi", (1, 3, 7, 8)),),
)

TOTAL_ODA = Indicator(
    id="oecd.dac1.total_oda",
    name="Total ODA",
    description="Total ODA.",
    binding=DAC1,
    kind=Sum(lines={"net_disbursement": 1010, "grant_equivalent": 11010}),
    dimensions=("provider", "year"),
    default_basis="headline",
    default_providers="dac_countries",
)
GNI = Indicator(
    id="oecd.dac1.gni",
    name="GNI",
    description="GNI.",
    binding=DAC1,
    kind=Sum(lines={"net_disbursement": 1}),
    dimensions=("provider", "year"),
    default_basis="net_disbursement",
    default_providers="dac_countries",
)
ODA_GNI = Indicator(
    id="oecd.dac1.oda_gni_ratio",
    name="ODA/GNI",
    description="ODA over GNI.",
    binding=DAC1,
    kind=Ratio(TOTAL_ODA, GNI),
    dimensions=("provider", "year"),
    default_basis="headline",
    default_providers="dac_countries",
)
BILATERAL = Indicator(
    id="oecd.crs.bilateral_oda",
    name="Bilateral ODA",
    description="Bilateral ODA.",
    binding=CRS,
    kind=Sum(),
    dimensions=(
        "provider",
        "recipient",
        "sector",
        "broad_sector",
        "finance_type",
        "year",
    ),
    default_basis="gross_disbursement",
    default_providers=None,
)
BILATERAL_NO_EU = Indicator(
    id="test.crs.bilateral_without_eu",
    name="Bilateral ODA without EU Institutions",
    description="Bilateral ODA, EU Institutions excluded.",
    binding=CRS,
    kind=Sum(),
    dimensions=("provider", "recipient", "year"),
    default_basis="gross_disbursement",
    default_providers=None,
    rows=(NotIn("provider", (918,), reason="its members already count its flows"),),
    caveats=(Caveat("CRS totals differ from DAC1 totals"),),
)
LINES = Indicator(
    id="oecd.dac1.lines",
    name="DAC1 lines",
    description="DAC1 lines.",
    binding=DAC1,
    kind=Sum(),
    dimensions=("provider", "year", "line"),
    default_basis="net_disbursement",
    default_providers="dac_countries",
)

TRIO = Group("trio", [4, 5, 12], label="Trio countries", dimension="provider")


@pytest.fixture(autouse=True)
def registry(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        _context,
        "INDICATORS",
        {i.id: i for i in (TOTAL_ODA, GNI, ODA_GNI, BILATERAL, BILATERAL_NO_EU, LINES)},
    )


@pytest.fixture
def tables() -> Iterator[FakeTables]:
    fake = FakeTables()
    yield fake
    fake.connection.close()


@pytest.fixture
def ctx(tables: FakeTables) -> Context:
    return Context._from_parts(tables, FakeFactors())


def _rows(frame, *columns: str) -> list[tuple]:
    return [tuple(row) for row in frame[list(columns)].itertuples(index=False)]


# Signatures -----------------------------------------------------------------------


def _parameters(function) -> list[inspect.Parameter]:
    parameters = list(inspect.signature(function).parameters.values())
    return [p for p in parameters if p.name != "self"]


def test_get_query_and_context_methods_share_one_signature() -> None:
    expected = _parameters(oda.get)
    for function in (oda.query, Context.get, Context.query):
        assert _parameters(function) == expected


def test_records_takes_the_filter_arguments_and_columns() -> None:
    get = {p.name: p for p in _parameters(oda.get)}
    filters = [p for name, p in get.items() if name not in {"by", "allow_partial"}]
    columns = inspect.Parameter(
        "columns",
        inspect.Parameter.KEYWORD_ONLY,
        default=(),
        annotation="Iterable[str]",
    )

    assert _parameters(oda.records) == [filters[0], columns, *filters[1:]]
    assert _parameters(Context.records) == _parameters(oda.records)


def test_indicator_is_positional_only_and_the_rest_keyword_only() -> None:
    kinds = [p.kind for p in _parameters(oda.get)]
    assert kinds[0] is inspect.Parameter.POSITIONAL_ONLY
    assert set(kinds[1:]) == {inspect.Parameter.KEYWORD_ONLY}


# Result schema --------------------------------------------------------------------

RESULT_DTYPES = {
    "provider": "string",
    "provider_name": "string",
    "year": "Int64",
    "indicator": "category",
    "indicator_version": "Int64",
    "basis": "category",
    "currency": "category",
    "prices": "category",
    "base_year": "Int64",
    "scale": "category",
    "is_aggregate": "bool",
    "value": "float64",
}


def test_get_returns_the_result_schema(ctx: Context) -> None:
    df = ctx.get("oecd.dac1.total_oda", years=2017, providers="France")

    assert {name: str(dtype) for name, dtype in df.dtypes.items()} == RESULT_DTYPES
    assert list(df.columns) == list(RESULT_DTYPES)
    assert df["base_year"].isna().all()
    assert df.drop(columns="base_year").iloc[0].to_dict() == {
        "provider": "4",
        "provider_name": "France",
        "year": 2017,
        "indicator": "oecd.dac1.total_oda",
        "indicator_version": 1,
        "basis": "net_disbursement",
        "currency": "USD",
        "prices": "current",
        "scale": "millions",
        "is_aggregate": False,
        "value": 10.0,
    }


def test_an_empty_result_keeps_the_schema(ctx: Context) -> None:
    df = ctx.get("oecd.dac1.total_oda", years=1990)

    assert len(df) == 0
    assert {name: str(dtype) for name, dtype in df.dtypes.items()} == RESULT_DTYPES


def test_ratio_rows_have_no_unit(ctx: Context) -> None:
    q = ctx.query("oecd.dac1.oda_gni_ratio", years=2017, providers=["France"])
    df = q.to_pandas()

    assert _rows(df, "provider", "scale", "value") == [("4", "percent", 1.0)]
    assert df["currency"].isna().all()
    assert df["base_year"].isna().all()
    assert "unit" not in q.provenance.defaults
    assert q.provenance.indicators == {
        "oecd.dac1.oda_gni_ratio": 1,
        "oecd.dac1.total_oda": 1,
        "oecd.dac1.gni": 1,
    }


def test_check_frame_refuses_a_wrong_dtype(ctx: Context) -> None:
    q = ctx.query("oecd.dac1.total_oda", years=2017)
    frame = q.to_pandas()
    frame["year"] = frame["year"].astype("float64")

    with pytest.raises(RuntimeError):
        _context.check_frame(frame, _context.result_schema(["provider", "year"]))


# Defaults -------------------------------------------------------------------------


def test_default_providers_are_the_indicators_group(ctx: Context) -> None:
    q = ctx.query("oecd.dac1.total_oda", by=["provider"])

    providers = set(q.to_pandas()["provider"])

    assert providers == {"4", "5", "12"}  # not 918, nor the aggregate 20001
    assert q.provenance.defaults == {
        "providers": "dac_countries",
        "basis": "headline",
        "unit": "current USD",
    }


def test_no_default_group_means_every_official_provider(ctx: Context) -> None:
    df = ctx.get("oecd.crs.bilateral_oda", by=["provider"])

    # The private donor 1601 and the aggregate 20001 drop out.
    assert _rows(df, "provider", "value") == [("4", 3.0), ("918", 2.0)]


def test_default_providers_leave_out_excluded_providers(ctx: Context) -> None:
    df = ctx.get("test.crs.bilateral_without_eu", by=["provider"])

    assert _rows(df, "provider", "value") == [("4", 3.0)]


def test_naming_an_excluded_provider_raises(ctx: Context) -> None:
    with pytest.raises(UnsupportedQueryError):
        ctx.get("test.crs.bilateral_without_eu", providers=918, by=["provider"])


def test_explain_names_excluded_providers_and_unscoped_caveats(ctx: Context) -> None:
    # The caveat names no basis or years, so it never warns; pytest turns a
    # warning into an error.
    text = ctx.query("test.crs.bilateral_without_eu").explain().splitlines()

    assert text[2] == "Leaves out EU Institutions: its members already count its flows."
    assert text[4] == "Caveat: CRS totals differ from DAC1 totals."


def test_provenance_records_the_provider_exclusion(ctx: Context) -> None:
    q = ctx.query("test.crs.bilateral_without_eu", by=["provider"])

    assert q.provenance.defaults["excluded_providers"] == (
        "918: its members already count its flows"
    )


def test_a_part_of_a_ratio_leaves_out_its_excluded_provider(
    ctx: Context, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Named, the provider raises with the part's reason; by default, the
    # call reads every provider but it.
    share = dataclasses.replace(
        BILATERAL_NO_EU,
        id="test.crs.share",
        kind=Ratio(BILATERAL_NO_EU, BILATERAL),
        caveats=(),
        rows=(),
    )
    monkeypatch.setitem(_context.INDICATORS, share.id, share)

    with pytest.raises(UnsupportedQueryError, match="its members already count"):
        ctx.get("test.crs.share", providers=918, by=["provider"])
    df = ctx.get("test.crs.share", by=["provider"])
    assert "918" not in set(df["provider"])


def test_describe_and_a_default_call_agree_on_918_for_every_indicator() -> None:
    """On every registered indicator with providers, ``describe()`` says a sum
    across providers leaves 918 out exactly when a call that names none drops
    918 from a sum and keeps it in a split by provider, and only then do the
    two sets differ."""
    con = duckdb.connect()
    for table in TABLES:
        con.execute(f"CREATE TABLE {table} AS FROM (VALUES (4), (918)) t(provider)")
    with_providers = [i for i in REGISTRY.values() if "provider" in i.dimensions]

    assert any(_info(i).leaves_out_eu_institutions for i in with_providers)
    for indicator in with_providers:
        tables = _context.tables_for(indicator)
        summed, _ = _context._default_providers(
            con, indicator, tables, (2023,), by=("year",), releases=UNCACHED
        )
        split, _ = _context._default_providers(
            con, indicator, tables, (2023,), by=("provider",), releases=UNCACHED
        )
        left_out = 918 in split and 918 not in summed
        assert _info(indicator).leaves_out_eu_institutions == left_out, indicator.id
        assert split - summed <= {918}, indicator.id


# Finance type ----------------------------------------------------------------------


def test_finance_types_filter_by_name_and_split(ctx: Context) -> None:
    q = ctx.query(
        "oecd.crs.bilateral_oda",
        finance_types="Standard grant",
        by=["finance_type"],
    )

    assert _rows(q.to_pandas(), "finance_type", "finance_type_name", "value") == [
        ("110", "Standard grant", 3.0)
    ]
    assert "Finance types: Standard grant." in q.explain().splitlines()


@pytest.mark.parametrize("arguments", [{"finance_types": 110}, {"by": "finance_type"}])
def test_finance_type_on_dac1_names_flows_before_the_query_runs(
    ctx: Context, tables: FakeTables, arguments: dict
) -> None:
    with pytest.raises(DimensionError, match="flows="):
        ctx.query("oecd.dac1.total_oda", **arguments)
    assert tables.cursors == 0


# Groups and names -----------------------------------------------------------------


def test_group_total_without_years_covers_every_table_year(ctx: Context) -> None:
    df = ctx.get("oecd.dac1.total_oda", providers=[TRIO, total(TRIO)])

    totals = df[df["is_aggregate"]]
    assert _rows(totals, "provider", "provider_name", "year", "value") == [
        ("trio", "Trio countries", 2017, 60.0),
        ("trio", "Trio countries", 2018, 66.0),
    ]
    assert len(df) == 8


def test_a_member_that_did_not_report_empties_the_total(ctx: Context) -> None:
    pair = Group("pair", [12, 55], dimension="provider")
    q = ctx.query("oecd.dac1.total_oda", providers=total(pair))

    df = q.to_pandas()

    assert df["value"].isna().tolist() == [False, True]  # 55 is absent in 2018
    assert "which is empty in 2018" in q.explain()


@pytest.mark.parametrize(
    ("named", "norway_in_1990"),
    [([], False), ([9], True)],
    ids=["group-only", "member-named-too"],
)
def test_a_dated_group_shows_each_member_in_its_years(
    tables: FakeTables, named: list[int], norway_in_1990: bool
) -> None:
    # Norway (9) left the DAC in 1973 and rejoined in 1991.
    tables.connection.execute("DELETE FROM dac1")
    tables.connection.executemany(
        "INSERT INTO dac1 VALUES (?, ?, ?, ?, 'A', ?)",
        [
            (4, 1990, 1010, 1140, 10.0),
            (9, 1990, 1010, 1140, 20.0),
            (4, 2024, 11010, 1160, 10.0),
            (9, 2024, 11010, 1160, 20.0),
        ],
    )
    ctx = Context._from_parts(tables, FakeFactors())
    dac = oda.groups.dac_countries.dated()
    providers = [dac, total(dac), *named]
    columns = ("provider", "year", "value")

    both = ctx.get("oecd.dac1.total_oda", years=[1990, 2024], providers=providers)
    each = pd.concat(
        ctx.get("oecd.dac1.total_oda", years=year, providers=providers)
        for year in (1990, 2024)
    )
    summed = ctx.get(
        "oecd.dac1.total_oda", years=[1990, 2024], providers=dac, by=["year"]
    )

    assert sorted(_rows(both, *columns)) == sorted(_rows(each, *columns))
    assert (("9", 1990, 20.0) in _rows(both, *columns)) is norway_in_1990
    totals = both[both["is_aggregate"]]
    assert _rows(totals, *columns) == [
        ("dac_countries", 1990, 10.0),
        ("dac_countries", 2024, 30.0),
    ]
    assert _rows(summed, "year", "value") == [(1990, 10.0), (2024, 30.0)]


def test_groups_of_one_name_in_two_dimensions_keep_their_own_totals(
    ctx: Context,
) -> None:
    donors = Group("focus", [4], label="Focus donors", dimension="provider")
    places = Group("focus", [248], label="Focus places", dimension="recipient")
    q = ctx.query(
        "oecd.crs.bilateral_oda",
        years=2019,
        providers=[4, total(donors)],
        recipients=[248, total(places)],
        by=["provider", "recipient"],
    )

    df = q.to_pandas()

    columns = ("provider", "provider_name", "recipient", "recipient_name", "value")
    assert _rows(df[df["is_aggregate"]], *columns) == [
        ("focus", "Focus donors", "248", "Kenya", 3.0),
        ("4", "France", "focus", "Focus places", 3.0),
    ]
    assert "The total of 1 Focus donors" in q.explain()
    assert "the total of 1 Focus places" in q.explain()


def test_broad_sectors_split_crs_rows(ctx: Context) -> None:
    df = ctx.get("oecd.crs.bilateral_oda", by=["broad_sector"])

    assert _rows(df, "broad_sector", "broad_sector_name", "value") == [
        ("education", "Education", 2.0),
        ("health", "Health", 3.0),
    ]


def test_units_add_rows_and_record_the_factor_release(tables: FakeTables) -> None:
    ctx = Context._from_parts(tables, FakeFactors())
    q = ctx.query(
        "oecd.crs.bilateral_oda",
        providers="France",
        by=["provider"],
        unit=[USD, constant("EUR", 2023)],
    )

    df = q.to_pandas()

    assert _rows(df, "currency", "base_year", "value") == [
        ("EUR", 2023, 6.0),
        ("USD", pd.NA, 3.0),
    ]
    assert dict(q.provenance.tables) == {"crs": CRS_RELEASE, "dac1": DAC1_RELEASE}


class CrsOnlyTables(FakeTables):
    """An offline cache that holds CRS and no DAC1."""

    def release(self, table: str) -> Release:
        if table == "dac1":
            raise OfflineCacheMissError("the cache holds no dac1 table")
        return super().release(table)

    def path(self, table: str) -> object:
        raise OfflineCacheMissError(f"the cache holds no {table} table")


def test_a_current_usd_call_reads_no_dac1() -> None:
    tables = CrsOnlyTables()
    ctx = Context._from_parts(tables, DacFactors(tables))
    q = ctx.query("oecd.crs.bilateral_oda", providers="France", by=["provider"])

    assert _rows(q.to_pandas(), "value") == [(3.0,)]
    assert dict(q.provenance.tables) == {"crs": CRS_RELEASE}


# Records ---------------------------------------------------------------------------


def test_records_returns_rows_with_names_and_extra_columns(ctx: Context) -> None:
    df = ctx.records(
        "oecd.crs.bilateral_oda", providers="France", columns="project_title"
    )

    assert list(df.columns) == [
        "provider",
        "provider_name",
        "recipient",
        "recipient_name",
        "sector",
        "sector_name",
        "broad_sector",
        "broad_sector_name",
        "finance_type",
        "finance_type_name",
        "year",
        "project_title",
        "indicator",
        "indicator_version",
        "basis",
        "currency",
        "prices",
        "base_year",
        "scale",
        "value",
    ]
    row = df.iloc[0]
    assert (row["recipient_name"], row["sector"], row["project_title"]) == (
        "Kenya",
        "12220",
        "Clinic",
    )
    assert str(df["year"].dtype) == "Int64"
    assert str(df["basis"].dtype) == "category"


@pytest.mark.parametrize("column", ["sector_name", "value", "year"])
def test_records_refuse_a_column_the_result_already_holds(
    ctx: Context, column: str
) -> None:
    with pytest.raises(UnsupportedQueryError, match=column):
        ctx.records("oecd.crs.bilateral_oda", columns=["project_title", column])


def test_records_refuses_a_ratio(ctx: Context) -> None:
    with pytest.raises(UnsupportedQueryError):
        ctx.records("oecd.dac1.oda_gni_ratio", years=2017)


# Query -----------------------------------------------------------------------------


def test_query_runs_once_on_first_use(ctx: Context, tables: FakeTables) -> None:
    q = ctx.query("oecd.dac1.total_oda", years=2017)
    assert tables.cursors == 0

    q.to_pandas()
    q.to_arrow()
    q.explain()
    _ = q.provenance

    assert tables.cursors == 1


def test_to_parquet_writes_the_provenance(ctx: Context, tmp_path) -> None:
    q = ctx.query("oecd.dac1.total_oda", years=2017)
    path = tmp_path / "total_oda.parquet"
    buffer = io.BytesIO()

    q.to_parquet(path)
    q.to_parquet(buffer)

    assert oda.provenance(path) == q.provenance
    assert pq.read_table(io.BytesIO(buffer.getvalue())).num_rows == 3
    assert q.provenance.tables == {"dac1": DAC1_RELEASE}
    assert q.provenance.indicators == {"oecd.dac1.total_oda": 1}
    assert oda.provenance(path).call == q.provenance.call is not None


FRANCE_2017 = {"years": 2017, "providers": "France", "by": ["year"]}


@pytest.mark.parametrize(
    "change",
    [
        {"providers": "Germany"},
        {"providers": ["France", "Germany"]},
        {"providers": None},
        {"providers": TRIO},
        {"years": 2018},
        {"by": ["provider", "year"]},
        {"unit": constant("EUR", 2023)},
        {"allow_partial": True},
    ],
)
def test_calls_that_differ_record_different_calls(ctx: Context, change) -> None:
    # France and Germany collapsed by year differ only in their values.
    base = ctx.query("oecd.dac1.total_oda", **FRANCE_2017).provenance
    other = ctx.query("oecd.dac1.total_oda", **FRANCE_2017 | change).provenance

    assert other.call != base.call
    assert to_metadata(other) != to_metadata(base)


def test_a_code_shown_beside_a_group_total_records_a_different_call(
    ctx: Context,
) -> None:
    # France sits in the trio either way; only the second call shows its row.
    summed = ctx.query("oecd.dac1.total_oda", years=2017, providers=total(TRIO))
    shown = ctx.query("oecd.dac1.total_oda", years=2017, providers=[4, total(TRIO)])
    assert summed.provenance.call is not None and shown.provenance.call is not None

    assert summed.provenance.call.filters == {"provider": ()}
    assert shown.provenance.call.filters == {"provider": (4,)}
    assert to_metadata(summed.provenance) != to_metadata(shown.provenance)


def test_the_call_record_names_codes_groups_and_default_providers(
    ctx: Context,
) -> None:
    named = ctx.query("oecd.dac1.total_oda", **FRANCE_2017).provenance.call
    grouped = ctx.query(
        "oecd.dac1.total_oda", years=2017, providers=total(TRIO), by=["provider"]
    ).provenance.call
    defaulted = ctx.query("oecd.dac1.total_oda", years=2017).provenance.call
    assert named is not None and grouped is not None and defaulted is not None

    assert dict(named.filters) == {"provider": (4,)}
    assert named.default_providers is None
    assert (named.basis, named.years, named.by) == ("headline", (2017,), ("year",))
    assert named.units == (USD,)
    assert grouped.groups == (
        oda.CallGroup(
            "provider", "trio", "Trio countries", False, True, {2017: (4, 5, 12)}
        ),
    )
    assert defaulted.filters == {}
    providers = defaulted.default_providers or ()
    assert {4, 5, 12} <= set(providers) and list(providers) == sorted(providers)


def test_the_same_call_writes_byte_identical_provenance(ctx: Context) -> None:
    first = ctx.query("oecd.dac1.total_oda", years=[2018, 2017], providers=TRIO)
    second = ctx.query("oecd.dac1.total_oda", years=[2017, 2018], providers=TRIO)

    assert to_metadata(first.provenance) == to_metadata(second.provenance)


def test_to_polars_holds_the_same_rows(ctx: Context) -> None:
    q = ctx.query("oecd.dac1.total_oda", years=2017)

    frame = q.to_polars()

    assert frame["value"].to_list() == q.to_pandas()["value"].tolist()


def test_explain_describes_the_call(ctx: Context) -> None:
    q = ctx.query(
        "oecd.dac1.total_oda",
        providers=[TRIO, total(TRIO)],
        unit=[USD, constant("USD", 2023), constant("EUR", 2023)],
    )

    assert q.explain() == (
        "Total ODA (DAC1), headline basis: net flows 2017, grant equivalent 2018.\n"
        "3 Trio countries, plus their total, which covers all 3 in every year.\n"
        "Current USD, and constant 2023 USD and EUR using DAC deflators from the "
        "same DAC1 release.\n"
        "DAC1 downloaded 2026-10-12. 24 rows."
    )


@pytest.mark.parametrize(
    ("indicator", "first_line"),
    [
        ("oecd.dac1.total_oda", "Total ODA (DAC1), net flows 2017."),
        ("oecd.crs.bilateral_oda", "Bilateral ODA (CRS), net disbursements 2019."),
    ],
)
def test_explain_names_net_disbursements_as_each_table_does(
    ctx: Context, indicator: str, first_line: str
) -> None:
    years = 2017 if indicator.startswith("oecd.dac1") else 2019
    q = ctx.query(indicator, years=years, basis="net_disbursement")

    assert q.explain().splitlines()[0] == first_line


@pytest.mark.parametrize(
    ("years", "expected"),
    [
        (None, "headline basis: net flows"),
        ((2017, 2018), "headline basis: net flows 2017-2018"),
    ],
)
def test_a_headline_of_one_basis_reads_as_one_span(
    years: tuple[int, ...] | None, expected: str
) -> None:
    # DAC2A's headline is net disbursements on both sides of its switch year.
    binding = Binding(
        table="dac1",
        dimensions=DAC1.dimensions,
        bases={
            "net_disbursement": Flow(1140),
            "headline": Headline("net_disbursement", "net_disbursement", year=1960),
        },
    )
    indicator = Indicator(
        id="test.dac1.one_basis",
        name="One basis",
        description="One basis.",
        binding=binding,
        kind=Sum(lines={"net_disbursement": 1010}),
        dimensions=("provider", "year"),
        default_basis="headline",
        default_providers=None,
    )

    assert _context._basis_text(indicator, "headline", years) == expected


def test_explain_states_defaults_and_fallback_deflators(tables: FakeTables) -> None:
    ctx = Context._from_parts(tables, FakeFactors(fallback=frozenset({918})))
    q = ctx.query("oecd.crs.bilateral_oda", unit=constant("USD", 2023))

    text = q.explain().splitlines()

    assert text[1].endswith("aggregates and private donors excluded (the default).")
    assert text[2].startswith("Recipients: every recipient in the table")
    assert "EU Institutions took the DAC total's deflators" in text[3]
    assert text[4] == "CRS downloaded 2026-09-01; DAC1 downloaded 2026-10-12. 2 rows."


# Arguments --------------------------------------------------------------------------


def test_a_line_on_a_basis_the_indicator_lacks_raises(ctx: Context) -> None:
    with pytest.raises(UnsupportedQueryError, match="gross_disbursement"):
        ctx.get("oecd.dac1.lines", lines=1010, basis="gross_disbursement")


def test_unknown_indicator_suggests_near_ids(ctx: Context) -> None:
    with pytest.raises(UnknownIndicatorError) as error:
        ctx.query("oecd.dac1.total_odaa")

    assert error.value.suggestions[0] == "oecd.dac1.total_oda"


def test_a_2x_code_raises_with_its_3_0_call_before_reading_a_table(
    ctx: Context, tables: FakeTables
) -> None:
    with pytest.raises(UnknownIndicatorError) as error:
        ctx.get("DAC1.10.1010", years=2017)

    call = 'oda.get("oecd.dac1.total_oda", basis="net_disbursement")'
    assert error.value.suggestions == (call,)
    assert tables.cursors == 0


@pytest.mark.parametrize(
    ("arguments", "error"),
    [
        ({"by": ["provider", "sectors"]}, DimensionError),
        ({"years": []}, ValueError),
        ({"years": "2020"}, TypeError),
        ({"unit": "USD"}, TypeError),
        # A currency oda-data cannot convert to, refused before any table read.
        ({"unit": [USD, constant("XYZ", 2023)]}, UnsupportedQueryError),
    ],
)
def test_bad_arguments_fail_before_the_query_runs(
    ctx: Context, tables: FakeTables, arguments: dict, error: type[Exception]
) -> None:
    with pytest.raises(error):
        ctx.query("oecd.dac1.total_oda", **arguments)
    assert tables.cursors == 0


# Context lifetime and the default context -------------------------------------------


def test_leaving_the_block_closes_the_tables_and_drops_factors(
    tables: FakeTables,
) -> None:
    built: list[FakeFactors] = []

    def make() -> FakeFactors:
        built.append(FakeFactors())
        return built[-1]

    ctx = Context._from_parts(tables, FakeFactors())
    ctx._make_factors = make
    with ctx:
        ctx.get("oecd.dac1.total_oda", years=2017)
    ctx.get("oecd.dac1.total_oda", years=2017)

    assert tables.closed
    assert len(built) == 2


class _LoggedCursor:
    """A DuckDB cursor that records the SQL text of each ``sql`` call."""

    def __init__(self, cursor: duckdb.DuckDBPyConnection, log: list[str]) -> None:
        self._cursor = cursor
        self._log = log

    def sql(self, query: str, *args: object, **kwargs: object) -> object:
        self._log.append(query)
        return self._cursor.sql(query, *args, **kwargs)

    def __getattr__(self, name: str) -> object:
        return getattr(self._cursor, name)


def test_table_scans_run_once_per_held_release(
    tables: FakeTables, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The distinct years and providers of a table and its last year are read
    once per release across calls, again for a new release, and again after
    ``close()``."""
    log: list[str] = []
    cursor = tables.cursor
    monkeypatch.setattr(
        tables, "cursor", lambda names: _LoggedCursor(cursor(names), log)
    )
    held = {"crs": CRS_RELEASE}
    monkeypatch.setattr(tables, "release", held.__getitem__)

    def scans() -> Counter[str]:
        kinds = {
            'DISTINCT "year"': "years",
            'DISTINCT "provider"': "providers",
            'SELECT max(CAST("year" AS BIGINT)) FROM "crs"': "last year",
        }
        return Counter(
            kind
            for query in log
            for text, kind in kinds.items()
            if text in query and "GROUP BY" not in query
        )

    ctx = Context._from_parts(tables, FakeFactors())
    for _ in range(2):
        ctx.get("oecd.crs.bilateral_oda", by=["year"])
    assert scans() == {"years": 1, "providers": 1, "last year": 1}

    held["crs"] = Release("v20261001", datetime(2026, 10, 1))
    ctx.get("oecd.crs.bilateral_oda", by=["year"])
    assert scans() == {"years": 2, "providers": 2, "last year": 2}

    ctx.close()
    ctx.get("oecd.crs.bilateral_oda", by=["year"])
    assert scans() == {"years": 3, "providers": 3, "last year": 3}


def test_the_default_context_is_built_once_across_threads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    built: list[object] = []
    barrier = threading.Barrier(8)

    class Counting:
        def __init__(self) -> None:
            built.append(self)

    monkeypatch.setattr(_context, "_default", None)
    monkeypatch.setattr(_context, "Context", Counting)
    seen: list[object] = []

    def call() -> None:
        barrier.wait()
        seen.append(_context.default_context())

    threads = [threading.Thread(target=call) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(built) == 1
    assert len(seen) == 8
    assert all(context is built[0] for context in seen)


def test_module_functions_use_the_default_context(
    monkeypatch: pytest.MonkeyPatch, ctx: Context
) -> None:
    monkeypatch.setattr(_context, "_default", ctx)

    df = oda.get("oecd.dac1.total_oda", years=2017, by="provider")
    rows = oda.records("oecd.dac1.total_oda", years=2017)

    assert _rows(df, "provider", "value") == [("4", 10.0), ("5", 20.0), ("12", 30.0)]
    assert len(rows) == 3
    assert isinstance(oda.query("oecd.dac1.total_oda"), oda.Query)


def test_a_filter_outside_by_narrows_the_rows(ctx: Context) -> None:
    df = ctx.get("oecd.crs.bilateral_oda", recipients="Kenya", by=["broad_sector"])

    assert _rows(df, "broad_sector", "value") == [("education", 2.0), ("health", 3.0)]

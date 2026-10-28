"""Reporting gaps at each table's reporting grain, on in-memory tables.

2025 is the last year of every table here. On DAC1, France (4) reports total
ODA in 2025 and not yet in-donor refugee costs (line 1820), which it reported
in 2024; Germany (5) reports both. Austria (1) reports nothing in 2025.
Italy (6) splits its gross disbursements into grants (flow 1121) in 2024 and
not yet in 2025.
"""

import warnings
from collections.abc import Iterator

import duckdb
import pyarrow as pa
import pytest

from oda_data._combine import absent_text, combine
from oda_data._completeness import Outcome, holding, outcomes
from oda_data._errors import OdaDataWarning
from oda_data._model import (
    Binding,
    Flow,
    In,
    Incomplete,
    Indicator,
    Line,
    ResolvedCall,
    ResolvedGroup,
    Sum,
    Value,
)
from oda_data._registry import dac1, one
from oda_data._store import UNCACHED
from oda_data._units import current

USD = current("USD")
AUSTRIA, FRANCE, GERMANY, ITALY = 1, 4, 5, 6
KENYA, EGYPT = 248, 142

DAC1_ROWS = [
    # provider, year, line, flow, value
    (FRANCE, 2024, 11010, 1160, 100.0),
    (FRANCE, 2024, 1820, 1160, 10.0),
    (FRANCE, 2025, 11010, 1160, 110.0),
    (GERMANY, 2024, 11010, 1160, 200.0),
    (GERMANY, 2024, 1820, 1160, 20.0),
    (GERMANY, 2025, 11010, 1160, 210.0),
    (GERMANY, 2025, 1820, 1160, 22.0),
    (AUSTRIA, 2024, 11010, 1160, 30.0),
    (AUSTRIA, 2024, 1820, 1160, 3.0),
    # Gross disbursements (1120) and their parts, grants and non-grants.
    (FRANCE, 2024, 1010, 1120, 90.0),
    (FRANCE, 2024, 1010, 1121, 60.0),
    (FRANCE, 2024, 1010, 1122, 30.0),
    (FRANCE, 2024, 1820, 1120, 9.0),
    (FRANCE, 2024, 1820, 1121, 9.0),
    (GERMANY, 2024, 1010, 1120, 180.0),
    (GERMANY, 2024, 1010, 1121, 120.0),
    (GERMANY, 2024, 1010, 1122, 60.0),
    (GERMANY, 2024, 1820, 1120, 18.0),
    (GERMANY, 2024, 1820, 1121, 18.0),
    (GERMANY, 2025, 1010, 1120, 190.0),
    (GERMANY, 2025, 1010, 1121, 130.0),
    (GERMANY, 2025, 1010, 1122, 60.0),
    (GERMANY, 2025, 1820, 1120, 19.0),
    (GERMANY, 2025, 1820, 1121, 19.0),
    (FRANCE, 2025, 1010, 1120, 95.0),
    (FRANCE, 2025, 1010, 1121, 70.0),
    (FRANCE, 2025, 1010, 1122, 25.0),
    (ITALY, 2024, 1010, 1120, 50.0),
    (ITALY, 2024, 1010, 1121, 50.0),
    (ITALY, 2025, 1010, 1120, 55.0),
]

CRS_ROWS = [
    # provider, recipient, sector, channel, year, value. Provider 1 gave Kenya,
    # sector 110 and channel 1000 money in 2024 only, and reports 2025.
    (AUSTRIA, KENYA, 110, 1000, 2024, 3.0),
    (AUSTRIA, EGYPT, 120, 2000, 2025, 4.0),
    (FRANCE, KENYA, 110, 1000, 2024, 6.0),
    (FRANCE, KENYA, 110, 1000, 2025, 7.0),
    (GERMANY, KENYA, 110, 1000, 2024, 5.0),  # Germany has no 2025 row
]

DAC2A_ROWS = [
    # provider, recipient, year, line, value
    (AUSTRIA, KENYA, 2024, 206, 3.0),
    (AUSTRIA, EGYPT, 2025, 206, 4.0),
    (FRANCE, KENYA, 2024, 206, 6.0),
    (FRANCE, KENYA, 2025, 206, 7.0),
    (GERMANY, KENYA, 2024, 206, 5.0),
]


@pytest.fixture
def con() -> Iterator[duckdb.DuckDBPyConnection]:
    connection = duckdb.connect()
    connection.execute(
        "CREATE TABLE dac1 (provider BIGINT, year BIGINT, line BIGINT, flow BIGINT,"
        " amount_type VARCHAR, value DOUBLE)"
    )
    connection.executemany("INSERT INTO dac1 VALUES (?, ?, ?, ?, 'A', ?)", DAC1_ROWS)
    connection.execute(
        "CREATE TABLE crs (provider BIGINT, recipient BIGINT, sector BIGINT,"
        " channel BIGINT, year BIGINT, disbursement DOUBLE)"
    )
    connection.executemany("INSERT INTO crs VALUES (?, ?, ?, ?, ?, ?)", CRS_ROWS)
    connection.execute(
        "CREATE TABLE dac2a (provider BIGINT, recipient BIGINT, year BIGINT,"
        " line BIGINT, amount_type VARCHAR, value DOUBLE)"
    )
    connection.executemany("INSERT INTO dac2a VALUES (?, ?, ?, ?, 'A', ?)", DAC2A_ROWS)
    yield connection
    connection.close()


LINES = Indicator(
    id="oecd.dac1.lines",
    name="DAC1 lines",
    description="DAC1 lines.",
    binding=Binding(
        table="dac1",
        dimensions={
            "provider": "provider",
            "line": "line",
            "flow": "flow",
            "year": "year",
        },
        bases={
            "grant_equivalent": Flow(1160, only_lines=(11010,)),
            "gross_disbursement": Flow(1120, parts=(1121, 1122)),
        },
        rows=(In("amount_type", ("A",)),),
        reporting=("line",),
    ),
    kind=Sum(),
    dimensions=("provider", "year", "line", "flow"),
    default_basis="grant_equivalent",
    default_providers=None,
)

_CRS_DIMENSIONS = ("provider", "recipient", "sector", "channel", "year")
CRS_ODA = Indicator(
    id="oecd.crs.bilateral_oda",
    name="Bilateral ODA",
    description="Bilateral ODA.",
    binding=Binding(
        table="crs",
        dimensions={d: d for d in _CRS_DIMENSIONS},
        bases={"gross_disbursement": Value("disbursement")},
        reporting=(),
    ),
    kind=Sum(),
    dimensions=_CRS_DIMENSIONS,
    default_basis="gross_disbursement",
    default_providers=None,
)

RECIPIENT_ODA = Indicator(
    id="oecd.dac2a.recipient_oda",
    name="Recipient ODA",
    description="ODA by recipient.",
    binding=Binding(
        table="dac2a",
        dimensions={"provider": "provider", "recipient": "recipient", "year": "year"},
        bases={"net_disbursement": Line(206)},
        rows=(In("amount_type", ("A",)),),
        reporting=("line",),
    ),
    kind=Sum(),
    dimensions=("provider", "recipient", "year"),
    default_basis="net_disbursement",
    default_providers=None,
)


def lines_call(
    lines: tuple[int, ...],
    *,
    basis: str = "grant_equivalent",
    flows: tuple[int, ...] = (),
    providers: frozenset[int] = frozenset({FRANCE, GERMANY}),
    by: tuple[str, ...] = ("provider", "year"),
    total: bool = True,
    allow_partial: bool = False,
) -> ResolvedCall:
    """A call on ``LINES`` for 2025, with a total over ``providers`` alone
    when ``total`` is true and the providers summed outside ``by`` when not."""
    members = {"provider": providers, "line": frozenset(lines)}
    if flows:
        members["flow"] = frozenset(flows)
    group = ResolvedGroup("pair", "provider", {2025: providers})
    return ResolvedCall(
        basis=basis,
        units=(USD,),
        by=by,
        years=(2025,),
        members=members,
        groups=(group,) if total else (),
        totals=(group.key,) if total else (),
        rows={"provider": frozenset()} if total else {},
        allow_partial=allow_partial,
    )


def by_line(table: pa.Table) -> dict[tuple, float | None]:
    return {(row["line"], row.get("flow")): row["value"] for row in table.to_pylist()}


@pytest.mark.parametrize("allow_partial", [False, True], ids=["null", "allow-partial"])
@pytest.mark.parametrize(
    ("basis", "lines", "flows"),
    [
        ("grant_equivalent", (11010, 1820), ()),
        ("gross_disbursement", (1010, 1820), ()),
        ("gross_disbursement", (1010, 1820), (1121, 1122)),
    ],
    ids=["grant-equivalent", "gross", "gross-by-flow"],
)
def test_bundled_lines_give_what_separate_calls_give(
    con: duckdb.DuckDBPyConnection,
    allow_partial: bool,
    basis: str,
    lines: tuple[int, ...],
    flows: tuple[int, ...],
) -> None:
    """France has not reported line 1820 for 2025, so the pair's 1820 total
    is null, or Germany's alone with allow_partial, whether the call names
    1820 alone or beside a line France has reported, whose total holds both."""

    def run(named: tuple[int, ...]) -> dict:
        found = combine(
            con,
            LINES,
            lines_call(named, basis=basis, flows=flows, allow_partial=allow_partial),
            FakeFactors(),
            releases=UNCACHED,
        )
        return by_line(found.table)

    bundled = run(lines)
    separate = run(lines[:1]) | run(lines[1:])

    assert bundled == separate
    refugees = [value for (line, _), value in bundled.items() if line == "1820"]
    others = [value for (line, _), value in bundled.items() if line != "1820"]
    assert refugees and others
    assert all(value is not None for value in others)
    if allow_partial:
        assert all(value is not None for value in refugees)
    else:
        assert all(value is None for value in refugees)


def test_a_line_one_member_has_not_reported_leaves_other_totals_whole(
    con: duckdb.DuckDBPyConnection,
) -> None:
    found = combine(
        con, LINES, lines_call((11010, 1820)), FakeFactors(), releases=UNCACHED
    )

    assert by_line(found.table) == {("11010", None): 320.0, ("1820", None): None}
    assert found.incomplete == (
        Incomplete(FRANCE, 2025, frozenset({FRANCE}), keys=(("line", 1820),)),
    )


def test_a_provider_that_stopped_has_one_cell_for_the_whole_year(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """Austria has no 2025 row, so it has stopped, and its cell covers each
    line it had reported."""
    providers = frozenset({AUSTRIA, GERMANY})

    found = combine(
        con,
        LINES,
        lines_call((11010, 1820), providers=providers),
        FakeFactors(),
        releases=UNCACHED,
    )

    assert by_line(found.table) == {("11010", None): None, ("1820", None): None}
    assert found.incomplete == (Incomplete(AUSTRIA, 2025, frozenset({AUSTRIA})),)


def test_a_sum_across_providers_warns_of_the_line_not_yet_reported(
    con: duckdb.DuckDBPyConnection,
) -> None:
    call = lines_call((11010, 1820), by=("year",), total=False)

    with pytest.warns(
        OdaDataWarning,
        match="2025 is partial: 1 provider that reported line 1820 in 2024 has "
        "not reported it in 2025",
    ):
        found = combine(con, LINES, call, FakeFactors(), releases=UNCACHED)

    assert by_line(found.table) == {("11010", None): 320.0, ("1820", None): 22.0}


@pytest.mark.parametrize(
    ("indicator", "dimension", "code", "basis"),
    [
        (CRS_ODA, "recipient", KENYA, "gross_disbursement"),
        (CRS_ODA, "sector", 110, "gross_disbursement"),
        (CRS_ODA, "channel", 1000, "gross_disbursement"),
        (RECIPIENT_ODA, "recipient", KENYA, "net_disbursement"),
    ],
    ids=["crs-recipient", "crs-sector", "crs-channel", "dac2a-recipient"],
)
@pytest.mark.parametrize(
    ("providers", "expected"),
    [({AUSTRIA, FRANCE}, 7.0), ({FRANCE, GERMANY}, None)],
    ids=["an-empty-slice-is-zero", "a-provider-with-no-row-is-null"],
)
def test_an_activity_slice_with_no_rows_in_the_last_year_counts_as_zero(
    con: duckdb.DuckDBPyConnection,
    indicator: Indicator,
    dimension: str,
    code: int,
    basis: str,
    providers: set[int],
    expected: float | None,
) -> None:
    """Austria reports 2025 with no row for the slice, which counts as zero;
    Germany has no 2025 row at all, so a total holding it is null."""
    group = ResolvedGroup("pair", "provider", {2025: frozenset(providers)})
    call = ResolvedCall(
        basis=basis,
        units=(USD,),
        by=("provider",),
        years=(2025,),
        members={"provider": frozenset(providers), dimension: frozenset({code})},
        groups=(group,),
        totals=(group.key,),
        rows={"provider": frozenset()},
    )

    found = combine(con, indicator, call, FakeFactors(), releases=UNCACHED)

    assert found.table.column("value").to_pylist() == [expected]


def grants_call(
    indicator: Indicator,
    *,
    by: tuple[str, ...] = ("provider", "year"),
    total: bool = True,
    allow_partial: bool = False,
) -> ResolvedCall:
    """A 2025 call on Italy's and Germany's grants (flow 1121) of line 1010,
    with a total over the two when ``total`` is true."""
    providers = frozenset({ITALY, GERMANY})
    members = {"provider": providers, "flow": frozenset({1121})}
    if "line" in indicator.dimensions:
        members["line"] = frozenset({1010})
    group = ResolvedGroup("pair", "provider", {2025: providers})
    return ResolvedCall(
        basis="gross_disbursement",
        units=(USD,),
        by=by,
        years=(2025,),
        members=members,
        groups=(group,) if total else (),
        totals=(group.key,) if total else (),
        rows={"provider": frozenset()} if total else {},
        allow_partial=allow_partial,
    )


_DAC1_INDICATORS = pytest.mark.parametrize(
    "indicator", [dac1.TOTAL_ODA, dac1.LINES], ids=["total-oda", "lines"]
)


@_DAC1_INDICATORS
@pytest.mark.parametrize(
    ("allow_partial", "expected"),
    [(False, None), (True, 130.0)],
    ids=["null", "allow-partial"],
)
def test_a_flow_part_not_yet_reported_nulls_the_provider_total(
    con: duckdb.DuckDBPyConnection,
    indicator: Indicator,
    allow_partial: bool,
    expected: float | None,
) -> None:
    """Italy reported grants in 2024 and only gross disbursements in 2025, so
    the pair's 2025 grants total is null, or Germany's grants alone with
    allow_partial."""
    call = grants_call(indicator, allow_partial=allow_partial)

    found = combine(con, indicator, call, FakeFactors(), releases=UNCACHED)

    assert found.table.column("value").to_pylist() == [expected]


@_DAC1_INDICATORS
def test_a_sum_across_providers_warns_of_a_flow_part_not_yet_reported(
    con: duckdb.DuckDBPyConnection, indicator: Indicator
) -> None:
    call = grants_call(indicator, by=("year",), total=False)

    with pytest.warns(
        OdaDataWarning,
        match="2025 is partial: 1 provider that reported line 1010, flow 1121 in "
        "2024 has not reported it in 2025",
    ):
        found = combine(con, indicator, call, FakeFactors(), releases=UNCACHED)

    assert found.table.column("value").to_pylist() == [130.0]


def test_explain_names_a_part_a_shown_provider_has_no_row_for(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """Italy shows on its own row and has no 2025 grants row, so no row holds
    its cell and explain() names it."""
    call = grants_call(dac1.TOTAL_ODA, total=False)

    found = combine(con, dac1.TOTAL_ODA, call, FakeFactors(), releases=UNCACHED)

    assert found.table.column("provider").to_pylist() == ["5"]
    assert found.incomplete == ()
    assert absent_text(2025, [ITALY], part="line 1010, flow 1121") in found.notes


def core_call(
    providers: frozenset[int], *, allow_partial: bool = False
) -> ResolvedCall:
    """A 2025 call on core ODA on grant equivalents, summed across
    ``providers`` by year."""
    return ResolvedCall(
        basis="grant_equivalent",
        units=(USD,),
        by=("year",),
        years=(2025,),
        members={"provider": providers},
        allow_partial=allow_partial,
    )


@pytest.mark.parametrize(
    ("allow_partial", "expected"),
    [(False, None), (True, 110.0 + 210.0 - 22.0)],
    ids=["null", "allow-partial"],
)
def test_a_sum_that_holds_a_subtracted_part_not_yet_reported_is_null(
    con: duckdb.DuckDBPyConnection, allow_partial: bool, expected: float | None
) -> None:
    """France's 2025 total ODA is in the sum and its refugee costs are not
    yet taken out, so the sum would overstate. Austria has not reported 2025,
    which alone leaves the sum short and warns, but a null sum says why it is
    null and does not also warn."""
    providers = frozenset({AUSTRIA, FRANCE, GERMANY})
    call = core_call(providers, allow_partial=allow_partial)

    with warnings.catch_warnings():
        warnings.simplefilter("error", OdaDataWarning)
        found = combine(con, one.CORE_ODA, call, FakeFactors(), releases=UNCACHED)

    assert found.table.column("value").to_pylist() == [expected]
    assert Incomplete(
        FRANCE, 2025, frozenset({FRANCE}), keys=(("line", 1820), ("flow", 1160))
    ) in set(found.incomplete)


def test_a_sum_without_the_provider_that_lacks_a_subtracted_part_keeps_its_value(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """Austria has no 2025 row, so its missing refugee costs subtract nothing
    from the sum, which warns that it covers only Germany."""
    call = core_call(frozenset({AUSTRIA, GERMANY}))

    with pytest.warns(OdaDataWarning, match="2025 is partial: 1 provider"):
        found = combine(con, one.CORE_ODA, call, FakeFactors(), releases=UNCACHED)

    assert found.table.column("value").to_pylist() == [210.0 - 22.0]


_LAST = 2025
_WAITING = Incomplete(FRANCE, _LAST, frozenset({FRANCE}), keys=(("line", 1820),))
_STOPPED = Incomplete(AUSTRIA, _LAST, frozenset({AUSTRIA}))
_STOPPED_BEFORE = Incomplete(AUSTRIA, _LAST - 1, frozenset({AUSTRIA}))
_METHOD = Incomplete(918, _LAST, frozenset({FRANCE}), inputs="line 2102")


@pytest.mark.parametrize(
    ("cell", "own", "ratio", "subtracted", "outcome"),
    [
        (_WAITING, {_LAST}, False, False, Outcome.NULL_ROW),
        (_WAITING, set(), False, False, Outcome.WARN_SUM),
        (_WAITING, set(), False, True, Outcome.NULL_SUM),
        (_WAITING, set(), True, False, Outcome.WARN_SUM),
        (_METHOD, set(), False, False, Outcome.NULL_SUM),
        (_STOPPED, set(), False, False, Outcome.NOTE),
        (_STOPPED, set(), True, False, Outcome.NULL_SUM),
    ],
    ids=[
        "own-row",
        "not-yet-reported",
        "subtracted",
        "ratio-not-yet-reported",
        "method",
        "stopped-before",
        "ratio-side-stopped",
    ],
)
def test_each_cell_has_one_outcome(
    cell: Incomplete, own: set[int], ratio: bool, subtracted: bool, outcome: Outcome
) -> None:
    """A provider that stopped before the last year has a cell in the year
    before too, so its last-year cell is not a part not yet reported."""
    providers = frozenset({AUSTRIA, FRANCE, 918})
    call = ResolvedCall(
        basis="grant_equivalent",
        units=(USD,),
        by=("year",),
        years=(_LAST - 1, _LAST),
        members={"provider": providers},
    )
    cells = frozenset({cell, _STOPPED_BEFORE})

    found = outcomes(
        cells,
        call,
        own=own,
        years=[_LAST - 1, _LAST],
        last=_LAST,
        ratio=ratio,
        subtracted=frozenset({cell}) if subtracted else frozenset(),
    )

    assert found[cell] is outcome


def test_a_row_that_holds_two_cells_is_marked_once(
    con: duckdb.DuckDBPyConnection,
) -> None:
    rows = con.sql("SELECT 4 AS provider, 2025 AS year, 1820 AS line, 1.0 AS value")
    cells = [
        Incomplete(FRANCE, 2025, frozenset({FRANCE})),
        Incomplete(FRANCE, 2025, frozenset({7}), keys=(("line", 1820),)),
        Incomplete(FRANCE, 2025, frozenset({8}), keys=(("line", 1010),)),
    ]

    marked = holding(con, rows, cells).fetchall()

    assert marked == [(4, 2025, 1820, 1.0, True)]


class FakeFactors:
    """Current USD only, whose factor is 1."""

    def table(self, keys: pa.Table, units: tuple, /) -> object:
        raise AssertionError("current USD needs no factors")

    def release(self) -> object:
        raise NotImplementedError

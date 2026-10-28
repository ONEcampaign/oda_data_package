"""Reporting gaps at each table's reporting grain, on in-memory tables.

2025 is the last year of every table here. On DAC1, France (4) reports total
ODA in 2025 and not yet in-donor refugee costs (line 1820), which it reported
in 2024; Germany (5) reports both. Austria (1) reports nothing in 2025.
Italy (6) splits its gross disbursements into grants (flow 1121) in 2024 and
not yet in 2025.
"""

import re
import warnings
from collections.abc import Iterator
from contextlib import nullcontext
from dataclasses import replace

import duckdb
import pyarrow as pa
import pytest

from oda_data._combine import (
    Effect,
    LastYearPart,
    combine,
    last_year_warning,
    unreported_text,
    unsummed_text,
)
from oda_data._completeness import Outcome, holding, outcomes
from oda_data._errors import OdaDataWarning
from oda_data._model import (
    Binding,
    Derived,
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
from oda_data._registry import dac1, dac2a, one
from oda_data._store import UNCACHED
from oda_data._units import current

USD = current("USD")
#: The opening of every warning on a part of 2025 not yet reported.
PARTIAL = (
    "2025 is partial: providers that reported a part in 2024 have not reported it "
    "in 2025. "
)
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

MULTISYSTEM_ODA = Indicator(
    id="oecd.multisystem.core_contributions",
    name="Core contributions",
    description="Core contributions.",
    binding=Binding(
        table="multisystem",
        dimensions={"provider": "provider", "year": "year"},
        bases={"gross_disbursement": Value("value")},
        reporting=(),
    ),
    kind=Sum(),
    dimensions=("provider", "year"),
    default_basis="gross_disbursement",
    default_providers=None,
)

#: CRS bilateral ODA plus Multisystem core contributions, as
#: ``one.crs.bilateral_plus_multilateral_oda`` adds them.
BOTH_TABLES = Indicator(
    id="one.crs.bilateral_plus_multilateral_oda",
    name="Bilateral and multilateral ODA",
    description="Bilateral and multilateral ODA.",
    binding=MULTISYSTEM_ODA.binding,
    kind=Derived(plus=(CRS_ODA, MULTISYSTEM_ODA)),
    dimensions=("provider", "year"),
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
        short = allow_partial and 1820 in named
        with (
            pytest.warns(OdaDataWarning, match="short in the pair total")
            if short
            else nullcontext()
        ):
            found = combine(
                con,
                LINES,
                lines_call(
                    named, basis=basis, flows=flows, allow_partial=allow_partial
                ),
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

    with pytest.warns(OdaDataWarning) as caught:
        found = combine(con, LINES, call, FakeFactors(), releases=UNCACHED)

    assert [str(w.message) for w in caught] == [
        f"{LINES.id}: {PARTIAL}Line 1820 is left out of the sum for 1 provider. "
        "Show the providers on their own rows to see who reported."
    ]
    assert (
        "2025 is partial: 1 provider that reported line 1820 in 2024 has not "
        "reported it in 2025, so its sum covers only those that have; add "
        "'provider' to by to see who reported"
    ) in found.notes

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


DEVELOPING_COUNTRIES, IDA = 10100, 905


@pytest.mark.parametrize(
    ("rows_2025", "recipient", "expected"),
    [
        (
            [(FRANCE, DEVELOPING_COUNTRIES, 11.0), (GERMANY, KENYA, 5.0)],
            DEVELOPING_COUNTRIES,
            [11.0],
        ),
        ([(FRANCE, IDA, 2.0), (GERMANY, KENYA, 5.0)], KENYA, [5.0]),
    ],
    ids=["only-a-recipient-total", "only-an-excluded-recipient"],
)
def test_a_provider_with_last_year_rows_outside_the_slice_has_reported(
    rows_2025: list[tuple[int, int, float]],
    recipient: int,
    expected: list[float],
) -> None:
    """France reported line 206 by recipient and in total in 2024. Its 2025
    rows go only to the developing-countries total (10100), which the screen
    of recipient totals leaves out of a sum, or only to IDA (905), which
    DAC2A leaves out as a recipient. Either way France has reported line 206
    for 2025, so the pair's total holds its value, and a Kenya slice without
    it is a zero."""
    connection = duckdb.connect()
    connection.execute(
        "CREATE TABLE dac2a (provider BIGINT, recipient BIGINT, year BIGINT,"
        " line BIGINT, amount_type VARCHAR, value DOUBLE)"
    )
    rows = [
        (FRANCE, KENYA, 2024, 6.0),
        (FRANCE, DEVELOPING_COUNTRIES, 2024, 10.0),
        (GERMANY, KENYA, 2024, 4.0),
        *((provider, code, 2025, value) for provider, code, value in rows_2025),
    ]
    connection.executemany("INSERT INTO dac2a VALUES (?, ?, ?, 206, 'A', ?)", rows)
    providers = frozenset({FRANCE, GERMANY})
    group = ResolvedGroup("pair", "provider", {2025: providers})
    call = ResolvedCall(
        basis="net_disbursement",
        units=(USD,),
        by=("provider",),
        years=(2025,),
        members={"provider": providers, "recipient": frozenset({recipient})},
        groups=(group,),
        totals=(group.key,),
        rows={"provider": frozenset()},
    )

    found = combine(
        connection, dac2a.RECIPIENT_ODA, call, FakeFactors(), releases=UNCACHED
    )
    connection.close()

    assert found.table.column("value").to_pylist() == expected
    assert found.incomplete == ()


MULTILATERAL = 901


@pytest.mark.parametrize(
    ("lines", "excluded"),
    [((106,), True), ((106, 206), False)],
    ids=["excluded-on-every-line-read", "counted-on-another-line"],
)
def test_a_provider_the_call_leaves_out_has_no_cell(
    lines: tuple[int, ...], excluded: bool
) -> None:
    """``oecd.dac2a.lines`` leaves multilateral providers out of line 106.
    A multilateral (901) that reported lines 106 and 206 in 2024 and nothing
    in 2025 has stopped, but a call on line 106 alone never counts it, so the
    sum by year holds France's value with no cell and no warning. A call that
    also reads line 206 counts it there, so the sum warns of its stop."""
    connection = duckdb.connect()
    connection.execute(
        "CREATE TABLE dac2a (provider BIGINT, recipient BIGINT, year BIGINT,"
        " line BIGINT, amount_type VARCHAR, value DOUBLE)"
    )
    rows = [
        (FRANCE, KENYA, 2024, 106, 6.0),
        (FRANCE, KENYA, 2025, 106, 7.0),
        (FRANCE, KENYA, 2024, 206, 6.0),
        (FRANCE, KENYA, 2025, 206, 8.0),
        (MULTILATERAL, KENYA, 2024, 106, 2.0),
        (MULTILATERAL, KENYA, 2024, 206, 3.0),
    ]
    connection.executemany("INSERT INTO dac2a VALUES (?, ?, ?, ?, 'A', ?)", rows)
    call = ResolvedCall(
        basis="net_disbursement",
        units=(USD,),
        by=("year",),
        years=(2025,),
        members={
            "provider": frozenset({FRANCE, MULTILATERAL}),
            "line": frozenset(lines),
        },
    )

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        found = combine(connection, dac2a.LINES, call, FakeFactors(), releases=UNCACHED)
    connection.close()

    partial = [w for w in caught if "is partial" in str(w.message)]
    if excluded:
        assert by_line(found.table) == {("106", None): 7.0}
        assert found.incomplete == ()
        assert partial == []
    else:
        assert by_line(found.table) == {("106", None): 7.0, ("206", None): 8.0}
        assert found.incomplete == (
            Incomplete(MULTILATERAL, 2025, frozenset({MULTILATERAL})),
        )
        assert len(partial) == 1


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
    short = "Line 1010, flow 1121 is short in the pair total, which lacks Italy (6)."

    with (
        pytest.warns(OdaDataWarning, match=re.escape(short))
        if allow_partial
        else nullcontext()
    ):
        found = combine(con, indicator, call, FakeFactors(), releases=UNCACHED)

    assert found.table.column("value").to_pylist() == [expected]


@_DAC1_INDICATORS
def test_a_sum_across_providers_warns_of_a_flow_part_not_yet_reported(
    con: duckdb.DuckDBPyConnection, indicator: Indicator
) -> None:
    call = grants_call(indicator, by=("year",), total=False)

    with pytest.warns(
        OdaDataWarning,
        match=re.escape(
            f"{PARTIAL}Line 1010, flow 1121 is left out of the sum for 1 provider."
        ),
    ):
        found = combine(con, indicator, call, FakeFactors(), releases=UNCACHED)

    assert found.table.column("value").to_pylist() == [130.0]


def test_explain_names_a_part_a_shown_provider_has_no_row_for(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """Italy shows on its own row and has no 2025 grants row, so no row holds
    its cell, and the result warns of it and explain() names it."""
    call = grants_call(dac1.TOTAL_ODA, total=False)

    with pytest.warns(OdaDataWarning, match="2025 is partial") as caught:
        found = combine(con, dac1.TOTAL_ODA, call, FakeFactors(), releases=UNCACHED)

    assert found.table.column("provider").to_pylist() == ["5"]
    assert found.incomplete == ()
    part = "line 1010, flow 1121"
    assert [str(w.message) for w in caught] == [
        f"oecd.dac1.total_oda: {PARTIAL}Line 1010, flow 1121 is missing for Italy (6)."
    ]
    assert unreported_text(2025, [ITALY], part=part, nulled=False) in found.notes


def refugee_call(providers: frozenset[int], years: tuple[int, ...]) -> ResolvedCall:
    """A call on in-donor refugee costs by provider and year."""
    return ResolvedCall(
        basis="grant_equivalent",
        units=(USD,),
        by=("provider", "year"),
        years=years,
        members={"provider": providers},
    )


@pytest.mark.parametrize(
    ("years", "expected"),
    [((2024, 2025), [("4", 2024, 10.0)]), ((2025,), [])],
    ids=["a-short-result", "an-empty-result"],
)
def test_a_shown_provider_with_no_row_for_a_part_of_the_last_year_warns(
    con: duckdb.DuckDBPyConnection, years: tuple[int, ...], expected: list
) -> None:
    """France reported refugee costs in 2024 and not yet in 2025, so the
    result has no 2025 row, and warns of it whether or not a row survives."""
    call = refugee_call(frozenset({FRANCE}), years)
    indicator = dac1.IN_DONOR_REFUGEE_COSTS

    with pytest.warns(OdaDataWarning) as caught:
        found = combine(con, indicator, call, FakeFactors(), releases=UNCACHED)

    part = "line 1820, flow 1160"
    assert [str(w.message) for w in caught] == [
        f"{indicator.id}: {PARTIAL}Line 1820, flow 1160 is missing for France (4)."
    ]
    text = unreported_text(2025, [FRANCE], part=part, nulled=False)
    assert text.startswith("2025 is partial: France (4) reported line 1820")
    assert text in found.notes
    assert found.incomplete == ()
    columns = found.table.select(["provider", "year", "value"]).to_pydict().values()
    assert list(zip(*columns, strict=True)) == expected


@pytest.mark.parametrize("total", [False, True], ids=["by-year", "provider-total"])
def test_a_sum_none_of_whose_providers_reported_the_last_year_warns(
    con: duckdb.DuckDBPyConnection, total: bool
) -> None:
    """France has not yet reported 2025 refugee costs and Austria nothing of
    2025, so a sum over the two has no 2025 row, and warns of it."""
    pair = frozenset({AUSTRIA, FRANCE})
    group = ResolvedGroup("pair", "provider", {2024: pair, 2025: pair})
    call = ResolvedCall(
        basis="grant_equivalent",
        units=(USD,),
        by=("provider", "year") if total else ("year",),
        years=(2024, 2025),
        members={"provider": pair},
        groups=(group,) if total else (),
        totals=(group.key,) if total else (),
        rows={"provider": frozenset()} if total else {},
    )
    indicator = dac1.IN_DONOR_REFUGEE_COSTS

    with pytest.warns(OdaDataWarning) as caught:
        found = combine(con, indicator, call, FakeFactors(), releases=UNCACHED)

    where = "the pair total" if total else "the sum"
    assert [str(w.message) for w in caught] == [
        f"{indicator.id}: {PARTIAL}Line 1820, flow 1160 is missing from {where} "
        "for 2 providers. Show the providers on their own rows to see who reported."
    ]
    assert found.notes == (
        "2025 is partial: 2 providers that reported line 1820, flow 1160 in 2024 "
        f"have not reported it in 2025, so {where} has no 2025 row of it",
    )
    assert found.table.column("value").to_pylist() == [13.0]


def test_a_shown_provider_beside_a_total_gets_one_warning_that_counts_it(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """France shows on its own row and sits in the pair total. Neither it nor
    Austria has reported 2025, so the warning names France and counts it in
    the total."""
    pair = frozenset({AUSTRIA, FRANCE})
    group = ResolvedGroup("pair", "provider", {2024: pair, 2025: pair})
    call = ResolvedCall(
        basis="grant_equivalent",
        units=(USD,),
        by=("provider", "year"),
        years=(2024, 2025),
        members={"provider": pair},
        groups=(group,),
        totals=(group.key,),
        rows={"provider": frozenset({FRANCE})},
    )
    indicator = dac1.IN_DONOR_REFUGEE_COSTS

    with pytest.warns(OdaDataWarning) as caught:
        combine(con, indicator, call, FakeFactors(), releases=UNCACHED)

    assert [str(w.message) for w in caught] == [
        f"{indicator.id}: {PARTIAL}Line 1820, flow 1160 is missing for France (4), "
        "and missing from the pair total for 2 providers. Show the providers on "
        "their own rows to see who reported."
    ]


def _pair_total(
    providers: frozenset[int], lines: tuple[int, ...], basis: str
) -> ResolvedCall:
    """A 2024-2025 call on ``lines`` for the total of ``providers`` alone."""
    group = ResolvedGroup("pair", "provider", {2024: providers, 2025: providers})
    return ResolvedCall(
        basis=basis,
        units=(USD,),
        by=("provider", "year"),
        years=(2024, 2025),
        members={"provider": providers, "line": frozenset(lines)},
        groups=(group,),
        totals=(group.key,),
        rows={"provider": frozenset()},
    )


@pytest.mark.parametrize(
    ("germany_106", "count"), [(True, 3), (False, 2)], ids=["reported", "never"]
)
@pytest.mark.parametrize("lines", [(206, 106), (106,)], ids=["bundled", "alone"])
def test_a_total_counts_a_whole_year_gap_at_each_line_it_has_no_row_for(
    lines: tuple[int, ...], germany_106: bool, count: int
) -> None:
    """Austria and France reported lines 206 and 106 in 2024 and only line
    206 in 2025, and Germany nothing of 2025. The total's 2025 row of line
    206 holds Germany's gap, and no row of line 106 exists, so the warning
    counts all three on line 106, whether or not the call also reads 206. A
    Germany that reported only line 206 in 2024 is not missing line 106."""
    connection = duckdb.connect()
    connection.execute(
        "CREATE TABLE dac2a (provider BIGINT, recipient BIGINT, year BIGINT,"
        " line BIGINT, amount_type VARCHAR, value DOUBLE)"
    )
    rows = [
        *DAC2A_ROWS,
        (AUSTRIA, KENYA, 2024, 106, 1.0),
        (FRANCE, KENYA, 2024, 106, 2.0),
    ]
    if germany_106:
        rows.append((GERMANY, KENYA, 2024, 106, 3.0))
    connection.executemany("INSERT INTO dac2a VALUES (?, ?, ?, ?, 'A', ?)", rows)
    call = _pair_total(frozenset({AUSTRIA, FRANCE, GERMANY}), lines, "net_disbursement")

    with pytest.warns(OdaDataWarning) as caught:
        found = combine(connection, dac2a.LINES, call, FakeFactors(), releases=UNCACHED)
    connection.close()

    assert [str(w.message) for w in caught] == [
        f"{dac2a.LINES.id}: {PARTIAL}Line 106 is missing from the pair total for "
        f"{count} providers. Show the providers on their own rows to see who "
        "reported."
    ]
    assert unsummed_text(2025, count, part="line 106", where="the pair total") in (
        found.notes
    )


def test_a_whole_year_gap_is_not_counted_at_a_line_the_total_never_holds() -> None:
    """Germany reported nothing of 2025, and in 2024 line 206 to Kenya and
    line 213 to Egypt alone. A total of Kenya has rows of line 206 alone, in
    every year, so Germany's gap is held by its 2025 row of line 206 and the
    result does not warn of a line 213 it never holds."""
    connection = duckdb.connect()
    connection.execute(
        "CREATE TABLE dac2a (provider BIGINT, recipient BIGINT, year BIGINT,"
        " line BIGINT, amount_type VARCHAR, value DOUBLE)"
    )
    rows = [*DAC2A_ROWS, (GERMANY, EGYPT, 2024, 213, 1.0)]
    connection.executemany("INSERT INTO dac2a VALUES (?, ?, ?, ?, 'A', ?)", rows)
    pair = frozenset({AUSTRIA, FRANCE, GERMANY})
    call = _pair_total(pair, (), "net_disbursement")
    call = replace(call, members={"provider": pair, "recipient": frozenset({KENYA})})

    found = combine(connection, dac2a.LINES, call, FakeFactors(), releases=UNCACHED)
    connection.close()

    assert found.notes == ()
    assert found.table.column("value").to_pylist() == [3.0 + 6.0 + 5.0, None]


@pytest.mark.parametrize(
    ("indicator", "part"),
    [(LINES, "line 1820"), (dac1.LINES, "line 1820, flow 1160")],
    ids=["lines", "lines-and-flows"],
)
def test_a_total_counts_a_whole_year_gap_at_each_dac1_part(
    con: duckdb.DuckDBPyConnection, indicator: Indicator, part: str
) -> None:
    """Austria reported nothing of 2025 and France not yet line 1820. The
    total's 2025 row of line 11010 holds Austria's gap, and no row of line
    1820 exists, so the warning counts both on line 1820."""
    call = _pair_total(frozenset({AUSTRIA, FRANCE}), (11010, 1820), "grant_equivalent")

    with pytest.warns(OdaDataWarning) as caught:
        combine(con, indicator, call, FakeFactors(), releases=UNCACHED)

    assert [str(w.message) for w in caught] == [
        f"{indicator.id}: {PARTIAL}{part[0].upper()}{part[1:]} is missing from the "
        "pair total for 2 providers. Show the providers on their own rows to see "
        "who reported."
    ]


@pytest.mark.parametrize(
    ("indicator", "providers", "line", "short", "notes", "expected"),
    [
        (
            LINES,
            frozenset({AUSTRIA, FRANCE, GERMANY}),
            1820,
            ["Line 1820 is short in the pair total, which sums only Germany (5)."],
            [
                "2025 is partial: the pair total of line 1820 sums only Germany (5), "
                "since 2 members that reported it in 2024 have not reported it in "
                "2025"
            ],
            22.0,
        ),
        (
            LINES,
            frozenset({AUSTRIA, FRANCE, GERMANY}),
            11010,
            ["Line 11010 is short in the pair total, which lacks Austria (1)."],
            [
                "2025 is partial: the pair total of line 11010 lacks Austria (1), "
                "which reported it in 2024 and has not reported it in 2025"
            ],
            320.0,
        ),
        (LINES, frozenset({FRANCE, GERMANY}), 11010, [], [], 320.0),
        (
            one.CORE_ODA,
            frozenset({AUSTRIA, FRANCE, GERMANY, ITALY}),
            None,
            [
                "Line 11010, flow 1160 is short in the pair total, which lacks "
                "Austria (1).",
                "Line 1820, flow 1160 is short in the pair total, which sums only "
                "Germany (5).",
            ],
            [
                "2025 is partial: the pair total of line 11010, flow 1160 lacks "
                "Austria (1), which reported it in 2024 and has not reported it in "
                "2025",
                "2025 is partial: the pair total of line 1820, flow 1160 sums only "
                "Germany (5), since 2 members that reported it in 2024 have not "
                "reported it in 2025",
            ],
            110.0 + 210.0 + 44.0 - 22.0,
        ),
        (
            CRS_ODA,
            frozenset({FRANCE, GERMANY, ITALY}),
            None,
            ["All of 2025 is short in the pair total, which sums only France (4)."],
            [
                "2025 is partial: the pair total sums only France (4), since 2 "
                "members that reported 2024 have not reported 2025"
            ],
            7.0,
        ),
    ],
    ids=["sums-only", "lacks", "all-reported", "core-oda", "crs"],
)
def test_an_allow_partial_total_names_the_members_it_sums_or_lacks(
    con: duckdb.DuckDBPyConnection,
    indicator: Indicator,
    providers: frozenset[int],
    line: int | None,
    short: list[str],
    notes: list[str],
    expected: float,
) -> None:
    """Austria reported nothing of 2025 and France not yet line 1820, so with
    allow_partial the pair's 2025 total of line 1820 sums Germany alone and
    its total of line 11010 lacks Austria alone. France and Germany both
    reported line 11010, so their total is whole and does not warn. Italy
    reports total ODA (line 11010) and never refugee costs, so core ODA's
    total, whose rows carry no line, sums the line for Germany alone. On the
    CRS, which providers report a year whole, Germany and Italy reported 2024
    and not 2025, so the total sums France alone."""
    con.executemany(
        "INSERT INTO dac1 VALUES (?, ?, 11010, 1160, 'A', ?)",
        [(ITALY, 2024, 40.0), (ITALY, 2025, 44.0)],
    )
    con.execute(f"INSERT INTO crs VALUES ({ITALY}, {KENYA}, 110, 1000, 2024, 2.0)")
    if line is None:
        group = ResolvedGroup("pair", "provider", {2025: providers})
        call = ResolvedCall(
            basis=indicator.default_basis,
            units=(USD,),
            by=("provider", "year"),
            years=(2025,),
            members={"provider": providers},
            groups=(group,),
            totals=(group.key,),
            rows={"provider": frozenset()},
            allow_partial=True,
        )
    else:
        call = lines_call((line,), providers=providers, allow_partial=True)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", OdaDataWarning)
        found = combine(con, indicator, call, FakeFactors(), releases=UNCACHED)

    expected_warnings = [f"{indicator.id}: {PARTIAL}{' '.join(short)}"] if short else []
    assert [str(w.message) for w in caught] == expected_warnings
    assert list(found.notes) == notes
    assert found.table.column("value").to_pylist() == [expected]


@pytest.mark.parametrize(
    ("indicator", "total", "multisystem_2025", "short", "expected"),
    [
        (
            BOTH_TABLES,
            True,
            True,
            "Its CRS part is short in the pair total, which lacks Germany (5). "
            "Its Multisystem part is short in the pair total, which lacks "
            "France (4).",
            4.0 + 7.0 + 40.0 + 50.0,
        ),
        (
            BOTH_TABLES,
            True,
            False,
            "Its CRS part is short in the pair total, which lacks Germany (5). "
            "Its Multisystem part is missing from the pair total for 3 providers. "
            "Show the providers on their own rows to see who reported.",
            4.0 + 7.0,
        ),
        (
            BOTH_TABLES,
            False,
            True,
            "Its CRS part is left out of the sum for 1 provider. Its Multisystem "
            "part is left out of the sum for 1 provider. Show the providers on "
            "their own rows to see who reported.",
            4.0 + 7.0 + 40.0 + 50.0,
        ),
        (
            CRS_ODA,
            True,
            True,
            "All of 2025 is short in the pair total, which lacks Germany (5).",
            4.0 + 7.0,
        ),
    ],
    ids=["two-tables-total", "no-multisystem-row", "two-tables-sum", "one-table"],
)
def test_each_table_of_a_whole_year_indicator_has_its_own_gap(
    con: duckdb.DuckDBPyConnection,
    indicator: Indicator,
    total: bool,
    multisystem_2025: bool,
    short: str,
    expected: float,
) -> None:
    """Germany has CRS rows for 2024 alone and France Multisystem rows for
    2024 alone, so a 2025 total over both tables lacks Germany's CRS part
    and France's Multisystem part, and holds the other part of each. With
    no 2025 Multisystem row from the pair, the total has no Multisystem
    part, whatever CRS rows it holds."""
    con.execute("CREATE TABLE multisystem (provider BIGINT, year BIGINT, value DOUBLE)")
    con.executemany(
        "INSERT INTO multisystem VALUES (?, ?, ?)",
        [
            (AUSTRIA, 2024, 30.0),
            (FRANCE, 2024, 20.0),
            (GERMANY, 2024, 45.0),
            *(
                [(AUSTRIA, 2025, 40.0), (GERMANY, 2025, 50.0)]
                if multisystem_2025
                # Italy, outside the pair, keeps 2025 the table's last year.
                else [(ITALY, 2025, 1.0)]
            ),
        ],
    )
    providers = frozenset({AUSTRIA, FRANCE, GERMANY})
    group = ResolvedGroup("pair", "provider", {2025: providers})
    call = ResolvedCall(
        basis="gross_disbursement",
        units=(USD,),
        by=("provider", "year") if total else ("year",),
        years=(2025,),
        members={"provider": providers},
        groups=(group,) if total else (),
        totals=(group.key,) if total else (),
        rows={"provider": frozenset()} if total else {},
        allow_partial=total,
    )

    with pytest.warns(OdaDataWarning) as caught:
        found = combine(con, indicator, call, FakeFactors(), releases=UNCACHED)

    assert [str(w.message) for w in caught] == [f"{indicator.id}: {PARTIAL}{short}"]
    assert found.table.column("value").to_pylist() == [expected]
    if indicator is BOTH_TABLES and total and multisystem_2025:
        assert found.notes == (
            "2025 is partial: the pair total of its CRS part lacks Germany (5), "
            "which reported it in 2024 and has not reported it in 2025",
            "2025 is partial: the pair total of its Multisystem part lacks "
            "France (4), which reported it in 2024 and has not reported it in 2025",
        )


def test_a_sum_short_of_one_part_and_without_another_warns_once(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """Austria has not reported 2025, so the 2025 sum of line 11010 covers
    France alone, and neither has reported line 1820, so the sum has no 2025
    row of it."""
    call = lines_call(
        (11010, 1820), providers=frozenset({AUSTRIA, FRANCE}), by=("year",), total=False
    )

    with pytest.warns(OdaDataWarning) as caught:
        combine(con, LINES, call, FakeFactors(), releases=UNCACHED)

    assert [str(w.message) for w in caught] == [
        f"{LINES.id}: {PARTIAL}All of 2025 is left out of the sum for 1 provider. "
        "Line 1820 is missing from the sum for 2 providers. Show the providers on "
        "their own rows to see who reported."
    ]


def test_the_warning_names_five_parts_and_counts_the_rest() -> None:
    parts = [
        LastYearPart(
            2025,
            f"line {line}",
            Effect.NULL if line == 1 else Effect.MISSING,
            frozenset({FRANCE}),
        )
        for line in range(1, 8)
    ]

    text = last_year_warning(2025, parts)

    assert text.endswith(
        "Line 4 is missing for France (4). Line 5 is missing for France (4). "
        "2 more parts are partial too. allow_partial=True returns the partial "
        "values of the null rows."
    )
    assert text.count("France") == 5


def test_a_result_whose_shown_providers_all_reported_does_not_warn(
    con: duckdb.DuckDBPyConnection,
) -> None:
    call = refugee_call(frozenset({GERMANY}), (2024, 2025))

    with warnings.catch_warnings():
        warnings.simplefilter("error", OdaDataWarning)
        found = combine(
            con, dac1.IN_DONOR_REFUGEE_COSTS, call, FakeFactors(), releases=UNCACHED
        )

    assert found.notes == ()
    assert found.table.column("value").to_pylist() == [20.0, 22.0]


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

    with pytest.warns(
        OdaDataWarning, match="All of 2025 is left out of the sum for 1 provider"
    ):
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
        (_WAITING, {_LAST}, False, False, Outcome.WARN_ROW),
        (_STOPPED, {_LAST}, False, False, Outcome.NULL_ROW),
        (_WAITING, set(), False, False, Outcome.WARN_SUM),
        (_WAITING, set(), False, True, Outcome.NULL_SUM),
        (_WAITING, set(), True, False, Outcome.WARN_SUM),
        (_METHOD, set(), False, False, Outcome.NULL_SUM),
        (_STOPPED, set(), False, False, Outcome.NOTE),
        (_STOPPED, set(), True, False, Outcome.NULL_SUM),
    ],
    ids=[
        "shown-own-row",
        "own-row-stopped-before",
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

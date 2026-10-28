"""The 2.x map's calls against the cached DAC1, DAC2A and CRS tables.

2.8.2: oda-data 2.8.2's ``OECDClient(**arguments, use_bulk_download=True)
.get_indicators(code)``, with its bulk readers fed the same cached Parquet
files (the releases in ``goldens.json``'s ``v2_map_releases``), gave the
yearly sums in ``goldens.json``'s ``v2_map`` section, for the providers and
recipients of ``ARGUMENTS``. A sampled row whose status is ``exact`` must give
the same sums. A ``changes`` row differs by design, and its reason says why.
``scripts/refresh_goldens.py`` recomputes the sums on a new release.

The data facts the generator holds as constants are checked here too.
"""

import ast
import hashlib
import importlib.util
import json
import sys
import types
from collections.abc import Iterator
from pathlib import Path

import pandas as pd
import pytest

import oda_data as oda
from oda_data import _compat, _context
from oda_data._errors import OfflineCacheMissError, ReleaseMismatchError
from oda_data._registry import dac2a
from oda_data._tables import OecdTables

pytestmark = pytest.mark.data

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "generate_v2_map.py"


def _generator() -> types.ModuleType:
    spec = importlib.util.spec_from_file_location("generate_v2_map", _SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module  # dataclasses look the module up
    spec.loader.exec_module(module)
    return module


generator = _generator()

_GOLDENS = json.loads((Path(__file__).parent / "goldens.json").read_text())
RELEASES: dict[str, str] = _GOLDENS["v2_map_releases"]

#: Rows sampled from each table's 2.x codes, and the two ONE codes with a 2.x
#: function that runs.
QUOTAS = {"DAC1": 12, "DAC2A": 8, "CRS": 8}
ONE_CODES = ("ONE.10.1010_11010", "ONE.10.1010C")

_RECIPIENTS = [248, 625, 85, 342, 289, 998]

#: The years, providers and recipients each table's rows are compared on.
ARGUMENTS: dict[str, dict[str, list[int]]] = {
    "DAC1": {"years": [2015, 2022], "providers": [4, 12, 302]},
    "DAC2A": {
        "years": [2010, 2022],
        "providers": [4, 12, 302, 701],
        "recipients": _RECIPIENTS,
    },
    "CRS": {"years": [2022], "providers": [4, 12]},
    "ONE": {"years": [2015, 2017, 2018, 2022], "providers": [4, 12, 302]},
}

#: 2.8.2's sums by year, in current USD millions, for each sampled exact row.
#: Grant-equivalent rows read 2018 in place of 2015.
V2_8_2: dict[str, dict[int, float]] = {
    code: {int(year): value for year, value in row["expected"].items()}
    for code, row in _GOLDENS["v2_map"].items()
}


def _sample() -> list[str]:
    """The sampled codes: per table, the rows with a call whose code has the
    lowest SHA-256, then ``ONE_CODES``."""
    rows = [row for row in _compat.v2_map().values() if row.source == "catalogue"]
    found: list[str] = []
    for table, quota in QUOTAS.items():
        candidates = [r.code for r in rows if r.call and r.code.split(".")[0] == table]
        candidates.sort(key=lambda code: hashlib.sha256(code.encode()).hexdigest())
        found += candidates[:quota]
    return [*found, *ONE_CODES]


SAMPLE = _sample()


@pytest.fixture(scope="module")
def pinned() -> Iterator[oda.Context]:
    context = oda.Context(offline=True, releases=RELEASES)
    try:
        for indicator in (
            "oecd.dac1.lines",
            "oecd.dac2a.lines",
            "oecd.crs.bilateral_oda",
        ):
            context.get(indicator, years=2022, providers=4)
    except (OfflineCacheMissError, ReleaseMismatchError):
        pytest.skip(
            "the 2.8.2 comparison needs DAC1, DAC2A and CRS at the pinned "
            "releases; run scripts/refresh_goldens.py on the new releases"
        )
    yield context
    context.close()


@pytest.fixture(scope="module")
def tables() -> Iterator[OecdTables]:
    found = OecdTables(offline=True)
    try:
        found.cursor(("dac1", "dac2a"))
    except OfflineCacheMissError:
        pytest.skip("DAC1 or DAC2A is not cached; run scripts/warm_cache.py")
    yield found
    found.close()


def run(context: oda.Context, row: _compat.V2Row, **arguments: object) -> pd.DataFrame:
    """Run ``row``'s call on ``context``, every ``oda.get`` taking ``arguments``
    too; ``recipients`` only where the indicator has recipients."""

    def get(indicator: str, /, **given: object) -> pd.DataFrame:
        extra = dict(arguments)
        if "recipient" not in _context.find_indicator(indicator).dimensions:
            extra.pop("recipients", None)
        return context.get(indicator, **extra, **given)

    shim = types.SimpleNamespace(get=get, sectors=oda.sectors, groups=oda.groups)
    namespace: dict[str, object] = {"oda": shim, "pd": pd}
    tree = ast.parse("\n".join(row.call))
    *body, last = tree.body
    exec(compile(ast.Module(body, []), row.code, "exec"), namespace)
    if isinstance(last, ast.Expr):
        return eval(compile(ast.Expression(last.value), row.code, "eval"), namespace)
    exec(compile(ast.Module([last], []), row.code, "exec"), namespace)
    found = namespace["df"]
    assert isinstance(found, pd.DataFrame)
    return found


def _arguments(row: _compat.V2Row) -> dict[str, object]:
    arguments: dict[str, object] = dict(ARGUMENTS[row.code.split(".")[0]])
    if 'basis="grant_equivalent"' in row.call_text:
        arguments["years"] = [2018 if y == 2015 else y for y in arguments["years"]]  # type: ignore[union-attr]
    return arguments


def test_the_sample_covers_thirty_rows_across_tables() -> None:
    assert len(SAMPLE) == len(set(SAMPLE)) == 30
    assert {code.split(".")[0] for code in SAMPLE} == {"DAC1", "DAC2A", "CRS", "ONE"}
    exact = [c for c in SAMPLE if _compat.v2_map()[c].status == "exact"]
    assert sorted(exact) == sorted(V2_8_2)


@pytest.mark.parametrize("code", SAMPLE)
def test_a_sampled_call_runs_and_an_exact_one_matches_2_8_2(
    pinned: oda.Context, code: str
) -> None:
    row = _compat.v2_map()[code]
    found = run(pinned, row, **_arguments(row))
    sums = found.groupby("year")["value"].sum() if len(found) else pd.Series()
    if row.status != "exact":
        assert row.reason
        return
    expected = V2_8_2[code]
    assert {int(year) for year in sums.index} == set(expected)
    for year, value in expected.items():
        assert sums[year] == pytest.approx(value, rel=1e-9, abs=1e-6)


def test_dac1_holds_no_covid_line(tables: OecdTables) -> None:
    held = tables.cursor(("dac1",)).execute(
        f"SELECT count(*) FROM dac1 WHERE line IN {tuple(generator.COVID_LINES)}"
    )
    assert held.fetchone() == (0,)


def test_dac2a_lines_without_agency_rows_are_exactly_those(
    tables: OecdTables,
) -> None:
    agencies = tuple(dac2a.MULTILATERAL_RECIPIENTS)
    rows = tables.cursor(("dac2a",)).execute(
        "SELECT line, count(*) FILTER (WHERE recipient IN "
        f"{agencies}) FROM dac2a WHERE amount_type = 'A' GROUP BY line"
    )
    without = {line for line, agency_rows in rows.fetchall() if agency_rows == 0}
    codes = {
        int(row.code.split(".")[-1])
        for row in _compat.v2_map().values()
        if row.code.startswith("DAC2A.")
    }
    assert without & codes == set(generator.DAC2A_LINES_WITHOUT_AGENCY_ROWS)


def test_crs_purpose_codes_start_with_their_sector_heading(
    tables: OecdTables,
) -> None:
    # The map reads a 2.x sector heading as every purpose code that starts
    # with its three digits.
    path = tables.path("crs")
    rows = tables.cursor(("crs",)).execute(
        f"SELECT count(*) FROM read_parquet('{path}') WHERE purpose_code IS NOT NULL "
        "AND left(purpose_code::VARCHAR, 3) <> sector_code::VARCHAR"
    )
    assert rows.fetchone() == (0,)


def test_rows_with_no_modality_come_back_with_a_null_modality(
    pinned: oda.Context,
) -> None:
    found = pinned.get(
        "oecd.crs.bilateral_oda", years=2005, providers=4, by=["modality", "year"]
    )
    assert found["modality"].isna().any()

"""Each named DAC1 and DAC2A indicator against its line in the lines indicator.

A named indicator on one line is a filter on that line of ``oecd.dac1.lines``
or ``oecd.dac2a.lines``, with the same row exclusions, so both calls give the
same number on every basis they share.
"""

from collections.abc import Iterator

import pytest

import oda_data as oda
from oda_data._errors import OfflineCacheMissError
from oda_data._model import basis_lines
from oda_data._registry import dac1, dac2a
from oda_data._tables import OecdTables

pytestmark = pytest.mark.data

#: Years every basis covers on both tables; DAC2A's 2025 is partial.
YEARS = range(2018, 2025)


@pytest.fixture(scope="module", autouse=True)
def tables() -> Iterator[OecdTables]:
    tables = OecdTables(offline=True)
    try:
        tables.cursor(("dac1", "dac2a"))
    except OfflineCacheMissError:
        pytest.skip("DAC1 or DAC2A is not cached; run scripts/warm_cache.py")
    yield tables
    tables.close()


def _cases() -> list[tuple[str, str, str, int]]:
    return [
        (indicator.id, lines.id, basis, line)
        for module in (dac1, dac2a)
        for indicator in module.INDICATORS
        for lines in [module.LINES]
        for basis, line in basis_lines(indicator).items()
        if indicator is not lines
    ]


@pytest.mark.parametrize(("indicator", "lines", "basis", "line"), _cases())
def test_a_named_indicator_equals_its_line(
    indicator: str, lines: str, basis: str, line: int
) -> None:
    named = oda.get(indicator, years=YEARS, basis=basis, by=["year"])
    found = oda.get(lines, years=YEARS, basis=basis, lines=line, by=["year"])

    assert found["year"].tolist() == named["year"].tolist() == list(YEARS)
    assert found["value"].tolist() == pytest.approx(named["value"].tolist(), rel=1e-9)


def test_grant_equivalent_only_lines_are_the_money_lines_flow_1160_holds_alone(
    tables: OecdTables,
) -> None:
    held = tables.cursor(("dac1",)).execute(
        "SELECT line FROM dac1 WHERE amount_type = 'A' "
        "GROUP BY line HAVING count(DISTINCT flow) = 1 AND min(flow) = 1160"
    )
    alone = {line for (line,) in held.fetchall()} - set(dac1.NON_MONEY_LINES)
    assert alone == set(dac1.GRANT_EQUIVALENT_ONLY_LINES)

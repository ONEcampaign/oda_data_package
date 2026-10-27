"""The DAC1 view over the real table in the oda-reader cache."""

import pytest

from oda_data._errors import OfflineCacheMissError
from oda_data._model import VOCABULARY
from oda_data._tables import OecdTables

pytestmark = pytest.mark.data


def test_dac1_view_reads_the_cached_table():
    tables = OecdTables(offline=True)
    try:
        cursor = tables.cursor(("dac1",))
    except OfflineCacheMissError:
        pytest.skip("DAC1 is not cached; run scripts/warm_cache.py")

    columns = {row[0]: row[1] for row in cursor.execute("DESCRIBE dac1").fetchall()}
    first_year, total_2022 = cursor.execute(
        "SELECT min(year), sum(value) FILTER (WHERE year = 2022) FROM dac1 "
        "WHERE provider = 20001 AND line = 1010 AND flow = 1140 "
        "AND amount_type = 'A'"
    ).fetchone()

    assert columns == {c.name: c.type for c in VOCABULARY["dac1"].columns}
    assert first_year == 1960
    # DAC countries' net ODA in 2022 was about USD 211 billion.
    assert 150_000 < total_2022 < 300_000
    assert tables.release("dac1").token
    tables.close()

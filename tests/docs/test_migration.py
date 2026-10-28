"""The migration guide covers every removed 2.x name and states the map's counts."""

import importlib.util
import re
import sys
from collections import Counter
from pathlib import Path

from oda_data._compat import REMOVED, v2_map
from oda_data._names import load_json

REPO = Path(__file__).resolve().parents[2]
GUIDE = REPO / "docs" / "docs" / "migration.md"

#: The reason behind each "N codes:" bullet of "Numbers that change", in order.
BULLET_REASONS = (
    "crs_bilateral_rows",
    "crs_psi",
    "crs_other_categories",
    "crs_oof_rows",
    "crs_private_providers",
    "recipient_totals",
    "agency_recipients",
    "imputed_providers",
    "no_2x_rows",
)


def _changes() -> list[dict]:
    """The map's ``changes`` rows, with their reason keys."""
    codes = load_json("v2_map.json")["codes"].values()
    return [row for row in codes if row["status"] == "changes"]


def _table_rows(text: str, heading: str) -> list[list[str]]:
    """The body rows of the first table after ``heading``, as stripped cells."""
    section = text.split(heading, 1)[1]
    lines = [line for line in section.splitlines() if line.startswith("|")]
    return [[cell.strip() for cell in line.strip("|").split("|")] for line in lines[2:]]


def test_guide_names_every_removed_name():
    text = GUIDE.read_text()
    missing = [name for name in REMOVED if f"`{name}`" not in text]
    assert not missing, f"migration.md does not name {missing}"


def test_guide_states_the_map_counts():
    text = GUIDE.read_text()
    rows = v2_map().values()
    table = {
        cells[0].strip("` "): cells[1].strip()
        for line in text.splitlines()
        if line.startswith("| `")
        for cells in [line.strip("|").split("|")]
    }
    for status, count in Counter(row.status for row in rows).items():
        assert table.get(status) == f"{count:,}", status
    assert f"{len(rows):,} entries" in text


def test_guide_states_the_reason_counts():
    text = GUIDE.read_text()
    counts = Counter(reason for row in _changes() for reason in row["reasons"])
    bullets = re.findall(r"^- ([\d,]+) codes:", text, flags=re.MULTILINE)
    assert bullets == [f"{counts[reason]:,}" for reason in BULLET_REASONS]

    sector_rows = [row for row in _changes() if "crs_sectors" in row["reasons"]]
    with_nulls = [
        row for row in sector_rows if any(".isna()" in line for line in row["call"])
    ]
    assert f"{len(sector_rows):,} CRS codes filter on a sector" in text
    assert f"the {len(with_nulls):,} that also select rows" in text


def test_every_size_row_is_a_call_the_delta_script_runs():
    sys.path.insert(0, str(REPO / "scripts"))
    try:
        spec = importlib.util.spec_from_file_location(
            "imputation_delta", REPO / "scripts" / "imputation_delta.py"
        )
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    finally:
        sys.path.remove(str(REPO / "scripts"))
    runs = {(delta.label, f"`{delta.code}`") for delta in module.DELTAS}
    rows = _table_rows(GUIDE.read_text(), "### Size of the changes")
    assert rows
    assert [row[:2] for row in rows if (row[0], row[1]) not in runs] == []

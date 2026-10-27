"""Tests for scripts/imputation_delta.py and scripts/v2_reference.py.

Both scripts live outside ``src/oda_data`` and are loaded by file path. No test
here runs oda-data 2.x or reads a table.
"""

import importlib.util
import sys
import types
from pathlib import Path

import pandas as pd
import pytest

SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"


def _load(name: str) -> types.ModuleType:
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module  # imputation_delta imports v2_reference by name
    spec.loader.exec_module(module)
    return module


v2_reference = _load("v2_reference")
imputation_delta = _load("imputation_delta")


def test_sums_key_whole_floats_as_integers_and_nulls_as_na() -> None:
    frame = pd.DataFrame(
        {
            "donor_code": [4.0, 4.0, 12.0],
            "purpose_code": [11110.0, None, 11110.0],
            "value": [1.5, 2.0, 3.0],
        }
    )

    found = v2_reference._sums(frame, ["provider", "sector"])

    assert found == {"4|11110": 1.5, "4|<NA>": 2.0, "12|11110": 3.0}


def test_sums_give_null_where_2x_drops_the_value_column() -> None:
    frame = pd.DataFrame({"donor_code": [918], "year": [2022]})

    found = v2_reference._sums(frame, ["year"])

    assert list(found) == ["2022"]
    assert pd.isna(found["2022"])


@pytest.mark.parametrize(
    ("old", "new", "cells"),
    [
        (10.0, 12.5, ["10.00", "12.50", "2.5000"]),
        (None, 190.0, ["no rows", "190.00", "n/a"]),
        (float("nan"), 0.1355, ["null", "0.1355", "n/a"]),
        (7127.17, 7127.17 + 1e-9, ["7,127.17", "7,127.17", "0.0000"]),
    ],
)
def test_delta_table_shows_missing_and_null_2x_values(
    old: float | None, new: float, cells: list[str]
) -> None:
    row = {
        "label": "Call",
        "code": "DAC1.10.1010",
        "year": 2022,
        "old": old,
        "new": new,
        "status": "changes",
        "reason": "a | b",
    }

    *_, line = imputation_delta.delta_table([row]).splitlines()

    assert line.split(" | ")[3:6] == cells
    assert line.endswith("a \\| b |")

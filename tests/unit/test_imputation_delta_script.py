"""Tests for helpers in scripts/imputation_delta.py."""

import importlib.util
import sys
from pathlib import Path

SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "imputation_delta.py"


def _load_script_module():
    spec = importlib.util.spec_from_file_location("imputation_delta", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


script = _load_script_module()


class TestTruncatedYears:
    def test_contiguous_range_loses_its_first_two_years(self):
        years = list(range(2015, 2025))
        assert script._truncated_years(years, period_length=3) == {2015, 2016}

    def test_non_contiguous_request_uses_the_calendar_cutoff(self):
        assert script._truncated_years([2019, 2021, 2023], period_length=3) == {2019}

    def test_single_year_is_truncated(self):
        assert script._truncated_years([2022], period_length=3) == {2022}

    def test_empty_request(self):
        assert script._truncated_years([], period_length=3) == frozenset()

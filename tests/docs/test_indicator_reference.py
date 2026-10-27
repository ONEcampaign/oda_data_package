"""The indicator reference page matches the registry."""

import importlib.util
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "generate_indicator_docs.py"


@pytest.fixture(scope="module")
def generator():
    spec = importlib.util.spec_from_file_location("generate_indicator_docs", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_page_is_current(generator):
    assert generator.TARGET.read_text() == generator.render(), (
        "docs/docs/reference/indicators.md is stale; "
        "run uv run python scripts/generate_indicator_docs.py"
    )


def test_page_has_one_entry_per_indicator(generator):
    from oda_data._registry import INDICATORS

    text = generator.render()
    for indicator_id in INDICATORS:
        assert text.count(f"### `{indicator_id}`\n") == 1, indicator_id

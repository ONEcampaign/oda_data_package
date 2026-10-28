"""Tests for scripts/check_codelists.py.

The script lives outside ``src/oda_data`` and is loaded here by file path. No
test makes a network call: the comparison runs on fixture codelists.
"""

import importlib.util
import sys
from pathlib import Path

import pandas as pd
import pytest

SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "check_codelists.py"


def _load_script_module():
    spec = importlib.util.spec_from_file_location("check_codelists", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


script = _load_script_module()

LIVE = {
    "provider": {"1": frozenset({"Austria"}), "1601": frozenset({"Gates Foundation"})},
    "recipient": {"55": frozenset({"Türkiye"}), "130": frozenset({"Algeria"})},
    "sector": {
        "110": frozenset({"Education"}),
        "11110": frozenset({"Education policy"}),
    },
    "channel": {"11000": frozenset({"Provider Government"})},
    "modality": {"A02": frozenset({"Sector budget support"})},
    "finance_type": {"110": frozenset({"Standard grant"})},
}


def _used(**codes: list[str]) -> dict:
    return {
        dimension: {code: {"test"} for code in listed}
        for dimension, listed in codes.items()
    }


def _compare(used, labels=None, new=None):
    return script.compare(
        used,
        labels or {},
        {**{d: {} for d in script.CODELIST_IDS}, **LIVE},
        aggregates={"recipient": {"10001"}},
        new=new or {},
    )


def test_listed_codes_pass():
    used = _used(provider=["1"], recipient=["130"], sector=["11110"], modality=["A02"])
    assert _compare(used) == []


def test_unknown_code_is_reported_with_its_sources():
    used = {"sector": {"11199": {"names.json", "sector_groups.json health"}}}
    [finding] = _compare(used)
    assert (finding.dimension, finding.code, finding.kind) == (
        "sector",
        "11199",
        "unknown",
    )
    assert finding.detail == "names.json, sector_groups.json health"


@pytest.mark.parametrize(
    ("dimension", "code"),
    [
        ("provider", "20001"),  # a provider aggregate
        ("recipient", "10001"),  # a recipient aggregate
        ("sector", "100"),  # a DAC5 heading the codelist lacks
        ("recipient", "9998"),  # a DAC2A-only row
        ("recipient", "68"),  # a DAC2A Part II recipient
        ("recipient", "905"),  # a multilateral agency DAC2A lists as a recipient
    ],
)
def test_codes_no_codelist_holds_are_exempt(dimension, code):
    assert _compare(_used(**{dimension: [code]})) == []


def test_renamed_code_is_reported_and_spelling_variants_are_not():
    labels = {
        "provider": {"1": "AUSTRIA", "1601": "Bill & Melinda Gates Foundation"},
        "recipient": {"55": "Turkiye"},
    }
    [finding] = _compare({}, labels)
    assert (finding.dimension, finding.code, finding.kind) == (
        "provider",
        "1601",
        "renamed",
    )
    assert "Gates Foundation" in finding.detail


def test_three_digit_sector_headings_are_not_compared_by_name():
    assert _compare({}, {"sector": {"110": "I.1. Education"}}) == []
    [finding] = _compare({}, {"sector": {"11110": "Education, policy"}})
    assert finding.kind == "renamed"


def test_unknown_codes_come_before_renamed_and_new_ones():
    findings = _compare(
        _used(finance_type=["999"]),
        {"provider": {"1601": "Other name"}},
        {"modality": frozenset({"A02"})},
    )
    assert [f.kind for f in findings] == ["unknown", "renamed", "new"]


def test_a_live_code_the_package_does_not_name_is_new():
    labels = {"sector": {"110": "I.1. Education"}, "channel": {}}
    new = {"sector": frozenset({"110", "11110"}), "channel": frozenset({"11000"})}
    findings = _compare({}, labels, new)
    assert [(f.dimension, f.code, f.kind, f.detail) for f in findings] == [
        ("channel", "11000", "new", "Provider Government"),
        ("sector", "11110", "new", "Education policy"),
    ]


def test_new_codes_are_active_crs_codes_that_head_no_other_code():
    frame = pd.DataFrame(
        {
            "codelist_id": ["15"] * 7 + ["10"],
            "code": ["100", "110", "111", "112", "1", "0", "999", "11110"],
            "label": ["GRANTS", "a", "b", "c", "GNI", "NON FLOW", "d", "e"],
            "status": ["active"] * 3 + ["withdrawn"] + ["active"] * 4,
            "crs": ["1", "1", "0", "1", "1", "1", "1", "1"],
            "category": [None, "100", "100", "100", "0", None, "100", "111"],
            "parent_code": [None] * 8,
        }
    )
    # 100 and 0 head other codes, 111 is TOSSD only, 112 is withdrawn, and
    # 1 is a non-flow item; 11110 belongs to codelist 10.
    assert script.new_codes(frame, "15") == {"110", "999"}


def test_live_codes_keeps_every_label_of_a_code():
    frame = pd.DataFrame(
        {
            "codelist_id": ["10", "10", "10", "14"],
            "code": [110, 110, 111, 110],
            "label": ["Education ", "Education, all", "Level unspecified", "Other"],
        }
    )
    assert script.live_codes(frame, "10") == {
        "110": frozenset({"Education", "Education, all"}),
        "111": frozenset({"Level unspecified"}),
    }


def test_filter_codes_reads_nested_and_conditional_filters():
    from oda_data._model import AnyOf, In, NotIn

    filters = (
        In("modality", ("A02",)),
        AnyOf((NotIn("provider", (918,), "reason", when=In("recipient", (55,))),)),
    )
    assert list(script._filter_codes(filters)) == [
        ("modality", ("A02",)),
        ("provider", (918,)),
        ("recipient", (55,)),
    ]


def test_collect_used_reads_the_bundled_data():
    used, labels = script.collect_used()
    assert set(used) == set(script.CODELIST_IDS)
    assert "10" in used["provider"] or "1" in used["provider"]
    assert all(code for codes in used.values() for code in codes)
    # Czechia (68) is named in names.json alone, outside every recipient group.
    assert used["recipient"]["68"] == {"names.json"}
    assert labels["channel"]["11000"]
    assert set(script.BUNDLES_NAMES) <= set(labels)

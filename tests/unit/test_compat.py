"""The 2.x map, the errors 2.x codes and names raise, and search on 2.x codes."""

import ast
import importlib.util
import inspect
import re
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

import oda_data as oda
from oda_data import _compat, _context
from oda_data._model import Headline, basis_lines, line_basis
from oda_data._names import names
from oda_data._registry import INDICATORS, dac1, dac2a

MAP = _compat.v2_map()
GET_ARGUMENTS = set(inspect.signature(oda.get).parameters) - {"indicator"}

#: The names oda-data 2.8.1 exported from ``oda_data.__all__``, and the
#: research function the 3.0 docs point users of ``imputation_quality`` at.
V2_NAMES = [
    "AidDataData",
    "CRSData",
    "DAC1Data",
    "DAC2AData",
    "MultiSystemData",
    "ODAData",
    "OECDClient",
    "add_broad_sectors",
    "add_gni_share_column",
    "add_names_columns",
    "add_sectors",
    "bilateral_policy_marker",
    "cache",
    "clear_cache",
    "disable_cache",
    "enable_cache",
    "provider_groupings",
    "recipient_groupings",
    "sector_imputations",
    "set_cache_root",
    "set_data_path",
    "imputation_quality_report",
]

#: The ``get`` arguments that take codes, and the names those codes come from.
CODE_ARGUMENTS = {
    "finance_types": "finance_type",
    "modalities": "modality",
    "sectors": "sector",
}


def _gets(row: _compat.V2Row) -> Iterator[ast.Call]:
    for node in ast.walk(ast.parse("\n".join(row.call))):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "get"
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "oda"
        ):
            yield node


def _literal(node: ast.expr) -> object:
    return ast.literal_eval(node)


def _codes(node: ast.expr) -> list[object]:
    """The literal codes of an argument, leaving out ``oda.sectors`` groups."""
    items = node.elts if isinstance(node, ast.List) else [node]
    return [_literal(item) for item in items if not isinstance(item, ast.Attribute)]


def _groups(node: ast.expr) -> list[str]:
    items = node.elts if isinstance(node, ast.List) else [node]
    return [item.attr for item in items if isinstance(item, ast.Attribute)]


# The map ----------------------------------------------------------------------


def test_the_map_holds_every_2x_code_and_slug() -> None:
    sources = [row.source for row in MAP.values()]
    assert sources.count("catalogue") == 3049
    assert sources.count("settings") == 108
    prefixes = {
        code.split(".")[0] for code, row in MAP.items() if row.source == "catalogue"
    }
    assert prefixes == {"CRS", "DAC1", "DAC2A", "ONE"}


def test_every_one_code_is_hand_mapped_with_a_reason_or_an_exact_call() -> None:
    ones = [row for row in MAP.values() if row.code.startswith("ONE.")]
    assert len(ones) == 6
    assert all(row.hand_mapped and row.call for row in ones)


def test_a_row_without_a_call_says_why_and_a_changed_row_says_how() -> None:
    for row in MAP.values():
        assert row.status in ("exact", "changes", "no_data", "no_2x_number")
        assert bool(row.call) == (row.status != "no_data"), row.code
        if row.status != "exact":
            assert row.reason, row.code
        assert row.indicator is None or row.indicator in INDICATORS, row.code


@pytest.mark.parametrize("code", list(MAP))
def test_every_call_parses_and_names_real_indicators_and_arguments(code: str) -> None:
    row = MAP[code]
    calls = list(_gets(row))
    assert bool(calls) == bool(row.call)
    if calls:
        assert row.indicator in {_literal(call.args[0]) for call in calls}
    for call in calls:
        assert len(call.args) == 1
        indicator = INDICATORS[_literal(call.args[0])]
        for keyword in call.keywords:
            assert keyword.arg in GET_ARGUMENTS, keyword.arg
            if keyword.arg == "basis":
                assert _literal(keyword.value) in indicator.binding.bases
            elif keyword.arg == "by":
                assert set(_literal(keyword.value)) <= set(indicator.dimensions)
            elif keyword.arg == "lines":
                known = names("line", table=indicator.binding.table)
                assert set(_codes(keyword.value)) <= set(known)
            elif keyword.arg in CODE_ARGUMENTS:
                known = names(CODE_ARGUMENTS[keyword.arg])
                assert set(_codes(keyword.value)) <= set(known)
                assert set(_groups(keyword.value)) <= set(oda.sectors)


def _line_rows() -> list[_compat.V2Row]:
    return [
        row
        for row in MAP.values()
        if row.code.startswith(("DAC1.", "DAC2A."))
        and row.indicator not in (None, "oecd.dac1.lines", "oecd.dac2a.lines")
        and len(row.call) == 1
        and row.indicator in {i.id for i in (*dac1.INDICATORS, *dac2a.INDICATORS)}
        and basis_lines(INDICATORS[row.indicator])
    ]


def test_a_code_mapped_to_a_named_indicator_reads_the_codes_line() -> None:
    checked = 0
    for row in _line_rows():
        indicator = INDICATORS[row.indicator or ""]
        (call,) = _gets(row)
        given = {k.arg: _literal(k.value) for k in call.keywords}
        basis = given.get("basis", indicator.default_basis)
        rule = indicator.binding.bases[basis]
        used = (rule.before, rule.after) if isinstance(rule, Headline) else (basis,)
        line = int(row.code.split(".")[-1])
        assert {basis_lines(indicator)[name] for name in used} == {line}, row.code
        checked += 1
    assert checked >= 15


@pytest.mark.parametrize(("table", "module"), [("DAC1", dac1), ("DAC2A", dac2a)])
def test_a_code_mapped_to_a_lines_indicator_reads_its_line_on_a_basis_that_holds_it(
    table: str, module: object
) -> None:
    lines = module.LINES  # type: ignore[attr-defined]
    rows = [
        r
        for r in MAP.values()
        if r.code.startswith(f"{table}.") and r.indicator == lines.id
    ]
    assert rows
    for row in rows:
        (call, *_) = _gets(row)
        given = {k.arg: _literal(k.value) for k in call.keywords}
        line = given["lines"]
        if isinstance(line, int):
            assert given.get("basis", lines.default_basis) == line_basis(lines, line)


def test_dac1_grant_equivalent_sums_read_each_line_on_grant_equivalents() -> None:
    row = MAP["total_oda_grants_ge"]
    assert row.call[0] == (
        'df = oda.get("oecd.dac1.lines", lines=[11016, 12100], '
        'basis="grant_equivalent")'
    )
    assert row.call[1].startswith('df = df.groupby(["provider", "year"]')
    assert row.call[-1] == 'df["share"] = df["value"] / df["value_total"]'


def test_linked_slugs_read_the_headline_basis() -> None:
    assert MAP["idrc_ge_linked"].call[0] == (
        'df = oda.get("oecd.dac1.in_donor_refugee_costs")'
    )
    assert MAP["scholarships_ge_linked"].call[0] == (
        'df = oda.get("oecd.dac1.lines", lines=1510, basis="headline")'
    )


def test_covid_lines_map_with_no_data() -> None:
    for slug in ("total_health_covid_oda_ge", "total_covid_ancillary_oda_ge_linked"):
        row = MAP[slug]
        assert row.status == "no_data"
        assert "8852" in row.reason or "8856" in row.reason


def test_crs_codes_name_their_filters_and_split_by_their_perspective() -> None:
    assert MAP["CRS.P.10.100"].call == (
        'oda.get("oecd.crs.bilateral_oda", finance_types=[110, 210])',
    )
    assert MAP["CRS.R.21"].call == (
        'oda.get("oecd.crs.bilateral_oof", by=["recipient", "year"])',
    )
    assert MAP["CRS.P.30"].call == ('oda.get("oecd.crs.private_development_finance")',)
    assert MAP["CRS.P.30"].status == "exact"
    # By recipient, the default private donors leave out official providers'
    # category 30 rows.
    assert MAP["CRS.R.30"].status == "changes"
    assert "private_donors" in MAP["CRS.R.30"].reason
    assert "oecd.crs.multilateral_oof" in MAP["CRS.P.21"].reason


def test_the_generator_reads_a_2x_release_tag_by_default() -> None:
    """A tag, so the map stays put when ``main`` becomes 3.0."""
    script = Path(__file__).resolve().parents[2] / "scripts" / "generate_v2_map.py"
    spec = importlib.util.spec_from_file_location("generate_v2_map", script)
    assert spec is not None and spec.loader is not None
    generator = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = generator  # dataclasses look the module up
    spec.loader.exec_module(generator)

    assert re.fullmatch(r"v2\.\d+\.\d+", generator.DEFAULT_REF)


@pytest.mark.parametrize("code", ["DAC1.40.3", "DAC1.40.4", "DAC1.40.11002"])
def test_codes_2x_returned_no_rows_for_change_and_say_so(code: str) -> None:
    assert MAP[code].status == "changes"
    assert MAP[code].reason.startswith("2.x returned no rows")


def test_population_maps_to_its_indicator() -> None:
    assert MAP["DAC1.40.4"].call == ('oda.get("oecd.dac1.population")',)


def test_no_settings_slug_claims_a_2x_number() -> None:
    """No 2.x code read settings/indicators.json."""
    slugs = [row for row in MAP.values() if row.source == "settings"]
    assert {row.status for row in slugs} == {"no_2x_number", "no_data"}
    for row in slugs:
        if row.status == "no_2x_number":
            assert row.reason.startswith("No 2.x code read"), row.code


def test_core_oda_maps_to_2x_default_net_flows() -> None:
    row = MAP["ONE.10.1010C"]
    assert row.call == ('oda.get("one.dac1.core_oda", basis="net_disbursement")',)
    assert row.status == "exact"


def test_dac2a_calls_keep_the_recipient_and_name_2x_recipient_totals() -> None:
    rows = [
        row
        for row in MAP.values()
        if row.code.startswith("DAC2A.")
        or row.code in ("ONE.10.206_106", "recipient_total_flow_net")
    ]
    assert len(rows) >= 22
    for row in rows:
        assert 'by=["provider", "recipient", "year"]' in row.call[0], row.code
        assert "recipient totals" in row.reason, row.code


@pytest.mark.parametrize(
    "code", ["ONE.40.1010_11010_1", "ONE.40.1010_1", "oda_gni_flow", "oda_gni_ge"]
)
def test_oda_gni_codes_give_eu_institutions_the_2x_eu27_gni(code: str) -> None:
    assert MAP[code].indicator == "one.dac1.oda_gni_ratio_fixed_eu27"


# Errors -----------------------------------------------------------------------


def test_find_matches_a_code_in_any_case() -> None:
    assert _compat.find(" dac1.10.1010 ") is MAP["DAC1.10.1010"]
    assert _compat.find("oecd.dac1.total_oda") is None


def test_get_with_a_2x_code_raises_with_the_3_0_call() -> None:
    with pytest.raises(oda.UnknownIndicatorError) as error:
        _context.find_indicator("DAC1.10.1010")
    expected = 'oda.get("oecd.dac1.total_oda", basis="net_disbursement")'
    assert error.value.suggestions == (expected,)
    assert expected in str(error.value)


def test_get_with_a_2x_code_that_changes_names_the_reason() -> None:
    with pytest.raises(oda.UnknownIndicatorError) as error:
        _context.find_indicator("DAC2A.10.106")
    assert "The number differs from 2.x" in str(error.value)
    assert "line 106" in str(error.value)


def test_get_with_a_2x_code_without_a_call_says_why() -> None:
    with pytest.raises(oda.UnknownIndicatorError) as error:
        _context.find_indicator("total_health_covid_oda_ge")
    assert error.value.suggestions == ()
    assert "no call" in str(error.value)


def test_removed_names_cover_2x_exports() -> None:
    assert set(_compat.REMOVED) == set(V2_NAMES)


@pytest.mark.parametrize("name", V2_NAMES)
def test_a_2x_name_raises_removed_in_3_with_its_replacement(name: str) -> None:
    with pytest.raises(oda.RemovedIn3Error) as error:
        getattr(oda, name)
    assert _compat.REMOVED[name] in str(error.value)
    assert name not in dir(oda)


def test_imputation_quality_report_points_at_allocation_status() -> None:
    with pytest.raises(oda.RemovedIn3Error, match="allocation_status"):
        oda.imputation_quality_report  # noqa: B018


# Search -----------------------------------------------------------------------


def test_search_puts_a_2x_codes_call_first() -> None:
    found = oda.search("DAC1.10.1010")
    first = found.iloc[0]
    assert (first["kind"], first["key"], first["table"]) == (
        "code",
        "DAC1.10.1010",
        "dac1",
    )
    assert first["use"] == 'oda.get("oecd.dac1.total_oda", basis="net_disbursement")'


def test_search_joins_a_longer_call_and_carries_the_reason() -> None:
    first = oda.search("total_oda_grants_ge").iloc[0]
    assert first["use"].count("; ") == 4
    assert first["note"].startswith("No 2.x code read settings/indicators.json")
    changed = oda.search("CRS.P.10").iloc[0]
    assert "bi_multi" in changed["note"]


def test_search_finds_no_code_row_for_other_text() -> None:
    assert "code" not in set(oda.search("total oda")["kind"])

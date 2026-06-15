"""
Tests for DAC1 indicator generator: common mapping helpers and OECD indicator builder.

Covers:
- flow_types(): loads flow_types.json into dict[int, str]
- dac1_aid_flow_type_mapping(): loads dac1_aid_flow_types.json, maps aidtype_code -> flowtype_code
- dac1_aid_name_mapping(): maps aidtype_code -> aidtype_name
- dac1_oecd_indicators(): builds the full indicator dict

NOTE on the concurrent fix:
  dac1_aid_flow_type_mapping() had a bug reading v["flow_type"] instead of v["flowtype_code"].
  The tests below assert the CORRECT post-fix behavior (int values from "flowtype_code").
  If that fix has not landed, the TestDac1AidFlowTypeMapping tests will fail with KeyError —
  that is expected and is NOT a test bug.

NOTE on a separate known data-integrity issue:
  dac1_aid_flow_types.json contains flowtype_code values (5, 1021) that do not exist in
  flow_types.json. After the flow_type->flowtype_code fix lands, dac1_oecd_indicators()
  raises KeyError: 5 in oecd.py line 32 (`flow_names[flow]`). The TestDac1OecdIndicators
  tests expose this second bug — they should pass once flow_types.json is extended or
  dac1_aid_flow_types.json is corrected to use only codes present in flow_types.json.
"""

from oda_data.indicators.dac1.common import (
    dac1_aid_flow_type_mapping,
    dac1_aid_name_mapping,
    flow_types,
)
from oda_data.indicators.dac1.oecd import dac1_oecd_indicators

# ============================================================================
# Tests for flow_types()
# ============================================================================


class TestFlowTypes:
    """Tests for the flow_types() helper."""

    def test_flow_types_returns_nonempty_dict(self):
        """flow_types() should return a non-empty mapping."""
        result = flow_types()
        assert isinstance(result, dict)
        assert len(result) > 0

    def test_flow_types_has_int_keys(self):
        """All keys must be ints (not strings)."""
        result = flow_types()
        assert all(isinstance(k, int) for k in result)

    def test_flow_types_has_str_values(self):
        """All values must be strings."""
        result = flow_types()
        assert all(isinstance(v, str) for v in result.values())

    def test_flow_types_known_entry(self):
        """Spot-check: code 40 maps to 'Non flow'."""
        result = flow_types()
        assert result[40] == "Non flow"

    def test_flow_types_known_entry_oda(self):
        """Spot-check: code 10 maps to 'ODA'."""
        result = flow_types()
        assert result[10] == "ODA"


# ============================================================================
# Tests for dac1_aid_flow_type_mapping()
# ============================================================================


class TestDac1AidFlowTypeMapping:
    """Tests for dac1_aid_flow_type_mapping().

    IMPORTANT: these tests assert the CORRECT behavior (reading 'flowtype_code').
    If the source still reads 'flow_type', these tests will fail with KeyError —
    that indicates the concurrent fix has not landed yet.
    """

    def test_loads_without_error(self):
        """dac1_aid_flow_type_mapping() should not raise."""
        result = dac1_aid_flow_type_mapping()
        assert result is not None

    def test_returns_nonempty_dict(self):
        """Mapping should contain at least one entry."""
        result = dac1_aid_flow_type_mapping()
        assert isinstance(result, dict)
        assert len(result) > 0

    def test_has_int_keys(self):
        """All aidtype keys must be ints."""
        result = dac1_aid_flow_type_mapping()
        assert all(isinstance(k, int) for k in result)

    def test_values_are_int_flowtype_codes(self):
        """Values must be the integer flowtype_code from the JSON (post-fix)."""
        result = dac1_aid_flow_type_mapping()
        # After the fix, values are ints coming from the "flowtype_code" field.
        assert all(isinstance(v, int) for v in result.values())

    def test_known_entry_aidtype_1_maps_to_flowtype_40(self):
        """Spot-check: aidtype_code 1 -> flowtype_code 40 (Non flow / GNI)."""
        result = dac1_aid_flow_type_mapping()
        assert result[1] == 40

    def test_entry_count_matches_json(self):
        """Mapping should have exactly 139 entries (matching dac1_aid_flow_types.json)."""
        result = dac1_aid_flow_type_mapping()
        assert len(result) == 139


# ============================================================================
# Tests for dac1_aid_name_mapping()
# ============================================================================


class TestDac1AidNameMapping:
    """Tests for dac1_aid_name_mapping()."""

    def test_loads_without_error(self):
        """dac1_aid_name_mapping() should not raise."""
        result = dac1_aid_name_mapping()
        assert result is not None

    def test_returns_nonempty_dict(self):
        """Mapping should contain at least one entry."""
        result = dac1_aid_name_mapping()
        assert isinstance(result, dict)
        assert len(result) > 0

    def test_has_int_keys(self):
        """All aidtype keys must be ints."""
        result = dac1_aid_name_mapping()
        assert all(isinstance(k, int) for k in result)

    def test_has_str_values(self):
        """All values must be strings (aidtype_name fields)."""
        result = dac1_aid_name_mapping()
        assert all(isinstance(v, str) for v in result.values())

    def test_known_entry_aidtype_1(self):
        """Spot-check: aidtype_code 1 -> 'Gross National Income (GNI)'."""
        result = dac1_aid_name_mapping()
        assert result[1] == "Gross National Income (GNI)"

    def test_entry_count_matches_json(self):
        """Mapping should have exactly 139 entries (matching dac1_aid_flow_types.json)."""
        result = dac1_aid_name_mapping()
        assert len(result) == 139


# ============================================================================
# Tests for dac1_oecd_indicators()
# ============================================================================


class TestDac1OecdIndicators:
    """Tests for dac1_oecd_indicators() — the main indicator builder."""

    def test_runs_without_error(self):
        """dac1_oecd_indicators() should not raise."""
        result = dac1_oecd_indicators()
        assert result is not None

    def test_returns_nonempty_dict(self):
        """Result must be a non-empty dict."""
        result = dac1_oecd_indicators()
        assert isinstance(result, dict)
        assert len(result) > 0

    def test_keys_are_dac1_indicator_codes(self):
        """Every key should start with 'DAC1' and contain a '.' separator."""
        result = dac1_oecd_indicators()
        for key in result:
            assert key.startswith("DAC1"), f"Key {key!r} does not start with 'DAC1'"
            assert "." in key, f"Key {key!r} has no '.' separator"

    def test_each_value_is_a_dict(self):
        """Each indicator definition must be a dict."""
        result = dac1_oecd_indicators()
        for key, value in result.items():
            assert isinstance(value, dict), f"Indicator {key!r} value is not a dict"

    def test_each_indicator_has_required_fields(self):
        """Every indicator dict must carry code, name, description, sources, type, filters."""
        required_fields = {"code", "name", "description", "sources", "type", "filters"}
        result = dac1_oecd_indicators()
        for key, value in result.items():
            missing = required_fields - value.keys()
            assert not missing, f"Indicator {key!r} is missing fields: {missing}"

    def test_sources_is_dac1(self):
        """Every indicator's sources list must be ['DAC1']."""
        result = dac1_oecd_indicators()
        for key, value in result.items():
            assert value["sources"] == ["DAC1"], (
                f"Indicator {key!r} has wrong sources: {value['sources']}"
            )

    def test_type_is_dac(self):
        """Every indicator's type must be 'DAC'."""
        result = dac1_oecd_indicators()
        for key, value in result.items():
            assert value["type"] == "DAC", (
                f"Indicator {key!r} has wrong type: {value['type']!r}"
            )

    def test_description_embeds_flow_name(self):
        """Description should reference a known flow name, confirming flow_names lookup resolved."""
        known_flow_names = {
            "ODA",
            "OOF",
            "Non flow",
            "Private development finance",
            "Officially supported export credits",
        }
        result = dac1_oecd_indicators()
        # At least one indicator should include a known flow name in its description
        descriptions = [v["description"] for v in result.values()]
        assert any(
            any(name in desc for name in known_flow_names) for desc in descriptions
        ), (
            "No description contains a known flow name — flow_names lookup may have failed"
        )

    def test_indicator_count_matches_aid_flow_entries(self):
        """There should be exactly one indicator per aid/flow mapping entry (139 total)."""
        result = dac1_oecd_indicators()
        assert len(result) == 139

    def test_filters_contain_aidtype_code(self):
        """Each indicator's filters must include an aidtype_code filter under 'DAC1'."""
        result = dac1_oecd_indicators()
        for key, value in result.items():
            assert "DAC1" in value["filters"], (
                f"Indicator {key!r} missing 'DAC1' filter key"
            )
            assert "aidtype_code" in value["filters"]["DAC1"], (
                f"Indicator {key!r} missing 'aidtype_code' in DAC1 filter"
            )

    def test_indicator_codes_are_unique(self):
        """All indicator code strings must be unique (dict keys guarantee this, but also check
        the 'code' field inside each value)."""
        result = dac1_oecd_indicators()
        inner_codes = [v["code"] for v in result.values()]
        assert len(inner_codes) == len(set(inner_codes)), (
            "Duplicate 'code' values found inside indicator dicts"
        )

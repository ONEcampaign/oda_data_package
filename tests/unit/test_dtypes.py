"""Tests for oda_data.clean_data.dtypes.

Covers set_default_types()'s handling of columns whose expected dtype
doesn't hold for every DAC source, notably ODASchema.FLOW_CODE, which is
purely numeric in CRS data but alphanumeric (e.g. "E01") in Multisystem data.
"""

import pandas as pd
import pytest

from oda_data.clean_data.dtypes import schema_types, set_default_types
from oda_data.clean_data.schema import ODASchema


class TestSetDefaultTypesFlowCode:
    """Tests for the flow_code column across sources."""

    def test_crs_style_numeric_flow_code_casts_to_numeric(self):
        """CRS-style numeric flow codes still land on the numeric dtype."""
        df = pd.DataFrame({ODASchema.FLOW_CODE: ["10", "11", "30"]})

        result = set_default_types(df)

        assert result[ODASchema.FLOW_CODE].dtype == "int32[pyarrow]"
        assert list(result[ODASchema.FLOW_CODE]) == [10, 11, 30]

    def test_multisystem_style_alphanumeric_flow_code_falls_back_to_string(self):
        """Multisystem-style alphanumeric flow codes survive as strings."""
        df = pd.DataFrame({ODASchema.FLOW_CODE: ["E01", "E02", "B03"]})

        result = set_default_types(df)

        assert result[ODASchema.FLOW_CODE].dtype == "string[pyarrow]"
        assert list(result[ODASchema.FLOW_CODE]) == ["E01", "E02", "B03"]

    def test_mixed_numeric_and_alphanumeric_flow_code_falls_back_to_string(self):
        """A column mixing numeric and alphanumeric codes falls back too."""
        df = pd.DataFrame({ODASchema.FLOW_CODE: ["10", "E01", "30"]})

        result = set_default_types(df)

        assert result[ODASchema.FLOW_CODE].dtype == "string[pyarrow]"
        assert list(result[ODASchema.FLOW_CODE]) == ["10", "E01", "30"]

    def test_numeric_flow_code_with_nulls_casts_to_numeric(self):
        """Null values don't force a fallback when the rest are numeric."""
        df = pd.DataFrame({ODASchema.FLOW_CODE: ["10", None, "30"]})

        result = set_default_types(df)

        assert result[ODASchema.FLOW_CODE].dtype == "int32[pyarrow]"
        assert result[ODASchema.FLOW_CODE].isna().sum() == 1

    def test_alphanumeric_flow_code_with_nulls_falls_back_to_string(self):
        """Null values alongside alphanumeric codes still fall back cleanly."""
        df = pd.DataFrame({ODASchema.FLOW_CODE: ["E01", None, "E02"]})

        result = set_default_types(df)

        assert result[ODASchema.FLOW_CODE].dtype == "string[pyarrow]"
        assert result[ODASchema.FLOW_CODE].isna().sum() == 1

    def test_fallback_is_logged(self, caplog):
        """The fallback is explicit and logged, not silent.

        The `oda_data` logger disables propagation to the root logger (see
        oda_data.logger.setup_logger), so caplog's handler is attached to it
        directly rather than relying on `caplog.at_level`.
        """
        import logging

        from oda_data.logger import logger as oda_logger

        df = pd.DataFrame({ODASchema.FLOW_CODE: ["E01", "E02"]})

        oda_logger.addHandler(caplog.handler)
        caplog.set_level(logging.WARNING, logger="oda_data")
        try:
            set_default_types(df)
        finally:
            oda_logger.removeHandler(caplog.handler)

        messages = [record.message for record in caplog.records]
        assert any(
            "flow_code" in message and "int32[pyarrow]" in message
            for message in messages
        )

    def test_save_mode_falls_back_to_category(self):
        """In save mode, the fallback dtype matches the save-mode scheme (category)."""
        df = pd.DataFrame({ODASchema.FLOW_CODE: ["E01", "E02"]})

        result = set_default_types(df, save=True)

        assert result[ODASchema.FLOW_CODE].dtype == "category"


class TestSetDefaultTypesGeneral:
    """General set_default_types behaviour, unrelated to flow_code."""

    def test_unmapped_column_is_untouched_by_default_type_lookup(self):
        """Columns absent from the schema fall back to the defaultdict default."""
        df = pd.DataFrame({"not_a_schema_column": ["a", "b"]})

        result = set_default_types(df)

        assert result["not_a_schema_column"].dtype == "string[pyarrow]"

    def test_does_not_mutate_input_dataframe(self):
        """The input dataframe is left untouched; a new one is returned."""
        df = pd.DataFrame({ODASchema.FLOW_CODE: ["10", "11"]})
        original_dtype = df[ODASchema.FLOW_CODE].dtype

        set_default_types(df)

        assert df[ODASchema.FLOW_CODE].dtype == original_dtype

    def test_all_schema_columns_have_a_resolvable_dtype(self):
        """Every column in the schema map resolves to a usable pandas dtype."""
        types = schema_types()

        assert len(types) > 0
        for column, dtype in types.items():
            assert isinstance(column, str)
            assert isinstance(dtype, str)


class TestSetDefaultTypesMoneyColumns:
    """A dirty money/measure column must raise, never silently become text.

    Falling back to a string dtype for a money column would make
    groupby(...).sum() concatenate strings instead of summing numbers (e.g.
    "10.5" + "20.5" -> "10.520.5"), silently breaking any conservation
    guarantee built on those sums. Only the declared alphanumeric DAC code
    columns (flow_code, aidtype_code, flows_code) are allowed to fall back.
    """

    def test_dirty_money_column_raises_instead_of_falling_back_to_string(self):
        """A value that can't cast to the expected numeric dtype raises."""
        df = pd.DataFrame({ODASchema.VALUE: ["10.5", "not-a-number", "20.5"]})

        with pytest.raises(ValueError, match=ODASchema.VALUE):
            set_default_types(df)

    def test_clean_money_column_casts_normally(self):
        """A money column whose values all cast cleanly is unaffected."""
        df = pd.DataFrame({ODASchema.VALUE: ["10.5", "20.5", "30"]})

        result = set_default_types(df)

        assert result[ODASchema.VALUE].dtype == "float64"
        assert list(result[ODASchema.VALUE]) == [10.5, 20.5, 30.0]

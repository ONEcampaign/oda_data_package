import json

from oda_data.config import OdaPATHS
from oda_data.indicators.dac1.common import update_mapping_file
from oda_data.indicators.indicator import Indicator


def total_oda_official() -> Indicator:
    return Indicator(
        code="ONE.10.1010_11010",
        name="Total ODA (Official Definition)",
        description=(
            "Official Development Assistance (ODA) according "
            "to the DAC definition for each year."
        ),
        sources=["DAC1"],
        type="ONE",
        filters={"DAC1": {"aidtype_code": ("in", [1010, 11010])}},
        custom_function="official_oda",
    )


def official_oda_gni() -> Indicator:
    return Indicator(
        code="ONE.40.1010_11010_1",
        name="Total ODA (official definition) as a percentage of GNI",
        description=(
            "Official Development Assistance (ODA) as a percentage of "
            "Gross National Income (GNI) for each year. Based on the official DAC "
            "definition for each year."
        ),
        sources=["DAC1"],
        filters={"DAC1": {"aidtype_code": ("in", [1010, 11010, 1])}},
        custom_function="official_oda_gni",
    )


def oda_gni_flow() -> Indicator:
    return Indicator(
        code="ONE.40.1010_1",
        name="Total ODA (net flows) as a percentage of GNI",
        description=(
            "Official Development Assistance (ODA) net flows as a percentage of "
            "Gross National Income (GNI) for each year."
        ),
        sources=["DAC1"],
        filters={"DAC1": {"aidtype_code": ("in", [1010, 1])}},
        custom_function="oda_gni_flow",
    )


def core_oda() -> Indicator:
    return Indicator(
        code="ONE.10.1010C",
        name="Total Core ODA (ONE Definition)",
        description="Official Development Assistance (ODA) excluding in-donor spending",
        sources=["DAC1"],
        filters={
            "DAC1": {
                "aidtype_code": (
                    "in",
                    [
                        1010,  # flow
                        11010,  # ge
                        1820,  # idrc
                        1500,  # students
                        1600,  # debt relief
                    ],
                )
            }
        },
        custom_function="one_core_oda",
    )


def dac1_one_indicators():
    """Generate a json file which defines the DAC1 indicator codes, and the filtering process
    to generate them."""

    indicators = [total_oda_official(), official_oda_gni(), oda_gni_flow(), core_oda()]

    # transform list to json-like dict
    indicators = {i.code: i.to_dict for i in indicators}

    return indicators


if __name__ == "__main__":
    one_dac1_indicators = dac1_one_indicators()

    update_mapping_file({"ONE": one_dac1_indicators})

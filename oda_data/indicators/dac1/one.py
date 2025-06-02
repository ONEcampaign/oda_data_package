from oda_data.config import ODAPaths
from oda_data.indicators.common import update_mapping_file
from oda_data.indicators.indicator import Indicator


def total_oda_official() -> Indicator:
    """
    Create the Indicator for total Official Development Assistance (ODA).

    Returns:
        Indicator: Indicator instance for total ODA.
    """
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
    """
    Create the Indicator for total ODA as a percentage of GNI.

    Returns:
        Indicator: Indicator instance for total ODA as a percentage of GNI.
    """
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
    """
    Create the Indicator for ODA net flows as a percentage of GNI.

    Returns:
        Indicator: Indicator instance for ODA net flows as a percentage of GNI.
    """
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
    """
    Create the Indicator for core ODA (ONE Definition).

    Returns:
        Indicator: Indicator instance for core ODA.
    """
    return Indicator(
        code="ONE.10.1010C",
        name="Total Core ODA (ONE Definition)",
        description="Official Development Assistance (ODA) excluding in-donor spending but"
        "including administrative costs.",
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
    """
    Generate a dictionary defining the DAC1 indicator codes and their filtering logic.

    Returns:
        dict[str, dict]: Dictionary of indicator codes and their serialized definitions.
    """

    indicators = [total_oda_official(), official_oda_gni(), oda_gni_flow(), core_oda()]

    # transform list to json-like dict
    indicators = {i.code: i.to_dict for i in indicators}

    return indicators


if __name__ == "__main__":
    one_dac1_indicators = dac1_one_indicators()
    update_mapping_file(
        {"ONE": one_dac1_indicators},
        file_path=ODAPaths.indicators / "dac1" / "dac1_indicators.json",
    )

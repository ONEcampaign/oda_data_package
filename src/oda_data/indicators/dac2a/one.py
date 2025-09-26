from oda_data.config import ODAPaths
from oda_data.indicators.common import update_mapping_file
from oda_data.indicators.indicator import Indicator


def one_bilateral_plus_imputed() -> Indicator:
    return Indicator(
        code="ONE.10.206_106",
        name="Total ODA (bilateral plus imputed multilateral)",
        description=(
            "Official Development Assistance (ODA) as net bilateral disbursements plus "
            "imputed multilateral aid."
        ),
        sources=["DAC2A"],
        type="ONE",
        filters={"DAC2A": {"aidtype_code": ("in", [206, 106])}},
        custom_function="total_bilateral_plus_imputed",
    )


def dac2a_one_indicators():
    """Generate a json file which defines the DAC2a indicator codes, and the filtering process
    to generate them."""

    indicators = [one_bilateral_plus_imputed()]

    # transform list to json-like dict
    indicators = {i.code: i.to_dict for i in indicators}

    return indicators


if __name__ == "__main__":
    one_dac2a_indicators = dac2a_one_indicators()

    update_mapping_file(
        {"ONE": one_dac2a_indicators},
        file_path=ODAPaths.indicators / "dac2a" / "dac2a_indicators.json",
    )

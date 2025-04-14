from oda_data.config import ODAPaths
from oda_data.indicators.common import update_mapping_file
from oda_data.indicators.dac2a.common import aid_type
from oda_data.indicators.indicator import Indicator, SEPARATOR


def dac2a_oecd_indicators():
    """Generate a json file which defines the DAC1 indicator codes, and the filtering process
    to generate them."""

    dac2a_base: str = "DAC2A"

    # aid and flow types
    aid_types = aid_type()

    indicators = []

    for aid_code, aid_name in aid_types.items():
        indicator_ = Indicator(
            code=f"{dac2a_base}{SEPARATOR}10{SEPARATOR}{aid_code}",
            name=f"{aid_name}",
            description=f"{dac2a_base} data for {aid_name}",
            sources=["DAC2A"],
            type="DAC",
            filters={"DAC2A": {"aidtype_code": ("==", aid_code)}},
        )
        indicators.append(indicator_)

    # transform list to json-like dict
    indicators = {i.code: i.to_dict for i in indicators}

    return indicators


if __name__ == "__main__":
    oecd_dac2a_indicators = dac2a_oecd_indicators()
    update_mapping_file(
        {"DAC": oecd_dac2a_indicators},
        file_path=ODAPaths.indicators / "dac2a" / "dac2a_indicators.json",
    )

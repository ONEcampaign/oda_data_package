from oda_data.config import ODAPaths
from oda_data.indicators.dac1.common import (
    dac1_aid_flow_type_mapping,
    dac1_aid_name_mapping,
    flow_types,
)
from oda_data.indicators.common import update_mapping_file
from oda_data.indicators.indicator import Indicator, SEPARATOR


def dac1_oecd_indicators():
    """Generate a json file which defines the DAC1 indicator codes, and the filtering process
    to generate them."""

    dac1_base: str = "DAC1"

    # aid and flow types
    aid_flow = dac1_aid_flow_type_mapping()

    # aid - names
    aid_names = dac1_aid_name_mapping()

    # flow names
    flow_names = flow_types()

    indicators = []

    for aid, flow in aid_flow.items():
        indicator_ = Indicator(
            code=f"{dac1_base}{SEPARATOR}{flow}{SEPARATOR}{aid}",
            name=f"{aid_names[aid]}",
            description=f"{dac1_base} data for {aid_names[aid]} [{flow_names[flow]}]",
            sources=["DAC1"],
            type="DAC",
            filters={"DAC1": {"aidtype_code": ("==", aid)}},
        )
        indicators.append(indicator_)

    # transform list to json-like dict
    indicators = {i.code: i.to_dict for i in indicators}

    return indicators


if __name__ == "__main__":
    oecd_dac1_indicators = dac1_oecd_indicators()
    update_mapping_file(
        {"DAC": oecd_dac1_indicators},
        file_path=ODAPaths.indicators / "dac1" / "dac1_indicators.json",
    )

import json

from oda_data.config import OdaPATHS


def flow_types() -> dict[int, str]:
    with open(OdaPATHS.settings / "flow_types.json", "r") as file:
        mapping = json.load(file)

    return {int(k): v for k, v in mapping.items()}


def dac1_aid_flow_type_mapping():
    # read mapping json
    with open(OdaPATHS.settings / "dac1_aid_flow_types.json", "r") as file:
        mapping = json.load(file)

    return {int(k): v["flow_type"] for k, v in mapping.items()}


def dac1_aid_name_mapping():
    # read mapping json
    with open(OdaPATHS.settings / "dac1_aid_flow_types.json", "r") as file:
        mapping = json.load(file)

    return {int(k): v["aidtype_name"] for k, v in mapping.items()}


def update_mapping_file(new_data: dict):
    path = OdaPATHS.indicators / "dac1" / "dac1_indicators.json"

    if path.exists():
        with open(OdaPATHS.indicators / "dac1" / "dac1_indicators.json", "r") as f:
            existing_file = json.load(f)
    else:
        existing_file = {}

    data = existing_file | new_data

    with open(path, "w") as f:
        json.dump(data, f, indent=2)

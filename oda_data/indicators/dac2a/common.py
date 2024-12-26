import json

from oda_data.config import OdaPATHS


def aid_type() -> dict[int, str]:
    with open(OdaPATHS.settings / "dac2a_aid_type.json", "r") as file:
        mapping = json.load(file)

    return {int(k): v for k, v in mapping.items()}


def update_mapping_file(new_data: dict):
    path = OdaPATHS.indicators / "dac2a" / "dac2a_indicators.json"

    if path.exists():
        with open(OdaPATHS.indicators / "dac2a" / "dac2a_indicators.json", "r") as f:
            existing_file = json.load(f)
    else:
        existing_file = {}

    data = existing_file | new_data

    with open(path, "w") as f:
        json.dump(data, f, indent=2)

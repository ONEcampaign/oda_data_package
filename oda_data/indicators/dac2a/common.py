import json

from oda_data.config import ODAPaths


def aid_type() -> dict[int, str]:
    with open(ODAPaths.settings / "dac2a_aid_type.json", "r") as file:
        mapping = json.load(file)

    return {int(k): v for k, v in mapping.items()}

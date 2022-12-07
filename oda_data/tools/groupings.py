import json
from pathlib import Path

from oda_data import config


def _read_grouping(path: Path) -> dict:
    with open(path, "r") as f:
        data = json.load(f)

    for k, v in data.items():
        if isinstance(v, list):
            data[k] = {k_: v_ for g in [data[c] for c in v] for k_, v_ in g.items()}
        elif isinstance(v, dict):
            data[k] = {int(k_): v_ for k_, v_ in v.items()}
    return data


def donor_groupings() -> dict:
    """Read the donor groupings from the json file"""
    path = config.OdaPATHS.settings / "donor_groupings.json"

    return _read_grouping(path)


def recipient_groupings() -> dict:
    """Read the recipient groupings from the json file"""
    path = config.OdaPATHS.settings / "recipient_groupings.json"

    return _read_grouping(path)

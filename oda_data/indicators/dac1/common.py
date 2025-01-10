import json
from pathlib import Path
from oda_data.config import OdaPATHS

def load_json_file(file_path: Path) -> dict:
    """
    Load a JSON file from the specified path.

    Args:
        file_path (Path): Path to the JSON file.

    Returns:
        dict: Parsed content of the JSON file.
    """
    with open(file_path, "r") as file:
        return json.load(file)

def flow_types() -> dict[int, str]:
    """
    Load and return the mapping of flow types from `flow_types.json`.

    Returns:
        dict[int, str]: Mapping of flow type IDs to their names.
    """
    file_path = OdaPATHS.settings / "flow_types.json"
    mapping = load_json_file(file_path)
    return {int(k): v for k, v in mapping.items()}

def dac1_aid_flow_type_mapping() -> dict[int, str]:
    """
    Load and return the DAC1 aid flow type mapping from `dac1_aid_flow_types.json`.

    Returns:
        dict[int, str]: Mapping of flow type IDs to flow types.
    """
    file_path = OdaPATHS.settings / "dac1_aid_flow_types.json"
    mapping = load_json_file(file_path)
    return {int(k): v["flow_type"] for k, v in mapping.items()}

def dac1_aid_name_mapping() -> dict[int, str]:
    """
    Load and return the DAC1 aid name mapping from `dac1_aid_flow_types.json`.

    Returns:
        dict[int, str]: Mapping of flow type IDs to aid type names.
    """
    file_path = OdaPATHS.settings / "dac1_aid_flow_types.json"
    mapping = load_json_file(file_path)
    return {int(k): v["aidtype_name"] for k, v in mapping.items()}

def update_mapping_file(new_data: dict) -> None:
    """
    Update or create the `dac1_indicators.json` file with new data.

    Args:
        new_data (dict): New data to be merged into the JSON file.
    """
    file_path = OdaPATHS.indicators / "dac1" / "dac1_indicators.json"

    if file_path.exists():
        existing_file = load_json_file(file_path)
    else:
        existing_file = {}

    data = existing_file | new_data

    with open(file_path, "w") as file:
        json.dump(data, file, indent=2)

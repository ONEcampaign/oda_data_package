from oda_data.config import ODAPaths
from oda_data.indicators.common import load_json_file


def flow_types() -> dict[int, str]:
    """
    Load and return the mapping of flow types from `flow_types.json`.

    Returns:
        dict[int, str]: Mapping of flow type IDs to their names.
    """
    file_path = ODAPaths.settings / "flow_types.json"
    mapping = load_json_file(file_path)
    return {int(k): v for k, v in mapping.items()}


def dac1_aid_flow_type_mapping() -> dict[int, str]:
    """
    Load and return the DAC1 aid flow type mapping from `dac1_aid_flow_types.json`.

    Returns:
        dict[int, str]: Mapping of flow type IDs to flow types.
    """
    file_path = ODAPaths.settings / "dac1_aid_flow_types.json"
    mapping = load_json_file(file_path)
    return {int(k): v["flow_type"] for k, v in mapping.items()}


def dac1_aid_name_mapping() -> dict[int, str]:
    """
    Load and return the DAC1 aid name mapping from `dac1_aid_flow_types.json`.

    Returns:
        dict[int, str]: Mapping of flow type IDs to aid type names.
    """
    file_path = ODAPaths.settings / "dac1_aid_flow_types.json"
    mapping = load_json_file(file_path)
    return {int(k): v["aidtype_name"] for k, v in mapping.items()}

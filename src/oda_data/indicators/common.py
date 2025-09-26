import json
from pathlib import Path


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


def update_mapping_file(new_data: dict, file_path: Path) -> None:
    """
    Update or create the `dac1_indicators.json` file with new data.

    Args:
        new_data (dict): New data to be merged into the JSON file.
        file_path (Path): Path to the JSON file.
    """

    if file_path.exists():
        existing_file = load_json_file(file_path)
    else:
        existing_file = {}

    data = existing_file | new_data

    with open(file_path, "w") as file:
        json.dump(data, file, indent=2)

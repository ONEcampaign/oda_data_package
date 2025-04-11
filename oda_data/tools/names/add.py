"""Adds human-readable name columns to a DataFrame based on code columns."""

import pandas as pd
from oda_data.tools.names.create_mapping import get_merged_names_mapping


def add_names_columns(
    data: pd.DataFrame, codes_columns: str | list[str]
) -> pd.DataFrame:
    """Adds readable name columns to a DataFrame based on code columns.

    For each specified column that contains coded identifiers (e.g., recipient_code),
    this function adds a new column with the corresponding name (e.g., recipient_name),
    using a predefined mapping.

    Args:
        data: Input DataFrame to which name columns will be added.
        codes_columns: A column name or list of column names that contain codes
            to map to names.

    Returns:
        The input DataFrame with new name columns inserted next to the corresponding
        code columns.

    Raises:
        ValueError: If a specified code column does not exist in the DataFrame
            or has no available mapping.
    """
    if data.empty:
        return data

    codes_to_names = get_merged_names_mapping()

    if isinstance(codes_columns, str):
        codes_columns = [codes_columns]

    for code_col in codes_columns:
        if code_col not in data.columns:
            raise ValueError(
                f"The specified code column '{code_col}' does not exist in the DataFrame."
            )

        if code_col not in codes_to_names:
            raise ValueError(f"No name mapping available for code column '{code_col}'.")

        name_col = f"{code_col.replace('_code', '')}_name"
        if name_col not in data.columns:
            data.insert(
                loc=data.columns.get_loc(code_col) + 1,
                column=name_col,
                value=data[code_col].astype(str).map(codes_to_names[code_col]),
            )

    return data

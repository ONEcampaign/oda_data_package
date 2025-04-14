"""
This module defines the process for generating CRS OECD indicators, including generating unique rows
for CRS indicators, mappings, and filtering, as well as updating indicator mapping files.
"""

from collections import OrderedDict

import pandas as pd

from oda_data import CRSData
from oda_data.config import ODAPaths
from oda_data.indicators.common import update_mapping_file
from oda_data.indicators.crs.common import (
    read_crs_type_of_flow,
    read_crs_type_of_finance_mapping,
    read_crs_modality_mapping,
    read_crs_purpose_mapping,
)
from oda_data.indicators.indicator import Indicator, SEPARATOR


def generate_totals(crs_indicators: pd.DataFrame) -> pd.DataFrame:
    """Generate totals for CRS indicators.

    Args:
        crs_indicators (pd.DataFrame): DataFrame containing CRS indicators.

    Returns:
        pd.DataFrame: DataFrame with aggregated totals.
    """
    totals = []
    columns = []
    columns_g = []

    # Iteratively group and aggregate data to generate totals
    for idx, column in enumerate(crs_indicators.columns):

        # Create a grouping Total
        if idx >= 2:
            columns_g.append(column)
            dft = crs_indicators.assign(**{c: "T" for c in columns_g}).drop_duplicates()
            totals.append(dft)

        # Track visited columns
        columns.append(column)

        if len(columns) >= len(crs_indicators.columns):
            continue

        df = (
            crs_indicators.groupby(columns, dropna=False, observed=True)
            .agg({c: lambda d: "T" for c in crs_indicators.columns if c not in columns})
            .reset_index()
        )

        totals.append(df)

    # Concatenate all total rows into a single DataFrame
    return pd.concat(totals, ignore_index=True)


def generate_partial_totals(crs_indicators: pd.DataFrame) -> pd.DataFrame:
    """
    Generate partial totals for specific columns in a DataFrame.

    Args:
        crs_indicators (pd.DataFrame): Input DataFrame with CRS indicators.

    Returns:
        pd.DataFrame: DataFrame with partial totals generated for specified columns.
    """
    # List of columns for which partial totals will be generated
    partial_totals = {"type_of_finance", "modality", "purpose_code"}

    # Create partial totals by modifying one column at a time
    indicators = [
        crs_indicators.assign(**{col: "T"}).drop_duplicates() for col in partial_totals
    ]

    # Concatenate all the modified DataFrames
    return pd.concat(indicators, ignore_index=True)


def map_column(
    data: pd.DataFrame, column: str, mapping_func: callable, fill_value=None
) -> pd.Series:
    """Map a column using a specified mapping function and fill missing values.

    Args:
        data (pd.DataFrame): DataFrame containing the column to map.
        column (str): Name of the column to map.
        mapping_func (callable): Function to retrieve mapping data.
        fill_value (optional): Value to fill for missing data after mapping.

    Returns:
        pd.Series: Mapped column with missing values filled.
    """
    mapping = {k: v["code"] for k, v in mapping_func().items()}
    mapped_column = data[column].map(mapping).fillna(data[column])

    # Only apply fill_value if it's not None
    if fill_value is not None:
        mapped_column = mapped_column.fillna(fill_value)

    return mapped_column


def add_perspectives(data: pd.DataFrame, perspective_values: list[str]) -> pd.DataFrame:
    """Add perspectives to the DataFrame by duplicating rows for each perspective.

    Args:
        data (pd.DataFrame): Original DataFrame.
        perspective_values (list[str]): List of perspectives to assign (e.g., ["P", "R"]).

    Returns:
        pd.DataFrame: DataFrame with perspectives added.
    """
    return pd.concat(
        [data.copy().assign(perspective=p) for p in perspective_values],
        ignore_index=True,
    )


def _filter_for_output(data: pd.DataFrame) -> pd.DataFrame:
    """Filter the data for output"""

    return data.filter(
        [
            "source",  # Source
            "perspective",  # Perspective
            "category",  # Type of flow
            "mapped_type_of_finance",  # Type of finance
            "mapped_modality",  # Modality
            "mapped_purpose",  # Purpose
        ]
    )


def _rename_columns_to_crs_names(data: pd.DataFrame) -> pd.DataFrame:
    """Rename columns to CRS names"""
    return data.rename(
        columns={
            "category": "type_of_flow",
            "mapped_type_of_finance": "type_of_finance",
            "mapped_modality": "modality",
            "mapped_purpose": "purpose_code",
        }
    )


def unique_crs_indicator_rows(crs: pd.DataFrame) -> pd.DataFrame:
    """Generate unique rows for CRS indicators based on specified mappings and fields.

    Args:
        crs (pd.DataFrame): DataFrame containing CRS data.

    Returns:
        pd.DataFrame: DataFrame with unique CRS indicator rows and totals.
    """
    # Add source column
    crs["source"] = "CRS"

    # Map relevant columns
    crs["mapped_type_of_finance"] = map_column(
        crs, "type_of_finance", read_crs_type_of_finance_mapping
    )
    crs["mapped_modality"] = map_column(
        crs, "modality", read_crs_modality_mapping, fill_value="X"
    )
    crs["mapped_purpose"] = map_column(
        crs, "sector_code", read_crs_purpose_mapping
    ).astype("int32[pyarrow]")

    # Add perspectives
    crs = add_perspectives(crs, perspective_values=["P", "R"])

    # Filter and rename columns to retain only unique rows
    crs = (
        crs.pipe(_filter_for_output)
        .drop_duplicates()
        .pipe(_rename_columns_to_crs_names)
    )

    # Generate totals
    totals = generate_totals(crs)

    # Generate partial totals
    partial_totals = generate_partial_totals(crs)

    # concatenate all indicators
    crs = pd.concat([totals, partial_totals, crs], ignore_index=True)

    return crs


def load_mapping(mapping_func: callable) -> dict:
    """Load and process mapping data using a specified function.

    Args:
        mapping_func (callable): Function to load mapping data.

    Returns:
        dict: Mapping dictionary with codes as keys and names as values.
    """
    return {v["code"]: v["name"] for k, v in mapping_func().items()}


def mappings() -> dict:
    """Retrieve all mappings for CRS indicators.

    Returns:
        dict: Dictionary containing mappings for perspectives, types of flow,
              types of finance, modalities, and purpose codes.
    """
    return {
        "perspectives": {"P": "Provider", "R": "Recipient"},
        "type_of_flow": load_mapping(read_crs_type_of_flow),
        "type_of_finance": load_mapping(read_crs_type_of_finance_mapping),
        "modality": load_mapping(read_crs_modality_mapping),
        "purpose_code": load_mapping(read_crs_purpose_mapping),
    }


def indicator_code(row) -> str:
    """Generate an indicator code for a given row.

    Args:
        row: Row containing indicator data.

    Returns:
        str: Indicator code.
    """
    return (
        f"{row.source}{SEPARATOR}{row.perspective}{SEPARATOR}{row.type_of_flow}{SEPARATOR}"
        f"{row.type_of_finance}{SEPARATOR}{row.modality}{SEPARATOR}{row.purpose_code}"
    ).rstrip(".T")


def indicator_name(row) -> str:
    """Generate a descriptive name for an indicator.

    Args:
        row: Row containing indicator data.

    Returns:
        str: Indicator name.
    """
    names = mappings()
    return (
        (
            f"{names['type_of_flow'].get(row.type_of_flow, '')}, "
            f"{names['type_of_finance'].get(row.type_of_finance, '')}, "
            f"{names['modality'].get(row.modality, '')}, "
            f"{names['purpose_code'].get(row.purpose_code, '')} "
        )
        .rstrip(", ")
        .strip()
    )


def format_description(*parts: str, perspective: str) -> str:
    """Format the indicator description string by cleaning up unnecessary whitespace.

    Args:
        parts (str): Variable-length list of description parts.
        perspective (str): The perspective to include in parentheses.

    Returns:
        str: Cleaned and formatted description.
    """
    description = " ".join(filter(None, parts)).strip()
    description = description.replace(" for", "").replace("   ", " ").replace("  ", " ")
    return f"Bilateral {description} ({perspective})"


def indicator_description(row, mapping: dict = None) -> str:
    """Generate a detailed description for an indicator.

    Args:
        row: Row containing indicator data.
        mappings(dict): A dictionary of mappings for type_of_flow, type_of_finance, etc.
                         If not provided, it will be fetched dynamically.

    Returns:
        str: Indicator description.
    """

    # Get description parts from mappings
    type_of_flow = mapping["type_of_flow"].get(row.type_of_flow, "")
    type_of_finance = mapping["type_of_finance"].get(row.type_of_finance, "")
    modality = mapping["modality"].get(row.modality, "")
    purpose_code = mapping["purpose_code"].get(row.purpose_code, "")
    perspective = mapping["perspectives"].get(row.perspective, "")

    # Format and clean up description
    return format_description(
        type_of_flow, type_of_finance, modality, purpose_code, perspective=perspective
    )


def apply_filter(
    row_value, mapping_func: callable, special_case: str | None = None
) -> tuple | None:
    """Generate a filter for a specific field based on the mapping function.

    Args:
        row_value: The value from the row to filter.
        mapping_func (callable): A function to retrieve mapping data.
        special_case (str | None): Value to handle as a special case (e.g., 'X').

    Returns:
        tuple | None: Filter condition or None if no filter is applied.
    """
    if pd.isna(row_value) or row_value == special_case:
        return ("is", None)
    elif row_value != "T":
        mapped_values = [k for k, v in mapping_func().items() if v["code"] == row_value]
        return ("in", mapped_values)


def filters(row) -> dict:
    """Generate filters for an indicator based on row data.

    Args:
        row: Row containing indicator data.

    Returns:
        dict: Filters as a dictionary.
    """
    fs = {}

    # Apply filters for type_of_flow
    if row.type_of_flow != "T":
        fs["category"] = ("==", row.type_of_flow)

    # Apply filters for type_of_finance, modality, and purpose_code
    fs["type_of_finance"] = apply_filter(
        row.type_of_finance, mapping_func=read_crs_type_of_finance_mapping
    )
    fs["modality"] = apply_filter(
        row.modality, mapping_func=read_crs_modality_mapping, special_case="X"
    )
    fs["purpose_code"] = apply_filter(
        row.purpose_code, mapping_func=read_crs_purpose_mapping
    )

    # Remove None filters to keep the output clean
    return {k: v for k, v in fs.items() if v is not None}


def crs_oecd_indicators(crs_years: list | int) -> dict:
    """Generate a JSON-like dictionary defining DAC1 indicator codes and filtering processes.

    Returns:
        dict: Dictionary containing indicator data.
    """
    crs_reader = CRSData(years=crs_years)
    crs = crs_reader.read(using_bulk_download=True)
    indicators_data = unique_crs_indicator_rows(crs)
    mapping = mappings()

    indicators = []

    # Create Indicator objects
    for row in indicators_data.itertuples(index=False):
        indicator_ = Indicator(
            code=indicator_code(row),
            name=indicator_name(row),
            description=indicator_description(row, mapping=mapping),
            sources=["CRS"],
            type="DAC",
            filters={row.source: filters(row)},
        )
        indicators.append(indicator_)

    # Exclude certain indicators from the final output
    excluded = ["CRS", "CRS.P", "CRS.R"]

    # Transform list to JSON-like dictionary and sort keys
    indicators = OrderedDict(
        sorted(
            {i.code: i.to_dict for i in indicators if i.code not in excluded}.items()
        )
    )

    return indicators


if __name__ == "__main__":
    # Generate and update CRS OECD indicators mapping file
    oecd = crs_oecd_indicators(2022)
    update_mapping_file(
        {"DAC": oecd}, file_path=ODAPaths.indicators / "crs" / "crs_indicators.json"
    )

import json
from typing import Callable, Dict, Any

import pandas as pd

from oda_data.api.constants import MEASURES, Measure
from oda_data.clean_data.schema import ODASchema
from oda_data.config import ODAPaths


def load_json(file_path: str) -> dict:
    """Load a JSON file and return its content."""
    with open(file_path, "r") as f:
        return json.load(f)


def save_json(data: dict, file_path: str) -> None:
    """Save a dictionary to a JSON file."""
    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)


def create_mapping_file(
    crs: pd.DataFrame,
    column: str,
    mapping_func: Callable[[Any], Dict[str, Any]],
    output_file: str,
    fill_value: Any = None,
) -> None:
    """Generate a mapping file from a CRS DataFrame.

    Args:
        crs (pd.DataFrame): Input DataFrame containing the relevant column.
        column (str): Column name to process.
        mapping_func (Callable): Function to map values to a dictionary structure.
        output_file (str): File path to save the mapping JSON.
        fill_value (Any, optional): Value to fill NaNs in the column.
    """
    # Select relevant column
    data = crs[[column]].copy()

    # Apply fillna only if a fill_value is provided
    if fill_value is not None:
        data[column] = data[column].fillna(fill_value)

    # Replace pandas.NA or other missing values with a valid key like "X"
    data[column] = data[column].apply(lambda x: "X" if pd.isna(x) else x)

    # Remove duplicates
    data = data.drop_duplicates()

    # Generate mapping
    mapping = {str(value): mapping_func(value) for value in data[column]}

    # Save mapping to file
    save_json(mapping, output_file)


def generate_crs_type_of_flow_mapping(crs: pd.DataFrame) -> None:
    """Generate a mapping of CRS flow types to category names."""
    flow_types = load_json(ODAPaths.settings / "flow_types.json")
    mapping_func = lambda category: {
        "code": int(category),
        "name": flow_types[str(int(category))],
    }
    create_mapping_file(
        crs=crs,
        column="category",
        mapping_func=mapping_func,
        output_file=ODAPaths.settings / "crs_flow_types.json",
    )


def read_crs_type_of_flow() -> dict:
    """Read CRS type of flow mapping."""
    return {
        int(k): v
        for k, v in load_json(ODAPaths.settings / "crs_flow_types.json").items()
    }


def generate_crs_type_of_finance_mapping(crs: pd.DataFrame) -> None:
    """Generate and save a mapping of CRS type_of_finance to broad categories."""
    broad_mapping = {
        0: "Non flows",
        100: "Grants",
        420: "Debt instruments",
        430: "Mezzanine finance instruments",
        500: "Equity and shares in collective investment vehicles",
        600: "Debt relief",
        1000: "Guarantees and other unfunded contingent liabilities",
    }

    def mapping_func(finance_type: int) -> dict:
        if 100 <= finance_type < 400:
            group = 100
        elif 420 <= finance_type < 430:
            group = 420
        elif 430 <= finance_type < 440:
            group = 430
        elif 500 <= finance_type < 600:
            group = 500
        elif 600 <= finance_type < 700:
            group = 600
        elif finance_type > 1000:
            group = 1000
        else:
            group = finance_type
        return {"code": group, "name": broad_mapping[group]}

    create_mapping_file(
        crs=crs,
        column="type_of_finance",
        mapping_func=mapping_func,
        output_file=ODAPaths.settings / "crs_type_of_finance.json",
    )


def read_crs_type_of_finance_mapping() -> dict:
    """Read CRS type of finance mapping."""
    return {
        int(k): v
        for k, v in load_json(ODAPaths.settings / "crs_type_of_finance.json").items()
    }


def generate_crs_modality_mapping(crs: pd.DataFrame) -> None:
    """Generate a mapping of CRS modalities."""
    modality_names = {
        "A": "Budget support",
        "B": "Core contributions and pooled programmes and funds",
        "C": "Project-type interventions",
        "D": "Experts and other technical assistance",
        "E": "Scholarships and student costs in donor countries",
        "F": "Debt relief",
        "G": "Administrative costs not included elsewhere",
        "H": "Other in-donor expenditures",
        "X": "Modality not specified",
    }

    def mapping_func(modality: str) -> dict:
        """Map a modality to its group and name."""
        if pd.isna(modality):  # Check explicitly if modality is NaN or NA
            group = "X"
        else:
            group = modality[0]
        # Use a fallback only for truly undefined modalities
        if group in modality_names:
            name = modality_names[group]
        else:
            name = "Undefined modality"
        return {"code": group, "name": name}

    create_mapping_file(
        crs=crs,
        column="modality",
        mapping_func=mapping_func,
        output_file=ODAPaths.settings / "crs_modalities.json",
    )


def read_crs_modality_mapping() -> dict:
    """Read CRS modality mapping."""
    return load_json(ODAPaths.settings / "crs_modalities.json")


def generate_crs_purpose_mapping(crs: pd.DataFrame) -> None:
    """Generate a mapping of CRS purpose codes."""
    sectors = {
        100: "Social infrastructure",
        110: "Education",
        120: "Health",
        130: "Population Policies/Programmes & Reproductive Health",
        140: "Water Supply & Sanitation",
        150: "Government & Civil Society",
        160: "Other Social Infrastructure & Services",
        200: "Economic infrastructure",
        210: "Transport & Storage",
        220: "Communications",
        230: "Energy",
        240: "Banking & Financial Services",
        250: "Business & Other Services",
        310: "Agriculture, Forestry, Fishing",
        320: "Industry, Mining, Construction",
        330: "Trade Policies & Regulations",
        410: "General Environment Protection",
        430: "Other Multisector",
        510: "General Budget Support",
        520: "Development Food Assistance",
        530: "Other Commodity Assistance",
        600: "Action Relating to Debt",
        720: "Emergency Response",
        730: "Reconstruction Relief & Rehabilitation",
        740: "Disaster Prevention & Preparedness",
        910: "Administrative Costs of Donors",
        930: "Refugees in Donor Countries",
        998: "Unallocated / Unspecified",
    }

    def mapping_func(sector_code: int) -> dict:
        group = max(k for k in sectors.keys() if k <= sector_code)
        return {"code": group, "name": sectors[group]}

    create_mapping_file(
        crs=crs,
        column="sector_code",
        mapping_func=mapping_func,
        output_file=ODAPaths.settings / "crs_purpose.json",
        fill_value=998,
    )


def read_crs_purpose_mapping() -> dict:
    """Read CRS purpose mapping."""
    return {
        int(k): v for k, v in load_json(ODAPaths.settings / "crs_purpose.json").items()
    }


def generate_crs_policy_markers() -> None:
    """Generate a mapping of CRS policy markers."""
    markers = {
        "gender": "GEN",
        "environment": "ENV",
        "dig_code": "DIG",
        "trade": "TRD",
        "rmnch_code": "RMNCH",
        "drr_code": "DRR",
        "nutrition": "NUT",
        "disability": "DIS",
        "biodiversity": "BIO",
        "climate_mitigation": "CM",
        "climate_adaptation": "CA",
        "desertification": "DES",
    }
    save_json(markers, ODAPaths.settings / "crs_policy_markers.json")


def read_crs_policy_markers() -> dict:
    """Read CRS policy markers mapping."""
    return load_json(ODAPaths.settings / "crs_policy_markers.json")


def crs_value_cols() -> dict:
    return {k: v["column"] for k, v in MEASURES["CRS"].items()}


def group_data_based_on_indicator(
    data: pd.DataFrame, indicator_code: str, measures: list[str] | Measure
) -> pd.DataFrame:
    """Group the data based on the indicator code"""
    columns = crs_value_cols()

    measures = [columns[measure] for measure in measures]

    idx = [
        "year",
        "donor_code",
        "de_donorcode",
        "donor_name",
        "recipient_code",
        "de_recipientcode",
        "recipient_name",
        "recipient_region_code",
        "recipient_region",
        "recipient_income_code",
        "incomegroup_name",
    ]
    parts = indicator_code.split(".")[2:]

    grouper = [
        "category",  # Type of flow
        "type_of_finance",  # Type of finance
        "modality",  # Modality
        "purpose_code",  # Purpose
    ][: len(parts)]

    valid_grouper = [c for c in idx + grouper if c in data.columns]

    data = (
        data.groupby(valid_grouper, dropna=False, observed=True)[measures]
        .sum()
        .reset_index()
    )

    data = data.melt(
        id_vars=valid_grouper, value_vars=measures, var_name=ODASchema.MEASURE
    )

    data[ODASchema.MEASURE] = data[ODASchema.MEASURE].map(
        {v: k for k, v in columns.items()}
    )

    return data

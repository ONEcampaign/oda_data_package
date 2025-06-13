import json

import pandas as pd

from oda_data.clean_data.schema import ODASchema
from oda_data.config import ODAPaths
from oda_data.indicators.common import update_mapping_file


def snake_to_pascal(name: str, *, keep_single_letter: bool = False) -> str:
    """
    Convert a snake_case string to PascalCase.

    Parameters
    ----------
    name : str
        The snake_case identifier to convert.
    keep_single_letter : bool, default False
        If True, preserve the underscore that precedes a 1-letter
        segment (e.g. 'aid_t' → 'Aid_T').  If False, drop it
        ('aid_t' → 'AidT').

    Returns
    -------
    str
        The PascalCase version.
    """
    parts = name.split("_")
    if not keep_single_letter:
        # Simple path: just capitalise everything and join.
        return "".join(p.capitalize() for p in parts)

    # Preserve “_” before any single-letter trailing part.
    out: list[str] = []
    for part in parts:
        if len(part) == 1 and out:
            out[-1] = out[-1] + "_" + part.upper()
        else:
            out.append(part.capitalize())
    return "".join(out)

def _code_name(df: pd.DataFrame, code_col: str, name_col: str) -> dict:
    """Return a dictionary mapping code to name."""
    return (
        df.filter([code_col, name_col])
        .drop_duplicates()
        .dropna(subset=[code_col])
        .set_index(code_col)[name_col]
        .to_dict()
    )


def dac1_codes_names():
    """Return a dictionary of code to name mappings for DAC1 columns."""
    from oda_data import DAC1Data

    dac1 = DAC1Data(years=[1990, 2000, 2010, 2020, 2023])

    columns = [
        ODASchema.PROVIDER_CODE,
        ODASchema.PROVIDER_NAME,
        ODASchema.AIDTYPE_CODE,
        ODASchema.AIDTYPE_NAME,
        ODASchema.FLOWS_CODE,
        ODASchema.FUND_FLOWS,
    ]

    df = dac1.read(using_bulk_download=True, columns=columns)

    data = {
        ODASchema.PROVIDER_CODE: _code_name(
            df, ODASchema.PROVIDER_CODE, ODASchema.PROVIDER_NAME
        ),
        ODASchema.AIDTYPE_CODE: _code_name(
            df, ODASchema.AIDTYPE_CODE, ODASchema.AIDTYPE_NAME
        ),
        ODASchema.FLOWS_CODE: _code_name(
            df, ODASchema.FLOWS_CODE, ODASchema.FUND_FLOWS
        ),
    }

    update_mapping_file(data, ODAPaths.names / "dac1_names.json")


def dac2a_codes_names():
    """Return a dictionary of code to name mappings for DAC2a columns."""
    from oda_data import DAC2AData

    dac2a = DAC2AData(years=[1990, 2000, 2010, 2020, 2023])

    columns = [
        ODASchema.PROVIDER_CODE,
        ODASchema.PROVIDER_NAME,
        ODASchema.RECIPIENT_CODE,
        ODASchema.RECIPIENT_NAME,
        ODASchema.AIDTYPE_CODE,
        ODASchema.AIDTYPE_NAME,
    ]

    df = dac2a.read(using_bulk_download=True, columns=columns)

    data = {
        ODASchema.PROVIDER_CODE: _code_name(
            df, ODASchema.PROVIDER_CODE, ODASchema.PROVIDER_NAME
        ),
        ODASchema.RECIPIENT_CODE: _code_name(
            df, ODASchema.RECIPIENT_CODE, ODASchema.RECIPIENT_NAME
        ),
        ODASchema.AIDTYPE_CODE: _code_name(
            df, ODASchema.AIDTYPE_CODE, ODASchema.AIDTYPE_NAME
        ),
    }

    update_mapping_file(data, ODAPaths.names / "dac2a_names.json")


def crs_codes_names():
    """Return a dictionary of code to name mappings for CRS columns."""
    from oda_data import CRSData

    crs = CRSData(years=[1995, 2010, 2020, 2023])

    columns = [
        ODASchema.PROVIDER_CODE,
        ODASchema.PROVIDER_NAME,
        ODASchema.PROVIDER_CODE_DE,
        ODASchema.AGENCY_CODE,
        ODASchema.AGENCY_NAME,
        ODASchema.RECIPIENT_CODE,
        ODASchema.RECIPIENT_CODE_DE,
        ODASchema.RECIPIENT_REGION_CODE,
        ODASchema.RECIPIENT_REGION_CODE_DE,
        ODASchema.RECIPIENT_INCOME_CODE,
        ODASchema.RECIPIENT_INCOME,
        ODASchema.RECIPIENT_REGION,
        ODASchema.RECIPIENT_NAME,
        ODASchema.FLOW_CODE,
        ODASchema.FLOW_NAME,
        ODASchema.PURPOSE_CODE,
        ODASchema.PURPOSE_NAME,
        ODASchema.SECTOR_CODE,
        ODASchema.SECTOR_NAME,
        ODASchema.CHANNEL_CODE,
        ODASchema.CHANNEL_NAME,
    ]

    df = crs.read(using_bulk_download=True, columns=columns)

    data = {
        ODASchema.PROVIDER_CODE: _code_name(
            df, ODASchema.PROVIDER_CODE, ODASchema.PROVIDER_NAME
        ),
        ODASchema.PROVIDER_CODE_DE: _code_name(
            df, ODASchema.PROVIDER_CODE_DE, ODASchema.PROVIDER_NAME
        ),
        ODASchema.AGENCY_CODE: _code_name(
            df, ODASchema.AGENCY_CODE, ODASchema.AGENCY_NAME
        ),
        ODASchema.RECIPIENT_CODE: _code_name(
            df, ODASchema.RECIPIENT_CODE, ODASchema.RECIPIENT_NAME
        ),
        ODASchema.RECIPIENT_CODE_DE: _code_name(
            df, ODASchema.RECIPIENT_CODE_DE, ODASchema.RECIPIENT_NAME
        ),
        ODASchema.RECIPIENT_REGION_CODE: _code_name(
            df, ODASchema.RECIPIENT_REGION_CODE, ODASchema.RECIPIENT_REGION
        ),
        ODASchema.RECIPIENT_REGION_CODE_DE: _code_name(
            df, ODASchema.RECIPIENT_REGION_CODE_DE, ODASchema.RECIPIENT_REGION
        ),
        ODASchema.RECIPIENT_INCOME_CODE: _code_name(
            df, ODASchema.RECIPIENT_INCOME_CODE, ODASchema.RECIPIENT_INCOME
        ),
        ODASchema.FLOW_CODE: _code_name(df, ODASchema.FLOW_CODE, ODASchema.FLOW_NAME),
        ODASchema.PURPOSE_CODE: _code_name(
            df, ODASchema.PURPOSE_CODE, ODASchema.PURPOSE_NAME
        ),
        ODASchema.SECTOR_CODE: _code_name(
            df, ODASchema.SECTOR_CODE, ODASchema.SECTOR_NAME
        ),
        ODASchema.CHANNEL_CODE: _code_name(
            df, ODASchema.CHANNEL_CODE, ODASchema.CHANNEL_NAME
        ),
    }

    update_mapping_file(data, ODAPaths.names / "crs_names.json")


def get_merged_names_mapping():

    files = ["dac1_names.json", "dac2a_names.json", "crs_names.json"]

    merged_mapping = {}

    for file in files:
        file_path = ODAPaths.names / file
        with open(file_path, "r") as f:
            mapping = json.load(f)
            for k, v in mapping.items():
                if k not in merged_mapping:
                    merged_mapping[k] = {}
                merged_mapping[k].update(v)

    # Remove keys with empty dictionaries
    merged_mapping = {k: v for k, v in merged_mapping.items() if v}

    return merged_mapping

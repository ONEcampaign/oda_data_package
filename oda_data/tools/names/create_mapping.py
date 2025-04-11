import json

import pandas as pd

from oda_data.clean_data.schema import OdaSchema
from oda_data.config import OdaPATHS
from oda_data.indicators.common import update_mapping_file


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
    from oda_data import Dac1Data

    dac1 = Dac1Data(years=[1990, 2000, 2010, 2020, 2023])

    columns = [
        OdaSchema.PROVIDER_CODE,
        OdaSchema.PROVIDER_NAME,
        OdaSchema.AIDTYPE_CODE,
        OdaSchema.AIDTYPE_NAME,
        OdaSchema.FLOWS_CODE,
        OdaSchema.FUND_FLOWS,
    ]

    df = dac1.read(using_bulk_download=True, columns=columns)

    data = {
        OdaSchema.PROVIDER_CODE: _code_name(
            df, OdaSchema.PROVIDER_CODE, OdaSchema.PROVIDER_NAME
        ),
        OdaSchema.AIDTYPE_CODE: _code_name(
            df, OdaSchema.AIDTYPE_CODE, OdaSchema.AIDTYPE_NAME
        ),
        OdaSchema.FLOWS_CODE: _code_name(
            df, OdaSchema.FLOWS_CODE, OdaSchema.FUND_FLOWS
        ),
    }

    update_mapping_file(data, OdaPATHS.names / "dac1_names.json")


def dac2a_codes_names():
    """Return a dictionary of code to name mappings for DAC2a columns."""
    from oda_data import Dac2aData

    dac2a = Dac2aData(years=[1990, 2000, 2010, 2020, 2023])

    columns = [
        OdaSchema.PROVIDER_CODE,
        OdaSchema.PROVIDER_NAME,
        OdaSchema.RECIPIENT_CODE,
        OdaSchema.RECIPIENT_NAME,
        OdaSchema.AIDTYPE_CODE,
        OdaSchema.AIDTYPE_NAME,
    ]

    df = dac2a.read(using_bulk_download=True, columns=columns)

    data = {
        OdaSchema.PROVIDER_CODE: _code_name(
            df, OdaSchema.PROVIDER_CODE, OdaSchema.PROVIDER_NAME
        ),
        OdaSchema.RECIPIENT_CODE: _code_name(
            df, OdaSchema.RECIPIENT_CODE, OdaSchema.RECIPIENT_NAME
        ),
        OdaSchema.AIDTYPE_CODE: _code_name(
            df, OdaSchema.AIDTYPE_CODE, OdaSchema.AIDTYPE_NAME
        ),
    }

    update_mapping_file(data, OdaPATHS.names / "dac2a_names.json")


def crs_codes_names():
    """Return a dictionary of code to name mappings for CRS columns."""
    from oda_data import CrsData

    crs = CrsData(years=[1995, 2010, 2020, 2023])

    columns = [
        OdaSchema.PROVIDER_CODE,
        OdaSchema.PROVIDER_NAME,
        OdaSchema.PROVIDER_CODE_DE,
        OdaSchema.AGENCY_CODE,
        OdaSchema.AGENCY_NAME,
        OdaSchema.RECIPIENT_CODE,
        OdaSchema.RECIPIENT_CODE_DE,
        OdaSchema.RECIPIENT_REGION_CODE,
        OdaSchema.RECIPIENT_REGION_CODE_DE,
        OdaSchema.RECIPIENT_INCOME_CODE,
        OdaSchema.RECIPIENT_INCOME,
        OdaSchema.RECIPIENT_REGION,
        OdaSchema.RECIPIENT_NAME,
        OdaSchema.FLOW_CODE,
        OdaSchema.FLOW_NAME,
        OdaSchema.PURPOSE_CODE,
        OdaSchema.PURPOSE_NAME,
        OdaSchema.SECTOR_CODE,
        OdaSchema.SECTOR_NAME,
        OdaSchema.CHANNEL_CODE,
        OdaSchema.CHANNEL_NAME,
    ]

    df = crs.read(using_bulk_download=True, columns=columns)

    data = {
        OdaSchema.PROVIDER_CODE: _code_name(
            df, OdaSchema.PROVIDER_CODE, OdaSchema.PROVIDER_NAME
        ),
        OdaSchema.PROVIDER_CODE_DE: _code_name(
            df, OdaSchema.PROVIDER_CODE_DE, OdaSchema.PROVIDER_NAME
        ),
        OdaSchema.AGENCY_CODE: _code_name(
            df, OdaSchema.AGENCY_CODE, OdaSchema.AGENCY_NAME
        ),
        OdaSchema.RECIPIENT_CODE: _code_name(
            df, OdaSchema.RECIPIENT_CODE, OdaSchema.RECIPIENT_NAME
        ),
        OdaSchema.RECIPIENT_CODE_DE: _code_name(
            df, OdaSchema.RECIPIENT_CODE_DE, OdaSchema.RECIPIENT_NAME
        ),
        OdaSchema.RECIPIENT_REGION_CODE: _code_name(
            df, OdaSchema.RECIPIENT_REGION_CODE, OdaSchema.RECIPIENT_REGION
        ),
        OdaSchema.RECIPIENT_REGION_CODE_DE: _code_name(
            df, OdaSchema.RECIPIENT_REGION_CODE_DE, OdaSchema.RECIPIENT_REGION
        ),
        OdaSchema.RECIPIENT_INCOME_CODE: _code_name(
            df, OdaSchema.RECIPIENT_INCOME_CODE, OdaSchema.RECIPIENT_INCOME
        ),
        OdaSchema.FLOW_CODE: _code_name(df, OdaSchema.FLOW_CODE, OdaSchema.FLOW_NAME),
        OdaSchema.PURPOSE_CODE: _code_name(
            df, OdaSchema.PURPOSE_CODE, OdaSchema.PURPOSE_NAME
        ),
        OdaSchema.SECTOR_CODE: _code_name(
            df, OdaSchema.SECTOR_CODE, OdaSchema.SECTOR_NAME
        ),
        OdaSchema.CHANNEL_CODE: _code_name(
            df, OdaSchema.CHANNEL_CODE, OdaSchema.CHANNEL_NAME
        ),
    }

    update_mapping_file(data, OdaPATHS.names / "crs_names.json")


def get_merged_names_mapping():

    files = ["dac1_names.json", "dac2a_names.json", "crs_names.json"]

    merged_mapping = {}

    for file in files:
        file_path = OdaPATHS.names / file
        with open(file_path, "r") as f:
            mapping = json.load(f)
            for k, v in mapping.items():
                if k not in merged_mapping:
                    merged_mapping[k] = {}
                merged_mapping[k].update(v)

    # Remove keys with empty dictionaries
    merged_mapping = {k: v for k, v in merged_mapping.items() if v}

    return merged_mapping
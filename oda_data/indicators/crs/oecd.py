import pandas as pd

from oda_data.indicators.crs.common import (
    update_mapping_file,
    read_crs_type_of_flow,
    read_crs_type_of_finance_mapping,
    read_crs_modality_mapping,
    read_crs_purpose_mapping,
    read_crs_policy_markers,
)
from oda_data.indicators.indicator import Indicator, SEPARATOR
from oda_data import read_crs

crs = read_crs(2022)


def unique_crs_indicator_rows(crs: pd.DataFrame):

    # 4. type_of_finance
    type_of_finance = {
        k: v["code"] for k, v in read_crs_type_of_finance_mapping().items()
    }

    # map types of finance
    crs["mapped_type_of_finance"] = (
        crs["type_of_finance"].fillna("X").map(type_of_finance)
    )

    # 5. Modality
    modality = {k: v["code"] for k, v in read_crs_modality_mapping().items()}
    crs["mapped_modality"] = crs["modality"].map(modality).fillna(crs["modality"])

    # 6. Purpose_code
    purpose_code = {k: v["code"] for k, v in read_crs_purpose_mapping().items()}
    crs["mapped_purpose"] = (
        crs["sector_code"].map(purpose_code).fillna(crs["sector_code"])
    )

    # find unique rows
    crs = crs.filter(
        ["category", "mapped_type_of_finance", "mapped_modality", "mapped_purpose"]
    ).drop_duplicates()

    return crs


unique_crs = unique_crs_indicator_rows(crs)


def crs_oecd_indicators():
    """Generate a json file which defines the DAC1 indicator codes, and the filtering process
    to generate them."""

    # 1. Source
    crs_base: str = "CRS"
    "[1-source].[2-type of flow].[3-perspective].[4-type of finance].[5-modality].[6-purpose].[7-policy marker]"
    "1-CRS.2-category.3-P.4-type_of_finance.5-modality.6-purpose_code.7-marker"

    # 2. type of flow
    category = read_crs_type_of_flow()

    # 3. perspective
    perspectives = ["P", "R"]

    # 4. type_of_finance
    type_of_finance = read_crs_type_of_finance_mapping()

    # 5. Modality
    modality = read_crs_modality_mapping()

    # 6. Purpose_code
    purpose_code = read_crs_purpose_mapping()

    # 7. policy marker
    policy_marker = read_crs_policy_markers()

    indicators = []

    indicator_ = Indicator(
        code=f"{crs_base}{SEPARATOR}",
        name=f"",
        description=f"",
        sources=["CRS"],
        type="DAC",
        filters={},
    )
    indicators.append(indicator_)

    # transform list to json-like dict
    indicators = {i.code: i.to_dict for i in indicators}

    return indicators


if __name__ == "__main__":
    oecd = crs_oecd_indicators()
    update_mapping_file({"DAC": oecd})

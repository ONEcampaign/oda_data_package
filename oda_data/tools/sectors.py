import json

import pandas as pd

from oda_data import config


def read_sector_lists() -> dict:
    with open(config.OdaPATHS.settings / "sectors.json", "r") as f:
        return json.load(f)


def sector_groups() -> dict:
    mapping = {}
    for group in read_sector_lists().values():
        mapping.update(group)

    return mapping


def add_sectors_column(
    data: pd.DataFrame,
    column_name: str = "sector",
    group: bool = True,
    missing_fill: str = "Other",
) -> pd.DataFrame:
    """Group data by sectors, given a list of columns to group by"""

    # Track columns to group by
    cols = [c for c in data.columns if "share" not in c and "value" not in c]

    # Load the sectors group
    sectors = sector_groups()

    for name, codes in sectors.items():
        data.loc[data.purpose_code.isin(codes), "sec"] = name

    data["sec"] = data["sec"].fillna(missing_fill)

    if group:
        data = (
            data.groupby(cols + ["sec"], observed=True, dropna=False)
            .sum(numeric_only=True)
            .filter([c for c in data if c != "purpose_code"], axis=1)
            .loc[lambda d: d.value != 0]
            .reset_index(drop=False)
        )

    return data.rename(columns={"sec": column_name})


def add_broad_sectors_column(
    data: pd.DataFrame,
    column_name: str = "broad_sector",
    group: bool = True,
    missing_fill: str = "Other",
) -> pd.DataFrame:
    """Group data by sectors, given a list of columns to group by"""

    data = data.copy(deep=True)

    # Track columns to group by
    cols = [c for c in data.columns if "share" not in c and "value" not in c]

    # Load the sectors group
    broad_sectors = read_sector_lists()

    for name, codes_dict in broad_sectors.items():
        for sector_name, codes in codes_dict.items():
            data.loc[data.purpose_code.isin(codes), "sec"] = name

    data["sec"] = data["sec"].fillna(missing_fill)

    if group:
        data = (
            data.groupby(cols + ["sec"], observed=True, dropna=False)
            .sum(numeric_only=True)
            .filter([c for c in data if c != "purpose_code"], axis=1)
            .loc[lambda d: d.value != 0]
            .reset_index(drop=False)
        )

    return data.rename(columns={"sec": column_name})

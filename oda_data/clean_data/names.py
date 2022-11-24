import pandas as pd

from oda_data import ODAData


def add_name(df: pd.DataFrame, name_id: str | list) -> pd.DataFrame:
    """Add the donor name to the dataframe.

    Args:
        df: a dataframe containing columns like donor_code or recipient_code
        name_id: the name of the column(s) to add the name to (e.g. donor_code)
    """

    # if a single column is passed, convert to a list
    if isinstance(name_id, str):
        name_id = [name_id]

    # get the name of the column to add the name to
    ids = [f"{i.split('_')[0]}_name" for i in name_id] + name_id

    # fetch a dataframe with codes and names
    names = (
        ODAData(years=2020)
        .load_indicator("crs_bilateral_flow_disbursement_gross")
        .get_data()
        .drop_duplicates(subset=ids)
        .dropna(subset=name_id, how="any")
        .filter(ids, axis=1)
        .reset_index(drop=True)
    )

    # merge the names to the dataframe
    return df.merge(names, on=name_id, how="left")

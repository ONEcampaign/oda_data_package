import pandas as pd


def total_bilateral_plus_imputed(data: pd.DataFrame) -> pd.DataFrame:
    """Filter the data to include bilateral plus imputed multilateral aid.
    Group the data by all columns except `aidtype_code`, `aid_type`, and `value`.
    """
    # Group by all columns except `aidtype_code`, `aid_type`, and `value`
    columns = data.columns
    idx = [c for c in columns if c not in ["aidtype_code", "aid_type", "value"]]
    data = data.groupby(idx, dropna=False, observed=True)[["value"]].sum().reset_index()

    # Assign the aidtype_code and aid_type
    data["aidtype_code"] = 206106
    data["aid_type"] = "Bilateral plus imputed multilateral"

    return data[columns]

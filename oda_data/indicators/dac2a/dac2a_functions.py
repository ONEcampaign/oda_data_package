import pandas as pd

from oda_data.logger import logger


def total_bilateral_plus_imputed(data: pd.DataFrame) -> pd.DataFrame:
    columns = data.columns
    idx = [c for c in columns if c not in ["aidtype_code", "aid_type", "value"]]
    data = data.groupby(idx, dropna=False, observed=True)[["value"]].sum().reset_index()

    data["aidtype_code"] = 206106
    data["aid_type"] = "Bilateral plus imputed multilateral"

    return data[columns]

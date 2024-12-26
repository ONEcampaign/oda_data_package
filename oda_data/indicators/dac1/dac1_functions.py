import pandas as pd

from oda_data.logger import logger


def official_oda(data: pd.DataFrame) -> pd.DataFrame:
    query = "(year < 2018 & aidtype_code == 1010) | (year >2018 & aidtype_code ==11010)"

    data = data.query(query)

    return data

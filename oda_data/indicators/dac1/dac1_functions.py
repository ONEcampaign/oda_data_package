import pandas as pd


def official_oda(data: pd.DataFrame) -> pd.DataFrame:
    """Filter the data to only include Official Development Assistance (ODA).
    Pre-2018, ODA is defined as aidtype_code 1010.
    Post-2018, ODA is defined as aidtype_code 11010.
    """
    query = "(year < 2018 & aidtype_code == 1010) | (year >2018 & aidtype_code ==11010)"

    data = data.query(query)

    return data

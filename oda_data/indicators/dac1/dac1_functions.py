import pandas as pd

from oda_data.clean_data.schema import ODASchema


def official_oda(data: pd.DataFrame) -> pd.DataFrame:
    """Filters the dataset to include only Official Development Assistance (ODA).

    ODA is defined as follows:
    - Before 2018: aidtype_code == 1010
    - From 2018 onward: aidtype_code == 11010

    Args:
        data: A pandas DataFrame containing aid data.

    Returns:
        A filtered pandas DataFrame with only official ODA rows.
    """
    oda_query = (
        "(year < 2018 & aidtype_code == 1010) | "
        "(year >= 2018 & aidtype_code == 11010)"
    )
    return data.query(oda_query)


def official_oda_gni(data: pd.DataFrame) -> pd.DataFrame:
    """Filters the dataset to include only Official Development Assistance (ODA)
    and computes the ratio of ODA to Gross National Income (GNI).

    Args:
        data: A pandas DataFrame containing aid data.

    Returns:
        A filtered pandas DataFrame with only official ODA rows.
    """
    oda = official_oda(data)

    gni = (
        data.query("aidtype_code == 1")
        .rename(columns={ODASchema.VALUE: "gni_value"})
        .filter([ODASchema.YEAR, ODASchema.PROVIDER_CODE, "gni_value"])
    )

    df = oda.merge(gni, on=[ODASchema.YEAR, ODASchema.PROVIDER_CODE], how="left")
    df[ODASchema.VALUE] = round(df[ODASchema.VALUE] / df["gni_value"], 6)

    df = df.drop(columns=["gni_value"])

    df[ODASchema.AIDTYPE_CODE] = 1010110101
    df[ODASchema.AIDTYPE_NAME] = (
        "Official Development Assistance, as a share of GNI (official definition)"
    )

    return df


def one_core_oda(data: pd.DataFrame) -> pd.DataFrame:
    """Computes ONE's Core ODA metric: ODA excluding in-donor spending
    but including administrative costs.

    If ge codes are present, it uses official_oda() to
    define the ODA baseline. Otherwise, it assumes 1010 definitions.

    Args:
        data: A pandas DataFrame containing aid data.

    Returns:
        A pandas DataFrame representing ONE Core ODA.
    """
    aidtype_col = ODASchema.AIDTYPE_CODE
    value_col = ODASchema.VALUE
    name_col = ODASchema.AIDTYPE_NAME

    has_ge_oda = 11010 in data[aidtype_col].unique()

    if has_ge_oda:
        total_oda = official_oda(data)
    else:
        total_oda = data.loc[data[aidtype_col] == 1010]

    in_donor = data.loc[~data[aidtype_col].isin([1010, 11010])].copy()
    in_donor[value_col] *= -1

    combined_df = pd.concat([total_oda, in_donor], ignore_index=True)

    combined_df[aidtype_col] = 91010
    combined_df[name_col] = "Core ODA"

    group_columns = [col for col in combined_df.columns if col != value_col]
    result_df = (
        combined_df.groupby(group_columns, dropna=False, observed=True)[[value_col]]
        .sum()
        .reset_index()
    )

    return result_df

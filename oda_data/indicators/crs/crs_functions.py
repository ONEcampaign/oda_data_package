import pandas as pd

from oda_data.clean_data.channels import add_multi_channel_codes, add_channel_names
from oda_data.clean_data.schema import ODASchema
from oda_data.indicators.crs.common import crs_value_cols


def _multi_donors_only(data: pd.DataFrame) -> pd.DataFrame:
    """Filter the data to only include multi-donors."""
    from oda_data import provider_groupings

    return data.loc[
        lambda d: d[ODASchema.PROVIDER_CODE].isin(provider_groupings()["multilateral"])
    ]


def _group_by_mapped_channel(df: pd.DataFrame) -> pd.DataFrame:
    idx = [
        ODASchema.YEAR,
        ODASchema.CHANNEL_CODE,
        ODASchema.RECIPIENT_CODE,
        ODASchema.RECIPIENT_NAME,
        ODASchema.PURPOSE_CODE,
        ODASchema.PURPOSE_NAME,
    ]
    values = list(crs_value_cols().values())
    df = df.groupby(idx, observed=True, dropna=False)[values].sum().reset_index()

    return df


def _rolling_period_total(df: pd.DataFrame, period_length=3) -> pd.DataFrame:
    """Calculate a rolling total of Y period length"""
    values = list(crs_value_cols().values())
    data = pd.DataFrame()
    cols = [c for c in df.columns if c not in [ODASchema.YEAR] + values]

    for y in range(df[ODASchema.YEAR].max(), df[ODASchema.YEAR].min() + 1, -1):
        years = [y - i for i in range(period_length)]
        _ = (
            df.copy(deep=True)
            .loc[lambda d: d[ODASchema.YEAR].isin(years)]
            .groupby(cols, observed=True, dropna=False)
            .agg({v: "sum" for v in values} | {ODASchema.YEAR: "max"})
            .assign(**{ODASchema.YEAR: y})
            .reset_index()
        )
        data = pd.concat([data, _], ignore_index=True)

    return (
        data.astype({ODASchema.YEAR: "int16[pyarrow]"})
        .loc[lambda d: d[ODASchema.YEAR].notna()]
        .reset_index(drop=True)
    )


def _purpose_share(value_row: pd.DataFrame, value_col: str) -> pd.Series:
    """Function to calculate the share of total for per purpose code."""

    cols = [ODASchema.YEAR, ODASchema.CHANNEL_CODE]

    return value_row.groupby(cols, observed=True, dropna=False)[value_col].transform(
        lambda p: p / p.sum()
    )


def _yearly_share(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate the yearly share of the total value for each purpose code."""
    values = list(crs_value_cols().values())

    for col in values:
        df[col] = _purpose_share(df, col)

    return df


def _rename_channel_column_add_names(data: pd.DataFrame) -> pd.DataFrame:
    data = (
        add_channel_names(
            df=data,
            codes_column=ODASchema.CHANNEL_CODE,
            target_column=ODASchema.PROVIDER_NAME,
        )
        .rename(columns={ODASchema.CHANNEL_CODE: ODASchema.PROVIDER_CODE})
        .astype({ODASchema.PROVIDER_NAME: "string[pyarrow]"})
    )

    return data


def drop_if_all_values_are_missing(data: pd.DataFrame) -> pd.DataFrame:
    values = list(crs_value_cols().values())

    return data.dropna(subset=values, how="all").reset_index(drop=True)


def drop_missing_donors(data: pd.DataFrame) -> pd.DataFrame:
    return data.loc[lambda d: d[ODASchema.PROVIDER_CODE].notna()].reset_index(drop=True)


def multilateral_purpose_spending_shares(data: pd.DataFrame) -> pd.DataFrame:
    """Calculate the shares of spending by purpose_code."""

    data = (
        data.pipe(add_multi_channel_codes)
        .pipe(_group_by_mapped_channel)
        .pipe(_rolling_period_total)
        .pipe(_yearly_share)
        .pipe(_rename_channel_column_add_names)
        .pipe(drop_if_all_values_are_missing)
        .pipe(drop_missing_donors)
    )

    return data

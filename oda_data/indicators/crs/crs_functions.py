import pandas as pd

from oda_data.clean_data.channels import add_multi_channel_codes
from oda_data.clean_data.schema import OdaSchema
from oda_data.indicators.crs.common import crs_value_cols


def _multi_donors_only(data: pd.DataFrame) -> pd.DataFrame:
    """Filter the data to only include multi-donors."""
    from oda_data import donor_groupings

    return data.loc[
        lambda d: d[OdaSchema.PROVIDER_CODE].isin(donor_groupings()["multilateral"])
    ]


def _group_by_mapped_channel(df: pd.DataFrame) -> pd.DataFrame:
    idx = [
        OdaSchema.YEAR,
        OdaSchema.CHANNEL_CODE,
        OdaSchema.RECIPIENT_CODE,
        OdaSchema.RECIPIENT_NAME,
        OdaSchema.PURPOSE_CODE,
        OdaSchema.PURPOSE_NAME,
    ]
    values = list(crs_value_cols().values())
    df = df.groupby(idx, observed=True, dropna=False)[values].sum().reset_index()

    return df


def _rolling_period_total(df: pd.DataFrame, period_length=3) -> pd.DataFrame:
    """Calculate a rolling total of Y period length"""
    values = list(crs_value_cols().values())
    data = pd.DataFrame()
    cols = [c for c in df.columns if c not in [OdaSchema.YEAR] + values]

    for y in range(df[OdaSchema.YEAR].max(), df[OdaSchema.YEAR].min() + 1, -1):
        years = [y - i for i in range(period_length)]
        _ = (
            df.copy(deep=True)
            .loc[lambda d: d[OdaSchema.YEAR].isin(years)]
            .groupby(cols, observed=True, dropna=False)
            .agg({v: "sum" for v in values} | {OdaSchema.YEAR: "max"})
            .assign(**{OdaSchema.YEAR: y})
            .reset_index()
        )
        data = pd.concat([data, _], ignore_index=True)

    return (
        data.astype({OdaSchema.YEAR: "int16[pyarrow]"})
        .loc[lambda d: d[OdaSchema.YEAR].notna()]
        .reset_index(drop=True)
    )


def _purpose_share(value_row: pd.DataFrame) -> pd.Series:
    """Function to calculate the share of total for per purpose code."""
    values = list(crs_value_cols().values())

    cols = [OdaSchema.YEAR, OdaSchema.CHANNEL_CODE]

    return value_row.groupby(cols, observed=True, dropna=False)[values].transform(
        lambda p: p / p.sum()
    )


def _yearly_share(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate the yearly share of the total value for each purpose code."""
    values = list(crs_value_cols().values())

    return (
        df.assign(**{v: lambda d: _purpose_share(d) for v in values})
        .loc[lambda d: d.share.notna()]
        .reset_index(drop=True)
    )


def multilateral_purpose_spending_shares(data: pd.DataFrame) -> pd.DataFrame:
    """Calculate the shares of spending by purpose_code."""

    data = (
        data.pipe(add_multi_channel_codes)
        .pipe(_group_by_mapped_channel)
        .pipe(_rolling_period_total)
        .pipe(_yearly_share)
    )

    return data

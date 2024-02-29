import copy
import json

import pandas as pd

from oda_data import config
from oda_data.clean_data.channels import add_multi_channel_codes
from oda_data.clean_data.common import keep_multi_donors_only
from oda_data.clean_data.dtypes import set_default_types
from oda_data.clean_data.schema import OdaSchema

# For typing purposes
ODAData: callable = "ODAData"


# -----------------------------------------------------------------------------
#                               Channel codes
# -----------------------------------------------------------------------------


def read_channel_codes() -> dict:
    """Get the mapping between CRS-like identifiers and channel codes from the
    Multisystem database. Return them as a dictionary."""
    with open(config.OdaPATHS.settings / "channel_codes.json") as f:
        return json.load(f)


# -----------------------------------------------------------------------------
#                               Helper functions
# -----------------------------------------------------------------------------


def _get_indicator(data: ODAData, indicator: str, columns: list) -> pd.DataFrame:
    """A wrapper to get the data from the indicator and simplify it."""

    data = copy.deepcopy(data)

    return (
        data.load_indicator(indicators=indicator)
        .simplify_output_df(columns_to_keep=columns)
        .get_data(indicators=indicator)
    )


def _rolling_period_total(df: pd.DataFrame, period_length=3) -> pd.DataFrame:
    """Calculate a rolling total of Y period length"""
    data = pd.DataFrame()
    cols = [c for c in df.columns if c not in ["year", "value"]]

    for y in range(df.year.max(), df.year.min() + 1, -1):
        years = [y - i for i in range(period_length)]
        _ = (
            df.copy(deep=True)
            .loc[lambda d: d.year.isin(years)]
            .groupby(cols, observed=True, dropna=False)
            .agg({"value": "sum", "year": "max"})
            .assign(year=y)
            .reset_index()
        )
        data = pd.concat([data, _], ignore_index=True)

    return (
        data.assign(year=lambda d: d.year.astype("int16[pyarrow]"))
        .loc[lambda d: d.year.notna()]
        .reset_index(drop=True)
    )


def _purpose_share(df_: pd.DataFrame) -> pd.Series:
    """Function to calculate the share of total for per purpose code."""
    cols = [
        OdaSchema.YEAR,
        OdaSchema.CURRENCY,
        OdaSchema.PRICES,
        OdaSchema.CHANNEL_CODE,
    ]
    return df_.groupby(cols, observed=True, dropna=False)[OdaSchema.VALUE].transform(
        lambda p: p / p.sum()
    )


def _yearly_share(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate the yearly share of the total value for each purpose code."""

    return (
        df.assign(share=lambda d: _purpose_share(d))
        .loc[lambda d: d.share.notna()]
        .reset_index(drop=True)
    )


def _group_by_mapped_channel(df: pd.DataFrame) -> pd.DataFrame:
    df = (
        df.groupby(
            [
                c
                for c in df.columns
                if c
                not in [
                    OdaSchema.PROVIDER_NAME,
                    OdaSchema.PROVIDER_CODE,
                    OdaSchema.AGENCY_CODE,
                    OdaSchema.AGENCY_NAME,
                    "name",
                    OdaSchema.VALUE,
                ]
            ],
            observed=True,
            dropna=False,
        )[[OdaSchema.VALUE]]
        .sum()
        .reset_index()
    )

    return df


# -----------------------------------------------------------------------------
#                               Components functions
# -----------------------------------------------------------------------------


def compute_imputations(
    core_contributions: pd.DataFrame, multi_spending_shares: pd.DataFrame
) -> pd.DataFrame:
    """Impute the multilateral spending by donor and agency."""

    return (
        multi_spending_shares.merge(
            core_contributions,
            on=[
                OdaSchema.CHANNEL_CODE,
                OdaSchema.YEAR,
                OdaSchema.CURRENCY,
                OdaSchema.PRICES,
            ],
            how="left",
        )
        .assign(value=lambda d: d[OdaSchema.VALUE] * d[OdaSchema.SHARE])
        .drop(labels=[OdaSchema.SHARE], axis=1)
        .loc[lambda d: d[OdaSchema.VALUE] != 0]
        .reset_index(drop=True)
    )


def multi_contributions_by_donor(
    data: ODAData,
) -> pd.DataFrame:
    """Get a simplified dataframe with the money contributed by donors to
    multilaterals (grouped by channel code)"""

    indicator = "multisystem_multilateral_contributions_disbursement_gross"
    cols = [
        OdaSchema.PROVIDER_CODE,
        OdaSchema.CHANNEL_CODE,
        OdaSchema.YEAR,
        OdaSchema.CURRENCY,
        OdaSchema.PRICES,
    ]

    return _get_indicator(data=data, indicator=indicator, columns=cols).pipe(
        set_default_types
    )


def bilat_outflows_by_donor(
    data: ODAData,
    purpose_column: str = "purpose_code",
) -> pd.DataFrame:
    """Get a simplified dataframe with the money contributed by donors to
    multilaterals (grouped by channel code)"""

    indicator = "crs_bilateral_total_flow_gross_by_purpose"

    cols = [
        OdaSchema.PROVIDER_CODE,
        purpose_column,
        OdaSchema.RECIPIENT_CODE,
        OdaSchema.YEAR,
        OdaSchema.CURRENCY,
        OdaSchema.PRICES,
    ]

    return _get_indicator(data=data, indicator=indicator, columns=cols)


def period_purpose_shares(
    data: ODAData,
    purpose_column: str = "purpose_code",
    period_length: int = 3,
) -> pd.DataFrame:
    """ """
    # Indicator
    indicator = "crs_bilateral_all_flows_disbursement_gross"

    # Columns to keep
    cols = [
        OdaSchema.PROVIDER_CODE,
        OdaSchema.PROVIDER_NAME,
        OdaSchema.AGENCY_CODE,
        OdaSchema.AGENCY_NAME,
        purpose_column,
        OdaSchema.RECIPIENT_CODE,
        OdaSchema.YEAR,
        OdaSchema.CURRENCY,
        OdaSchema.PRICES,
    ]

    df = _get_indicator(data=data, indicator=indicator, columns=cols)

    if data.donors is not None:
        df = df.loc[lambda d: d[OdaSchema.PROVIDER_CODE].isin(data.donors)]

    return (
        df.pipe(keep_multi_donors_only)
        .pipe(add_multi_channel_codes)
        .pipe(_group_by_mapped_channel)
        .pipe(_rolling_period_total, period_length)
        .pipe(_yearly_share)
    )

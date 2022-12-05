import copy
import json

import pandas as pd

from oda_data import config

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


def add_multi_channel_ids(df: pd.DataFrame) -> pd.DataFrame:
    """Add the multichannel ids to the dataframe. It adds a new channel_code
    column, which maps the ids to the multichannel ids."""
    channels_dict = read_channel_codes()
    return df.assign(
        ids=lambda m: "D."
        + m.donor_code.astype(str)
        + ".A."
        + m.agency_code.astype(str),
        channel_code=lambda d: d.ids.map(channels_dict).fillna(pd.NA).astype("Int32"),
    )


# -----------------------------------------------------------------------------
#                               Helper functions
# -----------------------------------------------------------------------------


def _get_indicator(data: ODAData, indicator: str, columns: list) -> pd.DataFrame:
    """A wrapper to get the data from the indicator and simplify it."""

    data = copy.deepcopy(data)

    return (
        data.load_indicator(indicator=indicator)
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
            .agg({"value": sum, "year": max})
            .assign(year=y)
            .reset_index()
        )
        data = pd.concat([data, _], ignore_index=True)

    return (
        data.assign(year=lambda d: d.year.astype("Int32"))
        .loc[lambda d: d.year.notna()]
        .reset_index(drop=True)
    )


def _purpose_share(df_: pd.DataFrame) -> pd.Series:
    """Function to calculate the share of total for per purpose code."""
    cols = ["donor_code", "year", "currency", "prices"]
    return df_.groupby(cols, observed=True)["value"].transform(lambda p: p / p.sum())


def _yearly_share(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate the yearly share of the total value for each purpose code."""

    return (
        df.assign(value=lambda d: _purpose_share(d))
        .loc[lambda d: d.value.notna()]
        .reset_index(drop=True)
    )


def _spending_summary(df: pd.DataFrame, purpose_column: str) -> pd.DataFrame:
    """Calculate the yearly share of the total value for each purpose code."""

    cols = [
        "year",
        "channel_code",
        "recipient_code",
        purpose_column,
        "currency",
        "prices",
        "value",
    ]

    return (
        df.pipe(add_multi_channel_ids)
        .loc[lambda d: d.channel_code.notna()]
        .filter(cols, axis=1)
        .groupby([c for c in cols if c != "value"], observed=True, dropna=False)
        .sum()["value"]
        .reset_index(drop=False)
        .rename(columns={"value": "share"})
    )


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
            on=["channel_code", "year", "currency", "prices"],
            how="left",
        )
        .assign(value=lambda d: d.value * d.share)
        .drop("share", axis=1)
        .loc[lambda d: d.value > 0]
        .reset_index(drop=True)
    )


def multi_contributions_by_donor(
    data: ODAData,
) -> pd.DataFrame:
    """Get a simplified dataframe with the money contributed by donors to
    multilaterals (grouped by channel code)"""

    indicator = "multisystem_multilateral_contributions_disbursement_gross"
    cols = ["donor_code", "channel_code", "year", "currency", "prices"]

    return _get_indicator(data=data, indicator=indicator, columns=cols)


def bilat_outflows_by_donor(
    data: ODAData,
    purpose_column: str = "purpose_code",
) -> pd.DataFrame:
    """Get a simplified dataframe with the money contributed by donors to
    multilaterals (grouped by channel code)"""

    indicator = "crs_bilateral_total_flow_gross_by_purpose"

    cols = [
        "donor_code",
        purpose_column,
        "recipient_code",
        "year",
        "currency",
        "prices",
    ]

    return _get_indicator(data=data, indicator=indicator, columns=cols)


def period_purpose_shares(
    data: ODAData,
    purpose_column: str = "purpose_code",
    period_length: int = 3,
) -> pd.DataFrame:
    """ """
    # Indicator
    indicator = "crs_bilateral_total_flow_gross_by_purpose"

    # Columns to keep
    cols = [
        "donor_code",
        "agency_code",
        purpose_column,
        "recipient_code",
        "year",
        "currency",
        "prices",
    ]

    return (
        _get_indicator(data=data, indicator=indicator, columns=cols)
        .pipe(_rolling_period_total, period_length)
        .pipe(_yearly_share)
        .pipe(_spending_summary, purpose_column=purpose_column)
    )

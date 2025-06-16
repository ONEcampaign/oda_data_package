import pandas as pd

from oda_data import provider_groupings
from oda_data.api.constants import (
    Measure,
    MEASURES,
    PROVIDER_PURPOSE_GROUPER,
    CHANNEL_PURPOSE_SHARE_GROUPER,
)
from oda_data.clean_data.channels import add_multi_channel_codes
from oda_data.clean_data.common import convert_units
from oda_data.clean_data.schema import ODASchema


def rolling_period_total(
    df: pd.DataFrame, period_length=3, grouper: list[str] | None = None
) -> pd.DataFrame:
    """Calculates a rolling total over a specified period length.

    Args:
        df (pd.DataFrame): Input dataframe containing time-series data.
        period_length (int, optional): Length of the rolling period. Defaults to 3.
        grouper (list[str] | None, optional): Columns to group by. Defaults to None.

    Returns:
        pd.DataFrame: Dataframe with rolling total calculations.
    """
    data = pd.DataFrame()

    if grouper is None:
        grouper = [c for c in df.columns if c not in [ODASchema.YEAR, ODASchema.VALUE]]

    for y in range(df[ODASchema.YEAR].max(), df[ODASchema.YEAR].min() + 1, -1):
        years = [y - i for i in range(period_length)]
        _ = (
            df.copy(deep=True)
            .loc[lambda d: d[ODASchema.YEAR].isin(years)]
            .groupby(grouper, observed=True, dropna=False)
            .agg({ODASchema.VALUE: "sum", ODASchema.YEAR: "max"})
            .assign(**{ODASchema.YEAR: y})
            .reset_index()
        )
        data = pd.concat([data, _], ignore_index=True)

    return (
        data.assign(year=lambda d: d[ODASchema.YEAR].astype("int16[pyarrow]"))
        .loc[lambda d: d[ODASchema.YEAR].notna()]
        .reset_index(drop=True)
    )


def share_by_purpose(
    df: pd.DataFrame, grouper: list[str] | None = None
) -> pd.DataFrame:
    """Calculates the share of the total for each purpose code.

    Args:
        df (pd.DataFrame): Input dataframe containing values to compute shares.
        grouper (list[str] | None, optional): Columns to group by. Defaults to None.

    Returns:
        pd.DataFrame: Dataframe with an additional share column.
    """
    df[ODASchema.SHARE] = df.groupby(grouper, observed=True, dropna=False)[
        ODASchema.VALUE
    ].transform(lambda p: p / p.sum())

    return df.loc[lambda d: d.share.notna()].reset_index(drop=True)


def _group_by_mapped_channel(df: pd.DataFrame) -> pd.DataFrame:
    """Groups data by mapped channels and sums values.

    Args:
        df (pd.DataFrame): Input dataframe.

    Returns:
        pd.DataFrame: Aggregated dataframe grouped by relevant columns.
    """
    df = (
        df.groupby(
            [
                c
                for c in df.columns
                if c
                not in [
                    ODASchema.PROVIDER_NAME,
                    ODASchema.PROVIDER_CODE,
                    ODASchema.AGENCY_CODE,
                    ODASchema.AGENCY_NAME,
                    "name",
                    ODASchema.VALUE,
                ]
            ],
            observed=True,
            dropna=False,
        )[[ODASchema.VALUE]]
        .sum()
        .reset_index()
    )

    return df


def period_purpose_shares(
    data: pd.DataFrame,
    period_length: int = 3,
    grouper: list[str] | None = None,
    share_by_grouper: list[str] | None = None,
) -> pd.DataFrame:
    """Computes period-based purpose shares.

    Args:
        data (pd.DataFrame): Input dataframe.
        period_length (int, optional): Length of the rolling period. Defaults to 3.
        grouper (list[str] | None, optional): Columns to group by for rolling total. Defaults to None.
        share_by_grouper (list[str] | None, optional): Columns to group by for share calculation. Defaults to None.

    Returns:
        pd.DataFrame: Dataframe with computed shares.
    """
    return data.pipe(
        rolling_period_total, period_length=period_length, grouper=grouper
    ).pipe(share_by_purpose, grouper=share_by_grouper)


def add_multi_channels_and_group(data: pd.DataFrame) -> pd.DataFrame:
    """Adds multi-channel codes and groups the data accordingly.

    Args:
        data (pd.DataFrame): Input dataframe.

    Returns:
        pd.DataFrame: Transformed dataframe with grouped channels.
    """
    return data.pipe(add_multi_channel_codes).pipe(_group_by_mapped_channel)


def spending_by_purpose(
    years: list | int | range = None,
    providers: list | int | None = None,
    measure: Measure | str = "gross_disbursement",
    oda_only: bool = False,
    currency: str = "USD",
    base_year: int | None = None,
) -> pd.DataFrame:
    """Retrieves and processes spending data by purpose.

    Args:
        years (list | int | range, optional): Years to filter the data. Defaults to None.
        providers (list | int | None, optional): Providers to filter the data. Defaults to None.
        measure (Measure | str, optional): Measure type. Defaults to "gross_disbursement".
        oda_only (bool, optional): Whether to include only ODA-related data. Defaults to False.
        currency (str, optional): Target currency. Defaults to "USD".
        base_year (int | None, optional): Base year for conversion. Defaults to None.

    Returns:
        pd.DataFrame: Dataframe with spending by purpose.
    """
    from oda_data.api.sources import CRSData

    # Get the relevant measure
    measure = MEASURES["CRS"][measure]["column"]

    # Set up grouper
    grouper = [
        c
        for c in PROVIDER_PURPOSE_GROUPER
        if c not in [ODASchema.CURRENCY, ODASchema.PRICES]
    ]

    # Set up filters
    if oda_only:
        filters = [("category", "in", [10, 60])]
    else:
        filters = []

    # Set up the CRS data object
    crs = CRSData(providers=providers, years=years)

    # Read the data and group by provider and purpose
    data = (
        crs.read(
            columns=grouper + [measure],
            additional_filters=filters,
            using_bulk_download=True,
        )
        .groupby(grouper, dropna=False, observed=True)[[measure]]
        .sum()
        .reset_index()
        .rename(columns={measure: "value"})
    )

    # Convert the data to the target currency and prices
    data = convert_units(data, currency=currency, base_year=base_year)

    return data


def multilateral_spending_shares_by_channel_and_purpose_smoothed(
    years: list | int | range = None,
    oda_only: bool = False,
    period_length: int = 3,
) -> pd.DataFrame:
    """Computes multilateral spending shares by channel and purpose, smoothed over a period.

    Args:
        years (list | int | range, optional): Years to filter the data. Defaults to None.
        oda_only (bool, optional): Whether to include only ODA-related data. Defaults to False.
        period_length (int, optional): Length of the rolling period. Defaults to 3.

    Returns:
        pd.DataFrame: Dataframe with spending shares.
    """

    # Get the multilateral providers
    multilateral_providers = list(provider_groupings()["multilateral"])

    # Get the spending data by purpose
    data = (
        spending_by_purpose(
            years=years, providers=multilateral_providers, oda_only=oda_only
        )
        .pipe(add_multi_channels_and_group)
        .pipe(
            period_purpose_shares,
            period_length=period_length,
            grouper=None,
            share_by_grouper=CHANNEL_PURPOSE_SHARE_GROUPER,
        )
    )

    return data.drop(columns=[ODASchema.VALUE, ODASchema.PRICES, ODASchema.CURRENCY])


def core_multilateral_contributions_by_provider(
    years: list | int | range = None,
    providers: list | int | None = None,
    channels: list | int | None = None,
    measure: Measure | str = "gross_disbursement",
    currency: str = "USD",
    base_year: int | None = None,
) -> pd.DataFrame:
    """Retrieves core multilateral contributions grouped by provider and channel.

    Args:
        years (list | int | range, optional): Years to filter the data. Defaults to None.
        providers (list | int | None, optional): Providers to filter the data. Defaults to None.
        channels (list | int | None, optional): Channels to filter the data. Defaults to None.
        measure (Measure | str, optional): Measure type. Defaults to "gross_disbursement".
        currency (str, optional): Target currency. Defaults to "USD".
        base_year (int | None, optional): Base year for conversion. Defaults to None.

    Returns:
        pd.DataFrame: Dataframe with core multilateral contributions.
    """

    from oda_data.api.sources import MultiSystemData

    # Get the relevant measure and columns
    measure = MEASURES["Multisystem"][measure]["filter"]
    cols = [ODASchema.PROVIDER_CODE, ODASchema.CHANNEL_CODE, ODASchema.YEAR]

    # Set up filters
    filters = [
        ("flow_type", "in", [measure]),
        ("amount_type", "in", ["Current prices"]),
    ]

    # Add provider filters, if any
    if isinstance(channels, int):
        channels = [channels]
    if channels:
        filters.append(("channel_code", "in", channels))

    # Set up the multisystem data object
    ms = MultiSystemData(
        providers=providers, years=years, indicators="Core contributions to"
    )

    # Read the data and group by provider and channel
    data = (
        ms.read(
            columns=cols + [ODASchema.AMOUNT],
            additional_filters=filters,
            using_bulk_download=True,
        )
        .groupby(cols, dropna=False, observed=True)[[ODASchema.AMOUNT]]
        .sum()
        .reset_index()
        .rename(columns={ODASchema.AMOUNT: "value"})
    )

    # Convert the data to the target currency and prices
    data = convert_units(data, currency=currency, base_year=base_year)

    return data


def _compute_imputations(
    core_contributions: pd.DataFrame, multi_spending_shares: pd.DataFrame
) -> pd.DataFrame:
    """Computes imputed multilateral spending by donor and agency.

    Args:
        core_contributions (pd.DataFrame): Dataframe with core contributions.
        multi_spending_shares (pd.DataFrame): Dataframe with multilateral spending shares.

    Returns:
        pd.DataFrame: Imputed multilateral spending data.
    """

    # Merge core contributions with spending shares
    data = multi_spending_shares.merge(
        core_contributions,
        on=[ODASchema.CHANNEL_CODE, ODASchema.YEAR],
        how="inner",
    )

    # Compute imputed spending
    data = (
        data.assign(
            **{ODASchema.VALUE: lambda d: d[ODASchema.VALUE] * d[ODASchema.SHARE]}
        )
        .drop(labels=[ODASchema.SHARE], axis=1)
        .loc[lambda d: d[ODASchema.VALUE] != 0]
        .reset_index(drop=True)
    )

    return data


def imputed_multilateral_by_purpose(
    years: list | int | range = None,
    providers: list | int | None = None,
    channels: list | int | None = None,
    measure: Measure | str = "gross_disbursement",
    currency: str = "USD",
    base_year: int | None = None,
    shares_based_on_oda_only: bool = False,
) -> pd.DataFrame:
    """Computes imputed multilateral spending by purpose.

    Args:
        years (list | int | range, optional): Years to filter the data. Defaults to None.
        providers (list | int | None, optional): Providers to filter the data. Defaults to None.
        channels (list | int | None, optional): Channels to filter the data. Defaults to None.
        measure (Measure | str, optional): Measure type. Defaults to "gross_disbursement".
        currency (str, optional): Target currency. Defaults to "USD".
        base_year (int | None, optional): Base year for conversion. Defaults to None.
        shares_based_on_oda_only (bool, optional): Whether to base shares on ODA-only data. Defaults to False.

    Returns:
        pd.DataFrame: Dataframe with imputed multilateral spending by purpose.
    """

    # Get core multilateral contributions by provider and channel
    core = core_multilateral_contributions_by_provider(
        years=years,
        providers=providers,
        channels=channels,
        measure=measure,
        currency=currency,
        base_year=base_year,
    )

    # Get multilateral spending shares by channel and purpose
    multi_shares = multilateral_spending_shares_by_channel_and_purpose_smoothed(
        years=years, oda_only=shares_based_on_oda_only
    )

    # Compute imputed multilateral spending by purpose
    data = _compute_imputations(
        core_contributions=core, multi_spending_shares=multi_shares
    )

    return data

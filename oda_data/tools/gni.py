"""Compute GNI share indicators, including EU Institutions if applicable."""

from copy import copy
import pandas as pd

from oda_data import OECDClient
from oda_data.tools.groupings import provider_groupings
from oda_data.clean_data.schema import ODASchema


def _get_eu27_gni_as_eu_institutions(gni_obj: OECDClient) -> pd.DataFrame:
    """Aggregate EU27 GNI and reassign it to EU Institutions (provider code 918).

    Args:
        gni_obj: An OECDClient object configured to fetch GNI data.

    Returns:
        A DataFrame with the aggregated GNI for EU27, assigned to provider 918.
    """
    # Create a copy of the GNI object and set the providers to EU27 countries
    eu27_obj = copy(gni_obj)
    eu27_obj.providers = list(provider_groupings()["eu27_countries"])

    # Fetch the data and set the provider code to 918 (EU Institutions)
    df = eu27_obj.get_indicators("DAC1.40.1")
    df[ODASchema.PROVIDER_CODE] = 918
    df[ODASchema.PROVIDER_NAME] = "EU Institutions"

    group_columns = [col for col in df.columns if col != ODASchema.VALUE]

    # Group by year and provider code, summing the GNI values
    return (
        df.groupby(group_columns, dropna=False, observed=True)[[ODASchema.VALUE]]
        .sum()
        .reset_index()
    )


def _get_gni_data(client_obj: OECDClient) -> pd.DataFrame:
    """Fetch GNI data and optionally include EU Institutions' aggregate.

    Args:
        client_obj: An OECDClient object for the main data context.

    Returns:
        A DataFrame with GNI values per provider and year.
    """
    # Create a copy of the indicators object and set the measure to "net_disbursement"
    gni_obj = copy(client_obj)
    gni_obj.measure = ["net_disbursement"]

    # Fetch GNI data for the specified indicators
    gni_df = gni_obj.get_indicators("DAC1.40.1")
    providers = client_obj.providers or []

    # Check if EU Institutions (provider code 918) is in the list of providers
    # If it is, aggregate GNI for EU27 countries and assign it to provider 918
    if 918 in providers or not providers:
        eu_gni_df = _get_eu27_gni_as_eu_institutions(gni_obj)
        gni_df = pd.concat([gni_df, eu_gni_df], ignore_index=True)

    return gni_df.filter([ODASchema.YEAR, ODASchema.PROVIDER_CODE, ODASchema.VALUE])


def add_gni_share_column(
    indicators_obj: OECDClient, indicators: str | list[str]
) -> pd.DataFrame:
    """Add a GNI share column (%) to the dataset.

    This function fetches GNI values per provider and year, merges them with the
    indicator data, and computes each value's share of GNI.

    Args:
        indicators_obj: Configured OECDClient object to fetch data.
        indicators: A string or list of indicator codes to retrieve.

    Returns:
        A DataFrame with the original indicator data and a `gni_share_pct` column.
    """
    gni_data = _get_gni_data(indicators_obj)
    indicator_data = indicators_obj.get_indicators(indicators)

    merged = indicator_data.merge(
        gni_data,
        how="left",
        on=[ODASchema.YEAR, ODASchema.PROVIDER_CODE],
        suffixes=("", "_gni"),
    )

    merged["gni_share_pct"] = round(
        100 * merged[ODASchema.VALUE] / merged[f"{ODASchema.VALUE}_gni"], 2
    )

    return merged

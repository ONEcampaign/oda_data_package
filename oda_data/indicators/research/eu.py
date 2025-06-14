"""Compute weighted ODA values including EU institutions and bilateral donors."""

import pandas as pd

from oda_data.api.constants import Measure
from oda_data.api.oecd import get_measure_filter, OECDClient
from oda_data.api.sources import DAC1Data
from oda_data.clean_data.schema import ODASchema


def _load_dac1_eui_data(
    years: list[int] | int | range,
    measure: Measure | str,
    use_bulk_download: bool,
) -> pd.DataFrame:
    """Loads DAC1 data with specific filters applied."""
    indicators = [1010, 2102]
    filters = [(ODASchema.AMOUNT_TYPE_CODE, "==", "A")]
    measure_filter = get_measure_filter("DAC1", measure)

    idx = [
        ODASchema.YEAR,
        ODASchema.FLOWS_CODE,
        ODASchema.FUND_FLOWS,
        ODASchema.AIDTYPE_CODE,
    ]

    df = (
        DAC1Data(years=years, indicators=indicators)
        .read(additional_filters=filters, using_bulk_download=use_bulk_download)
        .loc[lambda d: d[ODASchema.FLOWS_CODE] == measure_filter]
        .filter(idx + [ODASchema.PROVIDER_CODE, ODASchema.VALUE])
    )

    return df


def _compute_spending_by_eui(df: pd.DataFrame) -> pd.DataFrame:
    """Computes total spending by the EU institutions for aid type 1010."""
    return (
        df.loc[df[ODASchema.PROVIDER_CODE] == 918]
        .loc[df[ODASchema.AIDTYPE_CODE] == 1010]
        .groupby(
            [
                ODASchema.YEAR,
                ODASchema.FLOWS_CODE,
                ODASchema.FUND_FLOWS,
                ODASchema.AIDTYPE_CODE,
            ],
            dropna=False,
        )[[ODASchema.VALUE]]
        .sum()
        .reset_index()
        .rename(columns={ODASchema.VALUE: "spending"})
    )


def _compute_inflows_by_providers(
    df: pd.DataFrame, providers: list[int]
) -> pd.DataFrame:
    """Computes inflows for given providers for aid type 2102."""
    return (
        df.loc[df[ODASchema.PROVIDER_CODE].isin(providers)]
        .loc[df[ODASchema.AIDTYPE_CODE] == 2102]
        .groupby(
            [
                ODASchema.YEAR,
                ODASchema.FLOWS_CODE,
                ODASchema.FUND_FLOWS,
                ODASchema.AIDTYPE_CODE,
            ],
            dropna=False,
        )[[ODASchema.VALUE]]
        .sum()
        .reset_index()
    )


def get_eui_oda_weights(
    years: list[int] | int | range = None,
    providers: list[int] | int | None = None,
    measure: Measure | str = "gross_disbursement",
    use_bulk_download: bool = False,
) -> dict[int, float]:
    """Computes weight adjustments for EU institution ODA contributions.

    The weights are calculated such that the EU institution's contributions are
    proportionally adjusted based on bilateral donor inflows.

    Args:
        years: Year or range of years for which to compute weights.
        providers: List of provider codes or a single provider code.
        measure: The financial measure to use (e.g. gross/net disbursement).
        use_bulk_download: Whether to use bulk download for data retrieval.

    Returns:
        A dictionary mapping each year to its corresponding EU institution weight.
    """
    df = _load_dac1_eui_data(
        years=years, measure=measure, use_bulk_download=use_bulk_download
    )

    spending = _compute_spending_by_eui(df)
    inflows = _compute_inflows_by_providers(df, providers)

    inflow_weights = inflows.merge(spending, on=[ODASchema.YEAR], how="left").assign(
        weight=lambda d: 1 - (d.value / d.spending)
    )

    return (
        inflow_weights.filter([ODASchema.YEAR, "weight"])
        .set_index(ODASchema.YEAR)["weight"]
        .to_dict()
    )


def get_eui_plus_bilateral_providers_indicator(
    client_obj: OECDClient, indicator: str | list[str]
) -> pd.DataFrame:
    """Fetches indicator values with adjusted EU institution contributions.

    Args:
        client_obj: An `OECDClient` instance to fetch indicator data.
        indicator: Indicator code or list of codes.

    Returns:
        A DataFrame containing indicator values with adjusted EU contributions.
    """
    eui_weights = get_eui_oda_weights(
        years=client_obj.years,
        providers=client_obj.providers,
        measure=client_obj.measure[0],
        use_bulk_download=client_obj.use_bulk_download,
    )

    if 918 not in client_obj.providers:
        client_obj.providers.append(918)

    data = client_obj.get_indicators(indicator)
    eui_mask = data[ODASchema.PROVIDER_CODE] == 918

    data.loc[eui_mask, ODASchema.VALUE] = data.loc[
        eui_mask, ODASchema.VALUE
    ] * data.loc[eui_mask, ODASchema.YEAR].map(eui_weights)

    return data

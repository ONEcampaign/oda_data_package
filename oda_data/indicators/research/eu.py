"""Compute weighted ODA values including EU institutions and bilateral donors."""

import pandas as pd

from oda_data.api.constants import Measure
from oda_data.api.oecd import get_measure_filter, OECDData
from oda_data.api.sources import Dac1Data
from oda_data.clean_data.schema import OdaSchema


def _load_dac1_eui_data(
    years: list[int] | int | range,
    measure: Measure | str,
) -> pd.DataFrame:
    """Loads DAC1 data with specific filters applied."""
    indicators = [1010, 2102]
    filters = [(OdaSchema.AMOUNT_TYPE_CODE, "==", "A")]
    measure_filter = get_measure_filter("DAC1", measure)

    idx = [
        OdaSchema.YEAR,
        OdaSchema.FLOWS_CODE,
        OdaSchema.FUND_FLOWS,
        OdaSchema.AIDTYPE_CODE,
    ]

    df = (
        Dac1Data(years=years, indicators=indicators)
        .read(additional_filters=filters)
        .loc[lambda d: d[OdaSchema.FLOWS_CODE] == measure_filter]
        .filter(idx + [OdaSchema.PROVIDER_CODE, OdaSchema.VALUE])
    )

    return df


def _compute_spending_by_eui(df: pd.DataFrame) -> pd.DataFrame:
    """Computes total spending by the EU institutions for aid type 1010."""
    return (
        df.loc[df[OdaSchema.PROVIDER_CODE] == 918]
        .loc[df[OdaSchema.AIDTYPE_CODE] == 1010]
        .groupby(
            [
                OdaSchema.YEAR,
                OdaSchema.FLOWS_CODE,
                OdaSchema.FUND_FLOWS,
                OdaSchema.AIDTYPE_CODE,
            ],
            dropna=False,
        )[[OdaSchema.VALUE]]
        .sum()
        .reset_index()
        .rename(columns={OdaSchema.VALUE: "spending"})
    )


def _compute_inflows_by_providers(
    df: pd.DataFrame, providers: list[int]
) -> pd.DataFrame:
    """Computes inflows for given providers for aid type 2102."""
    return (
        df.loc[df[OdaSchema.PROVIDER_CODE].isin(providers)]
        .loc[df[OdaSchema.AIDTYPE_CODE] == 2102]
        .groupby(
            [
                OdaSchema.YEAR,
                OdaSchema.FLOWS_CODE,
                OdaSchema.FUND_FLOWS,
                OdaSchema.AIDTYPE_CODE,
            ],
            dropna=False,
        )[[OdaSchema.VALUE]]
        .sum()
        .reset_index()
    )


def get_eui_oda_weights(
    years: list[int] | int | range = None,
    providers: list[int] | int | None = None,
    measure: Measure | str = "gross_disbursement",
) -> dict[int, float]:
    """Computes weight adjustments for EU institution ODA contributions.

    The weights are calculated such that the EU institution's contributions are
    proportionally adjusted based on bilateral donor inflows.

    Args:
        years: Year or range of years for which to compute weights.
        providers: List of provider codes or a single provider code.
        measure: The financial measure to use (e.g. gross/net disbursement).

    Returns:
        A dictionary mapping each year to its corresponding EU institution weight.
    """
    df = _load_dac1_eui_data(years=years, measure=measure)

    spending = _compute_spending_by_eui(df)
    inflows = _compute_inflows_by_providers(df, providers)

    inflow_weights = inflows.merge(spending, on=[OdaSchema.YEAR], how="left").assign(
        weight=lambda d: 1 - (d.value / d.spending)
    )

    return (
        inflow_weights.filter([OdaSchema.YEAR, "weight"])
        .set_index(OdaSchema.YEAR)["weight"]
        .to_dict()
    )


def get_eui_plus_bilateral_providers_indicator(
    indicators_obj: OECDData, indicator: str | list[str]
) -> pd.DataFrame:
    """Fetches indicator values with adjusted EU institution contributions.

    Args:
        indicators_obj: An `OECDData` instance to fetch indicator data.
        indicator: Indicator code or list of codes.

    Returns:
        A DataFrame containing indicator values with adjusted EU contributions.
    """
    eui_weights = get_eui_oda_weights(
        years=indicators_obj.years,
        providers=indicators_obj.providers,
        measure=indicators_obj.measure[0],
    )

    if 918 not in indicators_obj.providers:
        indicators_obj.providers.append(918)

    data = indicators_obj.get_indicators(indicator)
    eui_mask = data[OdaSchema.PROVIDER_CODE] == 918

    data.loc[eui_mask, OdaSchema.VALUE] = data.loc[
        eui_mask, OdaSchema.VALUE
    ] * data.loc[eui_mask, OdaSchema.YEAR].map(eui_weights)

    return data

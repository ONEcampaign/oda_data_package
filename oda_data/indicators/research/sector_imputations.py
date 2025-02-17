import pandas as pd

from oda_data.api.constants import Measure, MEASURES
from oda_data.clean_data.common import convert_units
from oda_data.clean_data.schema import OdaSchema


def core_multilateral_contributions_by_provider(
    years: list | int | range = None,
    providers: list | int | None = None,
    channels: list | int | None = None,
    measure: Measure | str = "gross_disbursement",
    currency: str = "USD",
    base_year: int | None = None,
) -> pd.DataFrame:
    """Get a simplified dataframe with the money contributed by donors to
    multilaterals (grouped by channel code)"""

    from oda_data.api.sources import MultiSystemData

    measure = MEASURES["Multisystem"][measure]["filter"]
    cols = [OdaSchema.PROVIDER_CODE, OdaSchema.CHANNEL_CODE, OdaSchema.YEAR]

    filters = [
        ("flow_type", "in", [measure]),
        ("amount_type", "in", ["Current prices"]),
    ]

    if isinstance(channels, int):
        channels = [channels]
    if channels:
        filters.append(("channel_code", "in", channels))

    ms = MultiSystemData(
        providers=providers, years=years, indicators="Core contributions to"
    )

    data = (
        ms.read(columns=cols + [OdaSchema.AMOUNT], additional_filters=filters)
        .groupby(cols, dropna=False, observed=True)[[OdaSchema.AMOUNT]]
        .sum()
        .reset_index()
        .rename(columns={OdaSchema.AMOUNT: "value"})
    )

    data = convert_units(data, currency=currency, base_year=base_year)

    return data


if __name__ == "__main__":
    df = core_multilateral_contributions_by_provider(
        years=range(2013, 2023),
        providers=4,
        channels=[41116],
        currency="EUR",
        base_year=2023,
    )

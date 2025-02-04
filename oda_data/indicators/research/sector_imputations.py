import pandas as pd

from oda_data import read_multisystem
from oda_data.api.constants import Measure
from oda_data.clean_data.common import convert_units
from oda_data.clean_data.schema import OdaSchema
from oda_data.clean_data.validation import (
    validate_providers,
    validate_measure,
    validate_currency,
    validate_prices,
    validate_base_year,
)


def multi_contributions_by_donor(data) -> pd.DataFrame:
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


def build_filters(
    providers: list | int | None,
    measure: list[Measure],
    currency: str,
    measure_mapping: dict,
) -> list[tuple]:

    # Build filters
    filters = []

    # Apply provider filter
    if providers:
        filters.append(("donor_code", "in", providers))

    # Apply measure filter
    filters.append(("flow_type", "in", [measure_mapping[m] for m in measure]))

    if currency == "LCU":
        raise ValueError("Currency LCU is not supported for this indicator")
    else:
        filters.append(("amount_type", "==", "Current prices"))

    return filters


def core_multilateral_contributions_by_provider(
    years: list | int | range,
    providers: list | int | None = None,
    measure: list[Measure] | Measure = "gross_disbursement",
    currency: str = "USD",
    prices: str = "current",
    base_year: int | None = None,
):
    """"""

    # Validate the input parameters
    providers = validate_providers(providers)
    measure = validate_measure(measure)
    validate_currency(currency)
    validate_prices(prices)
    validate_base_year(base_year=base_year, prices=prices)

    # Define the measure mapping
    measure_mapping = {
        "gross_disbursement": "Disbursements",
        "commitment": "Commitments",
    }

    # Build filters
    filters = build_filters(
        providers=providers,
        measure=measure,
        currency=currency,
        measure_mapping=measure_mapping,
    )

    filters.append(("aid_to_or_thru", "==", "Core contributions to"))

    # Read the data
    df = read_multisystem(years=years, filters=filters)

    # Convert the units
    df = convert_units(data=df, currency=currency, prices=prices, base_year=base_year)

    return df


if __name__ == "__main__":
    df = core_multilateral_contributions_by_provider(years=2022, providers=4)

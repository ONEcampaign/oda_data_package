import pandas as pd

from oda_data.api.constants import (
    Measure,
    PolicyMarker,
    MEASURES,
    EXTENDED_PROVIDER_PURPOSE_GROUPER,
    MarkerScore,
)
from oda_data.clean_data.common import convert_units
from oda_data.clean_data.schema import ODASchema


def _marker_score_map(marker: PolicyMarker | str) -> list:
    return {
        "significant": [1],
        "principal": [2],
        "not_targeted": [0],
        "total_targeted": [1, 2],
    }.get(marker)


def _marker_modality_filter() -> list:
    return [
        (
            "modality",
            "in",
            [
                "A02",
                "B01",
                "B03",
                "B031",
                "B032",
                "B033",
                "B04",
                "C01",
                "D01",
                "D02",
                "E01",
            ],
        ),
    ]


def bilateral_policy_marker(
    years: list | int | range = None,
    providers: list | int | None = None,
    recipients: list | int | None = None,
    measure: Measure | str = "gross_disbursement",
    *,
    marker: PolicyMarker,
    marker_score: MarkerScore,
    oda_only: bool = True,
    currency: str = "USD",
    base_year: int | None = None,
) -> pd.DataFrame:
    """Gets policy marker data

    Args:
        years (list | int | range, optional): Years to filter the data. Defaults to None.
        providers (list | int | None, optional): Providers to filter the data. Defaults to None.
        recipients (list | int | None, optional): Recipients to filter the data. Defaults to None.
        measure (Measure | str, optional): Measure type. Defaults to "gross_disbursement".
        marker (PolicyMarker): Policy marker. Options are: "gender", "environment", "nutrition",
           "disability", "biodiversity"
        marker_score (MarkerScore): Policy marker score.
          Options are: "significant", "principal", "not_targeted", "not_screened", "total_targeted", "total_allocable".
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
        for c in EXTENDED_PROVIDER_PURPOSE_GROUPER
        if c not in [ODASchema.CURRENCY, ODASchema.PRICES]
    ]

    # Set up filters
    filters = _marker_modality_filter()

    # Map which scores are needed
    marker_filter = _marker_score_map(marker_score)

    if marker_filter:
        filters.append((marker, "in", marker_filter))

    if oda_only:
        filters.append(("category", "in", [10, 60]))

    # Set up the CRS data object
    crs = CRSData(providers=providers, years=years, recipients=recipients)

    # Read the data and group by provider and purpose
    data = crs.read(
        columns=grouper + [measure],
        additional_filters=filters,
        using_bulk_download=True,
    )

    # if marker is not_screened, we need to filter out the screened data
    if marker == "not_screened":
        data = data.loc[lambda d: d[marker].isna()]

    data = (
        data.groupby(grouper, dropna=False, observed=True)[[measure]]
        .sum()
        .reset_index()
        .rename(columns={measure: "value"})
        .assign(**{marker: marker_score})
    )

    # Convert the data to the target currency and prices
    data = convert_units(data, currency=currency, base_year=base_year)

    return data

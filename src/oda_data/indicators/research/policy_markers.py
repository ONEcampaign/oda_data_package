from typing import get_args

import pandas as pd

from oda_data.api.constants import (
    EXTENDED_PROVIDER_PURPOSE_GROUPER,
    MEASURES,
    MarkerScore,
    Measure,
    PolicyMarker,
)
from oda_data.clean_data.common import convert_units
from oda_data.clean_data.schema import ODASchema

# CRS marker values kept by each score. Desertification also reports 3, a principal
# objective that supports an action programme to combat desertification. The
# not_screened and total_allocable scores have no values: not_screened keeps blank
# markers and total_allocable keeps every row.
_MARKER_SCORE_VALUES: dict[str, list[int] | None] = {
    "significant": [1],
    "principal": [2, 3],
    "not_targeted": [0],
    "total_targeted": [1, 2, 3],
    "not_screened": None,
    "total_allocable": None,
}


def _marker_score_map(marker_score: MarkerScore | str) -> list[int] | None:
    """Return the CRS marker values a score keeps, or None if it filters no values.

    Raises:
        ValueError: If the score is not a MarkerScore.
    """
    if marker_score not in _MARKER_SCORE_VALUES:
        raise ValueError(
            f"Unknown marker_score {marker_score!r}; "
            f"expected one of {list(_MARKER_SCORE_VALUES)}"
        )
    return _MARKER_SCORE_VALUES[marker_score]


def _marker_modality_filter() -> list[tuple[str, str, list]]:
    return [
        (
            ODASchema.FLOW_MODALITY,
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
    years: list | int | range | None = None,
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
        marker (PolicyMarker): Policy marker. Options are: "gender", "environment",
           "climate_adaptation", "climate_mitigation", "biodiversity", "desertification",
           "disability", "nutrition".
        marker_score (MarkerScore): Policy marker score. "principal", "significant",
          "not_targeted" and "not_screened" (a blank marker) split allocable aid with no
          overlap. "total_targeted" is principal plus significant, and "total_allocable"
          is all allocable aid, screened or not.
        oda_only (bool, optional): Whether to include only ODA flows. Defaults to True.
        currency (str, optional): Target currency. Defaults to "USD".
        base_year (int | None, optional): Base year for conversion. Defaults to None.

    Returns:
        pd.DataFrame: Dataframe with spending by purpose.

    Raises:
        ValueError: If the marker or the score is not one of the options above.
    """
    from oda_data.api.sources import CRSData

    if marker not in get_args(PolicyMarker):
        raise ValueError(
            f"Unknown marker {marker!r}; expected one of {list(get_args(PolicyMarker))}"
        )

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
        filters.append(
            (
                ODASchema.FLOW_CODE,
                "in",
                [11, 13, 19, 60],
            )
        )

    # Set up the CRS data object
    crs = CRSData(providers=providers, years=years, recipients=recipients)

    # Read the data and group by provider and purpose
    data = crs.read(
        columns=[*grouper, measure, marker],
        additional_filters=filters,
        using_bulk_download=True,
    )

    # Keep only unscreened activities (blank marker)
    if marker_score == "not_screened":
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

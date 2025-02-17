import copy

import pandas as pd

from oda_data.clean_data.channels import add_multi_channel_codes
from oda_data.clean_data.common import keep_multi_donors_only
from oda_data.clean_data.dtypes import set_default_types
from oda_data.clean_data.schema import OdaSchema


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

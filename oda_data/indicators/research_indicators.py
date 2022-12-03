import pandas as pd

from oda_data.indicators.components import (
    multi_contributions_by_donor,
    period_purpose_shares,
    compute_imputations,
)


def multilateral_imputed_flows(
    years: list,
    currency: str,
    prices: str,
    base_year: int | None,
    donors: list | None,
    recipients: list | None,
    **kwargs,
) -> pd.DataFrame:
    from oda_data import ODAData

    # Create the basic ODAData object
    data_obj = ODAData(
        years=years, currency=currency, prices=prices, base_year=base_year
    )

    # --- Bilateral contributions to multilaterals ---
    core_contributions = multi_contributions_by_donor(data=data_obj)

    # --- Multilateral spending by sector (as shares) ---
    multi_spending_shares = period_purpose_shares(data=data_obj, period_length=3)

    # --- Multilateral spending by sector (as values) ---
    imputed = compute_imputations(
        core_contributions=core_contributions,
        multi_spending_shares=multi_spending_shares,
    )

    # --- Filter by donor and recipient, if applicable ---
    if donors is not None:
        imputed = imputed.loc[imputed.donor_code.isin(donors)]
    if recipients is not None:
        imputed = imputed.loc[imputed.recipient_code.isin(recipients)]

    return imputed.reset_index(drop=True)

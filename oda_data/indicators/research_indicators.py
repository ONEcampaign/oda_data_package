import pandas as pd

from oda_data.indicators.sector_components import (
    multi_contributions_by_donor,
    period_purpose_shares,
    compute_imputations,
    bilat_outflows_by_donor,
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


def total_bi_multi_flows(
    years: list,
    currency: str,
    prices: str,
    base_year: int | None,
    donors: list | None,
    recipients: list | None,
    **kwargs,
) -> pd.DataFrame:
    from oda_data import ODAData

    multi_data_obj = ODAData(
        years=years, currency=currency, prices=prices, base_year=base_year
    )

    # --- Multilateral spending by sector (as values) ---
    multi_indicator = "imputed_multi_flow_disbursement_gross"
    imputed_spending = multi_data_obj.load_indicator(multi_indicator).get_data(
        multi_indicator
    )

    # Create the basic ODAData object
    bilat_data_obj = ODAData(
        years=years, currency=currency, prices=prices, base_year=base_year
    )
    # --- Bilateral spending by sector ---
    bilat_spending = bilat_outflows_by_donor(
        data=bilat_data_obj, purpose_column="purpose_code"
    ).loc[lambda d: d.year.isin(imputed_spending.year.unique())]

    # --- Combine bilateral and multilateral spending ---
    df = (
        pd.concat([bilat_spending, imputed_spending], ignore_index=True)
        .drop("channel_code", axis=1)
        .groupby(
            by=[c for c in bilat_spending.columns if c != "value"],
            observed=True,
            dropna=False,
        )
        .sum(numeric_only=True)
        .reset_index()
    )

    # --- Filter by donor and recipient, if applicable ---
    if donors is not None:
        df = df.loc[lambda d: d.donor_code.isin(donors)]
    if recipients is not None:
        df = df.loc[lambda d: d.recipient_code.isin(recipients)]

    return df.reset_index(drop=True)

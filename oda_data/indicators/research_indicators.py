import pandas as pd

from oda_data.indicators.sector_components import (
    bilat_outflows_by_donor,
    compute_imputations,
    multi_contributions_by_donor,
    period_purpose_shares,
)


def multilateral_imputed_flows(
    years: list, currency: str, donors: list | None, recipients: list | None, **kwargs
) -> pd.DataFrame:
    from oda_data import ODAData

    # Create the basic ODAData object
    data_obj = ODAData(
        years=years,
        currency=currency,  # prices=prices, base_year=base_year
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
    years: list, currency: str, donors: list | None, recipients: list | None, **kwargs
) -> pd.DataFrame:
    from oda_data import ODAData

    multi_data_obj = ODAData(
        years=years,
        currency=currency,  # prices=prices, base_year=base_year
    )

    # --- Multilateral spending by sector (as values) ---
    multi_indicator = "imputed_multi_flow_disbursement_gross"
    imputed_spending = multi_data_obj.load_indicator(multi_indicator).get_data(
        multi_indicator
    )

    # Create the basic ODAData object
    bilat_data_obj = ODAData(
        years=years,
        currency=currency,  # prices=prices, base_year=base_year
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


def oda_gni_flow(
    years: list,
    currency: str,
    prices: str,
    donors: list | None,
    recipients: list | None,
    **kwargs,
) -> pd.DataFrame:
    from oda_data import ODAData

    return (
        ODAData(years=years, donors=donors, recipients=recipients)
        .load_indicator("total_oda_flow_net")
        .add_share_of_gni()
        .get_data()
        .drop("value", axis=1)
        .rename(columns={"gni_share": "value"})
        .assign(prices=prices, currency=currency)
    )


def oda_gni_ge(
    years: list,
    currency: str,
    prices: str,
    donors: list | None,
    recipients: list | None,
    **kwargs,
) -> pd.DataFrame:
    from oda_data import ODAData

    return (
        ODAData(years=years, donors=donors, recipients=recipients)
        .load_indicator("total_oda_ge")
        .add_share_of_gni()
        .get_data()
        .drop("value", axis=1)
        .rename(columns={"gni_share": "value"})
        .assign(prices=prices, currency=currency)
    )


def total_oda_official_definition(
    years: list,
    donors: list | None,
    **kwargs,
) -> pd.DataFrame:
    from oda_data import ODAData

    indicators = ["total_oda_flow_net", "total_oda_ge"]

    data = ODAData(years=years, donors=donors).load_indicator(indicators).get_data()
    query = (
        "(indicator == 'total_oda_flow_net' and year < 2018) or "
        "(indicator == 'total_oda_ge' and year >= 2018)"
    )

    return data.query(query).reset_index(drop=True)


def one_non_core_oda_ge_linked(
    years: list, donors: list | None, **kwargs
) -> pd.DataFrame:
    from oda_data import ODAData

    indicators = [
        "debt_relief_ge",
        "idrc_ge_linked",
        "total_in_donor_students_ge_linked",
    ]

    data = (
        ODAData(years=years, donors=donors)
        .load_indicator(indicators)
        .get_data()
        .loc[lambda d: d.year >= 2018]
    )

    cols = [c for c in data.columns if c not in ["value", "indicator", "aidtype_code"]]

    return (
        data.groupby(cols, observed=True, dropna=False)
        .sum(numeric_only=True)
        .reset_index()
        .assign(indicator="one_non_core_ge_linked")
    )


def _core_oda(oda_obj, indicators: list) -> pd.DataFrame:
    oda_obj.load_indicator(indicators)

    total = oda_obj.get_data([i for i in indicators if "total" in i])
    non_core = oda_obj.get_data([i for i in indicators if "core" in i]).assign(
        value=lambda d: -1 * d.value
    )

    cols = [c for c in total.columns if c not in ["value", "indicator", "aidtype_code"]]

    return (
        pd.concat([total, non_core], ignore_index=True)
        .groupby(cols, observed=True, dropna=False)
        .sum(numeric_only=True)
        .reset_index()
    )


def one_core_oda_flow(years: list, donors: list | None, **kwargs) -> pd.DataFrame:
    from oda_data import ODAData

    indicators = ["total_oda_flow_net", "one_non_core_oda_flow"]

    oda = ODAData(years=years, donors=donors)

    return _core_oda(oda, indicators)


def one_core_oda_ge(years: list, donors: list | None, **kwargs) -> pd.DataFrame:
    from oda_data import ODAData

    indicators = ["total_oda_ge", "one_non_core_oda_ge"]

    oda = ODAData(years=years, donors=donors)

    return _core_oda(oda, indicators).query("year >= 2018")


def one_core_oda_ge_linked(years: list, donors: list | None, **kwargs) -> pd.DataFrame:
    from oda_data import ODAData

    indicators = ["total_oda_ge", "one_non_core_oda_ge_linked"]

    oda = ODAData(years=years, donors=donors)

    return _core_oda(oda, indicators).query("year >= 2018")

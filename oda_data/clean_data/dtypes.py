from collections import defaultdict

import pandas as pd

from oda_data.clean_data.schema import ODASchema


def schema_types(save: bool = False) -> dict:
    """
    Returns a dictionary of schema types. Invalid columns are assumed to be strings.
    By default, pyarrow types are used for all columns.

    Returns:
        dict: A dictionary mapping attribute names to their corresponding data types.

    """
    category = "category" if save else "string[pyarrow]"
    numerical_cat = "category" if save else "int16[pyarrow]"
    long_numerical_cat = "category" if save else "int32[pyarrow]"
    numerical = "float"
    string = "string[pyarrow]"
    date = "datetime64[ns]"

    types = {
        ODASchema.YEAR: numerical_cat,
        ODASchema.PROVIDER_CODE: long_numerical_cat,
        ODASchema.PROVIDER_NAME: category,
        ODASchema.PROVIDER_TYPE: category,
        ODASchema.PROVIDER_DETAILED: category,
        ODASchema.AGENCY_CODE: numerical_cat,
        ODASchema.AGENCY_NAME: category,
        ODASchema.CRS_ID: string,
        ODASchema.PROJECT_ID: string,
        ODASchema.RECIPIENT_CODE: numerical_cat,
        ODASchema.RECIPIENT_NAME: category,
        ODASchema.RECIPIENT_REGION: category,
        ODASchema.RECIPIENT_REGION_CODE: numerical_cat,
        ODASchema.RECIPIENT_INCOME: category,
        ODASchema.RECIPIENT_INCOME_CODE: numerical_cat,
        ODASchema.FLOW_MODALITY: category,
        ODASchema.ALLOCABLE_SHARE: numerical,
        ODASchema.CONCESSIONALITY: category,
        ODASchema.FINANCIAL_INSTRUMENT: category,
        ODASchema.FLOW_TYPE: category,
        ODASchema.FINANCE_TYPE: numerical_cat,
        ODASchema.CHANNEL_NAME_DELIVERY: category,
        ODASchema.CHANNEL_CODE_DELIVERY: long_numerical_cat,
        ODASchema.CHANNEL_CODE: long_numerical_cat,
        ODASchema.CHANNEL_NAME: category,
        ODASchema.ADAPTATION: numerical_cat,
        ODASchema.MITIGATION: numerical_cat,
        ODASchema.CROSS_CUTTING: numerical_cat,
        ODASchema.ADAPTATION_VALUE: numerical,
        ODASchema.MITIGATION_VALUE: numerical,
        ODASchema.CROSS_CUTTING_VALUE: numerical,
        ODASchema.CLIMATE_OBJECTIVE: category,
        ODASchema.CLIMATE_FINANCE_VALUE: numerical,
        ODASchema.COMMITMENT_CLIMATE_SHARE: numerical,
        ODASchema.NOT_CLIMATE: numerical,
        ODASchema.CLIMATE_UNSPECIFIED: numerical,
        ODASchema.CLIMATE_UNSPECIFIED_SHARE: numerical,
        ODASchema.PURPOSE_CODE: long_numerical_cat,
        ODASchema.PURPOSE_NAME: category,
        ODASchema.SECTOR_CODE: long_numerical_cat,
        ODASchema.SECTOR_NAME: category,
        ODASchema.PROJECT_TITLE: string,
        ODASchema.PROJECT_DESCRIPTION: string,
        ODASchema.PROJECT_DESCRIPTION_SHORT: string,
        ODASchema.GENDER: numerical_cat,
        ODASchema.INDICATOR: category,
        ODASchema.VALUE: numerical,
        ODASchema.AMOUNT: numerical,
        ODASchema.TOTAL_VALUE: numerical,
        ODASchema.SHARE: numerical,
        ODASchema.CLIMATE_SHARE: numerical,
        ODASchema.CLIMATE_SHARE_ROLLING: numerical,
        ODASchema.FLOW_CODE: long_numerical_cat,
        ODASchema.FLOW_NAME: category,
        ODASchema.BI_MULTI: numerical_cat,
        ODASchema.CATEGORY: numerical_cat,
        ODASchema.USD_COMMITMENT: numerical,
        ODASchema.USD_DISBURSEMENT: numerical,
        ODASchema.USD_RECEIVED: numerical,
        ODASchema.USD_GRANT_EQUIV: numerical,
        ODASchema.USD_NET_DISBURSEMENT: numerical,
        ODASchema.REPORTING_METHOD: category,
        ODASchema.MULTILATERAL_TYPE: category,
        ODASchema.CONVERGED_REPORTING: category,
        ODASchema.COAL_FINANCING: category,
        ODASchema.LEVEL: category,
        ODASchema.USD_COMMITMENT_DEFL: numerical,
        ODASchema.USD_DISBURSEMENT_DEFL: numerical,
        ODASchema.USD_RECEIVED_DEFL: numerical,
        ODASchema.USD_ADJUSTMENT: numerical,
        ODASchema.USD_ADJUSTMENT_DEFL: numerical,
        ODASchema.USD_AMOUNT_UNTIED: numerical,
        ODASchema.USD_AMOUNT_UNTIED_DEFL: numerical,
        ODASchema.USD_AMOUNT_TIED: numerical,
        ODASchema.USD_AMOUNT_TIED_DEFL: numerical,
        ODASchema.USD_AMOUNT_PARTIAL_TIED: numerical,
        ODASchema.USD_AMOUNT_PARTIAL_TIED_DEFL: numerical,
        ODASchema.USD_IRTC_CODE: category,
        ODASchema.USD_EXPERT_COMMITMENT: numerical,
        ODASchema.USD_EXPERT_EXTENDED: numerical,
        ODASchema.USD_EXPORT_CREDIT: numerical,
        ODASchema.COMMITMENT_NATIONAL: numerical,
        ODASchema.DISBURSEMENT_NATIONAL: numerical,
        ODASchema.GRANT_EQUIV_NATIONAL: numerical,
        ODASchema.PARENT_CHANNEL_CODE: long_numerical_cat,
        ODASchema.LDC_FLAG_CODE: numerical_cat,
        ODASchema.LDC_FLAG: category,
        ODASchema.EXPECTED_START_DATE: date,
        ODASchema.COMPLETION_DATE: date,
        ODASchema.ENVIRONMENT: numerical_cat,
        ODASchema.DIG_CODE: numerical_cat,
        ODASchema.TRADE: category,
        ODASchema.RMNCH_CODE: numerical_cat,
        ODASchema.DRR_CODE: numerical_cat,
        ODASchema.NUTRITION: numerical_cat,
        ODASchema.DISABILITY: numerical_cat,
        ODASchema.FTC_CODE: numerical_cat,
        ODASchema.PBA_CODE: numerical_cat,
        ODASchema.INVESTMENT_PROJECT: numerical_cat,
        ODASchema.ASSOC_FINANCE: numerical_cat,
        ODASchema.BIODIVERSITY: numerical_cat,
        ODASchema.DESERTIFICATION: numerical_cat,
        ODASchema.COMMITMENT_DATE: date,
        ODASchema.TYPE_REPAYMENT: numerical_cat,
        ODASchema.NUMBER_REPAYMENT: numerical_cat,
        ODASchema.INTEREST1: category,
        ODASchema.INTEREST2: category,
        ODASchema.REPAYDATE1: date,
        ODASchema.REPAYDATE2: date,
        ODASchema.USD_INTEREST: numerical,
        ODASchema.USD_OUTSTANDING: numerical,
        ODASchema.USD_ARREARS_PRINCIPAL: numerical,
        ODASchema.USD_ARREARS_INTEREST: numerical,
        ODASchema.CAPITAL_EXPEND: numerical,
        ODASchema.PSI_FLAG: numerical_cat,
        ODASchema.PSI_ADD_TYPE: category,
        ODASchema.PSI_ADD_ASSESS: category,
        ODASchema.PSI_ADD_DEV_OBJECTIVE: category,
        ODASchema.PRICES: category,
        ODASchema.CURRENCY: category,
        ODASchema.AIDTYPE_CODE: long_numerical_cat,
        ODASchema.FLOWS_CODE: long_numerical_cat,
    }

    return defaultdict(lambda: "string[pyarrow]", types)


def set_default_types(df: pd.DataFrame, save: bool = False) -> pd.DataFrame:
    """
    Set the types of the columns in the dataframe.

    Args:
        df (pd.DataFrame): The input dataframe.

    Returns:
        pd.DataFrame: The dataframe with the types set.

    """
    default_types = schema_types(save=save)

    converted_types = {c: default_types[c] for c in df.columns}

    return df.astype(converted_types)

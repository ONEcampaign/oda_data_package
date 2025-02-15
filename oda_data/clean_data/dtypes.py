from collections import defaultdict

import pandas as pd

from oda_data.clean_data.schema import OdaSchema


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
    value = "float"

    types = {
        OdaSchema.YEAR: numerical_cat,
        OdaSchema.PROVIDER_CODE: long_numerical_cat,
        OdaSchema.PROVIDER_NAME: category,
        OdaSchema.PROVIDER_TYPE: category,
        OdaSchema.PROVIDER_DETAILED: category,
        OdaSchema.AGENCY_CODE: numerical_cat,
        OdaSchema.AGENCY_NAME: category,
        OdaSchema.CRS_ID: "string[pyarrow]",
        OdaSchema.PROJECT_ID: "string[pyarrow]",
        OdaSchema.RECIPIENT_CODE: numerical_cat,
        OdaSchema.RECIPIENT_NAME: category,
        OdaSchema.RECIPIENT_REGION: category,
        OdaSchema.RECIPIENT_REGION_CODE: numerical_cat,
        OdaSchema.RECIPIENT_INCOME: category,
        OdaSchema.RECIPIENT_INCOME_CODE: category,
        OdaSchema.FLOW_MODALITY: category,
        OdaSchema.ALLOCABLE_SHARE: value,
        OdaSchema.CONCESSIONALITY: category,
        OdaSchema.FINANCIAL_INSTRUMENT: category,
        OdaSchema.FLOW_TYPE: category,
        OdaSchema.FINANCE_TYPE: numerical_cat,
        OdaSchema.CHANNEL_NAME_DELIVERY: category,
        OdaSchema.CHANNEL_CODE_DELIVERY: long_numerical_cat,
        OdaSchema.CHANNEL_CODE: long_numerical_cat,
        OdaSchema.CHANNEL_NAME: category,
        OdaSchema.ADAPTATION: numerical_cat,
        OdaSchema.MITIGATION: numerical_cat,
        OdaSchema.CROSS_CUTTING: numerical_cat,
        OdaSchema.ADAPTATION_VALUE: value,
        OdaSchema.MITIGATION_VALUE: value,
        OdaSchema.CROSS_CUTTING_VALUE: value,
        OdaSchema.CLIMATE_OBJECTIVE: category,
        OdaSchema.CLIMATE_FINANCE_VALUE: value,
        OdaSchema.COMMITMENT_CLIMATE_SHARE: value,
        OdaSchema.NOT_CLIMATE: value,
        OdaSchema.CLIMATE_UNSPECIFIED: value,
        OdaSchema.CLIMATE_UNSPECIFIED_SHARE:value,
        OdaSchema.PURPOSE_CODE: long_numerical_cat,
        OdaSchema.PURPOSE_NAME: category,
        OdaSchema.SECTOR_CODE: long_numerical_cat,
        OdaSchema.SECTOR_NAME: category,
        OdaSchema.PROJECT_TITLE: "string[pyarrow]",
        OdaSchema.PROJECT_DESCRIPTION: "string[pyarrow]",
        OdaSchema.PROJECT_DESCRIPTION_SHORT: "string[pyarrow]",
        OdaSchema.GENDER: category,
        OdaSchema.INDICATOR: category,
        OdaSchema.VALUE: value,
        OdaSchema.AMOUNT: value,
        OdaSchema.TOTAL_VALUE: value,
        OdaSchema.SHARE: value,
        OdaSchema.CLIMATE_SHARE: value,
        OdaSchema.CLIMATE_SHARE_ROLLING: value,
        OdaSchema.FLOW_CODE: "int32[pyarrow]",
        OdaSchema.FLOW_NAME: category,
        OdaSchema.BI_MULTI: numerical_cat,
        OdaSchema.CATEGORY: numerical_cat,
        OdaSchema.USD_COMMITMENT: value,
        OdaSchema.USD_DISBURSEMENT: value,
        OdaSchema.USD_RECEIVED: value,
        OdaSchema.USD_GRANT_EQUIV: value,
        OdaSchema.USD_NET_DISBURSEMENT:value,
        OdaSchema.REPORTING_METHOD: category,
        OdaSchema.MULTILATERAL_TYPE: category,
        OdaSchema.CONVERGED_REPORTING: category,
        OdaSchema.COAL_FINANCING: category,
        OdaSchema.LEVEL: category,
        OdaSchema.USD_COMMITMENT_DEFL: value,
        OdaSchema.USD_DISBURSEMENT_DEFL: value,
        OdaSchema.USD_RECEIVED_DEFL: value,
        OdaSchema.USD_ADJUSTMENT: value,
        OdaSchema.USD_ADJUSTMENT_DEFL: value,
        OdaSchema.USD_AMOUNT_UNTIED: value,
        OdaSchema.USD_AMOUNT_UNTIED_DEFL: value,
        OdaSchema.USD_AMOUNT_TIED: value,
        OdaSchema.USD_AMOUNT_TIED_DEFL: value,
        OdaSchema.USD_AMOUNT_PARTIAL_TIED: value,
        OdaSchema.USD_AMOUNT_PARTIAL_TIED_DEFL: value,
        OdaSchema.USD_IRTC_CODE: category,
        OdaSchema.USD_EXPERT_COMMITMENT: value,
        OdaSchema.USD_EXPERT_EXTENDED: value,
        OdaSchema.USD_EXPORT_CREDIT: value,
        OdaSchema.COMMITMENT_NATIONAL:value,
        OdaSchema.DISBURSEMENT_NATIONAL: value,
        OdaSchema.GRANT_EQUIV_NATIONAL: value,
        OdaSchema.PARENT_CHANNEL_CODE: "int32[pyarrow]",
        OdaSchema.LDC_FLAG_CODE: numerical_cat,
        OdaSchema.LDC_FLAG: category,
        OdaSchema.EXPECTED_START_DATE: "datetime64[ns]",
        OdaSchema.COMPLETION_DATE: "datetime64[ns]",
        OdaSchema.ENVIRONMENT: numerical_cat,
        OdaSchema.DIG_CODE: numerical_cat,
        OdaSchema.TRADE: category,
        OdaSchema.RMNCH_CODE: numerical_cat,
        OdaSchema.DRR_CODE: numerical_cat,
        OdaSchema.NUTRITION: numerical_cat,
        OdaSchema.DISABILITY: numerical_cat,
        OdaSchema.FTC_CODE: numerical_cat,
        OdaSchema.PBA_CODE: numerical_cat,
        OdaSchema.INVESTMENT_PROJECT: numerical_cat,
        OdaSchema.ASSOC_FINANCE: numerical_cat,
        OdaSchema.BIODIVERSITY: numerical_cat,
        OdaSchema.DESERTIFICATION: numerical_cat,
        OdaSchema.COMMITMENT_DATE: "datetime64[ns]",
        OdaSchema.TYPE_REPAYMENT: numerical_cat,
        OdaSchema.NUMBER_REPAYMENT: numerical_cat,
        OdaSchema.INTEREST1: category,
        OdaSchema.INTEREST2: category,
        OdaSchema.REPAYDATE1: "datetime64[ns]",
        OdaSchema.REPAYDATE2: "datetime64[ns]",
        OdaSchema.USD_INTEREST: value,
        OdaSchema.USD_OUTSTANDING: value,
        OdaSchema.USD_ARREARS_PRINCIPAL:value,
        OdaSchema.USD_ARREARS_INTEREST: value,
        OdaSchema.CAPITAL_EXPEND: value,
        OdaSchema.PSI_FLAG: numerical_cat,
        OdaSchema.PSI_ADD_TYPE: category,
        OdaSchema.PSI_ADD_ASSESS: category,
        OdaSchema.PSI_ADD_DEV_OBJECTIVE: category,
        OdaSchema.PRICES: category,
        OdaSchema.CURRENCY: category,
        OdaSchema.AIDTYPE_CODE: "int32[pyarrow]",
        OdaSchema.FLOWS_CODE: "int32[pyarrow]",
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

from collections import defaultdict

import pandas as pd

from oda_data.clean_data.schema import OdaSchema


def schema_types() -> dict:
    """
    Returns a dictionary of schema types. Invalid columns are assumed to be strings.
    By default, pyarrow types are used for all columns.

    Returns:
        dict: A dictionary mapping attribute names to their corresponding data types.

    """
    types = {
        OdaSchema.YEAR: "int16[pyarrow]",
        OdaSchema.PROVIDER_CODE: "int16[pyarrow]",
        OdaSchema.PROVIDER_NAME: "string[pyarrow]",
        OdaSchema.PROVIDER_TYPE: "string[pyarrow]",
        OdaSchema.PROVIDER_DETAILED: "string[pyarrow]",
        OdaSchema.AGENCY_CODE: "int16[pyarrow]",
        OdaSchema.AGENCY_NAME: "string[pyarrow]",
        OdaSchema.CRS_ID: "string[pyarrow]",
        OdaSchema.PROJECT_ID: "string[pyarrow]",
        OdaSchema.RECIPIENT_CODE: "int16[pyarrow]",
        OdaSchema.RECIPIENT_NAME: "string[pyarrow]",
        OdaSchema.RECIPIENT_REGION: "string[pyarrow]",
        OdaSchema.RECIPIENT_REGION_CODE: "int16[pyarrow]",
        OdaSchema.RECIPIENT_INCOME: "string[pyarrow]",
        OdaSchema.RECIPIENT_INCOME_CODE: "int16[pyarrow]",
        OdaSchema.FLOW_MODALITY: "string[pyarrow]",
        OdaSchema.ALLOCABLE_SHARE: "float64[pyarrow]",
        OdaSchema.CONCESSIONALITY: "string[pyarrow]",
        OdaSchema.FINANCIAL_INSTRUMENT: "string[pyarrow]",
        OdaSchema.FLOW_TYPE: "string[pyarrow]",
        OdaSchema.FINANCE_TYPE: "int16[pyarrow]",
        OdaSchema.CHANNEL_NAME_DELIVERY: "string[pyarrow]",
        OdaSchema.CHANNEL_CODE_DELIVERY: "int32[pyarrow]",
        OdaSchema.CHANNEL_CODE: "int32[pyarrow]",
        OdaSchema.CHANNEL_NAME: "string[pyarrow]",
        OdaSchema.ADAPTATION: "int16[pyarrow]",
        OdaSchema.MITIGATION: "int16[pyarrow]",
        OdaSchema.CROSS_CUTTING: "int16[pyarrow]",
        OdaSchema.ADAPTATION_VALUE: "float64[pyarrow]",
        OdaSchema.MITIGATION_VALUE: "float64[pyarrow]",
        OdaSchema.CROSS_CUTTING_VALUE: "float64[pyarrow]",
        OdaSchema.CLIMATE_OBJECTIVE: "string[pyarrow]",
        OdaSchema.CLIMATE_FINANCE_VALUE: "float64[pyarrow]",
        OdaSchema.COMMITMENT_CLIMATE_SHARE: "float64[pyarrow]",
        OdaSchema.NOT_CLIMATE: "float64[pyarrow]",
        OdaSchema.CLIMATE_UNSPECIFIED: "float64[pyarrow]",
        OdaSchema.CLIMATE_UNSPECIFIED_SHARE: "float64[pyarrow]",
        OdaSchema.PURPOSE_CODE: "int32[pyarrow]",
        OdaSchema.PURPOSE_NAME: "string[pyarrow]",
        OdaSchema.SECTOR_CODE: "int32[pyarrow]",
        OdaSchema.SECTOR_NAME: "string[pyarrow]",
        OdaSchema.PROJECT_TITLE: "string[pyarrow]",
        OdaSchema.PROJECT_DESCRIPTION: "string[pyarrow]",
        OdaSchema.PROJECT_DESCRIPTION_SHORT: "string[pyarrow]",
        OdaSchema.GENDER: "string[pyarrow]",
        OdaSchema.INDICATOR: "string[pyarrow]",
        OdaSchema.VALUE: "float64[pyarrow]",
        OdaSchema.AMOUNT: "float64[pyarrow]",
        OdaSchema.TOTAL_VALUE: "float64[pyarrow]",
        OdaSchema.SHARE: "float64[pyarrow]",
        OdaSchema.CLIMATE_SHARE: "float64[pyarrow]",
        OdaSchema.CLIMATE_SHARE_ROLLING: "float64[pyarrow]",
        OdaSchema.FLOW_CODE: "int32[pyarrow]",
        OdaSchema.FLOW_NAME: "string[pyarrow]",
        OdaSchema.BI_MULTI: "Int16[pyarrow]",
        OdaSchema.CATEGORY: "int16[pyarrow]",
        OdaSchema.USD_COMMITMENT: "float64[pyarrow]",
        OdaSchema.USD_DISBURSEMENT: "float64[pyarrow]",
        OdaSchema.USD_RECEIVED: "float64[pyarrow]",
        OdaSchema.USD_GRANT_EQUIV: "float64[pyarrow]",
        OdaSchema.USD_NET_DISBURSEMENT: "float64[pyarrow]",
        OdaSchema.REPORTING_METHOD: "string[pyarrow]",
        OdaSchema.MULTILATERAL_TYPE: "string[pyarrow]",
        OdaSchema.CONVERGED_REPORTING: "string[pyarrow]",
        OdaSchema.COAL_FINANCING: "string[pyarrow]",
        OdaSchema.LEVEL: "string[pyarrow]",
        OdaSchema.USD_COMMITMENT_DEFL: "float64[pyarrow]",
        OdaSchema.USD_DISBURSEMENT_DEFL: "float64[pyarrow]",
        OdaSchema.USD_RECEIVED_DEFL: "float64[pyarrow]",
        OdaSchema.USD_ADJUSTMENT: "float64[pyarrow]",
        OdaSchema.USD_ADJUSTMENT_DEFL: "float64[pyarrow]",
        OdaSchema.USD_AMOUNT_UNTIED: "float64[pyarrow]",
        OdaSchema.USD_AMOUNT_UNTIED_DEFL: "float64[pyarrow]",
        OdaSchema.USD_AMOUNT_TIED: "float64[pyarrow]",
        OdaSchema.USD_AMOUNT_TIED_DEFL: "float64[pyarrow]",
        OdaSchema.USD_AMOUNT_PARTIAL_TIED: "float64[pyarrow]",
        OdaSchema.USD_AMOUNT_PARTIAL_TIED_DEFL: "float64[pyarrow]",
        OdaSchema.USD_IRTC_CODE: "string[pyarrow]",
        OdaSchema.USD_EXPERT_COMMITMENT: "float64[pyarrow]",
        OdaSchema.USD_EXPERT_EXTENDED: "float64[pyarrow]",
        OdaSchema.USD_EXPORT_CREDIT: "float64[pyarrow]",
        OdaSchema.COMMITMENT_NATIONAL: "float64[pyarrow]",
        OdaSchema.DISBURSEMENT_NATIONAL: "float64[pyarrow]",
        OdaSchema.GRANT_EQUIV_NATIONAL: "float64[pyarrow]",
        OdaSchema.PARENT_CHANNEL_CODE: "int32[pyarrow]",
        OdaSchema.LDC_FLAG_CODE: "int16[pyarrow]",
        OdaSchema.LDC_FLAG: "string[pyarrow]",
        OdaSchema.EXPECTED_START_DATE: "datetime64[ns]",
        OdaSchema.COMPLETION_DATE: "datetime64[ns]",
        OdaSchema.ENVIRONMENT: "int16[pyarrow]",
        OdaSchema.DIG_CODE: "int16[pyarrow]",
        OdaSchema.TRADE: "string[pyarrow]",
        OdaSchema.RMNCH_CODE: "int16[pyarrow]",
        OdaSchema.DRR_CODE: "int16[pyarrow]",
        OdaSchema.NUTRITION: "int16[pyarrow]",
        OdaSchema.DISABILITY: "int16[pyarrow]",
        OdaSchema.FTC_CODE: "int16[pyarrow]",
        OdaSchema.PBA_CODE: "int16[pyarrow]",
        OdaSchema.INVESTMENT_PROJECT: "int16[pyarrow]",
        OdaSchema.ASSOC_FINANCE: "int16[pyarrow]",
        OdaSchema.BIODIVERSITY: "int16[pyarrow]",
        OdaSchema.DESERTIFICATION: "int16[pyarrow]",
        OdaSchema.COMMITMENT_DATE: "datetime64[ns]",
        OdaSchema.TYPE_REPAYMENT: "int16[pyarrow]",
        OdaSchema.NUMBER_REPAYMENT: "int16[pyarrow]",
        OdaSchema.INTEREST1: "string[pyarrow]",
        OdaSchema.INTEREST2: "string[pyarrow]",
        OdaSchema.REPAYDATE1: "datetime64[ns]",
        OdaSchema.REPAYDATE2: "datetime64[ns]",
        OdaSchema.USD_INTEREST: "float64[pyarrow]",
        OdaSchema.USD_OUTSTANDING: "float64[pyarrow]",
        OdaSchema.USD_ARREARS_PRINCIPAL: "float64[pyarrow]",
        OdaSchema.USD_ARREARS_INTEREST: "float64[pyarrow]",
        OdaSchema.CAPITAL_EXPEND: "float64[pyarrow]",
        OdaSchema.PSI_FLAG: "int16[pyarrow]",
        OdaSchema.PSI_ADD_TYPE: "string[pyarrow]",
        OdaSchema.PSI_ADD_ASSESS: "string[pyarrow]",
        OdaSchema.PSI_ADD_DEV_OBJECTIVE: "string[pyarrow]",
        OdaSchema.PRICES: "string[pyarrow]",
        OdaSchema.CURRENCY: "string[pyarrow]",
        OdaSchema.AIDTYPE_CODE: "int32[pyarrow]",
        OdaSchema.FLOWS_CODE: "int32[pyarrow]",
    }

    return defaultdict(lambda: "string[pyarrow]", types)


def set_default_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Set the types of the columns in the dataframe.

    Args:
        df (pd.DataFrame): The input dataframe.

    Returns:
        pd.DataFrame: The dataframe with the types set.

    """
    default_types = schema_types()

    converted_types = {c: default_types[c] for c in df.columns}

    return df.astype(converted_types)

from dataclasses import dataclass


@dataclass
class OdaSchema:
    YEAR: str = "year"
    PROVIDER_CODE: str = "donor_code"
    PROVIDER_NAME: str = "donor_name"
    PROVIDER_TYPE: str = "donor_type"
    PROVIDER_DETAILED: str = "provider_detailed"
    AGENCY_CODE: str = "agency_code"
    AGENCY_NAME: str = "agency_name"
    CRS_ID: str = "crs_id"
    PROJECT_ID: str = "project_id"
    RECIPIENT_CODE: str = "recipient_code"
    RECIPIENT_NAME: str = "recipient_name"
    RECIPIENT_REGION: str = "recipient_region"
    RECIPIENT_REGION_CODE: str = "recipient_region_code"
    RECIPIENT_INCOME: str = "recipient_income"
    RECIPIENT_INCOME_CODE: str = "recipient_income_code"
    FLOW_MODALITY: str = "modality"
    ALLOCABLE_SHARE: str = "allocable_share"
    CONCESSIONALITY: str = "concessionality"
    FINANCIAL_INSTRUMENT: str = "financial_instrument"
    FLOW_TYPE: str = "flow_type"
    FINANCE_TYPE: str = "type_of_finance"
    CHANNEL_NAME_DELIVERY: str = "channel_name_delivery"
    CHANNEL_CODE_DELIVERY: str = "channel_code_delivery"
    CHANNEL_CODE: str = "channel_code"
    CHANNEL_NAME: str = "channel_name"
    ADAPTATION: str = "climate_adaptation"
    MITIGATION: str = "climate_mitigation"
    CROSS_CUTTING: str = "climate_cross_cutting"
    ADAPTATION_VALUE: str = "climate_adaptation_value"
    MITIGATION_VALUE: str = "climate_mitigation_value"
    CROSS_CUTTING_VALUE: str = "overlap_commitment_current"
    CLIMATE_OBJECTIVE: str = "climate_objective"
    CLIMATE_FINANCE_VALUE: str = "climate_finance_value"
    COMMITMENT_CLIMATE_SHARE: str = "commitment_climate_share"
    NOT_CLIMATE: str = "not_climate_relevant"
    CLIMATE_UNSPECIFIED: str = "climate_total"
    CLIMATE_UNSPECIFIED_SHARE: str = "climate_total_share"
    PURPOSE_CODE: str = "purpose_code"
    SECTOR_CODE: str = "sector_code"
    PURPOSE_NAME: str = "purpose_name"
    SECTOR_NAME: str = "sector_name"
    PROJECT_TITLE: str = "project_title"
    PROJECT_DESCRIPTION: str = "description"
    PROJECT_DESCRIPTION_SHORT: str = "short_description"
    GENDER: str = "gender"
    INDICATOR: str = "indicator"
    VALUE: str = "value"
    TOTAL_VALUE: str = "total_value"
    SHARE: str = "share"
    CLIMATE_SHARE: str = "climate_share"
    CLIMATE_SHARE_ROLLING: str = "climate_share_rolling"
    FLOW_CODE: str = "flow_code"
    FLOW_NAME: str = "flow_name"
    BI_MULTI: str = "bi_multi"
    CATEGORY: str = "category"
    USD_COMMITMENT: str = "usd_commitment"
    USD_DISBURSEMENT: str = "usd_disbursement"
    USD_RECEIVED: str = "usd_received"
    USD_GRANT_EQUIV: str = "usd_grant_equiv"
    USD_NET_DISBURSEMENT: str = "usd_net_disbursement"
    REPORTING_METHOD: str = "reporting_method"
    MULTILATERAL_TYPE: str = "multilateral_type"
    CONVERGED_REPORTING: str = "converged_reporting"
    COAL_FINANCING: str = "coal_related_financing"
    LEVEL: str = "level"
    USD_COMMITMENT_DEFL: str = "usd_commitment_constant"
    USD_DISBURSEMENT_DEFL: str = "usd_disbursement_constant"
    USD_RECEIVED_DEFL: str = "usd_received_constant"
    USD_ADJUSTMENT: str = "usd_adjustment"
    USD_ADJUSTMENT_DEFL: str = "usd_adjustment_constant"
    USD_AMOUNT_UNTIED: str = "usd_amount_untied"
    USD_AMOUNT_UNTIED_DEFL: str = "usd_amount_untied_constant"
    USD_AMOUNT_TIED: str = "usd_amount_tied"
    USD_AMOUNT_TIED_DEFL: str = "usd_amount_tied_constant"
    USD_AMOUNT_PARTIAL_TIED: str = "usd_amount_partial_tied"
    USD_AMOUNT_PARTIAL_TIED_DEFL: str = "usd_amount_partial_tied_constant"
    USD_IRTC_CODE: str = "usd_irtc_code"
    USD_EXPERT_COMMITMENT: str = "usd_expert_commitment"
    USD_EXPERT_EXTENDED: str = "usd_expert_extended"
    USD_EXPORT_CREDIT: str = "usd_export_credit"
    COMMITMENT_NATIONAL: str = "commitment_national"
    DISBURSEMENT_NATIONAL: str = "disbursement_national"
    GRANT_EQUIV_NATIONAL: str = "grant_equiv_national"
    PARENT_CHANNEL_CODE: str = "parent_channel_code"
    LDC_FLAG_CODE: str = "ldcflag_code"
    LDC_FLAG: str = "ldcflag_name"
    EXPECTED_START_DATE: str = "expected_start_date"
    COMPLETION_DATE: str = "completion_date"
    ENVIRONMENT: str = "environment"
    DIG_CODE: str = "dig_code"
    TRADE: str = "trade"
    RMNCH_CODE: str = "rmnch_code"
    DRR_CODE: str = "drr_code"
    NUTRITION: str = "nutrition"
    DISABILITY: str = "disability"
    FTC_CODE: str = "ftc_code"
    PBA_CODE: str = "pba_code"
    INVESTMENT_PROJECT: str = "investment_project"
    ASSOC_FINANCE: str = "assoc_finance"
    BIODIVERSITY: str = "biodiversity"
    DESERTIFICATION: str = "desertification"
    COMMITMENT_DATE: str = "commitment_date"
    TYPE_REPAYMENT: str = "type_repayment"
    NUMBER_REPAYMENT: str = "number_repayment"
    INTEREST1: str = "interest1"
    INTEREST2: str = "interest2"
    REPAYDATE1: str = "repaydate1"
    REPAYDATE2: str = "repaydate2"
    USD_INTEREST: str = "usd_interest"
    USD_OUTSTANDING: str = "usd_outstanding"
    USD_ARREARS_PRINCIPAL: str = "usd_arrears_principal"
    USD_ARREARS_INTEREST: str = "usd_arrears_interest"
    CAPITAL_EXPEND: str = "capital_expend"
    PSI_FLAG: str = "psiflag"
    SDG_FOCUS: str = "sdgfocus"
    PSI_ADD_TYPE: str = "psiadd_type"
    PSI_ADD_ASSESS: str = "psiadd_assess"
    PSI_ADD_DEV_OBJECTIVE: str = "psiadd_dev_obj"
    CURRENCY: str = "currency"
    PRICES: str = "prices"
    AMOUNT: str = "amount"
    AIDTYPE_CODE: str = "aidtype_code"
    FLOWS_CODE: str = "flows_code"


CRS_MAPPING: dict[str, str] = {
    "donor_code": OdaSchema.PROVIDER_CODE,
    "donor_name": OdaSchema.PROVIDER_NAME,
    "provider": OdaSchema.PROVIDER_NAME,
    "provider_type": OdaSchema.PROVIDER_TYPE,
    "provider_detailed": OdaSchema.PROVIDER_DETAILED,
    "provider_code": OdaSchema.PROVIDER_CODE,
    "agency_code": OdaSchema.AGENCY_CODE,
    "agency_name": OdaSchema.AGENCY_NAME,
    "extending_agency": OdaSchema.AGENCY_NAME,
    "crs_identification_n": OdaSchema.CRS_ID,
    "donor_project_n": OdaSchema.PROJECT_ID,
    "project_number": OdaSchema.PROJECT_ID,
    "recipient_code": OdaSchema.RECIPIENT_CODE,
    "recipient_name": OdaSchema.RECIPIENT_NAME,
    "region_name": OdaSchema.RECIPIENT_REGION,
    "region_code": OdaSchema.RECIPIENT_REGION_CODE,
    "incomegoup_name": OdaSchema.RECIPIENT_INCOME,
    "incomegroup_code": OdaSchema.RECIPIENT_INCOME_CODE,
    "recipient_income_group_oecd_classification": OdaSchema.RECIPIENT_INCOME,
    "development_cooperation_modality": OdaSchema.FLOW_MODALITY,
    "aid_t": OdaSchema.FLOW_MODALITY,
    "financial_instrument": OdaSchema.FINANCIAL_INSTRUMENT,
    "type_of_finance": OdaSchema.FINANCE_TYPE,
    "finance_t": OdaSchema.FINANCE_TYPE,
    "channel_of_delivery": OdaSchema.CHANNEL_NAME_DELIVERY,
    "channel_of_delivery_code": OdaSchema.CHANNEL_CODE_DELIVERY,
    "channel_code": OdaSchema.CHANNEL_CODE,
    "channel_name": OdaSchema.CHANNEL_NAME,
    "adaptation_objective_applies_to_rio_marked_data_only": OdaSchema.ADAPTATION,
    "mitigation_objective_applies_to_rio_marked_data_only": OdaSchema.MITIGATION,
    "adaptation_related_development_finance_commitment_current": OdaSchema.ADAPTATION_VALUE,
    "mitigation_related_development_finance_commitment_current": OdaSchema.MITIGATION_VALUE,
    "climate_objective_applies_to_rio_marked_data_only_or_climate_component": OdaSchema.CLIMATE_OBJECTIVE,
    "climate_related_development_finance_commitment_current": OdaSchema.CLIMATE_FINANCE_VALUE,
    "share_of_the_underlying_commitment_when_available": OdaSchema.COMMITMENT_CLIMATE_SHARE,
    "overlap_commitment_current": OdaSchema.CROSS_CUTTING_VALUE,
    "sector_detailed": OdaSchema.SECTOR_NAME,
    "sub_sector": OdaSchema.PURPOSE_NAME,
    "purpose_code": OdaSchema.PURPOSE_CODE,
    "sector_code": OdaSchema.SECTOR_CODE,
    "project_title": OdaSchema.PROJECT_TITLE,
    "long_description": OdaSchema.PROJECT_DESCRIPTION,
    "short_description": OdaSchema.PROJECT_DESCRIPTION_SHORT,
    "flow_code": OdaSchema.FLOW_CODE,
    "flow_name": OdaSchema.FLOW_NAME,
    "bi_multi": OdaSchema.BI_MULTI,
    "gender": OdaSchema.GENDER,
    "category": OdaSchema.CATEGORY,
    "usd_commitment": OdaSchema.USD_COMMITMENT,
    "usd_disbursement": OdaSchema.USD_DISBURSEMENT,
    "usd_received": OdaSchema.USD_RECEIVED,
    "usd_grant_equiv": OdaSchema.USD_GRANT_EQUIV,
    "usd_net_disbursement": OdaSchema.USD_NET_DISBURSEMENT,
    "usd_commitment_defl": OdaSchema.USD_COMMITMENT_DEFL,
    "usd_disbursement_defl": OdaSchema.USD_DISBURSEMENT_DEFL,
    "usd_received_defl": OdaSchema.USD_RECEIVED_DEFL,
    "usd_adjustment": OdaSchema.USD_ADJUSTMENT,
    "usd_adjustment_defl": OdaSchema.USD_ADJUSTMENT_DEFL,
    "usd_amount_untied": OdaSchema.USD_AMOUNT_UNTIED,
    "usd_amount_untied_defl": OdaSchema.USD_AMOUNT_UNTIED_DEFL,
    "usd_amount_tied": OdaSchema.USD_AMOUNT_TIED,
    "usd_amount_tied_defl": OdaSchema.USD_AMOUNT_TIED_DEFL,
    "usd_amount_partial_tied": OdaSchema.USD_AMOUNT_PARTIAL_TIED,
    "usd_amount_partial_tied_defl": OdaSchema.USD_AMOUNT_PARTIAL_TIED_DEFL,
    "usd_irtc_code": OdaSchema.USD_IRTC_CODE,
    "usd_expert_commitment": OdaSchema.USD_EXPERT_COMMITMENT,
    "usd_expert_extended": OdaSchema.USD_EXPERT_EXTENDED,
    "usd_export_credit": OdaSchema.USD_EXPORT_CREDIT,
    "commitment_national": OdaSchema.COMMITMENT_NATIONAL,
    "disbursement_national": OdaSchema.DISBURSEMENT_NATIONAL,
    "grant_equiv": OdaSchema.GRANT_EQUIV_NATIONAL,
    "parent_channel_code": OdaSchema.PARENT_CHANNEL_CODE,
    "ld_cflag": OdaSchema.LDC_FLAG_CODE,
    "ldcflag": OdaSchema.LDC_FLAG_CODE,
    "ld_cflag_name": OdaSchema.LDC_FLAG,
    "ldcflag_name": OdaSchema.LDC_FLAG,
    "sdg_focus": OdaSchema.SDG_FOCUS,
    "sdgfocus": OdaSchema.SDG_FOCUS,
    "sd_gfocus": OdaSchema.SDG_FOCUS,
    "expected_start_date": OdaSchema.EXPECTED_START_DATE,
    "completion_date": OdaSchema.COMPLETION_DATE,
    "environment": OdaSchema.ENVIRONMENT,
    "dig_code": OdaSchema.DIG_CODE,
    "dig": OdaSchema.DIG_CODE,
    "trade": OdaSchema.TRADE,
    "rmnch": OdaSchema.RMNCH_CODE,
    "drr": OdaSchema.DRR_CODE,
    "rmnch_code": OdaSchema.RMNCH_CODE,
    "drr_code": OdaSchema.DRR_CODE,
    "nutrition": OdaSchema.NUTRITION,
    "disability": OdaSchema.DISABILITY,
    "ftc": OdaSchema.FTC_CODE,
    "ftc_code": OdaSchema.FTC_CODE,
    "pba": OdaSchema.PBA_CODE,
    "pba_code": OdaSchema.PBA_CODE,
    "investment_project": OdaSchema.INVESTMENT_PROJECT,
    "assoc_finance": OdaSchema.ASSOC_FINANCE,
    "biodiversity": OdaSchema.BIODIVERSITY,
    "desertification": OdaSchema.DESERTIFICATION,
    "commitment_date": OdaSchema.COMMITMENT_DATE,
    "type_repayment": OdaSchema.TYPE_REPAYMENT,
    "number_repayment": OdaSchema.NUMBER_REPAYMENT,
    "interest1": OdaSchema.INTEREST1,
    "interest2": OdaSchema.INTEREST2,
    "repaydate1": OdaSchema.REPAYDATE1,
    "repaydate2": OdaSchema.REPAYDATE2,
    "usd_interest": OdaSchema.USD_INTEREST,
    "usd_outstanding": OdaSchema.USD_OUTSTANDING,
    "usd_arrears_principal": OdaSchema.USD_ARREARS_PRINCIPAL,
    "usd_arrears_interest": OdaSchema.USD_ARREARS_INTEREST,
    "capital_expend": OdaSchema.CAPITAL_EXPEND,
    "ps_iflag": OdaSchema.PSI_FLAG,
    "psiflag": OdaSchema.PSI_FLAG,
    "psi_flag": OdaSchema.PSI_FLAG,
    "ps_iadd_type": OdaSchema.PSI_ADD_TYPE,
    "psiadd_type": OdaSchema.PSI_ADD_TYPE,
    "psi_add_type": OdaSchema.PSI_ADD_TYPE,
    "ps_iadd_assess": OdaSchema.PSI_ADD_ASSESS,
    "psiadd_assess": OdaSchema.PSI_ADD_ASSESS,
    "psi_add_assess": OdaSchema.PSI_ADD_ASSESS,
    "ps_iadd_dev_obj": OdaSchema.PSI_ADD_DEV_OBJECTIVE,
    "psiadd_dev_obj": OdaSchema.PSI_ADD_DEV_OBJECTIVE,
    "psi_add_dev_obj": OdaSchema.PSI_ADD_DEV_OBJECTIVE,
    "oecd_climate_total": OdaSchema.CLIMATE_UNSPECIFIED,
    "reporting_type": OdaSchema.REPORTING_METHOD,
    "type": OdaSchema.MULTILATERAL_TYPE,
    "converged_reporting": OdaSchema.CONVERGED_REPORTING,
    "coal_related_financing": OdaSchema.COAL_FINANCING,
    "flow_type": OdaSchema.FLOW_TYPE,
    "type_of_flow": OdaSchema.FLOW_TYPE,
    "aidtype_code": OdaSchema.AIDTYPE_CODE,
    "flows_code": OdaSchema.FLOWS_CODE,
}

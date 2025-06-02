from dataclasses import dataclass


@dataclass
class ODASchema:
    YEAR: str = "year"
    PROVIDER_CODE: str = "donor_code"
    PROVIDER_CODE_DE: str = "de_donorcode"
    PROVIDER_NAME: str = "donor_name"
    PROVIDER_TYPE: str = "donor_type"
    PROVIDER_DETAILED: str = "provider_detailed"
    AGENCY_CODE: str = "agency_code"
    AGENCY_NAME: str = "agency_name"
    CRS_ID: str = "crs_id"
    PROJECT_ID: str = "project_id"
    RECIPIENT_CODE: str = "recipient_code"
    RECIPIENT_CODE_DE: str = "de_recipientcode"
    RECIPIENT_NAME: str = "recipient_name"
    RECIPIENT_REGION: str = "recipient_region"
    RECIPIENT_REGION_CODE: str = "recipient_region_code"
    RECIPIENT_REGION_CODE_DE: str = "de_regioncode"
    RECIPIENT_INCOME: str = "incomegroup_name"
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
    AIDTYPE_NAME: str = "aid_type"
    FLOWS_CODE: str = "flows_code"
    FUND_FLOWS: str = "fund_flows"
    AID_TO_THRU: str = "aid_to_or_thru"
    AMOUNT_TYPE_CODE: str = "amounttype_code"
    MEASURE: str = "measure"


CRS_MAPPING: dict[str, str] = {
    "donor_code": ODASchema.PROVIDER_CODE,
    "donor_name": ODASchema.PROVIDER_NAME,
    "provider": ODASchema.PROVIDER_NAME,
    "provider_type": ODASchema.PROVIDER_TYPE,
    "provider_detailed": ODASchema.PROVIDER_DETAILED,
    "provider_code": ODASchema.PROVIDER_CODE,
    "agency_code": ODASchema.AGENCY_CODE,
    "agency_name": ODASchema.AGENCY_NAME,
    "extending_agency": ODASchema.AGENCY_NAME,
    "crs_identification_n": ODASchema.CRS_ID,
    "donor_project_n": ODASchema.PROJECT_ID,
    "project_number": ODASchema.PROJECT_ID,
    "recipient_code": ODASchema.RECIPIENT_CODE,
    "recipient_name": ODASchema.RECIPIENT_NAME,
    "region_name": ODASchema.RECIPIENT_REGION,
    "region_code": ODASchema.RECIPIENT_REGION_CODE,
    "incomegoup_name": ODASchema.RECIPIENT_INCOME,
    "incomegroup_code": ODASchema.RECIPIENT_INCOME_CODE,
    "recipient_income_group_oecd_classification": ODASchema.RECIPIENT_INCOME,
    "development_cooperation_modality": ODASchema.FLOW_MODALITY,
    "aid_t": ODASchema.FLOW_MODALITY,
    "financial_instrument": ODASchema.FINANCIAL_INSTRUMENT,
    "type_of_finance": ODASchema.FINANCE_TYPE,
    "finance_t": ODASchema.FINANCE_TYPE,
    "channel_of_delivery": ODASchema.CHANNEL_NAME_DELIVERY,
    "channel_of_delivery_code": ODASchema.CHANNEL_CODE_DELIVERY,
    "channel_code": ODASchema.CHANNEL_CODE,
    "channel_name": ODASchema.CHANNEL_NAME,
    "adaptation_objective_applies_to_rio_marked_data_only": ODASchema.ADAPTATION,
    "mitigation_objective_applies_to_rio_marked_data_only": ODASchema.MITIGATION,
    "adaptation_related_development_finance_commitment_current": ODASchema.ADAPTATION_VALUE,
    "mitigation_related_development_finance_commitment_current": ODASchema.MITIGATION_VALUE,
    "climate_objective_applies_to_rio_marked_data_only_or_climate_component": ODASchema.CLIMATE_OBJECTIVE,
    "climate_related_development_finance_commitment_current": ODASchema.CLIMATE_FINANCE_VALUE,
    "share_of_the_underlying_commitment_when_available": ODASchema.COMMITMENT_CLIMATE_SHARE,
    "overlap_commitment_current": ODASchema.CROSS_CUTTING_VALUE,
    "sector_detailed": ODASchema.SECTOR_NAME,
    "sub_sector": ODASchema.PURPOSE_NAME,
    "purpose_code": ODASchema.PURPOSE_CODE,
    "sector_code": ODASchema.SECTOR_CODE,
    "project_title": ODASchema.PROJECT_TITLE,
    "long_description": ODASchema.PROJECT_DESCRIPTION,
    "short_description": ODASchema.PROJECT_DESCRIPTION_SHORT,
    "flow_code": ODASchema.FLOW_CODE,
    "flow_name": ODASchema.FLOW_NAME,
    "bi_multi": ODASchema.BI_MULTI,
    "gender": ODASchema.GENDER,
    "category": ODASchema.CATEGORY,
    "usd_commitment": ODASchema.USD_COMMITMENT,
    "usd_disbursement": ODASchema.USD_DISBURSEMENT,
    "usd_received": ODASchema.USD_RECEIVED,
    "usd_grant_equiv": ODASchema.USD_GRANT_EQUIV,
    "usd_net_disbursement": ODASchema.USD_NET_DISBURSEMENT,
    "usd_commitment_defl": ODASchema.USD_COMMITMENT_DEFL,
    "usd_disbursement_defl": ODASchema.USD_DISBURSEMENT_DEFL,
    "usd_received_defl": ODASchema.USD_RECEIVED_DEFL,
    "usd_adjustment": ODASchema.USD_ADJUSTMENT,
    "usd_adjustment_defl": ODASchema.USD_ADJUSTMENT_DEFL,
    "usd_amount_untied": ODASchema.USD_AMOUNT_UNTIED,
    "usd_amount_untied_defl": ODASchema.USD_AMOUNT_UNTIED_DEFL,
    "usd_amount_tied": ODASchema.USD_AMOUNT_TIED,
    "usd_amount_tied_defl": ODASchema.USD_AMOUNT_TIED_DEFL,
    "usd_amount_partial_tied": ODASchema.USD_AMOUNT_PARTIAL_TIED,
    "usd_amount_partial_tied_defl": ODASchema.USD_AMOUNT_PARTIAL_TIED_DEFL,
    "usd_irtc_code": ODASchema.USD_IRTC_CODE,
    "usd_expert_commitment": ODASchema.USD_EXPERT_COMMITMENT,
    "usd_expert_extended": ODASchema.USD_EXPERT_EXTENDED,
    "usd_export_credit": ODASchema.USD_EXPORT_CREDIT,
    "commitment_national": ODASchema.COMMITMENT_NATIONAL,
    "disbursement_national": ODASchema.DISBURSEMENT_NATIONAL,
    "grant_equiv": ODASchema.GRANT_EQUIV_NATIONAL,
    "parent_channel_code": ODASchema.PARENT_CHANNEL_CODE,
    "ld_cflag": ODASchema.LDC_FLAG_CODE,
    "ldcflag": ODASchema.LDC_FLAG_CODE,
    "ld_cflag_name": ODASchema.LDC_FLAG,
    "ldcflag_name": ODASchema.LDC_FLAG,
    "sdg_focus": ODASchema.SDG_FOCUS,
    "sdgfocus": ODASchema.SDG_FOCUS,
    "sd_gfocus": ODASchema.SDG_FOCUS,
    "expected_start_date": ODASchema.EXPECTED_START_DATE,
    "completion_date": ODASchema.COMPLETION_DATE,
    "environment": ODASchema.ENVIRONMENT,
    "dig_code": ODASchema.DIG_CODE,
    "dig": ODASchema.DIG_CODE,
    "trade": ODASchema.TRADE,
    "rmnch": ODASchema.RMNCH_CODE,
    "drr": ODASchema.DRR_CODE,
    "rmnch_code": ODASchema.RMNCH_CODE,
    "drr_code": ODASchema.DRR_CODE,
    "nutrition": ODASchema.NUTRITION,
    "disability": ODASchema.DISABILITY,
    "ftc": ODASchema.FTC_CODE,
    "ftc_code": ODASchema.FTC_CODE,
    "pba": ODASchema.PBA_CODE,
    "pba_code": ODASchema.PBA_CODE,
    "investment_project": ODASchema.INVESTMENT_PROJECT,
    "assoc_finance": ODASchema.ASSOC_FINANCE,
    "biodiversity": ODASchema.BIODIVERSITY,
    "desertification": ODASchema.DESERTIFICATION,
    "commitment_date": ODASchema.COMMITMENT_DATE,
    "type_repayment": ODASchema.TYPE_REPAYMENT,
    "number_repayment": ODASchema.NUMBER_REPAYMENT,
    "interest1": ODASchema.INTEREST1,
    "interest2": ODASchema.INTEREST2,
    "repaydate1": ODASchema.REPAYDATE1,
    "repaydate2": ODASchema.REPAYDATE2,
    "usd_interest": ODASchema.USD_INTEREST,
    "usd_outstanding": ODASchema.USD_OUTSTANDING,
    "usd_arrears_principal": ODASchema.USD_ARREARS_PRINCIPAL,
    "usd_arrears_interest": ODASchema.USD_ARREARS_INTEREST,
    "capital_expend": ODASchema.CAPITAL_EXPEND,
    "ps_iflag": ODASchema.PSI_FLAG,
    "psiflag": ODASchema.PSI_FLAG,
    "psi_flag": ODASchema.PSI_FLAG,
    "ps_iadd_type": ODASchema.PSI_ADD_TYPE,
    "psiadd_type": ODASchema.PSI_ADD_TYPE,
    "psi_add_type": ODASchema.PSI_ADD_TYPE,
    "ps_iadd_assess": ODASchema.PSI_ADD_ASSESS,
    "psiadd_assess": ODASchema.PSI_ADD_ASSESS,
    "psi_add_assess": ODASchema.PSI_ADD_ASSESS,
    "ps_iadd_dev_obj": ODASchema.PSI_ADD_DEV_OBJECTIVE,
    "psiadd_dev_obj": ODASchema.PSI_ADD_DEV_OBJECTIVE,
    "psi_add_dev_obj": ODASchema.PSI_ADD_DEV_OBJECTIVE,
    "oecd_climate_total": ODASchema.CLIMATE_UNSPECIFIED,
    "reporting_type": ODASchema.REPORTING_METHOD,
    "type": ODASchema.MULTILATERAL_TYPE,
    "converged_reporting": ODASchema.CONVERGED_REPORTING,
    "coal_related_financing": ODASchema.COAL_FINANCING,
    "flow_type": ODASchema.FLOW_TYPE,
    "type_of_flow": ODASchema.FLOW_TYPE,
    "aidtype_code": ODASchema.AIDTYPE_CODE,
    "aid_type": ODASchema.AIDTYPE_NAME,
    "flows_code": ODASchema.FLOWS_CODE,
    "fund_flows": ODASchema.FUND_FLOWS,
    "amounttype_code": ODASchema.AMOUNT_TYPE_CODE,
    "de_donorcode": ODASchema.PROVIDER_CODE_DE,
    "de_recipientcode": ODASchema.RECIPIENT_CODE_DE,
    "de_regioncode": ODASchema.RECIPIENT_REGION_CODE_DE,
    "incomegroup_name": ODASchema.RECIPIENT_INCOME,
}


@dataclass
class AidDataSchema:
    AIDDATA_ID: str = "aiddata_id"
    RECOMMENDED_FOR_AGGREGATES: str = "recommended_for_aggregates"
    AIDDATA_PARENT_ID: str = "aiddata_parent_id"
    UMBRELLA: str = "umbrella"
    PROVIDER_NAME: str = "donor_name"
    RECIPIENT_NAME: str = "recipient_name"
    RECIPIENT_ISO3: str = "recipient_iso3"
    REGION_NAME: str = "region_name"
    COMMITMENT_YEAR: str = "commitment_year"
    IMPLEMENTATION_START_YEAR: str = "start_year"
    COMPLETION_YEAR: str = "completion_year"
    PROJECT_TITLE: str = "project_title"
    LONG_DESCRIPTION: str = "long_description"
    STAFF_COMMENTS: str = "staff_comments"
    PROJECT_STATUS: str = "project_status"
    PROJECT_INTENT: str = "project_intent"
    FLOW_TYPE: str = "flow_type"
    FLOW_TYPE_SIMPLIFIED: str = "flow_type_simplified"
    ODA_CONCESSIONALITY_THRESHOLD: str = "oda_concessionality_threshold"
    FLOW_CLASS: str = "flow_class"
    SECTOR_CODE: str = "sector_code"
    SECTOR_NAME: str = "sector_name"
    INFRASTRUCTURE: str = "infrastructure"
    COVID: str = "covid"
    DONOR_AGENCIES_NAME: str = "donor_agencies_name"
    DONOR_AGENCIES_TYPE: str = "donor_agencies_type"
    CO_FINANCED: str = "co_financed"
    CO_FINANCING_AGENCIES_NAME: str = "co_financing_agencies_name"
    CO_FINANCING_AGENCIES_TYPE: str = "co_financing_agencies_type"
    DIRECT_RECIPIENT_AGENCIES_NAME: str = "direct_recipient_agencies_name"
    DIRECT_RECIPIENT_AGENCIES_TYPE: str = "direct_recipient_agencies_type"
    INDIRECT_RECIPIENT_AGENCIES_NAME: str = "indirect_recipient_agencies_name"
    INDIRECT_RECIPIENT_AGENCIES_TYPE: str = "indirect_recipient_agencies_type"
    ON_LENDING: str = "on_lending"
    IMPLEMENTING_AGENCIES_NAME: str = "implementing_agencies_name"
    IMPLEMENTING_AGENCIES_TYPE: str = "implementing_agencies_type"
    GUARANTEE_PROVIDED: str = "guarantee_provided"
    GUARANTOR_NAME: str = "guarantor_name"
    GUARANTOR_TYPE: str = "guarantor_type"
    INSURANCE_PROVIDED: str = "insurance_provided"
    INSURANCE_PROVIDER_NAME: str = "insurance_provider_name"
    INSURANCE_PROVIDER_TYPE: str = "insurance_provider_type"
    COLLATERALIZED: str = "collateralized"
    COLLATERAL_PROVIDER_NAME: str = "collateral_provider_name"
    COLLATERAL_PROVIDER_TYPE: str = "collateral_provider_type"
    COLLATERAL_AGENT_NAME: str = "collateral_agent_name"
    COLLATERAL_AGENT_TYPE: str = "collateral_agent_type"
    COLLATERAL: str = "collateral"
    VALUE_ORIGINAL_CURRENCY: str = "commitment_original_currency"
    ORIGINAL_CURRENCY: str = "original_currency"
    VALUE_ESTIMATED: str = "amount_estimated"
    VALUE_CONSTANT_USD_2021: str = "usd_commitment_constant"
    VALUE_CURRENT_USD: str = "usd_commitment"
    ADJUSTED_VALUE_ORIGINAL_CURRENCY: str = "adjustment_original_currency"
    ADJUSTED_VALUE_CONSTANT_USD_2021: str = "usd_adjustment_constant"
    ADJUSTED_VALUE_CURRENT_2021: str = "usd_adjustment"
    FINANCIAL_DISTRESS: str = "financial_distress"
    COMMITMENT_DATE: str = "commitment_date"
    COMMITMENT_DATE_ESTIMATED: str = "commitment_date_estimated"
    PLANNED_IMPLEMENTATION_DATE: str = "expected_start_date"
    ACTUAL_IMPLEMENTATION_DATE: str = "actual_start_date"
    ACTUAL_IMPLEMENTATION_DATE_ESTIMATED: str = "actual_start_date_estimated"
    DEVIATION_PLANNED_ACTUAL_IMPLEMENTATION_DATE: str = (
        "deviation_expected_actual_start_date"
    )
    PLANNED_COMPLETION_DATE: str = "expected_completion_date"
    ACTUAL_COMPLETION_DATE: str = "actual_completion_date"
    ACTUAL_COMPLETION_DATE_ESTIMATED: str = "actual_completion_date_estimated"
    DEVIATION_PLANNED_ACTUAL_COMPLETION_DATE: str = (
        "deviation_expected_actual_completion_date"
    )
    MATURITY: str = "maturity"
    INTEREST_RATE: str = "interest_rate"
    GRACE_PERIOD: str = "grace_period"
    MANAGEMENT_FEE: str = "management_fee"
    COMMITMENT_FEE: str = "commitment_fee"
    INSURANCE_FEE_PERCENT: str = "insurance_fee_percent"
    INSURANCE_FEE_CURRENT_USD: str = "insurance_fee_usd"
    DEFAULT_INTEREST_RATE: str = "default_interest_rate"
    FIRST_LOAN_REPAYMENT_DATE: str = "repaydate1"
    LAST_LOAN_REPAYMENT_DATE: str = "repaydate2"
    GRANT_ELEMENT_FLOW: str = "grant_element_flow"
    GRANT_ELEMENT_GRANT_EQUIVALENT: str = "grant_element_grant_equiv"
    GRANT_ELEMENT_IMF: str = "grant_element_imf"
    NUMBER_OF_LENDERS: str = "number_of_lenders"
    EXPORT_BUYERS_CREDIT: str = "export_buyers_credit"
    SUPPLIERS_CREDIT_EXPORT_SELLERS_CREDIT: str = (
        "suppliers_credit_export_sellers_credit"
    )
    INTEREST_FREE_LOAN: str = "interest_free_loan"
    REFINANCING: str = "refinancing"
    INVESTMENT_PROJECT_LOAN: str = "investment_project_loan"
    M_A: str = "ma"
    WORKING_CAPITAL: str = "working_capital"
    EPCF: str = "epcf"
    LEASE: str = "lease"
    FXSL_BOP: str = "fxsl_bop"
    CC_IRS: str = "cc_irs"
    RCF: str = "rcf"
    GCL: str = "gcl"
    PBC: str = "pbc"
    PXF_COMMODITY_PREPAYMENT: str = "pxf_commodity_prepayment"
    INTER_BANK_LOAN: str = "inter_bank_loan"
    OVERSEAS_PROJECT_CONTRACTING_LOAN: str = "overseas_project_contracting_loan"
    DPA: str = "dpa"
    PROJECT_FINANCE: str = "project_finance"
    INVOLVING_MULTILATERAL: str = "involving_multilateral"
    NON_CHINESE_FINANCIER: str = "non_chinese_financier"
    SHORT_TERM: str = "short_term"
    RESCUE: str = "rescue"
    PROJECT_JV_SPV_HOST_GOVERNMENT_OWNERSHIP: str = (
        "project_jv_spv_host_government_ownership"
    )
    PROJECT_JV_SPV_CHINESE_GOVERNMENT_OWNERSHIP: str = (
        "project_jv_spv_chinese_government_ownership"
    )
    LEVEL_OF_PUBLIC_LIABILITY: str = "level_of_public_liability"
    TOTAL_SOURCE_COUNT: str = "total_source_count"
    OFFICIAL_SOURCE_COUNT: str = "official_source_count"
    SOURCE_URLS: str = "source_urls"
    SOURCE_TITLES: str = "source_titles"
    SOURCE_PUBLISHERS: str = "source_publishers"
    SOURCE_RESOURCE_TYPES: str = "source_resource_types"
    CONTACT_NAME: str = "contact_name"
    CONTACT_POSITION: str = "contact_position"
    ODA_ELIGIBLE_RECIPIENT: str = "oda_eligible_recipient"
    INCOME_GROUP_NAME: str = "income_group_name"
    LOCATION_NARRATIVE: str = "location_narrative"
    GEOGRAPHIC_PRECISION_LEVEL: str = "geographic_precision_level"
    ADM1_LEVEL_AVAILABLE: str = "adm1_level_available"
    ADM2_LEVEL_AVAILABLE: str = "adm2_level_available"
    GEOSPATIAL_FEATURE_AVAILABLE: str = "geospatial_feature_available"
    SOURCE_QUALITY: str = "source_quality"
    DATA_COMPLETENESS: str = "data_completeness"
    IMPLEMENTATION_DETAIL: str = "implementation_detail"
    LOAN_DETAIL: str = "loan_detail"

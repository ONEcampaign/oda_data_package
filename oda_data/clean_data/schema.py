from dataclasses import dataclass


@dataclass
class OdaSchema:
    YEAR: str = "year"
    PROVIDER_CODE: str = "oecd_provider_code"
    PROVIDER_NAME: str = "provider"
    PROVIDER_TYPE: str = "provider_type"
    PROVIDER_DETAILED: str = "provider_detailed"
    AGENCY_CODE: str = "oecd_agency_code"
    AGENCY_NAME: str = "agency"
    CRS_ID: str = "crs_id"
    PROJECT_ID: str = "project_id"
    RECIPIENT_CODE: str = "oecd_recipient_code"
    RECIPIENT_NAME: str = "recipient"
    RECIPIENT_REGION: str = "recipient_region"
    RECIPIENT_REGION_CODE: str = "oecd_recipient_region_code"
    RECIPIENT_INCOME: str = "oecd_recipient_income"
    FLOW_MODALITY: str = "modality"
    ALLOCABLE_SHARE: str = "allocable_share"
    CONCESSIONALITY: str = "concessionality"
    FINANCIAL_INSTRUMENT: str = "financial_instrument"
    FLOW_TYPE: str = "flow_type"
    FINANCE_TYPE: str = "type_of_finance"
    CHANNEL_NAME_DELIVERY: str = "oecd_channel_name_delivery"
    CHANNEL_CODE_DELIVERY: str = "oecd_channel_code_delivery"
    CHANNEL_CODE: str = "oecd_channel_code"
    CHANNEL_NAME: str = "oecd_channel_name"
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
    "gender": OdaSchema.GENDER,
    "category": OdaSchema.CATEGORY,
    "usd_commitment": OdaSchema.USD_COMMITMENT,
    "usd_disbursement": OdaSchema.USD_DISBURSEMENT,
    "usd_received": OdaSchema.USD_RECEIVED,
    "usd_grant_equiv": OdaSchema.USD_GRANT_EQUIV,
    "usd_net_disbursement": OdaSchema.USD_NET_DISBURSEMENT,
    "oecd_climate_total": OdaSchema.CLIMATE_UNSPECIFIED,
    "reporting_type": OdaSchema.REPORTING_METHOD,
    "type": OdaSchema.MULTILATERAL_TYPE,
    "converged_reporting": OdaSchema.CONVERGED_REPORTING,
    "coal_related_financing": OdaSchema.COAL_FINANCING,
    "flow_type": OdaSchema.FLOW_TYPE,
    "type_of_flow": OdaSchema.FLOW_TYPE,
}

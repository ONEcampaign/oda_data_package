# Constants
from typing import Literal

from oda_data.clean_data.schema import OdaSchema

MarkerScore = Literal[
    "significant",
    "principal",
    "not_targeted",
    "not_screened",
    "total_targeted",
    "total_allocable",
]

PolicyMarker = Literal[
    "gender", "environment", "nutrition", "disability", "biodiversity"
]

CURRENCIES: dict[str, str] = {
    "USD": "USA",
    "EUR": "EUI",
    "GBP": "GBR",
    "CAD": "CAN",
    "LCU": "LCU",
}

PRICES = {
    "DAC1": {"column": "amounttype_code"},
    "DAC2A": {"column": "data_type_code"},
}

MEASURES: dict[str, dict] = {
    "DAC1": {
        "commitment": {
            "column": "flows_code",
            "filter": 1150,
        },
        "commitment_grant": {
            "column": "flows_code",
            "filter": 1151,
        },
        "commitment_non_grant": {
            "column": "flows_code",
            "filter": 1152,
        },
        "grant_equivalent": {
            "column": "flows_code",
            "filter": 1160,
        },
        "net_disbursement": {
            "column": "flows_code",
            "filter": 1140,
        },
        "net_disbursement_grant": {
            "column": "flows_code",
            "filter": 1121,
        },
        "gross_disbursement": {
            "column": "flows_code",
            "filter": 1120,
        },
        "gross_disbursement_non_grant": {
            "column": "flows_code",
            "filter": 1122,
        },
        "received": {
            "column": "flows_code",
            "filter": 1130,
        },
    },
    "DAC2A": {
        "net_disbursement": {
            "column": "flow_type_code",
            "filter": "D",
        },
        "gross_disbursement": {
            "column": "flow_type_code",
            "filter": "D",
        },
    },
    "CRS": {
        "commitment": {"column": "usd_commitment", "filter": None},
        "grant_equivalent": {"column": "usd_grant_equiv", "filter": None},
        "gross_disbursement": {"column": "usd_disbursement", "filter": None},
        "received": {"column": "usd_received", "filter": None},
        "expert_commitment": {"column": "usd_expert_commitment", "filter": None},
        "expert_extended": {"column": "usd_expert_extended", "filter": None},
        "export_credit": {"column": "usd_export_credit", "filter": None},
    },
    "Multisystem": {
        "commitment": {
            "column": "flow_type",
            "filter": "Commitments",
        },
        "gross_disbursement": {
            "column": "flow_type",
            "filter": "Disbursements",
        },
    },
}

Measure = Literal[
    "commitment",
    "commitment_grant",
    "commitment_non_grant",
    "expert_commitment",
    "expert_extended",
    "export_credit",
    "grant_equivalent",
    "gross_disbursement",
    "gross_disbursement_non_grant",
    "net_disbursement",
    "net_disbursement_grant",
    "received",
]


_EXCLUDE = [
    "amounttype_code",
    "amount_type",
    "base_period",
    "data_type_code",
    "unit_measure_name",
    "unit_measure_code",
]

EXTENDED_PROVIDER_PURPOSE_GROUPER = [
    OdaSchema.PROVIDER_CODE,
    OdaSchema.PROVIDER_NAME,
    OdaSchema.AGENCY_CODE,
    OdaSchema.AGENCY_NAME,
    OdaSchema.RECIPIENT_CODE,
    OdaSchema.FLOW_MODALITY,
    OdaSchema.FINANCE_TYPE,
    OdaSchema.PURPOSE_CODE,
    OdaSchema.YEAR,
    OdaSchema.CURRENCY,
    OdaSchema.PRICES,
]

PROVIDER_PURPOSE_GROUPER = [
    OdaSchema.PROVIDER_CODE,
    OdaSchema.PROVIDER_NAME,
    OdaSchema.AGENCY_CODE,
    OdaSchema.AGENCY_NAME,
    OdaSchema.PURPOSE_CODE,
    OdaSchema.RECIPIENT_CODE,
    OdaSchema.YEAR,
    OdaSchema.CURRENCY,
    OdaSchema.PRICES,
]

CHANNEL_PURPOSE_GROUPER = [
    OdaSchema.CHANNEL_CODE,
    OdaSchema.PURPOSE_CODE,
    OdaSchema.RECIPIENT_CODE,
    OdaSchema.YEAR,
    OdaSchema.CURRENCY,
    OdaSchema.PRICES,
]

CHANNEL_PURPOSE_SHARE_GROUPER = [
    OdaSchema.YEAR,
    OdaSchema.CURRENCY,
    OdaSchema.PRICES,
    OdaSchema.CHANNEL_CODE,
]

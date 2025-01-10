# Constants
from typing import Literal

from oda_data.read_data.read import read_dac1, read_dac2a, read_crs, read_multisystem

Measure = Literal[
    "grant_equivalent", "gross_disbursement", "commitment", "net_disbursement"
]

READERS: dict[str, callable] = {
    "dac1": read_dac1,
    "DAC1": read_dac1,
    "dac2a": read_dac2a,
    "DAC2A": read_dac2a,
    "crs": read_crs,
    "CRS": read_crs,
    "multisystem": read_multisystem,
}

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
        "grant_equivalent": {
            "column": "flows_code",
            "filter": 1160,
        },
        "net_disbursement": {
            "column": "flows_code",
            "filter": 1140,
        },
        "gross_disbursement": {
            "column": "flows_code",
            "filter": 1120,
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
}

_EXCLUDE = [
    "amounttype_code",
    "amount_type",
    "base_period",
    "data_type_code",
    "unit_measure_name",
    "unit_measure_code",
]

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
    "multisystem": read_multisystem,
}

CURRENCIES: dict[str, str] = {
    "USD": "USA",
    "EUR": "EUI",
    "GBP": "GBR",
    "CAD": "CAN",
    "LCU": "LCU",
}

PRICES = {"DAC1": {"column": "amounttype_code"}, "DAC2A": {"column": "data_type_code"}}

MEASURES: dict[str, dict] = {
    "DAC1": {
        "column": "flows_code",
        "commitment": 1150,
        "grant_equivalent": 1160,
        "net_disbursement": 1140,
        "gross_disbursement": 1120,
    },
    "DAC2A": {
        "column": "flow_type_code",
        "net_disbursement": "D",
        "gross_disbursement": "D",
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

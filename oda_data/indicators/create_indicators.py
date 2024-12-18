import json

import pandas as pd
from oda_reader import download_dac1

from oda_data import read_dac1
from oda_data.logger import logger


SEPARATOR = "."


def dac1_flow_type_mapping(): ...


def dac1_indicators():
    """Generate a json file which defines the DAC1 indicator codes, and the filtering process
    to generate them."""

    dac1_base: str = "DAC1"

    dac1 = read_dac1(years=range(2018, 2025))

    return NotImplemented

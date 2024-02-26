import pandas as pd

from oda_data import ODAData, config, set_data_path
from oda_data.indicators.linked_indicators import linked_indicator

set_data_path(config.OdaPATHS.test_files)

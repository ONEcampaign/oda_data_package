[![pypi](https://img.shields.io/pypi/v/oda_data.svg)](https://pypi.org/project/oda_data/)
[![python](https://img.shields.io/pypi/pyversions/oda_data.svg)](https://pypi.org/project/oda_data/)
[![codecov](https://codecov.io/gh/ONEcampaign/oda_data_package/branch/main/graph/badge.svg?token=G8N8BWWPL8)](https://codecov.io/gh/ONEcampaign/oda_data_package)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

# The ODA Data Package
This package contains key tools used by The ONE Campaign to analyse Official Development Assistance (ODA) data from
the OECD [DAC](https://www.oecd.org/dac/stats/) databases.

Interacting with the DAC databases can be a complex task. There are many databases, tables, and web interfaces which
can be used to get the data you need. This means that getting the right ODA data can require expert knowledge not only
of ODA, but also of how the DAC databases and tools are organised.

This package aims to simplify this process and make it easier for users to get the data they need.

Please submit questions, feedback or requests via 
the [issues page](https://github.com/ONEcampaign/oda_data_package/issues).

## Getting started

### Installation
The package can be installed using pip:

```bash
pip install oda-data --upgrade
```

The package is compatible with Python 3.10 and above.

### Basic usage

Most users can get the data they need by using the `ODAData` class.

An object of this class can handle:
- getting data for specific indicators (one or more)
- filtering the data for specific donors, recipients(if relevant), years.
- returning the data in a variety of currency/prices combinations.

For example, to get Total ODA in net flows and grant equivalents, in constant 2021 Euros, for 2018-2021.

```python
from oda_data import ODAData, set_data_path

# set the path to the folder where the data should be stored
set_data_path("path/to/data/folder")

# create object, specifying key details of the desired output
data = ODAData(years=range(2018,2022), currency="EUR", prices="constant", base_year=2021)

# load the desired indicators
data.load_indicator(indicators = ["total_oda_flow_net", "total_oda_ge"])

# get the data
df = data.get_data()

print(df.head(6))
```
This would result in the following dataframe:

|   donor_code | donor_name   |   year |   value | indicator          | currency   | prices   |
|-------------:|:-------------|-------:|--------:|:-------------------|:-----------|:---------|
|            1 | Austria      |   2021 | 1261.76 | total_oda_flow_net | EUR        | constant |
|            1 | Austria      |   2021 | 1240.31 | total_oda_ge       | EUR        | constant |
|            2 | Belgium      |   2021 | 2176.38 | total_oda_flow_net | EUR        | constant |
|            2 | Belgium      |   2021 | 2174.38 | total_oda_ge       | EUR        | constant |
|            3 | Denmark      |   2021 | 2424.51 | total_oda_flow_net | EUR        | constant |
|            3 | Denmark      |   2021 | 2430.65 | total_oda_ge       | EUR        | constant |


To print the full list of available indicators, you can call `.get_available_indicators()`.

For full details on the available indicators and how we calculate them,
see the indicators [documentation](oda_data/settings/Available indicators.md)

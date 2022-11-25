[![pypi](https://img.shields.io/pypi/v/oda_data.svg)](https://pypi.org/project/oda_data/)
[![python](https://img.shields.io/pypi/pyversions/oda_data.svg)](https://pypi.org/project/oda_data/)
[![codecov](https://codecov.io/gh/ONEcampaign/oda_data_package/branch/main/graph/badge.svg?token=G8N8BWWPL8)](https://codecov.io/gh/ONEcampaign/oda_data_package)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

# The ODA Data Package
This package contains key tools used by The ONE Campaign to analyse Official Development Assistance (ODA) data from
the OECD [DAC](https://www.oecd.org/dac/stats/) databases.

**This package is currently in active development. Features and APIs may change.** Please submit questions, feedback
or requests via the [issues page](https://github.com/ONEcampaign/oda_data_package/issues).

## Getting started

Most users can get the data they need by using the `ODAData` class.

An object of this class can handle:
- getting data for specific indicators (one or more)
- filtering the data for specific donors, recipients(if relevant), years.
- returning the data in a variety of currency/prices combinations.

For example, to get Total ODA in net flows and grant equivalents, in constant 2021 Euros, for 2018-2021.

```python
from oda_data import ODAData

# create object, specifying key details of the desired output
data = ODAData(years=range(2018,2022), currency="EUR", prices="constant", base_year=2021)

# load the desired indicators
data.load_indicator(indicator="total_oda_flow_net")
data.load_indicator(indicator="total_oda_ge")

# get the data
df = data.get_data(indicators='all')

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


To view the full list of available indicators, you can call `.get_available_indicators()`.

```python
from oda_data import ODAData

# create object
data = ODAData()

# get the list of available indicators
data.available_indicators()
```
This logs the list of indicators to the console. For example, the first several:
```markdown
Available indicators:
total_oda_flow_net
total_oda_ge
total_oda_bilateral_flow_net
total_oda_bilateral_ge
total_oda_multilateral_flow_net
total_oda_multilateral_ge
total_oda_flow_gross
total_oda_flow_commitments
total_oda_grants_flow
total_oda_grants_ge
total_oda_non_grants_flow
total_oda_non_grants_ge
gni
oda_gni_flow
od_gni_ge
.
.
.
```

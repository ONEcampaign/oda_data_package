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

## Tutorials
For more detailed examples of how to use the package, see the [tutorials](tutorials).
- A [tutorial notebook](tutorials/1.%20total_donor_oda.ipynb) on loading the package and getting total oda data
- A [tutorial notebook](tutorials/2.%20total_recipient_oda_by_donor.ipynb) on getting ODA by donor and recipient
  (including both bilateral and imputed multilateral data)
- A [tutorial notebook](tutorials/3.%20sector_analysis_by_donor_and_recipient.ipynb) on getting ODA by sectors
  (including both bilateral and imputed multilateral data)

Please reach out if you have questions or need help with using the package for your analysis.

## Key features

- **Speed up analysis** - The package handles downloading, cleaning and loading all the data, so you can focus on the 
analysis. The data is downloaded from the bulk download service of the OECD, and once it is stored locally, producing
the output is extremely fast.
- **Access all of our analysis** - Besides the classic OECD DAC indicators, the package also provides access to the
data and analysis produced by ONE. This includes gender or climate data in gross disbursement terms (instead of
commitments) and our multilateral sectors imputations.
- **Get data in the currency and prices you need** - ODA data is only available in US dollars (current or constant 
prices) and local currency units (current prices). The package allows you to view the data in US dollars, Euros,
British Pounds and Canadian dollars, in both current and constant prices. We can add any other DAC currency if you
request it via the [issues page](https://github.com/ONEcampaign/oda_data_package/issues)

## Contributing
Interested in contributing to the package? Please reach out.

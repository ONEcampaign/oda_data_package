[![pypi](https://img.shields.io/pypi/v/oda_data.svg)](https://pypi.org/project/oda_data/)
[![python](https://img.shields.io/pypi/pyversions/oda_data.svg)](https://pypi.org/project/oda_data/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

# The ODA Data Package
This is a Python package designed for accessing, processing, and analyzing Official Development Assistance (ODA) data from OECD DAC. With an intuitive API, it simplifies common tasks like retrieving ODA indicators, converting currencies, filtering data, and handling bulk downloads.

**Note: This is the new version of the package (V2). This release includes many breaking changes and previous workflows
will not work. The previous 1.5.x version remains available on PyPI and we will continue to support it at least until
August 2025.**

Interacting with the DAC databases can be a complex task. There are many databases, tables, and web interfaces which
can be used to get the data you need. This means that getting the right ODA data can require expert knowledge not only
of ODA, but also of how the DAC databases and tools are organised.

This package aims to simplify this process and make it easier for users to get the data they need.

Please submit questions, feedback or requests via 
the [issues page](https://github.com/ONEcampaign/oda_data_package/issues).

## Getting started

### Installation

The latest version of the package can be installed using pip:

```bash
pip install oda-data --upgrade
```

The package is compatible with Python 3.10 and above.

## Basic usage

Most users can get the data they need by using the `OECDClient` class.

An object of this class can handle:
- getting data for specific indicators (one or more)
- filtering the data for specific donors, recipients(if relevant), years.
- returning the data in a variety of currency/prices combinations.

At a minimum, the class expects users to specify years. By default it would then get 
data for all providers, for all recipients (if applicable), as net disbursements in USD, in current prices,
using the OECD data-explorer API.

In the example below, we get all data for Total ODA (net disbursements), for 2012 to 2022.

```python
from oda_data import OECDClient, set_data_path

# set the path to the folder where the raw data should be cached
set_data_path("path/to/data/folder")

# create object, specifying key details of the desired output
indicator = OECDClient(years=range(2012, 2023))

# Get the desired indicator
data = indicator.get_indicators("DAC1.10.1010")
```

Here's an example to get Bilateral ODA for France and the US, for 2020 and 2021. The data will be in grant equivalents, in constant 2019 Euros.

```python
from oda_data import OECDClient, set_data_path

# set the path to the folder where the data should be stored
set_data_path("path/to/data/folder")

# create object, specifying key details of the desired output
client = OECDClient(
    years=[2020, 2021],
    providers=[4, 302],  # Example provider codes
    measure="grant_equivalent",  # Options: "commitment", "grant_equivalent", etc.
    currency="EUR",  # Default is USD; others include EUR, GBP, CAD, LCU
    base_year=2019,  # Adjust data to constant prices of the specified year
)

# Get the desired indicator
data = client.get_indicators("DAC1.10.11015")
```

If you intend to download many indicators, we recommend using the Bulk download file instead. The OECD data-explorer
API has pretty low usage limits, and you could get a temporary block if you make too many repeated calls. While downloading a particular bulk file may time a few minutes, subsequently getting indicator data is much faster.

```python
from oda_data import OECDClient, set_data_path

# set the path to the folder where the data should be stored
set_data_path("path/to/data/folder")

# define a list of indicators to get
research_indicators = ["DAC1.10.1100", "DAC1.10.1410", "DAC1.10.2102", "DAC1.10.1500"]

# create object, specifying key details of the desired output
client = OECDClient(
    years=[2019, 2020, 2021],
    measure="net_disbursement",  # Options: "commitment", "grant_equivalent", etc.
    currency="LCU",  # To get the data in providers' own currencies
    use_bulk_download=True
)

# Get the desired indicators
data = client.get_indicators(research_indicators)
```

### OECDClient

Over three thousand indicators are currently supported. They are mostly produced by filtering (and sometimes aggregating) data from the different DAC Tables and databases.

You can get a dictionary with all supported indicators, with their code, name, description and source by using the `.available_indicators()` method:

```python
from oda_data import OECDClient

all_indicators = OECDClient.available_indicators()
```

Alternatively, you can export the indicators to a CSV by using the `.export_available_indicators()` method.

```python
from oda_data import OECDClient

OECDClient.export_available_indicators(export_folder="path/to/folder/")
```

### Providers
You can get a dictionary with all available providers, with their code and name, by using the `.available_providers()` method:

```python
from oda_data import OECDClient

providers = OECDClient.available_providers()
```

### Recipients
You can get a dictionary with all available recipients, with their code and name, by using the `.available_recipients()` method:

```python
from oda_data import OECDClient

recipients = OECDClient.available_recipients()
```

### Currencies
You can get a dictionary with all available currencies, with their code and name, by using the `.available_currencies()` method:

```python
from oda_data import OECDClient

currencies = OECDClient.available_currencies()
```

## Accessing the data
For more advanced users, you can use this package to get and work with different tables and databases from the OECD DAC.

### DAC 1
The `DAC1Data` class can retrieve and return DAC 1 data.

You can optionally specify:
- years: to filter the data for specific years
- providers: to filter the data for specific providers (based on their code).
- indicators: to filter the data for a specific indicator (for DAC1 this means AIDTYPE_CODE)

Not specifying any or all of these parameters will return all the available data.

After instantiating the object, you can call:
- `download`: to download the data from the data-explorer API or the bulk download service. This method returns a DataFrame.
- `read`: to read data as stored in the specified folder. If the data is not available, it will be downloaded.

```python
from oda_data import DAC1Data, set_data_path

# set the path to the folder where the data should be stored
set_data_path("path/to/data/folder")

# instantiate a DAC1Data object
dac1 = DAC1Data()

# Perform a bulk download
dac1.download(bulk=True)

# Read the stored data in full
data = dac1.read(using_bulk_download=True)
```

Use cases can be quite elaborate. For example, here we will download data for France for 2022 only, for total ODA (net flows).

When reading, we will also filter to get only current prices, and to only read a few columns.

```python
from oda_data import DAC1Data, set_data_path

# set the path to the folder where the data should be stored
set_data_path("path/to/data/folder")

# instantiate a DAC1Data object
dac1 = DAC1Data(years=2022, providers=4, indicators=1010)

# Get the data by calling on read. It will be downloaded since it isn't already available.
data = dac1.read(
    additional_filters=[("amount_type", "==", "Current prices")],
    columns=["donor_name", "aid_type", "fund_flows", "amount_type", "year", "value"],
)
```


### DAC 2A
The `DAC2AData` class can retrieve and return DAC2a data.

You can optionally specify:
- years: to filter the data for specific years
- providers: to filter the data for specific providers (based on their code).
- recipients: to filter the data for specific recipients (based on their code).
- indicators: to filter the data for a specific indicator (for DAC2a this means AIDTYPE_CODE)

Not specifying any or all of these parameters will return all the available data.

After instantiating the object, you can call:
- `download`: to download the data from the data-explorer API or the bulk download service. This method returns a DataFrame.
- `read`: to read data as stored in the specified folder. If the data is not available, it will be downloaded.

```python
from oda_data import DAC2AData, set_data_path

# set the path to the folder where the data should be stored
set_data_path("path/to/data/folder")

# instantiate a DAC1Data object
dac2a = DAC2AData()

# Perform a bulk download
dac2a.download(bulk=True)

# Read the stored data in full
data = dac2a.read(using_bulk_download=True)
```

Use cases can be quite elaborate. For example, here we will download data for France - Togo for 2023 only, for imputed
multilateral aid.

When reading, we will also filter to get only current prices, and to only read a few columns.

```python
from oda_data import DAC2AData, set_data_path

# set the path to the folder where the data should be stored
set_data_path("path/to/data/folder")

# instantiate a DAC2aData object
dac2a = DAC2AData(years=2022, providers=4, recipients=283, indicators=106)

# Get the data by calling on read. It will be downloaded since it isn't already available.
data = dac2a.read(
    additional_filters=[("amount_type", "==", "Current prices")],
    columns=[
        "donor_name",
        "recipient_name",
        "aid_type",
        "flow_type_name",
        "amount_type",
        "year",
        "value",
    ],
)
```

### CRS
The `CRSData` class can retrieve and return CRS data.

You can optionally specify:
- years: to filter the data for specific years
- providers: to filter the data for specific providers (based on their code).
- recipients: to filter the data for specific recipients (based on their code).

Not specifying any or all of these parameters will return all the available data.

You can refer to the examples above for DAC1 and DAC2a to understand the usage of these classes.
**Note**: given the speed and rate-limiting of the dac-explorer APIs, it's recommended to always use the 
bulk option when using the CRS class.

```python
from oda_data import CRSData, set_data_path

# set the path to the folder where the data should be stored
set_data_path("path/to/data/folder")

# instantiate a CRSData object
crs = CRSData(years=range(2010, 2024))

# Get the data by calling on read. It will be downloaded since it isn't already available.
data = crs.read(using_bulk_download=True)
```


### MultiSystem
The `MultiSystemData` class can retrieve and return Providers Total Use of the Multilateral System data.

You can optionally specify:
- years: to filter the data for specific years
- providers: to filter the data for specific providers (based on their code).
- recipients: to filter the data for specific recipients (based on their code).
- indicators: to filter for specific indicators (aid_to_or_thru in this case)
- sectors: to filter for specific sectors (if its aid through the multilateral system only)

Not specifying any or all of these parameters will return all the available data.

You can refer to the examples above for DAC1 and DAC2a to understand the usage of these classes.
**Note**: given the speed and rate-limiting of the dac-explorer APIs, consider using the bulk option
unless you're looking for something very specific.


## Key features

- **Speed up analysis** - The package handles downloading, cleaning and loading all the data, so you can focus on the 
analysis.
- **Get data in the currency and prices you need** - ODA data is only available in US dollars (current or constant 
prices) and local currency units (current prices). The package allows you to view the data in US dollars, Euros,
British Pounds and Canadian dollars, in both current and constant prices. We can add any other DAC currency if you
request it via the [issues page](https://github.com/ONEcampaign/oda_data_package/issues)

## Contributing
Interested in contributing to the package? Please reach out.

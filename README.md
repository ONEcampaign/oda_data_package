[![pypi](https://img.shields.io/pypi/v/oda_data.svg)](https://pypi.org/project/oda_data/)
[![python](https://img.shields.io/pypi/pyversions/oda_data.svg)](https://pypi.org/project/oda_data/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Tests](https://github.com/ONEcampaign/oda_data_package/actions/workflows/test.yml/badge.svg)](https://github.com/ONEcampaign/oda_data_package/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/ONEcampaign/oda_data_package/branch/main/graph/badge.svg)](https://codecov.io/gh/ONEcampaign/oda_data_package)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Downloads](https://static.pepy.tech/badge/oda-data)](https://pepy.tech/project/oda-data)

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

The package is compatible with Python 3.11 and above.

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

Thousands of indicators are currently supported across DAC1, DAC2A, and CRS databases. These indicators are produced by filtering and sometimes aggregating data from the different DAC Tables and databases.

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

### Measures

Different measures are available depending on the data source. Here's what each source supports:

**DAC1 Measures:**
- `commitment` - Total commitments
- `commitment_grant` - Grant commitments only
- `commitment_non_grant` - Non-grant commitments
- `grant_equivalent` - Grant equivalent values
- `net_disbursement` - Net disbursements (default)
- `net_disbursement_grant` - Net grant disbursements
- `gross_disbursement` - Gross disbursements
- `gross_disbursement_non_grant` - Gross non-grant disbursements
- `received` - Amounts received

**DAC2A Measures:**
- `net_disbursement` - Net disbursements (default)
- `gross_disbursement` - Gross disbursements

**CRS Measures:**
- `commitment` - Commitment amounts
- `grant_equivalent` - Grant equivalent values
- `gross_disbursement` - Gross disbursement amounts
- `received` - Amounts received
- `expert_commitment` - Expert commitments
- `expert_extended` - Expert extended
- `export_credit` - Export credits

**MultiSystem Measures:**
- `commitment` - Commitments
- `gross_disbursement` - Gross disbursements

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


## Utility Functions

The package provides several utility functions to enhance and enrich your ODA data analysis:

### Adding Human-Readable Names

Convert code columns to human-readable names:

```python
from oda_data import OECDClient, add_names_columns

# Get indicator data
client = OECDClient(years=range(2020, 2023))
data = client.get_indicators("DAC2A.10.1010")

# Add human-readable names for code columns
data = add_names_columns(data, ["recipient_code", "provider_code"])
```

### Adding GNI Share

Calculate ODA as a percentage of Gross National Income:

```python
from oda_data import OECDClient, add_gni_share_column

# Create client with specific configuration
client = OECDClient(
    years=range(2018, 2023),
    providers=[4, 302],  # France and USA
    currency="USD",
    base_year=2021
)

# Get data with GNI share column
data = add_gni_share_column(client, "DAC1.10.1010")
# Returns dataframe with 'gni_share_pct' column
```

### Sector Classification

Add sector classifications to your data:

```python
from oda_data import add_sectors, add_broad_sectors

# Add detailed sector information
data = add_sectors(data)

# Or add broad sector categories
data = add_broad_sectors(data)
```

### Provider and Recipient Groupings

Access predefined groupings of providers and recipients:

```python
from oda_data import provider_groupings, recipient_groupings

# Get all provider groupings (DAC members, EU27, etc.)
providers = provider_groupings()

# Get all recipient groupings (LDCs, regions, income groups, etc.)
recipients = recipient_groupings()

# Example: Get all DAC members
dac_members = providers["dac_members"]
```

### Cache Management

Control the package's caching behavior:

```python
from oda_data import clear_cache, disable_cache, enable_cache

# Clear all cached data
clear_cache()

# Temporarily disable caching (for development/testing)
disable_cache()

# Re-enable caching
enable_cache()
```

### Compatibility with Version 1.x

For users migrating from v1.x, a compatibility layer is available:

```python
from oda_data import ODAData

# Use the v1.x style interface
oda = ODAData(years=range(2018, 2022), donors=[4, 302])
data = oda.load_indicator("total_oda_flow_net")
```

## Advanced Features

### Policy Marker Analysis

Extract bilateral ODA by policy marker (gender, climate, etc.):

```python
from oda_data import bilateral_policy_marker

# Get gender-focused ODA (principal objective)
gender_data = bilateral_policy_marker(
    years=range(2015, 2023),
    providers=[4, 302],
    marker="gender",
    marker_score="principal",
    measure="gross_disbursement",
    currency="USD",
    base_year=2021
)

# Available markers: "gender", "environment", "nutrition", "disability", "biodiversity"
# Available scores: "significant", "principal", "not_targeted", "not_screened",
#                   "total_targeted", "total_allocable"
```

### Sector Imputations

Access sector imputation functions for multilateral aid analysis:

```python
from oda_data.indicators.research import sector_imputations

# Use specialized sector imputation functions
# (See module documentation for available functions)
```

### AidData Integration

Access project-level data from AidData's dataset:

```python
from oda_data import AidDataData, set_data_path

set_data_path("path/to/data/folder")

# Initialize AidData source
aiddata = AidDataData(
    years=range(2010, 2020),
    recipients=["Kenya", "Tanzania"],
    sectors=[110, 120]  # Sector codes
)

# Read the data (automatically uses bulk download)
data = aiddata.read()
```

## Key features

- **Speed up analysis** - The package handles downloading, cleaning and loading all the data, so you can focus on the
analysis.
- **Get data in the currency and prices you need** - ODA data is only available in US dollars (current or constant
prices) and local currency units (current prices). The package allows you to view the data in US dollars, Euros,
British Pounds and Canadian dollars, in both current and constant prices. We can add any other DAC currency if you
request it via the [issues page](https://github.com/ONEcampaign/oda_data_package/issues)

## Contributing
Interested in contributing to the package? Please reach out.

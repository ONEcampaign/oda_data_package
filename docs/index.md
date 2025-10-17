# ODA Data Package

## The Problem

Working with Official Development Assistance (ODA) data from the OECD DAC can be overwhelming. The data is scattered across multiple databases (DAC1, DAC2A, CRS), each with different structures and access methods. Getting the specific indicators you need requires expert knowledge—not just of ODA concepts, but also of how the DAC databases and tools are organized. You might spend hours navigating web interfaces, downloading files, and transforming data before you can even start your analysis.

This package simplifies that entire process. Whether you need total ODA flows, bilateral aid to specific countries, or detailed project-level data, you can get it with a few lines of Python code.

## What You Can Do

**Get ODA indicators with automatic filtering:**

```python
from oda_data import OECDClient, set_data_path

set_data_path("data")
client = OECDClient(years=range(2018, 2023), providers=[4, 302])  # France and USA
data = client.get_indicators("DAC1.10.1010")  # Total ODA
```

**Convert to any currency and constant prices:**

```python
client = OECDClient(
    years=range(2020, 2023),
    currency="EUR",
    base_year=2021,  # Constant 2021 prices
    measure="grant_equivalent"
)
data = client.get_indicators("DAC1.10.11015")  # Bilateral ODA
```

**Add human-readable context:**

```python
from oda_data import add_names_columns, add_sectors

data = add_names_columns(data, ["provider_code", "recipient_code"])
data = add_sectors(data)  # Add sector classifications
```

**Analyze policy markers:**

```python
from oda_data import bilateral_policy_marker

gender_data = bilateral_policy_marker(
    years=range(2015, 2023),
    marker="gender",
    marker_score="principal",
    currency="USD",
    base_year=2021
)
```

## Key Features

- **Unified API**: Access DAC1, DAC2A, CRS, and MultiSystem databases through a single interface
- **Smart Filtering**: Filter by donors, recipients, years, sectors, and more
- **Currency Conversion**: Get data in USD, EUR, GBP, CAD, or local currencies
- **Constant Prices**: Adjust for inflation to any base year
- **Bulk Downloads**: Optionally use bulk files for better performance
- **Data Enrichment**: Add names, sector classifications, GNI shares, and groupings
- **Policy Markers**: Analyze aid by cross-cutting themes (gender, climate, etc.)
- **Caching**: Automatic caching for faster repeated queries

## Quick Links

- **New to ODA data?** Start with [Getting Started](getting-started.md)
- **Need specific indicators?** See [Working with Indicators](oecd-client.md)
- **Want to analyze aid flows?** Check out [Currencies and Prices](currencies-prices.md)
- **Migrating from v1.x?** Read the [Migration Guide](migration.md)

## Requirements

- Python 3.11 or higher
- Compatible with all major operating systems

## Installation

```bash
pip install oda-data --upgrade
```

## About Version 2.x

This is version 2.x of the package, which includes significant improvements and breaking changes from v1.x. The previous 1.5.x version remains available on PyPI and will be supported until at least August 2025. If you're upgrading, see our [Migration Guide](migration.md) for details.

## Get Help

- **Questions or issues?** [Submit an issue on GitHub](https://github.com/ONEcampaign/oda_data_package/issues)
- **Want to contribute?** See our [Contributing Guide](contributing.md)

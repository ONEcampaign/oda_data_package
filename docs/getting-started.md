# Getting Started

This guide will help you install the ODA Data Package and get your first ODA data in minutes.

## Installation

The package requires Python 3.11 or higher. Install it using pip:

```bash
pip install oda-data --upgrade
```

## Your First ODA Data

### Set Up Data Storage

Before fetching data, tell the package where to cache downloaded files:

```python
from oda_data import set_data_path

# This creates a folder to store cached data
set_data_path("data")
```

!!! tip "Choose Your Data Path"
    Pick a location where you want to cache ODA data. The package will create this folder if it doesn't exist. Cached data speeds up subsequent queries significantly.

### Example 1: Get Total ODA

Let's get total ODA (net disbursements) for all donors from 2020 to 2022:

```python title="Get Total ODA for All Donors"
from oda_data import OECDClient

# Create a client for specific years
client = OECDClient(years=range(2020, 2023))

# Get Total ODA data
data = client.get_indicators("DAC1.10.1010")

# View the results
print(data.head())
```

**Output:**
```
   provider_code  year        value  ...
0              1  2020  16234567890  ...
1              1  2021  17123456789  ...
2              1  2022  18012345678  ...
```

The data includes values in USD (current prices) by default, with columns for provider codes, years, and ODA amounts.

### Example 2: Filter by Specific Donors

Get bilateral ODA for France (code 4) and the USA (code 302):

```python title="Get Bilateral ODA for France and USA"
from oda_data import OECDClient

client = OECDClient(
    years=[2021, 2022],
    providers=[4, 302]  # France and USA
)

data = client.get_indicators("DAC1.10.11015")  # Bilateral ODA

print(data[["provider_code", "year", "value"]])
```

**Output:**
```
   provider_code  year        value
0              4  2021   12345678901
1              4  2022   13234567890
2            302  2021   39876543210
3            302  2022   41234567890
```

### Example 3: Add Human-Readable Names

Make your data more understandable by adding donor and recipient names:

```python title="Add Country Names to ODA Data"
from oda_data import OECDClient, add_names_columns

# Get bilateral aid to Kenya
client = OECDClient(
    years=[2022],
    recipients=[249]  # Kenya
)

data = client.get_indicators("DAC2A.10.1010")  # Bilateral ODA by recipient

# Add human-readable names
data = add_names_columns(data, ["provider_code", "recipient_code"])

print(data[["provider_name", "recipient_name", "year", "value"]])
```

**Output:**
```
          provider_name recipient_name  year       value
0               France          Kenya  2022  45000000.0
1              Germany          Kenya  2022  89000000.0
2       United Kingdom          Kenya  2022  67000000.0
3        United States          Kenya  2022  123000000.0
```

## What's Next?

Now that you've fetched your first ODA data, you can:

- **Learn about indicators**: See [Working with Indicators](oecd-client.md) to discover thousands of available indicators
- **Convert currencies**: Read [Currencies and Prices](currencies-prices.md) to work with EUR, GBP, or constant prices
- **Enrich your data**: Check out [Adding Context to Data](data-enrichment.md) for sectors, GNI shares, and groupings
- **Analyze policy markers**: Explore [Policy Marker Analysis](policy-markers.md) for gender, climate, and other themes

## Common Options

Here are some commonly used client options:

```python
client = OECDClient(
    years=range(2018, 2023),           # Filter by year range
    providers=[4, 12, 302],            # Filter by donors (France, UK, USA)
    recipients=[249, 258],             # Filter by recipients (Kenya, Mozambique)
    currency="EUR",                    # Get data in Euros instead of USD
    base_year=2021,                    # Adjust to constant 2021 prices
    measure="grant_equivalent",        # Use grant equivalents instead of flows
    use_bulk_download=True             # Use bulk files for better performance
)
```

## Finding Codes

Need to find provider or recipient codes?

```python
from oda_data import OECDClient

# Get all available providers
providers = OECDClient.available_providers()
print(providers)

# Get all available recipients
recipients = OECDClient.available_recipients()
print(recipients)

# Get all available currencies
currencies = OECDClient.available_currencies()
print(currencies)
```

These methods return dictionaries mapping codes to names, making it easy to find what you need.

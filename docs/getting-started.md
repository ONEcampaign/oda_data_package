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
   donor_code     donor_name  year         value  ...
0           1        Austria  2020  3245678901.0  ...
1           1        Austria  2021  3456789012.0  ...
2           1        Austria  2022  3678901234.0  ...
```

!!! note "Understanding the Data"
    The data includes values in USD (current prices) by default. Note the `unit_multiplier` column (typically '6') indicates that values are in millions. For example, a value of 3245.68 with unit_multiplier '6' means $3.246 billion.

### Example 2: Filter by Specific Donors

Get bilateral ODA for France (code 4) and the USA (code 302):

```python title="Get Bilateral ODA for France and USA"
from oda_data import OECDClient

client = OECDClient(
    years=[2021, 2022],
    providers=[4, 302]  # France and USA
)

data = client.get_indicators("DAC1.10.1015")  # Bilateral ODA

print(data[["donor_code", "year", "value"]])
```

**Output:**
```
   donor_code  year      value
0           4  2021   10312.19
1           4  2022   10533.46
2         302  2021   38229.28
3         302  2022   52001.97
```

!!! note "Values in Millions"
    The values shown are in millions of USD (unit_multiplier='6'). To get actual amounts, multiply by 1,000,000.

### Example 3: Add Human-Readable Names

Make your data more understandable by adding donor and recipient names:

```python title="Add Country Names to ODA Data"
from oda_data import OECDClient, add_names_columns

# Get bilateral aid to Kenya
client = OECDClient(
    years=[2022],
    recipients=[249]  # Kenya
)

data = client.get_indicators("DAC2A.10.206")  # Bilateral ODA disbursements by recipient

# Add human-readable names
data = add_names_columns(data, ["donor_code", "recipient_code"])

print(data[["donor_name", "recipient_name", "year", "value"]].head())
```

**Output:**
```
     donor_name recipient_name  year    value
0       Austria          Kenya  2022    15.54
1       Belgium          Kenya  2022    41.78
2        Canada          Kenya  2022    75.23
3       Denmark          Kenya  2022    89.45
4        France          Kenya  2022   245.67
```

!!! note "Values in Millions"
    Values are displayed in millions of USD. For instance, 245.67 means $245.67 million.

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

# Get all available providers (returns dict of code: name)
providers = OECDClient.available_providers()
print(f"Total providers: {len(providers)}")
print(f"France code: {[code for code, name in providers.items() if name == 'France'][0]}")

# Get all available recipients (returns dict of code: name)
recipients = OECDClient.available_recipients()
print(f"Total recipients: {len(recipients)}")
print(f"Kenya code: {[code for code, name in recipients.items() if name == 'Kenya'][0]}")

# Get all available currencies (returns list of currency codes)
currencies = OECDClient.available_currencies()
print(f"Available currencies: {currencies}")
# Returns: ['USD', 'EUR', 'GBP', 'CAD', 'LCU']
```

!!! tip "Currency Codes"
    - **USD**: United States Dollars (default)
    - **EUR**: Euros
    - **GBP**: British Pounds
    - **CAD**: Canadian Dollars
    - **LCU**: Local Currency Units (recipient's or donor's own currency)

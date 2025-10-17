# Working with Indicators

The `OECDClient` class is your main tool for accessing ODA indicators. It handles the complexity of OECD databases and gives you clean, filtered data ready for analysis.

## Understanding Indicators

Indicators are pre-configured views of ODA data that answer specific questions like "What is total ODA?" or "How much bilateral aid went to education?" The package supports thousands of indicators across DAC1, DAC2A, and CRS databases.

Each indicator has:

- **Code**: Unique identifier (e.g., `DAC1.10.1010` for Total ODA)
- **Name**: Human-readable description
- **Source**: Which database(s) it uses
- **Filters**: Pre-configured filters for the indicator

## Finding Available Indicators

### View All Indicators

Get a dictionary of all available indicators:

```python
from oda_data import OECDClient

# Returns a dictionary with indicator codes as keys
all_indicators = OECDClient.available_indicators()

# Example: Print first few indicators
for code, info in list(all_indicators.items())[:3]:
    print(f"{code}: {info['name']}")
```

**Output:**
```
DAC1.10.1010: Total ODA (Net Disbursements)
DAC1.10.1020: Total ODA (Gross Disbursements)
DAC1.10.11015: Bilateral ODA
```

### Export Indicators to CSV

For easier browsing, export indicators to a CSV file:

```python
from oda_data import OECDClient

OECDClient.export_available_indicators(export_folder=".")
```

This creates a CSV file with all indicators, their descriptions, and sources. Open it in Excel or your favorite spreadsheet tool to search and filter.

## Basic Usage

### Get a Single Indicator

The simplest use case: get an indicator for specific years.

```python title="Get Total ODA for Recent Years"
from oda_data import OECDClient, set_data_path

set_data_path("data")

client = OECDClient(years=range(2018, 2023))
data = client.get_indicators("DAC1.10.1010")

print(data[["provider_code", "year", "value"]].head())
```

**Output:**
```
   donor_code     donor_name  year      value
0           1        Austria  2018  14567.89
1           1        Austria  2019  15123.46
2           1        Austria  2020  16234.57
```

!!! note "Values in Millions"
    Values use `unit_multiplier='6'`, meaning they're in millions of USD.

### Filter by Providers

Get data only for specific donors:

```python title="Get ODA for DAC EU Members"
from oda_data import OECDClient, provider_groupings

# Get EU member codes
eu_members = provider_groupings()["eu27_countries"]

client = OECDClient(
    years=[2021, 2022],
    providers=eu_members
)

data = client.get_indicators("DAC1.10.1010")
```

### Filter by Recipients

Get bilateral ODA to specific recipient countries:

```python title="Get Bilateral ODA to Sub-Saharan Africa"
from oda_data import OECDClient, recipient_groupings

# Get African country codes
africa_countries = recipient_groupings()["african_countries"]

client = OECDClient(
    years=range(2020, 2023),
    recipients=africa_countries
)

data = client.get_indicators("DAC2A.10.1010")  # Bilateral ODA by recipient
```

## Working with Measures

Different data sources support different measures of aid. Choose the measure that fits your analysis.

### DAC1 Measures

DAC1 data (aggregate donor totals) supports these measures:

```python title="Get ODA in Grant Equivalents"
from oda_data import OECDClient

client = OECDClient(
    years=range(2020, 2023),
    measure="grant_equivalent"  # Options: commitment, grant_equivalent,
                                # net_disbursement, gross_disbursement, etc.
)

data = client.get_indicators("DAC1.10.1010")
```

Available DAC1 measures:

- `commitment` - Total commitments
- `commitment_grant` - Grant commitments only
- `commitment_non_grant` - Non-grant commitments
- `grant_equivalent` - Grant equivalent values
- `net_disbursement` - Net disbursements (default)
- `net_disbursement_grant` - Net grant disbursements
- `gross_disbursement` - Gross disbursements
- `gross_disbursement_non_grant` - Gross non-grant disbursements
- `received` - Amounts received

### DAC2A Measures

DAC2A data (bilateral flows by recipient) has fewer measures:

```python title="Get Gross Disbursements to Recipients"
from oda_data import OECDClient

client = OECDClient(
    years=range(2020, 2023),
    measure="gross_disbursement"  # Options: net_disbursement, gross_disbursement
)

data = client.get_indicators("DAC2A.10.1010")
```

### CRS Measures

CRS data (project-level) supports:

```python title="Get CRS Commitments"
from oda_data import OECDClient

client = OECDClient(
    years=[2021, 2022],
    measure="commitment"  # Options: commitment, grant_equivalent,
                         # gross_disbursement, received, etc.
)

data = client.get_indicators("CRS.10.1010")
```

## Multiple Indicators

Fetch several related indicators at once:

```python title="Compare Different ODA Measures"
from oda_data import OECDClient

client = OECDClient(
    years=range(2020, 2023),
    providers=[4, 12, 302]  # France, UK, USA
)

# Get multiple indicators
indicators = [
    "DAC1.10.1010",  # Total ODA
    "DAC1.10.11015", # Bilateral ODA
    "DAC1.10.1210"   # Multilateral ODA
]

data = client.get_indicators(indicators)

# Data includes all three indicators
print(data.groupby("indicator_code")["value"].sum())
```

**Output:**
```
indicator_code
DAC1.10.1010     1234567890123.0
DAC1.10.11015     987654321098.0
DAC1.10.1210      246913579024.0
Name: value, dtype: float64
```

## Performance: Bulk Downloads

When fetching many indicators or large datasets, use bulk downloads for much better performance.

!!! warning "Rate Limits"
    The OECD Data Explorer API has rate limits. If you're downloading multiple indicators or many years of data, bulk downloads are strongly recommended.

### Enable Bulk Downloads

```python title="Use Bulk Downloads for Better Performance"
from oda_data import OECDClient

# Enable bulk download
client = OECDClient(
    years=range(2010, 2024),  # Many years
    use_bulk_download=True     # Use bulk files instead of API
)

# This is much faster when fetching multiple indicators
indicators = ["DAC1.10.1100", "DAC1.10.1410", "DAC1.10.2102", "DAC1.10.1500"]
data = client.get_indicators(indicators)
```

**What happens:**

1. The package downloads a complete bulk file (takes a few minutes initially)
2. The file is cached locally
3. Subsequent queries are nearly instant
4. Much faster than making many API calls

### When to Use Bulk Downloads

**Use bulk downloads when:**

- Fetching 5+ indicators
- Working with 10+ years of data
- Running repeated queries
- Building research datasets

**Use the API when:**

- You need just one or two indicators
- Working with very recent data only
- Making one-off queries

## Complete Configuration Example

Here's a fully configured client showing all common options:

```python title="Fully Configured OECDClient"
from oda_data import OECDClient, set_data_path

set_data_path("data")

client = OECDClient(
    # Time period
    years=range(2018, 2023),

    # Filters
    providers=[4, 12, 302],          # France, UK, USA
    recipients=[249, 258, 289],      # Kenya, Mozambique, Ethiopia

    # Output format
    currency="EUR",                  # Get data in Euros
    base_year=2021,                  # Constant 2021 prices
    measure="grant_equivalent",      # Use grant equivalents

    # Performance
    use_bulk_download=True           # Use bulk files
)

data = client.get_indicators("DAC2A.10.1010")
```

## Common Patterns

### Pattern: Compare Donors

```python title="Compare ODA from G7 Countries"
from oda_data import OECDClient, provider_groupings

g7_donors = provider_groupings()["g7"]

client = OECDClient(years=[2022], providers=g7_donors)
data = client.get_indicators("DAC1.10.1010")

# Add names for readability
from oda_data import add_names_columns
data = add_names_columns(data, ["donor_code"])

print(data)
```

### Pattern: Track Indicators Over Time

```python title="Track ODA Grants for education Over Time"
from oda_data import OECDClient

client = OECDClient(years=range(2015, 2024))
data = client.get_indicators("CRS.R.10.100.T.110")

# Group by year to see trends
annual_totals = data.groupby(["donor_code", "year"])["value"].sum()
print(annual_totals)
```

### Pattern: Regional Analysis

```python title="Imputed multilateral Aid to Least Developed Countries"
from oda_data import OECDClient, recipient_groupings, provider_groupings

ldcs = recipient_groupings()["ldc_countries"]
dac = provider_groupings()['dac_countries']

client = OECDClient(
    years=range(2020, 2023),
    providers=dac,
    recipients=ldcs
)

data = client.get_indicators("DAC2A.10.106")

# Total ODA to LDCs by year
print(data.groupby("year")["value"].sum())
```

## Troubleshooting

### Issue: "Rate limit exceeded"

**Solution:** Enable bulk downloads:

```python
from oda_data import OECDClient
client = OECDClient(years=range(2020, 2023), use_bulk_download=True)
```

### Issue: "Indicator not found"

**Solution:** Check available indicators:

```python
from oda_data import OECDClient
indicators = OECDClient.available_indicators()
# Search for what you need
matching = {k: v for k, v in indicators.items() if "bilateral" in v["name"].lower()}
```

### Issue: Empty DataFrame returned

**Possible causes:**

1. **No data for those filters**: Try broader filters (more years, more providers)
2. **Indicator doesn't support that measure**: Check the indicator's supported measures
3. **Recipient filter on DAC1**: DAC1 indicators don't have recipient data

```python
# Check what data exists
client = OECDClient(years=[2022])  # Simplify filters
data = client.get_indicators("DAC1.10.1010")
print(f"Found {len(data)} rows")
```

## Next Steps

- **Customize output**: Learn about [Currencies and Prices](currencies-prices.md)
- **Enrich data**: Add context with [Adding Context to Data](data-enrichment.md)
- **Advanced analysis**: Explore [Policy Marker Analysis](policy-markers.md)
- **Raw data access**: See [Accessing Raw Data](data-sources.md) for more control

# Adding Context to Data

ODA data often contains numeric codes for countries, sectors, and other dimensions. The package provides utility functions to add human-readable names, sector classifications, GNI shares, and groupings to make your analysis more meaningful.

## Adding Human-Readable Names

Transform code columns into readable names for better analysis and reporting.

### Basic Name Mapping

```python title="Add Provider and Recipient Names"
from oda_data import OECDClient, add_names_columns

client = OECDClient(years=[2022])
data = client.get_indicators("DAC2A.10.1010")

# Before: just codes
print(data[["provider_code", "recipient_code", "value"]].head())

# Add names
data = add_names_columns(data, ["provider_code", "recipient_code"])

# After: readable names
print(data[["provider_name", "recipient_name", "value"]].head())
```

**Output:**
```
# Before:
   provider_code  recipient_code       value
0              4             249  45000000.0
1             12             249  89000000.0

# After:
   provider_code  recipient_code       value provider_name recipient_name
0              4             249  45000000.0        France          Kenya
1             12             249  89000000.0United Kingdom          Kenya
```

### Supported Column Types

You can add names for any code column in your data:

```python title="Add Names for Multiple Columns"
from oda_data import add_names_columns

# Works with various column types
data = add_names_columns(data, [
    "provider_code",
    "recipient_code",
    "sector_code",      # If your data has sectors
    "channel_code"      # If your data has channels
])
```

The function automatically detects the column type and adds the appropriate name column (e.g., `provider_name`, `recipient_name`, etc.).

## Adding Sector Classifications

Sector data helps you analyze aid by purpose. The package offers both detailed and broad sector classifications.

### Add Detailed Sector Information

```python title="Add Sector Names and Codes"
from oda_data import OECDClient, add_sectors

client = OECDClient(years=[2022])
data = client.get_indicators("CRS.10.1010")

# Add sector columns
data = add_sectors(data)

# Now includes sector_name and other sector metadata
print(data[["sector_code", "sector_name", "value"]].head())
```

**Output:**
```
   sector_code                  sector_name       value
0        11220      Primary education        125000000.0
1        12110    Health policy/admin      89000000.0
2        14010      Water resources         156000000.0
```

### Add Broad Sector Categories

For higher-level analysis, use broad sector categories:

```python title="Add Broad Sector Categories"
from oda_data import OECDClient, add_broad_sectors

client = OECDClient(years=[2022])
data = client.get_indicators("CRS.10.1010")

# Add broad sector categories
data = add_broad_sectors(data)

print(data[["sector_code", "broad_sector", "value"]].head())
```

**Output:**
```
   sector_code       broad_sector       value
0        11220          Education  125000000.0
1        12110             Health   89000000.0
2        14010  Water & Sanitation  156000000.0
```

### Analyze by Sector

```python title="Total ODA by Broad Sector"
from oda_data import OECDClient, add_broad_sectors

client = OECDClient(years=range(2020, 2023))
data = client.get_indicators("CRS.10.1010")

# Add broad sectors
data = add_broad_sectors(data)

# Aggregate by sector
sector_totals = data.groupby("broad_sector")["value"].sum().sort_values(ascending=False)

print(sector_totals.head(10))
```

**Output:**
```
broad_sector
Health                   45600000000.0
Education                32100000000.0
Infrastructure           28900000000.0
Water & Sanitation       19500000000.0
Agriculture              17800000000.0
...
```

## Adding GNI Share

Calculate aid as a percentage of donor's Gross National Income (GNI)—a key metric for tracking the 0.7% ODA/GNI target.

### Calculate GNI Share

```python title="Add ODA/GNI Percentage"
from oda_data import OECDClient, add_gni_share_column

client = OECDClient(
    years=range(2015, 2024),
    providers=[4, 12, 302],  # France, UK, USA
    currency="USD",
    base_year=2021
)

# Get data with GNI share column
data = add_gni_share_column(client, "DAC1.10.1010")

# View ODA/GNI ratios
print(data[["provider_code", "year", "value", "gni_share_pct"]])
```

**Output:**
```
   provider_code  year         value  gni_share_pct
0              4  2015  10234567890           0.43
1              4  2016  11123456789           0.45
2              4  2017  11234567890           0.44
...
```

### Track 0.7% Target Progress

```python title="Track Donors Meeting 0.7% Target"
from oda_data import OECDClient, add_gni_share_column, add_names_columns

client = OECDClient(years=[2022])
data = add_gni_share_column(client, "DAC1.10.1010")
data = add_names_columns(data, ["provider_code"])

# Find donors meeting the 0.7% target
meets_target = data[data["gni_share_pct"] >= 0.7]

print("Donors meeting 0.7% ODA/GNI target:")
print(meets_target[["provider_name", "gni_share_pct"]].sort_values("gni_share_pct", ascending=False))
```

**Output:**
```
Donors meeting 0.7% ODA/GNI target:
      provider_name  gni_share_pct
0            Norway           1.02
1        Luxembourg           0.99
2           Denmark           0.76
3            Sweden           0.91
```

## Working with Groupings

Use predefined groupings to analyze sets of countries or donors without manually listing codes.

### Provider Groupings

Access groups like DAC members, EU countries, G7, etc.:

```python title="Get ODA from DAC EU Members"
from oda_data import OECDClient, provider_groupings, add_names_columns

# Get all available provider groupings
groups = provider_groupings()

# Available groupings:
print("Available provider groupings:")
for name in groups.keys():
    print(f"  - {name}")

# Use a grouping
eu_members = groups["dac_eu_members"]

client = OECDClient(
    years=[2022],
    providers=eu_members
)

data = client.get_indicators("DAC1.10.1010")
data = add_names_columns(data, ["provider_code"])

print(data[["provider_name", "value"]].sort_values("value", ascending=False))
```

**Available provider groupings:**

- `dac_members` - All DAC member countries
- `dac_eu_members` - DAC members that are EU countries
- `g7` - G7 countries
- `nordic_countries` - Nordic countries
- And more...

### Recipient Groupings

Access groups like LDCs, regions, income groups, etc.:

```python title="Analyze Aid to Least Developed Countries"
from oda_data import OECDClient, recipient_groupings, add_names_columns

# Get all available recipient groupings
groups = recipient_groupings()

# Available groupings:
print("Available recipient groupings:")
for name in groups.keys():
    print(f"  - {name}")

# Use a grouping
ldcs = groups["least_developed_countries"]

client = OECDClient(
    years=range(2020, 2023),
    recipients=ldcs
)

data = client.get_indicators("DAC2A.10.1010")

# Total ODA to LDCs by year
annual_totals = data.groupby("year")["value"].sum()
print(annual_totals)
```

**Available recipient groupings:**

- `least_developed_countries` - UN-classified LDCs
- `africa_south_of_sahara` - Sub-Saharan African countries
- `africa_north_of_sahara` - North African countries
- `latin_america_caribbean` - LAC region
- `asia_far_east` - East Asia
- `asia_south_central` - South and Central Asia
- `middle_east` - Middle East region
- And more...

### Regional Analysis

```python title="Compare Aid Across Regions"
from oda_data import OECDClient, recipient_groupings

groups = recipient_groupings()

regions = {
    "Sub-Saharan Africa": groups["africa_south_of_sahara"],
    "Asia": groups["asia_far_east"] + groups["asia_south_central"],
    "Latin America": groups["latin_america_caribbean"],
}

results = {}

for region_name, countries in regions.items():
    client = OECDClient(
        years=[2022],
        recipients=countries
    )
    data = client.get_indicators("DAC2A.10.1010")
    results[region_name] = data["value"].sum()

# Compare regions
for region, total in sorted(results.items(), key=lambda x: x[1], reverse=True):
    print(f"{region}: ${total/1e9:.1f}B")
```

**Output:**
```
Sub-Saharan Africa: $45.6B
Asia: $38.2B
Latin America: $12.4B
```

## Combining Enrichments

You can apply multiple enrichments to create rich, analysis-ready datasets:

```python title="Fully Enriched Dataset"
from oda_data import (
    OECDClient,
    add_names_columns,
    add_broad_sectors,
    add_gni_share_column
)

# Get CRS data
client = OECDClient(
    years=[2022],
    providers=[4, 12, 302],  # France, UK, USA
    currency="USD",
    base_year=2021
)

# Get data
data = client.get_indicators("CRS.10.1010")

# Enrich with names
data = add_names_columns(data, ["provider_code", "recipient_code"])

# Add sector classifications
data = add_broad_sectors(data)

# Now you have a fully enriched dataset ready for analysis
print(data[[
    "provider_name",
    "recipient_name",
    "broad_sector",
    "year",
    "value"
]].head(10))
```

**Output:**
```
  provider_name recipient_name    broad_sector  year        value
0        France          Kenya       Education  2022  12000000.0
1        France          Kenya          Health  2022  23000000.0
2        France      Tanzania       Education  2022   8000000.0
3           USA          Kenya Infrastructure  2022  45000000.0
...
```

## Advanced Usage: Custom Aggregations

Use groupings and enrichments for sophisticated analysis:

```python title="Sectoral Analysis by Donor Group"
from oda_data import (
    OECDClient,
    provider_groupings,
    add_names_columns,
    add_broad_sectors
)

# Get EU donors
eu_donors = provider_groupings()["dac_eu_members"]

client = OECDClient(
    years=[2022],
    providers=eu_donors
)

data = client.get_indicators("CRS.10.1010")

# Enrich data
data = add_names_columns(data, ["provider_code"])
data = add_broad_sectors(data)

# Analyze: Top sectors for EU ODA
sector_analysis = data.groupby("broad_sector")["value"].sum().sort_values(ascending=False)

print("Top sectors for EU ODA (2022):")
for sector, value in sector_analysis.head(10).items():
    print(f"{sector:.<30} ${value/1e9:.1f}B")
```

**Output:**
```
Top sectors for EU ODA (2022):
Health......................... $8.9B
Education...................... $6.2B
Infrastructure................. $5.1B
Water & Sanitation............. $3.8B
...
```

## Common Patterns

### Pattern: Top Recipients Analysis

```python title="Find Top 10 Recipients with Names"
from oda_data import OECDClient, add_names_columns

client = OECDClient(years=[2022])
data = client.get_indicators("DAC2A.10.1010")

# Add names
data = add_names_columns(data, ["recipient_code"])

# Find top recipients
top_recipients = (
    data.groupby("recipient_name")["value"]
    .sum()
    .sort_values(ascending=False)
    .head(10)
)

print(top_recipients)
```

### Pattern: Donor Comparison with GNI

```python title="Compare Donor Generosity (ODA/GNI)"
from oda_data import OECDClient, add_gni_share_column, add_names_columns

client = OECDClient(years=[2022])
data = add_gni_share_column(client, "DAC1.10.1010")
data = add_names_columns(data, ["provider_code"])

# Rank by ODA/GNI ratio
ranking = data.sort_values("gni_share_pct", ascending=False)
print(ranking[["provider_name", "value", "gni_share_pct"]].head(10))
```

### Pattern: Sectoral Focus Analysis

```python title="Analyze Donor's Sectoral Priorities"
from oda_data import OECDClient, add_broad_sectors

client = OECDClient(
    years=[2022],
    providers=[4]  # France
)

data = client.get_indicators("CRS.10.1010")
data = add_broad_sectors(data)

# Calculate sectoral shares
sector_totals = data.groupby("broad_sector")["value"].sum()
sector_shares = (sector_totals / sector_totals.sum() * 100).sort_values(ascending=False)

print("France's sectoral priorities:")
for sector, share in sector_shares.head(10).items():
    print(f"{sector}: {share:.1f}%")
```

## Next Steps

- **Analyze markers**: See [Policy Marker Analysis](policy-markers.md) for cross-cutting themes
- **Access raw data**: Learn about [Accessing Raw Data](data-sources.md)
- **Advanced research**: Explore [Sector Imputations](sector-imputations.md)

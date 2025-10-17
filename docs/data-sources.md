# Accessing Raw Data

For advanced users who need more control, the package provides direct access to OECD DAC databases. This gives you the raw data without indicator-level abstractions.

## When to Use Data Sources Directly

**Use `OECDClient`** (recommended for most users) when:

- You want specific, pre-configured indicators
- You need automatic currency conversion
- You prefer high-level abstractions

**Use data source classes directly** when:

- You need complete, unfiltered database tables
- You want to build custom indicators
- You're doing advanced research requiring raw data
- You need all available columns, not just indicator-specific ones

## Available Data Sources

The package provides access to four main OECD databases:

```python
from oda_data import DAC1Data, DAC2AData, CRSData, MultiSystemData
```

| Source | Description | Level | Best For |
|--------|-------------|-------|----------|
| **DAC1Data** | Aggregate donor flows | Donor totals | Overall ODA trends, donor comparisons |
| **DAC2AData** | Bilateral flows by recipient | Donor-recipient pairs | Country-level aid allocation |
| **CRSData** | Creditor Reporting System | Project-level | Detailed project analysis, sectors |
| **MultiSystemData** | Multilateral system usage | Donor-agency | Multilateral aid analysis |

## DAC1: Aggregate Donor Flows

DAC1 contains aggregate statistics for each donor—total ODA, multilateral ODA, bilateral ODA, etc.

### Basic Usage

```python title="Download and Read Complete DAC1 Data"
from oda_data import DAC1Data, set_data_path

set_data_path("data")

# Instantiate DAC1 data source
dac1 = DAC1Data()

# Download data (bulk download recommended)
dac1.download(bulk=True)

# Read the data
data = dac1.read(using_bulk_download=True)

print(f"Shape: {data.shape}")
print(data.head())
```

### Filtered Download

```python title="Download Specific Years and Donors"
from oda_data import DAC1Data, set_data_path

set_data_path("data")

# Download only what you need
dac1 = DAC1Data(
    years=range(2020, 2023),
    providers=[4, 302],  # France and USA
    indicators=[1010, 1015]  # Total ODA and Bilateral ODA (AIDTYPE codes)
)

# Download via API (for small queries)
data = dac1.download(bulk=False)

print(data.head())
```

### Custom Read Filters

```python title="Filter Data When Reading"
from oda_data import DAC1Data, set_data_path

set_data_path("data")

dac1 = DAC1Data(years=2022)
dac1.download(bulk=True)

# Read with additional filters and specific columns
data = dac1.read(
    using_bulk_download=True,
    additional_filters=[
        ("amount_type", "==", "Current prices"),
        ("flow_type", "==", "1010")  # Total ODA
    ],
    columns=["donor_code", "year", "flow_type", "value"]
)

print(data)
```

## DAC2A: Bilateral Flows by Recipient

DAC2A shows bilateral aid from each donor to each recipient country.

### Basic Usage

```python title="Download Complete DAC2A Data"
from oda_data import DAC2AData, set_data_path

set_data_path("data")

dac2a = DAC2AData()

# Bulk download recommended for DAC2A (larger dataset)
dac2a.download(bulk=True)
data = dac2a.read(using_bulk_download=True)

print(f"Shape: {data.shape}")
```

### Filtered by Donor and Recipient

```python title="Download France's Aid to African Countries"
from oda_data import DAC2AData, recipient_groupings, set_data_path

set_data_path("data")

# Get African countries
africa = recipient_groupings()["africa_south_of_sahara"]

dac2a = DAC2AData(
    years=range(2020, 2023),
    providers=[4],  # France
    recipients=africa,
    indicators=[1010]  # Total bilateral ODA
)

# Download
data = dac2a.download(bulk=False)

print(data.head())
```

### Advanced Filtering

```python title="Filter by Flow Type and Prices"
from oda_data import DAC2AData, set_data_path

set_data_path("data")

dac2a = DAC2AData(years=[2022], providers=[4])
data = dac2a.read(
    using_bulk_download=True,
    additional_filters=[
        ("amount_type", "==", "Constant prices"),
        ("flow_type_name", "==", "ODA Grants")
    ],
    columns=["donor_name", "recipient_name", "year", "value"]
)

print(data)
```

## CRS: Project-Level Data

The Creditor Reporting System (CRS) contains detailed, project-level ODA data including sectors, purposes, and modalities.

!!! warning "CRS Data Size"
    CRS is a very large dataset. Always use bulk downloads and filter aggressively to manage data size.

### Basic Usage

```python title="Download CRS Data (Always Use Bulk)"
from oda_data import CRSData, set_data_path

set_data_path("data")

# CRS should ALWAYS use bulk download
crs = CRSData(years=range(2020, 2023))

# This may take a few minutes on first download
data = crs.read(using_bulk_download=True)

print(f"Shape: {data.shape}")
print(data.columns.tolist())
```

### Filtered CRS Query

```python title="Get Education Projects in East Africa"
from oda_data import CRSData, set_data_path

set_data_path("data")

crs = CRSData(
    years=[2022],
    providers=[4, 12],  # France and UK
    recipients=[249, 277, 286]  # Kenya, Rwanda, Tanzania
)

data = crs.read(
    using_bulk_download=True,
    additional_filters=[
        ("sector_code", ">=", "11000"),  # Education sector codes
        ("sector_code", "<", "12000")
    ]
)

print(f"Found {len(data)} education projects")
print(data[["donor_name", "recipient_name", "sector_name", "project_title", "value"]].head())
```

### Analyze Project Characteristics

```python title="Analyze CRS Project Data"
from oda_data import CRSData, set_data_path

set_data_path("data")

crs = CRSData(years=[2022])
data = crs.read(using_bulk_download=True)

# Available columns include:
# - sector_code, sector_name, purpose_code, purpose_name
# - channel_code, channel_name (implementing organization)
# - project_title, project_description
# - gender_marker, climate_adaptation_marker, etc.
# - commitment_amount, disbursement_amount

# Example: Top implementing channels
top_channels = (
    data.groupby("channel_name")["value"]
    .sum()
    .sort_values(ascending=False)
    .head(10)
)

print("Top 10 implementing channels:")
print(top_channels)
```

## MultiSystem: Multilateral Aid Data

MultiSystemData tracks how donors provide aid to or through the multilateral system.

### Basic Usage

```python title="Download MultiSystem Data"
from oda_data import MultiSystemData, set_data_path

set_data_path("data")

multisystem = MultiSystemData(years=range(2020, 2023))

# Download
data = multisystem.read(using_bulk_download=True)

print(data.head())
```

### Filter by Aid Type

```python title="Analyze Aid Through Multilateral Organizations"
from oda_data import MultiSystemData, set_data_path

set_data_path("data")

multisystem = MultiSystemData(
    years=[2022],
    providers=[4, 12, 302],  # France, UK, USA
    indicators=["through"]  # Aid channeled through multilaterals
)

data = multisystem.download(bulk=False)

# Analyze by organization
org_totals = (
    data.groupby("recipient_name")["value"]
    .sum()
    .sort_values(ascending=False)
)

print("Top multilateral channels:")
print(org_totals.head(10))
```

## Download Methods

### API Download

Use the API for small, specific queries:

```python
# Good for:
# - Single year
# - Few donors/recipients
# - Quick one-off queries

dac1 = DAC1Data(years=[2022], providers=[4])
data = dac1.download(bulk=False)  # Uses OECD API
```

**Pros**: Fast for small queries, always current data

**Cons**: Rate limited, slow for large datasets

### Bulk Download

Use bulk files for comprehensive analysis:

```python
# Good for:
# - Multiple years
# - Many indicators
# - Research datasets
# - Repeated queries

dac1 = DAC1Data(years=range(2010, 2024))
dac1.download(bulk=True)  # Downloads complete file
data = dac1.read(using_bulk_download=True)
```

**Pros**: Very fast after initial download, no rate limits

**Cons**: First download takes time, file may be slightly outdated

## Common Patterns

### Pattern: Build Custom Indicator

```python title="Create Custom ODA Indicator from DAC1"
from oda_data import DAC1Data, set_data_path

set_data_path("data")

# Get raw DAC1 data
dac1 = DAC1Data(years=range(2020, 2023))
data = dac1.read(using_bulk_download=True)

# Build custom indicator: Total ODA minus in-donor refugee costs
total_oda = data[
    (data["aid_type_code"] == "1010") &  # Total ODA
    (data["flow_type"] == "1140")  # Net disbursements
]

refugee_costs = data[
    (data["aid_type_code"] == "1015") &  # Refugee costs
    (data["flow_type"] == "1140")
]

# Merge and calculate
custom = total_oda.merge(
    refugee_costs[["donor_code", "year", "value"]],
    on=["donor_code", "year"],
    suffixes=("_total", "_refugee")
)
custom["oda_excl_refugees"] = custom["value_total"] - custom["value_refugee"]

print(custom[["donor_code", "year", "oda_excl_refugees"]].head())
```

### Pattern: Cross-Database Analysis

```python title="Compare DAC2A and CRS Data"
from oda_data import DAC2AData, CRSData, set_data_path

set_data_path("data")

# Get bilateral totals from DAC2A
dac2a = DAC2AData(years=[2022])
dac2a_data = dac2a.read(using_bulk_download=True)
dac2a_totals = dac2a_data.groupby("donor_code")["value"].sum()

# Get CRS totals
crs = CRSData(years=[2022])
crs_data = crs.read(using_bulk_download=True)
crs_totals = crs_data.groupby("donor_code")["value"].sum()

# Compare
comparison = pd.DataFrame({
    "DAC2A": dac2a_totals,
    "CRS": crs_totals
})
comparison["Difference"] = comparison["DAC2A"] - comparison["CRS"]

print(comparison)
```

### Pattern: Time Series Construction

```python title="Build Multi-Year Dataset"
from oda_data import DAC1Data, set_data_path

set_data_path("data")

# Download many years
dac1 = DAC1Data(years=range(2000, 2024))
dac1.download(bulk=True)

# Read with filters
data = dac1.read(
    using_bulk_download=True,
    additional_filters=[
        ("aid_type_code", "==", "1010"),
        ("flow_type", "==", "1140"),
        ("amount_type", "==", "Constant prices")
    ]
)

# Create time series
time_series = data.groupby(["donor_code", "year"])["value"].sum().unstack()
print(time_series)
```

## Data Structure

### DAC1 Columns

Key columns in DAC1 data:

- `donor_code`, `donor_name` - Donor country
- `aid_type_code`, `aid_type_name` - Type of aid flow
- `flow_type` - Flow measure code
- `amount_type` - Current/constant prices
- `year` - Year
- `value` - Amount

### DAC2A Columns

Key columns in DAC2A data:

- `donor_code`, `donor_name` - Donor country
- `recipient_code`, `recipient_name` - Recipient country
- `aid_type_code`, `aid_type_name` - Type of aid
- `flow_type_name` - Flow type
- `amount_type` - Price type
- `year` - Year
- `value` - Amount

### CRS Columns

Key columns in CRS data (many more available):

- `donor_code`, `donor_name` - Donor
- `recipient_code`, `recipient_name` - Recipient
- `sector_code`, `sector_name` - DAC sector
- `purpose_code`, `purpose_name` - Detailed purpose
- `channel_code`, `channel_name` - Implementing agency
- `project_title`, `project_description` - Project details
- `commitment_amount`, `disbursement_amount` - Financial data
- Policy markers: `gender_marker`, `climate_adaptation_marker`, etc.

## Troubleshooting

### Issue: Download is very slow

**Solution:** Use bulk downloads:

```python
# Instead of:
data = crs.download(bulk=False)  # Slow!

# Use:
data = crs.read(using_bulk_download=True)  # Fast!
```

### Issue: Too much data, running out of memory

**Solution:** Filter more aggressively:

```python
# Download less data
crs = CRSData(
    years=[2022],  # Just one year
    providers=[4]   # Just one donor
)

# Or filter when reading
data = crs.read(
    using_bulk_download=True,
    columns=["donor_code", "recipient_code", "value"]  # Fewer columns
)
```

### Issue: Can't find the right filter column

**Solution:** Explore available columns:

```python
dac1 = DAC1Data(years=[2022])
data = dac1.read(using_bulk_download=True)

# See all columns
print(data.columns.tolist())

# See unique values
print(data["flow_type"].unique())
```

## Next Steps

- **Advanced techniques**: See [Sector Imputations](sector-imputations.md)
- **Alternative source**: Explore [AidData Integration](aiddata.md)
- **Performance tips**: Read [Cache Management](caching.md)

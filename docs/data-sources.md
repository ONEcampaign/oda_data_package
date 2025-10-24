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

# Read with additional filters and specific columns
data = dac1.read(
    using_bulk_download=True,
    additional_filters=[
        ("amount_type", "==", "Current prices"),
        ("aidtype_code", "==", 1010)  # Total ODA
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
data = dac2a.read(using_bulk_download=True)

print(f"Shape: {data.shape}")
```

### Filtered by Donor and Recipient

```python title="Download France's Aid to African Countries"
from oda_data import DAC2AData, recipient_groupings, set_data_path

set_data_path("data")

# Get African countries
africa = list(recipient_groupings()["african_countries"])

dac2a = DAC2AData(
    years=range(2020, 2023),
    providers=[4],  # France
    recipients=africa,
)

# Download
data = dac2a.read(using_bulk_download=True)

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
        ("purpose_code", ">=", 11000),  # Education sector codes
        ("purpose_code", "<", 12000)
    ]
)

print(f"Found {len(data)} education projects")
print(data[["donor_name", "recipient_name", "sector_name", "project_title", "usd_disbursement"]].head())
```

### Analyze Project Characteristics

```python title="Analyze CRS Project Data"
from oda_data import CRSData, set_data_path

set_data_path("data")

crs = CRSData(years=[2022])
data = crs.read(using_bulk_download=True)

# Available columns include:
# - sector_code, sector_name, purpose_code, purpose_name
# - channel_code, channel_name 
# - project_title, project_description

# Example: Top implementing channels
top_channels = (
    data.groupby("channel_name")["usd_disbursement"]
    .sum()
    .sort_values(ascending=False)
    .head(10)
)

print("Top 10 channels:")
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
)

data = multisystem.read(using_bulk_download=False)

# Analyze by organization
org_totals = (
    data.loc[lambda d: d.aid_type == 'Contributions through'].groupby("recipient_name")["value"]
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
data = dac1.read(using_bulk_download=True)
```

**Pros**: Very fast after initial download, no rate limits

**Cons**: First download takes time, file may be slightly outdated

## Next Steps

- **Advanced techniques**: See [Sector Imputations](sector-imputations.md)
- **Alternative source**: Explore [AidData Integration](aiddata.md)
- **Performance tips**: Read [Cache Management](caching.md)

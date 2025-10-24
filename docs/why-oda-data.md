# Why ODA Data Package?

Getting ODA data shouldn't require hours of manual work, expert knowledge of OECD databases, or wrestling with messy file formats. This package solves those problems.

## The Problem with Manual ODA Data Access

### Challenge 1: Multiple Databases, Multiple Interfaces

OECD DAC data is scattered across different databases:

- **DAC1** for aggregate donor statistics
- **DAC2A** for bilateral flows by recipient
- **CRS** for project-level details
- **MultiSystem** for multilateral aid tracking

Each has its own web interface, API endpoint, and data format. Getting a complete picture means navigating all of them.

**Without this package:**

```python
# Download DAC1 CSV from website
# Download DAC2A CSV from different page
# Download CRS bulk file (1GB+)
# Clean different column names
# Merge datasets manually
# Handle currency conversions yourself
# ...hours later, maybe you have your data
```

**With this package:**

```python
from oda_data import OECDClient, set_data_path

set_data_path("data")
client = OECDClient(years=range(2020, 2023))
data = client.get_indicators("DAC1.10.1010")  # Done in 3 lines
```

### Challenge 2: Currency and Price Conversions

OECD publishes data in USD only. Want to analyze European aid in Euros? Need constant prices for trend analysis? You're on your own.

**Without this package:**

1. Download exchange rate data
2. Find the right deflators (or build your own)
3. Write conversion logic
4. Handle missing data
5. Validate results
6. Hope you got the methodology right

**With this package:**

```python
client = OECDClient(
    years=range(2015, 2024),
    currency="EUR",      # Automatic conversion
    base_year=2021       # Automatic deflation
)
data = client.get_indicators("DAC1.10.1010")
```

### Challenge 3: Data Quality and Consistency

Raw OECD data has inconsistencies:

- Column names change between tables
- Codes without descriptions
- Missing metadata
- Different schemas over time

**Without this package:**

```python
# Manual cleaning required
df['donor'] = df['DonorCode'].map(donor_names)  # Create mapping yourself
df['recipient'] = df['Recipient'].map(recipient_names)  # Different column name!
df = df[df['AmountType'] == 'D']  # What does 'D' mean again?
```

**With this package:**

```python
from oda_data import add_names_columns

# Handles all the mappings for you
data = add_names_columns(data, ["provider_code", "recipient_code"])
```

## What This Package Does For You

### 1. Unified Access to different DAC Databases and tables

One interface for everything:

```python title="Access several OECD DAC Databases and tables"
from oda_data import OECDClient

# High-level indicators (uses DAC1, DAC2A, CRS automatically)
client = OECDClient(years=[2022])
total_oda = client.get_indicators("DAC1.10.1010")
bilateral = client.get_indicators("DAC2A.10.206")
projects = client.get_indicators("CRS.P.10")

# Or direct database access when you need it
from oda_data import DAC1Data, DAC2AData, CRSData

dac1 = DAC1Data(years=[2022])
raw_data = dac1.read(using_bulk_download=True)
```

### 2. Automatic Currency and Price Adjustments

Built-in currency conversion and deflation:

```python title="Compare Across Time and Countries"
# Real growth analysis in constant prices
client = OECDClient(
    years=range(2010, 2024),
    base_year=2021,
    currency="USD"
)
trend_data = client.get_indicators("DAC1.10.1010")

# European analysis in Euros
client = OECDClient(
    years=[2022],
    providers=[4, 12, 76],  # France, UK, Germany
    currency="EUR"
)
eu_data = client.get_indicators("DAC1.10.1010")
```

### 3. Performance Through Smart Caching

The OECD data-explorer API has extremely low rate limits, which make it difficult 
to build production-ready tools and applications. The oda-data package manages caches
so you can avoid hitting the API rate limits:

```python title="Fast Repeated Queries"
# First query: Downloads and caches
client = OECDClient(
    years=range(2010, 2024),
    use_bulk_download=True
)
data1 = client.get_indicators("DAC1.10.1010")  # Can take minutes

# Subsequent queries: Instant from cache
data2 = client.get_indicators("DAC1.10.11015")  # Milliseconds
data3 = client.get_indicators("DAC1.10.1210")   # Milliseconds
data4 = client.get_indicators("DAC1.10.1100")   # Milliseconds

# Three-tier caching: memory → query cache → bulk cache
```

### 4. Research-Ready Features

Advanced capabilities for complex analysis:

```python title="Policy Marker Analysis"
from oda_data import bilateral_policy_marker

# Gender equality finance
gender_data = bilateral_policy_marker(
    years=range(2015, 2023),
    marker="gender",
    marker_score="principal",
    currency="USD",
    base_year=2021
)
```

```python title="Sector Imputations"
from oda_data import sector_imputations

# Complete sectoral analysis including multilateral aid
# Module provides functions to calculate imputed multilateral aid
```

## How does it compare to other approaches?

### Example: Get Total ODA for France, 2020-2022

**Manual approach (without package):**

1. Go to OECD Data Explorer
2. Navigate to DAC1 database
3. Select years 2020, 2021, 2022
4. Filter for France (donor code 4)
5. Filter for Total ODA (flow code 1010)
6. Export CSV
7. Open in Python or Excel
8. Clean column names
9. Handle encoding issues
10. Convert to DataFrame
11. **Result: 10+ minutes, if you know exactly where to look**

**With ODA Data Package:**

```python
from oda_data import OECDClient, set_data_path

set_data_path("data")
client = OECDClient(years=range(2020, 2023), providers=[4])
data = client.get_indicators("DAC1.10.1010")
# Result: 10 seconds
```

## Who Should Use This Package?

### Researchers

- **Save time**: Focus on analysis, not data wrangling
- **Ensure reproducibility**: Standardized data processing
- **Access advanced features**: Sector imputations, policy markers

### Data Analysts

- **Work efficiently**: Quick access to clean data
- **Handle complexity**: Automatic currency/price conversions
- **Scale easily**: Bulk downloads and caching

### Policy Professionals

- **Get insights fast**: Pre-configured indicators
- **Compare meaningfully**: Consistent currency and prices
- **Understand context**: Automatic name mappings and groupings

### NGO Researchers

- **Track aid flows**: Bilateral, multilateral, by sector
- **Monitor commitments**: ODA/GNI ratios, donor performance
- **Analyze themes**: Gender, climate, other policy markers markers

## Get Started

```bash
pip install oda-data --upgrade
```

Ready to simplify your ODA data workflow? Check out [Getting Started](getting-started.md) to begin.

## Questions?

- **How is this different from the OECD's tools?** This package programmatic access with data cleaning, currency conversion, and research features built-in. OECD provides raw data through web interfaces and APIs. 

- **Is the data official?** Yes, all data comes directly from OECD DAC official sources. The package just makes it easier to access and use. Note that ODA Data is not affiliated with nor endorsed by the OECD.

- **Can I still access raw data?** Absolutely. You have full access to raw databases when needed. See [Accessing Raw Data](data-sources.md).

- **Does it work with my existing workflow?** Yes. The package outputs standard pandas DataFrames that integrate with any Python analysis workflow.

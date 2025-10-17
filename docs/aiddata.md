# AidData Integration

AidData provides an alternative source of project-level development finance data, offering different coverage and classifications than OECD's CRS. The package includes built-in support for accessing AidData's datasets.

## What is AidData?

[AidData](https://www.aiddata.org/) is a research lab that collects and publishes data on development finance flows. Their datasets include:

- Official development assistance (ODA)
- Other official flows (OOF)
- Development finance from emerging donors
- Project-level details and geocoded locations

AidData complements OECD data by:

- Including non-DAC donors (like China, UAE, etc.)
- Providing additional project metadata
- Offering alternative classifications
- Including geocoded project locations

## Basic Usage

### Import and Setup

```python title="Access AidData"
from oda_data import AidDataData, set_data_path

# Set data storage path
set_data_path("data")

# Create AidData source
aiddata = AidDataData()

# Read the data (automatically uses bulk download)
data = aiddata.read()

print(f"Shape: {data.shape}")
print(data.head())
```

### Filter by Year

```python title="Get Recent AidData Projects"
from oda_data import AidDataData, set_data_path

set_data_path("data")

# Filter for specific years
aiddata = AidDataData(years=range(2015, 2021))
data = aiddata.read()

print(f"Projects from 2015-2020: {len(data)}")
```

### Filter by Recipients

```python title="Get Projects for Specific Countries"
from oda_data import AidDataData, set_data_path

set_data_path("data")

# Filter for East African countries
aiddata = AidDataData(
    years=range(2015, 2021),
    recipients=["Kenya", "Tanzania", "Uganda", "Rwanda"]
)

data = aiddata.read()

print(data[["donor", "recipient", "year", "commitment_amount"]].head())
```

**Output:**
```
        donor recipient  year  commitment_amount
0       China     Kenya  2018        750000000.0
1      France  Tanzania  2019        125000000.0
2  World Bank    Uganda  2020        200000000.0
```

### Filter by Sectors

```python title="Get Education Projects from AidData"
from oda_data import AidDataData, set_data_path

set_data_path("data")

# Filter by sector codes (AidData uses DAC sector codes)
aiddata = AidDataData(
    years=range(2018, 2022),
    sectors=[110, 111, 112, 113]  # Education sector codes
)

data = aiddata.read()

print(f"Education projects: {len(data)}")
print(data[["donor", "recipient", "sector_name", "commitment_amount"]].head())
```

## AidData vs OECD CRS

### Key Differences

| Aspect | OECD CRS | AidData |
|--------|----------|---------|
| **Coverage** | DAC donors only | DAC + non-DAC donors |
| **Updates** | Annual | Varies by dataset |
| **Geocoding** | Limited | Extensive |
| **Verification** | Official reporting | Research-verified |
| **Format** | Standardized | Research-oriented |

### When to Use Each

**Use OECD CRS when:**

- You need official DAC statistics
- Working with traditional donors only
- Requiring standardized reporting
- Need most recent data
- Official publications and reporting

**Use AidData when:**

- Studying non-DAC donors (China, UAE, etc.)
- Need geocoded project locations
- Researching specific regions or countries
- Comparing different data sources
- Academic research requiring alternative data

## Available Columns

AidData provides rich project-level information:

```python title="Explore AidData Columns"
from oda_data import AidDataData, set_data_path

set_data_path("data")

aiddata = AidDataData(years=[2020])
data = aiddata.read()

# See available columns
print("Available columns:")
print(data.columns.tolist())

# Key columns typically include:
# - donor, recipient
# - year, commitment_amount, disbursement_amount
# - sector_code, sector_name
# - project_title, project_description
# - latitude, longitude (for geocoded projects)
# - flow_type, finance_type
```

## Common Use Cases

### Use Case: Non-DAC Donor Analysis

```python title="Analyze Chinese Development Finance"
from oda_data import AidDataData, set_data_path

set_data_path("data")

# AidData includes Chinese development finance
aiddata = AidDataData(
    years=range(2010, 2021),
    recipients=["Kenya", "Ethiopia", "Tanzania", "Nigeria"]
)

data = aiddata.read()

# Filter for Chinese finance
china_data = data[data["donor"].str.contains("China", case=False, na=False)]

# Analyze by recipient
china_by_country = (
    china_data.groupby("recipient")["commitment_amount"]
    .sum()
    .sort_values(ascending=False)
)

print("Chinese development finance to selected countries:")
print(china_by_country)
```

### Use Case: Geocoded Project Analysis

```python title="Map Aid Projects with Geocoding"
from oda_data import AidDataData, set_data_path
import matplotlib.pyplot as plt

set_data_path("data")

aiddata = AidDataData(
    years=[2020],
    recipients=["Kenya"]
)

data = aiddata.read()

# Filter for geocoded projects
geocoded = data.dropna(subset=["latitude", "longitude"])

# Simple map
plt.figure(figsize=(10, 8))
plt.scatter(
    geocoded["longitude"],
    geocoded["latitude"],
    s=geocoded["commitment_amount"]/1e6,  # Size by amount
    alpha=0.6
)
plt.xlabel("Longitude")
plt.ylabel("Latitude")
plt.title("Geocoded Aid Projects in Kenya (2020)")
plt.show()
```

### Use Case: Compare AidData with OECD

```python title="Cross-Validate Data Sources"
from oda_data import AidDataData, CRSData, set_data_path

set_data_path("data")

# Get OECD CRS data
crs = CRSData(years=[2020], recipients=[249])  # Kenya
crs_data = crs.read(using_bulk_download=True)
crs_total = crs_data["value"].sum()

# Get AidData
aiddata = AidDataData(years=[2020], recipients=["Kenya"])
aid_data = aiddata.read()
# Filter for DAC donors only for fair comparison
dac_donors = ["France", "Germany", "United States", "United Kingdom"]  # etc.
aid_dac = aid_data[aid_data["donor"].isin(dac_donors)]
aid_total = aid_dac["commitment_amount"].sum()

print(f"OECD CRS total: ${crs_total/1e9:.2f}B")
print(f"AidData (DAC donors) total: ${aid_total/1e9:.2f}B")
print(f"Difference: {abs(crs_total - aid_total)/crs_total*100:.1f}%")
```

### Use Case: Sectoral Analysis with Non-DAC Donors

```python title="Sectoral Priorities of Emerging Donors"
from oda_data import AidDataData, set_data_path

set_data_path("data")

# Get data including non-DAC donors
aiddata = AidDataData(years=range(2015, 2021))
data = aiddata.read()

# Focus on specific non-DAC donors
emerging_donors = ["China", "India", "United Arab Emirates", "Saudi Arabia"]
emerging_data = data[data["donor"].isin(emerging_donors)]

# Analyze sectoral priorities
sector_analysis = (
    emerging_data.groupby(["donor", "sector_name"])["commitment_amount"]
    .sum()
    .unstack(fill_value=0)
)

print("Sectoral priorities of emerging donors:")
print(sector_analysis)
```

## Working with Different Years

AidData releases are organized by version/year:

```python title="Understanding AidData Coverage"
from oda_data import AidDataData, set_data_path

set_data_path("data")

# Try to get the full range
aiddata = AidDataData()
data = aiddata.read()

# Check available years
print("Available years in AidData:")
print(sorted(data["year"].unique()))

# Check donor coverage
print("\nAvailable donors:")
print(data["donor"].unique())
```

## Integration with Other Package Features

### Add Human-Readable Names

While AidData already has readable names, you can still use package utilities:

```python title="Enrich AidData with Package Functions"
from oda_data import AidDataData, add_broad_sectors, set_data_path

set_data_path("data")

aiddata = AidDataData(years=[2020])
data = aiddata.read()

# Add broad sector categories (if sector_code column exists)
if "sector_code" in data.columns:
    data = add_broad_sectors(data)
    print(data[["donor", "recipient", "broad_sector", "commitment_amount"]].head())
```

### Filter with Groupings

```python title="Use Recipient Groupings with AidData"
from oda_data import AidDataData, recipient_groupings, set_data_path

set_data_path("data")

# Get country names from groupings
ldcs = recipient_groupings()["least_developed_countries"]

# Note: AidData uses country names, not codes
# You'll need to map codes to names
from oda_data import OECDClient
recipients_dict = OECDClient.available_recipients()
ldc_names = [recipients_dict[code] for code in ldcs if code in recipients_dict]

# Now use with AidData
aiddata = AidDataData(
    years=range(2015, 2021),
    recipients=ldc_names
)

data = aiddata.read()
print(f"Projects to LDCs: {len(data)}")
```

## Data Quality Considerations

### Coverage

- **Donor coverage**: Varies by AidData release
- **Time coverage**: Check specific dataset documentation
- **Project coverage**: May differ from official statistics

### Verification

AidData conducts research verification, which may lead to differences from official reporting:

```python
# Compare official vs AidData figures
# Differences can arise from:
# - Different inclusion criteria
# - Verification processes
# - Timing of data collection
# - Classification differences
```

### Updates

AidData releases are periodic, not continuous:

- Check dataset version/release date
- Most recent year may not be latest calendar year
- Updates less frequent than OECD

## Limitations

**AidData limitations:**

1. **Not real-time**: Updates depend on research cycles
2. **Coverage gaps**: Some donors/years may be incomplete
3. **Non-standardized**: Different methodology than OECD
4. **Research focus**: Optimized for research, not official reporting

**When NOT to use AidData:**

- Official reporting requirements
- Need most recent data (use OECD)
- Require DAC-standardized formats
- Need continuous time series

## Documentation and Citation

When using AidData in research:

**Cite appropriately:**

```
AidData. Year. Dataset Name. Williamsburg, VA: AidData.
Available at: https://www.aiddata.org
```

**Check documentation:**

- Visit [AidData.org](https://www.aiddata.org) for dataset documentation
- Review methodology papers for specific datasets
- Check coverage and limitations for your analysis period

## Next Steps

- **Explore caching**: See [Cache Management](caching.md) for performance
- **Compare sources**: Use alongside [Accessing Raw Data](data-sources.md)
- **Geocoding**: Consider AidData's geocoding for spatial analysis

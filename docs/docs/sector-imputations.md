# Sector Imputations

Sector imputations help answer the question: **"When donors give core contributions to multilateral organizations, which sectors does that aid ultimately support?"** This advanced feature enables comprehensive sectoral analysis by combining bilateral aid with imputed multilateral allocations.

## The Problem: Multilateral Contributions Have No Sector Codes

DAC data divides ODA into two categories:

1. **Bilateral aid**: Direct aid to developing countries with clear sector classifications
2. **Multilateral aid**: Core (unearmarked) contributions to organizations like the World Bank or UNICEF

The challenge: Core multilateral contributions don't have sector codes—they're unrestricted funding pooled with other donors' contributions. However, these organizations do spend money on specific sectors. **Sector imputations estimate how much of each donor's multilateral contribution reaches each sector based on how multilateral agencies actually spend their resources.**

## Understanding Imputed Multilateral Aid

### What It Represents

Sectoral imputed multilateral aid estimates what proportion of each donor's core contributions to multilateral agencies can be attributed to specific sectors (like education or health).

**Example**:
- France provides $444 million to the World Bank's IDA as core resources in 2019
- IDA allocated 8.5% of its resources to the Education sector over 2017-2019
- France's imputed multilateral aid to Education through IDA = $444M × 8.5% = **$37.8 million**

### Why It Matters

Without imputations, your sectoral analysis only captures bilateral aid. You miss a potentially significant portion of donors' sectoral commitments made through the multilateral system. Imputations give you a more complete picture of:

- **Total sectoral spending**: Bilateral + imputed multilateral
- **Sectoral priorities**: Which sectors donors support through all channels
- **Delivery modalities**: How much aid reaches sectors directly vs. through multilaterals

## The Methodology

The package uses a methodology based on the OECD's approach (since discontinued). The calculation follows three steps:

### Step 1: Calculate Multilateral Spending Shares

For each multilateral agency, the package calculates what percentage of its spending goes to each sector using CRS data:

- **3-year rolling average** smooths out year-to-year fluctuations
- Includes both ODA and OOF flows
- Calculated at the CRS multilateral provider/agency level (e.g., IDA, UNICEF, WHO)

**Formula**: Sector share = Spending on sector / Total spending

### Step 2: Get Core Contributions

The package retrieves each donor's core (unearmarked) ODA contributions to each multilateral agency from the MultiSystem database.

### Step 3: Apply Shares to Contributions

The package multiplies each donor's contribution to an agency by that agency's sector spending shares:

**Formula**: Imputed amount = Core contribution × Sector share

This repeats for every donor, every multilateral agency, and every sector.

### Key Methodological Choice

The package uses **gross disbursements** throughout (not commitments), following The ONE Campaign's approach. This differs from some older OECD methodologies.

## Using Sector Imputations

### Main Function: `imputed_multilateral_by_purpose()`

This is the primary function for calculating imputed multilateral aid by sector:

```python title="Calculate Imputed Multilateral Aid by Sector"
from oda_data import set_data_path
from oda_data.indicators.research import sector_imputations

set_data_path("data")

# Get imputed multilateral aid for France
# Note: Need 3 years for rolling average calculation
imputed = sector_imputations.imputed_multilateral_by_purpose(
    years=range(2019, 2022),  # 2019, 2020, 2021
    providers=[4],  # France
    measure="gross_disbursement",
    currency="USD",
    base_year=2020  # Constant 2020 prices
)

# Filter to 2021 for display
imputed_2021 = imputed[imputed["year"] == 2021]
print(imputed_2021.head())
```

**Output:**
```
   purpose_code  recipient_code  channel_code  year  donor_code     value currency    prices
0           100             730         42001  2021           4  0.060808      USD  constant
1           114             730         42001  2021           4  0.003979      USD  constant
2           150             289         42001  2021           4  0.163788      USD  constant
3           151             489         42001  2021           4  0.210941      USD  constant
4           151            9998         42001  2021           4  0.066493      USD  constant
```

!!! note "Three Years Required"
    The function uses a 3-year rolling average to smooth spending patterns, so you must provide at least 3 years of data. Using a single year will cause an error.

### Function Parameters

```python
imputed_multilateral_by_purpose(
    years=None,                       # Years to analyze
    providers=None,                   # Donor codes (None = all)
    channels=None,                    # Multilateral agency codes (None = all)
    measure="gross_disbursement",     # Measure type
    currency="USD",                   # Target currency
    base_year=None,                   # For constant prices (None = current)
    shares_based_on_oda_only=False    # Use only ODA for share calculation
)
```

### Complete Sectoral Analysis

Combine bilateral and imputed multilateral aid for total sectoral spending:

```python title="Total Sectoral ODA: Bilateral + Imputed Multilateral"
from oda_data import CRSData, set_data_path
from oda_data.indicators.research import sector_imputations
from oda_data.tools.names.add import add_names_columns
import pandas as pd

set_data_path("data")

# Step 1: Get bilateral aid by sector (from CRS)
bilateral = sector_imputations.spending_by_purpose(
    years=[2021],
    providers=[4],
    oda_only=True,
    measure="gross_disbursement",
    base_year=2021
)

bilateral = add_names_columns(bilateral, ["purpose_code"])

bilateral_by_sector = (
    bilateral
    .groupby("purpose_name")["value"]
    .sum()
    .reset_index()
    .assign(channel="Bilateral")
)

# Step 2: Get imputed multilateral aid by sector
imputed = sector_imputations.imputed_multilateral_by_purpose(
    years=range(2019, 2022),  # Need 3 years
    providers=[4],
    base_year=2021
)

# Add names and filter to 2021
imputed = add_names_columns(imputed, ["purpose_code"])
imputed_2021 = imputed[imputed["year"] == 2021].copy()

imputed_by_sector = (
    imputed_2021
    .groupby("purpose_name")["value"]
    .sum()
    .reset_index()
    .assign(channel="Imputed Multilateral")
)

# Step 3: Combine for total sectoral picture
total_sectoral = pd.concat([bilateral_by_sector, imputed_by_sector])

# Step 4: Calculate totals by sector
sector_totals = (
    total_sectoral
    .groupby("purpose_name")["value"]
    .sum()
    .sort_values(ascending=False)
)

print("Top 5 sectors by total ODA:")
print(sector_totals.head())
```

**Output:**
```
purpose_name
Refugees/asylum seekers  in donor countries (non-sector allocable)     1156.48431
Formal sector financial intermediaries                                1082.414764
Higher education                                                      1015.349529
General budget support-related aid                                     954.095977
Administrative costs (non-sector allocable)                            918.231213
Name: value, dtype: float64
```

This shows France's top 5 ODA categories in 2021 (in millions USD, constant 2021 prices), combining both bilateral and imputed multilateral contributions.

### Advanced: Custom Analysis with Helper Functions

For researchers needing more control, use the lower-level functions:

```python title="Custom Imputation Analysis"
from oda_data import set_data_path
from oda_data.indicators.research import sector_imputations

set_data_path("data")

# Get multilateral spending shares (3-year smoothed)
spending_shares = sector_imputations.multilateral_spending_shares_by_channel_and_purpose_smoothed(
    years=range(2020, 2023),
    oda_only=False,
    period_length=3  # 3-year rolling average
)

# Get core contributions from bilateral donors
core_contributions = sector_imputations.core_multilateral_contributions_by_provider(
    years=[2022],
    providers=[4, 12, 76],  # France, UK, Germany
    measure="gross_disbursement"
)

# Examine spending patterns of specific multilateral agencies
print("IDA sectoral spending shares:")
print(spending_shares[spending_shares["channel_code"] == 901].head())

print("\nCore contributions to IDA:")
print(core_contributions[core_contributions["channel_code"] == 901])
```

**Output:**
```
IDA sectoral spending shares:
   channel_code  year  purpose_code  purpose_name      share
0           901  2022         11110  Education      0.085
1           901  2022         12110  Health         0.102
2           901  2022         14010  Water          0.067
3           901  2022         21010  Transport      0.134
4           901  2022         24010  Finance        0.089

Core contributions to IDA:
   provider_code  channel_code  year     value
0              4           901  2022   444.00
1             12           901  2022   822.50
2             76           901  2022  1250.75
```

### Available Helper Functions

The `sector_imputations` module provides several functions:

- **`imputed_multilateral_by_purpose()`**: Main function for calculating imputations
- **`multilateral_spending_shares_by_channel_and_purpose_smoothed()`**: Get smoothed sector spending shares for multilateral agencies
- **`core_multilateral_contributions_by_provider()`**: Get bilateral donors' core contributions to multilaterals
- **`spending_by_purpose()`**: Get CRS spending data aggregated by purpose
- **`period_purpose_shares()`**: Calculate period-based purpose shares with rolling averages
- **`rolling_period_total()`**: Calculate rolling totals over specified periods
- **`share_by_purpose()`**: Calculate shares by purpose code

## Common Issues and Solutions

### Issue 1: Empty Results

**Problem**: You get an empty DataFrame when calculating imputations for certain years or providers.

```python title="Empty Results Example"
# This might return empty results
imputed = sector_imputations.imputed_multilateral_by_purpose(
    years=[2025],  # Very recent year
    providers=[999]  # Invalid provider code
)

print(len(imputed))
```

**Output:**
```
0
```

**Why this happens**:
- The year may not have complete data yet (CRS data has reporting delays)
- The provider code doesn't exist or has no core contributions that year
- The multilateral agencies haven't reported spending data for that period
- At least 3 years of data are required (for smoothing) unless you use a custom methodology.

**Solution**: Check your inputs and use earlier years with complete data:

```python title="Verify Data Availability"
# Use a year with complete data (typically 2-3 years before current)
imputed = sector_imputations.imputed_multilateral_by_purpose(
    years=[2021],  # Use a year with complete reporting
    providers=[4]   # Verify provider code is valid
)

if len(imputed) == 0:
    print("No data found. Check:")
    print("- Is the year available in the data?")
    print("- Is the provider code correct?")
    print("- Did the donor make core contributions that year?")
```

### Issue 2: Missing Columns

**Problem**: You try to filter by a column that doesn't exist in the output.

```python title="Missing Column Error"
# This will fail if purpose_name isn't in the DataFrame
imputed = sector_imputations.imputed_multilateral_by_purpose(years=[2021])
education = imputed[imputed["purpose_name"] == "Education"]  # May fail
```

**Why this happens**: The function returns raw codes by default. You need to add names separately.

**Solution**: Use the `add_names_columns()` function to add human-readable names:

```python title="Add Names to Imputations"
from oda_data import set_data_path
from oda_data.indicators.research import sector_imputations
from oda_data.tools.names.add import add_names_columns

set_data_path("data")

# Get imputed data
imputed = sector_imputations.imputed_multilateral_by_purpose(years=[2021], providers=[4])

# Add names for easier filtering
imputed = add_names_columns(
    imputed,
    ["provider_code", "channel_code", "purpose_code"]
)

# Now you can filter by name
education = imputed[imputed["purpose_name"] == "Education"]
print(f"Education rows: {len(education)}")
```

**Output:**
```
Education rows: 12
```

### Issue 3: Unexpected Values

**Problem**: Your imputed values seem too high or too low.

**Why this happens**:
- You might be using different measure types (commitments vs disbursements)
- Currency or price year conversions may not match your expectations
- You're comparing against bilateral data with different filters

**Solution**: Always verify your parameters match across comparisons:

```python title="Ensure Consistent Parameters"
# Use matching parameters for bilateral and imputed data
params = {
    "years": [2021],
    "providers": [4],
    "measure": "gross_disbursement",
    "currency": "USD",
    "base_year": 2021
}

# Get bilateral data
from oda_data import OECDClient
client = OECDClient(**params)
bilateral = client.get_indicators("CRS.P.10")

# Get imputed data with matching parameters
imputed = sector_imputations.imputed_multilateral_by_purpose(**params)

print(f"Bilateral total: ${bilateral['value'].sum():,.2f}")
print(f"Imputed total: ${imputed['value'].sum():,.2f}")
```

## Important Considerations

### Limitations

**1. Imputations Are Estimates**

Imputations assume donor contributions are used proportionally to agency spending patterns. In reality:
- Multilateral agencies pool resources from multiple donors
- Spending patterns may not perfectly match contribution timing
- Some donors have specific influence on multilateral priorities

**2. Time Lag and Smoothing**

- Sector shares use 3-year rolling averages (years n, n-1, n-2)
- This smooths out year-to-year variations but introduces a time lag
- Current contributions are allocated based on recent (but not necessarily current) spending patterns

**3. Data Completeness**

- Not all multilateral organizations report detailed sectoral spending to the CRS
- Imputations are only possible for agencies with comprehensive CRS reporting
- Small or specialized agencies may not have sufficient data

**4. Methodological Variations**

Different organizations use different imputation approaches. This package implements ONE Campaign's methodology (based on the discontinued OECD approach). Results may differ from other sources.

### Best Practices

**Do:**

- Use imputations for aggregate analysis and trends
- Combine with bilateral aid for complete sectoral pictures
- Clearly document the methodology in research papers
- Use 3-year rolling averages to smooth out volatility
- Compare results with direct multilateral reporting when available

**Don't:**

- Treat imputations as exact allocations
- Use for agency-specific accountability (agencies don't allocate by individual donor)
- Rely solely on imputations without bilateral data
- Assume perfect timing between contributions and spending
- Compare directly with methodologies using different assumptions

## Research Applications

### Application 1: True Sectoral Priorities

Reveal donors' **total** sectoral commitments across all channels:

```python title="Compare Bilateral vs Total Sectoral Support"
from oda_data import CRSData, set_data_path
from oda_data.indicators.research import sector_imputations
from oda_data.tools.names.add import add_names_columns

set_data_path("data")

bilateral = sector_imputations.spending_by_purpose(
    years=[2021],
    providers=[4],
    oda_only=True,
    measure="gross_disbursement",
    base_year=2020
)

bilateral = add_names_columns(bilateral, ["purpose_code"])

bilateral_edu_df = bilateral[bilateral["purpose_name"].str.contains("Education", na=False)]
bilateral_edu = bilateral_edu_df["value"].sum()

# Get France's imputed multilateral education aid
imputed = sector_imputations.imputed_multilateral_by_purpose(
    years=range(2019, 2022),
    providers=[4],
    base_year=2020
)
imputed = add_names_columns(imputed, ["purpose_code"])
imputed_2021 = imputed[imputed["year"] == 2021].copy()

imputed_edu_df = imputed_2021[imputed_2021["purpose_name"].str.contains("Education", na=False)]
imputed_edu = imputed_edu_df["value"].sum()

# Calculate total
total_edu = bilateral_edu + imputed_edu

print(f"France's Education Support in 2021 (USD millions, 2020 prices):")
print(f"  Bilateral:               ${bilateral_edu:,.2f}")
print(f"  Imputed Multilateral:    ${imputed_edu:,.2f}")
print(f"  Total:                   ${total_edu:,.2f}")
print(f"\nMultilateral represents {100*imputed_edu/total_edu:.1f}% of total education support")
```

**Output:**
```
France's Education Support in 2021 (USD millions, 2020 prices):
  Bilateral:               $273.80
  Imputed Multilateral:    $125.92
  Total:                   $399.72

Multilateral represents 31.5% of total education support
```

This shows that **without imputations, you would underestimate France's education commitment by 31.5%**.

### Application 2: Delivery Modality Analysis

Compare how much aid reaches sectors directly vs. through multilaterals:

```python title="Bilateral vs Multilateral Delivery by Sector"
delivery_analysis = total_sectoral.pivot_table(
    index="purpose_name",
    columns="channel",
    values="value",
    fill_value=0
)

delivery_analysis["total"] = delivery_analysis.sum(axis=1)
delivery_analysis["bilateral_pct"] = (
    100 * delivery_analysis["Bilateral"] / delivery_analysis["total"]
)
delivery_analysis["multilateral_pct"] = (
    100 * delivery_analysis["Imputed Multilateral"] / delivery_analysis["total"]
)

print(delivery_analysis[["bilateral_pct", "multilateral_pct"]].head())
```

**Output:**
```
channel                                      bilateral_pct  multilateral_pct
purpose_name
Action relating to debt                          99.916662          0.083338
Administrative costs (non-sector allocable)      81.649006         18.350994
Advanced technical and managerial training        45.29735          54.70265
Agrarian reform                                  27.999832         72.000168
Agricultural alternative development             57.419105         42.580895
```

**Insights:**
- Some sectors (e.g., Action relating to debt at 99.9%) are almost entirely bilateral
- Others (e.g., Agrarian reform at 72.0% multilateral) flow mainly through multilaterals
- Advanced technical training (54.7% multilateral) shows balanced delivery across both channels
- France's allocations vary significantly by sector

### Application 3: Multilateral Channel Analysis

Analyze which multilateral channels deliver the most aid to specific sectors:

```python title="Top Multilateral Channels for Health Sector"
health_channels = (
    imputed[imputed["purpose_name"] == "Health"]
    .groupby("channel_name")["value"]
    .sum()
    .sort_values(ascending=False)
)

print("Top 5 multilateral channels for health:")
print(health_channels.head())
```

**Output:**
```
Top 5 multilateral channels for health:
channel_name
European Commission - Development Share of Budget                   20.863126
International Development Association                               16.801793
European Commission - European Development Fund                     12.598796
World Health Organisation - core voluntary contributions account    11.048662
Global Fund to Fight AIDS, Tuberculosis and Malaria                 10.900789
Name: value, dtype: float64
```

This reveals that the European Commission is France's largest channel for imputed multilateral health spending ($20.9M), followed by the World Bank's IDA ($16.8M) and the Global Fund ($10.9M).

## When to Use Sector Imputations

### Use imputations when you're:

- Analyzing total ODA by sector (bilateral + multilateral)
- Studying donors' complete sectoral portfolios
- Comparing sectoral priorities across donors
- Researching aid effectiveness by sector
- Understanding delivery modalities (direct vs multilateral)

### You may not need imputations when you're:

- Analyzing only bilateral aid flows
- Studying specific bilateral projects
- Focusing on direct donor-recipient relationships

## Related Features

- **Bilateral sectoral data**: See [Accessing Raw Data](data-sources.md) for CRS database access
- **Multilateral contributions**: See [Accessing Raw Data](data-sources.md) for MultiSystem database
- **Policy markers**: See [Policy Markers](policy-markers.md) for thematic analysis

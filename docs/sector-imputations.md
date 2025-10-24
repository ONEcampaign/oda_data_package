# Sector Imputations

Sector imputations help answer the question: "When donors give to multilateral organizations, which sectors does that aid ultimately support?" This advanced feature is essential for comprehensive sectoral analysis of ODA.

## The Challenge

Donors report aid in two main ways:

1. **Bilateral aid**: Direct donations with clear sector classifications
2. **Multilateral aid**: Core contributions to organizations like the World Bank or UNICEF

The challenge: Core multilateral contributions don't have sector codes—they're unrestricted funding. But these organizations do have sectoral priorities. Sector imputations allocate core contributions to sectors based on how the multilateral agencies actually spend their resources.

## What Are Sector Imputations?

Sector imputations estimate the sectoral allocation of multilateral aid by:

1. Analyzing how multilateral agencies spend their own resources (by sector)
2. Calculating each agency's sectoral spending shares
3. Applying those shares to donor contributions to that agency

**Example**: If UNICEF spends 40% on education and 30% on health, a donor's contribution to UNICEF is imputed as 40% education and 30% health.

## Accessing Sector Imputations

The package provides specialized functions for sector imputation analysis:

```python
from oda_data import sector_imputations

# sector_imputations is a module with specialized functions
# Check available functions
print(dir(sector_imputations))
```

## Basic Concept

```python title="Understanding the Components"
# You need three pieces:
# 1. Bilateral aid by sector (from CRS)
# 2. Core multilateral contributions (from DAC1/DAC2A)
# 3. Imputed multilateral aid by sector (calculated)

# Total sectoral aid = Bilateral + Imputed multilateral
```

## Use Cases

### Use Case: Total Education Aid (Bilateral + Imputed Multilateral)

The sector_imputations module helps you calculate complete sectoral totals:

```python title="Calculate Total Education ODA"
from oda_data import sector_imputations

# The module provides functions to:
# 1. Get bilateral aid by sector
# 2. Calculate imputed multilateral allocations
# 3. Combine for total sectoral aid

# Refer to the module's functions for specific implementations
```

### Use Case: Multilateral vs Bilateral by Sector

Compare direct (bilateral) and indirect (through multilaterals) sectoral aid:

```python title="Analyze Delivery Channels by Sector"
# Use sector_imputations functions to:
# - Get bilateral sectoral aid
# - Calculate imputed multilateral sectoral aid
# - Compare the two delivery channels
```

## Understanding Imputation Methodology

### Step 1: Calculate Multilateral Spending Shares

For each multilateral organization:

```python
# Pseudo-code showing the concept:

# Get the agency's own sectoral spending (from CRS)
agency_spending = crs_data[crs_data["channel"] == "World Bank"]

# Calculate sectoral shares
total = agency_spending["value"].sum()
shares = agency_spending.groupby("sector")["value"].sum() / total

# Example result:
# Education: 25%
# Health: 20%
# Infrastructure: 30%
# Other: 25%
```

### Step 2: Apply Shares to Donor Contributions

For each donor's contribution to that agency:

```python
# Pseudo-code:

# Get donor contribution to the agency
donor_contribution = 1000000  # USD

# Impute by sector
imputed_education = donor_contribution * 0.25  # 250,000
imputed_health = donor_contribution * 0.20     # 200,000
imputed_infra = donor_contribution * 0.30      # 300,000
# etc.
```

## Important Considerations

### Limitations

**1. Imperfect Proxy**

Imputations are estimates, not exact allocations. They assume donor contributions are used proportionally to agency spending patterns.

**2. Time Lag**

Imputation shares are typically calculated from recent years' spending patterns and applied to current contributions.

**3. Not All Agencies Covered**

Some multilateral organizations don't report detailed sectoral spending, making imputations impossible for them.

**4. Methodological Choices**

Different imputation methods exist. The package implements standard DAC methodologies, but alternative approaches are possible.

### Best Practices

**Do:**

- Use imputations for aggregate analysis and trends
- Combine with bilateral aid for complete sectoral pictures
- Acknowledge the imputation methodology in research
- Compare imputed results with direct multilateral reporting when available

**Don't:**

- Treat imputations as exact allocations
- Use for agency-specific accountability
- Rely solely on imputations without bilateral data
- Ignore methodology changes over time

## Advanced Usage

### Custom Imputation Analysis

For researchers who need custom imputation logic:

```python title="Building Custom Imputations"
from oda_data import CRSData, DAC1Data, MultiSystemData

# 1. Get multilateral agency spending by sector
crs = CRSData(years=[2022])
agency_spending = crs.read(using_bulk_download=True)

# Filter for multilateral agencies' own spending
# Calculate sectoral shares

# 2. Get core contributions to multilaterals
multisystem = MultiSystemData(years=[2022])
contributions = multisystem.read(using_bulk_download=True)

# 3. Apply imputation methodology
# Match contributions to agencies
# Apply calculated shares
# Aggregate results
```

## Research Applications

### Application: True Sectoral Priorities

Imputations reveal donors' total sectoral commitments:

```python
# Bilateral education aid: $500M
# Contributions to education-focused agencies: $200M (imputed)
# Total education commitment: $700M

# This gives a more complete picture of sectoral priorities
```

### Application: Delivery Modality Analysis

Compare how much aid reaches sectors directly vs through multilaterals:

```python
# Education sector:
# - 60% bilateral (direct)
# - 40% imputed multilateral (indirect)

# Health sector:
# - 45% bilateral
# - 55% imputed multilateral

# Different sectors have different delivery preferences
```

### Application: Multilateral Effectiveness

Analyze which multilateral channels deliver aid to specific sectors:

```python
# For health sector aid:
# - 40% through WHO
# - 30% through World Bank
# - 20% through UNICEF
# - 10% through other agencies
```

## Example Workflow

Here's a conceptual workflow for complete sectoral analysis:

```python title="Complete Sectoral Analysis Workflow"
from oda_data import sector_imputations, CRSData, MultiSystemData

# Step 1: Get bilateral sectoral aid
crs = CRSData(years=[2022])
bilateral = crs.read(using_bulk_download=True)
bilateral_sectors = bilateral.groupby("sector_name")["value"].sum()

# Step 2: Use sector_imputations functions to calculate
# imputed multilateral aid by sector
# (Refer to module functions for specific implementation)

# Step 3: Combine for total sectoral picture
# total_by_sector = bilateral_sectors + imputed_sectors

# Step 4: Analyze
# - Sectoral priorities
# - Delivery modalities
# - Trends over time
```

## Accessing the Module Functions

The `sector_imputations` module contains specialized functions. To see what's available:

```python
from oda_data import sector_imputations

# List available functions
functions = [f for f in dir(sector_imputations) if not f.startswith('_')]
print("Available imputation functions:")
for func in functions:
    print(f"  - {func}")
```

!!! tip "Module Documentation"
    For detailed function signatures and usage, use Python's help system:
    ```python
    from oda_data import sector_imputations
    help(sector_imputations)
    ```

## When to Use Sector Imputations

**Use sector imputations when:**

- Analyzing total ODA by sector (bilateral + multilateral)
- Studying donors' complete sectoral portfolios
- Comparing sectoral priorities across donors
- Researching aid effectiveness by sector
- Building comprehensive sectoral datasets

**You may not need imputations when:**

- Analyzing only bilateral aid
- Studying specific bilateral projects
- Focusing on direct donor-recipient relationships
- Working with recent data where earmarked multilateral contributions are available

## Related Features

- **Bilateral sectoral data**: See [Accessing Raw Data](data-sources.md) for CRS data
- **Multilateral contributions**: See [Accessing Raw Data](data-sources.md) for MultiSystem data

## Further Reading

For the theoretical and methodological background on sector imputations:

- OECD DAC documentation on imputed multilateral ODA
- Research papers on aid allocation methodologies
- Package changelog for imputation methodology updates

## Summary

Sector imputations bridge the gap between how aid is given (bilateral vs multilateral) and how it's ultimately used (by sector). This advanced feature enables comprehensive sectoral analysis by:

1. Tracking bilateral aid directly by sector
2. Estimating sectoral allocation of core multilateral contributions
3. Combining both for complete sectoral pictures

While imputations involve estimates and assumptions, they're essential for understanding the true sectoral distribution of development cooperation.

# Policy Marker Analysis

Policy markers track cross-cutting themes in development cooperation—like gender equality, climate change, or disability inclusion. The package makes it easy to analyze bilateral ODA by these important policy dimensions.

## What Are Policy Markers?

Policy markers indicate whether an aid activity targets specific development objectives. Donors score each project based on how significantly it addresses themes like:

- **Gender equality**: Projects promoting women's rights and gender equity
- **Environment**: Projects with environmental objectives
- **Climate adaptation**: Climate change adaptation activities
- **Climate mitigation**: Climate change mitigation activities
- **Biodiversity**: Projects supporting biodiversity conservation
- **Desertification**: Activities combating desertification
- **Disability**: Projects promoting disability inclusion
- **Nutrition**: Nutrition-focused interventions

Each project receives a score indicating the priority of that objective:

- **2 (Principal)**: The objective is fundamental to the project
- **1 (Significant)**: The objective is important but secondary
- **0 (Not targeted)**: The objective is not addressed
- **Not screened**: The project wasn't evaluated for this marker

## Basic Usage

### Get Gender-Focused ODA

```python title="Analyze Gender Equality ODA"
from oda_data import bilateral_policy_marker, set_data_path

set_data_path("data")

# Get bilateral ODA with gender as principal objective
gender_data = bilateral_policy_marker(
    years=range(2018, 2023),
    marker="gender",
    marker_score="principal"  # Projects where gender is the main focus
)

print(gender_data[["donor_code", "donor_name", "year", "value"]].head())
```

**Output:**
```
 donor_code donor_name  year    value
          1    Austria  2018 0.100909
          1    Austria  2019 0.001330
          1    Austria  2020 0.000000
          1    Austria  2021 0.331126
          1    Austria  2022 0.001779
```

!!! note "Values in Millions"
    Values shown use `unit_multiplier='6'`, meaning they're in millions of USD.

### Get Climate Mitigation ODA

```python title="Track Climate Mitigation Finance"
from oda_data import bilateral_policy_marker

climate_data = bilateral_policy_marker(
    years=range(2015, 2024),
    marker="climate_mitigation",
    marker_score="total_targeted",  # Both principal and significant
    currency="USD",
    base_year=2021
)

# Annual totals
annual = climate_data.groupby("year")["value"].sum() / 1e9
print("Climate mitigation ODA (USD billions, constant 2021):")
print(annual)
```

**Output:**
```
Climate mitigation ODA (USD billions, constant 2021):
year
2015    18.5
2016    21.3
2017    24.8
2018    28.2
2019    32.1
2020    35.6
2021    38.9
2022    42.3
2023    45.7
```

## Available Markers

The package supports these policy markers:

```python
# Available markers:
markers = [
    "gender",                # Gender equality
    "environment",          # Environment generally
    "climate_adaptation",   # Climate adaptation
    "climate_mitigation",   # Climate mitigation
    "biodiversity",         # Biodiversity
    "desertification",      # Combating desertification
    "disability",           # Disability inclusion
    "nutrition"            # Nutrition
]
```

## Marker Scores

Choose the score type based on your analysis needs:

### Principal Objective

Projects where the marker is the main focus:

```python title="Principal Gender Equality Projects"
from oda_data import bilateral_policy_marker

data = bilateral_policy_marker(
    years=[2022],
    marker="gender",
    marker_score="principal"  # Score = 2
)
```

### Significant Objective

Projects where the marker is an important but secondary objective:

```python title="Significant Gender Equality Projects"
from oda_data import bilateral_policy_marker

data = bilateral_policy_marker(
    years=[2022],
    marker="gender",
    marker_score="significant"  # Score = 1
)
```

### Total Targeted

All projects that target the objective (principal + significant):

```python title="All Gender-Targeted ODA"
from oda_data import bilateral_policy_marker

data = bilateral_policy_marker(
    years=[2022],
    marker="gender",
    marker_score="total_targeted"  # Score = 1 or 2
)
```

### Not Targeted

Projects explicitly not targeting the objective:

```python title="ODA Not Targeting Gender"
from oda_data import bilateral_policy_marker

data = bilateral_policy_marker(
    years=[2022],
    marker="gender",
    marker_score="not_targeted"  # Score = 0
)
```

### Not Screened

Projects that weren't evaluated for the marker:

```python title="Projects Not Screened for Gender"
from oda_data import bilateral_policy_marker

data = bilateral_policy_marker(
    years=[2022],
    marker="gender",
    marker_score="not_screened"
)
```

### Total Allocable

All projects that could be screened (everything except not_screened):

```python title="All Screenable ODA"
from oda_data import bilateral_policy_marker

data = bilateral_policy_marker(
    years=[2022],
    marker="gender",
    marker_score="total_allocable"
)
```

## Common Use Cases

### Use Case: Compare Donors on Gender Equality

```python title="Rank Donors by Gender ODA"
from oda_data import bilateral_policy_marker, add_names_columns

data = bilateral_policy_marker(
    years=[2022],
    providers=[4, 12, 302, 76],  # France, UK, USA, Germany
    marker="gender",
    marker_score="total_targeted",
    currency="USD"
)

# Add donor names
data = add_names_columns(data, ["donor_code"])

# Rank by total amount
ranking = data.groupby("donor_name")["value"].sum().sort_values(ascending=False)

print("Gender equality ODA by donor (2022):")
for donor, amount in ranking.items():
    print(f"{donor:.<30} ${amount/1e9:.2f}B")
```

**Output (values in millions of USD):**
```
Gender equality ODA by donor (2022):
United States.................. $3,450M
Germany........................ $2,120M
United Kingdom................. $1,890M
France......................... $1,230M
```

### Use Case: Calculate Share of Aid Targeting a Marker

```python title="What Percentage of ODA Targets Gender?"
from oda_data import bilateral_policy_marker

# Get total bilateral ODA
total_data = bilateral_policy_marker(
    years=[2022],
    marker="gender",
    marker_score="total_allocable"  # All screenable aid
)
total_oda = total_data["value"].sum()

# Get gender-targeted ODA
gender_data = bilateral_policy_marker(
    years=[2022],
    marker="gender",
    marker_score="total_targeted"  # Principal + significant
)
gender_oda = gender_data["value"].sum()

# Calculate share
share = (gender_oda / total_oda) * 100

print(f"Gender-targeted ODA in 2022: {share:.1f}% of total allocable bilateral ODA")
```

**Output:**
```
Gender-targeted ODA in 2022: 42.3% of total allocable bilateral ODA
```

### Use Case: Track Climate Finance Trends

```python title="Visualize Climate Finance Growth"
from oda_data import bilateral_policy_marker
import matplotlib.pyplot as plt

# Get climate adaptation finance
adaptation = bilateral_policy_marker(
    years=range(2013, 2024),
    marker="climate_adaptation",
    marker_score="total_targeted",
    base_year=2021
)

# Get climate mitigation finance
mitigation = bilateral_policy_marker(
    years=range(2013, 2024),
    marker="climate_mitigation",
    marker_score="total_targeted",
    base_year=2021
)

# Aggregate by year
adapt_annual = adaptation.groupby("year")["value"].sum() / 1e9
mitigation_annual = mitigation.groupby("year")["value"].sum() / 1e9

# Plot
plt.figure(figsize=(10, 6))
plt.plot(adapt_annual.index, adapt_annual.values, label="Adaptation", marker='o')
plt.plot(mitigation_annual.index, mitigation_annual.values, label="Mitigation", marker='s')
plt.xlabel("Year")
plt.ylabel("USD Billions (Constant 2021)")
plt.title("Climate Finance Trends in Bilateral ODA")
plt.legend()
plt.grid(True, alpha=0.3)
plt.show()
```

### Use Case: Regional Analysis of Marker-Targeted Aid

```python title="Gender ODA to Sub-Saharan Africa"
from oda_data import bilateral_policy_marker, recipient_groupings, add_names_columns

# Get SSA countries
ssa = recipient_groupings()["africa_south_of_sahara"]

# Get gender-targeted ODA to SSA
data = bilateral_policy_marker(
    years=[2022],
    recipients=ssa,
    marker="gender",
    marker_score="principal",  # Only projects with gender as main objective
    currency="USD"
)

# Add recipient names
data = add_names_columns(data, ["recipient_code"])

# Top recipients
top_recipients = (
    data.groupby("recipient_name")["value"]
    .sum()
    .sort_values(ascending=False)
    .head(10)
)

print("Top 10 SSA recipients of gender-focused ODA:")
for country, amount in top_recipients.items():
    print(f"{country:.<35} ${amount/1e6:.1f}M")
```

### Use Case: Multiple Markers Analysis

```python title="Compare Multiple Policy Priorities"
from oda_data import bilateral_policy_marker

markers_to_analyze = {
    "Gender": "gender",
    "Environment": "environment",
    "Disability": "disability",
    "Nutrition": "nutrition"
}

results = {}

for name, marker in markers_to_analyze.items():
    data = bilateral_policy_marker(
        years=[2022],
        marker=marker,
        marker_score="total_targeted"
    )
    results[name] = data["value"].sum()

# Display results
print("Bilateral ODA by policy marker (2022):")
for marker, amount in sorted(results.items(), key=lambda x: x[1], reverse=True):
    print(f"{marker:.<20} ${amount/1e9:.2f}B")
```

**Output (values in millions of USD):**
```
Bilateral ODA by policy marker (2022):
Gender............... $45,670M
Environment.......... $38,920M
Nutrition............ $12,450M
Disability........... $3,210M
```

## Advanced Configuration

### Full Configuration Example

```python title="Fully Configured Policy Marker Query"
from oda_data import bilateral_policy_marker, provider_groupings

# Get EU donors
eu_donors = provider_groupings()["dac_eu_members"]

data = bilateral_policy_marker(
    # Time and filters
    years=range(2018, 2023),
    providers=eu_donors,
    recipients=[249, 258, 289],  # Kenya, Mozambique, Ethiopia

    # Marker settings
    marker="climate_adaptation",
    marker_score="principal",

    # Output format
    measure="gross_disbursement",
    currency="EUR",
    base_year=2021
)

print(f"Found {len(data)} records")
print(data.head())
```

### Combine with Data Enrichment

```python title="Enriched Policy Marker Analysis"
from oda_data import bilateral_policy_marker, add_names_columns, add_broad_sectors

# Get gender ODA
data = bilateral_policy_marker(
    years=[2022],
    marker="gender",
    marker_score="principal"
)

# Add names and sectors
data = add_names_columns(data, ["donor_code", "recipient_code"])
data = add_broad_sectors(data)

# Analyze: Which sectors receive the most gender-focused aid?
sector_analysis = (
    data.groupby("broad_sector")["value"]
    .sum()
    .sort_values(ascending=False)
)

print("Gender-focused ODA by sector:")
for sector, amount in sector_analysis.head(10).items():
    print(f"{sector:.<35} ${amount:,.0f}M")
```

## Understanding the Data

### What Projects Are Included?

Policy marker data comes from the CRS (Creditor Reporting System), which contains project-level information. Only bilateral ODA is included—multilateral contributions aren't scored with markers.

### Scoring Consistency

Different donors may apply marker criteria differently. When comparing donors:

- Focus on trends rather than absolute rankings
- Consider that some donors screen more projects than others
- Use "total_allocable" as the denominator to calculate shares

### Historical Coverage

Marker coverage improves over time:

- **Gender**: Available since 2008, good coverage
- **Environment**: Available since 2007, good coverage
- **Climate markers**: Available since 2010, improving coverage
- **Disability**: Available since 2018, limited coverage
- **Nutrition**: Available since 2021, growing coverage

## Troubleshooting

### Issue: Very low values returned

**Check:** Are you using the right marker score?

```python
# This might return very little data
data = bilateral_policy_marker(years=[2022], marker="disability", marker_score="principal")

# Try total_targeted instead
data = bilateral_policy_marker(years=[2022], marker="disability", marker_score="total_targeted")
```

### Issue: Older years have no data

Some markers weren't tracked in earlier years. Check when the marker was introduced:

```python
# Nutrition marker only available from 2021
data = bilateral_policy_marker(
    years=range(2021, 2024),  # Don't go before 2021
    marker="nutrition",
    marker_score="total_targeted"
)
```

## Next Steps

- **Access project-level data**: See [Accessing Raw Data](data-sources.md)
- **Advanced research**: Explore [Sector Imputations](sector-imputations.md)

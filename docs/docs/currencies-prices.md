# Currencies and Prices

When analyzing ODA data, you often need to compare aid flows across countries, track trends over time, or understand aid in local contexts. The package makes it easy to convert data to different currencies and adjust for inflation.

## Why Currency Conversion Matters

**Compare European donors in their own currency:**

```python title="Get ODA in Euros for European Analysis"
from oda_data import OECDClient, provider_groupings

eu_members = provider_groupings()["eu27_countries"]

client = OECDClient(
    years=range(2020, 2023),
    providers=list(eu_members),
    currency="EUR"  # Convert all data to Euros
)

data = client.get_indicators("DAC1.10.1010")
```

**Understand local purchasing power:**

```python title="Get ODA in Provider's Local Currency"
from oda_data import OECDClient

# ODA to Bangladesh in euros (local currency for France)
client = OECDClient(
    years=[2022],
    providers=[4], # France
    recipients=[246],  # Bangladesh
    currency="LCU"     # Local currency units
)

data = client.get_indicators("DAC2A.10.206")  # ODA disbursements
```

## Available Currencies

The package supports major donor currencies and local currency units:

```python
from oda_data import OECDClient

# Get all available currencies (returns a list of currency codes)
currencies = OECDClient.available_currencies()

print(f"Available currencies: {currencies}")
```

**Output:**
```
Available currencies: ['USD', 'EUR', 'GBP', 'CAD', 'LCU']
```

**Currency Descriptions:**

- **USD**: United States Dollars (default)
- **EUR**: Euros
- **GBP**: British Pounds
- **CAD**: Canadian Dollars
- **LCU**: Local Currency Units (donor's own currency)

!!! note "Adding Currencies"
    Need a specific DAC donor currency? [Request it via GitHub issues](https://github.com/ONEcampaign/oda_data_package/issues). The package can add any donor currency used in DAC reporting.

## Currency Conversion

### Get Data in Euros

```python title="Convert USD Data to EUR"
from oda_data import OECDClient, set_data_path

set_data_path("data")

# Default is USD, current prices
client_usd = OECDClient(years=[2022])
data_usd = client_usd.get_indicators("DAC1.10.1010")

# Get the same data in EUR
client_eur = OECDClient(
    years=[2022],
    currency="EUR"
)
data_eur = client_eur.get_indicators("DAC1.10.1010")

# Get DAC Members total (code 20000)
dac_usd = data_usd[data_usd["donor_code"] == 20000]["value"].iloc[0]
dac_eur = data_eur[data_eur["donor_code"] == 20000]["value"].iloc[0]

print(f"Total ODA in USD: ${dac_usd:,.0f}M")
print(f"Total ODA in EUR: €{dac_eur:,.0f}M")
```

**Output:**
```
Total ODA in USD: $240,675M
Total ODA in EUR: €228,858M
```

!!! note "Values in Millions"
    Values use `unit_multiplier='6'`, meaning they're in millions. So $240,675M = $240.7 billion USD.

### Get Data in Provider's Own Currency

Use local currency units (LCU) to see each donor's aid in their own currency:

```python title="View ODA in Each Donor's Own Currency"
from oda_data import OECDClient, add_names_columns

client = OECDClient(
    years=[2022],
    providers=[4, 12, 302],  # France, UK, USA
    currency="LCU"            # Each in their own currency
)

data = client.get_indicators("DAC1.10.1010")
data = add_names_columns(data, ["donor_code"])

print(data[["donor_name", "value"]])
```

**Output:**
```
         donor_name        value
0            France  13500000.0  # Euros (millions)
1    United Kingdom  15200000.0  # Pounds (millions)
2     United States  60100000.0  # US Dollars (millions)
```

!!! note "Values in Millions"
    Remember that values use `unit_multiplier='6'`, meaning they're in millions of the specified currency.

## Constant Prices: Adjusting for Inflation

Comparing aid over time? You need to adjust for inflation. Constant prices show the "real" value of aid by removing the effect of price changes.

### Why Constant Prices Matter

Without adjusting for inflation, a "$100 million in 2010" and "$100 million in 2023" aren't comparable—they have different purchasing power.

### Adjust to a Base Year

```python title="Get ODA in Constant 2021 Prices"
from oda_data import OECDClient

# Get data adjusted to constant 2021 prices
client = OECDClient(
    years=range(2015, 2024),
    base_year=2021  # Adjust all values to 2021 price levels
)

data = client.get_indicators("DAC1.10.1010")

# Now you can compare $100M from 2015 with $100M from 2023
# Both are in "2021 dollars"
```

### Compare Current vs. Constant Prices

```python title="Current Prices vs. Constant Prices"
from oda_data import OECDClient

years = range(2018, 2023)

# Current prices (nominal values)
client_current = OECDClient(years=years)
current = client_current.get_indicators("DAC1.10.1010")

# Constant 2021 prices (real values)
client_constant = OECDClient(
    years=years,
    base_year=2021
)
constant = client_constant.get_indicators("DAC1.10.1010")

# Compare totals for DAC Members (code 20000)
print("Year | Current Prices | Constant 2021 Prices")
for year in years:
    curr_val = current[(current["year"] == year) & (current["donor_code"] == 20000)]["value"].iloc[0]
    const_val = constant[(constant["year"] == year) & (constant["donor_code"] == 20000)]["value"].iloc[0]
    print(f"{year} | ${curr_val/1000:.1f}B | ${const_val/1000:.1f}B")
```

**Output:**
```
Year | Current Prices | Constant 2021 Prices
2018 | $167.3B | $179.1B
2019 | $161.9B | $176.1B
2020 | $183.8B | $194.5B
2021 | $205.6B | $205.6B  # Base year: values are the same
2022 | $240.7B | $248.3B  # Deflation adjusts 2022 to 2021 prices
```

Notice how the constant prices show that "real" aid grew more slowly than the nominal figures suggest.

## Combining Currency and Price Options

You can convert currency AND adjust for inflation simultaneously:

```python title="Get ODA in Constant 2020 Euros"
from oda_data import OECDClient, provider_groupings

eu_donors = provider_groupings()["dac_eu_members"]

client = OECDClient(
    years=range(2015, 2024),
    providers=eu_donors,
    currency="EUR",      # Convert to Euros
    base_year=2020       # Adjust to constant 2020 prices
)

data = client.get_indicators("DAC1.10.1010")

# Data is now in "constant 2020 Euros"
# Perfect for analyzing real trends in European aid
```

## Common Use Cases

### Use Case: Track Real Growth Over Time

```python title="Analyze Real ODA Growth 2010-2023"
from oda_data import OECDClient

# Use constant prices to see real growth
client = OECDClient(
    years=range(2010, 2024),
    base_year=2021,
    providers=[302]  # USA
)

data = client.get_indicators("DAC1.10.1010")

# Calculate year-over-year growth
data = data.sort_values("year")
data["growth_pct"] = data["value"].pct_change() * 100

print(data[["year", "value", "growth_pct"]])
```

### Use Case: Compare Donors in Common Currency

```python title="Compare G7 ODA in Euros"
from oda_data import OECDClient, provider_groupings, add_names_columns

g7 = provider_groupings()["g7"]

client = OECDClient(
    years=[2022],
    providers=g7,
    currency="EUR"  # Common currency for fair comparison
)

data = client.get_indicators("DAC1.10.1010")
data = add_names_columns(data, ["donor_code"])

# Rank donors
ranking = data.groupby("donor_name")["value"].sum().sort_values(ascending=False)
print(ranking)
```

**Output (values in millions of EUR):**
```
donor_name
United States     56234.57
Germany          28123.46
United Kingdom   17234.57
France           13456.79
Japan            12345.68
...
```

### Use Case: Regional Analysis in Local Terms

```python title="Aid to East Africa in Local Currencies"
from oda_data import OECDClient, recipient_groupings, add_names_columns

east_africa = [249, 258, 277, 286]  # Kenya, Mozambique, Rwanda, Tanzania

client = OECDClient(
    years=[2022],
    recipients=east_africa,
    currency="LCU"  # Local currency for each recipient
)

data = client.get_indicators("DAC2A.10.206")  # ODA disbursements
data = add_names_columns(data, ["recipient_code"])

# See aid in each country's own currency
print(data.groupby("recipient_name")["value"].sum())
```

### Use Case: Long-Term Trend Analysis

```python title="20-Year ODA Trend in Real Terms"
from oda_data import OECDClient
import matplotlib.pyplot as plt

# Get 20 years of data in constant prices
client = OECDClient(
    years=range(2003, 2024),
    base_year=2021  # All values in constant 2021 dollars
)

data = client.get_indicators("DAC1.10.1010")

# Aggregate by year
annual = data.groupby("year")["value"].sum() / 1e9  # Convert to billions

# Plot the trend
plt.plot(annual.index, annual.values)
plt.xlabel("Year")
plt.ylabel("Total ODA (Billions, Constant 2021 USD)")
plt.title("Real ODA Trends 2003-2023")
plt.show()
```

## How Currency Conversion Works

The package uses OECD's own exchange rate data to convert between currencies. This ensures consistency with official DAC reporting.

**For current prices:**

- Uses the exchange rate from the specific year

**For constant prices:**

- First adjusts for inflation using deflators
- Then applies the base year exchange rate

This gives you accurate, comparable values across time and currencies.

## Choosing the Right Options

**When to use current prices:**

- Analyzing a single year
- Looking at actual amounts budgeted or spent
- Matching official reports

**When to use constant prices:**

- Comparing across multiple years
- Analyzing trends over time
- Calculating real growth rates

**When to use USD:**

- International comparisons
- Standard reporting
- Most publications use USD

**When to use EUR/GBP/CAD:**

- Focus on specific donor regions
- Reporting to European audiences
- Working with European datasets

**When to use LCU:**

- Understanding local impact
- Recipient country analysis
- Budget support comparisons

## Troubleshooting

### Issue: Values seem too high/low after conversion

**Check:** Are you mixing current and constant prices?

```python
# Make sure both clients use the same price type
client1 = OECDClient(years=[2022], currency="USD")  # Current prices
client2 = OECDClient(years=[2022], currency="EUR", base_year=2021)  # Constant prices

# These aren't directly comparable!
```
## Next Steps

- **Advanced filtering**: See [Working with Indicators](oecd-client.md)
- **Policy analysis**: Explore [Policy Marker Analysis](policy-markers.md)

# Migration from v1.x to v2.x

Version 2.0 is a major refactor with significant improvements in performance, stability, and organization. This guide helps you migrate existing code from v1.x to v2.x.

## What's Changed

Version 2.x includes:

- ✅ **Faster**: Improved caching and bulk download performance
- ✅ **More stable**: Better error handling and thread safety
- ✅ **Better organized**: Clearer API structure
- ⚠️ **Breaking changes**: New API requires code updates

## Quick Migration Path

If you need to keep using v1.x syntax temporarily:

### Option 1: Use Compatibility Layer

```python title="Compatibility with v1.x Code"
from oda_data import ODAData  # v1.x compatible interface

# Old v1.x code works unchanged
oda = ODAData(years=range(2018, 2022), donors=[4, 302])
data = oda.load_indicator("total_oda_flow_net")

# This uses v2.x under the hood but maintains v1.x syntax
```

!!! warning "Compatibility Layer Limitations"
    The compatibility layer supports basic v1.x patterns but may not include all features. Migrate to v2.x API for full functionality.

### Option 2: Stay on v1.5.x

Version 1.5.x remains available and supported until at least November 2025:

```bash
# Install specific v1.x version
pip install oda-data==1.5.0
```

## Core API Changes

### Main Class Rename

**v1.x:**
```python
from oda_data import ODAData

oda = ODAData(years=range(2020, 2023))
```

**v2.x:**
```python
from oda_data import OECDClient

client = OECDClient(years=range(2020, 2023))
```

### Method Rename

**v1.x:**
```python
data = oda.load_indicator("total_oda_flow_net")
```

**v2.x:**
```python
data = client.get_indicators("DAC1.10.1010")  # New indicator code format
```

### Parameter Changes

| v1.x | v2.x | Notes |
|------|------|-------|
| `donors` | `providers` | More accurate terminology |
| `base_year` | `base_year` | Unchanged |
| `currency` | `currency` | Unchanged |
| `prices` | Use `base_year` | Set base_year for constant prices |

## Indicator Code Changes

v2.x uses explicit indicator codes instead of named indicators.

### Finding New Codes

**v1.x named indicators** → **v2.x codes:**

```python
# v1.x: Named indicators
oda.load_indicator("total_oda_flow_net")
oda.load_indicator("bilateral_oda_flow_net")
oda.load_indicator("multilateral_oda_flow_net")

# v2.x: Get equivalent codes
from oda_data import OECDClient

# Find available indicators
indicators = OECDClient.available_indicators()

# Search for what you need
for code, info in indicators.items():
    if "total oda" in info["name"].lower():
        print(f"{code}: {info['name']}")

# Use explicit codes
client = OECDClient(years=range(2020, 2023))
data = client.get_indicators("DAC1.10.1010")  # Total ODA
```

### Common v1.x to v2.x Mappings

| v1.x Indicator Name | v2.x Code | Description |
|---------------------|-----------|-------------|
| `total_oda_flow_net` | `DAC1.10.1010` | Total ODA, net disbursements |
| `bilateral_oda_flow_net` | `DAC1.10.11015` | Bilateral ODA |
| `multilateral_oda_flow_net` | `DAC1.10.1210` | Multilateral ODA |
| `total_oda_ge` | `DAC1.20.1010` | Total ODA, grant equivalent |

## Parameter Migration

### Donors → Providers

**v1.x:**
```python
oda = ODAData(
    years=range(2020, 2023),
    donors=[4, 302]  # Old parameter name
)
```

**v2.x:**
```python
client = OECDClient(
    years=range(2020, 2023),
    providers=[4, 302]  # New parameter name
)
```

### Prices Parameter

**v1.x:**
```python
oda = ODAData(
    years=range(2020, 2023),
    prices="constant",
    base_year=2020
)
```

**v2.x:**
```python
client = OECDClient(
    years=range(2020, 2023),
    base_year=2020  # base_year alone indicates constant prices
)
```

### Currency Conversion

**v1.x:**
```python
oda = ODAData(
    years=range(2020, 2023),
    currency="EUR"
)
```

**v2.x:**
```python
# Same syntax!
client = OECDClient(
    years=range(2020, 2023),
    currency="EUR"
)
```

## Data Source Access

Direct database access has a cleaner API in v2.x.

### DAC1 Access

**v1.x:**
```python
from oda_data import read_dac1

data = read_dac1(
    years=range(2020, 2023),
    donors=[4, 302]
)
```

**v2.x:**
```python
from oda_data import DAC1Data, set_data_path

set_data_path("data")

dac1 = DAC1Data(
    years=range(2020, 2023),
    providers=[4, 302]
)

data = dac1.read(using_bulk_download=True)
```

### CRS Access

**v1.x:**
```python
from oda_data import read_crs

data = read_crs(
    years=[2022],
    donors=[4]
)
```

**v2.x:**
```python
from oda_data import CRSData, set_data_path

set_data_path("data")

crs = CRSData(
    years=[2022],
    providers=[4]
)

data = crs.read(using_bulk_download=True)
```

## Utility Functions

### Adding Names

**v1.x:**
```python
from oda_data import add_names

data = add_names(data)
```

**v2.x:**
```python
from oda_data import add_names_columns

# More explicit: specify which columns
data = add_names_columns(data, ["provider_code", "recipient_code"])
```

### GNI Calculations

**v1.x:**
```python
oda = ODAData(years=range(2020, 2023))
data = oda.load_indicator("total_oda_flow_net")
data = oda.add_share_indicator(data, "gni")
```

**v2.x:**
```python
from oda_data import OECDClient, add_gni_share_column

client = OECDClient(years=range(2020, 2023))
data = add_gni_share_column(client, "DAC1.10.1010")

# Result has 'gni_share_pct' column
```

## Cache Management

### Data Path Setting

**v1.x:**
```python
from oda_data import set_data_path

set_data_path("path/to/data")
```

**v2.x:**
```python
# Same!
from oda_data import set_data_path

set_data_path("path/to/data")
```

### Cache Clearing

**v1.x:**
```python
from oda_data import clear_cache

clear_cache()
```

**v2.x:**
```python
# Same!
from oda_data import clear_cache

clear_cache()
```

## Complete Migration Example

Here's a complete v1.x script and its v2.x equivalent:

### v1.x Code

```python title="Old v1.x Script"
from oda_data import ODAData, set_data_path, add_names

set_data_path("data")

# Create ODA object
oda = ODAData(
    years=range(2018, 2023),
    donors=[4, 12, 302],
    currency="EUR",
    prices="constant",
    base_year=2020
)

# Load indicator
data = oda.load_indicator("total_oda_flow_net")

# Add names
data = add_names(data)

# Add GNI share
data = oda.add_share_indicator(data, "gni")

print(data.head())
```

### v2.x Equivalent

```python title="New v2.x Script"
from oda_data import OECDClient, set_data_path, add_names_columns, add_gni_share_column

set_data_path("data")

# Create client
client = OECDClient(
    years=range(2018, 2023),
    providers=[4, 12, 302],  # donors → providers
    currency="EUR",
    base_year=2020  # No need for prices="constant"
)

# Get indicator (use new code format)
data = client.get_indicators("DAC1.10.1010")  # Total ODA

# Add names (specify columns explicitly)
data = add_names_columns(data, ["provider_code"])

# Add GNI share (different function)
data = add_gni_share_column(client, "DAC1.10.1010")

print(data.head())
```

## Migration Checklist

Use this checklist to migrate your code:

- [ ] Update imports: `ODAData` → `OECDClient`
- [ ] Rename parameters: `donors` → `providers`
- [ ] Change method calls: `load_indicator()` → `get_indicators()`
- [ ] Update indicator names to codes (use `available_indicators()`)
- [ ] Remove `prices` parameter (use `base_year` only for constant prices)
- [ ] Update `add_names()` to `add_names_columns()` with column specification
- [ ] Update GNI functions: use `add_gni_share_column()`
- [ ] Test with small dataset first
- [ ] Check data output matches expectations
- [ ] Update documentation and comments

## Testing Your Migration

After migrating, verify your results:

```python title="Verify Migration Results"
# Compare v1.x and v2.x outputs

# Using compatibility layer (v1.x style)
from oda_data import ODAData
oda_v1 = ODAData(years=[2022], donors=[4])
data_v1 = oda_v1.load_indicator("total_oda_flow_net")

# Using new v2.x API
from oda_data import OECDClient
client_v2 = OECDClient(years=[2022], providers=[4])
data_v2 = client_v2.get_indicators("DAC1.10.1010")

# Compare totals (should be very close)
total_v1 = data_v1["value"].sum()
total_v2 = data_v2["value"].sum()

print(f"v1.x total: ${total_v1:,.0f}")
print(f"v2.x total: ${total_v2:,.0f}")
print(f"Difference: {abs(total_v1 - total_v2) / total_v1 * 100:.2f}%")
```

## Troubleshooting

### Issue: Indicator name not found

**v1.x worked, v2.x doesn't:**

```python
# v1.x: This worked
data = oda.load_indicator("bilateral_oda_flow_net")

# v2.x: Named indicators removed
# Find the equivalent code:
from oda_data import OECDClient

indicators = OECDClient.available_indicators()
for code, info in indicators.items():
    if "bilateral" in info["name"].lower() and "oda" in info["name"].lower():
        print(f"{code}: {info['name']}")

# Use the code
data = client.get_indicators("DAC1.10.11015")
```

### Issue: Different column names

v2.x uses standardized column names. Check the actual columns:

```python
# See what columns are available
print(data.columns.tolist())

# Common mappings:
# v1.x → v2.x
# 'donor_code' → 'provider_code'
# 'donor_name' → 'provider_name'
```

### Issue: Import errors

**If you see:**
```
ImportError: cannot import name 'ODAData'
```

**Solution:**
```python
# Old import (v1.x only):
from oda_data import ODAData  # Won't work in v2.x default

# New import for compatibility:
from oda_data import ODAData  # Works via compatibility layer

# Or migrate fully to:
from oda_data import OECDClient
```

## Getting Help

If you encounter migration issues:

1. **Check available indicators**: Use `OECDClient.available_indicators()` to find codes
2. **Use compatibility layer**: Import `ODAData` for gradual migration
3. **Review examples**: See other documentation pages for v2.x patterns
4. **Report issues**: [Open an issue on GitHub](https://github.com/ONEcampaign/oda_data_package/issues)

## Version Support Timeline

- **v1.5.x**: Supported until at least November 2025
- **v2.x**: Current stable version, recommended for new projects
- **Compatibility layer**: Available in v2.x for gradual migration

## Benefits of Migrating

Why upgrade to v2.x:

- **Performance**: 2-5x faster queries with improved caching
- **Stability**: Better error handling and thread safety
- **Features**: New capabilities like policy markers, sector imputations
- **Future-proof**: Active development focuses on v2.x
- **Better docs**: Comprehensive documentation and examples

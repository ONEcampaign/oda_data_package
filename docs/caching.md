# Cache Management

The package uses intelligent caching to speed up data access. Understanding how caching works helps you optimize performance and manage disk space.

## How Caching Works

The package implements a three-tier caching system:

```
┌─────────────────┐
│  Memory Cache   │  ← Fastest: In-RAM storage
├─────────────────┤
│   Query Cache   │  ← Fast: Filtered datasets
├─────────────────┤
│   Bulk Cache    │  ← Large: Complete database files
└─────────────────┘
```

**When you request data:**

1. **Check memory cache**: Is it already loaded in RAM?
2. **Check query cache**: Is this exact filtered query cached?
3. **Check bulk cache**: Is the bulk file downloaded?
4. **Fetch from API**: Last resort, download from OECD

## Setting Up Caching

### Set the Data Path

```python title="Configure Cache Location"
from oda_data import set_data_path

# All cached data goes here
set_data_path("data")

# Creates directory structure:
# data/
# ├── bulk/          # Bulk downloads
# ├── queries/       # Filtered query results
# └── .pydeflate/    # Currency conversion data
```

### Default Cache Location

If you don't set a path, the package uses a default:

```python
# Default: .raw_data/ in your working directory
# Automatically created when needed
```

## Cache Tiers Explained

### 1. Memory Cache

Fast, temporary storage while your Python session runs.

```python title="Memory Cache in Action"
from oda_data import OECDClient

client = OECDClient(years=[2022])

# First call: Downloads and caches in memory
data1 = client.get_indicators("DAC1.10.1010")  # Slow

# Second call: Retrieved from memory
data2 = client.get_indicators("DAC1.10.1010")  # Fast!

# Memory cache clears when Python session ends
```

**Characteristics:**

- Very fast access
- Limited by available RAM
- Cleared when Python exits
- Shared across queries in same session

### 2. Query Cache

Stores filtered datasets on disk for reuse.

```python title="Query Cache Persistence"
from oda_data import OECDClient, set_data_path

set_data_path("data")

client = OECDClient(
    years=[2022],
    providers=[4, 12],
    currency="EUR"
)

# First run: Processes and caches result
data = client.get_indicators("DAC1.10.1010")

# Next day, same query: Instant!
# Loads from query cache, no processing needed
```

**Characteristics:**

- Persists across sessions
- Specific to filter combinations
- Smaller than bulk files
- Automatically managed

### 3. Bulk Cache

Stores complete database downloads.

```python title="Bulk Cache for Large Datasets"
from oda_data import DAC1Data, set_data_path

set_data_path("data")

dac1 = DAC1Data(years=range(2010, 2024))

# First time: Downloads entire bulk file (few minutes)
dac1.download(bulk=True)

# Subsequent queries: Reads from cached file (seconds)
data = dac1.read(using_bulk_download=True)
```

**Characteristics:**

- Complete database tables
- Large file size (100s of MB)
- Persists across sessions
- Shared across all queries using that database

## Managing Cache

### Clear All Caches

```python title="Clear All Cached Data"
from oda_data import clear_cache

# Removes all cached data
clear_cache()

# Everything will be re-downloaded on next use
```

!!! warning "Disk Space"
    Clearing cache means the next queries will download data again. Only clear cache when you need to free disk space or force fresh downloads.

### Disable Caching Temporarily

```python title="Disable Cache for Development"
from oda_data import disable_cache, enable_cache

# Turn off caching
disable_cache()

# Now every query fetches fresh data
client = OECDClient(years=[2022])
data = client.get_indicators("DAC1.10.1010")  # Always downloads

# Re-enable caching
enable_cache()
```

**Use cases for disabling cache:**

- Testing with latest OECD data
- Debugging cache-related issues
- Development and testing
- Ensuring data freshness

### Check Cache Location

```python title="Find Your Cache Directory"
from oda_data.config import ODAPaths

print(f"Cache directory: {ODAPaths.raw_data}")
```

### Manual Cache Cleanup

You can manually manage cache files:

```bash
# View cache size
du -sh data/

# Remove old bulk files
rm data/bulk/*

# Remove query cache
rm -rf data/queries/
```

## Performance Optimization

### Pattern: Use Bulk Downloads for Multiple Queries

```python title="Optimize with Bulk Downloads"
from oda_data import OECDClient, set_data_path

set_data_path("data")

# Enable bulk download once
client = OECDClient(
    years=range(2015, 2024),
    use_bulk_download=True
)

# First call downloads bulk file (slow initial download)
data1 = client.get_indicators("DAC1.10.1010")

# Subsequent calls are very fast
data2 = client.get_indicators("DAC1.10.11015")
data3 = client.get_indicators("DAC1.10.1210")
# All fast because they use the same cached bulk file
```

## Cache Lifetime and Refresh

### When Cache is Invalidated

Cache is automatically refreshed when:

- Bulk file is older than 30 days (stale data check)
- You explicitly clear cache
- OECD releases new data versions

### Force Fresh Data

```python title="Force Download of Latest Data"
from oda_data import clear_cache, OECDClient

# Clear cache to force fresh download
clear_cache()

# Next query gets latest data
client = OECDClient(years=[2023])
fresh_data = client.get_indicators("DAC1.10.1010")
```

### Check Data Freshness

```python title="Verify When Data Was Cached"
import os
from pathlib import Path
from datetime import datetime
from oda_data.config import ODAPaths

# Check bulk file modification time
bulk_dir = ODAPaths.raw_data / "bulk"

if bulk_dir.exists():
    for file in bulk_dir.glob("*.parquet"):
        mtime = os.path.getmtime(file)
        mod_date = datetime.fromtimestamp(mtime)
        print(f"{file.name}: Last modified {mod_date}")
```

## Troubleshooting

### Issue: Cache taking too much space

**Solution:** Clear old caches or remove specific bulk files:

```python
from oda_data import clear_cache

# Nuclear option: clear everything
clear_cache()

# Or manually remove large bulk files you don't need
# rm data/bulk/CRS_*.parquet
```

### Issue: Getting old data

**Solution:** Force refresh:

```python
from oda_data import clear_cache, OECDClient

clear_cache()

# Now get fresh data
client = OECDClient(years=[2023])
data = client.get_indicators("DAC1.10.1010")
```

### Issue: Cache corrupted or causing errors

**Solution:** Clear and rebuild:

```python
from oda_data import clear_cache, set_data_path

# Clear all caches
clear_cache()

# Re-set path to ensure clean state
set_data_path("data")

# Cache will rebuild correctly on next use
```

### Issue: Working across multiple projects

**Solution:** Use project-specific cache paths:

```python
# Project 1
from oda_data import set_data_path
set_data_path("project1/data")

# Project 2
from oda_data import set_data_path
set_data_path("project2/data")

# Each project has its own cache
```

## Advanced: Multi-Process Safety

The package uses file locks to prevent cache corruption in multi-process scenarios:

```python title="Safe for Parallel Processing"
from multiprocessing import Pool
from oda_data import OECDClient, set_data_path

def fetch_indicator(indicator):
    set_data_path("data")  # Same cache for all processes
    client = OECDClient(years=[2022])
    return client.get_indicators(indicator)

# Safe: Multiple processes can share cache
if __name__ == "__main__":
    indicators = ["DAC1.10.1010", "DAC1.10.11015", "DAC1.10.1210"]
    with Pool(3) as pool:
        results = pool.map(fetch_indicator, indicators)
```

File locks prevent:

- Race conditions during downloads
- Corrupted cache files
- Duplicate downloads

## Best Practices

**Do:**

- Set a persistent data path for your project
- Use bulk downloads for multiple queries
- Reuse client configurations
- Clear cache periodically to free space
- Pre-download bulk files for offline work

**Don't:**

- Clear cache unnecessarily (wastes time re-downloading)
- Use different data paths for the same project
- Disable caching in production code
- Manually edit cache files (can corrupt them)
- Keep very old cache files (may be stale)

## Summary

The caching system makes the package fast and efficient:

- **Memory cache**: Instant access during your session
- **Query cache**: Fast retrieval of previous filtered queries
- **Bulk cache**: Quick access to complete databases

Manage caches with:

- `set_data_path()` - Configure where cache is stored
- `clear_cache()` - Remove all cached data
- `disable_cache()`/`enable_cache()` - Control caching behavior

Understanding caching helps you:

- Optimize query performance
- Manage disk space
- Ensure data freshness
- Work efficiently offline

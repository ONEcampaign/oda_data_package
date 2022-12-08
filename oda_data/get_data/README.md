# Get Data

This module contains tools to download data from the OECD DAC databases, using the OECD bulk download tool.

We make use of the bulk download tool as it is generally a faster approach to data analysis. There's a time/resource
investment to download the data files at first. However, when accessing anything beyond moderate amounts of data,
this is a much faster approach than using the official OECD.Stat API.

The downside of this approach is the need to store fairly large files. In total, if data is downloaded for a couple of
decades, the storage necessary is around 400mb (each file is a max of 30mb).
The files are stored as `.feather` files, which is a compressed binary format.
This is a much more efficient format than `.csv` files, and is also much faster to read into memory.

## Usage

In order to use this module, you must select the data source you want to download. Each source/table has its own
`.py` file with a `download` function.

All of the `download` functions have an optional 'small_version' argument. This is a boolean, and if set to `True`,
the functions will store a smaller version of the data by removing columns that ONE doesn't usually use for its
analyses.

### [crs.py](crs.py)

In order to download the CRS data, the script fetches a list of links from the bulk download service of OECD.Stat.
Users can use the `download_crs` function to download the data. The function takes a single year, or a list of years,
as input. For example:

```python
from oda_data.get_data import crs

# Download data for a single year
crs.download_crs(years=2019)

# Download data for multiple years
crs.download_crs(years=[2018, 2019])

# Download data for a range of years
crs.download_crs(years=range(2015, 2020))
```

### [dac1.py](dac1.py)

In order to download the DAC1 data, the script fetches the single bulk download link from the bulk download service of
OECD.Stat.
This link contains a zip file with data for all available years.

Users can use the `download_dac1` function to download the data. All years are downloaded automatically.

For example:

```python
from oda_data.get_data import dac1

# Download data for all years. Save a small version with only key columns
dac1.download_dac1(small_version=True)
```

### [dac2a.py](dac2a.py)

In order to download the DAC2a data, the script fetches the single bulk download link from the bulk download service of
OECD.Stat.
This link contains a zip file with data for all available years.

Users can use the `download_dac2a` function to download the data. All years are downloaded automatically.

For example:

```python
from oda_data.get_data import dac2a

# Download data for all years
dac2a.download_dac2a()
```

### [multisystem.py](multisystem.py)

In order to download the Multisystem data, the script fetches the single bulk download link from the bulk download
service of OECD.Stat.
This link contains a zip file with data for all available years.

Users can use the `download_multisystem` function to download the data. All years are downloaded automatically.

For example:

```python
from oda_data.get_data import multisystem

# Download data for all years
multisystem.download_multisystem()
```
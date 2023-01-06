# Read data

This module contains functions to make it easier to read data from the different databases.

The functions in this module assume that the data has been downloaded and is stored in the `raw_data` folder.
If that isn't the case, please see the [get_data](../../oda_data/get_data) module.

There is a function for each of the sources. To use, for exmple
````python
from oda_data.read_data import read

crs = read.read_crs(years=[2019,2020])

dac1 = read.read_dac1(years=2014)

dac2a = read.read_dac2a(years=range(2012,2021))

multisystem = read.read_multisystem(years=range(2012,2021))
````



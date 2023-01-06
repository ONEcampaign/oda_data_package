# Clean Data
The clean data module contains tools to clean the raw data. It also contains tools to add 'names' to 
different donor, recipient, channel codes.

## [common.py](common.py)
This file contains functions that are used in different cleaning steps.

It also contains `dac_exchange()` and `dac_deflate()` which are partial implementations of `pydeflate.deflate()`
which enforce certain shared settings (e.g. using the DAC as a source, setting a common base year, etc.)

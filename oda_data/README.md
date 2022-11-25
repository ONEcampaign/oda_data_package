# ODA Data Scripts

This folder contains all the scripts required to download, read, and analyse the DAC ODA data.

Each of the submodules contains a readme file with more information.

## Getting started

Most users can get the data they need by using the `ODAData` class in `oda_data.py`. 
An object of this class can handle:
- getting data for specific indicators (one or more)
- filtering the data for specific donors, recipients(if relevant), years.
- returning the data in a variety of currency/prices combinations.

For example, to get Total ODA in net flows and grant equivalents, in constant 2021 Euros, for 2018-2021.

```python
from oda_data import ODAData

# create object, specifying key details of the desired output
data = ODAData(years=range(2018,2022), currency="EUR", prices="constant", base_year=2021)

# load the desired indicators
data.load_indicator(indicator="total_oda_flow_net")
data.load_indicator(indicator="total_oda_ge")

# get the data
df = data.get_data(indicators='all')

print(df.head(6))
```
This would result in the following dataframe:

|   donor_code | donor_name   |   year |   value | indicator          | currency   | prices   |
|-------------:|:-------------|-------:|--------:|:-------------------|:-----------|:---------|
|            1 | Austria      |   2021 | 1261.76 | total_oda_flow_net | EUR        | constant |
|            1 | Austria      |   2021 | 1240.31 | total_oda_ge       | EUR        | constant |
|            2 | Belgium      |   2021 | 2176.38 | total_oda_flow_net | EUR        | constant |
|            2 | Belgium      |   2021 | 2174.38 | total_oda_ge       | EUR        | constant |
|            3 | Denmark      |   2021 | 2424.51 | total_oda_flow_net | EUR        | constant |
|            3 | Denmark      |   2021 | 2430.65 | total_oda_ge       | EUR        | constant |


To view the full list of available indicators, you can call `.get_available_indicators()`.

```python
from oda_data import ODAData

# create object
data = ODAData()

# get the list of available indicators
data.available_indicators()
```
This logs the list of indicators to the console. For example, the first several: 
```markdown
Available indicators:
total_oda_flow_net
total_oda_ge
total_oda_bilateral_flow_net
total_oda_bilateral_ge
total_oda_multilateral_flow_net
total_oda_multilateral_ge
total_oda_flow_gross
total_oda_flow_commitments
total_oda_grants_flow
total_oda_grants_ge
total_oda_non_grants_flow
total_oda_non_grants_ge
gni
oda_gni_flow
od_gni_ge
.
.
.
```

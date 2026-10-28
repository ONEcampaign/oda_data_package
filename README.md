[![pypi](https://img.shields.io/pypi/v/oda_data.svg)](https://pypi.org/project/oda_data/)
[![python](https://img.shields.io/pypi/pyversions/oda_data.svg)](https://pypi.org/project/oda_data/)
[![codecov](https://codecov.io/gh/ONEcampaign/oda_data_package/branch/main/graph/badge.svg)](https://codecov.io/gh/ONEcampaign/oda_data_package)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

# oda-data

oda-data gives you official development assistance (ODA) figures from the OECD's DAC tables by name, for any years and providers, in any currency, with a record of where each number came from.

```text
    provider_name  year             basis     value
0          France  2024  grant_equivalent  15448.26
1         Germany  2024  grant_equivalent  32836.11
2  United Kingdom  2024  grant_equivalent  17991.79
3   United States  2024  grant_equivalent  65475.37
```

```python
import oda_data as oda

df = oda.get("oecd.dac1.total_oda", years=2024, providers=["USA", "DEU", "GBR", "FRA"])
print(df[["provider_name", "year", "basis", "value"]])
```

Values are USD millions. The first call downloads the OECD's DAC1 table (about 10 MB) and keeps it on disk.

## Install

```bash
pip install oda-data
```

oda-data needs Python 3.12 or later. `pip install "oda-data[polars]"` adds `Query.to_polars()`.

## What it does

Most ODA figures depend on reading a DAC table correctly: DAC1 for official totals, DAC2A for what recipients receive, the CRS for sectors and channels. Each table has its own codes, bases, aggregate rows, and lines that nest inside each other. oda-data holds those rules in more than 40 named indicators, such as `oecd.dac1.total_oda`, `oecd.crs.bilateral_oda`, and `one.crs.imputed_multilateral_oda`, so one call returns one tidy table.

```python
import oda_data as oda

dac = oda.groups.dac_countries

# Each DAC country and the DAC total, in constant 2024 euros
oda.get(
    "oecd.dac1.total_oda",
    years=range(2015, 2025),
    providers=[dac, oda.total(dac)],
    unit=oda.constant("EUR", 2024),
)

# Bilateral ODA to Kenya by broad sector
oda.get("oecd.crs.bilateral_oda", years=2023, recipients="KEN", by=["broad_sector"])
```

Every result has the same columns, converts money provider by provider before any sum, and refuses a question its table cannot answer with an error that names the indicator that can. [oda-reader](https://github.com/ONEcampaign/oda_reader) downloads and caches the OECD tables, and [pydeflate](https://github.com/jm-rivera/pydeflate) supplies deflators and exchange rates from the same DAC1 release.

oda-data is the wrong tool for a pipeline that needs the raw OECD tables. Read those with oda-reader.

## Documentation

- [Tutorial](https://github.com/ONEcampaign/oda_data_package/blob/main/docs/docs/tutorial.md): chart ten years of G7 aid
- [How-to guides](https://github.com/ONEcampaign/oda_data_package/blob/main/docs/docs/how-to/find-an-indicator.md): finding the indicator for a question, units, groups, sectors, imputed multilateral sectors, records, provenance, offline use
- [Indicators](https://github.com/ONEcampaign/oda_data_package/blob/main/docs/docs/reference/indicators.md) and the [public API](https://github.com/ONEcampaign/oda_data_package/blob/main/docs/docs/reference/api.md)
- [Counting rules](https://github.com/ONEcampaign/oda_data_package/blob/main/docs/docs/explanation/counting-rules.md): which rows count, and why
- [Migrating from 2.x](https://github.com/ONEcampaign/oda_data_package/blob/main/docs/docs/migration.md): the 3.0 call for any 2.x code, and the numbers that change

3.0 replaces the 2.x API. `oda.search("DAC1.10.1010")` returns the 3.0 call for any 2.x indicator code.

Questions and bug reports go to the [issue tracker](https://github.com/ONEcampaign/oda_data_package/issues). [CONTRIBUTING.md](https://github.com/ONEcampaign/oda_data_package/blob/main/CONTRIBUTING.md) explains how to work on the package.

MIT licence. See [LICENSE](https://github.com/ONEcampaign/oda_data_package/blob/main/LICENSE).

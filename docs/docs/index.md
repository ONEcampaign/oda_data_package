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

Install it with `pip install oda-data`. It needs Python 3.12 or later.

## What it is for

An ODA figure depends on reading the right DAC table the right way. DAC1 holds each provider's official totals, DAC2A what each recipient receives, and the Creditor Reporting System (CRS) each activity with its sector and channel. The tables mix current and constant prices, hold aggregate rows beside their members, and publish lines that nest inside each other. Picking the wrong rows gives a number that looks right and is wrong.

oda-data holds those rules in named indicators. Each one names the table it reads, the rows that count, and the basis on which money is counted. A call chooses the years, providers, recipients, sectors, breakdown, and unit, and gets back one table with fixed columns. A question the table cannot answer raises an error that names the indicator that can.

oda-data reads the OECD's bulk files through [oda-reader](https://github.com/ONEcampaign/oda_reader), which downloads and caches them, and converts currencies with [pydeflate](https://github.com/jm-rivera/pydeflate), which reads its deflators from the same DAC1 release. A pipeline that needs the raw tables should read them with oda-reader.

## Where to go next

- [Chart ten years of G7 aid](tutorial.md) walks through a first call, constant prices, a group total, and a chart.
- The how-to guides cover single tasks: [finding the indicator for a question](how-to/find-an-indicator.md), [units](how-to/units.md), [groups and totals](how-to/groups-and-totals.md), [sectors and policy markers](how-to/sectors-and-markers.md), [imputed multilateral sectors](how-to/imputed-multilateral.md), [records](how-to/records.md), [provenance](how-to/provenance.md), and [offline use](how-to/offline-and-caching.md).
- [Indicators](reference/indicators.md) lists every indicator, and [Public API](reference/api.md) every function.
- [Counting rules](explanation/counting-rules.md) explains which rows count and why.
- [Migrating from 2.x](migration.md) gives the 3.0 call for any 2.x code and lists the numbers that change.

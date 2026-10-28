# Migrating from 2.x

oda-data 3.0 replaces the 2.x API. Every 2.x indicator code has a 3.0 call, and `oda.search()` gives it. This guide assumes you have 2.x code to move. 2.x stays installable with `pip install "oda-data<3"`, and 3.0 needs Python 3.12 or later.

## The default basis changed

The change that moves the most numbers is the default basis. 2.x defaulted to `measure="net_disbursement"` in every year. 3.0 defaults to `headline`, which follows the OECD's own series: net flows to 2017 and grant equivalents from 2018. Every 2.x call that took the default gives a different number from 2018 on.

```text
   year             basis          value
0  2017  net_disbursement  147502.810000
1  2018  net_disbursement  150440.750000
2  2023  net_disbursement  226734.741184
   year             basis          value
0  2017  net_disbursement  147502.810000
1  2018  grant_equivalent  153878.490000
2  2023  grant_equivalent  223921.171184
```

```python
import oda_data as oda

dac_total = oda.total(oda.groups.dac_countries)
years = [2017, 2018, 2023]

as_in_2x = oda.get("oecd.dac1.total_oda", years=years, providers=dac_total, by=["provider", "year"], basis="net_disbursement")
headline = oda.get("oecd.dac1.total_oda", years=years, providers=dac_total, by=["provider", "year"])

print(as_in_2x[["year", "basis", "value"]])
print(headline[["year", "basis", "value"]])
```

Pass `basis="net_disbursement"` to keep the 2.x series. The `basis` column of every result says which basis each row holds. [Counting rules](explanation/counting-rules.md#the-headline-basis) explains the headline basis.

## Find the 3.0 call for a 2.x code

`oda.search()` takes a 2.x indicator code, or a slug from 2.x's `settings/indicators.json`, and returns the call as its first row. `use` holds the call and `note` says why its number differs from 2.x, if it does.

```text
oda.get("oecd.dac2a.recipient_oda", by=["provider", "recipient", "year"])
3.0 leaves out DAC2A rows to multilateral organisations as recipients (98, 816, 878, 900-996), because they are core contributions, which the organisations' own rows carry on to the recipients they reach; 2.x's rows also held DAC2A recipient totals, such as developing countries (10001), which 3.0 returns only when recipients= names them, so 2.x's rows summed by provider count each flow more than once.
```

```python
import oda_data as oda

found = oda.search("DAC2A.10.206")
print(found.use.iloc[0])
print(found.note.iloc[0])
```

`oda.get()` with a 2.x code raises `UnknownIndicatorError` whose message carries the same call:

```text
'CRS.P.10.100.A' is an oda-data 2.x code. In 3.0:
    oda.get("oecd.crs.bilateral_oda", finance_types=[110, 210], modalities=["A01", "A02"])
The number differs from 2.x: 3.0 counts bi_multi 1, 3, 7 and 8 only, ... [unknown_indicator]
```

The map covers the 3,049 codes of the 2.x catalogue and the 108 slugs of `settings/indicators.json`, 3,157 entries in all.

| Status         | Entries | Meaning                                                                 |
| -------------- | ------- | ----------------------------------------------------------------------- |
| `exact`        | 156     | the call gives 2.x's number for the same provider, recipient, and year  |
| `changes`      | 2,893   | a counting rule, a 2.x defect, or a unit differs, and `note` says which |
| `no_2x_number` | 93      | 2.x computed no number to compare with, as for every slug               |
| `no_data`      | 15      | the OECD no longer publishes what the code read                         |

## Move a 2.x call

A 2.x `OECDClient` call becomes one `oda.get()` call. The arguments map one to one, except the currency, which becomes a unit.

```py
# 2.x
from oda_data import OECDClient

client = OECDClient(years=[2020, 2021], providers=[4, 302], measure="grant_equivalent", currency="EUR", base_year=2019)
data = client.get_indicators("DAC1.10.11015")
```

```python
import oda_data as oda

data = oda.get(
    "oecd.dac1.bilateral_oda",
    years=[2020, 2021],
    providers=[4, 302],
    basis="grant_equivalent",
    unit=oda.constant("EUR", 2019),
)
```

The first block is 2.x code. On 3.0 its import raises `RemovedIn3Error`, whose message names the replacement.

| 2.x                                        | 3.0                                                                    |
| ------------------------------------------ | ---------------------------------------------------------------------- |
| `OECDClient(...).get_indicators(code)`     | `oda.get(indicator, ...)`                                              |
| `years=`, `providers=`, `recipients=`      | the same arguments, which also take names and groups                   |
| `measure=`                                 | `basis=`                                                               |
| `currency=`, `base_year=`                  | `unit=oda.current(currency)` or `unit=oda.constant(currency, year)`    |
| `currency="LCU"`                           | each provider's currency as its own unit, such as `oda.current("JPY")` |
| `use_bulk_download=True`                   | nothing: 3.0 always reads the bulk files                               |
| `set_data_path(path)`                      | `oda.Context(cache_dir=path)`                                          |
| columns `donor_code`, `donor_name`         | `provider`, `provider_name`                                            |
| columns `recipient_code`, `recipient_name` | `recipient`, `recipient_name`                                          |
| column `one_indicator` or the 2.x code     | `indicator`, the 3.0 id                                                |

Two defaults also differ. A 2.x call without `providers=` read every provider in the table. A DAC1 call in 3.0 reads `oda.groups.dac_countries`, and a DAC2A or CRS call every official provider with aggregates and private donors left out. And 2.x returned the OECD's aggregate rows beside their members, which 3.0 returns only when a filter names them. Name a group, or pass `oda.total(group)`, to get a total.

## Removed names

Each 2.x top-level name raises `RemovedIn3Error`, an `ImportError`, whose message gives its replacement.

| 2.x name                                                                  | In 3.0                                                                                                                                        |
| ------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `OECDClient`                                                              | `oda.get(indicator, years=..., providers=..., basis=..., unit=...)`                                                                           |
| `ODAData`                                                                 | `oda.get()`; `oda.indicators()` lists the indicators                                                                                          |
| `DAC1Data`, `DAC2AData`, `CRSData`, `MultiSystemData`                     | `oda_reader.download_dac1()` and its siblings, or `oda_reader.scan("dac1")`, for the raw tables; `oda.records()` for the rows behind a number |
| `AidDataData`                                                             | `oda_reader.download_aiddata()`                                                                                                               |
| `add_names_columns`                                                       | `oda.add_names(df)`; every result already has its name columns                                                                                |
| `add_sectors`, `add_broad_sectors`                                        | `by=["sector"]` or `by=["broad_sector"]` on a CRS indicator                                                                                   |
| `add_gni_share_column`                                                    | `oecd.dac1.oda_gni_ratio`; `one.dac1.oda_gni_ratio_fixed_eu27` for EU Institutions as 2.x computed them                                       |
| `bilateral_policy_marker`                                                 | one indicator per marker, such as `oecd.crs.gender_marker`, with `by=["score"]`                                                               |
| `provider_groupings`, `recipient_groupings`                               | `oda.groups`, and `oda.total(group)` for a total                                                                                              |
| `sector_imputations`                                                      | `one.crs.imputed_multilateral_oda` with `by=["sector"]`                                                                                       |
| `imputation_quality_report`                                               | `oda.records("one.crs.imputed_multilateral_oda", ...)`, grouped by `allocation_status`                                                        |
| `cache`, `clear_cache`, `disable_cache`, `enable_cache`, `set_cache_root` | `oda.Context(cache_dir=...)`, and `oda_reader.cache_info()` and its siblings to manage the cache                                              |
| `set_data_path`                                                           | `oda.Context(cache_dir=...)`; `Query.to_parquet(path)` writes a result                                                                        |

The 2.x groups `dac_members`, `eu27_total`, `all_official`, and `dac1_aggregates` have no 3.0 group. Pass 918 or the OECD aggregate code itself, since a group holding 918 and EU members would count EU money twice.

## Numbers that change

A `changes` entry gives a different number from 2.x for one or more of these reasons. An entry can carry several, so the counts overlap.

3.0 also leaves out rows whose value is exactly zero, which 2.x returned, so a 3.0 result can have fewer rows with the same totals.

### 2.x CRS sector codes returned nothing

2,740 CRS codes filter on a sector. 2.x compared the CRS's five-digit purpose codes with three-digit sector headings, so it found only the activities reported at heading level. On today's CRS, 2.8.2 returns no rows for 2,290 of those codes, and raises an error for the 286 that also select rows with no modality or purpose code, which leaves 164 codes with a number. 3.0 reads every purpose code under each heading. Every 2.x sector figure is too low or missing.

### CRS rows that count

- 1,908 codes: 3.0 counts bilateral rows only (`bi_multi` 1, 3, 7, and 8). 2.x also counted rows with no `bi_multi` and multilateral organisations' own outflows. A multilateral provider's ODA is `oecd.crs.multilateral_oda`.
- 1,908 codes: on grant equivalents 3.0 adds private sector instruments (category 60) to category 10, which brings CRS bilateral grant equivalents within USD 30 million of DAC1 in 2023.
- 948 codes: 2.x summed every CRS category. 3.0 sums ODA (category 10) or other official flows (category 21), and private development finance (category 30) is `oecd.crs.private_development_finance`. Each of these codes also filters on a sector or on a missing modality, so 2.8.2 reads only rows reported at heading level or raises an error, and the category rule shows only beside the sector fix.
- 578 codes: the same bilateral rule for other official flows, with `oecd.crs.multilateral_oof` for multilateral providers.
- 198 codes: private development finance defaults to the private foundations in `oda.groups.private_donors`. The category 30 rows that Denmark, Japan, Korea, Hungary, and Finland report, USD 2.7 billion in all years, count only when a call names them.

### DAC2A rows that count

- 19 codes: 2.x's rows held DAC2A recipient totals, such as developing countries, so 2.x summed by provider counted each flow more than once. 3.0 returns a total only when `recipients=` names it.
- 18 codes: 3.0 leaves out multilateral organisations as DAC2A recipients (98, 816, 878, and 900 to 996), whose rows are core contributions.
- 2 codes: line 106 leaves out multilateral providers and the totals that hold them (20002, 20004, 20005), since the imputed shares are their own flows.

### DAC1 and ONE figures

- 3 codes: 2.x returned no rows on today's DAC1 (`DAC1.40.3`, `DAC1.40.4`, `DAC1.40.11002`). 3.0 gives line 3 as a per cent of GNI, population in millions of people through `oecd.dac1.population`, and ODA/GNI computed from ODA and GNI, which differs from the published per cent by up to 0.005 points of rounding.
- `ONE.40.1010_11010_1` and `ONE.40.1010_1`: ODA/GNI is in per cent, where 2.x gave a fraction, and EU Institutions' ratio divides by the GNI of today's EU27 in every year, as 2.x's `add_gni_share_column` did. 2.x shipped no function for the second code.
- `ONE.P.40.T.T.S_M`: 2.x gave three-year rolling shares of ODA and other official flows by channel. The 3.0 call gives single-year shares of ODA, and `one.crs.imputed_multilateral_oda` applies the three-year shares itself.

The 15 `no_data` entries are COVID-19 slugs that read DAC1 lines 8852 to 8856, which the OECD publishes in no year. `one.crs.covid_oda` counts bilateral ODA the CRS tags `#COVID-19`.

### Size of the changes

Each row runs one call in oda-data 2.8.2 and in 3.0 on the same OECD releases, so a difference comes from a counting rule. Values are current USD millions, and ODA/GNI is in per cent.

| Call                                                                  | 2.x code                  | Year |     2.8.2 |       3.0 | Why it moves                                                                                    |
| --------------------------------------------------------------------- | ------------------------- | ---: | --------: | --------: | ----------------------------------------------------------------------------------------------- |
| Headline total ODA, France                                            | `ONE.10.1010_11010`       | 2022 | 16,016.25 | 16,016.25 | Same number                                                                                     |
| ODA/GNI, EU Institutions                                              | `ONE.40.1010_11010_1`     | 2022 |      null |    0.1355 | 3.0 divides by the EU27's GNI                                                                   |
| DAC2A bilateral ODA, France, summed over recipients                   | `DAC2A.10.206`            | 2022 | 99,704.52 | 10,535.47 | 2.x's rows held recipient totals and multilateral recipients                                    |
| DAC2A imputed multilateral ODA, France, summed over recipients        | `DAC2A.10.106`            | 2022 | 50,586.66 |  6,504.19 | 2.x's rows held recipient totals                                                                |
| CRS bilateral ODA, IDA                                                | `CRS.P.10`                | 2022 | 23,610.30 |   no rows | IDA's rows are its own outflows (`bi_multi` 4), which 3.0 counts in `oecd.crs.multilateral_oda` |
| CRS bilateral ODA, grant equivalents, France                          | `CRS.P.10`                | 2023 |  8,268.05 |  8,488.86 | 3.0 adds France's private sector instruments (category 60), USD 220.81 million                  |
| CRS ODA and other official flows to education, every category, France | `CRS.P.T.T.T.110`         | 2022 |     79.77 |  1,600.22 | 2.x read only the rows reported at heading level, in every category                             |
| CRS private development finance, all recipients                       | `CRS.R.30`                | 2022 | 11,279.10 | 10,735.08 | 3.0 leaves out the category 30 rows of official providers                                       |
| CRS bilateral ODA grants to education projects, France                | `CRS.P.10.100.C.110`      | 2022 |   no rows |    190.00 | 2.x sector codes returned nothing                                                               |
| CRS bilateral ODA grants to health projects, France                   | `CRS.P.10.100.C.120`      | 2022 |   no rows |    472.12 | 2.x sector codes returned nothing                                                               |
| Gender marker, principal, Norway                                      | `bilateral_policy_marker` | 2022 |    190.74 |    190.74 | Same number, as in every marker sample                                                          |
| Core ODA, 3.0 headline basis against 2.x net flows, France            | `ONE.10.1010C`            | 2022 | 14,999.35 | 13,495.02 | The default basis is grant equivalents from 2018                                                |

Releases: dac1 `0x8DF2539D20A32DF`, dac2a `0x8DF2539E5D52F9D`, crs `0x8DF2533665E7CB7`, multisystem `0x8DF2522071A444B`. oda-data 2.8.2 against 3.0.0.

The marker indicators give the same numbers as 2.x's `bilateral_policy_marker` for ten DAC members, three scores, and 2012, 2018, and 2022, since that function already screened modalities B031 to B033. The marker entries in the map are ONE slugs that 2.x computed no number for.

`scripts/imputation_delta.py` builds the full comparison, with the 2.x map's reason for each row.

## See also

- [Counting rules](explanation/counting-rules.md) gives the reason for every rule above.
- [Indicators](reference/indicators.md) lists the 3.0 indicators.
- [Changelog](changelog.md) lists every change in 3.0.0.

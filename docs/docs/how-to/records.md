# How to get the rows behind a number

`oda.records()` returns the rows a summed indicator adds up, with the same filters and unit as `oda.get()`. Pass `columns=` to bring extra table columns along, such as a CRS project title.

## List the activities behind a figure

```text
                                  sector_name                                                  project_title  value
3                 Basic health infrastructure                   FOURNITURE EQUIPEMENT MEDICAL SANTE PUBLIQUE   6.83
1                             Basic nutrition                                             P209 - AAP - Kenya   1.08
20  Promotion of mental health and well-being  FID - Lever les obstacles aux soins de santé mentale au Kenya   0.65
```

```python
import oda_data as oda

rows = oda.records(
    "oecd.crs.bilateral_oda",
    years=2023,
    providers="FRA",
    recipients="KEN",
    sectors=oda.sectors.health,
    columns=["project_title", "crs_id"],
)
top = rows.sort_values("value", ascending=False)
print(top[["sector_name", "project_title", "value"]].head(3).round(2).to_string())
```

Each row carries every dimension of the indicator, each code with its name beside it, then the extra columns, then the indicator, basis, unit, and `value` columns. `records()` has no `by=` and no group totals.

## Check that the rows add up

The rows of a `records()` call sum to the `get()` call with the same filters.

```text
9.500114 9.500114
```

```python
import oda_data as oda

filters = dict(years=2023, providers="FRA", recipients="KEN", sectors=oda.sectors.health)

rows = oda.records("oecd.crs.bilateral_oda", **filters)
total = oda.get("oecd.crs.bilateral_oda", by=["provider"], **filters)
print(round(rows.value.sum(), 6), round(total.value.iloc[0], 6))
```

## Choose extra columns

`columns=` takes any column of the table the indicator reads. Ask for a name that does not exist and the error lists every column the rows hold. On the CRS these include `project_title`, `short_description`, `long_description`, `crs_id`, `project_number`, `agency_name`, `channel_reported_name`, and `keywords`.

A column that `records()` already produces, such as `provider_name`, raises `UnsupportedQueryError`, since two columns would share one name.

## Convert the rows

`unit=` converts each row with its provider's factor for its year, as `get()` does before it sums.

```text
                    sector_name currency    prices  base_year     value
0   Basic drinking water supply      EUR  constant       2023  0.030000
1  Health personnel development      EUR  constant       2023  0.277365
2              Higher education      EUR  constant       2023  0.055259
```

```python
import oda_data as oda

rows = oda.records(
    "oecd.crs.bilateral_oda",
    years=2023,
    providers="FRA",
    recipients="KEN",
    unit=oda.constant("EUR", 2023),
)
print(rows[["sector_name", "currency", "prices", "base_year", "value"]].head(3))
```

## Which indicators have records

`records()` works on `sum` and `method` indicators, which covers every CRS, Multisystem, DAC1, and DAC2A count and `one.crs.imputed_multilateral_oda`. DAC1 and DAC2A publish one row per provider, year, and line, so their records hold no more detail than `get()`. A `ratio` such as `oecd.dac1.oda_gni_ratio` and a `derived` indicator such as `one.dac1.core_oda` raise `UnsupportedQueryError`. [Indicators](../reference/indicators.md) gives each indicator's kind.

## See also

- [How to get imputed multilateral ODA by sector](imputed-multilateral.md) reads `allocation_status`, `share_channel`, and `share_years` from the imputation's records.
- [Public API](../reference/api.md#records) documents every argument of `records()`.

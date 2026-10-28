# How to break down ODA by sector and policy marker

Sectors and policy markers come from the CRS, so use a CRS indicator such as `oecd.crs.bilateral_oda`. Filter with `sectors=`, split with `by=["sector"]` or `by=["broad_sector"]`, and pick a marker by its indicator id.

## Split by broad sector and compute shares

```text
   provider_name            broad_sector_name     value  share
38       Germany  Refugees in Donor Countries  7286.220  0.247
6         France                    Education  1704.207  0.153
16        France  Refugees in Donor Countries  1281.810  0.115
28       Germany                    Education  3267.953  0.111
```

```python
import oda_data as oda

df = oda.get(
    "oecd.crs.bilateral_oda",
    years=2023,
    providers=["FRA", "DEU"],
    by=["provider", "broad_sector"],
)
df["share"] = df.value / df.groupby("provider").value.transform("sum")
print(df.sort_values("share", ascending=False)[["provider_name", "broad_sector_name", "value", "share"]].head(4).round(3))
```

A share is one line of pandas on the result. CRS values are gross disbursements by default. Pass `basis="commitment"`, `"net_disbursement"`, or `"grant_equivalent"` (2018 on) for the others.

## Filter to one sector

`sectors=` takes purpose codes, exact purpose names, and the sector groups in `oda.sectors`. A three-digit code such as 122 is a DAC5 heading and reads every purpose code under it. `oda.sectors.reported_at(122)` reads only the activities filed at the heading with no finer purpose code, which is how the built-in groups list them. A heading ending in zero covers the headings under it, so 120 (Health) covers 121, 122, and 123.

```text
   sector                                  sector_name  value
2   12182                             Medical research  296.6
20  13030                              Family planning  169.3
0   12110  Health policy and administrative management  124.9
19  13020                     Reproductive health care  105.1
7   12250                   Infectious disease control   79.3
```

```python
import oda_data as oda

df = oda.get(
    "oecd.crs.bilateral_oda",
    years=2023,
    providers="GBR",
    sectors=oda.sectors.health,
    by=["sector"],
)
print(df[["sector", "sector_name", "value"]].sort_values("value", ascending=False).head(5).round(1))
```

`oda.sectors.health` holds 31 codes, from basic health to population policies. To find a group, search for it. The `use` column gives the argument to pass:

```text
sector_group water_supply_sanitation sectors=oda.sectors.water_supply_sanitation
```

```python
import oda_data as oda

found = oda.search("water")
print(found[["kind", "key", "use"]].to_string(index=False, header=False))
```

## Split ODA by a policy marker

Each policy marker is its own indicator, such as `oecd.crs.gender_marker` or `oecd.crs.climate_adaptation_marker`, and `score` is its dimension. A score is `principal`, `significant`, `not_targeted`, or `not_screened`. Summed over scores, a marker indicator is all bilateral allocable ODA.

```text
score  not_screened  not_targeted  principal  significant  targeted_share
year
2019            NaN       461.870    536.587     2048.121           0.848
2020        165.010       528.552    501.770     2015.618           0.784
2021        251.170       712.896    507.865     2103.656           0.730
2022        305.370       643.958    338.521     1626.661           0.674
2023         92.333       813.747    289.782     1573.878           0.673
```

```python
import oda_data as oda

df = oda.get(
    "oecd.crs.gender_marker",
    years=range(2019, 2024),
    providers="SWE",
    by=["year", "score"],
)
wide = df.pivot(index="year", columns="score", values="value")
wide["targeted_share"] = (wide.principal + wide.significant) / wide.sum(axis=1)
print(wide.round(3))
```

Two-thirds of Sweden's bilateral allocable ODA in 2023 targeted gender equality, down from 85 per cent in 2019. A share over every score treats unscreened money as not targeted. Divide by `principal + significant + not_targeted` to leave it out.

The markers are gender, environment, participatory development (`dig_marker`), trade, reproductive and child health (`rmnch_marker`), disaster risk reduction (`drr_marker`), nutrition, disability, biodiversity, climate mitigation, climate adaptation, and desertification. [Indicators](../reference/indicators.md#crs) lists their ids.

/// warning | Heads up
Marker indicators start in 2010, the first year the CRS records the aid type of nearly every activity. An earlier year raises `BasisRangeError`. An activity can score on several markers at once, so adding the principal rows of mitigation and adaptation counts some money twice.
///

## See also

- [How to get the rows behind a number](records.md) shows the CRS activities, with project titles, behind a sector figure.
- [Imputed multilateral sectors](imputed-multilateral.md) adds the sector split of core contributions to multilateral organisations.
- [Counting rules](../explanation/counting-rules.md#crs-and-dac1-totals) explains why CRS totals differ from DAC1 totals.

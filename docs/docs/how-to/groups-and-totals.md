# How to work with groups and totals

Pass a group to `providers=` or `recipients=` to get one row per member. Pass `oda.total(group)` to get the group's total as one row. Pass both to get the members and their total.

## Get a group's members and its total

```text
  provider     provider_name  is_aggregate     value
0        3           Denmark         False   3161.02
1        8            Norway         False   5178.59
2       10            Sweden         False   5003.48
3       18           Finland         False   1408.00
4       20           Iceland         False    110.19
5  nordics  Nordic countries          True  14861.28
```

```python
import oda_data as oda

nordics = oda.Group("nordics", ["DNK", "FIN", "ISL", "NOR", "SWE"], label="Nordic countries")

df = oda.get(
    "oecd.dac1.total_oda",
    years=2024,
    providers=[nordics, oda.total(nordics)],
    by=["provider"],
)
print(df[["provider", "provider_name", "is_aggregate", "value"]])
```

Members take DAC codes, three-letter codes, exact names, and other groups. The total's row carries the group's name as its code and `is_aggregate = True`. Summing the rows where `is_aggregate` is false counts each provider once.

`oda.total()` needs its dimension in `by`, here `provider`. Without it the total would be added to its own members, so the call raises `UnsupportedQueryError`.

## Use a built-in group

`oda.groups` holds the provider and recipient groups that ship with the package. Print `list(oda.groups)` for every name.

- `dac_countries`: the 32 DAC member countries plus Romania, whose sum is the OECD's DAC total
- `non_dac_countries` and `all_bilateral`: official providers outside the DAC, and both sets together
- `g7` and `eu27`: today's G7 and EU members
- `multilateral` and `private_donors`: multilateral organisations, and private foundations
- `all_recipients` and `developing_countries`: every recipient, and the countries and regions that receive ODA
- `ldc_countries`, `african_countries`, `africa_regional`, `sahel`, and `france_priority`: recipient groups

```text
       recipient             recipient_name  is_aggregate         value
0  ldc_countries  Least developed countries          True  61109.519111
```

```python
import oda_data as oda

df = oda.get(
    "oecd.dac2a.recipient_oda",
    years=2023,
    recipients=oda.total(oda.groups.ldc_countries),
    by=["recipient"],
)
print(df[["recipient", "recipient_name", "is_aggregate", "value"]])
```

## Follow membership year by year

Built-in groups keep one composition in every year, so a ten-year series counts the same countries throughout. `group.dated()` uses the membership of each year instead. `dac_countries`, `eu27`, and `g7` have dates.

```text
        provider  provider_name  year  is_aggregate      value
0  dac_countries  DAC countries  1990          True   53131.14
1  dac_countries  DAC countries  2024          True  214466.55
```

```python
import oda_data as oda

dac_then = oda.groups.dac_countries.dated()

df = oda.get(
    "oecd.dac1.total_oda",
    years=[1990, 2024],
    providers=oda.total(dac_then),
    by=["provider", "year"],
)
print(df[["provider", "provider_name", "year", "is_aggregate", "value"]])
```

The fixed group gives USD 54.3 billion for 1990 and the dated group USD 53.1 billion, since Spain, Portugal, Korea, and four other countries that reported in 1990 joined the DAC later.

## Read an OECD aggregate row

An OECD aggregate code returns the OECD's own row, marked `is_aggregate = True`. The DAC countries total, 20001, equals `oda.total(oda.groups.dac_countries)` in every year.

```text
        provider         provider_name  year  is_aggregate      value
0          20001  DAC Countries, Total  1990          True   54342.39
1          20001  DAC Countries, Total  2024          True  215141.78
2  dac_countries         DAC countries  1990          True   54342.39
3  dac_countries         DAC countries  2024          True  215141.78
```

```python
import oda_data as oda

dac = oda.groups.dac_countries

df = oda.get(
    "oecd.dac1.total_oda",
    years=[1990, 2024],
    providers=[20001, oda.total(dac)],
    by=["provider", "year"],
)
print(df[["provider", "provider_name", "year", "is_aggregate", "value"]])
```

The name `"DAC countries"` could mean the OECD row or the group, so it raises `AmbiguousEntityError`. Pass the code or the group.

## Handle a member that has not reported yet

A group total is null in a year in which a member that reported before has not reported yet. That happens in the table's last year, when some providers have filed and others have not. `allow_partial=True` sums whoever reported.

```text
oecd.dac1.in_donor_refugee_costs: 2025 is partial: providers that reported a part in 2024 have not reported it in 2025. Line 1820, flow 1160 is short in the dac_countries total, which sums only Austria (1) and Portugal (9).
        provider  year     value
0  dac_countries  2024  27972.50
1  dac_countries  2025    127.67
```

```python
import warnings

import oda_data as oda

total = oda.total(oda.groups.dac_countries)

with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter("always")
    df = oda.get(
        "oecd.dac1.in_donor_refugee_costs",
        years=[2024, 2025],
        providers=total,
        by=["provider", "year"],
        allow_partial=True,
    )
for warning in caught:
    print(warning.message)
print(df[["provider", "year", "value"]])
```

Without `allow_partial=True`, the 2025 value is null, and `explain()` says the total "is empty in 2025, when a member that reported a part in 2024 has not yet reported it in 2025". USD 128 million is what Austria and Portugal have reported so far, against USD 28.0 billion for 2024.

/// warning | Heads up
EU Institutions (918) sit outside `dac_countries`. On DAC1 indicators, a sum of 918 with a provider that pays it in a year of the call raises `UnsupportedQueryError`, because the payer reports that money as its own multilateral ODA. A payer is an EU member that year or any provider that reports line 2102, such as the United Kingdom after 2020, Norway, and Türkiye. Multisystem has no line 2102 and refuses 918 beside EU members. `one.dac1.total_oda_eu_attributed` nets 918's row of the line 2102 every other provider reports, so 918 adds to any of its funders without that double count.
///

## See also

- [Counting rules](../explanation/counting-rules.md) explains the null-total rule, aggregate rows, and the 918 rule.
- [Public API](../reference/api.md) documents `Group`, `total`, and `oda.groups`.

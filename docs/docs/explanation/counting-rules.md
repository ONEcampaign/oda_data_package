# Counting rules

Every number oda-data returns comes from a rule about which rows of an OECD table count. The figures come from the DAC1, DAC2A, and CRS releases of October 2026.

## The headline basis

ODA is counted on a basis: net disbursements, gross disbursements, commitments, or grant equivalents. The OECD changed its headline measure in 2018. Up to 2017 it reported ODA as net flows, money disbursed less loan repayments. From 2018 it reports grant equivalents, which count a loan by how generous its terms are.

oda-data's default basis for DAC1 indicators, `headline`, follows the OECD's own series: line 1010 under net flows (flow 1140) to 2017, and line 11010 under grant equivalents (flow 1160) from 2018. On DAC2A, which has no grant equivalents, `headline` is net disbursements (line 206). CRS indicators default to gross disbursements, the basis the CRS reports most fully.

Each row's `basis` column holds the basis it was counted on, so a headline series shows `net_disbursement` to 2017 and `grant_equivalent` from 2018, and no row mixes the two. A series that needs one basis throughout names it, as in `basis="net_disbursement"`.

The [migration guide](../migration.md) shows what this changes for 2.x calls.

Grant equivalents start in 2018 on every named indicator, and an earlier year raises `BasisRangeError`. The OECD published grant equivalents for 2015 to 2017 too, but only the 25 to 27 DAC members that reported them early, so a total over those years compares a subset with a full set. `oecd.dac1.lines` reads them with `basis="grant_equivalent"` and warns.

`oecd.dac1.lines` on the `headline` basis reads each line under each year's headline flow. `lines=1010` therefore gives net flows to 2017 and line 1010 as published under grant equivalents from 2018. `oecd.dac1.total_oda` reads line 11010 from 2018. For DAC countries the two series match in every year from 2018 to 2024.

GNI is published only under net flows, and the OECD's grant-equivalent ODA/GNI ratios divide by it. oda-data reads GNI on net flows whatever the basis of the ODA beside it.

## Why lines are never summed

DAC1 and DAC2A publish money as numbered lines, and the lines nest. Line 1010, total ODA, contains line 1015, bilateral ODA, which contains in-donor refugee costs, line 1820. Adding 1010 to 1015 counts bilateral ODA twice. Other lines hold no money at all: GNI (line 1), ODA as a per cent of GNI (11002), population (4).

So `line` always splits a result, as basis and unit do. `oecd.dac1.lines` and `oecd.dac2a.lines` return each line as its own row and refuse lines that are not money. Population has its own indicator, `oecd.dac1.population`, a count that takes no unit.

On DAC2A each line carries its own basis. Line 206 and its parts are net disbursements and lines 204 and 240 are gross disbursements, so a call reads 240 with `basis="gross_disbursement"` and cannot mix 206 and 240. A sum of several lines belongs in pandas, after you have checked that they do not nest.

## Rows that would count money twice

### EU Institutions (918)

EU member states report their contributions to the EU budget for development as their own multilateral ODA (DAC1 line 2102). The EU Institutions then report that money again as their own ODA. Adding 918 to its members counts it twice.

On DAC1 and Multisystem indicators, oda-data refuses a sum or group total that holds 918 and a country that was an EU member in any year of the call, the United Kingdom from 1973 to 2020 included. On DAC1 it also refuses 918 beside any provider that reports line 2102 in a year of the call, such as the United Kingdom after 2020, Norway, and Türkiye. In DAC1's last year, a provider with line 2102 the year before counts too, since most have not reported it yet. Multisystem has no line 2102, so there the rule refuses 918 beside EU members only. `oda.groups.dac_countries` leaves 918 out. A default provider set on these indicators holds 918 only when `provider` is in `by`, which keeps the rule from firing. The provider rows then add up to more than the sum across providers, by 918's value. Of the registered indicators, only `one.crs.imputed_multilateral_oda` defaults to a set that holds 918. On the CRS and DAC2A, money is counted where it is spent, so 918 and its members add up and the rule does not apply. `one.dac1.total_oda_eu_attributed` gives the DAC1 sum. Its 918 row is 918's ODA less the line 2102 that every other provider reported that year, the United Kingdom, Norway, and Türkiye included. Each dollar then sits in the row of the provider that paid it, so 918 adds up once with any set of providers, and its row is the same whichever providers the call names. `oecd.dac1.total_oda` gives 918's own spending.

The 918 row is negative in a year in which providers recorded more contributions than the EU Institutions reported as ODA. That happens in 21 years from 1971 to 2019. In 2019, on grant equivalents, providers recorded USD 15,866 million against 918's 14,937 million, so the row is USD -928 million. oda-data shows the row as computed.

In DAC1's last year, a preliminary one, most providers have not yet reported line 2102. In 2025, 26 of the 28 providers that paid 918 in 2024 had not. A provider that paid in the year before the last and has no line 2102 in the last year makes 918's last-year row null, unless the call passes `allow_partial=True`. So is every sum that holds the row: an `oda.total()` group, and a sum across providers outside `by`, such as `providers=[oda.groups.eu27, 918]` with `by=["year"]`. A sum that kept the row would count the unreported payments twice, in the members' ODA and in 918's. `explain()` names those providers. A provider with no line 2102 in an earlier year paid nothing that year.

918 has no GNI on DAC1. `one.dac1.gni` gives it the summed GNI of each year's EU members, and `one.dac1.gni_fixed_eu27` the GNI of today's 27 members in every year.

### Multilateral organisations as DAC2A recipients

DAC2A lists multilateral organisations as recipients (codes 98, 816, 878, and 900 to 996). Those rows are core contributions, which the organisations' own rows carry on to the countries they reach. Named DAC2A indicators and `oecd.dac2a.lines` leave them out, and naming one raises `UnsupportedQueryError` with the reason. The recipients that remain sum to the OECD's developing countries total (10100) in every year from 1960 to 2024.

DAC2A also holds recipient totals, such as developing countries (10100) and regions. oda-data returns them only when `recipients=` names them, so a sum over recipients counts each flow once.

### Imputed multilateral shares on DAC2A

Line 106 attributes each multilateral organisation's outflows to the providers that fund it. A multilateral provider's row on that line is its own spending, so `oecd.dac2a.imputed_multilateral_oda` leaves out multilateral providers and the totals that hold them (20002, 20004, 20005), and refuses a filter that adds one.

### Aggregates

Provider codes from 20000 up, such as the DAC countries total (20001), are OECD aggregates of other rows. oda-data never sums them with member rows. An aggregate code passed as a filter returns the OECD's own row with `is_aggregate = True`. A ONE group, such as `oda.groups.dac_countries`, sums its members. A name that could mean both raises `AmbiguousEntityError`.

`dac_countries` is the 32 DAC member countries plus Romania, a DAC Associate that reports from 2008. That set sums to 20001 in every year from 1960 to 2025.

## Partial last year

The OECD publishes each table's latest year before every provider has reported it. In the October 2026 releases, 30 DAC countries had reported 2024 in-donor refugee costs and not 2025, and the CRS held 2025 activities from a handful of providers.

A total over those providers reads as the full total and is not. oda-data makes a group total null in a year in which a member that reported in an earlier year has reported nothing in that year or any later one. A member that has not started reporting counts as zero, and so does a member that skips a year and reports again: Spain and Portugal skip 1971 to 1979 in DAC1, Hungary 1993 to 2002. A sum across providers outside `by` returns its total for the last year, and the year's `OdaDataWarning` says which parts the sum is short of and for how many providers that reported them the year before, as does `explain()`. A provider that stopped earlier counts as zero there and does not count in the warning. `allow_partial=True` sums whoever reported and leaves the short sum out of the warning.

Providers report a table's last year in parts that differ by table. DAC1 providers report it line by line and flow by flow. A provider with rows for a line, or for a flow of it, in the year before the last and none in the last year has not yet reported that part. In 2025, Austria and Ireland alone split gross disbursements into grants (flow 1121) and non-grants (1122), against 47 providers in 2024, so a 2025 total of `flows=1121` over DAC countries is null. A group total of the part is null, and a total of a part the provider has reported keeps its value, whichever lines and flows the call names together. A call that asks for the last year gets one `OdaDataWarning` for that year. It names each part that providers have not yet reported once, with what the part does to the result: rows of a provider the call shows that are missing or null, with the provider named, and sums and `oda.total()` groups that are short of the part or have no row of it, with their providers counted. `explain()` gives one sentence per part. `oecd.dac1.in_donor_refugee_costs` for France over 2024 and 2025 returns 2024 alone and warns that line 1820, flow 1160 is missing for France. Over the G7 total for 2023 to 2025 it returns 2023 and 2024 and warns that the part is missing from the g7 total for 7 providers. With France shown beside the G7 total, the one warning names France and counts it among the 7. `explain()` also names the years the call asks for that the result has no rows for, and those the indicator's own table does not hold, before its first year or after its last, whether or not the result has a `year` column. `one.crs.imputed_multilateral_oda` reads its rows from Multisystem, which covers 2011 to 2025, so a call for 1980 says Multisystem does not hold it, though the imputation also reads the CRS from 1973. DAC2A providers report line by line too, but DAC2A's last year holds few providers. In the October 2026 release, 12 of the 131 with 2024 rows had reported 2025. A provider with any last-year row for a line has reported it, whichever recipient the row goes to, a recipient total such as 10100 or a multilateral agency included, and a recipient with no row in a reporting provider's last year counts as zero, as in any other year. CRS and Multisystem providers report a year whole, so a provider with any row in the last year has reported it, and a recipient, sector, or channel with no rows from that provider counts as zero.

The same rule applies to the parts of a derived indicator. In the table's last year, a provider that reported one of the lines core ODA subtracts the year before, and has not reported it yet, gets a null core ODA row. A recipient total of that provider's rows is null too. So is a sum across providers outside `by` that holds the provider's row, because its total ODA is in the sum and the line is not taken out, which would overstate the sum. In 2025, `one.dac1.core_oda` by year is null, and `allow_partial=True` returns 172,968.09, which subtracts only the lines reported so far. A sum that is null does not also warn that it covers only the providers that have reported.

A ratio over several providers, a group total or a sum outside `by`, leaves out a member with a denominator and no numerator. Once a member's numerator stops while its denominator goes on, as with GNI and no ODA, the ratio is null in those years unless the call passes `allow_partial=True`.

A row is one provider's when `provider` is in `by`, or when the call shows one provider in the row's year, as a dated group can. The rule follows each year, so a call for several years gives each year the row a call for that year alone gives.

### Check requested years against returned years

A call for 2024 and 2025 can return 2024 alone. France has not reported refugee costs for 2025, so the result has no row for it, and a chart of that result ends a year early. Compare the years you asked for with the years that came back, and read the warning.

```text
oecd.dac1.in_donor_refugee_costs: 2025 is partial: providers that reported a part in 2024 have not reported it in 2025. Line 1820, flow 1160 is missing for France (4).
  provider_name  year    value
0        France  2024  1227.19
missing years: [2025]
In-donor refugee costs (DAC1), headline basis: grant equivalent 2024.
The result has no rows for 2025.
France.
2025 is partial: France (4) reported line 1820, flow 1160 in 2024 and has not reported it in 2025, so the result has no 2025 rows of line 1820, flow 1160 for it.
Current USD (the default).
DAC1 downloaded 2026-10-28. 1 rows.
```

```python
import warnings

import oda_data as oda

requested = [2024, 2025]
q = oda.query("oecd.dac1.in_donor_refugee_costs", years=requested, providers="FRA")
with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter("always")
    df = q.to_pandas()

for warning in caught:
    print(warning.message)
print(df[["provider_name", "year", "value"]])
print("missing years:", sorted(set(requested) - set(df.year)))
print(q.explain())
```

`allow_partial=True` does not silence this warning, because the missing row has no partial value to return.

## CRS and DAC1 totals

The CRS reports activity by activity, and DAC1 reports each provider's totals. oda-data reads the CRS so that its bilateral ODA matches DAC1's:

- ODA rows are category 10 on cash bases. Category 10 matches DAC1 bilateral gross disbursements of DAC countries to within USD 1 million in 2016 and 2023.
- On grant equivalents, categories 10 and 60. From 2023 the OECD reports every private sector instrument in category 60 and gives a grant equivalent only to those that count as ODA. With category 60 the 2023 gap to DAC1 line 11015 is USD 30 million. Counting the instruments' cash flows would overstate 2023 by USD 10.7 billion.
- Bilateral rows are `bi_multi` 1, 3, 7, and 8. Code 2 is providers' core contributions to multilateral organisations, which DAC1 counts as multilateral ODA. Code 4 is the organisations' own outflows, `oecd.crs.multilateral_oda`.
- Private development finance is category 30 without `bi_multi` 2, the foundations' core contributions to multilateral organisations, since the organisations' own outflows count that money again.

Net disbursements agree within USD 0.5 billion in both years. For a headline total, use DAC1. Use the CRS for what DAC1 cannot split: recipients by sector, channel, modality, finance type, and policy marker.

## Finance types

`finance_types=` takes the CRS's raw type of finance codes, such as 110 (standard grant) or 421 (standard loan), and `finance_type` is a dimension of every CRS indicator. oda-data ships no grant or loan class above them, because each table already splits grants from other money in its own terms: CRS flows 11, 13, and 19, DAC1 flows 1121 (grants) and 1122 (non-grants) with `flows=`, and DAC2A lines such as 201 and 204. DAC1 and DAC2A have no finance type, and asking for one raises `DimensionError` pointing at `flows=` or `lines=`.

## Defaults

Indicators of what providers give, on DAC1 and Multisystem, default to `oda.groups.dac_countries`. DAC1 also holds 918, non-DAC reporters, and aggregates, and each choice gives a different total. Indicators of what recipients receive, on DAC2A and the CRS, default to every official provider in the table, aggregates and private donors excluded, since a recipient view without non-DAC providers understates what arrived. Recipients default to every recipient, regional and unallocated rows included. `describe()` states each indicator's defaults, and `explain()` and provenance record the ones a call used.

## Units

oda-data converts each provider's money with its own deflator for each year, before any sum, as the OECD does. A group's total in constant prices is the sum of its members' constant-price values. Ratios are computed in current USD and take no unit. A member's ratio is the same in any unit, since one factor scales its numerator and denominator alike. A group's ratio sums members whose deflators differ, so converting first would change the group's figure.

## Filters

Filters are sets. France named on its own and through `oda.groups.g7` and `oda.groups.eu27` counts once. Every filter takes a code or an exact name, since a loose match changes the number. `"Frnace"` raises `UnknownEntityError` and suggests France.

## See also

- [Indicators](../reference/indicators.md) gives each indicator's table, bases, and caveats.
- [How imputed multilateral sectors work](imputation-method.md) covers the rules of `one.crs.imputed_multilateral_oda`.

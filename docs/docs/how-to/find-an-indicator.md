# How to find the indicator for a question

Start from the question, then pick the indicator that counts what it asks. Each section below names the indicator, shows one call, and says which choice changes the number. [Indicators](../reference/indicators.md) lists all of them.

## Search with the words you have

`oda.search()` matches indicator names and descriptions, question words such as "aid effort" and "0.7", provider and recipient names, and a few region names such as "Sub-Saharan". The `use` column gives the call or argument to copy.

```text
aid effort    oda.get("oecd.dac1.oda_gni_ratio")
0.7           oda.get("oecd.dac1.oda_gni_ratio")
aid received  oda.get("oecd.dac2a.recipient_oda")
Sub-Saharan   recipients=10003
```

```python
import oda_data as oda

for words in ("aid effort", "0.7", "aid received", "Sub-Saharan"):
    print(f"{words:<14}{oda.search(words).use.iloc[0]}")
```

## How much does each donor give, and who meets 0.7%?

`oecd.dac1.oda_gni_ratio` is ODA as a share of gross national income, in percent. A value of 0.7 is 0.7%, so the UN target is `value >= 0.7`.

```text
   provider_name  value
2        Denmark   0.72
7         Norway   1.02
9         Sweden   0.79
15    Luxembourg   1.00
```

```python
import oda_data as oda

df = oda.get("oecd.dac1.oda_gni_ratio", years=2024, by=["provider"])
print(df[df.value >= 0.7][["provider_name", "value"]].round(2))
```

For a group, ask for `oda.total(group)`. Its ratio is the group's ODA over the group's GNI, which differs from the mean of the members' ratios. In 2024 the G7's ratio is 0.33% and the mean of its seven members is 0.41%.

```text
0.33 0.41
```

```python
import oda_data as oda

g7 = oda.groups.g7
total = oda.get("oecd.dac1.oda_gni_ratio", years=2024, providers=oda.total(g7), by=["provider"])
members = oda.get("oecd.dac1.oda_gni_ratio", years=2024, providers=g7, by=["provider"])
print(round(total.value.iloc[0], 2), round(members.value.mean(), 2))
```

For the amount in dollars, use `oecd.dac1.total_oda`. A ratio takes no `unit=`.

## How has aid changed in real terms?

Pass `unit=oda.constant("USD", 2024)` to `oecd.dac1.total_oda`. Every year then holds 2024 dollars, converted with each provider's own deflator.

```text
                 basis   current  constant_2024
year
2015  net_disbursement  131826.0       156229.0
2017  net_disbursement  147503.0       173086.0
2018  grant_equivalent  153878.0       172112.0
2024  grant_equivalent  215142.0       215142.0
```

```python
import pandas as pd

import oda_data as oda

call = {"years": [2015, 2017, 2018, 2024], "by": ["year"]}
current = oda.get("oecd.dac1.total_oda", **call).set_index("year")
constant = oda.get("oecd.dac1.total_oda", unit=oda.constant("USD", 2024), **call).set_index("year")
print(pd.DataFrame({"basis": current.basis, "current": current.value, "constant_2024": constant.value}).round(0))
```

The headline basis changes from net flows to grant equivalents in 2018, and constant prices keep that break, so 2017 and 2018 are counted differently. `explain()` names the basis of each span of years. [Constant prices and currencies](units.md) covers other base years and currencies, and [Counting rules](../explanation/counting-rules.md#the-headline-basis) explains the switch.

## How much does a country receive?

Two indicators answer, and they count different things. `oecd.dac2a.recipient_oda` is what the recipient received from every official provider, multilateral organisations' own outflows included, on net disbursements unless you pass `basis="gross_disbursement"`. `oecd.crs.bilateral_oda` reads the CRS activities of bilateral providers, so it splits by sector, channel, and marker, and leaves out the organisations' own outflows.

```text
DAC2A, net                            2,625.4
DAC2A, gross                          3,225.5
CRS bilateral, gross                  1,698.9
DAC2A, gross, DAC countries           1,516.4
CRS bilateral, gross, DAC countries   1,516.4
```

```python
import oda_data as oda

kenya = {"years": 2023, "recipients": "Kenya", "by": ["recipient"]}
dac = {"providers": oda.groups.dac_countries}
rows = {
    "DAC2A, net": oda.get("oecd.dac2a.recipient_oda", **kenya),
    "DAC2A, gross": oda.get("oecd.dac2a.recipient_oda", basis="gross_disbursement", **kenya),
    "CRS bilateral, gross": oda.get("oecd.crs.bilateral_oda", **kenya),
    "DAC2A, gross, DAC countries": oda.get("oecd.dac2a.recipient_oda", basis="gross_disbursement", **dac, **kenya),
    "CRS bilateral, gross, DAC countries": oda.get("oecd.crs.bilateral_oda", **dac, **kenya),
}
for label, df in rows.items():
    print(f"{label:<36}{df.value.iloc[0]:>9,.1f}")
```

The last two lines restrict both to the DAC countries, and they match. Quote DAC2A for what Kenya received, and use the CRS to say what the money was for.

## Compare a region

A region has three numbers on DAC2A, and they answer different questions. The sum of its countries (the African countries in `oda.groups.african_countries` less the five north of the Sahara) is the money assigned to named countries. The OECD total, South of Sahara, Total (10003), is larger, because it also holds programmes assigned to a region and no single country. South of Sahara, regional (289) holds only the programmes assigned to the whole region. `oda.search("Sub-Saharan")` finds 10003, and `oda.search("South of Sahara")` finds 10003 and 289.

```text
 58,242.5  sum of 49 countries
 63,186.4  South of Sahara, Total (10003)
  3,162.5  South of Sahara, regional (289)
 63,186.4  four sub-regional totals plus 289
```

```python
import oda_data as oda

north = [130, 133, 136, 139, 142]
south = [code for code in oda.groups.african_countries.members if code not in north]
call = {"years": 2023, "by": ["recipient"]}

countries = oda.get("oecd.dac2a.recipient_oda", recipients=south, **call)
totals = oda.get(
    "oecd.dac2a.recipient_oda", recipients=[10003, 289, 10050, 10051, 10052, 10053], **call
).set_index("recipient").value

print(f"{countries.value.sum():>9,.1f}  sum of {len(countries)} countries")
print(f"{totals['10003']:>9,.1f}  South of Sahara, Total (10003)")
print(f"{totals['289']:>9,.1f}  South of Sahara, regional (289)")
subregions = totals[["10050", "10051", "10052", "10053"]].sum()
print(f"{subregions + totals['289']:>9,.1f}  four sub-regional totals plus 289")
```

The country sum is USD 4.9 billion below the OECD total. The total equals the four sub-regional totals (Eastern, Middle, Southern, and Western Africa) plus 289. Say in the chart title which of the three you plotted. A sum over recipients leaves regional totals out unless `recipients=` names them, so each flow counts once.

## How much targets gender equality or climate?

Each policy marker is its own indicator, such as `oecd.crs.gender_marker`, `oecd.crs.climate_adaptation_marker`, and `oecd.crs.climate_mitigation_marker`, with a `score` dimension. A share needs a denominator, and the choice moves the number. Dividing by every score counts unscreened money as not targeted. Dividing by the screened scores leaves it out.

```text
all scores: 41.0%
screened only: 42.3%
```

```python
import oda_data as oda

df = oda.get("oecd.crs.gender_marker", years=2023, recipients="Kenya", by=["score"])
value = df.set_index("score").value
targeted = value.principal + value.significant
print(f"all scores: {targeted / value.sum():.1%}")
print(f"screened only: {targeted / (value.sum() - value.not_screened):.1%}")
```

An activity can score on several markers, so do not add the climate markers together. [Sectors and policy markers](sectors-and-markers.md) covers the scores and the marker years.

## What does multilateral attribution add?

A bilateral figure leaves out a provider's core contributions to multilateral organisations. Three indicators attribute that money back to the provider.

- `one.crs.imputed_multilateral_oda` spreads each provider's core contributions across recipients and sectors. [Imputed multilateral sectors](imputed-multilateral.md) shows it by sector.
- `one.dac2a.recipient_oda_with_imputed_multilateral` adds a provider's imputed share of multilateral outflows to its net ODA to a recipient.
- `one.dac1.total_oda_eu_attributed` reduces EU Institutions' row by what its members paid it, so 918 adds up with its funders.

```text
UK to Kenya, net: 60.1; with imputed multilateral: 222.8
EU Institutions: 26,873.3 reported, 8,223.9 net of funders
```

```python
import oda_data as oda

uk = {"years": 2023, "providers": "GBR", "recipients": "Kenya", "by": ["recipient"]}
direct = oda.get("oecd.dac2a.recipient_oda", **uk)
attributed = oda.get("one.dac2a.recipient_oda_with_imputed_multilateral", **uk)
print(f"UK to Kenya, net: {direct.value.iloc[0]:.1f}; with imputed multilateral: {attributed.value.iloc[0]:.1f}")

eu = {"years": 2023, "providers": 918, "by": ["provider"]}
reported = oda.get("oecd.dac1.total_oda", **eu)
netted = oda.get("one.dac1.total_oda_eu_attributed", **eu)
print(f"EU Institutions: {reported.value.iloc[0]:,.1f} reported, {netted.value.iloc[0]:,.1f} net of funders")
```

The attributed figures rest on each organisation's own spending shares, so label them as estimates.

## See also

- [Public API](../reference/api.md#search) sets out what `oda.search()` matches and how it ranks.
- [Counting rules](../explanation/counting-rules.md#check-requested-years-against-returned-years) shows how to check requested years against returned years before you plot a recent year.

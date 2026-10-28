# Chart ten years of G7 aid

By the end of this tutorial you'll have a chart of official development assistance (ODA) from each G7 country from 2015 to 2024, in constant 2024 dollars, and a note that says where every number came from.

## What you'll learn

- How to ask oda-data for an indicator, for chosen years and providers
- How to read the columns every result has
- How to convert to constant prices
- How to add a group's total beside its members
- How to get a plain-text description of a result

## What you'll need

- Python 3.12 or later
- About 15 minutes, and about 10 MB of disk for the OECD's DAC1 table
- matplotlib for the chart (`pip install matplotlib`)

Install oda-data:

```bash
pip install oda-data
```

## Step 1: get one number

Start with one provider and one year. oda-data names each indicator by who defines it, the table it reads, and what it counts, so `oecd.dac1.total_oda` is the OECD's total ODA from its DAC1 table.

You should see one row:

```text
  provider provider_name  year            indicator             basis     value
0        4        France  2024  oecd.dac1.total_oda  grant_equivalent  15448.26
```

```python
import oda_data as oda

df = oda.get("oecd.dac1.total_oda", years=2024, providers="France")
print(df[["provider", "provider_name", "year", "indicator", "basis", "value"]])
```

The first call downloads DAC1 from the OECD and keeps it on disk, so it takes a few seconds. Later calls read the copy on disk.

Every result has the same columns. The dimensions come first, here `provider` and `year`, each code with its name beside it. Then come the indicator, its version, the basis, the unit (`currency`, `prices`, `base_year`), the `scale`, `is_aggregate`, and the `value`. Money is in millions, so France gave USD 15.4 billion in 2024.

## Step 2: ask for ten years of the G7

Now widen the call. `providers=` takes codes, names, three-letter codes, and groups, and `oda.groups.g7` is the seven G7 countries. `years=` takes a range.

The output should start like this:

```text
  provider_name  year             basis     value
0        France  2015  net_disbursement   9039.30
1        France  2016  net_disbursement   9621.67
2        France  2017  net_disbursement  11330.86
3        France  2018  grant_equivalent  12136.17
4        France  2019  grant_equivalent  12211.36
5        France  2020  grant_equivalent  14124.70
```

```python
g7 = oda.groups.g7

df = oda.get("oecd.dac1.total_oda", years=range(2015, 2025), providers=g7)
print(df[["provider_name", "year", "basis", "value"]].head(6))
```

Notice that the `basis` changes in 2018. The OECD counts ODA as net flows up to 2017 and as grant equivalents from 2018, and oda-data's default basis, `headline`, follows the OECD's own series. The `basis` column tells you which one each row holds.

## Step 3: convert to constant prices

Money given in 2015 bought more than the same sum in 2024. To compare years, convert every row to constant 2024 US dollars. `oda.constant("USD", 2024)` builds that unit, and oda-data uses each provider's own deflator for each year.

```text
  provider_name  year currency    prices  base_year         value
0        France  2015      USD  constant       2024  10496.283891
1        France  2016      USD  constant       2024  11148.510457
2        France  2017      USD  constant       2024  12805.872280
```

```python
df = oda.get(
    "oecd.dac1.total_oda",
    years=range(2015, 2025),
    providers=g7,
    unit=oda.constant("USD", 2024),
)
print(df[["provider_name", "year", "currency", "prices", "base_year", "value"]].head(3))
```

France's 2015 ODA was USD 9.0 billion at the time and is USD 10.5 billion in 2024 dollars.

## Step 4: add the G7 total

Pass `oda.total(g7)` beside the group to get one more row per year with the G7's total. Its `is_aggregate` column is `True`, so you can always tell a total from a member.

```text
   provider provider_name  year  is_aggregate     value
70       g7  G7 countries  2015          True  111840.0
71       g7  G7 countries  2016          True  127607.0
72       g7  G7 countries  2017          True  130990.0
```

```python
df = oda.get(
    "oecd.dac1.total_oda",
    years=range(2015, 2025),
    providers=[g7, oda.total(g7)],
    unit=oda.constant("USD", 2024),
)
totals = df[df.is_aggregate]
print(totals[["provider", "provider_name", "year", "is_aggregate", "value"]].round(0).head(3))
```

## Step 5: draw the chart

Turn the members' rows into one column per country, then plot them. The total stays out of the chart, since it would dwarf every member.

```python
import matplotlib.pyplot as plt

members = df[~df.is_aggregate]
wide = members.pivot(index="year", columns="provider_name", values="value")

ax = wide.plot(title="ODA from G7 countries, constant 2024 USD millions")
ax.set_xlabel("")
plt.savefig("g7-oda.png", dpi=150, bbox_inches="tight")
```

Open `g7-oda.png`. You should see seven lines, with the United States far above the rest and Germany second.

## Step 6: describe the result

A chart in a report needs a note on its source. Build the same call with `oda.query` in place of `oda.get`. A `Query` runs once and keeps its result, and `explain()` describes it in plain text.

```text
Total ODA (DAC1), headline basis: net flows 2015-2017, grant equivalent 2018-2024.
7 G7 countries, plus their total, which covers all 7 in every year it returns.
Constant 2024 USD using DAC deflators from the same DAC1 release.
DAC1 downloaded 2026-10-28. 80 rows.
```

```python
q = oda.query(
    "oecd.dac1.total_oda",
    years=range(2015, 2025),
    providers=[g7, oda.total(g7)],
    unit=oda.constant("USD", 2024),
)
print(q.explain())
```

The note names the basis for each span of years, the providers, the unit, and the day the DAC1 table was downloaded.

## What you learned

- You asked for an indicator by id, with `years=` and `providers=`
- You read the fixed columns, including `basis` and `is_aggregate`
- You converted to constant prices with `unit=oda.constant("USD", 2024)`
- You added a group's total with `oda.total(group)`
- You described a result with `oda.query(...).explain()`

## What's next

- [Counting rules](explanation/counting-rules.md) explains why the basis changes in 2018 and why oda-data never adds one DAC1 line to another.
- [How to find the indicator for a question](how-to/find-an-indicator.md) maps donor effort, real terms, recipients, markers, and attributed multilateral aid to indicators.
- [How to use constant prices and other currencies](how-to/units.md) covers euros, several units in one call, and the deflators behind them.
- [Indicators](reference/indicators.md) lists every indicator, from bilateral ODA by sector to imputed multilateral sectors.

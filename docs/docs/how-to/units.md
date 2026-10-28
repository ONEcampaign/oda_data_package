# How to use constant prices and other currencies

Pass `unit=` to any money indicator. `oda.current(currency)` gives current prices and `oda.constant(currency, base_year)` gives constant prices of a base year. Without `unit=`, values are current US dollars.

## Convert to one unit

```text
  provider_name  year currency    prices  base_year     value
0        France  2015      EUR  constant       2024   9704.86
1        France  2024      EUR  constant       2024  14283.46
```

```python
import oda_data as oda

df = oda.get(
    "oecd.dac1.total_oda",
    years=[2015, 2024],
    providers="France",
    unit=oda.constant("EUR", 2024),
)
print(df[["provider_name", "year", "currency", "prices", "base_year", "value"]].round(2))
```

The `currency`, `prices`, and `base_year` columns say what each row holds. `base_year` is null for current prices.

## Ask for several units in one call

Pass a list. The call reads the tables once and returns one row per unit, so filter on `currency` and `prices` before you compare or sum.

```text
  provider_name  year currency    prices  base_year      value
0         Japan  2014      EUR  constant       2024     6856.0
1         Japan  2014      JPY   current       <NA>  1003783.0
2         Japan  2014      USD  constant       2024     7415.0
3         Japan  2014      USD   current       <NA>     9483.0
4         Japan  2024      EUR  constant       2024    15250.0
5         Japan  2024      JPY   current       <NA>  2497775.0
6         Japan  2024      USD  constant       2024    16494.0
7         Japan  2024      USD   current       <NA>    16494.0
```

```python
import oda_data as oda

df = oda.get(
    "oecd.dac1.total_oda",
    years=[2014, 2024],
    providers="Japan",
    unit=[
        oda.current("USD"),
        oda.constant("USD", 2024),
        oda.constant("EUR", 2024),
        oda.current("JPY"),
    ],
)
print(df[["provider_name", "year", "currency", "prices", "base_year", "value"]].round(0))
```

Japan's 2014 ODA is USD 9.5 billion at 2014 prices and USD 7.4 billion at 2024 prices, because the yen lost value against the dollar between the two years. DAC deflators account for both inflation and the exchange rate.

## Choose a currency

oda-data converts to the currencies of 17 DAC members: USD, EUR, GBP, JPY, CAD, AUD, CHF, CZK, DKK, HUF, ISK, KRW, NOK, NZD, PLN, RON, and SEK. Any other code raises `UnsupportedQueryError` before a table is read.

/// warning | Heads up
A unit needs factors for every provider and year in the call. Euro factors start in 1973, so `oda.current("EUR")` for 1971 raises `MissingFactorError`, which names the provider and year. Narrow `years=` or pick another currency.
///

## Check which deflators a result used

oda-data converts each provider's money with that provider's own deflator for each year, before any sum. A provider the deflator table lacks, such as a multilateral organisation, takes the DAC total's deflator, and `explain()` names it.

```text
Multilateral ODA (CRS), gross disbursements 2023.
International Development Association.
Recipients: every recipient in the table, regional and unallocated rows included (the default).
Constant 2023 EUR using DAC deflators from the DAC1 release. International Development Association took the DAC total's deflators.
CRS downloaded 2026-10-28; DAC1 downloaded 2026-10-28. 1 rows.
```

```python
import oda_data as oda

q = oda.query(
    "oecd.crs.multilateral_oda",
    years=2023,
    providers=905,
    by=["provider"],
    unit=oda.constant("EUR", 2023),
)
print(q.explain())
```

/// info | Why
Deflating a group's total with one deflator would weight every member by the group's price changes. Each provider's own factor applied before the sum matches how the OECD deflates its own series. The factors come from the same DAC1 release as the ODA figures, so a new OECD release moves both together.
///

## Ratios take no unit

`oecd.dac1.oda_gni_ratio` and the other ratios are per cent, computed in current USD. Passing `unit=` raises `UnsupportedQueryError`. The ratio's rows have null `currency`, `prices`, and `base_year`.

## See also

- [Counting rules](../explanation/counting-rules.md) explains why ratios use current prices.
- [Public API](../reference/api.md) documents `Unit`, `current`, and `constant`.

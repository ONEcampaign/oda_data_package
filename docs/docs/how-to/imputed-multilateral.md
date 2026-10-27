# How to get imputed multilateral ODA by sector

`one.crs.imputed_multilateral_oda` spreads each provider's core contributions to multilateral organisations across recipients and sectors, in the proportions each organisation spends its own money. Ask for it like any other indicator and split it with `by=`.

## Check how the money was spread

Every core dollar lands in one row, and `allocation_status` says how: `imputed` from the organisation's own spending, `stale_share` from an older window of it, `proxy` from a related fund's spending or one fixed purpose, or `unallocated`.

```text
  allocation_status   value
0           imputed  6060.3
1             proxy   451.3
2       unallocated   219.6
```

```python
import oda_data as oda

df = oda.get(
    "one.crs.imputed_multilateral_oda",
    years=2023,
    providers="GBR",
    by=["allocation_status"],
)
print(df[["allocation_status", "value"]].round(1))
```

The three rows add up to USD 6,731 million, the United Kingdom's core contributions in 2023 (`oecd.multisystem.core_contributions`). An allocation that loses or adds money raises `ImputationConservationError`.

## Add imputed to bilateral ODA by sector

Bilateral CRS ODA and imputed multilateral ODA share the gross disbursement basis, so the two add up sector by sector.

```text
                                bilateral  imputed   total
broad_sector_name
Refugees in Donor Countries        5311.0      0.0  5311.0
Health                              935.0   1970.0  2905.0
Humanitarian                        968.0    465.0  1433.0
Administrative Costs of Donors      881.0    330.0  1210.0
Multi-sector                        636.0    306.0   942.0
```

```python
import pandas as pd

import oda_data as oda

bilateral = oda.get("oecd.crs.bilateral_oda", years=2023, providers="GBR", by=["broad_sector"])
imputed = oda.get("one.crs.imputed_multilateral_oda", years=2023, providers="GBR", by=["broad_sector"])

both = pd.concat([bilateral, imputed])
wide = both.pivot_table(
    index="broad_sector_name", columns="indicator", values="value", aggfunc="sum", observed=True
)
wide.columns = ["bilateral", "imputed"]
wide["total"] = wide.bilateral + wide.imputed.fillna(0)
print(wide.sort_values("total", ascending=False).head(5).round(0))
```

Two-thirds of the United Kingdom's health ODA in 2023 went through multilateral organisations' core budgets, so a sector chart from bilateral CRS alone understates it threefold.

## See which channel lent its shares

`oda.records()` returns one row per provider, channel, recipient, and purpose, with `share_channel` and `share_years` naming the spending behind each share.

```text
channel  share_channel
44007    44002            409.6
Name: value, dtype: float64
```

```python
import oda_data as oda

rows = oda.records("one.crs.imputed_multilateral_oda", years=2023, providers="GBR")
proxy = rows[rows.allocation_status == "proxy"]
print(proxy.groupby(["channel", "share_channel"], observed=True).value.sum().round(1))
```

IDA's Multilateral Debt Relief Initiative (channel 44007) reports no spending of its own, so its USD 410 million takes the shares of IDA's ordinary lending (44002). The other USD 42 million of proxy money goes to one purpose with no recipient, so its `share_channel` is null.

/// warning | Heads up
Unallocated amounts and fixed-purpose proxies have no recipient. A call with `recipients=` leaves them out, so its total is smaller than the provider's core contributions.
///

## Troubleshooting

- `UnmappedChannelError` means a multilateral provider and agency pair in the CRS has no channel in the bundled crosswalk, usually a new OECD agency. The message names the pair and the money at stake. Report it on the issue tracker, since the crosswalk is reviewed by hand.
- `ImputationConservationError` means the rows of a provider, channel, and year did not sum to the core contribution. It signals a defect in oda-data. Report it with the call that raised it.

## See also

- [How imputed multilateral sectors work](../explanation/imputation-method.md) sets out the share windows, the fallbacks, and the judgement calls in the proxy table.
- [How to get the rows behind a number](records.md) covers `records()` for every summed indicator.

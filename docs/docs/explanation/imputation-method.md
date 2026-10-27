# How imputed multilateral sectors work

A provider's core contribution to the World Bank or UNICEF has no sector. It joins a pool that the organisation spends as it chooses. `one.crs.imputed_multilateral_oda` estimates where each provider's share of that pool went, by spreading the contribution across recipients and purposes in the proportions the organisation spends its own money. The method follows the OECD's sectoral imputation, which the OECD no longer publishes.

## The short answer

For each provider, channel, and year, oda-data takes the core contribution from the Multisystem table and the channel's own CRS spending over the three years ending in that year. The contribution is split in the spending's proportions. When a channel has no spending to split by, oda-data falls back, in order, to an older window of its spending, a reviewed proxy, or an `unallocated` row. Every dollar lands in exactly one row, and the method checks that it does.

A provider that gave USD 10 million to the Nordic Development Fund in 2021, when the fund spent 60 per cent of its 2019-2021 money on education and 40 per cent on health, has USD 6 million of imputed education ODA and USD 4 million of imputed health ODA through the fund.

## Matching CRS spending to a channel

Core contributions are reported under a Multisystem channel code, such as 44002 for IDA. The organisations' own spending is reported in the CRS under a provider and agency code. The bundled crosswalk, `multilateral_channel_crosswalk.csv`, maps each provider and agency pair to its channel by hand, with a reason on every row. Pairs marked `excluded` are left out, such as the EU's Macro-Financial Assistance instrument (provider 918, agency 5), which is funded by borrowing and has no core contribution channel. A pair with CRS money and no crosswalk row raises `UnmappedChannelError`, so a new OECD agency stops the calculation until someone reviews it.

We chose an exact, reviewed join over matching on names. A name match guesses, and a wrong guess moves money to the wrong organisation with nothing to show it happened.

## The share window

A channel's spending is the gross disbursements of its CRS rows in categories 10 (ODA) and 21 (other official flows), summed over the three years ending in the contribution's year. Rows that report a core contribution to another multilateral (`bi_multi` 2) are left out, since the Multisystem side counts that money.

Other official flows are in because several channels lend on market terms and report little or no ODA. IBRD (44001) received USD 7.07 billion of core contributions from 2015 to 2024, and IBRD, EBRD, IFC, and IDB Invest report almost nothing in category 10. On ODA alone their contributions would all be unallocated.

The three-year window smooths one-off disbursements, at the cost of a lag. A contribution is split by recent spending, which may differ from what the organisation spends in the year of the contribution. A window whose total is zero or negative gives no shares.

## The fallbacks

`allocation_status` records which step placed each row.

- `imputed`: the channel's own three-year window.
- `stale_share`: the channel has no spending in the current window, because it stopped reporting or because the Multisystem release runs ahead of the CRS. Its latest window ending at most five years earlier is used, and `share_years` names it.
- `proxy`: the channel reports no spending of its own. `channel_share_proxies.csv` names a reviewed stand-in, and `share_channel` says whose spending was used. A proxy never borrows from another proxy.
- `unallocated`: none of the above. The row carries the whole contribution with no recipient or sector.

The proxy table has two kinds of row. A `parent_fund` row gives a sub-fund its parent institution's spending mix. A `fixed_purpose` row puts the whole contribution on one purpose code with no recipient, for a channel that is a purpose more than an institution. A channel gets a proxy only when the stand-in has the same governing institution or the same operating model, and reports to the CRS across the whole window.

## The judgement calls

Two proxy rows decide where large sums go, and each could reasonably go the other way.

IDA's Multilateral Debt Relief Initiative (44007) received USD 7.08 billion of core contributions from 2015 to 2024 and reports almost nothing to the CRS. IDA itself reported USD 148.5 million of debt relief (purpose 60020) against USD 76.2 billion of ODA from 2022 to 2024. oda-data spreads the initiative's money across IDA's ordinary lending, since the payments compensate IDA for cancelled credits and restore its capacity to lend. Showing it as debt relief would also be defensible. IDA's HIPC trust fund (44003) takes the same treatment at about one eighteenth of the size.

UN peacekeeping core contributions (41310), USD 6.5 billion from 2015 to 2024, go entirely to purpose 15230, participation in international peacekeeping operations. No CRS reporter spends like a peacekeeping budget, and DAC rules already limit the ODA-eligible share of assessed contributions.

UN specialised agencies with no CRS-reporting peer, such as OCHA and UNESCO, and IFC (44004, USD 2.7 billion from 2015 to 2024) stay unallocated. Every multilateral private-sector arm has an empty category 10 pool, and IFC's own CRS rows are category 21 with no disbursements.

## Conservation

The rows of each provider, channel, and year must sum to the core contribution, or the calculation raises `ImputationConservationError`. The check runs in current USD before conversion, so a total by provider always equals `oecd.multisystem.core_contributions`.

## What the estimate cannot tell you

An imputed share assumes each provider's money is spent like the pool it joined. Organisations pool contributions and do not allocate by donor, so an imputed figure supports totals and trends, and says nothing about what one provider's money bought.

Unallocated and fixed-purpose rows have no recipient, so a call filtered by recipient leaves them out. A pandas `groupby` on `sector` with its default `dropna=True` drops the unallocated rows too. Group with `dropna=False`, or check `allocation_status`, before reading a sector total as complete.

## See also

- [How to get imputed multilateral ODA by sector](../how-to/imputed-multilateral.md) puts the indicator to work.
- [Counting rules](counting-rules.md) covers the rules every indicator shares.

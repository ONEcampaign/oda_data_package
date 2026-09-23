# Sector Imputations

Sector imputations help answer the question: **"When donors give core contributions to multilateral organizations, which sectors does that aid ultimately support?"** This advanced feature enables comprehensive sectoral analysis by combining bilateral aid with imputed multilateral allocations.

!!! note "2.8.0 rebuild"
The imputed multilateral pipeline was rebuilt in `oda_data` 2.8.0: a reviewed
provider/agency-to-channel crosswalk replaces fuzzy name matching, core
contributions that were previously silently dropped are now visible as
`unallocated`/`proxy`/`stale_share` rows, and the output carries a
provenance record. [Imputation Delta](imputation-delta.md) measures the
change in totals by year and donor, and the [Changelog](changelog.md) lists
the breaking changes.

## The Problem: Multilateral Contributions Have No Sector Codes

DAC data divides ODA into two categories:

1. **Bilateral aid**: Direct aid to developing countries with clear sector classifications
1. **Multilateral aid**: Core (unearmarked) contributions to organizations like the World Bank or UNICEF

The challenge: Core multilateral contributions don't have sector codes. They're unrestricted funding pooled with other donors' contributions. However, these organizations do spend money on specific sectors. **Sector imputations estimate how much of each donor's multilateral contribution reaches each sector based on how multilateral agencies actually spend their resources.**

## Understanding Imputed Multilateral Aid

### What It Represents

Sectoral imputed multilateral aid estimates what proportion of each donor's core contributions to multilateral agencies can be attributed to specific sectors (like education or health).

**Worked example** (computed from a small fixture, not live OECD data, see [Reproducing the worked examples](#reproducing-the-worked-examples)):

- Donor 4 gives $10 million in core contributions to the Nordic Development Fund (channel 47128) in 2021.
- Over its CRS-reported spending for 2019-2021, the Nordic Development Fund put 60% of its money towards education (purpose 11110) and 40% towards health (purpose 12110).
- Donor 4's imputed multilateral aid through the Nordic Development Fund is $6.0 million to education and $4.0 million to health.

### Why It Matters

Without imputations, your sectoral analysis only captures bilateral aid. You miss a potentially significant portion of donors' sectoral commitments made through the multilateral system. Imputations give you a more complete picture of:

- **Total sectoral spending**: Bilateral + imputed multilateral
- **Sectoral priorities**: Which sectors donors support through all channels
- **Delivery modalities**: How much aid reaches sectors directly vs. through multilaterals

## The Methodology

The package uses a methodology based on the OECD's approach (since discontinued), rebuilt in
2.8.0 to make every step explicit and every dollar traceable. Five steps:

### Step 1: Map CRS Rows to a Multilateral Channel

Before a multilateral agency's CRS-reported spending can be turned into sector shares, every
CRS row has to be attributed to a MultiSystem channel code, the same code the donor's core
contribution is reported under. `add_multilateral_channel_codes` does this with an exact join
on `(provider_code, agency_code)` against a reviewed crosswalk
(`src/oda_data/clean_data/multilateral_channel_crosswalk.csv`), replacing the fuzzy/regex name
matcher used before 2.8.0.

- A `(provider_code, agency_code)` pair whose crosswalk `status` is `excluded` is dropped (for
  example, the EU's Macro-Financial Assistance instrument, provider 918 agency 5: it is
  borrowing-funded and has no core-contribution channel of its own).
- A pair carrying nonzero CRS money that has no crosswalk entry at all raises
  `UnmappedChannelError`, naming the pairs, their names and the money at stake. A new OECD
  agency shows up as a loud failure, not a silently wrong allocation.
- Float-typed and missing agency codes (`agency_code == 1.0`, or no agency reported) are
  normalised before the join, so they match a crosswalk row keyed on the integer code or the
  reviewed "no agency" row.

`crs_channel_mapping.csv` (the full OECD channel codelist, refreshed via
`scripts/refresh_channel_crosswalk.py`) is a separate file: it is the descriptive code list used
by `add_channel_names`, not the crosswalk that resolves a provider/agency pair to a channel.

### Step 2: Compute Rolling Sector Shares

For each multilateral channel and year, `multilateral_spending_shares_by_channel_and_purpose_smoothed`
computes what share of that channel's CRS-reported spending went to each `(purpose_code, recipient_code)` pair, summed over a rolling window (`period_length`, default 3 years) ending at
that year.

The window's flow-type basis is controlled by `flow_types`, a tuple of any of `"ODA"` (CRS
category 10), `"OOF"` (21) and `"PSI"` (60). **The default is `("ODA", "OOF")`** (the
discontinued OECD sectoral-imputation practice) because several multilateral channels report
only OOF to the CRS: IBRD (channel 44001, $7.07 billion of core contributions 2015-2024), EBRD
(46015), IFC (44004) and IDB Invest all have zero Category-10 CRS rows. Under an ODA-only filter
their share pool is empty and their core money falls straight to `unallocated`. Pass
`flow_types=("ODA",)` for the stricter, official-ODA-definition basis. `spending_by_purpose`
(the bilateral counterpart used for direct CRS analysis) defaults to `flow_types=("ODA",)`;
before 2.8.0 its default applied no category filter.

### Step 3: Fall Back to a Stale Share for a Lapsed Reporter

A channel with no CRS rows in the window ending at the requested year (because it has stopped
reporting recently, or because a MultiSystem release runs a year ahead of the matching CRS
release) falls back to its most recent window with data, as long as that window is no more
than `max_share_age` years old (default 5). The output row is marked `allocation_status = "stale_share"`, and `share_years` names the years actually used, so a stale allocation is always
visible rather than silently mixed in with current ones.

A channel-window whose total spending is zero or negative counts as having **no share**, not a
divide-by-zero: it is never used, either for its own year or as a fallback for a later one.

### Step 4: Fall Back to a Proxy Share

A channel with no own current or stale share (because it never reports detailed CRS spending
of its own, typically a sub-fund of a larger institution) falls back to a reviewed proxy
channel's shares, read from `src/oda_data/clean_data/channel_share_proxies.csv`
(`use_proxy_shares=True` by default). The output row keeps `allocation_status = "proxy"` and
`channel_code` set to the *core-contribution* channel (never the proxy); `share_channel_code`
names which channel's shares were actually used. A proxy never chains: it is resolved against
the proxy channel's own current-or-stale shares only, never against a second proxy.

Two proxy types:

- **`parent_fund`**: the sub-fund's money is assumed to buy the same mix of things as its
  parent institution's own CRS-reported spending. For example, IDA's Multilateral Debt Relief
  Initiative (channel 44007) has essentially no CRS presence of its own (IDA reports only
  $148.5 million of debt-relief-purpose (60020) disbursements against $76.2 billion of
  Category-10 spending, 2022-2024), so its $7.08 billion of core contributions (2015-2024) is
  spread across IDA's ordinary lending mix (channel 44002) instead of being shown as debt
  relief. **This is a judgement call, not a data fact**: donor MDRI payments compensate IDA for
  cancelled credits and replenish its ordinary lending capacity, so treating that money as
  financing IDA's regular programme is economically defensible, but a `fixed_purpose` row
  showing it as debt relief (CRS purpose 60020) would be a reasonable alternative reading. The
  same reasoning, at roughly 1/18th the size, applies to IDA's HIPC Trust Fund (channel 44003).
  Other `parent_fund` rows (the Asian Development Fund onto ADB, three EBRD trust funds onto
  EBRD) are not judgement calls in the same sense. Those sub-funds have essentially no CRS
  identity of their own to weigh against the parent's.
- **`fixed_purpose`**: the money is allocated wholesale to one purpose code, with `recipient_code`
  null, because the channel represents a purpose rather than an institution with spending
  patterns of its own. UN peacekeeping core contributions (channel 41310, $6.5 billion
  2015-2024) are allocated entirely to purpose 15230 ("Participation in international
  peacekeeping operations"): no CRS-reporting institution stands in as a plausible share source
  for peacekeeping money, and DAC rules already cap the ODA-eligible share of assessed
  peacekeeping contributions before this pipeline sees it, so no second haircut is applied here.

A channel is added to the proxy table only if it passes a written test: same governing
institution or same operating model as the proxy channel, and the proxy channel reports to CRS
across the whole window with a rationale that names the evidence. Most UN bodies fail this test
and have no peer institution close enough to stand in for them (see Step 5).

### Step 5: Leave the Rest Unallocated

A channel with no own share, no stale share and no proxy is reported as its own row:
`allocation_status = "unallocated"`, `recipient_code` and `purpose_code` null, `value` equal to
the full core-contribution amount for that channel-year. This mostly affects UN specialised
agencies and funds with no CRS-reporting peer (generic UN core contributions, OCHA, UNESCO, OAS)
and IFC (channel 44004, $2.7 billion 2015-2024): every multilateral private-sector-investment
arm has an empty CRS Category-10 pool, and IFC's own CRS rows are 100% Category 21 with
`usd_disbursement == 0`, so no candidate passes the proxy test in Step 4.

Before 2.8.0, this money was not visible as a row at all (see
[What consumers need to know](#what-consumers-need-to-know-about-the-schema-change) below).

### Money Conservation

Every core dollar ends up in exactly one output row, whichever of Steps 2-5 resolved it. This is
enforced at build time, not assumed: `imputed_multilateral_by_purpose` sums the output back up
by `(year, donor_code, channel_code)` and compares it against the core contribution for that key,
both before and after currency conversion. A mismatch beyond `abs(diff) <= 1e-6 * abs(core) + 1e-9` raises `ImputationConservationError` rather than returning silently wrong totals.

## Output Schema

`imputed_multilateral_by_purpose` returns one row per `(year, donor_code, channel_code, recipient_code, purpose_code)` combination:

| Column               | Type             | Notes                                                                                                                                                                           |
| -------------------- | ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `year`               | int              |                                                                                                                                                                                 |
| `donor_code`         | int              | The core-contributing donor.                                                                                                                                                    |
| `channel_code`       | int              | Always the **core-contribution** channel, never the proxy channel.                                                                                                              |
| `recipient_code`     | nullable `Int64` | Null for `unallocated` and `fixed_purpose` proxy rows.                                                                                                                          |
| `purpose_code`       | nullable `Int64` | Null for `unallocated` rows only.                                                                                                                                               |
| `value`              | float            |                                                                                                                                                                                 |
| `currency`           | str              |                                                                                                                                                                                 |
| `prices`             | str              | `"current"` or `"constant"`.                                                                                                                                                    |
| `allocation_status`  | str              | One of `imputed`, `stale_share`, `proxy`, `unallocated`.                                                                                                                        |
| `share_channel_code` | nullable `Int64` | The channel whose CRS shares were used: equals `channel_code` for `imputed`/`stale_share`, the proxy's code for `proxy`, null for `unallocated` and `fixed_purpose` proxy rows. |
| `share_years`        | str or null      | `"YYYY-YYYY"`, the CRS years behind the share used. Null for `unallocated`.                                                                                                     |

`recipient_code`, `purpose_code` and `share_channel_code` are pandas nullable `Int64` (not
`float64`), so that a null `unallocated` row doesn't silently change the dtype of a column a
downstream `merge` depends on.

## What Consumers Need to Know About the Schema Change

Before 2.8.0, a channel with no resolvable share was simply missing from the output, and the
lost core money was invisible unless you separately reconciled against MultiSystem totals. From
2.8.0, that money is present as explicit `unallocated` (and `proxy`) rows, which changes how a
downstream total moves depending on how you aggregate:

- **`groupby(...)` with pandas' default `dropna=True`** silently drops rows with a null
  `purpose_code` (the `unallocated` rows) from any purpose-level grouping. A total built this
  way rises only by the money that moved from missing-entirely to `proxy` or `stale_share`, not
  by the full `unallocated` amount.
- **Summing `value` grouped by `donor_code` (or any grouping that doesn't touch `purpose_code`
  or `recipient_code`)** picks up the full `unallocated` amount, since those rows carry a real
  `value` and only their purpose/recipient columns are null.

[Imputation Delta](imputation-delta.md) measures how much money this moves for a given set of
donors and years.

## Using Sector Imputations

### Main Function: `imputed_multilateral_by_purpose()`

**Calculate Imputed Multilateral Aid by Sector:**

```python
from oda_data.indicators.research import sector_imputations

imputed = sector_imputations.imputed_multilateral_by_purpose(
    years=range(2019, 2022),  # 2019, 2020, 2021
    providers=[4],  # France
    measure="gross_disbursement",
    currency="USD",
    base_year=2020,  # Constant 2020 prices
)

imputed_2021 = imputed[imputed["year"] == 2021]
print(imputed_2021.head())
```

!!! note "Multiple Years Recommended"
The default 3-year rolling window means a single-year request can only use whatever CRS
history the reader already has cached; passing a `years` range that starts a few years before
the period you actually want gives every requested year a complete window instead of relying
on cache history alone.

### Function Parameters

```python
imputed_multilateral_by_purpose(
    years=None,                            # Years to analyze
    providers=None,                        # Donor codes (None = all)
    channels=None,                         # Multilateral channel codes (None = all)
    measure="gross_disbursement",          # Measure type
    currency="USD",                        # Target currency
    base_year=None,                        # For constant prices (None = current)
    shares_based_on_oda_only=None,         # Deprecated -- see below
    *,                                     # The parameters below are keyword-only
    flow_types=("ODA", "OOF"),             # CRS categories the shares are based on
    period_length=3,                       # Rolling window length, in years
    max_share_age=5,                       # Stale-share lookback, in years
    use_proxy_shares=True,                 # Fall back to a proxy channel's shares
    crs=None,                              # Pre-fetched CRS data (tests, pinned builds)
    multisystem=None,                      # Pre-fetched Multisystem data
    refresh=False,                         # Bypass the bulk cache and re-download
)
```

!!! note "Core Contributions Are Excluded from the Multilateral Spending Shares"
The multilateral spending shares this function imputes onto are built from CRS
data via `spending_by_purpose`, which excludes CRS rows reporting a donor's
core contribution to a multilateral organization (`bi_multi == 2`) by default.
Those core contributions are already the input on the other side of the
imputation (`core_multilateral_contributions_by_provider`, from MultiSystem
data). Including them again here would double count them.

### Deprecation: `oda_only` / `shares_based_on_oda_only`

Both flags are deprecated in favour of `flow_types` and emit a `DeprecationWarning` naming what
they used to do, since neither maps cleanly onto its replacement:

- `sector_imputations.spending_by_purpose(..., oda_only=True)` filtered CRS category `in (10, 60)` (ODA **and** Private Sector Instruments), not ODA alone.
- `imputed_multilateral_by_purpose(..., shares_based_on_oda_only=False)` (the old default)
  applied **no CRS category filter at all**, mixing OOF, export credits and every other flow
  category in alongside ODA. This is different from the new default, `flow_types=("ODA", "OOF")`, and there is no `flow_types` value that reproduces the pre-2.8.0 "no filter" basis
  exactly.

Pass `flow_types` directly instead.

## The Quality Report

`imputation_quality_report` looks at an already-built result and surfaces the things money
conservation alone does not catch:

**Check Imputation Quality:**

```python
from oda_data.indicators.research.imputation_quality import imputation_quality_report

report = imputation_quality_report(imputed)

print(report["totals_by_status"])       # money and share of total, per allocation_status
print(report["by_channel_year"])        # core money, status, staleness, per channel-year
print(report["single_purpose_concentration"])  # flags a share window dominated by one purpose
print(report["duplicates"])             # rows repeating the same output key
print(report["negatives"])              # rows with value < 0 (e.g. a CRS reversal)
print(report["crosswalk_misses"])       # rows with a null channel_code
```

`single_purpose_concentration` flags any shares-source window where one purpose code accounts
for 50% or more of that window's money by default (`concentration_threshold`), the pattern seen
in the EU's Macro-Financial Assistance instrument before it was excluded from the crosswalk, and
in the Islamic Development Bank's ITFC fold under `flow_types=("ODA", "OOF")`, where trade
finance (purpose 32262) dominates the pool.

## Provenance

Every result carries `result.attrs["provenance"]`: the upstream CRS and Multisystem release
identity, the crosswalk and proxy-table file vintage, the package version and the parameters the
call was made with.

```python
provenance = imputed.attrs["provenance"]
print(provenance["crs_release"])
print(provenance["crosswalk_vintage"])
print(provenance["package_version"])
```

!!! warning "Most pandas Operations Drop `.attrs`"
`DataFrame.attrs` is not preserved by most pandas operations, including many that look like
simple filters, such as some `groupby`/`merge` paths. Read `result.attrs["provenance"]` from
the frame `imputed_multilateral_by_purpose` returns directly, before any further
transformation. `imputation_quality_report` reads it from the same frame it is given, for the
same reason: if you pass it a result that has already passed through an operation that dropped
`.attrs`, `report["provenance"]` is `None`.

## Reproducing the Worked Examples

The worked examples above and in [Output Schema](#output-schema) are computed from small,
in-memory fixtures via `imputed_multilateral_by_purpose`'s `crs=`/`multisystem=` parameters
(not from a live OECD download), so they are exact and reproducible without a network call:

**Reproduce the Nordic Development Fund example:**

```python
import pandas as pd
from oda_data.clean_data.schema import ODASchema
from oda_data.indicators.research.sector_imputations import imputed_multilateral_by_purpose


def crs_rows(provider, agency, purpose_code, recipient_code, years, value, category=10):
    return [
        {
            ODASchema.PROVIDER_CODE: provider,
            ODASchema.PROVIDER_NAME: "Nordic Development Fund",
            ODASchema.AGENCY_CODE: agency,
            ODASchema.AGENCY_NAME: "Nordic Development Fund",
            ODASchema.PURPOSE_CODE: purpose_code,
            ODASchema.RECIPIENT_CODE: recipient_code,
            ODASchema.YEAR: y,
            ODASchema.CATEGORY: category,
            "usd_disbursement": value,
        }
        for y in years
    ]


crs = pd.concat(
    [
        pd.DataFrame(crs_rows(104, 1, 11110, 236, range(2019, 2022), 60.0)),  # education
        pd.DataFrame(crs_rows(104, 1, 12110, 236, range(2019, 2022), 40.0)),  # health
    ],
    ignore_index=True,
)

multisystem = pd.DataFrame(
    {
        ODASchema.PROVIDER_CODE: [4],
        ODASchema.CHANNEL_CODE: [47128],  # Nordic Development Fund
        ODASchema.YEAR: [2021],
        "amount": [10.0],
        "flow_type": ["Disbursements"],
        "amount_type": ["Current prices"],
    }
)

result = imputed_multilateral_by_purpose(years=[2021], crs=crs, multisystem=multisystem)
print(result)
```

**Output:**

```
   year  donor_code  channel_code  recipient_code  purpose_code  value currency   prices allocation_status  share_channel_code share_years
0  2021           4         47128             236         11110    6.0      USD  current           imputed               47128   2019-2021
1  2021           4         47128             236         12110    4.0      USD  current           imputed               47128   2019-2021
```

The same pattern, with a channel that has stopped reporting recently (`905`/`1`, IDA, channel
`44002`, CRS data only through 2018) and a core contribution in 2021, produces a `stale_share`
row instead:

**Output (stale_share):**

```
   year  donor_code  channel_code  recipient_code  purpose_code  value currency   prices allocation_status  share_channel_code share_years
0  2021          12         44002             998         11110    2.4      USD  current       stale_share               44002   2016-2018
1  2021          12         44002             998         32130    5.6      USD  current       stale_share               44002   2016-2018
```

And a proxy channel (IDA-MDRI, channel `44007`, no CRS presence of its own) alongside a channel
with no proxy (IFC, channel `44004`) produces one `proxy` row per purpose plus one `unallocated`
row:

**Output (proxy and unallocated):**

```
   year  donor_code  channel_code recipient_code purpose_code  value currency   prices allocation_status  share_channel_code share_years
0  2021          76         44007            998        11110    1.5      USD  current             proxy               44002   2019-2021
1  2021          76         44007            998        32130    3.5      USD  current             proxy               44002   2019-2021
2  2021          51         44004           <NA>         <NA>    6.0      USD  current       unallocated                <NA>        <NA>
```

The rest of this page's other worked examples (the France sectoral totals, the delivery-modality
breakdown, the top health channels) run against live CRS/Multisystem data and their output
figures are **illustrative**. They depend on the OECD release the example was last run against
and will not match exactly against a current download.

### Advanced: Custom Analysis with Helper Functions

For researchers needing more control, use the lower-level functions:

**Custom Imputation Analysis:**

```python
from oda_data.indicators.research import sector_imputations

# Get multilateral spending shares (3-year smoothed, ODA+OOF basis)
spending_shares = sector_imputations.multilateral_spending_shares_by_channel_and_purpose_smoothed(
    years=range(2020, 2023),
    flow_types=("ODA", "OOF"),
    period_length=3,
)

# Get core contributions from bilateral donors
core_contributions = sector_imputations.core_multilateral_contributions_by_provider(
    years=[2022],
    providers=[4, 12, 76],  # France, UK, Germany
    measure="gross_disbursement",
)

# Examine spending patterns of specific multilateral agencies
print("IDA sectoral spending shares:")
print(spending_shares[spending_shares["channel_code"] == 44002].head())

print("\nCore contributions to IDA:")
print(core_contributions[core_contributions["channel_code"] == 44002])
```

**Output** (illustrative):

```
IDA sectoral spending shares:
   year  channel_code  purpose_code  recipient_code  share allocation_status share_years
0  2022         44002         11110             998  0.085           imputed   2020-2022
1  2022         44002         12110             998  0.102           imputed   2020-2022
2  2022         44002         14010             998  0.067           imputed   2020-2022
3  2022         44002         21010             998  0.134           imputed   2020-2022
4  2022         44002         24010             998  0.089           imputed   2020-2022

Core contributions to IDA:
   donor_code  channel_code  year     value currency   prices
0           4         44002  2022   444.00      USD  current
1          12         44002  2022   822.50      USD  current
2          76         44002  2022  1250.75      USD  current
```

### Available Helper Functions

- **`imputed_multilateral_by_purpose()`**: Main function for calculating imputations
- **`multilateral_spending_shares_by_channel_and_purpose_smoothed()`**: Get smoothed sector spending shares for multilateral channels, with the stale-share fallback
- **`core_multilateral_contributions_by_provider()`**: Get bilateral donors' core contributions to multilaterals
- **`spending_by_purpose()`**: Get CRS spending data aggregated by purpose
- **`imputation_quality_report()`**: QA report over an already-built result (`oda_data.indicators.research.imputation_quality`)
- **`add_multilateral_channel_codes()`**: Join CRS rows to a multilateral channel code (`oda_data.clean_data.channels`)

## Common Issues and Solutions

### Issue 1: `UnmappedChannelError`

**Problem**: A `(provider_code, agency_code)` pair carrying nonzero CRS money has no entry in
the reviewed crosswalk, most often because the OECD has added a new multilateral agency since
the crosswalk was last refreshed.

**Unmapped Channel Error:**

```python
from oda_data.clean_data.channels import add_multilateral_channel_codes

mapped = add_multilateral_channel_codes(crs_df)
# oda_data.clean_data.channels.UnmappedChannelError: Unmapped multilateral channel pairs
# carry nonzero value and are not in the crosswalk. ... Pairs: provider_code=..., agency_code=...
```

**Solution**: Add the pair to `multilateral_channel_crosswalk.csv` (see
`scripts/refresh_channel_crosswalk.py`), or pass `on_unmapped="unallocated"` to keep those rows
with a null `channel_code` instead of raising:

```python
mapped = add_multilateral_channel_codes(crs_df, on_unmapped="unallocated")
```

`imputed_multilateral_by_purpose` itself does not expose `on_unmapped`. Its internal CRS join
always raises on an unmapped pair carrying money, so a crosswalk gap surfaces immediately rather
than silently missing sector shares.

### Issue 2: `ImputationConservationError`

**Problem**: The allocated output for some `(year, donor_code, channel_code)` combination does
not sum back to its core contribution amount. This should not happen against a correctly
patched/pinned `crs=`/`multisystem=` input; if it does against live data, it is worth reporting,
since it means a join or dtype mismatch dropped money silently rather than routing it to
`unallocated`.

### Issue 3: Empty Results

**Problem**: You get an empty DataFrame when calculating imputations for certain years or providers.

**Empty Results Example:**

```python
imputed = sector_imputations.imputed_multilateral_by_purpose(
    years=[2025],  # Very recent year
    providers=[999]  # Invalid provider code
)
print(len(imputed))
```

**Why this happens**:

- The year may not have complete data yet (CRS/MultiSystem data has reporting delays).
- The provider code doesn't exist or has no core contributions that year.

**Solution**: Use a year with complete reporting and verify the provider code:

**Verify Data Availability:**

```python
imputed = sector_imputations.imputed_multilateral_by_purpose(
    years=[2021],
    providers=[4],
)
if len(imputed) == 0:
    print("No data found. Check: is the year available? Is the provider code correct?")
```

### Issue 4: Missing Columns

**Problem**: You try to filter by a column that doesn't exist in the output, such as
`purpose_name`.

**Why this happens**: The function returns raw codes by default. Add names separately.

**Solution**: Use `add_names_columns()`:

**Add Names to Imputations:**

```python
from oda_data.tools.names.add import add_names_columns

imputed = sector_imputations.imputed_multilateral_by_purpose(years=[2021], providers=[4])
imputed = add_names_columns(imputed, ["provider_code", "channel_code", "purpose_code"])
education = imputed[imputed["purpose_name"] == "Education"]
```

## Important Considerations

### Limitations

**1. Imputations Are Estimates**

Imputations assume donor contributions are used proportionally to agency spending patterns. In reality:

- Multilateral agencies pool resources from multiple donors
- Spending patterns may not perfectly match contribution timing
- Some donors have specific influence on multilateral priorities

**2. Time Lag and Smoothing**

- Sector shares use a rolling average (3 years by default)
- This smooths out year-to-year variations but introduces a time lag
- Current contributions are allocated based on recent (but not necessarily current) spending patterns

**3. Judgement Calls Are Documented, Not Hidden**

A handful of crosswalk and proxy rows encode a judgement about how to treat money with no clean
institutional match (see [Step 4](#step-4-fall-back-to-a-proxy-share) above for IDA-MDRI). These
are recorded in `multilateral_channel_crosswalk.csv` and `channel_share_proxies.csv` with a
`rationale` column and `reviewed=true`, not silently chosen.

**4. Data Completeness**

- Not all multilateral organizations report detailed sectoral spending to the CRS.
- Some report only OOF, not ODA (Step 2); some report nothing recent enough for a stale share; a
  few have no plausible proxy and stay `unallocated` (Step 5).

**5. Methodological Variations**

Different organizations use different imputation approaches. This package implements ONE
Campaign's methodology (based on the discontinued OECD approach). Results may differ from other
sources.

### Best Practices

**Do:**

- Use imputations for aggregate analysis and trends
- Combine with bilateral aid for complete sectoral pictures
- Check `allocation_status` before treating every row as an equally solid `imputed` share
- Read `result.attrs["provenance"]` before any further transformation, and keep it if you need
  to trace a downstream figure back to an upstream release
- Compare results with direct multilateral reporting when available

**Don't:**

- Treat imputations as exact allocations
- Use for agency-specific accountability (agencies don't allocate by individual donor)
- Drop `unallocated`/`proxy`/`stale_share` rows without deciding whether that is the comparison you want (see [What consumers need to know](#what-consumers-need-to-know-about-the-schema-change))
- Assume perfect timing between contributions and spending

## Research Applications

The following applications run against live CRS/MultiSystem data; their output figures are
**illustrative**, from a past run, and will not match exactly against a current download.

### Application 1: True Sectoral Priorities

Reveal donors' **total** sectoral commitments across all channels:

**Compare Bilateral vs Total Sectoral Support:**

```python
from oda_data.indicators.research import sector_imputations
from oda_data.tools.names.add import add_names_columns

bilateral = sector_imputations.spending_by_purpose(
    years=[2021],
    providers=[4],
    flow_types=("ODA",),
    measure="gross_disbursement",
    base_year=2020,
)
bilateral = add_names_columns(bilateral, ["purpose_code"])
bilateral_edu = bilateral.loc[
    bilateral["purpose_name"].str.contains("Education", na=False), "value"
].sum()

imputed = sector_imputations.imputed_multilateral_by_purpose(
    years=range(2019, 2022),
    providers=[4],
    base_year=2020,
)
imputed = add_names_columns(imputed, ["purpose_code"])
imputed_2021 = imputed[imputed["year"] == 2021].copy()
imputed_edu = imputed_2021.loc[
    imputed_2021["purpose_name"].str.contains("Education", na=False), "value"
].sum()

total_edu = bilateral_edu + imputed_edu
print(f"Bilateral:            ${bilateral_edu:,.2f}")
print(f"Imputed Multilateral: ${imputed_edu:,.2f}")
print(f"Total:                ${total_edu:,.2f}")
print(f"Multilateral share:   {100 * imputed_edu / total_edu:.1f}%")
```

**Output** (illustrative, USD millions, 2020 prices):

```
Bilateral:            $273.80
Imputed Multilateral:    $125.92
Total:                   $399.72
Multilateral share:   31.5%
```

### Application 2: Delivery Modality Analysis

Compare how much aid reaches sectors directly vs. through multilaterals by pivoting a combined
bilateral + imputed frame on a `channel` label column (as built in Application 1), then computing
each side's share of the row total. This is a standard pandas `pivot_table` + row-percentage
pattern; see the package's test suite for a runnable fixture-based version.

### Application 3: Multilateral Channel Analysis

Analyze which multilateral channels deliver the most aid to a specific sector:

**Top Multilateral Channels for Health:**

```python
health_channels = (
    imputed[imputed["purpose_name"] == "Health"]
    .groupby("channel_name")["value"]
    .sum()
    .sort_values(ascending=False)
)
print(health_channels.head())
```

**Output** (illustrative):

```
channel_name
European Commission - Development Share of Budget                   20.86
International Development Association                                16.80
European Commission - European Development Fund                     12.60
World Health Organisation - core voluntary contributions account     11.05
Global Fund to Fight AIDS, Tuberculosis and Malaria                  10.90
Name: value, dtype: float64
```

## When to Use Sector Imputations

### Use imputations when you're:

- Analyzing total ODA by sector (bilateral + multilateral)
- Studying donors' complete sectoral portfolios
- Comparing sectoral priorities across donors
- Researching aid effectiveness by sector
- Understanding delivery modalities (direct vs multilateral)

### You may not need imputations when you're:

- Analyzing only bilateral aid flows
- Studying specific bilateral projects
- Focusing on direct donor-recipient relationships

## Related Features

- **Bilateral sectoral data**: See [Accessing Raw Data](data-sources.md) for CRS database access
- **Multilateral contributions**: See [Accessing Raw Data](data-sources.md) for MultiSystem database
- **What changed in 2.8.0**: See [Imputation Delta](imputation-delta.md) and the [Changelog](changelog.md)
- **Caching and provenance**: See [Cache Management](caching.md) for how the upstream release identity recorded in `result.attrs["provenance"]` is determined
- **Policy markers**: See [Policy Markers](policy-markers.md) for thematic analysis

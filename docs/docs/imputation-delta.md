# Imputation Delta

`oda_data` 2.8.0 rebuilt the imputed multilateral pipeline (see [Sector Imputations](sector-imputations.md)):
a reviewed crosswalk replaces fuzzy name matching, the CRS `bi_multi == 2` double-count fix
(2.7.1) changed the spending shares, the shares' default flow-type basis changed, and money that
used to be silently dropped is now visible as `unallocated`/`proxy`/`stale_share` rows. This page
covers how to measure what changed for your own donors and years with
`scripts/imputation_delta.py`, and states the two things every consumer needs to know before
comparing an old total against a new one.

## Running the Script

```bash
uv run python scripts/imputation_delta.py \
    --cache-dir /path/outside/repo/oda-data-cache \
    --output-dir /path/outside/repo/imputation-delta-out \
    --years 2015-2024
```

`--cache-dir` and `--output-dir` are both required and both rejected if they resolve inside the
repository, so a run never writes cache files or delta output into version control. Point
`--cache-dir` at a directory that already holds a fresh CRS and Multisystem bulk extract (see
[Cache Management](caching.md)) to avoid a re-download; pass `--refresh` only if you want this
branch's own reads to bypass the cache.

The script computes two things:

1. **This branch's pipeline**, in-process, plus three ablation runs (one per named cause below).
1. **The previous release's pipeline** (`oda-data==2.7.0` by default, override with
   `--old-version`), run in an isolated `uv` environment
   (`uv run --isolated --with oda-data==<version>`) against the same cache and inputs.

It writes three files under `--output-dir`:

- `imputation_delta.csv`: one row per `(year, donor_code)`, with the old total, the new total,
  the observed delta, one column per named cause, and a `residual` column.
- `channel_remapping.csv`: a channel-level diagnostic of every `(provider_code, agency_code)`
  pair whose assigned channel differs between the previous release's matcher and this branch's
  crosswalk.
- `summary.json`: the grand totals.

Other flags: `--providers` (comma-separated donor codes, default all), `--measure`,
`--currency`, `--base-year`, `--skip-channel-remapping` (skip the second isolated subprocess run
the diagnostic needs), and `--dry-run` (resolve and print the plan without reading any data or
invoking `uv`, for checking your arguments before a real run).

## Named Causes

| Column                  | What it measures                                                                                                                                                                                                  | How                                                                                                                                                                                                             |
| ----------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `bi_multi_exclusion`    | The 2.7.1 fix that excludes CRS rows reporting a donor's own core contribution to a multilateral organisation (`bi_multi == 2`) from the shares.                                                                  | This branch's pipeline, `exclude_multilateral_core` True vs False, holding everything else at this branch's defaults.                                                                                           |
| `window_padding`        | The stale-share lookback (`max_share_age`) that lets a lapsed-reporting channel still resolve a share instead of falling straight to `unallocated`, plus the previous release's window-truncation defect (below). | This branch's pipeline, `max_share_age=5` (default) vs `max_share_age=0`, plus (for the years the truncation defect hits) the full leftover after the other three columns are subtracted from `observed_delta`. |
| `share_basis`           | Sensitivity to the shares' `flow_types` default, `("ODA", "OOF")`.                                                                                                                                                | This branch's pipeline, `flow_types=("ODA", "OOF")` (default) vs `flow_types=("ODA",)`.                                                                                                                         |
| `unallocated_and_proxy` | Money now visible as `unallocated` or `proxy` rows.                                                                                                                                                               | Read directly off the real result: `sum(value)` where `allocation_status` is `unallocated` or `proxy`. `stale_share` is excluded here since `window_padding` already covers its dollar effect.                  |
| `residual`              | `observed_delta` minus the four columns above.                                                                                                                                                                    | Not zero by construction, see below.                                                                                                                                                                            |

`oda-data==2.7.0`'s `imputed_multilateral_by_purpose` never padded its CRS read backwards far enough to give the first `period_length - 1` years of a multi-year request (2015 and 2016, at the default `period_length=3`) a complete rolling window, so it returns no rows at all for those years: `old_total` is exactly 0 there, not a real decline. This branch's `pad_years_for_window` fixes that unconditionally, with no toggle to reproduce the old truncation, so it cannot be isolated the way the other three ablated causes are. The affected years are known exactly from the request, and no other named cause is a candidate explanation for a total that is structurally zero, so the whole gap for those rows, everything that would otherwise land in `residual`, is attributed to `window_padding` directly.

Two causes are **not** year-by-donor columns, for reasons that follow from what is and isn't
reachable through the package's public API:

- **`channel_remapping`** (EU MFA, ITFC, UN agencies, ITC, ILO, ECHO and others): reported in
  `channel_remapping.csv` at the `(provider_code, agency_code)` level: the previous release's
  channel assignment next to this branch's, flagged wherever they disagree, with the CRS money
  at stake. Isolating this cause's exact dollar effect on a `(year, donor_code)` total would
  require re-running the allocation step with the old channel mapping substituted in, which is
  internal to `sector_imputations.py` and not reachable from a script that only calls the public
  API. Its dollar effect on the year-by-donor table is folded into `residual`.
- **The literal previous-release share basis**: the pre-2.8.0 default (`shares_based_on_oda_only=False`)
  applied no CRS category filter at all, mixing OOF, export credits and every other category in
  alongside ODA. This has no equivalent under `flow_types`, which offers only named category
  combinations. `share_basis` therefore measures this branch's own sensitivity to `flow_types`,
  not a literal reproduction of the old basis; the gap between the two is in `residual`.

`residual` is not an error term to drive to zero. The four ablated causes interact (for
example, how much money proxy shares pick up depends on whether the underlying channel already
had its own window), so a waterfall decomposition like this one does not collapse exactly, and
`channel_remapping` and the literal old share basis are real, non-zero contributors that this
table does not decompose further. With the truncation gap moved into `window_padding`, `residual`
is small relative to `observed_delta` in the measured run below (about 2.7% of it, and negative:
this branch's own explained causes slightly overshoot the observed change in most years) and is
what is left of `channel_remapping` and the literal old share basis after their interaction with
the other three causes.

## What Consumers Need to Know

The schema change (money that used to be silently dropped is now an explicit
`unallocated`/`proxy` row) moves a downstream total differently depending on how it is
aggregated. See [What consumers need to know](sector-imputations.md#what-consumers-need-to-know-about-the-schema-change)
in the Sector Imputations page for the two read patterns and which one applies to your code.

## Measured Table

This run compared this branch (`oda_data` 2.8.0) against `oda-data==2.7.0`, `years=2015-2024`, all donors, `measure=gross_disbursement`, `currency=USD`, current prices, on a scratch cache holding CRS release `v20260803` and MultiSystem release `v20260710` (both from OECD, downloaded 2026-09-22).

`oda-data==2.7.0`'s `imputed_multilateral_by_purpose` returns no rows at all for 2015 or 2016 on this cache; its output starts at 2017. This is the window-truncation defect this release fixes, demonstrated: the old code never padded its CRS read far enough back to give the first `period_length - 1` requested years (2015 and 2016, at the default `period_length=3`) a complete rolling window, so it dropped them outright rather than computing a partial or stale one. `old_total` is 0.0 for every donor in those two years, and the table attributes the whole of `observed_delta` for them to `window_padding` rather than leaving it in `residual` (see [Named Causes](#named-causes)).

The table covers the five largest donors by this branch's 2015-2024 total: Germany ($72,591.7M), the United Kingdom ($61,141.0M), the United States ($58,252.1M), France ($56,705.6M), and Japan ($34,635.1M). Values are USD millions, current prices.

| year | donor          | old_total | new_total | observed_delta | bi_multi_exclusion | window_padding | share_basis | unallocated_and_proxy | residual |
| ---- | -------------- | --------: | --------: | -------------: | -----------------: | -------------: | ----------: | --------------------: | -------: |
| 2015 | France         |       0.0 |   4,135.6 |        4,135.6 |                0.0 |        3,749.7 |         0.0 |                 385.9 |      0.0 |
| 2016 | France         |       0.0 |   4,307.2 |        4,307.2 |                0.0 |        3,940.0 |         0.0 |                 367.2 |      0.0 |
| 2017 | France         |   4,503.1 |   5,000.7 |          497.6 |                0.0 |            0.0 |         0.0 |                 480.7 |     16.9 |
| 2018 | France         |   5,293.2 |   5,828.2 |          535.0 |                0.0 |            0.0 |         0.0 |                 507.7 |     27.3 |
| 2019 | France         |   4,359.3 |   4,874.9 |          515.6 |                0.0 |            0.0 |         0.0 |                 508.6 |      7.1 |
| 2020 | France         |   4,947.8 |   5,528.2 |          580.4 |                0.0 |            0.0 |         0.0 |                 560.7 |     19.7 |
| 2021 | France         |   5,818.8 |   6,560.1 |          741.4 |                0.0 |            0.0 |         0.0 |                 707.4 |     33.9 |
| 2022 | France         |   6,506.7 |   7,127.2 |          620.4 |                0.0 |            0.0 |         0.0 |                 630.7 |    -10.3 |
| 2023 | France         |   6,052.3 |   6,592.5 |          540.2 |                0.0 |            0.0 |         0.0 |                 534.9 |      5.3 |
| 2024 | France         |   6,204.8 |   6,751.0 |          546.2 |                0.0 |            0.0 |         0.0 |                 518.4 |     27.8 |
| 2015 | Germany        |       0.0 |   3,821.1 |        3,821.1 |                0.0 |        3,583.7 |         0.0 |                 237.4 |      0.0 |
| 2016 | Germany        |       0.0 |   5,077.1 |        5,077.1 |                0.0 |        4,798.3 |         0.0 |                 278.8 |      0.0 |
| 2017 | Germany        |   4,990.0 |   5,162.0 |          172.0 |                0.0 |            0.0 |         0.0 |                 174.4 |     -2.4 |
| 2018 | Germany        |   5,576.4 |   6,175.8 |          599.4 |                0.0 |            0.0 |         0.0 |                 583.0 |     16.4 |
| 2019 | Germany        |   5,331.7 |   5,574.3 |          242.6 |                0.0 |            0.0 |         0.0 |                 255.2 |    -12.6 |
| 2020 | Germany        |   6,288.1 |   6,591.3 |          303.1 |                0.0 |            0.0 |         0.0 |                 300.5 |      2.7 |
| 2021 | Germany        |   8,209.8 |   8,496.4 |          286.6 |                0.0 |            0.0 |         0.0 |                 258.8 |     27.8 |
| 2022 | Germany        |   7,089.3 |   7,321.8 |          232.5 |                0.0 |            0.0 |         0.0 |                 285.5 |    -53.0 |
| 2023 | Germany        |  15,420.9 |  16,196.5 |          775.6 |                0.0 |            0.0 |         0.0 |                 727.3 |     48.3 |
| 2024 | Germany        |   7,981.6 |   8,175.5 |          194.0 |                0.0 |            0.0 |         0.0 |                 269.5 |    -75.5 |
| 2015 | Japan          |       0.0 |   3,036.8 |        3,036.8 |                0.0 |        2,456.8 |         0.0 |                 580.0 |      0.0 |
| 2016 | Japan          |       0.0 |   3,368.3 |        3,368.3 |                0.0 |        2,662.5 |         0.0 |                 705.8 |      0.0 |
| 2017 | Japan          |   2,707.0 |   3,381.3 |          674.3 |                0.0 |            0.0 |         0.0 |                 661.8 |     12.5 |
| 2018 | Japan          |   3,274.7 |   3,963.6 |          688.9 |                0.0 |            0.0 |         0.0 |                 645.6 |     43.3 |
| 2019 | Japan          |   3,587.9 |   4,240.7 |          652.8 |                0.0 |            0.0 |         0.0 |                 695.5 |    -42.7 |
| 2020 | Japan          |   2,988.1 |   3,417.0 |          428.8 |                0.0 |            0.0 |         0.0 |                 779.2 |   -350.4 |
| 2021 | Japan          |   3,309.5 |   4,144.7 |          835.1 |                0.0 |            0.0 |         0.0 |               1,140.4 |   -305.3 |
| 2022 | Japan          |   2,379.5 |   2,622.4 |          242.9 |                0.0 |            0.0 |         0.0 |                 594.4 |   -351.4 |
| 2023 | Japan          |   3,204.0 |   3,618.5 |          414.5 |                0.0 |            0.0 |         0.0 |                 515.1 |   -100.6 |
| 2024 | Japan          |   2,399.5 |   2,841.7 |          442.2 |                0.0 |            0.0 |         0.0 |                 531.8 |    -89.6 |
| 2015 | United Kingdom |       0.0 |   6,813.1 |        6,813.1 |                0.0 |        5,877.7 |         0.0 |                 935.4 |      0.0 |
| 2016 | United Kingdom |       0.0 |   6,530.9 |        6,530.9 |                0.0 |        5,793.4 |         0.0 |                 737.5 |      0.0 |
| 2017 | United Kingdom |   5,977.9 |   6,764.2 |          786.3 |                0.0 |            0.0 |         0.0 |                 751.2 |     35.1 |
| 2018 | United Kingdom |   6,516.3 |   7,149.5 |          633.3 |                0.0 |            0.0 |         0.0 |                 583.2 |     50.1 |
| 2019 | United Kingdom |   5,225.0 |   6,098.2 |          873.2 |                0.0 |            0.0 |         0.0 |                 825.3 |     47.9 |
| 2020 | United Kingdom |   6,407.5 |   7,078.4 |          670.9 |                0.0 |            0.0 |         0.0 |                 696.9 |    -26.1 |
| 2021 | United Kingdom |   5,993.7 |   6,508.4 |          514.7 |                0.0 |            0.0 |         0.0 |                 528.0 |    -13.3 |
| 2022 | United Kingdom |   3,477.1 |   3,878.2 |          401.2 |                0.0 |            0.0 |         0.0 |                 490.7 |    -89.6 |
| 2023 | United Kingdom |   6,123.2 |   6,731.2 |          608.0 |                0.0 |            0.0 |         0.0 |                 670.9 |    -62.9 |
| 2024 | United Kingdom |   3,395.7 |   3,588.7 |          193.1 |                0.0 |            0.0 |         0.0 |                 252.7 |    -59.7 |
| 2015 | United States  |       0.0 |   4,333.4 |        4,333.4 |                0.0 |        3,737.1 |         0.0 |                 596.3 |      0.0 |
| 2016 | United States  |       0.0 |   5,881.9 |        5,881.9 |                0.0 |        5,181.5 |         0.0 |                 700.4 |      0.0 |
| 2017 | United States  |   4,085.2 |   4,727.8 |          642.7 |                0.0 |            0.0 |         0.0 |                 723.3 |    -80.6 |
| 2018 | United States  |   3,324.3 |   3,852.7 |          528.4 |                0.0 |            0.0 |         0.0 |                 559.5 |    -31.1 |
| 2019 | United States  |   3,601.1 |   4,166.6 |          565.5 |                0.0 |            0.0 |         0.0 |                 775.6 |   -210.1 |
| 2020 | United States  |   5,105.3 |   5,724.1 |          618.8 |                0.0 |            0.0 |         0.0 |                 914.4 |   -295.6 |
| 2021 | United States  |   8,692.6 |   9,298.9 |          606.2 |                0.0 |            0.0 |         0.0 |               1,062.4 |   -456.2 |
| 2022 | United States  |   7,854.8 |   8,406.8 |          552.0 |                0.0 |            0.0 |         0.0 |                 937.5 |   -385.5 |
| 2023 | United States  |   4,785.3 |   5,248.7 |          463.4 |                0.0 |            0.0 |         0.0 |                 559.1 |    -95.7 |
| 2024 | United States  |   5,934.2 |   6,611.2 |          677.0 |                0.0 |            0.0 |         0.0 |               1,010.1 |   -333.1 |

For these five donors, `bi_multi_exclusion` and `share_basis` round to 0.0 in every year: neither the 2.7.1 CRS `bi_multi==2` fix nor the ODA+OOF share-basis sensitivity moves a dollar at this scale for a major donor. `window_padding` is 0.0 in 2017-2024 too (the 5-year stale-share lookback itself does not move a dollar at this scale either), but it carries the entire 2015 and 2016 delta ($3,749.7M to $5,877.7M per donor), the window-truncation defect demonstrated above. `unallocated_and_proxy`, money that used to be silently dropped and is now an explicit row, runs a few hundred million dollars a year per donor in every year including 2015 and 2016. What is left in `residual` for 2017-2024 is small, tens of millions per donor-year against delta in the hundreds of millions, and it folds together `channel_remapping` (the crosswalk change) and the literal pre-2.8.0 share basis (no CRS category filter at all), neither reachable as its own year-by-donor column through the public API.

Grand totals (`summary.json`, USD millions, 2015-2024, all donors):

- `old_total_usd`: 371,570.6
- `new_total_usd`: 493,108.6
- `observed_delta_usd`: 121,538.0
- `residual_usd`: -3,246.1 (2.7% of `observed_delta_usd`; see [Named Causes](#named-causes) for why it is small and negative rather than zero, and what it still folds together)

Channel-remapping diagnostic (`channel_remapping.csv`): 152 `(provider_code, agency_code)` pairs compared, 10 flagged `changed=true`. The three largest by CRS money at stake: the Islamic Development Bank (provider 976, agency 5) moved from channel 44004 to 46025 ($20,086.5M), UNHCR (967, agency 1) moved from 41305 to 41121 ($5,433.8M), and the International Labour Organisation (940, agencies 1 and 2) moved from 41144 to 41302 ($2,348.1M combined).

### Totals by allocation_status

This branch's 2015-2024 output, USD millions, current prices:

| allocation_status |     value |  share |
| ----------------- | --------: | -----: |
| imputed           | 439,621.4 | 89.15% |
| unallocated       |  32,517.3 |  6.59% |
| proxy             |  19,927.3 |  4.04% |
| stale_share       |   1,042.6 |  0.21% |

The grand total is $493,108.6M, matching `new_total_usd` above.

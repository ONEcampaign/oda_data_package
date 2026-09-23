# Imputation delta

`scripts/imputation_delta.py` compares `imputed_multilateral_by_purpose` totals between an earlier `oda-data` release and the installed code, by year and donor, and splits the difference into named causes.

```bash
uv run python scripts/imputation_delta.py \
    --cache-dir /path/outside/repo/oda-data-cache \
    --output-dir /path/outside/repo/imputation-delta-out \
    --years 2015-2024
```

The earlier release (`--old-version`, default `2.7.0`) runs in an isolated environment through `uv run --isolated`, against the same cache as the current code. Both directories must be outside the repository. `--providers`, `--measure`, `--currency` and `--base-year` set the comparison, `--refresh` re-downloads the current code's inputs, `--skip-channel-remapping` skips the channel diagnostic, and `--dry-run` prints the plan without reading data.

## Output

`imputation_delta.csv` has one row per year and donor:

| Column                  | Meaning                                                                                                                                                                     |
| ----------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `old_total`             | The earlier release's total.                                                                                                                                                |
| `new_total`             | The current total.                                                                                                                                                          |
| `observed_delta`        | `new_total - old_total`.                                                                                                                                                    |
| `bi_multi_exclusion`    | Current code with `bi_multi == 2` rows excluded from the shares, minus current code with them kept.                                                                         |
| `share_basis`           | Current code with `flow_types=("ODA", "OOF")` minus current code with `("ODA",)`.                                                                                           |
| `window_padding`        | Current code with `max_share_age=5` minus current code with `0`, plus, in years the earlier release left empty, the rest of `observed_delta` after `unallocated_and_proxy`. |
| `unallocated_and_proxy` | The value of current rows with `allocation_status` `unallocated` or `proxy`.                                                                                                |
| `residual`              | `observed_delta` minus the four columns above.                                                                                                                              |

`channel_remapping.csv` lists each `(provider_code, agency_code)` pair with its channel under both versions and the CRS money at stake. `summary.json` holds the grand totals.

## Reading the table

Current output conserves core contributions. The rows for each year, donor and channel sum to the donor's MultiSystem core contribution, so a donor's total depends on its core contributions alone. The CRS shares only split that total across purposes and recipients. `bi_multi_exclusion`, `share_basis` and the `max_share_age` part of `window_padding` compare two current runs over the same contributions, so they are zero. Their effect falls on the purpose split, which this table does not show.

Totals move for three reasons. Releases before 2.8.0 return no rows for the first `period_length - 1` years of a multi-year request, and the script puts that gap in `window_padding`. Earlier releases also dropped core contributions to channels without a spending share, which the current code reports as `proxy` or `unallocated`. Differences in channel assignment and share basis make up the rest and land in `residual`, along with `stale_share` money, which earlier releases also dropped. A negative `residual` means the current code reports money as `unallocated` or `proxy` that the earlier release allocated.

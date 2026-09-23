"""QA report for `imputed_multilateral_by_purpose` output.

`imputed_multilateral_by_purpose` already enforces money conservation at
build time (`ImputationConservationError`). `imputation_quality_report`
looks at an already-built result and surfaces the things conservation alone
does not catch: how much money sits in each `allocation_status`, whether a
channel's shares are stale, whether a channel's purpose mix is suspiciously
concentrated in a single code (the ITFC/MFA pattern), duplicate rows,
negative values, and rows with no `channel_code` at all.
"""

import pandas as pd

from oda_data.clean_data.schema import ODASchema

# A channel-window whose money concentrates this much or more in a single
# purpose code is flagged (the ITFC/MFA pattern): a peer or parent-fund
# proxy, or a channel with an unusually narrow CRS footprint, can otherwise
# quietly dominate a whole sector.
DEFAULT_CONCENTRATION_THRESHOLD: float = 0.5

# Columns that identify a single allocation row; a repeat is a duplicate.
_ROW_KEY: list[str] = [
    ODASchema.YEAR,
    ODASchema.PROVIDER_CODE,
    ODASchema.CHANNEL_CODE,
    ODASchema.RECIPIENT_CODE,
    ODASchema.PURPOSE_CODE,
]


def _by_channel_year(result: pd.DataFrame) -> pd.DataFrame:
    """One row per (channel_code, year): total core money, allocation
    status, share staleness and row count.

    `staleness_years` is `year - share_years`'s end year (0 for `imputed`,
    positive for `stale_share`/a stale `proxy`, null for `unallocated` and
    `fixed_purpose` proxy rows, which carry no `share_years`).
    """
    grouped = result.groupby(
        [ODASchema.CHANNEL_CODE, ODASchema.YEAR], dropna=False, observed=True
    )

    summary = grouped.agg(
        core_value=(ODASchema.VALUE, "sum"),
        allocation_statuses=("allocation_status", lambda s: sorted(s.unique())),
        share_years=("share_years", "first"),
        n_rows=(ODASchema.VALUE, "size"),
    ).reset_index()

    share_end_year = pd.to_numeric(
        summary["share_years"].str.split("-").str[-1], errors="coerce"
    )
    summary["staleness_years"] = summary[ODASchema.YEAR] - share_end_year

    return summary


def _totals_by_status(result: pd.DataFrame) -> pd.DataFrame:
    """Total `value` and share of the grand total, per `allocation_status`."""
    totals = (
        result.groupby("allocation_status", observed=True)[ODASchema.VALUE]
        .sum()
        .reset_index()
        .rename(columns={ODASchema.VALUE: "value"})
    )
    grand_total = totals["value"].sum()
    totals["share_of_total"] = totals["value"] / grand_total if grand_total else pd.NA

    return totals.sort_values("value", ascending=False).reset_index(drop=True)


def _duplicates(result: pd.DataFrame) -> pd.DataFrame:
    """Rows that repeat the same (year, donor_code, channel_code,
    recipient_code, purpose_code) key -- a join or dtype bug, not a
    legitimate output shape."""
    mask = result.duplicated(subset=_ROW_KEY, keep=False)
    return result.loc[mask].sort_values(_ROW_KEY).reset_index(drop=True)


def _negatives(result: pd.DataFrame) -> pd.DataFrame:
    """Rows with a negative `value` (e.g. a CRS reversal flowing through a
    share)."""
    return result.loc[result[ODASchema.VALUE] < 0].reset_index(drop=True)


def _crosswalk_misses(result: pd.DataFrame) -> pd.DataFrame:
    """Rows with a null `channel_code`.

    `core_multilateral_contributions_by_provider` reads `channel_code`
    directly from Multisystem, not via the CRS crosswalk join, so this is
    normally empty; it only surfaces if a caller has concatenated in rows
    from a crosswalk join run with `on_unmapped="unallocated"`.
    """
    return result.loc[result[ODASchema.CHANNEL_CODE].isna()].reset_index(drop=True)


def _single_purpose_concentration(
    result: pd.DataFrame, threshold: float
) -> pd.DataFrame:
    """Per (share_channel_code, share_years) window, the largest single
    purpose's share of that window's total money.

    Computed from `share_channel_code`/`share_years` -- the shares *source*
    window -- rather than from the core-contribution `channel_code`, since a
    proxy channel's concentration is a property of the shares it lends, not
    of who borrows them. Rows with no share source (`unallocated`, and
    `fixed_purpose` proxy rows, which are 100% concentrated by construction
    and not informative to flag) are excluded.

    Args:
        result: `imputed_multilateral_by_purpose` output.
        threshold: Flag windows at or above this share. Defaults to
            `DEFAULT_CONCENTRATION_THRESHOLD`.

    Returns:
        pd.DataFrame: `share_channel_code, share_years, top_purpose_code,
        top_purpose_share, flagged`, one row per window, sorted by
        `top_purpose_share` descending.
    """
    shared = result.loc[result["share_channel_code"].notna()]
    if shared.empty:
        return pd.DataFrame(
            columns=[
                "share_channel_code",
                "share_years",
                "top_purpose_code",
                "top_purpose_share",
                "flagged",
            ]
        )

    window = ["share_channel_code", "share_years"]
    by_purpose = (
        shared.groupby([*window, ODASchema.PURPOSE_CODE], observed=True)[
            ODASchema.VALUE
        ]
        .sum()
        .reset_index()
    )
    window_totals = shared.groupby(window, observed=True)[ODASchema.VALUE].sum()

    top = (
        by_purpose.sort_values(ODASchema.VALUE, ascending=False)
        .drop_duplicates(subset=window)
        .rename(
            columns={
                ODASchema.PURPOSE_CODE: "top_purpose_code",
                ODASchema.VALUE: "top_purpose_value",
            }
        )
    )
    top["top_purpose_share"] = top["top_purpose_value"] / top.set_index(
        window
    ).index.map(window_totals)
    top["flagged"] = top["top_purpose_share"] >= threshold

    return (
        top.drop(columns="top_purpose_value")
        .sort_values("top_purpose_share", ascending=False)
        .reset_index(drop=True)
    )


def imputation_quality_report(
    result: pd.DataFrame,
    concentration_threshold: float = DEFAULT_CONCENTRATION_THRESHOLD,
) -> dict[str, pd.DataFrame | dict | None]:
    """Build a QA report over an `imputed_multilateral_by_purpose` result.

    Args:
        result: Output of `imputed_multilateral_by_purpose`.
        concentration_threshold: Threshold for `single_purpose_concentration`'s
            `flagged` column. Defaults to `DEFAULT_CONCENTRATION_THRESHOLD`.

    Returns:
        dict with:

        - `by_channel_year`: one row per (channel_code, year): core money,
          allocation statuses present, share_years, staleness_years, n_rows.
        - `totals_by_status`: money and share of total by allocation_status.
        - `duplicates`: rows repeating the same (year, donor_code,
          channel_code, recipient_code, purpose_code) key.
        - `negatives`: rows with value < 0.
        - `crosswalk_misses`: rows with a null channel_code.
        - `single_purpose_concentration`: per shares-source window, the
          largest single purpose's share of that window's money, flagged at
          `concentration_threshold` (the ITFC/MFA pattern).
        - `provenance`: `result.attrs.get("provenance")` -- None if `result`
          has passed through a pandas operation that dropped `.attrs`.
    """
    return {
        "by_channel_year": _by_channel_year(result),
        "totals_by_status": _totals_by_status(result),
        "duplicates": _duplicates(result),
        "negatives": _negatives(result),
        "crosswalk_misses": _crosswalk_misses(result),
        "single_purpose_concentration": _single_purpose_concentration(
            result, concentration_threshold
        ),
        "provenance": result.attrs.get("provenance"),
    }

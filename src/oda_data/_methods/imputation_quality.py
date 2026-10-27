"""Checks on the rows of ``one.crs.imputed_multilateral_oda``.

The imputation raises ``ImputationConservationError`` when it loses or adds
money. ``quality_report`` reads the rows ``oda.records`` returns for the
indicator and reports what that check leaves open: how much money each
allocation status holds, how stale each channel's shares are, rows that repeat
a key, negative rows, rows with no channel, and share windows whose money sits
mostly in one purpose code, the pattern of a narrow fund or proxy that can
dominate a sector.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

#: A share window whose largest purpose code holds this share of its money or
#: more is flagged.
CONCENTRATION_THRESHOLD = 0.5

#: The columns that identify one row of the imputation.
ROW_KEY = ["year", "provider", "channel", "recipient", "sector"]


def quality_report(
    rows: pd.DataFrame, *, concentration_threshold: float = CONCENTRATION_THRESHOLD
) -> dict[str, pd.DataFrame]:
    """Summarise the rows of an imputation.

    Args:
        rows: ``oda.records("one.crs.imputed_multilateral_oda", ...)`` in one
            unit, with ``year``, ``provider``, ``channel``, ``recipient``,
            ``sector``, ``allocation_status``, ``share_channel``,
            ``share_years`` and ``value``.
        concentration_threshold: The share of a window's money in one purpose
            code at which ``concentration`` flags the window.

    Returns:
        ``by_channel_year``: per channel and year, the money, the statuses
        present, the share window, ``staleness_years`` (the year minus the
        window's last year; null without a window) and the row count.
        ``totals_by_status``: money and share of the total per status.
        ``duplicates``: rows that repeat a key in ``ROW_KEY``.
        ``negatives``: rows below zero.
        ``missing_channels``: rows with no channel.
        ``concentration``: per share window (``share_channel`` and
        ``share_years``), its largest purpose code, that code's share, and
        ``flagged``.
    """
    return {
        "by_channel_year": _by_channel_year(rows),
        "totals_by_status": _totals_by_status(rows),
        "duplicates": rows.loc[rows.duplicated(ROW_KEY, keep=False)]
        .sort_values(ROW_KEY)
        .reset_index(drop=True),
        "negatives": rows.loc[rows["value"] < 0].reset_index(drop=True),
        "missing_channels": rows.loc[rows["channel"].isna()].reset_index(drop=True),
        "concentration": _concentration(rows, concentration_threshold),
    }


def _by_channel_year(rows: pd.DataFrame) -> pd.DataFrame:
    summary = (
        rows.groupby(["channel", "year"], dropna=False, observed=True)
        .agg(
            value=("value", "sum"),
            allocation_statuses=("allocation_status", lambda s: sorted(s.unique())),
            share_years=("share_years", "first"),
            rows=("value", "size"),
        )
        .reset_index()
    )
    last = summary["share_years"].str.split("-").str[-1].astype("Int64")
    summary["staleness_years"] = summary["year"] - last
    return summary


def _totals_by_status(rows: pd.DataFrame) -> pd.DataFrame:
    totals = rows.groupby("allocation_status", observed=True)["value"].sum()
    totals = totals.reset_index().sort_values("value", ascending=False)
    grand = totals["value"].sum()
    totals["share_of_total"] = totals["value"] / grand if grand else float("nan")
    return totals.reset_index(drop=True)


def _concentration(rows: pd.DataFrame, threshold: float) -> pd.DataFrame:
    """The largest purpose code's share of each share window's money.

    A window belongs to the channel whose shares spread the money, so a proxy's
    concentration is its parent fund's. Rows with no window (unallocated, and
    fixed-purpose proxies, which hold one code by construction) are left out.
    """
    window = ["share_channel", "share_years"]
    shared = rows.loc[rows["share_channel"].notna()]
    by_sector = (
        shared.groupby([*window, "sector"], dropna=False, observed=True)["value"]
        .sum()
        .reset_index()
    )
    totals = by_sector.groupby(window, observed=True)["value"].transform("sum")
    by_sector["top_share"] = by_sector["value"] / totals
    top = (
        by_sector.sort_values("value", ascending=False)
        .drop_duplicates(window)
        .rename(columns={"sector": "top_sector"})
        .drop(columns="value")
    )
    top["flagged"] = top["top_share"] >= threshold
    return top.sort_values("top_share", ascending=False).reset_index(drop=True)

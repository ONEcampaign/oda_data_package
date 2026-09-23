"""Multilateral spending shares: what each multilateral channel spends its
CRS-reported money on, by purpose, smoothed over a rolling window.

`multilateral_spending_shares_by_channel_and_purpose_smoothed` is the entry
point the allocation step (`sector_imputations.imputed_multilateral_by_purpose`)
consumes: for each multilateral channel and year, the share of that channel's
CRS spending going to each (purpose_code, recipient_code) pair, computed over
a `period_length`-year rolling window ending at that year.

A channel with no CRS rows in the window ending at the requested year (e.g.
a lapsed reporter, or a Multisystem release a year ahead of the CRS release)
falls back to its most recent window within `max_share_age` years
(`allocation_status="stale_share"`); a channel with no qualifying window at
all is left out of this module's output for that year, for the allocation
step to resolve via a proxy or leave `unallocated`.
"""

import warnings

import numpy as np
import pandas as pd

from oda_data.api.constants import MEASURES, PROVIDER_PURPOSE_GROUPER, Measure
from oda_data.clean_data.channels import add_multilateral_channel_codes
from oda_data.clean_data.common import convert_units
from oda_data.clean_data.schema import ODASchema
from oda_data.tools.groupings import provider_groupings

# CRS `category` codes for each flow type name accepted by `flow_types`.
# ODA (10) and OOF (21) together reproduce the discontinued OECD sectoral
# imputed-multilateral-aid methodology (both flow types, three-year rolling
# window of gross spending). PSI (60) is ODA-countable for the donor
# reporting it, but is its own CRS category, not folded into 10.
FLOW_TYPE_CATEGORIES: dict[str, int] = {"ODA": 10, "OOF": 21, "PSI": 60}

# Numerator: one row per channel x purpose x recipient x year.
SHARE_NUMERATOR_GROUPER: list[str] = [
    ODASchema.CHANNEL_CODE,
    ODASchema.PURPOSE_CODE,
    ODASchema.RECIPIENT_CODE,
    ODASchema.CURRENCY,
    ODASchema.PRICES,
]

# Denominator: the channel's total spending a year's purpose shares are taken
# out of.
SHARE_DENOMINATOR_GROUPER: list[str] = [
    ODASchema.CHANNEL_CODE,
    ODASchema.CURRENCY,
    ODASchema.PRICES,
]


def _resolve_flow_types(
    flow_types: tuple[str, ...],
    oda_only: bool | None,
    *,
    stacklevel: int = 3,
) -> tuple[str, ...]:
    """Resolve the flow-type filter, honouring the deprecated `oda_only`.

    `oda_only` takes precedence when given (not None), and raises a
    `DeprecationWarning` naming what the old True/False branches actually
    did, since neither matches its replacement `flow_types` value exactly.
    """
    if oda_only is None:
        unknown = sorted(set(flow_types) - set(FLOW_TYPE_CATEGORIES))
        if unknown:
            raise ValueError(
                f"Unknown flow_types: {unknown}. Valid values are "
                f"{sorted(FLOW_TYPE_CATEGORIES)}."
            )
        return flow_types

    warnings.warn(
        "oda_only/shares_based_on_oda_only is deprecated and will be removed "
        "in a future release; use flow_types instead. The old oda_only=True "
        "filtered CRS category in (10, 60) -- ODA and Private Sector "
        "Instruments (PSI); oda_only=False applied no category filter at "
        "all, mixing OOF, export credits and other flow categories in "
        "alongside ODA. For compatibility this call maps oda_only=True to "
        "flow_types=('ODA',) and oda_only=False to flow_types=('ODA', "
        "'OOF'), which differs from both old branches. Pass flow_types "
        "directly to control the filter explicitly.",
        DeprecationWarning,
        stacklevel=stacklevel,
    )
    return ("ODA",) if oda_only else ("ODA", "OOF")


def _normalise_years(years: list | int | range | None) -> list[int] | None:
    """Coerce `years` to a sorted list of ints, or None."""
    if years is None:
        return None
    if isinstance(years, int):
        return [years]
    return sorted(int(y) for y in years)


def pad_years_for_window(
    years: list | int | range | None,
    period_length: int = 3,
    max_share_age: int = 5,
) -> tuple[list[int] | None, list[int] | None]:
    """Compute the years to read from CRS so every requested year has a full
    rolling window, plus enough history for a stale-share fallback.

    Reads must extend `period_length - 1 + max_share_age` years before the
    earliest requested year: `period_length - 1` so that year's own window
    is complete, and `max_share_age` more so a lapsed channel's most recent
    window is still in scope. Without this padding, a single requested year
    has no history to build a window from at all, and the earliest
    `period_length - 1` years of a multi-year request never get a complete
    window, since CRS reads that mirror exactly the requested years leave no
    room to look backward.

    Args:
        years: Years requested by the caller (the years the output should
            cover), or None for "all available years".
        period_length: Rolling window length in years.
        max_share_age: Additional years of lookback for a stale-share
            fallback.

    Returns:
        `(read_years, requested_years)`: `read_years` is the (wider) year
        list/range to pass to the CRS reader; `requested_years` is the
        original, sorted request, to filter the final output back down to.
        Both are None when `years` is None (unrestricted read; the caller
        derives the requested years from the data it gets back).
    """
    requested = _normalise_years(years)
    if requested is None:
        return None, None
    if not requested:
        return requested, requested

    pad = period_length - 1 + max_share_age
    read_years = list(range(requested[0] - pad, requested[-1] + 1))

    return read_years, requested


def rolling_window_total(
    df: pd.DataFrame,
    period_length: int = 3,
    grouper: list[str] | None = None,
    value_cols: list[str] | None = None,
) -> pd.DataFrame:
    """Vectorised rolling total over `period_length` years, per `grouper`.

    Replaces a year-by-year deep-copy/concat loop with a single pivot and
    rolling-sum pass. A (grouper, year) combination is only emitted once its
    group has a full `period_length`-year window within the years present in
    `df` (a year absent from `df` within that window counts as zero); years
    without a full window are dropped. A caller that needs a window for
    every requested year -- including the earliest -- must read enough
    history first (`pad_years_for_window`); this function has no way to grow
    the input, only to summarise it.

    Args:
        df: Long-format data with a `year` column, `value_cols` and
            `grouper`.
        period_length: Number of years summed into each rolling window.
        grouper: Columns identifying a series. Defaults to every column
            other than `year` and `value_cols`.
        value_cols: Columns to sum. Defaults to `["value"]`.

    Returns:
        pd.DataFrame: `grouper + [year] + value_cols`, one row per
        (grouper, year) with a complete `period_length`-year window, `year`
        being the window's last (most recent) year.
    """
    if value_cols is None:
        value_cols = [ODASchema.VALUE]
    if grouper is None:
        grouper = [c for c in df.columns if c not in [ODASchema.YEAR, *value_cols]]

    empty = df.loc[:, [*grouper, ODASchema.YEAR, *value_cols]].head(0).copy()
    if df.empty:
        return empty

    min_year = int(df[ODASchema.YEAR].min())
    max_year = int(df[ODASchema.YEAR].max())
    if max_year - min_year + 1 < period_length:
        return empty

    full_years = list(range(min_year, max_year + 1))

    summed = df.groupby([*grouper, ODASchema.YEAR], observed=True, dropna=False)[
        value_cols
    ].sum()
    wide = summed.unstack(ODASchema.YEAR)

    # Roll one value column at a time: rolling over the whole wide frame at
    # once slides across the (value_col, year) column boundary and bleeds
    # one value column's trailing years into the next one's leading years.
    rolled_parts = []
    for col in value_cols:
        # A (grouper, year) combination absent from `df` is zero, whether it
        # is missing from `wide` entirely (a year outside its group's own
        # range -- filled in by reindex) or present as an unstacked gap (a
        # year between two others where this group had no rows -- reindex
        # alone leaves those NaN, since the column already exists).
        sub = wide[col].reindex(columns=full_years).fillna(0)
        rolled_sub = (
            sub.T.rolling(window=period_length, min_periods=period_length).sum().T
        )
        rolled_parts.append(pd.concat({col: rolled_sub}, axis=1))
    rolled = pd.concat(rolled_parts, axis=1)
    rolled.columns.names = [None, ODASchema.YEAR]

    result = rolled.stack(level=1, future_stack=True).dropna(how="all").reset_index()
    result[ODASchema.YEAR] = result[ODASchema.YEAR].astype("int16[pyarrow]")

    return result.reset_index(drop=True)


def resolve_channel_windows(
    windows: pd.DataFrame,
    requested_years: list[int],
    max_share_age: int,
    grouper: list[str],
) -> pd.DataFrame:
    """For each `grouper` key and requested year, pick the window to use.

    Implements the imputed/stale_share precedence: a window whose total
    (`value`) is positive in the year range ending at the requested year is
    used directly (`allocation_status="imputed"`); otherwise the most recent
    earlier window with a positive total, within `max_share_age` years, is
    used instead (`allocation_status="stale_share"`). A window whose total is
    zero or negative counts as having no share, not a divide-by-zero: it is
    never chosen as a source, whether for its own year or as a fallback for a
    later one. A `grouper` key with no qualifying window for a given
    requested year is left out of the result for that year.

    Args:
        windows: Output of `rolling_window_total` for the denominator series
            (one row per `grouper` key and window-end year).
        requested_years: Years the caller wants resolved.
        max_share_age: Maximum number of years a stale window may lag behind
            the requested year.
        grouper: The columns identifying a series in `windows`.

    Returns:
        pd.DataFrame: `grouper + [year, source_year, denominator,
        allocation_status]`, `year` being the requested year and
        `source_year` the window-end year actually used.
    """
    positive = windows.loc[windows[ODASchema.VALUE] > 0].rename(
        columns={ODASchema.YEAR: "source_year", ODASchema.VALUE: "denominator"}
    )

    columns = [
        *grouper,
        ODASchema.YEAR,
        "source_year",
        "denominator",
        "allocation_status",
    ]
    if positive.empty or not requested_years:
        return pd.DataFrame(columns=columns)

    # merge_asof requires the "on" columns to share a dtype; rolling_window_total
    # hands back a pyarrow-backed year, requested years are plain Python ints.
    positive = positive.astype({"source_year": "int64"})

    keys = positive[grouper].drop_duplicates()
    requested = pd.DataFrame({ODASchema.YEAR: sorted(set(requested_years))}).merge(
        keys, how="cross"
    )

    merged = pd.merge_asof(
        requested.sort_values(ODASchema.YEAR),
        positive.sort_values("source_year"),
        left_on=ODASchema.YEAR,
        right_on="source_year",
        by=grouper,
        direction="backward",
        tolerance=max_share_age,
    ).dropna(subset=["source_year"])

    merged["source_year"] = merged["source_year"].astype(int)
    merged["allocation_status"] = np.where(
        merged[ODASchema.YEAR] == merged["source_year"], "imputed", "stale_share"
    )

    return merged.reset_index(drop=True)[columns]


def rolling_period_total(
    df: pd.DataFrame, period_length: int = 3, grouper: list[str] | None = None
) -> pd.DataFrame:
    """Calculates a rolling total over a specified period length.

    Args:
        df (pd.DataFrame): Input dataframe containing time-series data.
        period_length (int, optional): Length of the rolling period. Defaults to 3.
        grouper (list[str] | None, optional): Columns to group by. Defaults to None.

    Returns:
        pd.DataFrame: Dataframe with rolling total calculations.
    """
    return rolling_window_total(df, period_length=period_length, grouper=grouper)


def share_by_purpose(
    df: pd.DataFrame, grouper: list[str] | None = None
) -> pd.DataFrame:
    """Calculates the share of the total for each purpose code.

    Args:
        df (pd.DataFrame): Input dataframe containing values to compute shares.
        grouper (list[str] | None, optional): Columns to group by. Defaults to None.

    Returns:
        pd.DataFrame: Dataframe with an additional share column.
    """
    df[ODASchema.SHARE] = df.groupby(grouper, observed=True, dropna=False)[
        ODASchema.VALUE
    ].transform(lambda p: p / p.sum())

    return df.loc[lambda d: d.share.notna()].reset_index(drop=True)


def _group_by_mapped_channel(df: pd.DataFrame) -> pd.DataFrame:
    """Groups data by mapped channels and sums values.

    Args:
        df (pd.DataFrame): Input dataframe.

    Returns:
        pd.DataFrame: Aggregated dataframe grouped by relevant columns.
    """
    df = (
        df.groupby(
            [
                c
                for c in df.columns
                if c
                not in [
                    ODASchema.PROVIDER_NAME,
                    ODASchema.PROVIDER_CODE,
                    ODASchema.AGENCY_CODE,
                    ODASchema.AGENCY_NAME,
                    "name",
                    ODASchema.VALUE,
                ]
            ],
            observed=True,
            dropna=False,
        )[[ODASchema.VALUE]]
        .sum()
        .reset_index()
    )

    return df


def period_purpose_shares(
    data: pd.DataFrame,
    period_length: int = 3,
    grouper: list[str] | None = None,
    share_by_grouper: list[str] | None = None,
) -> pd.DataFrame:
    """Computes period-based purpose shares.

    Args:
        data (pd.DataFrame): Input dataframe.
        period_length (int, optional): Length of the rolling period. Defaults to 3.
        grouper (list[str] | None, optional): Columns to group by for rolling total. Defaults to None.
        share_by_grouper (list[str] | None, optional): Columns to group by for share calculation. Defaults to None.

    Returns:
        pd.DataFrame: Dataframe with computed shares.
    """
    return data.pipe(
        rolling_period_total, period_length=period_length, grouper=grouper
    ).pipe(share_by_purpose, grouper=share_by_grouper)


def add_multi_channels_and_group(data: pd.DataFrame) -> pd.DataFrame:
    """Adds multilateral channel codes and groups the data accordingly.

    Args:
        data (pd.DataFrame): Input dataframe.

    Returns:
        pd.DataFrame: Transformed dataframe with grouped channels.
    """
    return data.pipe(add_multilateral_channel_codes).pipe(_group_by_mapped_channel)


def _as_list(values: list | int | range | None) -> list | None:
    """Coerce a scalar-or-iterable filter argument to a list, or None."""
    if values is None:
        return None
    if isinstance(values, int):
        return [values]
    return list(values)


def _filter_supplied_crs(
    crs: pd.DataFrame,
    *,
    years: list | int | range | None,
    providers: list | int | None,
    category_codes: list[int],
    exclude_multilateral_core: bool,
) -> pd.DataFrame:
    """Apply to a caller-supplied CRS frame the filters a `CRSData` read applies.

    Raises:
        ValueError: If `exclude_multilateral_core` is True and the frame has no
            `bi_multi` column, since core-contribution rows cannot then be told
            apart. Pass `exclude_multilateral_core=False` for a frame that is
            already filtered.
    """
    if exclude_multilateral_core and ODASchema.BI_MULTI not in crs.columns:
        raise ValueError(
            "The supplied crs frame has no 'bi_multi' column, so core "
            "contributions to multilaterals (bi_multi == 2) cannot be excluded. "
            "Include the column, or pass exclude_multilateral_core=False if the "
            "frame is already filtered."
        )

    mask = pd.Series(True, index=crs.index)
    if exclude_multilateral_core:
        bi_multi = crs[ODASchema.BI_MULTI]
        mask &= (bi_multi != 2) | bi_multi.isna()
    if category_codes:
        mask &= crs[ODASchema.CATEGORY].isin(category_codes)
    if (year_list := _as_list(years)) is not None:
        mask &= crs[ODASchema.YEAR].isin(year_list)
    if (provider_list := _as_list(providers)) is not None:
        mask &= crs[ODASchema.PROVIDER_CODE].isin(provider_list)

    return crs.loc[mask]


def spending_by_purpose(
    years: list | int | range | None = None,
    providers: list | int | None = None,
    measure: Measure | str = "gross_disbursement",
    oda_only: bool | None = None,
    currency: str = "USD",
    base_year: int | None = None,
    *,
    flow_types: tuple[str, ...] = ("ODA",),
    exclude_multilateral_core: bool = True,
    crs: pd.DataFrame | None = None,
    refresh: bool = False,
) -> pd.DataFrame:
    """Retrieves and processes spending data by purpose.

    By default, CRS rows that report a donor's core (unearmarked) contribution to a
    multilateral organisation (`bi_multi == 2`) are excluded. Those contributions are
    redistributed across purposes separately, by `imputed_multilateral_by_purpose`.
    A caller that leaves them in this bilateral total would double count them.

    Args:
        years (list | int | range, optional): Years to filter the data. Defaults to None.
        providers (list | int | None, optional): Providers to filter the data. Defaults to None.
        measure (Measure | str, optional): Measure type. Defaults to "gross_disbursement".
        oda_only (bool | None, optional): Deprecated; use `flow_types`. Defaults to None.
        flow_types (tuple[str, ...], optional): CRS flow types to include, any of
            "ODA" (category 10), "OOF" (21), "PSI" (60). Defaults to `("ODA",)`.
        currency (str, optional): Target currency. Defaults to "USD".
        base_year (int | None, optional): Base year for conversion. Defaults to None.
        exclude_multilateral_core (bool, optional): Whether to exclude CRS rows
            reporting a donor's core contribution to a multilateral organisation
            (`bi_multi == 2`). Defaults to True. Pass False for a total that
            includes those core-contribution rows.
        crs (pd.DataFrame | None, optional): Pre-fetched, row-level CRS data to use
            instead of reading it (for tests and pinned builds). Must carry
            `PROVIDER_PURPOSE_GROUPER` columns, the `measure` column, a
            `category` column unless `flow_types` is empty, and a `bi_multi`
            column unless `exclude_multilateral_core` is False. The same
            `years`, `providers`, flow-type and core-contribution filters a
            `CRSData` read applies are applied to it. Defaults to None (read
            from `CRSData`).
        refresh (bool, optional): If True, bypass the bulk cache and re-download
            (#162). Only has an effect when `crs` is None. Defaults to False.

    Returns:
        pd.DataFrame: Dataframe with spending by purpose.
    """
    resolved_flow_types = _resolve_flow_types(flow_types, oda_only, stacklevel=3)

    # Get the relevant measure
    measure_col = MEASURES["CRS"][measure]["column"]

    # Set up grouper
    grouper = [
        c
        for c in PROVIDER_PURPOSE_GROUPER
        if c not in [ODASchema.CURRENCY, ODASchema.PRICES]
    ]

    # Set up filters
    category_codes = [FLOW_TYPE_CATEGORIES[ft] for ft in resolved_flow_types]
    filters = [(ODASchema.CATEGORY, "in", category_codes)] if category_codes else []

    if crs is not None:
        raw = _filter_supplied_crs(
            crs,
            years=years,
            providers=providers,
            category_codes=category_codes,
            exclude_multilateral_core=exclude_multilateral_core,
        )
    else:
        from oda_data.api.sources import CRSData

        crs_source = CRSData(
            providers=providers,
            years=years,
            exclude_multilateral_core=exclude_multilateral_core,
        )
        raw = crs_source.read(
            columns=[*grouper, measure_col],
            additional_filters=filters,
            using_bulk_download=True,
            refresh=refresh,
        )

    # Group by provider and purpose
    data = (
        raw.groupby(grouper, dropna=False, observed=True)[[measure_col]]
        .sum()
        .reset_index()
        .rename(columns={measure_col: ODASchema.VALUE})
    )

    # Convert the data to the target currency and prices
    data = convert_units(data, currency=currency, base_year=base_year)

    return data


def multilateral_spending_shares_by_channel_and_purpose_smoothed(
    years: list | int | range | None = None,
    oda_only: bool | None = None,
    period_length: int = 3,
    *,
    flow_types: tuple[str, ...] = ("ODA", "OOF"),
    max_share_age: int = 5,
    crs: pd.DataFrame | None = None,
    refresh: bool = False,
) -> pd.DataFrame:
    """Computes multilateral spending shares by channel and purpose, smoothed
    over a rolling window, with a stale-share fallback for lapsed reporters.

    For each multilateral channel and requested year, this is the share of
    that channel's CRS spending (`flow_types`, gross by default) going to
    each (purpose_code, recipient_code) pair, over the `period_length` years
    ending at that year. A channel with no CRS rows in that window (e.g. it
    has not reported recently, or a Multisystem release runs a year ahead of
    the CRS release) falls back to its most recent window within
    `max_share_age` years (`allocation_status="stale_share"`; `share_years`
    names the years actually used). A channel with no qualifying window at
    all -- own or stale -- for a requested year has no rows in the output for
    that channel-year; the allocation step decides whether to fall back to a
    proxy channel or leave the money `unallocated`.

    A channel-window whose total spending is zero or negative counts as
    having no share, not a divide-by-zero: it is never used, either for its
    own year or as a stale fallback for a later one.

    Args:
        years (list | int | range, optional): Years to compute shares for. Defaults to None
            (every year derivable from the input).
        oda_only (bool | None, optional): Deprecated; use `flow_types`. Defaults to None.
        flow_types (tuple[str, ...], optional): CRS flow types the shares are based on.
            Defaults to `("ODA", "OOF")`, the discontinued OECD sectoral-imputation
            practice: channels that report only OOF to CRS (e.g. IBRD, EBRD, IFC,
            IDB Invest) would otherwise get an empty share pool under an ODA-only
            filter. Pass `("ODA",)` for the stricter, official-ODA-definition basis.
        period_length (int, optional): Rolling window length in years. Defaults to 3.
        max_share_age (int, optional): Maximum number of years a stale window may lag behind the requested year. Defaults to 5.
        crs (pd.DataFrame | None, optional): Pre-fetched, row-level CRS data to use instead of reading it (for tests and pinned builds). Filtered as `spending_by_purpose` filters a supplied frame, to multilateral providers and the padded years. Defaults to None.
        refresh (bool, optional): If True, bypass the bulk cache and re-download
            (#162). Only has an effect when `crs` is None. Defaults to False.

    Returns:
        pd.DataFrame: `year, channel_code, purpose_code, recipient_code,
        share, allocation_status, share_years`.
    """
    return _multilateral_spending_shares(
        years,
        flow_types=_resolve_flow_types(flow_types, oda_only, stacklevel=3),
        period_length=period_length,
        max_share_age=max_share_age,
        exclude_multilateral_core=True,
        crs=crs,
        refresh=refresh,
    )


def _multilateral_spending_shares(
    years: list | int | range | None,
    *,
    flow_types: tuple[str, ...],
    period_length: int,
    max_share_age: int,
    exclude_multilateral_core: bool,
    crs: pd.DataFrame | None,
    refresh: bool,
) -> pd.DataFrame:
    """Body of `multilateral_spending_shares_by_channel_and_purpose_smoothed`,
    with `flow_types` already resolved.

    `exclude_multilateral_core=False` keeps CRS rows reporting a core
    contribution to a multilateral organisation (`bi_multi == 2`) in the
    shares. It exists to measure the effect of excluding them
    (`scripts/imputation_delta.py`), and every public caller passes True.
    """
    multilateral_providers = list(provider_groupings()["multilateral"])

    read_years, requested_years = pad_years_for_window(
        years, period_length=period_length, max_share_age=max_share_age
    )

    data = (
        spending_by_purpose(
            years=read_years,
            providers=multilateral_providers,
            flow_types=flow_types,
            exclude_multilateral_core=exclude_multilateral_core,
            crs=crs,
            refresh=refresh,
        )
        .pipe(add_multilateral_channel_codes)
        .pipe(_group_by_mapped_channel)
    )

    if requested_years is None:
        requested_years = sorted(int(y) for y in data[ODASchema.YEAR].dropna().unique())

    denominator = (
        data.groupby(
            [*SHARE_DENOMINATOR_GROUPER, ODASchema.YEAR], observed=True, dropna=False
        )[[ODASchema.VALUE]]
        .sum()
        .reset_index()
    )
    denominator_windows = rolling_window_total(
        denominator, period_length=period_length, grouper=SHARE_DENOMINATOR_GROUPER
    )

    resolved = resolve_channel_windows(
        denominator_windows,
        requested_years=requested_years,
        max_share_age=max_share_age,
        grouper=SHARE_DENOMINATOR_GROUPER,
    )

    if resolved.empty:
        return pd.DataFrame(
            columns=[
                ODASchema.YEAR,
                ODASchema.CHANNEL_CODE,
                ODASchema.PURPOSE_CODE,
                ODASchema.RECIPIENT_CODE,
                ODASchema.SHARE,
                "allocation_status",
                "share_years",
            ]
        )

    numerator_windows = rolling_window_total(
        data, period_length=period_length, grouper=SHARE_NUMERATOR_GROUPER
    ).rename(columns={ODASchema.YEAR: "source_year"})
    # A wider CRS read (e.g. the padding pad_years_for_window adds, or a
    # multi-year request) surfaces (channel, purpose, recipient) windows
    # whose net spending is exactly zero for a given source_year -- present
    # somewhere in the read years, but idle in this particular window. Left
    # in, `merged` below broadcasts one such row per requested year per
    # channel, ballooning both this frame and the allocation join downstream
    # for no informational gain: a zero-numerator row always produces
    # share == 0. Dropping it here cannot change any channel-year's share
    # total (it contributes exactly 0), only keeps this frame (and the
    # allocation output) from growing with the read window instead of the
    # request. A genuine negative window (a net CRS reversal) is left in.
    numerator_windows = numerator_windows.loc[numerator_windows[ODASchema.VALUE] != 0]

    merged = resolved.merge(
        numerator_windows, on=[*SHARE_DENOMINATOR_GROUPER, "source_year"], how="left"
    )

    merged[ODASchema.SHARE] = merged[ODASchema.VALUE] / merged["denominator"]
    merged["share_years"] = (
        (merged["source_year"] - period_length + 1).astype(str)
        + "-"
        + merged["source_year"].astype(str)
    )

    return merged[
        [
            ODASchema.YEAR,
            ODASchema.CHANNEL_CODE,
            ODASchema.PURPOSE_CODE,
            ODASchema.RECIPIENT_CODE,
            ODASchema.SHARE,
            "allocation_status",
            "share_years",
        ]
    ].reset_index(drop=True)

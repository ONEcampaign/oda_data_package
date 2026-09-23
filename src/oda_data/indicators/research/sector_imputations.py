"""Imputed multilateral spending by purpose: money conservation.

`imputed_multilateral_by_purpose` takes every donor's core (unearmarked)
contribution to a multilateral organisation (from Multisystem) and spreads it
across purpose codes using that organisation's own CRS spending pattern (from
`imputation_shares.multilateral_spending_shares_by_channel_and_purpose_smoothed`).
Every core dollar ends up in exactly one output row: imputed against the
channel's own current shares, imputed against its own stale (lapsed-reporter)
shares, imputed against a proxy channel's shares, or reported `unallocated`
with the reason recorded in `channel_share_proxies.csv` / the crosswalk. The
`_check_conservation` guard makes silently dropping money a hard failure
(`ImputationConservationError`) rather than a warning.
"""

import hashlib
import json
from dataclasses import asdict
from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version
from pathlib import Path

import pandas as pd

from oda_data.api.constants import MEASURES, Measure
from oda_data.clean_data.common import convert_units
from oda_data.clean_data.schema import ODASchema
from oda_data.config import ODAPaths

# Share-side functions (window padding, vectorised rolling totals,
# flow_types, stale-share fallback) live in `imputation_shares.py`; these
# names are re-exported here so `sector_imputations.<name>` imports keep
# working.
from oda_data.indicators.research.imputation_shares import (  # noqa: F401
    _as_list,
    _multilateral_spending_shares,
    _resolve_flow_types,
    add_multi_channels_and_group,
    multilateral_spending_shares_by_channel_and_purpose_smoothed,
    period_purpose_shares,
    rolling_period_total,
    share_by_purpose,
    spending_by_purpose,
)

PROXY_TABLE_FILE = "channel_share_proxies.csv"
CROSSWALK_FILE = "multilateral_channel_crosswalk.csv"
CRS_CHANNEL_MAPPING_VINTAGE_FILE = "crs_channel_mapping_vintage.json"

# Columns present on every row of `imputed_multilateral_by_purpose`'s output,
# in the order of the output contract.
_OUTPUT_COLUMNS: list[str] = [
    ODASchema.YEAR,
    ODASchema.PROVIDER_CODE,
    ODASchema.CHANNEL_CODE,
    ODASchema.RECIPIENT_CODE,
    ODASchema.PURPOSE_CODE,
    ODASchema.VALUE,
    ODASchema.CURRENCY,
    ODASchema.PRICES,
    "allocation_status",
    "share_channel_code",
    "share_years",
]

# Output columns that must be nullable Int64: `unallocated` rows carry no
# recipient/purpose, and `fixed_purpose`/`unallocated` rows carry no
# share_channel_code. A plain int/float dtype would silently turn these into
# float64 the moment a null appears, changing dtype for every downstream
# consumer merging on them.
_NULLABLE_INT_COLUMNS: list[str] = [
    ODASchema.RECIPIENT_CODE,
    ODASchema.PURPOSE_CODE,
    "share_channel_code",
]


class ImputationConservationError(Exception):
    """`imputed_multilateral_by_purpose`'s output does not conserve every
    (year, donor_code, channel_code) core contribution amount.

    This is a hard failure by design: money silently dropped by a join or a
    dtype mismatch is a correctness bug, not something to warn about and
    move past.
    """


def core_multilateral_contributions_by_provider(
    years: list | int | range | None = None,
    providers: list | int | None = None,
    channels: list | int | None = None,
    measure: Measure | str = "gross_disbursement",
    currency: str = "USD",
    base_year: int | None = None,
    *,
    multisystem: pd.DataFrame | None = None,
    refresh: bool = False,
) -> pd.DataFrame:
    """Retrieves core multilateral contributions grouped by provider and channel.

    Args:
        years (list | int | range, optional): Years to filter the data. Defaults to None.
        providers (list | int | None, optional): Providers to filter the data. Defaults to None.
        channels (list | int | None, optional): Channels to filter the data. Defaults to None.
        measure (Measure | str, optional): Measure type. Defaults to "gross_disbursement".
        currency (str, optional): Target currency. Defaults to "USD".
        base_year (int | None, optional): Base year for conversion. Defaults to None.
        multisystem (pd.DataFrame | None, optional): Pre-fetched Multisystem data to
            use instead of reading it (for tests and pinned builds). Must carry
            `donor_code`, `channel_code`, `year` and `amount` columns, already
            filtered to core contributions. `years`, `providers` and `channels`
            are applied to it, and so are the `measure` flow type and current
            prices where it carries `flow_type` and `amount_type` columns.
            Defaults to None (read from `MultiSystemData`).
        refresh (bool, optional): If True, bypass the bulk cache and re-download
            (#162). Only has an effect when `multisystem` is None. Defaults to False.

    Returns:
        pd.DataFrame: Dataframe with core multilateral contributions.
    """
    data = _read_core_contributions(
        years=years,
        providers=providers,
        channels=channels,
        measure=measure,
        multisystem=multisystem,
        refresh=refresh,
    )

    return convert_units(data, currency=currency, base_year=base_year)


def _filter_supplied_multisystem(
    multisystem: pd.DataFrame,
    *,
    years: list | int | range | None,
    providers: list | int | None,
    channels: list | int | None,
    measure_filter: str,
) -> pd.DataFrame:
    """Apply to a caller-supplied Multisystem frame the filters a
    `MultiSystemData` read applies. The `flow_type` and `amount_type` filters
    apply only when the frame carries those columns."""
    mask = pd.Series(True, index=multisystem.index)
    for column, values in (
        (ODASchema.YEAR, _as_list(years)),
        (ODASchema.PROVIDER_CODE, _as_list(providers)),
        (ODASchema.CHANNEL_CODE, _as_list(channels)),
    ):
        if values is not None:
            mask &= multisystem[column].isin(values)
    if "flow_type" in multisystem.columns:
        mask &= multisystem["flow_type"] == measure_filter
    if "amount_type" in multisystem.columns:
        mask &= multisystem["amount_type"] == "Current prices"
    return multisystem.loc[mask]


def _read_core_contributions(
    years: list | int | range | None,
    providers: list | int | None,
    channels: list | int | None,
    measure: Measure | str,
    multisystem: pd.DataFrame | None,
    refresh: bool,
) -> pd.DataFrame:
    """Read raw (unconverted) core contributions, one row per (year,
    donor_code, channel_code)."""
    cols = [ODASchema.PROVIDER_CODE, ODASchema.CHANNEL_CODE, ODASchema.YEAR]
    measure_filter = MEASURES["Multisystem"][measure]["filter"]

    if multisystem is not None:
        raw = _filter_supplied_multisystem(
            multisystem,
            years=years,
            providers=providers,
            channels=channels,
            measure_filter=measure_filter,
        )
    else:
        from oda_data.api.sources import MultiSystemData

        filters = [
            ("flow_type", "in", [measure_filter]),
            ("amount_type", "in", ["Current prices"]),
        ]
        resolved_channels = [channels] if isinstance(channels, int) else channels
        if resolved_channels:
            filters.append((ODASchema.CHANNEL_CODE, "in", resolved_channels))

        ms = MultiSystemData(
            providers=providers, years=years, indicators="Core contributions to"
        )
        raw = ms.read(
            columns=[*cols, ODASchema.AMOUNT],
            additional_filters=filters,
            using_bulk_download=True,
            refresh=refresh,
        )

    return (
        raw.groupby(cols, dropna=False, observed=True)[[ODASchema.AMOUNT]]
        .sum()
        .reset_index()
        .rename(columns={ODASchema.AMOUNT: ODASchema.VALUE})
        .astype({ODASchema.CHANNEL_CODE: "int64"})
    )


def _get_channel_share_proxies() -> pd.DataFrame:
    """Read the proxy table, keeping only rows marked `reviewed=true`.

    Returns:
        pd.DataFrame: columns channel_code, channel_name, proxy_channel_code,
        proxy_type ("parent_fund" or "fixed_purpose"), fixed_purpose_code,
        rationale, reviewed. `proxy_channel_code` is null for `fixed_purpose`
        rows; `fixed_purpose_code` is null for `parent_fund` rows.

    Raises:
        ValueError: If the table has no `reviewed` column.
    """
    proxies = pd.read_csv(
        ODAPaths.cleaning / PROXY_TABLE_FILE,
        dtype={
            ODASchema.CHANNEL_CODE: "Int64",
            "proxy_channel_code": "Int64",
            "fixed_purpose_code": "Int64",
        },
    )
    if "reviewed" not in proxies.columns:
        raise ValueError(
            f"{PROXY_TABLE_FILE} has no 'reviewed' column, so unreviewed proxies "
            "cannot be told apart from reviewed ones."
        )
    return proxies.loc[proxies["reviewed"].eq(True)].reset_index(drop=True)


def _apply_proxies(
    unmatched: pd.DataFrame, shares: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Resolve unmatched core rows against the proxy table.

    A proxy never chains: a `parent_fund` row is resolved against the proxy
    channel's own current/stale shares only, never against a second proxy.

    Args:
        unmatched: Core rows (`year, donor_code, channel_code, value`) whose
            channel had no own share for that year.
        shares: Output of
            `multilateral_spending_shares_by_channel_and_purpose_smoothed`,
            used as the source of the proxy channel's own shares.

    Returns:
        `(proxy_rows, still_unmatched)`: rows resolved via a proxy
        (`_OUTPUT_COLUMNS`-shaped, minus currency/prices), and the remaining
        core rows with no proxy (or a proxy with no share pool of its own),
        which fall through to `unallocated`.
    """
    proxy_table = _get_channel_share_proxies()
    with_proxy = unmatched.merge(
        proxy_table[
            ["channel_code", "proxy_channel_code", "proxy_type", "fixed_purpose_code"]
        ],
        on=ODASchema.CHANNEL_CODE,
        how="left",
    )

    fixed = (
        with_proxy.loc[with_proxy["proxy_type"] == "fixed_purpose"]
        .assign(
            **{
                ODASchema.PURPOSE_CODE: lambda d: d["fixed_purpose_code"],
                ODASchema.RECIPIENT_CODE: pd.NA,
                "allocation_status": "proxy",
                "share_channel_code": pd.NA,
                "share_years": pd.NA,
            }
        )
        .drop(columns=["proxy_channel_code", "proxy_type", "fixed_purpose_code"])
    )

    parent_candidates = with_proxy.loc[with_proxy["proxy_type"] == "parent_fund"].drop(
        columns=["fixed_purpose_code", "proxy_type"]
    )
    parent_shares = shares.rename(
        columns={ODASchema.CHANNEL_CODE: "proxy_channel_code"}
    )
    parent_matched = parent_candidates.merge(
        parent_shares,
        on=[ODASchema.YEAR, "proxy_channel_code"],
        how="inner",
    ).assign(
        **{
            ODASchema.VALUE: lambda d: d[ODASchema.VALUE] * d[ODASchema.SHARE],
            "allocation_status": "proxy",
            "share_channel_code": lambda d: d["proxy_channel_code"],
        }
    )
    parent_matched = parent_matched.drop(
        columns=[ODASchema.SHARE, "proxy_channel_code"]
    )

    proxy_rows = pd.concat([fixed, parent_matched], ignore_index=True, sort=False)

    key = [ODASchema.YEAR, ODASchema.PROVIDER_CODE, ODASchema.CHANNEL_CODE]
    if proxy_rows.empty:
        return proxy_rows, unmatched

    resolved_keys = proxy_rows[key].drop_duplicates()
    joined = unmatched.merge(resolved_keys, on=key, how="left", indicator=True)
    still_unmatched = joined.loc[joined["_merge"] == "left_only"].drop(columns="_merge")

    return proxy_rows, still_unmatched


def _allocate(
    core: pd.DataFrame, shares: pd.DataFrame, *, use_proxy_shares: bool
) -> pd.DataFrame:
    """Outer-join core contributions against shares: own shares first, then
    (optionally) a proxy channel's shares, then `unallocated`.

    Args:
        core: `year, donor_code, channel_code, value` -- one row per core
            contribution, raw currency (pre-conversion).
        shares: Output of
            `multilateral_spending_shares_by_channel_and_purpose_smoothed`.
        use_proxy_shares: Whether unmatched channels fall back to a proxy
            channel's shares before being marked `unallocated`.

    Returns:
        pd.DataFrame: `_OUTPUT_COLUMNS` minus currency/prices (added later,
        once, by the caller).
    """
    key = [ODASchema.YEAR, ODASchema.PROVIDER_CODE, ODASchema.CHANNEL_CODE]

    own = core.merge(
        shares, on=[ODASchema.YEAR, ODASchema.CHANNEL_CODE], how="inner"
    ).assign(
        **{
            ODASchema.VALUE: lambda d: d[ODASchema.VALUE] * d[ODASchema.SHARE],
            "share_channel_code": lambda d: d[ODASchema.CHANNEL_CODE],
        }
    )
    own = own.drop(columns=[ODASchema.SHARE])

    matched_keys = own[key].drop_duplicates()
    joined = core.merge(matched_keys, on=key, how="left", indicator=True)
    unmatched = joined.loc[joined["_merge"] == "left_only"].drop(columns="_merge")

    proxy_rows = unmatched.iloc[0:0]
    if use_proxy_shares and not unmatched.empty:
        # `shares` may be empty (e.g. no channel in `core` has any CRS
        # presence): `fixed_purpose` proxy rows don't need it at all, and a
        # `parent_fund` row's merge against an empty `shares` simply matches
        # nothing, falling through to unallocated below -- so this must not
        # be skipped just because `shares` happens to be empty.
        proxy_rows, unmatched = _apply_proxies(unmatched, shares)

    unallocated = unmatched.assign(
        **{
            ODASchema.RECIPIENT_CODE: pd.NA,
            ODASchema.PURPOSE_CODE: pd.NA,
            "allocation_status": "unallocated",
            "share_channel_code": pd.NA,
            "share_years": pd.NA,
        }
    )

    parts = [df for df in (own, proxy_rows, unallocated) if not df.empty]
    if not parts:
        columns = [
            c
            for c in _OUTPUT_COLUMNS
            if c not in (ODASchema.CURRENCY, ODASchema.PRICES)
        ]
        return pd.DataFrame(columns=columns)

    return pd.concat(parts, ignore_index=True, sort=False)


def _drop_zero_value_rows(result: pd.DataFrame) -> pd.DataFrame:
    """Drop exact-zero-value rows from an allocation result.

    A row with `value == 0.0` exactly contributes nothing to its (year,
    donor_code, channel_code) conservation total, so dropping it cannot
    change whether that total conserves. Three things produce these rows:
    a purpose/recipient share window whose net CRS spending nets to exactly
    zero, a zero-dollar core contribution multiplying out to zero across
    every purpose/proxy it has a share in, and a zero-dollar core
    contribution that falls through to `unallocated` (no core money was
    actually left unallocated; there was none to begin with). None of these
    is a real allocation, so none should be emitted -- a single-year call
    and the matching year of a multi-year call must return the same rows.
    A negative value is a real CRS reversal flowing through a share and is
    kept.

    Args:
        result: `_allocate`'s output (`_OUTPUT_COLUMNS` minus
            currency/prices).

    Returns:
        pd.DataFrame: `result` with `value == 0.0` rows removed.
    """
    return result.loc[result[ODASchema.VALUE] != 0.0].reset_index(drop=True)


def _check_conservation(
    result: pd.DataFrame, core: pd.DataFrame, *, stage: str
) -> None:
    """Raise `ImputationConservationError` if `result` does not conserve
    every (year, donor_code, channel_code) amount in `core`.

    Args:
        result: The (possibly partial) allocation output.
        core: The core contributions being allocated, one row per key.
        stage: Human-readable label for where in the pipeline this check
            runs (e.g. "pre-conversion", "post-conversion"), used only in
            the error message.

    Raises:
        ImputationConservationError: If any (year, donor_code, channel_code)
            combination's allocated total differs from its core amount by
            more than `1e-6 * abs(core) + 1e-9`.
    """
    key = [ODASchema.YEAR, ODASchema.PROVIDER_CODE, ODASchema.CHANNEL_CODE]

    if core.empty:
        return

    allocated = (
        result.groupby(key, dropna=False, observed=True)[ODASchema.VALUE]
        .sum()
        .rename("allocated")
    )
    core_totals = core.set_index(key)[ODASchema.VALUE].rename("core")

    compared = pd.concat([core_totals, allocated], axis=1).fillna(0.0)
    compared["diff"] = compared["allocated"] - compared["core"]
    tolerance = 1e-6 * compared["core"].abs() + 1e-9
    violations = compared.loc[compared["diff"].abs() > tolerance]

    if violations.empty:
        return

    worst = violations.reindex(
        violations["diff"].abs().sort_values(ascending=False).index
    ).head(10)
    offenders = "; ".join(
        f"year={y}, donor_code={d}, channel_code={c}: core={row['core']:.6f}, "
        f"allocated={row['allocated']:.6f}, diff={row['diff']:.6f}"
        for (y, d, c), row in worst.iterrows()
    )
    raise ImputationConservationError(
        f"imputed_multilateral_by_purpose does not conserve core contributions "
        f"({stage}): {len(violations)} of {len(compared)} (year, donor_code, "
        f"channel_code) combinations violate abs(diff) <= 1e-6*abs(core) + 1e-9. "
        f"Worst offenders: {offenders}"
    )


def _cast_output_dtypes(result: pd.DataFrame) -> pd.DataFrame:
    """Cast nullable output columns to pandas Int64, so `unallocated` /
    `fixed_purpose` rows don't silently turn a whole column to float64."""
    return result.astype(dict.fromkeys(_NULLABLE_INT_COLUMNS, "Int64"))


def _empty_result() -> pd.DataFrame:
    """An empty, correctly typed `imputed_multilateral_by_purpose` result."""
    result = pd.DataFrame(columns=_OUTPUT_COLUMNS)
    return _cast_output_dtypes(result)


def _file_vintage(path: Path) -> dict:
    """Fingerprint a settings CSV: path, content hash, mtime, size.

    There is no separate vintage sidecar for the crosswalk or proxy table
    (unlike `crs_channel_mapping.csv`), so provenance is derived directly
    from the file on disk.
    """
    if not path.exists():
        return {"path": str(path), "exists": False}

    stat = path.stat()
    return {
        "path": str(path),
        "sha256_16": hashlib.sha256(path.read_bytes()).hexdigest()[:16],
        "mtime": datetime.fromtimestamp(stat.st_mtime, tz=UTC).isoformat(),
        "size_bytes": stat.st_size,
    }


def _crs_channel_mapping_vintage() -> dict | None:
    """Read the CRS channel codelist refresh vintage recorded by
    `scripts/refresh_channel_crosswalk.py`, or None if missing/unreadable."""
    path = ODAPaths.cleaning / CRS_CHANNEL_MAPPING_VINTAGE_FILE
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None


def _package_version() -> str:
    try:
        return _pkg_version("oda_data")
    except PackageNotFoundError:
        return "unknown"


def _release(source: str, *, supplied: bool, read: bool) -> dict | str | None:
    """The release a source's data came from: "supplied" for a caller-supplied
    frame, None when it was not read, else the cached release metadata."""
    from oda_data.cache import release_info

    if supplied:
        return "supplied"
    if not read:
        return None
    release = release_info(source)
    return asdict(release) if release is not None else None


def _build_provenance(
    *,
    crs_supplied: bool,
    multisystem_supplied: bool,
    crs_read: bool,
    parameters: dict,
) -> dict:
    """Build the `result.attrs["provenance"]` dict for
    `imputed_multilateral_by_purpose`.

    Called after the reads it describes, so the cached release metadata it
    records is the one those reads produced.
    """
    return {
        "crs_release": _release("CRSData", supplied=crs_supplied, read=crs_read),
        "multisystem_release": _release(
            "MultiSystemData", supplied=multisystem_supplied, read=True
        ),
        "crosswalk_vintage": _file_vintage(ODAPaths.cleaning / CROSSWALK_FILE),
        "proxy_table_vintage": _file_vintage(ODAPaths.cleaning / PROXY_TABLE_FILE),
        "crs_channel_mapping_vintage": _crs_channel_mapping_vintage(),
        "package_version": _package_version(),
        "parameters": parameters,
    }


def imputed_multilateral_by_purpose(
    years: list | int | range | None = None,
    providers: list | int | None = None,
    channels: list | int | None = None,
    measure: Measure | str = "gross_disbursement",
    currency: str = "USD",
    base_year: int | None = None,
    shares_based_on_oda_only: bool | None = None,
    *,
    flow_types: tuple[str, ...] = ("ODA", "OOF"),
    period_length: int = 3,
    max_share_age: int = 5,
    use_proxy_shares: bool = True,
    crs: pd.DataFrame | None = None,
    multisystem: pd.DataFrame | None = None,
    refresh: bool = False,
) -> pd.DataFrame:
    """Computes imputed multilateral spending by purpose.

    Every donor's core (unearmarked) contribution to a multilateral channel
    (Multisystem) is spread across purpose codes using that channel's own
    CRS spending pattern
    (`imputation_shares.multilateral_spending_shares_by_channel_and_purpose_smoothed`).
    A channel with no CRS rows of its own in the requested window falls back
    to its most recent window (`allocation_status="stale_share"`), then to a
    reviewed proxy channel's shares (`allocation_status="proxy"`,
    `channel_share_proxies.csv`), then to `unallocated` (full core amount,
    `recipient_code`/`purpose_code` null). Every core dollar ends up in
    exactly one output row: conservation is enforced, not assumed -- see
    `ImputationConservationError`.

    The multilateral spending shares this imputes onto exclude CRS rows
    reporting a donor's core contribution to a multilateral organisation
    (`bi_multi == 2`) by default. Those core contributions are already
    captured here, on the Multisystem leg. Including them on the CRS leg too
    would double count them.

    `result.attrs["provenance"]` records the upstream CRS/Multisystem
    release identity, the crosswalk/proxy-table vintage, the package version
    and this call's parameters, so a result can be traced back to what
    produced it. Most pandas operations (including many that look like
    simple filters, e.g. some groupby/merge paths) drop `.attrs`; read it
    from the frame this function returns, before further transformation.

    Args:
        years (list | int | range, optional): Years to filter the data. Defaults to None.
        providers (list | int | None, optional): Providers to filter the data. Defaults to None.
        channels (list | int | None, optional): Channels to filter the data. Defaults to None.
        measure (Measure | str, optional): Measure type. Defaults to "gross_disbursement".
        currency (str, optional): Target currency. Defaults to "USD".
        base_year (int | None, optional): Base year for conversion. Defaults to None.
        shares_based_on_oda_only (bool | None, optional): Deprecated; use
            `flow_types`. `None` (the default) means "not passed" and does not emit
            a warning; passing True/False emits a `DeprecationWarning` and overrides
            `flow_types`. Defaults to None.
        flow_types (tuple[str, ...], optional): CRS flow types the shares are based
            on. Defaults to `("ODA", "OOF")`, the discontinued OECD sectoral
            imputation practice, needed so OOF-only-reporting channels (IBRD, EBRD,
            IFC, IDB Invest) get a non-empty share pool. Pass `("ODA",)` for the
            stricter, official-ODA-definition basis.
        period_length (int, optional): Rolling window length in years for shares. Defaults to 3.
        max_share_age (int, optional): Maximum number of years a stale window may
            lag behind the requested year. Defaults to 5.
        use_proxy_shares (bool, optional): Whether a channel with no own current or
            stale share falls back to a reviewed proxy channel's shares
            before being marked `unallocated`. Defaults to True.
        crs (pd.DataFrame | None, optional): Pre-fetched, row-level CRS data to use
            instead of reading it (for tests and pinned builds). Forwarded to
            `multilateral_spending_shares_by_channel_and_purpose_smoothed`. Defaults
            to None.
        multisystem (pd.DataFrame | None, optional): Pre-fetched Multisystem data to
            use instead of reading it. Forwarded to
            `core_multilateral_contributions_by_provider`. Defaults to None.
        refresh (bool, optional): If True, bypass the bulk cache and re-download for
            both the CRS and Multisystem reads (#162). Only has an effect where the
            corresponding `crs`/`multisystem` input is None. Defaults to False.

    Returns:
        pd.DataFrame: `year, donor_code, channel_code, recipient_code,
        purpose_code, value, currency, prices, allocation_status,
        share_channel_code, share_years`. `recipient_code`, `purpose_code`
        and `share_channel_code` are nullable Int64. `channel_code` is
        always the core-contribution channel, never the proxy. Rows with
        `value == 0.0` exactly are never emitted (a wider read window than
        the requested years can surface a share window or a core
        contribution that nets to nothing); a single-year call and the
        matching year of a multi-year call therefore return the same rows.
        A negative `value` is a real CRS reversal and is kept.

    Raises:
        ImputationConservationError: If the sum of `value` for any (year,
            donor_code, channel_code) does not match its core contribution
            amount within `abs(diff) <= 1e-6 * abs(core) + 1e-9`, checked
            both before and after currency conversion.
    """
    return _imputed_multilateral_by_purpose(
        years=years,
        providers=providers,
        channels=channels,
        measure=measure,
        currency=currency,
        base_year=base_year,
        shares_based_on_oda_only=shares_based_on_oda_only,
        flow_types=flow_types,
        period_length=period_length,
        max_share_age=max_share_age,
        use_proxy_shares=use_proxy_shares,
        exclude_multilateral_core=True,
        crs=crs,
        multisystem=multisystem,
        refresh=refresh,
    )


def _imputed_multilateral_by_purpose(
    *,
    years: list | int | range | None,
    providers: list | int | None,
    channels: list | int | None,
    measure: Measure | str,
    currency: str,
    base_year: int | None,
    shares_based_on_oda_only: bool | None,
    flow_types: tuple[str, ...],
    period_length: int,
    max_share_age: int,
    use_proxy_shares: bool,
    exclude_multilateral_core: bool,
    crs: pd.DataFrame | None,
    multisystem: pd.DataFrame | None,
    refresh: bool,
) -> pd.DataFrame:
    """Body of `imputed_multilateral_by_purpose`.

    `exclude_multilateral_core=False` keeps CRS rows reporting a core
    contribution to a multilateral organisation (`bi_multi == 2`) in the
    shares. It exists to measure the effect of excluding them
    (`scripts/imputation_delta.py`), and `imputed_multilateral_by_purpose`
    always passes True.
    """
    # stacklevel=4 points a deprecation warning at the caller of the public
    # function, past _resolve_flow_types, this function and the public one.
    resolved_flow_types = _resolve_flow_types(
        flow_types, shares_based_on_oda_only, stacklevel=4
    )

    core_raw = _read_core_contributions(
        years=years,
        providers=providers,
        channels=channels,
        measure=measure,
        multisystem=multisystem,
        refresh=refresh,
    )

    parameters = {
        "years": years,
        "providers": providers,
        "channels": channels,
        "measure": measure,
        "currency": currency,
        "base_year": base_year,
        "flow_types": flow_types,
        "period_length": period_length,
        "max_share_age": max_share_age,
        "use_proxy_shares": use_proxy_shares,
        "exclude_multilateral_core": exclude_multilateral_core,
        "refresh": refresh,
        "shares_based_on_oda_only": shares_based_on_oda_only,
    }

    if core_raw.empty:
        result = _empty_result()
        result.attrs["provenance"] = _build_provenance(
            crs_supplied=crs is not None,
            multisystem_supplied=multisystem is not None,
            crs_read=False,
            parameters=parameters,
        )
        return result

    core_years = sorted(int(y) for y in core_raw[ODASchema.YEAR].unique())

    shares = _multilateral_spending_shares(
        core_years,
        flow_types=resolved_flow_types,
        period_length=period_length,
        max_share_age=max_share_age,
        exclude_multilateral_core=exclude_multilateral_core,
        crs=crs,
        refresh=refresh,
    )

    result_raw = _allocate(core_raw, shares, use_proxy_shares=use_proxy_shares)
    result_raw = _drop_zero_value_rows(result_raw)
    _check_conservation(result_raw, core_raw, stage="pre-conversion")

    result = convert_units(result_raw, currency=currency, base_year=base_year)
    core_converted = convert_units(core_raw, currency=currency, base_year=base_year)
    _check_conservation(result, core_converted, stage="post-conversion")

    result = _cast_output_dtypes(result[_OUTPUT_COLUMNS].reset_index(drop=True))
    result.attrs["provenance"] = _build_provenance(
        crs_supplied=crs is not None,
        multisystem_supplied=multisystem is not None,
        crs_read=True,
        parameters=parameters,
    )

    return result

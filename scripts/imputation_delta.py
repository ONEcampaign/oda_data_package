"""Delta script: what changed in `imputed_multilateral_by_purpose` between
the previous release and this branch, attributed to cause where separable.

Compares the previous release's output (installed in an isolated `uv`
environment, e.g. `oda-data==2.7.0`) against this branch's, on the same
pinned years/providers and the same on-disk cache, and writes a year x donor
delta table plus a set of named-cause columns computed by ablating one new
mechanism at a time from this branch's own pipeline. See
`docs/docs/imputation-delta.md` for the methodology and how to read the
output.

Named causes:

- `bi_multi_exclusion`: the 2.7.1 fix that excludes CRS rows reporting a
  donor's own core contribution to a multilateral organisation
  (`bi_multi == 2`) from the shares this pipeline imputes onto. Measured by
  running this branch's pipeline with `exclude_multilateral_core=False` and
  diffing against the real default (True).
- `window_padding`: the stale-share lookback (`max_share_age`) that lets a
  lapsed or early-window channel still resolve a share instead of falling
  straight to unallocated. Measured by running this branch's pipeline with
  `max_share_age=0` and diffing against the real default (5). This column
  also absorbs the previous release's window-truncation defect: with
  `period_length=3` (the default on both releases), `oda-data==2.7.0`
  emitted no rows at all for the first `period_length - 1` years of any
  multi-year request (`old_total` is exactly 0 there, not a real decline),
  because it never padded its CRS read backwards far enough for those
  years' own rolling window. This branch's `pad_years_for_window` fixes
  that unconditionally, with no toggle to reproduce the old truncation, so
  it cannot be isolated by ablating this branch's own defaults the way the
  other three causes are. Since the affected years are known exactly from
  the request (every year before `min(years) + period_length - 1`) and no other named cause is a
  candidate explanation for a total that is structurally zero, the whole
  `observed_delta` for those (year, donor) rows -- after subtracting the
  other ablated causes and `unallocated_and_proxy`, same as `residual`
  would -- is folded into `window_padding` instead of left in `residual`.
- `share_basis`: the shares' `flow_types` default, `("ODA", "OOF")` on this
  branch. Measured by running this branch's pipeline with `flow_types=
  ("ODA",)` and diffing against the real default. The previous release's
  actual default (`shares_based_on_oda_only=False`) applied no CRS category
  filter at all -- not reproducible with this branch's `flow_types`, which
  has no "no filter" option -- so this column is a sensitivity measure, not
  a literal reproduction of the old basis; the true old-vs-new gap from this
  cause is folded into `residual` along with `channel_remapping`.
- `unallocated_and_proxy`: money that is now visible as `unallocated` or
  `proxy` rows in this branch's output. Read directly off the real result
  (not an ablation): `sum(value)` where `allocation_status` is `unallocated`
  or `proxy`. `stale_share` rows are excluded here, since their dollar
  effect is already the `window_padding` cause above; counting them in both
  would double-count that money.
- `channel_remapping`: reported separately, at channel level, in
  `channel_remapping.csv` -- see `channel_remapping_diagnostic`. Not folded
  into the year x donor table, since isolating its per-(year, donor) dollar
  effect would require re-running the allocation step with the old channel
  mapping substituted in, which is internal to `sector_imputations.py` (S5)
  and not reachable from this script's public-API-only ablations.
- `residual`: `observed_delta - (bi_multi_exclusion + window_padding +
  share_basis + unallocated_and_proxy)`, with the window-truncation gap
  above already moved out of it and into `window_padding`. Ablated causes
  interact (e.g. proxy money depends on whether a channel already has its
  own window), and `channel_remapping`'s dollar effect and the literal old
  share-basis ("no filter") are not represented in the four named columns
  above, so `residual` does not collapse to zero and should not be read as
  an error -- but it should be small relative to `observed_delta` once the
  truncated years are accounted for; see `docs/docs/imputation-delta.md`
  for the measured size.

Nothing here re-downloads data unless `--refresh` is passed; `--cache-dir`
should point at a cache directory that already holds a fresh CRS and
Multisystem bulk extract (see CACHING.md). Output is written only under
`--output-dir`, which (like `--cache-dir`) is rejected if it resolves inside
this repository, so a run never commits cache files or delta output into
version control.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent

# Named causes ablated one at a time from this branch's own
# `imputed_multilateral_by_purpose` defaults, in the order `run_new_pipeline`
# accepts overrides for them.
_ABLATION_NAMES: tuple[str, ...] = (
    "bi_multi_exclusion",
    "window_padding",
    "share_basis",
)

# `imputed_multilateral_by_purpose`'s default rolling-window length, on both
# the previous release and this branch, and the value this script always
# runs with (never overridden). Used both to pad the CRS read for the
# bi_multi_exclusion ablation and to work out which years the previous
# release's window-truncation defect hits -- see `window_padding` in the
# module docstring.
_PERIOD_LENGTH = 3

_YEAR = "year"
_DONOR = "donor_code"
_PROVIDER = "provider_code"
_AGENCY = "agency_code"
_VALUE = "value"


def _parse_years(spec: str) -> list[int]:
    """Parse "2015-2024", "2021" or "2019,2021,2023" into a sorted year list."""
    years: set[int] = set()
    for raw_part in spec.split(","):
        part = raw_part.strip()
        if not part:
            continue
        if "-" in part:
            start, end = part.split("-", 1)
            years.update(range(int(start), int(end) + 1))
        else:
            years.add(int(part))
    if not years:
        raise SystemExit(f"--years {spec!r} did not parse to any year.")
    return sorted(years)


def _parse_providers(spec: str | None) -> list[int] | None:
    if not spec:
        return None
    return [int(p.strip()) for p in spec.split(",") if p.strip()]


def _reject_repo_path(path: Path, *, flag: str) -> Path:
    """Refuse a path that resolves inside the repository.

    Applied to `--cache-dir` and `--output-dir`, so a run never writes cache
    files or delta output into version control.
    """
    resolved = path.resolve()
    if resolved == REPO_ROOT or REPO_ROOT in resolved.parents:
        raise SystemExit(
            f"{flag} ({resolved}) is inside the repository ({REPO_ROOT}); "
            "point it at a directory outside the repo."
        )
    return resolved


def _year_donor_totals(result: pd.DataFrame, *, label: str) -> pd.DataFrame:
    """Sum `value` by (year, donor_code), for either package version's output."""
    if result.empty:
        return pd.DataFrame(columns=[_YEAR, _DONOR, label])
    return (
        result.groupby([_YEAR, _DONOR], dropna=False, observed=True)[_VALUE]
        .sum()
        .reset_index()
        .rename(columns={_VALUE: label})
    )


def _truncated_years(years: list[int], period_length: int) -> frozenset[int]:
    """The requested years that the previous release's window truncation
    defect hits: `oda-data==2.7.0` read CRS for the requested years only, so
    a year earlier than `min(years) + period_length - 1` never got a rolling
    window and `old_total` is exactly 0 there regardless of donor -- see
    `window_padding` in the module docstring. The cutoff is a calendar year,
    so a non-contiguous request such as 2019, 2021, 2023 loses 2019 only."""
    if not years:
        return frozenset()
    cutoff = min(years) + period_length - 1
    return frozenset(y for y in years if y < cutoff)


def _status_totals(
    result: pd.DataFrame, statuses: tuple[str, ...], *, label: str
) -> pd.DataFrame:
    """Sum `value` by (year, donor_code) for rows whose `allocation_status`
    is in `statuses`, used for the `unallocated_and_proxy` cause column."""
    subset = result.loc[result["allocation_status"].isin(statuses)]
    return _year_donor_totals(subset, label=label)


# ============================================================================
# This branch's own pipeline (in-process)
# ============================================================================


def run_new_pipeline(
    years: list[int],
    providers: list[int] | None,
    measure: str,
    currency: str,
    base_year: int | None,
    refresh: bool,
    *,
    max_share_age: int = 5,
    flow_types: tuple[str, ...] = ("ODA", "OOF"),
    exclude_multilateral_core: bool = True,
) -> pd.DataFrame:
    """Run this branch's imputation pipeline in-process.

    `max_share_age`, `flow_types` and `exclude_multilateral_core` default to
    this branch's real defaults; `compute_new_causes` overrides exactly one at
    a time to ablate the corresponding named cause. Calls the private
    `_imputed_multilateral_by_purpose`, since `exclude_multilateral_core` is
    not a parameter of the public `imputed_multilateral_by_purpose`.
    """
    from oda_data.indicators.research.sector_imputations import (
        _imputed_multilateral_by_purpose,
    )

    return _imputed_multilateral_by_purpose(
        years=years,
        providers=providers,
        channels=None,
        measure=measure,
        currency=currency,
        base_year=base_year,
        shares_based_on_oda_only=None,
        flow_types=flow_types,
        period_length=_PERIOD_LENGTH,
        max_share_age=max_share_age,
        use_proxy_shares=True,
        exclude_multilateral_core=exclude_multilateral_core,
        crs=None,
        multisystem=None,
        refresh=refresh,
    )


def compute_new_causes(
    years: list[int],
    providers: list[int] | None,
    measure: str,
    currency: str,
    base_year: int | None,
    refresh: bool,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    """Run the real pipeline plus one ablation per named cause.

    Returns `(new_total, cause_tables)`: `new_total` is
    `_year_donor_totals` of the real (all-defaults) result; `cause_tables`
    maps cause name to a `_year_donor_totals`-shaped delta frame (real minus
    ablated).
    """
    real = run_new_pipeline(years, providers, measure, currency, base_year, refresh)
    new_total = _year_donor_totals(real, label="new_total")

    ablated_runs = {
        "bi_multi_exclusion": run_new_pipeline(
            years,
            providers,
            measure,
            currency,
            base_year,
            refresh,
            exclude_multilateral_core=False,
        ),
        "window_padding": run_new_pipeline(
            years, providers, measure, currency, base_year, refresh, max_share_age=0
        ),
        "share_basis": run_new_pipeline(
            years,
            providers,
            measure,
            currency,
            base_year,
            refresh,
            flow_types=("ODA",),
        ),
    }

    real_total = _year_donor_totals(real, label="real_total")
    cause_tables: dict[str, pd.DataFrame] = {}
    for cause in _ABLATION_NAMES:
        ablated_total = _year_donor_totals(ablated_runs[cause], label="ablated_total")
        merged = real_total.merge(
            ablated_total, on=[_YEAR, _DONOR], how="outer"
        ).fillna(0.0)
        merged[cause] = merged["real_total"] - merged["ablated_total"]
        cause_tables[cause] = merged[[_YEAR, _DONOR, cause]]

    # `stale_share` is deliberately excluded here: it is the row status the
    # `window_padding` ablation (above) already isolates the dollar effect
    # of. Including it here too would double-count that money under both
    # causes.
    cause_tables["unallocated_and_proxy"] = _status_totals(
        real, ("unallocated", "proxy"), label="unallocated_and_proxy"
    )

    return new_total, cause_tables


# ============================================================================
# Previous release's pipeline (isolated subprocess, `uv run --isolated`)
# ============================================================================

_OLD_PIPELINE_SNIPPET = textwrap.dedent(
    """\
    import pandas as pd
    from oda_data.indicators.research.sector_imputations import (
        imputed_multilateral_by_purpose,
    )

    df = imputed_multilateral_by_purpose(
        years={years!r},
        providers={providers!r},
        measure={measure!r},
        currency={currency!r},
        base_year={base_year!r},
    )
    df.to_parquet({out_path!r})
    """
)

_OLD_CHANNEL_MAPPING_SNIPPET = textwrap.dedent(
    """\
    import pandas as pd
    from oda_data.api.sources import CRSData
    from oda_data.clean_data import channels
    from oda_data.tools.groupings import provider_groupings

    multilateral_providers = list(provider_groupings()["multilateral"])
    crs = CRSData(providers=multilateral_providers, years={years!r})
    raw = crs.read(
        columns=["donor_code", "donor_name", "agency_code", "agency_name",
                 "usd_disbursement"],
        using_bulk_download=True,
    )
    pairs = (
        raw.groupby(["donor_code", "agency_code"], dropna=False)
        .agg(
            donor_name=("donor_name", "first"),
            agency_name=("agency_name", "first"),
            value=("usd_disbursement", "sum"),
        )
        .reset_index()
    )
    mapped = channels.add_multi_channel_codes(
        pairs.rename(columns={{"value": "usd_disbursement"}})
    )
    mapped = mapped.rename(columns={{"channel_code": "channel_code_old"}})
    mapped[["donor_code", "agency_code", "donor_name", "agency_name",
            "channel_code_old", "usd_disbursement"]].to_parquet({out_path!r})
    """
)


def _run_isolated(
    old_version: str, cache_dir: Path, snippet: str, out_path: Path
) -> pd.DataFrame:
    """Run `snippet` inside an isolated `uv run --with oda-data==<old_version>`
    environment, pointed at `cache_dir`, and read back the parquet it wrote."""
    env = {**os.environ, "ODA_DATA_CACHE_DIR": str(cache_dir)}
    cmd = [
        "uv",
        "run",
        "--isolated",
        "--with",
        f"oda-data=={old_version}",
        "python",
        "-c",
        snippet,
    ]
    result = subprocess.run(cmd, env=env, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise RuntimeError(
            f"Isolated oda-data=={old_version} run failed (exit "
            f"{result.returncode}).\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    if not out_path.exists():
        raise RuntimeError(
            f"Isolated oda-data=={old_version} run did not write {out_path}.\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return pd.read_parquet(out_path)


def run_old_pipeline(
    old_version: str,
    years: list[int],
    providers: list[int] | None,
    measure: str,
    currency: str,
    base_year: int | None,
    cache_dir: Path,
    scratch_dir: Path,
) -> pd.DataFrame:
    """Run the previous release's `imputed_multilateral_by_purpose` in an
    isolated `uv` environment, on the same cache and inputs."""
    out_path = scratch_dir / f"old_{old_version}_result.parquet"
    snippet = _OLD_PIPELINE_SNIPPET.format(
        years=years,
        providers=providers,
        measure=measure,
        currency=currency,
        base_year=base_year,
        out_path=str(out_path),
    )
    return _run_isolated(old_version, cache_dir, snippet, out_path)


def old_channel_mapping(
    old_version: str, years: list[int], cache_dir: Path, scratch_dir: Path
) -> pd.DataFrame:
    """(provider_code, agency_code) -> channel_code under the previous
    release's fuzzy/regex matcher, aggregated with the CRS money at stake."""
    out_path = scratch_dir / f"old_{old_version}_channel_mapping.parquet"
    snippet = _OLD_CHANNEL_MAPPING_SNIPPET.format(years=years, out_path=str(out_path))
    return _run_isolated(old_version, cache_dir, snippet, out_path).rename(
        columns={"donor_code": _PROVIDER, "usd_disbursement": _VALUE}
    )


# ============================================================================
# Channel remapping diagnostic (channel level, not year x donor)
# ============================================================================


def new_channel_mapping(
    years: list[int], cache_dir: Path, refresh: bool
) -> pd.DataFrame:
    """(provider_code, agency_code) -> channel_code under this branch's
    crosswalk, aggregated with the CRS money at stake."""
    from oda_data.api.sources import CRSData
    from oda_data.clean_data.channels import add_multilateral_channel_codes
    from oda_data.clean_data.schema import ODASchema
    from oda_data.tools.groupings import provider_groupings

    multilateral_providers = list(provider_groupings()["multilateral"])
    crs = CRSData(providers=multilateral_providers, years=years)
    raw = crs.read(
        columns=[
            ODASchema.PROVIDER_CODE,
            ODASchema.PROVIDER_NAME,
            ODASchema.AGENCY_CODE,
            ODASchema.AGENCY_NAME,
            "usd_disbursement",
        ],
        using_bulk_download=True,
        refresh=refresh,
    )
    pairs = (
        raw.groupby([ODASchema.PROVIDER_CODE, ODASchema.AGENCY_CODE], dropna=False)
        .agg(
            donor_name=(ODASchema.PROVIDER_NAME, "first"),
            agency_name=(ODASchema.AGENCY_NAME, "first"),
            usd_disbursement=("usd_disbursement", "sum"),
        )
        .reset_index()
    )
    mapped = add_multilateral_channel_codes(pairs, on_unmapped="unallocated")
    return mapped.rename(
        columns={
            ODASchema.PROVIDER_CODE: _PROVIDER,
            "channel_code": "channel_code_new",
            "usd_disbursement": _VALUE,
        }
    )


def channel_remapping_diagnostic(
    old_mapping: pd.DataFrame, new_mapping: pd.DataFrame
) -> pd.DataFrame:
    """Per (provider_code, agency_code) pair, the channel code assigned by
    the previous release's matcher versus this branch's crosswalk, and the
    money at stake -- flagged wherever the two disagree.

    This is a channel-level diagnostic, not a year x donor dollar
    attribution: see the module docstring for why it is reported separately
    from the four ablated causes.
    """
    merged = old_mapping[[_PROVIDER, _AGENCY, "channel_code_old"]].merge(
        new_mapping[[_PROVIDER, _AGENCY, "channel_code_new", _VALUE]],
        on=[_PROVIDER, _AGENCY],
        how="outer",
    )
    merged["changed"] = merged["channel_code_old"] != merged["channel_code_new"]
    # NaN != NaN is True in pandas; a pair missing from both sides never
    # happens (outer join on a shared key set), but a pair present in only
    # one release's CRS extract legitimately has a null on the other side --
    # that is itself a mapping change, not a false positive, so it is left
    # flagged rather than being treated as a match.
    return merged.sort_values(_VALUE, ascending=False, na_position="last").reset_index(
        drop=True
    )


# ============================================================================
# Assembly
# ============================================================================


def build_delta_table(
    old_total: pd.DataFrame,
    new_total: pd.DataFrame,
    cause_tables: dict[str, pd.DataFrame],
    *,
    truncated_years: frozenset[int] = frozenset(),
) -> pd.DataFrame:
    """Merge the old/new totals and named-cause columns into one year x
    donor delta table, with `observed_delta` and `residual`.

    `truncated_years` (from `_truncated_years`) are years hit by the
    previous release's window-truncation defect, where `old_total` is
    structurally 0 rather than a real decline. For those rows, whatever
    would have landed in `residual` is folded into `window_padding` instead
    -- see `window_padding` in the module docstring for why that is the
    right cause to attribute it to, and why none of the other ablated
    causes can capture it.
    """
    table = old_total.merge(new_total, on=[_YEAR, _DONOR], how="outer").fillna(0.0)
    table["observed_delta"] = table["new_total"] - table["old_total"]

    named_causes: list[str] = []
    for cause, cause_table in cause_tables.items():
        table = table.merge(cause_table, on=[_YEAR, _DONOR], how="outer")
        table[cause] = table[cause].fillna(0.0)
        named_causes.append(cause)

    table["residual"] = table["observed_delta"] - table[named_causes].sum(axis=1)

    if truncated_years:
        truncated = table[_YEAR].isin(truncated_years)
        table.loc[truncated, "window_padding"] += table.loc[truncated, "residual"]
        table.loc[truncated, "residual"] = 0.0

    ordered = [_YEAR, _DONOR, "old_total", "new_total", "observed_delta"]
    ordered += sorted(named_causes)
    ordered += ["residual"]
    return table[ordered].sort_values([_YEAR, _DONOR]).reset_index(drop=True)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compare imputed_multilateral_by_purpose between a previous "
            "oda-data release and this branch, on the same pinned inputs, "
            "and write a year x donor delta table attributed to cause."
        )
    )
    parser.add_argument(
        "--cache-dir",
        required=True,
        type=Path,
        help=(
            "Cache directory to reuse for both the old and new pipeline "
            "runs (ODA_DATA_CACHE_DIR); must already hold a fresh CRS + "
            "Multisystem bulk extract to avoid a re-download. Rejected if "
            "it resolves inside this repository."
        ),
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        type=Path,
        help=(
            "Directory to write imputation_delta.csv, channel_remapping.csv "
            "and summary.json into. Rejected if it resolves inside this "
            "repository."
        ),
    )
    parser.add_argument(
        "--years",
        default="2015-2024",
        help='Years to compare, e.g. "2015-2024" or "2019,2021,2023". '
        'Defaults to "2015-2024".',
    )
    parser.add_argument(
        "--providers",
        default=None,
        help="Comma-separated donor codes to restrict to. Defaults to all donors.",
    )
    parser.add_argument(
        "--old-version",
        default="2.7.0",
        help='Previous oda-data release to compare against. Defaults to "2.7.0".',
    )
    parser.add_argument(
        "--measure",
        default="gross_disbursement",
        help='Measure type. Defaults to "gross_disbursement".',
    )
    parser.add_argument(
        "--currency", default="USD", help='Target currency. Defaults to "USD".'
    )
    parser.add_argument(
        "--base-year",
        type=int,
        default=None,
        help="Constant-price base year. Defaults to None (current prices).",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Bypass the bulk cache and re-download for this branch's own "
        "pipeline runs (does not apply to the isolated old-version "
        "subprocess, which manages its own cache staleness).",
    )
    parser.add_argument(
        "--skip-channel-remapping",
        action="store_true",
        help="Skip the channel-remapping diagnostic (it needs a second "
        "isolated old-version subprocess run over raw CRS).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Resolve and print the plan (years, providers, paths, isolated "
        "command) without reading any data or invoking uv. For smoke-testing "
        "argument handling without a network call.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    years = _parse_years(args.years)
    providers = _parse_providers(args.providers)
    cache_dir = _reject_repo_path(args.cache_dir, flag="--cache-dir")
    output_dir = _reject_repo_path(args.output_dir, flag="--output-dir")

    if args.dry_run:
        print(
            json.dumps(
                {
                    "years": years,
                    "providers": providers,
                    "cache_dir": str(cache_dir),
                    "output_dir": str(output_dir),
                    "old_version": args.old_version,
                    "measure": args.measure,
                    "currency": args.currency,
                    "base_year": args.base_year,
                    "causes": [*sorted(_ABLATION_NAMES), "unallocated_and_proxy"],
                    "channel_remapping": not args.skip_channel_remapping,
                },
                indent=2,
            )
        )
        return 0

    if not cache_dir.exists():
        raise SystemExit(
            f"--cache-dir {cache_dir} does not exist; point it at a cache "
            "directory that already holds a fresh bulk extract (see "
            "CACHING.md), or create it and pass --refresh to populate it."
        )

    output_dir.mkdir(parents=True, exist_ok=True)
    os.environ["ODA_DATA_CACHE_DIR"] = str(cache_dir)

    print(f"Running this branch's pipeline plus {len(_ABLATION_NAMES)} ablations...")
    new_total, cause_tables = compute_new_causes(
        years, providers, args.measure, args.currency, args.base_year, args.refresh
    )

    print(f"Running oda-data=={args.old_version} in an isolated environment...")
    old_result = run_old_pipeline(
        args.old_version,
        years,
        providers,
        args.measure,
        args.currency,
        args.base_year,
        cache_dir,
        output_dir,
    )
    old_total = _year_donor_totals(old_result, label="old_total")

    delta_table = build_delta_table(
        old_total,
        new_total,
        cause_tables,
        truncated_years=_truncated_years(years, _PERIOD_LENGTH),
    )
    delta_path = output_dir / "imputation_delta.csv"
    delta_table.to_csv(delta_path, index=False)
    print(f"Wrote {delta_path} ({len(delta_table)} rows).")

    if not args.skip_channel_remapping:
        print("Running the channel-remapping diagnostic...")
        old_mapping = old_channel_mapping(
            args.old_version, years, cache_dir, output_dir
        )
        new_mapping = new_channel_mapping(years, cache_dir, args.refresh)
        remap_table = channel_remapping_diagnostic(old_mapping, new_mapping)
        remap_path = output_dir / "channel_remapping.csv"
        remap_table.to_csv(remap_path, index=False)
        print(
            f"Wrote {remap_path} ({int(remap_table['changed'].sum())} of "
            f"{len(remap_table)} pairs changed channel)."
        )

    summary = {
        "years": years,
        "providers": providers,
        "old_version": args.old_version,
        "old_total_usd": float(old_total["old_total"].sum())
        if not old_total.empty
        else 0.0,
        "new_total_usd": float(new_total["new_total"].sum())
        if not new_total.empty
        else 0.0,
        "observed_delta_usd": float(delta_table["observed_delta"].sum()),
        "residual_usd": float(delta_table["residual"].sum()),
    }
    summary_path = output_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2))
    print(f"Wrote {summary_path}.")
    print(json.dumps(summary, indent=2))

    return 0


if __name__ == "__main__":
    sys.exit(main())

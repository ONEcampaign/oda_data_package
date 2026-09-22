"""Maintenance script: refresh the OECD channel codelist and the multilateral
channel crosswalk.

Two independent jobs, run via subcommands:

``refresh-codelist``
    Rewrites ``src/oda_data/clean_data/crs_channel_mapping.csv`` from a current
    OECD channel codelist snapshot, and records that snapshot's vintage in a
    sidecar JSON file (``crs_channel_mapping_vintage.json``) next to it.

``review``
    Reads a CRS extract, aggregates it to (provider_code, agency_code) pairs
    carrying nonzero money, and compares those pairs against the curated
    ``src/oda_data/clean_data/multilateral_channel_crosswalk.csv``. Writes a
    review report of pairs that are new, whose provider/agency name changed,
    or that are still marked ``reviewed=false``. Each such pair gets a
    candidate channel code from ResolveKit's ``Resolver``. New pairs are
    appended to the crosswalk as ``method="resolvekit", reviewed=false`` rows
    so nothing is silently dropped -- but a row already marked
    ``reviewed=true`` is never touched, and an already-present
    ``reviewed=false`` row is reported again rather than being rewritten (so a
    reviewer's in-progress edits are never clobbered by a second run).

This script is deliberately kept outside ``src/oda_data``: ``resolvekit`` is a
maintenance-time-only tool (see ``[dependency-groups.maintenance]`` in
pyproject.toml, ``uv sync --group maintenance``, Python >=3.12 required) and
must never be importable from the package's own runtime code paths. Every
import of ``resolvekit`` or ``yaml`` in this module is lazy and guarded, so
``python -c "import scripts.refresh_channel_crosswalk"`` (or any static
analysis of it) does not itself require either package to be installed.

Nothing in this script makes a network call unless ``review --download`` is
passed explicitly; the default ``review`` path reads a local CRS extract via
``--crs-source``.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
CLEAN_DATA_DIR = REPO_ROOT / "src" / "oda_data" / "clean_data"
CRS_CHANNEL_MAPPING_CSV = CLEAN_DATA_DIR / "crs_channel_mapping.csv"
CRS_CHANNEL_MAPPING_VINTAGE_JSON = CLEAN_DATA_DIR / "crs_channel_mapping_vintage.json"
CROSSWALK_CSV = CLEAN_DATA_DIR / "multilateral_channel_crosswalk.csv"

CROSSWALK_COLUMNS: tuple[str, ...] = (
    "provider_code",
    "agency_code",
    "provider_name",
    "agency_name",
    "channel_code",
    "status",
    "reason",
    "method",
    "reviewed",
)


class MissingMaintenanceDependencyError(ImportError):
    """resolvekit (or a dependency of the maintenance path) is not installed."""


def _require_resolvekit() -> Any:
    """Import and return the ``resolvekit`` module, or raise a clear error.

    Guards the only import site of resolvekit in this repository. Nothing in
    ``src/oda_data`` may import it.
    """
    try:
        import resolvekit
    except ImportError as exc:
        raise MissingMaintenanceDependencyError(
            "resolvekit is required for this operation but is not installed. "
            "Install the maintenance dependency group with "
            "`uv sync --group maintenance` (requires Python >=3.12; it is "
            "never a runtime dependency of the oda_data package itself)."
        ) from exc
    return resolvekit


def _require_yaml() -> Any:
    """Import and return the ``yaml`` module, or raise a clear error."""
    try:
        import yaml
    except ImportError as exc:
        raise MissingMaintenanceDependencyError(
            "pyyaml is required to read the ResolveKit codelist snapshot but "
            "is not installed. Install it with `uv sync --group maintenance`."
        ) from exc
    return yaml


# ---------------------------------------------------------------------------
# 1. Channel codelist refresh
# ---------------------------------------------------------------------------


def _dedupe_channel_records(records: list[dict]) -> list[dict]:
    """Collapse duplicate ``code`` entries from the OECD codelist snapshot.

    The snapshot carries more than one record for a subset of codes: most are
    exact duplicates (re-parsed language rows); a handful genuinely differ
    (e.g. an old "donor country" phrasing next to the current "provider
    country" one). Keep, per code, the record with the longest ``name_en``
    (the more descriptive, typically more current OECD text), breaking ties
    in favour of a record whose ``name_fr`` is an actual translation rather
    than a copy of ``name_en``.

    Args:
        records: Raw ``channels:`` list from the codelist snapshot, each a
            dict with ``code``, ``name_en``, ``name_fr``, ``acronym``.

    Returns:
        One record per distinct ``code``, order not significant.
    """
    best: dict[str, dict] = {}
    for record in records:
        code = record["code"]
        current = best.get(code)
        if current is None:
            best[code] = record
            continue
        candidate_key = (
            len(record["name_en"]),
            record["name_fr"] != record["name_en"],
        )
        current_key = (
            len(current["name_en"]),
            current["name_fr"] != current["name_en"],
        )
        if candidate_key > current_key:
            best[code] = record
    return list(best.values())


def load_oecd_channel_codelist(
    resolvekit_source_path: Path,
) -> tuple[pd.DataFrame, dict]:
    """Load the OECD channel codelist from a local ResolveKit source checkout.

    Args:
        resolvekit_source_path: Path to a ResolveKit *git checkout* (the
            directory containing its ``pyproject.toml``), e.g.
            ``~/src/resolvekit``. The OECD codelist snapshot
            ResolveKit's builder uses
            (``src/resolvekit/builder/data/oecd_dac.yaml``) is source-only:
            it ships with neither the sdist nor the wheel on PyPI, so an
            installed ``resolvekit`` package cannot supply it -- a local
            checkout of the ResolveKit repository is required.

    Returns:
        A tuple of (channel codelist DataFrame with columns ``channel_code``,
        ``en_acronym``, ``fr_acronym``, ``channel_name``, sorted by
        ``channel_code``; a vintage metadata dict).

    Raises:
        FileNotFoundError: If the snapshot file is not found under
            ``resolvekit_source_path``.
    """
    yaml = _require_yaml()

    yaml_path = (
        resolvekit_source_path
        / "src"
        / "resolvekit"
        / "builder"
        / "data"
        / "oecd_dac.yaml"
    )
    if not yaml_path.is_file():
        raise FileNotFoundError(
            f"No OECD codelist snapshot found at {yaml_path}. Pass a "
            "ResolveKit git checkout containing "
            "src/resolvekit/builder/data/oecd_dac.yaml via "
            "--resolvekit-source."
        )

    with yaml_path.open(encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    channels = _dedupe_channel_records(raw["channels"])
    frame = (
        pd.DataFrame(
            {
                "channel_code": [int(c["code"]) for c in channels],
                "en_acronym": [c["acronym"] for c in channels],
                "fr_acronym": [c["acronym"] for c in channels],
                "channel_name": [c["name_en"].strip() for c in channels],
            }
        )
        .sort_values("channel_code")
        .reset_index(drop=True)
    )

    generated_from = raw.get("generated_from", {})
    vintage = {
        "source": "resolvekit_bundled_snapshot",
        "resolvekit_snapshot_version": raw.get("version"),
        "oecd_query_date": generated_from.get("oecd_query_date"),
        "oecd_source_url": generated_from.get("source_url"),
        "resolvekit_source_path": str(resolvekit_source_path),
        "refreshed_at": datetime.now(UTC).isoformat(),
        "channel_count": len(frame),
    }
    return frame, vintage


def refresh_crs_channel_mapping(
    resolvekit_source_path: Path,
    output_csv: Path = CRS_CHANNEL_MAPPING_CSV,
    vintage_json: Path = CRS_CHANNEL_MAPPING_VINTAGE_JSON,
) -> pd.DataFrame:
    """Rewrite ``crs_channel_mapping.csv`` from the OECD codelist and record
    its vintage in a sidecar JSON file.

    Args:
        resolvekit_source_path: ResolveKit git checkout, see
            :func:`load_oecd_channel_codelist`.
        output_csv: Where to write the refreshed mapping CSV.
        vintage_json: Where to write the vintage sidecar.

    Returns:
        The refreshed channel codelist DataFrame that was written.
    """
    frame, vintage = load_oecd_channel_codelist(resolvekit_source_path)
    frame.to_csv(output_csv, index=False, encoding="utf-8-sig")
    vintage_json.write_text(json.dumps(vintage, indent=2) + "\n", encoding="utf-8")
    return frame


# ---------------------------------------------------------------------------
# 2. Crosswalk review: propose codes for new / changed / unresolved pairs
# ---------------------------------------------------------------------------


def load_crosswalk(path: Path = CROSSWALK_CSV) -> pd.DataFrame:
    """Load the curated multilateral channel crosswalk with normalised dtypes."""
    df = pd.read_csv(
        path,
        dtype={
            "provider_code": "Int64",
            "agency_code": "Int64",
            "channel_code": "Int64",
        },
    )
    df["reviewed"] = df["reviewed"].astype(bool)
    return df


def load_crs_multilateral_pairs(
    multilateral_providers: Iterable[int],
    crs_source: Path | None = None,
    years: Iterable[int] | None = None,
) -> pd.DataFrame:
    """Aggregate CRS rows to (provider_code, agency_code) pairs with money.

    Args:
        multilateral_providers: Provider codes to restrict to (normally
            ``oda_data.provider_groupings()["multilateral"]``).
        crs_source: Path to a local CRS extract (parquet or csv, already in
            oda_data's cleaned column names -- i.e. what
            ``CRSData(...).read()`` returns) to read instead of downloading.
            When ``None``, downloads via oda_data's own bulk-download path
            (a network call).
        years: Years to filter to. ``None`` reads all years in the source.

    Returns:
        DataFrame with columns ``provider_code``, ``agency_code``,
        ``provider_name``, ``agency_name``, ``total_disbursement``, one row
        per pair, restricted to pairs whose total is nonzero and whose
        provider is in ``multilateral_providers``.
    """
    from oda_data.clean_data.schema import ODASchema

    if crs_source is not None:
        df = (
            pd.read_parquet(crs_source)
            if crs_source.suffix == ".parquet"
            else pd.read_csv(crs_source)
        )
    else:
        from oda_data.api.sources import CRSData

        crs = CRSData(years=list(years) if years else None)
        df = crs.read(using_bulk_download=True)

    providers = set(multilateral_providers)
    df = df[df[ODASchema.PROVIDER_CODE].isin(providers)]

    grouped = (
        df.groupby([ODASchema.PROVIDER_CODE, ODASchema.AGENCY_CODE], dropna=False)
        .agg(
            provider_name=(ODASchema.PROVIDER_NAME, "first"),
            agency_name=(ODASchema.AGENCY_NAME, "first"),
            total_disbursement=(ODASchema.USD_DISBURSEMENT, "sum"),
        )
        .reset_index()
        .rename(
            columns={
                ODASchema.PROVIDER_CODE: "provider_code",
                ODASchema.AGENCY_CODE: "agency_code",
            }
        )
    )
    return grouped[grouped["total_disbursement"] != 0].reset_index(drop=True)


def _dedupe_acronyms_for_resolver(mapping: pd.DataFrame) -> pd.DataFrame:
    """Null out acronym values that would collide inside ResolveKit's alias table.

    ResolveKit's ``Resolver.from_records`` mints one alias row per (entity,
    alias-kind, normalised value); it raises ``sqlite3.IntegrityError`` on a
    UNIQUE-constraint violation whenever a single channel's ``en_acronym``
    and ``fr_acronym`` normalise to the same value (a large share of the
    codelist's acronyms are untranslated and identical in both languages).
    Blank ``fr_acronym`` whenever it repeats ``en_acronym`` for that row (a
    near-duplicate that differs only by whitespace collides the same way, so
    the comparison is on the stripped value); the row's ``channel_name``
    still resolves it either way.

    Also blanks a full (en_acronym, fr_acronym) pair for the second and
    later channel that shares an identical pair with an earlier one, as a
    defensive measure against any other alias-uniqueness path ResolveKit may
    enforce (e.g. namespace-wide, not just per-entity).
    """
    mapping = mapping.copy()
    en = mapping["en_acronym"].fillna("").str.strip()
    fr = mapping["fr_acronym"].fillna("").str.strip()

    self_dupe = mapping["fr_acronym"].notna() & (en == fr)
    mapping.loc[self_dupe, "fr_acronym"] = None

    pair_key = en + "|" + fr
    has_acronym = mapping["en_acronym"].notna()
    cross_dupe = has_acronym & pair_key.duplicated()
    mapping.loc[cross_dupe, ["en_acronym", "fr_acronym"]] = None

    return mapping


def build_resolver(mapping: pd.DataFrame) -> Any:
    """Build a standalone ResolveKit ``Resolver`` over the channel codelist.

    Args:
        mapping: A channel codelist DataFrame as returned by
            :func:`load_oecd_channel_codelist` (or
            ``get_crs_official_mapping()``'s shape): ``channel_code``,
            ``en_acronym``, ``fr_acronym``, ``channel_name``.

    Returns:
        A ``resolvekit.Resolver`` resolving free-text agency/provider names
        to channel codes.
    """
    resolvekit = _require_resolvekit()
    mapping = _dedupe_acronyms_for_resolver(mapping)
    return resolvekit.Resolver.from_records(
        mapping,
        domain="custom",
        namespace="crs_channel",
        name="channel_name",
        id="channel_code",
        codes=["channel_code"],
        aliases=["en_acronym", "fr_acronym"],
        cache=False,
        warm=False,
    )


@dataclass(frozen=True)
class ProposedMatch:
    """One ResolveKit candidate proposal for a (provider, agency) pair."""

    provider_code: int
    agency_code: int | None
    provider_name: str
    agency_name: str | None
    query: str
    status: str
    candidate_channel_code: int | None
    confidence: float | None
    match_tier: str | None


def propose_channel_codes(pairs: pd.DataFrame, resolver: Any) -> list[ProposedMatch]:
    """Propose a channel code for each (provider, agency) pair via ResolveKit.

    Never auto-accepts: a ``RESOLVED`` result still returns as a proposal for
    a human to confirm, since ResolveKit resolving cleanly against the
    channel codelist is evidence, not the same thing as the money at that
    provider/agency actually belonging to that channel.

    Args:
        pairs: DataFrame with ``provider_code``, ``agency_code``,
            ``provider_name``, ``agency_name`` columns (as returned by
            :func:`load_crs_multilateral_pairs`).
        resolver: A ``resolvekit.Resolver`` as returned by
            :func:`build_resolver`.

    Returns:
        One :class:`ProposedMatch` per input row, in the same order.
    """
    results = []
    for row in pairs.itertuples(index=False):
        agency_name = getattr(row, "agency_name", None)
        query = (
            f"{row.provider_name} - {agency_name}"
            if isinstance(agency_name, str) and agency_name
            else row.provider_name
        )
        result = resolver.resolve(query, as_result=True)
        top_candidate = result.candidates[0] if result.candidates else None

        channel_code = None
        if result.status.value == "resolved" and result.entity_id:
            channel_code = int(result.entity_id.rsplit("/", 1)[-1])

        results.append(
            ProposedMatch(
                provider_code=int(row.provider_code),
                agency_code=(
                    int(row.agency_code) if pd.notna(row.agency_code) else None
                ),
                provider_name=row.provider_name,
                agency_name=agency_name if isinstance(agency_name, str) else None,
                query=query,
                status=result.status.value,
                candidate_channel_code=channel_code,
                confidence=(
                    top_candidate.confidence if top_candidate else result.confidence
                ),
                match_tier=(
                    top_candidate.match_tier.value
                    if top_candidate and top_candidate.match_tier
                    else None
                ),
            )
        )
    return results


def diff_crs_pairs_against_crosswalk(
    crs_pairs: pd.DataFrame, crosswalk: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Split CRS pairs into (new, changed, unresolved) relative to the crosswalk.

    - ``new``: (provider_code, agency_code) present in ``crs_pairs`` but
      absent from ``crosswalk`` entirely.
    - ``changed``: present in both, but ``provider_name`` or ``agency_name``
      differs between the CRS extract and the crosswalk (an OECD rename or a
      stale crosswalk entry).
    - ``unresolved``: present in both, and the crosswalk row is marked
      ``reviewed=False`` -- still needs a human decision.

    Args:
        crs_pairs: As returned by :func:`load_crs_multilateral_pairs`.
        crosswalk: As returned by :func:`load_crosswalk`.

    Returns:
        Three DataFrames (new, changed, unresolved), each carrying
        ``crs_pairs``' columns.
    """
    key = ["provider_code", "agency_code"]
    merged = crs_pairs.merge(
        crosswalk, on=key, how="left", suffixes=("", "_crosswalk"), indicator=True
    )

    new = merged[merged["_merge"] == "left_only"][crs_pairs.columns]
    matched = merged[merged["_merge"] == "both"]

    def _differs(left: pd.Series, right: pd.Series) -> pd.Series:
        # Null-safe: two missing names are not a "change". A plain `!=`
        # would flag every null-agency_name pair as changed, since NaN != NaN.
        both_null = left.isna() & right.isna()
        return ~both_null & (left != right)

    changed_mask = _differs(
        matched["provider_name"], matched["provider_name_crosswalk"]
    ) | _differs(matched["agency_name"], matched["agency_name_crosswalk"])
    changed = matched[changed_mask][crs_pairs.columns]

    unresolved = matched[matched["reviewed"] == False][crs_pairs.columns]  # noqa: E712

    return (
        new.reset_index(drop=True),
        changed.reset_index(drop=True),
        unresolved.reset_index(drop=True),
    )


def build_review_report(
    new: pd.DataFrame,
    changed: pd.DataFrame,
    unresolved: pd.DataFrame,
    proposals: list[ProposedMatch],
) -> pd.DataFrame:
    """Build the human-readable review report for a `review` run.

    Args:
        new, changed, unresolved: As returned by
            :func:`diff_crs_pairs_against_crosswalk`.
        proposals: As returned by :func:`propose_channel_codes`, covering the
            union of ``new``, ``changed`` and ``unresolved`` pairs.

    Returns:
        One row per (review_reason, pair), with the ResolveKit candidate
        attached. A pair appearing under more than one reason (e.g. both
        "changed" and "unresolved") gets one row per reason.
    """
    proposal_by_key = {(p.provider_code, p.agency_code): p for p in proposals}
    rows = []
    for reason, frame in (
        ("new", new),
        ("changed", changed),
        ("unresolved", unresolved),
    ):
        for row in frame.itertuples(index=False):
            agency_code = row.agency_code if pd.notna(row.agency_code) else None
            proposal = proposal_by_key.get((int(row.provider_code), agency_code))
            rows.append(
                {
                    "review_reason": reason,
                    "provider_code": row.provider_code,
                    "agency_code": agency_code,
                    "provider_name": row.provider_name,
                    "agency_name": row.agency_name,
                    "total_disbursement": getattr(row, "total_disbursement", None),
                    "candidate_channel_code": (
                        proposal.candidate_channel_code if proposal else None
                    ),
                    "confidence": proposal.confidence if proposal else None,
                    "match_tier": proposal.match_tier if proposal else None,
                    "resolution_status": proposal.status if proposal else None,
                }
            )
    return pd.DataFrame(
        rows,
        columns=[
            "review_reason",
            "provider_code",
            "agency_code",
            "provider_name",
            "agency_name",
            "total_disbursement",
            "candidate_channel_code",
            "confidence",
            "match_tier",
            "resolution_status",
        ],
    )


def upsert_new_pairs(
    crosswalk: pd.DataFrame,
    new: pd.DataFrame,
    proposals: list[ProposedMatch],
) -> pd.DataFrame:
    """Append newly-seen (provider, agency) pairs to the crosswalk as proposed rows.

    Never mutates an existing row: rows already in ``crosswalk`` (whether
    ``reviewed=True`` or ``reviewed=False``) pass through unchanged. Only
    pairs absent from the crosswalk entirely (``new``) are added, each as
    ``method="resolvekit", reviewed=False`` so a human must confirm them
    before they take effect in ``add_multilateral_channel_codes``' join (an
    unreviewed row's ``channel_code`` is exactly as usable as any other row
    there -- ``reviewed`` is an editorial-workflow marker, not a join
    condition).

    Args:
        crosswalk: As returned by :func:`load_crosswalk`.
        new: As returned by :func:`diff_crs_pairs_against_crosswalk`.
        proposals: As returned by :func:`propose_channel_codes`.

    Returns:
        The crosswalk with ``new``'s rows appended (unchanged if ``new`` is
        empty).
    """
    if new.empty:
        return crosswalk

    proposal_by_key = {(p.provider_code, p.agency_code): p for p in proposals}
    added_rows = []
    for row in new.itertuples(index=False):
        agency_code = row.agency_code if pd.notna(row.agency_code) else None
        proposal = proposal_by_key.get((int(row.provider_code), agency_code))
        has_candidate = (
            proposal is not None and proposal.candidate_channel_code is not None
        )
        added_rows.append(
            {
                "provider_code": int(row.provider_code),
                "agency_code": agency_code,
                "provider_name": row.provider_name,
                "agency_name": row.agency_name,
                "channel_code": proposal.candidate_channel_code
                if has_candidate
                else None,
                "status": "mapped" if has_candidate else "excluded",
                "reason": (
                    f"Auto-proposed by scripts/refresh_channel_crosswalk.py "
                    f"(resolvekit status={proposal.status}, "
                    f"confidence={proposal.confidence})"
                    if proposal
                    else "New CRS pair; ResolveKit found no candidate. Needs manual review."
                ),
                "method": "resolvekit",
                "reviewed": False,
            }
        )
    return pd.concat([crosswalk, pd.DataFrame(added_rows)], ignore_index=True)


def write_crosswalk(crosswalk: pd.DataFrame, output_csv: Path = CROSSWALK_CSV) -> None:
    """Write the crosswalk back to disk in its canonical column order."""
    crosswalk[list(CROSSWALK_COLUMNS)].to_csv(output_csv, index=False)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _cmd_refresh_codelist(args: argparse.Namespace) -> None:
    frame = refresh_crs_channel_mapping(
        resolvekit_source_path=Path(args.resolvekit_source),
        output_csv=Path(args.output),
        vintage_json=Path(args.vintage_json),
    )
    print(f"Wrote {len(frame)} channel codes to {args.output}")
    print(f"Vintage recorded at {args.vintage_json}")


def _cmd_review(args: argparse.Namespace) -> None:
    from oda_data import provider_groupings

    crosswalk = load_crosswalk(Path(args.crosswalk))
    mapping = pd.read_csv(Path(args.mapping), encoding="utf-8-sig")
    resolver = build_resolver(mapping)

    multilateral_providers = list(provider_groupings()["multilateral"])
    crs_pairs = load_crs_multilateral_pairs(
        multilateral_providers,
        crs_source=Path(args.crs_source) if args.crs_source else None,
        years=args.years,
    )

    new, changed, unresolved = diff_crs_pairs_against_crosswalk(crs_pairs, crosswalk)
    to_propose = pd.concat(
        [new, changed, unresolved], ignore_index=True
    ).drop_duplicates(subset=["provider_code", "agency_code"])
    proposals = propose_channel_codes(to_propose, resolver)

    report = build_review_report(new, changed, unresolved, proposals)
    report_path = Path(args.report)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report.to_csv(report_path, index=False)

    print(
        f"new={len(new)} changed={len(changed)} unresolved={len(unresolved)} "
        f"pairs, {len(proposals)} proposals written to {report_path}"
    )
    by_status = report["resolution_status"].value_counts(dropna=False).to_dict()
    print(f"resolution_status counts: {by_status}")

    if not args.dry_run:
        updated = upsert_new_pairs(crosswalk, new, proposals)
        write_crosswalk(updated, Path(args.crosswalk))
        print(f"Appended {len(new)} new pair(s) to {args.crosswalk} (reviewed=false)")
    else:
        print(f"Dry run: {args.crosswalk} not modified ({len(new)} new pair(s) found)")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    refresh_codelist = sub.add_parser(
        "refresh-codelist",
        help="Rewrite crs_channel_mapping.csv from a ResolveKit OECD codelist snapshot.",
    )
    refresh_codelist.add_argument(
        "--resolvekit-source",
        required=True,
        help="Path to a ResolveKit git checkout (contains pyproject.toml).",
    )
    refresh_codelist.add_argument("--output", default=str(CRS_CHANNEL_MAPPING_CSV))
    refresh_codelist.add_argument(
        "--vintage-json", default=str(CRS_CHANNEL_MAPPING_VINTAGE_JSON)
    )
    refresh_codelist.set_defaults(func=_cmd_refresh_codelist)

    review = sub.add_parser(
        "review",
        help=(
            "Propose channel codes for new/changed/unresolved CRS "
            "(provider, agency) pairs and write a review report."
        ),
    )
    review.add_argument("--crosswalk", default=str(CROSSWALK_CSV))
    review.add_argument(
        "--mapping",
        default=str(CRS_CHANNEL_MAPPING_CSV),
        help="Channel codelist to build the ResolveKit resolver from.",
    )
    review.add_argument(
        "--crs-source",
        default=None,
        help=(
            "Path to a local CRS extract (parquet/csv) to read instead of "
            "downloading. Omit only with --download."
        ),
    )
    review.add_argument(
        "--download",
        action="store_true",
        help=(
            "Allow downloading CRS via oda_data's bulk-download path "
            "(network call) when --crs-source is not given. Off by default."
        ),
    )
    review.add_argument("--years", nargs="*", type=int, default=None)
    review.add_argument(
        "--report",
        default=str(REPO_ROOT / "channel_crosswalk_review.csv"),
        help="Where to write the review report CSV.",
    )
    review.add_argument(
        "--dry-run",
        action="store_true",
        help="Write the report but do not modify the crosswalk CSV.",
    )
    review.set_defaults(func=_cmd_review)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if args.command == "review" and args.crs_source is None and not args.download:
        parser.error(
            "review requires --crs-source PATH (offline) or --download "
            "(explicit network call) -- refusing to guess."
        )

    args.func(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Tests for scripts/refresh_channel_crosswalk.py.

The script lives outside `src/oda_data` (it must never be importable from
package runtime code) and is loaded here by file path. No test in this
module makes a network call. Tests that need `resolvekit` itself (only the
`build_resolver` / `propose_channel_codes` path) skip when it is not
installed -- it lives in the `maintenance` dependency group, not the default
`dev`/`test` groups (`uv sync --group maintenance` to run them for real).
"""

import importlib.util
import json
import sys
from pathlib import Path

import pandas as pd
import pytest
import yaml

SCRIPT_PATH = (
    Path(__file__).resolve().parents[2] / "scripts" / "refresh_channel_crosswalk.py"
)
_HAS_RESOLVEKIT = importlib.util.find_spec("resolvekit") is not None


def _load_script_module():
    spec = importlib.util.spec_from_file_location(
        "refresh_channel_crosswalk", SCRIPT_PATH
    )
    module = importlib.util.module_from_spec(spec)
    # Register in sys.modules before exec: @dataclass looks up
    # sys.modules[cls.__module__] during class creation, which would
    # otherwise be None for a module loaded this way.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


script = _load_script_module()


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def codelist_snapshot(tmp_path: Path) -> Path:
    """A minimal ResolveKit-shaped OECD codelist snapshot on disk.

    Includes one clean-duplicate code (44002, identical rows), one code with
    a genuinely differing pair (11000, "donor" vs "provider" phrasing) to
    exercise the dedupe tie-break, and one unique code.
    """
    checkout = tmp_path / "resolvekit"
    data_dir = checkout / "src" / "resolvekit" / "builder" / "data"
    data_dir.mkdir(parents=True)

    snapshot = {
        "version": 1,
        "generated_from": {
            "oecd_query_date": "2026-06-15",
            "source_url": "https://development-finance-codelists.oecd.org/CodesList.aspx",
        },
        "channels": [
            {"code": "44002", "name_en": "IDA", "name_fr": "IDA", "acronym": "IDA"},
            {"code": "44002", "name_en": "IDA", "name_fr": "IDA", "acronym": "IDA"},
            {
                "code": "11000",
                "name_en": "Donor Government",
                "name_fr": "Gouvernement du donneur",
                "acronym": None,
            },
            {
                "code": "11000",
                "name_en": "Provider Government",
                "name_fr": "Provider Government",
                "acronym": None,
            },
            {
                "code": "46015",
                "name_en": "European Bank for Reconstruction and Development",
                "name_fr": "Banque européenne pour la reconstruction et le développement",
                "acronym": "EBRD",
            },
        ],
    }
    (data_dir / "oecd_dac.yaml").write_text(yaml.safe_dump(snapshot), encoding="utf-8")
    return checkout


@pytest.fixture
def crosswalk_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "provider_code": [905, 918, 999],
            "agency_code": [1, 5, None],
            "provider_name": ["IDA", "EU Institutions", "New Provider"],
            "agency_name": ["IDA", "MFA", None],
            "channel_code": [44002, None, None],
            "status": ["mapped", "excluded", "excluded"],
            "reason": [None, "borrowing-funded", "still needs review"],
            "method": ["exact", "manual", "proposed"],
            "reviewed": [True, True, False],
        }
    )


@pytest.fixture
def crs_pairs_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "provider_code": [905, 918, 999, 1040],
            "agency_code": [1, 5, None, 3],
            "provider_name": ["IDA", "EU Institutions", "New Provider", "UNODC"],
            "agency_name": ["IDA", "MFA", None, "Regular budget"],
            "total_disbursement": [1000.0, 30.0, 5.0, 12.0],
        }
    )


# ============================================================================
# Channel codelist refresh
# ============================================================================


class TestDedupeChannelRecords:
    def test_collapses_exact_duplicates(self):
        records = [
            {"code": "1", "name_en": "A", "name_fr": "A", "acronym": None},
            {"code": "1", "name_en": "A", "name_fr": "A", "acronym": None},
        ]
        result = script._dedupe_channel_records(records)
        assert len(result) == 1

    def test_prefers_longer_name_on_real_difference(self):
        records = [
            {
                "code": "11000",
                "name_en": "Donor Government",
                "name_fr": "x",
                "acronym": None,
            },
            {
                "code": "11000",
                "name_en": "Provider Government",
                "name_fr": "y",
                "acronym": None,
            },
        ]
        result = script._dedupe_channel_records(records)
        assert len(result) == 1
        assert result[0]["name_en"] == "Provider Government"

    def test_tie_break_prefers_real_translation(self):
        records = [
            {
                "code": "1",
                "name_en": "New Development Bank",
                "name_fr": "New Development Bank",
                "acronym": "NDB",
            },
            {
                "code": "1",
                "name_en": "New Development Bank",
                "name_fr": "Nouvelle banque",
                "acronym": "NDB",
            },
        ]
        result = script._dedupe_channel_records(records)
        assert len(result) == 1
        assert result[0]["name_fr"] == "Nouvelle banque"

    def test_distinct_codes_all_kept(self):
        records = [
            {"code": "1", "name_en": "A", "name_fr": "A", "acronym": None},
            {"code": "2", "name_en": "B", "name_fr": "B", "acronym": None},
        ]
        result = script._dedupe_channel_records(records)
        assert {r["code"] for r in result} == {"1", "2"}


class TestLoadOecdChannelCodelist:
    def test_returns_one_row_per_code_and_vintage(self, codelist_snapshot: Path):
        frame, vintage = script.load_oecd_channel_codelist(codelist_snapshot)

        assert sorted(frame["channel_code"]) == [11000, 44002, 46015]
        assert frame.loc[frame["channel_code"] == 11000, "channel_name"].iloc[0] == (
            "Provider Government"
        )
        assert vintage["oecd_query_date"] == "2026-06-15"
        assert vintage["channel_count"] == 3
        assert vintage["resolvekit_source_path"] == str(codelist_snapshot)

    def test_acronym_columns_mirror_the_single_snapshot_acronym(
        self, codelist_snapshot: Path
    ):
        frame, _ = script.load_oecd_channel_codelist(codelist_snapshot)
        row = frame.loc[frame["channel_code"] == 46015].iloc[0]
        assert row["en_acronym"] == row["fr_acronym"] == "EBRD"

    def test_missing_snapshot_raises_file_not_found(self, tmp_path: Path):
        with pytest.raises(FileNotFoundError):
            script.load_oecd_channel_codelist(tmp_path / "does_not_exist")


class TestRefreshCrsChannelMapping:
    def test_writes_csv_and_vintage_sidecar(
        self, codelist_snapshot: Path, tmp_path: Path
    ):
        output_csv = tmp_path / "crs_channel_mapping.csv"
        vintage_json = tmp_path / "crs_channel_mapping_vintage.json"

        script.refresh_crs_channel_mapping(
            resolvekit_source_path=codelist_snapshot,
            output_csv=output_csv,
            vintage_json=vintage_json,
        )

        written = pd.read_csv(output_csv, encoding="utf-8-sig")
        assert list(written.columns) == [
            "channel_code",
            "en_acronym",
            "fr_acronym",
            "channel_name",
        ]
        assert len(written) == 3

        vintage = json.loads(vintage_json.read_text(encoding="utf-8"))
        assert vintage["channel_count"] == 3


# ============================================================================
# Crosswalk loading and CRS pair aggregation
# ============================================================================


class TestLoadCrosswalk:
    def test_normalises_dtypes(self, tmp_path: Path, crosswalk_df: pd.DataFrame):
        path = tmp_path / "crosswalk.csv"
        crosswalk_df.to_csv(path, index=False)

        result = script.load_crosswalk(path)

        assert result["reviewed"].dtype == bool
        assert str(result["provider_code"].dtype) == "Int64"
        assert str(result["agency_code"].dtype) == "Int64"


class TestLoadCrsMultilateralPairs:
    def test_aggregates_and_filters_to_multilateral_providers_with_money(
        self, tmp_path: Path
    ):
        from oda_data.clean_data.schema import ODASchema

        raw = pd.DataFrame(
            {
                ODASchema.PROVIDER_CODE: [905, 905, 918, 1],
                ODASchema.AGENCY_CODE: [1, 1, 5, 1],
                ODASchema.PROVIDER_NAME: ["IDA", "IDA", "EU Institutions", "Bilateral"],
                ODASchema.AGENCY_NAME: ["IDA", "IDA", "MFA", "Bilateral"],
                ODASchema.USD_DISBURSEMENT: [100.0, 50.0, 0.0, 999.0],
            }
        )
        source = tmp_path / "crs.parquet"
        raw.to_parquet(source)

        result = script.load_crs_multilateral_pairs(
            multilateral_providers=[905, 918], crs_source=source
        )

        assert set(
            zip(result["provider_code"], result["agency_code"], strict=True)
        ) == {(905, 1)}
        assert result.loc[0, "total_disbursement"] == 150.0

    def test_zero_total_pair_is_dropped(self, tmp_path: Path):
        from oda_data.clean_data.schema import ODASchema

        raw = pd.DataFrame(
            {
                ODASchema.PROVIDER_CODE: [918],
                ODASchema.AGENCY_CODE: [4],
                ODASchema.PROVIDER_NAME: ["EU Institutions"],
                ODASchema.AGENCY_NAME: ["ECHO"],
                ODASchema.USD_DISBURSEMENT: [0.0],
            }
        )
        source = tmp_path / "crs.parquet"
        raw.to_parquet(source)

        result = script.load_crs_multilateral_pairs(
            multilateral_providers=[918], crs_source=source
        )

        assert result.empty


# ============================================================================
# Diffing CRS pairs against the crosswalk
# ============================================================================


class TestDiffCrsPairsAgainstCrosswalk:
    def test_classifies_new_changed_and_unresolved(
        self, crs_pairs_df: pd.DataFrame, crosswalk_df: pd.DataFrame
    ):
        new, changed, unresolved = script.diff_crs_pairs_against_crosswalk(
            crs_pairs_df, crosswalk_df
        )

        # 1040/3 (UNODC) is absent from the crosswalk fixture entirely.
        assert set(zip(new["provider_code"], new["agency_code"], strict=True)) == {
            (1040, 3)
        }

        # 999/<null> is present but the crosswalk still marks it unreviewed.
        assert len(unresolved) == 1
        assert unresolved["provider_code"].iloc[0] == 999
        assert pd.isna(unresolved["agency_code"].iloc[0])

        # Nothing in this fixture has a renamed provider/agency.
        assert changed.empty

    def test_renamed_pair_is_flagged_changed(self, crosswalk_df: pd.DataFrame):
        pairs = pd.DataFrame(
            {
                "provider_code": [905],
                "agency_code": [1],
                "provider_name": ["International Development Association"],
                "agency_name": ["IDA"],
                "total_disbursement": [1000.0],
            }
        )
        new, changed, unresolved = script.diff_crs_pairs_against_crosswalk(
            pairs, crosswalk_df
        )

        assert new.empty
        assert unresolved.empty
        assert list(changed["provider_code"]) == [905]


# ============================================================================
# Review report and crosswalk upsert (no resolvekit needed -- proposals are
# passed in directly)
# ============================================================================


class TestBuildReviewReportAndUpsert:
    def _proposal(self, **overrides):
        defaults = {
            "provider_code": 1040,
            "agency_code": 3,
            "provider_name": "UNODC",
            "agency_name": "Regular budget",
            "query": "UNODC - Regular budget",
            "status": "ambiguous",
            "candidate_channel_code": 41128,
            "confidence": 0.7,
            "match_tier": "fuzzy",
        }
        defaults.update(overrides)
        return script.ProposedMatch(**defaults)

    def test_report_has_one_row_per_reason_and_pair(self, crs_pairs_df, crosswalk_df):
        new, changed, unresolved = script.diff_crs_pairs_against_crosswalk(
            crs_pairs_df, crosswalk_df
        )
        proposals = [self._proposal()]

        report = script.build_review_report(new, changed, unresolved, proposals)

        new_row = report[report["review_reason"] == "new"].iloc[0]
        assert new_row["provider_code"] == 1040
        assert new_row["candidate_channel_code"] == 41128
        assert new_row["confidence"] == 0.7

    def test_upsert_appends_new_pairs_as_unreviewed_resolvekit_rows(
        self, crs_pairs_df, crosswalk_df
    ):
        new, _changed, _unresolved = script.diff_crs_pairs_against_crosswalk(
            crs_pairs_df, crosswalk_df
        )
        proposals = [self._proposal()]

        updated = script.upsert_new_pairs(crosswalk_df, new, proposals)

        added = updated[updated["provider_code"] == 1040].iloc[0]
        assert added["channel_code"] == 41128
        assert added["status"] == "mapped"
        assert added["method"] == "resolvekit"
        assert added["reviewed"] == False  # noqa: E712 -- numpy bool from a pandas row

    def test_upsert_never_touches_existing_rows(self, crs_pairs_df, crosswalk_df):
        new, _changed, _unresolved = script.diff_crs_pairs_against_crosswalk(
            crs_pairs_df, crosswalk_df
        )
        proposals = [self._proposal()]

        updated = script.upsert_new_pairs(crosswalk_df, new, proposals)

        original_rows = updated[updated["provider_code"] != 1040].reset_index(drop=True)
        pd.testing.assert_frame_equal(
            original_rows, crosswalk_df.reset_index(drop=True), check_dtype=False
        )

    def test_upsert_with_no_new_pairs_is_a_no_op(self, crosswalk_df):
        empty = pd.DataFrame(
            columns=["provider_code", "agency_code", "provider_name", "agency_name"]
        )
        result = script.upsert_new_pairs(crosswalk_df, empty, proposals=[])
        assert result is crosswalk_df

    def test_new_pair_with_no_candidate_is_excluded_not_silently_mapped(
        self, crs_pairs_df, crosswalk_df
    ):
        new, _changed, _unresolved = script.diff_crs_pairs_against_crosswalk(
            crs_pairs_df, crosswalk_df
        )
        # No proposal at all for the new (1040, 3) pair.
        updated = script.upsert_new_pairs(crosswalk_df, new, proposals=[])

        added = updated[updated["provider_code"] == 1040].iloc[0]
        assert pd.isna(added["channel_code"])
        assert added["status"] == "excluded"
        assert added["reviewed"] == False  # noqa: E712 -- numpy bool from a pandas row


class TestWriteCrosswalk:
    def test_writes_columns_in_canonical_order(
        self, tmp_path: Path, crosswalk_df: pd.DataFrame
    ):
        # Shuffle columns to confirm write_crosswalk restores canonical order.
        shuffled = crosswalk_df[list(reversed(crosswalk_df.columns))]
        output = tmp_path / "crosswalk.csv"

        script.write_crosswalk(shuffled, output)

        written = pd.read_csv(output)
        assert list(written.columns) == list(script.CROSSWALK_COLUMNS)


# ============================================================================
# The real, committed crosswalk / proxy CSVs
# ============================================================================


class TestCommittedCrosswalkAndProxyFiles:
    def test_crosswalk_channel_codes_exist_in_the_refreshed_mapping(self):
        from oda_data.config import ODAPaths

        mapping = pd.read_csv(
            ODAPaths.cleaning / "crs_channel_mapping.csv", encoding="utf-8-sig"
        )
        crosswalk = pd.read_csv(
            ODAPaths.cleaning / "multilateral_channel_crosswalk.csv"
        )

        mapped_codes = set(
            crosswalk.loc[crosswalk["status"] == "mapped", "channel_code"].dropna()
        )
        assert mapped_codes.issubset(set(mapping["channel_code"]))

    def test_crosswalk_unreviewed_rows_are_method_proposed_or_resolvekit(self):
        from oda_data.config import ODAPaths

        crosswalk = pd.read_csv(
            ODAPaths.cleaning / "multilateral_channel_crosswalk.csv"
        )
        unreviewed = crosswalk[~crosswalk["reviewed"]]

        assert unreviewed["method"].isin(["proposed", "resolvekit"]).all()

    def test_proxy_table_columns_and_codes(self):
        from oda_data.config import ODAPaths

        mapping = pd.read_csv(
            ODAPaths.cleaning / "crs_channel_mapping.csv", encoding="utf-8-sig"
        )
        proxies = pd.read_csv(ODAPaths.cleaning / "channel_share_proxies.csv")

        expected_columns = {
            "channel_code",
            "channel_name",
            "proxy_channel_code",
            "proxy_type",
            "fixed_purpose_code",
            "rationale",
            "reviewed",
        }
        assert expected_columns.issubset(proxies.columns)
        assert (
            proxies["proxy_type"].isin(["parent_fund", "peer", "fixed_purpose"]).all()
        )

        fixed_purpose = proxies[proxies["proxy_type"] == "fixed_purpose"]
        assert fixed_purpose["fixed_purpose_code"].notna().all()
        assert fixed_purpose["proxy_channel_code"].isna().all()

        non_fixed = proxies[proxies["proxy_type"] != "fixed_purpose"]
        assert non_fixed["proxy_channel_code"].notna().all()
        assert set(non_fixed["proxy_channel_code"].astype(int)).issubset(
            set(mapping["channel_code"])
        )
        assert set(proxies["channel_code"].astype(int)).issubset(
            set(mapping["channel_code"])
        )

    def test_every_peer_row_carries_a_rationale(self):
        from oda_data.config import ODAPaths

        proxies = pd.read_csv(ODAPaths.cleaning / "channel_share_proxies.csv")
        peers = proxies[proxies["proxy_type"] == "peer"]
        assert (peers["rationale"].str.len() > 0).all()


# ============================================================================
# CLI argument handling (no resolvekit needed)
# ============================================================================


class TestCli:
    def test_review_requires_crs_source_or_download(self):
        with pytest.raises(SystemExit):
            script.main(["review", "--dry-run"])

    def test_refresh_codelist_requires_resolvekit_source(self):
        with pytest.raises(SystemExit):
            script.main(["refresh-codelist"])


# ============================================================================
# resolvekit-dependent paths -- skipped unless the maintenance group is
# installed (`uv sync --group maintenance`)
# ============================================================================


@pytest.fixture
def small_mapping_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "channel_code": [44002, 46015, 41128],
            "en_acronym": ["IDA", "EBRD", "UNODC"],
            "fr_acronym": ["IDA", "EBRD", "UNODC"],
            "channel_name": [
                "International Development Association",
                "European Bank for Reconstruction and Development",
                "United Nations Office on Drugs and Crime",
            ],
        }
    )


@pytest.mark.skipif(not _HAS_RESOLVEKIT, reason="maintenance-only dependency group")
class TestBuildResolverAndPropose:
    def test_resolves_a_clean_acronym_match(self, small_mapping_df: pd.DataFrame):
        resolver = script.build_resolver(small_mapping_df)
        pairs = pd.DataFrame(
            {
                "provider_code": [905],
                "agency_code": [1],
                "provider_name": ["IDA"],
                "agency_name": [None],
            }
        )

        proposals = script.propose_channel_codes(pairs, resolver)

        assert len(proposals) == 1
        assert proposals[0].status == "resolved"
        assert proposals[0].candidate_channel_code == 44002

    def test_deduped_acronyms_do_not_raise_on_build(self):
        # Two channels sharing an identical (en_acronym, fr_acronym) pair
        # must not raise sqlite3.IntegrityError (research-resolvekit.md).
        mapping = pd.DataFrame(
            {
                "channel_code": [1, 2],
                "en_acronym": ["AFC", "AFC"],
                "fr_acronym": ["AFC", "AFC"],
                "channel_name": ["Channel One", "Channel Two"],
            }
        )
        resolver = script.build_resolver(mapping)
        assert resolver is not None

# Changelog

All notable changes to the oda_data package will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.8.0] - 2026-09-22

This release rebuilds the imputed multilateral pipeline (`imputed_multilateral_by_purpose`, `multilateral_spending_shares_by_channel_and_purpose_smoothed`, `spending_by_purpose`) end to end. Every core contribution now ends up in exactly one output row: imputed against a channel's own CRS shares, a lapsed reporter's stale shares, a reviewed proxy channel's shares, or explicitly `unallocated`, instead of some being silently dropped or misattributed. See [Sector Imputations](https://oneecampaign.github.io/oda_data_package/sector-imputations/) for the full methodology, and [Imputation Delta](https://oneecampaign.github.io/oda_data_package/imputation-delta/) for a script that measures the change by year and donor. Closes #162, #163, #165.

### Added

- **`imputed_multilateral_by_purpose` output gains three columns**: `allocation_status` (one of `imputed`, `stale_share`, `proxy`, `unallocated`), `share_channel_code` (the channel whose CRS shares were actually used, the proxy's code for a `proxy` row, null for `unallocated`), and `share_years` (the `"YYYY-YYYY"` CRS years behind the share, null for `unallocated`). `recipient_code`, `purpose_code` and `share_channel_code` are now pandas nullable `Int64` rather than plain `int`/`float64`, so an `unallocated` row's nulls don't silently change a column's dtype for a downstream `merge`.
- **A reviewed provider/agency-to-channel crosswalk** (`src/oda_data/clean_data/multilateral_channel_crosswalk.csv`) and a proxy-share table (`src/oda_data/clean_data/channel_share_proxies.csv`), both with a `rationale` column and `reviewed=true`, replace fuzzy/regex name matching for CRS-to-channel resolution. The proxy table covers two documented judgement calls, most notably IDA's Multilateral Debt Relief Initiative, whose $7.08 billion of core contributions (2015-2024) is spread across IDA's ordinary lending mix rather than shown as debt relief; see Sector Imputations for the full reasoning.
- **A stale-share fallback for lapsed reporters** (`max_share_age`, default 5 years): a channel with no CRS rows in the current rolling window falls back to its most recent window with data, marked `allocation_status="stale_share"`, instead of contributing an incomplete or missing window.
- **`imputation_quality_report`** (`oda_data.indicators.research.imputation_quality`): money and share by `allocation_status`, staleness per channel-year, single-purpose concentration flagging (the pattern seen in the EU's Macro-Financial Assistance instrument and the Islamic Development Bank's ITFC fold), duplicate rows, negative values and crosswalk misses.
- **`result.attrs["provenance"]`** on every `imputed_multilateral_by_purpose` result: upstream CRS/Multisystem release identity, crosswalk/proxy-table file vintage, package version and call parameters. Most pandas operations (including some `groupby`/`merge` paths) drop `.attrs`; read it from the returned frame before further transformation.
- **`flow_types` parameter** (`tuple[str, ...]`, any of `"ODA"`, `"OOF"`, `"PSI"`) on `spending_by_purpose` and the shares/imputation functions, replacing the CRS category filter previously hidden inside `oda_only`/`shares_based_on_oda_only`.
- **`add_multilateral_channel_codes(df, on_unmapped="raise" | "unallocated", crosswalk=None)`** (`oda_data.clean_data.channels`): an exact join on `(provider_code, agency_code)` against the reviewed crosswalk. `on_unmapped="raise"` (the default) raises `UnmappedChannelError`, naming the unmapped pairs, their names and the money at stake, so a new OECD agency surfaces as a loud failure instead of a silently wrong allocation.
- **`scripts/refresh_channel_crosswalk.py`** (maintenance script, `uv sync --group maintenance` for its `resolvekit`/`pyyaml` dependencies, never imported at runtime): refreshes `crs_channel_mapping.csv` from a current OECD channel codelist snapshot (305 to 446 codes) and proposes crosswalk rows for new or changed `(provider_code, agency_code)` pairs for review, without overwriting already-reviewed rows.
- **`scripts/imputation_delta.py`**: compares this release's `imputed_multilateral_by_purpose` output against a previous release's, on the same pinned years/providers and cache, and writes a year-by-donor delta table attributed to named causes (the `bi_multi` core-contribution exclusion, the stale-share window padding, the `flow_types` basis change, and money now visible as `unallocated`/`proxy`) plus a channel-remapping diagnostic. See Imputation Delta for usage.
- **Upstream release-identity detection in the bulk cache** (#162): `oda_data.cache.release_info(dataset)` reports the OECD release a cached result reflects; `BulkCacheManager` compares a cheap upstream release probe against the cached manifest so a same-day OECD republish inside the cache's TTL window is detected instead of served stale. `refresh: bool = False` is threaded through `imputed_multilateral_by_purpose`/`core_multilateral_contributions_by_provider`/`spending_by_purpose`/the shares functions down to the underlying reads, so a caller can force a re-download without reaching into `DACSource.memory_cache` directly.
- **Per-column dtype fallback** in `set_default_types` (`clean_data/dtypes.py`): a column that can't be cast to its expected dtype (e.g. Multisystem's alphanumeric `flow_code` values like `"E01"`, against `CRSData`'s numeric `flow_code` schema entry) falls back to a string dtype with a logged warning, instead of raising and failing the whole read.

### Changed

- **Default share basis for `multilateral_spending_shares_by_channel_and_purpose_smoothed` / `imputed_multilateral_by_purpose` is now `flow_types=("ODA", "OOF")`** (CRS categories 10 and 21), reproducing the discontinued OECD sectoral-imputation practice. This is deliberately broader than ODA alone: several multilateral channels report only OOF to the CRS (IBRD, channel 44001, $7.07 billion of core contributions 2015-2024; EBRD; IFC; IDB Invest), and would otherwise get an empty share pool and fall entirely to `unallocated` under an ODA-only filter. Pass `flow_types=("ODA",)` for the stricter basis. `spending_by_purpose`'s own default (used for direct bilateral CRS analysis, not the imputation shares) is `flow_types=("ODA",)`, category 10 alone. Its previous default, `oda_only=False`, applied no category filter, so a call that passes no flow argument now returns ODA totals where it returned totals across every CRS category.
- **`oda_only`/`shares_based_on_oda_only` are deprecated** in favour of `flow_types` and emit a `DeprecationWarning` on use, naming what the old values actually filtered: `oda_only=True`/`shares_based_on_oda_only=True` selected CRS category `in (10, 60)`, ODA **and** Private Sector Instruments, not ODA alone, despite the parameter name and docstring (#165). `oda_only=False`/`shares_based_on_oda_only=False` (the previous default) applied **no CRS category filter at all**: OOF, export credits and every other flow category included alongside ODA. Neither old branch maps exactly onto a `flow_types` value.
- **New parameters on the imputation functions are keyword-only.** `oda_only` and `shares_based_on_oda_only` keep their positions from 2.7, so existing positional calls still work (with a `DeprecationWarning`). `flow_types`, `exclude_multilateral_core` on `spending_by_purpose`, `period_length` on `imputed_multilateral_by_purpose`, `max_share_age`, `use_proxy_shares`, `crs`, `multisystem` and `refresh` must be passed by name.
- **A caller-supplied `crs` or `multisystem` frame is filtered as a live read would be.** `spending_by_purpose(crs=...)` applies `years`, `providers`, the flow-type filter and the `bi_multi == 2` exclusion, and raises `ValueError` when the frame has no `bi_multi` column unless `exclude_multilateral_core=False` is passed. `core_multilateral_contributions_by_provider(multisystem=...)` applies `years`, `providers` and `channels`, plus the measure's flow type and current prices where the frame has `flow_type` and `amount_type` columns. Provenance records a supplied frame's release as `"supplied"`.
- **`add_multilateral_channel_codes` ignores crosswalk rows marked `reviewed=false`**, logging how many it skipped, so a pair proposed by `scripts/refresh_channel_crosswalk.py` raises `UnmappedChannelError` until a human marks it reviewed. A crosswalk without a `reviewed` column is refused with `ValueError`.
- **`add_multi_channel_codes` is now a deprecated alias of `add_multilateral_channel_codes`, and its behaviour changed**: it now raises `UnmappedChannelError` by default for an unmapped pair carrying nonzero value, instead of silently guessing a channel via fuzzy/regex name matching. **This is a breaking change for any caller that relied on the old silent-guess behaviour**; `climate-finance-package` is a known caller. Pass `on_unmapped="unallocated"` to keep the old "never raise" shape (with a null `channel_code` instead of a guessed one).
- **`oda-reader` floor raised to `>=1.9.0`** (from `>=1.6.0`): oda-reader 1.6.0's `bulk_download_crs()`/`bulk_download_multisystem()` both fail with `BulkDownloadHTTPError: HTTP 400`, since OECD moved bulk files off the `stats.oecd.org/wbos/fileview2.aspx?IDFile=<GUID>` scheme those versions parse; the fix shipped in oda-reader 1.8.0.
- The `ONE.P.40.T.T.S_M` indicator description (`indicators/crs/one.py`/`multilateral_spending_by_purpose_shares`) is reconciled with what it actually computes (ODA and OOF shares, not ODA alone).

### Removed

- **The fuzzy/regex channel-name matcher and the `thefuzz` dependency.** CRS-to-channel resolution is now an exact join against the reviewed crosswalk (see Added, above). This also removes `export_missing_path`, the CSV export of unmatched provider/agency names, closing #163 (values from OECD data written unescaped could be interpreted as spreadsheet formulas by Excel/Numbers/Google Sheets) by removing the code path entirely rather than escaping it.

## [2.7.1] - 2026-09-22

### Fixed

- **`CRSData` no longer double counts donor core contributions to multilateral
  organizations.** The OECD's 9 April 2026 CRS changelog added rows for donor
  core contributions to multilateral organizations (`bi_multi == 2`) alongside
  the pre-existing bilateral rows, back-filled to 2005, and stated that these
  rows "must be excluded" to correctly calculate bilateral flows. `CRSData`
  never filtered on `bi_multi`, so every CRS-derived aggregate, including the
  `CRS.*` indicator catalogue and `spending_by_purpose`, counted these rows
  twice: once as a bilateral CRS row, once again when
  `imputed_multilateral_by_purpose` redistributes the same core contribution
  across recipients via MultiSystem data. On the cache used to measure this,
  `CRS.P.10` (Bilateral ODA, all donors, 2023, current USD) was inflated by
  $62.6 billion (17.7%): Germany +35.5%, France +37.3%, UK +35.7%, USA +8.1%.
  `CRSData` now excludes `bi_multi == 2` rows by default, on every read path
  (bulk parquet, the on-disk query cache, the in-memory cache, and the API
  `download()` path); rows with a missing `bi_multi` value are kept. Pass
  `CRSData(..., exclude_multilateral_core=False)`, or
  `spending_by_purpose(..., exclude_multilateral_core=False)`, to opt back
  into the raw totals. Cached results computed under the old, uncorrected
  default are not reused under the new default, since the exclusion flag is
  part of the cache key.

## [2.7.0] - 2026-06-15

This release refreshes the DAC1 indicator catalogue to match the current
OECD DAC1 flow-type classification. A number of aid types were reassigned to
different flow types upstream, which changes their indicator codes (the
`DAC1.<flowtype>.<aidtype>` middle segment). The underlying data is unchanged —
only the codes used to address it. If you reference any of the codes below
directly, update them.

This also fixes a bug that made the DAC1 indicator generator unrunnable
(`dac1_aid_flow_type_mapping()` read a non-existent `flow_type` key instead of
`flowtype_code`), and adds test coverage for the previously-untested generator.

### Changed

- **17 DAC1 indicator codes changed** (same data, new flow-type segment):

  | Old code        | New code         | Indicator                                             |
  | --------------- | ---------------- | ----------------------------------------------------- |
  | `DAC1.50.5`     | `DAC1.5.5`       | Official and private flows                            |
  | `DAC1.37.415`   | `DAC1.30.415`    | (reassigned to Private development finance)           |
  | `DAC1.50.420`   | `DAC1.30.420`    | (reassigned to Private development finance)           |
  | `DAC1.50.425`   | `DAC1.30.425`    | (reassigned to Private development finance)           |
  | `DAC1.50.3300`  | `DAC1.37.3300`   | (reassigned to Other private market)                  |
  | `DAC1.50.3320`  | `DAC1.37.3320`   | (reassigned to Other private market)                  |
  | `DAC1.50.3530`  | `DAC1.37.3530`   | (reassigned to Other private market)                  |
  | `DAC1.50.359`   | `DAC1.37.359`    | (reassigned to Other private market)                  |
  | `DAC1.50.3840`  | `DAC1.37.3840`   | (reassigned to Other private market)                  |
  | `DAC1.50.3860`  | `DAC1.37.3860`   | (reassigned to Other private market)                  |
  | `DAC1.50.3890`  | `DAC1.37.3890`   | (reassigned to Other private market)                  |
  | `DAC1.50.7530`  | `DAC1.37.7530`   | (reassigned to Other private market)                  |
  | `DAC1.10.11002` | `DAC1.40.11002`  | (reassigned to Non flow)                              |
  | `DAC1.50.2231`  | `DAC1.1021.2231` | Memo: development finance in blended finance packages |
  | `DAC1.50.2232`  | `DAC1.1021.2232` | Memo: … through funds and facilities                  |
  | `DAC1.50.2233`  | `DAC1.1021.2233` | Memo: amounts mobilised from the private sector       |
  | `DAC1.50.2234`  | `DAC1.1021.2234` | Memo: … of which through guarantees                   |

- Two flow types added to the DAC1 flow-type vocabulary: `5`
  ("Total official and private flows") and `1021`
  ("Memo items (mobilisation and blended finance)").

### Added

- Two new DAC1 indicators: `DAC1.10.1623` (Debt buybacks) and `DAC1.37.1030`
  (Offsetting entry for debt relief — private claims, principal).
- Test coverage for the DAC1 indicator generator and its mapping loaders.

### Fixed

- `dac1_aid_flow_type_mapping()` raised `KeyError` (read `flow_type` instead of
  `flowtype_code`), which broke DAC1 indicator regeneration.

## [2.6.0] - 2026-04-28

This release reorganises how `oda_data` stores downloaded data on disk.
Caches now live in a standard per-user location instead of inside your
project folder, and a new `oda_data.cache.*` namespace gives you a clear,
typed way to inspect and manage them. Existing caches are migrated
automatically on first run. See [CACHING.md](CACHING.md) for the full
walkthrough.

### Added

- **Per-user cache by default.** Downloads now live under your OS's standard
  cache directory (`~/Library/Caches/oda-data/` on macOS, `~/.cache/oda-data/`
  on Linux, `%LOCALAPPDATA%\oda-data\` on Windows), versioned per release.
  This means cache is shared across projects instead of duplicated in every
  `.raw_data/` folder, and old versions don't silently get reused after an
  upgrade.

- **Override the cache location** with `oda_data.set_cache_root(path)` or the
  `ODA_DATA_CACHE_DIR` environment variable — useful for shared volumes or
  CI runners with limited home-directory space.

- **One place to manage the cache: `oda_data.cache`.** Inspect what's cached,
  clear specific parts, or invalidate a single dataset without touching the
  rest:

  ```python
  from oda_data import cache, CRSData

  cache.size()                # bytes per scope
  cache.clear("raw")          # drop only raw OECD zips
  cache.invalidate(CRSData)   # forget cached CRS, keep everything else
  ```

- **Per-call `refresh=True`** on every dataset's `read()` to force a fresh
  download for one call without permanently clearing the cache.

- **Automatic recovery from corrupt downloads.** When a freshly downloaded
  zip fails its integrity check, the bad file is removed and the download is
  retried once. If both attempts fail, you get a clear `BulkPayloadCorrupt`
  error pointing at the file and the reason.

### Changed

- **`set_data_path()` no longer controls the cache.** It now only sets where
  the package writes parquet *exports* (data you explicitly save out). If you
  used it to redirect cache storage, switch to `set_cache_root()` or
  `ODA_DATA_CACHE_DIR` — the package will print a one-time deprecation
  warning to remind you. The old call still works through 2.x and is removed
  in 3.0.
- **`clear_cache()`, `enable_cache()`, and `disable_cache()` still work**
  unchanged. They now delegate to the new `cache.*` namespace, so existing
  scripts keep running without edits.

### Migration

On the first cache-touching call after upgrading, the package looks for
pre-2.6 caches in their old locations (`./.raw_data/`, the per-OS
`oda-reader` directory) and moves them into the new layout. Caches on
synced drives (Dropbox, iCloud, OneDrive) are skipped with a clear log
message — re-run with `oda_data.cache.migrate(force=True)` to override.

## [2.5.1] - 2026-04-28

### Fixed

- OECD CRS bulk downloads using the newer PKZIP Deflate64 compression method
  no longer fail with `BadZipFile: File is not a zip file`. Pulled in via
  `oda-reader >= 1.5.1`. If you saw this error before upgrading, delete any
  stale file under your `oda-reader` bulk cache (path varies by OS) before
  retrying.

## [2.5.0] - 2026-04-09

### Changed

- Romania (code 77) is now classified as a DAC member/country (previously non-DAC), reflecting its new status as a DAC associate

## [2.4.2] - 2026-02-13

### Fixed

- Memory cache returning stale results when a cached DataFrame lacks columns requested by a subsequent query

## [2.4.1] - 2025-12-19

### Added

- CRS column mappings for `donor` → `provider_name` and `recipient` → `recipient_name`

## [2.4.0] - 2025-12-19

### Added

- New `DATA_TYPE_CODE` field to ODASchema and CRS column mapping for datatype_code column support

### Changed

- DAC2A bulk downloads now use dedicated `bulk_download_dac2a()` function from oda-reader for improved reliability
- Measure filters are now skipped for DAC2A when using bulk downloads (consistent with CRS behavior)
- Updated oda-reader dependency from >=1.3.1 to >=1.4.1

## [2.3.2] - 2025-12-19

### Added

- New Development Bank (code 1044) to provider groupings, CRS names, and DAC2A names
- Eurasian Fund for Stabilization and Development (code 1041) to provider groupings, CRS names, and DAC2A names
- UN Economic and Social Commission for Western Asia (code 1403) to provider groupings and DAC2A names

## [2.3.1] - 2025-12-15

### Added

- European Investment Bank (EIB, code 919) to provider/donor mappings across DAC1, DAC2A, CRS, and provider groupings
- New unspecified regional recipient codes: Southern Asia (6790), Micronesia (8600), Middle Africa (10280), Melanesia (10330), Polynesia (10350)
- Broad sector categories for top-level aggregation: Education, Health, Energy, General Environment Protection, Agriculture and Forestry & Fishing
- New sector/purpose mappings including Conflict/Peace/Security (152), Trade Policies (331), Refugees (930), Humanitarian Aid (700)

### Fixed

- Type conversion for code columns (sector_code, purpose_code, donor_code, agency_code) when adding name columns to handle mixed types
- Typo in sector name: "Unallocated/ Unspecified" → "Unallocated/ Unspecified"
- Capitalization in broad sector groups: "government & Civil Society" → "Government & Civil Society"
- Sector mapping now uses fallback for unmapped sectors and fills missing values with "Unallocated/ Unspecified"

## [2.3.0] - 2025-10-16

### Added

- Comprehensive test suite with unit and integration tests
- `clean_parquet_file_in_batches()` function for memory-efficient processing of large files
- Thread-safe memory caching with `ThreadSafeMemoryCache`
- Manifest-based bulk cache tracking system
- Query cache manager for filtered dataset results
- Contributing guidelines and pre-commit hooks

### Changed

- Complete caching refactor with three-tier architecture (memory, bulk, query caches)
- Improved thread and process safety using FileLock for cache coordination
- Better memory management with configurable cache size limits
- Atomic file operations for cache writes to prevent corruption
- Enhanced error handling in query filter construction

### Fixed

- Cache corruption issues in multi-threaded/multi-process environments
- Memory issues when processing large bulk files (now processes in batches)
- Race conditions in cache initialization across threads
- Stale cache detection and automatic refresh logic

## [2.2.2] - 2025-09-26

### Fixed

- Bug with marker calculations

## [2.2.1] - 2025-09-26

### Added

- Access to sector imputations via `from oda_data import sector_imputations`

### Fixed

- Issues with filter passing given schema changes in bulk files on the OECD side

## [2.1.2] - 2025-09-01

### Fixed

- Caching paths now respect user-defined data directories and default to a `.raw_data` folder relative to the working directory

## [2.1.1] - 2025-07-23

### Fixed

- Bug where GNI may not get converted to constant prices even if a base year is specified

## [2.1.0] - 2025-06-16

### Changed

- Improved AidDataData to behave more like other Sources

## [2.0.6] - 2025-06-16

### Fixed

- Bug when trying to calculate multilateral imputations in constant prices

## [2.0.5] - 2025-06-13

### Fixed

- Bug caused by the Providers multisystem dataset using a form of pascal case

## [2.0.4] - 2025-06-13

### Changed

- CRS research indicators now use bulk downloads by default

## [2.0.3] - 2025-06-13

### Changed

- Better bulk file memory management

## [2.0.2] - 2025-05-28

### Added

- Functionality to calculate the official ODA/GNI

## [2.0.1] - 2025-04-25

### Changed

- Improved caching performance by keeping both memory and disk cache of parquet files

## [2.0.0] - 2025-04-22

This major release is a complete refactoring of the `oda-data` package. It is now faster,
more stable, and better organized.

### Changed

- Complete package refactoring with improved performance and stability
- **BREAKING**: Major API changes - please refer to the project README for migration details
- Versions ~1.5.x will remain supported until at least August 2025 to allow time to migrate workflows

______________________________________________________________________

## [1.5.0] - 2024-11-29

### Changed

- Updated requirements to pydeflate >=2.0
- Removed climate indicators (given methodological challenges inherent in OECD data). For access to climate data, please see the climate-finance package

## [1.4.3] - 2024-11-29

### Fixed

- JSON validation error for recipient groupings

## [1.4.2] - 2024-11-26

### Fixed

- Donors and recipient groupings to fully align with recent schemas

## [1.4.1] - 2024-10-11

### Fixed

- Bug with how certain files are stored, moving them from feather to parquet

## [1.4.0] - 2024-10-11

This release introduces significant changes to how raw data files are managed. It is strongly recommended that all users update to this version.

### Changed

- Default storage format changed from feather to parquet files, allowing oda_data to leverage predicate pushdown and more efficiently load only the data it needs
- Removed data download tools from oda_data in favor of using the tools via oda-reader
- oda-reader package now uses the new data-explorer API and bulk downloads instead of relying on the old (and now inaccessible) bulk download service

## [1.3.3] - 2024-09-16

### Fixed

- Issues reading bulk files from the OECD (given that the bulk download service no longer exists)

## [1.3.1] - 2024-07-16

### Fixed

- Schema of the temporary fix to align with the expected CRS schema from the bulk download service

## [1.3.0] - 2024-07-16

### Added

- Workaround for the OECD bulk download service, which is down following the release of the new OECD website
- Uses a full CRS file shared by the OECD (note: nearly 1GB and can take a long time to download on slow connections)

## [1.2.0] - 2024-04-05

### Changed

- Now uses `oda_reader` to download data for DAC1 and DAC2a directly from the API
- Data is converted to the .Stat schema to ensure full backwards compatibility
- Updated dependencies

### Deprecated

- .Stat schema will be deprecated in a future version in favor of the explorer API schema

## [1.1.6] - 2024-03-14

### Changed

- Updated pydeflate dependency to deal with data download issue

## [1.1.5] - 2024-03-07

### Fixed

- Bug introduced by changes in the OECD bulk download service

## [1.1.4] - 2024-03-01

### Fixed

- Constant non-USD currencies bug for imputed sectors calculations

## [1.1.3] - 2024-02-29

### Fixed

- Sorting bug (arrow)

## [1.1.2] - 2024-02-29

### Added

- Support for reading the CRS from 1973-2004

### Fixed

- Removed a warning on pandas stack (for future behavior)

## [1.1.1] - 2024-02-29

### Security

- Security updates to dependencies

## [1.1.0] - 2024-02-29

### Added

- New indicators to separately produce multilateral sector spending shares and imputed multilateral spending totals
- Improved, automated method to map multilateral CRS spending (by agency) to the multilateral "channels" used in the multisystem database
- Tools to group purpose codes following ONE's sector groupings

## [1.0.11] - 2024-01-04

### Fixed

- Key COVID indicators

## [1.0.10] - 2023-12-11

### Changed

- Added UTF8 encoding

## [1.0.7] - 2023-12-11

### Security

- Updated requirements for security

## [1.0.6] - 2023-10-21

### Fixed

- Bug caused by new readme files in the bulk download service file

## [1.0.5] - 2023-08-24

### Changed

- Updated how the CRS codes are fetched given the connection issues outlined in the notes for 1.0.4
- Updated how the indicators that use the `multisystem` database work - the OECD quietly changed the output format of the database, which broke the parsing of the data. The new format is now supported

## [1.0.4] - 2023-08-24

### Added

- Backup solution to download bulk files from the OECD website using `selenium` (given an insecure SSL certificate that causes the normal download using `requests` to fail)
- Dependencies: `selenium` and `webdriver-manager`

## [1.0.3] - 2023-06-12

### Changed

- Updated requirements (pydeflate) to address the same OECD data bug as in 1.0.2

## [1.0.2] - 2023-06-12

### Fixed

- Encoding bug that affected CRS data given a new file encoding from the OECD bulk downloads

### Changed

- Updated requirements

## [1.0.1] - 2023-04-13

### Changed

- Updated requirements to a newer version of pydeflate, given data quality issues with the latest OECD release

## [1.0.0] - 2023-02-20

First major release of oda_data. We have settled on the basic functionality of the package and the basic API.

### Changed

- Updated requirements

## [0.4.1] - 2023-01-30

### Changed

- Updated requirements

## [0.4.0] - 2023-01-30

### Added

- Indicators for climate finance data

## [0.3.5] - 2023-01-12

### Fixed

- Issues with research indicators in non-USD data

## [0.3.4] - 2023-01-12

### Fixed

- Issues with gender data

## [0.3.3] - 2023-01-13

### Fixed

- Issues with multilateral non core ODA

## [0.3.2] - 2023-01-12

### Fixed

- Issues with multilateral sector imputations

## [0.3.0] - 2023-01-10

### Added

- ONE Core ODA indicators (flows, ge, linked ge), including 'non Core' indicators
- "Official definition" total ODA indicator

## [0.2.5] - 2022-12-21

### Added

- Ability to retrieve COVID-19 indicators

## [0.2.3] - 2022-12-16

### Fixed

- ODA GNI indicators, which returned mostly invalid data from the source
- Typo in the ODA GNI indicator name
- How `OECDClient` deals with adding shares to indicators for which shares don't make sense

## [0.2.1] - 2022-12-16

### Changed

- Download data for indicator automatically if not available in data folder

## [0.2.0] - 2022-12-16

### Added

- Method to OECDClient to add a "share" column to the output data
- Method to OECDClient to add a "gni_share" column to the output data

### Changed

- `OECDClient().load_indicator()` now accepts a list of indicators as input

## [0.1.10] - 2022-12-09

### Added

- Total (ODA + OOF, excluding export credits) indicator for the CRS

## [0.1.9] - 2022-12-07

### Added

- Ability to request a 'one_linked' indicator - these indicators are composed of a main indicator which is completed by a fallback indicator when values are missing (e.g., In-Donor Refugee Costs should be the same in Grant Equivalents or Flows; if values are missing in the former, they are filled by the latter)
- Option to get a simplified/summarized dataframe by calling `.simplify_output_df()` on the `OECDClient` object, which keeps only the requested columns and applies `.groupby().sum()` on the remaining columns
- Documentation for the `OECDClient` class

### Changed

- How indicators are grouped when requesting a 'one' indicator - instead of returning fewer columns than the raw indicators, it returns the same columns, excluding the ones that make up the requested indicator

## [0.1.8] - 2022-11-29

### Added

- More comprehensive tests of all core functionalities
- Tool to extract CRS codes from the DAC CRS code list

## [0.1.7] - 2022-11-24

This version mainly tweaks the file structure.

### Fixed

- Issue with trying to set a file path for both oda_data and pydeflate

## [0.1.6] - 2022-11-24

Minor improvements

## [0.1.5] - 2022-11-24

Minor improvements

## [0.1.4] - 2022-11-24

Minor improvements

## [0.1.3] - 2022-11-24

Minor improvements

## [0.1.2] - 2022-11-24

Minor improvements

## [0.1.1] - 2022-11-24

Minor improvements

## [0.1.0] - 2022-11-24

First release of oda_data

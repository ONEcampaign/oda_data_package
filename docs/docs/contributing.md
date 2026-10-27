# Contributing to oda-data

Bug reports, questions, and pull requests go to the [issue tracker](https://github.com/ONEcampaign/oda_data_package/issues) and the repository. This page covers setting up, where the code lives, adding an indicator, and the checks a pull request must pass.

## Set up

You need Python 3.12 or later and [uv](https://docs.astral.sh/uv/).

```bash
git clone https://github.com/ONEcampaign/oda_data_package.git
cd oda_data_package
uv sync --all-groups
uv run pre-commit install
```

The hooks run ruff, ty, codespell, mdformat, and JSON and YAML checks on every commit. Keep what mdformat writes, since it reflows Markdown tables and lists.

## Where the code lives

Every public name is defined in a private module under `src/oda_data/` and loaded on first use by `src/oda_data/__init__.py`, so `import oda_data` stays light.

- `_registry/`: the indicators, one module per source (`dac1.py`, `dac2a.py`, `crs.py`, `multisystem.py`, `one.py`). Each module defines its bindings, such as `DAC1` in `dac1.py`, and an `INDICATORS` tuple. `_registry/__init__.py` maps every id to its indicator.
- `_model.py`: `Indicator`, `Binding`, the bases (`Value`, `Flow`, `Line`, `Headline`), the filters (`In`, `NotIn`, `IsNull`, `NotNull`, `Between`, `HasTag`, `AnyOf`), and the kinds (`Sum`, `Ratio`, `Derived`, `Method`).
- `_compile.py` and `_combine.py`: turn an indicator and a resolved call into DuckDB SQL, then apply groups, totals, ratios, and the partial-year rules.
- `_context.py`: `Context`, `Query`, `get`, `query`, `records`, and `explain()`.
- `_tables.py`, `_factors.py`, `_provenance.py`: table views over oda-reader's `scan()`, conversion factors from pydeflate, and provenance records.
- `_resolve.py`, `_names.py`, `_groups.py`: names and codes, and the built-in groups.
- `_methods/`: the code behind `Method` indicators, such as the imputation of multilateral sectors.
- `_discover.py`: `indicators`, `search`, `describe`, and `add_names`.
- `_compat.py` and `_data/v2_map.json`: the 3.0 call for every 2.x code, and the replacements of removed 2.x names.
- `_data/`: bundled reference data (groups, names, sector groups, the channel crosswalk and proxy shares).

## Add or change an indicator

1. Define the `Indicator` in the module of the table it reads, on that table's binding, and add it to the module's `INDICATORS`. An indicator of ONE's own goes in `one.py` with an id that starts `one.`.

1. Add a conformance case to the module's unit test in `tests/unit/test_registry_*.py`. It is a small input table and the output the indicator must give.

1. Regenerate the files built from the registry:

   ```bash
   uv run python scripts/generate_ids.py
   uv run python scripts/generate_indicator_docs.py
   uv run python scripts/generate_v2_map.py
   ```

   `generate_ids.py` writes the `IndicatorId` literal in `src/oda_data/_ids.py`. `generate_indicator_docs.py` writes `docs/docs/reference/indicators.md`. `generate_v2_map.py` rewrites `src/oda_data/_data/v2_map.json` from the 2.x sources at tag `v2.8.2` (run `git fetch --tags` first), and matters when a change moves a 2.x code's call. Each takes `--check`, which CI runs.

1. A change to the rows an existing indicator counts changes its published numbers. Bump its `version` and state the size of the effect in `CHANGELOG.md`.

## Run the tests

```bash
uv run pytest -q -m "not data"
```

The unit tests need no network and no data. Tests marked `data` read the real OECD tables through oda-reader's cache. Warm a cache once, then point the tests at it:

```bash
export ODA_READER_CACHE_DIR=~/Library/Caches/oda-data-v3-shared
uv run python scripts/warm_cache.py
ODA_DATA_REQUIRE_DATA=1 uv run pytest -q -m data
```

A cold cache makes data tests skip. `ODA_DATA_REQUIRE_DATA=1` turns those skips into failures, so a run that should have read the data cannot pass without it. `scripts/warm_cache.py` downloads about 1.4 GB, most of it the CRS.

`tests/data/goldens.json` holds the expected values of the golden tests and the releases they were computed on. After a new OECD release, `uv run python scripts/refresh_goldens.py` recomputes them and prints the values that moved.

## Docs

The docs site lives in `docs/docs/` with its navigation in `docs/mkdocs.yml`. Build it with `uv run mkdocs build --strict -f docs/mkdocs.yml`.

Every fenced `python` block in `README.md` and the docs is an example. `tests/docs/test_examples.py` checks that each one names real functions, groups, and indicator ids, and under the `data` marker runs each page's examples in order against the cached tables. A `py` block is a signature listing and only compiles. Callouts use the `/// note`, `/// warning | Heads up`, `/// info | Why`, and `/// details | Under the hood` blocks, which survive mdformat.

## Checks a pull request must pass

```bash
uv run pre-commit run --all-files
uv run pytest -q
uv run python scripts/generate_ids.py --check
uv run python scripts/generate_indicator_docs.py --check
uv run python scripts/generate_v2_map.py --check
```

CI runs the same checks. A workflow runs `scripts/check_codelists.py` every Monday and compares every code the package uses with the OECD's latest codelists. It exits 1 and lists the codes that are unknown (no codelist holds them, in any status), renamed (`names.json` or the channel list names them differently from the codelist), or new (an active CRS purpose code, channel, finance type, or modality the package has no name for). A withdrawn code passes, since older data still carries it. Codes the codelists never held, such as the DAC5 headings 100 and 200, sit in the script's `UNLISTED`.

## Maintenance scripts

- `scripts/refresh_channel_crosswalk.py` refreshes the OECD channel list and proposes crosswalk rows for new provider and agency pairs. It needs `uv sync --group maintenance`. Proposed rows stay `reviewed=false`, and the imputation ignores them until someone reviews them.
- `scripts/imputation_delta.py` compares oda-data 2.8.2 and 3.0 on a fixed set of calls over the same cached tables, and prints a Markdown table for the migration guide.

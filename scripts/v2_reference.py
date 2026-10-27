"""Run oda-data 2.x on the OECD tables oda-data 3.0 reads, and print its sums.

Run as a script, it needs an environment with oda-data 2.x installed, never
3.0. ``run_in_2x``, which ``scripts/refresh_goldens.py`` and
``scripts/imputation_delta.py`` call from the 3.0 environment, starts it as

    uv run --no-project --python 3.12 --with oda-data==2.8.2 python -I \
        scripts/v2_reference.py < request.json > result.json

The request names the cached oda-reader Parquet file of each table and a list
of calls. 2.x's four bulk readers are patched to load those files through
2.x's own column cleaning, under a fresh 2.x cache, so 2.x reads the same
releases as 3.0 and downloads nothing.

The bulk DAC1 file codes EU Institutions as 918, which is the code 2.x's
``research.eu`` and ``tools.gni`` look for. 2.x's API reads code it as 20918,
which is why 2.x on its own returns no EU Institutions rows there.

Request::

    {"tables": {"dac1": "<path>", ...},
     "calls": [{"id": "...", "function": "indicator", "code": "DAC1.10.1010",
                "arguments": {"years": [2022], "providers": [4]},
                "by": ["provider", "year"]}, ...]}

``function`` is one of the keys of ``FUNCTIONS``. ``by`` names the 3.0
dimensions to sum by. The result maps each call id to ``{"rows": {key: value},
"row_count": n}``, where ``key`` joins the ``by`` values with ``|``.
"""

import contextlib
import json
import subprocess
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path

import pandas as pd

#: The four tables 3.0 reads.
TABLES = ("dac1", "dac2a", "crs", "multisystem")

#: 3.0 dimension names and the 2.x columns that hold them.
COLUMNS = {
    "provider": "donor_code",
    "recipient": "recipient_code",
    "year": "year",
    "channel": "channel_code",
    "sector": "purpose_code",
    "allocation_status": "allocation_status",
}


def _patch_sources(tables: dict[str, str]) -> None:
    """Point 2.x's bulk readers at ``tables`` and give each a fixed release id."""
    from oda_data.api import sources
    from oda_data.clean_data.common import clean_parquet_file_in_batches, clean_raw_df

    def frame_fetcher(path: str) -> Callable[[Path], None]:
        def fetch(target: Path) -> None:
            pd.read_parquet(path).pipe(clean_raw_df).to_parquet(target)

        return fetch

    def batch_fetcher(path: str) -> Callable[[Path], None]:
        def fetch(target: Path) -> None:
            clean_parquet_file_in_batches(Path(path), Path(target))

        return fetch

    readers = {
        "dac1": (sources.DAC1Data, frame_fetcher),
        "dac2a": (sources.DAC2AData, frame_fetcher),
        "crs": (sources.CRSData, batch_fetcher),
        "multisystem": (sources.MultiSystemData, batch_fetcher),
    }
    for table, (reader, make) in readers.items():
        if table not in tables:
            continue
        path = tables[table]
        reader._create_bulk_fetcher = lambda self, path=path, make=make: make(path)
        reader._get_release_id = lambda self, path=path: path


def _client(arguments: dict) -> object:
    from oda_data import OECDClient

    return OECDClient(**arguments, use_bulk_download=True)


def _indicator(call: dict) -> pd.DataFrame:
    return _client(call["arguments"]).get_indicators(call["code"])


def _gni(call: dict) -> pd.DataFrame:
    from oda_data.tools.gni import _get_gni_data

    return _get_gni_data(_client(call["arguments"]))


def _gni_share(call: dict) -> pd.DataFrame:
    """``add_gni_share_column``'s arithmetic, 100 x value / GNI, without its
    rounding to two decimals."""
    from oda_data.tools.gni import _get_gni_data

    client = _client(call["arguments"])
    gni = _get_gni_data(client).rename(columns={"value": "gni"})
    found = client.get_indicators(call["code"]).merge(
        gni, on=["year", "donor_code"], how="left"
    )
    return found.assign(value=100 * found["value"] / found["gni"])


def _eu_attributed(call: dict) -> pd.DataFrame:
    from oda_data.indicators.research.eu import (
        get_eui_plus_bilateral_providers_indicator,
    )

    return get_eui_plus_bilateral_providers_indicator(
        _client(call["arguments"]), call["code"]
    )


def _research(name: str) -> Callable[[dict], pd.DataFrame]:
    def run(call: dict) -> pd.DataFrame:
        from oda_data.indicators.research import policy_markers, sector_imputations

        module = (
            policy_markers if name == "bilateral_policy_marker" else sector_imputations
        )
        return getattr(module, name)(**call["arguments"])

    return run


FUNCTIONS: dict[str, Callable[[dict], pd.DataFrame]] = {
    "indicator": _indicator,
    "gni": _gni,
    "gni_share": _gni_share,
    "eu_attributed": _eu_attributed,
    "imputed_multilateral_by_purpose": _research("imputed_multilateral_by_purpose"),
    "core_multilateral_contributions_by_provider": _research(
        "core_multilateral_contributions_by_provider"
    ),
    "bilateral_policy_marker": _research("bilateral_policy_marker"),
}


def _sums(frame: pd.DataFrame, by: list[str]) -> dict[str, float]:
    if frame.empty:
        return {}
    if "value" not in frame.columns:  # 2.x drops it when every value is null
        frame = frame.assign(value=float("nan"))
    columns = [COLUMNS[name] for name in by]
    grouped = frame.groupby(columns, dropna=False, observed=True)["value"]
    summed = grouped.sum(min_count=1)
    found: dict[str, float] = {}
    for key, value in summed.items():
        parts = key if isinstance(key, tuple) else (key,)
        found["|".join(key_part(part) for part in parts)] = float(value)
    return found


def key_part(value: object) -> str:
    """One part of a result key: ``<NA>`` for a null, a whole float as an
    integer. ``tests/data/test_goldens.py`` builds 3.0's keys with it too."""
    if pd.isna(value):
        return "<NA>"
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def cached_tables() -> tuple[dict[str, str], dict[str, str]]:
    """The Parquet path and release token of each cached table, read through
    3.0 (``ODA_READER_CACHE_DIR`` chooses the cache)."""
    from oda_data._tables import OecdTables

    tables = OecdTables(offline=True)
    try:
        paths = {name: str(tables.path(name)) for name in TABLES}
        releases = {name: tables.release(name).token for name in TABLES}
    finally:
        tables.close()
    return paths, releases


def run_in_2x(version: str, tables: dict[str, str], calls: list[dict]) -> dict:
    """Run ``calls`` in oda-data ``version`` on ``tables`` and return the result.

    The 2.x environment starts in a scratch folder, so nothing in the
    repository shadows its imports.
    """
    request = json.dumps({"tables": tables, "calls": calls})
    command = [
        "uv",
        "run",
        "--no-project",
        "--python",
        "3.12",
        "--with",
        f"oda-data=={version}",
        "python",
        "-I",
        str(Path(__file__).resolve()),
    ]
    with tempfile.TemporaryDirectory(prefix="oda-data-2x-") as scratch:
        done = subprocess.run(
            command,
            input=request,
            capture_output=True,
            text=True,
            cwd=scratch,
            check=False,
        )
    if done.returncode != 0:
        raise SystemExit(f"oda-data {version} failed:\n{done.stderr[-4000:]}")
    return json.loads(done.stdout)


def main() -> int:
    """Read the request on stdin and write the result as JSON on stdout."""
    request = json.load(sys.stdin)

    import oda_data

    _patch_sources(request["tables"])
    results = {}
    with (
        tempfile.TemporaryDirectory(prefix="oda-data-2x-cache-") as cache,
        contextlib.redirect_stdout(sys.stderr),  # 2.x prints progress
    ):
        oda_data.set_cache_root(cache)
        for call in request["calls"]:
            try:
                frame = FUNCTIONS[call["function"]](call)
                rows = _sums(frame, call["by"])
            except Exception as error:
                error.add_note(f"in call {call['id']}")
                raise
            results[call["id"]] = {"rows": rows, "row_count": len(frame)}
    json.dump(results, sys.stdout, indent=1)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

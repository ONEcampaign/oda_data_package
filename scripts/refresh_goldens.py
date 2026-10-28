"""Recompute the expected values in ``tests/data/goldens.json`` on the cached tables.

Run after a new OECD release reaches the cache, when the golden tests skip
because the cached release differs from the pinned one:

    export ODA_READER_CACHE_DIR=~/Library/Caches/oda-data-v3-shared
    uv run python scripts/warm_cache.py
    uv run python scripts/refresh_goldens.py

Each golden's ``source`` says where its expected values come from:

- ``oda-data 2.8.2``: the ``request`` runs in oda-data 2.8.2 through
  ``scripts/v2_reference.py``, fed the same cached Parquet files.
- ``OECD``: the ``sql`` runs in DuckDB on the cached Parquet files as the
  OECD publishes them, with ``{dac1}`` and the like standing for each file.

The ``v2_map`` section holds 2.x's yearly sums for the 2.x map rows that
``tests/data/test_v2_map_data.py`` samples, and is refreshed the same way.
The script rewrites every expected value and release token, then prints the
values that moved, so the maintainer can read the size of the revision before
committing the file. ``git diff tests/data/goldens.json`` shows the same.
"""

import argparse
import json
import math
import sys
from pathlib import Path

import duckdb
from v2_reference import cached_tables, key_part, run_in_2x

REPO = Path(__file__).resolve().parents[1]
GOLDENS = REPO / "tests" / "data" / "goldens.json"


def run_sql(sql: str, paths: dict[str, str]) -> dict[str, float | None]:
    """Run ``sql`` on the Parquet files; the last column is the value and the
    others join into the key."""
    views = {name: f"read_parquet('{path}')" for name, path in paths.items()}
    rows = duckdb.sql(sql.format(**views)).fetchall()
    return {"|".join(key_part(part) for part in row[:-1]): row[-1] for row in rows}


def _clean(rows: dict[str, float | None]) -> dict[str, float | None]:
    """Nulls for NaN, sorted keys, and no rows of exactly zero, which 3.0
    leaves out where 2.x keeps them."""
    return {
        key: None if value is None or math.isnan(value) else value
        for key, value in sorted(rows.items())
        if value != 0
    }


def _moved(name: str, old: dict, new: dict) -> list[str]:
    lines = []
    for key in sorted(set(old) | set(new)):
        before, after = old.get(key), new.get(key)
        if before != after:
            lines.append(f"{name} [{key}]: {before} -> {after}")
    return lines


def main(argv: list[str] | None = None) -> int:
    """Rewrite ``tests/data/goldens.json`` from the cached tables."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument(
        "--check",
        action="store_true",
        help="print the values that would move and leave the file alone",
    )
    args = parser.parse_args(argv)

    data = json.loads(GOLDENS.read_text())
    version = data["oda_data_2x"]
    paths, releases = cached_tables()

    goldens = data["goldens"]
    calls = [
        {"id": golden["id"], **golden["source"]["request"]}
        for golden in goldens
        if golden["source"]["kind"].startswith("oda-data")
    ]
    v2_map = data["v2_map"]
    calls += [
        {
            "id": code,
            "function": "indicator",
            "code": code,
            "arguments": row["arguments"],
            "by": ["year"],
        }
        for code, row in v2_map.items()
    ]
    found = run_in_2x(version, paths, calls)

    moved: list[str] = []
    for golden in goldens:
        source = golden["source"]
        if source["kind"].startswith("oda-data"):
            result = found[golden["id"]]
            rows = result["rows"]
            if "row_count" in golden:
                golden["row_count"] = result["row_count"]
        else:
            rows = run_sql(source["sql"], paths)
        rows = _clean(rows)
        moved += _moved(golden["id"], golden.get("expected", {}), rows)
        golden["expected"] = rows
        golden["releases"] = {name: releases[name] for name in golden["tables"]}
    for code, row in v2_map.items():
        rows = _clean(found[code]["rows"])
        moved += _moved(code, row.get("expected", {}), rows)
        row["expected"] = rows
    data["v2_map_releases"] = {
        name: releases[name] for name in ("dac1", "dac2a", "crs")
    }

    print("\n".join(moved) or "no expected value moved")
    if not args.check:
        GOLDENS.write_text(json.dumps(data, indent=2) + "\n")
        print(f"wrote {GOLDENS.relative_to(REPO)}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

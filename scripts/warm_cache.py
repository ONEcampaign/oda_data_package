"""Download and convert the four OECD tables into the shared oda-reader cache.

Run once per machine before the data tests:

    export ODA_READER_CACHE_DIR=~/Library/Caches/oda-data-v3-shared
    uv run python scripts/warm_cache.py

Needs an oda-reader release that provides ``oda_reader.scan``.
"""

import os
import sys
from pathlib import Path

DATASETS = ("dac1", "dac2a", "multisystem", "crs")


def main() -> int:
    """Scan each dataset into ``$ODA_READER_CACHE_DIR`` and print what was cached."""
    cache_dir_value = os.environ.get("ODA_READER_CACHE_DIR")
    if not cache_dir_value:
        print("Set ODA_READER_CACHE_DIR to the cache folder to warm.", file=sys.stderr)
        return 1
    cache_dir = Path(cache_dir_value).expanduser()

    from oda_reader import scan

    for dataset in DATASETS:
        table = scan(dataset, cache_dir=cache_dir)
        print(f"{dataset}: release {table.release}, {table.path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

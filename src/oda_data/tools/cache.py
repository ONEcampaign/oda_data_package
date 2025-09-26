import hashlib
import json
import time
from pathlib import Path
from typing import Optional

import pandas as pd

from oda_data.logger import logger
from oda_data.tools.names.create_mapping import snake_to_pascal


def generate_param_hash(filters: list[tuple]) -> str:
    """Generates a reproducible hash from a list of tuples."""
    # convert list of tuples to dictionary
    params = {k: v for k, _, v in filters}

    sorted_params = {
        k: sorted(v) if isinstance(v, list) else v for k, v in params.items()
    }
    json_str = json.dumps(sorted_params, sort_keys=True)
    return hashlib.md5(json_str.encode("utf-8")).hexdigest()[:10]


class OnDiskCache:
    """
    Handles storing/loading DataFrames based on parameter hash on disk.
    """

    def __init__(self, base_dir: Path, ttl_seconds: Optional[int] = 604800):
        """Args:
        base_dir: Directory for storing cached data files.
        ttl_seconds: Time (in seconds) before cache files expire. Defaults to 1 week.
        """
        self.base_dir = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.ttl_seconds = ttl_seconds

    def get_file_path(self, dataset_name: str, param_hash: str) -> Path:
        """Returns the on-disk path for the given dataset and parameter hash."""
        return self.base_dir / f"{dataset_name}-{param_hash}.parquet"

    def load(
        self,
        dataset_name: str,
        param_hash: str,
        filters: list | None,
        columns: list | None,
    ) -> pd.DataFrame | None:
        """Loads a DataFrame from disk if available and not expired."""
        path = self.get_file_path(dataset_name, param_hash)
        if dataset_name == "MultiSystemData":
            columns = [snake_to_pascal(col) for col in columns] if columns else columns
            filters = (
                [(snake_to_pascal(col), op, val) for col, op, val in filters]
                if filters
                else None
            )
        if not path.exists():
            return None
        if self._is_expired(path):
            path.unlink(missing_ok=True)
            return None
        return pd.read_parquet(path, filters=filters, engine="pyarrow", columns=columns)

    def save(self, dataset_name: str, param_hash: str, df: pd.DataFrame):
        """Saves a DataFrame to disk."""
        df.to_parquet(self.get_file_path(dataset_name, param_hash))

    def _is_expired(self, path: Path) -> bool:
        """Checks if a file has expired based on its modified time."""
        return time.time() - path.stat().st_mtime > self.ttl_seconds

    def cleanup(self, hash_str: Optional[str] = None, force: bool = False):
        """Removes expired cache files."""
        pattern = f"*{hash_str}*.parquet"

        for file in self.base_dir.glob(pattern):
            if self._is_expired(file) or force:
                file.unlink(missing_ok=True)
                logger.info(f"Deleted cache file: {file}")

from dataclasses import dataclass
from pathlib import Path
from typing import Literal, get_args

Scope = Literal["all", "bulk", "query", "http", "raw"]
_SCOPE_VALUES: tuple[str, ...] = get_args(Scope)


@dataclass(frozen=True, slots=True)
class CacheRecord:
    """One row in oda_data.cache.entries().

    Attributes:
        key: Cache entry identifier (e.g., "CRSData_bulk").
        path: Absolute path to the cached file.
        size_bytes: On-disk size in bytes.
        age_days: Days since the entry was downloaded.
        version: Package version recorded when the entry was created.
        scope: Which cache scope owns this entry.
    """

    key: str
    path: Path
    size_bytes: int
    age_days: float
    version: str
    scope: Scope


@dataclass(frozen=True, slots=True)
class MigrationResult:
    """Outcome of migrating one pre-2.6 cache tree.

    Attributes:
        source: Old cache tree root that was scanned.
        status: Outcome category.
        files_moved: Count of files moved (0 for skipped/failed).
        bytes_moved: Total bytes moved (0 for skipped/failed).
        message: Human-readable detail (e.g. path of the synced-drive marker
            that triggered a skip, or the OSError message on failure).
    """

    source: Path
    status: Literal[
        "migrated",
        "skipped_synced_drive",
        "skipped_already_migrated",
        "skipped_disk_full",
        "failed",
    ]
    files_moved: int
    bytes_moved: int
    message: str


def _validate_scope(scope: object) -> Scope:
    """Raise ValueError if scope is not a known Scope literal.

    Args:
        scope: Value to validate.

    Returns:
        The same value, narrowed to Scope.

    Raises:
        ValueError: If scope is not one of the Scope literals.
    """
    if scope not in _SCOPE_VALUES:
        raise ValueError(f"Unknown scope {scope!r}; expected one of {_SCOPE_VALUES}")
    return scope  # type: ignore[return-value]

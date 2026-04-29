"""Cache auto-migration from pre-2.6 layouts.

Detects old cache trees (CWD .raw_data, oda-reader platformdirs defaults,
Windows LOCALAPPDATA) and moves/copies their contents into the unified 2.6
cache root.

Synced-drive parents are skipped unless ``force=True``. Cross-volume moves
fall back to copy2 + verify + unlink. All operations are idempotent via a
``.migrated_to`` breadcrumb written at the old root after a successful
migration.
"""

import errno
import logging
import os
import shutil
from collections.abc import Callable
from pathlib import Path

from oda_data.cache.config import oda_data_cache_root
from oda_data.cache.types import MigrationResult

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Synced-drive detection
# ---------------------------------------------------------------------------

# File/dir markers that indicate Dropbox ownership (checked in any ancestor).
_DROPBOX_MARKERS: frozenset[str] = frozenset({".dropbox", ".dropbox.cache"})

# File/dir markers that indicate iCloud ownership (checked in any ancestor).
# Note: Apple uses camelCase on disk; the brief's lowercase form was loose.
_ICLOUD_MARKERS: frozenset[str] = frozenset({"com.apple.iCloud"})

# Union precomputed once so the ancestor-walk loop does not rebuild it each step.
_FILE_MARKERS: frozenset[str] = _DROPBOX_MARKERS | _ICLOUD_MARKERS

# Prefix that identifies OneDrive path-segments (segment equality or prefix).
_ONEDRIVE_EXACT: str = "OneDrive"
_ONEDRIVE_PREFIX: str = "OneDrive - "


def _find_synced_marker(path: Path) -> str | None:
    """Return a human-readable description of the synced-drive marker, or None.

    Checks *path* and its ancestors up to (and including) the home directory.
    Returns ``None`` when no synced-drive marker is found.

    Args:
        path: Directory to check.

    Returns:
        Short description string (e.g. ``"iCloud (Mobile Documents)"``,
        ``"/home/user/.dropbox"``) when a marker is found, else ``None``.
    """
    home = Path.home()

    # iCloud Mobile Documents — path membership check (no filesystem access).
    try:
        path.relative_to(home / "Library" / "Mobile Documents")
        return "iCloud (Mobile Documents)"
    except ValueError:
        pass

    # Walk ancestors up to (but not past) the home directory.
    current = path.resolve()
    stop = home.resolve()

    while True:
        # Dropbox and iCloud file/dir markers.
        for marker in _FILE_MARKERS:
            candidate = current / marker
            if candidate.exists():
                return str(candidate)

        # OneDrive path-segment check (no per-folder marker file).
        for part in current.parts:
            if part == _ONEDRIVE_EXACT or part.startswith(_ONEDRIVE_PREFIX):
                return f"OneDrive segment '{part}'"

        if current in (stop, current.parent):
            break
        current = current.parent

    return None


# ---------------------------------------------------------------------------
# Cross-volume detection
# ---------------------------------------------------------------------------


def _is_cross_volume(src: Path, dst_parent: Path) -> bool:
    """Return True when *src* and *dst_parent* live on different volumes.

    This is the single boundary for cross-volume detection. Tests inject a
    replacement via the ``is_cross_volume`` parameter on ``_migrate_tree``
    instead of monkeypatching ``os.stat`` globally (which would corrupt
    ``shutil.move``'s own device checks).

    Args:
        src: Source directory being migrated.
        dst_parent: Parent of the destination directory.

    Returns:
        True when the two paths are on different block devices.
    """
    return src.stat().st_dev != dst_parent.stat().st_dev


# ---------------------------------------------------------------------------
# Old cache root detection
# ---------------------------------------------------------------------------


def _old_cache_paths_to_scan() -> list[Path]:
    """Return the candidate pre-2.6 cache roots that exist on disk.

    Returns:
        Existing paths from the fixed detection set: CWD/.raw_data, two
        oda-reader platformdirs defaults, and the Windows LOCALAPPDATA path.
    """
    candidates = [
        Path.cwd() / ".raw_data",
        Path("~/Library/Caches/oda-reader").expanduser(),
        Path("~/.cache/oda-reader").expanduser(),
    ]
    if local_app_data := os.environ.get("LOCALAPPDATA"):
        candidates.append(Path(local_app_data) / "oda-reader" / "Cache")
    return [p for p in candidates if p.exists()]


# ---------------------------------------------------------------------------
# Breadcrumb helpers
# ---------------------------------------------------------------------------


def _already_migrated(old_root: Path) -> bool:
    """Return True if a ``.migrated_to`` breadcrumb exists at *old_root*."""
    return (old_root / ".migrated_to").exists()


def _write_breadcrumb(old_root: Path, new_root: Path) -> None:
    """Write a ``.migrated_to`` breadcrumb at *old_root* pointing to *new_root*.

    Args:
        old_root: Source cache tree root.
        new_root: Destination cache tree root (absolute path written to the file).
    """
    (old_root / ".migrated_to").write_text(f"{new_root.resolve()}\n")


# ---------------------------------------------------------------------------
# Tree migration
# ---------------------------------------------------------------------------


def _migrate_tree(
    src: Path,
    dst: Path,
    *,
    force: bool,
    is_cross_volume: Callable[[Path, Path], bool] = _is_cross_volume,
) -> MigrationResult:
    """Migrate one old cache tree from *src* to *dst*.

    Steps (in order):
    1. Synced-drive guard — skip unless ``force=True``.
    2. Volume comparison — same-volume → ``shutil.move``; cross-volume →
       ``shutil.copy2`` + size-verify + ``unlink``.
    3. ``OSError(errno 28)`` (ENOSPC) → skipped_disk_full.
    4. Generic ``OSError`` → failed.

    Args:
        src: Old cache tree root directory.
        dst: Destination directory (will be created if absent).
        force: If True, the synced-drive guard is disabled.
        is_cross_volume: Predicate injected for testability. Default uses
            ``os.stat().st_dev`` comparison.

    Returns:
        A ``MigrationResult`` describing the outcome.
    """
    # --- synced-drive guard --------------------------------------------------
    marker_desc = None if force else _find_synced_marker(src)
    if marker_desc is not None:
        return MigrationResult(
            source=src,
            status="skipped_synced_drive",
            files_moved=0,
            bytes_moved=0,
            message=(
                f"Skipped: {src} appears to be on a synced drive "
                f"({marker_desc}). Run migrate(force=True) to override."
            ),
        )

    # --- collect source files ------------------------------------------------
    src_files = [f for f in src.rglob("*") if f.is_file()]
    if not src_files:
        # Empty tree — treat as successful with zero counts.
        _write_breadcrumb(src, dst)
        return MigrationResult(
            source=src,
            status="migrated",
            files_moved=0,
            bytes_moved=0,
            message=f"Migrated 0 files from {src} to {dst} (empty tree).",
        )

    dst.mkdir(parents=True, exist_ok=True)
    cross_vol = is_cross_volume(src, dst.parent)

    if cross_vol:
        log.info(
            "Cross-volume migration from %s to %s — "
            "this may take a few minutes for ~1 GB.",
            src,
            dst,
        )

    files_moved = 0
    bytes_moved = 0

    try:
        for src_file in src_files:
            rel = src_file.relative_to(src)
            dst_file = dst / rel
            dst_file.parent.mkdir(parents=True, exist_ok=True)

            src_size = src_file.stat().st_size

            if cross_vol:
                shutil.copy2(src_file, dst_file)
                # Verify size before unlinking the source.
                dst_size = dst_file.stat().st_size
                if dst_size != src_size:
                    dst_file.unlink(missing_ok=True)
                    raise OSError(
                        f"Size mismatch after copy: {src_file} ({src_size} B) "
                        f"!= {dst_file} ({dst_size} B)"
                    )
                src_file.unlink()
            else:
                shutil.move(str(src_file), dst_file)

            files_moved += 1
            bytes_moved += src_size

    except OSError as exc:
        if exc.errno == errno.ENOSPC:
            log.warning(
                "Disk full while migrating %s → %s. "
                "Both old and new trees are left in place. "
                "Free up space and re-run migrate().",
                src,
                dst,
            )
            return MigrationResult(
                source=src,
                status="skipped_disk_full",
                files_moved=files_moved,
                bytes_moved=bytes_moved,
                message=f"Disk full (ENOSPC): {exc}",
            )
        log.warning("Migration failed for %s: %s", src, exc)
        return MigrationResult(
            source=src,
            status="failed",
            files_moved=files_moved,
            bytes_moved=bytes_moved,
            message=str(exc),
        )

    _write_breadcrumb(src, dst)
    return MigrationResult(
        source=src,
        status="migrated",
        files_moved=files_moved,
        bytes_moved=bytes_moved,
        message=f"Migrated {files_moved} file(s) ({bytes_moved} B) from {src} to {dst}.",
    )


# ---------------------------------------------------------------------------
# Destination mapping
# ---------------------------------------------------------------------------

# Maps old cache-root stems (after stripping leading dots) to the canonical 2.6
# subtree under new_root.  Old .raw_data goes to oda-data/ (where
# BulkCacheManager and QueryCacheManager look); old oda-reader trees stay under
# oda-reader/ (matching oda_reader._cache.config's subdirectory names).
_DST_MAP: dict[str, str] = {
    "raw_data": "oda-data",
    "oda-reader": "oda-reader",
}


def _canonical_dst(old_root: Path, new_root: Path) -> Path:
    """Return the canonical 2.6 destination for *old_root*.

    For recognised old-root names, maps directly to the canonical subtree.
    Falls back to ``new_root / old_root.name.lstrip('.')`` for unknown names.

    Args:
        old_root: The old cache root being migrated.
        new_root: The new 2.6 cache root.

    Returns:
        Destination directory path under *new_root*.
    """
    stem = old_root.name.lstrip(".")
    subtree = _DST_MAP.get(stem, stem)
    return new_root / subtree


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def _check_downgrade_breadcrumb(new_root: Path) -> None:
    """Raise RuntimeError if a downgrade breadcrumb is found at *new_root*.

    A ``.migrated_to`` breadcrumb at the new root means this directory was
    *itself* a source that was migrated elsewhere (i.e. the user downgraded
    after a migration).  Running with a stale, partially-consumed cache in
    that state would produce confusing behaviour.

    Args:
        new_root: The resolved 2.6 cache root.

    Raises:
        RuntimeError: If ``.migrated_to`` exists at *new_root*.
    """
    breadcrumb = new_root / ".migrated_to"
    if breadcrumb.exists():
        destination = breadcrumb.read_text().strip()
        raise RuntimeError(
            f"Cache at {new_root} was already migrated to {destination}. "
            "It looks like oda_data was downgraded after a successful migration. "
            "Upgrade to oda_data>=2.6 or delete the old cache directory and "
            "re-run to start fresh."
        )


def migrate(*, force: bool = False) -> list[MigrationResult]:
    """Migrate any pre-2.6 cache directories to the unified cache root.

    Detects four old layouts (CWD ``.raw_data``, two oda-reader platformdirs
    defaults, Windows LOCALAPPDATA) and copies/moves their contents.
    Synced-drive parents are skipped unless ``force=True``.

    Args:
        force: If True, bypass the synced-drive guard. Cross-volume
            detection is preserved.

    Returns:
        One ``MigrationResult`` per detected source root. Status enumerates the
        outcome (``"migrated"``, ``"skipped_synced_drive"``,
        ``"skipped_already_migrated"``, ``"skipped_disk_full"``,
        ``"failed"``).
    """
    new_root = oda_data_cache_root()
    _check_downgrade_breadcrumb(new_root)
    results: list[MigrationResult] = []

    for old_root in _old_cache_paths_to_scan():
        if _already_migrated(old_root):
            results.append(
                MigrationResult(
                    source=old_root,
                    status="skipped_already_migrated",
                    files_moved=0,
                    bytes_moved=0,
                    message=f"Already migrated (breadcrumb exists at {old_root / '.migrated_to'}).",
                )
            )
            continue

        # Map each old root to the canonical 2.6 destination layout so that
        # BulkCacheManager / QueryCacheManager can find the files after migration.
        dst = _canonical_dst(old_root, new_root)
        result = _migrate_tree(old_root, dst, force=force)
        results.append(result)

    return results

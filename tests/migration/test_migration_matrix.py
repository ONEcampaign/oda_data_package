"""Parametrized migration-matrix tests for oda_data.cache.migrate.

The synced-drive marker list used here is the authoritative set from
``_is_synced_drive``:

- ``.dropbox``
- ``.dropbox.cache``
- ``com.apple.iCloud``
- path under ``~/Library/Mobile Documents/`` (``mobile_documents``)
- path segment exactly ``OneDrive``
- path segment ``OneDrive - Personal``
- path segment ``OneDrive - Some Tenant``
"""

import errno
import logging
import shutil
from pathlib import Path
from unittest.mock import patch

import pytest

from oda_data.cache._migrate import (
    MigrationResult,
    _is_cross_volume,
    _migrate_tree,
    migrate,
)

# ---------------------------------------------------------------------------
# Authoritative synced-drive marker list — must match _is_synced_drive exactly.
# Each tuple: (marker_name, setup_fn) where setup_fn(tmp_path) creates the
# marker under tmp_path and returns the path that should be detected as synced.
# ---------------------------------------------------------------------------

_SYNCED_DRIVE_MARKERS = [
    ".dropbox",
    ".dropbox.cache",
    "com.apple.iCloud",
    "mobile_documents",
    "OneDrive",
    "OneDrive - Personal",
    "OneDrive - Some Tenant",
]


def _make_old_tree(root: Path) -> None:
    """Create a small fake old-cache tree under *root* (3 files, 128 B each)."""
    for name in ("data/a.parquet", "data/b.parquet", "meta.json"):
        f = root / name
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_bytes(b"x" * 128)


# ---------------------------------------------------------------------------
# Same-volume / cross-volume
# ---------------------------------------------------------------------------


def test_same_volume_uses_move(tmp_path: Path) -> None:
    """Same-volume migration calls shutil.move (not copy2+unlink)."""
    src = tmp_path / "old_cache"
    dst = tmp_path / "new_cache"
    _make_old_tree(src)

    move_calls: list[tuple] = []
    real_move = shutil.move

    def spy_move(src_arg, dst_arg):
        move_calls.append((src_arg, dst_arg))
        return real_move(src_arg, dst_arg)

    with patch("oda_data.cache._migrate.shutil.move", side_effect=spy_move):
        result = _migrate_tree(src, dst, force=False, is_cross_volume=lambda *_: False)

    assert result.status == "migrated"
    assert len(move_calls) == 3
    assert result.files_moved == 3
    assert result.bytes_moved == 3 * 128


def test_cross_volume_uses_copy2_and_unlink(tmp_path: Path) -> None:
    """Cross-volume migration calls shutil.copy2 then unlink."""
    src = tmp_path / "old_cache"
    dst = tmp_path / "new_cache"
    _make_old_tree(src)

    copy2_calls: list[tuple] = []
    unlink_calls: list[Path] = []
    real_copy2 = shutil.copy2

    def spy_copy2(src_arg, dst_arg):
        copy2_calls.append((src_arg, dst_arg))
        return real_copy2(src_arg, dst_arg)

    original_unlink = Path.unlink

    def spy_unlink(self, *, missing_ok=False):
        if self.suffix in (".parquet", ".json"):
            unlink_calls.append(self)
        return original_unlink(self, missing_ok=missing_ok)

    with (
        patch("oda_data.cache._migrate.shutil.copy2", side_effect=spy_copy2),
        patch.object(Path, "unlink", spy_unlink),
    ):
        result = _migrate_tree(src, dst, force=False, is_cross_volume=lambda *_: True)

    assert result.status == "migrated"
    assert len(copy2_calls) == 3
    assert len(unlink_calls) == 3
    assert result.files_moved == 3
    assert result.bytes_moved == 3 * 128


def test_is_cross_volume_unit(tmp_path: Path) -> None:
    """_is_cross_volume returns False for two paths sharing the same tmp volume."""
    a = tmp_path / "alpha"
    a.mkdir()
    b = tmp_path / "beta"
    b.mkdir()
    assert _is_cross_volume(a, b) is False


# ---------------------------------------------------------------------------
# Synced-drive: single .dropbox marker (explicit test)
# ---------------------------------------------------------------------------


def test_synced_drive_marker_dropbox_skips(tmp_path: Path) -> None:
    """_migrate_tree returns skipped_synced_drive when .dropbox marker is present."""
    (tmp_path / ".dropbox").touch()
    src = tmp_path / "data"
    dst = tmp_path / "new_cache"
    _make_old_tree(src)

    result = _migrate_tree(src, dst, force=False)

    assert result.status == "skipped_synced_drive"
    assert result.files_moved == 0
    assert not (dst / "data" / "a.parquet").exists()


# ---------------------------------------------------------------------------
# Parametrized synced-drive matrix
# ---------------------------------------------------------------------------


def _setup_synced_marker(tmp_path: Path, marker_name: str) -> Path:
    """Create the named synced-drive marker under *tmp_path* and return the src dir.

    All callers also patch ``Path.home()`` to return *tmp_path* so the
    ancestor walk stops at our tmp root instead of the real home directory.
    """
    if marker_name == "mobile_documents":
        # iCloud Mobile Documents — path must be under ~/Library/Mobile Documents.
        mobile_docs = tmp_path / "Library" / "Mobile Documents"
        mobile_docs.mkdir(parents=True, exist_ok=True)
        src = mobile_docs / "data"
        src.mkdir(parents=True, exist_ok=True)
        return src

    if marker_name in (".dropbox", ".dropbox.cache", "com.apple.iCloud"):
        (tmp_path / marker_name).touch()
        src = tmp_path / "data"
        src.mkdir(parents=True, exist_ok=True)
        return src

    if marker_name in ("OneDrive", "OneDrive - Personal", "OneDrive - Some Tenant"):
        onedrive_dir = tmp_path / marker_name
        onedrive_dir.mkdir(parents=True, exist_ok=True)
        src = onedrive_dir / "data"
        src.mkdir(parents=True, exist_ok=True)
        return src

    raise ValueError(f"Unknown marker fixture: {marker_name!r}")


@pytest.mark.parametrize("marker_name", _SYNCED_DRIVE_MARKERS)
def test_synced_drive_matrix(tmp_path: Path, marker_name: str) -> None:
    """Each synced-drive marker triggers skipped_synced_drive status."""
    src = _setup_synced_marker(tmp_path, marker_name)
    _make_old_tree(src)
    dst = tmp_path / "new_cache"

    with patch("oda_data.cache._migrate.Path.home", return_value=tmp_path):
        result = _migrate_tree(src, dst, force=False)

    assert result.status == "skipped_synced_drive", (
        f"Expected skipped_synced_drive for marker={marker_name!r}, "
        f"got {result.status!r}: {result.message}"
    )
    assert result.files_moved == 0


# ---------------------------------------------------------------------------
# force=True bypasses synced-drive guard
# ---------------------------------------------------------------------------


def test_force_true_bypasses_synced_drive_guard(tmp_path: Path) -> None:
    """migrate(force=True) migrates even when a synced-drive marker is present."""
    (tmp_path / ".dropbox").touch()
    src = tmp_path / "data"
    dst = tmp_path / "new_cache"
    _make_old_tree(src)

    with patch("oda_data.cache._migrate.Path.home", return_value=tmp_path):
        result = _migrate_tree(src, dst, force=True)

    assert result.status == "migrated"
    assert result.files_moved == 3


# ---------------------------------------------------------------------------
# Breadcrumb
# ---------------------------------------------------------------------------


def test_breadcrumb_written(tmp_path: Path) -> None:
    """Migration writes a .migrated_to breadcrumb at the old root."""
    src = tmp_path / "old"
    dst = tmp_path / "new"
    _make_old_tree(src)

    result = _migrate_tree(src, dst, force=False, is_cross_volume=lambda *_: False)

    assert result.status == "migrated"
    breadcrumb = src / ".migrated_to"
    assert breadcrumb.exists(), "breadcrumb file must exist after migration"
    contents = breadcrumb.read_text().strip()
    assert contents == str(dst.resolve())


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------


def test_idempotent_via_breadcrumb(tmp_path: Path, monkeypatch) -> None:
    """Second migrate() call returns skipped_already_migrated for migrated roots."""
    # Build an old cache tree and mark it as already migrated.
    old_root = tmp_path / ".raw_data"
    _make_old_tree(old_root)
    new_root = tmp_path / "cache"
    (old_root / ".migrated_to").write_text(f"{new_root.resolve()}\n")

    # Redirect the scanner to only find our tmp tree.
    monkeypatch.setattr(
        "oda_data.cache._migrate._old_cache_paths_to_scan", lambda: [old_root]
    )
    monkeypatch.setattr("oda_data.cache._migrate.oda_data_cache_root", lambda: new_root)

    results = migrate()

    assert len(results) == 1
    assert results[0].status == "skipped_already_migrated"


# ---------------------------------------------------------------------------
# Disk-full
# ---------------------------------------------------------------------------


def test_disk_full_logs_warning(tmp_path: Path, caplog) -> None:
    """Disk-full OSError (errno 28) logs a WARNING and does not propagate."""
    src = tmp_path / "old"
    dst = tmp_path / "new"
    _make_old_tree(src)

    disk_full = OSError(errno.ENOSPC, "No space left on device")

    # The "oda_data" logger has propagate=False, so caplog's root handler doesn't
    # see records on its descendant loggers. Attach caplog.handler directly.
    _od_logger = logging.getLogger("oda_data")
    _od_logger.addHandler(caplog.handler)
    try:
        with (
            caplog.at_level(logging.WARNING),
            patch("oda_data.cache._migrate.shutil.move", side_effect=disk_full),
        ):
            result = _migrate_tree(
                src, dst, force=False, is_cross_volume=lambda *_: False
            )

        assert result.status == "skipped_disk_full"
        assert result.files_moved == 0
        assert any("Disk full" in r.message for r in caplog.records)
    finally:
        _od_logger.removeHandler(caplog.handler)

    # Both trees should still be present (old has data, new may be empty dir).
    src_files = list(src.rglob("*.parquet"))
    assert src_files, "source files must remain after disk-full"


# ---------------------------------------------------------------------------
# migrate() returns list[MigrationResult]
# ---------------------------------------------------------------------------


def test_migrate_returns_list_of_results(tmp_path: Path, monkeypatch) -> None:
    """migrate() returns a list of MigrationResult instances."""
    old1 = tmp_path / ".raw_data"
    old2 = tmp_path / "Library" / "Caches" / "oda-reader"
    for d in (old1, old2):
        _make_old_tree(d)

    new_root = tmp_path / "cache"

    monkeypatch.setattr(
        "oda_data.cache._migrate._old_cache_paths_to_scan", lambda: [old1, old2]
    )
    monkeypatch.setattr("oda_data.cache._migrate.oda_data_cache_root", lambda: new_root)

    results = migrate()

    assert len(results) == 2
    assert all(isinstance(r, MigrationResult) for r in results)


# ---------------------------------------------------------------------------
# Auto-migration laziness
# ---------------------------------------------------------------------------


def test_auto_migration_lazy(monkeypatch, tmp_path) -> None:
    """migrate() is not called at import; called exactly once on first cache access."""
    import oda_data.cache.config as cfg

    migrate_calls: list[bool] = []

    def fake_auto_migrate() -> None:
        """Drop-in for _auto_migrate_once that records invocations."""
        if cfg._AUTO_MIGRATED:
            return
        cfg._AUTO_MIGRATED = True
        migrate_calls.append(False)

    # Reset flag and install the spy.
    monkeypatch.setattr(cfg, "_AUTO_MIGRATED", False)
    monkeypatch.setattr(cfg, "_auto_migrate_once", fake_auto_migrate)

    # migrate should not have been called yet (import alone does not trigger).
    assert migrate_calls == []

    # First call to _auto_migrate_once executes migration.
    cfg._auto_migrate_once()
    assert len(migrate_calls) == 1

    # Second call is a no-op — flag is already True.
    cfg._auto_migrate_once()
    assert len(migrate_calls) == 1


# ---------------------------------------------------------------------------
# F-3: _auto_migrate_once exception policy
# ---------------------------------------------------------------------------


def _raises_oserror(*, force: bool = False) -> None:
    raise OSError("boom")


def _raises_runtime(*, force: bool = False) -> None:
    raise RuntimeError("downgrade")


def test_auto_migrate_once_swallows_oserror(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """_auto_migrate_once swallows OSError from migrate and logs a WARNING.

    Patches the source of the local import so the monkeypatch is effective
    regardless of when the local 'from ... import migrate as _do_migrate'
    executes inside _auto_migrate_once.
    """
    import oda_data.cache.config as cfg

    # Explicit reset — guards against flag leakage until Slice C autouse lands.
    monkeypatch.setattr(cfg, "_AUTO_MIGRATED", False)
    monkeypatch.setattr("oda_data.cache._migrate.migrate", _raises_oserror)

    # The "oda_data" logger has propagate=False; attach caplog.handler directly.
    _od_logger = logging.getLogger("oda_data")
    _od_logger.addHandler(caplog.handler)
    try:
        with caplog.at_level(logging.WARNING):
            cfg._auto_migrate_once()  # must not raise

        warning_messages = [
            r.message for r in caplog.records if r.levelno == logging.WARNING
        ]
        assert any("auto-migration failed" in m for m in warning_messages), (
            f"Expected a WARNING containing 'auto-migration failed'; got: {warning_messages}"
        )
    finally:
        _od_logger.removeHandler(caplog.handler)


def test_auto_migrate_once_propagates_runtimeerror(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_auto_migrate_once propagates RuntimeError (downgrade-detection signal)."""
    import oda_data.cache.config as cfg

    # Explicit reset — guards against flag leakage until Slice C autouse lands.
    monkeypatch.setattr(cfg, "_AUTO_MIGRATED", False)
    monkeypatch.setattr("oda_data.cache._migrate.migrate", _raises_runtime)

    with pytest.raises(RuntimeError, match="downgrade"):
        cfg._auto_migrate_once()

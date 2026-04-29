# Caching in oda_data

This document covers how `oda_data` manages its cache from version 2.6 onward.
For quick recovery commands, skip to [Recovery from a corrupt cache](#recovery-from-a-corrupt-cache).

---

## Overview

Starting in `oda_data 2.6`, the cache layer is split along responsibility lines:

- **`oda_reader`** owns the *raw bytes* layer: zip archives downloaded from the OECD
  live in `oda_reader`'s raw cache.
- **`oda_data`** owns the *cleaned parquet* layer: filtered, column-renamed DataFrames
  ready for analysis live in `oda_data`'s bulk and query caches.

Both layers are reachable through a single, version-stable public API at
`oda_data.cache.*`. You do not need to interact with `oda_reader`'s cache directly
unless you are a standalone `oda_reader` user (see
[Standalone oda_reader users](#standalone-oda_reader-users)).

---

## Cache locations per OS

The default cache root is determined by
[`platformdirs`](https://github.com/platformdirs/platformdirs) and is
version-segmented so that installs of different `oda_data` versions use separate
directories automatically.

| OS      | Default cache root                                                               |
|---------|----------------------------------------------------------------------------------|
| macOS   | `~/Library/Caches/oda-data/<version>/`                                           |
| Linux   | `~/.cache/oda-data/<version>/`                                                   |
| Windows | `C:\Users\<user>\AppData\Local\oda-data\oda-data\Cache\<version>\`               |

Inside that root, four subdirectories hold the respective cache scopes:

| Subdirectory  | Scope     | Contents                                         |
|---------------|-----------|--------------------------------------------------|
| `bulk_cache/` | `bulk`    | Cleaned parquet files extracted from OECD zips   |
| `query_cache/`| `query`   | Filter-specific parquet slices                   |
| `http/`       | `http`    | HTTP-level response cache (managed by oda_reader)|
| `raw/`        | `raw`     | Raw OECD zip archives (managed by oda_reader)    |

---

## Configuring the cache root

### Environment variable

Set `ODA_DATA_CACHE_DIR` before starting Python (or before the first cache-touching
call in a session) to override the default location:

```bash
export ODA_DATA_CACHE_DIR=/data/shared/oda-cache
```

### Python API

```python
import oda_data

oda_data.set_cache_root("/data/shared/oda-cache")
```

`set_cache_root` takes priority over `ODA_DATA_CACHE_DIR`. Both are respected lazily
on the first cache access, so they work even if called after `import oda_data`.

### Note on `set_data_path`

`set_data_path()` governs only *data outputs* (parquet exports) as of 2.6. It no
longer controls the cache location. Calling it emits a one-time `DeprecationWarning`
pointing at `set_cache_root` and `ODA_DATA_CACHE_DIR`. The shim is preserved through
`oda_data 2.x` and removed in `3.0`.

---

## The Python API

All public cache operations live under `oda_data.cache`. Import the namespace once
and call functions directly:

```python
from oda_data import cache
```

`Scope` is one of `"all"`, `"bulk"`, `"query"`, `"http"`, `"raw"`. Every function
that accepts a `scope` argument raises `ValueError` for unknown values.

### `cache.path(scope="all") -> Path`

Return the cache root directory, or the subdirectory for a specific scope.

```python
cache.path()          # e.g. ~/Library/Caches/oda-data/2.6.0/
cache.path("bulk")    # e.g. ~/Library/Caches/oda-data/2.6.0/oda-data/bulk_cache/
cache.path("raw")     # e.g. ~/Library/Caches/oda-data/2.6.0/oda-reader/raw/
```

### `cache.entries() -> dict[Scope, list[CacheRecord]]`

Return all cached files grouped by scope. Each `CacheRecord` has `key`, `path`,
`size_bytes`, `age_days`, `version`, and `scope` fields.

```python
for scope, records in cache.entries().items():
    for r in records:
        print(r.scope, r.key, r.size_bytes, r.age_days)
```

`"all"` is never a key in the returned dict — only the four concrete scopes
(`"bulk"`, `"query"`, `"http"`, `"raw"`) appear.

### `cache.clear(scope="all", *, blocking=True) -> dict[Scope, int | None]`

Delete cached files in the named scope. Returns the count of files deleted per
scope. For the `"bulk"` and `"query"` scopes (which `oda_data` writes under a
`FileLock`), the value is `None` if the scope was skipped because another process
held the write lock and `blocking=False` was passed. The `"http"` and `"raw"`
scopes always return an `int` — see the
[Multi-process behavior](#multi-process-behavior) section for why.

```python
# Clear everything, waiting for any write lock (default):
cache.clear()

# Clear only the raw zip cache:
cache.clear("raw")

# Non-blocking: skip contended scopes instead of waiting:
result = cache.clear("bulk", blocking=False)
if result.get("bulk") is None:
    print("bulk cache is busy — try again later")
```

The distinction between `0` (scope was reachable but empty) and `None` (scope was
locked by another writer) is intentional; it applies only to the `"bulk"` and
`"query"` scopes, which `oda_data` manages under a `FileLock`.

### `cache.size() -> dict[Scope, int]`

Return on-disk usage in bytes per scope (the four concrete scopes; not `"all"`).

```python
sizes = cache.size()
for scope, nbytes in sizes.items():
    print(f"{scope}: {nbytes / 2**20:.1f} MB")
```

As a side effect, if the combined size of the current cache root **and** any
sibling version directories (caches from older `oda_data` installs) exceeds 5 GB,
one `WARNING` is logged per process with a pointer to the older directories and the
`cache.clear(scope="raw")` recovery hint.

### `cache.invalidate(dataset: type[Source] | str) -> None`

Remove bulk and query cache entries for a single dataset. The argument can be
either the class itself or its name as an exact-match string (case-sensitive).
Raises `ValueError` for unrecognised names.

**Blocking semantics:** `cache.invalidate` acquires the bulk and query write locks
(60-second timeout each) so the unlink sequence cannot race with concurrent writers.
Do not call `cache.invalidate` from inside a `BulkCacheManager` or
`QueryCacheManager` write context — the caller would deadlock against itself.
On `filelock.Timeout`, retry after the current writer completes; persistent
timeouts may require a manual `cache.clear()`.

```python
from oda_data import cache, CRSData

# By class:
cache.invalidate(CRSData)

# By name string (exact, case-sensitive):
cache.invalidate("CRSData")
```

### `cache.enable_cache(scope="all")` / `cache.disable_cache(scope="all")`

Toggle caching for a specific scope, or all scopes at once. Disabling a scope
causes reads to bypass the cache layer for that scope until it is re-enabled.
Useful in tests or when disk space is constrained.

```python
cache.disable_cache("bulk")   # subsequent reads skip the bulk parquet cache
# ... do work ...
cache.enable_cache("bulk")    # restore normal behaviour
```

**Note:** `"http"` and `"raw"` share a single underlying enable/disable toggle
managed by `oda_reader`. Enabling or disabling either scope toggles both: for
example, `cache.disable_cache("http")` will also disable `"raw"`, and vice versa.
`oda_data` mirrors this coupling in its own per-scope flags so `is_scope_enabled`
reports a consistent state on both sides. To toggle only the `oda_data`-owned
scopes without affecting `oda_reader`, use `"bulk"` or `"query"` explicitly.

### `cache.migrate(*, force=False) -> list[MigrationResult]`

Manually re-run migration from pre-2.6 cache layouts (normally triggered
automatically on the first cache-touching call). Pass `force=True` to override
the synced-drive guard. Returns one `MigrationResult` per detected source root.

```python
results = cache.migrate(force=True)
for r in results:
    print(r.source, r.status, r.files_moved, r.bytes_moved)
```

### Scope reference

| Scope   | Owner        | What it holds                                                              |
|---------|--------------|----------------------------------------------------------------------------|
| `all`   | —            | Shorthand for every scope at once. Not a key in `entries()` or `size()`.  |
| `bulk`  | `oda_data`   | Cleaned parquet files extracted from OECD bulk zips.                       |
| `query` | `oda_data`   | Filter-specific parquet slices derived from bulk files.                    |
| `http`  | `oda_reader` | HTTP-level response cache.                                                 |
| `raw`   | `oda_reader` | Raw OECD zip archives before extraction.                                   |

**Note:** `"http"` and `"raw"` are independent on disk but share a single
enable/disable toggle (managed by `oda_reader`); see the
`cache.enable_cache` / `cache.disable_cache` section below for the implications.

When troubleshooting corrupt downloads, start with `cache.clear("raw")` — it
removes the raw zip archives without touching the cleaned parquet files. Clearing
`"bulk"` or `"query"` forces parquet re-extraction without re-downloading the
underlying zip.

### `read(refresh=True)` — per-call bypass

Pass `refresh=True` to any dataset read call to bypass the bulk cache and
re-download fresh data on that specific call. This is equivalent to calling
`cache.invalidate(dataset)` before reading, but scoped to the single call and
without permanently removing the cache entry.

```python
from oda_data import CRSData

crs = CRSData(years=[2023])
df = crs.read(using_bulk_download=True, refresh=True)
```

This works on all dataset classes: `CRSData`, `DAC1Data`, `DAC2AData`,
`MultiSystemData`, and `AidDataData`.

---

## Migration from older versions

On the first cache-touching call after upgrading to `oda_data 2.6`, the package
automatically detects pre-2.6 cache trees in the following locations and migrates
them to the new layout:

- `<cwd>/.raw_data/` (the old CWD-relative default)
- `~/Library/Caches/oda-reader/<old-version>/` (macOS)
- `~/.cache/oda-reader/<old-version>/` (Linux)
- `%LOCALAPPDATA%\oda-reader\Cache\` (Windows)

Migration rules:

- **Same-volume**: files are moved via `shutil.move` (fast, no extra disk space needed).
- **Cross-volume**: files are copied with `shutil.copy2`, size-verified, then the
  original is deleted. A progress message is logged because this can take several
  minutes for large caches (~1 GB).
- **Synced drive** (Dropbox, iCloud Drive, OneDrive): migration is skipped and a
  clear message is logged. The old cache remains readable for that session. Run
  `oda_data.cache.migrate(force=True)` in Python to override the guard.
- **Disk full** (`errno 28`): migration is skipped and a `WARNING` is logged. Both
  old and new locations are left intact.

After a successful migration, a `.migrated_to` breadcrumb file is written at the
old root containing the absolute new path. Running `oda_data 2.5.x` against a
migrated tree will raise a `RuntimeError` directing you to upgrade.

### Manual migration

```python
from oda_data import cache

# Re-run migration with the synced-drive guard disabled:
results = cache.migrate(force=True)
for r in results:
    print(r.source, r.status, r.files_moved, r.bytes_moved)
```

`cache.migrate()` returns a `list[MigrationResult]`, one per detected source root.
Each `MigrationResult` has `source`, `status`, `files_moved`, `bytes_moved`, and
`message` fields.

---

## Recovery from a corrupt cache

If `oda_data` raises `BulkPayloadCorrupt` or you see stale/corrupt data, clear the
raw cache and retry:

```python
from oda_data import cache, CRSData

# Wipe the raw zip cache managed by oda_reader:
cache.clear("raw")
# Or wipe everything: cache.clear("all")

# Re-read normally — the zip will be re-downloaded:
df = CRSData(years=[2023]).read(using_bulk_download=True)
```

`BulkPayloadCorrupt` is raised when a freshly downloaded zip fails `is_zipfile()` or
`ZipFile.testzip()` integrity checks. The exception carries `.path` (the corrupt
file's location) and `.reason` (which check failed). `oda_data` automatically clears
the bad entry and retries the download once; the exception only surfaces to the caller
if both the original download and the retry fail.

If you want to skip the raw cache entirely for a single call (useful when the network
feed may be temporarily corrupt but you do not want to wipe the whole cache), pass
`use_raw_cache=False` to the underlying `oda_reader` bulk functions directly, or call
`read` with `refresh=True` to bypass at the `oda_data` level:

```python
# Skip the raw zip cache for this call only; no disk footprint, re-downloads every time:
from oda_reader import bulk_download_crs
bulk_download_crs(save_to_path="/tmp/crs", use_raw_cache=False)
```

---

## Multi-process behavior

`oda_data` uses `filelock.FileLock` to coordinate cache writes across processes.
Concurrent reads are always safe because readers never hold a write lock.

Write coordination:

- **`BulkCacheManager`** acquires the lock before writing a new zip download. If
  two notebooks start `crs.read(using_bulk_download=True)` at the same time, the
  second blocks until the first completes and then reads from the already-written
  cache entry — only one network download occurs.
- **`cache.clear(blocking=True)`** (the default): acquires the lock and waits up
  to 1200 s for any in-progress write to finish.
- **`cache.clear(blocking=False)`**: skips scopes under contention instead of
  blocking. Returns `None` for each contended `"bulk"` / `"query"` scope (rather
  than `0`, which means the scope was reachable but empty). The `"http"` and
  `"raw"` scopes always return an `int` — `oda_reader` does not expose a public
  lock path for those caches, so contention cannot be detected; every reachable
  file is unlinked unconditionally. This distinction matters when the caller
  needs to confirm whether a clear was actually attempted.

Crash recovery: if a process is SIGKILLed mid-write, the incomplete file is left
at a `.tmp-<host>-<pid>` path. On the next `CacheManager` initialisation (which
happens at the start of every read session), any `*.tmp-*` file with an mtime
older than 24 hours is automatically removed. The `<host>` component in the suffix
prevents PID collisions on NFS or other shared mounts across multiple machines.

---

## Standalone `oda_reader` users

Users who import `oda_reader` directly without `oda_data` can continue to use the
four legacy helpers unchanged:

```python
import oda_reader

oda_reader.set_cache_dir("/my/cache")
oda_reader.clear_cache()
oda_reader.enable_cache()
oda_reader.disable_cache()
```

These functions are fully functional and will not be removed until `oda_reader 2.0`.

**If `oda_data` is also imported in the same process**, each of these functions emits
a one-time `DeprecationWarning` (once per function per session) directing you to the
equivalent `oda_data.cache.*` call:

| Legacy call                         | `oda_data` equivalent                    |
|-------------------------------------|------------------------------------------|
| `oda_reader.set_cache_dir(path)`    | `oda_data.set_cache_root(path)`          |
| `oda_reader.clear_cache()`          | `oda_data.cache.clear("all")`            |
| `oda_reader.enable_cache()`         | `oda_data.cache.enable_cache("all")`     |
| `oda_reader.disable_cache()`        | `oda_data.cache.disable_cache("all")`    |

Standalone `oda_reader` users who do **not** also import `oda_data` will never see
these warnings. The shims are preserved through `oda_reader 1.x` and removed in `2.0`.

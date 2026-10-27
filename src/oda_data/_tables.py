"""DuckDB views over the OECD tables that oda-reader's ``scan`` serves.

``OecdTables`` implements ``_model.Tables`` for one ``Context``. It holds one
in-memory DuckDB connection. The first call that needs a table scans it with
oda-reader, checks its release against the Context's pins, and creates a view
named after the table that exposes the logical columns in ``VOCABULARY``. The
table then keeps that release for the life of the Context, so every query and
the conversion factors read the same files.
"""

from __future__ import annotations

import threading
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, get_args

from oda_data._errors import (
    DataUnavailableError,
    OfflineCacheMissError,
    ReleaseMismatchError,
)
from oda_data._model import VOCABULARY, TableSpec
from oda_data._provenance import Release
from oda_data._types import TableName

if TYPE_CHECKING:
    import duckdb
    from oda_reader import ScannedTable

TABLES: tuple[TableName, ...] = get_args(TableName.__value__)


class OecdTables:
    """Table views for one ``Context``, following ``_model.Tables``.

    Args:
        cache_dir: The oda-reader cache root, or ``None`` for oda-reader's
            own (``ODA_READER_CACHE_DIR`` or the platform default).
        offline: Read only cached tables and make no network request.
        releases: Release tokens to hold tables to, keyed by table. A pinned
            table is served from the cache when the cached copy carries the
            pinned token, even online.

    Raises:
        ValueError: If ``releases`` names a table that does not exist.
    """

    def __init__(
        self,
        *,
        cache_dir: Path | None = None,
        offline: bool = False,
        releases: Mapping[str, str] | None = None,
    ) -> None:
        pins = dict(releases or {})
        unknown = sorted(set(pins) - set(TABLES))
        if unknown:
            raise ValueError(
                f"releases names unknown tables {unknown}; use one of {list(TABLES)}"
            )
        self._cache_dir = cache_dir
        self._offline = offline
        self._pins = pins
        self._scanned: dict[TableName, ScannedTable] = {}
        self._lock = threading.Lock()
        self._connection: duckdb.DuckDBPyConnection | None = None

    def cursor(self, tables: tuple[TableName, ...]) -> duckdb.DuckDBPyConnection:
        """Return a new cursor with a view for each of ``tables``.

        Only the tables named are scanned. Views of tables scanned earlier
        in the Context stay visible.

        Raises:
            OfflineCacheMissError: Offline, and a table is not cached.
            ReleaseMismatchError: A pinned release differs from the cache.
            DataUnavailableError: oda-reader could not fetch a table.
        """
        with self._lock:
            for table in tables:
                self._ensure(table)
            return self._open().cursor()

    def path(self, table: TableName) -> Path:
        """Return the Parquet file behind ``table``'s view, scanning it if needed."""
        with self._lock:
            return self._ensure(table).path

    def release(self, table: TableName) -> Release:
        """Return the release of ``table`` the views read, scanning it if needed."""
        with self._lock:
            scanned = self._ensure(table)
        return Release(
            token=scanned.release,
            downloaded=scanned.downloaded,
            label_release=scanned.label_release,
        )

    def close(self) -> None:
        """Close the DuckDB connection. Cursors taken from it stop working.

        A later call opens a new connection and scans again.
        """
        with self._lock:
            if self._connection is not None:
                self._connection.close()
                self._connection = None
            self._scanned.clear()

    def _open(self) -> duckdb.DuckDBPyConnection:
        if self._connection is None:
            import duckdb

            self._connection = duckdb.connect()
        return self._connection

    def _ensure(self, table: TableName) -> ScannedTable:
        """Scan ``table`` and create its view, once per Context. Hold the lock."""
        if (scanned := self._scanned.get(table)) is not None:
            return scanned
        if table not in VOCABULARY:
            raise ValueError(f"unknown table {table!r}; use one of {list(TABLES)}")

        scanned = self._scan(table)
        pin = self._pins.get(table)
        if pin is not None and scanned.release != pin:
            raise ReleaseMismatchError(
                f"The {table} table pinned to release {pin!r} is at release "
                f"{scanned.release!r} in the cache"
            )
        self._open().execute(_view_sql(VOCABULARY[table], scanned))
        self._scanned[table] = scanned
        return scanned

    def _scan(self, table: TableName) -> ScannedTable:
        """Scan ``table``, preferring a cached copy that carries the pinned release."""
        import oda_reader
        from oda_reader import BulkFileNotCachedError, BulkPayloadCorruptError
        from pyarrow import ArrowException
        from readerkit import ReaderkitError

        pin = self._pins.get(table)
        try:
            if pin is not None and not self._offline:
                try:
                    cached = oda_reader.scan(
                        table, cache_dir=self._cache_dir, offline=True
                    )
                except BulkFileNotCachedError:
                    pass
                else:
                    if cached.release == pin:
                        return cached
            return oda_reader.scan(
                table, cache_dir=self._cache_dir, offline=self._offline
            )
        except BulkFileNotCachedError as error:
            raise OfflineCacheMissError(
                f"The context is offline and the cache holds no {table} table. Run "
                "once online, or register a browser download with "
                f"oda_reader.register_bulk_file(path, dataset={table!r})"
            ) from error
        # BulkDownloadHTTPError is a ConnectionError, and network and disk
        # failures are OSErrors. ReaderkitError covers oda-reader's transport
        # and cache-folder failures, ArrowException a CSV that fails to
        # convert, and RuntimeError a bulk file another process replaced
        # while it was being converted.
        except (
            ArrowException,
            BulkPayloadCorruptError,
            OSError,
            ReaderkitError,
            RuntimeError,
        ) as error:
            raise DataUnavailableError(
                f"oda-reader could not provide the {table} table: {error}"
            ) from error


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _view_sql(spec: TableSpec, scanned: ScannedTable) -> str:
    """The ``CREATE VIEW`` statement mapping ``scanned``'s columns to ``spec``.

    Raises:
        DataUnavailableError: If the file lacks a column the spec maps.
    """
    missing = sorted({column.source for column in spec.columns} - set(scanned.columns))
    if missing:
        raise DataUnavailableError(
            f"The {spec.table} Parquet file at {scanned.path} lacks columns "
            f"{missing}; oda-reader's columns no longer match oda-data's"
        )

    select = [
        f"CAST({_quote(column.source)} AS {column.type}) AS {_quote(column.name)}"
        for column in spec.columns
    ]
    if spec.passthrough:
        # A raw column that shares a logical name (case-insensitively, as
        # DuckDB compares names) would shadow it, so it stays out.
        taken = {name.casefold() for name in spec.names}
        sources = {column.source for column in spec.columns}
        select += [
            _quote(name)
            for name in scanned.columns
            if name not in sources and name.casefold() not in taken
        ]
    path = str(scanned.path).replace("'", "''")
    return (
        f"CREATE OR REPLACE VIEW {_quote(spec.table)} AS SELECT "
        f"{', '.join(select)} FROM read_parquet('{path}')"
    )

"""Table views over oda-reader's ``scan``, on small registered bulk files.

Each test registers fixture files into a fresh oda-reader cache and blocks
the network, so every scan reads the registered copy.
"""

import socket
import zipfile
from dataclasses import replace
from pathlib import Path

import duckdb
import oda_reader
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from oda_data._errors import (
    DataUnavailableError,
    OfflineCacheMissError,
    ReleaseMismatchError,
)
from oda_data._model import CRS_MARKERS, VOCABULARY
from oda_data._provenance import Release
from oda_data._tables import OecdTables, _view_sql

_BASE = "https://webfs-dcd.oecd.org/files/dotStat"
_URLS = {
    "dac1": f"{_BASE}/DSD_DAC1/Table1_Data.zip",
    "dac2a": f"{_BASE}/DSD_DAC2/Table2a_Data.zip",
    "multisystem": f"{_BASE}/DSD_MULTI/MultiSystem%20entire%20dataset.zip",
    "crs": f"{_BASE}/DSD_CRS/CRS.parquet",
}
_TOKENS = {table: f"etag-{table}-1" for table in _URLS}

_CSV = {
    "dac1": (
        "DONOR,Donor,PART,Part,AIDTYPE,Aid type,FLOWS,Fund flows,AMOUNTTYPE,"
        "Amount type,TIME,Year,Value,Flags\n"
        "4,France,1,Official,1010,Net ODA,1140,Net Disbursements,A,"
        "Current prices,2022,2022,15000.5,\n"
        "4,France,1,Official,1010,Net ODA,1140,Net Disbursements,D,"
        "Constant prices,2022,2022,14000.0,\n"
    ),
    "dac2a": (
        "RECIPIENT,Recipient,DONOR,Donor,PART,Part,AIDTYPE,Aid type,DATATYPE,"
        "Amount type,TIME,Year,Value,Flags\n"
        "880,Samoa,4,France,1,Official,206,Net ODA,A,Current prices,2022,2022,1.5,\n"
    ),
    "multisystem": (
        "Year,DonorCode,DonorNameE,Amount,FlowType,AmountType,AidToOrThru,"
        "ChannelCode,RecipientCode,Purposecode,FlowCode,Aid_T\n"
        "2022,4,France,3.25,Disbursements,Current prices,Core contributions to,"
        "44001,998,99810,11,B02\n"
    ),
}


def _crs_table() -> pa.Table:
    columns = {
        "donor_code": [4],
        "recipient_code": [880],
        "year": [2022],
        "flow_code": [11],
        "bi_multi": [1],
        "category": [10],
        "finance_t": [110],
        "aid_t": ["C01"],
        "purpose_code": [12220],
        "channel_code": [11000],
        "keywords": ["#COVID-19"],
        "usd_commitment": [2.0],
        "usd_disbursement": [1.5],
        "usd_received": [0.25],
        "usd_grant_equiv": [1.4],
        # A raw column that shares a logical name stays out of the view.
        "modality": ["raw modality"],
        "project_title": ["Clinic"],
    }
    columns |= {marker: pa.array([None], pa.int64()) for marker in CRS_MARKERS}
    columns["gender"] = [1]
    return pa.table(columns)


def _zip(csv: str, folder: Path, name: str) -> Path:
    path = folder / name
    with zipfile.ZipFile(path, "w") as z:
        z.writestr(name.replace(".zip", ".csv"), csv)
    return path


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    """Refuse every socket, so a scan that tries the network fails."""

    def refuse(*_args, **_kwargs):
        raise OSError("network blocked in test")

    monkeypatch.setattr(socket, "getaddrinfo", refuse)
    monkeypatch.setattr(socket.socket, "connect", refuse)


@pytest.fixture
def cache_dir(tmp_path, monkeypatch):
    """A cache root with nothing in it. ``register`` fills it."""
    root = tmp_path / "cache"
    monkeypatch.setenv("ODA_READER_CACHE_DIR", str(root))
    oda_reader.reset_cache_dir()
    yield root
    oda_reader.reset_cache_dir()


@pytest.fixture
def register(cache_dir, tmp_path):
    """Register fixture bulk files for the named tables into ``cache_dir``."""

    def register(*tables: str) -> None:
        files = tmp_path / "files"
        files.mkdir(exist_ok=True)
        for table in tables:
            if table == "crs":
                path = files / "CRS.parquet"
                pq.write_table(_crs_table(), path)
            else:
                path = _zip(_CSV[table], files, f"{table}.zip")
            oda_reader.register_bulk_file(
                path, dataset=table, url=_URLS[table], version=_TOKENS[table]
            )

    return register


@pytest.fixture
def counted_scans(monkeypatch):
    """Count oda-reader scans per table."""
    calls: list[str] = []
    real = oda_reader.scan

    def scan(dataset, **kwargs):
        calls.append(dataset)
        return real(dataset, **kwargs)

    monkeypatch.setattr(oda_reader, "scan", scan)
    return calls


def _describe(cursor, table: str) -> dict[str, str]:
    return {row[0]: row[1] for row in cursor.execute(f"DESCRIBE {table}").fetchall()}


@pytest.mark.parametrize("table", ["dac1", "dac2a", "multisystem", "crs"])
def test_view_exposes_the_vocabulary_columns(register, cache_dir, table):
    register(table)
    tables = OecdTables(cache_dir=cache_dir, offline=True)

    columns = _describe(tables.cursor((table,)), table)

    spec = VOCABULARY[table]
    logical = {column.name: column.type for column in spec.columns}
    assert {name: columns[name] for name in logical} == logical
    if not spec.passthrough:
        assert set(columns) == set(logical)


def test_view_maps_converted_values(register, cache_dir):
    register("dac1", "multisystem")
    cursor = OecdTables(cache_dir=cache_dir, offline=True).cursor(
        ("dac1", "multisystem")
    )

    dac1 = cursor.execute(
        "SELECT provider, year, line, flow, amount_type, value FROM dac1 "
        "ORDER BY amount_type"
    ).fetchall()
    multisystem = cursor.execute(
        "SELECT provider, channel, sector, modality, aid_to_or_thru, value "
        "FROM multisystem"
    ).fetchall()

    assert dac1 == [
        (4, 2022, 1010, 1140, "A", 15000.5),
        (4, 2022, 1010, 1140, "D", 14000.0),
    ]
    assert multisystem == [(4, 44001, 99810, "B02", "Core contributions to", 3.25)]


def test_crs_passes_unmapped_columns_through(register, cache_dir):
    register("crs")
    cursor = OecdTables(cache_dir=cache_dir, offline=True).cursor(("crs",))

    row = cursor.execute("SELECT modality, project_title, sector FROM crs").fetchone()
    columns = _describe(cursor, "crs")

    assert row == ("C01", "Clinic", 12220)
    assert "aid_t" not in columns
    assert "donor_code" not in columns


def test_cursor_scans_only_the_tables_named(register, cache_dir, counted_scans):
    register("dac1")  # every other table is missing from the cache
    tables = OecdTables(cache_dir=cache_dir, offline=True)

    cursor = tables.cursor(("dac1",))

    assert counted_scans == ["dac1"]
    views = {
        row[0]
        for row in cursor.execute(
            "SELECT view_name FROM duckdb_views() WHERE NOT internal"
        ).fetchall()
    }
    assert views == {"dac1"}


def test_each_table_is_scanned_once_per_context(register, cache_dir, counted_scans):
    register("dac1")
    tables = OecdTables(cache_dir=cache_dir, offline=True)

    first = tables.cursor(("dac1",))
    tables.cursor(("dac1",))
    path = tables.path("dac1")
    release = tables.release("dac1")

    assert counted_scans == ["dac1"]
    assert first.execute("SELECT count(*) FROM dac1").fetchone() == (2,)
    assert path.suffix == ".parquet"
    assert pq.read_schema(path).names[0] == "donor_code"
    assert isinstance(release, Release)
    assert release.token == _TOKENS["dac1"]
    assert release.downloaded.tzinfo is not None


def test_release_carries_the_scan_label_release(register, cache_dir, monkeypatch):
    register("crs")
    real = oda_reader.scan

    def labelled(dataset, **kwargs):
        return replace(real(dataset, **kwargs), label_release="v20260803")

    monkeypatch.setattr(oda_reader, "scan", labelled)

    release = OecdTables(cache_dir=cache_dir, offline=True).release("crs")

    assert release.label_release == "v20260803"


def test_offline_cold_cache_raises_offline_cache_miss(cache_dir):
    tables = OecdTables(cache_dir=cache_dir, offline=True)

    with pytest.raises(OfflineCacheMissError) as caught:
        tables.cursor(("dac2a",))

    assert isinstance(caught.value.__cause__, oda_reader.BulkFileNotCachedError)


def test_online_fetch_failure_raises_data_unavailable(cache_dir):
    tables = OecdTables(cache_dir=cache_dir)

    with pytest.raises(DataUnavailableError) as caught:
        tables.path("dac1")

    assert caught.value.__cause__ is not None


def test_pinned_release_mismatch_raises(register, cache_dir):
    register("dac1")
    tables = OecdTables(cache_dir=cache_dir, offline=True, releases={"dac1": "other"})

    with pytest.raises(ReleaseMismatchError):
        tables.cursor(("dac1",))


@pytest.mark.parametrize("offline", [True, False])
def test_matching_pin_serves_the_cached_copy(register, cache_dir, offline):
    # Online, the network is blocked: the pin must be met from the cache.
    register("dac1")
    tables = OecdTables(
        cache_dir=cache_dir, offline=offline, releases={"dac1": _TOKENS["dac1"]}
    )

    assert tables.release("dac1").token == _TOKENS["dac1"]


def test_pins_must_name_known_tables(cache_dir):
    with pytest.raises(ValueError, match="unknown tables"):
        OecdTables(cache_dir=cache_dir, releases={"dac3": "x"})


def test_a_missing_source_column_raises_data_unavailable(tmp_path):
    path = tmp_path / "dac1.parquet"
    pq.write_table(pa.table({"donor_code": [4]}), path)
    scanned = oda_reader.ScannedTable(
        path=path,
        release="x",
        label_release=None,
        downloaded=None,  # unused by the view
        columns=("donor_code",),
    )

    with pytest.raises(DataUnavailableError, match="aidtype_code"):
        _view_sql(VOCABULARY["dac1"], scanned)


def test_close_closes_the_connection(register, cache_dir):
    register("dac1")
    tables = OecdTables(cache_dir=cache_dir, offline=True)
    cursor = tables.cursor(("dac1",))

    tables.close()

    with pytest.raises(Exception, match="closed"):
        cursor.execute("SELECT 1")
    assert tables.cursor(("dac1",)).execute("SELECT count(*) FROM dac1").fetchone() == (
        2,
    )


def test_paths_with_quotes_are_escaped(tmp_path):
    folder = tmp_path / "it's"
    folder.mkdir()
    path = folder / "dac1.parquet"
    pq.write_table(pa.table({"donor_code": [4]}), path)
    scanned = oda_reader.ScannedTable(
        path=path,
        release="x",
        label_release=None,
        downloaded=None,
        columns=("donor_code",),
    )
    spec = VOCABULARY["dac1"]
    narrow = type(spec)(spec.table, spec.columns[:1])
    connection = duckdb.connect()
    connection.execute(_view_sql(narrow, scanned))

    assert connection.execute("SELECT provider FROM dac1").fetchall() == [(4,)]

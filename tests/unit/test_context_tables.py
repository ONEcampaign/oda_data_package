"""A Context over real table views, on a registered DAC1 bulk file."""

import socket
import sys
import types
import zipfile
from pathlib import Path

import oda_reader
import pytest

from oda_data._context import Context
from oda_data._tables import OecdTables
from tests.unit.test_context import TOTAL_ODA, FakeFactors

_URL = "https://webfs-dcd.oecd.org/files/dotStat/DSD_DAC1/Table1_Data.zip"
_CSV = (
    "DONOR,Donor,PART,Part,AIDTYPE,Aid type,FLOWS,Fund flows,AMOUNTTYPE,"
    "Amount type,TIME,Year,Value,Flags\n"
    "4,France,1,Official,1010,Net ODA,1140,Net Disbursements,A,"
    "Current prices,2017,2017,15000.5,\n"
    "4,France,1,Official,1010,Net ODA,1140,Net Disbursements,D,"
    "Constant prices,2017,2017,14000.0,\n"
    "20001,DAC Countries,1,Official,1010,Net ODA,1140,Net Disbursements,A,"
    "Current prices,2017,2017,99000.0,\n"
)


@pytest.fixture(autouse=True)
def no_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*_args: object, **_kwargs: object) -> None:
        raise OSError("network blocked in test")

    monkeypatch.setattr(socket, "getaddrinfo", refuse)
    monkeypatch.setattr(socket.socket, "connect", refuse)


@pytest.fixture(autouse=True)
def registry(monkeypatch: pytest.MonkeyPatch) -> None:
    module = types.ModuleType("oda_data._registry")
    module.INDICATORS = {TOTAL_ODA.id: TOTAL_ODA}
    monkeypatch.setitem(sys.modules, "oda_data._registry", module)


@pytest.fixture
def cache_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    root = tmp_path / "cache"
    monkeypatch.setenv("ODA_READER_CACHE_DIR", str(root))
    oda_reader.reset_cache_dir()
    path = tmp_path / "dac1.zip"
    with zipfile.ZipFile(path, "w") as z:
        z.writestr("dac1.csv", _CSV)
    oda_reader.register_bulk_file(path, dataset="dac1", url=_URL, version="etag-1")
    yield root
    oda_reader.reset_cache_dir()


def test_a_context_reads_the_views_of_its_tables(cache_dir: Path) -> None:
    tables = OecdTables(cache_dir=cache_dir, offline=True)
    with Context._from_parts(tables, FakeFactors()) as ctx:
        q = ctx.query(TOTAL_ODA.id, years=2017)
        df = q.to_pandas()

    rows = list(zip(df["provider"], df["provider_name"], df["value"], strict=True))
    assert rows == [("4", "France", 15000.5)]
    assert q.provenance.tables["dac1"].token == "etag-1"


def test_a_context_takes_a_string_cache_dir(cache_dir: Path) -> None:
    ctx = Context(cache_dir=str(cache_dir), offline=True)

    assert ctx._tables.path("dac1").exists()
    ctx.close()

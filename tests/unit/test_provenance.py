"""Provenance records in Parquet schema metadata."""

import json
import pickle
from datetime import UTC, datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import oda_data
from oda_data._provenance import (
    FORMAT_VERSION,
    PROVENANCE_KEY,
    Provenance,
    Release,
    to_metadata,
    with_provenance,
)

RECORD = Provenance(
    oda_data="3.0.0",
    indicators={"oecd.dac1.total_oda": 1},
    tables={
        "dac1": Release(
            token='W/"0x8DF2539D20A32DF"',
            downloaded=datetime(2026, 10, 12, 9, 30, tzinfo=UTC),
        ),
        "crs": Release(
            token="etag-crs",
            downloaded=datetime(2026, 10, 1, tzinfo=UTC),
            label_release="v20260803",
        ),
    },
    pydeflate="2.7.0",
    resolvekit="0.1.13",
    reference_data="sha256:abc",
    defaults={"basis": "headline", "providers": "dac_countries"},
)


def _write(tmp_path, table: pa.Table) -> str:
    path = tmp_path / "result.parquet"
    pq.write_table(table, path)
    return str(path)


def test_round_trip_through_a_parquet_file(tmp_path):
    table = pa.Table.from_pandas(pd.DataFrame({"year": [2022], "value": [1.5]}))
    path = _write(tmp_path, with_provenance(table, RECORD))

    assert oda_data.provenance(path) == RECORD
    # pandas' own metadata survives beside the record.
    assert b"pandas" in pq.read_schema(path).metadata


def test_metadata_carries_the_format_version():
    document = json.loads(to_metadata(RECORD)[PROVENANCE_KEY])

    assert document["format"] == FORMAT_VERSION
    assert document["tables"]["dac1"] == {
        "token": 'W/"0x8DF2539D20A32DF"',
        "downloaded": "2026-10-12T09:30:00+00:00",
        "label_release": None,
    }
    assert document["tables"]["crs"]["label_release"] == "v20260803"


def test_a_format_1_file_without_label_release_still_reads(tmp_path):
    document = json.loads(to_metadata(RECORD)[PROVENANCE_KEY])
    for entry in document["tables"].values():
        del entry["label_release"]
    table = pa.table({"value": [1.0]}).replace_schema_metadata(
        {PROVENANCE_KEY: json.dumps(document).encode()}
    )

    read = oda_data.provenance(_write(tmp_path, table))

    assert read.tables["crs"].label_release is None
    assert read.tables["crs"].token == "etag-crs"


def test_a_file_without_a_record_raises(tmp_path):
    path = _write(tmp_path, pa.table({"value": [1.0]}))

    with pytest.raises(ValueError, match="no oda-data provenance"):
        oda_data.provenance(path)


def test_a_newer_format_raises(tmp_path):
    document = json.loads(to_metadata(RECORD)[PROVENANCE_KEY])
    document["format"] = FORMAT_VERSION + 1
    table = pa.table({"value": [1.0]}).replace_schema_metadata(
        {PROVENANCE_KEY: json.dumps(document).encode()}
    )

    with pytest.raises(ValueError, match="Upgrade oda-data"):
        oda_data.provenance(_write(tmp_path, table))


def test_record_is_read_only_and_pickles():
    with pytest.raises(TypeError):
        RECORD.indicators["x"] = 2

    assert pickle.loads(pickle.dumps(RECORD)) == RECORD


def test_public_names_point_at_the_provenance_module():
    assert oda_data.Release is Release
    assert oda_data.Provenance is Provenance

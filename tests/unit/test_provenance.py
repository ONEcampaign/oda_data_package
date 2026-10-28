"""Provenance records in Parquet schema metadata."""

import json
import pickle
from dataclasses import replace
from datetime import UTC, datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import oda_data
from oda_data._model import ResolvedCall, ResolvedGroup
from oda_data._provenance import (
    FORMAT_VERSION,
    PROVENANCE_KEY,
    Call,
    CallGroup,
    Provenance,
    Release,
    call_record,
    to_metadata,
    with_provenance,
)
from oda_data._units import constant, current

CALL = Call(
    indicator="oecd.crs.bilateral_oda",
    basis="gross_disbursement",
    years=(2022, 2023),
    by=("recipient", "year"),
    units=(current("USD"), constant("EUR", 2023)),
    allow_partial=True,
    filters={"provider": (4, 5, 12), "modality": ("B01", "C01")},
    groups=(
        CallGroup(
            dimension="provider",
            name="trio",
            label="Trio countries",
            shown=False,
            total=True,
            members={2022: (4, 5), 2023: (4, 5, 12)},
        ),
    ),
    default_providers=None,
    operation="records",
    columns=("project_title", "channel_name"),
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
    call=CALL,
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


def test_a_format_1_file_without_call_still_reads(tmp_path):
    document = json.loads(to_metadata(RECORD)[PROVENANCE_KEY])
    del document["call"]
    table = pa.table({"value": [1.0]}).replace_schema_metadata(
        {PROVENANCE_KEY: json.dumps(document).encode()}
    )

    read = oda_data.provenance(_write(tmp_path, table))

    assert read.call is None
    assert read.defaults == RECORD.defaults


def test_a_format_1_file_without_operation_or_columns_reads_as_get(tmp_path):
    document = json.loads(to_metadata(RECORD)[PROVENANCE_KEY])
    del document["call"]["operation"]
    del document["call"]["columns"]
    table = pa.table({"value": [1.0]}).replace_schema_metadata(
        {PROVENANCE_KEY: json.dumps(document).encode()}
    )

    read = oda_data.provenance(_write(tmp_path, table))

    assert read.call is not None
    assert (read.call.operation, read.call.columns) == ("get", ())


def test_a_format_1_file_without_a_group_label_still_reads(tmp_path):
    document = json.loads(to_metadata(RECORD)[PROVENANCE_KEY])
    del document["call"]["groups"][0]["label"]
    table = pa.table({"value": [1.0]}).replace_schema_metadata(
        {PROVENANCE_KEY: json.dumps(document).encode()}
    )

    read = oda_data.provenance(_write(tmp_path, table))

    assert read.call is not None
    assert read.call.groups[0].label is None
    assert read.call.groups[0].name == "trio"


def test_call_record_resolves_groups_and_sorts_codes():
    trio = ResolvedGroup(
        name="trio",
        dimension="provider",
        members={2023: frozenset({12, 4}), 2022: frozenset({5, 4})},
    )
    resolved = ResolvedCall(
        basis="gross_disbursement",
        units=(current("USD"),),
        by=("year",),
        years=(2022, 2023),
        members={"provider": frozenset({12, 5, 4}), "modality": frozenset({"C01"})},
        groups=(trio,),
        totals=(trio.key,),
        rows={"provider": frozenset({5}), "modality": frozenset({"C01"})},
    )

    labels = {trio.key: "Trio countries"}
    record = call_record(
        "x", resolved, filtered=["provider", "modality"], labels=labels, columns=None
    )

    # Only 5 was named outright; 4 and 12 feed the total and sit in groups.
    assert dict(record.filters) == {"modality": ("C01",), "provider": (5,)}
    assert record.groups == (
        CallGroup(
            "provider",
            "trio",
            "Trio countries",
            False,
            True,
            {2022: (4, 5), 2023: (4, 12)},
        ),
    )
    assert record.default_providers is None
    # The providers came from a default when the caller filtered none.
    defaulted = call_record(
        "x", resolved, filtered=["modality"], labels=labels, columns=None
    )
    assert defaulted.default_providers == (4, 5, 12)
    assert "provider" not in defaulted.filters


def test_call_record_tells_records_from_get_and_keeps_the_column_order():
    """The columns keep the call's order, which is their order in the
    result, so each call writes its own JSON."""
    resolved = ResolvedCall(
        basis="gross_disbursement", units=(current("USD"),), by=("year",), years=None
    )
    cases = {
        None: ("get", ()),
        (): ("records", ()),
        ("project_title", "channel_name"): (
            "records",
            ("project_title", "channel_name"),
        ),
        ("channel_name", "project_title"): (
            "records",
            ("channel_name", "project_title"),
        ),
    }

    records = {
        columns: call_record("x", resolved, filtered=[], labels={}, columns=columns)
        for columns in cases
    }

    assert {
        columns: (call.operation, call.columns) for columns, call in records.items()
    } == cases
    written = {
        to_metadata(replace(RECORD, call=call))[PROVENANCE_KEY]
        for call in records.values()
    }
    assert len(written) == len(cases)


def test_an_equal_record_writes_byte_identical_json():
    rebuilt = pickle.loads(pickle.dumps(RECORD))

    assert to_metadata(rebuilt) == to_metadata(RECORD)


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
    with pytest.raises(TypeError):
        CALL.filters["x"] = ()

    assert pickle.loads(pickle.dumps(RECORD)) == RECORD


def test_public_names_point_at_the_provenance_module():
    assert oda_data.Release is Release
    assert oda_data.Provenance is Provenance
    assert oda_data.Call is Call
    assert oda_data.CallGroup is CallGroup

"""The reviewed crosswalk and proxy tables the imputation joins against."""

import csv
import io
from collections.abc import Callable

import pyarrow as pa
import pytest

from oda_data._methods import channels
from oda_data._names import names, read_data

CROSSWALK = """provider_code,agency_code,provider_name,agency_name,channel_code,status,reason,method,reviewed
905,1,IDA,IDA,44002,mapped,,exact,true
905,1.0,IDA,IDA duplicate,44001,mapped,,exact,true
918,,EU Institutions,,42001,mapped,,exact,true
918,5,EU Institutions,MFA,,excluded,borrowing,manual,true
999,1,New,New,47000,mapped,,resolvekit,false
"""

PROXIES = """channel_code,channel_name,proxy_channel_code,proxy_type,fixed_purpose_code,rationale,reviewed
44007,MDRI,44002,parent_fund,,folds into IDA,true
41310,Peacekeeping,,fixed_purpose,15230,peacekeeping,true
46005,Asian Development Fund,46004,parent_fund,,unreviewed,false
"""


def test_crosswalk_keeps_reviewed_rows_once_per_reporter() -> None:
    table = channels.crosswalk_table(CROSSWALK).to_pylist()

    # The second 905/1 row (agency written 1.0) repeats a reporter, and 999 is
    # an unreviewed proposal; a missing agency joins as NO_AGENCY.
    assert table == [
        {"provider": 905, "agency": 1, "channel": 44002, "status": "mapped"},
        {
            "provider": 918,
            "agency": channels.NO_AGENCY,
            "channel": 42001,
            "status": "mapped",
        },
        {"provider": 918, "agency": 5, "channel": None, "status": "excluded"},
    ]


def test_proxies_keep_reviewed_rows() -> None:
    table = channels.proxy_table(PROXIES).to_pylist()

    assert table == [
        {
            "channel": 44007,
            "proxy_type": "parent_fund",
            "proxy_channel": 44002,
            "fixed_sector": None,
        },
        {
            "channel": 41310,
            "proxy_type": "fixed_purpose",
            "proxy_channel": None,
            "fixed_sector": 15230,
        },
    ]


@pytest.mark.parametrize(
    "read",
    [channels.crosswalk_table, channels.proxy_table],
    ids=["crosswalk", "proxies"],
)
def test_a_table_without_a_reviewed_column_is_refused(
    read: Callable[[str], pa.Table],
) -> None:
    with pytest.raises(ValueError, match="reviewed"):
        read("provider_code,agency_code,channel_code,status\n905,1,44002,mapped\n")


@pytest.mark.parametrize(
    "row",
    [
        "44007,MDRI,,parent_fund,,no parent,true",
        "41310,Peacekeeping,,fixed_purpose,,no purpose,true",
        "44007,MDRI,44002,peer,,unknown type,true",
    ],
)
def test_a_proxy_without_what_its_type_needs_is_refused(row: str) -> None:
    header = PROXIES.splitlines()[0]

    with pytest.raises(ValueError, match="proxy type"):
        channels.proxy_table(f"{header}\n{row}\n")


def test_bundled_tables_name_known_channels() -> None:
    known = set(names("channel"))
    crosswalk = channels.crosswalk().to_pylist()
    proxies = channels.proxies().to_pylist()

    mapped = {row["channel"] for row in crosswalk if row["status"] == "mapped"}
    lent = {row["proxy_channel"] for row in proxies} - {None}
    assert mapped | lent | {row["channel"] for row in proxies} <= known
    assert {row["status"] for row in crosswalk} == {"mapped", "excluded"}


def test_bundled_crosswalk_has_one_row_per_reporter() -> None:
    rows = list(csv.DictReader(io.StringIO(read_data(channels.CROSSWALK_FILE))))
    reporters = [(row["provider_code"], row["agency_code"]) for row in rows]

    assert len(reporters) == len(set(reporters))

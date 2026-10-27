"""The reviewed channel reference data the imputation reads.

Two bundled files decide where a multilateral's money goes:

- ``multilateral_channel_crosswalk.csv`` maps each CRS reporter, a provider
  code and an agency code, to the Multisystem channel its spending belongs to,
  or excludes it with a reason.
- ``channel_share_proxies.csv`` names, for a channel with no CRS spending of
  its own, a parent fund whose shares it borrows or one purpose code that
  takes its whole contribution.

Only rows marked ``reviewed`` take part. An unreviewed row is a proposal from
``scripts/refresh_channel_crosswalk.py`` that nobody has confirmed, so the pair
or channel it covers counts as unmapped. Both tables load as Arrow tables
that DuckDB joins against.
"""

from __future__ import annotations

import csv
import io
from functools import cache

import pyarrow as pa

from oda_data._names import read_data

CROSSWALK_FILE = "multilateral_channel_crosswalk.csv"
PROXY_FILE = "channel_share_proxies.csv"

#: The agency code that stands in for a missing one, so a CRS row with no
#: agency joins the crosswalk row that has none. No OECD agency uses -1.
NO_AGENCY = -1

#: The crosswalk as the imputation joins it: one row per CRS reporter.
CROSSWALK_SCHEMA = pa.schema(
    [
        ("provider", pa.int64()),
        ("agency", pa.int64()),
        ("channel", pa.int64()),
        ("status", pa.string()),
    ]
)

#: The proxy table as the imputation joins it: one row per channel.
PROXY_SCHEMA = pa.schema(
    [
        ("channel", pa.int64()),
        ("proxy_type", pa.string()),
        ("proxy_channel", pa.int64()),
        ("fixed_sector", pa.int64()),
    ]
)

PROXY_TYPES = ("parent_fund", "fixed_purpose")


def _int(text: str) -> int | None:
    """A code from a CSV cell, which may be empty or written as ``1.0``."""
    text = text.strip()
    return int(float(text)) if text else None


def reviewed_rows(text: str, *, source: str) -> list[dict[str, str]]:
    """The rows of a reference CSV marked ``reviewed`` true.

    Raises:
        ValueError: The file has no ``reviewed`` column, so proposals cannot
            be told apart from reviewed rows.
    """
    reader = csv.DictReader(io.StringIO(text))
    if "reviewed" not in (reader.fieldnames or ()):
        raise ValueError(
            f"{source} has no 'reviewed' column, so unreviewed proposals cannot "
            "be told apart from reviewed rows"
        )
    return [row for row in reader if row["reviewed"].strip().lower() == "true"]


def crosswalk_table(text: str, *, source: str = CROSSWALK_FILE) -> pa.Table:
    """The reviewed crosswalk rows, first row per reporter.

    Args:
        text: The CSV text, with ``provider_code``, ``agency_code``,
            ``channel_code``, ``status`` (``mapped`` or ``excluded``) and
            ``reviewed`` columns.
        source: The file name for error messages.
    """
    rows: dict[tuple[int, int], dict[str, object]] = {}
    for row in reviewed_rows(text, source=source):
        provider = int(float(row["provider_code"]))
        agency = _int(row["agency_code"])
        key = (provider, NO_AGENCY if agency is None else agency)
        rows.setdefault(
            key,
            {
                "provider": key[0],
                "agency": key[1],
                "channel": _int(row["channel_code"]),
                "status": row["status"].strip(),
            },
        )
    return pa.Table.from_pylist(list(rows.values()), schema=CROSSWALK_SCHEMA)


def proxy_table(text: str, *, source: str = PROXY_FILE) -> pa.Table:
    """The reviewed proxy rows.

    Args:
        text: The CSV text, with ``channel_code``, ``proxy_channel_code``,
            ``proxy_type`` (``parent_fund`` or ``fixed_purpose``),
            ``fixed_purpose_code`` and ``reviewed`` columns.
        source: The file name for error messages.

    Raises:
        ValueError: A row has another proxy type, or lacks the code its type
            needs.
    """
    rows = []
    for row in reviewed_rows(text, source=source):
        kind = row["proxy_type"].strip()
        found = {
            "channel": _int(row["channel_code"]),
            "proxy_type": kind,
            "proxy_channel": _int(row["proxy_channel_code"]),
            "fixed_sector": _int(row["fixed_purpose_code"]),
        }
        needed = "proxy_channel" if kind == "parent_fund" else "fixed_sector"
        if kind not in PROXY_TYPES or found[needed] is None:
            raise ValueError(
                f"{source}: channel {found['channel']} has proxy type {kind!r} "
                f"without its {needed}; the types are {', '.join(PROXY_TYPES)}"
            )
        rows.append(found)
    return pa.Table.from_pylist(rows, schema=PROXY_SCHEMA)


@cache
def crosswalk() -> pa.Table:
    """The bundled crosswalk, reviewed rows only."""
    return crosswalk_table(read_data(CROSSWALK_FILE))


@cache
def proxies() -> pa.Table:
    """The bundled proxy table, reviewed rows only."""
    return proxy_table(read_data(PROXY_FILE))

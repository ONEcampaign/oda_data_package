"""Provenance records and their Parquet metadata.

A ``Provenance`` names the package versions, indicator versions, and table
releases behind a result. ``Query.to_parquet`` stores it as JSON under
``PROVENANCE_KEY`` in the file's schema metadata, and ``provenance(path)``
reads it back. The JSON carries ``"format": FORMAT_VERSION`` so a later
release can change the layout and still read older files. Adding an optional
key leaves the format at 1, because a reader of format 1 ignores keys it does
not know, and ``from_metadata`` reads ``label_release`` as ``None`` when a file
omits it.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pyarrow as pa

#: The Parquet schema-metadata key that holds the provenance JSON.
PROVENANCE_KEY = b"oda_data.provenance"

#: The layout of the provenance JSON. Readers refuse a newer format.
FORMAT_VERSION = 1


@dataclass(frozen=True, slots=True)
class Release:
    """One release of an OECD table, as the local cache holds it.

    Args:
        token: The release token oda-reader reports: the file host's ETag or
            Last-Modified date for DAC1, DAC2A, and Multisystem, a readable
            release id such as ``"v20260803"`` for CRS, or ``"sha256:..."``
            for a registered file the host sent no token for. Two copies of
            one published file share a token.
        downloaded: When the file was downloaded or registered (UTC). It is
            recorded beside the token and is not part of it.
        label_release: The readable release id OECD appends to the dataset's
            label, such as ``"v20260803"`` for CRS, or ``None`` when the label
            carries none or the host sent no token to confirm it describes
            these bytes.
    """

    token: str
    downloaded: datetime
    label_release: str | None = None


@dataclass(frozen=True, slots=True)
class Provenance:
    """Where each number in a result came from.

    Args:
        oda_data: The oda-data version that computed the result.
        indicators: Each indicator id in the result and its version.
        tables: Each table the result read and its release.
        pydeflate: The pydeflate version behind the conversion factors, which
            come from the DAC1 release in ``tables``.
        resolvekit: The resolvekit version that resolved names to codes.
        reference_data: The version of the bundled groups, sector groups,
            and channel crosswalk.
        defaults: Each default the call relied on and the value it took,
            such as ``{"basis": "headline", "providers": "dac_countries"}``.
            ``excluded_providers`` holds the providers the indicator
            excludes from its count and why, as ``"<codes>: <reason>"``, one per exclusion
            joined by ``"; "``.
    """

    oda_data: str
    indicators: Mapping[str, int]
    tables: Mapping[str, Release]
    pydeflate: str
    resolvekit: str
    reference_data: str
    defaults: Mapping[str, str]

    def __post_init__(self) -> None:
        for name in ("indicators", "tables", "defaults"):
            object.__setattr__(self, name, MappingProxyType(dict(getattr(self, name))))

    def __reduce__(self) -> tuple[Any, ...]:
        # MappingProxyType does not pickle, so rebuild from plain dicts.
        return (
            Provenance,
            (
                self.oda_data,
                dict(self.indicators),
                dict(self.tables),
                self.pydeflate,
                self.resolvekit,
                self.reference_data,
                dict(self.defaults),
            ),
        )


def to_metadata(record: Provenance) -> dict[bytes, bytes]:
    """Return ``record`` as a Parquet schema-metadata entry.

    Args:
        record: The provenance record.

    Returns:
        A one-item mapping from ``PROVENANCE_KEY`` to the record's JSON.
    """
    document = {
        "format": FORMAT_VERSION,
        "oda_data": record.oda_data,
        "indicators": dict(record.indicators),
        "tables": {
            table: {
                "token": release.token,
                "downloaded": release.downloaded.isoformat(),
                "label_release": release.label_release,
            }
            for table, release in record.tables.items()
        },
        "pydeflate": record.pydeflate,
        "resolvekit": record.resolvekit,
        "reference_data": record.reference_data,
        "defaults": dict(record.defaults),
    }
    return {PROVENANCE_KEY: json.dumps(document, sort_keys=True).encode()}


def from_metadata(metadata: Mapping[bytes, bytes] | None) -> Provenance:
    """Read a provenance record from Parquet schema metadata.

    Args:
        metadata: The schema metadata, as ``pyarrow.Schema.metadata`` holds it.

    Returns:
        The record.

    Raises:
        ValueError: If the metadata holds no record, or holds one in a format
            newer than this version of oda-data reads.
    """
    raw = (metadata or {}).get(PROVENANCE_KEY)
    if raw is None:
        raise ValueError(
            "The file holds no oda-data provenance. Write it with Query.to_parquet."
        )
    document = json.loads(raw)
    found = document.get("format")
    if found != FORMAT_VERSION:
        raise ValueError(
            f"The file's provenance has format {found!r}; this oda-data reads "
            f"format {FORMAT_VERSION}. Upgrade oda-data to read it."
        )
    return Provenance(
        oda_data=document["oda_data"],
        indicators=document["indicators"],
        tables={
            table: Release(
                token=entry["token"],
                downloaded=datetime.fromisoformat(entry["downloaded"]),
                label_release=entry.get("label_release"),
            )
            for table, entry in document["tables"].items()
        },
        pydeflate=document["pydeflate"],
        resolvekit=document["resolvekit"],
        reference_data=document["reference_data"],
        defaults=document["defaults"],
    )


def with_provenance(table: pa.Table, record: Provenance) -> pa.Table:
    """Return ``table`` with ``record`` added to its schema metadata.

    Existing metadata, such as pandas' own entry, is kept.
    """
    return table.replace_schema_metadata(
        {**(table.schema.metadata or {}), **to_metadata(record)}
    )


def provenance(path: str | Path) -> Provenance:
    """Read the provenance record from a Parquet file that ``Query.to_parquet`` wrote.

    Args:
        path: The Parquet file.

    Returns:
        The record: package versions, indicator versions, and the release of
        every table the result read.

    Raises:
        ValueError: If the file holds no record, or one in a newer format.
    """
    import pyarrow.parquet as pq

    return from_metadata(pq.read_schema(path).metadata)


for _public in (Release, Provenance, provenance):
    _public.__module__ = "oda_data"

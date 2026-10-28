"""Provenance records and their Parquet metadata.

A ``Provenance`` names the package versions, indicator versions, and table
releases behind a result. ``Query.to_parquet`` stores it as JSON under
``PROVENANCE_KEY`` in the file's schema metadata, and ``provenance(path)``
reads it back. The JSON carries ``"format": FORMAT_VERSION`` so a later
release can change the layout and still read older files. Adding an optional
key leaves the format at 1, because a reader of format 1 ignores keys it does
not know, and ``from_metadata`` reads ``label_release`` as ``None`` when a file
omits it. ``call`` is such a key, and a file written without it reads with
``Provenance.call`` as ``None``. So is a call group's ``label``, which reads as
``None`` from a file that omits it.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, fields
from datetime import datetime
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

from oda_data._units import Unit

if TYPE_CHECKING:
    import pyarrow as pa

    from oda_data._model import GroupKey, ResolvedCall
    from oda_data._types import Basis, Code, Dimension

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


def _reduce(record: Any) -> tuple[Any, ...]:
    """Rebuild a frozen record from plain values, since ``MappingProxyType``
    does not pickle."""
    values = (getattr(record, f.name) for f in fields(record))
    return (
        type(record),
        tuple(dict(v) if isinstance(v, MappingProxyType) else v for v in values),
    )


@dataclass(frozen=True, slots=True)
class CallGroup:
    """A group a call named, with its members in each year.

    Args:
        dimension: The dimension the members belong to, such as
            ``"provider"``.
        name: The group's name, which identifies it in the call.
        label: The group's display label, which names its total row, or
            ``None`` in a file written before oda-data recorded it.
        shown: Whether each member shows as its own rows, in the years it is
            a member.
        total: Whether the call asked for the group's total row with
            ``oda.total()``.
        members: Each year the call read and the member codes in that year,
            sorted.
    """

    dimension: Dimension
    name: str
    label: str | None
    shown: bool
    total: bool
    members: Mapping[int, tuple[Code, ...]]

    def __post_init__(self) -> None:
        object.__setattr__(self, "members", MappingProxyType(dict(self.members)))

    def __reduce__(self) -> tuple[Any, ...]:
        return _reduce(self)


@dataclass(frozen=True, slots=True)
class Call:
    """What a call computed, with every name and group resolved to codes.

    Two calls that differ in any argument that changes the result hold
    different records, and equal calls write byte-identical JSON.

    Args:
        indicator: The indicator id.
        basis: The basis, the indicator's default when the call named none.
        years: The years asked for, sorted, or ``None`` for every year.
        by: The dimensions the result splits by, in the call's order.
        units: The units, current USD when the call named none. Empty for a
            ratio.
        allow_partial: Whether the call accepted partial values.
        filters: Each dimension the call filtered and the codes it named
            outright, sorted. These show as their own rows, so
            ``providers=[4, oda.total(g7)]`` records ``(4,)`` and
            ``providers=oda.total(g7)`` records ``()``.
        groups: The groups the call named, shown or totalled, sorted by
            dimension and name, with their members.
        default_providers: The providers the call read when it named none,
            sorted, or ``None`` when it named providers or the indicator has
            no provider dimension.
    """

    indicator: str
    basis: Basis
    years: tuple[int, ...] | None
    by: tuple[Dimension, ...]
    units: tuple[Unit, ...]
    allow_partial: bool
    filters: Mapping[Dimension, tuple[Code, ...]]
    groups: tuple[CallGroup, ...]
    default_providers: tuple[Code, ...] | None

    def __post_init__(self) -> None:
        object.__setattr__(self, "filters", MappingProxyType(dict(self.filters)))

    def __reduce__(self) -> tuple[Any, ...]:
        return _reduce(self)


def _sorted_codes(codes: Iterable[Code]) -> tuple[Code, ...]:
    """Codes in a fixed order: ints by value, then strings."""
    return tuple(sorted(codes, key=lambda code: (isinstance(code, str), code)))


def call_record(
    indicator: str,
    call: ResolvedCall,
    *,
    filtered: Iterable[Dimension],
    labels: Mapping[GroupKey, str],
) -> Call:
    """Return the record of a resolved call.

    ``filters`` takes each filtered dimension's ``call.rows``, the codes named
    outright. ``call.members`` also holds the members that only feed a group
    total, so it would give two calls that show different rows one record.

    Args:
        indicator: The indicator id.
        call: The resolved call.
        filtered: The dimensions the caller filtered. Any other dimension in
            ``call.members`` holds a default.
        labels: Each group's key and its display label. A group it lacks
            takes its name.
    """
    named = set(filtered)
    totals = set(call.totals)
    groups = sorted(call.groups, key=lambda group: group.key)
    defaulted = "provider" not in named and "provider" in call.members
    return Call(
        indicator=indicator,
        basis=call.basis,
        years=call.years,
        by=call.by,
        units=call.units,
        allow_partial=call.allow_partial,
        filters={
            dimension: _sorted_codes(call.rows[dimension])
            for dimension in sorted(named)
        },
        groups=tuple(
            CallGroup(
                dimension=group.dimension,
                name=group.name,
                label=labels.get(group.key, group.name),
                shown=group.shown,
                total=group.key in totals,
                members={
                    year: _sorted_codes(codes)
                    for year, codes in sorted(group.members.items())
                },
            )
            for group in groups
        ),
        default_providers=_sorted_codes(call.members["provider"])
        if defaulted
        else None,
    )


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
            excludes from its count and why, as ``"<codes>: <reason>"``, one
            per exclusion joined by ``"; "``.
        call: What the call computed, or ``None`` for a file written before
            oda-data recorded it.
    """

    oda_data: str
    indicators: Mapping[str, int]
    tables: Mapping[str, Release]
    pydeflate: str
    resolvekit: str
    reference_data: str
    defaults: Mapping[str, str]
    call: Call | None = None

    def __post_init__(self) -> None:
        for name in ("indicators", "tables", "defaults"):
            object.__setattr__(self, name, MappingProxyType(dict(getattr(self, name))))

    def __reduce__(self) -> tuple[Any, ...]:
        return _reduce(self)


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
    if record.call is not None:
        document["call"] = _call_document(record.call)
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
        call=_read_call(document["call"]) if "call" in document else None,
    )


def _call_document(call: Call) -> dict[str, Any]:
    return {
        "indicator": call.indicator,
        "basis": call.basis,
        "years": None if call.years is None else list(call.years),
        "by": list(call.by),
        "units": [
            {"currency": u.currency, "prices": u.prices, "base_year": u.base_year}
            for u in call.units
        ],
        "allow_partial": call.allow_partial,
        "filters": {d: list(codes) for d, codes in call.filters.items()},
        "groups": [
            {
                "dimension": group.dimension,
                "name": group.name,
                "label": group.label,
                "shown": group.shown,
                "total": group.total,
                "members": {str(y): list(c) for y, c in group.members.items()},
            }
            for group in call.groups
        ],
        "default_providers": None
        if call.default_providers is None
        else list(call.default_providers),
    }


def _read_call(document: Mapping[str, Any]) -> Call:
    years = document["years"]
    defaulted = document["default_providers"]
    return Call(
        indicator=document["indicator"],
        basis=document["basis"],
        years=None if years is None else tuple(years),
        by=tuple(document["by"]),
        units=tuple(Unit(**unit) for unit in document["units"]),
        allow_partial=document["allow_partial"],
        filters={d: tuple(codes) for d, codes in document["filters"].items()},
        groups=tuple(
            CallGroup(
                dimension=group["dimension"],
                name=group["name"],
                label=group.get("label"),
                shown=group["shown"],
                total=group["total"],
                members={int(y): tuple(c) for y, c in group["members"].items()},
            )
            for group in document["groups"]
        ),
        default_providers=None if defaulted is None else tuple(defaulted),
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


for _public in (Release, Provenance, Call, CallGroup, provenance):
    _public.__module__ = "oda_data"

"""Names for the codes in each dimension, and the aggregate codes.

The names come from bundled reference data: ``_data/names.json`` (providers,
recipients, CRS purpose codes, flows, modalities, CRS finance types, DAC1
and DAC2A lines, and the former names of renamed codes), ``_data/crs_channel_mapping.csv`` (channels) and
``_data/sector_groups.json`` (broad sectors). Nothing here imports resolvekit,
so ``_compile`` and ``_combine`` may use it.

Codes keep their table type: ints for every dimension except ``modality``
(``"B01"``) and ``broad_sector`` (``"health"``).
"""

from __future__ import annotations

import csv
import io
import json
import unicodedata
from collections.abc import Iterable, Mapping
from functools import cache
from importlib.resources import files
from types import MappingProxyType
from typing import Any

from oda_data._types import Code, Dimension, TableName

_LINE_TABLES: tuple[TableName, ...] = ("dac1", "dac2a")

#: Provider codes from 20000 up are OECD aggregates, such as 20001 (DAC countries).
PROVIDER_AGGREGATE_FROM = 20000

#: The EU Institutions' provider code. EU members report their contributions
#: to them as their own multilateral ODA, so 918 is not summed with them.
EU_INSTITUTIONS = 918


def read_data(name: str) -> str:
    """Return the text of a bundled reference file in ``_data``."""
    return files("oda_data").joinpath("_data", name).read_text(encoding="utf-8-sig")


@cache
def load_json(name: str) -> Any:
    """Parse a bundled JSON reference file once per process."""
    return json.loads(read_data(name))


def normalise(text: str) -> str:
    """Fold case, accents, curly quotes, and spacing, so names compare exactly.

    ``"Côte d\N{RIGHT SINGLE QUOTATION MARK}Ivoire"`` and ``"cote d'ivoire"`` normalise to the same text.
    ``"Frnace"`` and ``"France"`` do not.
    """
    decomposed = unicodedata.normalize("NFKD", text)
    stripped = "".join(c for c in decomposed if not unicodedata.combining(c))
    straight = stripped.replace("\N{RIGHT SINGLE QUOTATION MARK}", "'").replace(
        "\N{LEFT SINGLE QUOTATION MARK}", "'"
    )
    return " ".join(straight.casefold().split())


def _int_keys(mapping: Mapping[str, str]) -> Mapping[Code, str]:
    return MappingProxyType({int(code): name for code, name in mapping.items()})


@cache
def _channel_names() -> Mapping[Code, str]:
    rows = csv.DictReader(io.StringIO(read_data("crs_channel_mapping.csv")))
    return MappingProxyType(
        {int(row["channel_code"]): row["channel_name"].strip() for row in rows}
    )


@cache
def channel_acronyms() -> Mapping[Code, tuple[str, ...]]:
    """Each channel's English and French acronyms, such as ``UNDP`` and ``PNUD``."""
    rows = csv.DictReader(io.StringIO(read_data("crs_channel_mapping.csv")))
    return MappingProxyType(
        {
            int(row["channel_code"]): tuple(
                acronym
                for acronym in (row["en_acronym"].strip(), row["fr_acronym"].strip())
                if acronym
            )
            for row in rows
        }
    )


@cache
def names(
    dimension: Dimension, *, table: TableName | None = None
) -> Mapping[Code, str]:
    """Return every code of ``dimension`` and its name.

    Args:
        dimension: The dimension. ``year``, ``score``, and ``allocation_status``
            have no names and return an empty map.
        table: The table, for ``line`` only. DAC1 and DAC2A number their lines
            separately (line 207 is peacebuilding in DAC1 and technical
            co-operation in DAC2A).

    Raises:
        ValueError: ``line`` without a table that has lines.
    """
    data = load_json("names.json")
    match dimension:
        case "provider" | "recipient" | "sector" | "flow" | "finance_type":
            return _int_keys(data[dimension])
        case "modality":
            modalities: dict[Code, str] = dict(data["modality"])
            return MappingProxyType(modalities)
        case "channel":
            return _channel_names()
        case "line":
            if table not in _LINE_TABLES:
                raise ValueError(
                    f"line names need table='dac1' or 'dac2a', not {table!r}"
                )
            return _int_keys(data["line"][table])
        case "broad_sector":
            broad: dict[Code, str] = dict(
                load_json("sector_groups.json")["broad_sectors"]
            )
            return MappingProxyType(broad)
        case _:
            return MappingProxyType({})


#: Sector codes below this are DAC5 headings, such as 122 (Basic Health).
SECTOR_HEADING_BELOW = 1000


@cache
def purpose_codes(heading: int) -> frozenset[int]:
    """Return every sector code under the DAC5 heading ``heading``, itself included.

    DAC5 headings nest by digit, so a trailing zero widens a heading: 122
    (Basic Health) holds 122 and every code from 12210 to 12299, 120 (Health)
    holds 120, 121, 122, 123 and their purpose codes, and 100 holds every code
    that starts with 1. A heading thus holds each known sector code whose first
    three digits start with the heading's digits less its trailing zeros. Only
    the codes in ``names("sector")`` count.

    Raises:
        ValueError: ``heading`` is not a three-digit code.
    """
    if not 100 <= heading < SECTOR_HEADING_BELOW:
        raise ValueError(f"{heading} is not a three-digit DAC5 heading")
    prefix = str(heading).rstrip("0")
    return frozenset(
        int(code) for code in names("sector") if str(code)[:3].startswith(prefix)
    )


@cache
def former_names(dimension: Dimension) -> Mapping[Code, tuple[str, ...]]:
    """Return the names the OECD codelists gave codes of ``dimension`` before a rename.

    Code 1601 (provider) was ``"Bill & Melinda Gates Foundation"`` and is now
    ``"Gates Foundation"``. The resolver accepts both names.
    """
    former = load_json("names.json")["former_names"].get(dimension, {})
    return MappingProxyType({int(code): tuple(old) for code, old in former.items()})


def name_of(
    dimension: Dimension, code: Code, *, table: TableName | None = None
) -> str | None:
    """Return the name of one code, or ``None`` when the reference data lacks it."""
    return names(dimension, table=table).get(code)


@cache
def aggregate_codes(dimension: Dimension) -> frozenset[int]:
    """Return the OECD aggregate codes of ``dimension``, which already sum other rows.

    Providers: every listed code from 20000 up. Recipients: the DAC2A regional,
    income, and other totals (10001 Africa, Total; 10100 Developing Countries,
    Total; 15006 Regional and Unspecified, Total; ...). Regional and
    unspecified rows such as 289 (South of Sahara, regional) and 10280 (Middle
    Africa, unspecified) are not aggregates. Other dimensions have none.
    """
    if dimension not in ("provider", "recipient"):
        return frozenset()
    return frozenset(load_json("names.json")["aggregates"][dimension])


def is_aggregate(dimension: Dimension, code: Code) -> bool:
    """Whether ``code`` is an OECD aggregate of ``dimension``.

    A provider code from 20000 up is an aggregate even when the reference data
    does not list it. ``_compile`` and ``_combine`` apply the same rule
    in SQL.
    """
    if dimension == "provider" and isinstance(code, int):
        return code >= PROVIDER_AGGREGATE_FROM
    return code in aggregate_codes(dimension)


def eu_members(years: Iterable[int] | None) -> frozenset[int]:
    """Return the providers that were EU members in any of ``years``.

    The dates are in ``_data/group_membership.json`` under ``eu_members``:
    each EU27 country from its accession (the six founders from 1958), and
    the United Kingdom from 1973 to 2020. ``None`` means any year.
    """
    wanted = None if years is None else set(years)
    return frozenset(
        int(code)
        for code, spells in load_json("group_membership.json")["eu_members"].items()
        for start, end in spells
        if wanted is None
        or any(start <= year and (end is None or year <= end) for year in wanted)
    )


def eu27() -> frozenset[int]:
    """Return today's 27 EU members, the ``eu27`` group's codes."""
    return frozenset(
        int(code) for code in load_json("provider_groupings.json")["eu27_countries"]
    )

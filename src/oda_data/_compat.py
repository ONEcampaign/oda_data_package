"""oda-data 2.x codes and names, and the 3.0 calls that replace them.

``_data/v2_map.json`` holds one row for each 2.x indicator code (the 3,049
rows of the 2.x catalogue) and each slug of 2.x's ``settings/indicators.json``.
``scripts/generate_v2_map.py`` writes it from the 2.x sources. Each row gives
the 3.0 call as Python lines, the indicator it reads, and whether the call's
number differs from the number 2.x gave for the same provider, recipient and
year, with the reason.

A row's ``status`` is one of:

- ``exact``: the call gives 2.x's number.
- ``changes``: a counting rule, a 2.x defect, or a unit differs; ``reason``
  says which.
- ``no_data``: the table no longer publishes what the code read.
- ``no_2x_number``: 2.x computed no number to compare with, as for every
  slug of ``settings/indicators.json``, which no 2.x code read; the call
  follows the slug's definition, and ``reason`` says where it departs.

``REMOVED`` maps each 2.x top-level name to its replacement, for the
``RemovedIn3Error`` that ``oda_data.__getattr__`` raises.

Nothing here imports pandas, so ``oda_data`` and ``_context`` may use it.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from functools import cache
from types import MappingProxyType
from typing import Literal

from oda_data._names import load_json

type Status = Literal["exact", "changes", "no_data", "no_2x_number"]

#: The bundled map's file name in ``_data``.
MAP_FILE = "v2_map.json"


@dataclass(frozen=True, slots=True)
class V2Row:
    """The 3.0 replacement of one 2.x code or ``settings/indicators.json`` slug.

    Attributes:
        code: The 2.x code, such as ``"DAC1.10.1010"``, or slug.
        source: ``"catalogue"`` for a code, ``"settings"`` for a slug.
        name: The 2.x name of a code, or the slug.
        indicator: The 3.0 indicator the call reads first, or ``None``.
        call: The Python lines of the call. A one-line call is an
            expression; a longer one leaves its result in ``df``. Empty when
            ``status`` is ``no_data``.
        status: ``exact``, ``changes``, ``no_data``, or ``no_2x_number``.
        reason: Why the number changes, why 2.x has no number, or a note on
            an exact row.
        hand_mapped: Whether the generator's hand table wrote the row.
    """

    code: str
    source: str
    name: str
    indicator: str | None
    call: tuple[str, ...]
    status: Status
    reason: str
    hand_mapped: bool

    @property
    def call_text(self) -> str:
        """The call on one line, statements separated by ``"; "``."""
        return "; ".join(self.call)


@cache
def v2_map() -> Mapping[str, V2Row]:
    """Each 2.x code and slug and its row, in the file's order."""
    data = load_json(MAP_FILE)
    texts: Mapping[str, str] = data["reasons"]
    return MappingProxyType(
        {
            code: V2Row(
                code=code,
                source=row["source"],
                name=row["name"],
                indicator=row["indicator"],
                call=tuple(row["call"]),
                status=row["status"],
                reason=_sentences(texts[key] for key in row["reasons"]),
                hand_mapped=row["hand_mapped"],
            )
            for code, row in data["codes"].items()
        }
    )


def _sentences(reasons: Iterable[str]) -> str:
    """Reasons joined into one sentence that starts in capitals."""
    text = "; ".join(reasons)
    return f"{text[0].upper()}{text[1:]}." if text else ""


@cache
def _folded() -> Mapping[str, str]:
    return MappingProxyType({code.casefold(): code for code in v2_map()})


def find(text: str, /) -> V2Row | None:
    """The row of the 2.x code or slug ``text``, in any case, or ``None``."""
    code = _folded().get(text.strip().casefold())
    return None if code is None else v2_map()[code]


def message(row: V2Row, /) -> str:
    """The text of the ``UnknownIndicatorError`` a 2.x code raises."""
    kind = "code" if row.source == "catalogue" else "indicator slug"
    head = f"{row.code!r} is an oda-data 2.x {kind}"
    if not row.call:
        return f"{head}, and 3.0 has no call for it: {row.reason}"
    lines = "\n".join(f"    {line}" for line in row.call)
    text = f"{head}. In 3.0:\n{lines}\n"
    if row.status == "changes":
        return f"{text}The number differs from 2.x: {row.reason}"
    return f"{text}{row.reason}" if row.reason else text.rstrip("\n")


_RAW_TABLES = (
    "oda-reader reads the raw OECD tables (oda_reader.download_{table} or "
    "oda_reader.scan('{table}')). oda.records('{indicator}', ...) returns the "
    "rows behind a 3.0 number"
)
_CACHE = (
    "oda-reader keeps the downloaded tables. Pass oda.Context(cache_dir=...) "
    "to choose where, and use oda_reader.{function}() to manage the cache"
)

#: Each 2.x top-level name and what replaces it in 3.0.
REMOVED: Mapping[str, str] = MappingProxyType(
    {
        "OECDClient": "oda.get(indicator, years=..., providers=..., basis=..., "
        "unit=...) replaces OECDClient(...).get_indicators(...). The 2.x "
        "measure= argument is basis=, and oda.search('DAC1.10.1010') gives the "
        "3.0 call for a 2.x code",
        "ODAData": "oda.get(indicator, years=..., providers=..., unit=...) "
        "replaces ODAData(...).load_indicator(...).get_data(). oda.indicators() "
        "lists the indicators",
        "DAC1Data": _RAW_TABLES.format(table="dac1", indicator="oecd.dac1.lines"),
        "DAC2AData": _RAW_TABLES.format(table="dac2a", indicator="oecd.dac2a.lines"),
        "CRSData": _RAW_TABLES.format(table="crs", indicator="oecd.crs.bilateral_oda"),
        "MultiSystemData": _RAW_TABLES.format(
            table="multisystem", indicator="oecd.multisystem.core_contributions"
        ),
        "AidDataData": "AidData stays in oda-reader: oda_reader.download_aiddata()",
        "add_names_columns": "oda.add_names(df) adds a <dimension>_name column "
        "beside each code column, and every oda.get() result has them already",
        "add_sectors": "pass by=['sector'] to oda.get() on a CRS indicator. "
        "oda.sectors holds the sector groups",
        "add_broad_sectors": "pass by=['broad_sector'] to oda.get() on a CRS indicator",
        "add_gni_share_column": "oda.get('oecd.dac1.oda_gni_ratio') gives ODA "
        "as a per cent of GNI. one.dac1.oda_gni_ratio_fixed_eu27 adds EU "
        "Institutions over today's EU27 GNI in every year, as 2.x did, and "
        "one.dac1.oda_gni_ratio over the GNI of each year's EU members",
        "bilateral_policy_marker": "oda.get('oecd.crs.gender_marker', "
        "by=['provider', 'score', 'year']), with one indicator per marker, such "
        "as oecd.crs.climate_adaptation_marker",
        "provider_groupings": "oda.groups, such as oda.groups.dac_countries. "
        "pass oda.total(group) as a provider for the group's total",
        "recipient_groupings": "oda.groups, such as oda.groups.ldc_countries, "
        "passed to recipients=",
        "sector_imputations": "oda.get('one.crs.imputed_multilateral_oda', "
        "by=['provider', 'sector', 'year'])",
        "imputation_quality_report": "oda.records("
        "'one.crs.imputed_multilateral_oda', ...) carries allocation_status "
        "(imputed, stale_share, proxy, unallocated) on every row. Group by it "
        "to check an imputation",
        "cache": _CACHE.format(function="cache_info"),
        "clear_cache": _CACHE.format(function="clear_cache"),
        "disable_cache": _CACHE.format(function="disable_cache"),
        "enable_cache": _CACHE.format(function="enable_cache"),
        "set_cache_root": _CACHE.format(function="set_cache_dir"),
        "set_data_path": "oda.Context(cache_dir=...) sets where the tables are "
        "kept. Query.to_parquet(path) writes a result",
    }
)

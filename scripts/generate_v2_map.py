"""Write ``src/oda_data/_data/v2_map.json``: the 3.0 call for each oda-data 2.x code.

The 2.x sources are read from git, at tag ``v2.8.2``, the last 2.x release,
unless ``--ref`` names another revision (``git fetch --tags`` brings the tag):

- ``src/oda_data/oda_data_indicators.csv``, the catalogue of 3,049 codes;
- ``src/oda_data/indicators/{dac1,dac2a,crs}/*_indicators.json``, their filters;
- ``src/oda_data/settings/indicators.json``, 108 slugs of ONE's compositions.

Run it after a change to the registry that moves a 2.x code:

    uv run python scripts/generate_v2_map.py

``--check`` writes nothing and exits 1 when the committed map is stale.

Each row gives the call as Python lines that assume ``import oda_data as oda``
and ``import pandas as pd``. A one-line call is an expression; a longer one
leaves its result in ``df``. Most rows follow rules from the code's filters
and the registry; ``HAND`` holds the rest, each with its reason.
"""

import argparse
import csv
import io
import json
import subprocess
import sys
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path

from oda_data._groups import SectorGroup, sectors
from oda_data._model import Headline, Indicator, basis_lines, line_basis
from oda_data._names import purpose_codes
from oda_data._registry import INDICATORS, crs, dac1, dac2a
from oda_data._resolve import resolve_group
from oda_data._types import Basis

ROOT = Path(__file__).resolve().parent.parent
TARGET = ROOT / "src" / "oda_data" / "_data" / "v2_map.json"

CATALOGUE = "src/oda_data/oda_data_indicators.csv"
FILTERS = {
    table: f"src/oda_data/indicators/{table}/{table}_indicators.json"
    for table in ("dac1", "dac2a", "crs")
}
SETTINGS = "src/oda_data/settings/indicators.json"
#: The last 2.x release, which the map is written from by default.
DEFAULT_REF = "v2.8.2"

#: DAC1 lines that 2.x's COVID-19 slugs read. DAC1 holds none of them in any
#: year (``tests/data/test_v2_map_data.py`` checks).
COVID_LINES: tuple[int, ...] = (8852, 8853, 8854, 8855, 8856)

#: DAC2A lines with no row to a multilateral organisation as recipient, so
#: leaving those recipients out changes none of their numbers
#: (``tests/data/test_v2_map_data.py`` checks).
DAC2A_LINES_WITHOUT_AGENCY_ROWS: tuple[int, ...] = (106, 208, 214, 217, 221)

#: The grain of 2.x's DAC2A rows: one per provider, recipient and year.
DAC2A_BY = ["provider", "recipient", "year"]

#: Each 2.x DAC1 flow code and the 3.0 basis, with the flow part it names.
FLOW_BASES: Mapping[int, tuple[Basis, int | None]] = {
    1140: ("net_disbursement", None),
    1160: ("grant_equivalent", None),
    1120: ("gross_disbursement", None),
    1121: ("gross_disbursement", 1121),
    1122: ("gross_disbursement", 1122),
    1150: ("commitment", None),
    1151: ("commitment", 1151),
    1152: ("commitment", 1152),
}

# Reasons ----------------------------------------------------------------------

AGENCY_RECIPIENTS = (
    "3.0 leaves out DAC2A rows to multilateral organisations as recipients "
    "(98, 816, 878, 900-996), because they are core contributions, which the "
    "organisations' own rows carry on to the recipients they reach"
)
IMPUTED_PROVIDERS = (
    "3.0 leaves multilateral providers and the totals that hold them (20002, "
    "20004, 20005) out of line 106, since the imputed shares are their own flows"
)
CRS_BILATERAL_ROWS = (
    "3.0 counts bi_multi 1, 3, 7 and 8 only, so rows with no bi_multi and "
    "multilateral organisations' own outflows (bi_multi 4) are left out, and "
    "for a multilateral provider, oecd.crs.multilateral_oda holds its ODA "
    "outflows"
)
CRS_OOF_ROWS = (
    "3.0 counts bi_multi 1, 3, 7 and 8 only, so rows with no bi_multi and "
    "multilateral organisations' own outflows (bi_multi 4) are left out, and "
    "for a multilateral provider, oecd.crs.multilateral_oof holds its other "
    "official flows"
)
CRS_PSI = (
    "on grant equivalents 3.0 adds private sector instruments (category 60) "
    "to category 10"
)
CRS_SECTORS = (
    "2.x compared five-digit CRS purpose codes with three-digit sector "
    "headings, so it found only activities reported at heading level and "
    "returned nothing for most codes, while 3.0 reads every purpose code under "
    "those headings"
)
CRS_OTHER_CATEGORIES = (
    "2.x summed every CRS category, while 3.0 sums ODA (category 10) and other "
    "official flows (category 21), and oecd.crs.private_development_finance "
    "holds private development finance (category 30)"
)
CRS_PRIVATE_PROVIDERS = (
    "3.0 reads the private donors in oda.groups.private_donors when a call "
    "names no providers, so the category 30 rows that Denmark, Japan, Korea, "
    "Hungary and Finland report (USD 2.7bn in all years, against USD 100.3bn "
    "from private donors) count only when a call names them"
)
NO_COVID_LINE = (
    "DAC1 publishes no line {line} in any year. one.crs.covid_oda counts "
    "bilateral ODA the CRS tags #COVID-19"
)
NO_2X_NUMBER = (
    "no 2.x code read settings/indicators.json, so 2.x computed no number for "
    "this slug, and 3.0 follows the slug's definition"
)
NO_2X_ROWS = "2.x returned no rows for this code on today's DAC1"
RECIPIENT_TOTALS = (
    "2.x's rows also held DAC2A recipient totals, such as developing countries "
    "(10001), which 3.0 returns only when recipients= names them, so 2.x's rows "
    "summed by provider count each flow more than once"
)
EU_GNI = (
    "for EU Institutions (918), which DAC1 gives no GNI, the call takes the "
    "GNI of today's EU27 in every year, as 2.x's add_gni_share_column did"
)
PER_CENT = "3.0 gives ODA/GNI in per cent, while 2.x's ONE ratios were fractions"

#: The reasons many rows share, by the key the map stores them under.
SHARED_REASONS: Mapping[str, str] = {
    "agency_recipients": AGENCY_RECIPIENTS,
    "imputed_providers": IMPUTED_PROVIDERS,
    "crs_bilateral_rows": CRS_BILATERAL_ROWS,
    "crs_oof_rows": CRS_OOF_ROWS,
    "crs_psi": CRS_PSI,
    "crs_sectors": CRS_SECTORS,
    "crs_other_categories": CRS_OTHER_CATEGORIES,
    "crs_private_providers": CRS_PRIVATE_PROVIDERS,
    "per_cent": PER_CENT,
    "no_2x_number": NO_2X_NUMBER,
    "no_2x_rows": NO_2X_ROWS,
    "recipient_totals": RECIPIENT_TOTALS,
    "eu_gni": EU_GNI,
}

# Rows -------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Row:
    """One row of the map; see ``oda_data._compat.V2Row``."""

    code: str
    source: str
    name: str
    indicator: str | None
    call: tuple[str, ...]
    status: str
    reasons: tuple[str, ...] = ()
    hand_mapped: bool = False

    def as_json(self, keys: Mapping[str, str]) -> dict[str, object]:
        """The row as JSON, each reason given by its key in ``keys``."""
        return {
            "source": self.source,
            "name": self.name,
            "indicator": self.indicator,
            "call": list(self.call),
            "status": self.status,
            "reasons": [keys[reason] for reason in self.reasons],
            "hand_mapped": self.hand_mapped,
        }


@dataclass(frozen=True, slots=True)
class Mapped:
    """A call and its status, before it becomes a ``Row``."""

    indicator: str | None
    call: tuple[str, ...]
    status: str = "exact"
    reasons: tuple[str, ...] = ()


def _changes(indicator: str | None, call: tuple[str, ...], *reasons: str) -> Mapped:
    """A call whose number differs from 2.x's for ``reasons``."""
    return Mapped(indicator, call, "changes" if reasons else "exact", reasons)


def _exact(indicator: str | None, call: tuple[str, ...], *notes: str) -> Mapped:
    """A call that gives 2.x's number, with ``notes`` for the reader."""
    return Mapped(indicator, call, "exact", notes)


def _no_data(reason: str) -> Mapped:
    """A code whose table publishes nothing that it read."""
    return Mapped(None, (), "no_data", (reason,))


# Call text --------------------------------------------------------------------


class Expr(str):
    """Python text that a call holds as written, such as ``oda.sectors.health``."""

    __slots__ = ()


def _value(value: object) -> str:
    if isinstance(value, Expr):
        return str(value)
    if isinstance(value, str):
        return json.dumps(value)
    if isinstance(value, list | tuple):
        return "[" + ", ".join(_value(item) for item in value) + "]"
    return repr(value)


def get(indicator: str, /, **arguments: object) -> str:
    """The text of ``oda.get(indicator, **arguments)``, ``None`` values left out."""
    parts = [json.dumps(indicator)]
    parts += [f"{k}={_value(v)}" for k, v in arguments.items() if v is not None]
    return f"oda.get({', '.join(parts)})"


def _sum_lines(frames: Sequence[str], keys: Sequence[str]) -> str:
    """A pandas line that adds the value of ``frames`` by ``keys`` into ``df``."""
    joined = frames[0] if len(frames) == 1 else f"pd.concat([{', '.join(frames)}])"
    return f'df = {joined}.groupby({_value(list(keys))}, as_index=False)["value"].sum()'


def _assigned(call: tuple[str, ...]) -> tuple[str, ...]:
    """``call`` as statements that leave the result in ``df``."""
    return (f"df = {call[0]}",) if len(call) == 1 else call


def _basis_argument(indicator: Indicator, basis: Basis) -> Basis | None:
    """``basis``, or ``None`` when the indicator's default reads the same rows."""
    default = indicator.default_basis
    if basis == default:
        return None
    rule = indicator.binding.bases.get(default)
    if isinstance(rule, Headline) and rule.before == rule.after == basis:
        return None
    return basis


# DAC1 and DAC2A lines ---------------------------------------------------------


def _reads_line(indicator: Indicator, line: int, basis: Basis) -> bool:
    """Whether ``indicator`` reads ``line`` alone on ``basis``, or on both
    bases of a headline."""
    rule = indicator.binding.bases.get(basis)
    used = (rule.before, rule.after) if isinstance(rule, Headline) else (basis,)
    found = basis_lines(indicator)
    return all(found.get(name) == line for name in used)


def _named_on_line(
    module_indicators: Iterable[Indicator], line: int, basis: Basis
) -> Indicator | None:
    return next((i for i in module_indicators if _reads_line(i, line, basis)), None)


def line_call(
    table: str,
    line: int,
    *,
    basis: Basis | None = None,
    flow: int | None = None,
    by: list[str] | None = None,
) -> tuple[Indicator, str]:
    """The indicator and call that read one DAC1 or DAC2A line.

    A named indicator that reads the line on ``basis`` wins over the lines
    indicator. ``basis`` defaults to the basis the lines indicator reads the
    line on.
    """
    module = dac1 if table == "dac1" else dac2a
    lines = module.LINES
    basis = basis or line_basis(lines, line)
    named = _named_on_line(
        (i for i in module.INDICATORS if i is not lines), line, basis
    )
    if named is not None:
        return named, get(
            named.id, basis=_basis_argument(named, basis), flows=flow, by=by
        )
    return lines, get(
        lines.id, lines=line, basis=_basis_argument(lines, basis), flows=flow, by=by
    )


def dac1_line(line: int, *, basis: Basis | None = None) -> Mapped:
    indicator, call = line_call("dac1", line, basis=basis)
    if (
        indicator is dac1.LINES
        or basis_lines(indicator).get("grant_equivalent") != line
    ):
        return _exact(indicator.id, (call,))
    return _exact(
        indicator.id,
        (call,),
        f"grant equivalents start in 2018 on {indicator.id}, and oecd.dac1.lines "
        f'with lines={line}, basis="grant_equivalent" reads 2015 to 2017',
    )


def dac2a_line(line: int) -> Mapped:
    """A DAC2A line by provider and recipient, the grain of 2.x's rows."""
    indicator, call = line_call("dac2a", line, by=DAC2A_BY)
    if line == 106:
        return _changes(indicator.id, (call,), IMPUTED_PROVIDERS, RECIPIENT_TOTALS)
    if line in DAC2A_LINES_WITHOUT_AGENCY_ROWS:
        return _exact(indicator.id, (call,), RECIPIENT_TOTALS)
    return _changes(indicator.id, (call,), AGENCY_RECIPIENTS, RECIPIENT_TOTALS)


# CRS --------------------------------------------------------------------------


def _sector_codes(headings: Iterable[int]) -> list[int]:
    """Every purpose code under the three-digit sector ``headings``.

    The rule is ``purpose_codes``, the one ``sectors=`` applies to a heading.
    """
    return sorted(set().union(*(purpose_codes(heading) for heading in headings)))


def _group_codes(group: SectorGroup) -> frozenset[int]:
    """The purpose codes a built-in sector group reads, by ``sectors=``'s rule.

    The groups hold one composition in every year, so any year gives it.
    """
    (codes,) = resolve_group(group, "sector", years=(2000,)).members.values()
    return frozenset(int(code) for code in codes)


def sector_argument(headings: Iterable[int]) -> object:
    """The ``sectors=`` value for every purpose code under ``headings``.

    The ``oda.sectors`` groups whose codes add up to exactly that set when
    some do, larger groups first, and the headings themselves otherwise,
    which ``sectors=`` expands by the same rule.
    """
    headings = sorted(set(headings))
    target = set(_sector_codes(headings))
    chosen: list[str] = []
    covered: set[int] = set()
    by_size = sorted(sectors.items(), key=lambda item: (-len(item[1].members), item[0]))
    for slug, group in by_size:
        codes = set(_group_codes(group))
        if codes <= target and not codes <= covered:
            chosen.append(slug)
            covered |= codes
    if covered != target:
        return headings
    groups = [Expr(f"oda.sectors.{slug}") for slug in sorted(chosen)]
    return groups[0] if len(groups) == 1 else groups


#: The 2.x filter columns that may select null rows, and their 3.0 dimension.
_NULLABLE = {"modality": "modality", "purpose_code": "sector"}


def crs_code(code: str, filters: Mapping[str, list]) -> Mapped:
    """The call for a ``CRS.P.*`` or ``CRS.R.*`` code from its 2.x filters.

    ``P`` codes split by provider, ``R`` codes by recipient. Category 10 reads
    bilateral ODA, 21 bilateral OOF, 30 private development finance, and no
    category bilateral ODA and OOF. A filter on null
    modality or purpose (2.x's ``X`` and ``<NA>``) cannot be a 3.0 filter, so
    the call splits by that dimension and keeps its null rows in pandas.
    """
    perspective = code.split(".")[1]
    category = filters["category"][1] if "category" in filters else None
    reasons: list[str] = []
    arguments: dict[str, object] = {}
    if "type_of_finance" in filters:
        arguments["finance_types"] = sorted(filters["type_of_finance"][1])
    nulls: list[str] = []
    for column, dimension in _NULLABLE.items():
        if column not in filters:
            continue
        operator, value = filters[column]
        if operator == "is":
            nulls.append(dimension)
        elif dimension == "modality":
            arguments["modalities"] = sorted(value)
        else:
            arguments["sectors"] = sector_argument(value)
            reasons.append(CRS_SECTORS)

    keys = ["provider", "year"] if perspective == "P" else ["recipient", "year"]
    by = [keys[0], *nulls, "year"]
    if by != ["provider", "year"]:
        arguments["by"] = by
    kept = [f'df = df[df["{dimension}"].isna()]' for dimension in nulls]

    if category == 10:
        indicators = [crs.BILATERAL_ODA]
        reasons[:0] = [CRS_BILATERAL_ROWS, CRS_PSI]
    elif category == 21:
        indicators = [crs.BILATERAL_OOF]
        reasons[:0] = [CRS_OOF_ROWS]
    elif category == 30:
        indicators = [crs.PRIVATE_DEVELOPMENT_FINANCE]
        if perspective == "R":
            reasons[:0] = [CRS_PRIVATE_PROVIDERS]
    else:
        indicators = [crs.BILATERAL_ODA, crs.BILATERAL_OOF]
        reasons[:0] = [CRS_OTHER_CATEGORIES, CRS_BILATERAL_ROWS, CRS_PSI]

    if len(indicators) == 1:
        call = [get(indicators[0].id, **arguments)]
        if nulls:
            call = [f"df = {call[0]}", *kept]
    else:
        call = [f"bilateral = {get(crs.BILATERAL_ODA.id, **arguments)}"]
        call.append(f"oof = {get(crs.BILATERAL_OOF.id, **arguments)}")
        if nulls:
            call += ["df = pd.concat([bilateral, oof])", *kept]
            call.append(_sum_lines(["df"], keys))
        else:
            call.append(_sum_lines(["bilateral", "oof"], keys))
    return _changes(indicators[0].id, tuple(call), *reasons)


# settings/indicators.json -----------------------------------------------------


def _line_of(entry: Mapping) -> int | list[int] | None:
    return entry.get("filters", {}).get("aidtype_code")


def settings_dac(slug: str, entry: Mapping) -> Mapped:
    """A ``dac``-type slug on DAC1 or DAC2A."""
    line = _line_of(entry)
    if entry["source"] == "dac2a":
        assert isinstance(line, int), slug
        return dac2a_line(line)
    basis, flow = FLOW_BASES[entry["filters"]["flows_code"]]
    if isinstance(line, list):
        assert flow is None, slug
        call = get("oecd.dac1.lines", lines=line, basis=basis)
        return Mapped(
            dac1.LINES.id,
            (f"df = {call}", _sum_lines(["df"], ["provider", "year"])),
        )
    assert isinstance(line, int), slug
    if line in COVID_LINES:
        return _no_data(NO_COVID_LINE.format(line=line))
    indicator, call = line_call("dac1", line, basis=basis, flow=flow)
    if slug.endswith("_flow") and basis == "grant_equivalent":
        return _exact(
            indicator.id,
            (call,),
            "the 2.x definition reads grant equivalents (flow 1160), despite "
            'its name, and basis="net_disbursement" reads net flows',
        )
    return _exact(indicator.id, (call,))


def settings_linked(slug: str, entry: Mapping, settings: Mapping) -> Mapped:
    """A ``one_linked`` slug: net flows to 2017 and grant equivalents from 2018
    on one line, the ``headline`` basis."""
    found = {
        _line_of(settings[entry["components"][part]]) for part in ("main", "secondary")
    } - {None}
    assert len(found) == 1, slug
    (line,) = found
    assert isinstance(line, int), slug
    if line in COVID_LINES:
        return _no_data(NO_COVID_LINE.format(line=line))
    indicator, call = line_call("dac1", line, basis="headline")
    return Mapped(indicator.id, (call,))


def _gender(score: str) -> Mapped:
    call = (
        "df = " + get("oecd.crs.gender_marker", by=["provider", "score", "year"]),
        f'df = df[df["score"] == "{score}"]',
    )
    return _changes("oecd.crs.gender_marker", call, CRS_BILATERAL_ROWS)


def _bilateral_plus_oof() -> tuple[str, ...]:
    return (
        f"bilateral = {get('oecd.crs.bilateral_oda')}",
        f"oof = {get('oecd.crs.bilateral_oof')}",
        _sum_lines(["bilateral", "oof"], ["provider", "year"]),
    )


#: Rows the rules above cannot write, each with its reason: the six ``ONE.*``
#: catalogue codes and the slugs defined by a function or a list of slugs.
HAND: Mapping[str, Mapped] = {
    "ONE.10.1010_11010": _exact(
        "oecd.dac1.total_oda",
        (get("oecd.dac1.total_oda"),),
        "the headline basis, which reads line 1010 on net flows to 2017 and "
        "line 11010 on grant equivalents from 2018, as 2.x gave with "
        'measure=["net_disbursement", '
        '"grant_equivalent"]',
    ),
    "ONE.40.1010_11010_1": _changes(
        "one.dac1.oda_gni_ratio_fixed_eu27",
        (get("one.dac1.oda_gni_ratio_fixed_eu27"),),
        "3.0 gives per cent, while 2.x gave a fraction rounded to six decimals",
        EU_GNI,
    ),
    "ONE.40.1010_1": _changes(
        "one.dac1.oda_gni_ratio_fixed_eu27",
        (get("one.dac1.oda_gni_ratio_fixed_eu27", basis="net_disbursement"),),
        "2.x raised NotImplementedError, since it shipped no oda_gni_flow "
        "function, and 3.0 gives per cent",
        EU_GNI,
    ),
    "ONE.10.1010C": _exact(
        "one.dac1.core_oda",
        (get("one.dac1.core_oda", basis="net_disbursement"),),
        "2.x's default measure, net flows. The headline basis, as "
        'oda.get("one.dac1.core_oda"), subtracts each line on grant '
        "equivalents from 2018",
    ),
    "ONE.10.206_106": _changes(
        "one.dac2a.recipient_oda_with_imputed_multilateral",
        (get("one.dac2a.recipient_oda_with_imputed_multilateral", by=DAC2A_BY),),
        AGENCY_RECIPIENTS,
        IMPUTED_PROVIDERS,
        RECIPIENT_TOTALS,
    ),
    "ONE.P.40.T.T.S_M": _changes(
        "oecd.crs.multilateral_oda",
        (
            "df = "
            + get("oecd.crs.multilateral_oda", by=["provider", "sector", "year"]),
            'df["share"] = df["value"] / '
            'df.groupby(["provider", "year"])["value"].transform("sum")',
        ),
        "2.x gave three-year rolling shares of ODA and other official flows, "
        "while this call gives single-year shares of ODA. one.crs."
        "imputed_multilateral_oda applies the three-year shares itself",
    ),
    "total_oda_official_definition": _exact(
        "oecd.dac1.total_oda",
        (get("oecd.dac1.total_oda"),),
        "the headline basis, ONE.10.1010_11010 in 2.x",
    ),
    "total_covid_oda_ge": Mapped(
        "one.crs.covid_oda",
        (get("one.crs.covid_oda", basis="grant_equivalent"),),
    ),
    "total_covid_oda_flow": Mapped(
        "one.crs.covid_oda",
        (get("one.crs.covid_oda", basis="net_disbursement"),),
    ),
    "oda_gni_flow": _changes(
        "one.dac1.oda_gni_ratio_fixed_eu27",
        (get("one.dac1.oda_gni_ratio_fixed_eu27", basis="net_disbursement"),),
        PER_CENT,
        EU_GNI,
    ),
    "oda_gni_ge": _changes(
        "one.dac1.oda_gni_ratio_fixed_eu27",
        (get("one.dac1.oda_gni_ratio_fixed_eu27", basis="grant_equivalent"),),
        PER_CENT,
        EU_GNI,
    ),
    "one_non_core_oda_flow": Mapped(
        "one.dac1.non_core_oda",
        (get("one.dac1.non_core_oda", basis="net_disbursement"),),
    ),
    "one_non_core_oda_ge": Mapped(
        "one.dac1.non_core_oda",
        (get("one.dac1.non_core_oda", basis="grant_equivalent"),),
    ),
    "one_non_core_oda_ge_linked": Mapped(
        "one.dac1.non_core_oda", (get("one.dac1.non_core_oda"),)
    ),
    "one_core_oda_flow": Mapped(
        "one.dac1.core_oda",
        (get("one.dac1.core_oda", basis="net_disbursement"),),
    ),
    "one_core_oda_ge": Mapped(
        "one.dac1.core_oda",
        (get("one.dac1.core_oda", basis="grant_equivalent"),),
    ),
    "one_core_oda_ge_linked": Mapped("one.dac1.core_oda", (get("one.dac1.core_oda"),)),
    "recipient_total_flow_net": _changes(
        "one.dac2a.recipient_oda_with_imputed_multilateral",
        (get("one.dac2a.recipient_oda_with_imputed_multilateral", by=DAC2A_BY),),
        AGENCY_RECIPIENTS,
        IMPUTED_PROVIDERS,
        RECIPIENT_TOTALS,
    ),
    "crs_bilateral_flow_disbursement_gross": _changes(
        "oecd.crs.bilateral_oda",
        (get("oecd.crs.bilateral_oda"),),
        CRS_BILATERAL_ROWS,
    ),
    "crs_bilateral_ge": _changes(
        "oecd.crs.bilateral_oda",
        (get("oecd.crs.bilateral_oda", basis="grant_equivalent"),),
        CRS_BILATERAL_ROWS,
        CRS_PSI,
    ),
    "crs_bilateral_all_flows_disbursement_gross": _changes(
        "oecd.crs.bilateral_oda",
        _bilateral_plus_oof(),
        CRS_BILATERAL_ROWS,
        CRS_OOF_ROWS,
    ),
    "crs_gender_significant_flow_disbursement_gross": _gender("significant"),
    "crs_gender_principal_flow_disbursement_gross": _gender("principal"),
    "crs_gender_not_targeted_flow_disbursement_gross": _gender("not_targeted"),
    "crs_gender_not_screened_flow_disbursement_gross": _gender("not_screened"),
    "crs_gender_allocable_flow_disbursement_gross": _changes(
        "oecd.crs.gender_marker",
        (get("oecd.crs.gender_marker"),),
        CRS_BILATERAL_ROWS,
    ),
    "crs_gender_allocable_total_flow_disbursement_gross": _changes(
        "oecd.crs.gender_marker",
        (get("oecd.crs.gender_marker"),),
        CRS_BILATERAL_ROWS,
    ),
    "crs_gender_total_flow_gross": _changes(
        "oecd.crs.gender_marker",
        (
            "df = " + get("oecd.crs.gender_marker", by=["provider", "score", "year"]),
            'df = df[df["score"].isin(["principal", "significant"])]',
            _sum_lines(["df"], ["provider", "year"]),
        ),
        CRS_BILATERAL_ROWS,
    ),
    "crs_bilateral_total_flow_gross_by_purpose": _changes(
        "oecd.crs.bilateral_oda",
        (get("oecd.crs.bilateral_oda", by=["provider", "sector", "year"]),),
        CRS_BILATERAL_ROWS,
    ),
    "multisystem_multilateral_contributions_disbursement_gross": Mapped(
        "oecd.multisystem.core_contributions",
        (get("oecd.multisystem.core_contributions"),),
    ),
    "multisystem_multilateral_contributions_commitments_gross": Mapped(
        "oecd.multisystem.core_contributions",
        (get("oecd.multisystem.core_contributions", basis="commitment"),),
    ),
    "multilateral_purpose_spending_shares": _changes(
        "oecd.crs.multilateral_oda",
        (
            "df = "
            + get("oecd.crs.multilateral_oda", by=["provider", "sector", "year"]),
            'df["share"] = df["value"] / '
            'df.groupby(["provider", "year"])["value"].transform("sum")',
        ),
        "2.x gave three-year rolling shares of ODA and other official flows by "
        "channel, while this call gives single-year shares of ODA by provider. "
        "one.crs.imputed_multilateral_oda applies the three-year shares itself",
    ),
    "imputed_multi_flow_disbursement_gross": _exact(
        "one.crs.imputed_multilateral_oda",
        (get("one.crs.imputed_multilateral_oda"),),
        "the imputation of 2.x's sector_imputations, row for row",
    ),
    "total_bi_multi_flow_disbursement_gross": Mapped(
        "one.crs.bilateral_plus_multilateral_oda",
        (get("one.crs.bilateral_plus_multilateral_oda"),),
    ),
}

#: The DAC1 lines that are not money, which the lines indicator refuses.
DAC1_NOT_MONEY: Mapping[str, Mapped] = {
    "DAC1.40.3": _changes(
        "oecd.dac1.lines",
        (
            f"df = {get('oecd.dac1.lines', lines=5)}",
            f"gni = {get('oecd.dac1.gni')}",
            'df = df.merge(gni[["provider", "year", "value"]], '
            'on=["provider", "year"], suffixes=("", "_gni"))',
            'df["value"] = df["value"] / df["value_gni"] * 100',
        ),
        NO_2X_ROWS,
        "3.0 gives official and private flows (line 5) as a per cent of GNI, "
        "the definition of line 3",
    ),
    "DAC1.40.4": _changes(
        "oecd.dac1.population",
        (get("oecd.dac1.population"),),
        NO_2X_ROWS,
        "3.0 gives population in millions of people, so the call takes no "
        "unit=, and oecd.dac1.lines, which reads amounts of money, refuses "
        "line 4",
    ),
    "DAC1.40.11002": _changes(
        "oecd.dac1.oda_gni_ratio",
        (get("oecd.dac1.oda_gni_ratio", basis="grant_equivalent"),),
        NO_2X_ROWS,
        "3.0 computes ODA over GNI in per cent, which differs from the "
        "published per cent by its rounding, up to 0.005 points",
    ),
}


def _share(row: Mapped, total: Mapped) -> Mapped:
    """``row`` followed by its share of ``total`` by provider and year."""
    assert len(total.call) == 1, total
    call = (
        *_assigned(row.call),
        f"total = {total.call[0]}",
        'df = df.merge(total[["provider", "year", "value"]], '
        'on=["provider", "year"], suffixes=("", "_total"))',
        'df["share"] = df["value"] / df["value_total"]',
    )
    return replace(row, call=call)


# Assembly ---------------------------------------------------------------------


def git_text(path: str, ref: str) -> str:
    """The text of ``path`` at revision ``ref`` of this repository."""
    result = subprocess.run(
        ["git", "-C", str(ROOT), "show", f"{ref}:{path}"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise SystemExit(
            f"cannot read {path} at {ref!r} ({result.stderr.strip()}); run "
            "git fetch --tags or pass --ref"
        )
    return result.stdout


def catalogue_rows(ref: str) -> list[Row]:
    """A row for each code of the 2.x catalogue, in its order."""
    catalogue = list(csv.DictReader(io.StringIO(git_text(CATALOGUE, ref))))
    filters: dict[str, Mapping[str, list]] = {}
    for table, path in FILTERS.items():
        for group in json.loads(git_text(path, ref)).values():
            for code, entry in group.items():
                filters[code] = entry["filters"][table.upper()]
    assert {row["indicator"] for row in catalogue} == set(filters)

    rows = []
    for item in catalogue:
        code, name = item["indicator"], item["name"]
        hand = code in HAND or code in DAC1_NOT_MONEY
        if hand:
            mapped = HAND.get(code) or DAC1_NOT_MONEY[code]
        elif code.startswith("DAC1."):
            mapped = dac1_line(int(filters[code]["aidtype_code"][1]))
        elif code.startswith("DAC2A."):
            mapped = dac2a_line(int(filters[code]["aidtype_code"][1]))
        elif code.startswith("CRS."):
            mapped = crs_code(code, filters[code])
        else:
            raise SystemExit(f"no rule maps {code}; add it to HAND")
        rows.append(_row(code, "catalogue", name, mapped, hand_mapped=hand))
    return rows


def settings_rows(ref: str) -> list[Row]:
    """A row for each slug of 2.x's ``settings/indicators.json``, in its order.

    No 2.x code read the file, so a slug with a call has status
    ``no_2x_number``, its other reasons saying where the call departs from the
    slug's definition.
    """
    settings = json.loads(git_text(SETTINGS, ref))
    base: dict[str, Mapped] = {}
    for slug, entry in settings.items():
        if slug in HAND:
            base[slug] = HAND[slug]
        elif entry["type"] == "dac" and entry["source"] in ("dac1", "dac2a"):
            base[slug] = settings_dac(slug, entry)
        elif entry["type"] == "one_linked":
            base[slug] = settings_linked(slug, entry, settings)
        else:
            raise SystemExit(f"no rule maps the slug {slug}; add it to HAND")

    rows = []
    for slug, entry in settings.items():
        mapped = base[slug]
        total = entry.get("share_of", "")
        if total and total != slug and mapped.call:
            mapped = _share(mapped, base[total])
        if mapped.call:
            mapped = replace(
                mapped, status="no_2x_number", reasons=(NO_2X_NUMBER, *mapped.reasons)
            )
        rows.append(_row(slug, "settings", slug, mapped, hand_mapped=slug in HAND))
    return rows


def _row(
    code: str, source: str, name: str, mapped: Mapped, *, hand_mapped: bool
) -> Row:
    assert mapped.indicator is None or mapped.indicator in INDICATORS, code
    assert bool(mapped.call) == (mapped.status != "no_data"), code
    return Row(
        code=code,
        source=source,
        name=name,
        indicator=mapped.indicator,
        call=mapped.call,
        status=mapped.status,
        reasons=mapped.reasons,
        hand_mapped=hand_mapped,
    )


def render(rows: Sequence[Row]) -> str:
    """The text of ``v2_map.json``, formatted as the JSON pre-commit hook does.

    ``reasons`` holds each reason's text once, under its name in
    ``SHARED_REASONS`` or, for a reason one row alone gives, under that row's
    code; each row lists its reasons by key.
    """
    texts = dict(SHARED_REASONS)
    keys = {text: key for key, text in SHARED_REASONS.items()}
    for row in rows:
        for number, reason in enumerate(r for r in row.reasons if r not in keys):
            key = row.code if number == 0 else f"{row.code}#{number + 1}"
            keys[reason], texts[key] = key, reason
    data = {
        "reasons": texts,
        "codes": {row.code: row.as_json(keys) for row in rows},
    }
    return json.dumps(data, indent=2) + "\n"


def main(argv: list[str]) -> int:
    """Write the map, or with ``--check`` report whether it is current."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true", help="write nothing")
    parser.add_argument(
        "--ref", default=DEFAULT_REF, help=f"the 2.x git revision ({DEFAULT_REF})"
    )
    arguments = parser.parse_args(argv)

    rows = [*catalogue_rows(arguments.ref), *settings_rows(arguments.ref)]
    text = render(rows)
    if arguments.check:
        current = TARGET.read_text() if TARGET.exists() else ""
        if current != text:
            print(
                f"{TARGET} is stale; run scripts/generate_v2_map.py",
                file=sys.stderr,
            )
            return 1
        return 0
    TARGET.write_text(text)
    print(f"wrote {len(rows)} rows to {TARGET}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

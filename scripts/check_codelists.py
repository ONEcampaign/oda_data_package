"""Check the codes the package uses against the OECD's latest codelists.

    uv run python scripts/check_codelists.py

The script fetches the live OECD DAC codelists through ``oda_reader`` and
collects every code that selects rows or carries a name in the package: the
filters of the indicator registry, the groups in ``_data``, ``names.json``,
the channel list and the channel crosswalk. It exits 1 and prints the codes
that are

* unknown: the codelist holds no such code, in any status,
* renamed: ``names.json`` or the channel list names a code differently from
  the codelist, or
* new: the codelist holds an active CRS code of a dimension whose names the
  package bundles (purpose codes, channels, finance types, modalities) and
  the package lacks it (``new_codes``).

A withdrawn code is not a failure: the codelists keep withdrawn codes, and
older data still carries them.

Codes the OECD codelists never held, such as the DAC2A-only recipient codes
and the sector headings 100 to 700, sit in ``UNLISTED``. Provider and recipient
aggregates are exempt by rule. ``names.json`` names flows and DAC lines
with the package's own wording, so those two dimensions are not checked, and
neither are the modality names, which group the OECD's modalities.

``tests/unit/test_check_codelists.py`` covers the comparison on fixture
codelists. Only ``main`` here makes network calls.
"""

from __future__ import annotations

import csv
import json
import sys
from collections import defaultdict
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import pandas as pd

from oda_data._model import AnyOf, Filter, In, NotIn, Value
from oda_data._names import is_aggregate, normalise
from oda_data._registry import INDICATORS

DATA_DIR = Path(__file__).resolve().parent.parent / "src" / "oda_data" / "_data"

#: The OECD codelist id behind each checked dimension.
CODELIST_IDS: Mapping[str, str] = {
    "channel": "3",
    "provider": "5",
    "sector": "10",
    "recipient": "13",
    "modality": "14",
    "finance_type": "15",
}

#: Codes the package uses that no OECD codelist holds.
UNLISTED: Mapping[str, frozenset[str]] = {
    # DAC5 sector headings; the codelist starts at the three-digit sectors.
    "sector": frozenset({"100", "200", "300", "400", "500", "700"}),
    # Channels the CRS reports without a codelist entry.
    "channel": frozenset({"45000", "46999", "50000"}),
    # The codelist lists the CRS modalities; "X" is the unspecified row.
    "modality": frozenset({"X"}),
    # The European Investment Bank reports as a provider in DAC2A only.
    "provider": frozenset({"919"}),
    # DAC2A rows for regions, historical recipients and unspecified totals.
    "recipient": frozenset(
        {"237", "650", "6790", "8600", "9998", "10280", "10330", "10350"}
    ),
}

#: Dimensions whose codes the package names itself, so a code the OECD adds
#: needs a name in ``names.json`` or the channel list.
BUNDLES_NAMES = ("channel", "finance_type", "modality", "sector")

#: Codelist 15 files GNI, ODA % GNI and population under this category, which
#: are not finance types.
NON_FLOW_CATEGORY = "0"

#: Dimensions whose names in the package copy the OECD label.
NAMED_FROM_CODELIST = ("channel", "finance_type", "provider", "recipient", "sector")

#: Provider and recipient groups made of aggregates, which no codelist holds.
AGGREGATE_GROUPS = frozenset({"dac1_aggregates", "dac2a_aggregates"})

#: The registry's recipient filters exclude DAC2A rows for multilateral agencies
#: and historical recipients, which the recipient codelist does not hold.
DAC2A_ONLY_ROWS = frozenset({"recipient"})

#: Sector names from this code up are the OECD purpose labels; the three-digit
#: names are DAC5 headings the package numbers itself.
FIRST_FIVE_DIGIT_CODE = 1000

type Used = dict[str, dict[str, set[str]]]
type Live = Mapping[str, Mapping[str, frozenset[str]]]


@dataclass(frozen=True, order=True)
class Finding:
    """One code the latest codelist contradicts."""

    dimension: str
    code: str
    kind: Literal["unknown", "renamed", "new"]
    detail: str


def _add(used: Used, dimension: str, codes: Iterable[object], source: str) -> None:
    for code in map(str, codes):
        if code:
            used[dimension][code].add(source)


def _filter_codes(
    filters: Iterable[Filter],
) -> Iterator[tuple[str, tuple[object, ...]]]:
    """Yield ``(column, values)`` for every ``In`` and ``NotIn``, inside ``AnyOf``."""
    for item in filters:
        match item:
            case AnyOf(filters=inner):
                yield from _filter_codes(inner)
            case In(column=column, values=values):
                yield column, values
            case NotIn(column=column, values=values, when=when):
                yield column, values
                if when is not None:
                    yield when.column, when.values


def _registry_codes(used: Used) -> None:
    """Add the codes in the registry's filters, by the dimension each column holds."""
    for indicator in INDICATORS.values():
        binding = indicator.binding
        dimension_of = {column: name for name, column in binding.dimensions.items()}
        filters = [*binding.rows, *indicator.rows]
        for rule in binding.bases.values():
            if isinstance(rule, Value):
                filters.extend(rule.rows)
        for column, values in _filter_codes(filters):
            dimension = dimension_of.get(column)
            if dimension in CODELIST_IDS and dimension not in DAC2A_ONLY_ROWS:
                _add(used, dimension, values, f"registry {indicator.id}")


def _read_json(name: str) -> dict:
    return json.loads((DATA_DIR / name).read_text(encoding="utf-8"))


def _read_csv(name: str) -> list[dict[str, str]]:
    with (DATA_DIR / name).open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def collect_used() -> tuple[Used, dict[str, dict[str, str]]]:
    """Return the codes the package uses and the labels it gives them.

    Returns:
        The codes by dimension, each with the files that use it, and the
        package's label for each code of the dimensions in
        ``NAMED_FROM_CODELIST`` and ``BUNDLES_NAMES``.
    """
    used: Used = defaultdict(lambda: defaultdict(set))
    _registry_codes(used)

    names = _read_json("names.json")
    for dimension in ("provider", "sector", "modality", "finance_type"):
        _add(used, dimension, names[dimension], "names.json")
    labels = {
        dimension: dict(names[dimension])
        for dimension in ("provider", "recipient", "sector", "finance_type", "modality")
    }

    channels = {
        row["channel_code"]: row["channel_name"]
        for row in _read_csv("crs_channel_mapping.csv")
    }
    _add(used, "channel", channels, "crs_channel_mapping.csv")
    labels["channel"] = {code: name.strip() for code, name in channels.items()}
    for row in _read_csv("multilateral_channel_crosswalk.csv"):
        _add(
            used, "channel", [row["channel_code"]], "multilateral_channel_crosswalk.csv"
        )
    for row in _read_csv("channel_share_proxies.csv"):
        codes = [row["channel_code"], row["proxy_channel_code"]]
        _add(used, "channel", codes, "channel_share_proxies.csv")

    for dimension, file in (
        ("provider", "provider_groupings.json"),
        ("recipient", "recipient_groupings.json"),
    ):
        for group, members in _read_json(file).items():
            if isinstance(members, dict) and group not in AGGREGATE_GROUPS:
                _add(used, dimension, members, f"{file} {group}")
    for group, members in _read_json("group_membership.json").items():
        _add(used, "provider", members, f"group_membership.json {group}")
    for group, sector in _read_json("sector_groups.json")["sectors"].items():
        _add(used, "sector", sector["codes"], f"sector_groups.json {group}")
    return used, labels


def _is_exempt(dimension: str, code: str, aggregates: Mapping[str, set[str]]) -> bool:
    if code in UNLISTED.get(dimension, ()):
        return True
    if dimension == "provider":
        return is_aggregate("provider", int(code))
    return code in aggregates.get(dimension, ())


def compare(
    used: Used,
    labels: Mapping[str, Mapping[str, str]],
    live: Live,
    *,
    aggregates: Mapping[str, set[str]],
    new: Mapping[str, frozenset[str]],
) -> list[Finding]:
    """Return the used codes the live codelists do not confirm, and the new codes.

    Args:
        used: The codes in use by dimension, each with where it is used.
        labels: The package's label for each code, for the dimensions that
            copy OECD labels.
        live: Each dimension's live codelist as ``code -> labels``,
            withdrawn codes included.
        aggregates: Each dimension's aggregate codes, which no codelist holds.
        new: The live codes of each dimension in ``BUNDLES_NAMES`` that the
            package must name, as ``new_codes`` returns them.

    Returns:
        Unknown codes first by dimension and code, then renamed ones, then
        new ones.
    """
    findings: list[Finding] = []
    for dimension, codes in used.items():
        listed = live[dimension]
        for code, sources in codes.items():
            if _is_exempt(dimension, code, aggregates):
                continue
            if code not in listed:
                findings.append(
                    Finding(dimension, code, "unknown", ", ".join(sorted(sources)))
                )
    for dimension in NAMED_FROM_CODELIST:
        listed = live[dimension]
        for code, label in labels.get(dimension, {}).items():
            if dimension == "sector" and int(code) < FIRST_FIVE_DIGIT_CODE:
                continue
            if code in listed and normalise(label) not in map(normalise, listed[code]):
                current = " | ".join(sorted(listed[code]))
                findings.append(
                    Finding(dimension, code, "renamed", f"{label!r} is now {current!r}")
                )
    for dimension in BUNDLES_NAMES:
        named = labels.get(dimension, {})
        findings.extend(
            Finding(dimension, code, "new", " | ".join(sorted(live[dimension][code])))
            for code in new.get(dimension, ())
            if code not in named
        )
    order = {"unknown": 0, "renamed": 1, "new": 2}
    return sorted(findings, key=lambda f: (order[f.kind], f))


def live_codes(frame: pd.DataFrame, codelist_id: str) -> dict[str, frozenset[str]]:
    """Return one codelist of a ``CodelistSnapshot`` frame as ``code -> labels``.

    A code with several rows, such as one per reporting standard or
    activation date, keeps every label.
    """
    labels: dict[str, set[str]] = defaultdict(set)
    rows = frame[frame["codelist_id"] == codelist_id]
    for code, label in zip(rows["code"], rows["label"], strict=True):
        labels[str(code)].add(str(label).strip())
    return {code: frozenset(found) for code, found in labels.items()}


def new_codes(frame: pd.DataFrame, codelist_id: str) -> frozenset[str]:
    """Return the codes of one codelist that a CRS row can carry.

    A code counts when one of its rows is active and flagged for the CRS
    (``crs`` 1), no other code names it as its category or parent (a heading
    such as finance type 100 or modality ``B``), and it sits outside the
    non-flow category. TOSSD-only codes, such as the seven-digit purpose
    codes, carry ``crs`` 0. The OECD flags a few CRS purpose codes 0 as well
    (152, 910, 930), so a new code it flags that way goes unreported.
    """
    rows = frame[frame["codelist_id"] == codelist_id]
    headings = {
        str(code)
        for column in ("category", "parent_code")
        for code in rows[column].dropna()
    }
    in_crs = rows[
        (rows["status"] == "active")
        & (rows["crs"].astype(str) == "1")
        & (rows["category"].astype(str) != NON_FLOW_CATEGORY)
    ]
    return frozenset(str(code) for code in in_crs["code"]) - headings


def fetch_live() -> tuple[
    dict[str, dict[str, frozenset[str]]], dict[str, frozenset[str]]
]:
    """Fetch the OECD codelists behind ``CODELIST_IDS``.

    Returns:
        Each dimension's codelist as ``code -> labels`` (``live_codes``), and
        the codes of each dimension in ``BUNDLES_NAMES`` that a CRS row can
        carry (``new_codes``).
    """
    from oda_reader.codelists import fetch_code_categories, fetch_codelists

    area = {"provider", "recipient"}
    area_frame = fetch_codelists(
        codelist_ids=tuple(CODELIST_IDS[name] for name in sorted(area))
    ).frame
    category_frame = fetch_code_categories(
        codelist_ids=[CODELIST_IDS[n] for n in CODELIST_IDS if n not in area]
    ).frame
    live = {
        name: live_codes(area_frame if name in area else category_frame, codelist_id)
        for name, codelist_id in CODELIST_IDS.items()
    }
    new = {
        name: new_codes(category_frame, CODELIST_IDS[name]) for name in BUNDLES_NAMES
    }
    return live, new


def main() -> int:
    """Print the codes the live codelists contradict; return 1 if there are any."""
    from oda_reader.exceptions import CodelistError

    used, labels = collect_used()
    aggregates = {
        dimension: {str(code) for code in codes}
        for dimension, codes in _read_json("names.json")["aggregates"].items()
    }
    try:
        live, new = fetch_live()
    except CodelistError as error:
        print(f"could not fetch the OECD codelists: {error}", file=sys.stderr)
        return 2
    findings = compare(used, labels, live, aggregates=aggregates, new=new)
    checked = sum(len(codes) for codes in used.values())
    if not findings:
        print(
            f"{checked} codes in use match the OECD codelists, and every CRS code "
            f"they list in {', '.join(BUNDLES_NAMES)} has a name"
        )
        return 0
    print(f"{len(findings)} codes differ from the OECD codelists ({checked} in use):")
    for finding in findings:
        print(
            f"  {finding.kind:8} {finding.dimension} {finding.code}: {finding.detail}"
        )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

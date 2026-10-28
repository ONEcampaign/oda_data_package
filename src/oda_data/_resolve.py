"""Strict resolution of filter arguments into codes.

Every filter takes a code or an exact name. Providers, recipients, and sectors
also take groups. A name matches when its normalised text (``_names.normalise``)
equals one of:

1. a name in the bundled reference data for the dimension, a name the OECD
   gave the code before a rename (``"Bill & Melinda Gates Foundation"``), or
   an OECD aggregate's name without its ``", Total"`` suffix
   (``"DAC Countries"``);
2. a built-in group's name or label (``"g7"``, ``"G7 countries"``);
3. for providers and recipients only, a name, alias, three-letter code
   (``oecd:dotstat`` or ISO 3166 alpha-3), or DAC code of the entity
   resolvekit finds, which must carry a code in the dimension's system
   (``oecd:provider`` or ``oecd:recipient``).

A sector code below 1000 is a DAC5 heading and stands for every code under it
(``_names.purpose_codes``), so ``sectors=122`` reads 122 and 12220 to 12281.
``oda.sectors.reported_at(122)`` reads code 122 alone.

A name that matches both a code and a group, or several codes, raises
``AmbiguousEntityError``. resolvekit matches typos silently ("Frnace" gives
France at confidence 0.95), so its match counts only when the input equals one
of that entity's names or codes. Otherwise the match becomes a suggestion in
``UnknownEntityError``. The bundled data is consulted first, so ``"Congo"`` as a
recipient is DAC code 234, the OECD's own label.

The resolver is built once per process, on the first name the bundled data
cannot answer, and calls into it hold a lock.
"""

from __future__ import annotations

import difflib
import threading
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field, replace
from functools import cache
from types import MappingProxyType
from typing import TYPE_CHECKING

from oda_data._errors import (
    AmbiguousEntityError,
    UnknownEntityError,
    UnsupportedQueryError,
)
from oda_data._groups import Group, GroupTotal, ReportedAt, groups, sectors
from oda_data._model import ResolvedGroup
from oda_data._names import (
    SECTOR_HEADING_BELOW,
    aggregate_codes,
    channel_acronyms,
    former_names,
    is_aggregate,
    names,
    normalise,
    purpose_codes,
)

if TYPE_CHECKING:
    from resolvekit import EntityRecord, Resolver

    from oda_data._types import Code, Dimension, TableName

#: resolvekit modules for countries and for regions with DAC recipient codes.
_MODULES = ["geo.countries", "geo.regions"]

#: The resolvekit code system that holds each dimension's DAC code.
_SYSTEMS: Mapping[str, str] = MappingProxyType(
    {"provider": "oecd:provider", "recipient": "oecd:recipient"}
)

_THREE_LETTER = ("oecd:dotstat", "iso3")

#: The dimensions a call can filter on.
FILTER_DIMENSIONS: frozenset[str] = frozenset(
    {
        "provider",
        "recipient",
        "sector",
        "channel",
        "modality",
        "finance_type",
        "flow",
        "line",
    }
)

_GROUP_DIMENSIONS = frozenset({"provider", "recipient", "sector"})

_lock = threading.RLock()
_resolver: Resolver | None = None


def resolver() -> Resolver:
    """Return the process's resolvekit resolver, building it on first use."""
    global _resolver  # noqa: PLW0603 - one resolver per process, built lazily
    with _lock:
        if _resolver is None:
            from resolvekit import Resolver

            _resolver = Resolver.from_modules(module_ids=_MODULES, on_missing="null")
        return _resolver


@dataclass(frozen=True, slots=True)
class Selection:
    """One filter argument resolved into codes and groups.

    Args:
        dimension: The dimension filtered.
        codes: Every code the filter reads: codes named directly and the
            members of every group, in any year. This is the
            ``ResolvedCall.members`` entry.
        rows: The codes named directly, which appear as their own rows in
            every year. The members of a group passed without
            ``oda.total()`` appear in the years they are members, so the
            group is ``shown`` and its members stay out of ``rows``.
        groups: Every group named, with its members in each year.
        totals: Names of the groups passed as ``oda.total(group)``.
        labels: Each group's name and its display label, for total rows.
    """

    dimension: Dimension
    codes: frozenset[Code]
    rows: frozenset[Code]
    groups: tuple[ResolvedGroup, ...] = ()
    totals: tuple[str, ...] = ()
    labels: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "labels", MappingProxyType(dict(self.labels)))


def resolve_selection(
    dimension: Dimension,
    values: object,
    *,
    years: Iterable[int],
    table: TableName | None = None,
) -> Selection:
    """Resolve one filter argument, such as ``providers=``, into codes and groups.

    Args:
        dimension: The dimension the argument filters.
        values: One value or an iterable of values: codes, exact names, groups,
            and group totals. A bare string is one value.
        years: The call's years, for the members of each group in each year.
            A call with no ``years`` passes every year of its table.
        table: The indicator's table. ``line`` needs it.

    Raises:
        ValueError: ``dimension`` takes no filter, or ``years`` is empty.
        UnknownEntityError: A code or name matched nothing, or a group
            belongs to another dimension.
        AmbiguousEntityError: A name could mean more than one entity.
        UnsupportedQueryError: A group holds an aggregate code, or two groups
            share a name.
    """
    _check_dimension(dimension)
    year_list = tuple(sorted(set(years)))
    if not year_list:
        raise ValueError("resolve_selection needs at least one year")

    rows: set[Code] = set()
    resolved: dict[str, ResolvedGroup] = {}
    sources: dict[str, Group] = {}
    totals: list[str] = []
    for value in _as_values(values):
        as_total = isinstance(value, GroupTotal)
        target = value.group if isinstance(value, GroupTotal) else value
        if isinstance(target, ReportedAt):
            rows |= _reported_at(dimension, target)
            continue
        if not isinstance(target, Group):
            target = resolve_entity(dimension, target, table=table)
        if not isinstance(target, Group):
            rows |= _codes(dimension, target)
            continue

        known = sources.get(target.name)
        if known is not None and known != target:
            raise UnsupportedQueryError(
                f"two different groups are named {target.name!r}; rename one"
            )
        sources[target.name] = target
        group = resolved.get(target.name) or resolve_group(
            target, dimension, years=year_list, table=table
        )
        if as_total:
            if target.name not in totals:
                totals.append(target.name)
        else:
            group = replace(group, shown=True)
        resolved[target.name] = group

    codes = set(rows)
    for group in resolved.values():
        codes.update(*group.members.values())
    return Selection(
        dimension=dimension,
        codes=frozenset(codes),
        rows=frozenset(rows),
        groups=tuple(resolved.values()),
        totals=tuple(totals),
        labels={name: group.label for name, group in sources.items()},
    )


def resolve_group(
    group: Group,
    dimension: Dimension,
    *,
    years: Iterable[int],
    table: TableName | None = None,
) -> ResolvedGroup:
    """Resolve a group's members into codes for each year.

    A dated group (``group.dated()``) holds in each year only the members of
    that year. Any other group holds every member in every year. Nested
    groups contribute their members.

    A group may hold EU Institutions (918) and EU members. Whether their
    total counts money twice depends on the indicator, so
    ``_combine.combine`` decides.

    Raises:
        UnknownEntityError: A member matched nothing, or the group belongs to
            another dimension.
        AmbiguousEntityError: A member's name could mean more than one entity.
        UnsupportedQueryError: The group holds an aggregate code.
    """
    codes = group_codes(group, dimension, table=table)
    members = {
        year: frozenset(code for code in codes if group.is_member(code, year))
        for year in sorted(set(years))
    }
    _check_aggregates(group, dimension, codes)
    return ResolvedGroup(name=group.name, dimension=dimension, members=members)


def group_codes(
    group: Group, dimension: Dimension, *, table: TableName | None = None
) -> frozenset[Code]:
    """Every code a group holds in any year, nested groups' members included.

    Raises:
        UnknownEntityError: A member matched nothing, or the group belongs to
            another dimension.
        AmbiguousEntityError: A member's name could mean more than one entity.
        UnsupportedQueryError: The dimension takes no groups.
    """
    _check_dimension(dimension)
    if dimension not in _GROUP_DIMENSIONS:
        raise UnsupportedQueryError(f"{dimension} filters take no groups")
    return _member_codes(group, dimension, table=table, seen=())


def _member_codes(
    group: Group,
    dimension: Dimension,
    *,
    table: TableName | None,
    seen: tuple[str, ...],
) -> frozenset[Code]:
    if group.dimension is not None and group.dimension != dimension:
        raise UnknownEntityError(
            f"{group.name!r} is a {group.dimension} group; {dimension} filters "
            f"take {dimension} groups",
            system=dimension,
        )
    if group.name in seen:
        raise ValueError(f"group {group.name!r} contains itself")
    codes: set[Code] = set()
    for member in group.members:
        if isinstance(member, ReportedAt):
            codes |= _reported_at(dimension, member)
            continue
        target = (
            member
            if isinstance(member, Group)
            else resolve_entity(dimension, member, table=table)
        )
        if isinstance(target, Group):
            codes |= _member_codes(
                target, dimension, table=table, seen=(*seen, group.name)
            )
        else:
            codes |= _codes(dimension, target)
    return frozenset(codes)


def _codes(dimension: Dimension, code: Code) -> frozenset[Code]:
    """The codes one resolved code reads: a sector heading's codes, or itself."""
    if dimension == "sector" and isinstance(code, int) and code < SECTOR_HEADING_BELOW:
        return purpose_codes(code)
    return frozenset({code})


def _reported_at(dimension: Dimension, heading: ReportedAt) -> frozenset[Code]:
    """The one code a ``reported_at`` heading reads, in a sector filter only."""
    if dimension != "sector":
        raise UnknownEntityError(
            f"{heading!r} is a sector heading; {dimension} filters take "
            f"{dimension} codes",
            system=dimension,
        )
    return frozenset({heading.code})


def _check_aggregates(
    group: Group, dimension: Dimension, codes: frozenset[Code]
) -> None:
    aggregates = sorted(str(code) for code in codes if is_aggregate(dimension, code))
    if aggregates:
        raise UnsupportedQueryError(
            f"group {group.name!r} holds OECD aggregates {', '.join(aggregates)}, "
            "which already contain their members; pass an aggregate code on its own"
        )


def resolve_entity(
    dimension: Dimension, value: object, *, table: TableName | None = None
) -> Code | Group:
    """Resolve one code or exact name into a code, or a built-in group by name.

    Args:
        dimension: The dimension.
        value: A code (an int, or a digit string for numeric dimensions) or
            an exact name.
        table: The indicator's table. ``line`` needs it.

    Raises:
        UnknownEntityError: Nothing matched. ``suggestions`` holds near matches.
        AmbiguousEntityError: The name could mean more than one entity.
        TypeError: ``value`` is not an int or a string.
    """
    _check_dimension(dimension)
    if isinstance(value, bool) or not isinstance(value, int | str):
        raise TypeError(f"a {dimension} filter takes codes and names, not {value!r}")

    if dimension == "modality":
        code = str(value).strip().upper()
        if code in names("modality"):
            return code
    elif isinstance(value, int) or value.strip().isdigit():
        return _known_code(dimension, int(value), table=table)

    return _resolve_name(dimension, str(value), table=table)


def _known_code(dimension: Dimension, code: int, *, table: TableName | None) -> int:
    if code in _names_for(dimension, table) or code in aggregate_codes(dimension):
        return code
    system = _SYSTEMS.get(dimension)
    if system is not None and _resolvekit_knows(code, system):
        return code
    hint = (
        "; sector filters take CRS purpose codes, DAC5 headings, or oda.sectors groups"
        if dimension == "sector"
        else ""
    )
    raise UnknownEntityError(
        f"{code} is not a known {dimension} code{hint}",
        system=system or dimension,
    )


def _resolvekit_knows(code: int, system: str) -> bool:
    with _lock:
        found = resolver().resolve(str(code), from_system=system, to=system)
    return found is not None


def _resolve_name(
    dimension: Dimension, text: str, *, table: TableName | None
) -> Code | Group:
    key = normalise(text)
    codes = _name_index(dimension, table).get(key, frozenset())
    group = _group_index(dimension).get(key)

    if codes and group is not None:
        raise AmbiguousEntityError(
            f"{text!r} could mean {dimension} "
            f"{_describe(dimension, codes, table)} or the group "
            f"oda.{'sectors' if dimension == 'sector' else 'groups'}.{group.name}; "
            "pass the code or the group",
            candidates=[*(str(code) for code in sorted(codes, key=str)), group.name],
        )
    if group is not None:
        return group
    if len(codes) == 1:
        return next(iter(codes))
    if codes:
        raise AmbiguousEntityError(
            f"{text!r} names several {dimension} codes: "
            f"{_describe(dimension, codes, table)}; pass the code",
            candidates=[str(code) for code in sorted(codes, key=str)],
        )

    system = _SYSTEMS.get(dimension)
    if system is not None:
        return _resolve_with_resolvekit(dimension, text, system)
    raise UnknownEntityError(
        f"no {dimension} is named {text!r}",
        suggestions=_close_names(dimension, key, table),
        system=dimension,
    )


def _resolve_with_resolvekit(dimension: Dimension, text: str, system: str) -> int:
    from resolvekit import AmbiguousResolutionError

    key = normalise(text)
    with _lock:
        r = resolver()
        try:
            entity_ids = [r.resolve_id(text, on_ambiguous="raise")]
        except AmbiguousResolutionError as error:
            entity_ids = [candidate.entity_id for candidate in error.candidates]
        entities = [r.entity(entity_id) for entity_id in entity_ids if entity_id]

    exact = [
        entity
        for entity in entities
        if entity is not None and key in _entity_keys(entity, system)
    ]
    if len(exact) > 1:
        raise AmbiguousEntityError(
            f"{text!r} could mean {', '.join(e.canonical_name for e in exact)}; "
            "pass the DAC code or the three-letter code",
            candidates=[entity.canonical_name for entity in exact],
        )
    if not exact:
        near = [entity.canonical_name for entity in entities if entity is not None]
        raise UnknownEntityError(
            f"no {dimension} is named {text!r}",
            suggestions=near or _close_names(dimension, key, None),
            system=system,
        )

    entity = exact[0]
    code = entity.codes_dict.get(system)
    if code is None:
        raise UnknownEntityError(
            f"{entity.canonical_name} has no DAC {dimension} code",
            system=system,
        )
    return int(code)


def _entity_keys(entity: EntityRecord, system: str) -> frozenset[str]:
    names_ = [entity.canonical_name, *(name.value for name in entity.names)]
    codes = [
        code.value for code in entity.codes if code.system in (*_THREE_LETTER, system)
    ]
    return frozenset(normalise(value) for value in [*names_, *codes])


# Indexes ----------------------------------------------------------------------


def _names_for(dimension: Dimension, table: TableName | None) -> Mapping[Code, str]:
    if dimension != "line":
        return names(dimension)
    if table is not None:
        return names("line", table=table)
    return {**names("line", table="dac2a"), **names("line", table="dac1")}


@cache
def _name_index(
    dimension: Dimension, table: TableName | None
) -> Mapping[str, frozenset[Code]]:
    """Normalised name to codes: bundled names, former names, aggregate aliases."""
    entries: list[tuple[str, Code]] = []
    if dimension == "line" and table is None:
        for line_table in ("dac1", "dac2a"):
            entries += [
                (name, code) for code, name in names("line", table=line_table).items()
            ]
    else:
        entries += [(name, code) for code, name in _names_for(dimension, table).items()]
    for code in aggregate_codes(dimension):
        name = names(dimension).get(code, "")
        if name.endswith(", Total"):
            entries.append((name.removesuffix(", Total"), code))
    entries += [
        (name, code)
        for code, former in former_names(dimension).items()
        for name in former
    ]
    if dimension == "channel":
        entries += [
            (acronym, code)
            for code, acronyms in channel_acronyms().items()
            for acronym in acronyms
        ]

    index: dict[str, set[Code]] = {}
    for name, code in entries:
        index.setdefault(normalise(name), set()).add(code)
    return MappingProxyType({key: frozenset(codes) for key, codes in index.items()})


@cache
def _group_index(dimension: Dimension) -> Mapping[str, Group]:
    """Normalised name and label to built-in group, for this dimension."""
    namespace = sectors if dimension == "sector" else groups
    index: dict[str, Group] = {}
    for group in namespace.values():
        if group.dimension == dimension:
            index[normalise(group.name)] = group
            index[normalise(group.label)] = group
    return MappingProxyType(index)


def _describe(
    dimension: Dimension, codes: Iterable[Code], table: TableName | None
) -> str:
    known = _names_for(dimension, table)
    return ", ".join(
        f"{code} ({known.get(code, '?')})" for code in sorted(codes, key=str)
    )


def _close_names(dimension: Dimension, key: str, table: TableName | None) -> list[str]:
    candidates = {
        normalise(name): name for name in _names_for(dimension, table).values()
    }
    for group in _group_index(dimension).values():
        candidates.setdefault(normalise(group.label), group.label)
    near = difflib.get_close_matches(key, candidates, n=3, cutoff=0.75)
    return [candidates[match] for match in near]


def _check_dimension(dimension: str) -> None:
    if dimension not in FILTER_DIMENSIONS:
        raise ValueError(
            f"{dimension!r} takes no filter; filters exist for "
            f"{', '.join(sorted(FILTER_DIMENSIONS))}"
        )


def _as_values(values: object) -> tuple[object, ...]:
    if isinstance(values, str | int | Group | GroupTotal | ReportedAt):
        return (values,)
    if isinstance(values, Iterable):
        return tuple(values)
    return (values,)

"""Groups of providers, recipients, and sectors, and the built-in ``oda.groups`` and ``oda.sectors``.

A ``Group`` holds members as the caller wrote them: codes, names, or other
groups. ``_resolve`` turns them into codes for the dimension the group is
passed to. Built-in groups hold DAC codes.

Built-in groups keep one composition in every year. ``dac_countries`` is the
composition of the OECD's DAC countries total (20001): the 32 DAC member
countries plus Romania, a DAC Associate that reports to DAC1 from 2008. Its sum
equals 20001 in every year. ``group.dated()`` uses the membership in each year
instead, from ``_data/group_membership.json``, where the founding members start
in 1960, the first year of DAC1.

EU Institutions (918) sit outside every built-in provider group except
``multilateral``. On DAC1 and Multisystem, members report their EU
contributions as their own multilateral ODA, so ``_combine`` refuses a total
over a group holding 918 and a provider that was an EU member in a year of the
call (``Binding.members_hold_eu_share``).
The dates, the United Kingdom's 1973-2020 included, are under ``eu_members``
in ``_data/group_membership.json``.
"""

from __future__ import annotations

import difflib
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from functools import cache
from types import MappingProxyType
from typing import TYPE_CHECKING, Self

from oda_data._errors import UnsupportedQueryError
from oda_data._names import is_aggregate, load_json, names

if TYPE_CHECKING:
    from oda_data._types import Code, Dimension, Entity

#: The first and last year of a membership, both included. The last is ``None``
#: while the membership lasts.
type Spell = tuple[int, int | None]

#: The broad sector of a purpose code the sector lists miss.
UNALLOCATED_BROAD_SECTOR = "unallocated_unspecified"


@dataclass(frozen=True, slots=True, init=False)
class Group:
    """A named set of providers, recipients, or sectors.

    Pass a group to ``providers=``, ``recipients=``, or ``sectors=`` to filter
    on its members, and ``oda.total(group)`` beside it for its total row.

    Args:
        name: The name that labels the group's total row.
        members: DAC codes, three-letter codes, exact names, or other groups.
            A bare string or int is one member.
        label: A longer name for display. Defaults to ``name``.
        dimension: The dimension the members belong to, when fixed. A group
            passed to another dimension raises ``UnknownEntityError``.

    Raises:
        ValueError: An empty name or no members.
        TypeError: A member that is a ``GroupTotal`` or not a code, name, or group.
    """

    name: str
    members: tuple[Entity, ...]
    label: str
    dimension: Dimension | None
    is_dated: bool
    _spells: Mapping[Code, tuple[Spell, ...]] = field(compare=False, repr=False)

    def __init__(
        self,
        name: str,
        members: Iterable[Entity] | Entity,
        *,
        label: str | None = None,
        dimension: Dimension | None = None,
    ) -> None:
        if not isinstance(name, str) or not name.strip():
            raise ValueError("a group needs a non-empty name")
        items = _as_members(members)
        if not items:
            raise ValueError(f"group {name!r} has no members")
        for item in items:
            if isinstance(item, bool | GroupTotal) or not isinstance(
                item, int | str | Group
            ):
                raise TypeError(
                    f"group {name!r}: a member must be a code, a name, or a group, "
                    f"not {item!r}"
                )
        _set(self, name, items, label or name, dimension, False, {})

    def __repr__(self) -> str:
        dated = ", dated" if self.is_dated else ""
        return f"Group({self.name!r}, {len(self.members)} members{dated})"

    def dated(self) -> Self:
        """Return the group with its membership in each year.

        Raises:
            UnsupportedQueryError: The group has no membership by year. Built-in
                ``dac_countries``, ``eu27``, and ``g7`` have one.
        """
        if not self._spells:
            dated = load_json("group_membership.json")
            with_years = ", ".join(name for name, *_ in _BUILT_IN if name in dated)
            raise UnsupportedQueryError(
                f"group {self.name!r} has no membership by year; "
                f"these groups have one: {with_years}"
            )
        clone = object.__new__(type(self))
        _set(
            clone,
            self.name,
            self.members,
            self.label,
            self.dimension,
            True,
            self._spells,
        )
        return clone

    def is_member(self, code: Code, year: int) -> bool:
        """Whether ``code`` belongs to a dated group in ``year``.

        A group with one composition holds every member in every year.
        """
        if not self.is_dated:
            return True
        return any(
            start <= year and (end is None or year <= end)
            for start, end in self._spells.get(code, ())
        )


def _set(
    group: Group,
    name: str,
    members: tuple[Entity, ...],
    label: str,
    dimension: Dimension | None,
    is_dated: bool,
    spells: Mapping[Code, tuple[Spell, ...]],
) -> None:
    for attribute, value in (
        ("name", name),
        ("members", members),
        ("label", label),
        ("dimension", dimension),
        ("is_dated", is_dated),
        ("_spells", dict(spells)),
    ):
        object.__setattr__(group, attribute, value)


def _as_members(members: Iterable[Entity] | Entity) -> tuple[Entity, ...]:
    if isinstance(members, str | int | Group | GroupTotal):
        return (members,)
    return tuple(members)


class SectorGroup(Group):
    """A named set of CRS purpose codes, such as ``oda.sectors.health``.

    Args:
        name: The name that labels the group's total row.
        members: Purpose codes, exact purpose names, or other sector groups.
        label: A longer name for display. Defaults to ``name``.
    """

    __slots__ = ()

    def __init__(
        self,
        name: str,
        members: Iterable[int | str | SectorGroup] | int | str | SectorGroup,
        *,
        label: str | None = None,
    ) -> None:
        super().__init__(name, members, label=label, dimension="sector")

    def __repr__(self) -> str:
        return f"SectorGroup({self.name!r}, {len(self.members)} codes)"


@dataclass(frozen=True, slots=True)
class GroupTotal:
    """A group's total as one row. Build it with ``oda.total(group)``."""

    group: Group

    def __post_init__(self) -> None:
        if not isinstance(self.group, Group):
            raise TypeError(f"total() takes a group, not {self.group!r}")


def total(group: Group) -> GroupTotal:
    """Return a group's total as one row.

    Pass it beside the group, as in ``[dac, oda.total(dac)]``, for the
    members and the total. The call must group by the group's dimension.

    Raises:
        TypeError: ``group`` is not a ``Group``.
    """
    return GroupTotal(group)


class GroupNamespace[G: Group](Mapping[str, G]):
    """Read-only named groups, by attribute (``oda.groups.g7``) or key."""

    __slots__ = ("_groups", "_kind")

    def __init__(self, kind: str, groups: Mapping[str, G]) -> None:
        self._kind = kind
        self._groups = MappingProxyType(dict(groups))

    def __getattr__(self, name: str) -> G:
        if name.startswith("_"):
            raise AttributeError(name)
        try:
            return self._groups[name]
        except KeyError:
            near = difflib.get_close_matches(name, self._groups, n=3)
            hint = f"; did you mean {', '.join(near)}?" if near else ""
            raise AttributeError(f"oda.{self._kind} has no {name!r}{hint}") from None

    def __getitem__(self, name: str) -> G:
        return self._groups[name]

    def __iter__(self) -> Iterator[str]:
        return iter(self._groups)

    def __len__(self) -> int:
        return len(self._groups)

    def __dir__(self) -> list[str]:
        return sorted(self._groups)

    def __repr__(self) -> str:
        width = max(map(len, self._groups), default=0)
        lines = [
            f"  {name:<{width}}  {group.label} ({len(group.members)})"
            for name, group in self._groups.items()
        ]
        return f"oda.{self._kind}:\n" + "\n".join(lines)


# Built-in groups --------------------------------------------------------------

_BUILT_IN: tuple[tuple[str, str, Dimension, tuple[str, ...]], ...] = (
    # name, label, dimension, keys in provider_ or recipient_groupings.json
    ("dac_countries", "DAC countries", "provider", ("dac_countries",)),
    ("non_dac_countries", "Non-DAC countries", "provider", ("non_dac_countries",)),
    (
        "all_bilateral",
        "DAC and non-DAC countries",
        "provider",
        ("dac_countries", "non_dac_countries"),
    ),
    ("multilateral", "Multilateral organisations", "provider", ("multilateral",)),
    ("private_donors", "Private donors", "provider", ("private_donors",)),
    ("g7", "G7 countries", "provider", ("g7",)),
    ("eu27", "EU27 countries", "provider", ("eu27_countries",)),
    ("all_recipients", "All recipients", "recipient", ("all_recipients",)),
    (
        "developing_countries",
        "Developing countries and regions",
        "recipient",
        ("all_developing_countries_regions",),
    ),
    ("african_countries", "African countries", "recipient", ("african_countries",)),
    (
        "africa_regional",
        "African regional programmes",
        "recipient",
        ("africa_regional",),
    ),
    (
        "african_countries_regional",
        "African countries and regional programmes",
        "recipient",
        ("african_countries", "africa_regional"),
    ),
    ("sahel", "Sahel", "recipient", ("sahel",)),
    ("ldc_countries", "Least developed countries", "recipient", ("ldc_countries",)),
    (
        "france_priority",
        "France's priority countries",
        "recipient",
        ("france_priority",),
    ),
)


def _built_in_group(
    name: str, label: str, dimension: Dimension, keys: tuple[str, ...]
) -> Group:
    source = load_json(f"{dimension}_groupings.json")
    codes = sorted(
        {
            int(code)
            for key in keys
            for code in source[key]
            if not is_aggregate(dimension, int(code))
        }
    )
    group = Group(name, codes, label=label, dimension=dimension)
    spells = load_json("group_membership.json").get(name)
    if spells:
        object.__setattr__(
            group,
            "_spells",
            {
                int(code): tuple((start, end) for start, end in years)
                for code, years in spells.items()
            },
        )
    return group


@cache
def _sector_groups() -> Mapping[str, SectorGroup]:
    data = load_json("sector_groups.json")
    by_broad: dict[str, set[int]] = {slug: set() for slug in data["broad_sectors"]}
    found: dict[str, SectorGroup] = {}
    for slug, sector in data["sectors"].items():
        by_broad[sector["broad"]].update(sector["codes"])
        found[slug] = SectorGroup(slug, sector["codes"], label=sector["label"])
    # A broad sector holds every narrower sector that maps to it, so it wins a
    # shared name (``health`` is the broad sector, not purpose code 120 alone).
    for slug, label in data["broad_sectors"].items():
        found[slug] = SectorGroup(slug, sorted(by_broad[slug]), label=label)
    return MappingProxyType(dict(sorted(found.items())))


@cache
def broad_sectors() -> Mapping[int, str]:
    """Return each sector code and its broad sector, such as 12220 to ``health``.

    Every code in the sector lists and every known CRS purpose code appears
    once. A purpose code the lists miss falls to ``unallocated_unspecified``,
    as in oda-data 2.x. The names are in ``names("broad_sector")``.
    """
    data = load_json("sector_groups.json")
    mapping = {
        code: sector["broad"]
        for sector in data["sectors"].values()
        for code in sector["codes"]
    }
    for code in names("sector"):
        mapping.setdefault(int(code), UNALLOCATED_BROAD_SECTOR)
    return MappingProxyType(dict(sorted(mapping.items())))


#: Named provider and recipient groups, such as ``oda.groups.dac_countries``.
groups: GroupNamespace[Group] = GroupNamespace(
    "groups", {spec[0]: _built_in_group(*spec) for spec in _BUILT_IN}
)

#: Named sector groups, such as ``oda.sectors.health`` and ``oda.sectors.basic_education``.
sectors: GroupNamespace[SectorGroup] = GroupNamespace("sectors", _sector_groups())

for _public in (Group, SectorGroup, GroupTotal, total):
    _public.__module__ = "oda_data"

"""Groups, group totals, oda.groups, oda.sectors, and broad sectors."""

import dataclasses
import pickle
import subprocess
import sys

import pytest

import oda_data as oda
from oda_data._groups import broad_sectors
from oda_data._names import EU_INSTITUTIONS, is_aggregate, names
from oda_data._registry.dac2a import MULTILATERAL_RECIPIENTS
from oda_data._resolve import resolve_group


def test_dac_countries_is_the_members_plus_romania_without_eu_institutions() -> None:
    members = set(oda.groups.dac_countries.members)
    assert len(members) == 33
    assert 77 in members  # Romania
    assert EU_INSTITUTIONS not in members


def test_g7_and_eu27_members() -> None:
    assert set(oda.groups.g7.members) == {4, 5, 6, 12, 301, 302, 701}
    assert len(oda.groups.eu27.members) == 27
    assert EU_INSTITUTIONS not in oda.groups.eu27.members


@pytest.mark.parametrize(
    "name", [n for n, g in oda.groups.items() if g.dimension == "provider"]
)
def test_no_provider_group_holds_eu_institutions_and_an_eu_member(name) -> None:
    members = set(oda.groups[name].members)
    assert not (EU_INSTITUTIONS in members and members & set(oda.groups.eu27.members))


@pytest.mark.parametrize("name", list(oda.groups))
def test_no_built_in_group_holds_an_aggregate(name) -> None:
    group = oda.groups[name]
    assert not [c for c in group.members if is_aggregate(group.dimension, c)]


def test_all_recipients_drops_the_dac2a_totals_and_keeps_regional_rows() -> None:
    members = set(oda.groups.all_recipients.members)
    assert 10100 not in members  # Developing Countries, Total
    assert {289, 9998, 1027} <= members
    # Multilateral organisations as DAC2A recipients, such as 905 (IDA).
    assert not members & set(MULTILATERAL_RECIPIENTS)


def test_namespace_reads_by_attribute_and_key_and_suggests_on_typos() -> None:
    assert oda.groups.g7 is oda.groups["g7"]
    assert "dac_countries" in dir(oda.groups)
    with pytest.raises(AttributeError, match="dac_countries"):
        _ = oda.groups.dac_countrie


@pytest.mark.parametrize(
    ("members", "expected"),
    [
        (["France", 5, "DEU"], ("France", 5, "DEU")),
        (iter([4]), (4,)),
        ("France", ("France",)),
        (4, (4,)),
    ],
)
def test_group_takes_one_member_or_many(members, expected) -> None:
    group = oda.Group("mine", members)
    assert group.members == expected
    assert group.label == "mine"


@pytest.mark.parametrize(
    ("name", "members", "error"),
    [
        ("", [4], ValueError),
        ("empty", [], ValueError),
        ("total", [oda.total(oda.groups.g7)], TypeError),
        ("flag", [True], TypeError),
        ("float", [4.0], TypeError),
    ],
)
def test_group_refuses_bad_input(name, members, error) -> None:
    with pytest.raises(error):
        oda.Group(name, members)


def test_group_is_frozen_hashable_and_pickles() -> None:
    group = oda.groups.dac_countries.dated()
    with pytest.raises(dataclasses.FrozenInstanceError):
        group.name = "other"  # ty: ignore[invalid-assignment]
    assert hash(group) == hash(pickle.loads(pickle.dumps(group)))
    assert pickle.loads(pickle.dumps(group)) == group


def test_total_wraps_a_group_and_refuses_anything_else() -> None:
    dac = oda.groups.dac_countries
    assert oda.total(dac).group is dac
    with pytest.raises(TypeError):
        oda.total("dac_countries")  # ty: ignore[invalid-argument-type]


@pytest.mark.parametrize(
    ("code", "year", "member"),
    [
        (4, 1960, True),  # France, a founder; DAC1 starts in 1960
        (9, 1960, True),  # Portugal, before it left in 1974
        (9, 1980, False),  # Portugal left in 1974 and rejoined in 1991
        (9, 1995, True),
        (742, 2009, False),  # Korea joined in 2010
        (742, 2010, True),
        (77, 2024, False),  # Romania is not a member
    ],
)
def test_dated_dac_countries_follow_membership(code, year, member) -> None:
    assert oda.groups.dac_countries.dated().is_member(code, year) is member


def test_a_group_with_one_composition_holds_every_member_every_year() -> None:
    assert oda.groups.dac_countries.is_member(77, 1960)


def test_dated_needs_membership_by_year() -> None:
    with pytest.raises(oda.UnsupportedQueryError, match="dac_countries") as raised:
        oda.Group("mine", [4]).dated()

    # The dated EU list behind the 918 check is no group.
    assert "eu_members" not in str(raised.value)


@pytest.mark.parametrize("name", list(oda.sectors))
def test_every_sector_group_resolves_to_named_purpose_codes(name: str) -> None:
    group = resolve_group(oda.sectors[name], "sector", years=[2023])

    assert group.members[2023]
    assert group.members[2023] <= set(names("sector"))


def test_broad_sector_holds_its_narrower_sectors() -> None:
    health = set(oda.sectors.health.members)
    assert set(oda.sectors.basic_health.members) <= health
    assert oda.sectors.reported_at(120) in health
    assert oda.sectors.health.label == "Health"


def test_sector_groups_are_sector_groups_on_the_sector_dimension() -> None:
    group = oda.sectors.basic_education
    assert isinstance(group, oda.SectorGroup)
    assert isinstance(group, oda.Group)
    assert group.dimension == "sector"


def test_broad_sectors_cover_every_purpose_code_once() -> None:
    mapping = broad_sectors()
    assert set(names("sector")) <= set(mapping)
    assert set(mapping.values()) <= set(names("broad_sector"))
    assert mapping[12220] == "health"
    # 151 sat in two 2.x lists; the later one (conflict, peace and security) won.
    assert mapping[151] == "conflict_peace_and_security"


def test_broad_sector_groups_match_the_broad_sector_mapping() -> None:
    mapping = broad_sectors()
    for slug in names("broad_sector"):
        group = resolve_group(oda.sectors[slug], "sector", years=[2023])
        assert group.members[2023] == {
            code for code, broad in mapping.items() if broad == slug
        }


def test_groups_and_sectors_load_without_heavy_packages() -> None:
    heavy = ["pandas", "duckdb", "pyarrow", "resolvekit"]
    code = (
        "import sys, oda_data\n"
        "oda_data.groups.g7, oda_data.sectors.health, oda_data.total\n"
        f"print([m for m in {heavy!r} if m in sys.modules])\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    )
    assert result.stdout.strip() == "[]"

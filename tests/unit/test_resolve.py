"""Strict resolution of filter arguments.

Names that the bundled data lacks go to resolvekit's bundled country and region
modules, which read local files only.
"""

from concurrent.futures import ThreadPoolExecutor

import pytest

import oda_data as oda
from oda_data import _resolve
from oda_data._resolve import resolve_entity, resolve_group, resolve_selection


@pytest.mark.parametrize(
    ("dimension", "value", "code"),
    [
        ("provider", 4, 4),
        ("provider", "4", 4),
        ("provider", "France", 4),
        ("provider", "france", 4),
        ("provider", "FRA", 4),  # three-letter code, through resolvekit
        ("provider", "United States", 302),
        ("provider", "EU Institutions", 918),
        ("provider", "DAC Countries, Total", 20001),
        ("provider", 20001, 20001),
        ("recipient", "Congo", 234),  # the OECD's own label wins
        ("recipient", "Kosovo", 57),
        ("recipient", "XKX", 57),
        ("recipient", "Cote d'Ivoire", 247),
        ("recipient", "Turkey", 55),  # renamed Türkiye; resolvekit knows it
        ("recipient", "Türkiye", 55),
        ("provider", "Gates Foundation", 1601),
        ("provider", "Bill & Melinda Gates Foundation", 1601),  # former names
        ("provider", "United Nations Office on Drugs and Crime", 1040),
        ("provider", "Bernard van Leer Foundation", 1609),
        ("provider", "Arcadia Fund", 1637),
        ("sector", "Clean cooking appliances manufacturing", 32174),
        (
            "channel",
            "Integrated Framework for Trade-Related Technical Assistance to "
            "Least Developed Countries",
            47116,
        ),
        ("recipient", "South of Sahara, regional", 289),
        ("sector", 12220, 12220),
        ("sector", "Basic health care", 12220),
        ("channel", "UNDP", 41114),
        ("channel", "41114", 41114),
        ("modality", "b01", "B01"),
        ("finance_type", "Standard grant", 110),
        ("finance_type", "421", 421),
        ("flow", 1140, 1140),
    ],
)
def test_resolves_codes_and_exact_names(dimension, value, code) -> None:
    assert resolve_entity(dimension, value) == code


def test_lines_resolve_against_the_table() -> None:
    assert resolve_entity("line", 1010, table="dac1") == 1010
    with pytest.raises(oda.UnknownEntityError):
        resolve_entity("line", 1010, table="dac2a")


@pytest.mark.parametrize(
    ("dimension", "value", "suggestion"),
    [
        ("provider", "Frnace", "France"),  # resolvekit matches it silently
        ("provider", "Germny", "Germany"),
        (
            "channel",
            "United Nations Development Programm",
            "United Nations Development Programme",
        ),
    ],
)
def test_a_typo_raises_with_the_match_as_a_suggestion(
    dimension, value, suggestion
) -> None:
    with pytest.raises(oda.UnknownEntityError) as caught:
        resolve_entity(dimension, value)
    assert suggestion in caught.value.suggestions
    assert suggestion in str(caught.value)
    assert caught.value.code == "unknown_entity"


@pytest.mark.parametrize(
    ("dimension", "value", "system"),
    [
        ("provider", "Atlantis", "oecd:provider"),
        ("provider", "Kosovo", "oecd:provider"),  # a recipient, not a provider
        ("provider", 99999, "oecd:provider"),
        ("provider", "²", "oecd:provider"),  # a digit, not a decimal code
        ("sector", 99, "sector"),  # no purpose code
        ("flow", 7, "flow"),
    ],
)
def test_an_unknown_code_or_name_raises(dimension, value, system) -> None:
    with pytest.raises(oda.UnknownEntityError) as caught:
        resolve_entity(dimension, value)
    assert caught.value.system == system


@pytest.mark.parametrize(
    ("dimension", "value", "candidates"),
    [
        ("provider", "DAC countries", {"20001", "dac_countries"}),
        ("provider", "G7 Countries", {"20003", "g7"}),
        ("modality", "Budget support", {"A01", "A02"}),
    ],
)
def test_a_name_that_could_mean_two_things_raises(dimension, value, candidates) -> None:
    with pytest.raises(oda.AmbiguousEntityError) as caught:
        resolve_entity(dimension, value)
    assert set(caught.value.candidates) == candidates


def test_a_group_name_gives_the_group() -> None:
    assert resolve_entity("provider", "g7") is oda.groups.g7
    assert resolve_entity("sector", "Health") is oda.sectors.health


@pytest.mark.parametrize("value", [None, 4.0, True])
def test_a_value_that_is_not_a_code_or_name_raises(value) -> None:
    with pytest.raises(TypeError):
        resolve_entity("provider", value)


def test_a_dimension_without_filters_raises() -> None:
    with pytest.raises(ValueError, match="takes no filter"):
        resolve_entity("year", 2020)  # ty: ignore[invalid-argument-type]


def test_selection_keeps_member_rows_apart_from_total_only_groups() -> None:
    dac = oda.groups.dac_countries
    both = resolve_selection("provider", [dac, oda.total(dac), "Turkey"], years=[2023])
    assert both.rows == frozenset({55})
    assert both.codes == frozenset({*dac.members, 55})
    assert [(g.name, g.shown) for g in both.groups] == [("dac_countries", True)]
    assert both.totals == ("dac_countries",)
    assert both.labels == {"dac_countries": "DAC countries"}

    total_only = resolve_selection("provider", oda.total(oda.groups.g7), years=[2023])
    assert total_only.rows == frozenset()
    assert total_only.codes == frozenset(oda.groups.g7.members)
    assert [(g.name, g.shown) for g in total_only.groups] == [("g7", False)]


def test_filters_are_sets() -> None:
    selection = resolve_selection(
        "provider", [oda.groups.g7, oda.groups.eu27, "France"], years=[2023]
    )
    assert len(selection.codes) == len(
        {*oda.groups.g7.members, *oda.groups.eu27.members}
    )


def test_a_dated_group_holds_each_years_members() -> None:
    group = resolve_group(
        oda.groups.dac_countries.dated(), "provider", years=[1970, 2023]
    )
    assert 742 not in group.members[1970]  # Korea joined in 2010
    assert 742 in group.members[2023]
    fixed = resolve_group(oda.groups.dac_countries, "provider", years=[1970, 2023])
    assert fixed.members[1970] == fixed.members[2023]


def test_a_user_group_resolves_names_and_nested_groups() -> None:
    mine = oda.Group("mine", ["France", "DEU", oda.groups.g7])
    group = resolve_group(mine, "provider", years=[2023])
    assert group.members[2023] == frozenset(oda.groups.g7.members)


@pytest.mark.parametrize(
    ("group", "dimension", "error"),
    [
        (oda.Group("agg", [20001, 4]), "provider", oda.UnsupportedQueryError),
        (oda.groups.g7, "recipient", oda.UnknownEntityError),
        (oda.Group("odd", ["Frnace"]), "provider", oda.UnknownEntityError),
        (oda.groups.g7, "channel", oda.UnsupportedQueryError),
    ],
)
def test_a_group_that_cannot_be_summed_raises(group, dimension, error) -> None:
    with pytest.raises(error):
        resolve_group(group, dimension, years=[2023])


def test_eu_institutions_with_eu_members_resolve_for_providers() -> None:
    # Whether 918 sums with its members depends on the indicator's table, so
    # _combine decides; the group itself resolves.
    eu = oda.Group("eu", [*oda.groups.eu27.members, 12, 918])
    resolved = resolve_group(eu, "provider", years=[2020])
    assert {918, 4, 12} <= resolved.members[2020]


def test_eu_institutions_with_eu_members_is_fine_for_recipients() -> None:
    # 918 is EDF and 30 Cyprus as DAC2A recipients.
    group = resolve_group(oda.Group("mine", [918, 30]), "recipient", years=[2023])
    assert group.members[2023] == {918, 30}


def test_two_groups_with_one_name_raise() -> None:
    with pytest.raises(oda.UnsupportedQueryError, match="g7"):
        resolve_selection(
            "provider", [oda.groups.g7, oda.Group("g7", [4])], years=[2023]
        )


def test_selection_needs_years() -> None:
    with pytest.raises(ValueError, match="year"):
        resolve_selection("provider", 4, years=[])


def test_the_resolver_is_built_once_across_threads(monkeypatch) -> None:
    monkeypatch.setattr(_resolve, "_resolver", None)
    with ThreadPoolExecutor(max_workers=8) as pool:
        built = list(pool.map(lambda _: _resolve.resolver(), range(16)))
    assert len({id(resolver) for resolver in built}) == 1


def test_a_sector_heading_resolves_to_every_code_under_it() -> None:
    selection = resolve_selection("sector", 122, years=[2023])
    assert 122 in selection.codes
    assert 12220 in selection.codes
    assert all(str(code).startswith("122") for code in selection.codes)


def test_a_heading_means_the_same_in_a_group_as_in_sectors() -> None:
    group = resolve_group(oda.SectorGroup("x", [151]), "sector", years=[2023])
    assert group.members[2023] == resolve_selection("sector", 151, years=[2023]).codes


def test_reported_at_reads_the_rows_the_built_in_groups_list() -> None:
    built_in = resolve_group(
        oda.sectors.conflict_peace_and_security, "sector", years=[2023]
    ).members[2023]
    own = oda.SectorGroup("x", [oda.sectors.reported_at(151)])
    assert resolve_group(own, "sector", years=[2023]).members[2023] == {151}
    assert 151 in built_in
    assert 15110 not in built_in  # government, not conflict
    selection = resolve_selection("sector", oda.sectors.reported_at(151), years=[2023])
    assert selection.codes == {151}


def test_a_group_rebuilt_from_a_built_in_groups_members_reads_the_same_codes() -> None:
    scope = {"reported_at": oda.sectors.reported_at}
    for slug, group in oda.sectors.items():
        rebuilt = oda.SectorGroup(slug, eval(repr(group.members), scope))
        assert resolve_group(rebuilt, "sector", years=[2023]) == resolve_group(
            group, "sector", years=[2023]
        ), slug


@pytest.mark.parametrize("code", [12220, 1000, 199, 0])
def test_reported_at_takes_only_a_dac5_heading(code: int) -> None:
    with pytest.raises(ValueError, match="DAC5 heading"):
        oda.sectors.reported_at(code)


def test_reported_at_reads_only_in_a_sector_filter() -> None:
    with pytest.raises(oda.UnknownEntityError, match="sector heading"):
        resolve_selection("provider", oda.sectors.reported_at(151), years=[2023])

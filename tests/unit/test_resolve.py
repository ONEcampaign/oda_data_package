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
        ("recipient", "Turkey", 55),
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


@pytest.mark.parametrize(
    ("dimension", "value", "system"),
    [
        ("provider", "Atlantis", "oecd:provider"),
        ("provider", "Kosovo", "oecd:provider"),  # a recipient, not a provider
        ("provider", 99999, "oecd:provider"),
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
    assert both.rows == both.codes == frozenset({*dac.members, 55})
    assert both.totals == ("dac_countries",)
    assert both.labels == {"dac_countries": "DAC countries"}

    total_only = resolve_selection("provider", oda.total(oda.groups.g7), years=[2023])
    assert total_only.rows == frozenset()
    assert total_only.codes == frozenset(oda.groups.g7.members)
    assert [g.name for g in total_only.groups] == ["g7"]


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
        (oda.Group("eu", [918, "France"]), "provider", oda.UnsupportedQueryError),
        (oda.Group("agg", [20001, 4]), "provider", oda.UnsupportedQueryError),
        (oda.groups.g7, "recipient", oda.UnknownEntityError),
        (oda.Group("odd", ["Frnace"]), "provider", oda.UnknownEntityError),
        (oda.groups.g7, "channel", oda.UnsupportedQueryError),
    ],
)
def test_a_group_that_cannot_be_summed_raises(group, dimension, error) -> None:
    with pytest.raises(error):
        resolve_group(group, dimension, years=[2023])


@pytest.mark.parametrize(
    ("years", "raises"),
    [([2020], True), ([2021, 2022], False), ([2019, 2021], True)],
    ids=["a-member", "after-it-left", "a-member-in-one-year"],
)
def test_eu_institutions_clash_with_the_uk_while_it_was_an_eu_member(
    years: list[int], raises: bool
) -> None:
    uk = oda.Group("uk", [918, 12])
    if raises:
        with pytest.raises(oda.UnsupportedQueryError, match=r"\[12\]"):
            resolve_group(uk, "provider", years=years)
    else:
        assert resolve_group(uk, "provider", years=years).members[years[0]] == {918, 12}


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

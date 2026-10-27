"""Names per dimension and the OECD aggregate codes."""

import pytest

from oda_data._names import aggregate_codes, is_aggregate, name_of, names, normalise


@pytest.mark.parametrize(
    ("dimension", "code", "table", "name"),
    [
        ("provider", 4, None, "France"),
        ("provider", 918, None, "EU Institutions"),
        ("provider", 20001, None, "DAC Countries, Total"),
        ("recipient", 248, None, "Kenya"),
        ("recipient", 289, None, "South of Sahara, regional"),
        ("sector", 12220, None, "Basic health care"),
        ("channel", 41114, None, "United Nations Development Programme"),
        ("flow", 1140, None, "Disbursements, net"),
        ("modality", "A01", None, "Budget support"),
        ("finance_type", 110, None, "Standard grant"),
        ("line", 1010, "dac1", "Official Development Assistance (ODA)"),
        ("line", 207, "dac2a", "Technical cooperation, disbursements"),
        ("broad_sector", "health", None, "Health"),
    ],
)
def test_name_of_a_code(dimension, code, table, name) -> None:
    assert name_of(dimension, code, table=table) == name


@pytest.mark.parametrize("dimension", ["finance_type", "channel"])
def test_names_are_unique(dimension) -> None:
    # A shared name would make that name ambiguous as a filter.
    found = [normalise(name) for name in names(dimension).values()]
    assert found
    assert len(set(found)) == len(found)


def test_lines_are_numbered_per_table() -> None:
    assert name_of("line", 207, table="dac1") != name_of("line", 207, table="dac2a")


def test_line_names_need_a_table() -> None:
    with pytest.raises(ValueError, match="table"):
        names("line")


def test_dimensions_without_names_are_empty() -> None:
    assert names("year") == {}


def test_unknown_code_has_no_name() -> None:
    assert name_of("provider", 123456) is None


@pytest.mark.parametrize(
    ("dimension", "code", "expected"),
    [
        ("provider", 20001, True),
        ("provider", 21600, True),
        ("provider", 29999, True),  # every provider code from 20000 up
        ("provider", 918, False),
        ("recipient", 10100, True),
        ("recipient", 10024, True),
        ("recipient", 289, False),  # a regional row, not a total
        ("recipient", 1027, False),
        ("recipient", 9998, False),
        ("recipient", 10280, False),  # Middle Africa, unspecified
        ("recipient", 15006, True),  # Regional and Unspecified, Total
        ("sector", 12220, False),
    ],
)
def test_is_aggregate(dimension, code, expected) -> None:
    assert is_aggregate(dimension, code) is expected


def test_recipient_aggregates_all_have_names() -> None:
    recipient_names = names("recipient")
    assert aggregate_codes("recipient") <= set(recipient_names)


@pytest.mark.parametrize(
    ("left", "right"),
    [
        ("Côte d\N{RIGHT SINGLE QUOTATION MARK}Ivoire", "cote d'ivoire"),
        ("  Türkiye ", "turkiye"),
        ("DAC  Countries", "dac countries"),
    ],
)
def test_normalise_folds_case_accents_quotes_and_spaces(left, right) -> None:
    assert normalise(left) == normalise(right)


def test_normalise_keeps_typos_apart() -> None:
    assert normalise("Frnace") != normalise("France")

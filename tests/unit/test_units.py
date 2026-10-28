"""Unit validation."""

import dataclasses

import pytest

import oda_data


def test_current_has_no_base_year() -> None:
    assert oda_data.current("USD") == oda_data.Unit("USD", "current", None)


def test_constant_carries_its_base_year() -> None:
    unit = oda_data.constant("EUR", 2023)
    assert (unit.currency, unit.prices, unit.base_year) == ("EUR", "constant", 2023)


def test_unit_is_frozen_and_hashable() -> None:
    unit = oda_data.constant("EUR", 2023)
    with pytest.raises(dataclasses.FrozenInstanceError):
        unit.currency = "USD"  # type: ignore[misc]
    assert {unit: 1}[oda_data.constant("EUR", 2023)] == 1


@pytest.mark.parametrize(
    ("args", "error"),
    [
        (("USD", "constant"), ValueError),
        (("USD", "current", 2023), ValueError),
        (("USD", "real"), ValueError),
        (("USD", "constant", "2023"), TypeError),
        (("USD", "constant", True), TypeError),
        ((None, "current"), TypeError),
        (("", "current"), TypeError),
    ],
)
def test_invalid_unit_is_refused(args: tuple, error: type[Exception]) -> None:
    with pytest.raises(error):
        oda_data.Unit(*args)

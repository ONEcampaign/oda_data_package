"""Units of money: a currency with current or constant prices."""

from dataclasses import dataclass
from typing import Literal

type Prices = Literal["current", "constant"]

_PRICES = ("current", "constant")


@dataclass(frozen=True, slots=True)
class Unit:
    """A currency and a price basis.

    Constant prices need a base year, and current prices must not have one.

    Args:
        currency: Currency code, such as ``"USD"`` or ``"EUR"``.
        prices: ``"current"`` or ``"constant"``.
        base_year: The year whose prices a constant-price unit uses.

    Raises:
        ValueError: If ``prices`` is neither ``"current"`` nor ``"constant"``, if
            constant prices lack a base year, or if current prices carry one.
        TypeError: If ``currency`` is not a string or ``base_year`` is not an int.
    """

    currency: str
    prices: Prices = "current"
    base_year: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.currency, str) or not self.currency:
            raise TypeError(
                f"currency must be a non-empty string, got {self.currency!r}"
            )
        if self.prices not in _PRICES:
            raise ValueError(
                f"prices must be 'current' or 'constant', got {self.prices!r}"
            )
        if self.base_year is not None and (
            isinstance(self.base_year, bool) or not isinstance(self.base_year, int)
        ):
            raise TypeError(f"base_year must be an int, got {self.base_year!r}")
        if self.prices == "constant" and self.base_year is None:
            raise ValueError("constant prices need a base_year")
        if self.prices == "current" and self.base_year is not None:
            raise ValueError("current prices take no base_year")


def current(currency: str) -> Unit:
    """Return a unit in current prices.

    Args:
        currency: Currency code, such as ``"USD"``.

    Returns:
        A ``Unit`` with current prices and no base year.
    """
    return Unit(currency, "current")


def constant(currency: str, base_year: int) -> Unit:
    """Return a unit in constant prices of ``base_year``.

    Args:
        currency: Currency code, such as ``"EUR"``.
        base_year: The year whose prices the unit holds fixed.

    Returns:
        A ``Unit`` with constant prices.
    """
    return Unit(currency, "constant", base_year)


for _public in (Unit, current, constant):
    _public.__module__ = "oda_data"

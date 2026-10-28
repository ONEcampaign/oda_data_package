"""Placeholders for public names that have no implementation yet.

Each name here raises ``NotImplementedError`` when called or constructed.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, NoReturn

if TYPE_CHECKING:
    import pandas as pd

    from oda_data._types import IndicatorId


def _unbuilt(name: str) -> NoReturn:
    raise NotImplementedError(f"oda_data.{name} is not implemented yet")


def indicators() -> pd.DataFrame:
    """List every indicator."""
    _unbuilt("indicators")


def search(text: str) -> pd.DataFrame:
    """Find indicators, groups, sectors, and DAC lines that match ``text``."""
    _unbuilt("search")


def describe(indicator: IndicatorId | str, /) -> IndicatorInfo:
    """Return an indicator's definition."""
    _unbuilt("describe")


def add_names(df: pd.DataFrame) -> pd.DataFrame:
    """Add names beside the codes in a DataFrame."""
    _unbuilt("add_names")


class IndicatorInfo:
    """An indicator's definition, printed in notebooks."""

    def __init__(self) -> None:
        _unbuilt("IndicatorInfo")


for _public in (indicators, search, describe, add_names, IndicatorInfo):
    _public.__module__ = "oda_data"

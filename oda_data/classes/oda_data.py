from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from oda_data import config
from oda_data.clean_data import common as clean
from oda_data.get_data.common import check_integers
from oda_data.logger import logger
from oda_data.read_data.read import read_dac1, read_dac2a, read_crs, read_multisystem

READERS: dict[str, callable] = {
    "dac1": read_dac1,
    "dac2a": read_dac2a,
    "crs": read_crs,
    "multisystem": read_multisystem,
}

CURRENCIES: dict[str, str] = {
    "USD": "USA",
    "EUR": "EUI",
    "GBP": "GBR",
    "CAD": "CAN",
}


def _load_indicators() -> dict[str, dict]:
    return clean.read_settings(config.OdaPATHS.indicators / "indicators.json")


def _key_cols() -> dict[str, dict | list]:
    return clean.read_settings(config.OdaPATHS.cleaning_config / "key_columns.json")


@dataclass
class ODAData:
    years: list | int | range = field(default_factory=list)
    donors: list | int | None = None
    recipients: list | int | None = None
    currency: str = "USD"
    prices: str = "current"
    base_year: int | None = None

    def __post_init__(self) -> None:
        """Loads the indicators json to the object"""
        self.indicators_data: dict[str, pd.DataFrame] = {}
        self._data: dict[str, pd.DataFrame] = {}
        self._indicators_json: dict[str, dict] = _load_indicators()

        # make donors and recipients into lists
        if self.donors is not None:
            self.donors = check_integers(self.donors)
        if self.recipients is not None:
            self.recipients = check_integers(self.recipients)

        # Check that a valid currency has been requested
        if self.currency not in CURRENCIES:
            raise ValueError(f"Currency {self.currency} is not supported")

        # Check that a valid prices has been requested
        if self.prices not in ["current", "constant"]:
            raise ValueError(f"Prices must be 'current' or 'constant'")

        # Check that a valid base year has been requested
        if self.base_year is not None and self.prices == "current":
            raise ValueError(f"Base year can only be specified if prices are constant")
        if self.base_year is None and self.prices == "constant":
            raise ValueError(f"Base year must be specified for constant prices")

    def _load_raw_data(self, indicator: str) -> None:
        """Loads the data for the specified indicator, if the data is not
        already loaded."""

        # Identify the data source
        source: str = self._indicators_json[indicator]["source"]

        # Load the data if it is not already loaded
        if source not in self._data.keys():
            self._data[source] = READERS[source](years=self.years)

    def _filter_indicator_data(self, indicator: str) -> pd.DataFrame:
        """Filters the data for the specified indicator"""
        # track available columns
        available_cols = self._data[self._indicators_json[indicator]["source"]].columns

        # An empty list to track all the required filters
        conditions: list = []

        # go through all the filters and add them to the query string
        for dimension, value in self._indicators_json[indicator]["filters"].items():
            if isinstance(value, str):
                value = f"'{value}'"
            conditions.append(f"{dimension} == {value}")

        # Add the donor filters
        if self.donors is not None:
            conditions.append(f"donor_code in {self.donors}")

        # Add the recipient filter, checking that it is possible for this indicator
        if self.recipients is not None and "recipient_code" in available_cols:
            conditions.append(f"recipient_code in {self.recipients}")
        elif self.recipients is not None and "recipient_code" not in available_cols:
            logger.warning(f"Recipient filtering not available for {indicator}")

        # Build the query string
        query: str = " & ".join(conditions)

        # Load data into a temporary variable
        data_ = self._data[self._indicators_json[indicator]["source"]]

        # If settings detail a value column, use that. This is useful for the CRS
        if "value_column" in self._indicators_json[indicator]:
            data_ = data_.rename(
                columns={self._indicators_json[indicator]["value_column"]: "value"}
            )

        # Column settings
        column_settings = _key_cols()

        # Filter the data, keep only the important columns, assign the indicator name
        return (
            data_.query(query)
            .filter(
                column_settings[self._indicators_json[indicator]["source"]]["keep"],
                axis=1,
            )
            .rename(
                columns=column_settings[self._indicators_json[indicator]["source"]][
                    "rename"
                ]
            )
            .assign(indicator=indicator)
            .reset_index(drop=True)
        )

    def _build_one_indicator(self, indicator: str) -> pd.DataFrame:
        """Builds data for an indicator used by ONE, made up of other 'raw' indicators"""

        # Required indicators
        required_indicators: list = self._indicators_json[indicator]["indicators"]

        # Group by columns
        group_by_cols: list = self._indicators_json[indicator]["group_by"]

        return (
            pd.concat(
                [
                    self._filter_indicator_data(indicator)
                    for indicator in required_indicators
                ],
                ignore_index=True,
            )
            .assign(indicator=indicator)
            .groupby(group_by_cols, observed=True)
            .sum(numeric_only=True)
            .reset_index()
        )

    def _convert_units(self, indicator: str) -> None:
        """Converts to the requested units/prices combination"""
        if self.currency == "USD" and self.prices == "current":
            self.indicators_data[indicator] = self.indicators_data[indicator].assign(
                currency=self.currency, prices=self.prices
            )

        elif self.prices == "current":
            self.indicators_data[indicator] = clean.dac_exchange(
                df=self.indicators_data[indicator],
                target_currency=CURRENCIES[self.currency],
            ).assign(currency=self.currency, prices=self.prices)

        else:
            self.indicators_data[indicator] = clean.dac_deflate(
                df=self.indicators_data[indicator],
                base_year=self.base_year,
                target_currency=CURRENCIES[self.currency],
            ).assign(currency=self.currency, prices=self.prices)

    def available_indicators(self) -> None:
        """Logs a list of available indicators."""
        indicators = "\n".join(self._indicators_json)
        logger.info(f"Available indicators:\n{indicators}")

    def load_indicator(self, indicator: str) -> ODAData:
        """Loads data for the specified indicator. Any parameters specified for
        the object (years, donors, prices, etc.) are applied to the data.

        Args:
            indicator: a string with an indicator code. Call `available_indicators()` to
            view a list of available indicators. See project documentation for
            more details.

        """

        # Load necessary data to the object
        self._load_raw_data(indicator)

        # Indicator type
        ind_type = self._indicators_json[indicator]["type"]

        # Load the indicator data to the object
        if ind_type == "dac":
            self.indicators_data[indicator] = self._filter_indicator_data(indicator)
        elif ind_type == "one":
            self.indicators_data[indicator] = self._build_one_indicator(indicator)

        # Convert the units if necessary
        self._convert_units(indicator)

        return self

    def get_data(self, indicators: str | list[str] = "all") -> pd.DataFrame:
        """
        Get the data as a Pandas DataFrame
        Args:
            indicators: By default, all indicators are returned in a single DataFrame.
            If a list of indicators is passed, only those indicators will be returned.
            A single indicator can be passed as a string as well.

        Returns:
            A Pandas DataFrame with the data for the indicators requested.
        """
        # if indicator is a string, transform to list unless it is "all"
        if indicators != "all" and isinstance(indicators, str):
            indicators = [indicators]

        # if indicators is a list, load all the dataframes into a list
        if isinstance(indicators, list):
            indicators = [
                self.indicators_data[_]
                for _ in indicators
                if _ in list(self.indicators_data)
            ]

        # if indicator is "all", use all indicators
        elif indicators == "all":
            indicators = self.indicators_data.values()

        return pd.concat(indicators, ignore_index=True)

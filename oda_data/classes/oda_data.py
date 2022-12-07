from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from oda_data import config
from oda_data.classes.representations import _OdaList, _OdaDict
from oda_data.clean_data import common as clean
from oda_data.clean_data.common import reorder_columns
from oda_data.get_data.common import check_integers
from oda_data.indicators import research_indicators
from oda_data.indicators.linked_indicators import linked_indicator
from oda_data.logger import logger
from oda_data.read_data.read import read_dac1, read_dac2a, read_crs, read_multisystem
from oda_data.tools import names

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
    return clean.read_settings(config.OdaPATHS.settings / "indicators.json")


def _key_cols() -> dict[str, dict | list]:
    return clean.read_settings(config.OdaPATHS.settings / "key_columns.json")


def _group_output(df: pd.DataFrame, idx_cols: list) -> pd.DataFrame:
    """Group output by idx_cols and sum all other columns."""

    # Warn if some index columns aren't present in the data
    missing_cols = [col for col in idx_cols if col not in df.columns]
    if len(missing_cols) > 0:
        logger.warning(f"The following columns are not available: {missing_cols}")

    # Check that value wasn't included
    idx_cols = [c for c in idx_cols if c != "value"]

    # Ensure that columns actually exist in the dataframe
    cols = [c for c in df.columns if c in idx_cols]

    return (
        df.filter(cols + ["value"], axis=1)
        .groupby(cols, as_index=False, observed=True, dropna=False)["value"]
        .sum()
    )


def _drop_name_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Drop columns that contain names"""
    return df.drop(columns=[c for c in df.columns if "name" in c])


@dataclass
class ODAData:
    """A class for working with ODA data.

    Attributes:
        years: A list of years to include in the data. If not specified, no data is returned.
        donors: A list of donor codes to include in the data. If not specified,
            all donors are included.
        recipients: A list of recipient codes to include in the data. If not specified,
            all recipients are included.
        currency: The currency to use for the data. Defaults to USD. Other available currencies
            are EUR, GBP and CAD.
        prices: The prices to use for the data. Defaults to current prices but can
            also be 'constant'.
        base_year: The base year to use for constant prices. It is only required if
            prices is set to 'constant'. If not specified in that case, an error is raised.
        include_names: Whether to add names to the data. Defaults to False.
    """

    years: list | int | range = field(default_factory=list)
    donors: list | int | None = None
    recipients: list | int | None = None
    currency: str = "USD"
    prices: str = "current"
    base_year: int | None = None
    include_names: bool = False

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

        # set defaults for output settings
        self._output_config: dict = {
            "output_cols": None,
            "simplify_output": False,
            "add_names": self.include_names,
            "id_cols": None,
        }

    def _load_raw_data(self, indicator: str) -> None:
        """Loads the data for the specified indicator, if the data is not
        already loaded."""

        # Identify the data source
        source: str = self._indicators_json[indicator]["source"]

        # Load the data if it is not already loaded
        if source not in self._data.keys():
            self._data[source] = READERS[source](years=self.years)

    def _filter_indicator_data(self, indicator: str) -> pd.DataFrame:
        """Filters the data for the specified indicator

        Args:
            indicator: the indicator to filter
        """
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

        # columns to keep
        keep = column_settings[self._indicators_json[indicator]["source"]]["keep"]

        # columns names
        names = column_settings[self._indicators_json[indicator]["source"]]["rename"]

        # Filter the data, keep only the important columns, assign the indicator name
        return (
            data_.query(query)
            .filter(keep, axis=1)
            .rename(columns=names)
            .assign(indicator=indicator)
            .reset_index(drop=True)
            .pipe(_drop_name_cols)
        )

    def _build_one_indicator(self, indicator: str) -> pd.DataFrame:
        """Builds data for an indicator used by ONE, made up of other 'raw' indicators"""

        # Required indicators
        required_indicators: list = self._indicators_json[indicator]["indicators"]

        # Columns which should be ignored when grouping
        exclude_from_group = self._indicators_json[indicator]["group_by"] + ["value"]

        # combined data
        combined: pd.DataFrame = pd.concat(
            [
                self._filter_indicator_data(indicator)
                for indicator in required_indicators
            ],
            ignore_index=True,
        )

        # Group by columns
        group_cols = [col for col in combined.columns if col not in exclude_from_group]

        return (
            combined.assign(indicator=indicator)
            .groupby(group_cols, observed=True, dropna=False)
            .sum(numeric_only=True)
            .reset_index()
        )

    def _build_linked_indicator(self, indicator: str) -> pd.DataFrame:

        # Components dict
        components: dict = self._indicators_json[indicator]["components"]
        # Required indicators
        required_indicators: list = [components["main"], components["secondary"]]

        # Create a combined DataFrame
        combined: pd.DataFrame = pd.concat(
            [
                self._filter_indicator_data(indicator)
                for indicator in required_indicators
            ],
            ignore_index=True,
        )

        return linked_indicator(
            data=combined,
            main_indicator=components["main"],
            fallback_indicator=components["secondary"],
            indicator_name=components["new"],
        )

    def _build_research_indicator(self, indicator: str) -> pd.DataFrame:
        # Components dict
        function: str = self._indicators_json[indicator]["function"]

        # function as callable
        try:
            function_callable: callable = getattr(research_indicators, function)
        except AttributeError:
            raise NotImplementedError(f"Function {function} not found")

        return (
            function_callable(**self.arguments)
            .assign(indicator=indicator)
            .pipe(_drop_name_cols)
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

    @property
    def arguments(self):
        """Returns the arguments used by the object"""
        return {
            "years": self.years,
            "donors": self.donors,
            "recipients": self.recipients,
            "currency": self.currency,
            "prices": self.prices,
            "base_year": self.base_year,
        }

    @staticmethod
    def available_donors() -> dict:
        """Returns a dictionary of available donor codes and their names"""
        logger.info("Note that not all donors may be available for all indicators")
        return _OdaDict(names.donor_names())

    @staticmethod
    def available_recipients() -> dict:
        """Returns a dictionary of available recipient codes"""
        logger.info("Note that not all recipients may be available for all indicators")
        return _OdaDict(names.recipient_names())

    @staticmethod
    def available_currencies() -> list:
        """Returns a dictionary of available currencies"""
        return _OdaList(CURRENCIES)

    def available_indicators(self) -> list:
        """Returns a list of indicators"""
        return _OdaList(self._indicators_json.keys())

    def simplify_output_df(self, columns_to_keep: list) -> ODAData:
        """Simplifies the output DataFrame by summarising the data and removing
        the columns which are not needed.

        Args:
            columns_to_keep: the list of columns that should be kept in the output.
            Note that this function performs a `.groupby()` using this list"""

        # Store the list of columns as an instance variable
        self._output_config["output_cols"] = columns_to_keep
        # Store the 'request' to simply output as an instance variable
        self._output_config["simplify_output"] = True

        return self

    def add_names(self, id_columns: str | list[str] | None = None) -> ODAData:
        """Adds names to the output DataFrame."""
        self._output_config["add_names"] = True

        if isinstance(id_columns, str):
            id_columns = [id_columns]

        self._output_config["id_cols"] = id_columns

        return self

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
        elif ind_type == "one_linked":
            self.indicators_data[indicator] = self._build_linked_indicator(indicator)
        elif ind_type == "one_research":
            self.indicators_data[indicator] = self._build_research_indicator(indicator)

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

        data = pd.concat(indicators, ignore_index=True)

        if self._output_config["simplify_output"]:
            data = _group_output(data, self._output_config["output_cols"])

        if self._output_config["add_names"]:
            data = names.add_name(df=data, name_id=self._output_config["id_cols"])

        return data.pipe(reorder_columns)

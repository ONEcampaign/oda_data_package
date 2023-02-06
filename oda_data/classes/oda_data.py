from __future__ import annotations

import copy
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
            "add_share_of_total": False,
            "include_share_of": False,
            "add_share_of_gni": False,
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
            if isinstance(value, list):
                conditions.append(f"{dimension} in {value}")
            else:
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

        data = (
            data_.query(query)
            .filter(keep, axis=1)
            .rename(columns=names)
            .assign(indicator=indicator)
            .reset_index(drop=True)
            .pipe(_drop_name_cols)
        )

        if "group" not in self._indicators_json[indicator]:
            return data

        elif self._indicators_json[indicator]["group"]:
            return (
                data.groupby(
                    [c for c in data.columns if c != "value"],
                    dropna=False,
                    observed=True,
                )
                .sum(numeric_only=True)
                .reset_index()
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
            .assign(indicators=indicator)
            .pipe(_drop_name_cols)
        )

    def _convert_units(self, indicator: str) -> None:
        """Converts to the requested units/prices combination"""

        if self.currency == "USD" and self.prices == "current":
            self.indicators_data[indicator] = self.indicators_data[indicator].assign(
                currency=self.currency, prices=self.prices
            )

        # If indicator is a ratio, don't convert
        if indicator in ["oda_gni_flow", "oda_gni_ge"]:
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

    def _add_share(self, data: pd.DataFrame, include_share_of: bool) -> pd.DataFrame:
        """Adds a share column to the data

        Args:
            data: a DataFrame in its final form before returning to user
            include_share_of: whether to include the indicator based on which
            the share is calculated
        """

        def __add_indicator_share(
            object_: ODAData, d_: pd.DataFrame, indicator: str
        ) -> pd.DataFrame:
            """A helper function to add the share data to each indicator in the
            DataFrame that will be returned to the user."""

            # Filter the dataframe to keep only the relevant indicator. All columns which are
            # not relevant to the indicator are dropped.
            d_ = d_.loc[lambda d: d.indicator == indicator].dropna(axis=1, how="all")

            # Save the indicator that is used to calculate the share
            total_indicator = share_settings[indicator]

            # If there is no valid total dataframe, return all missing values
            if total_indicator == "":
                return d_.assign(share=pd.NA)

            # In the case of DAC2a, total shares must be calculated for "All Developing
            # Countries". This ensures that share is always of 'Total'.
            if self._indicators_json[indicator]["source"] == "dac2a":
                object_.recipients = [10100]
            else:
                # In all other cases, the recipients are set to None. This either captures
                # all recipients (like the CRS) or removes recipients when it's not pertinent (DAC1)
                object_.recipients = None

            # Get the total data for the indicator
            total_data = (
                object_.load_indicator(total_indicator)
                .simplify_output_df(columns_to_keep=["year", "donor_code", "indicator"])
                .get_data(total_indicator)
                .rename(columns={"indicator": "share_of", "value": "total_value"})
            )

            # Create a list of merge columns
            cols = [
                col
                for col in total_data.columns
                if col in d_.columns and col not in ["value", "indicator"]
            ]

            # Merge the data with the total data and calculate the share
            d_ = (
                d_.merge(total_data, on=cols, how="left")
                .assign(share=lambda d: round(100 * d.value / d.total_value, 5))
                .drop(columns=["total_value"])
            )

            return d_

        # For each indicator, load the corresponding 'total' indicator
        share_settings = {
            indicator: column["share_of"]
            for indicator, column in self._indicators_json.items()
            if "share_of" in column
        }

        # Create a copy of the object to handle the total indicators.
        obj = copy.deepcopy(self)
        obj._output_config["add_share_of_total"] = False
        obj._output_config["include_share_of"] = False

        # Create an empty DataFrame to store the share data
        share_data = pd.DataFrame()

        # For each indicator, add the share data
        for indicator in data.indicator.unique():
            i_data = __add_indicator_share(object_=obj, d_=data, indicator=indicator)
            share_data = pd.concat([share_data, i_data], ignore_index=True)

        # If not requested drop the share_of column
        if not include_share_of:
            try:
                share_data = share_data.drop(columns=["share_of"])
            except KeyError:
                pass

        return share_data

    def _add_gni_share(self, data: pd.DataFrame) -> pd.DataFrame:
        """Adds a share of GNI column to the data"""
        # Create a copy of the object to handle the total indicators.
        obj = copy.deepcopy(self)
        obj._output_config["add_share_of_gni"] = False
        obj.recipients = None

        # Load the GNI data
        gni_data = (
            obj.load_indicator("gni")
            .get_data("gni")
            .filter(["year", "donor_code", "value"], axis=1)
            .rename(columns={"value": "gni"})
        )

        # Merge the GNI data with the indicator data, calculate share of GNI, drop GNI
        data = (
            data.merge(gni_data, on=["year", "donor_code"], how="left")
            .assign(gni_share=lambda d: round(100 * d.value / d.gni, 5))
            .drop(columns=["gni"])
        )

        # if indicator is ratio, assign null to gni share
        ratios = ["oda_gni_flow", "oda_gni_ge"]
        data.loc[data.indicator.isin(ratios), "gni_share"] = pd.NA

        return data

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

    @classmethod
    def available_donors(cls) -> dict:
        """Returns a dictionary of available donor codes and their names"""
        logger.info("Note that not all donors may be available for all indicators")
        return _OdaDict(names.donor_names())

    @classmethod
    def available_recipients(cls) -> dict:
        """Returns a dictionary of available recipient codes"""
        logger.info("Note that not all recipients may be available for all indicators")
        return _OdaDict(names.recipient_names())

    @classmethod
    def available_currencies(cls) -> list:
        """Returns a dictionary of available currencies"""
        return _OdaList(CURRENCIES)

    @classmethod
    def available_indicators(cls) -> list:
        """Returns a list of indicators"""
        return _OdaList(_load_indicators().keys())

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

    def add_share_of_total(self, include_share_of: bool = False) -> ODAData:
        """Adds a column to the output DataFrame which shows the value as a
        share of total ODA. Since the definition of "total" can vary,
        you can optionally request a 'share_of' column which explicitly
        indicates the 'total' value used.

        Args:
            include_share_of: if True, adds a column which explicitly
            indicates the 'total' value used.
        """

        # Flag that a share of total column should be added
        self._output_config["add_share_of_total"] = True
        # Flag that a 'share_of' column should be added
        self._output_config["include_share_of"] = include_share_of

        return self

    def add_share_of_gni(self) -> ODAData:
        """Adds a column to the output DataFrame which shows the value as a
        share of GNI.
        """

        # Flag that a share of total column should be added
        self._output_config["add_share_of_gni"] = True

        return self

    def add_names(self, id_columns: str | list[str] | None = None) -> ODAData:
        """Adds names to the output DataFrame."""
        self._output_config["add_names"] = True

        if isinstance(id_columns, str):
            id_columns = [id_columns]

        self._output_config["id_cols"] = id_columns

        return self

    def load_indicator(self, indicators: str | list[str], **kwargs) -> ODAData:
        """Loads data for the specified indicator. Any parameters specified for
        the object (years, donors, prices, etc.) are applied to the data.

        Args:
            indicators: a string with an indicator code. A list of
            indicators can also be passed, which is equivalent to calling this
            method multiple times. Call `available_indicators()` to
            view a list of available indicators. See project documentation for
            more details.

        """

        if "indicator" in kwargs:
            indicators = kwargs.pop("indicator")

        if len(kwargs) > 0:
            raise ValueError(f"Unexpected keyword arguments: {kwargs}")

        if isinstance(indicators, str):
            indicators = [indicators]

        def __load_single_indicator(ind_: str) -> None:
            # Load necessary data to the object
            self._load_raw_data(ind_)

            # Indicator type
            ind_type = self._indicators_json[ind_]["type"]

            # Load the indicator data to the object
            if ind_type == "dac":
                self.indicators_data[ind_] = self._filter_indicator_data(ind_)
            elif ind_type == "one":
                self.indicators_data[ind_] = self._build_one_indicator(ind_)
            elif ind_type == "one_linked":
                self.indicators_data[ind_] = self._build_linked_indicator(ind_)
            elif ind_type == "one_research":
                self.indicators_data[ind_] = self._build_research_indicator(ind_)

            # Convert the units if necessary
            self._convert_units(ind_)

        # Load each indicator
        for single_indicator in indicators:
            __load_single_indicator(ind_=single_indicator)

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

        if self._output_config["add_share_of_total"]:
            data = self._add_share(data, self._output_config["include_share_of"])

        if self._output_config["add_share_of_gni"]:
            data = self._add_gni_share(data)

        return data.pipe(reorder_columns)

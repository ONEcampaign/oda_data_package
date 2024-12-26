from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Any

import pandas as pd

from oda_data import config
from oda_data.classes.representations import _OdaList, _OdaDict
from oda_data.clean_data import common as clean
from oda_data.get_data.common import check_integers
from oda_data.indicators.dac1 import dac1_functions

from oda_data.indicators.dac2a import dac2a_functions
from oda_data.logger import logger
from oda_data.read_data.read import read_dac1, read_dac2a, read_crs, read_multisystem
from oda_data.tools import names

Measure = Literal[
    "grant_equivalent", "gross_disbursement", "commitment", "net_disbursement"
]

READERS: dict[str, callable] = {
    "dac1": read_dac1,
    "DAC1": read_dac1,
    "dac2a": read_dac2a,
    "DAC2A": read_dac2a,
    "crs": read_crs,
    "multisystem": read_multisystem,
}

CURRENCIES: dict[str, str] = {
    "USD": "USA",
    "EUR": "EUI",
    "GBP": "GBR",
    "CAD": "CAN",
    "LCU": "LCU",
}

PRICES = {"DAC1": {"column": "amounttype_code"}, "DAC2A": {"column": "data_type_code"}}

MEASURES: dict[str, dict] = {
    "DAC1": {
        "column": "flows_code",
        "commitment": 1150,
        "grant_equivalent": 1160,
        "net_disbursement": 1140,
        "gross_disbursement": 1120,
    },
    "DAC2A": {
        "column": "flow_type_code",
        "net_disbursement": "D",
        "gross_disbursement": "D",
    },
}

_EXCLUDE = [
    "amounttype_code",
    "amount_type",
    "base_period",
    "data_type_code",
    "unit_measure_name",
    "unit_measure_code",
]


def validate_measure(source, measure):
    if measure not in MEASURES[source]:
        raise ValueError(f"{measure} is not valid for {source}")

    return MEASURES[source][measure]


def _load_indicators() -> dict[str, dict]:
    # TODO: point at combined indicators
    dac1_indicators = clean.read_settings(
        config.OdaPATHS.indicators / "dac1" / "dac1_indicators.json"
    )

    dac2a_indicators = clean.read_settings(
        config.OdaPATHS.indicators / "dac2a" / "dac2a_indicators.json"
    )

    combined = {}

    for k, v in dac1_indicators.items():
        combined = combined | v

    for k, v in dac2a_indicators.items():
        combined = combined | v

    return combined


@dataclass
class ODAData2:
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
    providers: list | int | None = None
    recipients: list | int | None = None
    measure: list[Measure] | Measure = "gross_disbursement"
    currency: str = "USD"
    prices: str = "current"
    base_year: int | None = None

    def __post_init__(self) -> None:
        """Loads the indicators json to the object"""
        self.indicators_data: dict[str, list[pd.DataFrame] | pd.DataFrame] = {}
        self._indicators: dict[str, dict] = _load_indicators()

        # make donors and recipients into lists
        if self.providers is not None:
            self.providers = check_integers(self.providers)
        if self.recipients is not None:
            self.recipients = check_integers(self.recipients)

        # Check that a valid currency has been requested
        if self.currency not in CURRENCIES:
            raise ValueError(f"Currency {self.currency} is not supported")

        # Check that a valid measure has been requested
        if isinstance(self.measure, str):
            self.measure = [self.measure]

        # Check that a valid prices has been requested
        if self.prices not in ["current", "constant"]:
            raise ValueError(f"Prices must be 'current' or 'constant'")

        # Check that a valid base year has been requested
        if self.base_year is not None and self.prices == "current":
            raise ValueError(f"Base year can only be specified if prices are constant")
        if self.base_year is None and self.prices == "constant":
            raise ValueError(f"Base year must be specified for constant prices")

    def _filters_from_settings(
        self, indicator, sources, filters: dict[Any, Any]
    ) -> dict:

        for source in sources:
            _ = self._indicators[indicator]["filters"][source]
            if source not in filters:
                filters[source] = []
            filters[source].append(list(((k, *v) for k, v in _.items()))[0])

        return filters

    def _filter_providers(self, sources, filters: dict[Any, Any]) -> dict:
        for source in sources:
            if self.providers is not None:
                filters[source].append(("donor_code", "in", self.providers))

        return filters

    def _filter_recipients(self, sources, filters: dict[Any, Any]) -> dict:
        for source in sources:
            if source not in ["DAC1"]:
                if self.recipients is not None:
                    filters[source].append(("recipient_code", "in", self.recipients))
            else:
                logger.info(
                    f"{source} data is not presented by Recipient. This parameter "
                    f"will be ignored for the selected indicator."
                )

        return filters

    def _filter_currency(self, sources, filters: dict[Any, Any]) -> dict:
        # Prepare currency filter
        for source in sources:
            if self.currency == "LCU":
                filters[source].append((PRICES[source]["column"], "==", "N"))
            else:
                filters[source].append((PRICES[source]["column"], "==", "A"))

        return filters

    def _filter_measure(self, sources, filters: dict[Any, Any]) -> dict:
        # Prepare measure filter
        for source in sources:
            filters[source].append(
                (
                    MEASURES[source]["column"],
                    "in",
                    [validate_measure(source=source, measure=m) for m in self.measure],
                )
            )
        return filters

    def _validate_dac2a_measure(self, indicator):
        if (
            "gross"
            not in self.indicators_data[indicator]["aid_type"].str.lower().unique()
            and "gross_disbursement" in self.measure
        ):
            logger.warning(f"Gross disbursements are not available for {indicator}")
            self.indicators_data[indicator] = pd.DataFrame(
                columns=self.indicators_data[indicator].columns
            )

    def _load_data(self, indicator: str) -> None:
        """Loads the data for the specified indicator, if the data is not
        already loaded."""

        filters: dict[Any, Any] = {}

        # Identify the data source
        sources = self._indicators[indicator]["sources"]

        # Load filters from settings
        filters = self._filters_from_settings(
            indicator=indicator, sources=sources, filters=filters
        )

        # Load providers filter
        filters = self._filter_providers(sources=sources, filters=filters)

        # Load recipients filter
        filters = self._filter_recipients(sources=sources, filters=filters)

        # Load currency filter
        filters = self._filter_currency(sources=sources, filters=filters)

        # Load measure filter
        filters = self._filter_measure(sources=sources, filters=filters)

        # Load the data
        data = []
        for source in sources:
            _ = READERS[source](years=self.years, filters=filters[source])
            data.append(_.filter([c for c in _.columns if c not in _EXCLUDE]))

        self.indicators_data[indicator] = data if len(data) > 1 else data[0]

    def _process_data(self, indicator: str) -> None:

        custom_function = self._indicators[indicator]["custom_function"]
        main_source = self._indicators[indicator]["sources"][0]

        if len(custom_function) > 0:
            if main_source == "DAC1":
                try:
                    function_callable = getattr(dac1_functions, custom_function)
                except AttributeError:
                    raise NotImplementedError(
                        f"No custom function available for this indicator"
                    )
            elif main_source == "DAC2A":
                try:
                    function_callable = getattr(dac2a_functions, custom_function)
                except AttributeError:
                    raise NotImplementedError(
                        f"No custom function available for this indicator"
                    )

            self.indicators_data[indicator] = function_callable(
                self.indicators_data[indicator]
            )

        if "DAC2A" in main_source:
            self._validate_dac2a_measure(indicator=indicator)

    def _convert_units(self, indicator: str) -> None:
        """Converts to the requested units/prices combination"""

        if ".40." in indicator:
            self.indicators_data[indicator] = self.indicators_data[indicator].assign(
                currency=self.currency, prices=self.prices
            )
            return

        if self.currency in ["USD", "LCU"] and self.prices == "current":
            self.indicators_data[indicator] = self.indicators_data[indicator].assign(
                currency=self.currency, prices=self.prices
            )

        elif self.prices == "current":
            self.indicators_data[indicator] = clean.dac_exchange(
                data=self.indicators_data[indicator],
                target_currency=CURRENCIES[self.currency],
            ).assign(currency=self.currency, prices=self.prices)

        else:
            self.indicators_data[indicator] = clean.dac_deflate(
                data=self.indicators_data[indicator],
                base_year=self.base_year,
                target_currency=CURRENCIES[self.currency],
            ).assign(currency=self.currency, prices=self.prices)

    @property
    def arguments(self):
        """Returns the arguments used by the object"""
        return {
            "years": self.years,
            "providers": self.providers,
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

    def get_indicators(self, indicators: str | list[str]) -> pd.DataFrame:
        """Loads data for the specified indicator. Any parameters specified for
        the object (years, donors, prices, etc.) are applied to the data.

        Args:
            indicators: a string with an indicator code. A list of
            indicators can also be passed, which is equivalent to calling this
            method multiple times. Call `available_indicators()` to
            view a list of available indicators. See project documentation for
            more details.

        """

        if isinstance(indicators, str):
            indicators = [indicators]

        def __load_single_indicator(ind_: str) -> None:
            logger.info(f"Fetching data for {ind_}")
            # Load necessary data to the object
            self._load_data(ind_)

            # proces data
            self._process_data(ind_)

            # Convert the units if necessary
            self._convert_units(ind_)

            logger.info(f"{ind_} successfully loaded.")

        # Load each indicator
        for single_indicator in indicators:
            __load_single_indicator(ind_=single_indicator)

        return pd.concat(
            [d.assign(one_indicator=i) for i, d in self.indicators_data.items()],
            ignore_index=True,
        )


if __name__ == "__main__":
    df = ODAData2(
        years=range(2022, 2023),
        providers=[4, 12],
        recipients=[55],
        measure=["net_disbursement"],
    ).get_indicators(
        [
            # "DAC2A.10.106",
            # "DAC1.10.1520",
            "ONE.10.206_106"
        ]
    )

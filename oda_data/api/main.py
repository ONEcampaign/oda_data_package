from dataclasses import dataclass, field

import pandas as pd

from oda_data import config
from oda_data.api.constants import (
    MEASURES,
    Measure,
    CURRENCIES,
    PRICES,
    _EXCLUDE,
)
from oda_data.api.representations import _OdaDict, _OdaList
from oda_data.api.sources import Dac1Data, Dac2Data, CrsData, MultiSystemData
from oda_data.clean_data import common as clean
from oda_data.clean_data.validation import validate_currency, validate_measure
from oda_data.indicators.crs import crs_functions
from oda_data.indicators.crs.common import group_data_based_on_indicator
from oda_data.indicators.dac1 import dac1_functions
from oda_data.indicators.dac2a import dac2a_functions
from oda_data.logger import logger
from oda_data.tools.groupings import donor_groupings, recipient_groupings

source_to_module = {
    "DAC1": dac1_functions,
    "DAC2A": dac2a_functions,
    "CRS": crs_functions,
}

READERS: dict[str, callable] = {
    "DAC1": Dac1Data,
    "DAC2A": Dac2Data,
    "CRS": CrsData,
    "MULTISYSTEM": MultiSystemData,
}


def get_measure_filter(source: str, measure: str) -> int | str:
    """Validate the requested measure against available measures for the source."""
    if measure not in MEASURES[source]:
        raise ValueError(f"{measure} is not valid for {source}")

    return MEASURES[source][measure]["filter"]


def load_indicators() -> dict[str, dict]:
    """Load indicator settings from JSON files."""
    dac1_indicators = clean.read_settings(
        config.OdaPATHS.indicators / "dac1" / "dac1_indicators.json"
    )

    dac2a_indicators = clean.read_settings(
        config.OdaPATHS.indicators / "dac2a" / "dac2a_indicators.json"
    )

    crs_indicators = clean.read_settings(
        config.OdaPATHS.indicators / "crs" / "crs_indicators.json"
    )

    combined = {}

    # Merge each inner dictionary into the combined dictionary
    for indicators in (dac1_indicators, dac2a_indicators, crs_indicators):
        for k, v in indicators.items():
            combined |= v

    return combined


@dataclass
class Indicators:
    years: list | int | range = field(default_factory=list)
    providers: list | int | None = None
    recipients: list | int | None = None
    measure: list[Measure] | Measure = "gross_disbursement"
    currency: str = "USD"
    base_year: int | None = None
    using_bulk_download: bool = False
    refresh_data: bool = False

    def __post_init__(self) -> None:
        """Initialize and validate the ODAData2 instance."""
        self.indicators_data: dict[str, list[pd.DataFrame] | pd.DataFrame] = {}
        self._indicators: dict[str, dict] = load_indicators()
        self.indicators_filter = None

        self.measure = validate_measure(self.measure)

        validate_currency(self.currency)

    def _apply_filters(self, indicator: str) -> dict:
        """Apply all filters based on the class attributes and indicator settings."""
        filters: dict[str, list[tuple]] = {}
        sources = self._indicators[indicator]["sources"]

        for source in sources:
            filters[source] = []
            # Apply settings filters
            settings_filters = self._indicators[indicator]["filters"][source]

            # Apply indicator filter
            for column, values in settings_filters.items():
                if column in ["aidtype_code"]:
                    self.indicators_filter = values[-1]
                else:
                    filters[source].extend((column, *filters))

            # Apply currency filter
            currency_column = PRICES.get(source, {}).get("column")
            if currency_column:
                if self.currency == "LCU":
                    filters[source].append((currency_column, "==", "N"))
                else:
                    filters[source].append((currency_column, "==", "A"))

            # Apply measure filter
            columns = set(
                [MEASURES[source][measure]["column"] for measure in self.measure]
            )
            for col in columns:
                to_filter = []
                for measure in self.measure:
                    to_filter.append(get_measure_filter(source, measure))
                if to_filter is not None:
                    filters[source].append((col, "in", to_filter))

            # Apply recipient filter
            if source in ["DAC1"] and self.recipients:
                logger.info(
                    f"{source} data is not presented by Recipient. This parameter "
                    f"will be ignored for the selected indicator."
                )

        return filters

    def _load_data(self, indicator: str) -> None:
        """Load and filter data for the specified indicator."""

        filters = self._apply_filters(indicator)
        sources = self._indicators[indicator]["sources"]

        data = []
        for source in sources:
            source = source.upper()
            if source not in READERS:
                raise ValueError(f"{source} is invalid")

            # Define reader kwargs
            reader_kwargs = {"years": self.years, "providers": self.providers}
            if source == "DAC2A":
                reader_kwargs["recipients"] = self.recipients
            if source in ["DAC1", "DAC2A", "MULTISYSTEM"]:
                reader_kwargs["indicators"] = self.indicators_filter

            # Initialize reader
            reader = READERS[source](**reader_kwargs)

            if self.refresh_data:
                reader.refresh_data = True

            file = reader.read(
                additional_filters=filters[source],
                using_bulk_download=self.using_bulk_download,
            )
            data.append(file.drop(columns=_EXCLUDE, errors="ignore"))

        self.indicators_data[indicator] = data if len(data) > 1 else data[0]

    def _process_data(self, indicator: str) -> None:
        """Apply custom processing functions to the loaded data."""
        custom_function = self._indicators[indicator].get("custom_function")
        if (len(custom_function) < 1) or custom_function is None:
            return

        main_source = self._indicators[indicator]["sources"][0]

        try:
            module = source_to_module[main_source]
        except KeyError:
            raise NotImplementedError(f"No module found for source {main_source}")

        try:
            function_callable = getattr(module, custom_function)
        except AttributeError:
            raise NotImplementedError(f"No custom function available for {indicator}")

        self.indicators_data[indicator] = function_callable(
            self.indicators_data[indicator]
        )

    def _group_data(self, indicator: str):
        """Group the data to create semi-aggregated indicators (for valid sources)"""
        main_source = self._indicators[indicator]["sources"][0]

        if main_source not in ["CRS"]:
            return

        self.indicators_data[indicator] = group_data_based_on_indicator(
            data=self.indicators_data[indicator],
            indicator_code=indicator,
            measures=self.measure,
        )

    def _convert_units(self, indicator: str) -> None:
        """Convert the data to the requested currency and prices."""

        self.indicators_data[indicator] = clean.convert_units(
            data=self.indicators_data[indicator],
            indicator=indicator,
            currency=self.currency,
            base_year=self.base_year,
        )

    @property
    def arguments(self):
        """Returns the arguments used by the object"""
        return {
            "years": self.years,
            "providers": self.providers,
            "recipients": self.recipients,
            "currency": self.currency,
            "base_year": self.base_year,
        }

    @classmethod
    def available_donors(cls) -> dict:
        """Returns a dictionary of available donor codes and their names"""
        logger.info("Note that not all donors may be available for all indicators")
        return _OdaDict(donor_groupings()["official_donors"])

    @classmethod
    def available_recipients(cls) -> dict:
        """Returns a dictionary of available recipient codes"""
        logger.info("Note that not all recipients may be available for all indicators")
        return _OdaDict(recipient_groupings()["all_developing_countries_regions"])

    @classmethod
    def available_currencies(cls) -> list:
        """Returns a dictionary of available currencies"""
        return _OdaList(CURRENCIES)

    @classmethod
    def available_indicators(cls) -> list:
        """Returns a list of indicators"""
        return _OdaList(load_indicators().keys())

    def get_indicators(self, indicators: str | list[str]) -> pd.DataFrame:
        """Fetch and process data for the specified indicators."""
        if isinstance(indicators, str):
            indicators = [indicators]

        for ind in indicators:
            logger.info(f"Fetching data for {ind}")
            self._load_data(ind)
            self._process_data(ind)
            self._group_data(ind)
            self._convert_units(ind)
            logger.info(f"{ind} successfully loaded.")

        return pd.concat(
            [d.assign(one_indicator=i) for i, d in self.indicators_data.items()],
            ignore_index=True,
        )

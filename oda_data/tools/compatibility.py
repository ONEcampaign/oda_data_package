import json
import warnings
from dataclasses import dataclass, field

import pandas as pd

from oda_data import config, OECDClient


@dataclass
class ODAData:
    years: list | int | range = field(default_factory=list)
    donors: list | int | None = None
    recipients: list | int | None = None
    currency: str = "USD"
    prices: str = "current"
    base_year: int | None = None
    use_bulk_download: bool = False

    def __post_init__(self):
        warnings.warn(
            "This feature is provided for partial compatibility. It will be "
            "removed when the package exits the beta stage",
            category=DeprecationWarning,
        )

        # read json file
        with open(config.ODAPaths.settings / "v1_indicators_mapping.json") as f:
            self._mapping = json.load(f)

        self.indicators = {}

    def load_indicator(self, indicators: str | list[str]):
        if isinstance(indicators, str):
            indicators = [indicators]

        for indicator in indicators:
            if indicator not in self._mapping:
                raise KeyError(
                    f"Indicator {indicator} is not supported by this backwards "
                    f"compatibility tool"
                )

            indicator_data = OECDClient(
                years=self.years,
                providers=self.donors,
                recipients=self.recipients,
                measure=self._mapping[indicator]["measure"],
                currency=self.currency,
                base_year=self.base_year,
                use_bulk_download=self.use_bulk_download
                | self._mapping[indicator]["bulk"],
            ).get_indicators(indicators=self._mapping[indicator]["indicator"])

            self.indicators[indicator] = indicator_data

        return self

    def get_data(self, indicators: str | list[str] = "all") -> pd.DataFrame:
        # if indicator is a string, transform to list unless it is "all"
        if indicators != "all" and isinstance(indicators, str):
            indicators = [indicators]

        # if indicators is a list, load all the dataframes into a list
        if isinstance(indicators, list):
            indicators = [
                self.indicators[_] for _ in indicators if _ in list(self.indicators)
            ]

        # if indicator is "all", use all indicators
        elif indicators == "all":
            indicators = self.indicators.values()

        return pd.concat(indicators, ignore_index=True)

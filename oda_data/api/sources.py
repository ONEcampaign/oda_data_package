from abc import ABC, abstractmethod
from typing import Optional

import pandas as pd
from oda_reader import (
    download_crs,
    bulk_download_crs,
    download_dac1,
    download_dac2a,
    bulk_download_multisystem,
    download_multisystem,
)

from oda_data import config
from oda_data.clean_data.common import (
    clean_raw_df,
    convert_dot_stat_to_data_explorer_codes,
)
from oda_data.clean_data.schema import OdaSchema
from oda_data.clean_data.validation import validate_years_providers_recipients, check_integers, \
    check_strings
from oda_data.logger import logger


class DacSource(ABC):
    """Abstract base class for accessing DAC datasets.

    This class handles validation of parameters and manages the retrieval
    of data in the form of a pandas DataFrame.
    """

    def add_filter(self, column: str, predicate: str, value: str | int | list) -> None:
        """Adds a filter to the dataset, ensuring no duplicate columns.

        If a filter for the same column already exists, it is replaced with a warning.

        Args:
            column (str): The name of the column to filter.
            predicate (str): The condition to apply (e.g., "in").
            value (str | int | list): The value(s) to filter by.
        """
        existing_filter = next((f for f in self.filters if f[0] == column), None)

        if existing_filter:
            logger.warning(
                f"A filter for {column} already exists. Replacing with new filter."
            )

        # Remove existing filter (if any) and add the new one
        self.filters = [(c, p, v) for c, p, v in self.filters if c != column]
        self.filters.append((column, predicate, value))

    def _get_filtered_download_filters(self):
        """Generates a dictionary of filters for downloading the dataset.

        Returns:
            dict: A dictionary mapping column names to filter values.
        """
        filters = {}
        if self.de_providers:
            filters["donor"] = self.de_providers
        if self.de_recipients:
            filters["recipient"] = self.de_recipients
        if self.de_indicators:
            filters["measure"] = self.de_indicators

        if self.de_sectors:
            filters["sector"] = self.de_sectors
        return filters

    @staticmethod
    def _check_completeness(
        data: pd.DataFrame, filters: list[tuple[str, str, list]]
    ) -> None:
        """Checks if all requested values are present in the filtered dataset.

        Args:
            data (pd.DataFrame): The dataset to check.
            filters (list[tuple[str, str, list]]): The list of filters applied.
        """

        if not filters:
            return

        for column, _, values in filters:
            missing_values = set(values) - set(data[column].unique())
            if missing_values:
                logger.warning(
                    f"Missing values in {column}: {', '.join(map(str, missing_values))}"
                )

    def _check_file_is_available(self, dataset: str, filtered: bool = False) -> None:
        """Checks if a dataset file is available locally, triggering a download if not.

        Args:
            dataset (str): The dataset name.
            filtered (bool, optional): Whether to check for a filtered version. Defaults to False.
        """

        prefix = "filtered" if filtered else "full"

        if not (config.OdaPATHS.raw_data / f"{prefix}{dataset}.parquet").exists():
            logger.info(f"{dataset} data not found. Downloading...")
            self.download(filtered=filtered)

    def _read(
        self,
        dataset: str,
        from_filtered_file: bool = True,
        additional_filters: Optional[list[tuple]] = None,
        columns: Optional[list] = None,
    ):
        """Reads a dataset from a local parquet file, applying filters if needed.

        Args:
            dataset (str): The name of the dataset.
            from_filtered_file (bool, optional): Whether to read from a filtered file. Defaults to True.
            additional_filters (Optional[list[tuple]], optional): Additional filters to apply. Defaults to None.
            columns (Optional[list], optional): Subset of columns to read. Defaults to None.

        Returns:
            pd.DataFrame: The loaded dataset.
        """

        self._check_file_is_available(dataset=dataset, filtered=from_filtered_file)

        prefix = "filtered" if from_filtered_file else "full"

        if (config.OdaPATHS.raw_data / f"{prefix}{dataset}.parquet").exists():
            logger.info(f"Reading {prefix} {dataset} file...")

            filters = (
                self.filters + additional_filters
                if additional_filters is not None
                else self.filters
            )

            if len(filters) == 0:
                filters = None

            df = pd.read_parquet(
                config.OdaPATHS.raw_data / f"{prefix}{dataset}.parquet",
                filters=filters,
                engine="pyarrow",
                columns=columns,
            )

            if from_filtered_file:
                self._check_completeness(data=df, filters=filters)

            return df

    @abstractmethod
    def download(self, **kwargs):
        """Abstract method to download the dataset."""
        pass

    @abstractmethod
    def read(self, **kwargs):
        """Abstract method to read the dataset."""
        pass


class Dac1Data(DacSource):
    """Class to handle DAC1 data"""

    def __init__(
        self,
        years: Optional[list[int] | range | int] = None,
        providers: Optional[list[int] | int] = None,
        indicators: Optional[list[int] | int] = None,
    ):
        """Initializes the DAC1 data handler.

        Args:
            years (Optional[list[int] | range | int]): The years to filter.
            providers (Optional[list[int] | int]): The provider codes.
            indicators (Optional[list[int] | int]): The indicator codes.
        """
        super().__init__()

        self.years, self.providers, _ = validate_years_providers_recipients(
            years=years, providers=providers, recipients=None
        )

        self.indicators = check_integers(indicators) if indicators else None

        self.de_providers = None

        self.filters = []

        if self.years is not None:
            self.start = min(self.years)
            self.end = max(self.years)
            self.add_filter(column=OdaSchema.YEAR, predicate="in", value=self.years)
        else:
            self.start, self.end = None, None

        if self.providers is not None:
            self.add_filter(
                column=OdaSchema.PROVIDER_CODE, predicate="in", value=self.providers
            )
            self.de_providers = convert_dot_stat_to_data_explorer_codes(
                codes=self.providers
            )
        if self.indicators is not None:
            self.add_filter(
                column=OdaSchema.AIDTYPE_CODE, predicate="in", value=self.indicators
            )
            self.de_indicators = check_strings(indicators)

    def download(self, filtered: bool = True):
        """Downloads DAC1 data, either filtered or full dataset.

        Args:
            filtered (bool, optional): Whether to download a filtered dataset. Defaults to True.
        """
        if filtered:
            df = download_dac1(
                start_year=self.start,
                end_year=self.end,
                filters=self._get_filtered_download_filters(),
            )
            prefix = "filtered"
        else:
            logger.warning("Bulk downloading DAC1 is not yet supported")
            raise NotImplementedError

        # Save
        df.to_parquet(config.OdaPATHS.raw_data / f"{prefix}DAC1.parquet")

        logger.info(f"DAC1 data downloaded successfully.")

    def read(
        self,
        from_filtered_file: bool = True,
        additional_filters: Optional[list[tuple]] = None,
        columns: Optional[list] = None,
    ):
        """Reads DAC1 data from a local parquet file.

        Args:
            from_filtered_file (bool, optional): Whether to read from a filtered file. Defaults to True.
            additional_filters (Optional[list[tuple]], optional): Additional filters. Defaults to None.
            columns (Optional[list], optional): Subset of columns to read. Defaults to None, which reads all.

        Returns:
            pd.DataFrame: The loaded dataset.
        """
        return self._read(
            dataset="DAC1",
            from_filtered_file=from_filtered_file,
            additional_filters=additional_filters,
            columns=columns,
        )


class Dac2Data(DacSource):
    def __init__(
        self,
        years: Optional[list[int] | range | int] = None,
        providers: Optional[list[int] | int] = None,
        recipients: Optional[list[int] | int] = None,
        indicators: Optional[list[int] | int] = None,
    ):
        """Initializes the DAC2A data handler.

        Args:
            years (Optional[list[int] | range | int]): The years to filter.
            providers (Optional[list[int] | int]): The provider codes.
            recipients (Optional[list[int] | int]): The recipient codes.
            indicators (Optional[list[int] | int]): The indicator codes.
        """
        super().__init__()

        self.years, self.providers, self.recipients = (
            validate_years_providers_recipients(
                years=years, providers=providers, recipients=recipients
            )
        )

        self.de_providers, self.de_recipients = None, None
        self.indicators = check_integers(indicators) if indicators else None

        self.filters = []

        if self.years is not None:
            self.start = min(self.years)
            self.end = max(self.years)
            self.add_filter(column=OdaSchema.YEAR, predicate="in", value=self.years)
        else:
            self.start, self.end = None, None

        if self.providers is not None:
            self.add_filter(
                column=OdaSchema.PROVIDER_CODE, predicate="in", value=self.providers
            )
            self.de_providers = convert_dot_stat_to_data_explorer_codes(
                codes=self.providers
            )

        if self.recipients is not None:
            self.add_filter(
                column=OdaSchema.RECIPIENT_CODE, predicate="in", value=self.recipients
            )
            self.de_recipients = convert_dot_stat_to_data_explorer_codes(
                codes=self.recipients
            )

        if self.indicators is not None:
            self.add_filter(
                column=OdaSchema.AIDTYPE_CODE, predicate="in", value=self.indicators
            )
            self.de_indicators = check_strings(indicators)

    def download(self, filtered: bool = True):
        """Downloads DAC2A data, either filtered or full dataset.

        Args:
            filtered (bool, optional): Whether to download a filtered dataset. Defaults to True.
        """
        if filtered:
            df = download_dac2a(
                start_year=self.start,
                end_year=self.end,
                filters=self._get_filtered_download_filters(),
            )
            prefix = "filtered"
        else:
            logger.warning("Bulk downloading DAC2a is not yet supported")
            raise NotImplementedError

        # Save
        df.to_parquet(config.OdaPATHS.raw_data / f"{prefix}DAC2a.parquet")

        logger.info(f"DAC2a data downloaded successfully.")

    def read(
        self,
        from_filtered_file: bool = True,
        additional_filters: Optional[list[tuple]] = None,
        columns: Optional[list] = None,
    ):
        """Reads DAC2A data from a local parquet file.

        Args:
            from_filtered_file (bool, optional): Whether to read from a filtered file. Defaults to True.
            additional_filters (Optional[list[tuple]], optional): Additional filters. Defaults to None.
            columns (Optional[list], optional): Subset of columns to read. Defaults to None.

        Returns:
            pd.DataFrame: The loaded dataset.
        """
        return self._read(
            dataset="DAC2a",
            from_filtered_file=from_filtered_file,
            additional_filters=additional_filters,
            columns=columns,
        )


class CrsData(DacSource):
    """Class to handle the CRS data."""

    def __init__(
        self,
        years: Optional[list[int] | range | int] = None,
        providers: Optional[list[int] | int] = None,
        recipients: Optional[list[int] | int] = None,
    ):
        """
        Initialize CrsData.

        Args:
            years (list[int] | range): List or range of years to filter.
            providers (Optional[list[int]]): List of provider codes.
            recipients (Optional[list[int]]): List of recipient codes.
        """
        super().__init__()

        self.years, self.providers, self.recipients = (
            validate_years_providers_recipients(
                years=years, providers=providers, recipients=recipients
            )
        )

        self.de_providers, self.de_recipients = None, None

        self.filters = []

        if self.years is not None:
            self.start = min(self.years)
            self.end = max(self.years)
            self.add_filter(column=OdaSchema.YEAR, predicate="in", value=self.years)
        else:
            self.start, self.end = None, None

        if self.providers is not None:
            self.add_filter(
                column=OdaSchema.PROVIDER_CODE, predicate="in", value=self.providers
            )
            self.de_providers = convert_dot_stat_to_data_explorer_codes(
                codes=self.providers
            )

        if self.recipients is not None:
            self.add_filter(
                column=OdaSchema.RECIPIENT_CODE, predicate="in", value=self.recipients
            )
            self.de_recipients = convert_dot_stat_to_data_explorer_codes(
                codes=self.recipients
            )

    def download(self, filtered: bool = False):
        """Downloads CRS data, either filtered or full dataset.

        Args:
            filtered (bool, optional): Whether to download a filtered dataset. Defaults to False.
        """

        if filtered:
            df = download_crs(
                start_year=self.start,
                end_year=self.end,
                filters=self._get_filtered_download_filters(),
            )
            prefix = "filtered"
        else:
            logger.info(
                "The full, detailed CRS is only available as a large file (>1GB). "
                "The package will now download the data, but it may take a while."
            )
            df = bulk_download_crs()
            prefix = "full"

        # Clean the DataFrame
        df = clean_raw_df(df)

        # Save
        df.to_parquet(config.OdaPATHS.raw_data / f"{prefix}CRS.parquet")

        logger.info(f"CRS data downloaded successfully.")

        return self

    def read(
        self,
        from_filtered_file: bool = False,
        additional_filters: Optional[list[tuple]] = None,
        columns: Optional[list] = None,
    ):
        """Reads CRS data from a local parquet file.

        Args:
            from_filtered_file (bool, optional): Whether to read from a filtered file. Defaults to False.
            additional_filters (Optional[list[tuple]], optional): Additional filters. Defaults to None.
            columns (Optional[list], optional): Subset of columns to read. Defaults to None.

        Returns:
            pd.DataFrame: The loaded dataset.
        """
        return self._read(
            dataset="CRS",
            from_filtered_file=from_filtered_file,
            additional_filters=additional_filters,
            columns=columns,
        )


class MultiSystemData(DacSource):
    """Class to handle the Providers total use of the multilateral system data."""

    def __init__(
        self,
        years: Optional[list[int] | range | int] = None,
        providers: Optional[list[int] | int] = None,
        recipients: Optional[list[int] | int] = None,
        indicators: Optional[list[int] | int] = None,
        sectors: Optional[list[int] | int] = None,
    ):
        """
        Initialize Multisystem data.

        Args:
            years (list[int] | range): List or range of years to filter.
            providers (Optional[list[int]]): List of provider codes.
            recipients (Optional[list[int]]): List of recipient codes.
            indicators (Optional[list[int]]): List of indicator codes.
            sectors (Optional[list[int]]): List of sector codes.
        """
        super().__init__()

        self.years, self.providers, self.recipients = (
            validate_years_providers_recipients(
                years=years, providers=providers, recipients=recipients
            )
        )

        self.de_providers, self.de_recipients = None, None
        self.indicators = check_integers(indicators) if indicators else None
        self.sectors = check_integers(sectors) if sectors else None

        self.filters = []

        if self.years is not None:
            self.start = min(self.years)
            self.end = max(self.years)
            self.add_filter(column=OdaSchema.YEAR, predicate="in", value=self.years)
        else:
            self.start, self.end = None, None

        if self.providers is not None:
            self.add_filter(
                column=OdaSchema.PROVIDER_CODE, predicate="in", value=self.providers
            )
            self.de_providers = convert_dot_stat_to_data_explorer_codes(
                codes=self.providers
            )

        if self.recipients is not None:
            self.add_filter(
                column=OdaSchema.RECIPIENT_CODE, predicate="in", value=self.recipients
            )
            self.de_recipients = convert_dot_stat_to_data_explorer_codes(
                codes=self.recipients
            )

        if self.indicators is not None:
            self.add_filter(
                column=OdaSchema.AID_TO_THRU, predicate="in", value=self.indicators
            )
            self.de_indicators = check_strings(indicators)

        if self.sectors is not None:
            self.add_filter(
                column=OdaSchema.SECTOR_CODE, predicate="in", value=self.sectors
            )
            self.de_sectors = check_strings(sectors)

    def download(self, filtered: bool = False):
        """Downloads Multisystem data, either filtered or full dataset.

        Args:
            filtered (bool, optional): Whether to download a filtered dataset. Defaults to False.
        """

        if filtered:
            df = download_multisystem(
                start_year=self.start,
                end_year=self.end,
                filters=self._get_filtered_download_filters(),
            )
            prefix = "filtered"
        else:
            logger.info(
                "The full, detailed Multisystem is a large file (>1GB). "
                "The package will now download the data, but it may take a while."
            )
            df = bulk_download_multisystem()
            prefix = "full"

        # Clean the DataFrame
        df = clean_raw_df(df)

        # Save
        df.to_parquet(config.OdaPATHS.raw_data / f"{prefix}Multisystem.parquet")

        logger.info(f"Multisystem data downloaded successfully.")

        return self

    def read(
        self,
        from_filtered_file: bool = False,
        additional_filters: Optional[list[tuple]] = None,
        columns: Optional[list] = None,
    ):
        """Reads Multisystem data from a local parquet file.

        Args:
            from_filtered_file (bool, optional): Whether to read from a filtered file. Defaults to False.
            additional_filters (Optional[list[tuple]], optional): Additional filters. Defaults to None.
            columns (Optional[list], optional): Subset of columns to read. Defaults to None.

        Returns:
            pd.DataFrame: The loaded dataset.
        """
        return self._read(
            dataset="Multisystem",
            from_filtered_file=from_filtered_file,
            additional_filters=additional_filters,
            columns=columns,
        )

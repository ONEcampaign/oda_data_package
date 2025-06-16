from abc import abstractmethod
from pathlib import Path
from typing import Optional

import pandas as pd
from cachetools import TTLCache
from oda_reader import (
    download_crs,
    bulk_download_crs,
    download_dac1,
    download_dac2a,
    bulk_download_multisystem,
    download_multisystem,
    download_aiddata,
)
from oda_reader._cache import memory

from oda_data import config
from oda_data.clean_data.common import (
    clean_raw_df,
    convert_dot_stat_to_data_explorer_codes,
)
from oda_data.clean_data.schema import ODASchema, AidDataSchema
from oda_data.clean_data.validation import (
    validate_years_providers_recipients,
    check_integers,
    check_strings,
)
from oda_data.config import ODAPaths
from oda_data.logger import logger
from oda_data.tools.cache import OnDiskCache, generate_param_hash


def _filters_to_query(filters: list[tuple[str, str, list]]) -> str:
    """Transform a list of filters into a query string."""
    query = ""
    for column, predicate, values in filters:
        if query:
            query += " and "
        if predicate == "in":
            query += f"{column} in {values}"
        if predicate == "==":
            query += f"{column} == '{values}'"
    return query


class Source:
    """Base class for accessing any dataset.

    This class handles validation of parameters and manages the retrieval
    of data in the form of a pandas DataFrame.
    """

    memory_cache = TTLCache(maxsize=20, ttl=6000)
    disk_cache = OnDiskCache(ODAPaths.raw_data, ttl_seconds=86400)

    def __init__(self):
        self.de_providers = None
        self.de_recipients = None
        self.de_indicators = None
        self.filters = None
        self.de_sectors = None
        self.schema = None
        self._param_hash = None

    def _add_filter(self, column: str, predicate: str, value: str | int | list) -> None:
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

    def _get_read_filters(
        self, additional_filters: Optional[list[tuple]] = None
    ) -> list[tuple[str, str, list]] | None:
        filters = (
            self.filters + additional_filters if additional_filters else self.filters
        )
        return filters or None

    def _bulk_hash(self) -> str:
        return generate_param_hash(filters=[("bulk", "==", self.__class__.__name__)])

    def _bulk_filename(self) -> Path:
        return self.disk_cache.get_file_path(
            dataset_name=self.__class__.__name__, param_hash=self._param_hash
        )

    def _get_cached_dataset(
        self, param_hash: str, filters: list[tuple], columns: list[str]
    ) -> pd.DataFrame | None:

        cached_df = self.memory_cache.get(param_hash)

        if cached_df is None:
            cached_df = self.disk_cache.load(
                self.__class__.__name__, param_hash, filters, columns
            )

        return cached_df


class DACSource(Source):
    """Base class for accessing DAC datasets.

    This class handles validation of parameters and manages the retrieval
    of data in the form of a pandas DataFrame.
    """

    memory_cache = TTLCache(maxsize=20, ttl=6000)
    disk_cache = OnDiskCache(ODAPaths.raw_data, ttl_seconds=86400)

    def __init__(self):
        super().__init__()

        self.schema = ODASchema

    def _init_filters(
        self,
        years: Optional[list[int] | range | int] = None,
        providers: Optional[list[int] | int] = None,
        recipients: Optional[list[int] | int] = None,
        sectors: Optional[list[int] | int] = None,
    ):

        self.years, self.providers, self.recipients = (
            validate_years_providers_recipients(
                years=years, providers=providers, recipients=recipients
            )
        )

        self.de_providers, self.de_recipients = None, None
        self.sectors = check_integers(sectors) if sectors else None

        self.filters = []

        if self.years is not None:
            self.start = min(self.years)
            self.end = max(self.years)
            self._add_filter(column=self.schema.YEAR, predicate="in", value=self.years)
        else:
            self.start, self.end = None, None

        if self.providers is not None:
            self._add_filter(
                column=self.schema.PROVIDER_CODE, predicate="in", value=self.providers
            )
            self.de_providers = convert_dot_stat_to_data_explorer_codes(
                codes=self.providers
            )

        if self.recipients is not None:
            self._add_filter(
                column=self.schema.RECIPIENT_CODE, predicate="in", value=self.recipients
            )
            self.de_recipients = convert_dot_stat_to_data_explorer_codes(
                codes=self.recipients
            )

        if self.sectors is not None:
            self._add_filter(
                column=self.schema.SECTOR_CODE, predicate="in", value=self.sectors
            )
            self.de_sectors = check_strings(sectors)

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
            if column not in data.columns:
                continue
            if isinstance(values, str):
                values_check = [values]
            elif isinstance(values, int):
                values_check = [values]
            else:
                values_check = values
            missing_values = set(values_check) - set(data[column].unique())
            if missing_values:
                logger.warning(
                    f"Missing values in {column}: {', '.join(map(str, missing_values))}"
                )

    def _cache_dataset(self, param_hash: str, df: pd.DataFrame) -> None:
        """Caches the dataset in memory and on disk."""
        size = df.memory_usage(deep=True).sum() / (1024 * 1024)  # in MB
        if size < 50:
            self.memory_cache[param_hash] = df.copy(deep=True)
        self.disk_cache.save(self.__class__.__name__, param_hash, df)

    def _apply_query_and_cache(
        self, df: pd.DataFrame, query: str | None, param_hash: str, bulk: bool
    ):
        if bulk:
            self._cache_dataset(param_hash=param_hash, df=df)

        if query:
            df = df.query(query)

        if not bulk:
            self._cache_dataset(param_hash=param_hash, df=df)

        return df

    def read(
        self,
        using_bulk_download: bool = False,
        additional_filters: Optional[list[tuple]] = None,
        columns: Optional[list] = None,
    ):
        """Reads a dataset from a local parquet file, applying filters if needed.

        Args:
            using_bulk_download (bool, optional): Whether to read from the bulk file. Defaults to False.
            additional_filters (Optional[list[tuple]], optional): Additional filters to apply. Defaults to None.
            columns (Optional[list], optional): Subset of columns to read. Defaults to None.

        Returns:
            pd.DataFrame: The loaded dataset.
        """
        # Create filters
        filters = self._get_read_filters(additional_filters=additional_filters)

        # Generate a query string from the filters
        query = _filters_to_query(filters) if filters else None

        # Generate a hash string with the filters and the type of file
        param_hash = (
            self._bulk_hash()
            if using_bulk_download
            else generate_param_hash(filters=filters)
        )

        self._param_hash = param_hash

        # If caching is not active, clear the cache
        if not memory().store_backend:
            self.disk_cache.cleanup(hash_str=param_hash, force=True)
            self.memory_cache.clear()

        # Check if the data is already cached in memory or on disk
        df = self._get_cached_dataset(
            param_hash=param_hash, filters=filters, columns=columns
        )

        # If not cached, download the data
        if df is None:
            df = self.download(bulk=using_bulk_download)
            if df is None:
                df = self._get_cached_dataset(
                    param_hash=param_hash, filters=filters, columns=columns
                )
            else:
                df = self._apply_query_and_cache(
                    df=df, query=query, param_hash=param_hash, bulk=using_bulk_download
                )

        df = df.pipe(clean_raw_df)

        if columns:
            df = df.filter(columns)

        return df

    @abstractmethod
    def download(self, **kwargs) -> pd.DataFrame:
        """Abstract method to download the dataset."""
        pass


class DAC1Data(DACSource):
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

        self._init_filters(years=years, providers=providers)
        self.indicators = check_integers(indicators) if indicators else None

        if self.indicators is not None:
            self._add_filter(
                column=self.schema.AIDTYPE_CODE, predicate="in", value=self.indicators
            )
            self.de_indicators = check_strings(indicators)

    def download(self, bulk: bool = False):
        """Downloads DAC1 data, either filtered or full dataset.

        Args:
            bulk (bool, optional): Whether to download bulk dataset. Defaults to False.
        """
        if not bulk:
            df = download_dac1(
                start_year=self.start,
                end_year=self.end,
                filters=self._get_filtered_download_filters(),
            )
        else:
            logger.info("Bulk downloading DAC1 may take a long time")
            df = download_dac1()

        logger.info(f"DAC1 data downloaded successfully.")

        return df


class DAC2AData(DACSource):
    """Class to handle the DAC2A data."""

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

        self._init_filters(years=years, providers=providers, recipients=recipients)
        self.indicators = check_integers(indicators) if indicators else None

        if self.indicators is not None:
            self._add_filter(
                column=self.schema.AIDTYPE_CODE, predicate="in", value=self.indicators
            )
            self.de_indicators = check_strings(indicators)

    def download(self, bulk: bool = False):
        """Downloads DAC2A data, either filtered or full dataset.

        Args:
            bulk (bool, optional): Whether to download the bulk dataset. Defaults to False.
        """
        if not bulk:
            df = download_dac2a(
                start_year=self.start,
                end_year=self.end,
                filters=self._get_filtered_download_filters(),
            )

        else:
            logger.info("Bulk downloading DAC2a may take a long time")
            df = download_dac2a()

        logger.info(f"DAC2a data downloaded successfully.")

        return df


class CRSData(DACSource):
    """Class to handle the CRS data."""

    def __init__(
        self,
        years: Optional[list[int] | range | int] = None,
        providers: Optional[list[int] | int] = None,
        recipients: Optional[list[int] | int] = None,
    ):
        """
        Initialize CRSData.

        Args:
            years (list[int] | range): List or range of years to filter.
            providers (Optional[list[int]]): List of provider codes.
            recipients (Optional[list[int]]): List of recipient codes.
        """
        super().__init__()
        self._init_filters(years=years, providers=providers, recipients=recipients)
        self._param_hash = None

    def download(self, bulk: bool = True):
        """Downloads CRS data, either filtered or full dataset.

        Args:
            bulk (bool, optional): Whether to download bulk dataset. Defaults to False.
        """

        if not bulk:
            df = download_crs(
                start_year=self.start,
                end_year=self.end,
                filters=self._get_filtered_download_filters(),
            )
            logger.info(f"CRS data downloaded successfully.")
            return clean_raw_df(df)

        else:
            logger.info(
                "The full, detailed CRS is only available as a large file (>1GB). "
                "The package will now download the data, but it may take a while."
            )
            bulk_download_crs(save_to_path=self._bulk_filename())
            return None


class MultiSystemData(DACSource):
    """Class to handle the Providers total use of the multilateral system data."""

    def __init__(
        self,
        years: Optional[list[int] | range | int] = None,
        providers: Optional[list[int] | int] = None,
        recipients: Optional[list[int] | int] = None,
        indicators: Optional[list[str] | str] = None,
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
        self._init_filters(
            years=years, providers=providers, recipients=recipients, sectors=sectors
        )

        self.indicators = check_strings(indicators) if indicators else None

        if self.indicators is not None:
            self._add_filter(
                column=self.schema.AID_TO_THRU, predicate="in", value=self.indicators
            )
            self.de_indicators = indicators

    def download(self, bulk: bool = True):
        """Downloads Multisystem data, either filtered or full dataset.

        Args:
            bulk (bool, optional): Whether to download bulk dataset. Defaults to true.
        """

        if not bulk:
            df = download_multisystem(
                start_year=self.start,
                end_year=self.end,
                filters=self._get_filtered_download_filters(),
            )
            logger.info(f"Multisystem data downloaded successfully.")
            return clean_raw_df(df)

        else:
            logger.info(
                "The full, detailed Multisystem is a large file (>1GB). "
                "The package will now download the data, but it may take a while."
            )
            bulk_download_multisystem(save_to_path=self._bulk_filename())
            return None


class AidDataSource(Source):
    """Base class for accessing AidData datasets.

    This class handles validation of parameters and manages the retrieval
    of data in the form of a pandas DataFrame.
    """

    memory_cache = TTLCache(maxsize=20, ttl=6000)
    disk_cache = OnDiskCache(ODAPaths.raw_data, ttl_seconds=86400)

    def __init__(self):
        super().__init__()

        self.schema = AidDataSchema

    def _init_filters(
        self,
        years: Optional[list[int] | range | int] = None,
        recipients: Optional[list[str] | str] = None,
        sectors: Optional[list[int] | int] = None,
    ):
        self.years = check_integers(years)
        self.recipients = check_strings(recipients)
        self.sectors = check_integers(sectors)

        self.filters = []

        if self.years is not None:
            self.start = min(self.years)
            self.end = max(self.years)
            self._add_filter(
                column=self.schema.COMMITMENT_YEAR, predicate="in", value=self.years
            )
        else:
            self.start, self.end = None, None

        if self.recipients is not None:
            self._add_filter(
                column=self.schema.RECIPIENT_NAME, predicate="in", value=self.recipients
            )

        if self.sectors is not None:
            self._add_filter(
                column=self.schema.SECTOR_CODE, predicate="in", value=self.sectors
            )


class AidDataData(AidDataSource):
    """Class to handle the AidData data."""

    memory_cache = TTLCache(maxsize=20, ttl=6000)
    disk_cache = OnDiskCache(config.ODAPaths.raw_data, ttl_seconds=86400)

    def __init__(
        self,
        years: Optional[list[int] | range | int] = None,
        recipients: Optional[list[str] | str] = None,
        sectors: Optional[list[int] | int] = None,
    ):
        """Initializes the AidData data handler.

        Args:
            years (Optional[list[int] | range | int]): Years to filter based on commitment year.
            recipients (Optional[list[str] | str]): Recipient names to filter.
            sectors (Optional[list[int] | int]): Sector codes to filter.
        """
        super().__init__()

        self._init_filters(years=years, recipients=recipients, sectors=sectors)

    def download(self) -> None:
        """Downloads the full AidData dataset."""
        download_aiddata(save_to_path=self._bulk_filename())

    def read(
        self,
        additional_filters: Optional[list[tuple]] = None,
        columns: Optional[list] = None,
    ):
        """Reads a dataset from a local parquet file, applying filters if needed.

        Args:
            additional_filters (Optional[list[tuple]], optional): Additional filters to apply. Defaults to None.
            columns (Optional[list], optional): Subset of columns to read. Defaults to None.

        Returns:
            pd.DataFrame: The loaded dataset.
        """
        # Create filters
        filters = self._get_read_filters(additional_filters=additional_filters)

        # Generate a hash string with the filters and the type of file
        param_hash = self._bulk_hash()

        self._param_hash = param_hash

        # If caching is not active, clear the cache
        if not memory().store_backend:
            self.disk_cache.cleanup(hash_str=param_hash, force=True)
            self.memory_cache.clear()

        # Check if the data is already cached in memory or on disk
        df = self._get_cached_dataset(
            param_hash=param_hash, filters=filters, columns=columns
        )

        # If not cached, download the data
        if df is None:
            self.download()
            df = self._get_cached_dataset(
                param_hash=param_hash, filters=filters, columns=columns
            )

        return df

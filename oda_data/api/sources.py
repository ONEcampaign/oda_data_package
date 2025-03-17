from abc import abstractmethod
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
)

from oda_data import config
from oda_data.clean_data.common import (
    clean_raw_df,
    convert_dot_stat_to_data_explorer_codes,
)
from oda_data.clean_data.schema import OdaSchema
from oda_data.clean_data.validation import (
    validate_years_providers_recipients,
    check_integers,
    check_strings,
)
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


class DacSource:
    """Base class for accessing DAC datasets.

    This class handles validation of parameters and manages the retrieval
    of data in the form of a pandas DataFrame.
    """

    memory_cache = TTLCache(maxsize=20, ttl=6000)
    disk_cache = OnDiskCache(config.OdaPATHS.raw_data, ttl_seconds=86400)

    def __init__(self):
        self.de_providers = None
        self.de_recipients = None
        self.de_indicators = None
        self.filters = None
        self.de_sectors = None
        self.refresh_data = False

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

        if self.sectors is not None:
            self.add_filter(
                column=OdaSchema.SECTOR_CODE, predicate="in", value=self.sectors
            )
            self.de_sectors = check_strings(sectors)

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
            if column not in data.columns:
                continue
            missing_values = set(values) - set(data[column].unique())
            if missing_values:
                logger.warning(
                    f"Missing values in {column}: {', '.join(map(str, missing_values))}"
                )

    def _check_file_is_available(
        self, dataset: str, using_bulk_download: bool = False
    ) -> None:
        """Checks if a dataset file is available locally, triggering a download if not.

        Args:
            dataset (str): The dataset name.
            using_bulk_download (bool, optional): Whether to check for a filtered version. Defaults to False.
        """

        prefix = "full" if using_bulk_download else "filtered"

        if not (config.OdaPATHS.raw_data / f"{prefix}{dataset}.parquet").exists():
            logger.info(f"{dataset} data not found. Downloading...")
            df = self.download(bulk=using_bulk_download).pipe(clean_raw_df)
            df.to_parquet(config.OdaPATHS.raw_data / f"{prefix}{dataset}.parquet")

    def _read(
        self,
        dataset: str,
        using_bulk_download: bool = False,
        additional_filters: Optional[list[tuple]] = None,
        columns: Optional[list] = None,
    ):
        """Reads a dataset from a local parquet file, applying filters if needed.

        Args:
            dataset (str): The name of the dataset.
            using_bulk_download (bool, optional): Whether to read from the bulk file. Defaults to False.
            additional_filters (Optional[list[tuple]], optional): Additional filters to apply. Defaults to None.
            columns (Optional[list], optional): Subset of columns to read. Defaults to None.

        Returns:
            pd.DataFrame: The loaded dataset.
        """
        # Create filters
        filters = (
            self.filters + additional_filters
            if additional_filters is not None
            else self.filters
        )
        if len(filters) == 0:
            filters = None

        if using_bulk_download:
            self._check_file_is_available(
                dataset=dataset, using_bulk_download=using_bulk_download
            )
            df = pd.read_parquet(
                config.OdaPATHS.raw_data / f"full{dataset}.parquet",
                filters=filters,
                engine="pyarrow",
                columns=columns,
            )
            self._check_completeness(data=df, filters=filters)
            return df

        # Create hash for caching
        param_hash = generate_param_hash(filters=filters)

        if self.refresh_data:
            self.disk_cache.cleanup(hash_str=param_hash, force=True)
            self.memory_cache.clear()

        # Check in memory cache
        df = self.memory_cache.get(param_hash)
        if df is not None:
            query = _filters_to_query(filters) if filters else None
            if query:
                df = df.query(query)
            if columns:
                df = df.filter(columns)
            self._check_completeness(data=df, filters=filters)
            logger.info(f"Returning {dataset} data from in-memory cache.")
            return df

        # Check on-disk cache
        df = self.disk_cache.load(dataset, param_hash, filters, columns)
        if df is not None:
            self._check_completeness(data=df, filters=filters)
            logger.info(f"Returning {dataset} data from disk cache.")
            self.memory_cache[param_hash] = df
            return df

        # 3. Not cached, perform download
        logger.info(f"{dataset} not found in cache. Downloading...")
        df = self.download().pipe(clean_raw_df)

        # 4. Store in caches
        self.disk_cache.save(dataset, param_hash, df)
        self.memory_cache[param_hash] = df.copy(deep=True)
        return df

    @abstractmethod
    def download(self, **kwargs) -> pd.DataFrame:
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

        self._init_filters(years=years, providers=providers)
        self.indicators = check_integers(indicators) if indicators else None

        if self.indicators is not None:
            self.add_filter(
                column=OdaSchema.AIDTYPE_CODE, predicate="in", value=self.indicators
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

    def read(
        self,
        using_bulk_download: bool = False,
        additional_filters: Optional[list[tuple]] = None,
        columns: Optional[list] = None,
    ):
        """Reads DAC1 data from a local parquet file.

        Args:
            using_bulk_download (bool, optional): Whether to read from bulk file. Defaults to False.
            additional_filters (Optional[list[tuple]], optional): Additional filters. Defaults to None.
            columns (Optional[list], optional): Subset of columns to read. Defaults to None, which reads all.

        Returns:
            pd.DataFrame: The loaded dataset.
        """
        return self._read(
            dataset="DAC1",
            using_bulk_download=using_bulk_download,
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

        self._init_filters(years=years, providers=providers, recipients=recipients)
        self.indicators = check_integers(indicators) if indicators else None

        self.filters = []

        if self.indicators is not None:
            self.add_filter(
                column=OdaSchema.AIDTYPE_CODE, predicate="in", value=self.indicators
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

    def read(
        self,
        using_bulk_download: bool = False,
        additional_filters: Optional[list[tuple]] = None,
        columns: Optional[list] = None,
    ):
        """Reads DAC2A data from a local parquet file.

        Args:
            using_bulk_download (bool, optional): Whether to read from a bulk download file. Defaults to False.
            additional_filters (Optional[list[tuple]], optional): Additional filters. Defaults to None.
            columns (Optional[list], optional): Subset of columns to read. Defaults to None.

        Returns:
            pd.DataFrame: The loaded dataset.
        """
        return self._read(
            dataset="DAC2a",
            using_bulk_download=using_bulk_download,
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
        self._init_filters(years=years, providers=providers, recipients=recipients)

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
        else:
            logger.info(
                "The full, detailed CRS is only available as a large file (>1GB). "
                "The package will now download the data, but it may take a while."
            )
            df = bulk_download_crs()

        # Clean the DataFrame
        df = clean_raw_df(df)

        logger.info(f"CRS data downloaded successfully.")

        return df

    def read(
        self,
        using_bulk_download: bool = True,
        additional_filters: Optional[list[tuple]] = None,
        columns: Optional[list] = None,
    ):
        """Reads CRS data from a local parquet file.

        Args:
            using_bulk_download (bool, optional): Whether to read from bulk file. Defaults to True.
            additional_filters (Optional[list[tuple]], optional): Additional filters. Defaults to None.
            columns (Optional[list], optional): Subset of columns to read. Defaults to None.

        Returns:
            pd.DataFrame: The loaded dataset.
        """
        return self._read(
            dataset="CRS",
            using_bulk_download=using_bulk_download,
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
            self.add_filter(
                column=OdaSchema.AID_TO_THRU, predicate="in", value=self.indicators
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
        else:
            logger.info(
                "The full, detailed Multisystem is a large file (>1GB). "
                "The package will now download the data, but it may take a while."
            )
            df = bulk_download_multisystem()

        # Clean the DataFrame
        df = clean_raw_df(df)

        logger.info(f"Multisystem data downloaded successfully.")

        return df

    def read(
        self,
        using_bulk_download: bool = True,
        additional_filters: Optional[list[tuple]] = None,
        columns: Optional[list] = None,
    ):
        """Reads Multisystem data from a local parquet file.

        Args:
            using_bulk_download (bool, optional): Whether to read from a bulk download file. Defaults to True.
            additional_filters (Optional[list[tuple]], optional): Additional filters. Defaults to None.
            columns (Optional[list], optional): Subset of columns to read. Defaults to None.

        Returns:
            pd.DataFrame: The loaded dataset.
        """
        return self._read(
            dataset="Multisystem",
            using_bulk_download=using_bulk_download,
            additional_filters=additional_filters,
            columns=columns,
        )

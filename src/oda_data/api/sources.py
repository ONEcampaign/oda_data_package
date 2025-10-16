import threading
from abc import abstractmethod
from collections.abc import Callable
from pathlib import Path

import pandas as pd
from oda_reader import (
    download_crs,
    download_dac1,
    download_dac2a,
    download_multisystem,
)

from oda_data.clean_data.common import (
    clean_raw_df,
    convert_dot_stat_to_data_explorer_codes,
)
from oda_data.clean_data.schema import (
    CORE_CRS_MAPPING,
    AidDataSchema,
    ODASchema,
)
from oda_data.clean_data.validation import (
    check_integers,
    check_strings,
    validate_years_providers_recipients,
)
from oda_data.config import ODAPaths
from oda_data.logger import logger
from oda_data.tools.cache import (
    BulkCacheEntry,
    BulkCacheManager,
    QueryCacheManager,
    ThreadSafeMemoryCache,
    create_aiddata_bulk_fetcher,
    create_crs_bulk_fetcher,
    create_multisystem_bulk_fetcher,
    generate_param_hash,
)


def translate_cols_and_filters_to_raw(columns, filters) -> tuple[list, list]:
    crs_mapping = {v: k for k, v in CORE_CRS_MAPPING.items()}
    cols = [crs_mapping.get(col, col) for col in columns] if columns else columns
    filters = (
        [(crs_mapping.get(col, col), op, val) for col, op, val in filters]
        if filters
        else None
    )

    return cols, filters


def _filters_to_query(filters: list[tuple[str, str, list]]) -> str:
    """Transform a list of filters into a query string.

    Args:
        filters: List of (column, predicate, values) tuples

    Returns:
        Query string for pandas DataFrame.query()
    """
    query = ""
    for column, predicate, values in filters:
        if query:
            query += " and "
        if predicate == "in":
            # Handle list of mixed types - quote strings, keep numbers/booleans as-is
            if isinstance(values, list):
                quoted = []
                for v in values:
                    if isinstance(v, str):
                        quoted.append(f"'{v}'")
                    else:
                        quoted.append(str(v))
                query += f"{column} in [{', '.join(quoted)}]"
            # Single value wrapped in list
            elif isinstance(values, str):
                query += f"{column} in ['{values}']"
            else:
                query += f"{column} in [{values}]"
        elif predicate == "==":
            # Quote strings, but not numbers or booleans
            if isinstance(values, str):
                query += f"{column} == '{values}'"
            else:
                query += f"{column} == {values}"
    return query


class Source:
    """Base class for accessing any dataset.

    This class handles validation of parameters and manages the retrieval
    of data in the form of a pandas DataFrame.
    """

    # Cache configuration constants
    MEMORY_CACHE_MAXSIZE = 20
    MEMORY_CACHE_TTL = 6000  # seconds
    MEMORY_CACHE_MAX_ITEM_MB = 50  # Maximum size of individual cached items

    # Shared thread-safe memory cache (class-level, shared across all instances)
    memory_cache = ThreadSafeMemoryCache(
        maxsize=MEMORY_CACHE_MAXSIZE, ttl=MEMORY_CACHE_TTL
    )

    # Shared bulk and query caches (singletons per process)
    _shared_bulk_cache: BulkCacheManager | None = None
    _shared_query_cache: QueryCacheManager | None = None
    _cache_lock = threading.Lock()

    def __init__(self):
        self.de_providers = None
        self.de_recipients = None
        self.de_indicators = None
        self.filters = None
        self.de_sectors = None
        self.schema = None
        self._param_hash = None

    @property
    def bulk_cache(self) -> BulkCacheManager:
        """Get bulk cache manager singleton.

        Creates one bulk cache per process, recreating if data directory changes.
        """
        current_raw = Path(ODAPaths.raw_data).resolve()
        # Fast-path without lock
        bc = Source._shared_bulk_cache
        if bc is not None and bc.base_dir.parent.resolve() == current_raw:
            return bc

        # Slow-path with lock
        with Source._cache_lock:
            bc = Source._shared_bulk_cache
            if bc is None or bc.base_dir.parent.resolve() != current_raw:
                Source._shared_bulk_cache = BulkCacheManager(current_raw)
            return Source._shared_bulk_cache

    @property
    def query_cache(self) -> QueryCacheManager:
        """Get query cache manager singleton.

        Creates one query cache per process, recreating if data directory changes.
        """
        current_raw = Path(ODAPaths.raw_data).resolve()
        # Fast-path without lock
        qc = Source._shared_query_cache
        if qc is not None and qc.base_dir.parent.resolve() == current_raw:
            return qc

        # Slow-path with lock
        with Source._cache_lock:
            qc = Source._shared_query_cache
            if qc is None or qc.base_dir.parent.resolve() != current_raw:
                Source._shared_query_cache = QueryCacheManager(current_raw)
            return Source._shared_query_cache

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
        self, additional_filters: list[tuple] | None = None
    ) -> list[tuple[str, str, list]] | None:
        filters = (
            self.filters + additional_filters if additional_filters else self.filters
        )
        return filters or None

    def _cache_in_memory(self, param_hash: str, df: pd.DataFrame) -> None:
        """Cache DataFrame in memory if it's small enough.

        Only caches if size is below MEMORY_CACHE_MAX_ITEM_MB threshold to avoid
        excessive memory usage.
        """
        size_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)
        if size_mb < self.MEMORY_CACHE_MAX_ITEM_MB:
            # Make a copy to avoid mutations affecting cache
            self.memory_cache[param_hash] = df.copy(deep=True)

    def _apply_columns_and_clean(
        self,
        df: pd.DataFrame,
        columns: list | None = None,
        already_cleaned: bool = False,
    ) -> pd.DataFrame:
        """Apply cleaning and column selection to DataFrame.

        IMPORTANT: Cleaning must happen BEFORE column selection because
        user-facing column names (e.g., "recipient_name") only exist after
        clean_raw_df() normalizes the raw column names.

        Args:
            df: DataFrame to process
            columns: Optional list of columns to select
            already_cleaned: If True, skip cleaning step (data already normalized)

        Returns:
            Cleaned and filtered DataFrame
        """
        # Clean first to normalize column names (unless already done)
        if not already_cleaned:
            df = df.pipe(clean_raw_df)

        # Then select columns if requested
        if columns:
            # Filter to only columns that exist (some might not be in this dataset)
            available_cols = [c for c in columns if c in df.columns]
            if available_cols:
                df = df[available_cols]

        return df


class DACSource(Source):
    """Base class for accessing DAC datasets.

    This class handles validation of parameters and manages the retrieval
    of data in the form of a pandas DataFrame.
    """

    # Use thread-safe memory cache with inherited constants
    memory_cache = ThreadSafeMemoryCache(
        maxsize=Source.MEMORY_CACHE_MAXSIZE, ttl=Source.MEMORY_CACHE_TTL
    )

    def __init__(self):
        super().__init__()

        self.schema = ODASchema

    def _init_filters(
        self,
        years: list[int] | range | int | None = None,
        providers: list[int] | int | None = None,
        recipients: list[int] | int | None = None,
        sectors: list[int] | int | None = None,
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
            values_check = [values] if isinstance(values, str | int) else values
            missing_values = set(values_check) - set(data[column].unique())
            if missing_values:
                logger.warning(
                    f"Missing values in {column}: {', '.join(map(str, missing_values))}"
                )

    def _get_package_version(self) -> str:
        """Get package version for cache invalidation."""
        try:
            from importlib.metadata import version

            return version("oda_data")
        except Exception:
            return "unknown"

    def _fetch_from_bulk_cache(
        self, filters: list[tuple] | None, columns: list | None
    ) -> pd.DataFrame:
        """Fetch data from bulk cache with download coordination.

        This method uses FileLock via BulkCacheManager - if another thread is
        downloading, this call blocks until download completes.

        Uses PyArrow predicate pushdown and column projection for maximum efficiency.
        """
        entry = BulkCacheEntry(
            key=f"{self.__class__.__name__}_bulk",
            fetcher=self._create_bulk_fetcher(),
            ttl_days=30,
            version=self._get_package_version(),
        )

        # This blocks if another thread is downloading (prevents concurrent waste)
        bulk_path = self.bulk_cache.ensure(entry, refresh=False)

        pyarrow_filters = filters if filters else None

        # Read with both predicate pushdown (filters) AND column selection
        # This minimizes I/O and memory usage
        df = pd.read_parquet(
            bulk_path, filters=pyarrow_filters, columns=columns, engine="pyarrow"
        )

        # Note: Data is already cleaned (cleaned in bulk fetcher before caching)
        return df

    @abstractmethod
    def _create_bulk_fetcher(self) -> Callable[[Path], None]:
        """Create a fetcher function for bulk download.

        Must be implemented by subclasses to return a function that:
        - Takes a Path argument (target file)
        - Downloads data and writes it as parquet to that path

        Returns:
            Fetcher function
        """
        pass

    def read(
        self,
        using_bulk_download: bool = False,
        additional_filters: list[tuple] | None = None,
        columns: list | None = None,
    ):
        """Reads a dataset with 3-tier cache coordination.

        Cache tiers:
        1. ThreadSafeMemoryCache (fast, in-memory, thread-safe)
        2. QueryCacheManager (on-disk parquet of filtered results)
        3. BulkCacheManager (on-disk parquet of full datasets)

        Args:
            using_bulk_download: Whether to read from bulk cache
            additional_filters: Additional filters to apply
            columns: Subset of columns to read

        Returns:
            Filtered and cleaned DataFrame
        """
        # Create filters and generate cache key
        filters = self._get_read_filters(additional_filters=additional_filters)
        param_hash = generate_param_hash(filters if filters else [])

        # 1. Try memory cache (thread-safe, fastest)
        df = self.memory_cache.get(param_hash)
        if df is not None:
            logger.info("Cache hit: memory")
            # Data is already cleaned, just select columns if needed
            return self._apply_columns_and_clean(df, columns, already_cleaned=True)

        # 2. Try query cache (on-disk parquet, fast)
        df = self.query_cache.load(self.__class__.__name__, param_hash, None, columns)
        if df is not None:
            logger.info("Cache hit: query cache")
            # Only cache in memory if we loaded full data (no column selection)
            # This way memory cache is reusable for queries with different column subsets
            if not columns:
                self._cache_in_memory(param_hash, df)
            # Data is already cleaned and column-selected at parquet read level
            return df

        # 3. Cache miss (query/memory) - need to fetch data
        if using_bulk_download:
            # Fetch from bulk cache with predicate pushdown and column selection
            logger.info("Query cache miss - reading from bulk cache")
            df = self._fetch_from_bulk_cache(filters, columns)
        else:
            # Download via API (uses init-time filters only)
            logger.info("Cache miss - downloading via API")
            df = self.download()

            # Clean data first (needed for filter column names to match)
            df = df.pipe(clean_raw_df)

            # Apply additional filters if provided
            # (download() only knows about init-time filters, not additional_filters)
            query = _filters_to_query(filters) if filters else None
            if query:
                df = df.query(query)

        # 4. Cache the result (cleaned, filtered data with all columns)
        self.query_cache.save(self.__class__.__name__, param_hash, df)
        self._cache_in_memory(param_hash, df)

        # 5. Apply column selection if requested (data is already cleaned)
        return self._apply_columns_and_clean(df, columns, already_cleaned=True)

    @abstractmethod
    def download(self, **kwargs) -> pd.DataFrame:
        """Abstract method to download the dataset."""
        pass


class DAC1Data(DACSource):
    """Class to handle DAC1 data"""

    def __init__(
        self,
        years: list[int] | range | int | None = None,
        providers: list[int] | int | None = None,
        indicators: list[int] | int | None = None,
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

    def _create_bulk_fetcher(self) -> Callable[[Path], None]:
        """Create fetcher for DAC1 bulk download.

        DAC1 bulk is downloaded directly as DataFrame (no zip file).
        Column names are cleaned before caching.
        """

        def fetcher(target_path: Path):
            logger.info("Downloading DAC1 bulk data")
            df = download_dac1()  # No filters = full dataset
            df = df.pipe(clean_raw_df)  # Clean column names before caching
            logger.info("Writing DAC1 bulk data to cache")
            df.to_parquet(target_path)

        return fetcher

    def download(self):
        """Downloads DAC1 data via API (filtered query).

        Note: For bulk downloads, use read(using_bulk_download=True) instead.
              Bulk downloads are handled by the cache system.
        """
        df = download_dac1(
            start_year=self.start,
            end_year=self.end,
            filters=self._get_filtered_download_filters(),
        )

        logger.info("DAC1 data downloaded successfully.")
        return df


class DAC2AData(DACSource):
    """Class to handle the DAC2A data."""

    def __init__(
        self,
        years: list[int] | range | int | None = None,
        providers: list[int] | int | None = None,
        recipients: list[int] | int | None = None,
        indicators: list[int] | int | None = None,
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

    def _create_bulk_fetcher(self) -> Callable[[Path], None]:
        """Create fetcher for DAC2A bulk download.

        DAC2A bulk is downloaded directly as DataFrame (no zip file).
        Column names are cleaned before caching.
        """

        def fetcher(target_path: Path):
            logger.info("Downloading DAC2A bulk data")
            df = download_dac2a()  # No filters = full dataset
            df = df.pipe(clean_raw_df)  # Clean column names before caching
            logger.info("Writing DAC2A bulk data to cache")
            df.to_parquet(target_path)

        return fetcher

    def download(self):
        """Downloads DAC2A data via API (filtered query).

        Note: For bulk downloads, use read(using_bulk_download=True) instead.
              Bulk downloads are handled by the cache system.
        """
        df = download_dac2a(
            start_year=self.start,
            end_year=self.end,
            filters=self._get_filtered_download_filters(),
        )

        logger.info("DAC2A data downloaded successfully.")
        return df


class CRSData(DACSource):
    """Class to handle the CRS data."""

    def __init__(
        self,
        years: list[int] | range | int | None = None,
        providers: list[int] | int | None = None,
        recipients: list[int] | int | None = None,
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

    def _create_bulk_fetcher(self) -> Callable[[Path], None]:
        """Create fetcher for CRS bulk download.

        CRS bulk is downloaded as a zip file containing parquet.
        """
        return create_crs_bulk_fetcher()

    def download(self):
        """Downloads CRS data via API (filtered query).

        Note: For bulk downloads, use read(using_bulk_download=True) instead.
              Bulk downloads are handled by the cache system.
        """
        df = download_crs(
            start_year=self.start,
            end_year=self.end,
            filters=self._get_filtered_download_filters(),
        )
        logger.info("CRS data downloaded successfully.")
        return clean_raw_df(df)


class MultiSystemData(DACSource):
    """Class to handle the Providers total use of the multilateral system data."""

    def __init__(
        self,
        years: list[int] | range | int | None = None,
        providers: list[int] | int | None = None,
        recipients: list[int] | int | None = None,
        indicators: list[str] | str | None = None,
        sectors: list[int] | int | None = None,
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

    def _create_bulk_fetcher(self) -> Callable[[Path], None]:
        """Create fetcher for MultiSystem bulk download.

        MultiSystem bulk is downloaded as a zip file containing parquet.
        """
        return create_multisystem_bulk_fetcher()

    def download(self):
        """Downloads MultiSystem data via API (filtered query).

        Note: For bulk downloads, use read(using_bulk_download=True) instead.
              Bulk downloads are handled by the cache system.
        """
        df = download_multisystem(
            start_year=self.start,
            end_year=self.end,
            filters=self._get_filtered_download_filters(),
        )
        logger.info("MultiSystem data downloaded successfully.")
        return clean_raw_df(df)


class AidDataSource(Source):
    """Base class for accessing AidData datasets.

    This class handles validation of parameters and manages the retrieval
    of data in the form of a pandas DataFrame.
    """

    # Use thread-safe memory cache with inherited constants
    memory_cache = ThreadSafeMemoryCache(
        maxsize=Source.MEMORY_CACHE_MAXSIZE, ttl=Source.MEMORY_CACHE_TTL
    )

    def __init__(self):
        super().__init__()

        self.schema = AidDataSchema

    def _init_filters(
        self,
        years: list[int] | range | int | None = None,
        recipients: list[str] | str | None = None,
        sectors: list[int] | int | None = None,
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

    # Use thread-safe memory cache with inherited constants
    memory_cache = ThreadSafeMemoryCache(
        maxsize=Source.MEMORY_CACHE_MAXSIZE, ttl=Source.MEMORY_CACHE_TTL
    )

    def __init__(
        self,
        years: list[int] | range | int | None = None,
        recipients: list[str] | str | None = None,
        sectors: list[int] | int | None = None,
    ):
        """Initializes the AidData data handler.

        Args:
            years (Optional[list[int] | range | int]): Years to filter based on commitment year.
            recipients (Optional[list[str] | str]): Recipient names to filter.
            sectors (Optional[list[int] | int]): Sector codes to filter.
        """
        super().__init__()

        self._init_filters(years=years, recipients=recipients, sectors=sectors)

    def _create_bulk_fetcher(self) -> Callable[[Path], None]:
        """Create fetcher for AidData bulk download.

        AidData bulk is downloaded as a zip file containing parquet.
        """
        return create_aiddata_bulk_fetcher()

    def _get_package_version(self) -> str:
        """Get package version for cache invalidation."""
        try:
            from importlib.metadata import version

            return version("oda_data")
        except Exception:
            return "unknown"

    def download(self) -> None:
        """Download method - not used (bulk handled by cache system)."""
        raise RuntimeError(
            "Use read() to access AidData. Bulk downloads are handled by cache system."
        )

    def read(
        self,
        additional_filters: list[tuple] | None = None,
        columns: list | None = None,
    ):
        """Reads AidData from bulk cache with filtering.

        Args:
            additional_filters: Additional filters to apply
            columns: Subset of columns to read

        Returns:
            Filtered DataFrame
        """
        # Create filters and generate cache key
        filters = self._get_read_filters(additional_filters=additional_filters)
        param_hash = generate_param_hash(filters if filters else [])

        # 1. Try memory cache
        df = self.memory_cache.get(param_hash)
        if df is not None:
            logger.info("Cache hit: memory")
            # Data is already cleaned, just select columns if needed
            return self._apply_columns_and_clean(df, columns, already_cleaned=True)

        # 2. Try query cache
        # Use column selection at parquet read level for efficiency
        df = self.query_cache.load(self.__class__.__name__, param_hash, None, columns)
        if df is not None:
            logger.info("Cache hit: query cache")
            # Only cache in memory if we loaded full data (no column selection)
            if not columns:
                self._cache_in_memory(param_hash, df)
            # Data is already cleaned and column-selected at parquet read level
            return df

        # 3. Fetch from bulk cache (always bulk for AidData)
        entry = BulkCacheEntry(
            key=f"{self.__class__.__name__}_bulk",
            fetcher=self._create_bulk_fetcher(),
            ttl_days=180,  # AidData updates less frequently
            version=self._get_package_version(),
        )

        bulk_path = self.bulk_cache.ensure(entry, refresh=False)

        # Use PyArrow predicate pushdown and column selection for maximum efficiency
        pyarrow_filters = filters if filters else None

        # If columns specified, read directly without caching (targeted read)
        if columns:
            logger.info(
                "Query cache miss - reading from bulk cache with column selection"
            )
            # Targeted read: use full PyArrow optimization, no caching
            df = pd.read_parquet(
                bulk_path, filters=pyarrow_filters, columns=columns, engine="pyarrow"
            )
            # Data is already cleaned and column-selected, return directly
            return df
        else:
            logger.info("Query cache miss - reading from bulk cache")
            # Full read: get all columns for caching
            df = pd.read_parquet(bulk_path, filters=pyarrow_filters, engine="pyarrow")

        # 4. Cache the result (cleaned, filtered data with all columns)
        self.query_cache.save(self.__class__.__name__, param_hash, df)
        self._cache_in_memory(param_hash, df)

        # 5. Return full data (already cleaned, no column selection needed)
        return df

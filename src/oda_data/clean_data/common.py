import contextlib
import json
import pathlib
from functools import partial
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pydeflate import oecd_dac_deflate, oecd_dac_exchange, set_pydeflate_path

from oda_data.clean_data.dtypes import set_default_types
from oda_data.clean_data.schema import CRS_MAPPING, ODASchema
from oda_data.clean_data.validation import validate_currency
from oda_data.config import ODAPaths
from oda_data.logger import logger


def clean_column_name(column_name: str) -> str:
    """Clean a column name by removing spaces, special characters, and lowercasing.

    Args:
        column_name (str): The column name to clean.

    Returns:
        str: The cleaned column name.

    Examples:
        >>> clean_column_name("Donor Code")
        "donor_code"
        >>> clean_column_name("recipientName")
        "recipient_name"
        >>> clean_column_name("sector-code")
        "sector_code"
    """

    # Check for all caps convention
    if column_name.isupper():
        column_name = column_name.lower() + "_code"

    single_string = ""

    # split the string into substrings when the case changes
    for i, char in enumerate(column_name):
        if char.isupper() and i != 0:
            if single_string[-1].isupper():
                single_string += char
            else:
                single_string += "_" + char
        else:
            single_string += char

    return (
        single_string.strip()
        .lower()
        .replace(" ", "_")
        .replace("__", "_")
        .replace("-", "")
    )


def read_settings(settings_file_path: pathlib.Path | str) -> dict:
    """Read the settings file for a DAC source.

    Args:
        settings_file_path: The path to the settings file.

    Returns:
        dict: A dictionary containing the settings.
    """

    with open(settings_file_path) as f:
        settings = json.load(f)

    return settings


def clean_raw_df(df: pd.DataFrame) -> pd.DataFrame:
    """Clean a raw dataframe by renaming columns, setting correct data types,
    and optionally dropping columns.

    Args:
        df (pd.DataFrame): The raw dataframe to clean.

    Returns:
        pd.DataFrame: The cleaned dataframe.
    """

    df = df.rename(columns=lambda c: clean_column_name(c))

    if "amount" in df.columns:
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce").astype(
            "float64[pyarrow]"
        )

    df = df.pipe(map_column_schema).replace("\x1a", pd.NA).pipe(set_default_types)

    return df


def clean_parquet_file_in_batches(
    input_path: Path, output_path: Path, batch_size: int = 100_000
) -> None:
    """Clean a large parquet file in batches to minimize memory usage.

    This function reads parquet data in batches, applies the same cleaning
    operations as clean_raw_df (column renaming, schema mapping, type conversion),
    and writes the result incrementally to a new parquet file.

    Memory usage stays low (~200MB) regardless of input file size because only
    one batch is in memory at a time.

    Args:
        input_path: Path to input parquet file
        output_path: Path to output parquet file
        batch_size: Number of rows per batch (default: 100,000)
    """
    logger.info(f"Processing parquet file in batches (batch_size={batch_size:,})")

    # Open parquet file for batch reading
    parquet_file = pq.ParquetFile(input_path)
    total_rows = parquet_file.metadata.num_rows
    logger.info(f"Total rows to process: {total_rows:,}")

    # Create column name mapping at PyArrow level
    original_schema = parquet_file.schema_arrow
    original_columns = original_schema.names

    # Step 1: Clean column names (e.g., CamelCase -> snake_case)
    cleaned_names = {col: clean_column_name(col) for col in original_columns}

    # Step 2: Apply schema mapping (e.g., donor_code -> donor_code, etc.)
    final_names = {
        cleaned: CRS_MAPPING.get(cleaned, cleaned) for cleaned in cleaned_names.values()
    }

    # Create reverse lookup: original -> final name
    col_mapping = {orig: final_names[cleaned_names[orig]] for orig in original_columns}

    writer = None
    processed_rows = 0

    try:
        # Process file in batches
        for batch_idx, record_batch in enumerate(
            parquet_file.iter_batches(batch_size=batch_size)
        ):
            # Convert to pandas for cleaning operations
            batch_df = record_batch.to_pandas()

            # Apply column name transformations
            batch_df = batch_df.rename(columns=col_mapping)

            # Handle special "amount" column if present
            if "amount" in batch_df.columns:
                batch_df["amount"] = pd.to_numeric(
                    batch_df["amount"], errors="coerce"
                ).astype("float64[pyarrow]")

            # Replace special character and set types
            batch_df = batch_df.replace("\x1a", pd.NA).pipe(set_default_types)

            # Convert back to PyArrow table
            table = pa.Table.from_pandas(batch_df, preserve_index=False)

            # Initialize writer with schema from first batch
            if writer is None:
                writer = pq.ParquetWriter(output_path, table.schema)

            # Write batch
            writer.write_table(table)

            processed_rows += len(batch_df)
            if (batch_idx + 1) % 10 == 0 or processed_rows == total_rows:
                logger.info(f"Processed {processed_rows:,}/{total_rows:,} rows")

        logger.info(f"Batch processing complete: {processed_rows:,} rows written")

    finally:
        if writer is not None:
            writer.close()


# Create a helper function to consistently exchange data
dac_exchange = partial(
    oecd_dac_exchange,
    source_currency="USA",
    id_column=ODASchema.PROVIDER_CODE,
    use_source_codes=True,
    value_column="value",
    target_value_column="value",
    year_column="year",
)

# Create a helper function to consistently deflate data
dac_deflate = partial(
    oecd_dac_deflate,
    source_currency="USA",
    id_column=ODASchema.PROVIDER_CODE,
    use_source_codes=True,
    value_column="value",
    target_value_column="value",
    year_column="year",
)


def map_column_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Map the column names to the schema"""
    return df.rename(columns=CRS_MAPPING)


def keep_multi_donors_only(df: pd.DataFrame) -> pd.DataFrame:
    from oda_data import provider_groupings

    multilateral = provider_groupings()["multilateral"]
    df = df.loc[lambda d: d[ODASchema.PROVIDER_CODE].isin(multilateral)]

    return df


def convert_units(
    data,
    indicator: str | None = None,
    currency: str = "USD",
    base_year: int | None = None,
):
    # Ensure pydeflate has the current data path and directory exists (lazy init)
    # In read-only environments, let downstream functions surface meaningful errors
    with contextlib.suppress(Exception):
        ODAPaths.raw_data.mkdir(parents=True, exist_ok=True)
    set_pydeflate_path(ODAPaths.raw_data)

    if indicator is None:
        indicator = ""

    if ((".40." in indicator) and (indicator != "DAC1.40.1")) or (
        currency == "USD" and base_year is None
    ):
        return data.assign(currency=currency, prices="current")

    elif base_year is None:
        return dac_exchange(
            data=data, target_currency=validate_currency(currency)
        ).assign(currency=currency, prices="current")

    else:
        return dac_deflate(
            data=data,
            base_year=base_year,
            target_currency=validate_currency(currency),
        ).assign(currency=currency, prices="constant")


def convert_dot_stat_to_data_explorer_codes(codes: list) -> list:
    from oda_reader.schemas.crs_translation import area_code_mapping

    mapping = area_code_mapping()

    result = []
    for code in codes:
        if code not in mapping:
            logger.warning(f"Code {code} does not have a new Data Explorer code.")
        else:
            result.append(mapping[code])

    return result

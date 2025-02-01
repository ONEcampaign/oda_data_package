from oda_reader import bulk_download_multisystem

from oda_data import config
from oda_data.clean_data.common import clean_raw_df
from oda_data.logger import logger


def download_multisystem() -> None:
    """Download the DAC1 file from OECD.Stat. Data for all years is downloaded at once.
    This function stores the raw data as a parquet file in the raw data folder.

    """

    logger.info(f"Downloading Multisystem data... This may take a while.")

    df = bulk_download_multisystem()

    # Clean the DataFrame
    df = clean_raw_df(df)

    # save the file
    df.to_parquet(config.OdaPATHS.raw_data / f"multisystem_raw.parquet")

    # log a message confirming the operation
    logger.info(f"multisystem_raw data downloaded and saved.")

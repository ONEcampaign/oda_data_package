from oda_reader import bulk_download_crs

from oda_data import config
from oda_data.clean_data.common import clean_raw_df
from oda_data.logger import logger


def download_crs() -> None:
    """Download full CRS file.

    Args:
        years: The year(s) for which to download the CRS data.

    """
    logger.info(
        "The full, detailed CRS is only available as a large file (>1GB). "
        "The package will now download the data, but it may take a while."
    )

    # Use oda_reader to download the full CRS data
    df = bulk_download_crs()

    # Clean the DataFrame
    df = clean_raw_df(df)

    df.to_parquet(config.OdaPATHS.raw_data / "fullCRS.parquet")
    logger.info("Full CRS data downloaded successfully.")

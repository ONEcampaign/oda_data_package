from pandas.util._decorators import deprecate_kwarg

from oda_data import config
from oda_data.get_data import common
from oda_data.logger import logger


def save_clean_crs():
    # Use oda_reader to download the full CRS data
    from oda_reader import bulk_download_crs

    df = bulk_download_crs()

    # Clean the DataFrame
    df = common.clean_raw_df(df)

    df.to_parquet(config.OdaPATHS.raw_data / "fullCRS.parquet")
    logger.info("Full CRS data downloaded successfully.")


@deprecate_kwarg(old_arg_name="method", new_arg_name=None)
def download_crs() -> None:
    """Download full CRS file.

    Args:
        years: The year(s) for which to download the CRS data.
        small_version: If True, only save a small version of the CRS data (default is False).
    """

    logger.info(
        "The full, detailed CRS is only available as a large file (>1GB). "
        "The package will now download the data, but it may take a while."
    )

    return save_clean_crs()

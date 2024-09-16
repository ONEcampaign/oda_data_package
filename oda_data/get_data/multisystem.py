from oda_data import config
from pathlib import Path

from oda_data.get_data import common
from oda_data.logger import logger


def cloud_multisystem_download(
    raw_file_name: str, output_file_name: str, save_path: Path
) -> None:

    # download the zip file from the website
    file_content = common.get_zip(config.MULTISYSTEM_URL)

    # Load the file into a DataFrame
    df = common.read_zip_content(request_content=file_content, file_name=raw_file_name)

    # Clean the DataFrame
    df = common.clean_raw_df(df)

    # save the file
    df.to_feather(save_path / f"{output_file_name}.feather")

    # log a message confirming the operation
    logger.info(f"{raw_file_name} data downloaded and saved.")


def download_multisystem() -> None:
    """Download the DAC1 file from OECD.Stat. Data for all years is downloaded at once.
    This function stores the raw data as a feather file in the raw data folder.

    Args:
        small_version: optionally save a smaller version of the file with only key
             columns (default is False)."""

    logger.info(f"Downloading Multisystem data... This may take a while.")
    cloud_multisystem_download(
        raw_file_name="MultiSystem entire dataset.txt",
        output_file_name="multisystem_raw",
        save_path=config.OdaPATHS.raw_data,
    )

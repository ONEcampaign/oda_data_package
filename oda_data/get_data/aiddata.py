from oda_reader import download_aiddata as api_download_aiddata

from oda_data import config


def download_aiddata(start_year: int | None = None, end_year: int | None = None) -> None:
    """Download the AidData file from their website. Data for all years is downloaded at once.
    This function stores the raw data as a parquet file in the raw data folder.

    Args:
        start_year: optionally specify the start year for the data download.
        end_year: optionally specify the end year for the data download.

    """

    df = api_download_aiddata(start_year=start_year, end_year=end_year)

    # save the file
    df.to_parquet(config.OdaPATHS.raw_data / f"aiddata.parquet")

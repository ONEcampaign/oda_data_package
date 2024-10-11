from oda_reader import download_dac1 as api_download_dac1

from oda_data import config


def download_dac1(start_year: int | None = None, end_year: int | None = None) -> None:
    """Download the DAC1 file from OECD.Stat. Data for all years is downloaded at once.
    This function stores the raw data as a parquet file in the raw data folder.

    Args:
        start_year: optionally specify the start year for the data download.
        end_year: optionally specify the end year for the data download.

    """

    suffix = ""

    if start_year is not None:
        suffix += f"_{start_year}"
    if end_year is not None:
        suffix += f"_{end_year}"

    df = api_download_dac1(start_year=start_year, end_year=end_year)

    # save the file
    df.to_parquet(config.OdaPATHS.raw_data / f"table1_raw{suffix}.parquet")

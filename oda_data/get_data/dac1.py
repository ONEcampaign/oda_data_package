from oda_data import config
from oda_data.get_data import common


def download_dac1(small_version: bool = False) -> None:
    """Download the DAC1 file from OECD.Stat. Data for all years is downloaded at once.
    This function stores the raw data as a feather file in the raw data folder."""

    common.download_single_table(
        bulk_url=config.TABLE1_URL,
        raw_file_name="Table1_Data.csv",
        output_file_name="table1_raw",
        save_path=config.OdaPATHS.raw_data,
        config_file_path=config.OdaPATHS.cleaning_config / "dac1_config.json",
        small_version=small_version,
    )

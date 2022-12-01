from oda_data import config

from oda_data.get_data import common


def download_multisystem(small_version: bool = False) -> None:
    """Download the DAC1 file from OECD.Stat. Data for all years is downloaded at once.
    This function stores the raw data as a feather file in the raw data folder."""

    common.download_single_table(
        bulk_url=config.MULTISYSTEM_URL,
        raw_file_name="MultiSystem entire dataset.txt",
        output_file_name="multisystem_raw",
        save_path=config.OdaPATHS.raw_data,
        config_file_path=config.OdaPATHS.settings / "multisystem_config.json",
        small_version=small_version,
    )

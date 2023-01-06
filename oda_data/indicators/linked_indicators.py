import pandas as pd

from oda_data.logger import logger


def linked_indicator(
    data: pd.DataFrame,
    main_indicator: str,
    fallback_indicator: str,
    indicator_name: str,
) -> pd.DataFrame:
    """Builds a linked indicator using two underlying indicators.

    Args:
        data: a Pandas Dataframe containing an 'indicator' column with exactly two
        indicators. The DataFrame must also contain a 'value' column with the values
        main_indicator: The indicator that will be given priority.
        fallback_indicator: The indicator that will be used to fill the gaps.
        indicator_name: The name that will be given to the new indicator.

    """

    # Create a grouper that excludes the calue column
    grouper = [c for c in data.columns if c not in ["value"]]

    # unstack the indicator column
    df = data.set_index(grouper).unstack("indicator")

    # new indicator
    new = ("value", indicator_name)
    # main indicator
    main = ("value", main_indicator)
    # fallback indicator
    fallback = ("value", fallback_indicator)

    # Create a new column with the new indicator name, using the main indicator
    # and filling missing values with the fallback indicator
    try:
        df[main]
    except KeyError:
        logger.info(f"Main indicator {main_indicator} has no data")
        main = fallback

    df[new] = df[main].fillna(df[fallback])

    # Stack the indicator column back and return
    return df.filter([new], axis=1).stack("indicator").reset_index()

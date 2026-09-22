import string
import warnings
from typing import Literal

import pandas as pd

from oda_data.clean_data.schema import ODASchema
from oda_data.config import ODAPaths
from oda_data.logger import logger

CROSSWALK_FILE = "multilateral_channel_crosswalk.csv"

# Money columns checked, in order, to decide whether an unmapped
# (provider_code, agency_code) pair carries a nonzero value. Whichever of
# these are present in the input frame are summed row-wise.
_MONEY_COLUMNS: tuple[str, ...] = (
    ODASchema.VALUE,
    ODASchema.USD_COMMITMENT,
    ODASchema.USD_DISBURSEMENT,
    ODASchema.USD_RECEIVED,
    ODASchema.USD_GRANT_EQUIV,
    ODASchema.USD_NET_DISBURSEMENT,
)

# Sentinel used to make missing agency codes join to each other. -1 is not
# used as a real OECD agency code.
_AGENCY_SENTINEL: int = -1


class UnmappedChannelError(Exception):
    """A (provider_code, agency_code) pair carrying nonzero value has no
    entry in the multilateral channel crosswalk."""


def get_crs_official_mapping() -> pd.DataFrame:
    """Get the CRS official mapping file."""
    return pd.read_csv(ODAPaths.cleaning / "crs_channel_mapping.csv")


def get_multilateral_channel_crosswalk() -> pd.DataFrame:
    """Get the reviewed multilateral channel crosswalk.

    Returns:
        pd.DataFrame: columns provider_code, agency_code, provider_name,
        agency_name, channel_code, status ("mapped" or "excluded"), reason,
        method, reviewed.
    """
    return pd.read_csv(ODAPaths.cleaning / CROSSWALK_FILE)


def clean_string(text_series: pd.Series | str) -> pd.Series:
    """Clean the text by removing punctuation (converted to spaces),
     converting to lowercase, removing unnecessary spacing.

    Args:
        text_series (pd.Series): The Series of text to be cleaned.

    Returns:
        pd.Series: The cleaned text as a Series.
    """
    # if string, convert to series
    if isinstance(text_series, str):
        text_series = pd.Series(text_series)
        as_string = True
    else:
        as_string = False

    # Convert to lowercase
    text_series = text_series.str.lower()

    # Remove punctuation (change to spaces)
    punct_translation = str.maketrans(string.punctuation, " " * len(string.punctuation))
    text_series = text_series.str.translate(punct_translation)

    # Replace multiple spaces with a single space
    text_series = text_series.str.replace(r"\s+", " ", regex=True)

    # Remove leading and trailing spaces
    text_series = text_series.str.strip()

    # if text was string, convert back to string
    if as_string:
        text_series = text_series[0]

    return text_series


def channel_to_code(map_to: str = "channel_name") -> dict[str, int]:
    """Get a dictionary mapping channel names to channel codes.

    Args:
        map_to (str, optional): The column to map to. Defaults to "channel_name".
        Other options are "en_acronym" and "fr_acronym".

    Returns:
        dict: A dictionary mapping channel names (or acronyms) to channel codes.

    """
    # Check that map_to is valid
    if map_to not in ["en_acronym", "fr_acronym", "channel_name"]:
        raise ValueError(
            "map_to must be one of 'en_acronym', 'fr_acronym', 'channel_name'"
        )

    # Get the CRS mapping data, filter the desired column, and drop duplicates
    mapping_data = (
        get_crs_official_mapping()
        .assign(channel_name=lambda d: clean_string(d.channel_name))
        .drop_duplicates(subset=[map_to, "channel_code"])
        .dropna(subset=[map_to])
        .sort_values(by=[map_to])
        .drop_duplicates(subset=[map_to], keep="last")
    )

    # Convert to dictionary and return
    return mapping_data.set_index(map_to)["channel_code"].to_dict()


def add_channel_names(
    df: pd.DataFrame,
    codes_column: str = "channel_code",
    target_column: str = "mapped_name",
) -> pd.DataFrame:
    """Add a column with the channel names.

    Args:
        df (pd.DataFrame): The dataframe containing the channel codes.
        codes_column (str, optional): The column containing the channel codes.
        Defaults to "channel_code".
        target_column (str, optional): The column to add the channel names to.
        Defaults to "mapped_name".

    Returns:
        pd.DataFrame: The dataframe with the channel names.
    """
    # Get a dictionary with channel codes to channel names
    channel_names = channel_to_code(map_to="channel_name")

    # Map the channel codes to channel names
    df[target_column] = df[codes_column].map({v: k for k, v in channel_names.items()})

    return df


def _row_money(df: pd.DataFrame) -> pd.Series:
    """Sum whichever known money columns are present in `df`, row-wise."""
    present = [c for c in _MONEY_COLUMNS if c in df.columns]

    if not present:
        return pd.Series(0.0, index=df.index)

    return df[present].fillna(0).sum(axis=1)


def _normalise_join_key(codes: pd.Series, *, is_agency: bool) -> pd.Series:
    """Coerce a provider or agency code column to a nullable Int64 join key.

    Normalises float-typed codes (e.g. 1.0) to integers. Agency codes are
    additionally filled with a sentinel so that missing agencies on both
    sides of the join match each other, since pandas merges never match
    NA to NA.
    """
    key = pd.to_numeric(codes, errors="coerce").astype("Int64")

    if is_agency:
        key = key.fillna(_AGENCY_SENTINEL)

    return key


def _summarise_unmapped(rows: pd.DataFrame, provider_col: str, agency_col: str) -> str:
    """Build a human-readable summary of unmapped (provider, agency) pairs
    and the money attached to each, largest first."""
    summary = (
        rows.assign(_money=_row_money(rows))
        .groupby([provider_col, agency_col], dropna=False)
        .agg(
            _money=("_money", "sum"),
            **(
                {ODASchema.PROVIDER_NAME: (ODASchema.PROVIDER_NAME, "first")}
                if ODASchema.PROVIDER_NAME in rows.columns
                else {}
            ),
            **(
                {ODASchema.AGENCY_NAME: (ODASchema.AGENCY_NAME, "first")}
                if ODASchema.AGENCY_NAME in rows.columns
                else {}
            ),
        )
        .reset_index()
        .sort_values("_money", ascending=False)
    )

    lines = []
    for _, row in summary.iterrows():
        names = [
            str(row[col])
            for col in (ODASchema.PROVIDER_NAME, ODASchema.AGENCY_NAME)
            if col in summary.columns and pd.notna(row[col])
        ]
        name_part = f" ({' / '.join(names)})" if names else ""
        lines.append(
            f"provider_code={row[provider_col]!r}, agency_code={row[agency_col]!r}"
            f"{name_part}: {row['_money']:,.2f}"
        )

    return "; ".join(lines)


def add_multilateral_channel_codes(
    df: pd.DataFrame,
    on_unmapped: Literal["raise", "unallocated"] = "raise",
    crosswalk: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Add channel codes by an exact join on (provider_code, agency_code)
    against the reviewed multilateral channel crosswalk.

    Rows whose crosswalk status is "excluded" are dropped; the dropped row
    count and value are logged at INFO with the reason. A (provider_code,
    agency_code) pair carrying nonzero value that has no crosswalk entry
    raises `UnmappedChannelError` naming the pairs, their names and the
    money, unless `on_unmapped="unallocated"`, in which case those rows are
    kept with a null `channel_code`.

    Args:
        df (pd.DataFrame): CRS-shaped data with provider_code and
        agency_code columns.
        on_unmapped (Literal["raise", "unallocated"], optional): What to do
        with unmapped pairs carrying nonzero value. Defaults to "raise".
        crosswalk (pd.DataFrame | None, optional): The crosswalk to join
        against. Defaults to `get_multilateral_channel_crosswalk()`.

    Returns:
        pd.DataFrame: `df` with a nullable `channel_code` column added.

    Raises:
        UnmappedChannelError: if `on_unmapped="raise"` and at least one
        unmapped pair carries nonzero value.
    """
    provider_col = ODASchema.PROVIDER_CODE
    agency_col = ODASchema.AGENCY_CODE

    if crosswalk is None:
        crosswalk = get_multilateral_channel_crosswalk()

    df = df.copy(deep=True).drop(
        columns=[c for c in ("channel_code", "status", "reason") if c in df.columns]
    )
    df["_provider_key"] = _normalise_join_key(df[provider_col], is_agency=False)
    df["_agency_key"] = _normalise_join_key(df[agency_col], is_agency=True)

    crosswalk_keyed = crosswalk.assign(
        _provider_key=_normalise_join_key(crosswalk["provider_code"], is_agency=False),
        _agency_key=_normalise_join_key(crosswalk["agency_code"], is_agency=True),
    ).drop_duplicates(subset=["_provider_key", "_agency_key"])

    merged = df.merge(
        crosswalk_keyed[
            ["_provider_key", "_agency_key", "channel_code", "status", "reason"]
        ],
        on=["_provider_key", "_agency_key"],
        how="left",
    ).drop(columns=["_provider_key", "_agency_key"])

    excluded_mask = merged["status"] == "excluded"
    if excluded_mask.any():
        excluded_money = _row_money(merged.loc[excluded_mask]).sum()
        reasons = sorted(merged.loc[excluded_mask, "reason"].dropna().unique())
        logger.info(
            "Dropped %d excluded multilateral channel rows (value=%.2f). Reasons: %s",
            int(excluded_mask.sum()),
            excluded_money,
            "; ".join(reasons) if reasons else "none recorded",
        )
        merged = merged.loc[~excluded_mask].reset_index(drop=True)

    unmapped_mask = merged["channel_code"].isna()
    if unmapped_mask.any():
        nonzero_unmapped = merged.loc[unmapped_mask & (_row_money(merged) != 0)]

        if not nonzero_unmapped.empty and on_unmapped == "raise":
            raise UnmappedChannelError(
                "Unmapped multilateral channel pairs carry nonzero value and are not "
                "in the crosswalk. Add them to the crosswalk, or pass "
                "on_unmapped='unallocated' to keep them with a null channel_code. "
                f"Pairs: {_summarise_unmapped(nonzero_unmapped, provider_col, agency_col)}"
            )

    merged["channel_code"] = merged["channel_code"].astype("Int32[pyarrow]")

    return merged.drop(columns=["status", "reason"])


def add_multi_channel_codes(
    df: pd.DataFrame,
    on_unmapped: Literal["raise", "unallocated"] = "raise",
) -> pd.DataFrame:
    """Deprecated alias for `add_multilateral_channel_codes`.

    Args:
        df (pd.DataFrame): CRS-shaped data with provider_code and
        agency_code columns.
        on_unmapped (Literal["raise", "unallocated"], optional): Forwarded
        to `add_multilateral_channel_codes`. Defaults to "raise".

    Returns:
        pd.DataFrame: `df` with a nullable `channel_code` column added.
    """
    warnings.warn(
        "add_multi_channel_codes is deprecated and will be removed in a future "
        "release; use add_multilateral_channel_codes instead. Behaviour has "
        "changed: this now RAISES UnmappedChannelError by default for any "
        "(provider_code, agency_code) pair carrying nonzero value that is not "
        "in the reviewed crosswalk, instead of silently guessing a channel via "
        "fuzzy/regex name matching. Pass on_unmapped='unallocated' to keep "
        "those rows with a null channel_code instead of raising.",
        DeprecationWarning,
        stacklevel=2,
    )
    return add_multilateral_channel_codes(df, on_unmapped=on_unmapped)

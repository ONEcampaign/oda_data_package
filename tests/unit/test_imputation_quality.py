"""``quality_report`` over rows shaped like ``oda.records`` of the imputation."""

import pandas as pd
import pytest

from oda_data._methods.imputation_quality import quality_report


@pytest.fixture
def rows() -> pd.DataFrame:
    """Two providers' imputation rows in 2022: channel 47128 on its own
    current shares, 44007 on IDA's (44002) stale shares through a proxy, and
    909090 unallocated."""
    return pd.DataFrame(
        {
            "year": [2022, 2022, 2022, 2022, 2022],
            "provider": ["4", "4", "5", "5", "12"],
            "channel": ["47128", "47128", "47128", "44007", "909090"],
            "recipient": ["248", "238", "248", "248", None],
            "sector": ["12220", "11110", "12220", "24010", None],
            "allocation_status": [
                "imputed",
                "imputed",
                "imputed",
                "proxy",
                "unallocated",
            ],
            "share_channel": pd.array([47128, 47128, 47128, 44002, None], "Int64"),
            "share_years": ["2020-2022", "2020-2022", "2020-2022", "2018-2020", None],
            "value": [750.0, 250.0, 75.0, 700.0, 250.0],
        }
    )


def test_by_channel_year_totals_money_and_staleness(rows: pd.DataFrame) -> None:
    summary = quality_report(rows)["by_channel_year"].set_index("channel")

    assert summary.loc["47128", "value"] == 1075.0
    assert summary.loc["47128", "staleness_years"] == 0
    assert summary.loc["44007", "staleness_years"] == 2
    assert pd.isna(summary.loc["909090", "staleness_years"])
    assert summary.loc["909090", "allocation_statuses"] == ["unallocated"]


def test_totals_by_status_add_up_to_the_whole(rows: pd.DataFrame) -> None:
    totals = quality_report(rows)["totals_by_status"].set_index("allocation_status")

    assert totals["value"].to_dict() == {
        "imputed": 1075.0,
        "proxy": 700.0,
        "unallocated": 250.0,
    }
    assert totals["share_of_total"].sum() == pytest.approx(1.0)


def test_a_clean_result_has_no_duplicates_negatives_or_missing_channels(
    rows: pd.DataFrame,
) -> None:
    report = quality_report(rows)

    assert report["duplicates"].empty
    assert report["negatives"].empty
    assert report["missing_channels"].empty


def test_repeated_keys_negative_rows_and_missing_channels_are_listed(
    rows: pd.DataFrame,
) -> None:
    broken = pd.concat([rows, rows.iloc[[0]]], ignore_index=True)
    broken.loc[1, "value"] = -5.0
    broken.loc[4, "channel"] = None

    report = quality_report(broken)

    assert len(report["duplicates"]) == 2
    assert report["negatives"]["value"].tolist() == [-5.0]
    assert len(report["missing_channels"]) == 1


def test_concentration_is_per_share_window_and_skips_rows_without_one(
    rows: pd.DataFrame,
) -> None:
    concentration = quality_report(rows)["concentration"]

    assert concentration[
        ["share_channel", "top_sector", "flagged"]
    ].values.tolist() == [
        [44002, "24010", True],
        [47128, "12220", True],
    ]
    assert concentration["top_share"].tolist() == pytest.approx([1.0, 825.0 / 1075.0])


def test_the_concentration_threshold_is_configurable(rows: pd.DataFrame) -> None:
    concentration = quality_report(rows, concentration_threshold=0.9)["concentration"]

    assert concentration["flagged"].tolist() == [True, False]

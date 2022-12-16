import pandas as pd
import pytest

from oda_data import set_data_path, config
from oda_data.classes.oda_data import ODAData

set_data_path(config.OdaPATHS.test_files)


def test_odadata():
    # set indicator
    indicator = "recipient_bilateral_flow_net"

    # single year, no recipients, current usd
    oda = ODAData(years=2019)
    oda.load_indicator(indicator)

    assert oda.base_year is None
    assert oda.currency == "USD"
    assert oda.prices == "current"
    assert oda.recipients is None

    # multiple years, some recipients, constant eur
    oda = ODAData(
        years=[2019, 2020],
        donors=[4, 12],
        recipients=[57, 63],
        currency="EUR",
        prices="constant",
        base_year=2019,
    )

    oda.load_indicator(indicator)

    data = oda.get_data(indicator)

    assert data.donor_code.nunique() == 2
    assert data.recipient_code.nunique() == 2
    assert data.prices.unique()[0] == "constant"
    assert data.currency.unique()[0] == "EUR"

    # load a second indicator
    oda.load_indicator("recipient_total_flow_net")
    data = oda.get_data("recipient_total_flow_net")

    assert len(oda.indicators_data) == 2
    assert "recipient_total_flow_net" in oda.indicators_data.keys()
    assert data.indicator.nunique() == 1

    all_data = oda.get_data("all")
    assert all_data.indicator.nunique() == 2

    oda = ODAData(years=2019, currency="EUR")
    oda.load_indicator("recipient_total_flow_net")
    data = oda.get_data("recipient_total_flow_net")

    assert data.currency.unique()[0] == "EUR"
    assert data.prices.unique()[0] == "current"

    with pytest.raises(ValueError):
        ODAData(years=2019, currency="YEN")

    with pytest.raises(ValueError):
        ODAData(years=2019, prices="daily")

    with pytest.raises(ValueError):
        ODAData(years=2019, prices="current", base_year=2020)

    with pytest.raises(ValueError):
        ODAData(years=2019, prices="constant", base_year=None)

    # test specifying recipients for an indicator that doesn't need them
    oda = ODAData(years=2019, recipients=[57, 63]).load_indicator("total_oda_ge")

    # test adding names
    cols = oda.get_data("all").columns

    # add names
    oda.add_names()

    # check that the names have been added
    assert len(cols) < len(oda.get_data("all").columns)

    # add names for specific column
    oda.add_names(id_columns="donor_code")

    assert "donor_name" in oda.get_data("all").columns


def test_oda_data_linked_indicator():
    # load a linked indicator
    oda = ODAData(years=2019).load_indicator("total_in_donor_students_ge_linked")

    assert oda.get_data().indicator.unique()[0] == "total_in_donor_students_ge_linked"


def test_oda_data_simplify_output(caplog):
    # Load a CRS indicator
    test = ODAData(years=2019, recipients=[89, 65]).load_indicator(
        "crs_bilateral_total_flow_gross_by_purpose"
    )

    # Get the full data to compare
    full = test.get_data("all")

    # Indicate that we want to simplify the output
    test.simplify_output_df(
        ["donor_code", "recipient_code", "indicator", "sector_code", "value"]
    )

    # Get the simplified data
    result1 = test.get_data("all")

    assert len(result1.columns) <= len(full.columns)
    assert len(result1.columns) == 5

    # Test not including the value
    test.simplify_output_df(
        ["donor_code", "recipient_code", "indicator", "sector_code"]
    )

    # Get the simplified data
    result2 = test.get_data("all")

    # Compare
    pd.testing.assert_frame_equal(result1, result2)

    # Test including an unavailable column (should show warning but continue)
    test.simplify_output_df(
        ["donor_code", "recipient_code", "indicator", "sector_code", "value", "test"]
    )

    # Get the simplified data
    result3 = test.get_data("all")

    pd.testing.assert_frame_equal(result1, result3)

    assert "['test']" in caplog.text


def test_build_oda_indicator():

    test = ODAData(years=range(2015, 2020))

    result = test._build_research_indicator("imputed_multi_flow_disbursement_gross")

    years = list(result.year.unique())
    assert years == [2017, 2018, 2019]

    result_from_obj = test.load_indicator("imputed_multi_flow_disbursement_gross")
    assert len(result) == len(result_from_obj.get_data())


def test_available_properties():
    oda = ODAData()

    assert isinstance(oda.available_indicators(), list)
    assert isinstance(oda.available_donors(), dict)
    assert isinstance(oda.available_recipients(), dict)
    assert isinstance(oda.available_currencies(), list)


def test_add_shares() -> None:
    oda = ODAData(years=range(2014, 2021), donors=[4, 12])

    oda.load_indicator("total_bi_multi_flow_disbursement_gross")
    oda.load_indicator("total_oda_grants_ge")
    oda.load_indicator("recipient_loans_flow_net")

    oda.add_share_of_total(include_share_of=True)

    bi_imputed_total = oda.get_data("total_bi_multi_flow_disbursement_gross")
    total_grants = oda.get_data("total_oda_grants_ge")
    recipient_loans = oda.get_data("recipient_loans_flow_net")

    assert "share" in bi_imputed_total.columns
    assert "share_of" in total_grants.columns

    assert (
        bi_imputed_total.share_of.unique()[0]
        == "total_bi_multi_flow_disbursement_gross"
    )

    assert total_grants.share_of.unique()[0] == "total_oda_ge"

    assert recipient_loans.share_of.unique()[0] == "recipient_total_flow_net"

    # Check that share total is 100
    assert (
        bi_imputed_total.groupby(["donor_code", "year"], as_index=False)
        .sum(numeric_only=True)
        .round(2)
        .share.unique()
    )[0] == 100

    oda = ODAData(years=[2020])
    oda.load_indicator("total_oda_grants_ge")
    oda.add_share_of_total(include_share_of=False)

    assert "share_of" not in oda.get_data().columns


def test_oda_gni():

    oda = ODAData(years=range(2014, 2021), donors=12)

    oda.load_indicator(["total_oda_ge", "total_oda_flow_net"])

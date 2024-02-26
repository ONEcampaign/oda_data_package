from oda_data.indicators.research_indicators import (
    multilateral_imputed_flows,
    one_core_oda_flow,
    one_core_oda_ge,
    one_core_oda_ge_linked,
    one_non_core_oda_ge_linked,
    total_bi_multi_flows,
    total_oda_official_definition,
)

from oda_data import set_data_path, config

set_data_path(config.OdaPATHS.test_files)


def test_multilateral_imputed_flows():
    # Test that the function runs without error
    df = multilateral_imputed_flows(
        years=list(range(2012, 2021)),
        currency="USD",
        prices="current",
        donors=None,
        recipients=None,
        base_year=None,
        other_data="test",
    )

    years = list(df.year.unique())
    prices = list(df.prices.unique())
    currencies = list(df.currency.unique())
    donors = list(df.oecd_donor_code.unique())
    recipients = list(df.oecd_recipient_code.unique())

    assert years == list(range(2020, 2013, -1))

    df2 = multilateral_imputed_flows(
        years=list(range(2012, 2021)),
        currency="USD",
        prices="current",
        donors=[4, 12],
        recipients=[189, 224],
        base_year=None,
    )

    prices2 = list(df2.prices.unique())
    currencies2 = list(df2.currency.unique())
    donors2 = list(df2.oecd_donor_code.unique())
    recipients2 = list(df2.oecd_recipient_code.unique())

    assert prices == prices2
    assert currencies == currencies2
    assert len(donors) > len(donors2)
    assert len(recipients) > len(recipients2)

    assert 189 in recipients2


def test_total_bi_multi_flows():
    df = total_bi_multi_flows(
        years=list(range(2012, 2021)),
        currency="USD",
        prices="current",
        donors=[4, 12],
        recipients=[189],
        base_year=None,
        other_data="test",
    )

    years = list(df.year.unique())
    prices = list(df.prices.unique())
    currencies = list(df.currency.unique())
    donors = list(df.oecd_donor_code.unique())
    recipients = list(df.oecd_recipient_code.unique())

    assert years == list(range(2020, 2013, -1))
    assert donors == [4, 12]
    assert recipients == [189]
    assert prices == ["current"]
    assert currencies == ["USD"]


def test_total_oda_official_def():
    df = total_oda_official_definition(years=[2017, 2018, 2021], donors=[4, 12])

    assert df.query("year==2017").indicator.values[0] == "total_oda_flow_net"
    assert df.query("year==2021").indicator.values[0] == "total_oda_ge"
    assert df.query("year==2018").indicator.values[0] == "total_oda_ge"


def test_oda_non_core_linked():
    df = one_non_core_oda_ge_linked(years=[2017, 2018, 2021], donors=[4, 12])

    # Check that only 1 indicator is returned
    assert df.indicator.nunique() == 1


def test_one_core_oda_flow():
    df = one_core_oda_flow(years=[2017, 2018, 2021], donors=[4, 12])

    # Check that data for all years is returned
    assert df.year.nunique() == 3


def test_one_core_oda_ge():
    df = one_core_oda_ge(years=[2014, 2017, 2018, 2021], donors=[4, 12])

    # Check that no data is provided for 2014 or 2017
    assert df.year.nunique() == 2


def test_one_core_oda_ge_linked():
    df = one_core_oda_ge_linked(years=[2014, 2017, 2018, 2021], donors=[4, 12])

    # Check that no data is provided for 2014 or 2017
    assert df.year.nunique() == 2

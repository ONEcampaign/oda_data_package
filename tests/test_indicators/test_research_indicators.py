from oda_data.indicators.research_indicators import (
    multilateral_imputed_flows,
    total_bi_multi_flows,
)

from oda_data import set_data_path, config

set_data_path(config.OdaPATHS.test_files)


def test_multilateral_imputed_flows():
    # Test that the function runs without error
    df = multilateral_imputed_flows(
        years=range(2012, 2021),
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
    donors = list(df.donor_code.unique())
    recipients = list(df.recipient_code.unique())

    assert years == list(range(2014, 2021))

    df2 = multilateral_imputed_flows(
        years=range(2012, 2021),
        currency="USD",
        prices="current",
        donors=[4, 12],
        recipients=[189, 224],
        base_year=None,
    )

    prices2 = list(df2.prices.unique())
    currencies2 = list(df2.currency.unique())
    donors2 = list(df2.donor_code.unique())
    recipients2 = list(df2.recipient_code.unique())

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
    donors = list(df.donor_code.unique())
    recipients = list(df.recipient_code.unique())

    assert years == list(range(2014, 2021))
    assert donors == [4, 12]
    assert recipients == [189]
    assert prices == ["current"]
    assert currencies == ["USD"]

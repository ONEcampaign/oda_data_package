import pytest

from oda_data.classes.oda_data import ODAData

from oda_data import set_data_path
from oda_data import config

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
    ODAData(years=2019, recipients=[57, 63]).load_indicator("total_oda_ge")

    # log availabe indicators
    oda = ODAData(years=2019)
    oda.available_indicators()

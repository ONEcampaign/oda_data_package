from oda_data.indicators.research_indicators import multilateral_imputed_flows

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

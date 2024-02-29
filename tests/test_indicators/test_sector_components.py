import copy

import pandas as pd

from oda_data import ODAData
from oda_data.clean_data.schema import OdaSchema
from oda_data.indicators import sector_components as sc

TEST_DF = pd.DataFrame(
    {
        OdaSchema.PROVIDER_CODE: [
            807,
            807,
            807,
            807,
            909,
            909,
            909,
            909,
            913,
            913,
            913,
            913,
        ],
        OdaSchema.CHANNEL_CODE: [1, 1, 1, 1, 1, 2, 1, 2, 2, 2, 2, 2],
        OdaSchema.YEAR: [
            2016,
            2017,
            2018,
            2019,
            2016,
            2017,
            2018,
            2019,
            2016,
            2017,
            2018,
            2019,
        ],
        OdaSchema.SECTOR_NAME: [20, 20, 20, 20, 20, 30, 30, 20, 30, 30, 30, 30],
        OdaSchema.VALUE: [
            200,
            100,
            100,
            100,
            200,
            pd.NA,
            100,
            100,
            200,
            100,
            100,
            pd.NA,
        ],
        OdaSchema.CURRENCY: [
            "USD",
            "USD",
            "USD",
            "USD",
            "USD",
            "USD",
            "USD",
            "USD",
            "USD",
            "USD",
            "USD",
            "USD",
        ],
        OdaSchema.PRICES: [
            "current",
            "current",
            "current",
            "current",
            "current",
            "current",
            "current",
            "current",
            "current",
            "current",
            "current",
            "current",
        ],
    }
)


def test_read_channel_codes():
    codes = sc.read_channel_codes()

    key_types = set([type(k) for k in codes.keys()])
    value_types = set([type(v) for v in codes.values()])

    assert isinstance(codes, dict)
    assert len(key_types) == 1
    assert len(value_types) == 1
    assert str in key_types
    assert int in value_types


def test__get_indicator():
    indicator = "multisystem_multilateral_contributions_disbursement_gross"
    cols = [
        OdaSchema.PROVIDER_CODE,
        OdaSchema.CHANNEL_CODE,
        OdaSchema.YEAR,
        OdaSchema.CURRENCY,
        OdaSchema.PRICES,
    ]

    original = ODAData(2020)
    original_copy = copy.deepcopy(original)

    result = sc._get_indicator(data=original, indicator=indicator, columns=cols)

    # Check that object hasn't been modified in the process
    assert original == original_copy

    # Check that the result columns are indeed the requested ones, plus 'value'
    assert all([c in result.columns for c in cols + ["value"]])

    test = original.load_indicator(indicator).get_data()

    assert len(test) > len(result)


def test_bilat_outflows_by_donor():
    oda = ODAData([2019, 2020], donors=[4, 12])

    result = sc.bilat_outflows_by_donor(oda)

    assert OdaSchema.PURPOSE_CODE in result.columns
    assert 4 in result[OdaSchema.PROVIDER_CODE].unique()
    assert 12 in result[OdaSchema.PROVIDER_CODE].unique()
    assert 2019 in result[OdaSchema.YEAR].unique()
    assert 2020 in result[OdaSchema.YEAR].unique()

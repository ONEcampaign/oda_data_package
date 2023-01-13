import copy

import pandas as pd

from oda_data import ODAData, config
from oda_data.indicators import sector_components as sc

TEST_DF = pd.DataFrame(
    {
        "donor_code": [807, 807, 807, 807, 909, 909, 909, 909, 913, 913, 913, 913],
        "agency_code": [1, 1, 1, 1, 1, 2, 1, 2, 2, 2, 2, 2],
        "year": [
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
        "sector": [20, 20, 20, 20, 20, 30, 30, 20, 30, 30, 30, 30],
        "value": [200, 100, 100, 100, 200, pd.NA, 100, 100, 200, 100, 100, pd.NA],
        "currency": [
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
        "prices": [
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


def test_add_multi_channel_ids():
    test_df = pd.DataFrame(
        {
            "donor_code": [807, 909, 913, 10],
            "agency_code": [1, 2, 1, 99],
            "year": [2017, 2018, 2019, 2020],
            "value": [1, 2, 3, 4],
        }
    )

    result = sc.add_multi_channel_ids(test_df)

    assert sum(result.channel_code.isna()) == 1
    assert pd.api.types.is_dtype_equal(result.channel_code, pd.Int32Dtype())
    assert sum(result.ids.isna()) == 0


def test__get_indicator():

    indicator = "multisystem_multilateral_contributions_disbursement_gross"
    cols = ["donor_code", "channel_code", "year", "currency", "prices"]

    original = ODAData(2020)
    original_copy = copy.deepcopy(original)

    result = sc._get_indicator(data=original, indicator=indicator, columns=cols)

    # Check that object hasn't been modified in the process
    assert original == original_copy

    # Check that the result columns are indeed the requested ones, plus 'value'
    assert all([c in result.columns for c in cols + ["value"]])

    test = original.load_indicator(indicator).get_data()

    assert len(test) > len(result)


def test__rolling_period_total():

    result = sc._rolling_period_total(TEST_DF, 3)

    assert result.query("year == 2019 and donor_code == 807").value.sum() == 300
    assert result.query("year == 2018 and donor_code == 807").value.sum() == 400
    assert (
        result.query(
            "year == 2019 and donor_code == 909 and sector == 30 and agency_code==2"
        ).value.sum()
        == 0
    )
    assert (
        result.query("year == 2018 and donor_code == 913 and sector == 30").value.sum()
        == 400
    )

    assert result.isna().sum().sum() == 0


def test__purpose_share():
    result = (
        (TEST_DF.pipe(sc._rolling_period_total, 3))
        .query("value>0")
        .drop_duplicates(["year", "donor_code"])
    )

    result["share"] = result.pipe(sc._purpose_share)

    # test that all shares equal 1
    shares_total = (
        result.groupby(["year", "donor_code", "prices", "currency"]).share.sum().values
    )

    assert all(x == 1.0 for x in shares_total)


def test__yearly_share():

    result = (
        TEST_DF.pipe(sc._rolling_period_total, 3)
        .query("value>0")
        .drop_duplicates(["year", "donor_code"])
        .pipe(sc._yearly_share)
    )

    shares_total = (
        result.groupby(["year", "donor_code", "prices", "currency"]).share.sum().values
    )

    assert all(x == 1.0 for x in shares_total)


def test__spending_summary():
    test_name = config.OdaPATHS.test_files / "purpose_test_df.csv"
    test_df = pd.read_csv(
        test_name,
        dtype={
            "donor_code": "Int32",
            "agency_code": "Int32",
            "year": "Int32",
            "recipient_code": "Int32",
            "purpose_code": "Int32",
        },
    ).assign(share=lambda d: d.value)

    result_p = sc._spending_summary(test_df, "purpose_code")

    assert "purpose_code" in result_p.columns
    assert "value" not in result_p.columns


def test_bilat_outflows_by_donor():

    oda = ODAData([2019, 2020], donors=[4, 12])

    result = sc.bilat_outflows_by_donor(oda)

    assert "purpose_code" in result.columns
    assert 4 in result.donor_code.unique()
    assert 12 in result.donor_code.unique()
    assert 2019 in result.year.unique()
    assert 2020 in result.year.unique()

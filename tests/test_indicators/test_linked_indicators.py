import pandas as pd

from oda_data import ODAData, config
from oda_data.indicators.linked_indicators import linked_indicator


def test_linked_indicator():
    # load component indicators

    oda = ODAData([2014, 2017, 2020])
    oda.load_indicator("total_in_donor_students_ge")
    oda.load_indicator("total_in_donor_students_flow")

    test_data = oda.get_data()

    ge_linked_result = pd.read_csv(
        config.OdaPATHS.test_files / "test_ge_linked_students.csv"
    ).filter(
        [
            "donor_code",
            "donor_name",
            "year",
            "currency",
            "prices",
            "indicator",
            "value",
        ],
        axis=1,
    )

    test = linked_indicator(
        data=test_data,
        main_indicator="total_in_donor_students_ge",
        fallback_indicator="total_in_donor_students_flow",
        indicator_name="total_in_donor_students_ge_linked",
    )

    assert len(test) == len(ge_linked_result)

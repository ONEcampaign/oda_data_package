from oda_data import set_data_path, config
from oda_data.clean_data.schema import OdaSchema
from oda_data.read_data import read

set_data_path(config.OdaPATHS.test_files)


def test_read_crs():
    # load the data
    crs = read.read_crs(2019)

    # check that the right year is loaded
    assert crs[OdaSchema.YEAR].dropna().unique() == [2019]

    # Check that agency is a column (only available in crs)
    assert OdaSchema.AGENCY_CODE in crs.columns

    # load multiple years
    crs = read.read_crs([2012, 2019])

    # check that the right years are loaded
    assert crs[OdaSchema.YEAR].dropna().unique() == [2012, 2019]


def test_read_dac1():
    dac1 = read.read_dac1(2019)

    assert dac1.year.dropna().unique() == [2019]

    # Check that agency is not a column (not available in dac1)
    assert OdaSchema.AGENCY_CODE not in dac1.columns


def test_read_dac2a():
    dac2a = read.read_dac2a(2019)

    assert dac2a.year.dropna().unique() == [2019]

    # Check that recipient is a column
    assert OdaSchema.RECIPIENT_CODE in dac2a.columns


def test_read_multisystem():
    multisystem = read.read_multisystem(2019)

    assert multisystem.year.dropna().unique() == [2019]

    # check aid_to_thru is a column
    assert "aid_to_or_thru" in multisystem.columns

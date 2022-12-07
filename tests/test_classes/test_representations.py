from oda_data.classes.representations import _OdaList, _OdaDict


def test_odalist_repr():
    # Create a list with some sample data
    data = [1, 2, 3, 4]

    # Create an instance of the _OdaList class
    oda_list = _OdaList(data)

    # Verify that the __repr__ method returns a string with each element on a new line
    assert oda_list.__repr__() == "[\n1,\n2,\n3,\n4\n]"


def test_odadict_repr():
    # Create a dictionary with some sample data
    data = {"a": 1, "b": 2, "c": 3}

    # Create an instance of the _OdaDict class
    oda_dict = _OdaDict(data)

    # Verify that the __repr__ method returns a string with each key-value pair on a new line
    assert oda_dict.__repr__() == "{\na: 1,\nb: 2,\nc: 3\n}"

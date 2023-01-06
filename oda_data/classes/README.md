# Classes

This directory contains the classes that are used by the package, or that are available for the user to interact
with the data.

## [oda_data.py](oda_data.py)

This file contains the `ODAData` class, which is the main class of the package. It is used to interact with the
data, and to get different indicators as pandas DataFrames.

This file also contains a few settings on the available currencies and 'readers', which are used to read specific
raw files.

## [representations.py](representations.py)

This file contains helper classes that are used to represent the data in a more convenient way. For example, the
`_OdaDict` class is used to represent the data as a dictionary, which when printed or shown in the console, will add
line breaks to make it more readable.
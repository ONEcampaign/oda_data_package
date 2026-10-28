"""A consumer snippet that `ty check` must accept: lazy exports keep their types.

The names behind ``oda_data.__getattr__`` are imported under ``TYPE_CHECKING``
in ``oda_data/__init__.py``. Without that block a checker types each of them
as ``object``, and every line below is an error. The file is never imported by
pytest; it is checked statically.
"""

from typing import assert_type

import oda_data as oda


def consumer(context: oda.Context, unit: oda.Unit) -> None:
    assert_type(oda.current("USD"), oda.Unit)
    assert_type(oda.constant("USD", 2023), oda.Unit)
    assert_type(unit, oda.Unit)
    assert_type(context, oda.Context)
    assert_type(oda.Context(), oda.Context)
    assert_type(oda.query("oecd.dac1.total_oda"), oda.Query)
    assert_type(context.query("oecd.dac1.total_oda"), oda.Query)
    assert_type(oda.describe("oecd.dac1.total_oda"), oda.IndicatorInfo)
    assert_type(oda.total(oda.groups["dac"]), oda.GroupTotal)
    # ``get`` returns a pandas frame, which the repo's ty config types as Any.
    oda.get("oecd.dac1.total_oda", years=2023, unit=oda.current("USD"))
    assert_type(oda.provenance("table.parquet"), oda.Provenance)
    assert issubclass(oda.UnknownIndicatorError, oda.OdaDataError)

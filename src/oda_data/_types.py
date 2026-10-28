"""Type aliases shared by the public signatures.

Nothing here imports pandas, DuckDB, or resolvekit at run time, so importing
the package stays cheap. The aliases that name classes defined elsewhere
resolve lazily.
"""

from collections.abc import Iterable
from typing import TYPE_CHECKING, Literal

from oda_data._ids import IndicatorId  # noqa: F401 (re-exported for the signatures)

if TYPE_CHECKING:
    from oda_data._groups import Group, GroupTotal

type Years = int | Iterable[int]
#: One value or an iterable of values. A bare ``str`` is one value.
type Many[T] = T | Iterable[T]

#: A DAC code, a three-letter code, an exact name, a group, or a group total.
type Entity = int | str | Group | GroupTotal

type Basis = Literal[
    "headline",
    "net_disbursement",
    "gross_disbursement",
    "commitment",
    "grant_equivalent",
]

type Dimension = Literal[
    "provider",
    "recipient",
    "year",
    "sector",
    "broad_sector",
    "channel",
    "modality",
    "finance_type",
    "flow",
    "line",
    "score",
    "allocation_status",
]

#: An OECD table. Each table's DuckDB view carries this name.
type TableName = Literal["dac1", "dac2a", "crs", "multisystem"]

#: A code as a table holds it: an int, or a string such as a CRS modality
#: (``"B01"``) or a DAC1 amount type (``"A"``).
type Code = int | str

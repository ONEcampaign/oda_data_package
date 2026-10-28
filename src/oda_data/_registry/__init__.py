"""Every indicator oda-data ships, by id.

Each module holds the indicators of one source in an ``INDICATORS`` tuple.
``INDICATORS`` here maps each id to its indicator, in module order.
``_context.find_indicator`` reads it, and ``scripts/generate_ids.py`` writes
``oda_data._ids`` from its keys. Building it checks each ``Ratio`` and
``Derived`` indicator against the leaves it reads (``_validate``).
"""

from collections.abc import Mapping
from types import MappingProxyType

from oda_data._model import Indicator
from oda_data._registry import crs, dac1, dac2a, multisystem, one
from oda_data._registry._validate import validate


def _by_id(*modules: tuple[Indicator, ...]) -> Mapping[str, Indicator]:
    found: dict[str, Indicator] = {}
    for indicators in modules:
        for indicator in indicators:
            if indicator.id in found:
                raise ValueError(f"two indicators have the id {indicator.id!r}")
            validate(indicator)
            found[indicator.id] = indicator
    return MappingProxyType(found)


INDICATORS: Mapping[str, Indicator] = _by_id(
    dac1.INDICATORS,
    dac2a.INDICATORS,
    crs.INDICATORS,
    multisystem.INDICATORS,
    one.INDICATORS,
)

"""The registry's check of ``Ratio`` and ``Derived`` indicators against their leaves.

Each case builds a small composite from shipped leaves that breaks one rule:
a dimension a leaf lacks, a basis a leaf lacks, a headline that switches in
another year, a ratio denominator that offers some bases but not all, and
leaves that read one dimension from columns holding different codes.
"""

from dataclasses import replace

import pytest

from oda_data import _registry
from oda_data._model import Derived, Headline, Indicator, Ratio, Sum
from oda_data._registry import crs, dac1, dac2a, multisystem
from oda_data._registry._validate import validate
from oda_data._types import Dimension


def _derived(
    *parts: Indicator, binding: Indicator, dimensions: tuple[Dimension, ...]
) -> Indicator:
    """A ``Derived`` over ``parts``, on the binding and default basis of
    ``binding``."""
    return Indicator(
        id="test.composite",
        name="Test composite",
        description="A composite built to break one rule.",
        binding=binding.binding,
        kind=Derived(plus=parts),
        dimensions=dimensions,
        default_basis=binding.default_basis,
        default_providers=None,
    )


_HEADLINE_2015 = replace(
    dac1.TOTAL_ODA,
    id="test.total_oda_headline_2015",
    binding=replace(
        dac1.DAC1,
        bases={
            **dac1.DAC1.bases,
            "headline": Headline("net_disbursement", "grant_equivalent", year=2015),
        },
    ),
)

#: GNI on two bases: neither basis-free nor on every basis of total ODA.
_GNI_TWO_BASES = replace(
    dac1.GNI,
    id="test.gni_two_bases",
    kind=Sum(lines={"net_disbursement": 1, "gross_disbursement": 1}),
)

_RATIO = Indicator(
    id="test.composite",
    name="Test ratio",
    description="A ratio whose denominator offers some bases but not all.",
    binding=dac1.DAC1,
    kind=Ratio(numerator=dac1.TOTAL_ODA, denominator=_GNI_TWO_BASES),
    dimensions=("provider", "year"),
    default_basis="headline",
    default_providers=None,
)

_GENDER, _DESERTIFICATION = crs.MARKERS[0], crs.MARKERS[-1]


@pytest.mark.parametrize(
    ("composite", "leaf"),
    [
        pytest.param(
            _derived(
                crs.BILATERAL_ODA,
                multisystem.CORE_CONTRIBUTIONS,
                binding=multisystem.CORE_CONTRIBUTIONS,
                dimensions=("provider", "recipient", "year"),
            ),
            multisystem.CORE_CONTRIBUTIONS,
            id="dimension",
        ),
        pytest.param(
            _derived(
                crs.BILATERAL_ODA,
                multisystem.CORE_CONTRIBUTIONS,
                binding=crs.BILATERAL_ODA,
                dimensions=("provider", "year"),
            ),
            multisystem.CORE_CONTRIBUTIONS,
            id="basis",
        ),
        pytest.param(
            _derived(
                dac1.TOTAL_ODA,
                _HEADLINE_2015,
                binding=dac1.TOTAL_ODA,
                dimensions=("provider", "year"),
            ),
            _HEADLINE_2015,
            id="headline-year",
        ),
        pytest.param(_RATIO, _GNI_TWO_BASES, id="denominator-bases"),
        pytest.param(
            _derived(
                dac2a.LINES,
                dac1.LINES,
                binding=dac2a.LINES,
                dimensions=("provider", "year", "line"),
            ),
            dac1.LINES,
            id="line-codes-per-table",
        ),
        pytest.param(
            _derived(
                _GENDER,
                _DESERTIFICATION,
                binding=_GENDER,
                dimensions=("provider", "year", "score"),
            ),
            _DESERTIFICATION,
            id="score-labels",
        ),
    ],
)
def test_composite_that_disagrees_with_a_leaf_raises(
    composite: Indicator, leaf: Indicator
) -> None:
    with pytest.raises(ValueError, match=f"^{composite.id}: {leaf.id} "):
        validate(composite)


def test_shipped_composites_pass() -> None:
    composites = [
        indicator
        for indicator in _registry.INDICATORS.values()
        if isinstance(indicator.kind, Ratio | Derived)
    ]
    assert composites
    for indicator in composites:
        validate(indicator)

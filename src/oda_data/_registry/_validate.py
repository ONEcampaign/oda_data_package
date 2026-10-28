"""Checks that a ``Ratio`` or ``Derived`` indicator agrees with the leaves it reads.

A composite passes its call to each leaf, the ``Sum`` and ``Method``
indicators under it, so each leaf must take what the composite accepts:

- Dimensions. Every dimension of the composite is a dimension of every leaf.
- Bases. Every basis the composite's binding offers is offered by every leaf,
  with the same headline switch. A ratio's denominator that offers a single
  basis is basis-free, such as GNI on net flows, and is read on that basis
  whatever the call asks (``Ratio``).
- Code domains. Leaves read each of the composite's dimensions from columns
  that hold the same codes, with the same column type, labels and ``domain``.
  Tables number some dimensions their own way (DAC1 flow 1140 is net
  disbursements, CRS flow 11 is ODA grants).

``oda_data._registry`` runs ``validate`` on every indicator it lists.
"""

from collections.abc import Iterator, Mapping

from oda_data._model import (
    VOCABULARY,
    Derived,
    Headline,
    Indicator,
    Ratio,
    offered_bases,
)
from oda_data._types import Code, Dimension

type _Domain = tuple[str, Mapping[Code | None, str] | None, str | None]


def validate(indicator: Indicator, /) -> None:
    """Check that a ``Ratio`` or ``Derived`` indicator agrees with its leaves.

    Any other kind passes.

    Raises:
        ValueError: A leaf lacks one of the composite's dimensions or bases,
            reads a headline that switches in another year, or reads a
            dimension from a column whose codes differ from another leaf's.
            The message names the composite, the leaf and the mismatch.
    """
    if not isinstance(indicator.kind, Ratio | Derived):
        return
    bases = indicator.binding.bases
    domains: dict[Dimension, tuple[Indicator, _Domain]] = {}
    for leaf, basis_free in _leaves(indicator, basis_free=False):
        missing = [d for d in indicator.dimensions if d not in leaf.dimensions]
        if missing:
            raise ValueError(f"{indicator.id}: {leaf.id} lacks dimensions {missing}")

        offered = offered_bases(leaf)
        if not (basis_free and len(offered) == 1):
            lacking = [basis for basis in bases if basis not in offered]
            if lacking:
                raise ValueError(f"{indicator.id}: {leaf.id} lacks bases {lacking}")
            for basis, rule in bases.items():
                own = leaf.binding.bases[basis]
                if isinstance(rule, Headline) and own != rule:
                    raise ValueError(
                        f"{indicator.id}: {leaf.id} reads basis {basis!r} as "
                        f"{own}, not {rule}"
                    )

        for dimension in indicator.dimensions:
            domain = _domain(leaf, dimension)
            first, seen = domains.setdefault(dimension, (leaf, domain))
            if seen != domain:
                raise ValueError(
                    f"{indicator.id}: {leaf.id} and {first.id} hold different "
                    f"codes in {dimension!r}"
                )


def _leaves(
    indicator: Indicator, *, basis_free: bool
) -> Iterator[tuple[Indicator, bool]]:
    """Yield each ``Sum`` and ``Method`` under the indicator, and whether it
    sits under a ratio's denominator, which may be basis-free."""
    match indicator.kind:
        case Ratio(numerator=numerator, denominator=denominator):
            yield from _leaves(numerator, basis_free=basis_free)
            yield from _leaves(denominator, basis_free=True)
        case Derived(plus=plus, minus=minus):
            for part in (*plus, *minus):
                yield from _leaves(part, basis_free=basis_free)
        case _:
            yield indicator, basis_free


def _domain(leaf: Indicator, dimension: Dimension) -> _Domain:
    """The codes a leaf's dimension holds: its column's type, labels and code
    domain. A dimension a ``Method`` declares beyond its binding's holds codes
    of its own."""
    name = leaf.binding.dimensions.get(dimension)
    if name is None:
        return (f"method {leaf.id}", None, None)
    column = next(c for c in VOCABULARY[leaf.binding.table].columns if c.name == name)
    return (column.type, column.labels, column.domain)

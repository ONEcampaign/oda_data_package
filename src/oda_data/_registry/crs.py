"""Indicators from the CRS, the OECD's activity-by-activity table.

Each CRS row is one activity a provider reports for one year. Which rows count:

- ODA is category 10 on the cash bases (gross and net disbursements,
  commitments) and categories 10 and 60 on grant equivalents. From 2023 the
  OECD reports every private sector instrument in category 60 and gives a
  grant equivalent only to those it counts as ODA, so their cash flows would
  overstate ODA while their grant equivalents belong in it.
- Bilateral flows are ``bi_multi`` 1 (bilateral, funds earmarked through
  multilateral organisations included), 3 (core contributions to NGOs, other
  private bodies, PPPs and research institutes), 7 (ex-post reporting on NGOs'
  activities funded through core contributions) and 8 (triangular
  co-operation). Code 2 is providers' core contributions to multilaterals,
  which DAC1 counts as multilateral ODA, and code 4 is the multilateral
  organisations' own outflows.
- Other official flows are category 21, and private development finance,
  the flows of private foundations, is category 30 without ``bi_multi`` 2,
  their core contributions to multilaterals. The CRS gives neither a grant
  equivalent, so both have the cash bases only.
- Net disbursements are disbursements minus amounts received.

On these rules, category 10 gross disbursements match DAC1 bilateral gross
disbursements, and categories 10 and 60 bring 2023 grant equivalents close to
DAC1 line 11015.
"""

from collections.abc import Mapping
from dataclasses import replace

from oda_data._model import (
    CRS_MARKERS,
    AnyOf,
    Binding,
    Caveat,
    Filter,
    In,
    Indicator,
    IsNull,
    NotIn,
    Sum,
    Value,
)
from oda_data._types import Basis, Dimension

#: CRS rows that are ODA on cash bases.
ODA_ROWS: tuple[Filter, ...] = (In("category", (10,)),)
#: CRS rows that are ODA on grant equivalents. Private sector instruments
#: (category 60) carry a grant equivalent only where it counts as ODA.
ODA_ROWS_WITH_PSI: tuple[Filter, ...] = (In("category", (10, 60)),)
#: CRS rows that are other official flows.
OOF_ROWS: tuple[Filter, ...] = (In("category", (21,)),)
#: CRS rows that are private development finance, the flows of private
#: foundations (mostly ``bi_multi`` 6).
PRIVATE_ROWS: tuple[Filter, ...] = (In("category", (30,)),)
#: Every flow except core contributions to multilateral organisations
#: (``bi_multi`` 2), which the organisations' own outflows count again.
#: Rows with no ``bi_multi`` stay in.
NOT_MULTILATERAL_CORE = AnyOf((IsNull("bi_multi"), NotIn("bi_multi", (2,))))

#: Flows a provider directs itself: bilateral (1), core contributions to NGOs
#: and other private bodies (3), ex-post reporting on NGOs' activities funded
#: through core contributions (7), and triangular co-operation (8).
BILATERAL_FLOWS = In("bi_multi", (1, 3, 7, 8))
#: Multilateral organisations' own outflows.
MULTILATERAL_OUTFLOWS = In("bi_multi", (4,))

#: Modalities of bilateral allocable aid, the activities the OECD screens
#: against policy markers. Budget support (A01), core contributions (B02),
#: debt relief (F01), administrative costs (G01), awareness (H01) and refugees
#: in the provider's country (H02) are not screened.
SCREENED_MODALITIES = In(
    "modality",
    ("A02", "B01", "B03", "B031", "B032", "B033", "B04", "C01", "D01", "D02", "E01"),
)

_DIMENSIONS: Mapping[Dimension, str] = {
    "provider": "provider",
    "recipient": "recipient",
    "sector": "sector",
    "broad_sector": "sector",
    "channel": "channel",
    "modality": "modality",
    "flow": "flow",
    "finance_type": "finance_type",
    "year": "year",
}
_INDICATOR_DIMENSIONS: tuple[Dimension, ...] = tuple(_DIMENSIONS)


def _cash_bases(rows: tuple[Filter, ...]) -> dict[Basis, Value]:
    return {
        "gross_disbursement": Value("disbursement", rows=rows),
        "commitment": Value("commitment", rows=rows),
        "net_disbursement": Value("disbursement", minus="received", rows=rows),
    }


#: The bases of the CRS ODA indicators. Grant equivalents start in 2018, the
#: first year every DAC member reports them.
_ODA_BASES: dict[Basis, Value] = {
    **_cash_bases(ODA_ROWS),
    "grant_equivalent": Value(
        "grant_equivalent", rows=ODA_ROWS_WITH_PSI, from_year=2018
    ),
}

#: The binding of the CRS ODA indicators. A provider reports a CRS year whole,
#: so no binding reports by a dimension, and a recipient, sector or channel
#: with no row in a reporting provider's year counts as zero.
CRS = Binding(table="crs", dimensions=_DIMENSIONS, bases=_ODA_BASES, reporting=())

#: The binding of other official flows, which have no grant equivalent.
CRS_OOF = Binding(table="crs", dimensions=_DIMENSIONS, bases=_cash_bases(OOF_ROWS))

#: The binding of private development finance. The CRS gives it no grant
#: equivalent in any year, so it has the cash bases only.
CRS_PRIVATE = Binding(
    table="crs", dimensions=_DIMENSIONS, bases=_cash_bases(PRIVATE_ROWS)
)

#: The binding of multilateral organisations' outflows (``bi_multi`` 4). The
#: CRS gives them no grant equivalent in any year, so they have the cash bases
#: only.
CRS_MULTILATERAL = Binding(
    table="crs", dimensions=_DIMENSIONS, bases=_cash_bases(ODA_ROWS)
)

BILATERAL_ODA = Indicator(
    id="oecd.crs.bilateral_oda",
    name="Bilateral ODA",
    description="Bilateral ODA reported activity by activity. It is category 10 "
    "(categories 10 and 60 on grant equivalents) with bi_multi 1, 3, 7 or 8.",
    binding=CRS,
    kind=Sum(),
    dimensions=_INDICATOR_DIMENSIONS,
    default_basis="gross_disbursement",
    default_providers=None,
    rows=(BILATERAL_FLOWS,),
    caveats=(
        Caveat(
            "CRS totals differ from DAC1 totals. Use oecd.dac1.bilateral_oda "
            "for headline figures"
        ),
    ),
)

MULTILATERAL_ODA = Indicator(
    id="oecd.crs.multilateral_oda",
    name="Multilateral ODA",
    description="ODA multilateral organisations give from their own resources, "
    "reported activity by activity. It is category 10 with bi_multi 4, on cash "
    "bases only, since the CRS gives these flows no grant equivalent. The providers "
    "are the organisations, except EU Institutions (918), which report as "
    "bilateral (bi_multi 1), so their outflows (USD 40.0bn in 2023) sit in "
    "oecd.crs.bilateral_oda. Members' core contributions to the organisations "
    "are oecd.dac1.multilateral_oda.",
    binding=CRS_MULTILATERAL,
    kind=Sum(),
    dimensions=_INDICATOR_DIMENSIONS,
    default_basis="gross_disbursement",
    default_providers=None,
    rows=(MULTILATERAL_OUTFLOWS,),
)

BILATERAL_OOF = Indicator(
    id="oecd.crs.bilateral_oof",
    name="Bilateral other official flows",
    description="Official flows that are not ODA, such as loans on market "
    "terms, reported activity by activity. They are category 21 with bi_multi "
    "1, 3, 7 or 8 and have no grant equivalent.",
    binding=CRS_OOF,
    kind=Sum(),
    dimensions=_INDICATOR_DIMENSIONS,
    default_basis="gross_disbursement",
    default_providers=None,
    rows=(BILATERAL_FLOWS,),
)

MULTILATERAL_OOF = Indicator(
    id="oecd.crs.multilateral_oof",
    name="Multilateral other official flows",
    description="Other official flows multilateral organisations give from "
    "their own resources, such as loans on market terms, reported activity by "
    "activity. They are category 21 with bi_multi 4 and have no grant "
    "equivalent. The providers are the organisations.",
    binding=CRS_OOF,
    kind=Sum(),
    dimensions=_INDICATOR_DIMENSIONS,
    default_basis="gross_disbursement",
    default_providers=None,
    rows=(MULTILATERAL_OUTFLOWS,),
)

PRIVATE_DEVELOPMENT_FINANCE = Indicator(
    id="oecd.crs.private_development_finance",
    name="Private development finance",
    description="Development finance from private foundations, reported "
    "activity by activity. It is CRS category 30, mostly bi_multi 6, and has "
    "no grant equivalent. The foundations' core contributions to multilateral "
    "organisations (bi_multi 2) are left out, since the organisations' own "
    "outflows count that money again. By default the providers are "
    "the private foundations in oda.groups.private_donors; a few official "
    "providers report category 30 rows of their own country's private flows, "
    "which a call that names them reads.",
    binding=CRS_PRIVATE,
    kind=Sum(),
    dimensions=_INDICATOR_DIMENSIONS,
    default_basis="gross_disbursement",
    default_providers="private_donors",
    rows=(NOT_MULTILATERAL_CORE,),
)

#: The first year of the marker indicators. Before 2010 the CRS leaves the aid
#: type of bilateral ODA blank (31% to 53% of it by value in 2006 to 2009, and
#: 97% or more before), so the screened-modality filter would drop those
#: activities. From 2010 it is filled on at least 99.7% of rows.
MARKERS_FROM = 2010

#: The marker indicators use the ODA bases from ``MARKERS_FROM``.
_MARKER_BASES: dict[Basis, Value] = {
    name: replace(rule, from_year=max(rule.from_year or 0, MARKERS_FROM))
    for name, rule in _ODA_BASES.items()
}

#: What each policy marker column tracks.
_MARKER_TOPICS: Mapping[str, str] = {
    "gender": "gender equality",
    "environment": "environment",
    "dig": "participatory development and good governance",
    "trade": "trade development",
    "rmnch": "reproductive, maternal, newborn and child health",
    "drr": "disaster risk reduction",
    "nutrition": "nutrition",
    "disability": "inclusion of persons with disabilities",
    "biodiversity": "biodiversity",
    "climate_mitigation": "climate change mitigation",
    "climate_adaptation": "climate change adaptation",
    "desertification": "desertification",
}


#: The search terms of the markers people ask about most by another name.
_MARKER_SEARCH_TERMS: Mapping[str, tuple[str, ...]] = {
    "gender": ("gender", "gender equality", "women"),
    "climate_mitigation": ("climate mitigation", "climate finance"),
    "climate_adaptation": ("climate adaptation", "climate finance"),
}


def _marker(column: str) -> Indicator:
    """The indicator of one policy marker, whose ``score`` dimension reads
    ``column``."""
    topic = _MARKER_TOPICS[column]
    return Indicator(
        id=f"oecd.crs.{column}_marker",
        name=f"Bilateral allocable ODA by {topic} marker",
        description=f"Bilateral allocable ODA by its {topic} policy marker "
        "score: principal, significant, not_targeted or not_screened. Group by "
        "score to split it. Summed over scores it is all bilateral allocable "
        f"ODA. It starts in {MARKERS_FROM}, the first year the CRS records the "
        "aid type of nearly every activity.",
        binding=Binding(
            table="crs",
            dimensions={**_DIMENSIONS, "score": column},
            bases=_MARKER_BASES,
        ),
        kind=Sum(),
        dimensions=(*_INDICATOR_DIMENSIONS, "score"),
        default_basis="gross_disbursement",
        default_providers=None,
        rows=(BILATERAL_FLOWS, SCREENED_MODALITIES),
        search_terms=_MARKER_SEARCH_TERMS.get(column, ()),
    )


#: One indicator per policy marker, in ``CRS_MARKERS`` order.
MARKERS: tuple[Indicator, ...] = tuple(map(_marker, CRS_MARKERS))

INDICATORS: tuple[Indicator, ...] = (
    BILATERAL_ODA,
    MULTILATERAL_ODA,
    BILATERAL_OOF,
    MULTILATERAL_OOF,
    PRIVATE_DEVELOPMENT_FINANCE,
    *MARKERS,
)

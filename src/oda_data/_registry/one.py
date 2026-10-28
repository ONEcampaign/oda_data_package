"""Indicators from ONE's own definitions.

Each builds on the OECD indicators in ``dac1``, ``dac2a`` and ``crs``:

- ``one.dac1.core_oda`` and ``one.dac1.non_core_oda`` split total ODA into
  the money that can reach developing countries and the money spent in the
  provider's own country or on debt.
- ``one.dac2a.recipient_oda_with_imputed_multilateral`` adds each provider's
  imputed share of multilateral outflows to its own flows to a recipient.
- ``one.dac1.total_oda_eu_attributed`` lets EU Institutions (918) be summed
  with the providers that fund them.
- ``one.dac1.gni`` and ``one.dac1.oda_gni_ratio`` give 918 the GNI of the
  countries that were EU members in each year, so 918 has an ODA/GNI ratio.
  ``one.dac1.gni_fixed_eu27`` and ``one.dac1.oda_gni_ratio_fixed_eu27`` use
  today's EU27 in every year.
- ``one.crs.covid_oda`` is bilateral ODA the OECD tags as COVID-19 response.
- ``one.crs.bilateral_plus_multilateral_oda`` adds providers' core
  contributions to multilateral organisations (Multisystem) to their bilateral
  ODA (CRS), activity-level data on both sides.
"""

from dataclasses import replace

from oda_data._methods.eu import EuAttribution, EuGni
from oda_data._model import (
    Caveat,
    Derived,
    Flow,
    HasTag,
    Indicator,
    Method,
    Ratio,
    Sum,
)
from oda_data._registry import crs, dac1, dac2a, multisystem
from oda_data._types import Dimension

_DAC1_DIMENSIONS: tuple[Dimension, ...] = ("provider", "year", "flow")
_GNI_DIMENSIONS: tuple[Dimension, ...] = ("provider", "year")

#: The lines core ODA leaves out: money spent in the provider's own country,
#: and debt relief.
_NON_CORE: tuple[Indicator, ...] = (
    dac1.IN_DONOR_REFUGEE_COSTS,
    dac1.IN_DONOR_STUDENT_COSTS,
    dac1.DEBT_RELIEF,
)

CORE_ODA = Indicator(
    id="one.dac1.core_oda",
    name="Core ODA",
    description="Total ODA less in-donor refugee costs (line 1820), in-donor "
    "student costs (line 1500) and debt relief (line 1600). Administrative "
    "costs stay in. Each line is read on the basis of each year, so the "
    "headline basis subtracts grant equivalents from 2018.",
    binding=dac1.DAC1,
    kind=Derived(plus=(dac1.TOTAL_ODA,), minus=_NON_CORE),
    dimensions=_DAC1_DIMENSIONS,
    default_basis="headline",
    default_providers="dac_countries",
)

NON_CORE_ODA = Indicator(
    id="one.dac1.non_core_oda",
    name="Non-core ODA",
    description="In-donor refugee costs (line 1820), in-donor student costs "
    "(line 1500) and debt relief (line 1600). This is the part of total ODA "
    "that one.dac1.core_oda leaves out.",
    binding=dac1.DAC1,
    kind=Derived(plus=_NON_CORE),
    dimensions=_DAC1_DIMENSIONS,
    default_basis="headline",
    default_providers="dac_countries",
)

RECIPIENT_ODA_WITH_IMPUTED_MULTILATERAL = Indicator(
    id="one.dac2a.recipient_oda_with_imputed_multilateral",
    name="ODA received, imputed multilateral included",
    description="Each provider's net ODA to each recipient (line 206) plus its "
    "imputed share of multilateral organisations' outflows to that recipient "
    "(line 106). Multilateral providers are left out, since the imputed shares "
    "are their own flows. Net disbursements only, the one basis of line 106.",
    binding=dac2a.IMPUTED_MULTILATERAL_ODA.binding,
    kind=Derived(plus=(dac2a.RECIPIENT_ODA, dac2a.IMPUTED_MULTILATERAL_ODA)),
    dimensions=("provider", "recipient", "year"),
    default_basis="headline",
    default_providers=None,
)

#: What a provider pays EU Institutions (918) as multilateral ODA: line 2102
#: on every basis of total ODA. ``one.dac1.total_oda_eu_attributed`` reads it.
#: The registry does not list it, and ``oecd.dac1.lines`` gives line 2102.
EU_CONTRIBUTIONS = Indicator(
    id="one.dac1.eu_institutions_contributions",
    name="Contributions to EU Institutions",
    description="Line 2102 of DAC1, multilateral ODA paid to EU Institutions.",
    binding=dac1.DAC1,
    kind=Sum(
        lines={
            "net_disbursement": 2102,
            "gross_disbursement": 2102,
            "commitment": 2102,
            "grant_equivalent": 2102,
        }
    ),
    dimensions=_GNI_DIMENSIONS,
    default_basis="headline",
    default_providers="dac_countries",
)

TOTAL_ODA_EU_ATTRIBUTED = Indicator(
    id="one.dac1.total_oda_eu_attributed",
    name="Total ODA, EU Institutions net of their funders",
    description="Total ODA, with the row of EU Institutions (918) reduced by "
    "what every other provider paid them that year (line 2102 on the same "
    "basis), so 918 adds up with any of its funders without counting that "
    "money twice. Every other provider's row is its total ODA. 918's row is "
    "the same whichever providers the call names; oecd.dac1.total_oda gives "
    "918's own spending.",
    binding=replace(dac1.DAC1, members_hold_eu_share=False),
    kind=Method(EuAttribution(dac1.TOTAL_ODA, EU_CONTRIBUTIONS), tables=("dac1",)),
    caveats=(
        Caveat(
            "918's row is negative in a year in which its funders' recorded "
            "contributions exceed the ODA EU Institutions report"
        ),
        Caveat(
            "918's row in DAC1's last year is null until the providers that "
            "paid it the year before report line 2102; allow_partial=True "
            "nets the contributions reported so far"
        ),
    ),
    dimensions=_GNI_DIMENSIONS,
    default_basis="headline",
    default_providers=None,
)

_GNI_BINDING = replace(dac1.DAC1, bases={"net_disbursement": Flow(1140)})
_GAPS = (
    "A member with no GNI in a year between two it reported counts as zero, "
    "and explain() names it. In a year after a member's last GNI, 918 has a "
    "null GNI unless allow_partial=True."
)

GNI = Indicator(
    id="one.dac1.gni",
    name="Gross national income, EU Institutions included",
    description="The provider's GNI (line 1, net flows), and for EU "
    "Institutions (918) the summed GNI of the countries that were EU members "
    "in each year, the United Kingdom to 2020 included. "
    f"{_GAPS} one.dac1.gni_fixed_eu27 gives 918 today's EU27 in every year.",
    binding=_GNI_BINDING,
    kind=Method(EuGni(dac1.GNI), tables=("dac1",)),
    dimensions=_GNI_DIMENSIONS,
    default_basis="net_disbursement",
    default_providers="dac_countries",
)

GNI_FIXED_EU27 = Indicator(
    id="one.dac1.gni_fixed_eu27",
    name="Gross national income, EU Institutions as today's EU27",
    description="The provider's GNI (line 1, net flows), and for EU "
    "Institutions (918) the summed GNI of today's 27 EU members in every year, "
    "so the United Kingdom is excluded. "
    f"{_GAPS} one.dac1.gni gives 918 the members of each year.",
    binding=_GNI_BINDING,
    kind=Method(EuGni(dac1.GNI, fixed_eu27=True), tables=("dac1",)),
    dimensions=_GNI_DIMENSIONS,
    default_basis="net_disbursement",
    default_providers="dac_countries",
)

ODA_GNI_RATIO = Indicator(
    id="one.dac1.oda_gni_ratio",
    name="ODA as a share of GNI, EU Institutions included",
    description="Total ODA as a percentage of GNI, as oecd.dac1.oda_gni_ratio, "
    "plus a ratio for EU Institutions (918) over the GNI of the countries that "
    "were EU members in each year, the United Kingdom to 2020 included "
    "(one.dac1.gni). Name 918 to see it. one.dac1.oda_gni_ratio_fixed_eu27 "
    "uses today's EU27 in every year.",
    binding=dac1.DAC1,
    kind=Ratio(numerator=dac1.TOTAL_ODA, denominator=GNI),
    dimensions=_GNI_DIMENSIONS,
    default_basis="headline",
    default_providers="dac_countries",
)

ODA_GNI_RATIO_FIXED_EU27 = Indicator(
    id="one.dac1.oda_gni_ratio_fixed_eu27",
    name="ODA as a share of GNI, EU Institutions as today's EU27",
    description="Total ODA as a percentage of GNI, as oecd.dac1.oda_gni_ratio, "
    "plus a ratio for EU Institutions (918) over the GNI of today's 27 EU "
    "members in every year (one.dac1.gni_fixed_eu27). Name 918 to see it. "
    "one.dac1.oda_gni_ratio uses the members of each year, the United Kingdom "
    "to 2020 included.",
    binding=dac1.DAC1,
    kind=Ratio(numerator=dac1.TOTAL_ODA, denominator=GNI_FIXED_EU27),
    dimensions=_GNI_DIMENSIONS,
    default_basis="headline",
    default_providers="dac_countries",
)

#: The CRS keyword the OECD asks providers to give COVID-19 activities.
COVID_KEYWORD = "#COVID-19"

COVID_ODA = replace(
    crs.BILATERAL_ODA,
    id="one.crs.covid_oda",
    name="COVID-19 ODA",
    description="Bilateral ODA whose CRS keywords include #COVID-19, the tag "
    "the OECD asks providers to give activities that respond to the pandemic. "
    "Its rows are otherwise those of oecd.crs.bilateral_oda.",
    rows=(*crs.BILATERAL_ODA.rows, HasTag("keywords", (COVID_KEYWORD,))),
    caveats=(
        *crs.BILATERAL_ODA.caveats,
        Caveat(
            "providers tag activities with #COVID-19 from 2020, and some leave "
            "the tag off activities that respond to the pandemic"
        ),
    ),
)

BILATERAL_PLUS_MULTILATERAL_ODA = Indicator(
    id="one.crs.bilateral_plus_multilateral_oda",
    name="Bilateral and multilateral ODA, activity data",
    description="Bilateral ODA from the CRS (oecd.crs.bilateral_oda) plus core "
    "contributions to multilateral organisations from the Multisystem table "
    "(oecd.multisystem.core_contributions), on gross disbursements or "
    "commitments, the bases both tables share.",
    binding=multisystem.MULTISYSTEM,
    kind=Derived(plus=(crs.BILATERAL_ODA, multisystem.CORE_CONTRIBUTIONS)),
    dimensions=_GNI_DIMENSIONS,
    default_basis="gross_disbursement",
    default_providers="dac_countries",
)

INDICATORS: tuple[Indicator, ...] = (
    CORE_ODA,
    NON_CORE_ODA,
    RECIPIENT_ODA_WITH_IMPUTED_MULTILATERAL,
    TOTAL_ODA_EU_ATTRIBUTED,
    GNI,
    GNI_FIXED_EU27,
    ODA_GNI_RATIO,
    ODA_GNI_RATIO_FIXED_EU27,
    COVID_ODA,
    BILATERAL_PLUS_MULTILATERAL_ODA,
)

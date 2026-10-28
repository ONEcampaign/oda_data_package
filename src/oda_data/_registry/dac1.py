"""Indicators read from DAC1, the OECD's official totals by provider.

DAC1 publishes each aid type (``line``) under several flows. Each basis reads
one flow:

- ``net_disbursement``: flow 1140, net flows.
- ``gross_disbursement``: flow 1120, gross disbursements.
- ``commitment``: flow 1150, commitments.
- ``grant_equivalent``: flow 1160, grant equivalents, from 2018.
- ``headline``: net flows to 2017 and grant equivalents from 2018, the OECD's
  own series.

Flows 1121 and 1122 split gross disbursements into grants and non-grants,
and 1151 and 1152 split commitments the same way. ``flows=`` reads those
parts on the gross and commitment bases, each part its own row. The parts do
not add up to their total in every row (line 1010 has gross disbursements
without parts in 69 provider-years), so they are not bases of their own and
no basis is the sum of its parts.

Every binding reads current USD rows (amount type ``A``) only. The files also
carry national-currency (``N``) and constant-price (``D``) rows of each value.
An EU member's multilateral ODA counts its contribution to EU Institutions
(918), so every binding sets ``members_hold_eu_share`` and a call that sums
918 with a member it names raises.

A provider reports DAC1's last year, a preliminary one, line by line and
flow by flow. Most report total ODA and few report in-donor refugee costs, and
in 2025 only Austria and Ireland report the grant and non-grant parts (1121
and 1122) of gross disbursements, against 47 providers in 2024. So every
binding reports by ``line`` and ``flow``.
"""

from collections.abc import Mapping

from oda_data._model import (
    Binding,
    Caveat,
    Flow,
    Headline,
    In,
    Indicator,
    NotIn,
    Ratio,
    Sum,
    line_exclusions,
)
from oda_data._types import Dimension

_CURRENT_USD = (In("amount_type", ("A",)),)
_DIMENSIONS: Mapping[Dimension, str] = {
    "provider": "provider",
    "line": "line",
    "flow": "flow",
    "year": "year",
}

_NET = Flow(1140)
_GROSS = Flow(1120, parts=(1121, 1122))
_COMMITMENT = Flow(1150, parts=(1151, 1152))
_HEADLINE = Headline("net_disbursement", "grant_equivalent", year=2018)

#: The binding of every named DAC1 indicator. Grant equivalents start in 2018,
#: the first year every DAC member reports them.
DAC1 = Binding(
    table="dac1",
    dimensions=_DIMENSIONS,
    bases={
        "headline": _HEADLINE,
        "net_disbursement": _NET,
        "gross_disbursement": _GROSS,
        "commitment": _COMMITMENT,
        "grant_equivalent": Flow(1160, from_year=2018),
    },
    rows=_CURRENT_USD,
    members_hold_eu_share=True,
    reporting=("line", "flow"),
)

#: DAC1 lines that are not amounts of money: ODA as a percentage of GNI (2 on
#: net flows, 11002 on grant equivalents), total flows as a percentage of GNI
#: (3), and population (4). Summing or converting them gives nonsense.
NON_MONEY_LINES: tuple[int, ...] = (2, 3, 4, 11002)

#: DAC1 money lines published under grant equivalents (flow 1160) alone: the
#: 11xxx grant-equivalent versions of the totals, the memo lines 1041
#: and 1045, and the multilateral ODA lines 12000, 12100 and 12110. Net flows
#: hold none of them, so a call reads them with ``basis="grant_equivalent"``.
GRANT_EQUIVALENT_ONLY_LINES: tuple[int, ...] = (
    1041,
    1045,
    11010,
    11015,
    11016,
    11017,
    11018,
    11019,
    11020,
    11021,
    11022,
    11026,
    11040,
    11041,
    11042,
    11044,
    12000,
    12100,
    12110,
)

#: The binding of ``oecd.dac1.lines``, whose grant equivalents reach back to
#: 2015, the first year DAC1 holds them, and which leaves out the lines that are
#: not money.
DAC1_LINES = Binding(
    table="dac1",
    dimensions=_DIMENSIONS,
    bases={
        "headline": _HEADLINE,
        "net_disbursement": _NET,
        "gross_disbursement": _GROSS,
        "commitment": _COMMITMENT,
        "grant_equivalent": Flow(
            1160, from_year=2015, only_lines=GRANT_EQUIVALENT_ONLY_LINES
        ),
    },
    rows=(
        *_CURRENT_USD,
        NotIn(
            "line",
            NON_MONEY_LINES,
            reason="lines 2, 3 and 11002 are percentages of GNI and line 4 is "
            "population. ODA as a share of GNI is oecd.dac1.oda_gni_ratio, and "
            "population is oecd.dac1.population",
        ),
    ),
    members_hold_eu_share=True,
    reporting=("line", "flow"),
)

_DAC_COUNTRIES = "dac_countries"
_NAMED_DIMENSIONS: tuple[Dimension, ...] = ("provider", "year", "flow")
#: GNI is one flow (1140) on every basis, so it has no flow to split by. ODA
#: over GNI has none either, since GNI does not share its numerator's flow, and
#: neither has population, also published under net flows alone.
_GNI_DIMENSIONS: tuple[Dimension, ...] = ("provider", "year")


def _same_line(line: int, *, grant_equivalent: int | None = None) -> Sum:
    """One line on the cash bases, and ``grant_equivalent`` (or the same line)
    on grant equivalents."""
    return Sum(
        lines={
            "net_disbursement": line,
            "gross_disbursement": line,
            "commitment": line,
            "grant_equivalent": line if grant_equivalent is None else grant_equivalent,
        }
    )


def _named(
    id_: str,
    name: str,
    description: str,
    kind: Sum,
    *,
    search_terms: tuple[str, ...] = (),
) -> Indicator:
    return Indicator(
        id=id_,
        name=name,
        description=description,
        binding=DAC1,
        kind=kind,
        dimensions=_NAMED_DIMENSIONS,
        default_basis="headline",
        default_providers=_DAC_COUNTRIES,
        search_terms=search_terms,
    )


TOTAL_ODA = _named(
    "oecd.dac1.total_oda",
    "Total ODA",
    "Official development assistance from each provider. Read from line 1010 "
    "on cash bases and line 11010 on grant equivalents.",
    _same_line(1010, grant_equivalent=11010),
    search_terms=("aid", "foreign aid", "total aid"),
)

BILATERAL_ODA = _named(
    "oecd.dac1.bilateral_oda",
    "Bilateral ODA",
    "ODA a provider gives to developing countries itself, funds earmarked "
    "through multilateral organisations included. Read from line 1015 on cash "
    "bases and line 11015 on grant equivalents.",
    _same_line(1015, grant_equivalent=11015),
)

MULTILATERAL_ODA = _named(
    "oecd.dac1.multilateral_oda",
    "Multilateral ODA",
    "Core contributions to multilateral organisations, capital subscriptions "
    "included. Read from line 2000 on cash bases and line 12000 on grant "
    "equivalents.",
    _same_line(2000, grant_equivalent=12000),
)

IN_DONOR_REFUGEE_COSTS = _named(
    "oecd.dac1.in_donor_refugee_costs",
    "In-donor refugee costs",
    "Spending on refugees and asylum seekers in the provider's own country "
    "during their first year (line 1820).",
    _same_line(1820),
    search_terms=("refugees", "asylum seekers"),
)

IN_DONOR_STUDENT_COSTS = _named(
    "oecd.dac1.in_donor_student_costs",
    "Scholarships and student costs in donor countries",
    "Scholarships and imputed costs of students from developing countries "
    "studying in the provider's country (line 1500).",
    _same_line(1500),
)

DEBT_RELIEF = _named(
    "oecd.dac1.debt_relief",
    "Debt relief",
    "Debt forgiveness, rescheduling and other action on debt counted as "
    "ODA (line 1600).",
    _same_line(1600),
)

ADMINISTRATIVE_COSTS = _named(
    "oecd.dac1.administrative_costs",
    "Administrative costs",
    "Administrative costs of the provider's aid programme not included "
    "elsewhere (line 1700).",
    _same_line(1700),
)

DEVELOPMENT_AWARENESS = _named(
    "oecd.dac1.development_awareness",
    "Development awareness",
    "Spending in the provider's country to raise awareness of development "
    "co-operation (line 1810).",
    _same_line(1810),
)

PRIVATE_SECTOR_INSTRUMENTS = _named(
    "oecd.dac1.private_sector_instruments",
    "Private sector instruments",
    "ODA through private sector instruments, reported from 2018 (line 11030).",
    _same_line(11030),
)

GNI = Indicator(
    id="oecd.dac1.gni",
    name="Gross national income",
    description="The provider's gross national income (line 1) under net flows, "
    "the only flow the OECD's ODA/GNI ratios use.",
    binding=DAC1,
    kind=Sum(lines={"net_disbursement": 1}),
    dimensions=_GNI_DIMENSIONS,
    default_basis="net_disbursement",
    default_providers=_DAC_COUNTRIES,
)

POPULATION = Indicator(
    id="oecd.dac1.population",
    name="Population",
    description="The provider's population in millions (line 4), published "
    "under net flows. It is a count of people, so it takes no unit=, and a "
    "group's total is its members' sum.",
    binding=DAC1,
    kind=Sum(lines={"net_disbursement": 4}),
    dimensions=_GNI_DIMENSIONS,
    default_basis="net_disbursement",
    default_providers=_DAC_COUNTRIES,
    measure="people",
)

ODA_GNI_RATIO = Indicator(
    id="oecd.dac1.oda_gni_ratio",
    name="ODA as a share of GNI",
    description="Total ODA as a percentage of GNI. A group's ratio is its "
    "members' total ODA over their total GNI, and GNI is net flows on every "
    "basis.",
    binding=DAC1,
    kind=Ratio(numerator=TOTAL_ODA, denominator=GNI),
    dimensions=_GNI_DIMENSIONS,
    default_basis="headline",
    default_providers=_DAC_COUNTRIES,
    search_terms=("aid effort", "0.7", "ODA/GNI"),
)

#: The named indicators that read one line each. ``oecd.dac1.lines`` carries
#: their row exclusions on their lines, so a line counts what its named
#: indicator counts.
_ONE_LINE: tuple[Indicator, ...] = (
    TOTAL_ODA,
    BILATERAL_ODA,
    MULTILATERAL_ODA,
    IN_DONOR_REFUGEE_COSTS,
    IN_DONOR_STUDENT_COSTS,
    DEBT_RELIEF,
    ADMINISTRATIVE_COSTS,
    DEVELOPMENT_AWARENESS,
    PRIVATE_SECTOR_INSTRUMENTS,
    GNI,
)

LINES = Indicator(
    id="oecd.dac1.lines",
    name="DAC1 lines",
    description="Any published DAC1 line that is an amount of money, named "
    "with lines=. Lines nest, so each line is its own row and no two lines are "
    "summed. Net flows by default. A line the basis's flow "
    "does not publish, such as GNI (line 1) on grant equivalents, raises and "
    "names the bases that hold it.",
    binding=DAC1_LINES,
    kind=Sum(),
    dimensions=("provider", "year", "line", "flow"),
    default_basis="net_disbursement",
    default_providers=_DAC_COUNTRIES,
    rows=line_exclusions(_ONE_LINE),
    caveats=(
        Caveat(
            "grant equivalents cover only the 25 to 27 DAC members that "
            "reported them before they became the headline basis in 2018",
            basis="grant_equivalent",
            years=(2015, 2017),
        ),
    ),
)

INDICATORS: tuple[Indicator, ...] = (
    TOTAL_ODA,
    BILATERAL_ODA,
    MULTILATERAL_ODA,
    IN_DONOR_REFUGEE_COSTS,
    IN_DONOR_STUDENT_COSTS,
    DEBT_RELIEF,
    ADMINISTRATIVE_COSTS,
    DEVELOPMENT_AWARENESS,
    PRIVATE_SECTOR_INSTRUMENTS,
    GNI,
    POPULATION,
    ODA_GNI_RATIO,
    LINES,
)

"""Indicators read from DAC2A, what each recipient receives from each provider.

DAC2A publishes each aid type as a line, and a line carries its own basis:
line 206 is net ODA disbursements and line 240 gross ODA disbursements. Each
named indicator has a binding whose bases are its lines:

- ``net_disbursement``: the indicator's line (206, 106, or 207).
- ``gross_disbursement``: line 240, on recipient ODA only.
- ``headline``: net disbursements in every year. DAC2A has no grant
  equivalents, so the OECD's recipient series is net disbursements
  throughout.

``oecd.dac2a.lines`` offers the net and gross bases and reads each line on
the basis it is published on, so a row of line 240 says gross disbursements.
Each of its lines carries the row exclusions of the named indicator on that
line, so line 106 counts what ``oecd.dac2a.imputed_multilateral_oda`` counts.

Every binding reads current USD rows (amount type ``A``) only. DAC2A holds a
constant-price row (``D``) beside each, and summing both mixes current and
constant prices.

Rows of both DAC2A parts count. Part I holds ODA to developing countries.
Part II holds official aid to the countries in transition and the more
advanced developing countries from 1990 to 2017 (Russia 1990-2004, Israel
1997-2014). A country sits in one part in a given year, so no country-year is
counted twice. Two kinds of cell appear in both parts. The OECD's recipient
totals do, and a call that names no recipient drops them. South America,
regional (489) does in 1997-2004, and its Part II rows are separate flows of
under USD 0.4 million a year.

Providers default to every official provider in the table: DAC countries,
non-DAC countries, and multilateral organisations, with aggregates (codes from
20000) and private donors left out. Recipients default to every recipient in
the table, regional and unspecified rows included, with the OECD's recipient
totals (``_names.aggregate_codes``) left out.

DAC2A also lists multilateral organisations as recipients (98, 816, 878, and
900-996). They receive a provider's core contributions, which the OECD totals as
10150 beside 10100 (developing countries). Every DAC2A indicator leaves them
out, since a multilateral provider's own row already carries that money to the
recipients it reaches, and a call that names one raises. What remains of each
provider's Part I rows adds up to its 10100 row, except Saudi Arabia's, whose
10100 row also holds ODA it reports by no recipient.
"""

from collections.abc import Mapping

from oda_data._groups import groups
from oda_data._model import (
    BasisRule,
    Binding,
    Caveat,
    Headline,
    In,
    Indicator,
    Line,
    NotIn,
    Sum,
    Value,
    line_exclusions,
)
from oda_data._types import Basis, Dimension

#: Recipient codes that are multilateral organisations, such as 905 (IDA) and
#: 959 (UNDP). DAC2A holds 816 and 900-989 in Part I and 98, 878, 990, and 996
#: in Part II (1990-2004). The range keeps a new agency code out as well.
MULTILATERAL_RECIPIENTS: tuple[int, ...] = (98, 816, 878, *range(900, 997))

#: Current USD rows (amount type ``A``) to recipients that are not
#: multilateral organisations.
_ROWS = (
    In("amount_type", ("A",)),
    NotIn(
        "recipient",
        MULTILATERAL_RECIPIENTS,
        reason="DAC2A lists multilateral organisations as recipients of core "
        "contributions, which their own rows carry on to the recipients they "
        "reach, so counting both counts that money twice",
    ),
)
_DIMENSIONS: Mapping[Dimension, str] = {
    "provider": "provider",
    "recipient": "recipient",
    "year": "year",
}
_NAMED_DIMENSIONS: tuple[Dimension, ...] = ("provider", "recipient", "year")

#: Net disbursements in every year. The switch year is DAC2A's first, so no
#: year reads the ``before`` side.
_HEADLINE = Headline("net_disbursement", "net_disbursement", year=1960)


def _binding(*, net: int, gross: int | None = None) -> Binding:
    """A DAC2A binding whose bases each read one line."""
    bases: dict[Basis, BasisRule] = {
        "headline": _HEADLINE,
        "net_disbursement": Line(net),
    }
    if gross is not None:
        bases["gross_disbursement"] = Line(gross)
    return Binding(table="dac2a", dimensions=_DIMENSIONS, bases=bases, rows=_ROWS)


def _named(
    id_: str,
    name: str,
    description: str,
    binding: Binding,
    *,
    rows: tuple[NotIn, ...] = (),
    caveats: tuple[Caveat, ...] = (),
) -> Indicator:
    return Indicator(
        id=id_,
        name=name,
        description=description,
        binding=binding,
        kind=Sum(),
        dimensions=_NAMED_DIMENSIONS,
        default_basis="headline",
        default_providers=None,
        rows=rows,
        caveats=caveats,
    )


#: The providers ``oda.groups.multilateral`` holds, EU Institutions (918)
#: included. DAC2A has no multilateral provider outside it.
MULTILATERAL_PROVIDERS: frozenset[int] = frozenset(
    code for code in groups.multilateral.members if isinstance(code, int)
)

#: Provider totals that hold multilateral organisations: 20002 (multilaterals),
#: 20004 (DAC EU countries and EU Institutions), and 20005 (official donors).
#: Their line 106 rows include EU Institutions' own imputed shares.
MULTILATERAL_AGGREGATES: tuple[int, ...] = (20002, 20004, 20005)


def _non_dac_gap(gap: str) -> str:
    """The caveat that a sum over providers falls short of the OECD's totals,
    with ``gap``, the shortfall on the indicator's own line."""
    return (
        "summed over providers, recipient totals fall short of the OECD's "
        "official donors total (provider 20005), which also holds non-DAC "
        f"estimates that no provider reports by recipient. {gap}"
    )


RECIPIENT_ODA = _named(
    "oecd.dac2a.recipient_oda",
    "ODA received",
    "ODA each recipient receives from each provider: net disbursements (line "
    "206) by default, and gross disbursements (line 240) on request.",
    _binding(net=206, gross=240),
    caveats=(
        Caveat(
            _non_dac_gap(
                "On line 206 the gap is Saudi Arabia's ODA that appears in its "
                "developing-countries total (10100) only, from USD 0.6bn to USD "
                "6.4bn a year from 2015 (USD 4.5bn in 2024). Pass providers=20005 "
                "for that total"
            )
        ),
    ),
)

IMPUTED_MULTILATERAL_ODA = _named(
    "oecd.dac2a.imputed_multilateral_oda",
    "Imputed multilateral ODA",
    "The share of multilateral organisations' outflows to each recipient that "
    "the OECD attributes to each provider's core contributions (line 106). "
    "Multilateral providers are left out, since the shares are their own "
    "flows.",
    _binding(net=106),
    rows=(
        NotIn(
            "provider",
            (*sorted(MULTILATERAL_PROVIDERS), *MULTILATERAL_AGGREGATES),
            reason="multilateral providers and the provider totals that hold "
            "them, whose imputed shares would count money that members already "
            "impute",
        ),
    ),
    caveats=(
        Caveat(
            _non_dac_gap(
                "On line 106 the gap is Saudi Arabia's imputed shares that appear "
                "in its developing-countries total (10100) only, from USD 16m to "
                "USD 417m a year from 2017 (USD 417m in 2024)"
            )
        ),
    ),
)

TECHNICAL_COOPERATION = _named(
    "oecd.dac2a.technical_cooperation",
    "Technical co-operation",
    "Grants that pay for know-how, training, research, and the experts who "
    "provide them, by recipient (line 207).",
    _binding(net=207),
    caveats=(
        Caveat(
            _non_dac_gap(
                "On line 207 the gap is Saudi Arabia's technical co-operation "
                "that appears in its developing-countries total (10100) only, "
                "from USD 21m to USD 159m a year in 2015-2021 and none from "
                "2022. Pass providers=20005 for that total"
            )
        ),
    ),
)

#: DAC2A lines that are not amounts of money: ODA as a percentage of the
#: recipient's GNI (286) and ODA per capita (296).
NON_MONEY_LINES: tuple[int, ...] = (286, 296)

#: The lines of gross disbursements are gross ODA loans (204) and gross ODA (240).
#: Every other line that is money is net disbursements (206) or a part of
#: them, such as grants (201), net loans (218) and repayments (205), or a memo
#: item on that basis, such as interest received (209).
GROSS_LINES: tuple[int, ...] = (204, 240)

_GROSS_ONLY = In("line", GROSS_LINES)
_NET_ONLY = NotIn("line", GROSS_LINES)

#: The binding of ``oecd.dac2a.lines``. A DAC2A line carries its own basis, so
#: each basis reads the lines published on it: gross disbursements read
#: ``GROSS_LINES``, net disbursements every other line.
DAC2A_LINES = Binding(
    table="dac2a",
    dimensions={**_DIMENSIONS, "line": "line"},
    bases={
        "net_disbursement": Value("value", rows=(_NET_ONLY,)),
        "gross_disbursement": Value("value", rows=(_GROSS_ONLY,)),
    },
    rows=(
        *_ROWS,
        NotIn(
            "line",
            NON_MONEY_LINES,
            reason="lines 286 and 296 are ODA as a percentage of the "
            "recipient's GNI and ODA per capita, not money",
        ),
    ),
)

LINES = Indicator(
    id="oecd.dac2a.lines",
    name="DAC2A lines",
    description="Any published DAC2A line that is an amount of money, named "
    "with lines=. Each line carries its own basis. Net disbursements are the "
    "default (line 206 and its parts), and lines 204 and 240 read gross "
    "disbursements with basis='gross_disbursement'. Lines nest, so each line "
    "is its own row and no two lines are summed. A line counts the "
    "rows its named indicator counts, so line 106 leaves out multilateral "
    "providers as oecd.dac2a.imputed_multilateral_oda does.",
    binding=DAC2A_LINES,
    kind=Sum(),
    dimensions=(*_NAMED_DIMENSIONS, "line"),
    default_basis="net_disbursement",
    default_providers=None,
    rows=line_exclusions(
        (RECIPIENT_ODA, IMPUTED_MULTILATERAL_ODA, TECHNICAL_COOPERATION)
    ),
)

INDICATORS: tuple[Indicator, ...] = (
    RECIPIENT_ODA,
    IMPUTED_MULTILATERAL_ODA,
    TECHNICAL_COOPERATION,
    LINES,
)

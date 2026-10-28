"""Indicators built on the Multisystem table, providers' contributions to
multilateral organisations.

Each Multisystem row is one provider's contribution to one channel (a
multilateral organisation or fund) in one year. Which rows count:

- Current prices only. The table holds every value twice, under
  ``amount_type`` ``"Current prices"`` and ``"Constant prices"``.
- Core contributions only: ``aid_to_or_thru`` ``"Core contributions to"``,
  the unearmarked money DAC1 counts as multilateral ODA. ``"Contributions
  through"`` rows are earmarked money a provider sends through the channel,
  which the CRS counts as bilateral.
- The basis picks ``flow_type``: ``"Disbursements"`` for gross disbursements
  and ``"Commitments"`` for commitments. The table has no net flows or grant
  equivalents.

EU members' contributions include their share of the EU budget given to EU
Institutions (918), which in turn contribute to other channels, so a sum of 918
with a member counts that money twice and raises.

``one.crs.imputed_multilateral_oda`` reads its contributions through
``oecd.multisystem.core_contributions`` and spreads them over what each
channel spends, so the same rows count in both.
"""

from functools import partial

from oda_data._methods.imputation import imputed_rows
from oda_data._model import Binding, Caveat, In, Indicator, Method, Sum, Value

#: Core contributions in current prices.
CORE_ROWS = (
    In("amount_type", ("Current prices",)),
    In("aid_to_or_thru", ("Core contributions to",)),
)

MULTISYSTEM = Binding(
    table="multisystem",
    dimensions={"provider": "provider", "channel": "channel", "year": "year"},
    bases={
        "gross_disbursement": Value(
            "value", rows=(In("flow_type", ("Disbursements",)),)
        ),
        "commitment": Value("value", rows=(In("flow_type", ("Commitments",)),)),
    },
    rows=CORE_ROWS,
    members_hold_eu_share=True,
    reporting=(),
)

CORE_CONTRIBUTIONS = Indicator(
    id="oecd.multisystem.core_contributions",
    name="Core contributions to multilateral organisations",
    description="Providers' core (unearmarked) contributions to multilateral "
    "organisations and funds, by channel. These are the Multisystem rows "
    "'Core contributions to' in current prices; earmarked money sent through "
    "a channel is bilateral and sits in oecd.crs.bilateral_oda.",
    binding=MULTISYSTEM,
    kind=Sum(),
    dimensions=("provider", "channel", "year"),
    default_basis="gross_disbursement",
    default_providers="dac_countries",
)

IMPUTED_MULTILATERAL_ODA = Indicator(
    id="one.crs.imputed_multilateral_oda",
    name="Imputed multilateral ODA",
    description="Each provider's core contributions to multilateral "
    "organisations, spread across recipients and sectors in the proportions "
    "each organisation spends its own money in the CRS over three years. "
    "allocation_status says how each amount was spread: imputed, stale_share, "
    "proxy or unallocated.",
    binding=MULTISYSTEM,
    kind=Method(
        partial(imputed_rows, core=CORE_CONTRIBUTIONS), tables=("multisystem", "crs")
    ),
    dimensions=(
        "provider",
        "recipient",
        "sector",
        "broad_sector",
        "channel",
        "allocation_status",
        "year",
    ),
    default_basis="gross_disbursement",
    default_providers=None,
    caveats=(
        Caveat(
            "Shares come from each organisation's CRS gross disbursements of ODA "
            "and other official flows. Unallocated amounts and fixed-purpose "
            "proxies have no recipient, so a filter on recipients leaves them out"
        ),
    ),
)

INDICATORS: tuple[Indicator, ...] = (CORE_CONTRIBUTIONS, IMPUTED_MULTILATERAL_ODA)

"""Imputed multilateral ODA: providers' core contributions spread over what
each multilateral organisation spends.

Each provider's core contribution to a multilateral channel (the Multisystem
rows ``oecd.multisystem.core_contributions`` counts) is spread across
recipients and purpose codes in the proportions in which the channel spends its
own money, read from the CRS. Every core dollar lands in exactly one row, whose
``allocation_status`` says how:

- ``imputed``: the channel's own shares over the ``PERIOD_LENGTH`` years
  ending in the contribution's year.
- ``stale_share``: the channel spent nothing in that window, so its latest
  window ending at most ``MAX_SHARE_AGE`` years earlier.
- ``proxy``: the channel has neither, and ``channel_share_proxies.csv``
  names a parent fund whose own shares (current or stale) it borrows, or one
  purpose code that takes the whole contribution with no recipient. A proxy
  never chains to another proxy.
- ``unallocated``: none of these, so the whole contribution with no recipient
  or sector.

A channel's spending is the gross disbursements of the CRS rows its
multilateral reporters file (``multilateral_channel_crosswalk.csv`` maps each
provider and agency to a channel), in categories 10 (ODA) and 21 (other
official flows), the OECD's former sectoral imputation basis, which gives
channels that lend on market terms (IBRD, EBRD, IFC) a share pool. Rows that
report a core contribution to another multilateral (``bi_multi`` 2) are left
out, since the Multisystem side counts that money. A window whose total is
zero or negative gives no shares. Rows of exactly zero are not returned. A
negative row, a reversal in the channel's spending, is.

The allocation must conserve each provider's contribution to each channel in
each year, or ``ImputationConservationError`` is raised. The function returns
current USD, and the engine converts each row with its provider's factor.

Channel spending and shares are the same for every call that reads the same
CRS release and share years, so ``imputed_rows`` keeps them in the memo the
engine passes, the Context's store keyed by the CRS release the call holds.
They go in the store's large section, which drops the least recently used. A
spending table for nine share years is about 10 MB and a shares table 2-4 MB.
Each call allocates its own contributions and checks conservation.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from typing import TYPE_CHECKING

import pyarrow as pa

from oda_data._compile import filtered, literal, literals
from oda_data._errors import ImputationConservationError, UnmappedChannelError
from oda_data._groups import broad_sectors
from oda_data._methods import channels
from oda_data._model import MethodRows, ResolvedCall
from oda_data._names import load_json

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection

    from oda_data._model import Indicator
    from oda_data._store import Releases

#: Years in a share window.
PERIOD_LENGTH = 3
#: How many years a stale window may end before the contribution's year.
MAX_SHARE_AGE = 5
#: CRS categories a channel's spending counts: ODA (10) and other official
#: flows (21).
SHARE_CATEGORIES = (10, 21)
#: The ``bi_multi`` code of a core contribution to a multilateral.
CORE_CONTRIBUTION = 2
#: The allocation statuses, in fallback order.
STATUSES = ("imputed", "stale_share", "proxy", "unallocated")
#: A provider-channel-year conserves when its rows sum to its contribution
#: within this share of the contribution, plus ``ABSOLUTE_TOLERANCE``.
RELATIVE_TOLERANCE = 1e-6
ABSOLUTE_TOLERANCE = 1e-9

#: The rows ``imputed_rows`` returns. ``channel`` is always the channel the
#: provider contributed to; ``share_channel`` is the channel whose shares
#: spread it (a proxy's parent fund), null for a fixed-purpose proxy or an
#: unallocated row. ``share_years`` is the window, such as ``"2020-2022"``.
OUTPUT_SCHEMA = pa.schema(
    [
        ("provider", pa.int64()),
        ("year", pa.int64()),
        ("channel", pa.int64()),
        ("recipient", pa.int64()),
        ("sector", pa.int64()),
        ("broad_sector", pa.string()),
        ("allocation_status", pa.string()),
        ("share_channel", pa.int64()),
        ("share_years", pa.string()),
        ("basis", pa.string()),
        ("value", pa.float64()),
    ]
)

_CORE = "_imputation_core"
_SPEND = "_imputation_spend"
_SPENDING = "_imputation_spending"
_SHARES = "_imputation_shares"
_ROWS = "_imputation_rows"
_CROSSWALK = "_imputation_crosswalk"
_PROXIES = "_imputation_proxies"
_BROAD = "_imputation_broad_sectors"


def imputed_rows(
    cursor: DuckDBPyConnection,
    call: ResolvedCall,
    /,
    *,
    core: Indicator,
    memo: Releases,
) -> MethodRows:
    """Spread core contributions over their channels' spending.

    Follows ``_model.MethodFunction`` once ``core`` is bound.

    Args:
        cursor: A cursor with the ``multisystem`` and ``crs`` views.
        call: The resolved call. Its basis, years, providers and channels
            select the contributions; the shares read every channel's CRS
            spending.
        core: The indicator whose rows are the core contributions.
        memo: Keeps channel spending and shares per CRS release.

    Returns:
        Rows in ``OUTPUT_SCHEMA``. A contribution is spread whole, so no row
        is incomplete.

    Raises:
        UnmappedChannelError: A multilateral reporter with spending in the
            share years has no reviewed crosswalk row.
        ImputationConservationError: The rows do not add up to the
            contributions.
    """
    contributions = core_contributions(cursor, call, core)
    if contributions.num_rows == 0:
        return MethodRows(OUTPUT_SCHEMA.empty_table())

    years = sorted(set(contributions["year"].to_pylist()))
    proxy_channels = channels.proxies()["proxy_channel"].drop_null().to_pylist()
    wanted = sorted({*contributions["channel"].to_pylist(), *proxy_channels})
    first = years[0] - (PERIOD_LENGTH - 1 + MAX_SHARE_AGE)
    last = years[-1]

    def spending() -> pa.Table:
        return memo.get(
            "crs",
            ("imputation_spending", first, last),
            lambda: channel_spending(cursor, first=first, last=last),
            large=True,
        )

    shares = memo.get(
        "crs",
        ("imputation_shares", first, tuple(years), tuple(wanted)),
        lambda: channel_shares(cursor, spending(), years=years, wanted=wanted),
        large=True,
    )
    rows = allocate(cursor, contributions, shares)
    check_conservation(cursor, rows, contributions)

    broad = broad_sectors()
    sectors = pa.table(
        {
            "sector": pa.array(list(broad), pa.int64()),
            "broad_sector": pa.array(list(broad.values()), pa.string()),
        }
    )
    with _views(cursor, {_ROWS: rows, _BROAD: sectors}):
        table = _read(
            cursor,
            f"""
            SELECT r.provider, r.year, r.channel, r.recipient, r.sector,
                b.broad_sector, r.allocation_status, r.share_channel,
                r.share_years, {literal(call.basis)} AS basis, r.value
            FROM {_ROWS} r LEFT JOIN {_BROAD} b ON b.sector = r.sector
            """,
            OUTPUT_SCHEMA,
        )
    return MethodRows(table)


def core_contributions(
    cursor: DuckDBPyConnection, call: ResolvedCall, core: Indicator
) -> pa.Table:
    """The call's core contributions, one row per year, provider and channel.

    Reads ``core``'s rows under the call's basis, years, providers and
    channels, so the Multisystem counting rules live in one indicator.
    """
    read = ResolvedCall(
        basis=call.basis,
        units=call.units,
        by=("channel",),
        years=call.years,
        members={
            dimension: codes
            for dimension, codes in call.members.items()
            if dimension in ("provider", "channel")
        },
    )
    keys = "CAST(year AS BIGINT) AS year, CAST(provider AS BIGINT) AS provider, "
    keys += "CAST(channel AS BIGINT) AS channel"
    return (
        filtered(cursor, core, read)
        .aggregate(f"{keys}, sum(value) AS value", "year, provider, channel")
        .to_arrow_table()
    )


def channel_spending(cursor: DuckDBPyConnection, *, first: int, last: int) -> pa.Table:
    """Each channel's CRS spending by sector, recipient and year.

    Columns ``channel``, ``sector``, ``recipient``, ``year`` and ``value``
    (gross disbursements, current USD millions). Sector and recipient keep
    their nulls.

    Raises:
        UnmappedChannelError: A multilateral reporter with nonzero spending
            has no reviewed crosswalk row.
    """
    providers = sorted(
        int(code) for code in load_json("provider_groupings.json")["multilateral"]
    )
    with _views(cursor, {_CROSSWALK: channels.crosswalk()}):
        spend = cursor.sql(
            f"""
            SELECT s.*, c.channel, c.status
            FROM (
                SELECT provider,
                    coalesce(CAST(agency_code AS BIGINT), {channels.NO_AGENCY})
                        AS agency,
                    sector, recipient, CAST(year AS BIGINT) AS year,
                    sum(coalesce(disbursement, 0)) AS value,
                    any_value(donor_name) AS provider_name,
                    any_value(agency_name) AS agency_name
                FROM crs
                WHERE provider IN ({literals(providers)})
                    AND category IN ({literals(SHARE_CATEGORIES)})
                    AND bi_multi IS DISTINCT FROM {CORE_CONTRIBUTION}
                    AND year BETWEEN {int(first)} AND {int(last)}
                GROUP BY provider, agency, sector, recipient, year
            ) s
            LEFT JOIN {_CROSSWALK} c
                ON c.provider = s.provider AND c.agency = s.agency
            """
        ).to_arrow_table()

    with _views(cursor, {_SPEND: spend}):
        unmapped = cursor.sql(
            f"""
            SELECT provider, agency, any_value(provider_name),
                any_value(agency_name), sum(value) AS money
            FROM {_SPEND}
            WHERE status IS NULL AND value <> 0
            GROUP BY provider, agency
            ORDER BY money DESC
            """
        ).fetchall()
        if unmapped:
            pairs = "; ".join(
                f"provider {provider}, agency "
                f"{'none' if agency == channels.NO_AGENCY else agency} "
                f"({provider_name} / {agency_name}): {money:,.2f}"
                for provider, agency, provider_name, agency_name, money in unmapped
            )
            raise UnmappedChannelError(
                f"{len(unmapped)} multilateral CRS reporters with spending in "
                f"{first}-{last} have no reviewed row in {channels.CROSSWALK_FILE}, "
                "so their spending has no channel. Add them with scripts/"
                "refresh_channel_crosswalk.py review and mark them reviewed. "
                f"Reporters (USD millions): {pairs}"
            )
        return cursor.sql(
            f"""
            SELECT channel, sector, recipient, year, sum(value) AS value
            FROM {_SPEND}
            WHERE status = 'mapped'
            GROUP BY channel, sector, recipient, year
            """
        ).to_arrow_table()


def channel_shares(
    cursor: DuckDBPyConnection,
    spending: pa.Table,
    *,
    years: list[int],
    wanted: list[int],
) -> pa.Table:
    """Each channel's share of its spending by sector and recipient, for each
    of ``years``.

    A window is the ``PERIOD_LENGTH`` years ending in its last year. Windows
    end from the ``PERIOD_LENGTH``-th year of ``spending`` to its last year,
    and a year with no spending inside one counts as zero. For each year, a
    channel takes its window ending in that year when the window's total is
    positive (``imputed``), else its latest positive window ending at most
    ``MAX_SHARE_AGE`` years earlier (``stale_share``). A channel with neither
    has no rows for the year.

    Args:
        cursor: Any cursor.
        spending: ``channel_spending`` rows.
        years: The years to give shares for.
        wanted: The channels to give shares for.

    Returns:
        Columns ``year``, ``channel``, ``sector``, ``recipient``, ``share``,
        ``allocation_status`` and ``share_years``. A window entry whose
        spending nets to zero has no row.
    """
    with _views(cursor, {_SPENDING: spending}):
        [(low, high)] = cursor.sql(
            f"SELECT min(year), max(year) FROM {_SPENDING}"
        ).fetchall()
        if low is None or not years or not wanted:
            return _read(cursor, _NO_SHARES, _SHARES_SCHEMA)
        back = PERIOD_LENGTH - 1
        return _read(
            cursor,
            f"""
            WITH totals AS (
                SELECT channel, year, sum(value) AS value
                FROM {_SPENDING}
                WHERE channel IN ({literals(wanted)})
                GROUP BY channel, year
            ),
            windows AS (
                SELECT t.channel, e.range AS end_year, sum(t.value) AS total
                FROM totals t
                JOIN range({low + back}, {high + 1}) e
                    ON t.year BETWEEN e.range - {back} AND e.range
                GROUP BY t.channel, e.range
                HAVING sum(t.value) > 0
            ),
            resolved AS (
                SELECT y.year, w.channel, max(w.end_year) AS source_year
                FROM (SELECT unnest([{literals(years)}]) AS year) y
                JOIN windows w
                    ON w.end_year BETWEEN y.year - {MAX_SHARE_AGE} AND y.year
                GROUP BY y.year, w.channel
            )
            SELECT r.year, r.channel, s.sector, s.recipient,
                sum(s.value) / any_value(w.total) AS share,
                CASE WHEN r.year = r.source_year THEN 'imputed'
                    ELSE 'stale_share' END AS allocation_status,
                printf('%d-%d', r.source_year - {back}, r.source_year)
                    AS share_years
            FROM resolved r
            JOIN windows w
                ON w.channel = r.channel AND w.end_year = r.source_year
            JOIN {_SPENDING} s
                ON s.channel = r.channel
                AND s.year BETWEEN r.source_year - {back} AND r.source_year
            GROUP BY r.year, r.channel, r.source_year, s.sector, s.recipient
            HAVING sum(s.value) <> 0
            """,
            _SHARES_SCHEMA,
        )


_SHARES_SCHEMA = pa.schema(
    [
        ("year", pa.int64()),
        ("channel", pa.int64()),
        ("sector", pa.int64()),
        ("recipient", pa.int64()),
        ("share", pa.float64()),
        ("allocation_status", pa.string()),
        ("share_years", pa.string()),
    ]
)
_NO_SHARES = (
    "SELECT NULL::BIGINT AS year, NULL::BIGINT AS channel, NULL::BIGINT AS sector, "
    "NULL::BIGINT AS recipient, NULL::DOUBLE AS share, "
    "NULL::VARCHAR AS allocation_status, NULL::VARCHAR AS share_years LIMIT 0"
)


def allocate(
    cursor: DuckDBPyConnection, contributions: pa.Table, shares: pa.Table
) -> pa.Table:
    """Spread each contribution over its channel's shares, then a proxy's,
    then leave it unallocated.

    Args:
        cursor: Any cursor.
        contributions: ``core_contributions`` rows.
        shares: ``channel_shares`` rows covering the contributions' channels
            and every proxy's parent fund.

    Returns:
        Columns ``year``, ``provider``, ``channel``, ``recipient``,
        ``sector``, ``value``, ``allocation_status``, ``share_channel`` and
        ``share_years``, with no row of exactly zero.
    """
    views = {_CORE: contributions, _SHARES: shares, _PROXIES: channels.proxies()}
    with _views(cursor, views):
        return cursor.sql(
            f"""
            WITH own AS (
                SELECT c.year, c.provider, c.channel, s.recipient, s.sector,
                    c.value * s.share AS value, s.allocation_status,
                    c.channel AS share_channel, s.share_years
                FROM {_CORE} c
                JOIN {_SHARES} s ON s.year = c.year AND s.channel = c.channel
            ),
            unmatched AS (
                SELECT c.* FROM {_CORE} c
                ANTI JOIN {_SHARES} s ON s.year = c.year AND s.channel = c.channel
            ),
            fixed AS (
                SELECT u.year, u.provider, u.channel, NULL::BIGINT AS recipient,
                    p.fixed_sector AS sector, u.value, 'proxy' AS allocation_status,
                    NULL::BIGINT AS share_channel, NULL::VARCHAR AS share_years
                FROM unmatched u
                JOIN {_PROXIES} p
                    ON p.channel = u.channel AND p.proxy_type = 'fixed_purpose'
            ),
            parent AS (
                SELECT u.year, u.provider, u.channel, s.recipient, s.sector,
                    u.value * s.share AS value, 'proxy' AS allocation_status,
                    p.proxy_channel AS share_channel, s.share_years
                FROM unmatched u
                JOIN {_PROXIES} p
                    ON p.channel = u.channel AND p.proxy_type = 'parent_fund'
                JOIN {_SHARES} s
                    ON s.year = u.year AND s.channel = p.proxy_channel
            ),
            proxied AS (SELECT * FROM fixed UNION ALL SELECT * FROM parent),
            unallocated AS (
                SELECT u.year, u.provider, u.channel, NULL::BIGINT AS recipient,
                    NULL::BIGINT AS sector, u.value,
                    'unallocated' AS allocation_status,
                    NULL::BIGINT AS share_channel, NULL::VARCHAR AS share_years
                FROM unmatched u
                ANTI JOIN proxied p
                    ON p.year = u.year AND p.provider = u.provider
                    AND p.channel = u.channel
            )
            SELECT * FROM (
                SELECT * FROM own
                UNION ALL SELECT * FROM proxied
                UNION ALL SELECT * FROM unallocated
            )
            WHERE value <> 0
            """
        ).to_arrow_table()


def check_conservation(
    cursor: DuckDBPyConnection, rows: pa.Table, contributions: pa.Table
) -> None:
    """Raise unless ``rows`` add up to each contribution.

    Each year, provider and channel's rows must sum to its contribution
    within ``RELATIVE_TOLERANCE`` of the contribution plus
    ``ABSOLUTE_TOLERANCE``. Rows for a key with no contribution count
    against a contribution of zero.

    Raises:
        ImputationConservationError: Some keys do not conserve. The message
            names up to ten, the largest gaps first.
    """
    with _views(cursor, {_ROWS: rows, _CORE: contributions}):
        compared = cursor.sql(
            f"""
            SELECT year, provider, channel,
                coalesce(c.value, 0) AS core, coalesce(a.value, 0) AS allocated
            FROM (SELECT year, provider, channel, sum(value) AS value FROM {_CORE}
                GROUP BY ALL) c
            FULL JOIN (SELECT year, provider, channel, sum(value) AS value
                FROM {_ROWS} GROUP BY ALL) a
                USING (year, provider, channel)
            """
        )
        gap = "abs(allocated - core)"
        limit = f"{RELATIVE_TOLERANCE} * abs(core) + {ABSOLUTE_TOLERANCE}"
        [(total,)] = compared.aggregate("count(*)").fetchall()
        broken = compared.filter(f"{gap} > {limit}").order(f"{gap} DESC").fetchall()
    if not broken:
        return
    worst = "; ".join(
        f"year {year}, provider {provider}, channel {channel}: contributed "
        f"{core:.6f}, allocated {allocated:.6f}"
        for year, provider, channel, core, allocated in broken[:10]
    )
    raise ImputationConservationError(
        f"the imputation lost or added money for {len(broken)} of {total} "
        f"provider-channel-years. Largest gaps: {worst}"
    )


@contextmanager
def _views(
    cursor: DuckDBPyConnection, tables: Mapping[str, pa.Table]
) -> Iterator[None]:
    """Register Arrow tables on the cursor for the block, then drop them."""
    for name, table in tables.items():
        cursor.register(name, table)
    try:
        yield
    finally:
        for name in tables:
            cursor.unregister(name)


def _read(cursor: DuckDBPyConnection, sql: str, schema: pa.Schema) -> pa.Table:
    return cursor.sql(sql).to_arrow_table().select(schema.names).cast(schema)

"""Conversion factors from the Context's DAC1 table, through pydeflate.

``DacFactors`` implements ``_model.Factors``. It hands pydeflate the DAC1
Parquet file the Context's views read, so the factors and the ODA figures
come from one DAC1 release. A factor multiplies a value in current US
dollars. Current USD needs no factor, so pydeflate is not called for it.

pydeflate rebuilds its rate table on every call, so each ``DacFactors`` keeps
the factors it has fetched per pydeflate currency and base year, for every
year in the DAC1 table. The first fetch for a unit asks for every DAC1 donor
as well as the providers in hand, so a later call rarely needs another.
"""

from __future__ import annotations

import threading
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING

import pyarrow as pa

from oda_data._errors import (
    DataUnavailableError,
    MissingFactorError,
    UnsupportedQueryError,
)
from oda_data._model import FactorTable

if TYPE_CHECKING:
    import pandas as pd

    from oda_data._model import Tables
    from oda_data._provenance import Release
    from oda_data._units import Unit

#: ISO 4217 codes oda-data converts to, and the pydeflate code of each.
#:
#: These are the currencies of the DAC members, whose DAC1 rows carry
#: national-currency amounts. pydeflate names a currency by the ISO3 code of a
#: DAC1 donor that uses it; the euro is EU Institutions' (``EUI``).
CURRENCIES: Mapping[str, str] = MappingProxyType(
    {
        "USD": "USA",
        "EUR": "EUI",
        "GBP": "GBR",
        "JPY": "JPN",
        "CAD": "CAN",
        "AUD": "AUS",
        "CHF": "CHE",
        "CZK": "CZE",
        "DKK": "DNK",
        "HUF": "HUN",
        "ISK": "ISL",
        "KRW": "KOR",
        "NOK": "NOR",
        "NZD": "NZL",
        "PLN": "POL",
        "RON": "ROU",
        "SEK": "SWE",
    }
)

#: The columns of ``FactorTable.factors``.
FACTOR_SCHEMA = pa.schema(
    [
        ("provider", pa.int64()),
        ("year", pa.int64()),
        ("currency", pa.string()),
        ("prices", pa.string()),
        ("base_year", pa.int64()),
        ("factor", pa.float64()),
    ]
)

#: Provider-years a ``MissingFactorError`` message lists before counting.
_SHOWN = 5


@dataclass(slots=True)
class _Fetched:
    """Factors pydeflate returned for one currency and base year.

    Args:
        frame: Columns ``entity_code``, ``year``, ``factor`` and ``fallback``,
            for every DAC1 year of each entity in ``entities``.
        entities: The entity codes ``frame`` covers.
    """

    frame: pd.DataFrame
    entities: frozenset[int]


class DacFactors:
    """Conversion factors from one Context's DAC1 table.

    Thread-safe: one lock guards the fetched factors, so concurrent calls
    for the same unit fetch it once.

    Args:
        tables: The Context's tables. ``tables.path("dac1")`` is the file
            pydeflate reads, and ``tables.release("dac1")`` its release.
    """

    def __init__(self, tables: Tables) -> None:
        self._tables = tables
        self._lock = threading.Lock()
        self._fetched: dict[tuple[str, int | None], _Fetched] = {}
        self._dac1: tuple[frozenset[int], tuple[int, ...]] | None = None

    def table(self, keys: pa.Table, units: tuple[Unit, ...], /) -> FactorTable:
        """Return factors for every provider-year key and unit.

        Args:
            keys: Distinct ``provider`` and ``year`` pairs (int64).
            units: The units asked for.

        Returns:
            One factor row per key and unit, and the providers that took the
            DAC total's (20001) factors because the DAC1 table has none of
            their own. Current USD rows have factor 1 and add no fallback.

        Raises:
            UnsupportedQueryError: A unit's currency is not in ``CURRENCIES``.
            MissingFactorError: The DAC1 table has no factor for a key, or no
                data for a unit's currency or base year. The message names
                the unit and the first provider-years that lack one.
            DataUnavailableError: pydeflate cannot read the DAC1 table.

        Any other pydeflate error propagates unchanged.
        """
        import pandas as pd

        units = tuple(dict.fromkeys(units))
        for unit in units:
            pydeflate_currency(unit.currency)

        keys_frame = (
            keys.select(["provider", "year"])
            .to_pandas()
            .astype({"provider": "int64", "year": "int64"})
        )
        providers = frozenset(int(code) for code in keys_frame["provider"])

        parts: list[pa.Table] = []
        fallback: set[int] = set()
        for unit in units:
            if unit.currency == "USD" and unit.prices == "current":
                factors = pd.Series(1.0, index=keys_frame.index)
            else:
                matched = keys_frame.merge(
                    self._fetch(unit, providers).rename(
                        columns={"entity_code": "provider"}
                    ),
                    on=["provider", "year"],
                    how="left",
                    validate="one_to_one",
                )
                _check_complete(matched, unit)
                took = matched["fallback"].astype(bool)
                fallback.update(int(code) for code in matched.loc[took, "provider"])
                factors = matched["factor"]
            parts.append(_unit_rows(keys_frame, unit, factors))

        table = pa.concat_tables(parts) if parts else FACTOR_SCHEMA.empty_table()
        return FactorTable(factors=table, fallback_providers=frozenset(fallback))

    def release(self) -> Release:
        """Return the DAC1 release the factors come from."""
        return self._tables.release("dac1")

    def _fetch(self, unit: Unit, providers: frozenset[int]) -> pd.DataFrame:
        """Return the fetched factors for ``unit``, fetching any provider not
        yet covered."""
        import pandas as pd

        key = (pydeflate_currency(unit.currency), unit.base_year)
        with self._lock:
            fetched = self._fetched.get(key)
            covered = fetched.entities if fetched is not None else frozenset()
            needed = providers - covered
            if not needed:
                assert fetched is not None
                return fetched.frame

            donors, years = self._dac1_index()
            if fetched is None:
                needed |= donors
            frame = self._convert(unit, entities=needed, years=years)
            if fetched is not None:
                frame = pd.concat([fetched.frame, frame], ignore_index=True)
            self._fetched[key] = _Fetched(frame, covered | needed)
            return frame

    def _dac1_index(self) -> tuple[frozenset[int], tuple[int, ...]]:
        """The donor codes and years of the DAC1 table. Hold the lock."""
        if self._dac1 is None:
            import pyarrow.parquet as pq

            index = pq.read_table(
                self._tables.path("dac1"), columns=["donor_code", "year"]
            )
            donors = frozenset(index["donor_code"].unique().to_pylist())
            years = tuple(sorted(index["year"].unique().to_pylist()))
            self._dac1 = (donors, years)
        return self._dac1

    def _convert(
        self, unit: Unit, *, entities: Iterable[int], years: Iterable[int]
    ) -> pd.DataFrame:
        """Ask pydeflate for ``unit``'s factors from the DAC1 table."""
        from pydeflate import MissingDataError, get_oecd_dac_conversion_factors
        from pydeflate.exceptions import DataSourceError

        try:
            result = get_oecd_dac_conversion_factors(
                self._tables.path("dac1"),
                entities=entities,
                years=years,
                target_currency=pydeflate_currency(unit.currency),
                base_year=unit.base_year,
            )
        except DataSourceError as error:
            raise DataUnavailableError(
                f"pydeflate could not read the DAC1 table: {error}"
            ) from error
        except MissingDataError as error:
            raise MissingFactorError(
                f"The DAC1 table gives no {_label(unit)} factors: {error}"
            ) from error
        return result.factors


def pydeflate_currency(currency: str) -> str:
    """Return pydeflate's code for an ISO 4217 currency.

    Raises:
        UnsupportedQueryError: If oda-data does not convert to ``currency``.
    """
    code = CURRENCIES.get(currency)
    if code is None:
        raise UnsupportedQueryError(
            f"oda-data cannot convert to currency {currency!r}; use one of "
            f"{', '.join(CURRENCIES)}"
        )
    return code


def _check_complete(matched: pd.DataFrame, unit: Unit) -> None:
    """Raise if a key has no factor for ``unit``.

    Raises:
        MissingFactorError: Naming the first provider-years without one.
    """
    missing = matched[matched["factor"].isna()]
    if missing.empty:
        return
    # An unmatched key has a null ``fallback``, so compare rather than cast.
    from_total = missing["fallback"].eq(True)
    shown = [
        f"provider {provider} in {year}"
        + (" (from the DAC total, 20001)" if took else "")
        for provider, year, took in zip(
            missing["provider"], missing["year"], from_total, strict=True
        )
    ]
    more = len(shown) - _SHOWN
    listed = "; ".join(shown[:_SHOWN]) + (f"; and {more} more" if more > 0 else "")
    raise MissingFactorError(
        f"The DAC1 table has no {_label(unit)} factor for {listed}"
    )


def _unit_rows(keys: pd.DataFrame, unit: Unit, factors: pd.Series) -> pa.Table:
    """The factor rows of one unit, in ``FACTOR_SCHEMA``."""
    n = len(keys)
    return pa.table(
        {
            "provider": pa.array(keys["provider"], pa.int64()),
            "year": pa.array(keys["year"], pa.int64()),
            "currency": pa.array([unit.currency] * n, pa.string()),
            "prices": pa.array([unit.prices] * n, pa.string()),
            "base_year": pa.array([unit.base_year] * n, pa.int64()),
            "factor": pa.array(factors, pa.float64()),
        },
        schema=FACTOR_SCHEMA,
    )


def _label(unit: Unit) -> str:
    if unit.base_year is None:
        return f"current {unit.currency}"
    return f"constant {unit.base_year} {unit.currency}"

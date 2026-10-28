"""Conversion factors from a small DAC1 table, through pydeflate."""

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pydeflate
import pytest

from oda_data._errors import (
    DataUnavailableError,
    MissingFactorError,
    UnsupportedQueryError,
)
from oda_data._factors import CURRENCIES, FACTOR_SCHEMA, DacFactors
from oda_data._provenance import Release
from oda_data._types import TableName
from oda_data._units import Unit, constant, current

_RELEASE = Release(token="etag-dac1-1", downloaded=datetime(2026, 10, 1, tzinfo=UTC))

# Total ODA (line 11010, grant equivalents) per donor and year: current USD (A),
# national currency (N) and constant USD (D). France reports no constant
# figure for 2021. The DAC total publishes no national-currency amount.
_DAC1 = {
    (4, "France"): {2021: (90, 80, None), 2022: (100, 95, 110), 2023: (120, 108, 120)},
    (918, "EU Institutions"): {2022: (100, 95, 110), 2023: (120, 108, 120)},
    (302, "United States"): {2022: (500, 500, 520), 2023: (500, 500, 500)},
    (20001, "DAC Countries, Total"): {
        2022: (1000, None, 1050),
        2023: (1000, None, 1000),
    },
}


@pytest.fixture
def dac1(tmp_path: Path) -> Path:
    rows = [
        {
            "donor_code": code,
            "donor_name": name,
            "aidtype_code": 11010,
            "flows_code": 1160,
            "amounttype_code": amount_type,
            "year": year,
            "value": value,
        }
        for (code, name), years in _DAC1.items()
        for year, values in years.items()
        for amount_type, value in zip("AND", values, strict=True)
        if value is not None
    ]
    path = tmp_path / "dac1.parquet"
    pd.DataFrame(rows).to_parquet(path)
    return path


class _Tables:
    def __init__(self, path: Path) -> None:
        self._path = path

    def path(self, table: TableName) -> Path:
        assert table == "dac1"
        return self._path

    def release(self, table: TableName) -> Release:
        assert table == "dac1"
        return _RELEASE


@pytest.fixture
def factors(dac1: Path) -> DacFactors:
    return DacFactors(_Tables(dac1))


@pytest.fixture
def calls(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, object]]:
    """Record each pydeflate call's arguments."""
    recorded: list[dict[str, object]] = []
    real = pydeflate.get_oecd_dac_conversion_factors

    def spy(dac1: Path, **kwargs: object) -> pydeflate.DacConversionFactors:
        recorded.append(kwargs)
        return real(dac1, **kwargs)

    monkeypatch.setattr(pydeflate, "get_oecd_dac_conversion_factors", spy)
    return recorded


def _keys(*pairs: tuple[int, int]) -> pa.Table:
    providers, years = zip(*pairs, strict=True)
    return pa.table(
        {
            "provider": pa.array(providers, pa.int64()),
            "year": pa.array(years, pa.int64()),
        }
    )


def _by_key(table: pa.Table) -> dict[tuple, float]:
    return {
        (r["provider"], r["year"], r["currency"], r["prices"], r["base_year"]): r[
            "factor"
        ]
        for r in table.to_pylist()
    }


def test_current_usd_is_one_without_pydeflate(factors, calls):
    result = factors.table(_keys((4, 2022), (901, 1960)), (current("USD"),))

    assert result.factors.schema == FACTOR_SCHEMA
    assert result.factors.column("factor").to_pylist() == [1.0, 1.0]
    assert result.fallback_providers == frozenset()
    assert calls == []


def test_factors_follow_the_dac1_amounts(factors):
    units = (current("USD"), current("EUR"), constant("USD", 2023))
    result = factors.table(_keys((4, 2022), (302, 2022)), units)

    assert result.factors.schema == FACTOR_SCHEMA
    assert _by_key(result.factors) == pytest.approx(
        {
            (4, 2022, "USD", "current", None): 1.0,
            (302, 2022, "USD", "current", None): 1.0,
            # EU Institutions' N / A is the euro rate.
            (4, 2022, "EUR", "current", None): 0.95,
            (302, 2022, "EUR", "current", None): 0.95,
            # Each donor's own D / A.
            (4, 2022, "USD", "constant", 2023): 1.10,
            (302, 2022, "USD", "constant", 2023): 1.04,
        }
    )
    assert result.fallback_providers == frozenset()


def test_provider_absent_from_dac1_takes_the_dac_total(factors):
    result = factors.table(_keys((901, 2022), (4, 2022)), (constant("USD", 2023),))

    by_key = _by_key(result.factors)
    assert by_key[(901, 2022, "USD", "constant", 2023)] == pytest.approx(1.05)
    assert by_key[(4, 2022, "USD", "constant", 2023)] == pytest.approx(1.10)
    assert result.fallback_providers == frozenset({901})


@pytest.mark.parametrize(
    ("key", "unit", "named"),
    [
        # France is in the table but has no constant figure that year.
        ((4, 2021), constant("USD", 2023), "provider 4 in 2021"),
        # A year the table does not reach.
        ((4, 2030), current("EUR"), "provider 4 in 2030"),
        # The DAC total, which a fallback provider borrows, lacks the year.
        ((901, 2021), constant("USD", 2023), "provider 901 in 2021 (from the DAC"),
    ],
)
def test_missing_factor_names_provider_and_year(factors, key, unit, named):
    with pytest.raises(MissingFactorError) as raised:
        factors.table(_keys(key, (4, 2022)), (unit,))

    assert raised.value.code == "missing_factor"
    assert named in raised.value.message
    assert "provider 4 in 2022" not in raised.value.message


@pytest.mark.parametrize(
    "unit",
    [
        # A currency oda-data maps whose country the table lacks.
        current("GBP"),
        # A base year the table does not reach.
        constant("USD", 1990),
    ],
)
def test_unit_the_table_cannot_serve_raises_missing_factor(factors, unit):
    with pytest.raises(MissingFactorError) as raised:
        factors.table(_keys((4, 2022)), (unit,))

    assert isinstance(raised.value.__cause__, pydeflate.MissingDataError)


def test_other_pydeflate_errors_propagate(factors, monkeypatch):
    def broken(*args: object, **kwargs: object) -> None:
        raise pd.errors.MergeError("not a one-to-one merge")

    monkeypatch.setattr(pydeflate, "get_oecd_dac_conversion_factors", broken)

    with pytest.raises(pd.errors.MergeError):
        factors.table(_keys((4, 2022)), (current("EUR"),))


@pytest.mark.parametrize("currency", ["XYZ", "usd", "USA"])
def test_unknown_currency_is_refused(factors, calls, currency):
    with pytest.raises(UnsupportedQueryError) as raised:
        factors.table(_keys((4, 2022)), (current("USD"), Unit(currency)))

    assert raised.value.code == "unsupported_query"
    assert calls == []


def test_currencies_map_to_pydeflate_codes():
    assert set(CURRENCIES) >= {"USD", "EUR", "GBP", "CAD"}
    assert all(len(code) == 3 and code.isupper() for code in CURRENCIES.values())


def test_unreadable_table_raises_data_unavailable(tmp_path):
    path = tmp_path / "dac1.parquet"
    pd.DataFrame({"donor_code": [4], "year": [2022]}).to_parquet(path)
    factors = DacFactors(_Tables(path))

    with pytest.raises(DataUnavailableError):
        factors.table(_keys((4, 2022)), (current("EUR"),))


def test_fetches_once_per_currency_and_base_year(factors, calls):
    eur = current("EUR")
    factors.table(_keys((4, 2022)), (eur,))
    factors.table(_keys((302, 2023), (918, 2022)), (eur,))
    assert len(calls) == 1
    # The first fetch covers every DAC1 donor, for every DAC1 year.
    assert set(calls[0]["entities"]) == {4, 302, 918, 20001}
    assert list(calls[0]["years"]) == [2021, 2022, 2023]

    factors.table(_keys((901, 2022)), (eur,))
    assert set(calls[1]["entities"]) == {901}

    factors.table(_keys((4, 2022)), (constant("EUR", 2023), constant("EUR", 2022)))
    assert len(calls) == 4

    again = factors.table(_keys((901, 2022), (4, 2023)), (eur,))
    assert len(calls) == 4
    assert again.fallback_providers == frozenset({901})


def test_concurrent_calls_fetch_once(factors, calls):
    unit = constant("EUR", 2023)
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(
            pool.map(lambda _: factors.table(_keys((4, 2022)), (unit,)), range(8))
        )

    assert len(calls) == 1
    assert [r.factors.column("factor")[0].as_py() for r in results] == (
        [pytest.approx(0.99)] * 8
    )


def test_repeated_unit_gives_one_row_per_key(factors):
    result = factors.table(_keys((4, 2022)), (current("EUR"), current("EUR")))

    assert result.factors.num_rows == 1


def test_release_is_the_dac1_release(factors):
    assert factors.release() == _RELEASE

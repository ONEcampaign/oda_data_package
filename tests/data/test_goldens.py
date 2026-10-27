"""Golden numbers: 3.0 calls against values computed outside oda-data 3.0.

``goldens.json`` holds each golden's call, its expected values, where they come
from, and the release tokens of the tables they were computed on. A source is
either ``oda-data 2.8.2``, run by ``scripts/v2_reference.py`` on the same
cached Parquet files (its ``request`` is the exact call), or ``OECD``, a DuckDB
query on those files as the OECD publishes them (its ``sql`` reads the rows,
such as provider 20001, the DAC countries' total). The constant-price goldens
compare ``unit=oda.constant("USD", 2024)`` with DAC1's own constant-price rows
(amount type ``D``), which the OECD publishes in 2024 USD on this release.

Each golden runs on a context pinned to its releases. When the cache holds a
different release the golden skips, which ``ODA_DATA_REQUIRE_DATA=1`` turns
into a failure. ``scripts/refresh_goldens.py`` recomputes every expected value
on the new release.

The comparisons with OECD aggregates in ``test_dac1_golden.py``,
``test_dac2a_golden.py`` and ``test_crs_golden.py`` read their expected values
from the cached table itself, so they hold on any release and need no
refresh.
"""

import importlib.util
import json
import math
import types
from collections.abc import Iterator
from pathlib import Path

import pandas as pd
import pytest

import oda_data as oda
from oda_data._errors import OfflineCacheMissError, ReleaseMismatchError

pytestmark = pytest.mark.data

GOLDENS = json.loads((Path(__file__).parent / "goldens.json").read_text())["goldens"]


def _v2_reference() -> types.ModuleType:
    """``scripts/v2_reference.py``, whose ``key_part`` wrote the expected keys."""
    path = Path(__file__).resolve().parents[2] / "scripts" / "v2_reference.py"
    spec = importlib.util.spec_from_file_location("v2_reference", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


v2_reference = _v2_reference()

#: Cyprus (30) reports no GNI for these years of DAC1 release ``DAC1_RELEASE``.
CYPRUS_GNI_GAPS = (2016, 2017, 2025)
DAC1_RELEASE = "0x8DF2539D20A32DF"


@pytest.fixture(scope="module")
def contexts() -> Iterator[dict[frozenset, oda.Context]]:
    """One offline context for each set of pinned releases."""
    found: dict[frozenset, oda.Context] = {}
    yield found
    for context in found.values():
        context.close()


def _context(contexts: dict[frozenset, oda.Context], releases: dict) -> oda.Context:
    key = frozenset(releases.items())
    if key not in contexts:
        contexts[key] = oda.Context(offline=True, releases=releases)
    return contexts[key]


def _providers(spec: object) -> object:
    """``"total:<group>"`` and ``"group:<group>"`` name a group in
    ``oda.groups``; a list holds codes and such names."""
    if isinstance(spec, list):
        return [_providers(item) for item in spec]
    if isinstance(spec, str) and ":" in spec:
        kind, name = spec.split(":", 1)
        group = getattr(oda.groups, name)
        return oda.total(group) if kind == "total" else group
    return spec


def _unit(spec: dict) -> oda.Unit:
    """``{"constant": "USD", "base_year": 2024}`` is ``oda.constant("USD", 2024)``."""
    return oda.constant(spec["constant"], spec["base_year"])


def _found(frame: pd.DataFrame, by: list[str], select: dict) -> dict[str, float]:
    for column, value in select.items():
        frame = frame[frame[column] == value]
    key = [column for column in by if column not in select]
    return {
        "|".join(v2_reference.key_part(part) for part in parts): value
        for *parts, value in frame[[*key, "value"]].itertuples(index=False)
    }


def _skip_reason(golden: dict, error: Exception) -> str:
    return (
        f"golden {golden['id']} holds for {golden['releases']}: {error}; "
        "run scripts/refresh_goldens.py on the new release"
    )


@pytest.mark.parametrize("golden", GOLDENS, ids=[g["id"] for g in GOLDENS])
def test_a_golden_call_gives_its_expected_values(
    contexts: dict[frozenset, oda.Context], golden: dict
) -> None:
    call = dict(golden["call"])
    indicator = call.pop("indicator")
    if "providers" in call:
        call["providers"] = _providers(call["providers"])
    if "unit" in call:
        call["unit"] = _unit(call["unit"])
    context = _context(contexts, golden["releases"])
    try:
        frame = context.get(indicator, **call)
        records = (
            context.records(indicator, **{k: v for k, v in call.items() if k != "by"})
            if "row_count" in golden
            else None
        )
    except (OfflineCacheMissError, ReleaseMismatchError) as error:
        pytest.skip(_skip_reason(golden, error))

    found = _found(frame, call["by"], golden.get("select", {}))
    expected = golden["expected"]
    tolerance = golden["tolerance"]

    assert expected, "a golden needs expected values"
    assert sorted(found) == sorted(expected)
    for key, value in expected.items():
        if value is None:
            assert math.isnan(found[key]), key
        else:
            assert found[key] == pytest.approx(value, **tolerance), key
    if records is not None:
        assert len(records) == golden["row_count"]


def test_eu_institutions_gni_keeps_cyprus_gap_years(
    contexts: dict[frozenset, oda.Context],
) -> None:
    """Cyprus has no GNI for 2016, 2017 or 2025, the table's last year. 2016
    and 2017 sum the other members; 2025 is null and named in explain()."""
    context = _context(contexts, {"dac1": DAC1_RELEASE})
    try:
        cyprus = context.get("oecd.dac1.gni", years=range(2015, 2026), providers=30)
        query = context.query("one.dac1.gni", years=range(2015, 2026), providers=918)
    except (OfflineCacheMissError, ReleaseMismatchError) as error:
        pytest.skip(f"DAC1 {DAC1_RELEASE} is not the cached release: {error}")
    df = query.to_pandas()
    found = dict(zip(df["year"], df["value"], strict=True))

    assert set(range(2015, 2026)) - set(cyprus["year"]) == set(CYPRUS_GNI_GAPS)
    assert sorted(found) == list(range(2015, 2026))
    assert all(not math.isnan(found[year]) for year in (2016, 2017))
    assert math.isnan(found[2025])
    assert "Cyprus (30)" in query.explain()

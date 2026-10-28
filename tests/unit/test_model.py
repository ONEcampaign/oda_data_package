"""The engine contracts: vocabulary, bindings, indicators, and resolved calls."""

import dataclasses
import subprocess
import sys

import pytest

from oda_data._model import (
    VOCABULARY,
    AnyOf,
    Between,
    Binding,
    Flow,
    Headline,
    In,
    Indicator,
    IsNull,
    Line,
    Method,
    NotIn,
    ResolvedCall,
    ResolvedGroup,
    Sum,
    Value,
)
from oda_data._units import Unit

ODA_ROWS = In("category", (10,))


def crs_binding(**overrides: object) -> Binding:
    fields: dict = {
        "table": "crs",
        "dimensions": {
            "provider": "provider",
            "recipient": "recipient",
            "sector": "sector",
            "broad_sector": "sector",
            "channel": "channel",
            "modality": "modality",
            "flow": "flow",
            "year": "year",
            "score": "gender",
        },
        "bases": {
            "gross_disbursement": Value("disbursement", rows=(ODA_ROWS,)),
            "net_disbursement": Value(
                "disbursement", minus="received", rows=(ODA_ROWS,)
            ),
            "grant_equivalent": Value(
                "grant_equivalent", rows=(In("category", (10, 60)),), from_year=2018
            ),
        },
        "rows": (In("bi_multi", (1, 3, 7, 8)),),
    }
    return Binding(**(fields | overrides))


def dac1_binding() -> Binding:
    return Binding(
        table="dac1",
        dimensions={"provider": "provider", "line": "line", "year": "year"},
        bases={
            "net_disbursement": Flow(1140),
            "grant_equivalent": Flow(1160, from_year=2018),
            "headline": Headline("net_disbursement", "grant_equivalent"),
        },
        rows=(In("amount_type", ("A",)),),
    )


def total_oda(**overrides: object) -> Indicator:
    fields: dict = {
        "id": "oecd.dac1.total_oda",
        "name": "Total ODA",
        "description": "Official development assistance from each provider.",
        "binding": dac1_binding(),
        "kind": Sum(lines={"net_disbursement": 1010, "grant_equivalent": 11010}),
        "dimensions": ("provider", "year"),
        "default_basis": "headline",
        "default_providers": "dac_countries",
    }
    return Indicator(**(fields | overrides))


def test_design_bindings_build() -> None:
    crs_binding()
    dac1_binding()
    Binding(
        table="dac2a",
        dimensions={"provider": "provider", "recipient": "recipient", "year": "year"},
        bases={"net_disbursement": Line(206), "headline": Line(206)},
        rows=(In("amount_type", ("A",)), In("part", (1,))),
    )


@pytest.mark.parametrize("table", sorted(VOCABULARY))
def test_vocabulary_names_and_sources_are_unique(table: str) -> None:
    spec = VOCABULARY[table]
    assert spec.table == table
    names = [column.name for column in spec.columns]
    sources = [column.source.lower() for column in spec.columns]
    assert len(set(names)) == len(names)
    assert len(set(sources)) == len(sources)


@pytest.mark.parametrize(
    "overrides",
    [
        {"dimensions": {"provider": "donor_code"}},
        {"rows": (AnyOf((IsNull("gender"), In("not_a_column", (1,)))),)},
        {"bases": {"commitment": Value("usd_commitment")}},
        {"bases": {"net_disbursement": Value("disbursement", minus="usd_received")}},
        {"bases": {"commitment": Value("commitment", rows=(Between("TIME", 1, 2),))}},
        {"bases": {"gross_disbursement": Flow(1120)}},
        {"bases": {"headline": Headline("net_disbursement", "grant_equivalent")}},
    ],
    ids=[
        "raw-dimension",
        "nested-filter",
        "value",
        "minus",
        "basis-rows",
        "flow-on-crs",
        "headline-missing-basis",
    ],
)
def test_binding_refuses_columns_outside_vocabulary(overrides: dict) -> None:
    with pytest.raises(ValueError, match=r"vocabulary|lacks"):
        crs_binding(**overrides)


def test_headline_cannot_point_at_a_headline() -> None:
    with pytest.raises(ValueError, match="headline"):
        Binding(
            table="dac2a",
            dimensions={"year": "year"},
            bases={
                "net_disbursement": Line(206),
                "headline": Headline("net_disbursement", "headline"),
            },
        )


def test_line_needs_a_line_column() -> None:
    with pytest.raises(ValueError, match="vocabulary"):
        Binding(table="crs", dimensions={}, bases={"headline": Line(206)})


def test_binding_is_frozen_and_detached_from_its_inputs() -> None:
    dimensions = {"provider": "provider", "year": "year"}
    binding = Binding(
        table="dac1", dimensions=dimensions, bases={"net_disbursement": Flow(1140)}
    )
    dimensions["line"] = "line"
    assert "line" not in binding.dimensions
    with pytest.raises(TypeError):
        binding.dimensions["line"] = "line"  # type: ignore[index]
    with pytest.raises(dataclasses.FrozenInstanceError):
        binding.table = "crs"  # type: ignore[misc]


def test_indicator_builds_and_is_frozen() -> None:
    indicator = total_oda(excluded_providers=frozenset({918}))
    assert indicator.kind.lines["grant_equivalent"] == 11010
    with pytest.raises(dataclasses.FrozenInstanceError):
        indicator.version = 2  # type: ignore[misc]
    with pytest.raises(TypeError):
        indicator.kind.lines["headline"] = 1  # type: ignore[index]


@pytest.mark.parametrize(
    ("overrides", "match"),
    [
        ({"rows": (NotIn("sector", (1,)),)}, "vocabulary"),
        ({"default_basis": "commitment"}, "default basis"),
        ({"dimensions": ("provider", "recipient")}, "dimensions"),
        ({"kind": Sum(lines={"commitment": 1015})}, "unknown bases"),
    ],
    ids=["rows", "default-basis", "sum-dimension", "sum-lines"],
)
def test_indicator_refuses_inconsistent_definition(overrides: dict, match: str) -> None:
    with pytest.raises(ValueError, match=match):
        total_oda(**overrides)


def test_method_may_declare_dimensions_outside_its_binding() -> None:
    def run(cursor: object, call: object, /) -> object:
        raise NotImplementedError

    indicator = Indicator(
        id="one.crs.imputed_multilateral_oda",
        name="Imputed multilateral ODA",
        description="Multilateral spending attributed to providers.",
        binding=crs_binding(),
        kind=Method(function=run, tables=("crs", "multisystem")),  # type: ignore[arg-type]
        dimensions=("provider", "year", "allocation_status"),
        default_basis="gross_disbursement",
        default_providers=None,
    )
    assert indicator.kind.tables == ("crs", "multisystem")


def test_resolved_call_holds_codes_and_is_frozen() -> None:
    members = {"provider": frozenset({4, 5})}
    call = ResolvedCall(
        basis="net_disbursement",
        units=(Unit("USD"),),
        by=("provider", "year"),
        years=(2022, 2023),
        members=members,
        groups=(
            ResolvedGroup(
                name="G2",
                dimension="provider",
                members={2022: frozenset({4, 5}), 2023: frozenset({4, 5})},
            ),
        ),
        totals=("G2",),
    )
    members["recipient"] = frozenset({248})
    assert set(call.members) == {"provider"}
    with pytest.raises(TypeError):
        call.groups[0].members[2024] = frozenset()  # type: ignore[index]
    with pytest.raises(dataclasses.FrozenInstanceError):
        call.allow_partial = True  # type: ignore[misc]


def test_resolved_call_refuses_a_total_without_its_group() -> None:
    with pytest.raises(ValueError, match="totals"):
        ResolvedCall(basis="headline", units=(), by=("year",), totals=("G7",))


def test_model_import_stays_light() -> None:
    code = (
        "import sys, oda_data._model; "
        "heavy = {'duckdb', 'pyarrow', 'pandas'} & set(sys.modules); "
        "assert not heavy, heavy"
    )
    subprocess.run([sys.executable, "-c", code], check=True)

"""Discovery: ``indicators``, ``describe``, ``search``, and ``add_names``."""

import dataclasses
import re
import subprocess
import sys
from collections.abc import Mapping

import pandas as pd
import pytest

import oda_data as oda
from oda_data import _context, _discover, _names, _registry
from oda_data._model import Binding, Caveat, Indicator, Ratio, Sum, Value, basis_lines
from oda_data._registry import dac1, dac2a, multisystem

CRS = Binding(
    table="crs",
    dimensions={
        "provider": "provider",
        "recipient": "recipient",
        "sector": "sector",
        "broad_sector": "sector",
        "year": "year",
    },
    bases={
        "net_disbursement": Value("disbursement", minus="received"),
        "gross_disbursement": Value("disbursement"),
        "commitment": Value("commitment"),
        "grant_equivalent": Value("grant_equivalent", from_year=2018),
    },
)

CRS_BILATERAL = Indicator(
    id="test.crs.bilateral_oda",
    name="Bilateral ODA by activity",
    description="Bilateral ODA reported activity by activity.",
    binding=CRS,
    kind=Sum(),
    dimensions=("provider", "recipient", "sector", "broad_sector", "year"),
    default_basis="gross_disbursement",
    default_providers=None,
    caveats=(
        Caveat("CRS totals differ from DAC1 totals."),
        Caveat("grant equivalents start in 2018", basis="grant_equivalent"),
        Caveat("a gap in the series", years=(2010, 2012)),
    ),
    version=3,
)

REGISTRY = {i.id: i for i in (*dac1.INDICATORS, *dac2a.INDICATORS, CRS_BILATERAL)}


@pytest.fixture(autouse=True)
def registry(monkeypatch: pytest.MonkeyPatch) -> Mapping[str, Indicator]:
    """Discovery reads ``_context.INDICATORS``, which ``find_indicator`` shares."""
    monkeypatch.setattr(_context, "INDICATORS", REGISTRY)
    return REGISTRY


# describe ---------------------------------------------------------------------


def test_describe_prints_the_design_form() -> None:
    assert repr(oda.describe("test.crs.bilateral_oda")) == "\n".join(
        [
            "test.crs.bilateral_oda  (version 3, table CRS)",
            "Bilateral ODA reported activity by activity.",
            "Dimensions: provider, recipient, sector, broad_sector, year",
            "Bases: gross_disbursement (default), net_disbursement, commitment, "
            "grant_equivalent (2018 on)",
            "Providers by default: every official provider in the table, "
            "aggregates excluded",
            "Values: USD millions unless unit= says otherwise",
            "Caveat: CRS totals differ from DAC1 totals.",
            "Caveat: Grant equivalents start in 2018 (grant_equivalent basis).",
            "Caveat: A gap in the series (2010 to 2012).",
            'Example: oda.get("test.crs.bilateral_oda", years=2023, '
            'by=["provider", "broad_sector"])',
        ]
    )


def test_describe_names_the_default_provider_group_and_the_headline_basis() -> None:
    text = repr(oda.describe("oecd.dac1.total_oda"))
    assert "Providers by default: DAC countries (oda.groups.dac_countries)" in text
    assert "Bases: headline (default), net_disbursement," in text


def test_describe_says_when_a_sum_across_providers_leaves_out_eu_institutions() -> None:
    clause = "EU Institutions (918) are left out of a sum across providers"
    imputed = repr(_discover._info(multisystem.IMPUTED_MULTILATERAL_ODA))
    assert f"aggregates excluded; {clause} and shown when split by provider" in imputed
    # dac_countries holds no 918, so the clause stays out on a flagged binding.
    total = oda.describe("oecd.dac1.total_oda")
    assert total.leaves_out_eu_institutions is False
    assert clause not in repr(total)


def test_describe_offers_only_the_bases_an_indicator_has_a_line_for() -> None:
    gni = oda.describe("oecd.dac1.gni")
    assert gni.bases == (("net_disbursement", None),)


#: Each indicator's bases in `indicators()` order, the default first, with the first
#: year of a basis that starts late as `@year`.
_BASES: Mapping[str, str] = {
    "oecd.dac1.total_oda": "headline, net_disbursement, gross_disbursement, commitment, grant_equivalent@2018",
    "oecd.dac1.bilateral_oda": "headline, net_disbursement, gross_disbursement, commitment, grant_equivalent@2018",
    "oecd.dac1.multilateral_oda": "headline, net_disbursement, gross_disbursement, commitment, grant_equivalent@2018",
    "oecd.dac1.in_donor_refugee_costs": "headline, net_disbursement, gross_disbursement, commitment, grant_equivalent@2018",
    "oecd.dac1.in_donor_student_costs": "headline, net_disbursement, gross_disbursement, commitment, grant_equivalent@2018",
    "oecd.dac1.debt_relief": "headline, net_disbursement, gross_disbursement, commitment, grant_equivalent@2018",
    "oecd.dac1.administrative_costs": "headline, net_disbursement, gross_disbursement, commitment, grant_equivalent@2018",
    "oecd.dac1.development_awareness": "headline, net_disbursement, gross_disbursement, commitment, grant_equivalent@2018",
    "oecd.dac1.private_sector_instruments": "headline, net_disbursement, gross_disbursement, commitment, grant_equivalent@2018",
    "oecd.dac1.gni": "net_disbursement",
    "oecd.dac1.population": "net_disbursement",
    "oecd.dac1.oda_gni_ratio": "headline, net_disbursement, gross_disbursement, commitment, grant_equivalent@2018",
    "oecd.dac1.lines": "net_disbursement, headline, gross_disbursement, commitment, grant_equivalent@2015",
    "oecd.dac2a.recipient_oda": "headline, net_disbursement, gross_disbursement",
    "oecd.dac2a.imputed_multilateral_oda": "headline, net_disbursement",
    "oecd.dac2a.technical_cooperation": "headline, net_disbursement",
    "oecd.dac2a.lines": "net_disbursement, gross_disbursement",
    "oecd.crs.bilateral_oda": "gross_disbursement, commitment, net_disbursement, grant_equivalent@2018",
    "oecd.crs.multilateral_oda": "gross_disbursement, commitment, net_disbursement",
    "oecd.crs.bilateral_oof": "gross_disbursement, commitment, net_disbursement",
    "oecd.crs.multilateral_oof": "gross_disbursement, commitment, net_disbursement",
    "oecd.crs.private_development_finance": "gross_disbursement, commitment, net_disbursement",
    "oecd.crs.gender_marker": "gross_disbursement@2010, commitment@2010, net_disbursement@2010, grant_equivalent@2018",
    "oecd.crs.environment_marker": "gross_disbursement@2010, commitment@2010, net_disbursement@2010, grant_equivalent@2018",
    "oecd.crs.dig_marker": "gross_disbursement@2010, commitment@2010, net_disbursement@2010, grant_equivalent@2018",
    "oecd.crs.trade_marker": "gross_disbursement@2010, commitment@2010, net_disbursement@2010, grant_equivalent@2018",
    "oecd.crs.rmnch_marker": "gross_disbursement@2010, commitment@2010, net_disbursement@2010, grant_equivalent@2018",
    "oecd.crs.drr_marker": "gross_disbursement@2010, commitment@2010, net_disbursement@2010, grant_equivalent@2018",
    "oecd.crs.nutrition_marker": "gross_disbursement@2010, commitment@2010, net_disbursement@2010, grant_equivalent@2018",
    "oecd.crs.disability_marker": "gross_disbursement@2010, commitment@2010, net_disbursement@2010, grant_equivalent@2018",
    "oecd.crs.biodiversity_marker": "gross_disbursement@2010, commitment@2010, net_disbursement@2010, grant_equivalent@2018",
    "oecd.crs.climate_mitigation_marker": "gross_disbursement@2010, commitment@2010, net_disbursement@2010, grant_equivalent@2018",
    "oecd.crs.climate_adaptation_marker": "gross_disbursement@2010, commitment@2010, net_disbursement@2010, grant_equivalent@2018",
    "oecd.crs.desertification_marker": "gross_disbursement@2010, commitment@2010, net_disbursement@2010, grant_equivalent@2018",
    "oecd.multisystem.core_contributions": "gross_disbursement, commitment",
    "one.crs.imputed_multilateral_oda": "gross_disbursement, commitment",
    "one.dac1.core_oda": "headline, net_disbursement, gross_disbursement, commitment, grant_equivalent@2018",
    "one.dac1.non_core_oda": "headline, net_disbursement, gross_disbursement, commitment, grant_equivalent@2018",
    "one.dac2a.recipient_oda_with_imputed_multilateral": "headline, net_disbursement",
    "one.dac1.total_oda_eu_attributed": "headline, net_disbursement, gross_disbursement, commitment, grant_equivalent@2018",
    "one.dac1.gni": "net_disbursement",
    "one.dac1.gni_fixed_eu27": "net_disbursement",
    "one.dac1.oda_gni_ratio": "headline, net_disbursement, gross_disbursement, commitment, grant_equivalent@2018",
    "one.dac1.oda_gni_ratio_fixed_eu27": "headline, net_disbursement, gross_disbursement, commitment, grant_equivalent@2018",
    "one.crs.covid_oda": "gross_disbursement, commitment, net_disbursement, grant_equivalent@2018",
    "one.crs.bilateral_plus_multilateral_oda": "gross_disbursement, commitment",
}


def test_describe_offers_the_same_bases_for_every_shipped_indicator() -> None:
    found = {
        info.id: ", ".join(
            basis if year is None else f"{basis}@{year}" for basis, year in info.bases
        )
        for info in map(_discover._info, _registry.INDICATORS.values())
    }
    assert found == _BASES


def test_describe_gives_a_ratio_its_numerators_bases_and_per_cent_values() -> None:
    ratio = oda.describe("oecd.dac1.oda_gni_ratio")
    assert ratio.kind == "ratio"
    assert ratio.bases == oda.describe("oecd.dac1.total_oda").bases
    assert "Values: per cent" in repr(ratio)
    assert isinstance(REGISTRY["oecd.dac1.oda_gni_ratio"].kind, Ratio)


def test_describe_names_the_table_as_the_oecd_writes_it() -> None:
    info = _discover._info(multisystem.CORE_CONTRIBUTIONS)
    assert repr(info).splitlines()[0].endswith("(version 1, table Multisystem)")


def test_describe_unknown_id_suggests_close_ids() -> None:
    with pytest.raises(oda.UnknownIndicatorError) as caught:
        oda.describe("oecd.dac1.total_odaa")
    assert caught.value.suggestions[0] == "oecd.dac1.total_oda"


def test_indicator_info_is_frozen_and_prints_with_print() -> None:
    info = oda.describe("oecd.dac1.gni")
    with pytest.raises(dataclasses.FrozenInstanceError):
        info.id = "x"  # type: ignore[misc]
    assert str(info) == repr(info)
    assert hash(info) == hash(oda.describe("oecd.dac1.gni"))


# indicators -------------------------------------------------------------------


def test_indicators_schema_and_rows_follow_the_registry() -> None:
    frame = oda.indicators()
    assert list(frame.columns) == list(_discover.INDICATORS_SCHEMA)
    assert {c: str(t) for c, t in frame.dtypes.items()} == dict(
        _discover.INDICATORS_SCHEMA
    )
    assert list(frame["id"]) == list(REGISTRY)

    row = frame.set_index("id").loc["test.crs.bilateral_oda"]
    assert row["table"] == "crs"
    assert row["kind"] == "sum"
    assert row["dimensions"] == "provider, recipient, sector, broad_sector, year"
    assert row["bases"].startswith("gross_disbursement, ")
    assert row["default_basis"] == "gross_disbursement"
    assert row["version"] == 3


def test_indicators_of_an_empty_registry_keeps_the_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_context, "INDICATORS", {})
    frame = oda.indicators()
    assert frame.empty
    assert {c: str(t) for c, t in frame.dtypes.items()} == dict(
        _discover.INDICATORS_SCHEMA
    )


# search -----------------------------------------------------------------------


def test_search_schema_and_use_column() -> None:
    frame = oda.search("g7")
    assert {c: str(t) for c, t in frame.dtypes.items()} == dict(_discover.SEARCH_SCHEMA)
    row = frame.iloc[0]
    assert (row["kind"], row["key"]) == ("group", "g7")
    assert row["use"] == "providers=oda.groups.g7"
    assert pd.isna(row["table"])


def test_search_covers_indicators_groups_sector_groups_and_lines() -> None:
    kinds = set(oda.search("health")["kind"]) | set(oda.search("total")["kind"])
    assert {"indicator", "sector_group", "line"} <= kinds
    assert set(oda.search("africa")["kind"]) >= {"group"}


def test_search_ranks_exact_then_prefix_then_contains_then_description() -> None:
    frame = oda.search("bilateral oda")
    first = frame.iloc[0]
    assert first["kind"] == "indicator"
    assert first["key"] in {"oecd.dac1.bilateral_oda", "test.crs.bilateral_oda"}

    # "activity" is in the CRS description only.
    assert list(oda.search("activity")["key"]) == ["test.crs.bilateral_oda"]

    exact = oda.search("Total ODA")
    assert exact.iloc[0]["key"] == "oecd.dac1.total_oda"


def test_search_matches_each_word_in_any_case_and_order_of_words() -> None:
    assert "oecd.dac1.total_oda" in set(oda.search("ODA total")["key"])
    assert "oecd.dac1.total_oda" in set(oda.search("oecd.dac1.TOTAL_oda")["key"])


def test_search_lists_a_line_once_for_each_table_that_has_it() -> None:
    lines = oda.search("207")
    lines = lines[(lines["kind"] == "line") & (lines["key"] == "207")]
    assert sorted(lines["table"]) == ["dac1", "dac2a"]
    assert list(lines["use"]) == ["lines=207", "lines=207"]


@pytest.mark.parametrize(
    ("table", "line", "use"),
    [
        ("dac2a", "206", "lines=206"),
        ("dac2a", "240", 'lines=240, basis="gross_disbursement"'),
        ("dac2a", "204", 'lines=204, basis="gross_disbursement"'),
        ("dac1", "1010", "lines=1010"),
        ("dac1", "11010", 'lines=11010, basis="grant_equivalent"'),
        ("dac1", "12100", 'lines=12100, basis="grant_equivalent"'),
    ],
)
def test_search_names_the_basis_a_line_needs(table: str, line: str, use: str) -> None:
    found = oda.search(line)
    row = found[(found["kind"] == "line") & (found["table"] == table)]
    assert list(row[row["key"] == line]["use"]) == [use]


def test_search_basis_for_a_named_line_is_one_its_indicators_read() -> None:
    """Whatever basis a line row names, an indicator on that line reads it there."""
    read: dict[tuple[str, int], set[str]] = {}
    for indicator in REGISTRY.values():
        for basis, line in basis_lines(indicator).items():
            read.setdefault((indicator.binding.table, line), set()).add(basis)
    checked = 0
    for entry in _discover._entries():
        bases = (
            read.get((entry.table, int(entry.key))) if entry.kind == "line" else None
        )
        if bases is None:
            continue
        match = re.search(r'basis="(\w+)"', entry.use)
        assert (match.group(1) if match else "net_disbursement") in bases, entry.use
        checked += 1
    assert checked > 10


def test_search_leaves_out_lines_the_lines_indicators_refuse() -> None:
    listed = {(e.table, e.key) for e in _discover._entries() if e.kind == "line"}
    refused = {("dac1", str(code)) for code in dac1.NON_MONEY_LINES}
    refused |= {("dac2a", str(code)) for code in dac2a.NON_MONEY_LINES}
    assert len(refused) == 6
    assert not listed & refused
    assert ("dac1", "1") in listed  # GNI is money, read on net flows


@pytest.mark.parametrize(
    ("text", "kind", "key", "use"),
    [
        ("aid effort", "indicator", "oecd.dac1.oda_gni_ratio", None),
        ("0.7", "indicator", "oecd.dac1.oda_gni_ratio", None),
        ("Sub-Saharan", "recipient", "10003", "recipients=10003"),
        ("sub-saharan africa", "recipient", "10003", "recipients=10003"),
        ("South of Sahara", "recipient", "10003", "recipients=10003"),
        ("Germany", "provider", "5", "providers=5"),
    ],
)
def test_search_answers_common_questions_and_names(
    text: str, kind: str, key: str, use: str | None
) -> None:
    first = oda.search(text).iloc[0]
    assert (first["kind"], first["key"]) == (kind, key)
    assert first["use"] == (use or f'oda.get("{key}")')


@pytest.mark.parametrize(
    ("text", "kind", "key"),
    [
        ("UK", "provider", "12"),  # what providers="UK" reads, not Ukraine
        ("USA", "provider", "302"),
        ("Ken", "recipient", "248"),  # a word start
        ("Sub", "recipient", "10003"),  # a word start of "Sub-Saharan Africa"
    ],
)
def test_search_first_entity_row_is_what_a_filter_reads(
    text: str, kind: str, key: str
) -> None:
    found = oda.search(text)
    entities = found[found["kind"].isin(["provider", "recipient"])]
    assert (entities.iloc[0]["kind"], entities.iloc[0]["key"]) == (kind, key)


def test_search_takes_a_digit_that_is_not_a_decimal_code() -> None:
    assert len(oda.search("²")) > 0  # resolving "²" as a code raised ValueError


def test_search_matches_entity_codes_exactly() -> None:
    found = oda.search("1010")
    assert not found["kind"].isin(["provider", "recipient"]).any()


def test_search_lists_providers_and_recipients_after_every_other_row() -> None:
    kinds = list(oda.search("total")["kind"])
    entities = [i for i, kind in enumerate(kinds) if kind in {"provider", "recipient"}]
    others = [
        i for i, kind in enumerate(kinds) if kind not in {"provider", "recipient"}
    ]
    assert entities and others
    assert max(others) < min(entities)


def test_region_search_terms_name_recipient_totals() -> None:
    totals = {
        code
        for code in _discover._REGION_SEARCH_TERMS
        if code in _names.names("recipient") and _names.is_aggregate("recipient", code)
    }
    assert totals == set(_discover._REGION_SEARCH_TERMS)


def test_search_without_a_match_returns_the_empty_schema() -> None:
    frame = oda.search("zzzz no such thing")
    assert frame.empty
    assert list(frame.columns) == list(_discover.SEARCH_SCHEMA)


@pytest.mark.parametrize(
    ("text", "error"), [("", ValueError), ("   ", ValueError), (5, TypeError)]
)
def test_search_refuses_blank_and_non_text(text: object, error: type[Exception]):
    with pytest.raises(error):
        oda.search(text)  # type: ignore[arg-type]


def test_search_puts_2x_code_rows_above_every_other_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    row = _discover._Entry(
        "code", "CRS.P.10", "2.x code", None, 'oda.get("oecd.crs.x")'
    )
    monkeypatch.setattr(_discover, "_legacy_rows", lambda text: [row])
    frame = oda.search("g7")
    assert frame.iloc[0]["kind"] == "code"
    assert frame.iloc[1]["key"] == "g7"


# add_names --------------------------------------------------------------------


def _codes() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "provider": ["4", "dac_countries", "999999"],
            "recipient": ["625", "10100", "1"],
            "sector": ["12220", "health", None],
            "broad_sector": ["health", "education", "nope"],
            "channel": ["41114", "0", "1"],
            "modality": ["B01", "A01", "ZZ"],
            "finance_type": ["110", "421", "0"],
            "flow": ["11", "13", "0"],
            "value": [1.0, 2.0, 3.0],
        }
    )


def test_add_names_puts_each_name_beside_its_code() -> None:
    named = oda.add_names(_codes())
    columns = list(named.columns)
    for dimension in [
        "provider",
        "recipient",
        "sector",
        "broad_sector",
        "channel",
        "modality",
        "finance_type",
        "flow",
    ]:
        assert columns.index(f"{dimension}_name") == columns.index(dimension) + 1
        assert str(named[f"{dimension}_name"].dtype) == "string"
    assert columns[-1] == "value"


@pytest.mark.parametrize(
    "dimension",
    [
        "provider",
        "recipient",
        "sector",
        "broad_sector",
        "channel",
        "modality",
        "finance_type",
        "flow",
    ],
)
def test_add_names_uses_the_names_results_carry(dimension: str) -> None:
    known = _context._string_names(dimension, "crs")
    codes = list(known)[:50]
    named = oda.add_names(pd.DataFrame({dimension: codes}))
    for code, name in zip(codes, named[f"{dimension}_name"], strict=True):
        # A group label overrides a name only for a built-in group's key.
        assert name == known[code]


def test_add_names_names_groups_and_leaves_unknown_and_null_codes_null() -> None:
    named = oda.add_names(_codes())
    assert named.loc[1, "provider_name"] == "DAC countries"
    assert named.loc[1, "sector_name"] == oda.sectors.health.label
    assert pd.isna(named.loc[2, "provider_name"])
    assert pd.isna(named.loc[2, "sector_name"])


def test_add_names_accepts_integer_codes() -> None:
    named = oda.add_names(pd.DataFrame({"provider": [4, 5]}))
    assert list(named["provider_name"]) == [
        _context._string_names("provider", "crs")["4"],
        _context._string_names("provider", "crs")["5"],
    ]


def test_add_names_keeps_an_existing_name_column_and_the_original_frame() -> None:
    frame = _codes()[["provider"]].assign(provider_name=["a", "b", "c"])
    before = frame.copy()
    named = oda.add_names(frame)
    assert list(named["provider_name"]) == ["a", "b", "c"]
    pd.testing.assert_frame_equal(frame, before)


def test_add_names_takes_each_rows_line_table_from_its_indicator() -> None:
    frame = pd.DataFrame(
        {
            "indicator": ["oecd.dac1.lines", "test.dac2a.x", "test.crs.bilateral_oda"],
            "line": ["207", "207", "207"],
        }
    )
    other = Indicator(
        id="test.dac2a.x",
        name="x",
        description="x",
        binding=Binding(
            table="dac2a",
            dimensions={"provider": "provider", "line": "line", "year": "year"},
            bases={"net_disbursement": Value("value")},
        ),
        kind=Sum(),
        dimensions=("provider", "line", "year"),
        default_basis="net_disbursement",
        default_providers=None,
    )
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(_context, "INDICATORS", {**REGISTRY, other.id: other})
        named = oda.add_names(frame)
    dac1_name = _context._string_names("line", "dac1")["207"]
    dac2a_name = _context._string_names("line", "dac2a")["207"]
    assert dac1_name != dac2a_name
    assert named.loc[0, "line_name"] == dac1_name
    assert named.loc[1, "line_name"] == dac2a_name
    assert pd.isna(named.loc[2, "line_name"])  # CRS has no lines


def test_add_names_names_lines_from_the_table_argument() -> None:
    named = oda.add_names(pd.DataFrame({"line": ["1010"]}), table="dac1")
    assert named.loc[0, "line_name"] == _context._string_names("line", "dac1")["1010"]


def test_add_names_refuses_lines_without_a_table() -> None:
    with pytest.raises(ValueError, match="table"):
        oda.add_names(pd.DataFrame({"line": ["1010"]}))
    with pytest.raises(ValueError, match="table"):
        oda.add_names(pd.DataFrame({"line": ["1010"]}), table="crs")  # type: ignore[arg-type]


def test_add_names_refuses_a_non_frame() -> None:
    with pytest.raises(TypeError):
        oda.add_names([1, 2])  # type: ignore[arg-type]


def test_add_names_ignores_frames_with_no_code_columns() -> None:
    frame = pd.DataFrame({"year": [2023], "value": [1.0]})
    pd.testing.assert_frame_equal(oda.add_names(frame), frame)


# import cost ------------------------------------------------------------------


def test_importing_the_module_leaves_pandas_unloaded() -> None:
    code = "import sys, oda_data._discover; print('pandas' in sys.modules)"
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    )
    assert result.stdout.strip() == "False"

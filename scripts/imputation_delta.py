"""Compare oda-data 2.8.2 with 3.0 on a fixed set of calls, as a Markdown table.

Both versions read the same cached OECD tables: 3.0 through its own views,
2.8.2 through ``scripts/v2_reference.py``, which feeds 2.x's bulk readers the
same Parquet files. A difference is a counting rule or a 2.x defect.
Each row names the 2.x code, its status in the 2.x map, and the
map's reason, so the table can go straight into the migration guide's
"numbers that change":

    export ODA_READER_CACHE_DIR=~/Library/Caches/oda-data-v3-shared
    uv run python scripts/imputation_delta.py > delta.md

Values are current USD millions. ODA/GNI is in per cent. 2.x returns a
fraction, which the table shows multiplied by 100.
"""

import argparse
import ast
import json
import math
import sys
import types
from dataclasses import dataclass

import pandas as pd
from v2_reference import cached_tables, run_in_2x

import oda_data as oda
from oda_data import _compat, _context, _names

FRANCE, NORWAY, EU_INSTITUTIONS, IDA, UKRAINE = 4, 8, 918, 905, 85


@dataclass(frozen=True)
class Delta:
    """One compared call.

    Attributes:
        label: What the row counts, for the table.
        code: The 2.x code whose map row gives the status and reason.
        request: The 2.x call, as ``scripts/v2_reference.py`` takes it.
        arguments: The ``years``, ``providers`` and ``recipients`` given to
            the 3.0 call.
        call: A 3.0 call to run in place of the map row's, as Python lines.
        status: The status to show in place of the map row's, for a call that
            is not the map row's.
        reason: The reason to show where the map has no row for ``code``.
        scale: What 2.x's value is multiplied by, 100 for a fraction.
    """

    label: str
    code: str
    request: dict
    arguments: dict
    call: tuple[str, ...] = ()
    status: str | None = None
    reason: str = ""
    scale: float = 1.0


def _indicator(code: str, **arguments: object) -> dict:
    return {"function": "indicator", "code": code, "arguments": arguments}


def _crs(code: str) -> dict:
    return _indicator(
        code, years=[2022], providers=[FRANCE], measure=["gross_disbursement"]
    )


_BOTH = ["net_disbursement", "grant_equivalent"]
_FRANCE_2022 = {"years": [2022], "providers": [FRANCE]}
_FRANCE_2015_2022 = {"years": [2015, 2022], "providers": [FRANCE]}
_FRANCE_2023 = {"years": [2023], "providers": [FRANCE]}

DELTAS = (
    Delta(
        "Headline total ODA, France",
        "ONE.10.1010_11010",
        _indicator("ONE.10.1010_11010", **_FRANCE_2015_2022, measure=_BOTH),
        _FRANCE_2015_2022,
    ),
    Delta(
        "Total ODA, net flows, EU Institutions",
        "DAC1.10.1010",
        _indicator("DAC1.10.1010", years=[2022], providers=[EU_INSTITUTIONS]),
        {"years": [2022], "providers": [EU_INSTITUTIONS]},
    ),
    Delta(
        "ODA/GNI, France",
        "ONE.40.1010_11010_1",
        _indicator("ONE.40.1010_11010_1", **_FRANCE_2015_2022, measure=_BOTH),
        _FRANCE_2015_2022,
        scale=100,
    ),
    Delta(
        "ODA/GNI, EU Institutions",
        "ONE.40.1010_11010_1",
        _indicator(
            "ONE.40.1010_11010_1",
            years=[2022],
            providers=[EU_INSTITUTIONS],
            measure=_BOTH,
        ),
        {"years": [2022], "providers": [EU_INSTITUTIONS]},
        scale=100,
    ),
    Delta(
        "EU Institutions' attributed ODA, net flows",
        "get_eui_plus_bilateral_providers_indicator",
        {
            "function": "eu_attributed",
            "code": "DAC1.10.1010",
            "arguments": {
                "years": [2019],
                "providers": [*sorted(_names.eu27()), EU_INSTITUTIONS],
                "measure": ["net_disbursement"],
            },
        },
        {"years": [2019], "providers": [EU_INSTITUTIONS]},
        call=('oda.get("one.dac1.total_oda_eu_attributed", basis="net_disbursement")',),
        status="changes",
        reason="2.x netted the line 2102 of today's EU27 only; 3.0 nets that of "
        "every provider other than 918.",
    ),
    Delta(
        "DAC2A bilateral ODA, France to Ukraine",
        "DAC2A.10.206",
        _indicator("DAC2A.10.206", **_FRANCE_2022, recipients=[UKRAINE]),
        {**_FRANCE_2022, "recipients": [UKRAINE]},
    ),
    Delta(
        "DAC2A bilateral ODA, France, summed over recipients",
        "DAC2A.10.206",
        _indicator("DAC2A.10.206", **_FRANCE_2022),
        _FRANCE_2022,
    ),
    Delta(
        "DAC2A imputed multilateral ODA, France, summed over recipients",
        "DAC2A.10.106",
        _indicator("DAC2A.10.106", **_FRANCE_2022),
        _FRANCE_2022,
    ),
    Delta("CRS bilateral ODA, France", "CRS.P.10", _crs("CRS.P.10"), _FRANCE_2022),
    Delta(
        "CRS bilateral ODA, IDA",
        "CRS.P.10",
        _indicator(
            "CRS.P.10", years=[2022], providers=[IDA], measure=["gross_disbursement"]
        ),
        {"years": [2022], "providers": [IDA]},
    ),
    Delta(
        "CRS bilateral ODA, grant equivalents, France",
        "CRS.P.10",
        _indicator("CRS.P.10", **_FRANCE_2023, measure=["grant_equivalent"]),
        {**_FRANCE_2023, "basis": "grant_equivalent"},
    ),
    Delta(
        "CRS ODA and other official flows to education, every category, France",
        "CRS.P.T.T.T.110",
        _crs("CRS.P.T.T.T.110"),
        _FRANCE_2022,
    ),
    Delta(
        "CRS private development finance, all recipients",
        "CRS.R.30",
        _indicator("CRS.R.30", years=[2022], measure=["gross_disbursement"]),
        {"years": [2022]},
    ),
    Delta(
        "Gender marker, principal, Norway",
        "bilateral_policy_marker",
        {
            "function": "bilateral_policy_marker",
            "arguments": {
                "years": [2022],
                "providers": [NORWAY],
                "marker": "gender",
                "marker_score": "principal",
            },
        },
        {"years": [2022], "providers": [NORWAY]},
        call=(
            'df = oda.get("oecd.crs.gender_marker", by=["provider", "score", "year"])',
            'df = df[df["score"] == "principal"]',
        ),
        status="exact",
    ),
    Delta(
        "CRS bilateral ODA grants to education projects, France",
        "CRS.P.10.100.C.110",
        _crs("CRS.P.10.100.C.110"),
        _FRANCE_2022,
    ),
    Delta(
        "CRS bilateral ODA grants to health projects, France",
        "CRS.P.10.100.C.120",
        _crs("CRS.P.10.100.C.120"),
        _FRANCE_2022,
    ),
    Delta(
        "Imputed multilateral ODA (CRS), France",
        "sector_imputations",
        {"function": "imputed_multilateral_by_purpose", "arguments": _FRANCE_2022},
        _FRANCE_2022,
        call=('oda.get("one.crs.imputed_multilateral_oda")',),
        status="exact",
        reason="2.x's imputed_multilateral_by_purpose became "
        "one.crs.imputed_multilateral_oda, which gives the same rows.",
    ),
    Delta(
        "Core ODA, net flows, France",
        "ONE.10.1010C",
        _indicator("ONE.10.1010C", **_FRANCE_2015_2022),
        _FRANCE_2015_2022,
    ),
    Delta(
        "Core ODA, 3.0 headline basis against 2.x net flows, France",
        "ONE.10.1010C",
        _indicator("ONE.10.1010C", **_FRANCE_2015_2022),
        _FRANCE_2015_2022,
        call=('oda.get("one.dac1.core_oda")',),
        status="changes",
    ),
)


def run_3x(
    context: oda.Context, lines: tuple[str, ...], arguments: dict
) -> pd.DataFrame:
    """Run a 3.0 call written as Python lines on ``context``, every ``oda.get``
    taking ``arguments`` too, ``recipients`` only where the indicator has
    recipients. A one-line call is an expression. A longer one leaves its
    result in ``df``."""

    def get(indicator: str, /, **given: object) -> pd.DataFrame:
        extra = dict(arguments)
        if "recipient" not in _context.find_indicator(indicator).dimensions:
            extra.pop("recipients", None)
        return context.get(indicator, **extra, **given)

    shim = types.SimpleNamespace(get=get, sectors=oda.sectors, groups=oda.groups)
    namespace: dict[str, object] = {"oda": shim, "pd": pd}
    *body, last = ast.parse("\n".join(lines)).body
    exec(compile(ast.Module(body, []), "<call>", "exec"), namespace)
    if isinstance(last, ast.Expr):
        return eval(compile(ast.Expression(last.value), "<call>", "eval"), namespace)
    exec(compile(ast.Module([last], []), "<call>", "exec"), namespace)
    found = namespace["df"]
    assert isinstance(found, pd.DataFrame)
    return found


def _by_year(frame: pd.DataFrame) -> dict[int, float]:
    if frame.empty:
        return {}
    summed = frame.groupby("year")["value"].sum()
    return {int(year): float(value) for year, value in summed.items()}


def _number(value: float | None) -> str:
    if value is None:
        return "no rows"
    if math.isnan(value):
        return "null"
    if abs(value) < 10:
        return f"{value:,.4f}"
    return f"{value:,.2f}"


def delta_table(rows: list[dict]) -> str:
    """The Markdown table of ``rows``, one line each."""
    lines = [
        "| Call | 2.x code | Year | 2.8.2 | 3.0 | Difference | Status | Reason |",
        "| --- | --- | ---: | ---: | ---: | ---: | --- | --- |",
    ]
    for row in rows:
        old, new = row["old"], row["new"]
        both = old is not None and new is not None and not math.isnan(old - new)
        cells = [
            row["label"],
            f"`{row['code']}`",
            str(row["year"]),
            _number(old),
            _number(new),
            _number(0.0 if round(new - old, 4) == 0 else new - old) if both else "n/a",
            row["status"],
            row["reason"],
        ]
        lines.append("| " + " | ".join(c.replace("|", "\\|") for c in cells) + " |")
    return "\n".join(lines)


def compare(version: str) -> tuple[list[dict], dict[str, str]]:
    """Run every ``DELTAS`` call in 2.x and 3.0 on the cached tables, and
    return one row per call and year, and the releases read."""
    paths, releases = cached_tables()
    calls = [
        {"id": str(index), **delta.request, "by": ["year"]}
        for index, delta in enumerate(DELTAS)
    ]
    old = run_in_2x(version, paths, calls)
    rows = []
    with oda.Context(offline=True, releases=releases) as context:
        for index, delta in enumerate(DELTAS):
            mapped = _compat.v2_map().get(delta.code)
            lines = delta.call or (mapped.call if mapped else ())
            new = _by_year(run_3x(context, lines, delta.arguments))
            before = {
                int(year): value * delta.scale
                for year, value in old[str(index)]["rows"].items()
            }
            rows += [
                {
                    "label": delta.label,
                    "code": delta.code,
                    "year": year,
                    "old": before.get(year),
                    "new": new.get(year),
                    "status": delta.status or (mapped.status if mapped else ""),
                    "reason": mapped.reason if mapped else delta.reason,
                }
                for year in delta.arguments["years"]
            ]
    return rows, releases


def main(argv: list[str] | None = None) -> int:
    """Print the comparison as a Markdown table and the releases it read."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--version", default="2.8.2", help="the oda-data 2.x release")
    parser.add_argument(
        "--json", action="store_true", help="print the rows as JSON, not Markdown"
    )
    args = parser.parse_args(argv)

    rows, releases = compare(args.version)
    if args.json:
        json.dump(rows, sys.stdout, indent=2)
        return 0
    print(delta_table(rows))
    print()
    tokens = ", ".join(f"{name} `{token}`" for name, token in releases.items())
    print(f"Releases: {tokens}. oda-data {args.version} against {oda.__version__}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

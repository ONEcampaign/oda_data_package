"""The public entry points: ``Context``, ``Query``, ``get``, ``query``, ``records``.

A ``Context`` holds one ``OecdTables`` (a DuckDB connection over oda-reader's
cached tables) and the conversion factors built from that context's DAC1
release. Each call takes its own cursor, so one context serves many threads.
The module functions use one default context, built on first use under a lock.

A call runs in four steps:

1. The indicator is looked up in the registry (``find_indicator``).
2. Its arguments become a ``ResolvedCall``: filters through
   ``_resolve.resolve_selection``, defaults for basis, unit, and providers, and
   group membership for every year the call reads.
3. ``_combine.combine`` (or ``_combine.records``) computes the rows.
4. The rows take the result schema (``result_schema``): a name column beside
   each code, and the dtypes the public API promises.
"""

from __future__ import annotations

import difflib
import hashlib
import threading
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from functools import cache, partial
from importlib.metadata import version
from importlib.resources import files
from pathlib import Path
from typing import TYPE_CHECKING, BinaryIO, Protocol, Self, get_args

import duckdb
import pyarrow as pa

from oda_data import _compat
from oda_data._combine import (
    LISTED,
    caveats_for,
    check_left_out,
    combine,
    tables_for,
)
from oda_data._combine import records as combined_records
from oda_data._compile import (
    BROAD_SECTORS,
    UNLABELLED_CODE,
    check_basis,
    ident,
    literals,
)
from oda_data._errors import (
    DimensionError,
    UnknownEntityError,
    UnknownIndicatorError,
    UnsupportedQueryError,
)
from oda_data._factors import converts, pydeflate_currency
from oda_data._groups import broad_sectors, groups
from oda_data._model import Flow, Headline, NotIn, Ratio, ResolvedCall, Sum, Tables
from oda_data._names import EU_INSTITUTIONS, is_aggregate, names
from oda_data._provenance import Provenance, call_record, with_provenance
from oda_data._registry import INDICATORS
from oda_data._resolve import (
    Selection,
    group_codes,
    resolve_group,
    resolve_selection,
)
from oda_data._rows import bindings, walk
from oda_data._store import Releases, Store
from oda_data._tables import OecdTables
from oda_data._types import Dimension
from oda_data._units import Unit, current

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl
    from duckdb import DuckDBPyConnection

    from oda_data._groups import ReportedAt, SectorGroup
    from oda_data._model import Factors, Indicator
    from oda_data._provenance import Release
    from oda_data._types import (
        Basis,
        Code,
        Entity,
        IndicatorId,
        Many,
        TableName,
        Years,
    )

# Result schema ----------------------------------------------------------------

_CATEGORY = pa.dictionary(pa.int32(), pa.string())

#: The columns after the dimensions, in order, and their Arrow types.
_FIXED: Mapping[str, pa.DataType] = {
    "indicator": _CATEGORY,
    "indicator_version": pa.int64(),
    "basis": _CATEGORY,
    "currency": _CATEGORY,
    "prices": _CATEGORY,
    "base_year": pa.int64(),
    "scale": _CATEGORY,
    "is_aggregate": pa.bool_(),
    "value": pa.float64(),
}

#: The fixed columns of a ``records()`` result, which holds no totals.
_RECORD_FIXED = tuple(name for name in _FIXED if name != "is_aggregate")

#: Dimensions whose codes need no name column beside them.
_UNNAMED: frozenset[str] = frozenset({"year", "score", "allocation_status"})

#: The pandas dtype each Arrow type of the result schema converts to.
_PANDAS_DTYPES: Mapping[pa.DataType, str] = {
    _CATEGORY: "category",
    pa.int64(): "Int64",
    pa.string(): "string",
    pa.bool_(): "bool",
    pa.float64(): "float64",
}


def result_schema(dimensions: Iterable[str]) -> pa.Schema:
    """Return the Arrow schema of a result that splits by ``dimensions``.

    Each dimension's codes come first as strings (``year`` as int64), each
    followed by ``<dimension>_name``, except ``year``, ``score``, and
    ``allocation_status``. Then ``indicator``, ``indicator_version``,
    ``basis``, ``currency``, ``prices``, ``base_year``, ``scale``,
    ``is_aggregate``, and ``value``. Categories are dictionary columns, which
    pandas reads as ``category``.
    """
    fields = []
    for dimension in dimensions:
        if dimension == "year":
            fields.append(pa.field("year", pa.int64()))
            continue
        fields.append(pa.field(dimension, pa.string()))
        if dimension not in _UNNAMED:
            fields.append(pa.field(f"{dimension}_name", pa.string()))
    return pa.schema([*fields, *(pa.field(n, t) for n, t in _FIXED.items())])


def check_frame(frame: pd.DataFrame, schema: pa.Schema) -> None:
    """Check that ``frame`` has the columns and pandas dtypes of ``schema``.

    Raises:
        RuntimeError: A column is missing, extra, or of another dtype. This is
            a bug in oda-data.
    """
    if list(frame.columns) != schema.names:
        raise RuntimeError(
            f"result columns {list(frame.columns)} differ from the schema's "
            f"{schema.names}; this is a bug in oda-data"
        )
    for field in schema:
        expected = _PANDAS_DTYPES[field.type]
        found = str(frame[field.name].dtype)
        if found != expected:
            raise RuntimeError(
                f"result column {field.name!r} has dtype {found}, not {expected}; "
                "this is a bug in oda-data"
            )


def _to_pandas(table: pa.Table) -> pd.DataFrame:
    """Convert once, with nullable ints and strings, and categories."""
    import pandas as pd

    mapper = {pa.int64(): pd.Int64Dtype(), pa.string(): pd.StringDtype()}
    return table.to_pandas(types_mapper=mapper.get)


# Registry ---------------------------------------------------------------------


def find_indicator(indicator_id: str, /) -> Indicator:
    """Return the indicator with ``indicator_id`` from the registry.

    The registry, ``oda_data._registry.INDICATORS``, maps each id to its
    ``Indicator``.

    Raises:
        TypeError: ``indicator_id`` is not a string.
        UnknownIndicatorError: No indicator has that id. For an oda-data 2.x
            code or slug, the message and ``suggestions`` give the 3.0 call.
    """
    if not isinstance(indicator_id, str):
        raise TypeError(f"an indicator id is a string, not {indicator_id!r}")
    found = INDICATORS.get(indicator_id)
    if found is not None:
        return found
    legacy = _compat.find(indicator_id)
    if legacy is not None:
        raise UnknownIndicatorError(
            _compat.message(legacy),
            suggestions=(legacy.call_text,) if legacy.call else (),
        )
    near = difflib.get_close_matches(indicator_id, list(INDICATORS), n=3, cutoff=0.6)
    hint = f"; did you mean {', '.join(near)}?" if near else ""
    raise UnknownIndicatorError(
        f"no indicator is named {indicator_id!r}{hint}", suggestions=near
    )


# Requests ---------------------------------------------------------------------

#: Each filter argument and the dimension it filters, in explain() order.
_FILTERS: tuple[tuple[str, Dimension], ...] = (
    ("providers", "provider"),
    ("recipients", "recipient"),
    ("sectors", "sector"),
    ("channels", "channel"),
    ("modalities", "modality"),
    ("finance_types", "finance_type"),
    ("flows", "flow"),
    ("lines", "line"),
)

_USD = current("USD")

#: Each dimension by name.
_DIMENSIONS: Mapping[str, Dimension] = {d: d for d in get_args(Dimension.__value__)}


@dataclass(frozen=True, slots=True)
class _Request:
    """A call's arguments, checked and normalised.

    ``columns`` is ``None`` for ``get`` and ``query``, and the extra columns
    for ``records``.
    """

    indicator: Indicator
    years: tuple[int, ...] | None
    filters: Mapping[Dimension, object]
    by: tuple[Dimension, ...]
    basis: Basis | None
    units: tuple[Unit, ...] | None
    allow_partial: bool
    columns: tuple[str, ...] | None


def _request(
    indicator: str,
    *,
    years: object,
    filters: Mapping[str, object],
    by: object,
    basis: Basis | None,
    unit: object,
    allow_partial: bool,
    columns: object = None,
) -> _Request:
    found = find_indicator(indicator)
    passthrough = None if columns is None else _strings("columns", columns)
    if passthrough is not None:
        _check_passthrough(found, passthrough)
    used = {
        dimension: filters[argument]
        for argument, dimension in _FILTERS
        if filters.get(argument) is not None
    }
    split = found.dimensions if columns is not None else _dimensions(by)
    _check_dimensions(found, (*used, *split))
    return _Request(
        indicator=found,
        years=_years(years),
        filters=used,
        by=split,
        basis=basis,
        units=None if unit is None else _units(unit),
        allow_partial=allow_partial,
        columns=passthrough,
    )


#: Extra advice for a table that lacks a dimension, naming the argument that
#: gives the same split on that table. ``DimensionError`` prints it beside the
#: indicator that has the dimension.
_DIMENSION_HINTS: Mapping[tuple[TableName, Dimension], str] = {
    ("dac1", "finance_type"): "DAC1 splits grants from other flows with flows=, "
    "such as 1121 (disbursements, grants) and 1122 (non-grants)",
    ("dac2a", "finance_type"): "DAC2A splits grants from loans by line: use "
    "oecd.dac2a.lines with lines=, such as 201 (ODA grants) and 218 (net ODA "
    "loans), or 204 (gross ODA loans) with basis='gross_disbursement'",
}


def _check_dimensions(indicator: Indicator, used: Iterable[Dimension]) -> None:
    """Raise ``DimensionError`` when a call filters or splits by a dimension
    the indicator lacks, naming the table's own path where one exists and an
    indicator that has the dimension."""
    for dimension in dict.fromkeys(used):
        if dimension in indicator.dimensions:
            continue
        hints = [_DIMENSION_HINTS.get((indicator.binding.table, dimension))]
        holder = _holder(indicator, dimension)
        if holder is not None:
            hints.append(f"Use {holder.id} for figures by {_label(dimension)}")
        raise DimensionError(
            f"{indicator.id} has no {dimension!r} dimension; its dimensions are "
            f"{', '.join(indicator.dimensions)}"
            + "".join(f". {hint}" for hint in hints if hint)
        )


def _holder(indicator: Indicator, dimension: Dimension) -> Indicator | None:
    """The first indicator in the registry with ``dimension``, one on the
    indicator's own table first."""
    holders = [found for found in INDICATORS.values() if dimension in found.dimensions]
    table = indicator.binding.table
    same = [found for found in holders if found.binding.table == table]
    return next(iter(same or holders), None)


def _label(dimension: Dimension) -> str:
    return dimension.replace("_", " ")


def _check_passthrough(indicator: Indicator, columns: tuple[str, ...]) -> None:
    """Raise when a ``records()`` column has the name of a column the result
    already holds, such as ``channel_name`` beside ``channel``.

    The result would hold one of the two under that name, and the caller
    could not tell which.
    """
    dimensions = ("provider", *indicator.dimensions)
    produced = {
        *dimensions,
        *(f"{d}_name" for d in dimensions if d not in _UNNAMED),
        *_RECORD_FIXED,
    }
    clashes = [name for name in dict.fromkeys(columns) if name in produced]
    if clashes:
        raise UnsupportedQueryError(
            f"records() already returns {', '.join(map(repr, clashes))} for "
            f"{indicator.id}; drop {'it' if len(clashes) == 1 else 'them'} "
            "from columns="
        )


def _years(years: object) -> tuple[int, ...] | None:
    if years is None:
        return None
    values = (years,) if isinstance(years, int) else years
    if not isinstance(values, Iterable):
        raise TypeError(f"years takes an int or an iterable of ints, not {years!r}")
    found = tuple(values)
    if any(isinstance(y, bool) or not isinstance(y, int) for y in found):
        raise TypeError(f"years takes an int or an iterable of ints, not {years!r}")
    if not found:
        raise ValueError("years is empty; pass None for every year")
    return tuple(sorted(set(found)))


def _strings(argument: str, value: object) -> tuple[str, ...]:
    values = (value,) if isinstance(value, str) else value
    if not isinstance(values, Iterable):
        raise TypeError(f"{argument} takes a string or strings, not {value!r}")
    found = tuple(dict.fromkeys(values))
    strings = tuple(item for item in found if isinstance(item, str))
    if len(strings) != len(found):
        raise TypeError(f"{argument} takes a string or strings, not {value!r}")
    return strings


def _dimensions(by: object) -> tuple[Dimension, ...]:
    found = _strings("by", by)
    unknown = [name for name in found if name not in _DIMENSIONS]
    if unknown:
        raise DimensionError(
            f"{', '.join(map(repr, unknown))} is not a dimension; dimensions are "
            f"{', '.join(sorted(_DIMENSIONS))}"
        )
    return tuple(_DIMENSIONS[name] for name in found)


def _units(unit: object) -> tuple[Unit, ...]:
    values = (unit,) if isinstance(unit, Unit) else unit
    if not isinstance(values, Iterable):
        raise TypeError(f"unit takes a Unit or Units, not {unit!r}")
    found = tuple(dict.fromkeys(values))
    units = tuple(item for item in found if isinstance(item, Unit))
    if not units or len(units) != len(found):
        raise TypeError(
            f"unit takes a Unit or Units, such as oda.constant('EUR', 2023), "
            f"not {unit!r}"
        )
    for item in units:
        pydeflate_currency(item.currency)  # raises before any table is read
    return units


# Resolution -------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Plan:
    """A resolved call, the selections behind it, and the defaults it took.

    Args:
        call: The call ``_combine`` runs.
        selections: Each filtered dimension and its resolved filter.
        defaults: Each default the call relied on, for provenance.
        default_text: Each defaulted dimension and a phrase for explain().
    """

    call: ResolvedCall
    selections: Mapping[Dimension, Selection]
    defaults: Mapping[str, str]
    default_text: Mapping[Dimension, str]


def _plan(
    cursor: DuckDBPyConnection,
    request: _Request,
    tables: tuple[TableName, ...],
    releases: Releases,
) -> _Plan:
    """Turn a request into a ``ResolvedCall``.

    Groups take members for each year the call reads: its ``years``, or
    every year in its tables when it names none.
    """
    indicator = request.indicator
    table = indicator.binding.table
    read_years = request.years or _distinct(cursor, tables, "year", releases)

    selections = {
        dimension: resolve_selection(dimension, values, years=read_years, table=table)
        for dimension, values in request.filters.items()
    }
    members = {dimension: s.codes for dimension, s in selections.items()}
    defaults: dict[str, str] = {}
    default_text: dict[Dimension, str] = {}
    exclusions = _provider_exclusions(indicator)
    if exclusions:
        defaults["excluded_providers"] = "; ".join(
            f"{', '.join(map(str, rule.values))}{_scope_text(indicator, rule)}: "
            f"{rule.reason}"
            for rule in exclusions
        )

    if "provider" in indicator.dimensions and "provider" not in selections:
        codes, text = _default_providers(
            cursor, indicator, tables, read_years, by=request.by, releases=releases
        )
        members["provider"] = codes
        defaults["providers"] = indicator.default_providers or "official_providers"
        default_text["provider"] = text
    if "recipient" in indicator.dimensions and "recipient" not in selections:
        defaults["recipients"] = "all"
        default_text["recipient"] = (
            "every recipient in the table, regional and unallocated rows "
            "included (the default)"
        )

    basis = request.basis or indicator.default_basis
    if request.basis is None:
        defaults["basis"] = basis
    units = request.units
    if units is None:
        units = (_USD,) if indicator.is_money else ()
        if units:
            defaults["unit"] = "current USD"

    call = ResolvedCall(
        basis=basis,
        units=units,
        by=request.by,
        years=request.years,
        members=members,
        groups=tuple(group for s in selections.values() for group in s.groups),
        totals=tuple((d, name) for d, s in selections.items() for name in s.totals),
        allow_partial=request.allow_partial,
        rows={d: s.rows for d, s in selections.items()},
    )
    # A left-out code explains a line the call cannot read, so it goes first.
    check_left_out(indicator, call)
    _check_lines(cursor, indicator, call)
    return _Plan(call, selections, defaults, default_text)


def _check_lines(
    cursor: DuckDBPyConnection, indicator: Indicator, call: ResolvedCall
) -> None:
    """Raise when a line the call names is never published under a flow its
    basis reads, in any headline span the call covers.

    Applies to indicators that take ``line`` as a dimension over DAC1 flows,
    such as ``oecd.dac1.lines``. The check reads the whole table, so a line
    published under the flow in other years than those asked for passes.
    """
    lines = call.members.get("line")
    kind = indicator.kind
    if not lines or "line" not in indicator.dimensions:
        return
    if not isinstance(kind, Sum) or kind.lines:
        return
    check_basis(indicator, call)
    bases = indicator.binding.bases
    used = _concrete_bases(bases[call.basis], call)
    flows = {basis: bases[basis] for basis in used}
    if not flows or not all(isinstance(rule, Flow) for rule in flows.values()):
        return

    line = ident(indicator.binding.dimensions["line"])
    published = set(
        cursor.sql(
            f"SELECT DISTINCT {line}, flow FROM {ident(indicator.binding.table)} "
            f"WHERE {line} IN ({literals(sorted(lines, key=str))})"
        ).fetchall()
    )
    for basis, rule in flows.items():
        assert isinstance(rule, Flow)
        named = call.members.get("flow") if rule.parts else None
        read = sorted(named) if named else [rule.code]
        for code in sorted(lines, key=str):
            if any((code, flow) in published for flow in read):
                continue
            holders = [
                name
                for name, other in bases.items()
                if isinstance(other, Flow) and (code, other.code) in published
            ]
            named = _join([repr(name) for name in holders])
            where = (
                "; no basis publishes it"
                if not holders
                else f"; the {named} basis publishes it"
                if len(holders) == 1
                else f"; the {named} bases publish it"
            )
            within = f" (part of {call.basis!r})" if basis != call.basis else ""
            raise UnsupportedQueryError(
                f"{indicator.id} reads flow {', '.join(map(str, read))} on the "
                f"{basis!r} basis{within}, which publishes no line {code}{where}"
            )


def _concrete_bases(rule: object, call: ResolvedCall) -> list[Basis]:
    """The bases a call's basis reads over the years it covers."""
    if not isinstance(rule, Headline):
        return [call.basis]
    years = call.years
    used: list[Basis] = []
    if years is None or years[0] < rule.year:
        used.append(rule.before)
    if years is None or years[-1] >= rule.year:
        used.append(rule.after)
    return used


def _default_providers(
    cursor: DuckDBPyConnection,
    indicator: Indicator,
    tables: tuple[TableName, ...],
    years: tuple[int, ...],
    *,
    by: tuple[Dimension, ...],
    releases: Releases,
) -> tuple[frozenset[Code], str]:
    """The providers a call that names none reads, and a phrase for them.

    A named default is that group's members. ``None`` is every provider in
    the indicator's tables except aggregates and private donors. Either way
    the providers the indicator or an indicator it combines excludes are
    dropped, since naming one raises.

    Where ``sums_leave_out_eu_institutions`` holds, a call that sums
    providers (``provider`` outside ``by``) leaves EU Institutions (918) out.
    A call split by provider keeps 918's row, so its provider rows may add up
    to more than the sum by 918's value.
    """
    name = indicator.default_providers
    if name is not None:
        group = groups[name]
        codes = _member_union(resolve_group(group, "provider", years=years).members)
        text = f"{len(codes)} {group.label} (the default)"
    else:
        private = _member_union(
            resolve_group(groups.private_donors, "provider", years=years).members
        )
        found = _distinct(cursor, tables, "provider", releases)
        codes = frozenset(c for c in found if not is_aggregate("provider", c))
        codes -= private
        text = (
            "every official provider in the table, aggregates and private donors "
            "excluded (the default)"
        )
    excluded = {
        code
        for rule in _provider_exclusions(indicator)
        if rule.when is None
        for code in rule.values
    }
    codes -= excluded
    if EU_INSTITUTIONS not in codes or not sums_leave_out_eu_institutions(indicator):
        return codes, text
    if "provider" in by:
        return codes, (
            f"{text}. EU Institutions (918) has its own row, and its members' "
            "figures hold what they pay it, so the provider rows add up to more "
            "than a sum across providers by 918's value"
        )
    return codes - {EU_INSTITUTIONS}, (
        f"{text}. EU Institutions (918) is left out of the sum, since its "
        "members' figures hold what they pay it; add 'provider' to by to see "
        "its row"
    )


def sums_leave_out_eu_institutions(indicator: Indicator) -> bool:
    """Whether a call that names no providers leaves EU Institutions (918) out
    of a sum across providers and shows its row when split by provider.

    It holds on an indicator with a provider dimension and a leaf binding
    whose members' figures hold what they pay 918, since a sum holding 918
    would count that money twice, when its default providers can hold 918
    (every official provider in the table, or a group with 918 as a member),
    and no exclusion drops 918 from every row. ``describe()`` states it and
    ``_default_providers`` applies it.
    """
    if "provider" not in indicator.dimensions:
        return False
    if not any(binding.members_hold_eu_share for binding in bindings(indicator)):
        return False
    if any(
        EU_INSTITUTIONS in rule.values and rule.when is None
        for rule in _provider_exclusions(indicator)
    ):
        return False
    name = indicator.default_providers
    return name is None or EU_INSTITUTIONS in group_codes(groups[name], "provider")


def _member_union(members: Mapping[int, frozenset[Code]]) -> frozenset[Code]:
    return frozenset().union(*members.values())


def _distinct(
    cursor: DuckDBPyConnection,
    tables: tuple[TableName, ...],
    column: str,
    releases: Releases,
) -> tuple[int, ...]:
    """The distinct non-null values of ``column`` across ``tables``, sorted,
    each table read once per held release."""
    found: set[int] = set()
    for table in tables:
        found.update(
            releases.get(
                table, ("distinct", column), partial(_scan, cursor, table, column)
            )
        )
    return tuple(sorted(found))


def _scan(cursor: DuckDBPyConnection, table: TableName, column: str) -> frozenset[int]:
    """The distinct non-null values of ``column`` in ``table``."""
    sql = f"SELECT DISTINCT {ident(column)} FROM {ident(table)}"
    return frozenset(v for (v,) in cursor.sql(sql).fetchall() if v is not None)


@cache
def _broad_sector_table() -> pa.Table:
    mapping = broad_sectors()
    return pa.table(
        {
            "sector": pa.array(list(mapping), pa.int64()),
            "broad_sector": pa.array(list(mapping.values()), pa.string()),
        }
    )


# Shaping ----------------------------------------------------------------------


def _shape_result(
    rows: pa.Table, indicator: Indicator, plan: _Plan
) -> tuple[pa.Table, pa.Schema]:
    """Add name columns to ``combine``'s rows and cast them to the schema."""
    dimensions = [name for name in rows.column_names if name not in _FIXED]
    schema = result_schema(dimensions)
    columns = _dimension_columns(rows, dimensions, indicator, plan)
    columns |= {name: rows[name] for name in _FIXED}
    return pa.table(columns).cast(schema), schema


def _shape_records(rows: pa.Table, indicator: Indicator, plan: _Plan) -> pa.Table:
    """Give ``records``' rows names, the indicator columns, and result dtypes.

    Dimension codes and names come first, then the extra columns the caller
    asked for, then the fixed columns except ``is_aggregate``.
    """
    known = {"provider", "line", *indicator.dimensions}
    dimensions = [name for name in rows.column_names if name in known]
    extras = [
        name for name in rows.column_names if name not in known and name not in _FIXED
    ]
    columns = _dimension_columns(rows, dimensions, indicator, plan)
    columns |= {name: rows[name] for name in extras}
    count = rows.num_rows
    fixed = {
        "indicator": pa.repeat(indicator.id, count),
        "indicator_version": pa.repeat(indicator.version, count),
        "basis": rows["basis"],
        "currency": rows["currency"],
        "prices": rows["prices"],
        "base_year": rows["base_year"],
        "scale": pa.repeat("millions", count),
        "value": rows["value"],
    }
    table = pa.table(columns | fixed)
    types = {name: _FIXED[name] for name in _RECORD_FIXED}
    types |= {f.name: f.type for f in result_schema(dimensions) if f.name in columns}
    schema = pa.schema(
        [pa.field(f.name, types.get(f.name, f.type)) for f in table.schema]
    )
    return table.cast(schema)


def _dimension_columns(
    rows: pa.Table, dimensions: list[str], indicator: Indicator, plan: _Plan
) -> dict[str, pa.ChunkedArray | pa.Array]:
    columns: dict[str, pa.ChunkedArray | pa.Array] = {}
    for dimension in dimensions:
        if dimension == "year":
            columns["year"] = rows["year"].cast(pa.int64())
            continue
        codes = rows[dimension].cast(pa.string())
        columns[dimension] = codes
        if dimension not in _UNNAMED:
            known = _string_names(dimension, indicator.binding.table)
            selection = plan.selections.get(dimension)
            if selection is not None:
                known = {**known, **selection.labels}
            columns[f"{dimension}_name"] = pa.array(
                [known.get(code) for code in codes.to_pylist()], pa.string()
            )
    return columns


@cache
def _string_names(dimension: str, table: TableName) -> dict[str, str]:
    """Each code of ``dimension`` as a string, and its name."""
    found = _DIMENSIONS.get(dimension)
    if found is None or (found == "line" and table not in ("dac1", "dac2a")):
        return {}
    known = names(found, table=table if found == "line" else None)
    return {str(code): name for code, name in known.items()}


# Provenance -------------------------------------------------------------------


@cache
def _reference_data() -> str:
    """A content hash of the bundled reference data in ``_data``."""
    digest = hashlib.sha256()
    folder = files("oda_data").joinpath("_data")
    for entry in sorted(folder.iterdir(), key=lambda item: item.name):
        if entry.is_file():
            digest.update(entry.name.encode())
            digest.update(entry.read_bytes())
    return f"sha256:{digest.hexdigest()[:16]}"


# Context ----------------------------------------------------------------------


class _ContextTables(Tables, Protocol):
    """The tables a context holds: ``_model.Tables`` that can be closed."""

    def close(self) -> None:
        """Release the connection."""
        ...


def _dac_factors(tables: OecdTables) -> Factors:
    from oda_data._factors import DacFactors

    return DacFactors(tables)


@dataclass(frozen=True, slots=True)
class _Result:
    table: pa.Table
    schema: pa.Schema | None
    provenance: Provenance
    explanation: str


class Context:
    """Cache folder, offline mode, and release pins for an application.

    A context holds one DuckDB connection, the conversion factors from its
    DAC1 release, and what it reads once per table release, such as each
    table's providers and years, and serves many threads at once. Use it as a
    context manager, or call ``close()``, to release the connection and the
    leases on the release files it read, which keep oda-reader from deleting
    them.

    Args:
        cache_dir: oda-reader's cache folder, or ``None`` for its default
            (``ODA_READER_CACHE_DIR`` or the platform cache folder).
        offline: Read only cached tables and make no network request.
        releases: Release tokens to hold tables to, keyed by table name
            (``"dac1"``, ``"dac2a"``, ``"crs"``, ``"multisystem"``). A call
            fails with ``ReleaseMismatchError`` if the cached release differs.

    Raises:
        ValueError: ``releases`` names a table that does not exist.
    """

    def __init__(
        self,
        *,
        cache_dir: str | Path | None = None,
        offline: bool = False,
        releases: Mapping[str, str] | None = None,
    ) -> None:
        tables = OecdTables(
            cache_dir=None if cache_dir is None else Path(cache_dir),
            offline=offline,
            releases=releases,
        )
        self._setup(tables, partial(_dac_factors, tables))

    @classmethod
    def _from_parts(cls, tables: _ContextTables, factors: Factors) -> Self:
        """Build a context over given tables and factors, for tests."""
        context = object.__new__(cls)
        context._setup(tables, lambda: factors)
        return context

    def _setup(
        self, tables: _ContextTables, make_factors: Callable[[], Factors]
    ) -> None:
        self._tables = tables
        self._make_factors = make_factors
        self._factor_source: Factors | None = None
        self._store = Store()
        self._lock = threading.Lock()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def close(self) -> None:
        """Close the context's DuckDB connection, release its leases on the
        release files, and drop its factors and the values it read from them.

        oda-reader may then delete the files of superseded releases. A later
        call opens a new connection and reads the tables again.
        """
        with self._lock:
            self._tables.close()
            self._factor_source = None
            self._store.clear()

    def get(
        self,
        indicator: IndicatorId | str,
        /,
        *,
        years: Years | None = None,
        providers: Many[Entity] | None = None,
        recipients: Many[Entity] | None = None,
        sectors: Many[int | str | ReportedAt | SectorGroup] | None = None,
        channels: Many[int | str] | None = None,
        modalities: Many[str] | None = None,
        finance_types: Many[int | str] | None = None,
        flows: Many[int | str] | None = None,
        lines: Many[int | str] | None = None,
        by: Many[Dimension] = ("provider", "year"),
        basis: Basis | None = None,
        unit: Many[Unit] | None = None,
        allow_partial: bool = False,
    ) -> pd.DataFrame:
        """Compute an indicator and return a DataFrame.

        Takes the arguments of ``oda_data.get``.
        """
        return self.query(
            indicator,
            years=years,
            providers=providers,
            recipients=recipients,
            sectors=sectors,
            channels=channels,
            modalities=modalities,
            finance_types=finance_types,
            flows=flows,
            lines=lines,
            by=by,
            basis=basis,
            unit=unit,
            allow_partial=allow_partial,
        ).to_pandas()

    def query(
        self,
        indicator: IndicatorId | str,
        /,
        *,
        years: Years | None = None,
        providers: Many[Entity] | None = None,
        recipients: Many[Entity] | None = None,
        sectors: Many[int | str | ReportedAt | SectorGroup] | None = None,
        channels: Many[int | str] | None = None,
        modalities: Many[str] | None = None,
        finance_types: Many[int | str] | None = None,
        flows: Many[int | str] | None = None,
        lines: Many[int | str] | None = None,
        by: Many[Dimension] = ("provider", "year"),
        basis: Basis | None = None,
        unit: Many[Unit] | None = None,
        allow_partial: bool = False,
    ) -> Query:
        """Return a ``Query`` that runs on first use and keeps its result.

        Takes the arguments of ``oda_data.get``.
        """
        request = _request(
            indicator,
            years=years,
            filters={
                "providers": providers,
                "recipients": recipients,
                "sectors": sectors,
                "channels": channels,
                "modalities": modalities,
                "finance_types": finance_types,
                "flows": flows,
                "lines": lines,
            },
            by=by,
            basis=basis,
            unit=unit,
            allow_partial=allow_partial,
        )
        return Query(self, request)

    def records(
        self,
        indicator: IndicatorId | str,
        /,
        *,
        columns: Iterable[str] = (),
        years: Years | None = None,
        providers: Many[Entity] | None = None,
        recipients: Many[Entity] | None = None,
        sectors: Many[int | str | ReportedAt | SectorGroup] | None = None,
        channels: Many[int | str] | None = None,
        modalities: Many[str] | None = None,
        finance_types: Many[int | str] | None = None,
        flows: Many[int | str] | None = None,
        lines: Many[int | str] | None = None,
        basis: Basis | None = None,
        unit: Many[Unit] | None = None,
    ) -> pd.DataFrame:
        """Return the rows a summed indicator adds up.

        Takes the arguments of ``oda_data.records``.
        """
        request = _request(
            indicator,
            years=years,
            filters={
                "providers": providers,
                "recipients": recipients,
                "sectors": sectors,
                "channels": channels,
                "modalities": modalities,
                "finance_types": finance_types,
                "flows": flows,
                "lines": lines,
            },
            by=(),
            basis=basis,
            unit=unit,
            allow_partial=False,
            columns=columns,
        )
        return _to_pandas(self._run(request).table)

    def _factors(self) -> Factors:
        with self._lock:
            if self._factor_source is None:
                self._factor_source = self._make_factors()
            return self._factor_source

    def _run(self, request: _Request) -> _Result:
        """Compute a request on its own cursor."""
        indicator = request.indicator
        tables = tables_for(indicator)
        cursor = self._tables.cursor(tables)
        # The tables the call read, and DAC1 when a unit took its factors.
        releases: dict[str, Release] = {t: self._tables.release(t) for t in tables}
        held = Releases(self._store, {t: releases[t].token for t in tables})
        try:
            plan = _plan(cursor, request, tables, held)
            if "broad_sector" in plan.call.by:
                cursor.register(BROAD_SECTORS, _broad_sector_table())
            factors = self._factors()
            if request.columns is None:
                combined = combine(cursor, indicator, plan.call, factors, releases=held)
            else:
                combined = combined_records(
                    cursor,
                    indicator,
                    plan.call,
                    factors,
                    columns=request.columns,
                    releases=held,
                )
        except duckdb.InvalidInputException as error:
            message = str(error).removeprefix("Invalid Input Error: ")
            if UNLABELLED_CODE not in message:
                raise
            raise UnknownEntityError(message) from error
        finally:
            cursor.close()

        if indicator.is_money and any(converts(unit) for unit in plan.call.units):
            releases.setdefault("dac1", factors.release())
        record = Provenance(
            oda_data=version("oda_data"),
            indicators={part.id: part.version for part in walk(indicator)},
            tables=releases,
            pydeflate=version("pydeflate"),
            resolvekit=version("resolvekit"),
            reference_data=_reference_data(),
            defaults=plan.defaults,
            call=call_record(indicator.id, plan.call, filtered=request.filters),
        )
        if request.columns is not None:
            table = _shape_records(combined.table, indicator, plan)
            return _Result(table, None, record, "")
        table, schema = _shape_result(combined.table, indicator, plan)
        text = explain(
            indicator,
            plan,
            table,
            releases,
            combined.fallback_providers,
            notes=combined.notes,
        )
        return _Result(table, schema, record, text)


class Query:
    """A call that runs once, on first use, and keeps its result.

    Build one with ``oda_data.query`` or ``Context.query``. Every method reads
    the same result, so ``explain()`` and ``to_parquet()`` read the tables
    once.
    """

    def __init__(self, context: Context, request: _Request) -> None:
        self._context = context
        self._request = request
        self._lock = threading.Lock()
        self._result: _Result | None = None

    def __repr__(self) -> str:
        state = "run" if self._result is not None else "not run"
        return f"Query({self._request.indicator.id!r}, {state})"

    def _run(self) -> _Result:
        with self._lock:
            if self._result is None:
                self._result = self._context._run(self._request)
            return self._result

    @property
    def provenance(self) -> Provenance:
        """The package versions, indicator versions, and table releases behind
        the result, and the defaults the call relied on."""
        return self._run().provenance

    def explain(self) -> str:
        """Describe the result in plain text, for a log or a methodology note.

        Names the indicator, basis, years, providers and other filters, every
        default the call relied on, the providers the indicator leaves out,
        the caveats that concern the call, the units and deflators, the
        providers that took the DAC total's deflators, and the table
        releases.
        """
        return self._run().explanation

    def to_pandas(self) -> pd.DataFrame:
        """Return the result as a pandas DataFrame, in the result schema."""
        result = self._run()
        frame = _to_pandas(result.table)
        if result.schema is not None:
            check_frame(frame, result.schema)
        return frame

    def to_arrow(self) -> pa.Table:
        """Return the result as an Arrow table. Categories are dictionaries."""
        return self._run().table

    def to_polars(self) -> pl.DataFrame:
        """Return the result as a polars DataFrame.

        Raises:
            ImportError: polars is not installed. Install ``oda-data[polars]``.
        """
        try:
            import polars as pl
        except ImportError as error:
            raise ImportError(
                "to_polars() needs polars; install oda-data[polars]"
            ) from error
        frame = pl.from_arrow(self._run().table)
        assert isinstance(frame, pl.DataFrame)  # a Table converts to a DataFrame
        return frame

    def to_parquet(self, target: str | Path | BinaryIO) -> None:
        """Write the result to Parquet, with the provenance in its metadata.

        ``oda_data.provenance(path)`` reads the record back.

        Args:
            target: A file path, or a binary file object such as
                ``sys.stdout.buffer``.
        """
        import pyarrow.parquet as pq

        result = self._run()
        where = str(target) if isinstance(target, Path) else target
        pq.write_table(with_provenance(result.table, result.provenance), where)


# Explain ----------------------------------------------------------------------

#: Each table's name as the OECD writes it.
TABLE_LABELS: Mapping[str, str] = {
    "dac1": "DAC1",
    "dac2a": "DAC2A",
    "crs": "CRS",
    "multisystem": "Multisystem",
}

#: Each basis in explain(). DAC1 calls its net disbursements net flows.
_BASIS_LABELS: Mapping[str, str] = {
    "headline": "headline",
    "net_disbursement": "net disbursements",
    "gross_disbursement": "gross disbursements",
    "commitment": "commitments",
    "grant_equivalent": "grant equivalent",
}


def _basis_label(basis: Basis, table: TableName) -> str:
    if basis == "net_disbursement" and table == "dac1":
        return "net flows"
    return _BASIS_LABELS[basis]


_PLURALS: Mapping[str, str] = {
    "provider": "providers",
    "recipient": "recipients",
    "sector": "sectors",
    "channel": "channels",
    "modality": "modalities",
    "finance_type": "finance types",
    "flow": "flows",
    "line": "lines",
}


def explain(
    indicator: Indicator,
    plan: _Plan,
    result: pa.Table,
    releases: Mapping[str, Release],
    fallback: frozenset[int],
    *,
    notes: tuple[str, ...] = (),
) -> str:
    """Describe a result in plain text, one sentence per line.

    ``notes`` is ``Combined.notes``, the sentences on the result's incomplete
    cells and the notes of its ``Method`` parts.
    """
    table = indicator.binding.table
    years = _result_years(result, plan.call)
    lines = [
        f"{indicator.name} ({TABLE_LABELS[table]}), "
        f"{_basis_text(indicator, plan.call.basis, years)}."
    ]
    for _, dimension in _FILTERS:
        selection = plan.selections.get(dimension)
        if selection is not None:
            phrase = _selection_text(selection, plan.call, result, table)
        elif dimension in plan.default_text:
            phrase = plan.default_text[dimension]
        else:
            continue
        if dimension == "provider":
            lines.append(f"{phrase[0].upper()}{phrase[1:]}.")
            lines.extend(_excluded_text(indicator, plan.call, table))
        else:
            lines.append(f"{_PLURALS[dimension].capitalize()}: {phrase}.")
    lines.extend(
        f"Caveat: {_sentence(caveat.text)}"
        for caveat in caveats_for(indicator, plan.call)
    )
    lines.extend(_sentence(note) for note in notes)
    lines.append(_unit_text(indicator, plan, fallback))
    downloads = "; ".join(
        f"{TABLE_LABELS[name]} downloaded {release.downloaded:%Y-%m-%d}"
        for name, release in releases.items()
    )
    lines.append(f"{downloads}. {result.num_rows:,} rows.")
    return "\n".join(lines)


def _provider_exclusions(indicator: Indicator) -> tuple[NotIn, ...]:
    """The provider exclusions of the indicator and the indicators it
    combines, each once."""
    return tuple(
        dict.fromkeys(
            rule for part in walk(indicator) for rule in part.provider_exclusions
        )
    )


def _excluded_text(
    indicator: Indicator, call: ResolvedCall, table: TableName
) -> list[str]:
    """A sentence for each provider exclusion the call can meet, naming the
    providers, the rows it covers, and the reason."""
    return [
        f"Leaves out {_codes_text('provider', frozenset(rule.values), table)}"
        f"{_scope_text(indicator, rule)}: {rule.reason.removesuffix('.')}."
        for rule in _provider_exclusions(indicator)
        if _meets(indicator, call, rule)
    ]


def _scope_text(indicator: Indicator, rule: NotIn) -> str:
    """`` on line 106`` for an exclusion scoped to line 106, or nothing."""
    if rule.when is None:
        return ""
    codes = _join([str(code) for code in rule.when.values])
    return f" on {_scope_dimension(indicator, rule) or rule.when.column} {codes}"


def _meets(indicator: Indicator, call: ResolvedCall, rule: NotIn) -> bool:
    """Whether a call can read rows a scoped exclusion covers: it filters
    none of the scope's dimension, or names a code in the scope."""
    if rule.when is None:
        return True
    dimension = _scope_dimension(indicator, rule)
    named = None if dimension is None else call.members.get(dimension)
    return named is None or bool(named & set(rule.when.values))


def _scope_dimension(indicator: Indicator, rule: NotIn) -> Dimension | None:
    """The dimension whose codes scope an exclusion, if one holds its column."""
    if rule.when is None:
        return None
    columns = {column: d for d, column in indicator.binding.dimensions.items()}
    return columns.get(rule.when.column)


def _sentence(text: str) -> str:
    text = text[0].upper() + text[1:]
    return text if text.endswith(".") else f"{text}."


def _result_years(result: pa.Table, call: ResolvedCall) -> tuple[int, ...] | None:
    if "year" in result.column_names:
        found = {year for year in result["year"].to_pylist() if year is not None}
        return tuple(sorted(found))
    return call.years


def _basis_text(
    indicator: Indicator, basis: Basis, years: tuple[int, ...] | None
) -> str:
    table = indicator.binding.table
    rule = indicator.binding.bases[basis]
    if not isinstance(rule, Headline):
        label = _basis_label(basis, table)
        return f"{label} {_span(years)}" if years else label
    before = _basis_label(rule.before, table)
    after = _basis_label(rule.after, table)
    if before == after:
        label = f"headline basis: {before}"
        return f"{label} {_span(years)}" if years else label
    if years is None:
        parts = [f"{before} to {rule.year - 1}", f"{after} from {rule.year}"]
    else:
        spans = (
            (before, [year for year in years if year < rule.year]),
            (after, [year for year in years if year >= rule.year]),
        )
        parts = [f"{label} {_span(part)}" for label, part in spans if part]
    return "headline basis: " + ", ".join(parts)


def _selection_text(
    selection: Selection, call: ResolvedCall, result: pa.Table, table: TableName
) -> str:
    parts = []
    in_groups: set[Code] = set()
    for group in selection.groups:
        members = _member_union(group.members)
        label = selection.labels.get(group.name, group.name)
        if group.shown:
            in_groups |= members
        counted = f"{len(members)} {label}"
        if group.name not in selection.totals:
            parts.append(counted)
            continue
        text = (
            f"{counted}, plus their total" if group.shown else f"the total of {counted}"
        )
        if selection.dimension == "provider":
            text += ", " + _coverage(group.name, group.members, call, result)
        parts.append(text)
    direct = selection.rows - in_groups
    if direct:
        parts.append(_codes_text(selection.dimension, direct, table))
    return "; ".join(parts) or f"no {_PLURALS[selection.dimension]}"


def _coverage(
    name: str,
    members: Mapping[int, frozenset[Code]],
    call: ResolvedCall,
    result: pa.Table,
) -> str:
    if call.allow_partial:
        return "which sums the members that reported in each year"
    years = result["year"].to_pylist() if "year" in result.column_names else None
    empty = [
        i
        for i, (code, value) in enumerate(
            zip(
                result["provider"].to_pylist(), result["value"].to_pylist(), strict=True
            )
        )
        if code == name and value is None
    ]
    if not empty:
        if len(set(members.values())) > 1:
            return "which covers its members in every year"
        return f"which covers all {len(_member_union(members))} in every year"
    if years is None:
        return "which is empty, because a member has stopped reporting"
    found = sorted({years[i] for i in empty})
    return f"which is empty in {_span(found)}, when a member had stopped reporting"


def _codes_text(dimension: Dimension, codes: frozenset[Code], table: TableName) -> str:
    if len(codes) > LISTED:
        return f"{len(codes)} {_PLURALS[dimension]}"
    known = _string_names(dimension, table)
    listed = [known.get(str(code), str(code)) for code in sorted(codes, key=str)]
    return _join(listed)


def _unit_text(indicator: Indicator, plan: _Plan, fallback: frozenset[int]) -> str:
    if isinstance(indicator.kind, Ratio):
        return "Per cent, computed in current USD."
    if not indicator.is_money:
        return f"Millions of {indicator.measure}, a count that takes no unit=."
    clauses: dict[tuple[str, int | None], list[str]] = {}
    for unit in plan.call.units:
        clauses.setdefault((unit.prices, unit.base_year), []).append(unit.currency)
    texts = [
        f"{prices if year is None else f'constant {year}'} {_join(currencies)}"
        for (prices, year), currencies in clauses.items()
    ]
    text = (
        texts[-1] if len(texts) == 1 else ", ".join(texts[:-1]) + ", and " + texts[-1]
    )
    text = text[0].upper() + text[1:]
    if "unit" in plan.defaults:
        text += " (the default)"
    if any(unit != _USD for unit in plan.call.units):
        same = "the same" if indicator.binding.table == "dac1" else "the"
        text += f" using DAC deflators from {same} DAC1 release"
        if fallback:
            text += (
                f". {_codes_text('provider', frozenset(fallback), 'dac1')} "
                "took the DAC total's deflators"
            )
    return text + "."


def _span(years: Iterable[int]) -> str:
    ordered = sorted(years)
    if len(ordered) == 1:
        return str(ordered[0])
    if ordered[-1] - ordered[0] == len(ordered) - 1:
        return f"{ordered[0]}-{ordered[-1]}"
    return ", ".join(map(str, ordered))


def _join(items: list[str]) -> str:
    if len(items) <= 2:
        return " and ".join(items)
    return ", ".join(items[:-1]) + ", and " + items[-1]


# Default context and module functions ----------------------------------------

_default: Context | None = None
_default_lock = threading.Lock()


def default_context() -> Context:
    """Return the context the module functions use, building it on first use.

    It is built once per process under a lock and shared by every thread.
    """
    global _default  # noqa: PLW0603 - one default context per process, built lazily
    with _default_lock:
        if _default is None:
            _default = Context()
        return _default


def get(
    indicator: IndicatorId | str,
    /,
    *,
    years: Years | None = None,
    providers: Many[Entity] | None = None,
    recipients: Many[Entity] | None = None,
    sectors: Many[int | str | ReportedAt | SectorGroup] | None = None,
    channels: Many[int | str] | None = None,
    modalities: Many[str] | None = None,
    finance_types: Many[int | str] | None = None,
    flows: Many[int | str] | None = None,
    lines: Many[int | str] | None = None,
    by: Many[Dimension] = ("provider", "year"),
    basis: Basis | None = None,
    unit: Many[Unit] | None = None,
    allow_partial: bool = False,
) -> pd.DataFrame:
    """Compute an indicator and return a DataFrame.

    Every result has the same columns: the ``by`` dimensions, each code with
    its name beside it, then ``indicator``, ``indicator_version``, ``basis``,
    ``currency``, ``prices``, ``base_year``, ``scale``, ``is_aggregate``, and
    ``value``.

    Args:
        indicator: An indicator id, such as ``"oecd.dac1.total_oda"``.
        years: A year or years. ``None`` reads every year in the table.
        providers: Codes, exact names, groups, and ``oda.total(group)``.
            ``None`` takes the indicator's default providers.
        recipients: Codes, exact names, groups, and group totals.
        sectors: CRS purpose codes, names, and ``oda.sectors`` groups.
        channels: Channel codes or names.
        modalities: Modality codes, such as ``"B01"``.
        finance_types: CRS type of finance codes or names, such as 110
            (``"Standard grant"``). DAC1 splits grants from loans with
            ``flows=``, and DAC2A with ``lines=``.
        flows: Flow codes or names.
        lines: DAC1 or DAC2A line codes or names.
        by: The dimensions the result splits by.
        basis: The basis, or ``None`` for the indicator's default.
        unit: A unit or units. ``None`` is current USD. A ratio takes none.
        allow_partial: Accept partial values. Without it a group total is
            null in a year in which a member has stopped reporting, and a sum
            across providers warns when providers that reported the year
            before have not reported the table's last year. A row for one
            provider is null where a derived indicator, such as core ODA,
            misses a part in the table's last year that the provider
            reported the year before. A row is null, alone or summed with
            other providers, where it holds EU Institutions' (918) GNI after
            an EU member stopped reporting GNI, or 918's attributed ODA in
            DAC1's last year before its funders report line 2102. A
            ratio's group total is null when a member's numerator or
            denominator is. With it the group total sums the members that
            reported, those rows hold the parts reported, and the sum across
            providers does not warn. ``explain()`` names the
            incomplete values either way. A member that has not yet reported, or that
            skips a year and reports again, counts as zero either way.

    Raises:
        UnknownIndicatorError: No indicator has that id. An oda-data 2.x
            code, such as ``"DAC1.10.1010"``, gives its 3.0 call.
        UnknownEntityError: A filter value matched nothing, or the table
            holds a policy-marker score with no label.
        AmbiguousEntityError: A name could mean more than one entity.
        DimensionError: The indicator lacks a dimension the call uses.
        BasisRangeError: The basis does not cover a year asked for.
        UnsupportedQueryError: The call cannot be answered as asked, such as
            ``unit=`` on a ratio or ``oda.total()`` outside ``by``.
        MissingFactorError: A provider lacks a conversion factor in a year.
        DataUnavailableError: oda-reader could not fetch a table.
        OfflineCacheMissError: Offline, and the cache lacks a table.
        ReleaseMismatchError: A pinned release differs from the cache.
    """
    return default_context().get(
        indicator,
        years=years,
        providers=providers,
        recipients=recipients,
        sectors=sectors,
        channels=channels,
        modalities=modalities,
        finance_types=finance_types,
        flows=flows,
        lines=lines,
        by=by,
        basis=basis,
        unit=unit,
        allow_partial=allow_partial,
    )


def query(
    indicator: IndicatorId | str,
    /,
    *,
    years: Years | None = None,
    providers: Many[Entity] | None = None,
    recipients: Many[Entity] | None = None,
    sectors: Many[int | str | ReportedAt | SectorGroup] | None = None,
    channels: Many[int | str] | None = None,
    modalities: Many[str] | None = None,
    finance_types: Many[int | str] | None = None,
    flows: Many[int | str] | None = None,
    lines: Many[int | str] | None = None,
    by: Many[Dimension] = ("provider", "year"),
    basis: Basis | None = None,
    unit: Many[Unit] | None = None,
    allow_partial: bool = False,
) -> Query:
    """Return a ``Query`` that runs on first use and keeps its result.

    The query explains itself, carries its provenance, and writes Parquet
    with the provenance in the file's metadata. Takes the arguments of
    ``get``.
    """
    return default_context().query(
        indicator,
        years=years,
        providers=providers,
        recipients=recipients,
        sectors=sectors,
        channels=channels,
        modalities=modalities,
        finance_types=finance_types,
        flows=flows,
        lines=lines,
        by=by,
        basis=basis,
        unit=unit,
        allow_partial=allow_partial,
    )


def records(
    indicator: IndicatorId | str,
    /,
    *,
    columns: Iterable[str] = (),
    years: Years | None = None,
    providers: Many[Entity] | None = None,
    recipients: Many[Entity] | None = None,
    sectors: Many[int | str | ReportedAt | SectorGroup] | None = None,
    channels: Many[int | str] | None = None,
    modalities: Many[str] | None = None,
    finance_types: Many[int | str] | None = None,
    flows: Many[int | str] | None = None,
    lines: Many[int | str] | None = None,
    basis: Basis | None = None,
    unit: Many[Unit] | None = None,
) -> pd.DataFrame:
    """Return the rows a summed indicator adds up, converted to ``unit``.

    Each row carries every dimension of the indicator, codes with names
    beside them, then the extra ``columns`` asked for, then the indicator,
    basis, and unit columns, ``scale``, and ``value``. Takes the filter
    arguments of ``get``.

    Args:
        indicator: A ``Sum`` or ``Method`` indicator id.
        columns: Extra table columns, such as ``"project_title"`` on CRS.

    Raises:
        UnsupportedQueryError: The indicator is a ratio or a derived
            indicator, or a method lacks one of ``columns``. Also as ``get``.
    """
    return default_context().records(
        indicator,
        columns=columns,
        years=years,
        providers=providers,
        recipients=recipients,
        sectors=sectors,
        channels=channels,
        modalities=modalities,
        finance_types=finance_types,
        flows=flows,
        lines=lines,
        basis=basis,
        unit=unit,
    )


for _public in (get, query, records, Query, Context):
    _public.__module__ = "oda_data"

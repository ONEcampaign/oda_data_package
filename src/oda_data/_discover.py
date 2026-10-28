"""Finding the right indicator: ``indicators``, ``describe``, ``search``, ``add_names``.

The data comes from the indicator registry, the built-in groups and sector
groups, and the bundled names of DAC lines. Nothing here imports pandas at
module level, so the package import stays light. Each function that returns
a DataFrame imports it on first call.

Both DataFrames have a fixed schema, ``INDICATORS_SCHEMA`` and
``SEARCH_SCHEMA``, mapping each column to its pandas dtype. A test pins them.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from functools import cache
from types import MappingProxyType
from typing import TYPE_CHECKING

from oda_data import _compat, _context
from oda_data._groups import groups, sectors
from oda_data._model import (
    Caveat,
    Headline,
    Indicator,
    NotIn,
    Ratio,
    Sum,
    line_basis,
    offered_bases,
)
from oda_data._names import names, normalise

if TYPE_CHECKING:
    import pandas as pd

    from oda_data._types import Basis, Code, Dimension, IndicatorId, TableName

#: The columns of ``indicators()`` and their pandas dtypes. ``dimensions`` and
#: ``bases`` hold comma-separated names, the default basis first in ``bases``.
INDICATORS_SCHEMA: Mapping[str, str] = MappingProxyType(
    {
        "id": "string",
        "name": "string",
        "table": "string",
        "kind": "string",
        "dimensions": "string",
        "bases": "string",
        "default_basis": "string",
        "version": "Int64",
        "description": "string",
    }
)

#: The columns of ``search()`` and their pandas dtypes. ``table`` is null for
#: groups and sector groups, and ``note`` is null except on a 2.x code.
SEARCH_SCHEMA: Mapping[str, str] = MappingProxyType(
    {
        "kind": "string",
        "key": "string",
        "name": "string",
        "table": "string",
        "use": "string",
        "note": "string",
    }
)

#: Dimensions that carry a ``<dimension>_name`` column beside their code.
NAMED_DIMENSIONS: tuple[Dimension, ...] = (
    "provider",
    "recipient",
    "sector",
    "broad_sector",
    "channel",
    "modality",
    "finance_type",
    "flow",
    "line",
)

_LINE_TABLES: tuple[TableName, ...] = ("dac1", "dac2a")

#: The year in an example call, and the dimensions an example prefers to add to
#: ``by``, in order.
_EXAMPLE_YEAR = 2023
_EXAMPLE_BY = ("broad_sector", "recipient", "line", "sector", "channel", "modality")


# Indicator definitions --------------------------------------------------------


@dataclass(frozen=True, slots=True, repr=False)
class IndicatorInfo:
    """An indicator's definition. Notebooks and ``print`` show it as text.

    Attributes:
        id: The indicator id.
        name: A short title.
        description: What the indicator counts.
        table: The OECD table it reads.
        kind: ``sum``, ``ratio``, ``derived``, or ``method``.
        version: Bumped when a published number changes.
        dimensions: The dimensions a call may filter on and group by.
        bases: Each basis the indicator offers with the first year it covers
            (``None`` for every year). The default basis comes first.
        default_basis: The basis a call gets when it passes none.
        default_providers: The name of a group in ``oda.groups`` a call gets
            when it names no providers, or ``None`` for every provider in the
            table except aggregates.
        caveats: Notes on what the indicator counts. A note that names a
            basis or years, shown after it in brackets, also warns when a call
            reads that basis in those years.
        example: A call that returns the indicator.
        measure: What the values count: ``money``, or ``people`` for a count
            such as population, which takes no ``unit=``.
        leaves_out_eu_institutions: Whether a call that names no providers
            leaves EU Institutions (918) out of a sum across providers and
            shows its row when split by provider, since its members' figures
            hold what they pay it. The provider rows of such a split add up
            to more than the sum by 918's value.
    """

    id: str
    name: str
    description: str
    table: TableName
    kind: str
    version: int
    dimensions: tuple[Dimension, ...]
    bases: tuple[tuple[Basis, int | None], ...]
    default_basis: Basis
    default_providers: str | None
    caveats: tuple[str, ...]
    example: str
    measure: str = "money"
    leaves_out_eu_institutions: bool = False

    def __repr__(self) -> str:
        bases = ", ".join(_basis_text(self, *pair) for pair in self.bases)
        if self.kind == "ratio":
            values = "per cent"
        elif self.measure != "money":
            values = f"millions of {self.measure}, a count that takes no unit="
        else:
            values = "USD millions unless unit= says otherwise"
        lines = [
            f"{self.id}  (version {self.version}, table "
            f"{_context.TABLE_LABELS[self.table]})",
            self.description,
            f"Dimensions: {', '.join(self.dimensions)}",
            f"Bases: {bases}",
            "Providers by default: "
            f"{_default_providers_text(self.default_providers)}"
            f"{_eu_institutions_clause(self)}",
            f"Values: {values}",
            *(f"Caveat: {caveat}" for caveat in self.caveats),
            f"Example: {self.example}",
        ]
        return "\n".join(lines)


def describe(indicator: IndicatorId | str, /) -> IndicatorInfo:
    """Return an indicator's definition, printed as text in a notebook.

    Args:
        indicator: The indicator id, such as ``"oecd.dac1.total_oda"``.

    Raises:
        TypeError: ``indicator`` is not a string.
        UnknownIndicatorError: No indicator has that id. The error suggests
            close ids.
    """
    return _info(_context.find_indicator(indicator))


def _info(indicator: Indicator) -> IndicatorInfo:
    pairs = _bases(indicator)
    default = indicator.default_basis
    ordered = sorted(pairs, key=lambda pair: pair[0] != default)
    return IndicatorInfo(
        id=indicator.id,
        name=indicator.name,
        description=indicator.description,
        table=indicator.binding.table,
        kind=_kind(indicator),
        version=indicator.version,
        dimensions=indicator.dimensions,
        bases=tuple(ordered),
        default_basis=default,
        default_providers=indicator.default_providers,
        caveats=tuple(_caveat_text(c) for c in indicator.caveats),
        example=_example(indicator),
        measure=indicator.measure,
        leaves_out_eu_institutions=_context.sums_leave_out_eu_institutions(indicator),
    )


def _kind(indicator: Indicator) -> str:
    return type(indicator.kind).__name__.lower()


def _bases(indicator: Indicator) -> list[tuple[Basis, int | None]]:
    """Each basis a call may ask for and its first year. A ratio offers its
    numerator's bases, since the denominator falls back to its own default
    where it has none."""
    if isinstance(indicator.kind, Ratio):
        return _bases(indicator.kind.numerator)
    return [
        (basis, None if isinstance(rule, Headline) else rule.from_year)
        for basis, rule in offered_bases(indicator).items()
    ]


def _basis_text(info: IndicatorInfo, basis: Basis, from_year: int | None) -> str:
    notes = []
    if basis == info.default_basis:
        notes.append("default")
    if from_year is not None:
        notes.append(f"{from_year} on")
    return f"{basis} ({', '.join(notes)})" if notes else basis


def _default_providers_text(group: str | None) -> str:
    if group is None:
        return "every official provider in the table, aggregates excluded"
    found = groups.get(group)
    return f"{found.label} (oda.groups.{group})" if found else group


def _eu_institutions_clause(info: IndicatorInfo) -> str:
    """The sentence tail the default providers line takes when a sum across
    providers leaves EU Institutions (918) out, or an empty string."""
    if not info.leaves_out_eu_institutions:
        return ""
    return (
        "; EU Institutions (918) are left out of a sum across providers and "
        "shown when split by provider"
    )


def _caveat_text(caveat: Caveat) -> str:
    text = caveat.text[0].upper() + caveat.text[1:]
    scope = []
    if caveat.basis is not None:
        scope.append(f"{caveat.basis} basis")
    if caveat.years is not None:
        first, last = caveat.years
        scope.append(f"{first} to {last}")
    if scope:
        text += f" ({', '.join(scope)})"
    return text if text.endswith(".") else text + "."


def _example(indicator: Indicator) -> str:
    call = f'oda.get("{indicator.id}", years={_EXAMPLE_YEAR}'
    extra = next((d for d in _EXAMPLE_BY if d in indicator.dimensions), None)
    if extra is None:
        return call + ")"
    return call + f', by=["provider", "{extra}"])'


# Indicator table --------------------------------------------------------------


def indicators() -> pd.DataFrame:
    """List every indicator, one row each, in registry order.

    The columns and dtypes are ``INDICATORS_SCHEMA``: ``id``, ``name``,
    ``table``, ``kind`` (``sum``, ``ratio``, ``derived``, or ``method``),
    ``dimensions`` and ``bases`` (comma-separated, the default basis first),
    ``default_basis``, ``version`` (``Int64``), and ``description``.
    """
    rows: list[dict[str, object]] = []
    for indicator in _context.INDICATORS.values():
        info = _info(indicator)
        rows.append(
            {
                "id": info.id,
                "name": info.name,
                "table": info.table,
                "kind": info.kind,
                "dimensions": ", ".join(info.dimensions),
                "bases": ", ".join(basis for basis, _ in info.bases),
                "default_basis": info.default_basis,
                "version": info.version,
                "description": info.description,
            }
        )
    return _frame(rows, INDICATORS_SCHEMA)


def _frame(rows: list[dict[str, object]], schema: Mapping[str, str]) -> pd.DataFrame:
    import pandas as pd

    return pd.DataFrame(
        {
            column: pd.Series([row[column] for row in rows], dtype=dtype)
            for column, dtype in schema.items()
        }
    )


# Search -----------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Entry:
    kind: str
    key: str
    name: str
    table: str | None
    use: str
    detail: str = ""
    note: str | None = None


#: The order kinds appear in among results that match equally well.
_KIND_ORDER = ("code", "indicator", "group", "sector_group", "line")

#: The ``get`` argument that takes each dimension's groups.
_GROUP_ARGUMENT: Mapping[str, str] = MappingProxyType(
    {"provider": "providers", "recipient": "recipients", "sector": "sectors"}
)


def search(text: str) -> pd.DataFrame:
    """Find indicators, groups, sector groups, and DAC lines that match ``text``.

    When ``text`` is an oda-data 2.x indicator code or ``settings`` slug, such
    as ``"DAC1.10.1010"``, the first row is of kind ``code``: ``use`` holds the
    3.0 call, its statements separated by ``"; "``, and ``note`` says why its
    number differs from 2.x's, if it does.

    A row matches when every word of ``text`` appears, in any case and
    accent, in the id or name, or for an indicator also in its description.
    Rows come best match first: the id or name equals ``text``, then starts
    with it, then contains every word, then only the description does.

    The columns and dtypes are ``SEARCH_SCHEMA``: ``kind`` (``code``,
    ``indicator``, ``group``, ``sector_group``, or ``line``), ``key`` (the 2.x
    code, the indicator id, the name in ``oda.groups`` or ``oda.sectors``, or
    the line code), ``name``, ``table`` (null for groups), ``use``, the
    argument or call that puts the row to work, such as
    ``providers=oda.groups.g7`` or ``lines=1010``, and ``note``.
    A line that only a non-default basis reads names the basis too, such as
    ``lines=240, basis="gross_disbursement"``. DAC1 and DAC2A number lines
    separately, so a line appears once for each table that has it.

    Raises:
        TypeError: ``text`` is not a string.
        ValueError: ``text`` is blank.
    """
    if not isinstance(text, str):
        raise TypeError(f"search takes text, not {text!r}")
    query = normalise(text)
    if not query:
        raise ValueError("search needs some text")
    tokens = query.split()

    ranked = []
    for entry in _entries():
        tier = _tier(entry, query, tokens)
        if tier is not None:
            ranked.append((tier, _KIND_ORDER.index(entry.kind), entry))
    ranked.sort(key=lambda found: found[:2])  # stable: ties keep source order

    rows = [*_legacy_rows(text), *(entry for _, _, entry in ranked)]
    return _frame(
        [
            {
                "kind": row.kind,
                "key": row.key,
                "name": row.name,
                "table": row.table,
                "use": row.use,
                "note": row.note,
            }
            for row in rows
        ],
        SEARCH_SCHEMA,
    )


def _legacy_rows(text: str) -> list[_Entry]:
    """The row of kind ``code`` when ``text`` is a 2.x code or slug, with its
    3.0 call in ``use``. ``search`` puts it above every other match.

    A code 3.0 has no call for keeps ``use`` empty and says why in ``note``.
    """
    row = _compat.find(text)
    if row is None:
        return []
    indicator = _context.INDICATORS.get(row.indicator or "")
    return [
        _Entry(
            "code",
            row.code,
            row.name,
            indicator.binding.table if indicator is not None else None,
            row.call_text,
            note=row.reason or None,
        )
    ]


def _tier(entry: _Entry, query: str, tokens: list[str]) -> int | None:
    """How well ``entry`` matches: 0 is best, ``None`` is no match."""
    key, name = normalise(entry.key), normalise(entry.name)
    if query in (key, name):
        return 0
    if key.startswith(query) or name.startswith(query):
        return 1
    label = f"{key} {name}"
    if all(token in label for token in tokens):
        return 2
    if entry.detail:
        full = f"{label} {normalise(entry.detail)}"
        if all(token in full for token in tokens):
            return 3
    return None


def _entries() -> Iterator[_Entry]:
    for indicator in _context.INDICATORS.values():
        yield _Entry(
            "indicator",
            indicator.id,
            indicator.name,
            indicator.binding.table,
            f'oda.get("{indicator.id}")',
            indicator.description,
        )
    for slug, group in groups.items():
        argument = _GROUP_ARGUMENT[group.dimension or "provider"]
        yield _Entry("group", slug, group.label, None, f"{argument}=oda.groups.{slug}")
    for slug, sector_group in sectors.items():
        yield _Entry(
            "sector_group",
            slug,
            sector_group.label,
            None,
            f"sectors=oda.sectors.{slug}",
        )
    for table in _LINE_TABLES:
        reader = _lines_indicator(table)
        refused = _refused_lines(reader)
        for code, name in names("line", table=table).items():
            if code not in refused:
                yield _Entry("line", str(code), name, table, _line_use(reader, code))


def _lines_indicator(table: TableName) -> Indicator | None:
    """The indicator that reads any line of ``table``, if the registry has one."""
    return next(
        (
            indicator
            for indicator in _context.INDICATORS.values()
            if indicator.binding.table == table
            and "line" in indicator.dimensions
            and isinstance(indicator.kind, Sum)
            and not indicator.kind.lines
        ),
        None,
    )


def _refused_lines(reader: Indicator | None) -> frozenset[Code]:
    """The lines ``reader`` refuses in every call, such as lines that are not
    money: the values of its unscoped ``NotIn`` rules on the line column."""
    if reader is None:
        return frozenset()
    column = reader.binding.dimensions["line"]
    return frozenset(
        code
        for rule in (*reader.binding.rows, *reader.rows)
        if isinstance(rule, NotIn) and rule.column == column and rule.when is None
        for code in rule.values
    )


def _line_use(reader: Indicator | None, code: Code) -> str:
    """The ``get`` arguments that read line ``code``: ``lines=`` alone on the
    default basis, and ``basis=`` too when only another basis reads it."""
    if reader is None:
        return f"lines={code}"
    basis = line_basis(reader, code)
    if basis == reader.default_basis:
        return f"lines={code}"
    return f'lines={code}, basis="{basis}"'


# Names beside codes -----------------------------------------------------------


@cache
def _code_names(dimension: Dimension, table: TableName | None) -> Mapping[str, str]:
    """Each code of ``dimension`` as a string and its name, with the built-in
    groups' labels, as in the ``<dimension>_name`` columns of a result."""
    found = {str(code): name for code, name in names(dimension, table=table).items()}
    for namespace in (groups, sectors):
        for group in namespace.values():
            if group.dimension == dimension:
                found[group.name] = group.label
    return MappingProxyType(found)


def add_names(df: pd.DataFrame, *, table: TableName | None = None) -> pd.DataFrame:
    """Add a ``<dimension>_name`` column beside each code column of ``df``.

    Recognised code columns are ``provider``, ``recipient``, ``sector``,
    ``broad_sector``, ``channel``, ``modality``, ``finance_type``, ``flow``,
    and ``line``. The names are the ones results carry, and built-in group
    names such as ``dac_countries`` are named too. A code with no name gets a null. A
    column that already has its name column keeps it.

    DAC1 and DAC2A number lines separately, so ``line`` needs a table. Each
    row takes it from its ``indicator`` column, which every result has. A frame
    without one needs ``table=``, which applies to all rows. A row of an
    indicator on another table gets a null.

    Args:
        df: Any DataFrame. It is not changed.
        table: ``"dac1"`` or ``"dac2a"``, to name ``line`` codes in a frame
            without an ``indicator`` column.

    Returns:
        A copy of ``df`` with the name columns, of dtype ``string``.

    Raises:
        TypeError: ``df`` is not a DataFrame.
        ValueError: ``df`` has a ``line`` column to name and neither an
            ``indicator`` column nor ``table=``, or ``table`` has no lines.
    """
    import pandas as pd

    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"add_names takes a DataFrame, not {type(df).__name__}")
    if table is not None and table not in _LINE_TABLES:
        raise ValueError(f"table must be one of {_LINE_TABLES}, not {table!r}")

    out = df.copy()
    for dimension in NAMED_DIMENSIONS:
        name_column = f"{dimension}_name"
        if dimension not in out.columns or name_column in out.columns:
            continue
        codes = out[dimension].astype("string")
        if dimension == "line":
            labelled = _line_names(out, codes, table)
        else:
            labelled = codes.map(_code_names(dimension, None)).astype("string")
        out.insert(out.columns.get_loc(dimension) + 1, name_column, labelled)
    return out


def _line_names(
    df: pd.DataFrame, codes: pd.Series, table: TableName | None
) -> pd.Series:
    import pandas as pd

    if table is not None:
        tables = pd.Series(table, index=df.index, dtype="string")
    elif "indicator" in df.columns:
        table_of = {id_: i.binding.table for id_, i in _context.INDICATORS.items()}
        tables = df["indicator"].astype("string").map(table_of).astype("string")
    else:
        raise ValueError(
            "DAC1 and DAC2A number lines separately; add an indicator column "
            "or pass table='dac1' or table='dac2a'"
        )
    labelled = pd.Series(pd.NA, index=df.index, dtype="string")
    for line_table in _LINE_TABLES:
        rows = tables.eq(line_table).to_numpy(dtype=bool, na_value=False)
        known = _code_names("line", line_table)
        labelled[rows] = codes[rows].map(known).astype("string")
    return labelled


for _public in (indicators, search, describe, add_names, IndicatorInfo):
    _public.__module__ = "oda_data"

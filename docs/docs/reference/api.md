# Public API

As of oda-data 3.0. Every public name is importable from `oda_data`, which the examples import as `oda`. `import oda_data` loads nothing heavy: pandas, DuckDB, pyarrow, resolvekit, oda-reader, and pydeflate load on the first call that needs them. Names that start with an underscore are private and carry no promise.

The indicator is positional. Every other argument is keyword-only.

## Computing indicators

### `get`

```py
def get(
    indicator: IndicatorId | str, /, *,
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
) -> pd.DataFrame: ...
```

Computes an indicator and returns a DataFrame in the [result schema](#result-columns).

| Argument        | Takes                                                                                                                                                                    | Default                                                |
| --------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------ |
| `indicator`     | An indicator id, such as `"oecd.dac1.total_oda"`. Required                                                                                                               |                                                        |
| `years`         | A year or an iterable of years                                                                                                                                           | every year in the table                                |
| `providers`     | DAC codes, three-letter codes, exact names, groups, and `oda.total(group)`                                                                                               | the indicator's default providers                      |
| `recipients`    | As `providers`                                                                                                                                                           | every recipient, regional rows included                |
| `sectors`       | CRS purpose codes, purpose names, `oda.sectors.reported_at(code)`, and `oda.sectors` groups                                                                              | every sector                                           |
| `channels`      | Channel codes or names                                                                                                                                                   | every channel                                          |
| `modalities`    | CRS aid type codes, such as `"B01"`                                                                                                                                      | every modality                                         |
| `finance_types` | CRS type of finance codes or names, such as `110` or `"Standard grant"`. CRS indicators only                                                                             | every finance type                                     |
| `flows`         | Flow codes or names                                                                                                                                                      | the basis's flow                                       |
| `lines`         | DAC1 or DAC2A line codes or names, on `oecd.dac1.lines` and `oecd.dac2a.lines`                                                                                           | every money line the basis publishes, each its own row |
| `by`            | The dimensions the result splits by                                                                                                                                      | `("provider", "year")`                                 |
| `basis`         | `"headline"`, `"net_disbursement"`, `"gross_disbursement"`, `"commitment"`, or `"grant_equivalent"`                                                                      | the indicator's default basis                          |
| `unit`          | A `Unit` or several. Ratios take none                                                                                                                                    | current USD                                            |
| `allow_partial` | `True` returns partial values where it would return null: a group whose members did not all report, a row with a missing part. It also silences the partial-year warning | `False`                                                |

A filter value that is a bare string is one value. Filters are sets, so a provider named twice, or in two groups, counts once. Basis, unit, and line always split the result.

Raises `UnknownIndicatorError`, `UnknownEntityError`, `AmbiguousEntityError`, `DimensionError`, `BasisRangeError`, `UnsupportedQueryError`, `MissingFactorError`, `DataUnavailableError`, `OfflineCacheMissError`, and `ReleaseMismatchError`. [Errors and warnings](errors.md) says when.

```python
import oda_data as oda

oda.get("oecd.dac1.total_oda", years=range(2020, 2025), providers=oda.groups.g7, by=["year"])
```

### `query`

```py
def query(indicator: IndicatorId | str, /, **arguments_of_get) -> Query: ...
```

Takes the arguments of `get` and returns a `Query` that runs on first use and keeps its result.

```python
import oda_data as oda

q = oda.query("oecd.dac1.oda_gni_ratio", years=2024, providers=oda.groups.eu27)
print(q.explain())
```

### `records`

```py
def records(
    indicator: IndicatorId | str, /, *,
    columns: Iterable[str] = (),
    years=None, providers=None, recipients=None, sectors=None, channels=None,
    modalities=None, finance_types=None, flows=None, lines=None,
    basis=None, unit=None,
) -> pd.DataFrame: ...
```

Returns the rows a `sum` or `method` indicator adds up, converted to `unit`. Each row carries every dimension of the indicator with its name, then the `columns` asked for, then `indicator`, `indicator_version`, `basis`, `currency`, `prices`, `base_year`, `scale`, and `value`. It takes the filter arguments of `get` and has no `by` or `allow_partial`.

Raises `UnsupportedQueryError` for a `ratio` or `derived` indicator, a column the table lacks, or a column the result already has.

```python
import oda_data as oda

oda.records("oecd.crs.bilateral_oda", years=2023, providers="FRA", recipients="KEN", columns=["project_title"])
```

### `Query`

Built by `oda.query` or `Context.query`. The call runs once, on first use, and every method reads the same result.

| Member               | Returns                                                                                                                                           |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| `explain()`          | `str`. The indicator, basis, years, filters, defaults, exclusions, caveats, units, deflator fallbacks, and table downloads, one sentence per line |
| `provenance`         | `Provenance`. Property                                                                                                                            |
| `to_pandas()`        | `pd.DataFrame` in the result schema                                                                                                               |
| `to_arrow()`         | `pa.Table`. Categories are dictionary columns                                                                                                     |
| `to_polars()`        | `pl.DataFrame`. Needs `pip install "oda-data[polars]"`, and raises `ImportError` without it                                                       |
| `to_parquet(target)` | `None`. Writes to a path or a binary file object, with the provenance in the file metadata                                                        |

### `Context`

```py
class Context:
    def __init__(
        self, *,
        cache_dir: str | Path | None = None,
        offline: bool = False,
        releases: Mapping[str, str] | None = None,
    ) -> None: ...
    def get(self, indicator, /, **arguments_of_get) -> pd.DataFrame: ...
    def query(self, indicator, /, **arguments_of_get) -> Query: ...
    def records(self, indicator, /, **arguments_of_records) -> pd.DataFrame: ...
    def close(self) -> None: ...
    def __enter__(self) -> Self: ...
    def __exit__(self, *exc_info) -> None: ...
```

Holds one DuckDB connection over the cached OECD tables and the conversion factors from its DAC1 release. Its `get`, `query`, and `records` have the signatures of the module functions, which use one default context built on first use. One context serves many threads.

- `cache_dir`: oda-reader's cache folder. `None` uses `ODA_READER_CACHE_DIR` or the platform cache folder.
- `offline`: read only cached tables and make no network request. A table missing from the cache raises `OfflineCacheMissError`.
- `releases`: release tokens keyed by table name (`"dac1"`, `"dac2a"`, `"crs"`, `"multisystem"`). A call raises `ReleaseMismatchError` when the cached release differs. An unknown table name raises `ValueError`.

`close()`, or leaving a `with` block, closes the connection. A later call opens a new one and reads the tables again.

```python
import oda_data as oda

with oda.Context(cache_dir="oda-cache", offline=True) as ctx:
    df = ctx.get("oecd.dac1.total_oda", years=2024)
```

## Finding indicators

### `indicators`

```py
def indicators() -> pd.DataFrame: ...
```

Lists every indicator, one row each, with columns `id`, `name`, `table`, `kind`, `dimensions`, `bases` (comma-separated, the default first), `default_basis`, `version`, and `description`.

### `search`

```py
def search(text: str) -> pd.DataFrame: ...
```

Finds indicators, groups, sector groups, and DAC lines whose id or name holds every word of `text`, in any case and accent, best match first. Columns: `kind` (`code`, `indicator`, `group`, `sector_group`, or `line`), `key`, `name`, `table`, `use` (the argument or call that puts the row to work), and `note`.

An oda-data 2.x indicator code or slug returns a first row of kind `code` whose `use` is the 3.0 call and whose `note` says why the number differs from 2.x, if it does. Raises `TypeError` for a non-string and `ValueError` for blank text.

```text
   kind           key                                   name table                                                       use  note
0  code  DAC1.10.1010  Official Development Assistance (ODA)  dac1  oda.get("oecd.dac1.total_oda", basis="net_disbursement")  <NA>
```

```python
import oda_data as oda

print(oda.search("DAC1.10.1010").to_string())
```

### `describe` and `IndicatorInfo`

```py
def describe(indicator: IndicatorId | str, /) -> IndicatorInfo: ...
```

Returns an indicator's definition, which prints as text. `IndicatorInfo` is a frozen dataclass with `id`, `name`, `description`, `table`, `kind`, `version`, `dimensions`, `bases` (pairs of basis and first year), `default_basis`, `default_providers` (a group name, or `None` for every official provider), `caveats`, `example`, `measure` (`"money"` or `"people"`), and `leaves_out_eu_institutions`, which says whether a call that names no providers leaves EU Institutions (918) out of a sum across providers and shows its row when split by provider.

```text
oecd.dac1.total_oda  (version 1, table DAC1)
Official development assistance from each provider. Read from line 1010 on cash bases and line 11010 on grant equivalents.
Dimensions: provider, year, flow
Bases: headline (default), net_disbursement, gross_disbursement, commitment, grant_equivalent (2018 on)
Providers by default: DAC countries (oda.groups.dac_countries)
Values: USD millions unless unit= says otherwise
Example: oda.get("oecd.dac1.total_oda", years=2023)
```

```python
import oda_data as oda

print(oda.describe("oecd.dac1.total_oda"))
```

### `add_names`

```py
def add_names(df: pd.DataFrame, *, table: TableName | None = None) -> pd.DataFrame: ...
```

Returns a copy of `df` with a `<dimension>_name` column beside each `provider`, `recipient`, `sector`, `broad_sector`, `channel`, `modality`, `finance_type`, `flow`, or `line` column that lacks one. Line codes need a table, taken from each row's `indicator` column or from `table=` (`"dac1"` or `"dac2a"`). Raises `TypeError` for a non-DataFrame and `ValueError` for a `line` column with no table.

## Units

### `current`, `constant`, and `Unit`

```py
def current(currency: str) -> Unit: ...
def constant(currency: str, base_year: int) -> Unit: ...

@dataclass(frozen=True, slots=True)
class Unit:
    currency: str
    prices: Literal["current", "constant"] = "current"
    base_year: int | None = None
```

A `Unit` is a currency and a price basis. Constant prices need a base year and current prices take none, or the constructor raises `ValueError`. oda-data converts to USD, EUR, GBP, JPY, CAD, AUD, CHF, CZK, DKK, HUF, ISK, KRW, NOK, NZD, PLN, RON, and SEK, and a call with any other currency raises `UnsupportedQueryError`.

## Groups

### `Group`

```py
class Group:
    def __init__(self, name: str, members: Iterable[Member] | Member, *, label: str | None = None, dimension: Dimension | None = None) -> None: ...
    def dated(self) -> Group: ...
    def is_member(self, code: int | str, year: int) -> bool: ...
```

A named set of providers, recipients, or sectors. Members are DAC codes, three-letter codes, exact names, or other groups. `name` labels the group's total row and `label` is its display name. Groups are frozen.

`dated()` returns the group with its membership in each year. Built-in `dac_countries`, `eu27`, and `g7` have dates. Any other group raises `UnsupportedQueryError`.

### `total` and `GroupTotal`

```py
def total(group: Group) -> GroupTotal: ...
```

Returns a group's total as one row, with `is_aggregate = True`. Pass it beside the group, as in `providers=[g7, oda.total(g7)]`, for the members and the total. The call must have the group's dimension in `by`, or it raises `UnsupportedQueryError`.

### `groups` and `sectors`

Read-only mappings of the built-in groups, by attribute or key: `oda.groups.g7` or `oda.groups["g7"]`. `oda.groups` holds provider and recipient groups. `oda.sectors` holds `SectorGroup`s of CRS purpose codes, such as `oda.sectors.health`. A misspelt name raises `AttributeError` with near matches.

### `SectorGroup`

```py
class SectorGroup(Group):
    def __init__(self, name: str, members: Iterable[SectorMember] | SectorMember, *, label: str | None = None) -> None: ...
```

A `Group` of CRS purpose codes, purpose names, or other sector groups. A heading ending in zero covers the headings under it: 120 (Health) holds 121, 122, and 123. A three-digit code such as 122 is a DAC5 heading and reads every purpose code under it. `oda.sectors.reported_at(122)` reads only the activities filed at the heading with no finer purpose code, which is how the built-in groups list them.

### `ReportedAt`

```py
@dataclass(frozen=True)
class ReportedAt:
    code: int
```

`oda.sectors.reported_at(code)` returns a `ReportedAt`, which stands for the CRS activities filed at DAC5 heading `code` with no finer purpose code. Pass it to `sectors=` or hold it in a `SectorGroup`. It prints as `reported_at(151)`, and the built-in groups print their heading-level members that way, so a group built from `oda.sectors.conflict_peace_and_security.members` reads the same rows. A code of 1000 or more, or one that is not a heading, raises `ValueError`. Import `ReportedAt` to annotate a variable or to test a group member with `isinstance`.

## Provenance

### `provenance`

```py
def provenance(path: str | Path) -> Provenance: ...
```

Reads the provenance record from a Parquet file that `Query.to_parquet` wrote. Raises `ValueError` if the file holds no record, or one in a newer format.

### `Provenance`, `Release`, `Call`, and `CallGroup`

Frozen dataclasses.

- `Provenance.oda_data`: the oda-data version that computed the result.
- `Provenance.indicators`: each indicator id and its version, including the parts of a ratio or derived indicator.
- `Provenance.tables`: each table the result read and its `Release`.
- `Provenance.pydeflate` and `Provenance.resolvekit`: their versions.
- `Provenance.reference_data`: a hash of the bundled groups, sector groups, and channel crosswalk.
- `Provenance.defaults`: each default the call relied on and its value, such as `{"basis": "headline"}`.
- `Provenance.call`: the `Call` the result computed, or `None` for a file written before oda-data recorded it.
- `Release.token`: the file host's ETag or Last-Modified date, or `sha256:...` for a registered file the host sent no token for.
- `Release.downloaded`: when the file was downloaded, in UTC.
- `Release.label_release`: the readable release id, such as `v20261006` for the CRS, or `None`.
- `Call.indicator`, `Call.basis`, `Call.by`, and `Call.allow_partial`: the indicator id, the basis it read (the default when the call named none), the dimensions the result splits by, and whether the call accepted partial values.
- `Call.years`: the years asked for, sorted, or `None` for every year.
- `Call.units`: each `Unit`, current USD when the call named none. Empty for a ratio.
- `Call.filters`: each dimension the call filtered and the codes it named outright, sorted, which show as their own rows. `providers="France"` gives `{"provider": (4,)}`, `providers=oda.total(oda.groups.dac_countries)` gives `{"provider": ()}`, and `providers=[4, oda.total(oda.groups.dac_countries)]` gives `{"provider": (4,)}`.
- `Call.groups`: each group the call named, as a `CallGroup` with its members by year, sorted by dimension and name.
- `Call.default_providers`: the provider codes the call read when it named none, sorted, or `None`.
- `CallGroup.dimension` and `CallGroup.name`: the dimension and the group's name.
- `CallGroup.shown` and `CallGroup.total`: whether each member shows as its own rows, and whether the call asked for the total with `oda.total()`.
- `CallGroup.members`: each year the call read and the member codes in that year, sorted.

Two calls that differ in any of these hold different records, so the record tells apart two results that look alike, such as France and Germany split by year alone. The same call writes the same JSON byte for byte.

## Result columns

Every `get` result has the `by` dimensions first, each code followed by its `<dimension>_name`, then these columns in this order. `year`, `score`, and `allocation_status` have no name column.

| Column              | Dtype      | Holds                                                               |
| ------------------- | ---------- | ------------------------------------------------------------------- |
| dimension codes     | `string`   | the code, or a group's name on its total row                        |
| dimension names     | `string`   | the name                                                            |
| `year`              | `Int64`    |                                                                     |
| `indicator`         | `category` | the indicator id                                                    |
| `indicator_version` | `Int64`    | bumped when a published number changes                              |
| `basis`             | `category` | the basis of the row, with `headline` resolved to each year's basis |
| `currency`          | `category` | null for ratios and counts                                          |
| `prices`            | `category` | `current` or `constant`, null for ratios and counts                 |
| `base_year`         | `Int64`    | null for current prices                                             |
| `scale`             | `category` | `millions` for money and counts, `percent` for ratios               |
| `is_aggregate`      | `bool`     | `True` on a group total or an OECD aggregate row                    |
| `value`             | `float64`  | the value                                                           |

Within one basis and unit, summing the rows where `is_aggregate` is false counts each provider once. Ratios do not add up.

## Types

The signatures use these aliases. They describe what each argument takes and are not exported.

- `IndicatorId`: a `Literal` of every indicator id, which gives editor completion.
- `Years`: `int | Iterable[int]`.
- `Entity`: `int | str | Group | GroupTotal`.
- `Member`: `Entity | ReportedAt`, a `Group` member.
- `SectorMember`: `int | str | ReportedAt | SectorGroup`, a `SectorGroup` member.
- `Many[T]`: `T | Iterable[T]`. A bare `str` is one value.
- `Basis`: `"headline"`, `"net_disbursement"`, `"gross_disbursement"`, `"commitment"`, or `"grant_equivalent"`.
- `Dimension`: `"provider"`, `"recipient"`, `"year"`, `"sector"`, `"broad_sector"`, `"channel"`, `"modality"`, `"finance_type"`, `"flow"`, `"line"`, `"score"`, or `"allocation_status"`. Each indicator has some of them, listed in [Indicators](indicators.md).
- `TableName`: `"dac1"`, `"dac2a"`, `"crs"`, or `"multisystem"`.

`oda_data.__version__` is the installed version.

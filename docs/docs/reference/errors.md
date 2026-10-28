# Errors and warnings

Every error subclasses `OdaDataError` and, where one fits, a built-in exception, so you can catch by intent: `except LookupError` catches an unknown indicator or name. Each class has a stable `code` attribute, and each message ends with `[<code>]`. Codes and classes are part of the API. Message text can change between releases.

```text
no indicator is named 'oecd.dac1.totl_oda'; did you mean oecd.dac1.total_oda, oecd.dac1.bilateral_oda, oecd.dac1.multilateral_oda? [unknown_indicator]
('oecd.dac1.total_oda', 'oecd.dac1.bilateral_oda', 'oecd.dac1.multilateral_oda')
```

```python
import oda_data as oda

try:
    oda.get("oecd.dac1.totl_oda", years=2024)
except oda.UnknownIndicatorError as error:
    print(error)
    print(error.suggestions)
```

## Errors

| Class                         | `code`                    | Also a            | Raised when                                                                                                                                                                                                                    |
| ----------------------------- | ------------------------- | ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `OdaDataError`                | `oda_data_error`          |                   | Base class of every error below                                                                                                                                                                                                |
| `UnknownIndicatorError`       | `unknown_indicator`       | `LookupError`     | No indicator has the id. `suggestions` holds near ids. A 2.x code gives its 3.0 call in the message                                                                                                                            |
| `UnknownEntityError`          | `unknown_entity`          | `LookupError`     | A filter value matches no code or exact name. `suggestions` holds near names, the message names the first three, and `system` holds the code system tried                                                                      |
| `AmbiguousEntityError`        | `ambiguous_entity`        | `ValueError`      | A name could mean more than one entity, such as `"DAC countries"`, which is both an OECD aggregate and a group. `candidates` lists them                                                                                        |
| `DimensionError`              | `dimension`               | `ValueError`      | The indicator lacks a dimension used in `by=` or a filter. The message names an indicator that has it                                                                                                                          |
| `BasisRangeError`             | `basis_range`             | `ValueError`      | The basis does not cover a year asked for, such as grant equivalents before 2018. The message names the bases that do                                                                                                          |
| `UnsupportedQueryError`       | `unsupported_query`       | `ValueError`      | The call is undefined, such as `unit=` on a ratio, an unsupported currency, `records()` on a ratio, `total()` outside `by`, a code the indicator leaves out, or 918 summed with a provider that pays it on DAC1 or Multisystem |
| `MissingFactorError`          | `missing_factor`          |                   | A provider lacks a conversion factor in a year. The message names the unit and the provider-years                                                                                                                              |
| `DataUnavailableError`        | `data_unavailable`        | `ConnectionError` | oda-reader or pydeflate could not fetch or read a table. The cause is chained                                                                                                                                                  |
| `OfflineCacheMissError`       | `offline_cache_miss`      |                   | The context is offline and the cache lacks a table the call needs                                                                                                                                                              |
| `ReleaseMismatchError`        | `release_mismatch`        |                   | A pinned release differs from the cached one                                                                                                                                                                                   |
| `RemovedIn3Error`             | `removed_in_3`            | `ImportError`     | Code reads a 2.x name, such as `oda_data.OECDClient`. The message gives the replacement                                                                                                                                        |
| `ImputationConservationError` | `imputation_conservation` |                   | An imputation did not conserve the core contributions it spread                                                                                                                                                                |
| `UnmappedChannelError`        | `unmapped_channel`        |                   | A multilateral provider and agency pair in the CRS has no channel in the bundled crosswalk                                                                                                                                     |

`RemovedIn3Error` subclasses `ImportError` so that `from oda_data import ODAData` shows the replacement. Python turns an `AttributeError` raised there into a bare "cannot import name".

Every error survives a pickle round trip with its attributes, so it crosses process boundaries intact.

## Errors that name the fix

A question the indicator's table cannot answer raises an error that names the indicator or basis that can.

```text
oecd.dac1.total_oda has no 'sector' dimension; its dimensions are provider, year, flow. Use oecd.crs.bilateral_oda for figures by sector [dimension]
oecd.dac1.total_oda on the 'grant_equivalent' basis starts in 2018, so it has no 2016; basis 'headline' or 'net_disbursement' or 'gross_disbursement' or 'commitment' covers them [basis_range]
```

```python
import oda_data as oda

for call in (
    lambda: oda.get("oecd.dac1.total_oda", years=2023, by=["sector"]),
    lambda: oda.get("oecd.dac1.total_oda", years=2016, basis="grant_equivalent"),
):
    try:
        call()
    except (oda.DimensionError, oda.BasisRangeError) as error:
        print(error)
```

## Names that fail

A name that matches nothing raises `UnknownEntityError`, and its message ends with the nearest names. A name that matches two things raises `AmbiguousEntityError`, and its message says what to pass instead.

```text
no provider is named 'Frnace'; did you mean France? [unknown_entity]
'DAC countries' could mean provider 20001 (DAC Countries, Total) or the group oda.groups.dac_countries; pass the code or the group [ambiguous_entity]
('20001', 'dac_countries')
```

```python
import oda_data as oda

for name in ("Frnace", "DAC countries"):
    try:
        oda.get("oecd.dac1.total_oda", years=2024, providers=name)
    except (oda.UnknownEntityError, oda.AmbiguousEntityError) as error:
        print(error)
        if isinstance(error, oda.AmbiguousEntityError):
            print(error.candidates)
```

The two Congos are COG (234), the Republic of the Congo, and COD (235), the Democratic Republic of the Congo. `recipients="Congo"` means COG and raises no error.

## Warnings

`OdaDataWarning` subclasses `UserWarning` and flags a caveat on a result the call still returns:

- A call that asks for the table's last year, when providers reported a part of the indicator the year before and have not reported it yet. One warning per year reads "`<year>` is partial: providers that reported a part in `<year - 1>` have not reported it in `<year>`." It then names each part once with what the part does to the result, such as "Line 1820, flow 1160 is missing for France (4), and missing from the g7 total for 7 providers." A provider the call shows is named, with its rows of the part missing or null. A sum across providers outside `by` is short of the part or has no row of it, and an `oda.total()` group has no row of it, with their providers counted. The warning names five parts and five providers per part, and counts the rest. A provider that stopped reporting before the year before does not count. `allow_partial=True` returns the partial values of null rows and short sums and leaves them out of the warning, and a part with no row warns whatever it says. A sum that is null for another reason, such as a part a derived indicator subtracts and a provider has not yet reported, is not in the warning, and `explain()` says why it is null. `explain()` gives one sentence per part.
- A caveat scoped to a basis or years that the call reads, such as grant equivalents on `oecd.dac1.lines` from 2015 to 2017, which only 25 to 27 DAC members reported.

Caveats with no basis or years appear in `describe()` and `explain()` and raise no warning. To turn every caveat into an error in a pipeline, run Python with `-W error::oda_data.OdaDataWarning`.

## See also

- [Counting rules](../explanation/counting-rules.md) explains the rules behind `UnsupportedQueryError` and the partial-year warning.
- [Migrating from 2.x](../migration.md) covers `RemovedIn3Error` and 2.x codes.

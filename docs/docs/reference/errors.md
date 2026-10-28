# Errors and warnings

Every error subclasses `OdaDataError` and, where one fits, a built-in exception, so you can catch by intent: `except LookupError` catches an unknown indicator or name. Each class has a stable `code` attribute, and each message ends with `[<code>]`. Codes and classes are part of the API. Message text is not.

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

| Class                         | `code`                    | Also a            | Raised when                                                                                                                                                                                  |
| ----------------------------- | ------------------------- | ----------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `OdaDataError`                | `oda_data_error`          |                   | Base class of every error below                                                                                                                                                              |
| `UnknownIndicatorError`       | `unknown_indicator`       | `LookupError`     | No indicator has the id. `suggestions` holds near ids. A 2.x code gives its 3.0 call in the message                                                                                          |
| `UnknownEntityError`          | `unknown_entity`          | `LookupError`     | A filter value matches no code or exact name. `suggestions` holds near names and `system` the code system tried                                                                              |
| `AmbiguousEntityError`        | `ambiguous_entity`        | `ValueError`      | A name could mean more than one entity, such as `"Congo"`, or an OECD aggregate and a group. `candidates` lists them                                                                         |
| `DimensionError`              | `dimension`               | `ValueError`      | The indicator lacks a dimension used in `by=` or a filter. The message names an indicator that has it                                                                                        |
| `BasisRangeError`             | `basis_range`             | `ValueError`      | The basis does not cover a year asked for, such as grant equivalents before 2018. The message names the bases that do                                                                        |
| `UnsupportedQueryError`       | `unsupported_query`       | `ValueError`      | The call is not defined: `unit=` on a ratio, an unsupported currency, `records()` on a ratio, `total()` outside `by`, a code the indicator leaves out, or 918 summed with EU members on DAC1 |
| `MissingFactorError`          | `missing_factor`          |                   | A provider lacks a conversion factor in a year. The message names the unit and the provider-years                                                                                            |
| `DataUnavailableError`        | `data_unavailable`        | `ConnectionError` | oda-reader or pydeflate could not fetch or read a table. The cause is chained                                                                                                                |
| `OfflineCacheMissError`       | `offline_cache_miss`      |                   | The context is offline and the cache lacks a table the call needs                                                                                                                            |
| `ReleaseMismatchError`        | `release_mismatch`        |                   | A pinned release differs from the cached one                                                                                                                                                 |
| `RemovedIn3Error`             | `removed_in_3`            | `ImportError`     | Code reads a 2.x name, such as `oda_data.OECDClient`. The message gives the replacement                                                                                                      |
| `ImputationConservationError` | `imputation_conservation` |                   | An imputation did not conserve the core contributions it spread                                                                                                                              |
| `UnmappedChannelError`        | `unmapped_channel`        |                   | A multilateral provider and agency pair in the CRS has no channel in the bundled crosswalk                                                                                                   |

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

## Warnings

`OdaDataWarning` subclasses `UserWarning` and flags a caveat on a result the call still returns:

- A sum across providers in the table's last year, when providers that reported the year before have not reported it yet. The message counts them. `allow_partial=True` silences it, and `explain()` still names the partial year.
- A caveat scoped to a basis or years that the call reads, such as grant equivalents on `oecd.dac1.lines` from 2015 to 2017, which only 25 to 27 DAC members reported.

Caveats with no basis or years appear in `describe()` and `explain()` and raise no warning. To turn every caveat into an error in a pipeline, run Python with `-W error::oda_data.OdaDataWarning`.

## See also

- [Counting rules](../explanation/counting-rules.md) explains the rules behind `UnsupportedQueryError` and the partial-year warning.
- [Migrating from 2.x](../migration.md) covers `RemovedIn3Error` and 2.x codes.

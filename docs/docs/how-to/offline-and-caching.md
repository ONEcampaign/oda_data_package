# How to work offline and choose the cache folder

oda-data reads the OECD's bulk files through oda-reader, which downloads each table once and keeps it on disk. Build an `oda.Context` to choose the cache folder, to refuse network access, or to serve an application.

## Choose where tables are kept

Without a context, oda-data uses oda-reader's cache folder, which is `ODA_READER_CACHE_DIR` if it is set and the platform's cache folder otherwise. Set the variable before Python starts to move every call:

```bash
export ODA_READER_CACHE_DIR=~/data/oda-cache
```

To choose a folder for one application, pass `cache_dir=` to a context and call its methods, which take the same arguments as the module functions:

```text
  recipient_name   value
0       Ethiopia  5378.3
```

```python
import oda_data as oda

with oda.Context(cache_dir="oda-cache") as ctx:
    df = ctx.get("oecd.dac2a.recipient_oda", years=2023, recipients="Ethiopia", by=["recipient"])

print(df[["recipient_name", "value"]].round(1))
```

Leaving the `with` block closes the context's DuckDB connection. A context serves many threads at once, so an application needs one per cache folder.

## Know what a first call downloads

Each table downloads the first time an indicator reads it.

| Table       | Download | Read by                                                  |
| ----------- | -------- | -------------------------------------------------------- |
| DAC1        | 10 MB    | `oecd.dac1.*`, `one.dac1.*`, and every unit conversion   |
| DAC2A       | 118 MB   | `oecd.dac2a.*`, `one.dac2a.*`                            |
| Multisystem | 122 MB   | `oecd.multisystem.*`, `one.crs.imputed_multilateral_oda` |
| CRS         | 1.2 GB   | `oecd.crs.*`, `one.crs.*`                                |

oda-reader converts DAC1, DAC2A, and Multisystem to Parquet once per release, and DuckDB reads only the columns and row groups a call needs, so a CRS call reads a small part of the file.

## Work offline

Pass `offline=True` to make no network request. Calls read the cached tables whatever their age, and a table missing from the cache raises `OfflineCacheMissError`.

```text
  provider_name             basis    value
0        Norway  grant_equivalent  5178.59
```

```python
import oda_data as oda

with oda.Context(cache_dir="oda-cache", offline=True) as ctx:
    df = ctx.get("oecd.dac1.total_oda", years=2024, providers="NOR")

print(df[["provider_name", "basis", "value"]])
```

To fill the cache on a machine without access to the OECD's servers, download the bulk file in a browser and register it with `oda_reader.register_bulk_file(path, dataset="crs")`. The `OfflineCacheMissError` message gives the same instruction.

## See what the cache holds

```text
dac1         0x8DF2539D20A32DF          10 MB  stale=False
dac2a        0x8DF2539E5D52F9D         118 MB  stale=False
multisystem  0x8DF2522071A444B         122 MB  stale=False
crs          0x8DF2533665E7CB7       1,177 MB  stale=False
```

```python
import oda_reader

for entry in oda_reader.cache_info(cache_dir="oda-cache"):
    print(f"{entry.dataset:<12} {entry.release:<20} {entry.size_bytes / 1e6:>8,.0f} MB  stale={entry.stale}")
```

`oda_reader.scan("crs", refresh=True)` downloads a table again. A context built after the download reads the new release, and one built before keeps the release it first read, so every call through one context reads the same files. The module functions share one default context per process, so restart the Python session to read a new download through them.

## Serve an application

A data loader that runs at build time builds its own context and writes each result to Parquet, with the provenance in the file:

```python
import oda_data as oda

dac = oda.groups.dac_countries

with oda.Context(cache_dir="oda-cache") as ctx:
    q = ctx.query(
        "oecd.dac1.total_oda",
        years=range(2000, 2025),
        providers=[dac, oda.total(dac)],
        unit=[oda.current("USD"), oda.constant("USD", 2024)],
    )
    q.to_parquet("total_oda.parquet")
```

A loader whose framework reads standard output, as Observable Framework's do, passes `sys.stdout.buffer` to `to_parquet()` in place of a path.

## See also

- [How to record provenance and pin releases](provenance.md) holds a run to the releases a published figure used.
- [Public API](../reference/api.md#context) documents `Context`.

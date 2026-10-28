# How to record provenance and pin releases

Build a call with `oda.query()` to read its provenance, write it to Parquet with the provenance in the file, or hold a later run to the same OECD releases.

## Read a result's provenance

```text
3.0.0 {'oecd.crs.bilateral_oda': 1}
crs 0x8DF2533665E7CB7 v20261006 2026-10-27
dac1 0x8DF2539D20A32DF None 2026-10-27
{'recipients': 'all', 'basis': 'gross_disbursement'}
```

```python
import oda_data as oda

q = oda.query(
    "oecd.crs.bilateral_oda",
    years=2023,
    providers="FRA",
    by=["broad_sector"],
    unit=oda.constant("EUR", 2023),
)
record = q.provenance
print(record.oda_data, dict(record.indicators))
for table, release in record.tables.items():
    print(table, release.token, release.label_release, f"{release.downloaded:%Y-%m-%d}")
print(dict(record.defaults))
```

The record names the oda-data version, each indicator and its version, and each table the result read with its release. DAC1 appears because the euro factors come from it. `defaults` lists every default the call relied on. `pydeflate`, `resolvekit`, and `reference_data` name the versions behind the conversion factors, the name lookups, and the bundled groups and crosswalks.

A release token is the file host's ETag or Last-Modified date, so two machines that downloaded the same OECD file hold the same token. The CRS also carries a readable label such as `v20261006`.

## Write a result with its provenance

`to_parquet()` writes the result with the record in the file's metadata. `oda.provenance(path)` reads it back.

```text
v20261006
```

```python
import oda_data as oda

q = oda.query("oecd.crs.bilateral_oda", years=2023, providers="FRA", by=["broad_sector"])
q.to_parquet("bilateral.parquet")

record = oda.provenance("bilateral.parquet")
print(record.tables["crs"].label_release)
```

`to_parquet()` also takes a binary file object, such as `sys.stdout.buffer` in a data loader that writes to standard output. The `Query` runs once and keeps its result, so `explain()`, `provenance`, and `to_parquet()` read the tables one time between them.

## Hold a run to the same releases

Pass the tokens to `oda.Context(releases=...)`. A pinned table is read from the cache when the cached copy carries the pinned token, and a call fails with `ReleaseMismatchError` when it does not. `cache_dir` is the folder that holds the cached tables, here `oda-cache` beside the script.

```text
11106.0
```

```python
import oda_data as oda

pins = {name: release.token for name, release in oda.provenance("bilateral.parquet").tables.items()}

with oda.Context(cache_dir="oda-cache", releases=pins) as ctx:
    df = ctx.get("oecd.crs.bilateral_oda", years=2023, providers="FRA", by=["broad_sector"])

print(round(df.value.sum(), 1))
```

/// warning | Heads up
oda-reader keeps the files of the current and the previous release of each table and deletes older ones. A pin to a deleted release fails with `ReleaseMismatchError`, so keep the Parquet output of a published figure beside the code that made it.
///

## Verify it worked

`oda.provenance(path)` on the file you wrote returns a record equal to `q.provenance`:

```python
import oda_data as oda

q = oda.query("oecd.dac1.total_oda", years=2024)
q.to_parquet("total_oda.parquet")
assert oda.provenance("total_oda.parquet") == q.provenance
```

## See also

- [Versions and releases](../explanation/versioning.md) explains what changes a number and how each change is announced.
- [Public API](../reference/api.md#provenance) documents `Provenance` and `Release`.

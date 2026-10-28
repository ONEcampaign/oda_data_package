# Versions and releases

A number oda-data returns depends on three things that change on their own schedules: the package, the indicator definition, and the OECD release it read. Each carries its own version, and provenance records all three.

## The package

oda-data follows semantic versioning for its documented API. The API is every name in `oda_data.__all__` and the behaviour this site documents. Modules whose names start with an underscore are private and change without notice.

A breaking change waits for a major release. It is any of these:

- removing or renaming a public name, argument, indicator id, dimension, or basis
- changing a default
- changing a result column's name, order, dtype, or missing-value form
- changing an error class or its `code`

Message text, `explain()` wording, and `repr` text carry no promise, so match errors on their class or `code`. A deprecation warns with the version that removes the name and the call that replaces it.

## The indicator

Each indicator has a `version`, shown in `describe()`, in the `indicator_version` column, and in provenance. A change to the rows an indicator counts, which changes a published number, bumps its version in a minor release, and the changelog states the size of the effect. Adding an indicator is a minor release too.

So a pinned package version gives the same definition, and comparing `indicator_version` across two saved results tells you whether a definition moved between them.

## The OECD release

The OECD revises its tables several times a year, sometimes back to the 1960s. oda-data reads whatever release oda-reader has cached and records its token. Two results computed from the same tokens with the same package version are identical. Pin the tokens with `oda.Context(releases=...)` to rerun a published figure, and keep its Parquet output, since oda-reader keeps only the current and previous release of each table.

The conversion factors come from the DAC1 release in the same context, so ODA figures and deflators always describe the same data.

## How the numbers are checked

Each indicator has a conformance case, a small input table with its expected output. Golden tests compare about 30 indicator and filter combinations with the OECD's published totals and with oda-data 2.8.2 run on the same tables, among them the check that `oda.total(oda.groups.dac_countries)` equals the DAC countries total in current USD in every year. Every Monday, `scripts/check_codelists.py` compares the package with the OECD's latest codelists. It fails when a code the package uses is missing from them, when the package names a code differently, and when they list an active CRS purpose code, channel, finance type, or modality the package has no name for. The codelists flag TOSSD-only codes, such as the seven-digit purpose codes, as outside the CRS, so those pass.

## 2.x

oda-data 2.8 is the last 2.x release and stays installable with `pip install "oda-data<3"`. [Migrating from 2.x](../migration.md) maps every 2.x code to its 3.0 call.

## See also

- [How to record provenance and pin releases](../how-to/provenance.md)
- [Changelog](../changelog.md)

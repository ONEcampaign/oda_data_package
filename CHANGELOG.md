# Changes to the oda_data package

## [2.0.0bx]
This is the first beta of version 2. Version 2 is a complete refactoring of the package 
which breaks backwards compatibility. We will continue to support v1.5.x for the coming months 
but we expect to fully transition to v2 by the fall of 2025.

---

## [1.5.0] 2024-11-29
* Updated requirements to pydeflate >=2.0
* Updated indicators to remove climate indicators (given the methodological challenges inherent in OECD data). For access to climate data, please see the climate-finance package.


## [1.4.3] 2024-11-29
* Fixes a json validation error for recipient groupings

## [1.4.2] 2024-11-26
* Fixes donors and recipient groupings to fully align with recent schemas.

## [1.4.1] 2024-10-11
* Fixes a bug with how certain files are stored, to move them from feather to parquet.

## [1.4.0] 2024-10-11
* This release introduces significant changes to how raw data files are managed. Instead of storing as feather files, the default is now parquet. This allows oda_data to leverage predicate pushdown and more efficiently load only the data it needs.
* This release removes data download tools from oda_data in favour of using the tools via oda-reader. Our oda-reader package uses the new data-explorer API and bulk downloads to get the data, instead of relying on the old (and now inaccessible) bulk download service.
* It is strongly recommended that all users update to this version.

## [1.3.3] 2024-09-16
* This release fixes issues reading bulk files from the OECD (given that the bulk download service doesn't exist as such anymore)


## [1.3.1] 2024-07-16
* This release aligns the schema of the temporary fix with the expected CRS schema from the bulk download service.


## [1.3.0] 2024-07-16
* This release includes a workaround for the OECD bulk download service, which is down following the release of the new OECD website. The workaround uses a full CRS file shared by the OECD, which
can take a long time to download, especially on slow connections (its nearly 1GB).

## [1.2] 2024-04-05
* This release uses `oda_reader` to download data for DAC1 and DAC2a directly from the API.
For now, the data is converted to the .Stat schema in order to ensure full backwards compatibility.
A future version of `oda_data` will deprecated the .Stat schema in favor of the explorer API schema.
* Other dependencies are updated.

## [1.1.6] 2024-03-14
* Update pydeflate dependency to deal with data download issue.

## [1.1.5] 2024-03-07
* Fixed a bug introduced by changes in the OECD bulk download service.

## [1.1.4] 2024-03-01
* Fix constant non-USD currencies bug for imputed sectors calculations.

## [1.1.3] 2024-02-29
* Fix sorting bug (arrow)

## [1.1.2] 2024-02-29
* Reading the CRS from 1973-2004 is now possible.
* Removed a warning on pandas stack (for future behaviour)

## [1.1.1] 2024-02-29
* Security updates to dependencies

## [1.1.0] 2024-02-29
* Introduces important changes:
  - New indicators to separately produce multilateral sector spending shares and imputed multilateral spending totals.
  - Introduces an improved, automated method to map multilateral CRS spending (by agency) to the multilateral "channels" used in the multisystem database.
  - Introduces tools to group purpose codes following ONE's sector groupings.

## [1.0.11] 2024-01-04
* Fix key COVID indicators.

## [1.0.10] 2023-12-11
* Add UTF8 encoding.

## [1.0.7] 2023-12-11
* Update requirements for security.

## [1.0.6] 2023-10-21
* Fixed bug caused by new readme files in the bulk download service file.

## [1.0.5] 2023-08-24
* Updated how the CRS codes are fetched given the connection issues outlined in the notes for 1.0.4.

## [1.0.5] 2023-08-24
* Updated how the indicators that use the `multisystem` database work. The OECD quietly changed
the output format of the database, which broke the parsing of the data. The new format is now
supported.

## [1.0.4] 2023-08-24
* Developed a backup solution to download bulk files from the OECD website. Given an insecure
SSL certificate, the normal download using `requests` fails. The backup solution uses `selenium` to
download the files using a browser. This is a bit slower, but it works.
* Updated requirements to add `selenium` and `webdriver-manager`


## [1.0.3] 2023-06-12
* Updated requirements (pydeflate) to address the same OECD data bug as in 1.0.2

## [1.0.2] 2023-06-12
* Updated requirements
* Fixed an encoding bug that affected CRS data given a new file encoding from the OECD bulk downloads

## [1.0.1] 2023-04-13
* Updated requirements to a newer version of pydeflate, given data quality issues with the latest oecd release

### Updated
* Updated requirements

## [1.0.0] 2023-02-20

First major release of the oda_data. We have settled on the basic functionality of the package and
the basic API.

### Updated
* Updated requirements


## [0.4.1] 2023-01-30

### Updated

* Updated requirements

## [0.4.0] 2023-01-30

### Added

* Added indicators for climate finance data

## [0.3.5] 2023-01-12


### Fixed

* Issues with research indicators in non-usd data

## [0.3.4] 2023-01-12


### Fixed

* Issues with gender data

## [0.3.3] 2023-01-13


### Fixed

* Issues with multilateral non core ODA


## [0.3.2] 2023-01-12


### Fixed

* Issues with multilateral sector imputations

## [0.3.0] 2023-01-10


### Added

* ONE Core ODA indicators (flows, ge, linked ge), including 'non Core' indicators
* An "official definition" total ODA indicator

## [0.2.5] 2022-12-21


### Added

* The ability to retrieve COVID-19 indicators


## [0.2.3] 2022-12-16

### Fixed

* The ODA GNI indicators, which returned mostly invalid data from the source
* A typo in the ODA GNI indicator name
* How `OECDClient` deals with adding shares to indicators for which shares don't make sense

## [0.2.1] 2022-12-16

### Changed

* Download data for indicator automatically if not available in data folder

## [0.2.0] 2022-12-16

### Changed

* `OECDClient().load_indicator()` now accepts a list of indicators as input.

### Added

* A method to OECDClient in order to add a "share" column to the output data.
* A method to OECDClient in order to add a "gni_share"" column to the output data.

## [0.1.10] 2022-12-09

### Added

* A total (ODA + OOF, excluding export credits) indicator for the CRS

## [0.1.9] 2022-12-07

### Changed

* Changed how indicators are grouped when requesting a 'one' indicator.
  instead of returning fewer columns than the raw indicators, it will return the
  same columns, excluding the ones that make up the requested indicator

### Added

* The ability to request a 'one_linked' indicator. These indicators are composed of a main indicator
  which is _completed_ by a fallback indicator, when the values are missing. For example, In-Donor Refugee Costs
  should be the same in Grant Equivalents or Flows. If the values are missing in the former, they are filled by the
  latter.
* An option to get a simplified/summarised dataframe. Calling `.simplify_output_df()` on the `OECDClient` object will
* keep only the requested columns, applying a `.groupby().sum()` on the remaining columns.
* Added documentation for the `OECDClient` class

## [0.1.8] 2022-11-29

### Added

* More comprehensive tests of all core functionalities
* A tool to extract CRS codes from the DAC CRS code list

## [0.1.7] 2022-11-24

This version mainly tweaks the file structure.

### Fixed

* Fixed an issue with trying to set a file path for both oda_data and pydeflate

## [0.1.6] 2022-11-24

Minor improvements

## [0.1.5] 2022-11-24

Minor improvements

## [0.1.4] 2022-11-24

Minor improvements

## [0.1.3] 2022-11-24

Minor improvements

## [0.1.2] 2022-11-24

Minor improvements

## [0.1.1] 2022-11-24

Minor improvements

## [0.1.0] 2022-11-24

First release of oda_data
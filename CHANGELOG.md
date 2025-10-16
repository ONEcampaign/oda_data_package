# Changelog

All notable changes to the oda_data package will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.2.2] - 2025-09-26

### Fixed
- Bug with marker calculations

## [2.2.1] - 2025-09-26

### Added
- Access to sector imputations via `from oda_data import sector_imputations`

### Fixed
- Issues with filter passing given schema changes in bulk files on the OECD side

## [2.1.2] - 2025-09-01

### Fixed
- Caching paths now respect user-defined data directories and default to a `.raw_data` folder relative to the working directory

## [2.1.1] - 2025-07-23

### Fixed
- Bug where GNI may not get converted to constant prices even if a base year is specified

## [2.1.0] - 2025-06-16

### Changed
- Improved AidDataData to behave more like other Sources

## [2.0.6] - 2025-06-16

### Fixed
- Bug when trying to calculate multilateral imputations in constant prices

## [2.0.5] - 2025-06-13

### Fixed
- Bug caused by the Providers multisystem dataset using a form of pascal case

## [2.0.4] - 2025-06-13

### Changed
- CRS research indicators now use bulk downloads by default

## [2.0.3] - 2025-06-13

### Changed
- Better bulk file memory management

## [2.0.2] - 2025-05-28

### Added
- Functionality to calculate the official ODA/GNI

## [2.0.1] - 2025-04-25

### Changed
- Improved caching performance by keeping both memory and disk cache of parquet files

## [2.0.0] - 2025-04-22

This major release is a complete refactoring of the `oda-data` package. It is now faster,
more stable, and better organized.

### Changed
- Complete package refactoring with improved performance and stability
- **BREAKING**: Major API changes - please refer to the project README for migration details
- Versions ~1.5.x will remain supported until at least August 2025 to allow time to migrate workflows

---

## [1.5.0] - 2024-11-29

### Changed
- Updated requirements to pydeflate >=2.0
- Removed climate indicators (given methodological challenges inherent in OECD data). For access to climate data, please see the climate-finance package

## [1.4.3] - 2024-11-29

### Fixed
- JSON validation error for recipient groupings

## [1.4.2] - 2024-11-26

### Fixed
- Donors and recipient groupings to fully align with recent schemas

## [1.4.1] - 2024-10-11

### Fixed
- Bug with how certain files are stored, moving them from feather to parquet

## [1.4.0] - 2024-10-11

This release introduces significant changes to how raw data files are managed. It is strongly recommended that all users update to this version.

### Changed
- Default storage format changed from feather to parquet files, allowing oda_data to leverage predicate pushdown and more efficiently load only the data it needs
- Removed data download tools from oda_data in favor of using the tools via oda-reader
- oda-reader package now uses the new data-explorer API and bulk downloads instead of relying on the old (and now inaccessible) bulk download service

## [1.3.3] - 2024-09-16

### Fixed
- Issues reading bulk files from the OECD (given that the bulk download service no longer exists)

## [1.3.1] - 2024-07-16

### Fixed
- Schema of the temporary fix to align with the expected CRS schema from the bulk download service

## [1.3.0] - 2024-07-16

### Added
- Workaround for the OECD bulk download service, which is down following the release of the new OECD website
- Uses a full CRS file shared by the OECD (note: nearly 1GB and can take a long time to download on slow connections)

## [1.2.0] - 2024-04-05

### Changed
- Now uses `oda_reader` to download data for DAC1 and DAC2a directly from the API
- Data is converted to the .Stat schema to ensure full backwards compatibility
- Updated dependencies

### Deprecated
- .Stat schema will be deprecated in a future version in favor of the explorer API schema

## [1.1.6] - 2024-03-14

### Changed
- Updated pydeflate dependency to deal with data download issue

## [1.1.5] - 2024-03-07

### Fixed
- Bug introduced by changes in the OECD bulk download service

## [1.1.4] - 2024-03-01

### Fixed
- Constant non-USD currencies bug for imputed sectors calculations

## [1.1.3] - 2024-02-29

### Fixed
- Sorting bug (arrow)

## [1.1.2] - 2024-02-29

### Added
- Support for reading the CRS from 1973-2004

### Fixed
- Removed a warning on pandas stack (for future behavior)

## [1.1.1] - 2024-02-29

### Security
- Security updates to dependencies

## [1.1.0] - 2024-02-29

### Added
- New indicators to separately produce multilateral sector spending shares and imputed multilateral spending totals
- Improved, automated method to map multilateral CRS spending (by agency) to the multilateral "channels" used in the multisystem database
- Tools to group purpose codes following ONE's sector groupings

## [1.0.11] - 2024-01-04

### Fixed
- Key COVID indicators

## [1.0.10] - 2023-12-11

### Changed
- Added UTF8 encoding

## [1.0.7] - 2023-12-11

### Security
- Updated requirements for security

## [1.0.6] - 2023-10-21

### Fixed
- Bug caused by new readme files in the bulk download service file

## [1.0.5] - 2023-08-24

### Changed
- Updated how the CRS codes are fetched given the connection issues outlined in the notes for 1.0.4
- Updated how the indicators that use the `multisystem` database work - the OECD quietly changed the output format of the database, which broke the parsing of the data. The new format is now supported

## [1.0.4] - 2023-08-24

### Added
- Backup solution to download bulk files from the OECD website using `selenium` (given an insecure SSL certificate that causes the normal download using `requests` to fail)
- Dependencies: `selenium` and `webdriver-manager`

## [1.0.3] - 2023-06-12

### Changed
- Updated requirements (pydeflate) to address the same OECD data bug as in 1.0.2

## [1.0.2] - 2023-06-12

### Fixed
- Encoding bug that affected CRS data given a new file encoding from the OECD bulk downloads

### Changed
- Updated requirements

## [1.0.1] - 2023-04-13

### Changed
- Updated requirements to a newer version of pydeflate, given data quality issues with the latest OECD release

## [1.0.0] - 2023-02-20

First major release of oda_data. We have settled on the basic functionality of the package and the basic API.

### Changed
- Updated requirements


## [0.4.1] - 2023-01-30

### Changed
- Updated requirements

## [0.4.0] - 2023-01-30

### Added
- Indicators for climate finance data

## [0.3.5] - 2023-01-12

### Fixed
- Issues with research indicators in non-USD data

## [0.3.4] - 2023-01-12

### Fixed
- Issues with gender data

## [0.3.3] - 2023-01-13

### Fixed
- Issues with multilateral non core ODA

## [0.3.2] - 2023-01-12

### Fixed
- Issues with multilateral sector imputations

## [0.3.0] - 2023-01-10

### Added
- ONE Core ODA indicators (flows, ge, linked ge), including 'non Core' indicators
- "Official definition" total ODA indicator

## [0.2.5] - 2022-12-21

### Added
- Ability to retrieve COVID-19 indicators

## [0.2.3] - 2022-12-16

### Fixed
- ODA GNI indicators, which returned mostly invalid data from the source
- Typo in the ODA GNI indicator name
- How `OECDClient` deals with adding shares to indicators for which shares don't make sense

## [0.2.1] - 2022-12-16

### Changed
- Download data for indicator automatically if not available in data folder

## [0.2.0] - 2022-12-16

### Added
- Method to OECDClient to add a "share" column to the output data
- Method to OECDClient to add a "gni_share" column to the output data

### Changed
- `OECDClient().load_indicator()` now accepts a list of indicators as input

## [0.1.10] - 2022-12-09

### Added
- Total (ODA + OOF, excluding export credits) indicator for the CRS

## [0.1.9] - 2022-12-07

### Added
- Ability to request a 'one_linked' indicator - these indicators are composed of a main indicator which is completed by a fallback indicator when values are missing (e.g., In-Donor Refugee Costs should be the same in Grant Equivalents or Flows; if values are missing in the former, they are filled by the latter)
- Option to get a simplified/summarized dataframe by calling `.simplify_output_df()` on the `OECDClient` object, which keeps only the requested columns and applies `.groupby().sum()` on the remaining columns
- Documentation for the `OECDClient` class

### Changed
- How indicators are grouped when requesting a 'one' indicator - instead of returning fewer columns than the raw indicators, it returns the same columns, excluding the ones that make up the requested indicator

## [0.1.8] - 2022-11-29

### Added
- More comprehensive tests of all core functionalities
- Tool to extract CRS codes from the DAC CRS code list

## [0.1.7] - 2022-11-24

This version mainly tweaks the file structure.

### Fixed
- Issue with trying to set a file path for both oda_data and pydeflate

## [0.1.6] - 2022-11-24

Minor improvements

## [0.1.5] - 2022-11-24

Minor improvements

## [0.1.4] - 2022-11-24

Minor improvements

## [0.1.3] - 2022-11-24

Minor improvements

## [0.1.2] - 2022-11-24

Minor improvements

## [0.1.1] - 2022-11-24

Minor improvements

## [0.1.0] - 2022-11-24

First release of oda_data

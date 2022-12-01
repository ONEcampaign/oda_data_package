# Changes to the oda_data package

## [0.1.9] Unreleased


### Changed
* Changed how indicators are grouped when requesting a 'one' indicator.
instead of returning fewer columns than the raw indicators, it will return the
same columns, excluding the ones that make up the requested indicator

### Added
* The ability to request a 'one_linked' indicator. These indicators are composed of a main indicator
which is _completed_ by a fallback indicator, when the values are missing. For example, In-Donor Refugee Costs
should be the same in Grant Equivalents or Flows. If the values are missing in the former, they are filled by the latter.


### Fixed
* ...

### Removed
* ...

### Breaking changes
* ...

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
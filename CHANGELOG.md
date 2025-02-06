# Changelog

## [0.6.0]
### Updates
- Adding compression support
- Making start optional in getData method

## [0.5.3]
### Fixes
- Updating eslint to version 9

## [0.5.2]
### Fixes
- In write method options did not work when writing data with create and finalize actions
- Updating dependencies
- Adding type definition for JSONStreamParser

## [0.5.1]
### Fixes
- Dependency update, fixing a bug in the filters

## [0.5.0]
### Fixes
- Moving filter functionality to js-array-filter library
- Adding write functionality with 2 methods: write and writeData


## [0.4.3]
### Fixes
- Fixed issue with filters having array of values
- Updated tests

## [0.4.1]
### Fixes
- Added Filter type definitions to export
- Adding more operators for strings

## [0.4.0]
### Updates
- Added data filtering
- Add encoding option in constructor
- Option filterColumns now also works for type = "array" in the getData method

## [0.3.4]
### Fixes
- Fixed definitions: ItemType type definition, removed isReferenceData

## [0.3.3]
### Fixes
- Fixed type export

## [0.3.2]
### Added
- Added type export

## [0.3.1]
### Fixes
- Fixed first row issue for NDJSON

## [0.3.0]
### Added
- NDJSON support
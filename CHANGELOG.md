# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

## [v0.2.2](https://github.com/NGS360/PAML.git/releases/tag/v0.2.2) - 2025-01-27

## [v0.3-rc3](https://github.com/NGS360/PAML.git/releases/tag/v0.3-rc3) - 2024-12-16

### Changed

- Update support Python version >= 3.9

## [v0.3-rc2](https://github.com/NGS360/PAML.git/releases/tag/v0.3-rc2) - 2024-12-16

### Changed

- Update support Python version >= 3.11

## [v0.3-rc1](https://github.com/NGS360/PAML.git/releases/tag/v0.3-rc1) - 2024-12-16

### Added

- Add create_project method
- Add get_user method
- Various unit tests

### Changed

- Update support Python version to 3.13
- Update version of arvados-python to 3.0

### Fixed

- Rename parameter executing_settings to execution_settings in submit_task
- Addressed various pylint issues
- Handle list outputs in CWL workflows
- Check Arvados Outputs collection for cwl output prior to accessing

## [v0.2.1](https://github.com/NGS360/PAML/releases/tag/v0.2.1) - 2024-10-21

### Fixed

- Catch SevenBridges exception when creating invalid task

## [v0.2](https://github.com/NGS360/PAML/releases/tag/v0.2) - 2024-06-14

### Added

- get_task_outputs method
- rename_output_files
- roll_file method

### Fixed

- Fixed bug in how project is retrieved from SevenBridges

## [v0.1.1](https://github.com/NGS360/PAML/releases/tag/v0.1.1) - 2024-04-09

### Fixed

- Fixed how we iterate files in a SevenBridges project

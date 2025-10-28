# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

## [v0.3.3](https://github.com/NGS360/PAML.git/releases/tag/v0.3.3) - 2025-10-28

### Fixed

- Address issue where a newly created task was added to the task list. submit_task was returning a task without a name in Arvados (PR #91)
- Fix incorrect type hint for Arvados in get_task_by_name (PR #96)
- Fix get_task_state for Arvados not properly checking container request status (PR #97)

## [v0.3.2](https://github.com/NGS360/PAML.git/releases/tag/v0.3.2) - 2025-07-12

### Fixed

- ArvadosPlatform::get_tasks_by_name was not properly handling list of tasks provided

## [v0.3.1](https://github.com/NGS360/PAML.git/releases/tag/v0.3.1) - 2025-06-09

### Added

- get_tasks_by_name can now return all tasks in a project, to assist with caching

### Changed

- Update Arvados Python Client requirement to use >=3.0.0 and <3.2.0

## [v0.3](https://github.com/NGS360/PAML.git/releases/tag/v0.3) - 2025-06-02

### Added

- Added get_costs method
- Added get_projects method

### Changed

- Update Arvados Python Client to 3.1.1
- get_task_by_name now checks for equilevant input values 
- Remove SevenBridges Endpoint and make that a parameter to connect()
- Update get_task_input to handle list of files
- Resolve failing SBG integration test

## [v0.2.5](https://github.com/NGS360/PAML.git/releases/tag/v0.2.5) - 2025-03-11

### Added

- Added get_files() method along with associated integration tests
- Added various other method to support project loading/exporting including:
    add_user_to_project()
    delete_project_by_name()
    get_workflows()
    get_project_users()

### Changed

- Remove support for Python 3.8 and Python 3.13, so only supporting 3.9 - 3.12
- Rename upload_file_to_project to upload_file

### Fixed

- Fixed get_user method to ignore case sensitivity

## [v0.2.4](https://github.com/NGS360/PAML.git/releases/tag/v0.2.4) - 2025-02-14

- Fix get_task_output for optional fields when queried in Arvados

## [v0.2.3](https://github.com/NGS360/PAML.git/releases/tag/v0.2.3) - 2025-02-12

- Fix removal of collection.all_files() from Arvados Python SDK.

## [v0.2.2](https://github.com/NGS360/PAML.git/releases/tag/v0.2.2) - 2025-01-27

- Downgraded support for arvados-python to 2.7.4

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

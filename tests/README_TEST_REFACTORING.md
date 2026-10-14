# Test Refactoring Guide

## Overview

This document describes the pattern for refactoring unit tests across the three platform implementations (Arvados, SevenBridges, NGS360) to reduce duplication and ensure consistency.

## Current State

The three platform test files contain significant duplication:
- `test_arvados_platform.py`: 43 tests (1069 lines)
- `test_sevenbridges_platform.py`: 59 tests (1245 lines)
- `test_ngs360_ga4gh_platform.py`: 33 tests (957 lines)

Many tests validate the same abstract `Platform` interface contract but are duplicated across files.

## Refactoring Pattern

### Base Test Class (`test_base_platform.py`)

A `BasePlatformTests` abstract class has been created that contains shared test logic. Platform-specific test classes inherit from this base and implement required abstract methods for:

1. **Platform-specific mocking**: `setup_platform_mocks()`
2. **Creating test objects**: `create_mock_task()`, `create_mock_file()`, `create_mock_project()`
3. **Helper methods**: `get_task_name()`, `get_task_id()`, etc.

### Example: Arvados Implementation

```python
class TestArvadosPlaform(BasePlatformTests, unittest.TestCase):
    '''Test Class for Arvados Platform'''
    
    def setUp(self):
        self.platform = ArvadosPlatform('Arvados')
        self.platform.api = MagicMock()
        self.platform.keep_client = MagicMock()
        self.setup_platform_mocks()
        return super().setUp()

    def setup_platform_mocks(self):
        '''Set up Arvados-specific mocks'''
        pass  # Already set up in setUp()

    def create_mock_task(self, task_id, name, state='Complete', inputs=None, outputs=None):
        '''Create an Arvados-specific mock task'''
        container_request = {
            'name': name,
            'uuid': task_id,
            'container_uuid': f'container-{task_id}',
            'properties': {'cwl_input': inputs or {}}
        }
        # ... platform-specific implementation
        return ArvadosTask(container_request=container_request, container=container)

    # Implement other abstract methods...
```

## Tests Suitable for Sharing

The following test patterns are good candidates for extraction to the base class:

### Task Management Tests
- ✅ `test_get_tasks_by_name()` - basic name filtering
- ✅ `test_get_tasks_by_name_match_all()` - return all when no name
- ✅ `test_get_tasks_by_name_from_provided_tasks()` - use provided task list
- ✅ `test_get_tasks_by_name_with_new_task_added()` - handle missing metadata
- ⚠️  `test_get_tasks_by_name_match_name_and_inputs()` - requires input comparison (platform-specific)

### Input/Output Tests  
- ✅ `test_get_task_input_non_file_obj()` - simple value inputs
- ⚠️  `test_get_task_input_file_obj()` - file input (platform-specific format)
- ⚠️  `test_get_task_input_list_of_file_obj()` - file list (platform-specific format)

### Output Tests
- ⚠️  `test_get_task_output_filename_single_file()` - requires mock setup
- ⚠️  `test_get_task_output_filename_list()` - requires mock setup
- ✅ `test_output_filename_nonexistant_output_name()` - error handling
- ✅ `test_output_filename_none()` - null handling

**Legend:**
- ✅ Easy to share - minimal platform-specific mocking
- ⚠️  Moderate - requires platform-specific helper methods
- ❌ Keep separate - too platform-specific

## Challenges and Recommendations

###  Challenge 1: Platform-Specific Mocking Patterns

**Problem**: Each platform has unique mocking requirements:
- **Arvados**: Uses `container_request` dict with nested `properties.cwl_input`
- **SevenBridges**: Uses SDK objects like `sevenbridges.Task` and `sevenbridges.File`
- **NGS360**: Uses REST API response dicts with `run_id` and `request.workflow_params`

**Recommendation**: Focus on sharing test logic for methods that don't require complex mocking, such as:
- Simple getter methods
- Error handling paths
- Edge cases (None values, empty lists, etc.)

### Challenge 2: Input/Output Comparison Logic

**Problem**: Each platform implements file/directory comparison differently:
- **Arvados**: `_compare_inputs()` compares CWL `location` fields
- **SevenBridges**: `_compare_platform_object()` handles SDK objects with `.id` attributes
- **NGS360**: Direct JSON comparison of workflow parameters

**Recommendation**: Keep comparison logic tests platform-specific. These are core functionality that benefit from detailed, platform-specific test coverage.

### Challenge 3: Test Coverage Consistency

**Problem**: Not all platforms test the same scenarios. For example:
- Arvados has 43 tests
- SevenBridges has 59 tests (includes extra file operation tests)
- NGS360 has 33 tests (newer implementation, less coverage)

**Recommendation**: Use the base class to document the expected test coverage for the `Platform` interface, ensuring new platforms implement all required tests.

## Migration Strategy

### Phase 1: Documentation (CURRENT)
- ✅ Create base test class with abstract methods
- ✅ Document refactoring pattern
- ✅ Identify sharable vs. platform-specific tests

### Phase 2: Gradual Refactoring (FUTURE)
1. Start with simplest tests (error handling, edge cases)
2. Extract one test at a time, verify all platforms still pass
3. Remove duplicates only after base class test is proven
4. Keep test count the same or increase

### Phase 3: New Platform Implementations
- New platforms can inherit from `BasePlatformTests`
- Implement abstract methods for platform-specific behavior
- Automatically inherit shared test coverage

## Running Tests

```bash
# Run all tests for a specific platform
pytest tests/test_arvados_platform.py -v

# Run shared tests across all platforms  
pytest tests/test_*_platform.py -k "get_tasks_by_name" -v

# Check test coverage
pytest --cov=src/cwl_platform --cov-report=html
```

## Best Practices

1. **Keep tests independent**: Each test should set up its own mocks
2. **Use descriptive names**: Test names should clearly describe what's being tested
3. **Test one thing**: Each test method should verify a single behavior
4. **Platform-specific tests are OK**: Not everything needs to be shared
5. **Consistency over DRY**: If sharing complicates tests, keep them separate

## Example: Removing a Duplicate Test

```python
# BEFORE: Duplicate test in test_arvados_platform.py
class TestArvadosPlaform(unittest.TestCase):
    def test_get_tasks_by_name_with_new_task_added(self):
        # Arvados-specific implementation...
        
# AFTER: Inherited from base class
class TestArvadosPlaform(BasePlatformTests, unittest.TestCase):
    # Test now inherited from BasePlatformTests
    # Only implement abstract helper methods
```

## Current Status

- **Base class created**: `test_base_platform.py` with 13 shared test methods
- **Arvados updated**: Inherits from base class, implements all abstract methods
- **SevenBridges**: Not yet refactored (pending)
- **NGS360**: Not yet refactored (pending)

## Next Steps

1. Verify Arvados tests still pass with base class inheritance
2. Identify and remove safe-to-remove duplicate tests from Arvados
3. Apply same pattern to SevenBridges platform
4. Apply pattern to NGS360 platform
5. Add missing tests to ensure consistent coverage

## Benefits

- **Consistency**: All platforms tested with same comprehensive suite
- **Maintainability**: Update interface tests in one place
- **Documentation**: Base class serves as executable spec of Platform interface
- **Extensibility**: New platforms easier to add

## Notes

- This is a gradual refactoring - tests remain functional throughout
- Platform-specific tests should remain in individual files
- Focus on test structure, not test behavior
- Continue using existing mocking patterns for consistency

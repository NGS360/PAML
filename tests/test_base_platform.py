'''
Base Platform Test Class

This module provides shared infrastructure and helper methods for platform tests.
Platform-specific test classes should inherit from BasePlatformTests to access
common utilities and maintain consistent test patterns.

Note: This base class provides infrastructure and documentation, but concrete
tests remain in platform-specific files due to significant differences in mocking
patterns across platforms.
'''
import unittest
from abc import ABC, abstractmethod


class BasePlatformTests(ABC):
    '''
    Abstract base class providing shared infrastructure for platform tests.

    Platform-specific test classes inherit from this to gain:
    - Common helper methods for creating mock objects
    - Standardized test patterns
    - Documentation of expected test coverage

    Concrete platform test classes must implement the abstract methods to provide
    platform-specific mock objects and behaviors.

    Example:
        class TestArvadosPlatform(BasePlatformTests, unittest.TestCase):
            def setUp(self):
                self.platform = ArvadosPlatform('Arvados')
                self.setup_platform_mocks()

            def setup_platform_mocks(self):
                self.platform.api = MagicMock()
                # ... additional platform-specific setup
    '''

    # Abstract methods for platform-specific implementations

    @abstractmethod
    def setup_platform_mocks(self):
        '''
        Set up platform-specific mocks after platform instance is created.
        This should mock API clients, authentication, etc.

        Example (Arvados):
            def setup_platform_mocks(self):
                self.platform.api = MagicMock()
                self.platform.keep_client = MagicMock()
        '''

    @abstractmethod
    def create_mock_task(self, task_id, name, state='Complete', inputs=None, outputs=None):
        '''
        Create a platform-specific mock task object.

        Args:
            task_id: Unique task identifier
            name: Task name
            state: Task state (Complete, Running, Failed, Queued, etc.)
            inputs: Dictionary of task inputs
            outputs: Dictionary of task outputs

        Returns:
            Platform-specific task object

        Example (Arvados):
            def create_mock_task(self, task_id, name, state='Complete', inputs=None, outputs=None):
                container_request = {'name': name, 'uuid': task_id, ...}
                container = {'uuid': f'container-{task_id}', ...}
                return ArvadosTask(container_request=container_request, container=container)
        '''

    @abstractmethod
    def create_mock_file(self, file_id, filename):
        '''
        Create a platform-specific mock file object.

        Args:
            file_id: Unique file identifier
            filename: File name

        Returns:
            Platform-specific file object

        Example (SevenBridges):
            def create_mock_file(self, file_id, filename):
                mock_file = MagicMock(spec=sevenbridges.File)
                mock_file.id = file_id
                mock_file.name = filename
                return mock_file
        '''

    @abstractmethod
    def create_mock_project(self, project_id, name):
        '''
        Create a platform-specific mock project object.

        Args:
            project_id: Unique project identifier
            name: Project name

        Returns:
            Platform-specific project object

        Example (NGS360):
            def create_mock_project(self, project_id, name):
                return {'project_id': project_id, 'name': name}
        '''

    # Helper methods with default implementations (can be overridden)

    def create_platform_file_input(self, file_id):
        '''
        Create a platform-specific file input representation.

        Default implementation returns a CWL-style dict. Override for platform-specific formats.

        Args:
            file_id: File identifier

        Returns:
            Platform-specific file input object/dict
        '''
        return {
            'class': 'File',
            'path': file_id
        }

    def get_task_name(self, task):
        '''
        Get the name from a platform-specific task object.

        Default implementation assumes task has a 'name' attribute.
        Override if platform uses different structure.

        Args:
            task: Platform-specific task object

        Returns:
            Task name as string
        '''
        return task.name if hasattr(task, 'name') else task.get('name')

    def get_task_id(self, task):
        '''
        Get the ID from a platform-specific task object.

        Default implementation assumes task has an 'id' or 'uuid' attribute.
        Override if platform uses different structure.

        Args:
            task: Platform-specific task object

        Returns:
            Task ID as string
        '''
        if hasattr(task, 'id'):
            return task.id
        elif hasattr(task, 'uuid'):
            return task.uuid
        elif isinstance(task, dict):
            return task.get('id') or task.get('uuid')
        return None


# Expected test coverage for Platform interface implementations
#
# All platform test classes should implement tests for the following methods:
#
# Task Management:
# - test_get_tasks_by_name() - Filter tasks by name
# - test_get_tasks_by_name_match_all() - Return all tasks when no name provided
# - test_get_tasks_by_name_from_provided_tasks() - Use provided task list
# - test_get_tasks_by_name_with_new_task_added() - Handle tasks with missing metadata
# - test_get_tasks_by_name_match_name_and_inputs() - Filter by both name and inputs (optional)
# - test_submit_task() - Basic task submission
# - test_delete_task() - Task deletion
# - test_get_task_state() - Task state retrieval
#
# Input/Output:
# - test_get_task_input_non_file_obj() - Simple value inputs
# - test_get_task_input_file_obj() - Single file input
# - test_get_task_input_list_of_file_obj() - List of file inputs
# - test_get_task_output() - Basic output retrieval
# - test_get_task_output_filename_single_file() - Filename from single file
# - test_get_task_output_filename_list() - Filenames from multiple files
# - test_output_filename_nonexistant_output_name() - Missing output handling
# - test_output_filename_none() - Null output handling
#
# Connection:
# - test_connect() - Successful connection
# - test_detect_platform() - Platform detection
#
# File Operations:
# - test_upload_file() - Basic file upload
# - test_copy_file() - File copying (platform-specific)
# - test_copy_folder() - Folder copying (platform-specific)
#
# Platform-Specific:
# - Each platform should test its unique features (e.g., Arvados collection operations,
#   SevenBridges directory operations, NGS360 GA4GH WES API specifics)

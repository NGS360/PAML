'''
Base Platform Test Class

This module contains shared test logic for all platform implementations.
Platform-specific test classes should inherit from BasePlatformTests and
implement the required abstract methods.
'''
import unittest
from abc import ABC, abstractmethod


class BasePlatformTests(ABC):
    '''
    Abstract base class containing shared tests for all platform implementations.

    Concrete platform test classes must implement:
    - setup_platform_mocks(): Configure platform-specific mocks
    - create_mock_task(): Create platform-specific task objects
    - create_mock_file(): Create platform-specific file objects
    - create_mock_project(): Create platform-specific project objects
    '''

    # Abstract methods that must be implemented by platform-specific test classes

    @abstractmethod
    def setup_platform_mocks(self):
        '''
        Set up platform-specific mocks after platform instance is created.
        This should mock API clients, authentication, etc.
        '''

    @abstractmethod
    def create_mock_task(self, task_id, name, state='Complete', inputs=None, outputs=None):
        '''
        Create a platform-specific mock task object.

        :param task_id: Unique task identifier
        :param name: Task name
        :param state: Task state (Complete, Running, Failed, etc.)
        :param inputs: Dictionary of task inputs
        :param outputs: Dictionary of task outputs
        :return: Platform-specific task object
        '''

    @abstractmethod
    def create_mock_file(self, file_id, filename):
        '''
        Create a platform-specific mock file object.

        :param file_id: Unique file identifier
        :param filename: File name
        :return: Platform-specific file object
        '''

    @abstractmethod
    def create_mock_project(self, project_id, name):
        '''
        Create a platform-specific mock project object.

        :param project_id: Unique project identifier
        :param name: Project name
        :return: Platform-specific project object
        '''

    # Shared test methods for task management

    def test_get_tasks_by_name(self):
        '''Test get_tasks_by_name method with task name filtering'''
        matching_task_name = "matching_task"
        non_matching_task_name = "non_matching_task"

        # Create mock tasks
        mock_task_match = self.create_mock_task('task1', matching_task_name)
        mock_task_not_matching = self.create_mock_task('task2', non_matching_task_name)

        # Set up platform-specific mocks to return both tasks
        self.mock_get_all_tasks([mock_task_match, mock_task_not_matching])

        # Create mock project
        project = self.create_mock_project('project_uuid', 'Test Project')

        # Test - should only return matching task
        result = self.platform.get_tasks_by_name(project, matching_task_name)

        # Assert
        self.assertEqual(len(result), 1)
        self.assertEqual(self.get_task_name(result[0]), matching_task_name)

    def test_get_tasks_by_name_match_all(self):
        '''Test get_tasks_by_name method with no task name (should return all tasks)'''
        # Create mock tasks
        task1 = self.create_mock_task('task1', 'Task 1')
        task2 = self.create_mock_task('task2', 'Task 2')

        # Set up platform-specific mocks
        self.mock_get_all_tasks([task1, task2])

        # Create mock project
        project = self.create_mock_project('project_uuid', 'Test Project')

        # Test - should return all tasks
        result = self.platform.get_tasks_by_name(project)

        # Assert
        self.assertEqual(len(result), 2)
        self.assertEqual(self.get_task_name(result[0]), 'Task 1')
        self.assertEqual(self.get_task_name(result[1]), 'Task 2')

    def test_get_tasks_by_name_match_name_and_inputs(self):
        '''Test get_tasks_by_name method with task name and matching inputs'''
        task_name = "sample_task"

        # Define inputs to compare
        inputs_to_compare = {
            'input1': {
                'class': 'File',
                'path': 'file1'
            },
            'input2': [
                {
                    'class': 'File',
                    'path': 'file2'
                },
                {
                    'class': 'File',
                    'path': 'file3'
                }
            ]
        }

        # Create task with matching inputs
        task1_inputs = {
            'input1': self.create_platform_file_input('file1'),
            'input2': [
                self.create_platform_file_input('file2'),
                self.create_platform_file_input('file3')
            ]
        }
        task1 = self.create_mock_task('task1', task_name, inputs=task1_inputs)

        # Create task with different inputs
        task2_inputs = {
            'input1': self.create_platform_file_input('file1'),
            'input2': [
                self.create_platform_file_input('file2'),
                self.create_platform_file_input('different_file')
            ]
        }
        task2 = self.create_mock_task('task2', task_name, inputs=task2_inputs)

        # Set up platform-specific mocks
        self.mock_get_all_tasks([task1, task2])

        # Create mock project
        project = self.create_mock_project('project_uuid', 'Test Project')

        # Test - should only return task1
        result = self.platform.get_tasks_by_name(
            project,
            task_name=task_name,
            inputs_to_compare=inputs_to_compare
        )

        # Assert
        self.assertEqual(len(result), 1)
        self.assertEqual(self.get_task_id(result[0]), 'task1')

    def test_get_tasks_by_name_from_provided_tasks(self):
        '''Test that get_tasks_by_name can use provided tasks'''
        # Create mock tasks
        task1 = self.create_mock_task('task1', 'Task 1')
        task2 = self.create_mock_task('task2', 'Task 2')
        tasks = [task1, task2]

        # Create mock project
        project = self.create_mock_project('project_uuid', 'Test Project')

        # Test with provided tasks
        result = self.platform.get_tasks_by_name(
            project=project,
            task_name='Task 1',
            tasks=tasks
        )

        # Assert
        self.assertEqual(len(result), 1)
        self.assertEqual(self.get_task_name(result[0]), 'Task 1')

    def test_get_tasks_by_name_with_new_task_added(self):
        '''Test get_tasks_by_name with a new task that may only have id, not name'''
        # Create mock task
        task1 = self.create_mock_task('task1', 'Task 1')
        tasks = [task1]

        # Create mock project
        project = self.create_mock_project('project_uuid', 'Test Project')

        # Test searching for task that doesn't exist
        result = self.platform.get_tasks_by_name(
            project=project,
            task_name='Task 2',
            tasks=tasks
        )

        # Assert
        self.assertListEqual(result, [])

    def test_delete_task(self):
        '''Test delete_task method'''
        # Create mock task
        task = self.create_mock_task('task123', 'Test Task')

        # Set up mock for delete operation
        self.mock_delete_task(task)

        # Test
        self.platform.delete_task(task)

        # Assert - verify delete was called (platform-specific verification)
        self.verify_task_deleted(task)

    # Shared test methods for task input/output

    def test_get_task_input_non_file_obj(self):
        '''Test get_task_input method where the input is not a File object (e.g. string)'''
        test_value = "test_value"

        # Create mock task with string input
        task = self.create_mock_task('task1', 'Test Task', inputs={'input1': test_value})

        # Test
        actual_result = self.platform.get_task_input(task, 'input1')

        # Assert
        self.assertEqual(actual_result, test_value)

    def test_get_task_input_file_obj(self):
        '''Test get_task_input method with a single File object'''
        test_file_id = 'file123'

        # Create mock task with file input
        file_input = self.create_platform_file_input(test_file_id)
        task = self.create_mock_task('task1', 'Test Task', inputs={'input1': file_input})

        # Test
        actual_result = self.platform.get_task_input(task, 'input1')

        # Assert - should return file ID
        self.assertEqual(actual_result, test_file_id)

    def test_get_task_input_list_of_file_obj(self):
        '''Test get_task_input method with a list of File objects'''
        test_file1_id = 'file1'
        test_file2_id = 'file2'

        # Create mock task with list of file inputs
        file_inputs = [
            self.create_platform_file_input(test_file1_id),
            self.create_platform_file_input(test_file2_id)
        ]
        task = self.create_mock_task('task1', 'Test Task', inputs={'input1': file_inputs})

        # Test
        actual_result = self.platform.get_task_input(task, 'input1')

        # Assert
        self.assertEqual(actual_result, [test_file1_id, test_file2_id])

    def test_get_task_output_filename_single_file(self):
        '''Test get_task_output_filename when output is a single file'''
        expected_filename = "output_file.txt"

        # Create mock task with file output
        file_output = self.create_mock_file('file123', expected_filename)
        task = self.create_mock_task('task1', 'Test Task', outputs={'output_name': file_output})

        # Set up platform-specific mocks for output retrieval
        self.mock_get_task_output(task, 'output_name', file_output)

        # Test
        filename = self.platform.get_task_output_filename(task, 'output_name')

        # Assert
        self.assertEqual(filename, expected_filename)

    def test_get_task_output_filename_list(self):
        '''Test get_task_output_filename when output is a list of files'''
        expected_filenames = ["output_file1.txt", "output_file2.txt"]

        # Create mock task with list of file outputs
        file_outputs = [
            self.create_mock_file('file1', expected_filenames[0]),
            self.create_mock_file('file2', expected_filenames[1])
        ]
        task = self.create_mock_task('task1', 'Test Task', outputs={'output_name': file_outputs})

        # Set up platform-specific mocks for output retrieval
        self.mock_get_task_output(task, 'output_name', file_outputs)

        # Test
        filenames = self.platform.get_task_output_filename(task, 'output_name')

        # Assert
        self.assertListEqual(filenames, expected_filenames)

    def test_output_filename_nonexistant_output_name(self):
        '''Test get_task_output_filename when output name does not exist'''
        # Create mock task
        task = self.create_mock_task('task1', 'Test Task', outputs={'output_name': []})

        # Set up platform-specific mocks for missing output
        self.mock_get_task_output(task, 'not_an_output_name', None)

        # Test - should raise ValueError
        with self.assertRaises(ValueError):
            self.platform.get_task_output_filename(task, 'not_an_output_name')

    def test_output_filename_none(self):
        '''Test get_task_output_filename when value of output_name is None'''
        # Create mock task
        task = self.create_mock_task('task1', 'Test Task', outputs={'output_name': None})

        # Set up platform-specific mocks for None output
        self.mock_get_task_output(task, 'output_name', None)

        # Test - should raise ValueError
        with self.assertRaises(ValueError):
            self.platform.get_task_output_filename(task, 'output_name')

    # Helper methods that must be implemented by platform-specific test classes

    @abstractmethod
    def mock_get_all_tasks(self, tasks):
        '''
        Set up platform-specific mocks to return the given list of tasks.

        :param tasks: List of mock task objects to return
        '''

    @abstractmethod
    def mock_delete_task(self, task):
        '''
        Set up platform-specific mocks for task deletion.

        :param task: Mock task object to delete
        '''

    @abstractmethod
    def verify_task_deleted(self, task):
        '''
        Verify that the task deletion was called correctly.

        :param task: Mock task object that should have been deleted
        '''

    @abstractmethod
    def create_platform_file_input(self, file_id):
        '''
        Create a platform-specific file input representation.

        :param file_id: File identifier
        :return: Platform-specific file input object/dict
        '''

    @abstractmethod
    def mock_get_task_output(self, task, output_name, output_value):
        '''
        Set up platform-specific mocks for get_task_output.

        :param task: Mock task object
        :param output_name: Name of the output
        :param output_value: Value to return for the output
        '''

    @abstractmethod
    def get_task_name(self, task):
        '''
        Get the name from a platform-specific task object.

        :param task: Platform-specific task object
        :return: Task name as string
        '''

    @abstractmethod
    def get_task_id(self, task):
        '''
        Get the ID from a platform-specific task object.

        :param task: Platform-specific task object
        :return: Task ID as string
        '''

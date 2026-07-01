'''
Test Module for SevenBridges Platform
'''
# pylint: disable=protected-access
import unittest
import os
import logging
import mock
from mock import MagicMock
import sevenbridges

from cwl_platform.sevenbridges_platform import SevenBridgesPlatform

class TestSevenBridgesPlaform(unittest.TestCase):
    '''
    Test Class for SevenBridges Platform
    '''
    def setUp(self) -> None:
        os.environ['SESSION_ID'] = 'dummy'
        self.platform = SevenBridgesPlatform('SevenBridges')
        self.platform.api = MagicMock()

        logging.basicConfig(level=logging.INFO)

        return super().setUp()

    def test_add_user_to_project(self):
        ''' Test that we can add a user to a project '''
        # Set up test parameters
        platform_user = 'auser'
        project = MagicMock()
        permission = 'admin'
        # Test
        self.platform.add_user_to_project(platform_user, project, permission)
        # Check results
        project.add_member.assert_called_once()

    @mock.patch('sevenbridges.Api')
    def test_connect(self, mock_api_client):
        ''' Test connect method '''
        mock_api_client.return_value = MagicMock()

        self.platform.connect()
        self.assertTrue(self.platform.connected)

    def test__compare_platform_object_string(self):
        '''
        Test that we can compare two non-list, non File inputs: string edition
        '''
        test_value = "test_string"
        test_platform_input = test_value
        test_cwl_input = test_value

        result = self.platform._compare_platform_object(test_platform_input, test_cwl_input)

        self.assertTrue(result)

    def test__compare_platform_object_int(self):
        '''
        Test that we can compare two non-list, non File inputs: int edition
        '''
        test_value = 123
        test_platform_input = test_value
        test_cwl_input = test_value

        result = self.platform._compare_platform_object(test_platform_input, test_cwl_input)

        self.assertTrue(result)

    def test__compare_platform_object_simple_unequal(self):
        '''
        Test that we can compare two non-list, non File inputs: not equal, string vs int
        '''
        test_value = 123
        test_platform_input = test_value
        test_cwl_input = "123"

        result = self.platform._compare_platform_object(test_platform_input, test_cwl_input)

        self.assertFalse(result)

    def test__compare_platform_object_file(self):
        '''
        Test that we can compare two equivalent File-type objects
        '''
        test_file_id = 'a1234'
        mock_file = MagicMock(spec=sevenbridges.File, id = test_file_id)
        mock_file.is_folder.return_value = False
        test_platform_input = mock_file

        test_cwl_input = {
            'class': 'File',
            'path': test_file_id
        }

        result = self.platform._compare_platform_object(test_platform_input, test_cwl_input)
        self.assertTrue(result)

    def test__compare_platform_object_file_not_equal(self):
        '''
        Test that we can correctly return false when comparing differing File-type objects
        '''
        test_file_id = 'a1234'
        mock_file = MagicMock(spec=sevenbridges.File, id = test_file_id)
        mock_file.is_folder.return_value = False
        test_platform_input = mock_file

        test_cwl_input = {
            'class': 'File',
            'path': "not the same id"
        }

        result = self.platform._compare_platform_object(test_platform_input, test_cwl_input)
        self.assertFalse(result)

    def test__compare_platform_object_file_not_file(self):
        '''
        Test that we can correctly return false when comparing a File and a non-file dict
        '''
        test_file_id = 'a1234'
        mock_file = MagicMock(spec=sevenbridges.File, id = test_file_id)
        mock_file.is_folder.return_value = False
        test_platform_input = mock_file

        test_cwl_input = {
            'class': 'NotAFile'
        }

        result = self.platform._compare_platform_object(test_platform_input, test_cwl_input)
        self.assertFalse(result)

    def test__compare_platform_simple_array(self):
        '''
        Test that we can compare two arrays with simple objects
        '''
        test_value = ["thing1", "thing2"]
        test_platform_input = test_value
        test_cwl_input = test_value

        result = self.platform._compare_platform_object(test_platform_input, test_cwl_input)

        self.assertTrue(result)

    def test__compare_platform_file_array(self):
        '''
        Test that we can compare two File arrays
        '''

        test_file_id1 = 'a1234'
        test_file_id2 = 'b2345'
        mock_file1 = MagicMock(spec=sevenbridges.File, id = test_file_id1)
        mock_file1.is_folder.return_value = False
        mock_file2 = MagicMock(spec=sevenbridges.File, id = test_file_id2)
        mock_file2.is_folder.return_value = False
        test_platform_input = [mock_file1, mock_file2]
        test_cwl_input = [{
            'class': 'File',
            'path': test_file_id1
        },
            {
            'class': 'File',
            'path': test_file_id2
        }]
        result = self.platform._compare_platform_object(test_platform_input, test_cwl_input)

        self.assertTrue(result)

    def test__compare_platform_simple_array_differing_length(self):
        '''
        Test that we can compare two arrays with simple objects but unequal length
        '''
        test_value = ["thing1", "thing2"]
        test_platform_input = test_value
        test_cwl_input = ["thing1"]

        result = self.platform._compare_platform_object(test_platform_input, test_cwl_input)

        self.assertFalse(result)

    def test__compare_platform_simple_array_not_equal(self):
        '''
        Test that we can compare two arrays of simple objects with unequal values
        '''
        test_value = ["thing1", "thing2"]
        test_platform_input = test_value
        test_cwl_input = ["thing1",2]

        result = self.platform._compare_platform_object(test_platform_input, test_cwl_input)

        self.assertFalse(result)

    def test__compare_platform_file_array_not_equal(self):
        '''
        Test that we can compare two File arrays with unequal values
        '''
        test_file_id1 = 'a1234'
        test_file_id2 = 'b2345'
        mock_file1 = MagicMock(spec=sevenbridges.File, id = test_file_id1)
        mock_file1.is_folder.return_value = False
        mock_file2 = MagicMock(spec=sevenbridges.File, id = test_file_id2)
        mock_file2.is_folder.return_value = False
        test_platform_input = [mock_file1, mock_file2]
        test_cwl_input = [{
            'class': 'File',
            'path': test_file_id1
        },
            {
            'class': 'File',
            'path': "not the same id"
        }]
        result = self.platform._compare_platform_object(test_platform_input, test_cwl_input)

        self.assertFalse(result)


    def test__compare_platform_directory(self):
        '''
        Test that we can compare two Directories with identical contents
        This is complex just to test multiple cases all in one go:

        Project root
        |- file_in_root
        |- folder
            |- file_inside_folder
            |- nested_folder
                |- file1_inside_nested_folder
                |- file2_inside_nested_folder
        '''
        file_in_root_id = 'file_in_root'
        folder_id = 'folder'
        file_inside_folder_id = 'file_inside_folder'
        nested_folder_id = 'nested_folder'
        file1_inside_nested_folder_id = 'file1_inside_nested_folder'
        file2_inside_nested_folder_id = 'file2_inside_nested_folder'

        mock_file_in_root = MagicMock(spec=sevenbridges.File, id = file_in_root_id)
        mock_file_in_root.is_folder.return_value = False
        mock_folder = MagicMock(spec=sevenbridges.File, id = folder_id)
        mock_folder.is_folder.return_value = True
        mock_file_inside_folder = MagicMock(spec=sevenbridges.File, id = file_inside_folder_id)
        mock_file_inside_folder.is_folder.return_value = False
        mock_nested_folder = MagicMock(spec=sevenbridges.File, id = nested_folder_id)
        mock_nested_folder.is_folder.return_value = True
        mock_file1_inside_nested_folder = MagicMock(spec=sevenbridges.File,
                                                    id = file1_inside_nested_folder_id)
        mock_file1_inside_nested_folder.is_folder.return_value = False
        mock_file2_inside_nested_folder = MagicMock(spec=sevenbridges.File,
                                                    id = file2_inside_nested_folder_id)
        mock_file2_inside_nested_folder.is_folder.return_value = False

        # nested folder mocks
        nested_folder_all_mock = MagicMock()
        nested_folder_all_mock.return_value = [
            mock_file1_inside_nested_folder, mock_file2_inside_nested_folder
        ]
        nested_list_files_mock = MagicMock()
        nested_list_files_mock.all = nested_folder_all_mock
        mock_nested_folder.list_files.return_value = nested_list_files_mock

        # first level folder mocks
        folder_all_mock = MagicMock()
        folder_all_mock.return_value = [mock_file_inside_folder, mock_nested_folder]
        folder_list_files_mock = MagicMock()
        folder_list_files_mock.all = folder_all_mock
        mock_folder.list_files.return_value = folder_list_files_mock

        test_platform_input = [mock_file_in_root, mock_folder]
        test_cwl_input = [
            {
                'class': 'File',
                'path': file_in_root_id
            },
            {
                'class': 'Directory',
                'path': folder_id,
                'listing': [
                    {
                        'class': 'File',
                        'path': file_inside_folder_id
                    },
                    {
                        'class': 'Directory',
                        'path': nested_folder_id,
                        'listing': [
                            {
                                'class': 'File',
                                'path': file1_inside_nested_folder_id
                            },
                            {
                                'class': 'File',
                                'path': file2_inside_nested_folder_id
                            }
                        ]
                    }
                ]
            }
        ]

        result = self.platform._compare_platform_object(test_platform_input, test_cwl_input)

        self.assertTrue(result)

    def test__compare_platform_directory_unequal_length(self):
        '''
        Test that we can compare two Directories with differing number of files inside
        The only difference in inputs is a missing file2 in inside the nested folder

        Project root
        |- file_in_root
        |- folder
            |- file_inside_folder
            |- nested_folder
                |- file1_inside_nested_folder
                |- file2_inside_nested_folder [present in test_platform_input,
                                               but missing in test_cwl_input]
        '''
        file_in_root_id = 'file_in_root'
        folder_id = 'folder'
        file_inside_folder_id = 'file_inside_folder'
        nested_folder_id = 'nested_folder'
        file1_inside_nested_folder_id = 'file1_inside_nested_folder'
        file2_inside_nested_folder_id = 'file2_inside_nested_folder'

        mock_file_in_root = MagicMock(spec=sevenbridges.File, id = file_in_root_id)
        mock_file_in_root.is_folder.return_value = False
        mock_folder = MagicMock(spec=sevenbridges.File, id = folder_id)
        mock_folder.is_folder.return_value = True
        mock_file_inside_folder = MagicMock(spec=sevenbridges.File, id = file_inside_folder_id)
        mock_file_inside_folder.is_folder.return_value = False
        mock_nested_folder = MagicMock(spec=sevenbridges.File, id = nested_folder_id)
        mock_nested_folder.is_folder.return_value = True
        mock_file1_inside_nested_folder = MagicMock(spec=sevenbridges.File,
                                                    id = file1_inside_nested_folder_id)
        mock_file1_inside_nested_folder.is_folder.return_value = False
        mock_file2_inside_nested_folder = MagicMock(spec=sevenbridges.File,
                                                    id = file2_inside_nested_folder_id)
        mock_file2_inside_nested_folder.is_folder.return_value = False

        # nested folder mocks
        nested_folder_all_mock = MagicMock()
        nested_folder_all_mock.return_value = [
            mock_file1_inside_nested_folder, mock_file2_inside_nested_folder
        ]
        nested_list_files_mock = MagicMock()
        nested_list_files_mock.all = nested_folder_all_mock
        mock_nested_folder.list_files.return_value = nested_list_files_mock

        # first level folder mocks
        folder_all_mock = MagicMock()
        folder_all_mock.return_value = [mock_file_inside_folder, mock_nested_folder]
        folder_list_files_mock = MagicMock()
        folder_list_files_mock.all = folder_all_mock
        mock_folder.list_files.return_value = folder_list_files_mock

        test_platform_input = [mock_file_in_root, mock_folder]
        test_cwl_input = [
            {
                'class': 'File',
                'path': file_in_root_id
            },
            {
                'class': 'Directory',
                'path': folder_id,
                'listing': [
                    {
                        'class': 'File',
                        'path': file_inside_folder_id
                    },
                    {
                        'class': 'Directory',
                        'path': nested_folder_id,
                        'listing': [
                            {
                                'class': 'File',
                                'path': file1_inside_nested_folder_id
                            }
                        ]
                    }
                ]
            }
        ]

        result = self.platform._compare_platform_object(test_platform_input, test_cwl_input)

        self.assertFalse(result)

    def test__compare_platform_directory_different_inner_elements(self):
        '''
        Test that we can compare two Directories which have different inner files
        The only difference in inputs is that the second file inside the nested folder is different

        Project root
        |- file_in_root
        |- folder
            |- file_inside_folder
            |- nested_folder
                |- file1_inside_nested_folder
                |- file2_inside_nested_folder [present in test_platform_input,
                                               but differing file in test_cwl_input]
        '''
        file_in_root_id = 'file_in_root'
        folder_id = 'folder'
        file_inside_folder_id = 'file_inside_folder'
        nested_folder_id = 'nested_folder'
        file1_inside_nested_folder_id = 'file1_inside_nested_folder'
        file2_inside_nested_folder_id = 'file2_inside_nested_folder'

        mock_file_in_root = MagicMock(spec=sevenbridges.File, id = file_in_root_id)
        mock_file_in_root.is_folder.return_value = False
        mock_folder = MagicMock(spec=sevenbridges.File, id = folder_id)
        mock_folder.is_folder.return_value = True
        mock_file_inside_folder = MagicMock(spec=sevenbridges.File, id = file_inside_folder_id)
        mock_file_inside_folder.is_folder.return_value = False
        mock_nested_folder = MagicMock(spec=sevenbridges.File, id = nested_folder_id)
        mock_nested_folder.is_folder.return_value = True
        mock_file1_inside_nested_folder = MagicMock(spec=sevenbridges.File,
                                                    id = file1_inside_nested_folder_id)
        mock_file1_inside_nested_folder.is_folder.return_value = False
        mock_file2_inside_nested_folder = MagicMock(spec=sevenbridges.File,
                                                    id = file2_inside_nested_folder_id)
        mock_file2_inside_nested_folder.is_folder.return_value = False

        # nested folder mocks
        nested_folder_all_mock = MagicMock()
        nested_folder_all_mock.return_value = [mock_file1_inside_nested_folder,
                                               mock_file2_inside_nested_folder]
        nested_list_files_mock = MagicMock()
        nested_list_files_mock.all = nested_folder_all_mock
        mock_nested_folder.list_files.return_value = nested_list_files_mock

        # first level folder mocks
        folder_all_mock = MagicMock()
        folder_all_mock.return_value = [mock_file_inside_folder, mock_nested_folder]
        folder_list_files_mock = MagicMock()
        folder_list_files_mock.all = folder_all_mock
        mock_folder.list_files.return_value = folder_list_files_mock

        test_platform_input = [mock_file_in_root, mock_folder]
        test_cwl_input = [
            {
                'class': 'File',
                'path': file_in_root_id
            },
            {
                'class': 'Directory',
                'path': folder_id,
                'listing': [
                    {
                        'class': 'File',
                        'path': file_inside_folder_id
                    },
                    {
                        'class': 'Directory',
                        'path': nested_folder_id,
                        'listing': [
                            {
                                'class': 'File',
                                'path': file1_inside_nested_folder_id
                            },
                            {
                                'class': 'File',
                                'path': "different file id"
                            }
                        ]
                    }
                ]
            }
        ]

        result = self.platform._compare_platform_object(test_platform_input, test_cwl_input)

        self.assertFalse(result)

    def test_delete_task(self):
        ''' Test delete_task method '''
        # Set up mocks
        task = MagicMock()
        task.id = '12345'
        # Test
        self.platform.delete_task(task)
        # Assert
        task.delete.assert_called_once_with()

    def test_detect_platform(self):
        ''' Test detect_platform method '''
        self.assertTrue(SevenBridgesPlatform.detect())

    def test_get_project(self):
        ''' Test that get_project returns None when we do not have a TASK_ID '''
        self.platform.api = MagicMock()

        actual_value = self.platform.get_project()
        self.assertIsNone(actual_value)

    def test_get_task_output_filename_single_file(self):
        ''' Test get_task_output_filename when output_name is a single file '''
        # Set up mocks
        expected_filename = "output_file.txt"
        task = MagicMock()
        task.id = 'a12345'
        task.outputs = {
            'output_name': MagicMock(spec = sevenbridges.File)
        }
        task.outputs['output_name'].name = expected_filename
        self.platform.api = MagicMock()
        self.platform.api.tasks.get.return_value = task

        # Test
        actual_filename = self.platform.get_task_output_filename(task, 'output_name')

        # Assert
        self.platform.api.tasks.get.assert_called_once()
        self.assertEqual(actual_filename, expected_filename)

    def test_get_task_output_filename_list(self):
        ''' Test get_task_output_filename when output_name is a list of files '''
        # Set up mocks
        expected_filenames = ["output_file1.txt", "output_file2.txt"]
        task = MagicMock()
        task.id = '12345'
        task.outputs = {
            'output_name': [
                MagicMock(),
                MagicMock()
            ]
        }
        task.outputs['output_name'][0].name = expected_filenames[0]
        task.outputs['output_name'][1].name = expected_filenames[1]
        self.platform.api = MagicMock()
        self.platform.api.tasks.get.return_value = task

        # Test
        actual_filenames = self.platform.get_task_output_filename(task, 'output_name')

        # Assert
        self.assertListEqual(actual_filenames, expected_filenames)

    def test_output_filename_nonexistant_output_name(self):
        ''' Test get_task_output_filename when output_name does not exist '''
        # Set up mocks
        task = MagicMock()
        task.id = '12345'
        task.outputs = {"output_name": []}
        self.platform.api = MagicMock()
        self.platform.api.tasks.get.return_value = task

        # Test
        with self.assertRaises(ValueError):
            self.platform.get_task_output_filename(task, 'not_an_output_name')

    def test_output_filename_none(self):
        ''' Test get_task_output_filename when value of output_name is None '''
        # Set up mocks
        task = MagicMock()
        task.id = '12345'
        task.outputs = {"output_name": None}
        self.platform.api = MagicMock()
        self.platform.api.tasks.get.return_value = task

        # Test
        with self.assertRaises(ValueError):
            self.platform.get_task_output_filename(task, 'output_name')

    def test_submit_task(self):
        ''' Test submit_task method is able to properly parse and a list of integers '''
        # Set up test parameters
        # Set up mocks
        self.platform.api = MagicMock()
        name = "test_task"
        project = "test_project"
        workflow = "test_workflow"
        parameters = {
            'capture_regions': {
                'class': 'File',
                'path': '65fc33432348e03d3a73d727'
            },
            'ploidy': [2],
            'tumor_bam': {
                'class': 'File',
                'path': '65eb61ac7aaf1d5a95e3d581',
                'secondaryFiles': [
                    {'class': 'File', 'path': '65eb61ac7aaf1d5a95e3d581.bai'},
                ]
            }
        }
        # Test
        task = self.platform.submit_task(
            name,
            project,
            workflow,
            parameters
        )
        # Assert
        task.run.assert_called_once_with()

    def test_get_tasks_by_name(self):
        ''' Test get_tasks_by_name method '''
        matching_task_name = "matching_task"
        non_matching_task_name = "non_matching_task"

        # 2 total task present, only the one with a matching name should be returned
        mock_all = MagicMock()
        mock_task_match = MagicMock(spec=sevenbridges.Task)
        mock_task_match.name = matching_task_name

        mock_task_not_matching = MagicMock(spec=sevenbridges.Task)
        mock_task_not_matching.name = non_matching_task_name
        mock_all.return_value = [mock_task_not_matching, mock_task_match]

        self.platform.api.tasks.query.return_value.all = mock_all

        result = self.platform.get_tasks_by_name("test_project", matching_task_name)

        self.platform.api.tasks.query.assert_called_once()
        self.assertIn(mock_task_match, result,
                      "Expected task with matching name to be returned, but it wasn't.")
        self.assertNotIn(mock_task_not_matching, result,
                        "Expected task with non-matching name to not be returned, but it was.")
        self.assertEqual(len(result), 1, "Expected only a single task to be returned")

    def test_get_tasks_by_name_match_all(self):
        ''' Test get_tasks_by_name method '''
        task1_name = "task1"
        task2_name = "task2"

        # 2 total task present, no name provided so we should return all
        mock_all = MagicMock()
        mock_task1 = MagicMock(spec=sevenbridges.Task)
        mock_task1.name = task1_name

        mock_task2 = MagicMock(spec=sevenbridges.Task)
        mock_task2.name = task2_name
        mock_all.return_value = [mock_task1, mock_task2]

        self.platform.api.tasks.query.return_value.all = mock_all

        result = self.platform.get_tasks_by_name("test_project")

        self.assertEqual(result, [mock_task1, mock_task2])

    def test_get_tasks_by_name_match_name_and_inputs(self):
        '''
        Query on name as well as inputs eg to see if a task is equivalent for reuse
        '''
        task_name = "sampleA_task"
        query_input = {
            'input1':{
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

        # 2 total task present, both match names but task2 has a different input
        file1 = MagicMock(spec=sevenbridges.File, id = 'file1')
        file1.is_folder.return_value = False
        file2 = MagicMock(spec=sevenbridges.File, id = 'file2')
        file2.is_folder.return_value = False
        file3 = MagicMock(spec=sevenbridges.File, id = 'file3')
        file3.is_folder.return_value = False
        different_file3 = MagicMock(spec=sevenbridges.File, id = 'different_file3')
        different_file3.is_folder.return_value = False

        task1 = MagicMock(spec=sevenbridges.Task)
        task1.name = task_name
        task1.inputs = {
            'input1': file1,
            'input2': [file2, file3],
        }

        task2 = MagicMock(spec=sevenbridges.Task)
        task2.name = task_name
        task2.inputs = {
            'input1': file1,
            'input2': [file2, different_file3],
        }

        mock_all = MagicMock()
        mock_all.return_value = [task1, task2]
        self.platform.api.tasks.query.return_value.all = mock_all

        result = self.platform.get_tasks_by_name(project = "test_project",
                                                 task_name = task_name,
                                                 inputs_to_compare = query_input)
        self.assertIn(task1, result,
                      "Expected task1 with matching name and inputs to be returned, but it wasn't.")
        self.assertNotIn(task2, result,
                        "Expected task2 with non-matching inputs to not be returned, but it was.")
        self.assertEqual(len(result), 1, "Expected only a single task to be returned")

    def test_get_task_input_non_file_obj(self):
        '''
        Test get_task_input method where the input is not a File object (e.g. string)
        '''
        test_value = "test_value"

        mock_task = MagicMock(spec=sevenbridges.Task)
        mock_task.inputs = {'input1': test_value}

        actual_result = self.platform.get_task_input(mock_task, 'input1')

        self.assertEqual(actual_result, test_value)

    def test_get_task_input_file_obj(self):
        '''
        Test get_task_input method with a single File object
        '''
        test_file_id = 'a1234'

        mock_file = MagicMock(spec=sevenbridges.File, id = test_file_id)
        mock_task = MagicMock(spec=sevenbridges.Task)
        mock_task.inputs = {'input1': mock_file}

        actual_result = self.platform.get_task_input(mock_task, 'input1')

        self.assertEqual(actual_result, test_file_id)

    def test_get_task_input_list_of_file_obj(self):
        '''
        Test get_task_input method with a list of File objects
        '''
        test_file1_id = 'a1234'
        test_file2_id = 'b2345'

        mock_file1 = MagicMock(spec=sevenbridges.File, id = test_file1_id)
        mock_file2 = MagicMock(spec=sevenbridges.File, id = test_file2_id)
        mock_task = MagicMock(spec=sevenbridges.Task)
        mock_task.inputs = {'input1': [mock_file1, mock_file2]}

        actual_result = self.platform.get_task_input(mock_task, 'input1')

        self.assertEqual(actual_result, [test_file1_id, test_file2_id])

    def test_find_or_create_path_root_returns_none(self):
        '''Test _find_or_create_path'''
        project = MagicMock()

        ## root path should return None
        result = self.platform._find_or_create_path(project, '/')
        self.assertIsNone(result)

    @mock.patch('cwl_platform.sevenbridges_platform.SevenBridgesPlatform._find_or_create_path')
    def test_upload_file(self, mock_find_or_create_path):
        '''
        Test that we can upload a file
        '''
        # Set up test parameters
        filename = "file.txt"
        project = {'uuid': 'aproject'}
        dest_folder = '/inputs'
        # Set up supporting mocks
        self.platform.api = MagicMock()

        upload_state = MagicMock()
        upload_state.result().id = 1
        self.platform.api.files.upload.return_value = upload_state

        parentfolder = MagicMock()
        parentfolder.id = 1
        mock_find_or_create_path.return_value = parentfolder

        # Test
        actual_result = self.platform.upload_file(
            filename, project, dest_folder, destination_filename=None, overwrite=False)
        # Check results
        self.platform.api.files.upload.assert_called()
        self.assertEqual(actual_result, 1)

    @mock.patch('cwl_platform.sevenbridges_platform.SevenBridgesPlatform._find_or_create_path')
    def test_upload_file_to_root(self, mock_find_or_create_path):
        '''Uploading with dest_folder=None should upload to project root'''
        project = MagicMock()
        self.platform.api.files.query.return_value = []

        upload_state = MagicMock()
        upload_state.status = 'COMPLETED'
        upload_state.result().id = 'file-123'
        self.platform.api.files.upload.return_value = upload_state

        result = self.platform.upload_file('local_file.txt', project)

        mock_find_or_create_path.assert_not_called()
        self.platform.api.files.upload.assert_called_once_with(
            'local_file.txt',
            overwrite=False,
            parent=None,
            file_name='local_file.txt',
            project=project,
            wait=True
        )
        self.assertEqual(result, 'file-123')

    @mock.patch('cwl_platform.sevenbridges_platform.SevenBridgesPlatform._find_or_create_path')
    def test_upload_file_to_subfolder(self, mock_find_or_create_path):
        '''Uploading with dest_folder should create the path and upload into it'''
        project = MagicMock()
        parent_folder = MagicMock()
        parent_folder.id = 'folder-456'
        mock_find_or_create_path.return_value = parent_folder
        self.platform.api.files.query.return_value = []

        upload_state = MagicMock()
        upload_state.status = 'COMPLETED'
        upload_state.result().id = 'file-789'
        self.platform.api.files.upload.return_value = upload_state

        result = self.platform.upload_file('data.csv', project, dest_folder='/inputs')

        mock_find_or_create_path.assert_called_once_with(project, '/inputs')
        self.platform.api.files.upload.assert_called_once_with(
            'data.csv',
            overwrite=False,
            parent='folder-456',
            file_name='data.csv',
            project=None,
            wait=True
        )
        self.assertEqual(result, 'file-789')

    @mock.patch('cwl_platform.sevenbridges_platform.SevenBridgesPlatform._find_or_create_path')
    def test_upload_file_with_destination_filename(self, mock_find_or_create_path):
        '''destination_filename should override the uploaded file name'''
        project = MagicMock()
        mock_find_or_create_path.return_value = None
        self.platform.api.files.query.return_value = []

        upload_state = MagicMock()
        upload_state.status = 'COMPLETED'
        upload_state.result().id = 'file-abc'
        self.platform.api.files.upload.return_value = upload_state

        result = self.platform.upload_file(
            '/tmp/local_file.txt', project,
            dest_folder='/outputs',
            destination_filename='renamed.txt')

        self.platform.api.files.upload.assert_called_once_with(
            '/tmp/local_file.txt',
            overwrite=False,
            parent=None,
            file_name='renamed.txt',
            project=project,
            wait=True
        )
        self.assertEqual(result, 'file-abc')

    @mock.patch('cwl_platform.sevenbridges_platform.SevenBridgesPlatform._find_or_create_path')
    def test_upload_file_skips_existing_when_no_overwrite(self, mock_find_or_create_path):
        '''If file already exists and overwrite=False, return existing file id'''
        project = MagicMock()
        mock_find_or_create_path.return_value = None

        existing_file = MagicMock()
        existing_file.id = 'existing-file-id'
        self.platform.api.files.query.return_value = [existing_file]

        result = self.platform.upload_file(
            'file.txt', project, dest_folder='/inputs', overwrite=False)

        self.platform.api.files.upload.assert_not_called()
        self.assertEqual(result, 'existing-file-id')

    @mock.patch('cwl_platform.sevenbridges_platform.SevenBridgesPlatform._find_or_create_path')
    def test_upload_file_overwrites_existing(self, mock_find_or_create_path):
        '''If overwrite=True, upload even if file already exists'''
        project = MagicMock()
        parent_folder = MagicMock()
        parent_folder.id = 'folder-456'
        mock_find_or_create_path.return_value = parent_folder

        existing_file = MagicMock()
        existing_file.id = 'old-file-id'
        self.platform.api.files.query.return_value = [existing_file]

        upload_state = MagicMock()
        upload_state.status = 'COMPLETED'
        upload_state.result().id = 'new-file-id'
        self.platform.api.files.upload.return_value = upload_state

        result = self.platform.upload_file(
            'file.txt', project, dest_folder='/inputs', overwrite=True)

        self.platform.api.files.upload.assert_called_once()
        self.assertEqual(result, 'new-file-id')

    @mock.patch('cwl_platform.sevenbridges_platform.SevenBridgesPlatform._find_or_create_path')
    def test_upload_file_returns_none_on_failure(self, mock_find_or_create_path):
        '''If upload fails, return None'''
        project = MagicMock()
        mock_find_or_create_path.return_value = None
        self.platform.api.files.query.return_value = []

        upload_state = MagicMock()
        upload_state.status = 'FAILED'
        self.platform.api.files.upload.return_value = upload_state

        result = self.platform.upload_file('file.txt', project, dest_folder='/inputs')

        self.assertIsNone(result)

    @mock.patch('cwl_platform.sevenbridges_platform.SevenBridgesPlatform._find_or_create_path')
    def test_upload_file_strips_trailing_slash(self, mock_find_or_create_path):
        '''Trailing slash on dest_folder should be stripped'''
        project = MagicMock()
        parent_folder = MagicMock()
        parent_folder.id = 'folder-789'
        mock_find_or_create_path.return_value = parent_folder
        self.platform.api.files.query.return_value = []

        upload_state = MagicMock()
        upload_state.status = 'COMPLETED'
        upload_state.result().id = 'file-xyz'
        self.platform.api.files.upload.return_value = upload_state

        result = self.platform.upload_file('file.txt', project, dest_folder='/inputs/')

        mock_find_or_create_path.assert_called_once_with(project, '/inputs')
        self.assertEqual(result, 'file-xyz')

    @mock.patch('cwl_platform.sevenbridges_platform.SevenBridgesPlatform._find_or_create_path')
    def test_upload_file_dest_folder_just_slash(self, mock_find_or_create_path):
        '''dest_folder="/" should upload to root (no folder creation)'''
        project = MagicMock()
        self.platform.api.files.query.return_value = []

        upload_state = MagicMock()
        upload_state.status = 'COMPLETED'
        upload_state.result().id = 'file-root'
        self.platform.api.files.upload.return_value = upload_state

        result = self.platform.upload_file('file.txt', project, dest_folder='/')

        mock_find_or_create_path.assert_not_called()
        self.platform.api.files.upload.assert_called_once_with(
            'file.txt',
            overwrite=False,
            parent=None,
            file_name='file.txt',
            project=project,
            wait=True
        )
        self.assertEqual(result, 'file-root')

    @mock.patch('cwl_platform.sevenbridges_platform.SevenBridgesPlatform._find_or_create_path')
    def test_upload_file_extracts_basename(self, mock_find_or_create_path):
        '''When filename has a path, only the basename should be used as file_name'''
        project = MagicMock()
        mock_find_or_create_path.return_value = None
        self.platform.api.files.query.return_value = []

        upload_state = MagicMock()
        upload_state.status = 'COMPLETED'
        upload_state.result().id = 'file-base'
        self.platform.api.files.upload.return_value = upload_state

        result = self.platform.upload_file(
            '/home/user/data/sample.fastq', project, dest_folder='/inputs')

        self.platform.api.files.upload.assert_called_once_with(
            '/home/user/data/sample.fastq',
            overwrite=False,
            parent=None,
            file_name='sample.fastq',
            project=project,
            wait=True
        )
        self.assertEqual(result, 'file-base')

    def test_get_project_by_name_exact_match(self):
        ''' Test that get_project_by_name returns the project with an exact name match '''
        mock_project = MagicMock()
        mock_project.name = 'MyProject'

        self.platform.api.projects.query.return_value = [mock_project]

        result = self.platform.get_project_by_name('MyProject')

        self.platform.api.projects.query.assert_called_once_with(name='MyProject')
        self.assertEqual(result, mock_project)

    def test_get_project_by_name_no_results(self):
        ''' Test that get_project_by_name returns None when query returns empty '''
        self.platform.api.projects.query.return_value = []

        result = self.platform.get_project_by_name('NonExistent')

        self.assertIsNone(result)

    def test_get_project_by_name_no_exact_match(self):
        ''' Test that get_project_by_name returns None when no project name matches exactly '''
        mock_project = MagicMock()
        mock_project.name = 'MyProject-Extra'

        self.platform.api.projects.query.return_value = [mock_project]

        result = self.platform.get_project_by_name('MyProject')

        self.assertIsNone(result)

    def test_get_project_by_name_multiple_matches_returns_none(self):
        ''' Test that get_project_by_name returns None when multiple exact matches exist '''
        mock_project1 = MagicMock()
        mock_project1.name = 'MyProject'
        mock_project2 = MagicMock()
        mock_project2.name = 'MyProject'

        self.platform.api.projects.query.return_value = [mock_project1, mock_project2]

        result = self.platform.get_project_by_name('MyProject')

        self.assertIsNone(result)

    def test_get_project_by_name_filters_partial_matches(self):
        ''' Test that get_project_by_name ignores partial matches and returns exact one '''
        mock_partial = MagicMock()
        mock_partial.name = 'MyProject-Dev'
        mock_exact = MagicMock()
        mock_exact.name = 'MyProject'

        self.platform.api.projects.query.return_value = [mock_partial, mock_exact]

        result = self.platform.get_project_by_name('MyProject')

        self.assertEqual(result, mock_exact)

    def test_get_project_by_name_query_returns_none(self):
        ''' Test that get_project_by_name handles None from api query '''
        self.platform.api.projects.query.return_value = None

        result = self.platform.get_project_by_name('MyProject')

        self.assertIsNone(result)

    def test_get_file_id_raises_on_url(self):
        ''' Test that get_file_id raises ValueError when given a URL '''
        project = MagicMock()
        with self.assertRaises(ValueError) as context:
            self.platform.get_file_id(project, 'http://example.com/file.txt')
        self.assertIn('cannot be a URL', str(context.exception))

    def test_get_file_id_raises_on_https_url(self):
        ''' Test that get_file_id raises ValueError when given an HTTPS URL '''
        project = MagicMock()
        with self.assertRaises(ValueError) as context:
            self.platform.get_file_id(project, 'https://example.com/file.txt')
        self.assertIn('cannot be a URL', str(context.exception))

    def test_get_file_id_s3_path_extracts_filename(self):
        ''' Test that get_file_id strips S3 prefix and uses only the filename '''
        project = MagicMock()
        mock_file = MagicMock(name='sample.fastq')
        mock_file.name = 'sample.fastq'
        mock_file.id = 'file-123'

        query_result = MagicMock()
        query_result.all.return_value = [mock_file]
        self.platform.api.files.query.return_value = query_result

        result = self.platform.get_file_id(project, 's3://bucket/path/to/sample.fastq')

        self.platform.api.files.query.assert_called_once_with(
            project=project,
            names=['sample.fastq'],
            limit=100
        )
        self.assertEqual(result, 'file-123')

    def test_get_file_id_root_level_file(self):
        ''' Test that get_file_id finds a file at the project root '''
        project = MagicMock()
        mock_file = MagicMock(name='data.csv')
        mock_file.name = 'data.csv'
        mock_file.id = 'file-456'

        query_result = MagicMock()
        query_result.all.return_value = [mock_file]
        self.platform.api.files.query.return_value = query_result

        result = self.platform.get_file_id(project, 'data.csv')

        self.platform.api.files.query.assert_called_once_with(
            project=project,
            names=['data.csv'],
            limit=100
        )
        self.assertEqual(result, 'file-456')

    @mock.patch.object(SevenBridgesPlatform, '_list_files_in_folder')
    def test_get_file_id_file_in_folder(self, mock_list_files):
        ''' Test that get_file_id finds a file inside a folder '''
        project = MagicMock()
        mock_file = MagicMock(name='result.bam')
        mock_file.name = 'result.bam'
        mock_file.id = 'file-789'

        mock_list_files.return_value = [mock_file]

        result = self.platform.get_file_id(project, 'outputs/result.bam')

        mock_list_files.assert_called_once_with(
            project=project,
            folder='outputs'
        )
        self.assertEqual(result, 'file-789')

    @mock.patch.object(SevenBridgesPlatform, '_list_files_in_folder')
    def test_get_file_id_file_in_nested_folder(self, mock_list_files):
        ''' Test that get_file_id handles a nested folder path '''
        project = MagicMock()
        mock_file = MagicMock(name='variants.vcf')
        mock_file.name = 'variants.vcf'
        mock_file.id = 'file-nested'

        mock_list_files.return_value = [mock_file]

        result = self.platform.get_file_id(project, 'results/pipeline/variants.vcf')

        mock_list_files.assert_called_once_with(
            project=project,
            folder='results/pipeline'
        )
        self.assertEqual(result, 'file-nested')

    def test_get_file_id_file_not_found_raises(self):
        ''' Test that get_file_id raises ValueError when file is not found '''
        project = MagicMock()

        query_result = MagicMock()
        query_result.all.return_value = []
        self.platform.api.files.query.return_value = query_result

        with self.assertRaises(ValueError) as context:
            self.platform.get_file_id(project, 'nonexistent.txt')
        self.assertIn('File not found', str(context.exception))

    @mock.patch.object(SevenBridgesPlatform, '_list_files_in_folder')
    def test_get_file_id_file_not_found_in_folder_raises(self, mock_list_files):
        ''' Test that get_file_id raises ValueError when file is not in folder '''
        project = MagicMock()
        mock_other_file = MagicMock(name='other.txt')
        mock_other_file.name = 'other.txt'

        mock_list_files.return_value = [mock_other_file]

        with self.assertRaises(ValueError) as context:
            self.platform.get_file_id(project, 'folder/missing.txt')
        self.assertIn('File not found', str(context.exception))

    def test_get_file_id_returns_first_match(self):
        ''' Test that get_file_id returns the first file when multiple matches exist '''
        project = MagicMock()
        mock_file1 = MagicMock(name='data.csv')
        mock_file1.name = 'data.csv'
        mock_file1.id = 'file-first'
        mock_file2 = MagicMock(name='data.csv')
        mock_file2.name = 'data.csv'
        mock_file2.id = 'file-second'

        query_result = MagicMock()
        query_result.all.return_value = [mock_file1, mock_file2]
        self.platform.api.files.query.return_value = query_result

        result = self.platform.get_file_id(project, 'data.csv')

        self.assertEqual(result, 'file-first')

    def test_get_file_id_filters_by_name(self):
        ''' Test that get_file_id filters results to match the exact filename '''
        project = MagicMock()
        mock_file_wrong = MagicMock(name='data_old.csv')
        mock_file_wrong.name = 'data_old.csv'
        mock_file_wrong.id = 'file-wrong'
        mock_file_right = MagicMock(name='data.csv')
        mock_file_right.name = 'data.csv'
        mock_file_right.id = 'file-right'

        query_result = MagicMock()
        query_result.all.return_value = [mock_file_wrong, mock_file_right]
        self.platform.api.files.query.return_value = query_result

        result = self.platform.get_file_id(project, 'data.csv')

        self.assertEqual(result, 'file-right')


if __name__ == '__main__':
    unittest.main()

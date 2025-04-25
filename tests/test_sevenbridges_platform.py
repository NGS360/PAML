'''
Test Module for SevenBridges Platform
'''
import unittest
import os
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

    def test_get_task_input_file_obj(self):
        '''
        Test get_task_input method with a single File object
        '''
        test_file_id = 'a1234'

        mock_file = MagicMock(spec=sevenbridges.File)
        mock_file.id = test_file_id
        mock_task = MagicMock(spec=sevenbridges.Task)
        mock_task.inputs = {'input1': mock_file}

        actual_result = self.platform.get_task_input(mock_task, 'input1')

        self.assertEqual(actual_result, test_file_id)

    def test_get_task_input_non_file_obj(self):
        '''
        Test get_task_input method with a single File object
        '''
        test_value = "test_value"

        mock_task = MagicMock(spec=sevenbridges.Task)
        mock_task.inputs = {'input1': test_value}

        actual_result = self.platform.get_task_input(mock_task, 'input1')

        self.assertEqual(actual_result, test_value)

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

if __name__ == '__main__':
    unittest.main()

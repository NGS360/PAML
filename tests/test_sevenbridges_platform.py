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

    def test_get_files(self):
        ''' Test get_files method '''
        # Set up test parameters
        project = {"uuid": "project_uuid"}
        filters = {
            'name': 'file1.txt',
            'prefix': 'file',
            'suffix': '.txt',
            'folder': 'folder1',
            'recursive': False
        }
        # Set up mocks
        mock_files_query = self.platform.api.files.query.return_value
        file1 = MagicMock()
        file1.name = 'file1.txt'
        file1.parent = MagicMock()
        file1.parent.name = 'folder1'
        mock_files_query.all.return_value = [
            file1,
            MagicMock(name='file2', type='file', parent=MagicMock(name='folder1')),
            MagicMock(name='file3', type='folder', parent=MagicMock(name='folder1'))
        ]

        # Test
        results = self.platform.get_files(project, filters)

        # Check results
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].name, 'file1.txt')


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


if __name__ == '__main__':
    unittest.main()

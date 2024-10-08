'''
Test Module for SevenBridges Platform
'''
import unittest
import os
import mock
from mock import MagicMock, patch

from cwl_platform.sevenbridges_platform import SevenBridgesPlatform

class TestSevenBridgesPlaform(unittest.TestCase):
    '''
    Test Class for SevenBridges Platform
    '''
    def setUp(self) -> None:
        os.environ['SESSION_ID'] = 'dummy'
        self.platform = SevenBridgesPlatform('SevenBridges')
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

    def test_get_project(self):
        ''' Test that get_project returns None when we do not have a TASK_ID '''
        self.platform.api = MagicMock()

        actual_value = self.platform.get_project()
        self.assertIsNone(actual_value)

    def test_roll_file(self):
        ''' Test that we roll a specific file '''
        # Set up test parameters
        mock_file = MagicMock()
        mock_file.name = "output.txt"
        mock_file.id = 1
        mock_file_2 = MagicMock()
        mock_file_2.name = "sampleA_workflow1_output.txt"
        mock_file_3 = MagicMock()
        mock_file_3.name = "sampleB_workflow2_output.txt"

        project_files = [
            mock_file,
            mock_file_2,
            mock_file_3
        ]
        # Set up mocks
        self.platform.api = MagicMock()
        self.platform.api.files.query.return_value.all.return_value = project_files
        self.platform.api.files.query.return_value.__len__.return_value = 1
        self.platform.api.files.query.return_value.__getitem__.return_value = mock_file

        # Test
        with patch('cwl_platform.sevenbridges_platform.SevenBridgesPlatform.rename_file') as mock_rename:
            self.platform.roll_file('test_project', 'output.txt')
            # Test that output.txt -> _1_output.txt and no other files in project are affected.
            mock_rename.assert_called_once_with(mock_file.id, '_1_output.txt')


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

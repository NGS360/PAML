'''
Test Module for SevenBridges Platform
'''
import unittest
import os
import mock
from mock import MagicMock

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

    def test_submit_task(self):
        ''' Test submit_task method '''
        # Set up test parameters
        # Set up mocks
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

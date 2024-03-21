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
            'sequence_dictionary': {
                'class': 'File',
                'path': '65fba49b337d9b7faeff9d15'
            },
            'gatk_panel_of_normals': {
                'class': 'File',
                'path': '65fc34004eef1760f3f443c4'
            },
            'chromosome_sizes': {
                'class': 'File',
                'path': '65fb8f27337d9b7faeff9d0f'
            },
            'snp_file': {
                'class': 'File',
                'path': '65fb8f27337d9b7faeff9cfd'
            },
            'in_reference': {
                'class': 'File',
                'path': '65fb8f27337d9b7faeff9d00'
            },
            'sex': 'XY',
            'mate_orientation_control': 'FR',
            'mate_orientation_sample': 'FR',
            'noisy_data': True,
            'ploidy': [2],
            'memory_per_job_alleliccounts': 30000,
            'memory_per_job_modelsegments': 32000,
            'bin_length': 1000,
            'tumor_bam': {
                'class': 'File',
                'path': '65eb61ac7aaf1d5a95e3d581',
                'secondaryFiles': [
                    {'class': 'File', 'path': '65eb61ac7aaf1d5a95e3d581.bai'},
                    {'class': 'File', 'path': '65eb61ac7aaf1d5a95e3d581'}
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

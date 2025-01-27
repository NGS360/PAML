'''
Test Module for Arvados Platform
'''
import json
import os

import unittest
import mock
from mock import MagicMock

from cwl_platform.arvados_platform import ArvadosPlatform, ArvadosTask

class TestArvadosPlaform(unittest.TestCase):
    '''
    Test Class for Arvados Platform
    '''
    def setUp(self) -> None:
        self.platform = ArvadosPlatform('Arvados')
        self.platform.api = MagicMock()
        self.platform.keep_client = MagicMock()
        return super().setUp()

    @mock.patch("arvados.api_from_config")
    @mock.patch("arvados.KeepClient")
    def test_connect(self, mock_keep_client, mock_arvados_api):
        ''' Test connect method '''
        mock_keep_client.return_value = MagicMock()
        mock_arvados_api.return_value = MagicMock()
        self.platform.api_config = {
            'ARVADOS_API_HOST': 'host',
            'ARVADOS_API_TOKEN': 'token'
        }
        self.platform.connect()
        self.assertTrue(self.platform.connected)

    @mock.patch('cwl_platform.arvados_platform.ArvadosPlatform._load_cwl_output')
    def test_get_task_output(self, mock__load_cwl_output):
        ''' Test that get_task_output can handle cases when the cwl_output is {} '''
        # Set up test parameters
        task = ArvadosTask(container_request={"uuid":"uuid", "output_uuid": "output_uuid"}, container={})
        # Set up supporting mocks
        mock__load_cwl_output.return_value = {}
        # Test
        actual_value = self.platform.get_task_output(task, 'some_output_field')
        # Check results
        self.assertIsNone(actual_value)

    @mock.patch("arvados.collection.Collection")
    def test_get_task_output_filename_single_file(self, mock_collection):
        ''' Test get_task_output_filename method with single dictionary output '''
        # Set up mocks
        expected_filename = "output_file.txt"
        task = ArvadosTask(container_request={"uuid":"uuid", "output_uuid": "output_uuid"}, container={})
        mock_collection.return_value.open.return_value.__enter__.return_value.read.return_value = json.dumps({
            "output_name": {"basename": expected_filename}
        })

        # Test
        filename = self.platform.get_task_output_filename(task, "output_name")

        # Assert
        mock_collection.assert_called_once()
        mock_collection.return_value.open.assert_called_once()
        self.assertEqual(filename, expected_filename)

    @mock.patch("arvados.collection.Collection")
    def test_get_task_output_filename_list(self, mock_collection):
        ''' Test get_task_output_filename method with list output '''
        # Set up mocks
        expected_filenames = ["output_file1.txt", "output_file2.txt"]
        task = ArvadosTask(container_request={"uuid":"uuid", "output_uuid": "output_uuid"}, container={})
        mock_collection.return_value.open.return_value.__enter__.return_value.read.return_value = json.dumps({
            "output_name": [{"basename": expected_filenames[0]}, {"basename": expected_filenames[1]}]
        })

        # Test
        filenames = self.platform.get_task_output_filename(task, "output_name")

        # Assert
        self.assertListEqual(filenames, expected_filenames)

    @mock.patch("arvados.collection.Collection")
    def test_output_filename_nonexistant_output_name(self, mock_collection):
        ''' Test get_task_output_filename method when output name does not exist '''
        # Set up mocks
        task = ArvadosTask(container_request={"uuid":"uuid", "output_uuid": "output_uuid"}, container={})
        mock_collection.return_value.open.return_value.__enter__.return_value.read.return_value = json.dumps({
            "output_name": []
        })

        # Test
        with self.assertRaises(ValueError):
            self.platform.get_task_output_filename(task, "not_an_output_name")

    @mock.patch("arvados.collection.Collection")
    def test_output_filename_none(self, mock_collection):
        ''' Test get_task_output_filename method when value of output_name is None '''
        # Set up mocks
        task = ArvadosTask(container_request={"uuid":"uuid", "output_uuid": "output_uuid"}, container={})
        mock_collection.return_value.open.return_value.__enter__.return_value.read.return_value = json.dumps({
            "output_name": None
        })

        # Test
        with self.assertRaises(ValueError):
            self.platform.get_task_output_filename(task, "output_name")

    def test_delete_task(self):
        ''' Test delete_task method '''
        # Set up mocks
        task = ArvadosTask(container_request={"uuid": "12345"}, container={"uuid": "67890"})
        # Test
        self.platform.delete_task(task)
        # Assert
        self.platform.api.container_requests().delete.assert_called_once_with(uuid="12345")

    def test_detect_platform(self):
        ''' Test detect_platform method '''
        os.environ['ARVADOS_API_HOST'] = 'some host'
        self.assertTrue(ArvadosPlatform.detect())

if __name__ == '__main__':
    unittest.main()

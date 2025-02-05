'''
Test Module for Arvados Platform
'''
import json
import os

import unittest
import mock
from mock import MagicMock

from cwl_platform.arvados_platform import ArvadosPlatform, ArvadosTask, StreamFileReader

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
        
    @mock.patch("arvados.collection.Collection")  # Ensure this patch decorator is correctly placed
    @mock.patch("cwl_platform.arvados_platform.ArvadosPlatform._get_files_list_in_collection")
    def test_copy_folder_success(self, mock_get_files_list, MockCollection):
        ''' Test copy_folder method with file streaming'''
        # Mocking the source collection                
        # Mocking the API responses for finding the source and destination collections
        source_collection = {
            'items': [
                {
                    'uuid': 'source-uuid', 
                    'name': 'source-folder', 
                    'description': 'source collection'
                }
            ]
        }
        self.platform.api.collections().list.return_value.execute.return_value = source_collection
        
        # Mocking the destination collection empty              
        # Mocking the API responses for finding the source and destination collections
        destination_collection = {
            'items': [
                {
                    'uuid': 'destination-uuid', 
                    'name': 'source-folder', 
                    'description': 'destination collection',
                    "preserve_version": True
                }
            ]
        }
        self.platform.api.collections().list.return_value.execute.side_effect = [source_collection, destination_collection]

        # Simulate the files being streamed using StreamFileReader
        file1_stream = MagicMock()
        file1_stream.name = "file1.txt"
        file1_stream.stream_name.return_value = "stream1"
        file1_stream.read.return_value = b'file1 contents'

        file2_stream = MagicMock()
        file2_stream.name = "file2.txt"
        file2_stream.stream_name.return_value = "stream2"
        file2_stream.read.return_value = b'file2 contents'

        # Wrap the mock streams with StreamFileReader
        file1_reader = StreamFileReader(file1_stream)
        file2_reader = StreamFileReader(file2_stream)

        # Mock _get_files_list_in_collection to return the file readers (file-like objects)
        mock_get_files_list.return_value = [file1_reader, file2_reader]
        
        # Call the copy_folder method
        source_project = {"uuid": "source-uuid"}
        destination_project = {"uuid": "destination-uuid"}
        source_folder = "source-folder"
                
        result = self.platform.copy_folder(source_project, source_folder, destination_project)
                
        # Assertions
        self.assertIsNotNone(result)  # Ensure the result is not None
        self.assertEqual(result['uuid'], 'destination-uuid')  # Ensure we got the correct destination UUID
        self.assertEqual(self.platform.api.collections().list.call_count, 2) # Ensure the collection listing function was called twice
        self.assertEqual(mock_get_files_list.call_count, 2) # Ensure the file listing function was called twice

    @mock.patch("arvados.collection.Collection")  # Ensure this patch decorator is correctly placed
    @mock.patch("cwl_platform.arvados_platform.ArvadosPlatform._get_files_list_in_collection")
    @unittest.skip("This test is skipped for now.")
    def test_copy_folder_source_collection_notfound(self, mock_get_files_list, MockCollection):
        ''' Test copy_folder method with file streaming when the source collection is NOT found'''
        # Mocking the source collection empty              
        # Mocking the API responses for finding the source and destination collections
        self.platform.api.collections().list.return_value.execute.return_value = {'items': []}

        # Call the copy_folder method
        source_project = {"uuid": "source-uuid"}
        destination_project = {"uuid": "destination-uuid"}
        source_folder = "source-folder"
                
        result = self.platform.copy_folder(source_project, source_folder, destination_project)
                
        # Assertions
        self.assertIsNotNone(result)  # Ensure the result is not None

    @mock.patch("arvados.collection.Collection")  # Ensure this patch decorator is correctly placed
    @mock.patch("cwl_platform.arvados_platform.ArvadosPlatform._get_files_list_in_collection")
    def test_copy_folder_create_destination_collection(self, mock_get_files_list, MockCollection):
        ''' Test copy_folder method with file streaming to CREATE the destination collection'''
        # Mocking the source collection                
        # Mocking the API responses for finding the source and destination collections
        source_collection = {
            'items': [
                {
                    'uuid': 'source-uuid', 
                    'name': 'source-folder', 
                    'description': 'source collection'
                }
            ]
        }
        self.platform.api.collections().list.return_value.execute.return_value = source_collection
        
        # Mocking the destination collection empty              
        # Mocking the API responses for finding the source and destination collections
        destination_collection = {
            'items': []
            }
        self.platform.api.collections().list.return_value.execute.side_effect = [source_collection, destination_collection]

        self.platform.api.collections().create.return_value.execute.return_value = {
            "uuid": "destination-uuid",
            "name": "source-folder",
            "description": "destination collection",
            "preserve_version": True,
        }

        # Simulate the files being streamed using StreamFileReader
        file1_stream = MagicMock()
        file1_stream.name = "file1.txt"
        file1_stream.stream_name.return_value = "stream1"
        file1_stream.read.return_value = b'file1 contents'

        file2_stream = MagicMock()
        file2_stream.name = "file2.txt"
        file2_stream.stream_name.return_value = "stream2"
        file2_stream.read.return_value = b'file2 contents'

        # Wrap the mock streams with StreamFileReader
        file1_reader = StreamFileReader(file1_stream)
        file2_reader = StreamFileReader(file2_stream)

        # Mock _get_files_list_in_collection to return the file readers (file-like objects)
        mock_get_files_list.return_value = [file1_reader, file2_reader]
        
        # Call the copy_folder method
        source_project = {"uuid": "source-uuid"}
        destination_project = {"uuid": "destination-uuid"}
        source_folder = "source-folder"
                
        result = self.platform.copy_folder(source_project, source_folder, destination_project)

        # Assertions
        self.assertIsNotNone(result)  # Ensure the result is not None
        self.assertEqual(result['uuid'], 'destination-uuid')  # Ensure we got the correct destination UUID
        self.assertEqual(self.platform.api.collections().list.call_count, 2) # Ensure the collection listing function was called once
        self.assertEqual(self.platform.api.collections().create.call_count, 1) # Ensure the collection creating function was called once
        self.assertEqual(mock_get_files_list.call_count, 2) # Ensure the file listing function was called twice

if __name__ == '__main__':
    unittest.main()

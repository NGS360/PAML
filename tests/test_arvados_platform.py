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

    def test_add_user_to_project(self):
        # Set up test parameters
        platform_user = {'uuid': 'auser'}
        project = {'uuid': 'aproject'}
        permission = 'admin'
        # Test
        self.platform.add_user_to_project(platform_user, project, permission)
        # Check results 
        self.platform.api.links().create().execute.assert_called_once

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

    def test_get_files(self):
        '''
        Test get_files method returns a single file matching filter criteria given there are 3 files in the project.
        '''
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
        self.platform.api.collections().list().execute.return_value = {
            'items': [
                MagicMock(name="collection1"),
            ]
        }
        file1 = MagicMock()
        file1.name = 'file1.txt'
        file1.parent = MagicMock()
        file1.parent.name = 'folder1'
        # Test
        with mock.patch("cwl_platform.arvados_platform.ArvadosPlatform._get_files_list_in_collection") as mock_gflic:
            mock_gflic.return_value = [file1]
            results = self.platform.get_files(project, filters)

        # Check results
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].name, 'file1.txt')

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

    @mock.patch('cwl_platform.arvados_platform.ArvadosPlatform._load_cwl_output')
    def test_get_task_output_optional_step_file_missing(self, mock__load_cwl_output):
        ''' Test that get_task_output can handle cases when an optional step file is missing in cwl_output '''
        # Set up test parameters
        task = ArvadosTask(container_request={"uuid":"uuid", "output_uuid": "output_uuid"}, container={})
        # Set up supporting mocks
        mock__load_cwl_output.return_value = {'some_output_field': None}
        # Test
        actual_value = self.platform.get_task_output(task, 'some_output_field')
        # Check results
        self.assertIsNone(actual_value)
    @mock.patch('cwl_platform.arvados_platform.ArvadosPlatform._load_cwl_output')
    def test_get_task_output_nonexistent_output(self, mock__load_cwl_output):
        ''' Test that get_task_output can handle cases when the output is non-existent in cwl_output '''
        # Set up test parameters
        task = ArvadosTask(container_request={"uuid":"uuid", "output_uuid": "output_uuid"}, container={})
        # Set up supporting mocks
        mock__load_cwl_output.return_value = {'other_output_field': None}
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

    @mock.patch("cwl_platform.arvados_platform.ArvadosPlatform._lookup_collection_from_foldername")
    @mock.patch("cwl_platform.arvados_platform.ArvadosPlatform._get_files_list_in_collection")
    def test_copy_folder_success(self, mock_get_files_list, mock_lookup_folder_name):
        ''' Test copy_folder method with file streaming'''
        # Set up test parameters
        source_project = {"uuid": "source-project-uuid"}
        source_folder = "/folder-to-be-copied"
        destination_project = {"uuid": "destination-project-uuid"}

        # Set up mocks
        # Mocking the source collection
        source_collection = {
            'uuid': 'source-uuid', 
            'name': 'source-folder', 
            'description': 'source collection'
        }

        # Mocking the destination collection empty
        destination_collection = {
            'uuid': 'destination-uuid', 
            'name': 'source-folder', 
            'description': 'destination collection',
            "preserve_version": True
        }

        mock_lookup_folder_name.side_effect = [
            source_collection, destination_collection
        ]

        # Mock the source files
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
        # Assume file2 already exists in the destination and we only need to copy file1
        mock_get_files_list.side_effect = [
            [file1_reader, file2_reader],       # This is for the source collection
            [file2_reader]                      # This is for the destination collection
        ]

        with mock.patch('arvados.collection.Collection') as mock_source_collection_object:
            with mock.patch('arvados.collection.Collection') as mock_destination_collection_object:
                # Test
                result = self.platform.copy_folder(source_project, source_folder, destination_project)

                # Assertions
                self.assertIsNotNone(result)  # Ensure the result is not None
                self.assertEqual(result['uuid'], 'destination-uuid')  # Ensure we got the correct destination UUID
                # Ensure a file is copied to the destination collection
                self.assertEqual(mock_destination_collection_object().copy.call_count, 1)
                self.assertEqual(mock_destination_collection_object().save.call_count, 1)

    @mock.patch("cwl_platform.arvados_platform.ArvadosPlatform._lookup_collection_from_foldername")
    def test_copy_folder_source_collection_notfound(self, mock_lookup_folder_name):
        ''' Test copy_folder method with file streaming when the source collection is NOT found'''
        # Set up test parameters
        source_project = {"uuid": "source-project-uuid"}
        source_folder = "/folder-to-be-copied"
        destination_project = {"uuid": "destination-project-uuid"}

        # Set up mocks
        # Mocking an empty source collection
        source_collection = None

        mock_lookup_folder_name.side_effect = [source_collection]

        result = self.platform.copy_folder(source_project, source_folder, destination_project)

        # Assertions
        self.assertIsNone(result)

    @mock.patch("cwl_platform.arvados_platform.ArvadosPlatform._lookup_collection_from_foldername")
    @mock.patch("cwl_platform.arvados_platform.ArvadosPlatform._get_files_list_in_collection")
    def test_copy_folder_create_destination_collection(self, mock_get_files_list, mock_lookup_folder_name):
        ''' Test copy_folder method with file streaming to CREATE the destination collection'''
        # Set up test parameters
        source_project = {"uuid": "source-project-uuid"}
        source_folder = "/folder-to-be-copied"
        destination_project = {"uuid": "destination-project-uuid"}

        # Set up mocks
        # Mocking the source collection
        source_collection = {
            'uuid': 'source-uuid', 
            'name': 'source-folder', 
            'description': 'source collection'
        }

        destination_collection = None

        mock_lookup_folder_name.side_effect = [
            source_collection, destination_collection
        ]

        # Mock the source files
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
        # Assume file2 already exists in the destination and we only need to copy file1
        mock_get_files_list.side_effect = [
            [file1_reader, file2_reader],       # This is for the source collection
            []                                  # This is for the destination collection
        ]

        with mock.patch('arvados.collection.Collection') as mock_source_collection_object:
            with mock.patch('arvados.collection.Collection') as mock_destination_collection_object:
                # Test
                result = self.platform.copy_folder(source_project, source_folder, destination_project)

                # Assertions
                self.assertIsNotNone(result)  # Ensure the result is not None
                # Ensure both files are copied to the destination collection
                self.assertEqual(mock_destination_collection_object().copy.call_count, 2)
                self.assertEqual(mock_destination_collection_object().save.call_count, 1)

    @mock.patch('arvados.collection.Collection')
    def test_upload_file(self, mock_collection):
        # Set up test parameters
        filename = "file.txt"
        project = {'uuid': 'aproject'}
        dest_folder = '/inputs'
        # Set up supporting mocks
        self.platform.api.collections().create().execute.return_value = {'uuid': 'a_destination_collection'}
        # Test
        actual_result = self.platform.upload_file(filename, project, dest_folder, destination_filename=None, overwrite=False)
        # Check results
        self.assertEqual(actual_result, "keep:a_destination_collection/file.txt")

if __name__ == '__main__':
    unittest.main()

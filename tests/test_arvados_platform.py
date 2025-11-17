'''
Test Module for Arvados Platform
'''
# pylint: disable=protected-access
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
        ''' Test that we can add a user to a project '''
        # Set up test parameters
        platform_user = {'uuid': 'auser'}
        project = {'uuid': 'aproject'}
        permission = 'admin'
        # Test
        self.platform.add_user_to_project(platform_user, project, permission)
        # Check results
        self.platform.api.links().create().execute.assert_called_once()

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

    def test_get_task_input_non_file_obj(self):
        '''
        Test get_task_input method where the input is not a File object (e.g. string)
        '''
        test_value = "test_value"

        mock_task = MagicMock()
        mock_task.container_request = {
            'properties': {
                'cwl_input': {
                    'input1': test_value
                }
            }
        }

        actual_result = self.platform.get_task_input(mock_task, 'input1')

        self.assertEqual(actual_result, test_value)

    def test_get_task_input_file_obj(self):
        '''
        Test get_task_input method with a single File object
        '''
        test_location = "keep:a1ed2cf316addc5e751f1560ac6cc260+238884/somefile.txt"

        mock_task = MagicMock()
        mock_task.container_request = {
            'properties': {
                'cwl_input': {
                    'input1': {
                        'location': test_location,
                    }
                }
            }
        }

        actual_result = self.platform.get_task_input(mock_task, 'input1')

        self.assertEqual(actual_result, test_location)

    def test_get_task_input_list_of_file_obj(self):
        '''
        Test get_task_input method with a single File object
        '''
        test_location1 = "keep:a1ed2cf316addc5e751f1560ac6cc260+238884/somefile1.txt"
        test_location2 = "keep:a1ed2cf316addc5e751f1560ac6cc260+238884/somefile2.txt"

        mock_task = MagicMock()
        mock_task.container_request = {
            'properties': {
                'cwl_input': {
                    'input1': [
                        {'location': test_location1},
                        {'location': test_location2}
                    ]
                }
            }
        }

        actual_result = self.platform.get_task_input(mock_task, 'input1')

        self.assertEqual(actual_result, [test_location1, test_location2])

    @mock.patch('cwl_platform.arvados_platform.ArvadosPlatform._load_cwl_output')
    def test_get_task_output(self, mock__load_cwl_output):
        ''' Test that get_task_output can handle cases when the cwl_output is {} '''
        # Set up test parameters
        task = ArvadosTask(container_request={"uuid":"uuid",
                                              "output_uuid": "output_uuid"},
                           container={})
        # Set up supporting mocks
        mock__load_cwl_output.return_value = {}
        # Test
        actual_value = self.platform.get_task_output(task, 'some_output_field')
        # Check results
        self.assertIsNone(actual_value)

    @mock.patch('cwl_platform.arvados_platform.ArvadosPlatform._load_cwl_output')
    def test_get_task_output_optional_step_file_missing(self, mock__load_cwl_output):
        ''' 
        Test that get_task_output can handle cases when an optional 
        step file is missing in cwl_output 
        '''
        # Set up test parameters
        task = ArvadosTask(container_request={"uuid":"uuid", "output_uuid": "output_uuid"},
                           container={})
        # Set up supporting mocks
        mock__load_cwl_output.return_value = {'some_output_field': None}
        # Test
        actual_value = self.platform.get_task_output(task, 'some_output_field')
        # Check results
        self.assertIsNone(actual_value)
    @mock.patch('cwl_platform.arvados_platform.ArvadosPlatform._load_cwl_output')
    def test_get_task_output_nonexistent_output(self, mock__load_cwl_output):
        ''' 
        Test that get_task_output can handle cases when the 
        output is non-existent in cwl_output
        '''
        # Set up test parameters
        task = ArvadosTask(container_request={"uuid":"uuid", "output_uuid": "output_uuid"},
                           container={})
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
        task = ArvadosTask(container_request={"uuid":"uuid", "output_uuid": "output_uuid"},
                           container={})
        mock_collection.return_value. \
            open.return_value \
            .__enter__.return_value \
            .read.return_value = json.dumps({
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
        task = ArvadosTask(container_request={"uuid":"uuid", "output_uuid": "output_uuid"},
                           container={})
        mock_collection.return_value \
            .open.return_value \
            .__enter__.return_value \
            .read.return_value = json.dumps({
            "output_name": [{"basename": expected_filenames[0]},
                            {"basename": expected_filenames[1]}]
        })

        # Test
        filenames = self.platform.get_task_output_filename(task, "output_name")

        # Assert
        self.assertListEqual(filenames, expected_filenames)

    @mock.patch("arvados.collection.Collection")
    def test_output_filename_nonexistant_output_name(self, mock_collection):
        ''' Test get_task_output_filename method when output name does not exist '''
        # Set up mocks
        task = ArvadosTask(container_request={"uuid":"uuid", "output_uuid": "output_uuid"},
                           container={})
        mock_collection.return_value \
            .open.return_value \
            .__enter__.return_value \
            .read.return_value = json.dumps({
                "output_name": []
            })

        # Test
        with self.assertRaises(ValueError):
            self.platform.get_task_output_filename(task, "not_an_output_name")

    @mock.patch("arvados.collection.Collection")
    def test_output_filename_none(self, mock_collection):
        ''' Test get_task_output_filename method when value of output_name is None '''
        # Set up mocks
        task = ArvadosTask(container_request={"uuid": "uuid",
                                              "output_uuid": "output_uuid"},
                           container={})
        mock_collection.return_value \
            .open.return_value \
            .__enter__.return_value \
            .read.return_value = json.dumps({
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

        with mock.patch('arvados.collection.Collection') as _:
            with mock.patch('arvados.collection.Collection') as mock_destination_collection_object:
                # Test
                result = self.platform.copy_folder(source_project,
                                                   source_folder,
                                                   destination_project)

                # Assertions
                self.assertIsNotNone(result)
                self.assertEqual(result['uuid'], 'destination-uuid')
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
    def test_copy_folder_create_destination_collection(self,
                                                       mock_get_files_list,
                                                       mock_lookup_folder_name):
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

        with mock.patch('arvados.collection.Collection') as _:
            with mock.patch('arvados.collection.Collection') as mock_destination_collection_object:
                # Test
                result = self.platform.copy_folder(source_project,
                                                   source_folder,
                                                   destination_project)

                # Assertions
                self.assertIsNotNone(result)  # Ensure the result is not None
                # Ensure both files are copied to the destination collection
                self.assertEqual(mock_destination_collection_object().copy.call_count, 2)
                self.assertEqual(mock_destination_collection_object().save.call_count, 1)

    @mock.patch('arvados.collection.Collection')
    def test_upload_file(self, _):
        '''
        Test that upload_file returns a keep id
        '''
        # Set up test parameters
    @mock.patch("cwl_platform.arvados_platform.ArvadosPlatform._get_files_list_in_collection")
    @mock.patch("cwl_platform.arvados_platform.ArvadosPlatform._lookup_collection_from_foldername")
    @mock.patch("arvados.collection.Collection")
    def test_copy_folder_destination_collection_does_not_exist(
        self,
        mock_collection_cls,
        mock_lookup_folder_name,
        mock_get_files_list
    ):
        """Test copy_folder when destination collection does not exist."""
        source_project = {"uuid": "source-uuid"}
        destination_project = {"uuid": "dest-uuid"}
        source_folder = "folderA"
        source_collection = {"uuid": "source-coll-uuid", "name": "folderA", "description": "desc"}
        destination_collection = None  # Simulate not found

        # Mock lookup: source found, destination not found
        mock_lookup_folder_name.side_effect = [source_collection, destination_collection]
        # Mock API create
        self.platform.api.collections().create().execute.return_value = {
            "uuid": "dest-coll-uuid",
            "name": "folderA",
            "description": "desc"
        }
        # Mock files
        class MockFile:
            """Mock file object for testing."""
            def __init__(self, stream, name):
                """Initialize MockFile."""
                self._stream = stream
                self._name = name
            def stream_name(self):
                """Return stream name."""
                return self._stream
            def name(self):
                """Return file name."""
                return self._name
        mock_get_files_list.side_effect = [
            [MockFile("folderA", "file1.txt")],  # source files
            []  # destination files
        ]
        mock_source_coll = MagicMock()
        mock_dest_coll = MagicMock()
        mock_collection_cls.side_effect = [mock_source_coll, mock_dest_coll]
        result = self.platform.copy_folder(source_project, source_folder, destination_project)
        self.assertIsNotNone(result)
        mock_dest_coll.copy.assert_called_once()
        mock_dest_coll.save.assert_called_once()

    @mock.patch("cwl_platform.arvados_platform.ArvadosPlatform._get_files_list_in_collection")
    @mock.patch("cwl_platform.arvados_platform.ArvadosPlatform._lookup_collection_from_foldername")
    @mock.patch("arvados.collection.Collection")
    def test_copy_folder_destination_not_up_to_date(
        self,
        mock_collection_cls,
        mock_lookup_folder_name,
        mock_get_files_list
    ):
        """Test copy_folder when destination collection exists but is missing files."""
        source_project = {"uuid": "source-uuid"}
        destination_project = {"uuid": "dest-uuid"}
        source_folder = "folderA"
        source_collection = {"uuid": "source-coll-uuid", "name": "folderA", "description": "desc"}
        destination_collection = {"uuid": "dest-coll-uuid", "name": "folderA", "description": "desc"}
        mock_lookup_folder_name.side_effect = [source_collection, destination_collection]
        class MockFile:
            """Mock file object for testing."""
            def __init__(self, stream, name):
                """Initialize MockFile."""
                self._stream = stream
                self._name = name
            def stream_name(self):
                """Return stream name."""
                return self._stream
            def name(self):
                """Return file name."""
                return self._name
        mock_get_files_list.side_effect = [
            [
                MockFile("folderA", "file1.txt"),
                MockFile("folderA", "file2.txt")
            ],  # source
            [
                MockFile("folderA", "file1.txt")
            ]  # destination (missing file2.txt)
        ]
        mock_source_coll = MagicMock()
        mock_dest_coll = MagicMock()
        mock_collection_cls.side_effect = [mock_source_coll, mock_dest_coll]
        result = self.platform.copy_folder(source_project, source_folder, destination_project)
        self.assertIsNotNone(result)
        mock_dest_coll.copy.assert_called_with(
            "folderA/file2.txt",
            target_path="folderA/file2.txt",
            source_collection=mock_source_coll
        )
        mock_dest_coll.save.assert_called_once()

    @mock.patch("cwl_platform.arvados_platform.ArvadosPlatform._get_files_list_in_collection")
    @mock.patch("cwl_platform.arvados_platform.ArvadosPlatform._lookup_collection_from_foldername")
    @mock.patch("arvados.collection.Collection")
    def test_copy_folder_source_with_subfolders(
        self,
        mock_collection_cls,
        mock_lookup_folder_name,
        mock_get_files_list
    ):
        """Test copy_folder when source collection contains subfolders."""
        source_project = {"uuid": "source-uuid"}
        destination_project = {"uuid": "dest-uuid"}
        source_folder = "folderA"
        source_collection = {"uuid": "source-coll-uuid", "name": "folderA", "description": "desc"}
        destination_collection = {"uuid": "dest-coll-uuid", "name": "folderA", "description": "desc"}
        mock_lookup_folder_name.side_effect = [source_collection, destination_collection]
        class MockFile:
            """Mock file object for testing."""
            def __init__(self, stream, name):
                """Initialize MockFile."""
                self._stream = stream
                self._name = name
            def stream_name(self):
                """Return stream name."""
                return self._stream
            def name(self):
                """Return file name."""
                return self._name
        mock_get_files_list.side_effect = [
            [MockFile("folderA", "file1.txt"), MockFile("folderA/subfolder", "file2.txt")],  # source
            []  # destination
        ]
        mock_source_coll = MagicMock()
        mock_dest_coll = MagicMock()
        # Add extra mocks to avoid StopIteration if more calls are made
        mock_collection_cls.side_effect = [mock_source_coll, mock_dest_coll, MagicMock(), MagicMock()]
        result = self.platform.copy_folder(source_project, source_folder, destination_project)
        self.assertIsNotNone(result)
        self.assertEqual(mock_dest_coll.copy.call_count, 2)
        mock_dest_coll.save.assert_called_once()
        filename = "file.txt"
        project = {'uuid': 'aproject'}
        dest_folder = '/inputs'
        # Set up supporting mocks
        self.platform.api.collections().create().execute.return_value = {
            'uuid': 'a_destination_collection'
        }
        # Test
        actual_result = self.platform.upload_file(
            filename, project, dest_folder, destination_filename=None, overwrite=False)
        # Check results
        self.assertEqual(actual_result, "keep:a_destination_collection/file.txt")

    def test_get_tasks_by_name(self):
        ''' Test get_tasks_by_name method with task name only '''
        matching_task_name = "matching_task"
        non_matching_task_name = "non_matching_task"

        # Mock container requests and containers
        mock_container_request1 = {
            'name': matching_task_name,
            'container_uuid': 'container1',
            'uuid': 'request1'
        }
        mock_container_request2 = {
            'name': non_matching_task_name,
            'container_uuid': 'container2',
            'uuid': 'request2'
        }
        mock_container1 = {'uuid': 'container1'}
        mock_container2 = {'uuid': 'container2'}

        # Mock the API calls
        self.platform.api.container_requests().list.return_value = MagicMock()
        mock_keyset_list_all = MagicMock(return_value = [mock_container_request1,
                                                       mock_container_request2])

        with mock.patch('arvados.util.keyset_list_all', mock_keyset_list_all):
            self.platform.api.containers().get().execute.side_effect = [
                mock_container1, mock_container2
            ]

            # Test
            result = self.platform.get_tasks_by_name({'uuid': 'project_uuid'}, matching_task_name)

            # Assert
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0].container_request['name'], matching_task_name)

    def test_get_tasks_by_name_match_all(self):
        ''' Test get_tasks_by_name method with no task name (should return all tasks) '''
        project = {'uuid': 'project_uuid'}

        # Add two tasks with different names to the project
        task1_name = "task1"
        task2_name = "task2"

        # Mock container requests and containers
        mock_container_request1 = {
            'name': task1_name,
            'container_uuid': 'container1',
            'uuid': 'request1'
        }
        mock_container_request2 = {
            'name': task2_name,
            'container_uuid': 'container2',
            'uuid': 'request2'
        }
        mock_container1 = {'uuid': 'container1'}
        mock_container2 = {'uuid': 'container2'}

        # Mock the API calls
        #self.platform.api.container_requests().list.return_value = MagicMock()
        mock_keyset_list_all = MagicMock(return_value=[mock_container_request1,
                                                       mock_container_request2])

        with mock.patch('arvados.util.keyset_list_all', mock_keyset_list_all):
            self.platform.api.containers().get().execute.side_effect = [
                mock_container1, mock_container2
            ]

            # Test
            result = self.platform.get_tasks_by_name(project)

            # Assert
            self.assertEqual(len(result), 2)
            self.assertEqual(result[0].container_request['name'], task1_name)
            self.assertEqual(result[1].container_request['name'], task2_name)

    def test_get_tasks_by_name_from_provided_tasks(self):
        ''' Test that get_task_by_name can use provided tasks '''
        project = {'uuid': 'project_uuid'}

        # Mock container requests and containers
        container_request1 = {'name': 'task1', 'uuid': 'request1', 'container_uuid': 'container1'}
        container_request2 = {'name': 'task2', 'uuid': 'request2', 'container_uuid': 'container2'}
        container1 = {'uuid': 'container1'}
        container2 = {'uuid': 'container2'}

        tasks = [
            ArvadosTask(container_request=container_request1, container=container1),
            ArvadosTask(container_request=container_request2, container=container2)
        ]
        # Test
        self.platform.api.containers().get().execute.side_effect = [
            container1, container2
        ]
        result = self.platform.get_tasks_by_name(project, task_name='task1', tasks=tasks)
        # Assert
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].container_request['name'], 'task1')

    def test_get_tasks_by_name_match_name_and_inputs(self):
        ''' Test get_tasks_by_name method with task name and matching inputs '''
        task_name = "sample_task"

        # Define inputs to compare
        inputs_to_compare = {
            'input1': {
                'class': 'File',
                'path': 'keep:file1'
            },
            'input2': [
                {
                    'class': 'File',
                    'path': 'keep:file2'
                },
                {
                    'class': 'File',
                    'path': 'keep:file3'
                }
            ]
        }

        # Mock container requests with matching and non-matching inputs
        mock_container_request1 = {
            'name': task_name,
            'container_uuid': 'container1',
            'uuid': 'request1',
            'properties': {
                'cwl_input': {
                    'input1': {
                        'class': 'File',
                        'location': 'keep:file1'
                    },
                    'input2': [
                        {
                            'class': 'File',
                            'location': 'keep:file2'
                        },
                        {
                            'class': 'File',
                            'location': 'keep:file3'
                        }
                    ]
                }
            }
        }

        mock_container_request2 = {
            'name': task_name,
            'container_uuid': 'container2',
            'uuid': 'request2',
            'properties': {
                'cwl_input': {
                    'input1': {
                        'class': 'File',
                        'location': 'keep:file1'
                    },
                    'input2': [
                        {
                            'class': 'File',
                            'location': 'keep:file2'
                        },
                        {
                            'class': 'File',
                            'location': 'keep:different_file3'  # Different file
                        }
                    ]
                }
            }
        }

        mock_container1 = {'uuid': 'container1'}
        mock_container2 = {'uuid': 'container2'}

        # Mock the API calls
        self.platform.api.container_requests().list.return_value = MagicMock()
        mock_keyset_list_all = MagicMock(return_value=[
            mock_container_request1, mock_container_request2
        ])

        with mock.patch('arvados.util.keyset_list_all', mock_keyset_list_all):
            self.platform.api.containers().get().execute.side_effect = [
                mock_container1, mock_container2
            ]

            # Test
            result = self.platform.get_tasks_by_name(
                {'uuid': 'project_uuid'},
                task_name=task_name,
                inputs_to_compare=inputs_to_compare
            )

            # Assert
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0].container_request['uuid'], 'request1')

    def test_get_tasks_by_name_with_new_task_added(self):
        '''
        Test get_tasks_by_name method with a new task added to the project
        will still work correctly. A new task may only have a uuid and not 
        a name associated with it.
        '''
        project = {'uuid': 'project_uuid'}

        # Mock container requests and containers
        mock_container_request1 = {
            'name': 'task1',
            'uuid': 'request1'
        }
        tasks = [ArvadosTask(
            container_request=mock_container_request1,
            container=None
        )]

        # Test
        result = self.platform.get_tasks_by_name(project, 'task2', tasks=tasks)

        # Assert
        self.assertListEqual(result, [])

    def test_compare_inputs_simple_values(self):
        ''' Test the _compare_inputs helper method with simple values '''
        # Test string values
        self.assertTrue(self.platform._compare_inputs("test", "test"))
        self.assertFalse(self.platform._compare_inputs("test", "different"))

        # Test numeric values
        self.assertTrue(self.platform._compare_inputs(123, 123))
        self.assertFalse(self.platform._compare_inputs(123, 456))

        # Test boolean values
        self.assertTrue(self.platform._compare_inputs(True, True))
        self.assertFalse(self.platform._compare_inputs(True, False))

        # Test None values
        self.assertTrue(self.platform._compare_inputs(None, None))
        self.assertFalse(self.platform._compare_inputs(None, "not none"))

    def test_compare_inputs_file_objects(self):
        ''' Test the _compare_inputs helper method with File objects '''
        # Test matching File objects
        file1 = {
            'class': 'File',
            'location': 'keep:file1'
        }
        file1_compare = {
            'class': 'File',
            'path': 'keep:file1'
        }
        self.assertTrue(self.platform._compare_inputs(file1, file1_compare))

        # Test non-matching File objects
        file2 = {
            'class': 'File',
            'location': 'keep:file2'
        }
        self.assertFalse(self.platform._compare_inputs(file1, file2))

        # Test File object with missing location
        file_missing_location = {
            'class': 'File'
        }
        self.assertFalse(self.platform._compare_inputs(file_missing_location, file1_compare))

    def test_compare_inputs_lists(self):
        ''' Test the _compare_inputs helper method with lists '''
        # Test matching simple lists
        list1 = [1, 2, 3]
        list2 = [1, 2, 3]
        self.assertTrue(self.platform._compare_inputs(list1, list2))

        # Test non-matching simple lists
        list3 = [1, 2, 4]
        self.assertFalse(self.platform._compare_inputs(list1, list3))

        # Test lists of different lengths
        list4 = [1, 2]
        self.assertFalse(self.platform._compare_inputs(list1, list4))

        # Test lists with File objects
        list_files1 = [
            {'class': 'File', 'location': 'keep:file1'},
            {'class': 'File', 'location': 'keep:file2'}
        ]
        list_files2 = [
            {'class': 'File', 'path': 'keep:file1'},
            {'class': 'File', 'path': 'keep:file2'}
        ]
        self.assertTrue(self.platform._compare_inputs(list_files1, list_files2))

        # Test lists with non-matching File objects
        list_files3 = [
            {'class': 'File', 'location': 'keep:file1'},
            {'class': 'File', 'location': 'keep:different_file'}
        ]
        self.assertFalse(self.platform._compare_inputs(list_files1, list_files3))

    def test_compare_inputs_nested_structures(self):
        ''' Test the _compare_inputs helper method with nested structures '''
        # Test matching nested dictionaries
        nested1 = {
            'a': 1,
            'b': {
                'c': 2,
                'd': [3, 4]
            }
        }
        nested2 = {
            'a': 1,
            'b': {
                'c': 2,
                'd': [3, 4]
            }
        }
        self.assertTrue(self.platform._compare_inputs(nested1, nested2))

        # Test nested dictionaries with different values
        nested3 = {
            'a': 1,
            'b': {
                'c': 2,
                'd': [3, 5]  # Different value
            }
        }
        self.assertFalse(self.platform._compare_inputs(nested1, nested3))

        # This should now return False because we require dictionaries to have identical keys
        self.assertFalse(self.platform._compare_inputs(nested1, {'a': 1}))

        # Test nested structure with Directory objects
        dir1 = {
            'class': 'Directory',
            'location': 'keep:dir1',
            'listing': [
                {'class': 'File', 'location': 'keep:file1'},
                {'class': 'File', 'location': 'keep:file2'}
            ]
        }
        dir2 = {
            'class': 'Directory',
            'path': 'keep:dir1',
            'listing': [
                {'class': 'File', 'path': 'keep:file1'},
                {'class': 'File', 'path': 'keep:file2'}
            ]
        }
        self.assertTrue(self.platform._compare_inputs(dir1, dir2))

        # Test nested structure with non-matching Directory objects
        dir3 = {
            'class': 'Directory',
            'path': 'keep:dir1',
            'listing': [
                {'class': 'File', 'path': 'keep:file1'},
                {'class': 'File', 'path': 'keep:different_file'}
            ]
        }
        self.assertFalse(self.platform._compare_inputs(dir1, dir3))

    def test_submit_task(self):
        '''
        Test that submit_task returns an ArvadosTask object where container_request 
        has a name and uuid
        '''
        name = "test_task"
        project = {'uuid': 'test_project_uuid'}
        workflow = {'uuid': 'test_workflow_uuid'}
        parameters = {'param1': 'value1', 'param2': 'value2'}

        with mock.patch('subprocess.check_output') as mock_subprocess_check_output:
            mock_subprocess_check_output.return_value = b"container_request_uuid"
            # Test
            task = self.platform.submit_task(name,
                                             project,
                                             workflow,
                                             parameters,
                                             execution_settings=None)

        # Assert that the returned task is an instance of ArvadosTask
        self.assertIsInstance(task, ArvadosTask)
        # Assert that the container_request has a name and uuid
        self.assertIn('name', task.container_request)
        self.assertIn('uuid', task.container_request)
        self.assertEqual(task.container_request['name'], name)
        self.assertIsNotNone(task.container_request['uuid'])
        # Assert that the container is None (as per the current implementation)
        self.assertIsNone(task.container)

    # Tests for get_task_state method
    def test_get_task_state_uncommitted(self):
        ''' Test get_task_state returns Cancelled for Uncommitted state '''
        task = ArvadosTask(
            container_request={'state': 'Uncommitted', 'uuid': 'test-uuid'},
            container={'state': 'Queued'}
        )
        result = self.platform.get_task_state(task)
        self.assertEqual(result, 'Cancelled')

    def test_get_task_state_container_none(self):
        ''' Test get_task_state returns Queued when container is None '''
        task = ArvadosTask(
            container_request={'state': 'Committed', 'uuid': 'test-uuid'},
            container=None
        )
        result = self.platform.get_task_state(task)
        self.assertEqual(result, 'Queued')

    def test_get_task_state_locked(self):
        ''' Test get_task_state returns Queued for Locked container state '''
        task = ArvadosTask(
            container_request={'state': 'Committed', 'uuid': 'test-uuid'},
            container={'state': 'Locked'}
        )
        result = self.platform.get_task_state(task)
        self.assertEqual(result, 'Queued')

    def test_get_task_state_queued(self):
        ''' Test get_task_state returns Queued for Queued container state '''
        task = ArvadosTask(
            container_request={'state': 'Committed', 'uuid': 'test-uuid'},
            container={'state': 'Queued'}
        )
        result = self.platform.get_task_state(task)
        self.assertEqual(result, 'Queued')

    def test_get_task_state_running(self):
        ''' Test get_task_state returns Running for Running container state '''
        task = ArvadosTask(
            container_request={'state': 'Committed', 'uuid': 'test-uuid'},
            container={'state': 'Running'}
        )
        result = self.platform.get_task_state(task)
        self.assertEqual(result, 'Running')

    def test_get_task_state_complete_committed(self):
        '''
        Test get_task_state returns Running when container Complete but request still Committed
        '''
        task = ArvadosTask(
            container_request={'state': 'Committed', 'uuid': 'test-uuid'},
            container={'state': 'Complete', 'exit_code': 0}
        )
        result = self.platform.get_task_state(task)
        self.assertEqual(result, 'Running')

    def test_get_task_state_complete_success(self):
        ''' Test get_task_state returns Complete for successful completion '''
        task = ArvadosTask(
            container_request={'state': 'Final', 'uuid': 'test-uuid'},
            container={'state': 'Complete', 'exit_code': 0}
        )
        result = self.platform.get_task_state(task)
        self.assertEqual(result, 'Complete')

    def test_get_task_state_complete_failed(self):
        ''' Test get_task_state returns Failed for failed completion '''
        task = ArvadosTask(
            container_request={'state': 'Final', 'uuid': 'test-uuid'},
            container={'state': 'Complete', 'exit_code': 1}
        )
        result = self.platform.get_task_state(task)
        self.assertEqual(result, 'Failed')

    def test_get_task_state_cancelled(self):
        ''' Test get_task_state returns Cancelled for Cancelled container state '''
        task = ArvadosTask(
            container_request={'state': 'Final', 'uuid': 'test-uuid'},
            container={'state': 'Cancelled'}
        )
        result = self.platform.get_task_state(task)
        self.assertEqual(result, 'Cancelled')

    def test_get_task_state_with_refresh(self):
        ''' Test get_task_state with refresh=True fetches fresh state from API '''
        # Set up task with initial state
        task = ArvadosTask(
            container_request={'state': 'Committed',
                               'uuid': 'test-uuid',
                               'container_uuid': 'container-uuid'},
            container=None
        )

        # Mock API responses
        updated_container_request = {
            'state': 'Final',
            'uuid': 'test-uuid',
            'container_uuid': 'container-uuid'
        }
        updated_container = {
            'state': 'Complete',
            'exit_code': 0
        }

        mock_container_request_get = MagicMock()
        mock_container_request_get.execute.return_value = updated_container_request
        self.platform.api.container_requests().get.return_value = mock_container_request_get

        mock_container_get = MagicMock()
        mock_container_get.execute.return_value = updated_container
        self.platform.api.containers().get.return_value = mock_container_get

        # Test
        result = self.platform.get_task_state(task, refresh=True)

        # Assert
        self.assertEqual(result, 'Complete')
        self.platform.api.container_requests().get.assert_called_once_with(uuid='test-uuid')
        self.platform.api.containers().get.assert_called_once_with(uuid='container-uuid')

    def test_get_task_state_unknown_state(self):
        ''' Test get_task_state raises ValueError for unknown state combination '''
        task = ArvadosTask(
            container_request={'state': 'SomeUnknownState', 'uuid': 'test-uuid'},
            container={'state': 'SomeOtherUnknownState'}
        )
        with self.assertRaises(ValueError) as context:
            self.platform.get_task_state(task)
        self.assertIn('Unknown task state', str(context.exception))


if __name__ == '__main__':
    unittest.main()

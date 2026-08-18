'''
Test WES Platform implementation
'''
import json
import os
import unittest
from contextlib import contextmanager
from unittest.mock import patch, MagicMock
import requests

from cwl_platform.ngs360_platform import NGS360Platform, WESTask

# connect() and submit_task() read these as fallbacks, so one left set in the
# shell running the tests would change what those tests exercise.
LAUNCHER_ENV_VARS = (
    'NGS360_AUTH_TOKEN',
    'WES_SERVICE_KEY',
    'WES_ON_BEHALF_OF',
    'WES_RUN_ID',
)


@contextmanager
def hidden_launcher_env():
    '''
    Hide the launcher's environment variables for the duration of a test

    Removes only the variables in LAUNCHER_ENV_VARS and restores the
    environment on exit; everything else is left alone.
    '''
    with patch.dict('os.environ'):
        for var in LAUNCHER_ENV_VARS:
            os.environ.pop(var, None)
        yield


class TestNGS360Platform(unittest.TestCase):
    '''
    Test WES Platform implementation
    '''
    def setUp(self):
        '''
        Set up test environment
        '''
        self.platform = NGS360Platform('WES')

        self.platform.ga4gh_api_endpoint = 'https://wes.example.com/ga4gh/wes/v1'

        self.platform.ngs360_endpoint = 'https://ngs360.example.com'
        self.platform._ngs360_auth_config = {'token': 'test_token'} # pylint: disable=protected-access

        self.platform.connected = True

    @patch('requests.request')
    def test_make_request(self, mock_request):
        '''
        Test _make_request method
        '''
        # Mock response
        mock_response = MagicMock()
        mock_response.content = json.dumps({'key': 'value'}).encode('utf-8')
        mock_response.json.return_value = {'key': 'value'}
        mock_request.return_value = mock_response

        # Test GET request
        result = self.platform._make_request('GET', 'service-info') # pylint: disable=protected-access
        mock_request.assert_called_with(
            method='GET',
            url='https://wes.example.com/ga4gh/wes/v1/service-info',
            headers={'Authorization': 'Bearer test_token'},
            data=None,
            files=None,
            params=None,
            timeout=120
        )
        self.assertEqual(result, {'key': 'value'})

    @patch('requests.get')
    @patch('requests.request')
    def test_connect(self, mock_request, mock_get):
        '''
        Test connect method
        '''
        # Mock WES API response
        mock_response = MagicMock()
        mock_response.content = json.dumps({
            'workflow_type_versions': {
                'CWL': {'workflow_type_version': ['v1.0']}
            }
        }).encode('utf-8')
        mock_response.json.return_value = {
            'workflow_type_versions': {
                'CWL': {'workflow_type_version': ['v1.0']}
            }
        }
        mock_request.return_value = mock_response

        # Mock NGS360 API response
        mock_ngs360_response = MagicMock()
        mock_ngs360_response.status_code = 200
        mock_get.return_value = mock_ngs360_response

        # Test connect
        platform = NGS360Platform('WES')
        result = platform.connect(
            api_endpoint='https://wes.example.com/ga4gh/wes/v1',
            ngs360_auth_token='test_token',
            ngs360_endpoint='https://ngs360.example.com'
        )
        self.assertTrue(result)
        # Verify NGS360 token is stored and used for both NGS360 and GA4GH WES requests
        self.assertEqual(platform._ngs360_auth_config['token'], 'test_token') # pylint: disable=protected-access

        # Verify the bearer token is sent on the WES service-info request
        _, kwargs = mock_request.call_args
        self.assertEqual(kwargs['headers']['Authorization'], 'Bearer test_token')

    @patch('requests.get')
    @patch('requests.request')
    def test_connect_with_service_key(self, mock_request, mock_get):
        '''
        Test connect method with a service key instead of a bearer token
        '''
        # Mock WES API response
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'workflow_type_versions': {
                'CWL': {'workflow_type_version': ['v1.0']}
            }
        }
        mock_request.return_value = mock_response

        platform = NGS360Platform('WES')
        with hidden_launcher_env():
            result = platform.connect(
                api_endpoint='https://wes.example.com/ga4gh/wes/v1',
                ngs360_endpoint='https://ngs360.example.com',
                wes_service_key='test_service_key',
                wes_on_behalf_of='someuser'
            )
        self.assertTrue(result)

        # The service key is not a user token, so /auth/me is not probed
        mock_get.assert_not_called()

        # Verify the service key headers are sent instead of a bearer token
        _, kwargs = mock_request.call_args
        self.assertEqual(kwargs['headers']['X-Internal-Service-Key'], 'test_service_key')
        self.assertEqual(kwargs['headers']['X-On-Behalf-Of'], 'someuser')
        self.assertNotIn('Authorization', kwargs['headers'])

    @patch('requests.get')
    @patch('requests.request')
    def test_connect_prefers_token_over_service_key(self, mock_request, mock_get):
        '''
        Test connect method uses the bearer token when both credentials are given
        '''
        # Mock WES API response
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'workflow_type_versions': {
                'CWL': {'workflow_type_version': ['v1.0']}
            }
        }
        mock_request.return_value = mock_response

        # Mock NGS360 API response
        mock_ngs360_response = MagicMock()
        mock_ngs360_response.status_code = 200
        mock_get.return_value = mock_ngs360_response

        platform = NGS360Platform('WES')
        result = platform.connect(
            api_endpoint='https://wes.example.com/ga4gh/wes/v1',
            ngs360_endpoint='https://ngs360.example.com',
            ngs360_auth_token='test_token',
            wes_service_key='test_service_key'
        )
        self.assertTrue(result)

        _, kwargs = mock_request.call_args
        self.assertEqual(kwargs['headers']['Authorization'], 'Bearer test_token')
        self.assertNotIn('X-Internal-Service-Key', kwargs['headers'])

    @patch('requests.request')
    def test_connect_requires_a_credential(self, mock_request):
        '''
        Test connect method raises when neither credential is available
        '''
        # Mock WES API response
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'workflow_type_versions': {
                'CWL': {'workflow_type_version': ['v1.0']}
            }
        }
        mock_request.return_value = mock_response

        platform = NGS360Platform('WES')
        with hidden_launcher_env():
            with self.assertRaises(ValueError):
                platform.connect(
                    api_endpoint='https://wes.example.com/ga4gh/wes/v1',
                    ngs360_endpoint='https://ngs360.example.com'
                )

    @patch('requests.request')
    def test_connect_wes_failure(self, mock_request):
        '''
        Test connect method with WES API failure
        '''
        # Mock failed WES API response
        mock_request.side_effect = requests.RequestException("Connection failed")

        # Test connect failure
        platform = NGS360Platform('WES')
        result = platform.connect(
            api_endpoint='https://wes.example.com/ga4gh/wes/v1',
            ngs360_auth_token='test_token',
            ngs360_endpoint='https://ngs360.example.com'
        )

        self.assertFalse(result)

    @patch('requests.get')
    @patch('requests.request')
    def test_connect_ngs360_failure(self, mock_request, mock_get):
        '''
        Test connect method with NGS360 API failure
        '''
        # Mock successful WES API response
        mock_wes_response = MagicMock()
        mock_wes_response.json.return_value = {'workflow_type_versions': {'CWL': {'workflow_type_version': ['v1.0']}}}
        mock_request.return_value = mock_wes_response

        # Mock failed NGS360 API response
        mock_get.side_effect = requests.RequestException("NGS360 connection failed")

        # Test connect failure
        platform = NGS360Platform('WES')
        result = platform.connect(
            api_endpoint='https://wes.example.com/ga4gh/wes/v1',
            ngs360_auth_token='test_token',
            ngs360_endpoint='https://ngs360.example.com'
        )

        self.assertFalse(result)

    @patch('requests.request')
    def test_submit_task(self, mock_request):
        '''
        Test submit_task method calls the GA4GH API correctly
        '''
        # Set up parameters
        workflow_url = 'workflow_id'
        workflow_parameters = {'input': 'value'}

        # Mock the GA4GH response for submit_task
        mock_response = MagicMock()
        mock_response.json.return_value = {'run_id': 'test_run_id'}
        mock_request.return_value = mock_response

        # Test
        task = self.platform.submit_task(
            name='Test Task',
            project={'project_id': "P-1234567", 'name': 'Test Project'},
            workflow=workflow_url,
            parameters=workflow_parameters,
            execution_settings={"use_spot_instance": False}
        )

        # Verify GA4GH API request withing submit_task was made correctly
        mock_request.assert_called_with(
            method='POST',
            url='https://wes.example.com/ga4gh/wes/v1/runs',
            headers={'Authorization': 'Bearer test_token'},
            data={
                'workflow_params': json.dumps(workflow_parameters),
                'workflow_type': 'CWL',
                'workflow_type_version': 'v1.0',
                'workflow_url': workflow_url,
                'tags': '{"ProjectId": "P-1234567", "TaskName": "Test Task"}',
                'workflow_engine_parameters': '{}'
            },
            files=None,
            params=None,
            timeout=120
        )

        # Verify task
        self.assertIsNotNone(task)
        self.assertEqual(task.run_id, 'test_run_id')
        self.assertEqual(task.name, 'Test Task')
        self.assertEqual(task.state, 'Queued')
        self.assertEqual(task.inputs, workflow_parameters)

    @patch('requests.request')
    def test_get_task_state(self, mock_request):
        '''
        Test get_task_state method
        '''
        # Mock response
        mock_response = MagicMock()
        mock_response.content = json.dumps({
            'run_id': 'test_run_id',
            'state': 'RUNNING',
            'outputs': {'output': 'value'}
        }).encode('utf-8')
        mock_response.json.return_value = {
            'run_id': 'test_run_id',
            'state': 'RUNNING',
            'outputs': {'output': 'value'}
        }
        mock_request.return_value = mock_response

        # Create task
        task = WESTask('test_run_id', 'Test Task')

        # Test get_task_state with refresh
        state = self.platform.get_task_state(task, refresh=True)
        self.assertEqual(state, 'Running')
        self.assertEqual(task.state, 'Running')
        self.assertEqual(task.outputs, {'output': 'value'})

        # Verify request
        mock_request.assert_called_with(
            method='GET',
            url='https://wes.example.com/ga4gh/wes/v1/runs/test_run_id',
            headers={'Authorization': 'Bearer test_token'},
            data=None,
            files=None,
            params=None,
            timeout=120
        )

    @patch('requests.request')
    def test_get_task_output(self, mock_request):
        '''
        Test get_task_output method
        '''
        # Mock response
        mock_response = MagicMock()
        mock_response.content  = json.dumps({
            "outputs": {
                "output_mapping":  {
                    'output': 'value'
                }
            }
        }).encode('utf-8')
        mock_response.json.return_value = {
            "outputs": {
                "output_mapping":  {
                    'output': 'value'
                }
            }
        }
        mock_request.return_value = mock_response

        # Create task with outputs
        task = WESTask('test_run_id', 'Test Task')

        # Test get_task_output
        output = self.platform.get_task_output(task, 'output')
        self.assertEqual(output, 'value')

        # Test get_task_output with non-existent output
        output = self.platform.get_task_output(task, 'non_existent')
        self.assertIsNone(output)

    @patch('requests.request')
    def test_get_task_outputs(self, mock_request):
        '''
        Test get_task_outputs method
        '''
        # Mock response
        mock_response = MagicMock()
        mock_response.content  = json.dumps({
            "outputs": {
                "output_mapping":  {
                    'output1': 'value1',
                    'output2': 'value2'
                }
            }
        }).encode('utf-8')
        mock_response.json.return_value = {
            "outputs": {
                "output_mapping":  {
                    'output1': 'value1',
                    'output2': 'value2'
                }
            }
        }
        mock_request.return_value = mock_response
        # Create task with outputs
        task = WESTask('test_run_id', 'Test Task', outputs={'output1': 'value1', 'output2': 'value2'})

        # Test get_task_outputs
        outputs = self.platform.get_task_outputs(task)
        self.assertEqual(outputs, ['output1', 'output2'])

    @patch('requests.request')
    def test_get_tasks_by_name(self, mock_request):
        '''
        Test get_tasks_by_name method with task name filtering
        '''
        # Mock response - API filters by name, so only matching task is returned
        mock_response = MagicMock()
        mock_response.content = json.dumps({
            'runs': [
                {
                    'run_id': 'run1',
                    'name': 'Task 1',
                    'state': 'COMPLETE'
                }
            ]
        }).encode('utf-8')
        mock_response.json.return_value = {
            'runs': [
                {
                    'run_id': 'run1',
                    'name': 'Task 1',
                    'state': 'COMPLETE'
                }
            ]
        }
        mock_request.return_value = mock_response

        # Test get_tasks_by_name with specific task name
        project = {'project_id': 'test_project', 'name': 'Test Project'}
        tasks = self.platform.get_tasks_by_name(project, task_name='Task 1')

        # Verify request includes task name in filters
        mock_request.assert_called_with(
            method='GET',
            url='https://wes.example.com/ga4gh/wes/v1/runs',
            headers={'Authorization': 'Bearer test_token'},
            data=None,
            files=None,
            params={"filters": '{"tags": {"ProjectId": "test_project", "TaskName": "Task 1"}}'},
            timeout=120
        )

        # Verify only matching task returned
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0].run_id, 'run1')
        self.assertEqual(tasks[0].name, 'Task 1')
        self.assertEqual(tasks[0].state, 'Complete')

    @patch('requests.request')
    def test_get_tasks_by_name_match_all(self, mock_request):
        '''
        Test get_tasks_by_name method with no task name (should return all tasks)
        '''
        # Mock response
        mock_response = MagicMock()
        mock_response.content = json.dumps({
            'runs': [
                {
                    'run_id': 'run1',
                    'name': 'Task 1',
                    'state': 'COMPLETE'
                },
                {
                    'run_id': 'run2',
                    'name': 'Task 2',
                    'state': 'RUNNING'
                }
            ]
        }).encode('utf-8')
        mock_response.json.return_value = {
            'runs': [
                {
                    'run_id': 'run1',
                    'name': 'Task 1',
                    'state': 'COMPLETE'
                },
                {
                    'run_id': 'run2',
                    'name': 'Task 2',
                    'state': 'RUNNING'
                }
            ]
        }
        mock_request.return_value = mock_response

        # Test get_tasks_by_name without task name
        project = {'project_id': 'test_project', 'name': 'Test Project'}
        tasks = self.platform.get_tasks_by_name(project)

        # Verify all tasks returned
        self.assertEqual(len(tasks), 2)
        self.assertEqual(tasks[0].run_id, 'run1')
        self.assertEqual(tasks[0].name, 'Task 1')
        self.assertEqual(tasks[0].state, 'Complete')
        self.assertEqual(tasks[1].run_id, 'run2')
        self.assertEqual(tasks[1].name, 'Task 2')
        self.assertEqual(tasks[1].state, 'Running')

    @patch('requests.request')
    def test_get_tasks_by_name_match_name_and_inputs(self, mock_request):
        '''
        Test get_tasks_by_name method with task name and matching inputs
        '''
        # Define inputs to compare (must match exactly as JSON)
        inputs_to_compare = {
            'input1': {'class': 'File', 'path': 'file1'},
            'input2': [
                {'class': 'File', 'path': 'file2'},
                {'class': 'File', 'path': 'file3'}
            ]
        }

        # Mock initial response with two tasks with same name
        mock_list_response = MagicMock()
        mock_list_response.json.return_value = {
            'runs': [
                {
                    'run_id': 'run1',
                    'name': 'Task 1',
                    'state': 'COMPLETE',
                },
                {
                    'run_id': 'run2',
                    'name': 'Task 1',
                    'state': 'RUNNING',
                }
            ]
        }

        # Mock detailed responses for each task
        mock_detail_response1 = MagicMock()
        mock_detail_response1.json.return_value = {
            'run_id': 'run1',
            'request': {
                'workflow_params': inputs_to_compare  # Exact match
            }
        }

        mock_detail_response2 = MagicMock()
        mock_detail_response2.json.return_value = {
            'run_id': 'run2',
            'request': {
                'workflow_params': {
                    'input1': {'class': 'File', 'path': 'file1'},
                    'input2': [
                        {'class': 'File', 'path': 'file2'},
                        {'class': 'File', 'path': 'different_file'}  # Different
                    ]
                }
            }
        }

        # Set up mock to return different responses for different calls
        mock_request.side_effect = [
            mock_list_response,
            mock_detail_response1,
            mock_detail_response2
        ]

        # Test get_tasks_by_name with name and inputs
        project = {'project_id': 'test_project', 'name': 'Test Project'}
        tasks = self.platform.get_tasks_by_name(
            project,
            task_name='Task 1',
            inputs_to_compare=inputs_to_compare
        )

        # Verify only matching task returned
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0].run_id, 'run1')

    @patch('requests.request')
    def test_get_tasks_by_name_from_provided_tasks(self, mock_request):
        '''
        Test get_tasks_by_name can use provided tasks parameter
        NOTE: NGS360 platform currently ignores tasks parameter and always queries API
        '''
        project = {'project_id': 'test_project', 'name': 'Test Project'}

        # Mock API response
        mock_response = MagicMock()
        mock_response.content = json.dumps({
            'runs': [
                {
                    'run_id': 'run1',
                    'name': 'Task 1',
                    'state': 'COMPLETE'
                }
            ]
        }).encode('utf-8')
        mock_response.json.return_value = {
            'runs': [
                {
                    'run_id': 'run1',
                    'name': 'Task 1',
                    'state': 'COMPLETE'
                }
            ]
        }
        mock_request.return_value = mock_response

        # Create mock tasks (currently ignored by NGS360 implementation)
        task1 = WESTask('run1', 'Task 1')
        task2 = WESTask('run2', 'Task 2')
        tasks = [task1, task2]

        # Test with provided tasks
        result = self.platform.get_tasks_by_name(
            project=project,
            task_name='Task 1',
            tasks=tasks
        )

        # Verify only matching task returned (from API, not from provided tasks)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].name, 'Task 1')

    def test_get_tasks_by_name_with_new_task_added(self):
        '''
        Test get_tasks_by_name with a new task that may only have id, not name
        '''
        project = {'project_id': 'test_project', 'name': 'Test Project'}

        # Create mock task
        task1 = WESTask('run1', 'Task 1')
        tasks = [task1]

        # Test searching for task that doesn't exist
        result = self.platform.get_tasks_by_name(
            project=project,
            task_name='Task 2',
            tasks=tasks
        )

        # Verify empty list returned
        self.assertListEqual(result, [])

    @patch('requests.request')
    def test_get_tasks_by_name_match_subset_of_inputs(self, mock_request):
        '''
        Test get_tasks_by_name with inputs_to_compare as subset of task inputs
        Task should match even if it has additional inputs beyond those compared
        '''
        # Define inputs to compare (subset)
        inputs_to_compare = {
            'input1': 'value1'
        }

        # Mock initial response with task that has MORE inputs
        mock_list_response = MagicMock()
        mock_list_response.json.return_value = {
            'runs': [
                {
                    'run_id': 'run1',
                    'name': 'Task 1',
                    'state': 'COMPLETE',
                }
            ]
        }

        # Mock detailed response - task has input1, input2, and input3
        mock_detail_response = MagicMock()
        mock_detail_response.json.return_value = {
            'run_id': 'run1',
            'request': {
                'workflow_params': {
                    'input1': 'value1',      # Matches
                    'input2': 'value2',      # Extra (should be ignored)
                    'input3': 'value3'       # Extra (should be ignored)
                }
            }
        }

        mock_request.side_effect = [mock_list_response, mock_detail_response]

        # Test
        project = {'project_id': 'test_project', 'name': 'Test Project'}
        tasks = self.platform.get_tasks_by_name(
            project,
            task_name='Task 1',
            inputs_to_compare=inputs_to_compare
        )

        # Verify task matched despite having extra inputs
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0].run_id, 'run1')

    @patch('requests.request')
    def test_get_tasks_by_name_no_match_different_value(self, mock_request):
        '''
        Test get_tasks_by_name rejects task when input values don't match
        '''
        # Define inputs to compare
        inputs_to_compare = {
            'input1': 'value1'
        }

        # Mock initial response
        mock_list_response = MagicMock()
        mock_list_response.json.return_value = {
            'runs': [
                {
                    'run_id': 'run1',
                    'name': 'Task 1',
                    'state': 'COMPLETE',
                }
            ]
        }

        # Mock detailed response - task has DIFFERENT value for input1
        mock_detail_response = MagicMock()
        mock_detail_response.json.return_value = {
            'run_id': 'run1',
            'request': {
                'workflow_params': {
                    'input1': 'different_value',  # Does NOT match
                    'input2': 'value2'
                }
            }
        }

        mock_request.side_effect = [mock_list_response, mock_detail_response]

        # Test
        project = {'project_id': 'test_project', 'name': 'Test Project'}
        tasks = self.platform.get_tasks_by_name(
            project,
            task_name='Task 1',
            inputs_to_compare=inputs_to_compare
        )

        # Verify no tasks matched
        self.assertEqual(len(tasks), 0)

    @patch('requests.request')
    def test_delete_task(self, mock_request):
        '''
        Test delete_task method
        '''
        # Mock response
        mock_response = MagicMock()
        mock_response.content = b''
        mock_request.return_value = mock_response

        # Create task
        task = WESTask('test_run_id', 'Test Task')

        # Test delete_task
        result = self.platform.delete_task(task)
        self.assertTrue(result)

        # Verify request
        mock_request.assert_called_with(
            method='DELETE',
            url='https://wes.example.com/ga4gh/wes/v1/runs/test_run_id',
            headers={'Authorization': 'Bearer test_token'},
            data=None,
            files=None,
            params=None,
            timeout=120
        )

    @patch('requests.get')
    def test_project_methods(self, mock_get):
        '''
        Test project methods
        '''
        # Mock NGS360 API response for get_project_by_name
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'data': [{'name': 'Test Project', 'project_id': 'test_id'}]
        }
        mock_get.return_value = mock_response

        # Test create_project
        project = self.platform.create_project('Test Project', 'Test Description')
        self.assertEqual(project['name'], 'Test Project')
        self.assertEqual(project['description'], 'Test Description')

        # Test get_project_by_name (calls NGS360 API)
        project2 = self.platform.get_project_by_name('Test Project')
        self.assertEqual(project2['name'], 'Test Project')
        self.assertEqual(project2['project_id'], 'test_id')

        # Test get_projects (local virtual projects)
        projects = self.platform.get_projects()
        self.assertEqual(len(projects), 1)
        self.assertEqual(projects[0]['name'], 'Test Project')

        # Test delete_project_by_name
        result = self.platform.delete_project_by_name('Test Project')
        self.assertTrue(result)
        self.assertEqual(len(self.platform.projects), 0)

    @patch('requests.post')
    @patch('builtins.open')
    def test_upload_file(self, mock_open, mock_post):
        '''
        Test upload_file method
        '''
        # Mock file content
        mock_file = MagicMock()
        mock_open.return_value.__enter__.return_value = mock_file

        # Mock successful response
        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_response.json.return_value = {'id': 'test_file_id'}
        mock_post.return_value = mock_response

        # Test upload_file
        project = {'project_id': 'test_project_id'}
        result = self.platform.upload_file(
            'test_file.txt',
            project,
            dest_folder='data',
            destination_filename='uploaded_file.txt'
        )

        # Verify result
        self.assertEqual(result, 'ngs360://test_file_id')

        # Verify API call
        mock_post.assert_called_once()
        _, kwargs = mock_post.call_args
        self.assertIn('files', kwargs)
        self.assertIn('data', kwargs)
        self.assertIn('project_id', 'test_project_id')

    @patch('requests.post')
    @patch('builtins.open')
    def test_upload_file_failure(self, mock_open, mock_post):
        '''
        Test upload_file method with failure response
        '''
        # Mock file content
        mock_file = MagicMock()
        mock_open.return_value.__enter__.return_value = mock_file

        # Mock failure response
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.json.return_value = {'error': 'Bad request'}
        mock_post.return_value = mock_response

        # Test upload_file
        project = {'project_id': 'test_project_id'}
        result = self.platform.upload_file('test_file.txt', project, dest_folder='data')

        # Verify result
        self.assertIsNone(result)

    @patch('requests.get')
    def test_download_file_url(self, mock_get):
        '''
        Test download_file method with URL
        '''
        # Mock response
        mock_response = MagicMock()
        mock_response.iter_content.return_value = [b'test content']
        mock_get.return_value = mock_response

        # Mock file writing
        with patch('builtins.open', MagicMock()) as mock_open:
            mock_file = MagicMock()
            mock_open.return_value.__enter__.return_value = mock_file

            # Test download_file
            result = self.platform.download_file('http://example.com/file.txt', '/tmp')

            # Verify result
            self.assertEqual(result, '/tmp/file.txt')
            mock_get.assert_called_once()

    def test_download_file_invalid(self):
        '''
        Test download_file method with invalid file
        '''
        result = self.platform.download_file('invalid_file', '/tmp')
        self.assertIsNone(result)

    def test_export_file(self):
        '''
        Test export_file method (not supported in WES)
        '''
        self.assertIsNone(self.platform.export_file('file_id', 'bucket', 'prefix'))

    @patch('requests.get')
    def test_get_project_by_id(self, mock_get):
        '''
        Test get_project_by_id method
        '''
        # Mock successful response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'project_id': 'test_id', 'name': 'Test Project'}
        mock_get.return_value = mock_response

        # Test get_project_by_id
        result = self.platform.get_project_by_id('test_id')
        self.assertEqual(result['project_id'], 'test_id')
        self.assertEqual(result['name'], 'Test Project')

    @patch('requests.get')
    def test_get_project_by_id_failure(self, mock_get):
        '''
        Test get_project_by_id method with failure
        '''
        # Mock failed response
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.json.return_value = {'error': 'Not found'}
        mock_get.return_value = mock_response

        # Test get_project_by_id failure
        result = self.platform.get_project_by_id('nonexistent_id')
        self.assertIsNone(result)

    def test_workflow_methods(self):
        '''
        Test workflow-related methods (not supported in WES)
        '''
        project = {'project_id': 'test_project'}

        # Test copy_workflow
        result = self.platform.copy_workflow('src_workflow', project)
        self.assertEqual(result, 'src_workflow')

        # Test copy_workflows
        result = self.platform.copy_workflows('ref_project', project)
        self.assertEqual(result, [])

        # Test get_workflows
        result = self.platform.get_workflows(project)
        self.assertEqual(result, [])

    def test_file_methods_not_supported(self):
        '''
        Test file methods that are not supported in WES
        '''
        project = {'project_id': 'test_project'}

        # Test get_files
        result = self.platform.get_files(project)
        self.assertEqual(result, [])

        # Test get_file_id
        result = self.platform.get_file_id(project, 'test/file.txt')
        self.assertEqual(result, 'test/file.txt')

        # Test get_folder_id
        result = self.platform.get_folder_id(project, 'test/folder')
        self.assertEqual(result, 'test/folder')

        # Test rename_file
        self.assertIsNone(self.platform.rename_file('file_id', 'new_name.txt'))

        # Test roll_file
        self.assertIsNone(self.platform.roll_file(project, 'file_name.txt'))

    def test_task_methods_edge_cases(self):
        '''
        Test task methods with edge cases
        '''
        # Test methods with None task
        result = self.platform.get_task_state(None)
        self.assertEqual(result, "Unknown")

        result = self.platform.get_task_output(None, 'output_name')
        self.assertIsNone(result)

        result = self.platform.get_task_outputs(None)
        self.assertIsNone(result)

        result = self.platform.get_task_input(None, 'input_name')
        self.assertIsNone(result)

        result = self.platform.delete_task(None)
        self.assertFalse(result)

    def test_task_input_output_methods(self):
        '''
        Test task input and output methods
        '''
        # Create task with inputs
        task = WESTask('test_run_id', 'Test Task', inputs={'input1': 'value1'})

        # Test get_task_input
        result = self.platform.get_task_input(task, 'input1')
        self.assertEqual(result, 'value1')

        result = self.platform.get_task_input(task, 'nonexistent')
        self.assertIsNone(result)

    def test_get_task_output_filename(self):
        '''
        Test get_task_output_filename method (deprecated)
        '''
        with patch.object(self.platform, 'get_task_output') as mock_get_output:
            # Test with string output
            mock_get_output.return_value = 'path/to/file.txt'
            task = WESTask('test_run_id', 'Test Task')
            result = self.platform.get_task_output_filename(task, 'output1')
            self.assertEqual(result, 'file.txt')

            # Test with list output
            mock_get_output.return_value = ['path/to/file1.txt', 'path/to/file2.txt']
            result = self.platform.get_task_output_filename(task, 'output2')
            self.assertEqual(result, ['file1.txt', 'file2.txt'])

            # Test with no output
            mock_get_output.return_value = None
            result = self.platform.get_task_output_filename(task, 'nonexistent')
            self.assertIsNone(result)

    def test_user_methods(self):
        '''
        Test user methods (not supported in WES)
        '''
        project = {'project_id': 'test_project'}

        # Test add_user_to_project
        self.assertIsNone(self.platform.add_user_to_project('user_id', project, 'READ'))

        # Test get_user
        self.assertIsNone(self.platform.get_user('user_id'))

    def test_cost_methods(self):
        '''
        Test cost methods (not supported in WES)
        '''
        project = {'project_id': 'test_project'}
        task = WESTask('test_run_id', 'Test Task')

        # Test get_project_cost
        self.assertIsNone(self.platform.get_project_cost(project))

        # Test get_task_cost
        self.assertIsNone(self.platform.get_task_cost(task))

    def test_current_task_and_users(self):
        '''
        Test get_current_task and get_project_users methods
        '''
        project = {'project_id': 'test_project'}

        # Test get_current_task
        self.assertIsNone(self.platform.get_current_task())

        # Test get_project_users
        result = self.platform.get_project_users(project)
        self.assertEqual(result, [])

    def test_detect_method(self):
        '''
        Test detect class method
        '''
        # Test with WES_API_ENDPOINT set
        with patch.dict('os.environ', {'WES_API_ENDPOINT': 'https://wes.example.com'}):
            result = NGS360Platform.detect()
            self.assertTrue(result)

        # Test without WES_API_ENDPOINT
        with patch.dict('os.environ', {}, clear=True):
            result = NGS360Platform.detect()
            self.assertFalse(result)

if __name__ == '__main__':
    unittest.main()

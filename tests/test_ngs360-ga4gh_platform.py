'''
Test WES Platform implementation
'''
import json
import unittest
from unittest.mock import patch, MagicMock

from src.cwl_platform.ngs360_platform import NGS360Platform, WESTask

class TestNGS360Platform(unittest.TestCase):
    '''
    Test WES Platform implementation
    '''
    def setUp(self):
        '''
        Set up test environment
        '''
        self.platform = NGS360Platform('WES')
        self.platform.api_endpoint = 'https://wes.example.com/ga4gh/wes/v1'
        self.platform._auth_config['token'] = 'test_token'
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
            timeout=120,
            auth=None
        )
        self.assertEqual(result, {'key': 'value'})

    @patch('requests.request')
    def test_connect(self, mock_request):
        '''
        Test connect method
        '''
        # Mock response
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

        # Test connect
        platform = NGS360Platform('WES')
        result = platform.connect(api_endpoint='https://wes.example.com/ga4gh/wes/v1', auth_token='test_token')
        self.assertTrue(result)
        self.assertTrue(platform.connected)
        self.assertEqual(platform.api_endpoint, 'https://wes.example.com/ga4gh/wes/v1')
        self.assertEqual(platform.auth_token, 'test_token')

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
            project={'id': "P-1234567", 'name': 'Test Project'},
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
                'tags': '{"ProjectName": "Test Project", "TaskName": "Test Task"}'
            },
            files=None,
            params=None,
            timeout=120,
            auth=None
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
            timeout=120,
            auth=None
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
        Test get_tasks_by_name method
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

        # Test get_tasks_by_name
        project = {'id': 'test_project', 'name': 'Test Project'}
        tasks = self.platform.get_tasks_by_name(project)

        # Verify request
        mock_request.assert_called_with(
            method='GET',
            url='https://wes.example.com/ga4gh/wes/v1/runs',
            headers={'Authorization': 'Bearer test_token'},
            data=None,
            files=None,
            params={'tags': '{"Project": "Test Project"}'},
            timeout=120,
            auth=None
        )

        # Verify tasks
        self.assertEqual(len(tasks), 2)
        self.assertEqual(tasks[0].run_id, 'run1')
        self.assertEqual(tasks[0].name, 'Task 1')
        self.assertEqual(tasks[0].state, 'Complete')
        self.assertEqual(tasks[1].run_id, 'run2')
        self.assertEqual(tasks[1].name, 'Task 2')
        self.assertEqual(tasks[1].state, 'Running')

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
            timeout=120,
            auth=None
        )

    def test_project_methods(self):
        '''
        Test project methods
        '''
        # Test create_project
        project = self.platform.create_project('Test Project', 'Test Description')
        self.assertEqual(project['name'], 'Test Project')
        self.assertEqual(project['description'], 'Test Description')

        # Test get_project_by_name
        project2 = self.platform.get_project_by_name('Test Project')
        self.assertEqual(project2['name'], 'Test Project')
        self.assertEqual(project2['description'], 'Test Description')

        # Test get_projects
        projects = self.platform.get_projects()
        self.assertEqual(len(projects), 1)
        self.assertEqual(projects[0]['name'], 'Test Project')

        # Test delete_project_by_name
        result = self.platform.delete_project_by_name('Test Project')
        self.assertTrue(result)
        self.assertEqual(len(self.platform.projects), 0)

if __name__ == '__main__':
    unittest.main()

'''
AWS HealthOmics class
'''

import logging
import json
import boto3
import botocore
import httpx

from tenacity import retry, wait_fixed, stop_after_attempt

from .base_platform import Platform

logger = logging.getLogger(__name__)

class OmicsPlatform(Platform):
    ''' AWS HealthOmics Platform class '''
    def __init__(self, name):
        super().__init__(name)
        self.api = None
        self.output_bucket = None
        self.role_arn = None
        self.wes_client = None
        self.wes_url = None

    def _list_file_in_s3(self, s3path):
        '''
        List all files in S3 path.
        s3path: S3 path to directory, formatted as s3://bucket/path/to/folder/.

        return list of files within the directory with full s3 path.

        '''
        bucket = s3path.split('s3://')[1].split('/')[0]
        prefix = s3path.split(bucket+'/')[1]
        response = self.s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix)
        files = []
        for element in response.get('Contents', []):
            files += ['s3://'+bucket+'/'+element['Key']]
        return files

    def connect(self, **kwargs):
        '''
        Connect to AWS Omics platform

        If ~/.aws/credentials or ~/.aws/config does not provide a region, region
        should be specified in the AWS_DEFAULT_REGION environment variable.
        '''
        self.api = boto3.client('omics')
        self.output_bucket = kwargs['output_bucket']
        self.role_arn = kwargs['role_arn']
        self.s3_client = boto3.client('s3')
        
        # WES API connection parameters
        self.wes_url = kwargs.get('wes_url', 'http://localhost:8000/ga4gh/wes/v1')
        self.wes_username = kwargs.get('wes_username')
        self.wes_password = kwargs.get('wes_password')

    def copy_folder(self, source_project, source_folder, destination_project):
        '''
        Do nothing and return reference folder, which should be an S3 path.
        NOTE: Launchers copy the reference folder to the destination project so that everything is co-located.
              However this can cause lots of data duplication in S3.  For now we will just use the reference folder
              until another use-case is identified that we need to copy the data.
        '''
        return source_folder

    def download_file(self, file, dest_folder):
        """
        Download a file to a local directory
        :param file: SevenBridges file id (or object) to download
        :param dest_folder: Destination folder to download file to
        :return: Name of local file downloaded or None
        """
        ''' TODO '''
        return

    def export_file(self, file, bucket_name, prefix):
        """
        Use platform specific functionality to copy a file from a platform to an S3 bucket.
        :param file: File to export
        :param bucket_name: S3 bucket name
        :param prefix: Destination S3 folder to export file to, path/to/folder
        :return: Export job of file
        For SevenBridges, there are two differences from the expected base implementation:
        1. the bucket name is translated to a volume name, replacing all dashes with underscores.
        2. the return value is the export job object, not the S3 file path.
        """
        ''' TODO '''
        return

    def copy_workflow(self, src_workflow, destination_project):
        '''Do nothing and return workflow id'''
        return src_workflow

    def copy_workflows(self, reference_project, destination_project):
        '''Do nothing. This function seems not used in launcher?'''
        pass

    def get_workflows(self, project):
        '''
        Get workflows in a project

        :param: Platform Project
        :return: List of workflows
        '''
        ''' TODO '''
        return

    def create_project(self, project_name, project_description, **kwargs):
        '''
        Create project
        '''
        self.api.create_run_group(name=project_name)

    def delete_project_by_name(self, project_name):
        '''
        Delete a project on the platform 
        '''
        ''' TODO '''
        return

    def delete_task(self, task):
        '''
        Cancel a workflow run via WES API
        task: A dictionary containing the run information, including the WES run ID
        '''
        # Set up auth if provided
        auth = None
        if self.wes_username and self.wes_password:
            auth = (self.wes_username, self.wes_password)
        
        # Extract the WES run ID from the task dictionary
        run_id = task.get('id')
        if not run_id:
            logger.error("No run ID found in task object")
            return False
        
        try:
            # Cancel the run via WES API
            cancel_url = f"{self.wes_url}/runs/{run_id}/cancel"
            response = httpx.post(cancel_url, auth=auth, timeout=30.0)
            response.raise_for_status()
            logger.info(f"Successfully cancelled run {run_id}")
            return True
        except Exception as e:
            logger.error(f"Error cancelling run {run_id}: {e}")
            return False

    @classmethod
    def detect(cls):
        return False

    def get_current_task(self):
        ''' Get the current task '''
        return None

    def get_task_cost(self, task):
        ''' Return task cost '''
        return 0

    def get_file_id(self, project, file_path):
        '''Return file s3 path for Omics job input'''
        return file_path

    def get_files(self, project, filters=None):
        """
        Retrieve files in a project matching the filter criteria

        :param project: Project to search for files
        :param filters: Dictionary containing filter criteria
            {
                'name': 'file_name',
                'prefix': 'file_prefix',
                'suffix': 'file_suffix',
                'folder': 'folder_name',
                'recursive': True/False
            }
        :return: List of tuples (file path, file object) matching filter criteria
        """
        ''' TODO '''
        return

    def get_folder_id(self, project, folder_path):
        '''
        There is not unique ID for a folder in s3, so just return the folder_path
        The one caveat is that Omics wants trailing slashes on folder paths, so add one.
        '''
        if folder_path.endswith("/"):
            return folder_path
        return folder_path + "/"

    def get_task_input(self, task, input_name):
        '''
        Retrieve the input field of the task using WES API
        task: A dictionary containing the run information, including the WES run ID
        input_name: Name of the input to retrieve
        '''
        # Set up auth if provided
        auth = None
        if self.wes_username and self.wes_password:
            auth = (self.wes_username, self.wes_password)
        
        # Extract the WES run ID from the task dictionary
        run_id = task.get('id')
        if not run_id:
            logger.error("No run ID found in task object")
            return None
        
        # Check if parameters are already in the task object
        if task.get('parameters') and input_name in task['parameters']:
            return task['parameters'][input_name]
        
        try:
            # Get full run details from WES API
            run_details_url = f"{self.wes_url}/runs/{run_id}"
            response = httpx.get(run_details_url, auth=auth, timeout=30.0)
            response.raise_for_status()
            run_details = response.json()
            
            # Check if request and workflow_params are available
            if not run_details.get('request') or not run_details['request'].get('workflow_params'):
                logger.warning(f"No workflow parameters found for run {run_id}")
                return None
            
            # Parse workflow parameters
            try:
                params = json.loads(run_details['request']['workflow_params'])
                
                # Check if the specific input is available
                if input_name in params:
                    return params[input_name]
                else:
                    logger.warning(f"Input '{input_name}' not found in run {run_id}")
                    return None
                    
            except json.JSONDecodeError:
                logger.error(f"Could not parse workflow parameters for run {run_id}")
                return None
                
        except Exception as e:
            logger.error(f"Error getting input {input_name} for run {run_id}: {e}")
            return None

    def get_task_outputs(self, task):
        '''
        Get all outputs for a task using WES API
        task: A dictionary containing the run information, including the WES run ID
        '''
        # Set up auth if provided
        auth = None
        if self.wes_username and self.wes_password:
            auth = (self.wes_username, self.wes_password)
        
        # Extract the WES run ID from the task dictionary
        run_id = task.get('id')
        if not run_id:
            logger.error("No run ID found in task object")
            return []
            
        try:
            # Get full run details from WES API
            run_details_url = f"{self.wes_url}/runs/{run_id}"
            response = httpx.get(run_details_url, auth=auth, timeout=30.0)
            response.raise_for_status()
            run_details = response.json()
            if not run_details.get('outputs'):
                logger.warning(f"No outputs available for run {run_id}")
                return []

            task_outputUri = run_details['outputs'].get('output_location')
            if not task_outputUri:
                logger.warning(f"No output_location found for run {run_id}")
                return []
                
            output_json = task_outputUri.split(self.output_bucket+'/')[1] + run_id + "/logs/outputs.json"
            response = self.s3_client.get_object(Bucket=self.output_bucket, Key=output_json)
            content = response['Body'].read().decode("utf-8")
            mapping = json.loads(content)

            return list(mapping.keys())

        except Exception as e:
            logger.error(f"Error getting outputs for run {run_id}: {e}")
            return []

    def get_task_state(self, task, refresh=False):
        '''
        Get status of run by task_id using WES API.
        task: A dictionary containing the run information, including the WES run ID.
        return status of the run (Complete, Failed, Running, Cancelled, Queued).
        '''
        # Set up auth if provided
        auth = None
        if self.wes_username and self.wes_password:
            auth = (self.wes_username, self.wes_password)
        
        # Extract the WES run ID from the task dictionary
        run_id = task.get('id')
        if not run_id:
            logger.error("No run ID found in task object")
            raise ValueError("No run ID found in task object")
        
        try:
            # Get run status from WES API
            run_status_url = f"{self.wes_url}/runs/{run_id}/status"
            response = httpx.get(run_status_url, auth=auth, timeout=30.0)
            response.raise_for_status()
            result = response.json()
            
            # Map WES state to PAML state
            wes_state = result.get('state')
            logger.debug(f"WES state for run {run_id}: {wes_state}")


            logging.info('check state results')
            logging.info(task)
            logging.info(wes_state)
            a=input()

            if wes_state == 'COMPLETE':
                return 'Complete'
            if wes_state == 'EXECUTOR_ERROR' or wes_state == 'SYSTEM_ERROR':
                return 'Failed'
            if wes_state == 'RUNNING' or wes_state == 'INITIALIZING':
                return 'Running'
            if wes_state == 'CANCELED':
                return 'Cancelled'
            if wes_state == 'QUEUED' or wes_state == 'PAUSED':
                return 'Queued'
            
            # Default to queued if we don't recognize the state
            logger.warning(f"Unknown WES state: {wes_state} for run {run_id}")
            return 'Queued'
            
        except Exception as e:
            logger.error(f"Error getting status for run {run_id}: {e}")
            raise ValueError(f'No status information found for job {run_id}. Check job status.')

    def get_task_output(self, task, output_name):
        '''
        Retrieve the output field of the task using WES API
        task: A dictionary containing the run information, including the WES run ID
        output_name: Name of the output to retrieve
        '''
        # Set up auth if provided
        auth = None
        if self.wes_username and self.wes_password:
            auth = (self.wes_username, self.wes_password)
        
        # Extract the WES run ID from the task dictionary
        run_id = task.get('id')
        if not run_id:
            logger.error("No run ID found in task object")
            raise KeyError("No run ID found in task object")
        
        try:
            # Get full run details from WES API
            run_details_url = f"{self.wes_url}/runs/{run_id}"
            response = httpx.get(run_details_url, auth=auth, timeout=30.0)
            response.raise_for_status()
            run_details = response.json()
            if not run_details.get('outputs'):
                raise KeyError(f"No outputs available for run {run_id}")

            task_outputUri = run_details['outputs'].get('output_location')
            if not task_outputUri:
                raise KeyError(f"No output_location found for run {run_id}")
                
            output_json = task_outputUri.split(self.output_bucket+'/')[1] + run_id + "/logs/outputs.json"
            response = self.s3_client.get_object(Bucket=self.output_bucket, Key=output_json)
            content = response['Body'].read().decode("utf-8")
            mapping = json.loads(content)

            if output_name not in mapping:
                raise KeyError(f"Output field '{output_name}' not found in output json file.")
            all_outputs = mapping[output_name]
            if isinstance(all_outputs, list):
                outputs = [c["location"] for c in all_outputs if "location" in c]
                return outputs

            if "location" in all_outputs:
                return all_outputs["location"]
            else:
                raise KeyError(f"Could not find path for '{output_name}'")

        except Exception as e:
            logger.error(f"Error getting output {output_name} for run {run_id}: {e}")
            raise KeyError(f"Could not retrieve output '{output_name}' for run {run_id}: {str(e)}")

    def get_task_output_filename(self, task, output_name):
        '''
        Retrieve the output field of the task and return filename
        task: A dictionary containing the run information, including the WES run ID
        output_name: Name of the output to retrieve
        '''
        task_output_url = self.get_task_output(task, output_name)
        
        if isinstance(task_output_url, list):
            # Handle list of file URLs
            task_output_name = [fileurl.split('/')[-1] for fileurl in task_output_url]
        elif isinstance(task_output_url, str):
            # Handle single file URL
            task_output_name = task_output_url.split('/')[-1]
        else:
            # Handle other types of outputs
            run_id = task.get('id')
            logger.warning(f"Output {output_name} for run {run_id} is not a file URL: {task_output_url}")
            task_output_name = str(task_output_url)
            
        return task_output_name

    def get_tasks_by_name(self,
                          project,
                          task_name=None,
                          inputs_to_compare=None,
                          tasks=None):
        '''
        Get all processes/tasks in a project with a specified name, or all tasks
        if no name is specified. Optionally, compare task inputs to ensure
        equivalency (eg for reuse).
        :param project: The project to search (run group ID)
        :param task_name: The name of the process to search for (if None return all tasks)
        :param inputs_to_compare: Inputs to compare to ensure task equivalency
        :param tasks: List of tasks to search in (if None, query all tasks in project)
        :return: List of task dictionaries with run information
        '''
        # Set up auth if provided
        auth = None
        if self.wes_username and self.wes_password:
            auth = (self.wes_username, self.wes_password)
        
        matching_tasks = []
        
        try:
            # If tasks is not provided, query tasks from WES API
            if tasks is None:
                # Get list of runs from WES API
                list_runs_url = f"{self.wes_url}/runs"
                response = httpx.get(list_runs_url, auth=auth, timeout=30.0)
                response.raise_for_status()
                result = response.json()
                
                # Extract the list of runs
                runs = result.get('runs', [])
                tasks = []
                
                # Filter by project tag if project is provided
                if project:
                    for run in runs:
                        if run.get('tags') and run['tags'].get('Project') == project:
                            # If task_name is provided, filter by name
                            if task_name is None or run.get('name') == task_name:
                                # Create a task dictionary with the necessary information
                                task = {
                                    "id": run.get('run_id'),
                                    "name": run.get('name'),
                                    "status": run.get('state'),
                                    "project": project
                                }
                                tasks.append(task)
                else:
                    # Create task dictionaries for all runs
                    for run in runs:
                        task = {
                            "id": run.get('run_id'),
                            "name": run.get('name'),
                            "status": run.get('state')
                        }
                        tasks.append(task)
            else:
                if task_name is not None:
                    tasks = [task for task in tasks if task.get('name',None) == task_name]

            # If inputs_to_compare is provided, we need to get full run details for each task
            if inputs_to_compare:
                for task in tasks:
                    task_id = task.get('id')
                    if not task_id:
                        continue
                    
                    # Get full run details to check inputs
                    try:
                        run_details_url = f"{self.wes_url}/runs/{task_id}"
                        details_response = httpx.get(run_details_url, auth=auth, timeout=30.0)
                        details_response.raise_for_status()
                        details = details_response.json()
                        
                        # Check if name matches
                        if task_name is not None and details.get('name') != task_name:
                            continue
                        
                        # Check if inputs match
                        if details.get('request') and details['request'].get('workflow_params'):
                            # Parse workflow params
                            try:
                                params = json.loads(details['request']['workflow_params'])
                                all_inputs_match = True
                                
                                for input_name, input_value in inputs_to_compare.items():
                                    if input_name not in params:
                                        all_inputs_match = False
                                        break
                                    
                                    if params[input_name] != input_value:
                                        all_inputs_match = False
                                        break
                                
                                if all_inputs_match:
                                    # Update task with parameters
                                    task['parameters'] = params
                                    matching_tasks.append(task)
                            except json.JSONDecodeError:
                                logger.warning(f"Could not parse workflow params for run {task_id}")
                    except Exception as e:
                        logger.warning(f"Error getting details for run {task_id}: {e}")
            else:
                # If no inputs to compare, just return the tasks
                matching_tasks = tasks
            
            return matching_tasks
            
        except Exception as e:
            logger.error(f"Error getting tasks by name: {e}")
            return []

    def get_project(self):
        '''
        Since there is no concept of project in Omics, raise an error.
        '''
        raise ValueError("Omics does not support get_project. Use get_project_by_id or get_project_by_name instead.")

    def get_project_by_name(self, project_name):
        ''' Return the run group ID as the project identifier '''
        response = self.api.list_run_groups(
            name=project_name, maxResults=100
        )
        if len(response['items'])>0:
            run_group_id = response['items'][0]['id']
            return run_group_id

        logger.error('Could not find project with name: %s', project_name)
        return None

    def get_project_by_id(self, project_id):
        ''' Return the project ID directly as the run group ID '''
        return project_id

    def get_project_cost(self, project):
        ''' Return project cost '''
        # TODO: Return total cost from run_group_id
        return 0

    def get_project_users(self, project):
        ''' Return a list of user objects associated with a project '''
        ''' TODO '''
        return None

    def get_projects(self):
        ''' Get list of all projects '''
        # TODO: I think this should return a list of project names using run group id
        return None

    def add_user_to_project(self, platform_user, project, permission):
        """ 
        Add a user to a project on the platform
        :param platform_user: platform user (from get_user)
        :param project: platform project
        :param permission: permission (permission="read|write|execute|admin")
        """
        ''' TODO '''
        return

    def get_user(self, user):
        '''Get a user object from their (platform) user id or email address'''
        raise ValueError("Not yet implemented")

    def rename_file(self, fileid, new_filename):
        raise ValueError("Not yet implemented")

    def roll_file(self, project, file_name):
        raise ValueError("Not yet implemented")

    def stage_output_files(self, project, output_files):
        ''' TODO '''
        return

    def stage_task_output(self, task, project, output_to_export, output_directory_name):
        ''' TODO '''
        return

    @retry(wait=wait_fixed(1), stop=stop_after_attempt(3))
    def submit_task(self, name, project, workflow, parameters, execution_settings=None):
        '''
        Submit workflow for one sample using GA4GH WES API.
        name: sample ID.
        project: string containing the run group ID
        workflow: workflow ID in omics.
        parameters: dictionary of input parameters.

        return: Dictionary containing run information including ID and name
        '''
        base_output_path = f"s3://{self.output_bucket}/Project/"
        base_output_path += f"{project}/{workflow}/{name.replace(' ','')}/"

        # Prepare workflow engine parameters
        workflow_engine_params = {
            "roleArn": self.role_arn,
            "runGroupId": project,
            "name": name,
            "outputUri": base_output_path,
            "storageType": "DYNAMIC"
        }
        
        # Add cache settings if provided
        if execution_settings and "cacheId" in execution_settings:
            workflow_engine_params["cacheId"] = execution_settings["cacheId"]
            #workflow_engine_params["cacheBehavior"] = "CACHE_ON_FAILURE"
            workflow_engine_params["cacheBehavior"] = "CACHE_ALWAYS"
        
        # Prepare tags
        tags = {"Project": project}
        
        try:
            logger.debug("Starting run for %s via WES API", name)
            
            # Create WES API request
            url = f"{self.wes_url}/runs"
            
            # Prepare request data
            data = {
                "workflow_url": f"omics:{workflow}",
                "workflow_type": "CWL",  # Adjust as needed based on your workflow type
                "workflow_type_version": "v1.0",
                "workflow_params": json.dumps(parameters),
                "workflow_engine_parameters": json.dumps(workflow_engine_params),
                "tags": json.dumps(tags),
                "name": name
            }
            
            # Set up auth if provided
            auth = None
            if self.wes_username and self.wes_password:
                auth = (self.wes_username, self.wes_password)
            
            # Submit the workflow
            response = httpx.post(
                url,
                data=data,
                auth=auth,
                timeout=30.0
            )
            response.raise_for_status()
            
            # Parse response
            result = response.json()
            run_id = result.get("run_id")
            
            logger.info('Started run for %s, WES RunID: %s', name, run_id)
            
            # Create a job object with the necessary information
            job = {
                "id": run_id,
                "name": name,
                "status": "PENDING",
                "project": project,
                "workflow": workflow,
                "parameters": parameters,
                "outputUri": base_output_path
            }
            
            return job
            
        except httpx.HTTPStatusError as err:
            logger.error('Could not start run for %s: HTTP error %s - %s',
                        name, err.response.status_code, err.response.text)
            return None
        except Exception as err:
            logger.error('Could not start run for %s: %s', name, err)
            return None

    def upload_file(self, filename, project, dest_folder=None, destination_filename=None, overwrite=False): # pylint: disable=too-many-arguments
        self.logger.info("Uploading file %s to project %s", filename, project)
        target_bucket = self.output_bucket
        target_filepath = f"Project/{project}" + dest_folder
        if destination_filename:
            target_filepath += destination_filename
        else:
            target_filepath += filename.split('/')[-1]
        try:
            self.s3_client.upload_file(filename, target_bucket, target_filepath)
            file_id = f"s3://{self.output_bucket}/"+target_filepath
            return file_id
        except Exception as e:
            logger.error('Could not upload file %s', filename)
            return None

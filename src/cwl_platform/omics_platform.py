'''
AWS HealthOmics class
'''

import logging
import json
import boto3
import botocore

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
        ''' Delete a task/workflow/process '''
        self.logger.info('TBD: Deleting task %s', task)

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
        ''' Retrieve the input field of the task '''
        self.logger.info("TBD: Getting input for task %s", task)
        return None

    def get_task_outputs(self, task):
        self.logger.info('TBD: Getting task outputs %s', task)
        return []

    def get_task_state(self, task, refresh=False):
        ''' 
        Get status of run by task_id.
        task: A dictionary of omics response from start_run. Includes Run ID, Name, Tags, etc.
        return status of the run (Complete, Failed, Running, Cancelled, Queued).
        '''

        try:
            run_info = self.api.get_run(id=task['id'])
            job_status = run_info['status']
        except:
            raise ValueError('No Status information found for job %s. Check job status.', task['id'])

        if job_status == 'COMPLETED':
            return 'Complete'
        if job_status == 'FAILED':
            return 'Failed'
        if job_status in ['STARTING','RUNNING','STOPPING']:
            return 'Running'
        if job_status in ['CANCELLED','DELETED']:
            return 'Cancelled'
        if job_status == 'PENDING':
            return 'Queued'

        raise ValueError('Unknown task state: %s : %s', task['id'], job_status)

    def get_task_output(self, task, output_name):
        ''' Retrieve the output field of the task '''
        # Get output file mapping from outputs.json
        taskinfo = self.api.get_run(id=task["id"])
        output_json = taskinfo['outputUri'].split(self.output_bucket+'/')[1] + task["id"] + "/logs/outputs.json"
        response = self.s3_client.get_object(Bucket=self.output_bucket, Key=output_json)
        content = response['Body'].read().decode("utf-8")
        mapping = json.loads(content)

        if output_name not in mapping:
            raise KeyError(f"Output field '{output_name}' not found in output json file.")
        all_outputs = mapping[output_name]
        if isinstance(all_outputs, list):
            outputs = [c["location"] for c in all_outputs if "location" in c]
            return outputs
        try:
            return all_outputs["location"]
        except KeyError:
            raise KeyError(f"Could not find path for '{output_name}'")

    def get_task_output_filename(self, task, output_name):
        ''' Retrieve the output field of the task and return filename'''
        self.logger.info("TBD: Getting output filename for task %s", task)
        task_output_url = self.get_task_output(task, output_name)
        if isinstance(task_output_url, list):
            task_output_name = [fileurl.split('/')[-1] for fileurl in task_output_url]
        else:
            task_output_name = task_output_url.split('/')[-1]
        return task_output_name

    def get_tasks_by_name(self, project, task_name=None):
        ''' Get a tasks by its name '''
        tasks = []
        runs = self.api.list_runs(name=task_name, runGroupId=project['RunGroupId'])
        for item in runs['items']:
            run = self.api.get_run(id=item['id'])
            tasks.append(run)
        return tasks

    def get_project(self):
        '''
        Since there is no concept of project in Omics, raise an error.
        '''
        raise ValueError("Omics does not support get_project. Use get_project_by_id or get_project_by_name instead.")

    def get_project_by_name(self, project_name):
        ''' Return a dictionary of project to provide RunGroupId tag info for omics jobs '''
        response = self.api.list_run_groups(
            name=project_name, maxResults=100
        )
        if len(response['items'])>0:
            run_group_id = response['items'][0]['id']

            project = {
                'RunGroupId': run_group_id
            }
            return project

        logger.error('Could not find project with name: %s', project_name)
        return {}

    def get_project_by_id(self, project_id):
        ''' Return a dictionary of project to provide RunGroupId tag info for omics jobs'''
        project = {
            'RunGroupId': project_id
        }
        return project

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
        Submit workflow for one sample.
        name: sample ID.
        project: dictionary of {'RunGroupId': 'string'}
        workflow: workflow ID in omics.
        parameters: dictionary of input parameters.

        return omics response for start_run.
        '''
        base_output_path = f"s3://{self.output_bucket}/Project/"
        base_output_path += f"{project['RunGroupId']}/{workflow}/{name.replace(' ','')}/"

        try:
            logger.debug("Starting run for %s", name)
            # TODO: The roleArn should be a parameter to this function, and not hard-coded.
            # Put this in the pipeline_config.py.
            job = self.api.start_run(workflowId=workflow,
                                     workflowType='PRIVATE',
                                     roleArn=self.role_arn,
                                     parameters=parameters,
                                     name=name,
                                     runGroupId=project["RunGroupId"],
                                     tags={"Project": project["RunGroupId"]},
                                     outputUri=base_output_path,
                                     storageType="DYNAMIC")
            logger.info('Started run for %s, RunID: %s',name,job['id'])
            return job
        except botocore.exceptions.ClientError as err:
            logger.error('Could not start run for %s: %s', name, err)
            return None

    def upload_file(self, filename, project, dest_folder=None, destination_filename=None, overwrite=False): # pylint: disable=too-many-arguments
        self.logger.info("Uploading file %s to project %s", filename, project)
        target_bucket = self.output_bucket
        target_filepath = f"Project/{project['RunGroupId']}" + dest_folder
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

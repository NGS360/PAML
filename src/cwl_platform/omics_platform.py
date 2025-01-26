'''
AWS HealthOmics class
'''

import logging
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

        If ~/.aws/credentials or ~/.aws/config does not provide a region, region should be specified in the AWS_DEFAULT_REGION environment variable.
        '''
        self.api = boto3.client('omics',  # TODO - Still needs to provide aws key here
            region_name='us-east-1')
        self.output_bucket = kwargs.get('output_bucket', "bmsrd-ngs-omics/Outputs")
        self.role_arn = kwargs.get('role_arn', "arn:aws:iam::483421617021:role/ngs360-servicerole")
        self.s3_client = boto3.client('s3', # TODO - Still needs to provide aws key here
            region_name='us-east-1')

    def copy_folder(self, source_project, source_folder, destination_project):
        '''
        Do nothing and return reference folder, which should be an S3 path.
        NOTE: Launchers copy the reference folder to the destination project so that everything is co-located.
              However this can cause lots of data duplication in S3.  For now we will just use the reference folder
              until another use-case is identified that we need to copy the data.
        '''
        return source_folder

    def copy_workflow(self, src_workflow, destination_project):
        '''Do nothing and return workflow id'''
        return src_workflow

    def copy_workflows(self, reference_project, destination_project):
        '''Do nothing. This function seems not used in launcher?'''
        pass

    def create_project(self, project_name, project_description, **kwargs):
        ''' Do nothing'''
        pass

    def delete_task(self, task):
        ''' Delete a task/workflow/process '''
        self.logger.info('TBD: Deleting task %s', task)

    @classmethod
    def detect(cls):
        return False

    def get_current_task(self):
        ''' Get the current task '''
        return None

    def get_file_id(self, project, file_path):
        '''Return file s3 path for Omics job input'''
        return file_path

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
        raise ValueError("Not yet implemented")

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
        # Get all files in output folder
        taskinfo = self.api.get_run(id=task["id"])
        outputUri = taskinfo['outputUri'] + task["id"] + "/out/"
        outputUri_files = self._list_file_in_s3(outputUri)

        # Match output_name with existing files in output folder
        if "*" not in output_name:
            output = outputUri + output_name
            if output in outputUri_files:
                return output
            else:
                return None
        else:
            output = []
            prefix=output_name.split('*')[0]
            suffix=output_name.split('*')[-1]
            for output_file in outputUri_files:
                filename = output_file.split('/')[-1]
                if (prefix == "" or filename.startswith(prefix)) and \
                    (suffix == "" or filename.endswith(suffix)):
                    output+=[output_file]
            return output

    def get_task_output_filename(self, task, output_name):
        ''' Retrieve the output field of the task and return filename'''
        self.logger.info("TBD: Getting output filename for task %s", task)
        return None

    def get_tasks_by_name(self, project, task_name):
        ''' Get a tasks by its name '''
        tasks = []
        runs = self.api.list_runs(name=task_name, runGroupId=project['ProjectId'])
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
        ''' Return a dictionary of project to provide project_name tag info for omics jobs '''
        project = {
            'ProjectName': project_name
        }
        return project

    def get_project_by_id(self, project_id):
        ''' Return a dictionary of project to provide project_id tag info for omics jobs'''
        project = {
            'ProjectId': project_id
        }
        return project

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
        project: dictionary of {'ProjectName': 'string'} or {'ProjectId': 'string'}
        workflow: workflow ID in omics.
        parameters: dictionary of input parameters.

        return omics response for start_run.
        '''
        base_output_path = f"s3://{self.output_bucket}/"
        if 'ProjectName' in project:
            base_output_path += f"{project['ProjectName']}/{workflow}/{name}/"
        else:
            base_output_path += f"{project['ProjectId']}/{workflow}/{name}/"

        try:
            logger.debug("Starting run for %s", name)
            # TODO: The roleArn should be a parameter to this function, and not hard-coded.
            # Put this in the pipeline_config.py.
            job = self.api.start_run(workflowId=workflow,
                                     workflowType='PRIVATE',
                                     roleArn=self.role_arn,
                                     parameters=parameters,
                                     name=name,
                                     runGroupId=project["ProjectId"],
                                     tags={"ProjectId": project["ProjectId"]},
                                     outputUri=base_output_path)
            logger.info('Started run for %s, RunID: %s',name,job['id'])
            return job
        except botocore.exceptions.ClientError as err:
            logger.error('Could not start run for %s: %s', name, err)
            return None

    def upload_file_to_project(self, filename, project, dest_folder, destination_filename=None, overwrite=False): # pylint: disable=too-many-arguments
        self.logger.info("TBD: Uploading file %s to project %s", filename, project)
        return None

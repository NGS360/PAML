'''
AWS HealthOmics class
'''

import logging
import boto3
import botocore

from tenacity import retry, wait_fixed

from .base_platform import Platform

logger = logging.getLogger(__name__)

class OmicsPlatform(Platform):
    ''' AWS HealthOmics Platform class '''
    def __init__(self, name):
        super().__init__(name)
        self.api = None
        self.output_bucket = None
        self.role_arn = None

    def connect(self, **kwargs):
        ''' 
        Connect to AWS Omics platform

        If ~/.aws/credentials or ~/.aws/config does not provide a region, region should be specified in the AWS_DEFAULT_REGION environment variable.
        '''
        self.api = boto3.client('omics')
        self.output_bucket = kwargs.get('output_bucket')
        self.role_arn = kwargs.get('role_arn')

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
        taskinfo = self.api.get_run(id=task)
        # TODO: get_run only returns OutputUri. Get file path based on output_name (filename)?
        filename = None
        # TODO: We shouldn't be hard-coding stuff like this.  these functions should be very generic.
        if output_name == 'RecalibratedBAM':
            filename = taskinfo.name + '.bam'
        if filename == None:
            raise ValueError(f"Cannot find output file for: {output_name}")
        return taskinfo['outputUri'] + filename

    def get_task_output_filename(self, task, output_name):
        ''' Retrieve the output field of the task and return filename'''
        self.logger.info("TBD: Getting output filename for task %s", task)
        return None

    def get_tasks_by_name(self, project, task_name):
        ''' Get a tasks by its name '''
        tasks = []
        runs = self.api.list_runs(name=task_name)
        for item in runs['items']:
            run = self.api.get_run(id=item['id'])
            if 'ProjectId' in project:
                if run['tags']['ProjectId'] == project['ProjectId']:
                    tasks.append(run)
            elif 'ProjectName' in project:
                if run['tags']['ProjectName'] == project['ProjectName']:
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

    @retry(wait=wait_fixed(1))
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
                                     tags=project,
                                     outputUri=base_output_path)
            logger.info('Started run for %s, RunID: %s',name,job['id'])
            return job
        except botocore.exceptions.ClientError as err:
            logger.error('Could not start run for %s: %s', name, err)
            return None

    def upload_file_to_project(self, filename, project, dest_folder, destination_filename=None, overwrite=False): # pylint: disable=too-many-arguments
        self.logger.info("TBD: Uploading file %s to project %s", filename, project)
        return None

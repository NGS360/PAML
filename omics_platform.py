'''
AWS HealthOmics class
'''

import logging
import boto3

logger = logging.getLogger(__name__)

class OmicsPlatform():
    ''' AWS HealthOmics Platform class '''
    def __init__(self):
        self.api = None

    def connect(self):
        ''' Connect to AWS Omics platform'''
        self.api = boto3.client('omics')

    def copy_folder(self, reference_project, reference_folder, destination_project):
        '''
        Do nothing and return reference folder, which should be an S3 path.
        '''
        return reference_folder

    def copy_workflow(self, src_workflow, destination_project):
        '''Do nothing and return workflow id'''
        return src_workflow

    def copy_workflows(self, reference_project, destination_project):
        '''Do nothing. This function seems not used in launcher?'''
        pass

    @classmethod
    def detect(cls):
        return False

    def get_file_id(self, project, file_path):
        '''Return file s3 path for Omics job input'''
        return file_path

    def get_folder_id(self, project, folder_path):
        '''
        There is not unique ID for a folder in s3, so just return the folder_path
        The one caveat is that Omics wants trailing slashes on folder paths, so add one.
        '''
        return folder_path + "/"

    def get_task_state(self, task, refresh=False):
        ''' 
        Get status of run by task_id.
        task: A dictionary of omics response from start_run. Includes Run ID, Name, Tags, etc.
        return status of the run (Complete, Failed, Running, Cancelled, Queued).
        '''

        runinfo = self.api.get_run(id=task['id'])
        try:
            jobstatus = runinfo['status']
        except:
            logger.error('No Status information found for job %s. Check job status.',task['id'])
            sys.exit(1)

        if runinfo['status'] == 'COMPLETED':
            return 'Complete'
        if runinfo['status'] == 'FAILED':
            return 'Failed'
        if runinfo['status'] in ['STARTING','RUNNING','STOPPING']:
            return 'Running'
        if runinfo['status'] in ['CANCELLED','DELETED']:
            return 'Cancelled'
        if runinfo['status'] == 'PENDING':
            return 'Queued'

        logger.error('Unknown task state: %s : %s', task['id'], runinfo['status'])
        sys.exit(1)

    def get_task_output(self, task, output_name):
        ''' Retrieve the output field of the task '''
        taskinfo = self.api.get_run(id=task)
        #TODO get_run only returns OutputUri. Get file path based on output_name (filename)?
        filename = None
        if output_name == 'RecalibratedBAM':
            filename = taskinfo.name + '.bam'
        if filename == None:
            raise ValueError(f"Cannot find output file for: {output_name}")
        return taskinfo['outputUri'] + filename

    def get_tasks_by_name(self, project, task_name):
        '''
        Omics do not allow get run by name. A Run ID is required for searching runs.
        # TODO: aws omics list-runs does not support filter by name. Need to get all runs and filter by name.
        Return None to force submitting job without reuse.
        '''
        return None

    def get_project(self):
        '''
        Since there is no concept of project in Omics, raise an error.
        '''
        raise ValueError("Omics does not support project. Use get_project_by_id or get_project_by_name instead.")

    def get_project_by_name(self, project_name):
        ''' Return a dictionary of project to provide project_name tag info for omics jobs'''
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

    def submit_task(self, name, project, workflow, parameters):
        '''
        Submit workflow for one sample.
        name: sample ID.
        project: dictionary of {'ProjectName':'string'} or {'ProjectId':'string'}, used for add run tag.
        workflow: workflow ID in omics.
        parameters: dictionary of input parameters.

        return omics response for start_run.
        '''
        # This current outfilepath will allow 1 invocation of the workflow to overwrite another
        # invocation of the same workflow.
        # TODO: We need a unique output path for each invocation of the workflow.  how are we going to track this? We can't have workflow invocations overwriting each other.
        # Find a space to save output files (outUri)
        if 'ProjectName' in project:
            outfilepath = 's3://bmsrd-ngs-omics/omics_output/'+project['ProjectName']+'/'
        else:
            outfilepath = 's3://bmsrd-ngs-omics/omics_output/'+project['ProjectId']+'/'

        try:
            logger.debug("Starting run for %s", name)
            # TODO: The roleArn should be a parameter to this function, and not hard-coded.  Put this in the pipeline_config.py.
            job = self.api.start_run(workflowId=workflow,
                                     workflowType='PRIVATE',
                                     roleArn='arn:aws:iam::483421617021:role/ngs360-servicerole',
                                     parameters=parameters,
                                     name=name,
                                     tags=project,
                                     outputUri=outfilepath)
            logger.info('Started run for %s, RunID: %s',name,job['id'])
            return job
        except:
            logger.error('Could not start run for %s',name)
            return None

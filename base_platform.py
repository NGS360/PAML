''' Base Platform class '''
from abc import ABC, abstractmethod
import logging

class Platform(ABC):
    ''' abstract Platform class '''
    def __init__(self, name):
        self.logger = logging.getLogger(__name__)
        self.name = name

    @abstractmethod
    def connect(self, **kwargs):
        ''' Connect to the platform '''

    @abstractmethod
    def copy_folder(self, source_project, source_folder, destination_project):
        ''' Copy source folder to destination project '''

    @abstractmethod
    def copy_workflow(self, src_workflow, destination_project):
        '''
        Copy a workflow from one project to another, if a workflow with the same name
        does not already exist in the destination project.

        :param src_workflow: The workflow to copy
        :param destination_project: The project to copy the workflow to
        :return: The workflow that was copied or exists in the destination project
        '''

    @abstractmethod
    def copy_workflows(self, reference_project, destination_project):
        '''
        Copy all workflows from the reference_project to project, IF the workflow (by name) does
        not already exist in the project.

        :param reference_project: The project to copy workflows from
        :param destination_project: The project to copy workflows to
        :return: List of workflows that were copied
        '''

    @abstractmethod
    def delete_task(self, task):
        ''' Delete a task/workflow/process '''

    @classmethod
    def detect(cls):
        ''' Detect platform we are running on '''

    @abstractmethod
    def get_current_task(self):
        ''' Get the current task '''

    @abstractmethod
    def get_file_id(self, project, file_path):
        ''' Get a file id by its full path name '''

    @abstractmethod
    def get_folder_id(self, project, folder_path):
        ''' Get a folder id by its full path name '''

    @abstractmethod
    def get_task_input(self, task, input_name):
        ''' Retrieve the input field of the task '''

    @abstractmethod
    def get_task_state(self, task, refresh=False):
        '''
        Get workflow/task state

        :param task: The task to search for. Task is a dictionary containing a
            container_request_uuid and container dictionary.
        :param refresh: Refresh task state before returning (Default: False)
        :return: The state of the task (Queued, Running, Complete, Failed, Cancelled)
        '''

    @abstractmethod
    def get_task_output(self, task, output_name):
        ''' Retrieve the output field of the task '''

    @abstractmethod
    def get_task_output_filename(self, task, output_name):
        ''' Retrieve the output field of the task and return filename'''

    @abstractmethod
    def get_tasks_by_name(self, project, task_name):
        ''' Get a tasks by its name '''

    @abstractmethod
    def get_project(self):
        ''' Determine what project we are running in '''

    @abstractmethod
    def get_project_by_name(self, project_name):
        ''' Get a project by its name '''

    @abstractmethod
    def get_project_by_id(self, project_id):
        ''' Get a project by its id '''

    def set_logger(self, logger):
        ''' Set the logger '''
        self.logger = logger

    @abstractmethod
    def stage_output_files(self, project, output_files):
        '''
        Stage output files to a project

        :param project: The project to stage files to
        :param output_files: A list of output files to stage
        :return: None
        '''

    @abstractmethod
    def stage_task_output(self, task, project, output_to_export, output_directory_name):
        '''
        Prepare/Copy output files of a task for export.

        For Arvados, copy selected files to output collection/folder.
        For SBG, add OUTPUT tag for output files.

        :param task: Task object to export output files
        :param project: The project to export task outputs
        :param output_to_export: A list of CWL output IDs that needs to be exported
            (for example: ['raw_vcf','annotated_vcf'])
        :param output_directory_name: Name of output folder that output files are copied into
        :return: None
        '''

    @abstractmethod
    def submit_task(self, name, project, workflow, parameters):
        ''' Submit a workflow on the platform '''

    @abstractmethod
    def upload_file_to_project(self, filename, project, dest_folder, destination_filename=None, overwrite=False): # pylint: disable=too-many-arguments
        '''
        Upload a local file to project 
        :param filename: filename of local file to be uploaded.
        :param project: project that the file is uploaded to.
        :param dest_folder: The target path to the folder that file will be uploaded to. None will upload to root.
        :param destination_filename: File name after uploaded to destination folder.
        :param overwrite: Overwrite the file if it already exists.
        :return: ID of uploaded file.
        '''

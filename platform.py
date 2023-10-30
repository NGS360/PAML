''' Base Platform class '''
from abc import ABC, abstractmethod
import logging

class Platform(ABC):
    ''' abstract Platform class '''
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    @abstractmethod
    def connect(self):
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
    def get_file_id(self, project, file_path):
        ''' Get a file id by its full path name '''

    @abstractmethod
    def get_folder_id(self, project, folder_path):
        ''' Get a folder id by its full path name '''

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
    def submit_task(self, name, project, workflow, parameters):
        ''' Submit a workflow on the platform '''

    @abstractmethod
    def upload_file_to_project(self, filename, project, filepath, overwrite=False):
        '''
        Upload a local file to project 
        :param filename: filename of local file to be uploaded.
        :param project: project that the file is uploaded to.
        :param filepath: The target path to the folder that file will be uploaded to. None will upload to root.
        :return: ID of uploaded file.
        '''

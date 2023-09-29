'''
CWL Execution Platform Implementations
'''
from abc import ABC, abstractmethod

from .arvados_platform import ArvadosPlatform
from .sevenbridges_platform import SevenBridgesPlatform
#from .omics_platform import OmicsPlatform

# Move this for a config file
SUPPORTED_PLATFORMS = {
    'Arvados': ArvadosPlatform,
#    'Omics': OmicsPlatform,
    'SevenBridges': SevenBridgesPlatform
}

class Platform(ABC):
    ''' abstract Platform class '''
    @abstractmethod
    def connect(self):
        ''' Connect to the platform '''

    @abstractmethod
    def copy_folder(self, reference_project, reference_folder, destination_project):
        ''' Copy reference folder to destination project '''

    @abstractmethod
    def copy_reference_data(self, reference_project, destination_project):
        '''
        Copy all data from the reference_project to project, IF the data (by name) does not already
        exist in the project.
        '''

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
        Copy all workflows from the reference_project to project, IF the workflow (by name) does not already
        exist in the project.

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

        :param task: The task to search for.  Task is a dictionary containing a container_request_uuid and container dictionary.
        :param refresh: Refresh task state before returning (Default: False)
        :return: The state of the task (Queued, Running, Complete, Failed, Cancelled)
        '''

    @abstractmethod
    def get_task_output(self, task, output_name):
        ''' Retrieve the output field of the task '''

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

    @abstractmethod
    def submit_task(self, name, project, workflow, parameters):
        ''' Submit a workflow on the platform '''

class PlatformFactory():
    ''' PlatformFactory '''

    def __init__(self):
        self._creators = {}
        for platform, creator in SUPPORTED_PLATFORMS.items():
            self._creators[platform] = creator

    def detect_platform(self):
        '''
        Detect what platform we are running on
        '''
        for platform, creator in SUPPORTED_PLATFORMS.items():
            if creator.detect():
                return platform
        raise ValueError("Unable to detect platform")

    def get_platform(self, platform):
        '''
        Create a project type
        '''
        creator = self._creators.get(platform)
        if creator:
            return creator()
        raise ValueError(f"Unknown platform: {platform}")

    def register_platform_type(self, platform, creator):
        '''
        Register a platform with the factory
        '''
        self._creators[platform] = creator

''' Base Platform class '''
from abc import ABC, abstractmethod
import logging

class Platform(ABC):
    ''' abstract Platform class '''
    def __init__(self, name):
        self.logger = logging.getLogger(__name__)
        self.name = name
        self.connected = False

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
    def download_file(self, file, dest_folder):
        '''
        Download a file to a local directory

        :param fileid: File to download
        :param dest_folder: Destination folder to download file to
        :return: Name of local file downloaded or None
        '''

    @abstractmethod
    def export_file(self, file, bucket_name, prefix):
        '''
        Use platform specific functionality to copy a file from a platform to an S3 bucket.

        :param file: File to export
        :param bucket_name: S3 bucket name
        :param prefix: Destination S3 folder to export file to, path/to/folder
        :return: s3 file path or None
        '''

    @abstractmethod
    def get_current_task(self):
        ''' Get the current task '''

    # TODO: These file objects should be abstracted to a common File object interface
    @abstractmethod
    def get_file_id(self, project, file_path):
        ''' Get a file id by its full path name '''

    @abstractmethod
    def get_file(self, file_id):
        ''' Get a file by its id '''
        
    @abstractmethod
    def get_file_name(self, file):
        ''' Get a file name by its object or id '''

    @abstractmethod
    def get_files(self, project, filter):
        '''
        Retrieve files in a project matching the filter criteria

        :param project: Project to search for files
        :param filter: Dictionary containing filter criteria
            {
                'name': 'file_name',
                'prefix': 'file_prefix',
                'suffix': 'file_suffix',
                'folder': 'folder_name',
                'recursive': True/False
            }
        :return: List of file objects matching filter criteria
        '''

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
    def get_task_outputs(self, task):
        ''' Return a list of output fields of the task '''

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

    @abstractmethod
    def rename_file(self, fileid, new_filename):
        '''
        Rename a file to new_filename.

        :param file: File ID to rename
        :param new_filename: str of new filename
        '''

    @abstractmethod
    def roll_file(self, project, file_name):
        '''
        Roll (find and rename) a file in a project.

        :param project: The project the file is located in
        :param file_name: The filename that needs to be rolled
        '''

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
        DEPRECATED: Use stage_output_files instead

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
    def submit_task(self, name, project, workflow, parameters, executing_settings=None):
        '''
        Submit a workflow on the platform
        :param name: Name of the task to submit
        :param project: Project to submit the task to
        :param workflow: Workflow to submit
        :param parameters: Parameters for the workflow
        :param executing_settings: {use_spot_instance: True/False}
        :return: Task object or None
        '''

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

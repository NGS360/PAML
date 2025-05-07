"""
GA4GH WES Platform class

This module implements the Platform abstract base class using the GA4GH Workflow Execution Service (WES) API.
The WES API provides a standard way to submit and manage workflows across different workflow execution systems.
"""

import os
import json
import logging
import time
import uuid
import requests
from urllib.parse import urljoin

from .base_platform import Platform


class WESTask:
    """
    WES Task class to encapsulate task functionality
    """

    def __init__(self, run_id, name, state=None, outputs=None, inputs=None):
        self.run_id = run_id
        self.name = name
        self.state = state
        self.outputs = outputs or {}
        self.inputs = inputs or {}

    def to_dict(self):
        """Convert to dictionary"""
        return {
            "run_id": self.run_id,
            "name": self.name,
            "state": self.state,
            "outputs": self.outputs,
            "inputs": self.inputs,
        }

    @classmethod
    def from_dict(cls, task_dict):
        """Convert from dictionary"""
        return cls(
            task_dict["run_id"],
            task_dict["name"],
            task_dict.get("state"),
            task_dict.get("outputs"),
            task_dict.get("inputs"),
        )


class WESPlatform(Platform):
    """GA4GH WES Platform class"""

    # WES API state mapping to Platform states
    STATE_MAP = {
        "UNKNOWN": "Unknown",
        "QUEUED": "Queued",
        "INITIALIZING": "Queued",
        "RUNNING": "Running",
        "PAUSED": "Running",
        "COMPLETE": "Complete",
        "EXECUTOR_ERROR": "Failed",
        "SYSTEM_ERROR": "Failed",
        "CANCELED": "Cancelled",
        "CANCELING": "Cancelled",
    }

    def __init__(self, name):
        """
        Initialize WES Platform
        """
        super().__init__(name)
        self.logger = logging.getLogger(__name__)
        self.api_endpoint = None
        self.auth_token = None
        self.projects = {}  # Map project names to project objects
        self.workflows = {}  # Map workflow names to workflow objects
        self.files = {}  # Map file paths to file objects

    def connect(self, **kwargs):
        """
        Connect to the WES API

        :param kwargs: Connection parameters
            - api_endpoint: WES API endpoint URL
            - auth_token: Authentication token for the WES API
        """
        self.api_endpoint = kwargs.get("api_endpoint")
        self.auth_token = kwargs.get("auth_token")

        if not self.api_endpoint:
            raise ValueError("WES API endpoint URL is required")

        # Test connection by getting service info
        try:
            response = self._make_request("GET", "service-info")
            self.logger.info(
                f"Connected to WES API: {response.get('workflow_type_versions', {})}"
            )
            self.connected = True
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to WES API: {e}")
            self.connected = False
            return False

    def _make_request(self, method, path, data=None, files=None, params=None):
        """
        Make a request to the WES API

        :param method: HTTP method (GET, POST, etc.)
        :param path: API path
        :param data: Request data
        :param files: Files to upload
        :param params: Query parameters
        :return: Response JSON
        """
        # Ensure path doesn't start with a slash to avoid urljoin issues
        if path.startswith("/"):
            path = path[1:]

        # Make sure the API endpoint ends with a slash for proper joining
        endpoint = self.api_endpoint
        if not endpoint.endswith("/"):
            endpoint = endpoint + "/"

        url = urljoin(endpoint, path)
        headers = {}

        if self.auth_token:
            headers["Authorization"] = f"Bearer {self.auth_token}"

        response = requests.request(
            method=method,
            url=url,
            headers=headers,
            json=data,
            files=files,
            params=params,
        )

        response.raise_for_status()

        if response.content:
            return response.json()
        return {}

    # File methods
    def copy_folder(self, source_project, source_folder, destination_project):
        """
        Copy source folder to destination project

        Note: WES API doesn't have a direct concept of folders, so this is a no-op
        """
        self.logger.warning("WES API doesn't support folder operations directly")
        return None

    def download_file(self, file, dest_folder):
        """
        Download a file to a local directory

        :param file: File ID to download
        :param dest_folder: Destination folder to download file to
        :return: Name of local file downloaded or None
        """
        if not file or not dest_folder:
            return None

        # In WES context, file might be a URL
        if file.startswith("http"):
            filename = os.path.basename(file)
            dest_path = os.path.join(dest_folder, filename)

            response = requests.get(file, stream=True)
            response.raise_for_status()

            with open(dest_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            return dest_path
        else:
            self.logger.error(f"Unsupported file format: {file}")
            return None

    def export_file(self, file, bucket_name, prefix):
        """
        Use platform specific functionality to copy a file from a platform to an S3 bucket.

        :param file: File to export
        :param bucket_name: S3 bucket name
        :param prefix: Destination S3 folder to export file to, path/to/folder
        :return: s3 file path or None
        """
        self.logger.warning("WES API doesn't support direct S3 export")
        return None

    def get_file_id(self, project, file_path):
        """
        Get a file id by its full path name

        Note: WES API doesn't have a direct concept of files, so this returns the path
        """
        return file_path

    def get_files(self, project, filters=None):
        """
        Retrieve files in a project matching the filter criteria

        Note: WES API doesn't have a direct concept of files, so this returns an empty list

        :param project: Project to search for files
        :param filters: Dictionary containing filter criteria
        :return: List of tuples (file path, file object) matching filter criteria
        """
        self.logger.warning("WES API doesn't support listing files")
        return []

    def get_folder_id(self, project, folder_path):
        """
        Get a folder id by its full path name

        Note: WES API doesn't have a direct concept of folders, so this returns None
        """
        self.logger.warning("WES API doesn't support folder operations")
        return None

    def rename_file(self, fileid, new_filename):
        """
        Rename a file to new_filename.

        Note: WES API doesn't have a direct concept of files, so this is a no-op
        """
        self.logger.warning("WES API doesn't support file operations")
        return None

    def roll_file(self, project, file_name):
        """
        Roll (find and rename) a file in a project.

        Note: WES API doesn't have a direct concept of files, so this is a no-op
        """
        self.logger.warning("WES API doesn't support file operations")
        return None

    def stage_output_files(self, project, output_files):
        """
        Stage output files to a project

        Note: WES API doesn't have a direct concept of files, so this is a no-op
        """
        self.logger.warning("WES API doesn't support file staging operations")
        return None

    def upload_file(
        self,
        filename,
        project,
        dest_folder=None,
        destination_filename=None,
        overwrite=False,
    ):
        """
        Upload a local file to project

        Note: WES API doesn't have a direct concept of files, so this returns the filename

        :param filename: filename of local file to be uploaded.
        :param project: project that the file is uploaded to.
        :param dest_folder: The target path to the folder that file will be uploaded to. None will upload to root.
        :param destination_filename: File name after uploaded to destination folder.
        :param overwrite: Overwrite the file if it already exists.
        :return: ID of uploaded file.
        """
        self.logger.warning("WES API doesn't support file upload operations directly")
        return filename

    # Task/Workflow methods
    def copy_workflow(self, src_workflow, destination_project):
        """
        Copy a workflow from one project to another

        Note: WES API doesn't have a direct concept of projects, so this returns the workflow
        """
        return src_workflow

    def copy_workflows(self, reference_project, destination_project):
        """
        Copy all workflows from the reference_project to project

        Note: WES API doesn't have a direct concept of projects, so this returns an empty list
        """
        return []

    def get_workflows(self, project):
        """
        Get workflows in a project

        Note: WES API doesn't have a direct concept of projects, so this returns an empty list
        """
        return []

    def delete_task(self, task):
        """
        Delete a task/workflow/process

        :param task: WESTask object
        :return: True if successful, False otherwise
        """
        if not task or not hasattr(task, "run_id"):
            return False

        try:
            self._make_request("DELETE", f"runs/{task.run_id}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to delete task: {e}")
            return False

    def get_current_task(self):
        """
        Get the current task

        Note: WES API doesn't have a concept of current task, so this returns None
        """
        return None

    def get_task_cost(self, task):
        """
        Return task cost

        Note: WES API doesn't provide cost information, so this returns None
        """
        return None

    def get_task_input(self, task, input_name):
        """
        Retrieve the input field of the task

        :param task: WESTask object
        :param input_name: Name of the input field
        :return: Value of the input field or None
        """
        if not task or not hasattr(task, "inputs"):
            return None

        return task.inputs.get(input_name)

    def get_task_state(self, task, refresh=False):
        """
        Get workflow/task state

        :param task: WESTask object
        :param refresh: Refresh task state before returning (Default: False)
        :return: The state of the task (Queued, Running, Complete, Failed, Cancelled)
        """
        if not task or not hasattr(task, "run_id"):
            return "Unknown"

        if refresh:
            try:
                response = self._make_request("GET", f"runs/{task.run_id}")
                wes_state = response.get("state", "UNKNOWN")
                task.state = self.STATE_MAP.get(wes_state, "Unknown")
                task.outputs = response.get("outputs", {})
            except Exception as e:
                self.logger.error(f"Failed to refresh task state: {e}")
                return "Unknown"

        return task.state or "Unknown"

    def get_task_output(self, task, output_name):
        """
        Retrieve the output field of the task

        :param task: WESTask object
        :param output_name: Name of the output field
        :return: Value of the output field or None
        """
        if not task or not hasattr(task, "outputs"):
            return None

        return task.outputs.get(output_name)

    def get_task_outputs(self, task):
        """
        Return a list of output fields of the task

        :param task: WESTask object
        :return: Dictionary of output fields
        """
        if not task or not hasattr(task, "outputs"):
            return {}

        return task.outputs

    def get_task_output_filename(self, task, output_name):
        """
        Retrieve the output field of the task and return filename
        NOTE: This method is deprecated as of v0.2.5 of PAML.  Will be removed in v1.0.

        :param task: WESTask object
        :param output_name: Name of the output field
        :return: Filename of the output field or None
        """
        output = self.get_task_output(task, output_name)
        if not output:
            return None

        # If output is a URL, extract the filename
        if isinstance(output, str) and (
            output.startswith("http") or output.startswith("file:")
        ):
            return os.path.basename(output)

        return str(output)

    def get_tasks_by_name(self, project, task_name=None):
        """
        Get all processes/tasks in a project with a specified name

        :param project: The project to search
        :param task_name: The name of the process to search for (if None return all tasks)
        :return: List of tasks
        """
        try:
            params = {}
            if task_name:
                params["name"] = task_name

            response = self._make_request("GET", "runs", params=params)
            tasks = []

            for run in response.get("runs", []):
                task = WESTask(
                    run_id=run.get("run_id"),
                    name=run.get("name", ""),
                    state=self.STATE_MAP.get(run.get("state", "UNKNOWN"), "Unknown"),
                )
                tasks.append(task)

            return tasks
        except Exception as e:
            self.logger.error(f"Failed to get tasks: {e}")
            return []

    def stage_task_output(self, task, project, output_to_export, output_directory_name):
        """
        DEPRECATED: Use stage_output_files instead

        Prepare/Copy output files of a task for export.

        Note: WES API doesn't have a direct concept of files, so this is a no-op
        """
        self.logger.warning("WES API doesn't support file staging operations")
        return None

    def submit_task(self, name, project, workflow, parameters, execution_settings=None):
        """
        Submit a workflow on the platform

        :param name: Name of the task to submit
        :param project: Project to submit the task to (not used in WES)
        :param workflow: Workflow to submit (URL or file path to the workflow)
        :param parameters: Parameters for the workflow
        :param execution_settings: {use_spot_instance: True/False} (not used in WES)
        :return: WESTask object or None
        """
        if not workflow:
            self.logger.error("Workflow is required")
            return None

        # Prepare the request data
        workflow_url = workflow
        workflow_type = "CWL"  # Default to CWL
        workflow_type_version = "v1.0"  # Default to v1.0

        # Check if workflow is a file path or URL
        if os.path.exists(workflow):
            # For WES, we need to upload the workflow file
            workflow_type = "CWL"  # Assuming CWL, adjust as needed
            with open(workflow, "rb") as f:
                workflow_content = f.read()

            files = {
                "workflow_attachment": (os.path.basename(workflow), workflow_content)
            }
            workflow_url = os.path.basename(workflow)
        else:
            files = None

        # Prepare the request data
        data = {
            "workflow_params": json.dumps(parameters),
            "workflow_type": workflow_type,
            "workflow_type_version": workflow_type_version,
            "workflow_url": workflow_url,
            "tags": {"name": name},
        }

        try:
            response = self._make_request("POST", "runs", data=data, files=files)
            run_id = response.get("run_id")

            if not run_id:
                self.logger.error("Failed to submit task: No run_id returned")
                return None

            # Create a WESTask object
            task = WESTask(run_id=run_id, name=name, state="Queued", inputs=parameters)

            return task
        except Exception as e:
            self.logger.error(f"Failed to submit task: {e}")
            return None

    # Project methods
    def create_project(self, project_name, project_description, **kwargs):
        """
        Create a project

        Note: WES API doesn't have a concept of projects, so this creates a virtual project

        :param project_name: Name of the project
        :param project_description: Description of the project
        :param kwargs: Additional arguments for creating a project
        :return: Project object
        """
        project = {
            "id": str(uuid.uuid4()),
            "name": project_name,
            "description": project_description,
        }
        self.projects[project_name] = project
        return project

    def delete_project_by_name(self, project_name):
        """
        Delete a project on the platform

        Note: WES API doesn't have a concept of projects, so this removes the virtual project
        """
        if project_name in self.projects:
            del self.projects[project_name]
            return True
        return False

    def get_project(self):
        """
        Determine what project we are running in

        Note: WES API doesn't have a concept of projects, so this returns None
        """
        return None

    def get_project_by_name(self, project_name):
        """
        Get a project by its name

        Note: WES API doesn't have a concept of projects, so this returns the virtual project
        """
        if project_name in self.projects:
            return self.projects[project_name]

        # Create a new project if it doesn't exist
        return self.create_project(project_name, f"Virtual project for {project_name}")

    def get_project_by_id(self, project_id):
        """
        Get a project by its id

        Note: WES API doesn't have a concept of projects, so this returns the virtual project
        """
        for project in self.projects.values():
            if project["id"] == project_id:
                return project
        return None

    def get_project_cost(self, project):
        """
        Return project cost

        Note: WES API doesn't provide cost information, so this returns None
        """
        return None

    def get_project_users(self, project):
        """
        Return a list of user objects associated with a project

        Note: WES API doesn't have a concept of projects, so this returns an empty list
        """
        return []

    def get_projects(self):
        """
        Get list of all projects

        Note: WES API doesn't have a concept of projects, so this returns the virtual projects
        """
        return list(self.projects.values())

    # User methods
    def add_user_to_project(self, platform_user, project, permission):
        """
        Add a user to a project on the platform

        Note: WES API doesn't have a concept of projects, so this is a no-op
        """
        self.logger.warning("WES API doesn't support user management")
        return None

    def get_user(self, user):
        """
        Get a user object from their (platform) user id or email address

        Note: WES API doesn't have a concept of users, so this returns None
        """
        return None

    @classmethod
    def detect(cls):
        """
        Detect platform we are running on

        Note: This method checks if we're running in a WES environment
        """
        # Check if WES API endpoint is set in environment variables
        wes_api_endpoint = os.environ.get("WES_API_ENDPOINT")
        if wes_api_endpoint:
            return True
        return False

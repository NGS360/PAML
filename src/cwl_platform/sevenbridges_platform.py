'''
SevenBridges Platform class
'''
import os
import logging
import sevenbridges
from sevenbridges.errors import SbgError
from sevenbridges.http.error_handlers import rate_limit_sleeper, maintenance_sleeper, general_error_sleeper

from .base_platform import Platform

class SevenBridgesPlatform(Platform):
    ''' SevenBridges Platform class '''
    def __init__(self, name):
        '''
        Initialize SevenBridges Platform 
        
        We need either a session id or an api_config object to connect to SevenBridges
        '''
        super().__init__(name)
        self.api = None
        self.api_config = None
        self._session_id = os.environ.get('SESSION_ID')
        self.logger = logging.getLogger(__name__)

        self.api_endpoint = 'https://api.sbgenomics.com/v2'
        self.token = 'dummy'

        if not self._session_id:
            if os.path.exists(os.path.expanduser("~") + '/.sevenbridges/credentials') is True:
                self.api_config = sevenbridges.Config(profile='default')
            else:
                raise ValueError('No SevenBridges credentials found')

    # File methods
    def _find_or_create_path(self, project, path):
        """
        Go down virtual folder path, creating missing folders.

        :param path: virtual folder path as string
        :param project: `sevenbridges.Project` entity
        :return: `sevenbridges.File` entity of type folder to which path indicates
        """
        folders = path.split("/")
        # If there is a leading slash, remove it
        if not folders[0]:
            folders = folders[1:]

        parent = self.api.files.query(project=project, names=[folders[0]])
        if len(parent) == 0:
            parent = None
            for folder in folders:
                parent = self.api.files.create_folder(
                    name=folder,
                    parent=parent,
                    project=None if parent else project
                )
        elif not parent[0].is_folder():
            raise FileExistsError(f"File with name {parent[0].name} already exists!")
        else:
            parent = parent[0]
            for folder in folders[1:]:
                nested = [x for x in parent.list_files().all() if x.name == folder]
                if not nested:
                    parent = self.api.files.create_folder(
                        name=folder,
                        parent=parent,
                    )
                else:
                    parent = nested[0]
                    if not parent.is_folder():
                        raise FileExistsError(
                            f"File with name {parent.name} already exists!")
        return parent

    def _list_all_files(self, files=None, project=None):
        """
        Returns a list of files (files within folders are included).
        Provide a list of file objects OR a project object/id
        :param files: List of file (and folder) objects
        :param project: Project object or id
        :return: Flattened list of files (no folder objects)
        """
        if not files and not project:
            self.logger.error("Provide either a list of files OR a project object/id")
            return []

        if project and not files:
            self.logger.info("Recursively listing all files in project %s", project)
            files = self.api.files.query(project=project, limit=100).all()
        file_list = []
        for file in files:
            if not file.is_folder():
                file_list.append(file)
            elif file.is_folder():
                child_nodes = self.api.files.get(id=file.id).list_files().all()
                file_list.extend(self._list_all_files(files=child_nodes))
        return file_list

    def _list_files_in_folder(self, project=None, folder=None, recursive=False):
        """
        List all file contents of a folder.
        
        Project object and folder path both required
        Folder path format "folder_1/folder_2"
        
        Option to list recursively, eg. return file objects only
        :param project: `sevenbridges.Project` entity
        :param folder: Folder string name
        :param recursive: Boolean
        :return: List of file objects
        """
        parent = self._find_or_create_path(
            path=folder,
            project=project
        )
        if recursive:
            file_list = self._list_all_files(
                files=[parent]
            )
        else:
            file_list = self.api.files.get(id=parent.id).list_files().all()
        return file_list

    def copy_folder(self, source_project, source_folder, destination_project):
        '''
        Copy reference folder to destination project

        :param source_project: The source project
        :param source_folder: The source folder
        :param destination_project: The destination project
        :return: The destination folder
        '''
        # get the reference project folder
        #sbg_reference_folder = self._find_or_create_path(reference_project, reference_folder)
        # get the destination project folder
        sbg_destination_folder = self._find_or_create_path(destination_project, source_folder)
        # Copy the files from the reference project to the destination project
        reference_files = self._list_files_in_folder(project=source_project, folder=source_folder)
        destination_files = list(self._list_files_in_folder(project=destination_project, folder=source_folder))
        for reference_file in reference_files:
            if reference_file.is_folder():
                source_folder_rec = os.path.join(source_folder, reference_file.name)
                self.copy_folder(source_project, source_folder_rec, destination_project)
            if reference_file.name not in [f.name for f in destination_files]:
                if reference_file.is_folder():
                    continue
                reference_file.copy_to_folder(parent=sbg_destination_folder)
        return sbg_destination_folder

    def download_file(self, file, dest_folder):
        """
        Download a file to a local directory
        :param file: SevenBridges file id (or object) to download
        :param dest_folder: Destination folder to download file to
        :return: Name of local file downloaded or None
        """
        # If file is a str, assume its a file id, else is a file object
        if isinstance(file, str):
            file = self.api.files.get(id=file)
        dest_file = os.path.join(dest_folder, os.path.basename(file.name))
        file.download(dest_file)
        return dest_file

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
        # If file is a str, assume its a file id, else is a file object
        if isinstance(file, str):
            file = self.api.files.get(id=file)

        volume = None
        volume_name = bucket_name.replace("-", "_")
        for v in self.api.volumes.query().all():
            if v.name == volume_name:
                volume = v
                break
        if not volume:
            self.logger.error("Volume %s not found", volume_name)
            return None

        export = self.api.exports.submit_export(
            file=file, volume=volume, location=f"{prefix}/{file.name}", copy_only=True
        )
        return export

    def get_file_id(self, project, file_path):
        '''
        Get the file id for a file in a project

        :param project: The project to search for the file
        :param file_path: The path to the file
        :return: The file id
        '''
        if file_path.startswith('http'):
            raise ValueError(f'File ({file_path}) path cannot be a URL')

        if file_path.startswith('s3://'):
            file_path = file_path.split('/')[-1]

        path_parts = list(filter(None, file_path.rsplit("/", 1)))
        if len(path_parts) == 1 :
            file_name = path_parts[0]
            file_list = self.api.files.query(
                project=project,
                names=[file_path],
                limit=100
            ).all()
        else:
            folder = path_parts[0]
            file_name = path_parts[1]
            file_list = self._list_files_in_folder(
                project=project,
                folder=folder
            )
        file_list = [x for x in file_list if x.name == file_name]
        if file_list:
            return file_list[0].id

        raise ValueError(f"File not found in specified folder: {file_path}")

    def _get_folder_contents(self, path, folder, filters):
        '''
        Recusivelly returns all the files in a directory and subdirectories in a SevenBridges project.

        :param path: path of file
        :param folder: SBG Folder reference
        :return: List of tuples (file path, file object) matching filter criteria
        '''
        files = []
        for f in folder.list_files().all():
            if f.type == 'folder':
                files += self._get_folder_contents(f"{path}/{f.name}", f, filters)
            else:
                files.append((f"{path}/{f.name}", f))
        return files

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
        files = []
        for f in self.api.files.query(project, limit=1000, cont_token='init').all():
            if f.type == 'folder':
                files += self._get_folder_contents(f'/{f.name}', f, filters)
            else:
                files.append((f'/{f.name}', f))

        if filters:
            matching_files = []
            # Now that we have all the files, let's apply the filters
            for filepath, file in files:
                filename = os.path.basename(filepath)
                if 'name' in filters and filters['name'] != filename:
                    continue
                if 'prefix' in filters and not filename.startswith(filters['prefix']):
                    continue
                if 'suffix' in filters and not filename.endswith(filters['suffix']):
                    continue
                if 'folder' in filters and filters['folder'] != os.path.dirname(filepath):
                    continue
                matching_files.append((filepath, file))
        else:
            matching_files = files

        return matching_files

    def get_folder_id(self, project, folder_path):
        '''
        Get the folder id in a project

        :param project: The project to search for the file
        :param file_path: The path to the folder
        :return: The file id of the folder
        '''
        folder_tree = folder_path.split("/")
        if not folder_tree[0]:
            folder_tree = folder_tree[1:]
        parent = None
        for folder in folder_tree:
            if parent:
                file_list = self.api.files.query(
                    parent=parent, names=[folder], limit=100).all()
            else:
                file_list = self.api.files.query(
                    project=project, names=[folder], limit=100).all()
            for file in file_list:
                if file.name == folder:
                    parent = file.id
                    break
        return parent

    def rename_file(self, fileid, new_filename):
        '''
        Rename a file to new_filename.

        :param file: File ID to rename
        :param new_filename: str of new filename
        '''
        file = self.api.files.get(id=fileid)
        file.name = new_filename
        file.save()

    def roll_file(self, project, file_name):
        ''' 
        Roll (find and rename) a file in a project.

        :param project: The project the file is located in
        :param file_name: The filename that needs to be rolled
        '''
        # 1. Get the file reference of the file to be renamed
        sbg_file = self.api.files.query(project=project, names=[file_name])
        if not sbg_file: # Do nothing if file not exists
            return
        sbg_file = sbg_file[0]

        # 2. Determine what new filename to be used
        ## Get a list of all files with file_name in the files name...
        file_list = self.api.files.query(project=project).all()
        existing_filenames = []
        for x in file_list:
            if file_name in x.name:
                existing_filenames += [x.name]
        i = 1
        ## "Roll" the filename by adding a number to the filename
        ## and make sure it doesn't already exist in existing_filenames
        new_filename = "_" + str(i) + "_" + file_name
        while new_filename in existing_filenames:
            i += 1
            new_filename = "_" + str(i) + "_" + file_name

        # 3. Rename the file
        self.rename_file(sbg_file.id, new_filename)

    def _add_tag_to_file(self, target_file, newtag):
        ''' Add a tag to a file '''
        if newtag not in target_file.tags:
            target_file.tags += [newtag]
            target_file.save()
        if hasattr(target_file,'secondary_files') and target_file.secondary_files is not None:
            secondary_files = target_file.secondary_files
            for secfile in secondary_files:
                if isinstance(secfile,sevenbridges.models.file.File) and secfile.tags and newtag not in secfile.tags:
                    secfile.tags += [newtag]
                    secfile.save()

    def _add_tag_to_folder(self,target_folder, newtag):
        ''' Add a tag to all files in a folder '''
        folder = self.api.files.get(id=target_folder.id)
        allfiles = folder.list_files()
        for file in allfiles:
            if not isinstance(file,sevenbridges.models.file.File):
                continue
            if file.type == "file":
                self._add_tag_to_file(file, newtag)
            elif file.type == "folder":
                self._add_tag_to_folder(file, newtag)

    def stage_output_files(self, project, output_files):
        '''
        Stage output files to a project

        :param project: The project to stage files to
        :param output_files: A list of output files to stage
        :return: None
        '''
        self.logger.warning("stage_output_files to be deprecated in future release.")
        for output_file in output_files:
            self.logger.info("Staging output file %s -> %s", output_file['source'], output_file['destination'])
            outfile = self.api.files.get(id=output_file['source'])
            if isinstance(outfile, sevenbridges.models.file.File):
                if outfile.type == "file":
                    self._add_tag_to_file(outfile, "OUTPUT")
                elif outfile.type == "folder":
                    self._add_tag_to_folder(outfile, "OUTPUT")
            if isinstance(outfile, list):
                for file in outfile:
                    if isinstance(file, sevenbridges.models.file.File):
                        if file.type == "file":
                            self._add_tag_to_file(file, "OUTPUT")
                        elif file.type == "folder":
                            self._add_tag_to_folder(file, "OUTPUT")

    def upload_file(self, filename, project, dest_folder=None, destination_filename=None, overwrite=False): # pylint: disable=too-many-arguments
        '''
        Upload a local file to project 
        :param filename: filename of local file to be uploaded.
        :param project: project that the file is uploaded to.
        :param dest_folder: The target path to the folder that file will be uploaded to. None will upload to root.
        :param destination_filename: File name after uploaded to destination folder.
        :param overwrite: Overwrite the file if it already exists.
        :return: ID of uploaded file.
        '''
        if destination_filename is None:
            destination_filename = filename.split('/')[-1]

        if dest_folder is not None:
            if dest_folder[-1] == '/': # remove slash at the end
                dest_folder = dest_folder[:-1]
            parent_folder = self._find_or_create_path(project, dest_folder)
            parent_folder_id = parent_folder.id
        else:
            parent_folder_id = None

        # check if file already exists on SBG
        existing_file = self.api.files.query(names=[destination_filename],
                                             parent=parent_folder_id,
                                             project=None if parent_folder_id else project)

        # upload file if overwrite is True or if file does not exists
        if overwrite or len(existing_file) == 0:
            update_state = self.api.files.upload(filename, overwrite=overwrite,
                                                 parent=parent_folder_id,
                                                 file_name=destination_filename,
                                                 project=None if parent_folder_id else project,
                                                 wait=True)
            return None if update_state.status == 'FAILED' else update_state.result().id

        # return file id if file already exists
        return existing_file[0].id

    # Task/Workflow methods
    def copy_workflow(self, src_workflow, destination_project):
        '''
        Copy a workflow from one project to another, if a workflow with the same name
        does not already exist in the destination project.

        :param src_workflow: The workflow to copy
        :param destination_project: The project to copy the workflow to
        :return: The workflow that was copied or exists in the destination project
        '''
        app = self.api.apps.get(id=src_workflow)
        wf_name = app.name

        # Get the existing (if any) workflow in the destination project with the same name as the
        # reference workflow
        for existing_workflow in destination_project.get_apps().all():
            if existing_workflow.name == wf_name:
                return existing_workflow.id
        return app.copy(project=destination_project.id).id

    def copy_workflows(self, reference_project, destination_project):
        '''
        Copy all workflows from the reference_project to project,
        IF the workflow (by name) does not already exist in the project.

        :param reference_project: The project to copy workflows from
        :param destination_project: The project to copy workflows to
        :return: List of workflows that were copied
        '''
        # Get list of reference workflows
        reference_workflows = reference_project.get_apps().all()
        destination_workflows = list(destination_project.get_apps().all())
        # Copy the workflow if it doesn't already exist in the destination project
        for workflow in reference_workflows:
            # NOTE This is also copies archived apps.  How do we filter those out?  Asked Nikola, waiting for response.
            if workflow.name not in [wf.name for wf in destination_workflows]:
                destination_workflows.append(workflow.copy(project=destination_project.id))
        return destination_workflows

    def get_workflows(self, project):
        '''
        Get workflows in a project

        :param: Platform Project
        :return: List of workflows
        '''
        return list(project.get_apps().all())

    def delete_task(self, task: sevenbridges.Task):
        ''' Delete a task/workflow/process '''
        task.delete()

    def get_current_task(self) -> sevenbridges.Task:
        ''' Get the current task '''

        task_id = os.environ.get('TASK_ID')
        if not task_id:
            raise ValueError("ERROR: Environment variable TASK_ID not set.")
        self.logger.info("TASK_ID: %s", task_id)
        task = self.api.tasks.get(id=task_id)
        return task

    def get_task_cost(self, task):
        task_cost = 0.0
        try:
            task_cost = task.price.amount
        except:
            pass
        return task_cost

    def get_task_input(self, task: sevenbridges.Task, input_name):
        ''' Retrieve the input field of the task '''
        input_value = task.inputs[input_name]
        if isinstance(input_value, sevenbridges.File):
            return input_value.id
        if (isinstance(input_value, list) and
            all(isinstance(element, sevenbridges.File) for element in input_value)):
            return [element.id for element in input_value]
        return input_value

    def get_task_state(self, task: sevenbridges.Task, refresh=False):
        ''' Get workflow/task state '''
        sbg_state = {
            'COMPLETED': 'Complete',
            'FAILED': 'Failed',
            'QUEUED': 'Queued',
            'RUNNING': 'Running',
            'ABORTED': 'Cancelled',
            'DRAFT': 'Cancelled'
        }
        if refresh:
            task = self.api.tasks.get(id=task.id)
        return sbg_state[task.status]

    def get_task_output(self, task: sevenbridges.Task, output_name):
        '''
        Retrieve the output field of the task

        :param task: The task object to retrieve the output from
        :param output_name: The name of the output to retrieve
        '''
        # Refresh the task.  If we don't the outputs fields may be empty.
        task = self.api.tasks.get(id=task.id)
        self.logger.debug("Getting output %s from task %s", output_name, task.name)
        self.logger.debug("Task has outputs: %s", task.outputs)
        if isinstance(task.outputs[output_name], list):
            return [output.id for output in task.outputs[output_name]]
        if isinstance(task.outputs[output_name], sevenbridges.File):
            return task.outputs[output_name].id
        return task.outputs[output_name]

    def get_task_outputs(self, task: sevenbridges.Task):
        ''' Return a list of output fields of the task '''
        return list(task.outputs.keys())

    def get_task_output_filename(self, task: sevenbridges.Task, output_name):
        '''
        Retrieve the output field of the task and return filename
        NOTE: This method is deprecated as of v0.2.5 of PAML.  Will be removed in v1.0.
        '''
        task = self.api.tasks.get(id=task.id)
        if output_name in task.outputs:
            if isinstance(task.outputs[output_name], list):
                return [output.name for output in task.outputs[output_name]]
            if isinstance(task.outputs[output_name], sevenbridges.File):
                return task.outputs[output_name].name
        raise ValueError(f"Output {output_name} does not exist for task {task.name}.")

    def get_tasks_by_name(self, project, task_name=None): # -> list(sevenbridges.Task):
        '''
        Get all processes/tasks in a project with a specified name
        :param project: The project to search
        :param task_name: The name of the process to search for (if None return all tasks)
        :return: List of tasks
        '''
        tasks = []
        for task in self.api.tasks.query(project=project).all():
            if task_name is None or task.name == task_name:
                tasks.append(task)
        return tasks

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
        self.logger.warning("stage_task_output to be DEPRECATED, use stage_output_files instead.")
        task = self.api.tasks.get(id=task.id)
        alloutputs = task.outputs
        for output_id in alloutputs:
            if output_id not in output_to_export:
                continue
            outfile=alloutputs[output_id]
            if isinstance(outfile,sevenbridges.models.file.File):
                if outfile.type == "file":
                    self._add_tag_to_file(outfile, "OUTPUT")
                elif outfile.type == "folder":
                    self._add_tag_to_folder(outfile, "OUTPUT")
            if isinstance(outfile,list):
                for file in outfile:
                    if isinstance(file,sevenbridges.models.file.File):
                        if file.type == "file":
                            self._add_tag_to_file(file, "OUTPUT")
                        elif file.type == "folder":
                            self._add_tag_to_folder(file, "OUTPUT")

    def submit_task(self, name, project, workflow, parameters, execution_settings=None):
        '''
        Submit a workflow on the platform
        :param name: Name of the task to submit
        :param project: Project to submit the task to
        :param workflow: Workflow to submit
        :param parameters: Parameters for the workflow
        :param executing_settings: {use_spot_instance: True/False}
        :return: Task object or None
        '''
        def set_file_metadata(file, metadata):
            ''' Set metadata on a file '''
            if file and file.metadata != metadata:
                file.metadata = metadata
                file.save()

        def check_metadata(entry):
            if isinstance(entry, dict):
                if 'metadata' in entry and entry['class'] == 'File':
                    sbgfile = None
                    if 'path' in entry:
                        sbgfile = self.api.files.get(id=entry['path'])
                    elif 'location' in entry:
                        sbgfile = self.api.files.get(id=entry['location'])
                    set_file_metadata(sbgfile, entry['metadata'])

        use_spot_instance = execution_settings.get('use_spot_instance', True) if execution_settings else True
        sbg_execution_settings = { 'use_memoization': True}

        # This metadata code will come out as part of the metadata removal effort.
        for i in parameters:
            # if the parameter type is an array/list
            if isinstance(parameters[i], list):
                for j in parameters[i]:
                    check_metadata(j)

            ## if the parameter type is a regular file
            check_metadata(parameters[i])

        self.logger.debug("Submitting task (%s) with parameters: %s", name, parameters)
        try:
            task = self.api.tasks.create(name=name, project=project, app=workflow,inputs=parameters,
                                        interruptible=use_spot_instance,
                                        execution_settings=sbg_execution_settings)
        except sevenbridges.errors.BadRequest as e:
            self.logger.error("Error submitting task: %s", e)
            return None

        task.run()
        return task

    ### Project methods
    def create_project(self, project_name, project_description, **kwargs):
        '''
        Create a project
        
        :param project_name: Name of the project
        :param project_description: Description of the project
        :param kwargs: Additional arguments for creating a project
        :return: Project object
        '''
        project = self.api.projects.create(name=project_name,
                                           description=project_description,
                                           settings={'use_interruptible_instances':False})
        return project

    def delete_project_by_name(self, project_name):
        '''
        Delete a project on the platform 
        '''
        project = self.get_project_by_name(project_name)
        if project:
            project.delete()

    def get_project(self):
        ''' Determine what project we are running in '''
        task_id = os.environ.get('TASK_ID')
        if not task_id:
            return None

        try:
            task = self.api.tasks.get(id=task_id)
            return self.api.projects.get(id=task.project)
        except sevenbridges.errors.SbgError:
            return None

    def get_project_cost(self, project):
        # 1.  Get a list of tasks from the project
        tasks = self.get_tasks_by_name(project)

        # 2.  Iterate over tasks and sum up total cost
        total_cost = 0.0
        for task in tasks:
            total_cost += self.get_task_cost(task)
        return total_cost

    def get_project_by_name(self, project_name):
        ''' Get a project by its name '''
        projects = self.api.projects.query(name=project_name)
        if projects:
            return projects[0]
        return None

    def get_project_by_id(self, project_id):
        ''' Get a project by its id '''
        return self.api.projects.get(project_id)

    def get_project_users(self, project):
        ''' Return a list of user objects associated with a project '''
        return list(project.get_members())

    def get_projects(self):
        ''' Get list of all projects '''
        projects = None
        try:
            projects = list(self.api.projects.query().all())
        except SbgError as err:
            self.logger.error(err)
        return projects

    ### User Methods
    def add_user_to_project(self, platform_user, project, permission):
        """
        Add a user to a project on the platform
        :param platform_user: platform user (from get_user)
        :param project: platform project
        :param permission: permission (permission="read|write|execute|admin")
        """
        user_permissions = {
            'read': False,
            'write': False,
            'copy': False,
            'execute': False,
            'admin': False
        }
        if permission == 'read':
            user_permissions['read'] = True
        elif permission == 'write':
            user_permissions['read'] = True
            user_permissions['write'] = True
        elif permission == 'execute':
            user_permissions['read'] = True
            user_permissions['write'] = True
            user_permissions['execute'] = True
        elif permission == 'admin':
            user_permissions['read'] = True
            user_permissions['write'] = True
            user_permissions['execute'] = True
            user_permissions['admin'] = True
        # If the user already exists in the project, an exception will be raised.
        try:
            project.add_member(user=platform_user, permissions=user_permissions)
        except sevenbridges.errors.SbgError:
            pass

    def get_user(self, user):
        """
        Get a user object from the (platform) user id or email address

        :param user: user id or email address
        :return: User object or None
        """
        divisions = self.api.divisions.query().all()
        for division in divisions:
            platform_users = self.api.users.query(division=division, limit=500).all()
            for platform_user in platform_users:
                if user.lower() in platform_user.username.lower() or platform_user.email.lower() == user.lower():
                    return platform_user
        return None

    # Other methods
    def connect(self, **kwargs):
        ''' Connect to Sevenbridges '''
        self.api_endpoint = kwargs.get('api_endpoint', self.api_endpoint)
        self.token = kwargs.get('token', self.token)

        if self._session_id:
            self.api = sevenbridges.Api(url=self.api_endpoint, token=self.token,
                                        error_handlers=[rate_limit_sleeper,
                                                        maintenance_sleeper,
                                                        general_error_sleeper],
                                        advance_access=True)
            self.api._session_id = self._session_id  # pylint: disable=protected-access
        else:
            self.api = sevenbridges.Api(config=self.api_config,
                                        error_handlers=[rate_limit_sleeper,
                                                        maintenance_sleeper,
                                                        general_error_sleeper],
                                        advance_access=True)
        self.connected = True

    @classmethod
    def detect(cls):
        '''
        Determine if we are running on this platform
        '''
        session_id = os.environ.get('SESSION_ID')
        if session_id:
            return True
        return False

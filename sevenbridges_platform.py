'''
SevenBridges Platform class
'''
import os
import logging
import sevenbridges
from sevenbridges.http.error_handlers import rate_limit_sleeper, maintenance_sleeper, general_error_sleeper

class SevenBridgesPlatform():
    ''' SevenBridges Platform class '''
    def __init__(self, api_endpoint='https://bms-api.sbgenomics.com/v2', token='dummy'):
        '''
        Initialize SevenBridges Platform 
        
        We need either a session id or an api_config object to connect to SevenBridges
        '''
        self.api = None
        self.api_config = None
        self._session_id = os.environ.get('SESSION_ID')

        self.api_endpoint = api_endpoint
        self.token = token

        if not self._session_id and os.path.exists(os.path.expanduser("~") + '/.sevenbridges/credentials') is True:
            self.api_config = sevenbridges.Config(profile='default')
        else:
            raise ValueError('No SevenBridges credentials found')

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
            logging.error("Folder cannot be created with the same name as an "
                        "existing file")
            raise FileExistsError(f"File with name {parent[0].name} already exists!")
        else:
            parent = parent[0]
            for folder in folders[1:]:
                nested = [x for x in parent.list_files() if x.name == folder]
                if not nested:
                    parent = self.api.files.create_folder(
                        name=folder,
                        parent=parent,
                    )
                else:
                    parent = nested[0]
                    if not parent.is_folder():
                        logging.error(
                            "Folder cannot be created with the same name as an "
                            "existing file")
                        raise FileExistsError(
                            f"File with name {parent.name} already exists!")
        return parent

    def _get_folder_contents(self, folder, tag, path):
        '''
        Recusivelly returns all the files in a directory and subdirectories in a SevenBridges project.
        :param folder: SB Project reference
        :param tag: tag to filter by
        :param path: path of file
        :return: List of files and paths
        '''
        files = []
        for file in folder.list_files().all():
            if file.type == 'folder':
                files += self._get_folder_contents(file, tag, f"{path}/{file.name}")
            else:
                if tag is None:
                    files.append((file, path))
                elif file.tags and (tag in file.tags):
                    files.append((file, path))
        return files

    def _get_project_files(self, sb_project_id, tag=None, name=None):
        '''
        Get all (named)) files (with tag) from a project

        :param sb_project_id: SevenBridges project id
        :param tag: Tag to filter by (can only specify one, but should be changed)
        :param name: Name(s) of file to filter by
        :return: List of SevenBridges file objects and their paths e.g.
                 [(file1, path1), (file2, path2)]
        '''
        project = self.api.projects.get(sb_project_id)
        sb_files = []
        limit = 1000
        if name is None:
            for file in self.api.files.query(project, cont_token='init', limit=limit).all():
                if file.type == 'folder':
                    sb_files += self._get_folder_contents(file, tag, f'/{file.name}')
                else:
                    if tag is None:
                        sb_files.append((file, '/'))
                    elif file.tags and (tag in file.tags):
                        sb_files.append((file, '/'))
        else:
            for i in range(0, len(name), limit):
                files_chunk = name[i:i+limit]
                sb_files_chunk = self.api.files.query(project=sb_project_id, names=[files_chunk])
                for file in sb_files_chunk:
                    sb_files.append((file, '/'))
        return sb_files

    def _list_all_files(self, files=None, project=None):
        """
        Returns a list of files (files within folders are included).
        Provide a list of file objects OR a project object/id
        :param files: List of file (and folder) objects
        :param project: Project object or id
        :return: Flattened list of files (no folder objects)
        """
        if not files and not project:
            logging.error("Provide either a list of files OR a project object/id")
            return []

        if project and not files:
            logging.info("Recursively listing all files in project %s", project)
            files = self.api.files.query(project=project,
                                    limit=100).all()
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

    def connect(self):
        ''' Connect to Sevenbridges '''
        if self._session_id:
            self.api = sevenbridges.Api(url=self.api_endpoint, token=self.token,
                                        error_handlers=[rate_limit_sleeper,
                                                        maintenance_sleeper,
                                                        general_error_sleeper],
                                        advance_access=True)
            # We were doing this before, but I'm not convinced we need to.
            #self.api._session_id = self._session_id
        else:
            self.api = sevenbridges.Api(config=self.api_config,
                                        error_handlers=[rate_limit_sleeper,
                                                        maintenance_sleeper,
                                                        general_error_sleeper],
                                        advance_access=True)

    def copy_folder(self, reference_project, reference_folder, destination_project):
        '''
        Copy reference folder to destination project

        :param reference_project: The reference project
        :param reference_folder: The reference folder
        :param destination_project: The destination project
        :return: The destination folder
        '''
        # get the reference project folder
        #sbg_reference_folder = self._find_or_create_path(reference_project, reference_folder)
        # get the destination project folder
        sbg_destination_folder = self._find_or_create_path(destination_project, reference_folder)

        # Copy the files from the reference project to the destination project
        reference_files = self._list_files_in_folder(project=reference_project, folder=reference_folder)
        destination_files = list(self._list_files_in_folder(project=destination_project, folder=reference_folder))
        for reference_file in reference_files:
            if reference_file.name not in [f.name for f in destination_files]:
                reference_file.copy_to_folder(parent=sbg_destination_folder)
        return sbg_destination_folder

    def copy_reference_data(self, reference_project, destination_project):
        '''
        Copy all data from the reference_project to project, IF the data (by name) does not already
        exist in the project.
        '''
        # Get all the files from the source project
        reference_tag = "reference_files"
        source_files = self._get_project_files(reference_project.id, tag=reference_tag)

        for source_file, folder_path in source_files:
            parent = self._find_or_create_path(destination_project, folder_path)
            files = self._list_files_in_folder(folder=parent)
            if source_file.name not in [f.name for f in files]:
                source_file.copy_to_folder(parent=parent)

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

    def delete_task(self, task):
        ''' Delete a task/workflow/process '''
        task.delete()

    @classmethod
    def detect(cls):
        '''
        Determine if we are running on this platform
        '''
        session_id = os.environ.get('SESSION_ID')
        if session_id:
            return True
        return False

    def get_file_id(self, project, file_path):
        '''
        Get the file id for a file in a project
        :param project: The project to search for the file
        :param file_path: The path to the file
        :return: The file id
        '''
        if file_path.startswith('http'):
            raise ValueError('File path cannot be a URL')

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

        raise ValueError("File not found in specified folder")

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

    def get_task_state(self, task, refresh=False):
        ''' Get workflow/task state '''
        sbg_state = {
            'COMPLETED': 'Complete',
            'FAILED': 'Failed',
            'QUEUED': 'Queued',
            'RUNNING': 'Running',
            'ABORTED': 'Cancelled'
        }
        if refresh:
            task = self.api.tasks.get(id=task.id)
        return sbg_state[task.status]

    def get_task_output(self, task, output_name):
        '''
        Retrieve the output field of the task

        :param task: The task object to retrieve the output from
        :param output_name: The name of the output to retrieve
        '''
        task = self.api.tasks.get(id=task.id)
        alloutputs = task.outputs
        if output_name in alloutputs:
            outputfile = alloutputs[output_name]
            if outputfile:
                return outputfile.id
        raise ValueError(f"Output {output_name} does not exist for task {task.name}.")

    def get_tasks_by_name(self, project, process_name):
        ''' Get a process by its name '''
        tasks = []
        for task in self.api.tasks.query(project=project).all():
            if task.name == process_name:
                tasks.append(task)
        return tasks

    def get_project(self):
        ''' Determine what project we are running in '''
        task_id = os.environ.get('TASK_ID')
        task = self.api.tasks.get(id=task_id)
        return self.api.projects.get(id=task.project)

    def get_project_by_name(self, project_name):
        ''' Get a project by its name '''
        projects = self.api.projects.query(name=project_name)
        if projects:
            return projects[0]
        return None

    def get_project_by_id(self, project_id):
        ''' Get a project by its id '''
        return self.api.projects.get(project_id)

    def submit_task(self, name, project, workflow, parameters):
        ''' Submit a workflow on the platform '''
        task = self.api.tasks.create(name=name, project=project, app=workflow, inputs=parameters,
                                     execution_settings={'use_elastic_disk': True, 'use_memoization': True})
        task.run()
        return task

'''
Arvados Platform class
'''
import collections
import json
import logging
import os
import pathlib
import re
import subprocess
import tempfile

import chardet

import googleapiclient
import smart_open

import arvados

from .base_platform import Platform

def open_file_with_inferred_encoding(filename, mode='r'):
    ''' Try to auto-detecting file encoding and open file with that encoding '''
    with open(filename, 'rb') as file:
        rawdata = file.read()
    result = chardet.detect(rawdata)
    encoding = result['encoding']
    if encoding is None:
        raise ValueError("Failed to detect file encoding.")
    return open(filename, mode, encoding=encoding)

def find_collection_file_path(keep_uri):
    '''
    This is a helper function that given a keep_uri, return the collection_uuid and file path
    :param keep_uri: Keep URI (e.g. keep:asdf-asdf-asdf/some/file.txt)
    return: [collection uuid, file path]
    '''
    if keep_uri.startswith('keep:'):
        keep_uri = keep_uri[5:]
    keep_components = keep_uri.split('/')
    keep_uuid = keep_components[0]
    file_path = ''
    if len(keep_components) > 1:
        file_path = '/'.join(keep_components[1:])
    return keep_uuid, file_path

class ArvadosTask():
    '''
    Arvados Task class to encapsulate task functionality to mimick SevenBridges task class
    '''
    def __init__(self, container_request, container):
        self.container_request = container_request
        self.container = container

    def to_dict(self):
        ''' Convert to dictionary '''
        return {
            'container_request': self.container_request,
            'container': self.container
        }

    @classmethod
    def from_dict(cls, task_dict):
        ''' Convert from dictionary '''
        return cls(task_dict['container_request'], task_dict['container'])

# custom JSON encoder - this is needed if we want to dump this to a file i.e. save state
class ArvadosTaskEncoder(json.JSONEncoder):
    ''' Arvados Task Encoder class '''
    def default(self, o):
        ''' Default '''
        if isinstance(o, ArvadosTask):
            return o.to_dict()
        return super().default(o)

class StreamFileReader(arvados.arvfile.ArvadosFileReader):
    ''' This class replaces the deprecated StreamFileReader that existed in Arvados prior to 2.7.4 '''
    class _NameAttribute(str):
        # The Python file API provides a plain .name attribute.
        # Older SDK provided a name() method.
        # This class provides both, for maximum compatibility.
        def __call__(self):
            return self

    def __init__(self, arvadosfile):
        super().__init__(arvadosfile)
        self.name = self._NameAttribute(arvadosfile.name)

    def stream_name(self):
        return super().stream_name().lstrip("./")

# custom JSON decoder
def arvados_task_decoder(obj):
    ''' Arvados Task Decoder class '''
    if 'container_request' in obj and 'container' in obj:
        return ArvadosTask(obj['container_request'], obj['container'])
    return obj

class ArvadosPlatform(Platform):
    ''' Arvados Platform class '''
    def __init__(self, name):
        super().__init__(name)
        self.api_config = arvados.config.settings()
        self.api = None
        self.keep_client = None
        self.logger = logging.getLogger(__name__)

    def _all_files(self, root_collection):
        '''
        all_files yields tuples of (collection path, file object) for
        each file in the collection.
        '''

        stream_queue = collections.deque([pathlib.PurePosixPath('.')])
        while stream_queue:
            stream_path = stream_queue.popleft()
            subcollection = root_collection.find(str(stream_path))
            for name, item in subcollection.items():
                if isinstance(item, arvados.arvfile.ArvadosFile):
                    yield StreamFileReader(item)
                else:
                    stream_queue.append(stream_path / name)

    def _get_files_list_in_collection(self, collection_uuid, subdirectory_path=None):
        '''
        Get list of files in collection, if subdirectory_path is provided, return only files in that subdirectory.

        :param collection_uuid: uuid of the collection
        :param subdirectory_path: subdirectory path to filter files in the collection
        :return: list of files in the collection
        '''
        the_col = arvados.collection.CollectionReader(manifest_locator_or_text=collection_uuid)
        file_list = self._all_files(the_col)
        if subdirectory_path:
            return [fl for fl in file_list if os.path.basename(fl.stream_name()) == subdirectory_path]
        return list(file_list)

    def _load_cwl_output(self, task: ArvadosTask):
        '''
        Load CWL output from task
        '''
        cwl_output_collection = arvados.collection.Collection(task.container_request['output_uuid'],
                                                              api_client=self.api,
                                                              keep_client=self.keep_client)
        if len(cwl_output_collection.items()) == 0:
            return None

        cwl_output = None
        with cwl_output_collection.open('cwl.output.json') as cwl_output_file:
            cwl_output = json.load(cwl_output_file)
        return cwl_output

    def _lookup_collection_from_foldername(self, project, folder):
        '''
        Give a folder path, return the corresponding collection in the project.
        
        :param project: The arvados project
        :param folder: The "file path" of the folder e.g. /rootfolder
        :return: Collection object for folder
        '''
        # 1. Get the source collection
        # The first element of the source_folder path is the name of the collection.
        if folder.startswith('/'):
            collection_name = folder.split('/')[1]
        else:
            collection_name = folder.split('/')[0]

        # Look up the collect
        search_result = self.api.collections().list(filters=[
            ["owner_uuid", "=", project["uuid"]],
            ["name", "=", collection_name]
            ]).execute()
        if len(search_result['items']) > 0:
            self.logger.debug("Found source collection %s in project %s", collection_name, project["uuid"])
            return search_result['items'][0]

        self.logger.error("Source collection %s not found in project %s", collection_name, project["uuid"])
        return None

    # File methods
    def copy_folder(self, source_project, source_folder, destination_project):
        '''
        Copy folder to destination project

        :param source_project: The source project
        :param source_folder: The source folder
        :param destination_project: The destination project
        :return: The destination folder or None if not found
        '''
        self.logger.debug("Copying folder %s from project %s to project %s",
                     source_folder, source_project["uuid"], destination_project["uuid"])

        source_collection = self._lookup_collection_from_foldername(source_project, source_folder)
        if not source_collection:
            return None

        # 2. Get the destination project collection
        destination_collection = self._lookup_collection_from_foldername(destination_project, source_folder)
        if not destination_collection:
            destination_collection = self.api.collections().create(body={
                "owner_uuid": destination_project["uuid"],
                "name": source_collection['name'],
                "description": source_collection["description"],
                "preserve_version":True}).execute()

        # Copy the files from the reference project to the destination project
        self.logger.debug("Get list of files in source collection, %s", source_collection["uuid"])
        source_files = self._get_files_list_in_collection(source_collection["uuid"])
        self.logger.debug("Getting list of files in destination collection, %s", destination_collection["uuid"])
        destination_files = self._get_files_list_in_collection(destination_collection["uuid"])

        source_collection = arvados.collection.Collection(source_collection["uuid"])
        target_collection = arvados.collection.Collection(destination_collection['uuid'])

        for source_file in source_files:
            source_path = f"{source_file.stream_name()}/{source_file.name()}"
            if source_path not in [f"{destination_file.stream_name()}/{destination_file.name()}"
                                for destination_file in destination_files]:
                target_collection.copy(source_path, target_path=source_path, source_collection=source_collection)
        target_collection.save()

        self.logger.debug("Done copying folder.")
        return destination_collection

    # @override
    def get_files(self, project, filters=None):
        """
        Retrieve files in a project matching the filter criteria.

        :param project: Project to search for files
        :param filters: Dictionary containing filter criteria
            {
                'name': 'file_name',
                'prefix': 'file_prefix',
                'suffix': 'file_suffix',
                'folder': 'folder_name',  # Here, a root folder is the name of the collection.
                'recursive': True/False
            }
        :return: List of tuples (file path, file object) matching filter criteria
        """
        # Iterate over all collections and find collections matching filter criteria.
        collection_filter = [["owner_uuid", "=", project["uuid"]]]
        if filters and "folder" in filters:
            folder = filters['folder'].lstrip('/')
            collection_filter.append(["name", "=", folder])
        matching_collections = []
        search_result = self.api.collections().list(filters=collection_filter).execute()
        if len(search_result['items']) > 0:
            matching_collections = search_result['items']

        # Iterate over the collections an find files matching filter criteria
        matching_files = []
        for _, collection in enumerate(matching_collections):
            collection_name = collection['name']
            files = self._get_files_list_in_collection(
                collection["uuid"]
            )
            for f in files:
                if filters and 'name' in filters:
                    if filters['name'] == f.name:
                        matching_files.append((f'/{collection_name}/{f.name}', f))
                else:
                    matching_files.append((f'/{collection_name}/{f.name}', f))
        self.logger.debug("Return list of %d files", len(matching_files))
        return matching_files

    def download_file(self, file, dest_folder):
        """
        Download a file to a local directory
        :param file: File to download e.g. keep:asdf-asdf-asdf/some/file.txt
        :param dest_folder: Destination folder to download file to
        :return: Name of local file downloaded or None
        """
        dest_file = os.path.join(dest_folder, os.path.basename(file))
        collection_uuid, file_path = find_collection_file_path(file)

        c = arvados.collection.CollectionReader(manifest_locator_or_text=collection_uuid, api_client=self.api)
        with c.open(file_path, "rb") as reader:
            with open(dest_file, "wb") as writer:
                content = reader.read(128*1024)
                while content:
                    writer.write(content)
                    content = reader.read(128*1024)
        return dest_file

    def export_file(self, file, bucket_name, prefix):
        """
        Use platform specific functionality to copy a file from a platform to an S3 bucket.
        :param file: File to export, keep:<collection_uuid>/<file_path>
        :param bucket_name: S3 bucket name
        :param prefix: Destination S3 folder to export file to, path/to/folder
        :return: s3 file path or None
        """
        collection_uuid, key = find_collection_file_path(file)
        c = arvados.collection.CollectionReader(
            manifest_locator_or_text=collection_uuid, api_client=self.api
        )
        # If the file is in the keep collection
        if key in c:
            with c.open(key, "rb") as reader:
                with smart_open.smart_open(
                    f"s3://{bucket_name}/{prefix}/{key}", "wb"
                ) as writer:
                    content = reader.read(128 * 1024)
                    while content:
                        writer.write(content)
                        content = reader.read(128 * 1024)

    def get_file_id(self, project, file_path):
        '''
        Get a file id by its full path name

        :param project: The project to search
        :param file_path: The full path of the file to search for
        :return: The file id or None if not found
        '''
        if file_path.startswith('http') or file_path.startswith('keep'):
            return file_path

        # Get the collection
        # file_path is assumed to a full path name, starting with a '/'.
        # the first folder in the path is the name of the collection.
        folder_tree = file_path.split('/')
        if not folder_tree[0]:
            folder_tree = folder_tree[1:]

        # The first folder is the name of the collection.
        collection_name = folder_tree[0]
        search_result = self.api.collections().list(filters=[
            ["owner_uuid", "=", project["uuid"]],
            ["name", "=", collection_name]
            ]).execute()
        if len(search_result['items']) > 0:
            collection = search_result['items'][0]
        else:
            raise ValueError(f"Collection {collection_name} not found in project {project['uuid']}")

        # Do we need to check for the file in the collection?  That could add a lot of overhead to query the collection
        # for the file.  Lets see if this comes up before implementing it.
        return f"keep:{collection['portable_data_hash']}/{'/'.join(folder_tree[1:])}"

    def get_folder_id(self, project, folder_path):
        '''
        Get the folder id in a project

        :param project: The project to search for the file
        :param file_path: The path to the folder
        :return: The file id of the folder
        '''
        # The first folder is the name of the collection.
        collection_name, folder_path = os.path.split(folder_path)
        collection_name = collection_name.lstrip("/")
        search_result = self.api.collections().list(filters=[
            ["owner_uuid", "=", project["uuid"]],
            ["name", "=", collection_name]
            ]).execute()
        if len(search_result['items']) > 0:
            collection = search_result['items'][0]
        else:
            return None
        return f"keep:{collection['uuid']}/{folder_path}"

    def rename_file(self, fileid, new_filename):
        '''
        Rename a file to new_filename.

        :param file: File ID to rename
        :param new_filename: str of new filename
        '''
        collection_uuid = fileid.split('keep:')[1].split('/')[0]
        filepath = fileid.split(collection_uuid+'/')[1]
        if len(filepath.split('/'))>1:
            newpath = '/'.join(filepath.split('/')[:-1]+[new_filename])
        else:
            newpath = new_filename
        collection = arvados.collection.Collection(collection_uuid, api_client=self.api)
        collection.copy(filepath, newpath)
        collection.remove(filepath, recursive=True)
        collection.save()

    def roll_file(self, project, file_name):
        '''
        Roll (find and rename) a file in a project.

        :param project: The project the file is located in
        :param file_name: The filename that needs to be rolled
        '''
        # Each run of a workflow will have a unique output collection, hence there will be no
        # name conflicts.

    def stage_output_files(self, project, output_files):
        '''
        Stage output files to a project

        :param project: The project to stage files to
        :param output_files: A list of output files to stage
        :return: None
        '''
        # Get the output collection with name of output_directory_name
        output_collection_name = 'results'
        search_result = self.api.collections().list(filters=[
            ["owner_uuid", "=", project["uuid"]],
            ["name", "=", output_collection_name]
            ]).execute()
        if len(search_result['items']) > 0:
            outputs_collection = search_result['items'][0]
        else:
            outputs_collection = self.api.collections().create(body={
                "owner_uuid": project["uuid"],
                "name": output_collection_name}).execute()
        outputs_collection = arvados.collection.Collection(outputs_collection['uuid'], api_client=self.api)

        with outputs_collection.open("sources.txt", "a+") as sources:
            copied_outputs = [n.strip() for n in sources.readlines()]

            for output_file in output_files:
                self.logger.info("Staging output file %s -> %s", output_file['source'], output_file['destination'])
                if output_file['source'] in copied_outputs:
                    continue

                # Get the source collection
                #keep:asdf-asdf-asdf/some/file.txt
                source_collection_uuid = output_file['source'].split(':')[1].split('/')[0]
                source_file = '/'.join(output_file['source'].split(':')[1].split('/')[1:])
                source_collection = arvados.collection.Collection(source_collection_uuid, api_client=self.api)

                # Copy the file
                outputs_collection.copy(source_file, target_path=output_file['destination'],
                            source_collection=source_collection, overwrite=True)
                sources.write(output_file['source'] + "\n") # pylint: disable=E1101

        try:
            outputs_collection.save()
        except googleapiclient.errors.HttpError as exc:
            self.logger.error("Failed to save output files: %s", exc)

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

        if dest_folder is None:
            self.logger.error("Must provide a collection name for Arvados file upload.")
            return None

        # trim slash at beginning and end
        folder_tree = dest_folder.split('/')
        try:
            if not folder_tree[0]:
                folder_tree = folder_tree[1:]
            if not folder_tree[-1]:
                folder_tree = folder_tree[:-1]
            collection_name = folder_tree[0]
        except IndexError:
            self.logger.error("Must provide a collection name for Arvados file upload.")
            return None

        # Get the destination collection
        search_result = self.api.collections().list(filters=[
            ["owner_uuid", "=", project["uuid"]],
            ["name", "=", collection_name]
            ]).execute()
        if len(search_result['items']) > 0:
            destination_collection = search_result['items'][0]
        else:
            destination_collection = self.api.collections().create(body={
                "owner_uuid": project["uuid"],
                "name": collection_name}).execute()

        target_collection = arvados.collection.Collection(destination_collection['uuid'])

        if destination_filename is None:
            destination_filename = filename.split('/')[-1]

        if len(folder_tree) > 1:
            target_filepath = '/'.join(folder_tree[1:]) + '/' + destination_filename
        else:
            target_filepath = destination_filename

        if overwrite or target_collection.find(target_filepath) is None:
            with open_file_with_inferred_encoding(filename) as local_file:
                local_content = local_file.read()
            with target_collection.open(target_filepath, 'w') as arv_file:
                arv_file.write(local_content) # pylint: disable=no-member
            target_collection.save()
        return f"keep:{destination_collection['uuid']}/{target_filepath}"

    # Task/Workflow methods
    def copy_workflow(self, src_workflow, destination_project):
        '''
        Copy a workflow from one project to another, if a workflow with the same name
        does not already exist in the destination project.

        :param src_workflow: The workflow to copy
        :param destination_project: The project to copy the workflow to
        :return: The workflow that was copied or exists in the destination project
        '''
        self.logger.debug("Copying workflow %s to project %s", src_workflow, destination_project["uuid"])
        # Get the workflow we want to copy
        try:
            workflow = self.api.workflows().get(uuid=src_workflow).execute()
        except arvados.errors.ApiError:
            self.logger.error("Source workflow %s not found", src_workflow)
            return None

        wf_name = workflow["name"]
        # Check if there is a git version at the end, and if so, strip it
        result = re.search(r' \(.*\)$', wf_name)
        # If the git hasn is present, strip it.
        if result:
            wf_name = wf_name[0:result.start()]
        self.logger.debug("Source workflow name: %s", wf_name)

        # Get the existing (if any) workflow in the destination project with the same name as the
        # reference workflow
        existing_workflows = self.api.workflows().list(filters=[
            ["owner_uuid", "=", destination_project["uuid"]],
            ["name", "like", f"{wf_name}%"]
            ]).execute()
        if len(existing_workflows["items"]):
            self.logger.debug("Workflow %s already exists in project %s", wf_name, destination_project["uuid"])
            # Return existing matching workflow
            return existing_workflows["items"][0]

        # Workflow does not exist in project, so copy it
        self.logger.debug("Workflow %s does not exist in project %s, copying", wf_name, destination_project["uuid"])
        workflow['owner_uuid'] = destination_project['uuid']
        del workflow['uuid']
        copied_workflow = self.api.workflows().create(body=workflow).execute()
        self.logger.debug("Copied workflow %s to project %s", wf_name, destination_project["uuid"])
        return copied_workflow

    def copy_workflows(self, reference_project, destination_project):
        '''
        Copy all workflows from the reference_project to project,
        IF the workflow (by name) does not already exist in the project.
        '''
        # Get list of reference workflows
        reference_workflows = self.get_workflows(reference_project)
        destination_workflows = self.get_workflows(destination_project)
        # Copy the workflow if it doesn't already exist in the destination project
        for workflow in reference_workflows["items"]:
            if workflow["name"] not in [workflow["name"] for workflow in destination_workflows["items"]]:
                workflow['owner_uuid'] = destination_project["uuid"]
                del workflow['uuid']
                destination_workflows.append(self.api.workflows().create(body=workflow).execute())
        return destination_workflows

    def get_workflows(self, project):
        '''
        Get workflows in a project

        :param: Platform Project
        :return: List of workflows
        '''
        workflows = self.api.workflows().list(filters=[
            ["owner_uuid", "=", project["uuid"]]
            ]).execute()
        return workflows["items"]

    def delete_task(self, task: ArvadosTask):
        ''' Delete a task/workflow/process '''
        self.api.container_requests().delete(uuid=task.container_request["uuid"]).execute()

    def get_current_task(self) -> ArvadosTask:
        '''
        Get the current task
        :return: ArvadosTask object
        '''

        try:
            current_container = self.api.containers().current().execute()
        except arvados.errors.ApiError as exc:
            raise ValueError("Current task not associated with a container") from exc
        request = self.api.container_requests().list(filters=[
                ["container_uuid", "=", current_container["uuid"]]
            ]).execute()
        if 'items' in request and len(request['items']) > 0:
            return ArvadosTask(request['items'][0], current_container)
        raise ValueError("Current task not associated with a container")

    def get_task_input(self, task, input_name):
        ''' Retrieve the input field of the task '''
        if input_name in task.container_request['properties']['cwl_input']:
            input_field = task.container_request['properties']['cwl_input'][input_name]
            if 'location' in input_field:
                return input_field['location']
            return input_field
        raise ValueError(f"Could not find input {input_name} in task {task.container_request['uuid']}")

    def get_task_state(self, task: ArvadosTask, refresh=False):
        '''
        Get workflow/task state

        :param project: The project to search
        :param task: The task to search for. Task is an ArvadosTask containing a container_request_uuid and
            container dictionary.
        :return: The state of the task (Queued, Running, Complete, Failed, Cancelled)
        '''
        if refresh:
            # On newly submitted jobs, we'll only have a container_request, uuid.
            # pylint gives a warning that avvados.api is not callable, when in fact it is.
            task.container_request = arvados.api().container_requests().get( # pylint: disable=not-callable
                uuid = task.container_request['uuid']
            ).execute()
            task.container = arvados.api().containers().get(uuid = task.container_request['container_uuid']).execute() # pylint: disable=not-callable

        if task.container['exit_code'] == 0:
            return 'Complete'
        if task.container['exit_code'] == 1:
            return 'Failed'
        if task.container['state'] == 'Running':
            return 'Running'
        if task.container['state'] == 'Cancelled':
            return 'Cancelled'
        if task.container['state'] in ['Locked', 'Queued']:
            return 'Queued'
        raise ValueError(f"TODO: Unknown task state: {task.container['state']}")

    def get_task_output(self, task: ArvadosTask, output_name):
        ''' Retrieve the output field of the task '''
        cwl_output = self._load_cwl_output(task)

        if cwl_output is None:
            return None
        if not output_name in cwl_output:
            return None

        output_field = cwl_output[output_name]

        if output_field is None:
            return None

        if isinstance(output_field, list):
            # If the output is a list, return a list of file locations
            output_files = []
            for output in output_field:
                if 'location' in output:
                    output_file = output['location']
                    output_files.append(f"keep:{task.container_request['output_uuid']}/{output_file}")
            return output_files

        if 'location' in output_field:
            # If the output is a single file, return the file location
            output_file = cwl_output[output_name]['location']
            return f"keep:{task.container_request['output_uuid']}/{output_file}"

        return None

    def get_task_outputs(self, task):
        ''' Return a list of output fields of the task '''
        cwl_output = self._load_cwl_output(task)
        return list(cwl_output.keys())

    def get_task_output_filename(self, task: ArvadosTask, output_name):
        '''
        Retrieve the output field of the task and return filename
        NOTE: This method is deprecated as of v0.2.5 of PAML.  Will be removed in v1.0.
        '''
        self.logger.warning("get_task_output_filename to be DEPRECATED (as of v0.2.5), use get_task_output instead.")
        cwl_output_collection = arvados.collection.Collection(task.container_request['output_uuid'],
                                                              api_client=self.api,
                                                              keep_client=self.keep_client)
        with cwl_output_collection.open('cwl.output.json') as cwl_output_file:
            cwl_output = json.load(cwl_output_file)
        if output_name in cwl_output:
            if isinstance(cwl_output[output_name], list):
                return [output['basename'] for output in cwl_output[output_name]]
            if isinstance(cwl_output[output_name], dict):
                return cwl_output[output_name]['basename']
        raise ValueError(f"Output {output_name} does not exist for task {task.container_request['uuid']}.")

    def get_tasks_by_name(self, project, task_name): # -> list(ArvadosTask):
        '''
        Get all processes (jobs) in a project with a specified name

        :param project: The project to search
        :param process_name: The name of the process to search for
        :return: List of container request uuids and associated containers
        '''
        # We must add priority>0 filter so we do not capture Cancelled jobs as Queued jobs.
        # According to Curii, 'Cancelled' on the UI = 'Queued' with priority=0, we are not interested in Cancelled
        # jobs here anyway, we will submit the job again
        tasks = []
        for container_request in arvados.util.keyset_list_all(
            self.api.container_requests().list,
            filters=[
                ["name", '=', task_name],
                ['owner_uuid', '=', project['uuid']], ['priority', '>', 0]
            ]
        ):
            # Get the container
            container = self.api.containers().get(uuid=container_request['container_uuid']).execute()
            tasks.append(ArvadosTask(container_request, container))
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

        # Get the output collection with name of output_directory_name
        search_result = self.api.collections().list(filters=[
            ["owner_uuid", "=", project["uuid"]],
            ["name", "=", output_directory_name]
            ]).execute()
        if len(search_result['items']) > 0:
            outputs_collection = search_result['items'][0]
        else:
            outputs_collection = self.api.collections().create(body={
                "owner_uuid": project["uuid"],
                "name": output_directory_name}).execute()
        outputs_collection = arvados.collection.Collection(outputs_collection['uuid'])

        # Get task output collection
        source_collection = arvados.collection.Collection(task.container_request["output_uuid"])

        # Copy the files from task output collection into /{output_directory_name}
        with source_collection.open('cwl.output.json') as cwl_output_file:
            cwl_output = json.load(cwl_output_file)

        for output_id in output_to_export:
            output_file = cwl_output[output_id]
            targetpath = output_file['location']
            # TODO: Before copying files, we should check if the file exists and is the same.
            # Can we check for md5 hash?
            # This method is replaced by stage_output_files
            outputs_collection.copy(targetpath, target_path=targetpath,
                    source_collection=source_collection, overwrite=True)

            if 'secondaryFiles' in output_file:
                for secondary_file in output_file['secondaryFiles']:
                    targetpath = secondary_file['location']
                    outputs_collection.copy(targetpath, target_path=targetpath,
                            source_collection=source_collection, overwrite=True)
        outputs_collection.save()

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
        with tempfile.NamedTemporaryFile() as parameter_file:
            with open(parameter_file.name, mode='w', encoding="utf-8") as fout:
                json.dump(parameters, fout)

            use_spot_instance = execution_settings.get('use_spot_instance', True) if execution_settings else True
            if use_spot_instance:
                cmd_spot_instance = "--enable-preemptible"
            else:
                cmd_spot_instance = "--disable-preemptible"

            cmd_str = ['arvados-cwl-runner', '--no-wait',
                    '--defer-download',
                    '--varying-url-params=AWSAccessKeyId,Signature,Expires',
                    '--prefer-cached-downloads',
                    '--debug',
                    cmd_spot_instance,
                    '--project-uuid', project['uuid'],
                    '--name', name,
                    workflow['uuid'],
                    parameter_file.name]
            try:
                self.logger.debug("Calling: %s", " ".join(cmd_str))
                runner_out = subprocess.check_output(cmd_str, stderr = subprocess.STDOUT)
                runner_log = runner_out.decode("UTF-8")
                container_request_uuid = list(filter(None, runner_log.split("\n")))[-1]
                return ArvadosTask({'uuid': container_request_uuid}, None)
            except subprocess.CalledProcessError as err:
                self.logger.error("ERROR LOG: %s", str(err))
                self.logger.error("ERROR LOG: %s", err.output)
            except IOError as err:
                self.logger.error("ERROR LOG: %s", str(err))
        return None

    ### Project methods
    def create_project(self, project_name, project_description, **kwargs):
        '''
        Create a project
        
        :param project_name: Name of the project
        :param project_description: Description of the project
        :param kwargs: Additional arguments for creating a project
        :return: Project object
        '''
        arvados_user = self.api.users().current().execute()
        project = self.api.groups().create(body={"owner_uuid": f'{arvados_user["uuid"]}',
                                                 "name": project_name,
                                                 "description": project_description,
                                                 "properties": {
                                                     "proj_owner": arvados_user["username"]
                                                 },
                                                 "group_class": "project"}
                                           ).execute()
        return project

    def delete_project_by_name(self, project_name):
        '''
        Delete a project on the platform 
        '''
        project = self.get_project_by_name(project_name)
        if project:
            self.api.groups().delete(uuid=project['uuid']).execute()

    def get_project(self):
        ''' Determine what project we are running in '''
        try:
            current_container = self.api.containers().current().execute()
            request = self.api.container_requests().list(filters=[
                    ["container_uuid", "=", current_container["uuid"]]
                ]).execute()
            return self.get_project_by_id(request["items"][0]['owner_uuid'])
        except arvados.errors.ApiError:
            return None

    def get_project_by_name(self, project_name):
        ''' Get a project by its name '''
        self.logger.debug("Searching for project %s", project_name)
        search_result = self.api.groups().list(filters=[["name", "=", project_name]]).execute()
        if len(search_result['items']) > 0:
            self.logger.debug("Found project %s", search_result['items'][0]['uuid'])
            return search_result['items'][0]
        self.logger.debug("Could not find project")
        return None

    def get_project_by_id(self, project_id):
        ''' Get a project by its id '''
        search_result = self.api.groups().list(filters=[["uuid", "=", project_id]]).execute()
        if len(search_result['items']) > 0:
            return search_result['items'][0]
        return None

    def get_project_users(self, project):
        ''' Return a list of user objects associated with a project '''
        users = []
        links = self.api.links().list(
            filters=[
                ["head_uuid", "=", project['uuid']]
            ]).execute()
        for link in links['items']:
            users.append(
                self.api.users().list(filters=[
                    ["uuid", "=", link['tail_uuid']]
                ]).execute()["items"][0]
            )
        return users

    ### User Methods
    def add_user_to_project(self, platform_user, project, permission):
        """
        Add a user to a project on the platform
        :param platform_user: platform user (from get_user)
        :param project: platform project
        :param permission: permission (permission="read|write|execute|admin")
        """
        a_permission = 'can_manage' if permission=="admin" else 'can_write' if permission=="write" else 'can_read'
        self.api.links().create(body={"link": {
                                            "link_class": "permission",
                                            "name": a_permission,
                                            "tail_uuid": platform_user['uuid'],
                                            "head_uuid": project['uuid']
                                       }
                                     }
                                ).execute()

    def get_user(self, user):
        """
        Get a user object from their (platform) user id or email address

        :param user: user id or email address
        :return: User object or None
        """
        if '@' in user:
            user_resp = self.api.users().list(filters=[["email","ilike",user]]).execute()
        else:
            user_resp = self.api.users().list(filters=[["username","ilike",user]]).execute()
        if len(user_resp['items']) > 0:
            return user_resp["items"][0]
        return None

    # Other methods
    def connect(self, **kwargs):
        ''' Connect to Arvados '''
        self.api = arvados.api_from_config(version='v1', apiconfig=self.api_config)
        self.keep_client = arvados.KeepClient(self.api)
        self.connected = True

    @classmethod
    def detect(cls):
        '''
        Detect if we are running in a Arvados environment
        '''
        if os.environ.get('ARVADOS_API_HOST', None):
            return True
        return False

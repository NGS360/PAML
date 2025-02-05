'''
Test copy_folder function using arvados-python-client=2.7.4 and arvados 3.0 server side
'''
import pathlib
import collections
import logging
import arvados
import os

# from cwl_platform import SUPPORTED_PLATFORMS, PlatformFactory
from cwl_platform.arvados_platform import ArvadosPlatform

logger = logging.getLogger(__name__)

# Connect to the platform
platform = ArvadosPlatform('Arvados')
platform.set_logger(logger)
platform.connect()

class StreamFileReader(arvados.arvfile.ArvadosFileReader):
    class _NameAttribute(str):
        # The Python file API provides a plain .name attribute.
        # Older SDK provided a name() method.
        # This class provides both, for maximum compatibility.
        def __call__(self):
            return self

    def __init__(self, arvadosfile):
        super(StreamFileReader, self).__init__(arvadosfile)
        self.name = self._NameAttribute(arvadosfile.name)

    def stream_name(self):
        return super().stream_name().lstrip("./")

def _all_files(root_collection):
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
#                yield (stream_path / name, item)
                 yield StreamFileReader(item)
            else:
                stream_queue.append(stream_path / name)

def _get_files_list_in_collection(collection_uuid, subdirectory_path=None):
    '''
    Get list of files in collection, if subdirectory_path is provided, return only files in that subdirectory.

    :param collection_uuid: uuid of the collection
    :param subdirectory_path: subdirectory path to filter files in the collection
    :return: list of files in the collection
    '''
    the_col = arvados.collection.CollectionReader(manifest_locator_or_text=collection_uuid)
    file_list = _all_files(the_col)
#    logger.error(list(file_list))
#    file_list = the_col.all_files()
    if subdirectory_path:
        return [fl for fl in file_list if os.path.basename(fl.stream_name()) == subdirectory_path]
    return list(file_list)

def copy_folder(platform, source_project, source_folder, destination_project):
    '''
    Copy folder to destination project

    :param source_project: The source project
    :param source_folder: The source folder
    :param destination_project: The destination project
    :return: The destination folder or None if not found
    '''
    
    logger.debug("Copying folder %s from project %s to project %s",
                     source_folder, source_project["uuid"], destination_project["uuid"])

    # 1. Get the source collection
    # The first element of the source_folder path is the name of the collection.
    if source_folder.startswith('/'):
        collection_name = source_folder.split('/')[1]
    else:
        collection_name = source_folder.split('/')[0]

    search_result = platform.api.collections().list(filters=[
        ["owner_uuid", "=", source_project["uuid"]],
        ["name", "=", collection_name]
        ]).execute()
    if len(search_result['items']) > 0:
        logger.debug("Found source collection %s in project %s", collection_name, source_project["uuid"])
        source_collection = search_result['items'][0]
    else:
        logger.error("Source collection %s not found in project %s", collection_name, source_project["uuid"])
        return None
    
    # 2. Get the destination project collection
    search_result = platform.api.collections().list(filters=[
        ["owner_uuid", "=", destination_project["uuid"]],
        ["name", "=", collection_name]
        ]).execute()
    if len(search_result['items']) > 0:
        logger.debug("Found destination folder %s in project %s", collection_name, destination_project["uuid"])
        destination_collection = search_result['items'][0]
    else:
        logger.debug("Destination folder %s not found in project %s, creating",
                            collection_name, destination_project["uuid"])
        destination_collection = platform.api.collections().create(body={
            "owner_uuid": destination_project["uuid"],
            "name": collection_name,
            "description": source_collection["description"],
            "preserve_version":True}).execute()

    # Copy the files from the reference project to the destination project
    logger.debug("Get list of files in source collection, %s", source_collection["uuid"])
    source_files = _get_files_list_in_collection(source_collection["uuid"])
    logger.debug("Getting list of files in destination collection, %s", destination_collection["uuid"])
    destination_files = list(_get_files_list_in_collection(destination_collection["uuid"]))

    source_collection = arvados.collection.Collection(source_collection["uuid"])
    target_collection = arvados.collection.Collection(destination_collection['uuid'])

#    logger.error(source_files)
    for source_file in source_files:
        source_path = f"{source_file.stream_name()}/{source_file.name()}"
        if source_path not in [f"{destination_file.stream_name()}/{destination_file.name()}"
                            for destination_file in destination_files]:
            target_collection.copy(source_path, target_path=source_path, source_collection=source_collection)
    target_collection.save()

    logger.debug("Done copying folder.")
    return destination_collection

destination_project = platform.get_project_by_id('xngs1-j7d0g-gzsxpu7lz1ewel1')
source_project = platform.get_project_by_name("RNA-Seq Reference")
source_folder = "/Reference_Files/GRCh38ERCC.ensembl91"
destination = copy_folder(platform, source_project, source_folder, destination_project)

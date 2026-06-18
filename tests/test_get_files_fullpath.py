'''
Integration Test demonstrating the differences between
get_files() and get_files_fullpath().

Key differences:
- get_files() returns (path, file_object) on SevenBridges, (path, file_id) on Arvados.
  On Arvados, files in subdirectories within a collection lose their subdirectory path.
- get_files_fullpath() returns (full_path, file_id) on all platforms,
  preserving the full internal directory structure.
'''
import unittest

from datetime import datetime
import random
import string
import os

from cwl_platform import SUPPORTED_PLATFORMS, PlatformFactory


def generate_random_file(prefix=''):
    '''Generate a temp file with random content for testing'''
    file_name = prefix + ''.join(random.choices(string.ascii_letters + string.digits, k=8)) + ".txt"
    with open(file_name, 'w', encoding='us-ascii') as f:
        f.write(''.join(random.choices(string.ascii_letters + string.digits, k=100)))
    return file_name


class TestGetFilesVsGetFilesFullpath(unittest.TestCase):
    '''
    Demonstrates the behavioral differences between get_files() and get_files_fullpath().
    '''

    def setUp(self):
        if 'INTEGRATION_TEST' not in os.environ:
            self.skipTest("Skipping live test. Set INTEGRATION_TEST=1 to run live tests")

        self.platforms = {}
        for platform_name in SUPPORTED_PLATFORMS:
            platform = PlatformFactory().get_platform(platform_name)
            platform.connect()
            self.platforms[platform_name] = platform
        self.project_name = f"TestGetFilesFullpath-{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}"
        self.local_files = []

    def tearDown(self):
        for _, platform in self.platforms.items():
            platform.delete_project_by_name(self.project_name)
        for f in self.local_files:
            if os.path.exists(f):
                os.remove(f)

    def _create_local_file(self, prefix=''):
        '''Helper to create a local file and track it for cleanup'''
        f = generate_random_file(prefix)
        self.local_files.append(f)
        return f

    def test_both_methods_return_same_paths_for_root_files(self):
        '''
        For files in the project root, both methods should return the same paths.
        '''
        local_file = self._create_local_file()

        for platform_name, platform in self.platforms.items():
            project = platform.create_project(
                self.project_name, 'Integration test for get_files vs get_files_fullpath')

            platform.upload_file(local_file, project)

            files = platform.get_files(project)
            files_fullpath = platform.get_files_fullpath(project)

            # Both should find the same file
            self.assertEqual(
                len(files), len(files_fullpath),
                f"[{platform_name}] get_files found {len(files)} files, "
                f"get_files_fullpath found {len(files_fullpath)}")

            # Paths should match
            paths = [path for path, _ in files]
            fullpaths = [path for path, _ in files_fullpath]
            self.assertEqual(
                paths, fullpaths,
                f"[{platform_name}] Paths differ for root files: "
                f"get_files={paths}, get_files_fullpath={fullpaths}")

    def test_both_methods_return_same_paths_for_subfolder_files(self):
        '''
        For files in a single-level subfolder, both methods should return
        the same paths.
        '''
        local_file = self._create_local_file()

        for platform_name, platform in self.platforms.items():
            project = platform.create_project(
                self.project_name, 'Integration test for get_files vs get_files_fullpath')

            platform.upload_file(local_file, project, dest_folder='/inputs')

            files = platform.get_files(project)
            files_fullpath = platform.get_files_fullpath(project)

            paths = [path for path, _ in files]
            fullpaths = [path for path, _ in files_fullpath]

            expected_path = f"/inputs/{local_file}"
            self.assertIn(
                expected_path, fullpaths,
                f"[{platform_name}] get_files_fullpath missing {expected_path}: {fullpaths}")
            self.assertIn(
                expected_path, paths,
                f"[{platform_name}] get_files missing {expected_path}: {paths}")

    def test_fullpath_preserves_nested_subdirectories(self):
        '''
        For files in nested subdirectories, get_files_fullpath() should preserve
        the full path. On Arvados, get_files() may flatten the subdirectory
        structure within a collection while get_files_fullpath() preserves it.
        '''
        local_file = self._create_local_file()

        for platform_name, platform in self.platforms.items():
            project = platform.create_project(
                self.project_name, 'Integration test for get_files vs get_files_fullpath')

            platform.upload_file(local_file, project, dest_folder='/inputs/run1/sample1')

            files_fullpath = platform.get_files_fullpath(project)
            fullpaths = [path for path, _ in files_fullpath]

            expected_path = f"/inputs/run1/sample1/{local_file}"
            self.assertIn(
                expected_path, fullpaths,
                f"[{platform_name}] get_files_fullpath should preserve nested path. "
                f"Expected {expected_path}, got {fullpaths}")

    def test_fullpath_returns_file_ids_not_objects(self):
        '''
        get_files_fullpath() should return file IDs (strings), not file objects.
        get_files() on SevenBridges returns file objects.
        '''
        local_file = self._create_local_file()

        for platform_name, platform in self.platforms.items():
            project = platform.create_project(
                self.project_name, 'Integration test for get_files vs get_files_fullpath')

            platform.upload_file(local_file, project, dest_folder='/data')

            files_fullpath = platform.get_files_fullpath(project)

            for path, file_id in files_fullpath:
                self.assertIsInstance(
                    file_id, str,
                    f"[{platform_name}] get_files_fullpath should return string file IDs, "
                    f"got {type(file_id)} for {path}")

    def test_filters_work_on_both_methods(self):
        '''
        Both methods should respect the same filter criteria.
        '''
        file1 = self._create_local_file(prefix='sample_')
        file2 = self._create_local_file(prefix='reference_')

        for platform_name, platform in self.platforms.items():
            project = platform.create_project(
                self.project_name, 'Integration test for get_files vs get_files_fullpath')

            platform.upload_file(file1, project, dest_folder='/data')
            platform.upload_file(file2, project, dest_folder='/data')

            # Filter by prefix
            files = platform.get_files(project, filters={'prefix': 'sample_'})
            files_fullpath = platform.get_files_fullpath(project, filters={'prefix': 'sample_'})

            self.assertEqual(
                len(files), 1,
                f"[{platform_name}] get_files with prefix filter should return 1 file, "
                f"got {len(files)}")
            self.assertEqual(
                len(files_fullpath), 1,
                f"[{platform_name}] get_files_fullpath with prefix filter should return 1 file, "
                f"got {len(files_fullpath)}")

    def test_folder_filter_works_on_both_methods(self):
        '''
        Both methods should correctly filter by folder.
        '''
        file1 = self._create_local_file()
        file2 = self._create_local_file()

        for platform_name, platform in self.platforms.items():
            project = platform.create_project(
                self.project_name, 'Integration test for get_files vs get_files_fullpath')

            platform.upload_file(file1, project, dest_folder='/inputs')
            platform.upload_file(file2, project, dest_folder='/outputs')

            # Filter to only /inputs
            files = platform.get_files(project, filters={'folder': '/inputs'})
            files_fullpath = platform.get_files_fullpath(project, filters={'folder': '/inputs'})

            self.assertEqual(
                len(files), 1,
                f"[{platform_name}] get_files with folder filter should return 1 file, "
                f"got {len(files)}: {[p for p, _ in files]}")
            self.assertEqual(
                len(files_fullpath), 1,
                f"[{platform_name}] get_files_fullpath with folder filter should return 1 file, "
                f"got {len(files_fullpath)}: {[p for p, _ in files_fullpath]}")


if __name__ == '__main__':
    unittest.main()

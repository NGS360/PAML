'''
Integration Test for PAML.get_files() to ensure all platforms work in the same way.

get_files() returns List[(full_path, file_ref)] where:
- full_path preserves the complete directory structure
- file_ref is a platform-native reference (SB file object or Arvados keep URI)
'''
import unittest

from datetime import datetime
import random
import string
import os

from cwl_platform import SUPPORTED_PLATFORMS, PlatformFactory

def generate_random_file():
    ''' Generate a temp file for testing '''
    file_name = ''.join(random.choices(string.ascii_letters + string.digits, k=8)) + ".txt"
    length = 100
    with open(file_name, 'w', encoding='us-ascii') as f:
        random_data = ''.join(random.choices(string.ascii_letters + string.digits, k=length))
        f.write(random_data)
    return file_name

class TestGetFiles(unittest.TestCase):
    ''' TestGetFiles '''
    def setUp(self):
        if 'INTEGRATION_TEST' not in os.environ:
            self.skipTest("Skipping live test. Set INTEGRATION_TEST=1 to run live tests")

        self.platforms = {}
        for platform_name in ['Arvados', 'SevenBridges']:
            platform = PlatformFactory().get_platform(platform_name)
            platform.connect()
            self.platforms[platform_name] = platform
        self.project_name = f"TestGetFiles-{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}"
        self.random_file = None

    def tearDown(self):
        for _, platform in self.platforms.items():
            platform.delete_project_by_name(self.project_name)
        if self.random_file:
            os.remove(self.random_file)
        return super().tearDown()

    def test_get_files_returns_zero_files(self):
        ''' Test that PAML.get_files returns an empty list for an empty project '''
        for _, platform in self.platforms.items():
            project = platform.create_project(self.project_name, 'Project for TestGetFiles integration test')
            self.assertIsNotNone(project, "Expected an empty project")

            files = platform.get_files(project)

            self.assertEqual(files, [])

    def test_get_files_returns_files_in_root_folder(self):
        '''
        Test that PAML.get_files returns all files in root folder of project.
        Arvados does not have a "root" folder collection.
        '''
        self.random_file = generate_random_file()

        for platform_name, platform in self.platforms.items():
            project = platform.create_project(self.project_name, 'Project for TestGetFiles integration test')
            self.assertIsNotNone(project, "Expected an empty project")

            file = platform.upload_file(self.random_file, project)

            # Handle Arvados case of no root folder
            if platform_name == 'Arvados':
                self.assertIsNone(file)
            else:
                files = platform.get_files(project)

                self.assertEqual(
                    len(files), 1,
                    f"Expected 1 file but found {len(files)}")

                actual_name, _ = files[0]
                self.assertEqual(
                    actual_name, f"/{self.random_file}",
                    f"Expected to find /{self.random_file}")

    def test_get_files_returns_files_in_subfolder(self):
        '''
        Test that PAML.get_files returns all files in a sub folder of project.
        '''
        self.random_file = generate_random_file()

        for platform_name, platform in self.platforms.items():
            project = platform.create_project(self.project_name, 'Project for TestGetFiles integration test')
            self.assertIsNotNone(project, "Expected an empty project")

            platform.upload_file(self.random_file, project, dest_folder='/inputs')

            files = platform.get_files(project, filters={'folder': '/inputs'})

            self.assertEqual(
                len(files), 1,
                f"Expected 1 file but found {len(files)} on {platform_name}")

            actual_name, _ = files[0]
            self.assertEqual(
                actual_name, f"/inputs/{self.random_file}",
                f"Expected to find /inputs/{self.random_file} on {platform_name} but found {actual_name} instead")

    def test_get_files_preserves_nested_subdirectories(self):
        '''
        Test that get_files() preserves subdirectory paths within collections.
        '''
        self.random_file = generate_random_file()

        for platform_name, platform in self.platforms.items():
            project = platform.create_project(self.project_name, 'Project for TestGetFiles integration test')
            self.assertIsNotNone(project, "Expected an empty project")

            platform.upload_file(self.random_file, project, dest_folder='/inputs/run1/sample1')

            files = platform.get_files(project)
            fullpaths = [path for path, _ in files]

            expected_path = f"/inputs/run1/sample1/{self.random_file}"
            self.assertIn(
                expected_path, fullpaths,
                f"[{platform_name}] get_files should preserve nested path. "
                f"Expected {expected_path}, got {fullpaths}")

    def test_get_files_filter_by_prefix(self):
        '''Test filtering by prefix'''
        self.random_file = generate_random_file()

        for platform_name, platform in self.platforms.items():
            project = platform.create_project(self.project_name, 'Project for TestGetFiles integration test')

            platform.upload_file(self.random_file, project, dest_folder='/data')

            files = platform.get_files(project, filters={'prefix': self.random_file[:4]})

            self.assertEqual(
                len(files), 1,
                f"[{platform_name}] Expected 1 file with prefix filter, got {len(files)}")

            files_no_match = platform.get_files(project, filters={'prefix': 'nonexistent_'})
            self.assertEqual(
                len(files_no_match), 0,
                f"[{platform_name}] Expected 0 files with non-matching prefix")

    def test_get_files_nested_dir_with_folder_filter(self):
        '''
        Test that uploading to a nested directory (inputs/run1/sample1) and
        filtering by that folder returns the correct path and file_id.
        '''
        self.random_file = generate_random_file()

        for platform_name, platform in self.platforms.items():
            project = platform.create_project(self.project_name, 'Project for TestGetFiles integration test')
            self.assertIsNotNone(project, "Expected an empty project")

            platform.upload_file(self.random_file, project, dest_folder='/inputs/run1/sample1')

            files = platform.get_files(project, filters={'folder': '/inputs/run1/sample1'})

            self.assertEqual(
                len(files), 1,
                f"[{platform_name}] Expected 1 file but found {len(files)}: "
                f"{[p for p, _ in files]}")

            actual_path, _ = files[0]
            self.assertEqual(
                actual_path, f"/inputs/run1/sample1/{self.random_file}",
                f"[{platform_name}] Expected /inputs/run1/sample1/{self.random_file} "
                f"but got {actual_path}")

if __name__ == '__main__':
    unittest.main()

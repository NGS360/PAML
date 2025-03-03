import unittest

from datetime import datetime
import random
import string
import os

from cwl_platform import SUPPORTED_PLATFORMS, PlatformFactory

def generate_random_file():
    # Random file name
    file_name = ''.join(random.choices(string.ascii_letters + string.digits, k=8)) + ".txt"
    # Write random data
    length = 100
    with open(file_name, 'w') as f:
        random_data = ''.join(random.choices(string.ascii_letters + string.digits, k=length))
        f.write(random_data)
    return file_name

class TestGetFiles(unittest.TestCase):
    def setUp(self):
        self.platforms = {
            'Arvados': None,
            'SevenBridges': None
        }
        for platform_name in self.platforms.keys():
            platform = PlatformFactory().get_platform(platform_name)
            platform.connect()
            self.platforms[platform_name] = platform
        self.project_name = f"TestGetFiles-{datetime.now().strftime('%Y-%m-%d-%H-%M')}"
        self.random_file = None

    def tearDown(self):
        # Remove the test project from the platform
        for _, platform in self.platforms.items():
            platform.delete_project_by_name(self.project_name)
        if self.random_file:
            os.remove(self.random_file)
        return super().tearDown()

    def test_get_files_returns_zero_files(self):
        ''' Test that PAML.get_files an empty list for an empty project, on all platforms '''
        for platform_name, platform in self.platforms.items():
            # create project
            project = platform.create_project(self.project_name, 'Project for TestGetFiles integration test')
            self.assertIsNotNone(project, "Expected an empty project")

            # Test
            files = platform.get_files(project)

            # Check results
            self.assertEqual(files, [])

    def test_get_files_returns_files_in_root_folder(self):
        '''
        Test that PAML.get_files returns all files in root folder of project.
        Arvados does not have a "root" folder collection.
        '''
        self.random_file = generate_random_file()

        for platform_name, platform in self.platforms.items():
            # create project
            project = platform.create_project(self.project_name, 'Project for TestGetFiles integration test')
            self.assertIsNotNone(project, "Expected an empty project")

            # Add files to root folder
            file = platform.upload_file(self.random_file, project)

            # Handle Arvados case of no root folder
            if platform_name == 'Arvados':
                self.assertIsNone(file)
            else:
                # Test
                files = platform.get_files(project)

                # Assert 1 file was returned
                self.assertEqual(
                    len(files), 1,
                    f"Expected 1 file but found {len(files)}")

                # Assert file is /sample_sheet.txt
                actual_name, _ = files[0]
                self.assertEqual(
                    actual_name, f"/{self.random_file}",
                    f"Expected to find /{self.random_file}")

    def test_get_files_returns_files_in_subfolder(self):
        '''
        Test that PAML.get_files returns all files in a sub folder of project.
        '''

        for platform_name, platform in self.platforms.items():
            # create project
            project = platform.create_project(self.project_name, 'Project for TestGetFiles integration test')
            self.assertIsNotNone(project, "Expected an empty project")

            # Add files to subfolder
            file = platform.upload_file(self.random_file, project, dest_folder='/inputs')

            # Test
            files = platform.get_files(project)

            # Assert 1 file was returned
            self.assertEqual(
                len(files), 1,
                f"Expected 1 file but found {len(files)}")

            # Assert file is /sample_sheet.txt
            actual_name, _ = files[0]
            self.assertEqual(
                actual_name, f"/inputs/{self.random_file}",
                f"Expected to find /inputs/{self.random_file} on {platform_name} but found {actual_name} instead")

    def Xtest_handle_mulitple_output_file_matches(self):
        '''
        Test that export_results can handle the case when multiple
        WES_output_files.txt exist:
        /WES_output_files.txt
        /_1_WES_output_files.txt
        '''

if __name__ == '__main__':
    unittest.main()

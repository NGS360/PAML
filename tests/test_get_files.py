import unittest

from datetime import datetime

from cwl_platform import SUPPORTED_PLATFORMS, PlatformFactory

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

    def tearDown(self):
        # Remove the test project from the platform
        for _, platform in self.platforms.items():
            platform.delete_project_by_name(self.project_name)
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

    def Xtest_get_files_returns_files_in_root_folder(self):
        '''
        Test that PAML.get_files works correctly on all platforms

        Arvados does not have a "root" folder collection
        '''

        for platform_name, platform in self.platforms.items():
            # create project
            project = platform.create_project(
                f'{self.project_id}-{self.assay}',
                'Project for export_results unit test'
            )
            # assert the project does exist on the platform
            self.assertIsNotNone(
                project,
                f'{self.project_id}-{self.assay} should exist on {platform_name} before test is run.'
            )

            # Add files to root folder
            file = platform.upload_file("test_data/sample_sheet.txt", project)
            
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
                    actual_name, "/sample_sheet.txt",
                    "Expected to find /sample_sheet.txt")

    def Xtest_get_files_returns_files_in_subfolder(self):
        '''
        Test that PAML.get_files works correctly on all platforms
        '''

        for platform_name, platform in self.platforms.items():
            # create project
            project = platform.create_project(
                f'{self.project_id}-{self.assay}',
                'Project for export_results unit test'
            )
            # assert the project does exist on the platform
            self.assertIsNotNone(
                project,
                f'{self.project_id}-{self.assay} should exist on {platform_name} before test is run.'
            )

            # Add files to root folder
            file = platform.upload_file("test_data/sample_sheet.txt", project, dest_folder='/inputs')

            # Test
            files = platform.get_files(project)

            # Assert 1 file was returned
            self.assertEqual(
                len(files), 1,
                f"Expected 1 file but found {len(files)}")

            # Assert file is /sample_sheet.txt
            actual_name, _ = files[0]
            self.assertEqual(
                actual_name, "/inputs/sample_sheet.txt",
                f"Expected to find /inputs/sample_sheet.txt on {platform_name} but found {actual_name} instead")

    def Xtest_handle_mulitple_output_file_matches(self):
        '''
        Test that export_results can handle the case when multiple
        WES_output_files.txt exist:
        /WES_output_files.txt
        /_1_WES_output_files.txt
        '''

if __name__ == '__main__':
    unittest.main()

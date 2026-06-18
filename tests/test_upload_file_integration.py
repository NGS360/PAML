'''
Integration Test for SevenBridgesPlatform.upload_file()
to verify behavior against the live API.
'''
import unittest

from datetime import datetime
import random
import string
import os

from cwl_platform import PlatformFactory


def generate_random_file(prefix=''):
    '''Generate a temp file with random content for testing'''
    file_name = prefix + ''.join(random.choices(string.ascii_letters + string.digits, k=8)) + ".txt"
    with open(file_name, 'w', encoding='us-ascii') as f:
        f.write(''.join(random.choices(string.ascii_letters + string.digits, k=100)))
    return file_name


class TestUploadFileIntegration(unittest.TestCase):
    '''Integration tests for upload_file'''

    def setUp(self):
        if 'INTEGRATION_TEST' not in os.environ:
            self.skipTest("Skipping live test. Set INTEGRATION_TEST=1 to run live tests")

        self.platform = PlatformFactory().get_platform('SevenBridges')
        self.platform.connect()
        self.project_name = f"TestUploadFile-{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}"
        self.project = self.platform.create_project(
            self.project_name, 'Integration test for upload_file')
        self.local_files = []

    def tearDown(self):
        if hasattr(self, 'platform') and hasattr(self, 'project_name'):
            self.platform.delete_project_by_name(self.project_name)
        for f in self.local_files:
            if os.path.exists(f):
                os.remove(f)

    def _create_local_file(self, prefix=''):
        '''Helper to create a local file and track it for cleanup'''
        f = generate_random_file(prefix)
        self.local_files.append(f)
        return f

    def test_upload_to_root(self):
        '''Upload a file to project root and verify it exists'''
        local_file = self._create_local_file()

        file_id = self.platform.upload_file(local_file, self.project)

        self.assertIsNotNone(file_id)
        files = self.platform.get_files(self.project)
        file_names = [name for name, _ in files]
        self.assertIn(f"/{local_file}", file_names)

    def test_upload_to_subfolder(self):
        '''Upload a file to a subfolder and verify it exists there'''
        local_file = self._create_local_file()

        file_id = self.platform.upload_file(
            local_file, self.project, dest_folder='/inputs')

        self.assertIsNotNone(file_id)
        files = self.platform.get_files(self.project, filters={'folder': '/inputs'})
        file_names = [name for name, _ in files]
        self.assertIn(f"/inputs/{local_file}", file_names)

    def test_upload_to_nested_subfolder(self):
        '''Upload a file to a nested subfolder'''
        local_file = self._create_local_file()

        file_id = self.platform.upload_file(
            local_file, self.project, dest_folder='/inputs/run1')

        self.assertIsNotNone(file_id)
        files = self.platform.get_files(self.project, filters={'folder': '/inputs/run1'})
        file_names = [name for name, _ in files]
        self.assertIn(f"/inputs/run1/{local_file}", file_names)

    def test_upload_with_destination_filename(self):
        '''Upload a file with a renamed destination filename'''
        local_file = self._create_local_file()
        dest_name = 'renamed_output.txt'

        file_id = self.platform.upload_file(
            local_file, self.project,
            dest_folder='/outputs',
            destination_filename=dest_name)

        self.assertIsNotNone(file_id)
        files = self.platform.get_files(self.project, filters={'folder': '/outputs'})
        file_names = [name for name, _ in files]
        self.assertIn(f"/outputs/{dest_name}", file_names)

    def test_upload_no_overwrite_returns_existing_id(self):
        '''Uploading the same file twice without overwrite should return the same id'''
        local_file = self._create_local_file()

        first_id = self.platform.upload_file(
            local_file, self.project, dest_folder='/inputs')
        second_id = self.platform.upload_file(
            local_file, self.project, dest_folder='/inputs', overwrite=False)

        self.assertEqual(first_id, second_id)

    def test_upload_with_overwrite(self):
        '''Uploading with overwrite=True should succeed even if file exists'''
        local_file = self._create_local_file()

        first_id = self.platform.upload_file(
            local_file, self.project, dest_folder='/inputs')
        second_id = self.platform.upload_file(
            local_file, self.project, dest_folder='/inputs', overwrite=True)

        self.assertIsNotNone(second_id)

    def test_upload_to_root_via_slash(self):
        '''dest_folder="/" should behave the same as dest_folder=None (upload to root)'''
        local_file = self._create_local_file()

        file_id = self.platform.upload_file(
            local_file, self.project, dest_folder='/')

        self.assertIsNotNone(file_id)
        files = self.platform.get_files(self.project)
        file_names = [name for name, _ in files]
        self.assertIn(f"/{local_file}", file_names)


if __name__ == '__main__':
    unittest.main()

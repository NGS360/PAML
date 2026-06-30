'''
Integration Test for SevenBridgesPlatform._find_or_create_path()
to verify behavior against the live API.
'''
import unittest

from datetime import datetime
import os

from cwl_platform import PlatformFactory


class TestFindOrCreatePath(unittest.TestCase):
    '''Integration tests for _find_or_create_path'''

    def setUp(self):
        if 'INTEGRATION_TEST' not in os.environ:
            self.skipTest("Skipping live test. Set INTEGRATION_TEST=1 to run live tests")

        self.platform = PlatformFactory().get_platform('SevenBridges')
        self.platform.connect()
        self.project_name = f"TestFindOrCreatePath-{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}"
        self.project = self.platform.create_project(
            self.project_name, 'Integration test for _find_or_create_path')

    def tearDown(self):
        if hasattr(self, 'platform') and hasattr(self, 'project_name'):
            self.platform.delete_project_by_name(self.project_name)

    def test_root_path_returns_none(self):
        '''Root path "/" should return None since there is no folder to create'''
        result = self.platform._find_or_create_path(self.project, '/')
        self.assertIsNone(result)

    def test_creates_single_folder(self):
        '''A single folder path should create and return that folder'''
        result = self.platform._find_or_create_path(self.project, '/inputs')
        self.assertIsNotNone(result)
        self.assertEqual(result.name, 'inputs')

    def test_creates_nested_path(self):
        '''A nested path should create all intermediate folders'''
        result = self.platform._find_or_create_path(self.project, '/inputs/sample1')
        self.assertIsNotNone(result)
        self.assertEqual(result.name, 'sample1')

    def test_existing_folder_is_reused(self):
        '''Calling twice with the same path should return the same folder, not create a duplicate'''
        first = self.platform._find_or_create_path(self.project, '/outputs')
        second = self.platform._find_or_create_path(self.project, '/outputs')
        self.assertEqual(first.id, second.id)

    def test_partially_existing_path(self):
        '''If parent exists but child does not, only the child should be created'''
        self.platform._find_or_create_path(self.project, '/results')
        result = self.platform._find_or_create_path(self.project, '/results/run1')
        self.assertIsNotNone(result)
        self.assertEqual(result.name, 'run1')


if __name__ == '__main__':
    unittest.main()

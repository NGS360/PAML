'''
Test Module for AWS Omics Platform
'''
import unittest
import os
import mock
from mock import MagicMock, patch

from cwl_platform.omics_platform import OmicsPlatform

class TestOmicsPlaform(unittest.TestCase):
    '''
    Test Class for Omics Platform
    '''
    def setUp(self) -> None:
        self.platform = OmicsPlatform('Omics')
        return super().setUp()

    def runTest(self):
        pass

if __name__ == '__main__':
    unittest.main()

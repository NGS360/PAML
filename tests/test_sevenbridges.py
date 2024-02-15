'''
Test Module for SevenBridges Platform
'''
import unittest
import mock
import os

from cwl_platform.sevenbridges_platform import SevenBridgesPlatform

class TestSevenBridgesPlaform(unittest.TestCase):
    '''
    Test Class for SevenBridges Platform
    '''
    def setUp(self) -> None:
        os.environ['SESSION_ID'] = 'dummy'
        self.platform = SevenBridgesPlatform('SevenBridges')
        return super().setUp()

    @mock.patch('sevenbridges.Api')
    def test_connect(self, mock_api_client):
        ''' Test connect method '''
        self.platform.connect()
        self.assertTrue(self.platform.connected)

if __name__ == '__main__':
    unittest.main()


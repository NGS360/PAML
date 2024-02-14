'''
Test Module for SevenBridges Platform
'''
import unittest
from src.sevenbridges_platform import SevenBridgesPlatform

class TestSevenBridgesPlaform(unittest.TestCase):
    '''
    Test Class for SevenBridges Platform
    '''
    def setUp(self) -> None:
        self.platform = SevenBridgesPlatform('SevenBridges')
        return super().setUp()

    def test_connect(self):
        ''' Test connect method '''
        self.platform.connect()
        self.assertTrue(self.platform.connected)

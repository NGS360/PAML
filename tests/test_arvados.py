'''
Test Module for Arvados Platform
'''
import unittest
from src.arvados_platform import ArvadosPlatform

class TestArvadosPlaform(unittest.TestCase):
    '''
    Test Class for Arvados Platform
    '''
    def setUp(self) -> None:
        self.platform = ArvadosPlatform('Arvados')
        return super().setUp()

    def test_connect(self):
        ''' Test connect method '''
        self.platform.connect()
        self.assertTrue(self.platform.connected)

'''
Test Module for Arvados Platform
'''
import unittest
import mock

from cwl_platform.arvados_platform import ArvadosPlatform

class TestArvadosPlaform(unittest.TestCase):
    '''
    Test Class for Arvados Platform
    '''
    def setUp(self) -> None:
        self.platform = ArvadosPlatform('Arvados')
        return super().setUp()

    @mock.patch('arvados.api')
    def test_connect(self, mock_arvados_api):
        ''' Test connect method '''
        self.platform.connect()
        self.assertTrue(self.platform.connected)

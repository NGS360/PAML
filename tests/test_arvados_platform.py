'''
Test Module for Arvados Platform
'''
import unittest
import mock
from mock import MagicMock

from cwl_platform.arvados_platform import ArvadosPlatform, ArvadosTask

class TestArvadosPlaform(unittest.TestCase):
    '''
    Test Class for Arvados Platform
    '''
    def setUp(self) -> None:
        self.platform = ArvadosPlatform('Arvados')
        return super().setUp()

    @mock.patch("arvados.api_from_config")
    @mock.patch("arvados.KeepClient")
    def test_connect(self, mock_keep_client, mock_arvados_api):
        ''' Test connect method '''
        mock_keep_client.return_value = MagicMock()
        mock_arvados_api.return_value = MagicMock()
        self.platform.api_config = {
            'ARVADOS_API_HOST': 'host',
            'ARVADOS_API_TOKEN': 'token'
        }
        self.platform.connect()
        self.assertTrue(self.platform.connected)

    @mock.patch("arvados.api_from_config")
    def test_delete_task(self, mock_arvados_api):
        ''' Test delete_task method '''
        # Set up mocks
        mock_arvados_api.return_value = MagicMock()
        task = ArvadosTask(container_request={"uuid": "12345"}, container={"uuid": "67890"})
        # Test
        self.platform.connect()
        self.platform.delete_task(task)
        # Assert
        mock_arvados_api.return_value.container_requests().delete.assert_called_once_with(uuid="12345")

if __name__ == '__main__':
    unittest.main()
